//! Reusable waiter slots with atomic ownership; no list-wide lock.
//!
//! Pages are appended, never unlinked, and freed only with the list. A future's
//! token keeps its slot unavailable until cancellation or acknowledgement of a
//! notification. A notifier takes exclusive ownership of the waker with a CAS;
//! cancellation can mark that ownership abandoned without waiting for it.
//!
//! Registration and close perform RMWs on the same `closing` flag. Either close
//! acquires the published registration, or registration acquires the already
//! published channel close before its caller rechecks the operation. This also
//! covers a newly appended page. The queue's parking RMW/recheck protocol
//! separately covers ordinary send/receive notifications.

use crate::sync::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use crate::sync::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, UnsafeCell};
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::ptr;
use std::task::Waker;

#[cfg(not(feature = "loom"))]
const PAGE_SLOTS: usize = 8;
#[cfg(feature = "loom")]
const PAGE_SLOTS: usize = 2;

const FREE: u64 = 0;
const WRITING: u64 = 1;
const WAITING: u64 = 2;
const NOTIFYING: u64 = 3;
const NOTIFIED: u64 = 4;
const CANCELLED: u64 = 5;
const STATE_MASK: u64 = 7;

struct Slot {
    // The high bits are a registration ticket; the low bits encode ownership.
    state: AtomicU64,
    waker: UnsafeCell<MaybeUninit<Waker>>,
}

// SAFETY: only the writer, a successful WAITING -> WRITING cancellation, or a
// successful WAITING -> NOTIFYING notification accesses the payload. Publishing
// WAITING transfers the initialized waker; FREE is published only after removal.
// Cancellation of NOTIFYING changes only the atomic state, not the payload.
unsafe impl Sync for Slot {}

impl Slot {
    fn new() -> Self {
        Self {
            state: AtomicU64::new(FREE),
            waker: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// The caller exclusively owns an initialized payload.
    unsafe fn take_waker(&self) -> Waker {
        self.waker.with_mut(|p| (*p).assume_init_read())
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        // Exclusive list destruction: there cannot be a writer/notifier in flight.
        // A forgotten future may leave WAITING behind; a forgotten notified token
        // leaves NOTIFIED with an already moved-out payload.
        let state = self.state.load(Relaxed) & STATE_MASK;
        debug_assert!(matches!(state, FREE | WAITING | NOTIFIED));
        if state == WAITING {
            // SAFETY: the list is exclusively owned and WAITING holds one waker.
            unsafe { drop(self.take_waker()) };
        }
    }
}

struct Page {
    index: usize,
    slots: [Slot; PAGE_SLOTS],
    next: AtomicPtr<Page>,
}

impl Page {
    fn new(index: usize) -> Self {
        Self {
            index,
            slots: std::array::from_fn(|_| Slot::new()),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }

    fn next(&self) -> Option<&Page> {
        let next = self.next.load(SeqCst);
        // SAFETY: published pages remain allocated until exclusive list Drop.
        unsafe { next.as_ref() }
    }

    fn next_or_append(&self) -> &Page {
        if let Some(next) = self.next() {
            return next;
        }
        let new = Box::into_raw(Box::new(Page::new(self.index + 1)));
        let next = match self
            .next
            .compare_exchange(ptr::null_mut(), new, SeqCst, SeqCst)
        {
            Ok(_) => new,
            Err(existing) => {
                // SAFETY: our empty candidate was never published.
                unsafe { drop(Box::from_raw(new)) };
                existing
            }
        };
        // SAFETY: either CAS winner published a page that lives as long as the list.
        unsafe { &*next }
    }
}

pub(crate) struct WaiterToken {
    // One-based index gives Option<WaiterToken> a niche without storing pointers
    // into the embedded first page (which can move before a channel is shared).
    slot: NonZeroUsize,
    ticket: u64,
}

// Page and slot are independent hints, so racing updates can mix them. Every
// combination still names a valid slot. Null always names the embedded page,
// keeping these hints valid when a privately owned list is moved.
struct Cursor {
    page: AtomicPtr<Page>,
    slot: AtomicUsize,
}
impl Cursor {
    fn new() -> Self {
        Self {
            page: AtomicPtr::new(ptr::null_mut()),
            slot: AtomicUsize::new(0),
        }
    }
}

#[derive(Clone, Copy)]
struct Position<'a> {
    page: &'a Page,
    slot: usize,
}
impl<'a> Position<'a> {
    fn advance(&mut self, first: &'a Page) {
        self.slot += 1;
        if self.slot == PAGE_SLOTS {
            self.slot = 0;
            self.page = self.page.next().unwrap_or(first);
        }
    }
    fn same(&self, other: Self) -> bool {
        ptr::eq(self.page, other.page) && self.slot == other.slot
    }
}

pub(crate) struct WaiterList {
    first: Page,
    closing: AtomicBool,
    register_cursor: Cursor,
    notify_cursor: Cursor,
}

/// Exclusive ownership of a waker removed from the set of selectable waiters.
struct Notification<'a> {
    slot: &'a Slot,
    ticket: u64,
}

impl Notification<'_> {
    fn wake(self, count: &AtomicUsize) {
        count.fetch_sub(1, SeqCst);
        // SAFETY: the successful WAITING -> NOTIFYING CAS transferred the waker
        // to this notifier. A racing cancellation cannot read or drop it.
        let waker = unsafe { self.slot.take_waker() };
        let old = self.slot.state.swap(self.ticket | NOTIFIED, SeqCst);
        debug_assert!(old == (self.ticket | NOTIFYING) || old == (self.ticket | CANCELLED));
        if old & STATE_MASK == CANCELLED {
            // The token was surrendered to us. No other operation can free this
            // slot until we do, so it cannot yet have been reused.
            self.slot.state.store(FREE, SeqCst);
        }
        // When old was NOTIFYING, the token owner may already have freed/reused
        // the slot. Never touch it again. User code runs outside slot ownership.
        waker.wake();
    }
}

impl WaiterList {
    pub(crate) fn new() -> Self {
        Self {
            first: Page::new(0),
            closing: AtomicBool::new(false),
            register_cursor: Cursor::new(),
            notify_cursor: Cursor::new(),
        }
    }

    fn position<'a>(&'a self, cursor: &Cursor) -> Position<'a> {
        let page = cursor.page.load(Acquire);
        // SAFETY: a non-null hint is an appended page, retained until list Drop.
        let page = if page.is_null() {
            &self.first
        } else {
            unsafe { &*page }
        };
        Position {
            page,
            slot: cursor.slot.load(Relaxed) % PAGE_SLOTS,
        }
    }

    fn save_position(&self, cursor: &Cursor, position: Position<'_>) {
        let page = if ptr::eq(position.page, &self.first) {
            ptr::null_mut()
        } else {
            position.page as *const Page as *mut Page
        };
        cursor.page.store(page, Release);
        cursor.slot.store(position.slot, Relaxed);
    }

    #[cold]
    pub(crate) fn register(
        &self,
        count: &AtomicUsize,
        next_id: &AtomicU64,
        token: &mut Option<WaiterToken>,
        waker: &Waker,
    ) {
        // Every future consumes its previous registration at the start of poll.
        debug_assert!(token.is_none());
        // Cloning can invoke arbitrary user code or panic. Do it before owning a
        // slot. No allocation or user callback occurs between claiming/publishing.
        let waker = waker.clone();
        let ticket = next_id.fetch_add(1, Relaxed).wrapping_shl(3);
        let mut position = self.position(&self.register_cursor);
        loop {
            let start = position;
            let mut tail = &self.first;
            loop {
                let slot = &position.page.slots[position.slot];
                let index = NonZeroUsize::new(position.page.index * PAGE_SLOTS + position.slot + 1)
                    .unwrap();
                if slot.state.load(Relaxed) == FREE
                    && slot
                        .state
                        .compare_exchange(FREE, ticket | WRITING, SeqCst, SeqCst)
                        .is_ok()
                {
                    position.advance(&self.first);
                    self.save_position(&self.register_cursor, position);
                    // SAFETY: FREE -> WRITING grants exclusive access to this slot.
                    slot.waker.with_mut(|p| unsafe {
                        (*p).write(waker);
                    });
                    // Count first: a notifier cannot decrement before this increment.
                    count.fetch_add(1, SeqCst);
                    slot.state.store(ticket | WAITING, SeqCst);
                    // Paired with close's RMW: an earlier close becomes visible
                    // to the caller's recheck, a later close sees this slot/page.
                    self.closing.fetch_or(false, AcqRel);
                    *token = Some(WaiterToken {
                        slot: index,
                        ticket,
                    });
                    return;
                }
                if position.slot + 1 == PAGE_SLOTS && position.page.next().is_none() {
                    tail = position.page;
                }
                position.advance(&self.first);
                if position.same(start) {
                    break;
                }
            }
            // Only grow after a full cycle found no free slot. A racing appender
            // may already have supplied the next page; next_or_append adopts it.
            position = Position {
                page: tail.next_or_append(),
                slot: 0,
            };
        }
    }

    fn slot(&self, token: &WaiterToken) -> &Slot {
        let index = token.slot.get() - 1;
        let mut page = &self.first;
        for _ in 0..index / PAGE_SLOTS {
            page = page.next().expect("registered waiter page");
        }
        &page.slots[index % PAGE_SLOTS]
    }

    #[cold]
    pub(crate) fn unregister(
        &self,
        count: &AtomicUsize,
        token: &mut Option<WaiterToken>,
        forward: bool,
    ) {
        let Some(token) = token.take() else { return };
        let slot = self.slot(&token);
        loop {
            let state = slot.state.load(SeqCst);
            debug_assert_eq!(state & !STATE_MASK, token.ticket);
            match state & STATE_MASK {
                WAITING => {
                    if slot
                        .state
                        .compare_exchange(state, token.ticket | WRITING, SeqCst, SeqCst)
                        .is_err()
                    {
                        continue;
                    }
                    count.fetch_sub(1, SeqCst);
                    // SAFETY: our CAS won ownership before any notifier could.
                    let waker = unsafe { slot.take_waker() };
                    slot.state.store(FREE, SeqCst);
                    // Dropping a user waker may re-enter the channel or panic.
                    drop(waker);
                    return;
                }
                NOTIFYING => {
                    if slot
                        .state
                        .compare_exchange(state, token.ticket | CANCELLED, SeqCst, SeqCst)
                        .is_err()
                    {
                        continue;
                    }
                    // The notifier reclaims our slot when it resumes. Forward now,
                    // so a paused notifier cannot delay cancellation's hand-off.
                    if forward {
                        self.notify_one(count);
                    }
                    return;
                }
                NOTIFIED => {
                    // The waker has been moved out, and only our token can free
                    // this slot. The notifier no longer accesses its payload/state.
                    slot.state.store(FREE, SeqCst);
                    if forward {
                        self.notify_one(count);
                    }
                    return;
                }
                _ => unreachable!("waiter token no longer owns its slot"),
            }
        }
    }

    fn claim(&self) -> Option<Notification<'_>> {
        let mut position = self.position(&self.notify_cursor);
        let start = position;
        loop {
            let slot = &position.page.slots[position.slot];
            let state = slot.state.load(SeqCst);
            let ticket = state & !STATE_MASK;
            if state & STATE_MASK == WAITING
                && slot
                    .state
                    .compare_exchange(state, ticket | NOTIFYING, SeqCst, SeqCst)
                    .is_ok()
            {
                position.advance(&self.first);
                self.save_position(&self.notify_cursor, position);
                return Some(Notification { slot, ticket });
            }
            position.advance(&self.first);
            if position.same(start) {
                return None;
            }
        }
    }

    /// Rotates through the available slots. Concurrent registrations/notifications
    /// do not promise FIFO waiter selection; the message queue's FIFO is unchanged.
    #[cold]
    pub(crate) fn notify_one(&self, count: &AtomicUsize) {
        if let Some(notification) = self.claim() {
            notification.wake(count);
        }
    }

    #[cold]
    pub(crate) fn notify_all(&self, count: &AtomicUsize) {
        // Used only for channel close, after Inner::closed has been published.
        // The RMW acquires every registration that completed its handshake first.
        self.closing.swap(true, AcqRel);
        while let Some(notification) = self.claim() {
            notification.wake(count);
        }
    }

    #[cold]
    pub(crate) fn notify_many(&self, count: &AtomicUsize, limit: usize) {
        for _ in 0..limit {
            match self.claim() {
                Some(notification) => notification.wake(count),
                None => break,
            }
        }
    }
}

impl Drop for WaiterList {
    fn drop(&mut self) {
        // Exclusive ownership of the entire page chain. The embedded first page
        // is dropped normally after this method; heap pages are dropped iteratively.
        let mut next = self.first.next.load(Relaxed);
        while !next.is_null() {
            // SAFETY: each appended page came from Box, was never unlinked and is
            // reachable by exactly one predecessor. No concurrent access remains.
            let page = unsafe { Box::from_raw(next) };
            next = page.next.load(Relaxed);
            drop(page);
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize as Count, Ordering::Relaxed};
    use std::sync::Arc;
    use std::task::Wake;

    struct Counter(Count);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Relaxed);
        }
    }

    fn pages(list: &WaiterList) -> usize {
        let mut n = 1;
        let mut page = &list.first;
        while let Some(next) = page.next() {
            n += 1;
            page = next;
        }
        n
    }

    #[test]
    fn notify_each_registration_once_and_reuse_after_cancellation() {
        let list = WaiterList::new();
        let count = AtomicUsize::new(0);
        let next = AtomicU64::new(1);
        let n = PAGE_SLOTS * 3 + 1;
        for round in 0..16 {
            let mut tokens: Vec<_> = (0..n).map(|_| None).collect();
            let wakes: Vec<_> = (0..n).map(|_| Arc::new(Counter(Count::new(0)))).collect();
            for (token, wake) in tokens.iter_mut().zip(&wakes) {
                list.register(&count, &next, token, &Waker::from(wake.clone()));
            }
            assert_eq!(pages(&list), 4);
            assert_eq!(count.load(Relaxed), n);
            if round % 2 == 0 {
                for i in 0..n {
                    list.notify_one(&count);
                    assert_eq!(
                        wakes.iter().map(|w| w.0.load(Relaxed)).sum::<usize>(),
                        i + 1
                    );
                    assert!(wakes.iter().all(|w| w.0.load(Relaxed) <= 1));
                    assert_eq!(count.load(Relaxed), n - i - 1);
                }
            }
            for token in &mut tokens {
                list.unregister(&count, token, false);
            }
            assert_eq!(count.load(Relaxed), 0);
        }
    }

    #[test]
    fn cancelled_claim_forwards_without_waiting_for_notifier() {
        let list = WaiterList::new();
        let count = AtomicUsize::new(0);
        let next = AtomicU64::new(1);
        let mut a = None;
        let mut b = None;
        let aw = Arc::new(Counter(Count::new(0)));
        let bw = Arc::new(Counter(Count::new(0)));
        list.register(&count, &next, &mut a, &Waker::from(aw.clone()));
        list.register(&count, &next, &mut b, &Waker::from(bw.clone()));
        // Pause the selected notifier before it moves the waker or adjusts count.
        let paused = list.claim().unwrap();
        list.unregister(&count, &mut a, true);
        assert_eq!(bw.0.load(Relaxed), 1);
        assert_eq!(aw.0.load(Relaxed), 0);
        // The paused owner does not hold up another complete registration/wake.
        let mut c = None;
        let cw = Arc::new(Counter(Count::new(0)));
        list.register(&count, &next, &mut c, &Waker::from(cw.clone()));
        list.notify_one(&count);
        assert_eq!(cw.0.load(Relaxed), 1);
        paused.wake(&count);
        assert_eq!(aw.0.load(Relaxed), 1);
        list.unregister(&count, &mut b, false);
        list.unregister(&count, &mut c, false);
        assert_eq!(count.load(Relaxed), 0);
        assert!(list
            .first
            .slots
            .iter()
            .all(|s| s.state.load(Relaxed) == FREE));
    }

    #[test]
    fn moving_a_used_list_preserves_embedded_and_heap_cursors() {
        let list = WaiterList::new();
        let count = AtomicUsize::new(0);
        let next = AtomicU64::new(1);
        let wake = Arc::new(Counter(Count::new(0)));
        let mut tokens: Vec<_> = (0..PAGE_SLOTS + 1).map(|_| None).collect();
        for token in &mut tokens {
            list.register(&count, &next, token, &Waker::from(wake.clone()));
        }
        list.notify_one(&count);
        // Notify cursor names the embedded page; registration cursor names a heap
        // page. Both must remain usable after moving the list into new storage.
        let list = Box::new(list);
        list.notify_all(&count);
        for token in &mut tokens {
            list.unregister(&count, token, false);
        }
        let mut again = None;
        list.register(&count, &next, &mut again, &Waker::from(wake.clone()));
        list.notify_one(&count);
        list.unregister(&count, &mut again, false);
        assert_eq!(wake.0.load(Relaxed), PAGE_SLOTS + 2);
        assert_eq!(count.load(Relaxed), 0);
    }

    #[test]
    fn paused_writer_does_not_own_the_list() {
        let list = WaiterList::new();
        let count = AtomicUsize::new(0);
        let next = AtomicU64::new(1);
        list.first.slots[0].state.store(WRITING, SeqCst);
        let mut token = None;
        let wake = Arc::new(Counter(Count::new(0)));
        list.register(&count, &next, &mut token, &Waker::from(wake.clone()));
        list.notify_one(&count);
        assert_eq!(wake.0.load(Relaxed), 1);
        list.unregister(&count, &mut token, false);
        list.first.slots[0].state.store(FREE, SeqCst);
    }

    #[test]
    fn list_drop_releases_forgotten_wakers_exactly_once() {
        struct Tracked(Arc<Count>);
        // The observable action is the custom Drop invoked when wake consumes Arc.
        #[allow(clippy::manual_noop_waker)]
        impl Wake for Tracked {
            fn wake(self: Arc<Self>) {}
        }
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Relaxed);
            }
        }
        let drops = Arc::new(Count::new(0));
        let count = AtomicUsize::new(0);
        let next = AtomicU64::new(1);
        let list = WaiterList::new();
        for _ in 0..PAGE_SLOTS * 3 {
            let waker = Waker::from(Arc::new(Tracked(drops.clone())));
            // Discarding a token models a mem::forgotten future.
            list.register(&count, &next, &mut None, &waker);
        }
        list.notify_many(&count, PAGE_SLOTS + 1);
        assert_eq!(drops.load(Relaxed), PAGE_SLOTS + 1);
        drop(list);
        assert_eq!(drops.load(Relaxed), PAGE_SLOTS * 3);
    }

    #[test]
    fn waker_drop_reenters_after_slot_is_free() {
        struct Reenter {
            list: Arc<WaiterList>,
            count: Arc<AtomicUsize>,
            next: Arc<AtomicU64>,
            calls: Arc<Count>,
        }
        // Drop re-enters the list; Waker::noop cannot exercise this ownership path.
        #[allow(clippy::manual_noop_waker)]
        impl Wake for Reenter {
            fn wake(self: Arc<Self>) {}
        }
        impl Drop for Reenter {
            fn drop(&mut self) {
                assert_eq!(self.list.first.slots[0].state.load(Relaxed), FREE);
                let mut token = None;
                self.list.register(
                    &self.count,
                    &self.next,
                    &mut token,
                    &futures::task::noop_waker(),
                );
                self.list.unregister(&self.count, &mut token, false);
                self.calls.fetch_add(1, Relaxed);
            }
        }
        let list = Arc::new(WaiterList::new());
        let count = Arc::new(AtomicUsize::new(0));
        let next = Arc::new(AtomicU64::new(1));
        let calls = Arc::new(Count::new(0));
        let waker = Waker::from(Arc::new(Reenter {
            list: list.clone(),
            count: count.clone(),
            next: next.clone(),
            calls: calls.clone(),
        }));
        let mut token = None;
        list.register(&count, &next, &mut token, &waker);
        drop(waker);
        list.unregister(&count, &mut token, false);
        assert_eq!(calls.load(Relaxed), 1);
        assert_eq!(count.load(Relaxed), 0);
    }
}

#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;
    use crate::sync::{Arc, AtomicBool};
    use loom::thread;
    use std::sync::Arc as StdArc;
    use std::task::Wake;

    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: StdArc<Self>) {
            self.0.fetch_add(1, SeqCst);
        }
    }

    fn model(f: impl Fn() + Send + Sync + 'static) {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(f);
    }

    #[test]
    fn notification_racing_cancellation_forwards_once() {
        model(|| {
            let list = Arc::new(WaiterList::new());
            let count = Arc::new(AtomicUsize::new(0));
            let next = AtomicU64::new(1);
            let mut a = None;
            let mut b = None;
            let aw = StdArc::new(Counter(AtomicUsize::new(0)));
            let bw = StdArc::new(Counter(AtomicUsize::new(0)));
            list.register(&count, &next, &mut a, &Waker::from(aw));
            list.register(&count, &next, &mut b, &Waker::from(bw.clone()));
            let l = list.clone();
            let c = count.clone();
            let notifier = thread::spawn(move || l.notify_one(&c));
            list.unregister(&count, &mut a, true);
            notifier.join().unwrap();
            assert_eq!(bw.0.load(SeqCst), 1);
            assert_eq!(count.load(SeqCst), 0);
            list.unregister(&count, &mut b, false);
        });
    }

    #[test]
    fn notification_racing_repoll_consumes_one_wake() {
        model(|| {
            let list = Arc::new(WaiterList::new());
            let count = Arc::new(AtomicUsize::new(0));
            let next = AtomicU64::new(1);
            let mut a = None;
            let mut b = None;
            let aw = StdArc::new(Counter(AtomicUsize::new(0)));
            let bw = StdArc::new(Counter(AtomicUsize::new(0)));
            list.register(&count, &next, &mut a, &Waker::from(aw.clone()));
            list.register(&count, &next, &mut b, &Waker::from(bw.clone()));
            let l = list.clone();
            let c = count.clone();
            let notifier = thread::spawn(move || l.notify_one(&c));
            list.unregister(&count, &mut a, false);
            notifier.join().unwrap();
            assert_eq!(aw.0.load(SeqCst) + bw.0.load(SeqCst), 1);
            list.unregister(&count, &mut b, false);
            assert_eq!(count.load(SeqCst), 0);
        });
    }

    #[test]
    fn close_cannot_miss_registration_on_appended_page() {
        model(|| {
            let list = Arc::new(WaiterList::new());
            let count = Arc::new(AtomicUsize::new(0));
            let next = Arc::new(AtomicU64::new(1));
            let closed = Arc::new(AtomicBool::new(false));
            let wake = StdArc::new(Counter(AtomicUsize::new(0)));
            let mut occupied: Vec<_> = (0..PAGE_SLOTS).map(|_| None).collect();
            for token in &mut occupied {
                list.register(&count, &next, token, &futures::task::noop_waker());
            }
            // Even when notified, the first-page slots stay occupied by these
            // unacknowledged tokens, so the racing registration must append.
            let l = list.clone();
            let c = count.clone();
            let n = next.clone();
            let flag = closed.clone();
            let w = wake.clone();
            let receiver = thread::spawn(move || {
                let mut token = None;
                l.register(&c, &n, &mut token, &Waker::from(w));
                let saw_close = flag.load(SeqCst);
                (token, saw_close)
            });
            closed.swap(true, SeqCst);
            list.notify_all(&count);
            let (mut token, saw_close) = receiver.join().unwrap();
            assert!(saw_close || wake.0.load(SeqCst) == 1);
            list.unregister(&count, &mut token, false);
            for token in &mut occupied {
                list.unregister(&count, token, false);
            }
            assert_eq!(count.load(SeqCst), 0);
        });
    }
}
