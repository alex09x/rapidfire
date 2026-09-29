//! The atomic block queue at the heart of the channel.
//!
//! # Design
//!
//! Values live in fixed-size **blocks** (`BLOCK_CAP` slots each) chained through a
//! `next` pointer.  Positions are absolute `usize` indices; a block covers one *lap* of
//! `LAP` consecutive positions and the last position of every lap (`offset ==
//! BLOCK_CAP`) is a **sentinel** that never holds a value.  A producer or consumer that
//! observes its index on the sentinel knows a block transition is in flight. Consumers
//! spin briefly, then return to the caller if the transitioning consumer is paused.
//!
//! * Producers claim a slot on `tail.index`, adaptively.  While no contention has been
//!   observed recently the claim is a wait-free `fetch_add`; contention (another claim
//!   landing between our load and our RMW, or a failed CAS) switches the queue to CAS
//!   claims for the next `CONTENTION_WINDOW` uncontended pushes.  That is deliberate:
//!   measured on Zen 4 and Neoverse-N1, `fetch_add` lets four producers claim four
//!   adjacent slots at once and then fight over the same slot cache line while
//!   writing, which stalls the consumer on the slowest of them (~4x worse), whereas
//!   CAS retries with backoff reduced overlapping adjacent-slot writes in those runs.
//!   This is a performance observation, not a guarantee that writers run serially:
//!   a producer can still be paused after claiming its slot and before publishing it.
//!   Both protocols are correct together, so the mode can flip at any moment and the
//!   mode word is only a hint.  The block for a claimed position
//!   is found by walking *backwards* from `tail.block` over `prev` links, comparing
//!   each block's `start` position; every block on that path is provably live because
//!   the claimed slot has not been written yet.  The producer that claims the last slot
//!   of a block installs the next block (from the pool, otherwise freshly allocated),
//!   links it, publishes its `start` with Release (so a walker that sees the start
//!   also sees the links) and publishes it as `tail.block` before writing its own
//!   value. Recycling clears the reader marks and resets `start`; links keep pointing
//!   at old, never freed blocks so a stale walker always lands on valid memory.
//! * Consumers first check the **slot state** of the head slot and claim it with a CAS
//!   on `head.index` only once the value is there.  They never read `tail.index`, so
//!   the producers' cache line stays private to producers: the only lines that cross
//!   cores per value are the slot lines themselves.  A consumer therefore never waits
//!   on a producer, and -- crucially for the block walk above -- the head never moves
//!   past an unwritten slot, so every block from the oldest unwritten position to the
//!   tail is live and linked.  (Claim-first consumers, as in `SegQueue`, would let the
//!   head run ahead of a pre-empted producer and let the blocks behind be recycled,
//!   which strands that producer's block lookup; that variant deadlocked on a 128-core
//!   box and was removed.)  The consumer that claims the last slot moves `head.block`
//!   to the next block and hands the old block back to the pool if its readers have
//!   finished. Otherwise it retires the block without waiting; an installing producer
//!   can reclaim it once every reader has finished.
//! * Slot states are **tagged with the lap number**, so they need no reset when a
//!   block is recycled: an old tag cannot be mistaken for the current lap's `WRITTEN`.
//! * Blocks are **never freed while the queue lives**; they cycle through a one-block
//!   spare slot and an atomic ownership pool (see below), accessed only when acquiring
//!   or recycling blocks. This makes it sound to dereference a block pointer that may be
//!   stale: it always points at valid memory, a pooled block carries `start ==
//!   usize::MAX`, a re-linked one carries a start that can never equal a lap it served
//!   before, a stale slot state can never match the current lap, and the CAS on
//!   `head.index` rejects a stale consumer claim.  Memory therefore stays at the
//!   channel's high-water mark until it is dropped.
//! * A bounded producer caches the last head it saw (`tail.cached`) and reloads the
//!   real head only when the cache says the queue looks full.
//!
//! # The block ownership pool
//!
//! Blocks that leave the live chain are parked in [`BlockPool`]: append-only linked
//! *pages* of `AtomicPtr` slots, the first page embedded in the queue so a channel whose
//! block high-water mark fits in it never allocates a page at all.  The pool replaces an
//! earlier mutex and rests on one rule -- **a slot is an ownership hand-off, not a
//! container**:
//!
//! * A block is *taken* with `swap(null, Acquire)`.  Exactly one thread can observe a
//!   given pointer, so two threads can never believe they own the same block.  Ownership
//!   is decided by the swap, never by comparing a pointer value, so re-publishing the
//!   same (reused) block into the same slot cannot be mistaken for the old one: there is
//!   no ABA to lose to, unlike a Treiber pop that reads a head pointer, dereferences it
//!   and CASes the pointer value back.
//! * A block is *published* with `compare_exchange(null -> block, Release, ..)`, by the
//!   thread that exclusively owns it and gives that ownership up.  The Release pairs with
//!   the taker's Acquire so the taker sees the reset that preceded publication.
//! * A **retired** block -- one whose last slot was consumed while an earlier reader had
//!   not finished -- is taken out of the pool *before* `readers_done` is consulted.
//!   Checking a still-published block and removing it afterwards would be unsound even
//!   with a CAS: in between, the block can be taken, reset, refilled and recycled into
//!   the same slot, so the CAS matches the identical pointer while the answer describes a
//!   different incarnation.  A block whose readers are unfinished is published straight
//!   back, unchanged; nobody ever waits for a reader.
//! * Page links are append-only, published with Release and never cleared, so every page
//!   pointer stays valid and every slot keeps its address until `Queue::drop` (which has
//!   `&mut self` and therefore exclusive access).  That is what makes the scan hints
//!   below safe to follow even when they are stale.
//! * `stored` and the scan hints are hints only: correctness never depends on finding a
//!   pooled block, because the caller just allocates instead.  `stored == 0` short-cuts
//!   the scan, so a channel that is only growing never walks the pool, and a scan is
//!   capped at `POOL_SCAN_BUDGET` slots, so no allocation is ever O(live blocks).
//!
//! Every pool operation finishes in a bounded number of its own steps and no operation
//! can leave the pool in a state that stalls another one -- a thread pre-empted while it
//! owns a block only delays the reuse of that block.  This says nothing about the
//! channel as a whole: the reservation protocol above still lets a pre-empted producer
//! stall consumers at a claimed slot, and that is unchanged here.
//!
//! # Memory ordering: the sleep/wake hand-off
//!
//! The channel layer must never lose a wake-up: a producer that pushes while a receiver
//! is going to sleep has to see the receiver's "I am waiting" flag, or the receiver has
//! to see the push.  The usual solution is a `SeqCst` load of the flag after the push,
//! but on AArch64 that is an `ldar` that stalls until the preceding `casal` on the
//! tail line has completed, and it sits on the hot path of every push.
//!
//! Instead the party going to sleep pays: after raising its flag it performs an RMW on
//! the *other side's* index (`tail.index.fetch_add(0, AcqRel)` for a receiver,
//! `head.index.fetch_add(0, AcqRel)` for a bounded sender).  Every later claim on that
//! index is an `AcqRel` RMW that reads from (the release sequence headed by) that RMW,
//! so it synchronizes-with the sleeper and a plain `Relaxed` load of the flag right after
//! the claim is guaranteed to observe it. Every modification of an index used for
//! parking must be an RMW (a plain store would end the release sequence), which is why the
//! block transition uses `swap` rather than `store`.  The RMW also returns the live index, so the
//! sleeper can tell *claims* from *writes*: if a slot below that index is claimed but
//! not yet written, the producer sampled the flag before the registration and will not
//! wake anyone, so the receiver must not sleep on it -- it polls briefly and otherwise
//! yields to its executor (see `Recv::poll`).  Producers therefore run with zero
//! `SeqCst` operations and zero fences; the extra RMW happens only when a channel is
//! empty (or full) and a task is about to park, where it is dwarfed by the park itself.
//!
//! `closed` is handled by the channel layer: `try_recv` reads it with `SeqCst` only
//! after `pop` reported empty, the close itself is a `SeqCst` swap, and the final
//! `Closed` verdict re-checks both indices with `SeqCst`, so a value pushed before the
//! close is always drained before `Closed` is reported.
//!
//! # Exclusive MPSC receive paths
//!
//! `pop_single` and `pop_single_batch` require one consumer for the queue's whole
//! lifetime, enforced by `mpsc::Receiver`'s private wrapper and exclusive borrows.
//! They read values before advancing head, omit reader marks, and recycle blocks
//! only after that sole reader has finished. Bounded head updates remain AcqRel
//! RMWs, including one RMW per batch chunk, to preserve sender wakeup ordering.
//! Unbounded senders never park on head; only this exclusive unbounded path may
//! use head stores. Tail updates still preserve the receiver-parking sequence.

use crate::sync::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use crate::sync::{
    spin_loop, yield_now, AtomicPtr, AtomicU8, AtomicUsize, CachePadded, UnsafeCell,
};
use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::mem::MaybeUninit;
use std::ptr;

/// log2 of positions per block.  Tiny blocks under the `loom` feature / `--cfg
/// fcrs_small_blocks` make block transitions happen every few operations so tests
/// exercise them.
#[cfg(any(feature = "loom", fcrs_small_blocks))]
pub(crate) const LAP_SHIFT: usize = 2;
#[cfg(not(any(feature = "loom", fcrs_small_blocks)))]
pub(crate) const LAP_SHIFT: usize = 6;

/// Positions per block (power of two).
pub(crate) const LAP: usize = 1 << LAP_SHIFT;
/// Values per block; the last position of a lap is the transition sentinel.
pub(crate) const BLOCK_CAP: usize = LAP - 1;
const OFFSET_MASK: usize = LAP - 1;

const TAG_WRITTEN: usize = 1;

#[inline(always)]
fn written_tag(lap: usize) -> usize {
    (lap << 2) | TAG_WRITTEN
}

/// Number of value positions in `[head, tail)`, excluding sentinels.
#[inline(always)]
fn items_between(head: usize, tail: usize) -> usize {
    if head >= tail {
        0
    } else {
        (tail - head) - ((tail >> LAP_SHIFT) - (head >> LAP_SHIFT))
    }
}

/// Spin strategies.
///
/// `spin` is for CAS retries: exponential up to 64 pause instructions, never yields
/// (the peer is running and will finish in nanoseconds).
///
/// `snooze` is for waiting on another thread's progress (a claimed-but-unwritten slot,
/// a block being installed).  Those waits normally end within a cross-core round trip,
/// so it polls after every single pause for a while, then backs off exponentially, and
/// only then yields the thread so a pre-empted peer can get scheduled.
pub(crate) struct Backoff(u32);

const SPIN_LIMIT: u32 = 6;
const TIGHT_POLLS: u32 = 64;
const SNOOZE_LIMIT: u32 = TIGHT_POLLS + 8;

impl Backoff {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Backoff(0)
    }

    /// Like `snooze`, but returns `false` instead of yielding once the spinning
    /// budget is used up, for callers that must not block their thread.
    // Keep this rare wait out of the successful pop/poll path's instruction footprint.
    #[cold]
    #[inline(never)]
    pub(crate) fn snooze_bounded(&mut self) -> bool {
        if self.0 >= SNOOZE_LIMIT {
            return false;
        }
        self.snooze();
        true
    }

    #[inline(always)]
    fn spin(&mut self) {
        for _ in 0..(1u32 << self.0.min(SPIN_LIMIT)) {
            spin_loop();
        }
        if self.0 <= SPIN_LIMIT {
            self.0 += 1;
        }
    }

    #[inline(always)]
    pub(crate) fn snooze(&mut self) {
        if self.0 < TIGHT_POLLS {
            spin_loop();
            self.0 += 1;
        } else if self.0 < SNOOZE_LIMIT {
            for _ in 0..(1u32 << (self.0 - TIGHT_POLLS)) {
                spin_loop();
            }
            self.0 += 1;
        } else {
            yield_now();
        }
    }
}

/// Publishes a slot: every store before this call (the value) becomes visible to a
/// consumer that observes `tag` through an Acquire load of `state`.
///
/// A plain Release store (`stlr` on AArch64, `mov` on x86).  Two alternatives were
/// measured and rejected: `dmb ishst` + relaxed store via inline asm (avoids the
/// `stlr`-then-`ldar` completion wait, but was 5-15% slower on Neoverse-N1 in every
/// scenario) and `fence(Release)` + relaxed store (`dmb ish`, slower still).  They stay
/// available behind `--cfg fcrs_publish_asm` / `--cfg fcrs_publish_fence` for
/// re-measuring on new hardware.
#[inline(always)]
fn publish(state: &AtomicUsize, tag: usize) {
    #[cfg(all(
        target_arch = "aarch64",
        fcrs_publish_asm,
        not(any(feature = "loom", miri))
    ))]
    {
        // SAFETY: a barrier instruction with no operands and no effect on registers.
        unsafe { core::arch::asm!("dmb ishst", options(nostack, preserves_flags)) };
        state.store(tag, Relaxed);
    }
    #[cfg(all(fcrs_publish_fence, not(any(feature = "loom", miri))))]
    {
        std::sync::atomic::fence(Release);
        state.store(tag, Relaxed);
    }
    #[cfg(all(fcrs_publish_rmw, not(any(feature = "loom", miri))))]
    {
        state.swap(tag, Release);
    }
    #[cfg(not(any(
        all(
            target_arch = "aarch64",
            fcrs_publish_asm,
            not(any(feature = "loom", miri))
        ),
        all(fcrs_publish_fence, not(any(feature = "loom", miri))),
        all(fcrs_publish_rmw, not(any(feature = "loom", miri)))
    )))]
    {
        state.store(tag, Release);
    }
}

/// Marks a slot as read (consumer side).  Experiment: `--cfg fcrs_mark_rmw` uses an
/// RMW instead of a release store.
#[inline(always)]
fn mark_read(mark: &AtomicU8) {
    #[cfg(all(fcrs_mark_rmw, not(any(feature = "loom", miri))))]
    {
        mark.swap(1, Release);
    }
    #[cfg(not(all(fcrs_mark_rmw, not(any(feature = "loom", miri)))))]
    {
        mark.store(1, Release);
    }
}

#[repr(C)]
struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    state: AtomicUsize,
}

/// Marker for a block that sits in the pool (not linked anywhere).
const POOLED: usize = usize::MAX;

/// Producer-side block header, on its own cache line: written once by the installing
/// producer, read by producers locating their block and once per block by the
/// recycling consumer (`next`).
#[repr(C)]
struct ProducerHeader<T> {
    /// First position of the lap this block serves, or `POOLED`.
    start: AtomicUsize,
    next: AtomicPtr<Block<T>>,
    prev: AtomicPtr<Block<T>>,
}

/// Consumer-side block header, on its own cache line: `read_marks` is written by
/// consumers and checked by the recycling consumer. Producers only check this line
/// when reclaiming a retired block on the allocation slow path. Consumers never write
/// to the slot lines, which producers own until the value is read.
#[repr(C)]
struct ConsumerHeader {
    read_marks: [AtomicU8; BLOCK_CAP],
}

#[repr(C)]
struct Block<T> {
    phdr: CachePadded<ProducerHeader<T>>,
    chdr: CachePadded<ConsumerHeader>,
    slots: [Slot<T>; BLOCK_CAP],
}

impl<T> Block<T> {
    const LAYOUT: Layout = Layout::new::<Block<T>>();

    fn allocate() -> *mut Block<T> {
        // SAFETY: the layout is never zero-sized (it holds an AtomicPtr), and every
        // field is initialised below before the block is handed out.
        unsafe {
            let block = alloc(Self::LAYOUT) as *mut Block<T>;
            if block.is_null() {
                handle_alloc_error(Self::LAYOUT);
            }
            ptr::addr_of_mut!((*block).phdr.0.start).write(AtomicUsize::new(POOLED));
            ptr::addr_of_mut!((*block).phdr.0.next).write(AtomicPtr::new(ptr::null_mut()));
            ptr::addr_of_mut!((*block).phdr.0.prev).write(AtomicPtr::new(ptr::null_mut()));
            let marks = ptr::addr_of_mut!((*block).chdr.0.read_marks) as *mut AtomicU8;
            for i in 0..BLOCK_CAP {
                marks.add(i).write(AtomicU8::new(0));
            }
            let slots = ptr::addr_of_mut!((*block).slots) as *mut Slot<T>;
            for i in 0..BLOCK_CAP {
                slots.add(i).write(Slot {
                    value: UnsafeCell::new(MaybeUninit::uninit()),
                    state: AtomicUsize::new(0),
                });
            }
            block
        }
    }

    /// # Safety
    /// `block` must come from [`Block::allocate`], be unreachable from the queue and
    /// hold no live values.
    unsafe fn release(block: *mut Block<T>) {
        ptr::drop_in_place(block);
        dealloc(block as *mut u8, Self::LAYOUT);
    }

    /// The last slot belongs to the retiring consumer itself; only earlier readers
    /// can still be using this block. An Acquire observes the end of each read.
    #[inline]
    fn readers_done(&self) -> bool {
        self.chdr.0.read_marks[..BLOCK_CAP - 1]
            .iter()
            .all(|mark| mark.load(Acquire) != 0)
    }

    /// # Safety
    /// All readers must have finished, and the caller must exclusively own recycling
    /// this block. It must not be reachable through the live head/tail chain.
    unsafe fn reset(&self) {
        for mark in &self.chdr.0.read_marks {
            mark.store(0, Relaxed);
        }
        // Keep the links intact for stale walkers, which re-validate `start`.
        self.phdr.0.start.store(POOLED, Relaxed);
    }
}

/// Slots in one pool page.  Smaller under the small-block configurations so tests reach
/// the multi-page paths quickly.
#[cfg(any(feature = "loom", fcrs_small_blocks))]
const POOL_PAGE_SLOTS: usize = 4;
#[cfg(not(any(feature = "loom", fcrs_small_blocks)))]
const POOL_PAGE_SLOTS: usize = 16;

/// Slots one take visits before giving up and letting the caller allocate.  This is what
/// bounds an allocation: a miss costs a fixed walk, never O(live blocks).
const POOL_SCAN_BUDGET: usize = 4 * POOL_PAGE_SLOTS;

/// Retired blocks one allocation inspects before allocating instead.  Each probe looks at
/// a *different* block (probed blocks are held out of the pool until the end), so this
/// caps the work even when many readers are stalled at once.
const RETIRED_PROBES: usize = 4;

/// One page of ownership slots.
///
/// `next` is append-only: published once with Release and never changed again while the
/// queue lives, so a page pointer -- including a stale scan hint -- always addresses a
/// live page whose slots are at a fixed address.
struct PoolPage<T> {
    slots: [AtomicPtr<Block<T>>; POOL_PAGE_SLOTS],
    next: AtomicPtr<PoolPage<T>>,
}

impl<T> PoolPage<T> {
    fn new() -> Self {
        PoolPage {
            slots: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }

    fn alloc() -> *mut PoolPage<T> {
        Box::into_raw(Box::new(PoolPage::new()))
    }

    /// # Safety
    /// `page` must come from [`PoolPage::alloc`] and be unreachable (never linked, or
    /// reached during exclusive teardown).
    unsafe fn release(page: *mut PoolPage<T>) {
        drop(Box::from_raw(page));
    }
}

/// Where a scan starts.  Purely a hint; both fields may be stale or disagree, and the
/// page pointer stays dereferenceable because pages are never freed before `Queue::drop`.
/// Takes and publications keep separate hints so that a sweep of occupied slots and a
/// sweep of free slots do not drag each other backwards.
struct PoolHint<T> {
    page: AtomicPtr<PoolPage<T>>,
    slot: AtomicUsize,
}

impl<T> PoolHint<T> {
    fn new() -> Self {
        PoolHint {
            // Null means "the embedded page": the queue is moved into its `Arc` after
            // construction, so no real page address may be recorded before that.
            page: AtomicPtr::new(ptr::null_mut()),
            slot: AtomicUsize::new(0),
        }
    }
}

/// Blocks outside the live chain, held in linked pages of ownership slots.
///
/// Two pools exist per queue: `ready` blocks are reusable immediately, `retired` blocks
/// may still belong to an outstanding reader and must pass `readers_done` -- checked only
/// by the thread that already took them -- before reuse.  See the module docs for the
/// ownership rules this relies on.
struct BlockPool<T> {
    /// First page, embedded so an ordinary channel never allocates pool memory.
    first: PoolPage<T>,
    /// Pages currently linked.  Only ever grows; used to size a full-cycle scan.
    pages: AtomicUsize,
    /// Count raised before publication and lowered after ownership is taken. A relaxed
    /// snapshot can be stale; it is only an optimization hint, never an ownership test.
    stored: AtomicUsize,
    take_hint: PoolHint<T>,
    put_hint: PoolHint<T>,
}

impl<T> BlockPool<T> {
    fn new() -> Self {
        BlockPool {
            first: PoolPage::new(),
            pages: AtomicUsize::new(1),
            stored: AtomicUsize::new(0),
            take_hint: PoolHint::new(),
            put_hint: PoolHint::new(),
        }
    }

    #[inline]
    fn hint_page(&self, hint: &PoolHint<T>) -> *const PoolPage<T> {
        let page = hint.page.load(Acquire);
        if page.is_null() {
            &self.first
        } else {
            page
        }
    }

    #[inline]
    fn set_hint(&self, hint: &PoolHint<T>, page: *const PoolPage<T>, slot: usize) {
        // Never retain a pointer into the embedded page: a privately owned pool
        // can move after use. Heap pages keep their address throughout the move.
        let page = if ptr::eq(page, &self.first) {
            ptr::null_mut()
        } else {
            page as *mut PoolPage<T>
        };
        hint.page.store(page, Release);
        hint.slot.store(slot, Relaxed);
    }

    /// Visits at most `budget` slots from `hint`, following the append-only page links
    /// and wrapping to the embedded page at the end of the list.  Stops as soon as `f`
    /// returns a value.
    fn scan<R>(
        &self,
        hint: &PoolHint<T>,
        budget: usize,
        mut f: impl FnMut(&AtomicPtr<Block<T>>, *const PoolPage<T>, usize) -> Option<R>,
    ) -> Option<R> {
        let mut page = self.hint_page(hint);
        let mut slot = hint.slot.load(Relaxed) % POOL_PAGE_SLOTS;
        let mut visited = 0;
        while visited < budget {
            // SAFETY: pages are only appended and are freed by `Queue::drop` alone, so
            // any pointer the hint or a `next` link yields addresses a live page.
            let slots = unsafe { &(*page).slots };
            while slot < POOL_PAGE_SLOTS && visited < budget {
                if let Some(found) = f(&slots[slot], page, slot) {
                    return Some(found);
                }
                visited += 1;
                slot += 1;
            }
            if slot == POOL_PAGE_SLOTS {
                // SAFETY: as above.
                let next = unsafe { (*page).next.load(Acquire) };
                page = if next.is_null() { &self.first } else { next };
                slot = 0;
            }
        }
        // A miss must advance the sweep too. Otherwise a stale hint pointing at
        // more than one budget of empty slots can hide ready blocks indefinitely.
        self.set_hint(hint, page, slot);
        None
    }

    /// Takes exclusive ownership of one pooled block, or `None` if the bounded scan found
    /// none.  `None` is always a valid answer: the caller allocates instead.
    fn take(&self) -> Option<*mut Block<T>> {
        if self.stored.load(Relaxed) == 0 {
            // A channel that is only growing never walks the pool.
            return None;
        }
        let found = self.scan(&self.take_hint, POOL_SCAN_BUDGET, |slot, page, index| {
            if slot.load(Relaxed).is_null() {
                return None;
            }
            // The swap is the hand-off: whoever sees a non-null pointer here is from
            // this moment its only owner.
            let block = slot.swap(ptr::null_mut(), Acquire);
            if block.is_null() {
                return None;
            }
            // Resume the sweep at the slot we emptied: the next take walks forward into
            // the still-occupied run, and the next publication refills it.
            self.set_hint(&self.take_hint, page, index);
            Some(block)
        });
        if found.is_some() {
            self.stored.fetch_sub(1, Relaxed);
        }
        found
    }

    /// Publishes a block the caller owns exclusively, giving that ownership up.
    ///
    /// Always succeeds: when every slot is occupied the page list grows by one page.
    /// The (rare) page allocation happens while the caller owns nothing but this block,
    /// so it blocks no other pool operation.
    ///
    /// # Safety
    /// `block` must be exclusively owned for recycling and unreachable from the
    /// live chain except through stale pointers that re-validate `start`. Ready
    /// blocks hold no live values. A retired block may still contain values claimed
    /// by paused readers; only a later successful readers_done check permits reset.
    unsafe fn put(&self, block: *mut Block<T>) {
        // Raised before publication so a successful take cannot underflow the count.
        self.stored.fetch_add(1, Relaxed);
        loop {
            // A full cycle: every slot of every page currently linked, once.
            let budget = self.pages.load(Relaxed) * POOL_PAGE_SLOTS;
            let published = self.scan(&self.put_hint, budget, |slot, page, index| {
                if !slot.load(Relaxed).is_null() {
                    return None;
                }
                match slot.compare_exchange(ptr::null_mut(), block, Release, Relaxed) {
                    Ok(_) => {
                        self.set_hint(&self.put_hint, page, index);
                        Some(())
                    }
                    Err(_) => None,
                }
            });
            if published.is_some() {
                return;
            }
            self.grow();
        }
    }

    /// Appends one page, or adopts the page another thread appended first.
    #[cold]
    fn grow(&self) {
        let fresh = PoolPage::<T>::alloc();
        let mut page = self.hint_page(&self.put_hint);
        loop {
            // SAFETY: pages live until `Queue::drop`; see `scan`.
            let next = unsafe { (*page).next.load(Acquire) };
            if !next.is_null() {
                page = next;
                continue;
            }
            // SAFETY: as above.  Release publishes the initialised slots of `fresh`.
            let linked = unsafe {
                (*page)
                    .next
                    .compare_exchange(ptr::null_mut(), fresh, Release, Acquire)
            };
            match linked {
                Ok(_) => {
                    self.pages.fetch_add(1, Relaxed);
                    self.set_hint(&self.put_hint, fresh, 0);
                }
                Err(theirs) => {
                    // Somebody else grew the pool; point the hint at their page so the
                    // retrying publication lands there, and drop ours, which was never
                    // linked or seen by anyone.
                    self.set_hint(&self.put_hint, theirs, 0);
                    // SAFETY: `fresh` comes from `PoolPage::alloc` and is unreachable.
                    unsafe { PoolPage::release(fresh) };
                }
            }
            return;
        }
    }

    /// Frees every pooled block and every appended page.
    ///
    /// # Safety
    /// Exclusive access to the pool; no pool operation may be in flight, and no pooled
    /// block may hold a live value.
    unsafe fn release_all(&mut self) {
        let mut page: *mut PoolPage<T> = &mut self.first;
        let mut heap = false;
        loop {
            for slot in (*page).slots.iter() {
                let block = slot.load(Relaxed);
                if !block.is_null() {
                    Block::release(block);
                }
            }
            let next = (*page).next.load(Relaxed);
            if heap {
                PoolPage::release(page);
            }
            if next.is_null() {
                return;
            }
            page = next;
            heap = true;
        }
    }
}

struct Position<T> {
    /// Absolute index of the next slot to claim on this side.
    index: AtomicUsize,
    /// Block containing `index` (except while `index` sits on a sentinel).
    block: AtomicPtr<Block<T>>,
    /// Producers only: last observed head index (bounded capacity check).
    cached: AtomicUsize,
    /// Producers only: contention hint, updated with plain stores -- how many more
    /// uncontended pushes before `fetch_add` claims are used again (0 = use
    /// `fetch_add`).  Unused on the head side.
    contended: AtomicUsize,
}

/// Uncontended pushes needed to leave CAS mode.
const CONTENTION_WINDOW: usize = 64;

/// `Queue::capacity` value meaning "unbounded".
const UNBOUNDED: usize = usize::MAX;

pub(crate) struct Queue<T> {
    head: CachePadded<Position<T>>,
    tail: CachePadded<Position<T>>,
    /// One-block fast path of the pool: filled by the recycling consumer, emptied by
    /// the producer that needs a new block.
    spare: CachePadded<AtomicPtr<Block<T>>>,
    /// Overflow pool of immediately reusable blocks (see the module docs on never
    /// freeing and on the ownership protocol).
    ready: BlockPool<T>,
    /// Blocks whose last slot was consumed while an earlier reader had not finished.
    retired: BlockPool<T>,
    /// Bounded capacity, or `UNBOUNDED`.  A plain word rather than `Option<usize>` so
    /// the hot path tests one load instead of a discriminant plus a value.
    capacity: usize,
}

// SAFETY: values are handed from producer to consumer through Release/Acquire slot
// states; blocks are only written by the party that owns the slot at the time and are
// never freed while the queue lives.
unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {
    pub(crate) fn new(capacity: Option<usize>) -> Self {
        let first = Block::<T>::allocate();
        // SAFETY: freshly allocated, exclusively owned.
        unsafe { (*first).phdr.0.start.store(0, Relaxed) };
        Queue {
            head: CachePadded(Position {
                index: AtomicUsize::new(0),
                block: AtomicPtr::new(first),
                cached: AtomicUsize::new(0),
                contended: AtomicUsize::new(0),
            }),
            tail: CachePadded(Position {
                index: AtomicUsize::new(0),
                block: AtomicPtr::new(first),
                cached: AtomicUsize::new(0),
                contended: AtomicUsize::new(0),
            }),
            spare: CachePadded(AtomicPtr::new(ptr::null_mut())),
            ready: BlockPool::new(),
            retired: BlockPool::new(),
            capacity: capacity.unwrap_or(UNBOUNDED),
        }
    }

    #[inline(always)]
    pub(crate) fn capacity(&self) -> Option<usize> {
        if self.capacity == UNBOUNDED {
            None
        } else {
            Some(self.capacity)
        }
    }

    /// Called by a receiver *after* it raised its waiting flag; returns the live tail
    /// (see the module docs).  If [`Queue::pop`] then reports nothing but
    /// [`Queue::claims_below`] is true, a producer claimed a slot before the flag was
    /// visible and the receiver must not sleep yet.
    #[inline]
    pub(crate) fn receiver_parking(&self) -> usize {
        self.tail.index.fetch_add(0, AcqRel)
    }

    /// `true` if positions below `tail_snapshot` are claimed and not yet consumed.
    #[inline]
    pub(crate) fn claims_below(&self, tail_snapshot: usize) -> bool {
        items_between(self.head.index.load(Acquire), tail_snapshot) != 0
    }

    /// Bounded-channel counterpart of [`Queue::receiver_parking`] for a sender that
    /// raised its waiting flag.
    #[inline]
    pub(crate) fn sender_parking(&self) {
        self.head.index.fetch_add(0, AcqRel);
    }

    /// `true` if no value is claimable right now, judged from SeqCst loads of both
    /// indices (used for the final verdict on a closed channel).
    #[inline]
    pub(crate) fn is_empty_seqcst(&self) -> bool {
        let head = self.head.index.load(SeqCst);
        let tail = self.tail.index.load(SeqCst);
        items_between(head, tail) == 0
    }

    /// Approximate number of values currently stored.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        let head = self.head.index.load(Relaxed);
        let tail = self.tail.index.load(Relaxed);
        // The independent snapshots can span a receive/refill cycle: an old
        // head paired with a newer tail can look larger than a bounded queue.
        // Keep this approximate observation within the channel's actual bound.
        // UNBOUNDED is usize::MAX, so it leaves unbounded estimates unchanged.
        items_between(head, tail).min(self.capacity)
    }

    #[inline(always)]
    fn take_block(&self) -> *mut Block<T> {
        let block = self.spare.swap(ptr::null_mut(), Acquire);
        if !block.is_null() {
            return block;
        }
        self.take_block_slow()
    }

    #[cold]
    fn take_block_slow(&self) -> *mut Block<T> {
        if let Some(block) = self.ready.take() {
            return block;
        }
        if let Some(block) = self.reclaim_retired() {
            return block;
        }
        // Never wait for a reader, and never trade a bounded scan for an unbounded one:
        // a fresh block is cheaper than either.
        Block::allocate()
    }

    /// Reclaims one retired block whose readers have all finished, if a bounded number of
    /// probes finds one.
    ///
    /// Each candidate is taken out of the pool *before* `readers_done` is consulted, so
    /// the probing thread owns it exclusively while it decides; see the module docs for
    /// why a check on a still-published block would be unsound however it is removed
    /// afterwards.  Candidates that are still owned by a paused reader are published back
    /// unchanged -- they are held out of the pool meanwhile, so no probe inspects the
    /// same block twice.
    #[cold]
    fn reclaim_retired(&self) -> Option<*mut Block<T>> {
        let mut parked: [*mut Block<T>; RETIRED_PROBES] = [ptr::null_mut(); RETIRED_PROBES];
        let mut held = 0;
        let mut reclaimed = None;
        while held < RETIRED_PROBES {
            let block = match self.retired.take() {
                Some(block) => block,
                None => break,
            };
            // SAFETY: the swap out of the pool made this thread the block's only owner;
            // retired blocks stay allocated and outside the live chain.  The Acquire
            // marks in `readers_done` observe every reader's Release.
            if unsafe { (*block).readers_done() } {
                // SAFETY: as above -- exclusive ownership, all readers finished.
                unsafe { (*block).reset() };
                reclaimed = Some(block);
                break;
            }
            parked[held] = block;
            held += 1;
        }
        for &block in &parked[..held] {
            // SAFETY: owned by this thread, still retired and unchanged; all of its
            // values were claimed; a paused reader may not have moved its value yet.
            unsafe { self.retired.put(block) };
        }
        reclaimed
    }

    /// Returns a block to the pool.
    ///
    /// # Safety
    /// `block` must be unreachable from the queue (except through stale pointers that
    /// re-validate by `start`), hold no live values and have `start == POOLED`.
    #[inline(always)]
    unsafe fn recycle(&self, block: *mut Block<T>) {
        if self
            .spare
            .compare_exchange(ptr::null_mut(), block, Release, Relaxed)
            .is_err()
        {
            self.recycle_slow(block);
        }
    }

    /// # Safety
    /// As [`Queue::recycle`].
    #[cold]
    unsafe fn recycle_slow(&self, block: *mut Block<T>) {
        self.ready.put(block);
    }

    /// Appends `value`.  Returns `Err(value)` only when a bounded queue is full.
    ///
    /// On success returns whether `wake_flag` was non-zero when sampled right after the
    /// slot was claimed (see the memory-ordering notes at the top of this file); the
    /// caller then wakes a parked receiver.
    #[inline(always)]
    pub(crate) fn push(&self, value: T, wake_flag: &AtomicUsize) -> Result<bool, T> {
        let cap = self.capacity;
        let tail = if cap == UNBOUNDED {
            if self.tail.contended.load(Relaxed) == 0 {
                self.claim_fetch_add()
            } else {
                self.claim_cas()
            }
        } else {
            match self.claim_bounded(cap) {
                Some(tail) => tail,
                None => return Err(value),
            }
        };

        // Sample the receivers' sleep flag *now*.  The AcqRel claim above reads from the
        // release sequence that a parking receiver started with its `fetch_add(0)` on
        // this index (see the module docs), so this Relaxed load is guaranteed to
        // observe that receiver's flag.  A receiver that saw our claim will wait for the
        // value itself, so publishing stays a plain Release store and the wake-up
        // happens afterwards.
        let wake = wake_flag.load(Relaxed) != 0;

        let offset = tail & OFFSET_MASK;
        let block = self.find_block(tail);

        // SAFETY: `block` is the live block serving `tail`'s lap (see `find_block`) and
        // the claim gives us exclusive ownership of slot `offset`.
        unsafe {
            if offset + 1 == BLOCK_CAP {
                self.install_next(block, tail);
            }
            let slot = (*block).slots.get_unchecked(offset);
            slot.value.with_mut(|p| p.write(MaybeUninit::new(value)));
            publish(&slot.state, written_tag(tail >> LAP_SHIFT));
        }
        Ok(wake)
    }

    /// Wait-free claim: one RMW, no retry.  A producer that draws a sentinel position
    /// simply draws again (that position is skipped by consumers).  If another claim
    /// landed between our load and our RMW, other producers are active: switch the
    /// queue to CAS claims.
    #[inline(always)]
    fn claim_fetch_add(&self) -> usize {
        loop {
            let expected = self.tail.index.load(Relaxed);
            let tail = self.tail.index.fetch_add(1, AcqRel);
            if tail != expected {
                self.tail.contended.store(CONTENTION_WINDOW, Relaxed);
            }
            if tail & OFFSET_MASK != BLOCK_CAP {
                return tail;
            }
        }
    }

    /// CAS claim for several producers.  Every push that succeeds without a single
    /// failed CAS counts down towards `fetch_add` mode; any failure resets the window.
    /// Out of line: it is the contended path and keeps the inlined push small.
    #[inline(never)]
    fn claim_cas(&self) -> usize {
        let mut backoff = Backoff::new();
        let mut tail = self.tail.index.load(Acquire);
        let mut failed = false;
        loop {
            match self
                .tail
                .index
                .compare_exchange_weak(tail, tail + 1, AcqRel, Acquire)
            {
                Ok(_) => {
                    if tail & OFFSET_MASK == BLOCK_CAP {
                        tail += 1;
                        continue;
                    }
                    let level = self.tail.contended.load(Relaxed);
                    if failed {
                        self.tail.contended.store(CONTENTION_WINDOW, Relaxed);
                    } else if level > 0 {
                        self.tail.contended.store(level - 1, Relaxed);
                    }
                    return tail;
                }
                Err(actual) => {
                    failed = true;
                    tail = actual;
                    backoff.spin();
                }
            }
        }
    }

    /// Bounded claim: check the capacity against the (cached, then real) head, then
    /// CAS.  Returns `None` when full.
    #[inline(never)]
    fn claim_bounded(&self, cap: usize) -> Option<usize> {
        let mut backoff = Backoff::new();
        let mut tail = self.tail.index.load(Acquire);
        loop {
            if tail & OFFSET_MASK == BLOCK_CAP {
                // Skip the sentinel position on behalf of everyone.
                match self
                    .tail
                    .index
                    .compare_exchange_weak(tail, tail + 1, AcqRel, Acquire)
                {
                    Ok(_) => tail += 1,
                    Err(actual) => tail = actual,
                }
                continue;
            }

            let mut head = self.tail.cached.load(Relaxed);
            if items_between(head, tail) >= cap {
                // The cache says full; confirm against the real head.  (This read costs
                // the consumers a cache-line transfer on their next pop, so a producer
                // that spins on `try_send` while the queue is full slows the consumer
                // down; `send().await` parks instead.  Throttling the re-read would make
                // `try_send` report `Full` after a pop freed a slot, so it stays exact.)
                head = self.head.index.load(Acquire);
                if items_between(head, tail) >= cap {
                    return None;
                }
                self.tail.cached.store(head, Relaxed);
            }

            match self
                .tail
                .index
                .compare_exchange_weak(tail, tail + 1, AcqRel, Acquire)
            {
                Ok(_) => return Some(tail),
                Err(actual) => {
                    tail = actual;
                    backoff.spin();
                }
            }
        }
    }

    /// Locates the block serving the lap of `pos`, walking backwards from `tail.block`.
    ///
    /// Every block from the one serving `pos` up to the current tail is live (the slot
    /// at `pos` is unwritten, so nothing at or after it can have been consumed), and a
    /// pointer that turns out to be stale is recognised by its `start`: `POOLED`, or a
    /// start that is too small (the installer has not published our block yet), makes
    /// us reload `tail.block`; a start that is too large means the block is ahead of
    /// ours and we follow `prev`.
    #[inline(always)]
    fn find_block(&self, pos: usize) -> *mut Block<T> {
        let start = pos & !OFFSET_MASK;
        let block = self.tail.block.load(Acquire);
        // SAFETY: blocks are never freed while the queue lives.
        if unsafe { (*block).phdr.0.start.load(Acquire) } == start {
            return block;
        }
        self.find_block_slow(pos)
    }

    /// The walk, out of line: taken only around block transitions.
    #[cold]
    #[inline(never)]
    fn find_block_slow(&self, pos: usize) -> *mut Block<T> {
        let start = pos & !OFFSET_MASK;
        let mut backoff = Backoff::new();
        let mut block = self.tail.block.load(Acquire);
        #[cfg(fcrs_debug)]
        let mut iters: u64 = 0;
        loop {
            #[cfg(fcrs_debug)]
            {
                iters += 1;
                if iters == 20_000_000 {
                    // SAFETY: blocks are never freed while the queue lives.
                    unsafe {
                        let tb = self.tail.block.load(Relaxed);
                        let mut chain = String::new();
                        let mut b = tb;
                        for _ in 0..6 {
                            if b.is_null() {
                                break;
                            }
                            chain += &format!(
                                "[{:p} start={} next={:p}] <-prev- ",
                                b,
                                (*b).phdr.0.start.load(Relaxed) as isize,
                                (*b).phdr.0.next.load(Relaxed)
                            );
                            b = (*b).phdr.0.prev.load(Relaxed);
                        }
                        eprintln!(
                            "FIND_BLOCK STUCK pos={} (lap {} off {}) target_start={} cur_block={:p} cur_start={} tail.index={} tail.block={:p} chain-from-tail: {}",
                            pos, pos >> LAP_SHIFT, pos & OFFSET_MASK, start, block,
                            (*block).phdr.0.start.load(Relaxed) as isize,
                            self.tail.index.load(Relaxed), tb, chain
                        );
                    }
                }
            }
            // SAFETY: blocks are never freed while the queue lives.
            let s = unsafe { (*block).phdr.0.start.load(Acquire) };
            if s == start {
                return block;
            }
            if s > start && s != POOLED {
                // SAFETY: as above.  `prev` of a live block ahead of ours is live; a
                // stale block's `prev` is valid memory too (links are never nulled on
                // recycling) and is re-validated by its own `start`.
                block = unsafe { (*block).phdr.0.prev.load(Acquire) };
                debug_assert!(!block.is_null(), "prev of a block ahead of the target");
                continue;
            }
            backoff.snooze();
            block = self.tail.block.load(Acquire);
        }
    }

    /// Installs the block for the lap after `block`'s, called by the producer that
    /// claimed the last slot of `block` (position `tail`), before it writes its value.
    ///
    /// # Safety
    /// `block` must be the live block serving `tail`'s lap.
    #[cold]
    unsafe fn install_next(&self, block: *mut Block<T>, tail: usize) {
        let next = self.take_block();
        (*next).phdr.0.prev.store(block, Relaxed);
        (*next).phdr.0.next.store(ptr::null_mut(), Relaxed);
        // `start` is the field a stale walker validates a block by, so it is published
        // last and with Release: whoever sees this start also sees the links above.
        (*next)
            .phdr
            .0
            .start
            .store((tail & !OFFSET_MASK) + LAP, Release);
        // Publish: consumers reach it through `next`, producers through `tail.block`.
        (*block).phdr.0.next.store(next, Release);
        self.tail.block.store(next, Release);
    }

    /// Removes the oldest value, or returns `None` if no *written* value is at the head
    /// (empty, the head slot is still being written, or a head transition exceeded
    /// the bounded spinning budget).
    ///
    /// On success also returns whether `wake_flag` (the parked-senders counter of a
    /// bounded channel) was non-zero when sampled right after the slot was claimed; the
    /// caller then wakes a parked sender.
    #[inline(always)]
    pub(crate) fn pop(&self, wake_flag: &AtomicUsize) -> Option<(T, bool)> {
        let mut backoff = Backoff::new();
        let mut head = self.head.index.load(Acquire);
        let mut block = self.head.block.load(Acquire);

        loop {
            let offset = head & OFFSET_MASK;

            // A consumer may be pre-empted while moving the head. Return to the
            // caller after a bounded spin so an async receiver can yield its executor.
            if offset == BLOCK_CAP {
                if !backoff.snooze_bounded() {
                    return None;
                }
                head = self.head.index.load(Acquire);
                block = self.head.block.load(Acquire);
                continue;
            }

            let lap = head >> LAP_SHIFT;
            let expected = written_tag(lap);
            // SAFETY: blocks are never freed while the queue lives, so `block` points at
            // valid memory even if it is stale; only an atomic is read before the CAS.
            let slot = unsafe { (*block).slots.get_unchecked(offset) };

            if slot.state.load(Acquire) != expected {
                // Not written.  If the head has not moved, `block` is provably the
                // block of `head` and the verdict is sound; otherwise the pair may be
                // stale, so look again.
                let now = self.head.index.load(Acquire);
                if now == head {
                    return None;
                }
                head = now;
                block = self.head.block.load(Acquire);
                continue;
            }

            match self
                .head
                .index
                .compare_exchange_weak(head, head + 1, AcqRel, Acquire)
            {
                Ok(_) => {
                    // Only bounded senders ever park, so unbounded pops skip the load.
                    let wake = self.capacity != UNBOUNDED && wake_flag.load(Relaxed) != 0;

                    // SAFETY: the successful CAS proves `head` was the live index, so the
                    // block loaded next to it is the block containing `head`, the slot
                    // was seen written, and the block cannot be recycled until this slot
                    // has been marked read.
                    unsafe {
                        let value = slot.value.with(|p| p.read().assume_init());
                        if offset + 1 == BLOCK_CAP {
                            self.finish_block(block, head);
                        } else {
                            mark_read((*block).chdr.0.read_marks.get_unchecked(offset));
                        }
                        return Some((value, wake));
                    }
                }
                Err(actual) => {
                    head = actual;
                    block = self.head.block.load(Acquire);
                    backoff.spin();
                }
            }
        }
    }
}

impl<T> Queue<T> {
    /// Receives with exclusive consumer access for the entire queue lifetime.
    ///
    /// # Safety
    /// No other receive operation may run concurrently, and this queue must never
    /// use the MPMC pop path: single-reader blocks deliberately have no read marks.
    #[inline(always)]
    pub(crate) unsafe fn pop_single(&self, wake_flag: &AtomicUsize) -> Option<(T, bool)> {
        let head = self.head.index.load(Relaxed);
        let block = self.head.block.load(Relaxed);
        let offset = head & OFFSET_MASK;
        debug_assert!(offset < BLOCK_CAP);
        let slot = (*block).slots.get_unchecked(offset);
        if slot.state.load(Acquire) != written_tag(head >> LAP_SHIFT) {
            return None;
        }
        // The only consumer reads the value before releasing capacity. No reader
        // can retain this block once this operation advances to the next one.
        let value = slot.value.with(|p| p.read().assume_init());
        let last = offset + 1 == BLOCK_CAP;
        if last {
            let next = (*block).phdr.0.next.load(Acquire);
            debug_assert!(!next.is_null());
            self.head.block.store(next, Release);
        }
        let advance = if last { 2 } else { 1 };
        let wake = if self.capacity == UNBOUNDED {
            // Unbounded senders never park on head, so no release sequence needs
            // preserving here. Receiver parking still uses the producer tail RMW.
            self.head.index.store(head + advance, Release);
            false
        } else {
            // Preserve the release sequence established by sender_parking(). A
            // plain store here would allow lost wakes even with one consumer.
            self.head.index.fetch_add(advance, AcqRel);
            wake_flag.load(Relaxed) != 0
        };
        if last {
            self.recycle_single(block);
        }
        Some((value, wake))
    }

    #[cold]
    #[inline(never)]
    unsafe fn recycle_single(&self, block: *mut Block<T>) {
        // All values were consumed serially; read marks were never set. Retain
        // links for stale producer walkers, just as in the MPMC reset path.
        (*block).phdr.0.start.store(POOLED, Relaxed);
        self.recycle(block);
    }

    /// Receives a ready prefix within one block, publishing capacity once.
    ///
    /// # Safety
    /// The same lifetime-wide exclusive-consumer contract as `pop_single` applies.
    /// The caller must notify the returned number of senders before another call
    /// which might panic (notably allocation for the next batch).
    #[inline]
    pub(crate) unsafe fn pop_single_batch(
        &self,
        buffer: &mut Vec<T>,
        limit: usize,
        wake_flag: &AtomicUsize,
    ) -> (usize, usize) {
        if limit == 0 {
            return (0, 0);
        }
        let head = self.head.index.load(Relaxed);
        let block = self.head.block.load(Relaxed);
        let offset = head & OFFSET_MASK;
        let tag = written_tag(head >> LAP_SHIFT);
        let max = limit.min(BLOCK_CAP - offset);
        if (*block).slots.get_unchecked(offset).state.load(Acquire) != tag {
            return (0, 0);
        }
        // Reserve before moving any values: after the first move, nothing may
        // unwind before head is advanced or Drop would destroy a moved value twice.
        buffer.reserve(max);
        let mut count = 0;
        while count < max {
            let slot = (*block).slots.get_unchecked(offset + count);
            if slot.state.load(Acquire) != tag {
                break;
            }
            buffer.push(slot.value.with(|p| p.read().assume_init()));
            count += 1;
        }
        let last = offset + count == BLOCK_CAP;
        if last {
            let next = (*block).phdr.0.next.load(Acquire);
            self.head.block.store(next, Release);
        }
        let advance = count + usize::from(last);
        let wake = if self.capacity == UNBOUNDED {
            self.head.index.store(head + advance, Release);
            0
        } else {
            self.head.index.fetch_add(advance, AcqRel);
            wake_flag.load(Relaxed).min(count)
        };
        if last {
            self.recycle_single(block);
        }
        (count, wake)
    }

    /// Block transition on the consumer side, run by the consumer that took the last
    /// slot of `block` (position `head`).  Out of line: once per 63 values.
    ///
    /// # Safety
    /// The caller has claimed and read slot `BLOCK_CAP - 1` of `block`.
    #[cold]
    #[inline(never)]
    unsafe fn finish_block(&self, block: *mut Block<T>, head: usize) {
        // Move the head to the next block first so other consumers wait as briefly as
        // possible.  The installing producer linked `next` before writing this slot,
        // and it published `tail.block = next` too, so nobody will look for this block
        // once it is recycled.
        let next = (*block).phdr.0.next.load(Acquire);
        debug_assert!(
            !next.is_null(),
            "last slot was published before its next block"
        );
        self.head.block.store(next, Release);
        // An RMW, not a plain store: a plain store would end the release sequence that
        // a parked bounded sender started with its `fetch_add(0)` on this index (see
        // the module docs), and later consumer CASes would no longer synchronise with it.
        self.head.index.swap(head + 2, AcqRel);

        // Every earlier slot was claimed before ours, but its reader may be paused
        // before copying the value or marking the read. Leave that block untouched
        // until a future installer observes every Release mark; do not hold this
        // consumer (and possibly an executor worker) hostage to the paused reader.
        if !(*block).readers_done() {
            // Retire it: every value here was claimed and moved out or is about to be by
            // the reader that claimed it, so the block holds nothing this queue owns.
            self.retired.put(block);
            return;
        }
        (*block).reset();
        self.recycle(block);
    }

    /// Debugging aid: a snapshot of the internal indices and the blocks at both ends.
    /// Racy by nature; only for diagnosing hangs.
    #[doc(hidden)]
    pub(crate) fn debug_dump(&self) -> String {
        // SAFETY: blocks are never freed while the queue lives.
        unsafe {
            let h = self.head.index.load(Relaxed);
            let t = self.tail.index.load(Relaxed);
            let hb = self.head.block.load(Relaxed);
            let tb = self.tail.block.load(Relaxed);
            let hs = (*hb).phdr.0.start.load(Relaxed);
            let ts = (*tb).phdr.0.start.load(Relaxed);
            let hoff = h & OFFSET_MASK;
            let hstate = if hoff < BLOCK_CAP {
                (*hb).slots.get_unchecked(hoff).state.load(Relaxed)
            } else {
                0
            };
            let mut chain = String::new();
            let mut b = tb;
            for _ in 0..6 {
                if b.is_null() {
                    break;
                }
                chain += &format!(
                    "[{:p} start={} next={:p}] <-prev- ",
                    b,
                    (*b).phdr.0.start.load(Relaxed) as isize,
                    (*b).phdr.0.next.load(Relaxed)
                );
                b = (*b).phdr.0.prev.load(Relaxed);
            }
            format!(
                "head={} (lap {} off {}) head.block={:p} start={} head_slot_state={} expected={} head.contended={} | tail={} tail.block={:p} start={} tail.contended={} | spare={:p} pool={} | chain: {}",
                h, h >> LAP_SHIFT, hoff, hb, hs as isize, hstate, written_tag(h >> LAP_SHIFT),
                self.head.contended.load(Relaxed),
                t, tb, ts as isize, self.tail.contended.load(Relaxed),
                self.spare.load(Relaxed),
                self.ready.stored.load(Relaxed) + self.retired.stored.load(Relaxed),
                chain
            )
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        // SAFETY: `&mut self` gives exclusive access; no operation is in flight.
        unsafe {
            let mut head = self.head.index.load(Relaxed);
            let tail = self.tail.index.load(Relaxed);
            let mut block = self.head.block.load(Relaxed);

            // Drop every value that was written but never read.
            while head < tail && !block.is_null() {
                let offset = head & OFFSET_MASK;
                if offset == BLOCK_CAP {
                    block = (*block).phdr.0.next.load(Relaxed);
                    head += 1;
                    continue;
                }
                let slot = (*block).slots.get_unchecked(offset);
                if slot.state.load(Relaxed) == written_tag(head >> LAP_SHIFT) {
                    slot.value
                        .with_mut(|p| ptr::drop_in_place((*p).as_mut_ptr()));
                }
                head += 1;
            }

            // Free the whole chain, the spare block and the pool.
            let mut block = self.head.block.load(Relaxed);
            while !block.is_null() {
                let next = (*block).phdr.0.next.load(Relaxed);
                Block::release(block);
                block = next;
            }
            let spare = self.spare.load(Relaxed);
            if !spare.is_null() {
                Block::release(spare);
            }
            // Exclusive channel destruction means even deferred readers have
            // finished. All values in retired blocks were already claimed and moved.
            // Every block ever allocated is in exactly one of: the chain above, the
            // spare slot, or one pool slot -- nothing is in flight under `&mut self` --
            // so this frees each of them exactly once.
            self.ready.release_all();
            self.retired.release_all();
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    /// Test-only views of the pool.  They walk every slot of every page, so they report
    /// the exact contents rather than the `stored` hint.
    impl<T> BlockPool<T> {
        /// Every block currently published, in page/slot order.
        fn snapshot(&self) -> Vec<*mut Block<T>> {
            let mut blocks = Vec::new();
            let mut page: *const PoolPage<T> = &self.first;
            loop {
                // SAFETY: pages live until `Queue::drop`.
                unsafe {
                    for slot in (*page).slots.iter() {
                        let block = slot.load(Acquire);
                        if !block.is_null() {
                            blocks.push(block);
                        }
                    }
                    let next = (*page).next.load(Acquire);
                    if next.is_null() {
                        return blocks;
                    }
                    page = next;
                }
            }
        }

        fn page_count(&self) -> usize {
            let mut pages = 1;
            let mut page: *const PoolPage<T> = &self.first;
            // SAFETY: as in `snapshot`.
            while let Some(next) = unsafe { (*page).next.load(Acquire).as_ref() } {
                pages += 1;
                page = next;
            }
            pages
        }
    }

    /// Every block this queue owns: the live chain, the spare slot and both pools.
    fn owned_blocks<T>(q: &Queue<T>) -> Vec<*mut Block<T>> {
        let mut blocks = Vec::new();
        let mut block = q.head.block.load(Acquire);
        while !block.is_null() {
            blocks.push(block);
            // SAFETY: blocks are never freed while the queue lives.
            block = unsafe { (*block).phdr.0.next.load(Acquire) };
        }
        let spare = q.spare.load(Acquire);
        if !spare.is_null() {
            blocks.push(spare);
        }
        blocks.extend(q.ready.snapshot());
        blocks.extend(q.retired.snapshot());
        blocks
    }

    fn unique(blocks: &[*mut u8]) -> bool {
        let mut sorted = blocks.to_vec();
        sorted.sort_unstable();
        let len = sorted.len();
        sorted.dedup();
        sorted.len() == len
    }

    fn as_addrs<T>(blocks: &[*mut Block<T>]) -> Vec<*mut u8> {
        blocks.iter().map(|&b| b as *mut u8).collect()
    }

    /// A block pointer a test thread may carry.  Sound for the same reason the queue's
    /// own `Send` impl is: blocks stay allocated and only the thread that owns one (here:
    /// took it out of a pool slot) ever touches it.
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    struct SendBlock<T>(*mut Block<T>);
    // SAFETY: as above.
    unsafe impl<T: Send> Send for SendBlock<T> {}

    #[test]
    fn items_between_skips_sentinels() {
        assert_eq!(items_between(0, 0), 0);
        assert_eq!(items_between(0, BLOCK_CAP), BLOCK_CAP);
        assert_eq!(items_between(0, LAP), BLOCK_CAP);
        assert_eq!(items_between(0, LAP + 1), BLOCK_CAP + 1);
        assert_eq!(items_between(BLOCK_CAP, LAP), 0);
        assert_eq!(items_between(5, 3), 0);
        assert_eq!(items_between(LAP - 2, LAP + 2), 3);
    }

    #[test]
    fn push_pop_across_many_blocks() {
        let q = Queue::new(None);
        let flag = AtomicUsize::new(0);
        for i in 0..10 * LAP {
            q.push(i, &flag).unwrap();
        }
        assert_eq!(q.len(), 10 * LAP);
        for i in 0..10 * LAP {
            assert_eq!(q.pop(&flag).map(|v| v.0), Some(i));
        }
        assert_eq!(q.pop(&flag).map(|v| v.0), None);
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn interleaved_push_pop_recycles_blocks() {
        let q = Queue::new(None);
        let flag = AtomicUsize::new(0);
        for round in 0..1000usize {
            q.push(round, &flag).unwrap();
            q.push(round + 1, &flag).unwrap();
            assert_eq!(q.pop(&flag).map(|v| v.0), Some(round));
            assert_eq!(q.pop(&flag).map(|v| v.0), Some(round + 1));
        }
        assert_eq!(q.pop(&flag).map(|v| v.0), None);
        assert!(q.ready.snapshot().len() <= 1);
    }

    #[test]
    fn backlog_grows_and_shrinks_through_pool() {
        let q = Queue::new(None);
        let flag = AtomicUsize::new(0);
        for round in 0..3 {
            for i in 0..20 * LAP {
                q.push(round * 100_000 + i, &flag).unwrap();
            }
            for i in 0..20 * LAP {
                assert_eq!(q.pop(&flag).map(|v| v.0), Some(round * 100_000 + i));
            }
            assert_eq!(q.pop(&flag).map(|v| v.0), None);
        }
        // Blocks were reused, not re-allocated: the pool holds the ones from round 0.
        assert!(q.ready.snapshot().len() >= 15);
    }

    #[test]
    fn bounded_is_exact() {
        let q = Queue::new(Some(3));
        let flag = AtomicUsize::new(0);
        q.push(1, &flag).unwrap();
        q.push(2, &flag).unwrap();
        q.push(3, &flag).unwrap();
        assert_eq!(q.push(4, &flag), Err(4));
        assert_eq!(q.pop(&flag).map(|v| v.0), Some(1));
        q.push(4, &flag).unwrap();
        assert_eq!(q.push(5, &flag), Err(5));
        for expected in 2..=4 {
            assert_eq!(q.pop(&flag).map(|v| v.0), Some(expected));
        }
        assert_eq!(q.pop(&flag).map(|v| v.0), None);
    }

    #[test]
    fn bounded_across_block_boundary() {
        let cap = LAP + 3;
        let q = Queue::new(Some(cap));
        let flag = AtomicUsize::new(0);
        for i in 0..cap {
            q.push(i, &flag).unwrap();
        }
        assert_eq!(q.push(usize::MAX, &flag), Err(usize::MAX));
        for i in 0..cap {
            assert_eq!(q.pop(&flag).map(|v| v.0), Some(i));
            q.push(cap + i, &flag).unwrap();
            assert_eq!(q.push(usize::MAX, &flag), Err(usize::MAX));
        }
    }

    #[test]
    fn parking_snapshot_reports_claims() {
        let q = Queue::new(None);
        let flag = AtomicUsize::new(0);
        assert!(!q.claims_below(q.receiver_parking()));
        q.push(7usize, &flag).unwrap();
        let tail = q.receiver_parking();
        assert!(q.claims_below(tail));
        assert_eq!(q.pop(&flag).map(|v| v.0), Some(7));
        assert!(!q.claims_below(tail));
    }

    #[test]
    fn retired_block_stays_live_until_its_reader_finishes() {
        for reclaim in [false, true] {
            let q = Queue::new(None);
            let flag = AtomicUsize::new(0);
            for i in 0..LAP {
                q.push(Box::new(i), &flag).unwrap();
            }

            // Pause a consumer after its successful head CAS, before it copies the
            // first value. Other consumers must keep making progress without reusing
            // this block, even as later blocks are repeatedly recycled.
            let block = q.head.block.load(Acquire);
            let slot = unsafe { &(*block).slots[0] };
            assert_eq!(slot.state.load(Acquire), written_tag(0));
            q.head
                .index
                .compare_exchange(0, 1, AcqRel, Acquire)
                .unwrap();
            for expected in 1..LAP {
                assert_eq!(*q.pop(&flag).unwrap().0, expected);
            }
            for value in LAP..8 * LAP {
                q.push(Box::new(value), &flag).unwrap();
                assert_eq!(*q.pop(&flag).unwrap().0, value);
            }
            assert_eq!(q.retired.snapshot(), vec![block]);

            // The paused reader still owns the original Box; recycling it early
            // would corrupt this value or cause a double-free (also checked by Miri).
            let held = unsafe { slot.value.with(|p| p.read().assume_init()) };
            assert_eq!(*held, 0);
            unsafe { mark_read(&(*block).chdr.0.read_marks[0]) };
            drop(held);

            if reclaim {
                // No ready overflow blocks exist in this interleaved workload, so
                // the next slow allocation must reclaim the now-finished block.
                assert!(q.ready.snapshot().is_empty());
                let reused = q.take_block_slow();
                assert_eq!(reused, block);
                assert!(q.retired.snapshot().is_empty());
                unsafe { q.recycle(reused) };
            }
            // Both reclamation and dropping a still-retired, now-quiescent block
            // must release its allocation without dropping any moved-out Box twice.
            drop(q);
        }
    }

    #[test]
    fn stalled_head_transition_returns_to_the_caller() {
        let q = Queue::new(None);
        let flag = AtomicUsize::new(0);
        for i in 0..LAP {
            q.push(i, &flag).unwrap();
        }
        for expected in 0..BLOCK_CAP - 1 {
            assert_eq!(q.pop(&flag).unwrap().0, expected);
        }

        // Pause the last consumer between claiming the final value and publishing
        // the next head. A peer's pop must return instead of waiting for this peer.
        let head = BLOCK_CAP - 1;
        let block = q.head.block.load(Acquire);
        let slot = unsafe { &(*block).slots[head] };
        assert_eq!(slot.state.load(Acquire), written_tag(0));
        q.head
            .index
            .compare_exchange(head, head + 1, AcqRel, Acquire)
            .unwrap();
        let held = unsafe { slot.value.with(|p| p.read().assume_init()) };
        assert_eq!(held, head);
        assert!(q.pop(&flag).is_none());
        // An async receiver must still recognize the queued next-block value and
        // arrange another poll instead of sleeping through this head transition.
        assert!(q.claims_below(q.receiver_parking()));
        unsafe { q.finish_block(block, head) };
        assert_eq!(q.pop(&flag).unwrap().0, BLOCK_CAP);
    }

    #[test]
    fn drop_releases_unread_values() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct D;
        impl Drop for D {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }
        {
            let q = Queue::new(None);
            let flag = AtomicUsize::new(0);
            for _ in 0..(3 * LAP + 7) {
                assert!(q.push(D, &flag).is_ok());
            }
            for _ in 0..(LAP + 2) {
                drop(q.pop(&flag));
            }
            assert_eq!(DROPS.load(Ordering::Relaxed), LAP + 2);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), 3 * LAP + 7);
    }

    #[test]
    fn threads_bounded_mpmc_all_values_arrive_once() {
        use std::sync::Arc;
        use std::thread;
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 2;
        const PER_PRODUCER: usize = 10_000;
        const CAP: usize = 5;

        let q = Arc::new(Queue::new(Some(CAP)));
        static FLAG: AtomicUsize = AtomicUsize::new(0);
        let mut handles = Vec::new();
        for p in 0..PRODUCERS {
            let q = q.clone();
            handles.push(thread::spawn(move || {
                for i in 0..PER_PRODUCER {
                    let mut v = p * PER_PRODUCER + i;
                    loop {
                        match q.push(v, &FLAG) {
                            Ok(_) => break,
                            Err(back) => {
                                v = back;
                                std::hint::spin_loop();
                            }
                        }
                    }
                }
            }));
        }
        let received = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut consumers = Vec::new();
        for _ in 0..CONSUMERS {
            let q = q.clone();
            let received = received.clone();
            consumers.push(thread::spawn(move || {
                let mut seen = Vec::new();
                loop {
                    if let Some((v, _)) = q.pop(&FLAG) {
                        seen.push(v);
                        if received.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1
                            == PRODUCERS * PER_PRODUCER
                        {
                            break;
                        }
                    } else if received.load(std::sync::atomic::Ordering::Relaxed)
                        == PRODUCERS * PER_PRODUCER
                    {
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                seen
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let mut all: Vec<usize> = consumers
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort_unstable();
        assert_eq!(all.len(), PRODUCERS * PER_PRODUCER);
        for (i, v) in all.iter().enumerate() {
            assert_eq!(*v, i);
        }
        assert_eq!(q.pop(&FLAG).map(|v| v.0), None);
    }

    #[test]
    fn threads_mpmc_all_values_arrive_once() {
        use std::sync::Arc;
        use std::thread;
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 4;
        const PER_PRODUCER: usize = 20_000;

        let q = Arc::new(Queue::new(None));
        static FLAG: AtomicUsize = AtomicUsize::new(0);
        let mut handles = Vec::new();
        for p in 0..PRODUCERS {
            let q = q.clone();
            handles.push(thread::spawn(move || {
                for i in 0..PER_PRODUCER {
                    q.push(p * PER_PRODUCER + i, &FLAG).unwrap();
                }
            }));
        }
        let received = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut consumers = Vec::new();
        for _ in 0..CONSUMERS {
            let q = q.clone();
            let received = received.clone();
            consumers.push(thread::spawn(move || {
                let mut seen = Vec::new();
                loop {
                    if let Some((v, _)) = q.pop(&FLAG) {
                        seen.push(v);
                        if received.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1
                            == PRODUCERS * PER_PRODUCER
                        {
                            break;
                        }
                    } else if received.load(std::sync::atomic::Ordering::Relaxed)
                        == PRODUCERS * PER_PRODUCER
                    {
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                seen
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let mut all: Vec<usize> = consumers
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort_unstable();
        assert_eq!(all.len(), PRODUCERS * PER_PRODUCER);
        for (i, v) in all.iter().enumerate() {
            assert_eq!(*v, i);
        }
        assert_eq!(q.pop(&FLAG).map(|v| v.0), None);
    }

    /// A backlog that recycles more blocks than one pool page holds.  A whole number of
    /// blocks, so a round leaves head and tail on a block boundary and the next round
    /// needs exactly as many blocks as the last one did.
    const MULTI_PAGE_VALUES: usize = (POOL_PAGE_SLOTS + 4) * BLOCK_CAP;

    #[test]
    fn pool_spans_pages_and_reuses_every_block() {
        let q = Queue::new(None);
        let flag = AtomicUsize::new(0);
        for i in 0..MULTI_PAGE_VALUES {
            q.push(i, &flag).unwrap();
        }
        for i in 0..MULTI_PAGE_VALUES {
            assert_eq!(q.pop(&flag).map(|v| v.0), Some(i));
        }

        // The embedded page overflowed, so the appended pages are in use.
        assert!(q.ready.page_count() >= 2, "pool did not grow a second page");
        let pooled = q.ready.snapshot();
        assert!(pooled.len() > POOL_PAGE_SLOTS);
        // One slot per block: publication is a CAS from null, so nothing is duplicated.
        assert!(unique(&as_addrs(&pooled)));
        let high_water = owned_blocks(&q);
        assert!(unique(&as_addrs(&high_water)));
        let pages = q.ready.page_count();

        // An identical second round must come entirely out of the pool.
        for i in 0..MULTI_PAGE_VALUES {
            q.push(i, &flag).unwrap();
        }
        assert!(
            q.ready.snapshot().len() < pooled.len(),
            "no block was taken"
        );
        for i in 0..MULTI_PAGE_VALUES {
            assert_eq!(q.pop(&flag).map(|v| v.0), Some(i));
        }
        let after = owned_blocks(&q);
        assert!(unique(&as_addrs(&after)));
        assert_eq!(
            after.len(),
            high_water.len(),
            "reuse allocated fresh blocks"
        );
        assert_eq!(q.ready.page_count(), pages, "reuse grew the pool");
    }

    #[test]
    fn multi_page_pool_drops_every_value_exactly_once() {
        use std::sync::atomic::{AtomicUsize as Counter, Ordering as Ord2};
        static DROPS: Counter = Counter::new(0);
        struct D;
        impl Drop for D {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ord2::Relaxed);
            }
        }
        let backlog = MULTI_PAGE_VALUES / 2;
        {
            let q = Queue::new(None);
            let flag = AtomicUsize::new(0);
            for _ in 0..MULTI_PAGE_VALUES {
                assert!(q.push(D, &flag).is_ok());
            }
            for _ in 0..MULTI_PAGE_VALUES {
                drop(q.pop(&flag));
            }
            assert!(q.ready.page_count() >= 2);
            assert_eq!(DROPS.load(Ord2::Relaxed), MULTI_PAGE_VALUES);
            // Leave values behind in blocks that came back out of the multi-page pool.
            for _ in 0..backlog {
                assert!(q.push(D, &flag).is_ok());
            }
        }
        assert_eq!(DROPS.load(Ord2::Relaxed), MULTI_PAGE_VALUES + backlog);
    }

    #[test]
    fn a_stale_hint_does_not_hide_blocks_after_a_long_empty_range() {
        let mut pool = BlockPool::<usize>::new();
        for _ in 0..2 * POOL_SCAN_BUDGET + 1 {
            // SAFETY: new empty blocks, exclusively owned until publication.
            unsafe { pool.put(Block::allocate()) };
        }
        for _ in 0..2 * POOL_SCAN_BUDGET {
            let block = pool.take().expect("populated range");
            // SAFETY: exclusively removed and never part of a live queue chain.
            unsafe { Block::release(block) };
        }
        // A taker paused before publishing its hint can legitimately restore this
        // old position after other threads empty two complete scan budgets.
        pool.set_hint(&pool.take_hint, &pool.first, 0);
        assert!(pool.take().is_none());
        assert!(pool.take().is_none());
        let last = pool
            .take()
            .expect("misses must advance past the empty range");
        // SAFETY: last owns the only remaining block; the pool owns its pages.
        unsafe {
            Block::release(last);
            pool.release_all();
        }
    }

    #[test]
    fn moving_a_used_pool_preserves_embedded_page_hints() {
        let pool = BlockPool::<usize>::new();
        for _ in 0..3 {
            // SAFETY: new empty blocks, exclusively owned until publication.
            unsafe { pool.put(Block::allocate()) };
        }
        let first = pool.take().unwrap();
        unsafe { Block::release(first) };
        // This invalidates pointers into the previous embedded page's storage.
        let mut pool = Box::new(pool);
        let a = pool.take().unwrap();
        let b = pool.take().unwrap();
        assert_ne!(a, b);
        assert!(pool.take().is_none());
        // Also exercise a put hint recorded before the move.
        unsafe {
            pool.put(a);
            pool.put(b);
            pool.release_all();
        }
    }

    #[test]
    fn pool_hands_each_block_to_exactly_one_taker() {
        use std::sync::Arc;
        use std::thread;
        const THREADS: usize = 4;
        // Fills four pages exactly, so one scan budget covers the whole pool and a take
        // only ever misses because a peer won the slot.
        let total = 3 * POOL_PAGE_SLOTS + 1;

        let q: Arc<Queue<usize>> = Arc::new(Queue::new(None));
        let mut made = Vec::with_capacity(total);
        for _ in 0..total {
            let block = Block::<usize>::allocate();
            made.push(block);
            // SAFETY: freshly allocated, owned by this thread, holding no values.
            unsafe { q.ready.put(block) };
        }
        assert_eq!(q.ready.snapshot().len(), total);
        assert_eq!(q.ready.page_count(), 4);

        let seen = Arc::new(AtomicUsize::new(0));
        let mut takers = Vec::new();
        for _ in 0..THREADS {
            let q = q.clone();
            let seen = seen.clone();
            takers.push(thread::spawn(move || {
                let mut mine = Vec::new();
                while seen.load(Relaxed) < total {
                    match q.ready.take() {
                        Some(block) => {
                            mine.push(SendBlock(block));
                            seen.fetch_add(1, Relaxed);
                        }
                        None => std::hint::spin_loop(),
                    }
                }
                mine
            }));
        }
        let mut taken: Vec<*mut Block<usize>> = takers
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .map(|b| b.0)
            .collect();

        // Every block came out once and only once, and no taker invented one.
        assert_eq!(taken.len(), total);
        assert!(unique(&as_addrs(&taken)));
        taken.sort_unstable();
        let mut expected = made.clone();
        expected.sort_unstable();
        assert_eq!(taken, expected);
        assert!(q.ready.snapshot().is_empty());

        for block in taken {
            // SAFETY: exclusively owned by this thread now, holding no values.
            unsafe { q.ready.put(block) };
        }
        // Dropping the last handle frees the queue's own block and all `total` of these.
        drop(q);
    }

    #[test]
    fn concurrent_take_and_publish_conserve_every_block() {
        use std::sync::Arc;
        use std::thread;
        const THREADS: usize = 4;
        const ROUNDS: usize = if cfg!(miri) { 8 } else { 2_000 };
        let total = POOL_PAGE_SLOTS + 3;

        let q: Arc<Queue<usize>> = Arc::new(Queue::new(None));
        let mut made = Vec::with_capacity(total);
        for _ in 0..total {
            let block = Block::<usize>::allocate();
            made.push(block);
            // SAFETY: freshly allocated and exclusively owned.
            unsafe { q.ready.put(block) };
        }

        let mut workers = Vec::new();
        for _ in 0..THREADS {
            let q = q.clone();
            workers.push(thread::spawn(move || {
                for _ in 0..ROUNDS {
                    if let Some(block) = q.ready.take() {
                        // SAFETY: taken exclusively, handed straight back untouched.
                        unsafe { q.ready.put(block) };
                    }
                }
            }));
        }
        for w in workers {
            w.join().unwrap();
        }

        let mut pooled = q.ready.snapshot();
        assert!(unique(&as_addrs(&pooled)), "a block was published twice");
        pooled.sort_unstable();
        made.sort_unstable();
        assert_eq!(pooled, made, "the pool lost or invented a block");
        drop(q);
    }

    #[test]
    fn retired_blocks_are_owned_before_the_readers_check() {
        let q: Queue<usize> = Queue::new(None);
        let mut retired = Vec::new();
        for _ in 0..RETIRED_PROBES {
            let block = Block::<usize>::allocate();
            // SAFETY: freshly allocated and exclusively owned; no read marks are set,
            // so each of these looks like a block a reader has not finished with.
            unsafe { q.retired.put(block) };
            retired.push(block);
        }

        // Nothing is reclaimable: every probe must hand its block back untouched.
        assert!(q.reclaim_retired().is_none());
        let mut parked = q.retired.snapshot();
        parked.sort_unstable();
        let mut expected = retired.clone();
        expected.sort_unstable();
        assert_eq!(parked, expected);

        // Finish the readers of one block. The probe budget covers every retired block
        // here, so exactly that one is taken, reset and handed out.
        let target = retired[RETIRED_PROBES - 1];
        // SAFETY: the block is allocated and quiescent.
        unsafe {
            for mark in &(*target).chdr.0.read_marks {
                mark_read(mark);
            }
        }
        assert_eq!(q.reclaim_retired(), Some(target));
        // SAFETY: as above.
        unsafe {
            assert_eq!((*target).phdr.0.start.load(Acquire), POOLED);
            assert!((*target)
                .chdr
                .0
                .read_marks
                .iter()
                .all(|m| m.load(Acquire) == 0));
        }
        let left = q.retired.snapshot();
        assert_eq!(left.len(), RETIRED_PROBES - 1);
        assert!(!left.contains(&target));

        // SAFETY: reset, exclusively owned; hand it back so the queue frees it.
        unsafe { q.ready.put(target) };
        drop(q);
    }
}

#[cfg(all(test, feature = "loom"))]
mod pool_loom_tests {
    use super::*;
    use crate::sync::Arc;
    use loom::thread;

    fn model(f: impl Fn() + Send + Sync + 'static) {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(f);
    }

    #[test]
    fn pool_transfers_exclusive_payload_access() {
        model(|| {
            let pool = Arc::new(BlockPool::<usize>::new());
            unsafe { pool.put(Block::allocate()) };
            let mut workers = Vec::new();
            for marker in 1..=2 {
                let pool = pool.clone();
                workers.push(thread::spawn(move || {
                    if let Some(block) = pool.take() {
                        // The tracked UnsafeCell remains borrowed over the yield.
                        // Duplicate ownership would produce an overlapping access.
                        unsafe {
                            (*block).slots[0].value.with_mut(|p| {
                                (*p).write(marker);
                                thread::yield_now();
                                assert_eq!((*p).assume_init_read(), marker);
                            });
                            pool.put(block);
                        }
                    }
                }));
            }
            for worker in workers {
                worker.join().unwrap();
            }
            let mut pool = match Arc::try_unwrap(pool) {
                Ok(p) => p,
                Err(_) => panic!("pool still shared"),
            };
            assert_eq!(pool.stored.load(Relaxed), 1);
            unsafe { pool.release_all() };
        });
    }

    #[test]
    fn concurrent_page_append_keeps_all_blocks() {
        model(|| {
            let pool = Arc::new(BlockPool::<usize>::new());
            for _ in 0..POOL_PAGE_SLOTS {
                unsafe { pool.put(Block::allocate()) };
            }
            let other = pool.clone();
            let worker = thread::spawn(move || unsafe { other.put(Block::allocate()) });
            unsafe { pool.put(Block::allocate()) };
            worker.join().unwrap();
            let mut pool = match Arc::try_unwrap(pool) {
                Ok(p) => p,
                Err(_) => panic!("pool still shared"),
            };
            let mut blocks = Vec::new();
            while let Some(block) = pool.take() {
                blocks.push(block);
            }
            assert_eq!(blocks.len(), POOL_PAGE_SLOTS + 2);
            for (i, &block) in blocks.iter().enumerate() {
                assert!(!blocks[..i].contains(&block), "duplicate block ownership");
            }
            for block in blocks {
                unsafe { Block::release(block) };
            }
            unsafe { pool.release_all() };
        });
    }
}
