//! The lock-free unbounded MPMC queue at the heart of the channel.
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
//!   CAS contention serialises the claims so each write lands before the next claim.
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
//!   spare slot and a mutex-protected overflow pool, accessed only when acquiring or
//!   recycling blocks. This makes it sound to dereference a block pointer that may be
//!   stale: it always points at valid memory, a pooled block carries `start ==
//!   usize::MAX`, a re-linked one carries a start that can never equal a lap it served
//!   before, a stale slot state can never match the current lap, and the CAS on
//!   `head.index` rejects a stale consumer claim.  Memory therefore stays at the
//!   channel's high-water mark until it is dropped.
//! * A bounded producer caches the last head it saw (`tail.cached`) and reloads the
//!   real head only when the cache says the queue looks full.
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
//! index is an `AcqRel` CAS that reads from (the release sequence headed by) that RMW,
//! so it synchronizes-with the sleeper and a plain `Relaxed` load of the flag right after
//! the CAS is guaranteed to observe it.  This needs every modification of those two
//! indices to be an RMW (a plain store would end the release sequence), which is why the
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

use crate::sync::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use crate::sync::{
    spin_loop, yield_now, AtomicPtr, AtomicU8, AtomicUsize, CachePadded, Mutex, UnsafeCell,
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
    #[inline(always)]
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

/// Blocks outside the live chain. A retired block still belongs to its outstanding
/// readers and must pass `readers_done` before it can be reused.
struct BlockPool<T> {
    ready: Vec<*mut Block<T>>,
    retired: Vec<*mut Block<T>>,
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
    /// Overflow pool for recycled blocks (see the module docs on never freeing).
    pool: Mutex<BlockPool<T>>,
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
            pool: Mutex::new(BlockPool {
                ready: Vec::new(),
                retired: Vec::new(),
            }),
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
        items_between(head, tail)
    }

    #[inline]
    fn lock_pool(&self) -> crate::sync::MutexGuard<'_, BlockPool<T>> {
        self.pool.lock().unwrap_or_else(|e| e.into_inner())
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
        let retired = {
            let mut pool = self.lock_pool();
            if let Some(block) = pool.ready.pop() {
                return block;
            }
            // Never wait for a reader. If no retired block is ready, allocate another
            // block. Removal under the lock gives this producer sole recycling rights.
            let ready = pool.retired.iter().position(|&block| {
                // SAFETY: retired blocks are allocated and stay outside the live chain.
                unsafe { (*block).readers_done() }
            });
            ready.map(|i| pool.retired.swap_remove(i))
        };
        if let Some(block) = retired {
            // SAFETY: every reader's Release mark was observed with Acquire above,
            // and no other producer can remove the same block from the retired list.
            unsafe { (*block).reset() };
            return block;
        }
        Block::allocate()
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

    #[cold]
    fn recycle_slow(&self, block: *mut Block<T>) {
        self.lock_pool().ready.push(block);
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
            self.lock_pool().retired.push(block);
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
                self.spare.load(Relaxed), {
                    let pool = self.lock_pool();
                    pool.ready.len() + pool.retired.len()
                }, chain
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
            let pool = &mut *self.lock_pool();
            // Exclusive channel destruction means even deferred readers have
            // finished. All values in retired blocks were already claimed and moved.
            for block in pool.ready.drain(..).chain(pool.retired.drain(..)) {
                Block::release(block);
            }
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

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
        assert!(q.lock_pool().ready.len() <= 1);
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
        assert!(q.lock_pool().ready.len() >= 15);
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
            assert_eq!(q.lock_pool().retired.as_slice(), &[block]);

            // The paused reader still owns the original Box; recycling it early
            // would corrupt this value or cause a double-free (also checked by Miri).
            let held = unsafe { slot.value.with(|p| p.read().assume_init()) };
            assert_eq!(*held, 0);
            unsafe { mark_read(&(*block).chdr.0.read_marks[0]) };
            drop(held);

            if reclaim {
                // No ready overflow blocks exist in this interleaved workload, so
                // the next slow allocation must reclaim the now-finished block.
                assert!(q.lock_pool().ready.is_empty());
                let reused = q.take_block_slow();
                assert_eq!(reused, block);
                assert!(q.lock_pool().retired.is_empty());
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
}
