//! # rapidfire
//!
//! An ultra low-latency, lock-free, dependency-free async MPMC channel for Rust.
//!
//! Designed for high-throughput, latency-critical pipelines (trading bots, WebSocket
//! fan-in, market-data multiplexers) where message loss is unacceptable and every
//! nanosecond on the hot path counts.
//!
//! ## Architecture
//!
//! - **Own lock-free block queue, zero dependencies.**  Values live in 63-slot blocks
//!   chained through `prev`/`next` pointers.  Producers claim a slot on the tail index
//!   (a wait-free `fetch_add` while uncontended, a CAS once contention is observed,
//!   because with several producers CAS serialises the claims and keeps adjacent-slot
//!   writes from fighting over one cache line); consumers check the head slot's state
//!   and claim it with a CAS only once the value is there, so they never touch the
//!   producers' cache line.  See `queue.rs` for the full protocol and its proofs.
//! - **Lap-tagged slot states, never-freed blocks.**  A slot's state word carries the
//!   block's lap number, so recycled slot states need no reset and a stale state can never
//!   be mistaken for a fresh one.  Blocks cycle through a spare slot and a small pool
//!   instead of being freed, which is what makes it sound to look at a slot before
//!   owning it.  Memory stays at the channel's high-water mark until it is dropped.
//! - **128-byte cache-line isolation.**  Producer state, consumer state, the spare
//!   block, each block's producer header and consumer header (read marks) and the
//!   rarely written wake-up flags all live on separate lines.  On the hot path the
//!   only lines that cross cores are the slot lines carrying the values.
//! - **Zero-cost sleep and wake, no `SeqCst` on the hot path.**  While messages flow
//!   no lock is taken and no waker is cloned.  A producer only does a `Relaxed` load
//!   of one read-mostly flag right after its claim; the party that goes to sleep pays
//!   instead, with one RMW on the other side's index that every later claim
//!   synchronises with (release-sequence argument, model-checked with loom).
//! - **Cancellation safe.**  Dropping a `Recv`/`Send` future (e.g. in `tokio::select!`)
//!   removes its waker; no counter leaks.
//! - **Drop-in API.**  Matches `async-channel`: [`unbounded`], [`bounded`], `send`,
//!   `recv`, `try_send`, `try_recv`, `close`, counts and lengths.
//!
//! ## Example
//!
//! ```
//! use rapidfire::unbounded;
//!
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let (tx, rx) = unbounded::<u64>();
//! tx.send(42).await.unwrap();
//! assert_eq!(rx.recv().await.unwrap(), 42);
//! # });
//! ```

#![warn(missing_docs)]

mod queue;
mod sync;

pub mod mpsc;

use crate::queue::{Backoff, Queue};
use crate::sync::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use crate::sync::{Arc, AtomicBool, AtomicU64, AtomicUsize, CachePadded, Mutex, MutexGuard};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

// ============================================================================
// Error Types
// ============================================================================

/// An error returned when attempting to send a message on a closed channel.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct SendError<T>(pub T);

impl<T> SendError<T> {
    /// Unwraps the message that could not be sent.
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sending on a closed channel")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// An error returned when attempting to receive from a closed and empty channel.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "receiving on an empty and closed channel")
    }
}

impl std::error::Error for RecvError {}

/// An error returned by [`Sender::try_send`].
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum TrySendError<T> {
    /// The channel is full (only applicable to bounded channels).
    Full(T),
    /// The channel is closed.
    Closed(T),
}

impl<T> TrySendError<T> {
    /// Unwraps the message that could not be sent.
    #[inline]
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(val) => val,
            TrySendError::Closed(val) => val,
        }
    }

    /// Returns `true` if the error was caused by a full channel.
    #[inline]
    pub fn is_full(&self) -> bool {
        matches!(self, TrySendError::Full(_))
    }

    /// Returns `true` if the error was caused by a closed channel.
    #[inline]
    pub fn is_closed(&self) -> bool {
        matches!(self, TrySendError::Closed(_))
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.debug_tuple("TrySendError::Full").finish(),
            TrySendError::Closed(_) => f.debug_tuple("TrySendError::Closed").finish(),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "sending on a full channel"),
            TrySendError::Closed(_) => write!(f, "sending on a closed channel"),
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// An error returned by [`Receiver::try_recv`].
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum TryRecvError {
    /// The channel is currently empty.
    Empty,
    /// The channel is empty and closed.
    Closed,
}

impl TryRecvError {
    /// Returns `true` if the channel is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(self, TryRecvError::Empty)
    }

    /// Returns `true` if the channel is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        matches!(self, TryRecvError::Closed)
    }
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "receiving on an empty channel"),
            TryRecvError::Closed => write!(f, "receiving on an empty and closed channel"),
        }
    }
}

impl std::error::Error for TryRecvError {}

// ============================================================================
// Shared state
// ============================================================================

/// Flags that the hot path only *reads*; written on clone/drop/sleep/wake.
struct Flags {
    closed: AtomicBool,
    waiting_receivers: AtomicUsize,
    waiting_senders: AtomicUsize,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    next_waiter_id: AtomicU64,
}

/// A FIFO of parked wakers.  Invariant: while the lock is held, `count` (the
/// matching `Flags` counter) equals `list.len()`.
struct WaiterList {
    list: Mutex<VecDeque<(u64, Waker)>>,
}

impl WaiterList {
    fn new() -> Self {
        WaiterList {
            list: Mutex::new(VecDeque::with_capacity(4)),
        }
    }

    #[inline]
    fn lock(&self) -> MutexGuard<'_, VecDeque<(u64, Waker)>> {
        self.list.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Registers (or refreshes) `id`'s waker and bumps `count` if a new entry was added.
    #[cold]
    fn register(
        &self,
        count: &AtomicUsize,
        next_id: &AtomicU64,
        id: &mut Option<u64>,
        waker: &Waker,
    ) {
        let mut list = self.lock();
        if let Some(my) = *id {
            if let Some(entry) = list.iter_mut().find(|(entry_id, _)| *entry_id == my) {
                if !entry.1.will_wake(waker) {
                    entry.1 = waker.clone();
                }
                return;
            }
        }
        let my = match *id {
            Some(my) => my,
            None => {
                let my = next_id.fetch_add(1, Relaxed);
                *id = Some(my);
                my
            }
        };
        list.push_back((my, waker.clone()));
        // Made visible to the other side by the caller's `*_parking` RMW (see queue.rs).
        count.fetch_add(1, Release);
    }

    /// Removes `id`'s entry if it is still parked.
    ///
    /// If the entry is gone, a `notify_one` already popped it: that wake-up was meant
    /// to make *someone* consume a value (or a freed slot).  A future that is being
    /// cancelled (`forward == true`) never will, so the wake-up is passed on to the
    /// next parked waiter instead of being lost.  A future that completed took its
    /// value itself and passes nothing on.
    #[cold]
    fn unregister(&self, count: &AtomicUsize, id: &mut Option<u64>, forward: bool) {
        if let Some(my) = id.take() {
            // This future's registration increment happens-before this load. A later
            // zero count therefore proves its entry was already removed by a notify.
            // Concurrent registrations cannot restore our entry. Cancellation keeps
            // the locked path so an absorbed wake-up is still forwarded.
            if !forward && count.load(Acquire) == 0 {
                return;
            }
            let mut list = self.lock();
            if let Some(pos) = list.iter().position(|(entry_id, _)| *entry_id == my) {
                list.remove(pos);
                count.fetch_sub(1, SeqCst);
                return;
            }
            drop(list);
            if forward {
                self.notify_one(count);
            }
        }
    }

    /// Wakes the longest-waiting entry, if any.
    #[cold]
    fn notify_one(&self, count: &AtomicUsize) {
        let waker = {
            let mut list = self.lock();
            match list.pop_front() {
                Some((_, waker)) => {
                    count.fetch_sub(1, SeqCst);
                    Some(waker)
                }
                None => None,
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Wakes every entry.
    #[cold]
    fn notify_all(&self, count: &AtomicUsize) {
        let wakers: Vec<Waker> = {
            let mut list = self.lock();
            count.store(0, SeqCst);
            list.drain(..).map(|(_, waker)| waker).collect()
        };
        for waker in wakers {
            waker.wake();
        }
    }

    /// Wakes up to `limit` senders after an exclusive receive batch frees slots.
    #[cold]
    fn notify_many(&self, count: &AtomicUsize, limit: usize) {
        if limit == 1 {
            self.notify_one(count);
            return;
        }
        let wakers: Vec<Waker> = {
            let mut list = self.lock();
            let n = limit.min(list.len());
            // Allocate before removing entries or decrementing their count.
            let mut wakers = Vec::with_capacity(n);
            for _ in 0..n {
                wakers.push(list.pop_front().unwrap().1);
            }
            count.fetch_sub(n, SeqCst);
            wakers
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

struct Inner<T> {
    queue: Queue<T>,
    flags: CachePadded<Flags>,
    recv_waiters: CachePadded<WaiterList>,
    send_waiters: CachePadded<WaiterList>,
}

impl<T> Inner<T> {
    fn new(capacity: Option<usize>) -> Self {
        Inner {
            queue: Queue::new(capacity),
            flags: CachePadded(Flags {
                closed: AtomicBool::new(false),
                waiting_receivers: AtomicUsize::new(0),
                waiting_senders: AtomicUsize::new(0),
                senders: AtomicUsize::new(1),
                receivers: AtomicUsize::new(1),
                next_waiter_id: AtomicU64::new(1),
            }),
            recv_waiters: CachePadded(WaiterList::new()),
            send_waiters: CachePadded(WaiterList::new()),
        }
    }

    #[inline(always)]
    fn is_closed(&self) -> bool {
        self.flags.closed.load(Acquire)
    }

    /// Closes the channel and wakes everyone.  Returns `false` if it was already closed.
    fn close(&self) -> bool {
        // SeqCst: a receiver that loads `closed` with SeqCst before popping is then
        // guaranteed to observe every value pushed before this close.
        if self.flags.closed.swap(true, SeqCst) {
            return false;
        }
        self.recv_waiters.notify_all(&self.flags.waiting_receivers);
        self.send_waiters.notify_all(&self.flags.waiting_senders);
        true
    }

    #[inline(always)]
    fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        // Relaxed is enough: a send that races with a close either lands or fails;
        // nothing is ordered by it.  As with tokio's and async-channel's channels, a
        // send that observes "open" and then races with `close()` can succeed after
        // the receivers have already reported `Closed`; that value is dropped with the
        // channel.  Closing the read side while senders are still active is the
        // caller's race to avoid; making it exact would cost a `SeqCst` load after
        // every claim.
        if self.flags.closed.load(Relaxed) {
            return Err(TrySendError::Closed(msg));
        }
        match self.queue.push(msg, &self.flags.waiting_receivers) {
            Ok(wake) => {
                if wake {
                    self.recv_waiters.notify_one(&self.flags.waiting_receivers);
                }
                Ok(())
            }
            Err(msg) => Err(TrySendError::Full(msg)),
        }
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut wait = Backoff::new();
        loop {
            match self.try_recv_nonblocking() {
                Ok(msg) => return Ok(msg),
                Err(RecvState::Empty) => return Err(TryRecvError::Empty),
                Err(RecvState::Closed) => return Err(TryRecvError::Closed),
                // Closed with an undrained value: wait for its writer or a pending
                // head transition rather than report `Closed` before draining it.
                Err(RecvState::Busy) => wait.snooze(),
            }
        }
    }

    /// One attempt to receive. `Busy` means the closed channel still has values
    /// awaiting publication or a head transition.
    #[inline(always)]
    fn try_recv_nonblocking(&self) -> Result<T, RecvState> {
        if let Some((msg, wake)) = self.queue.pop(&self.flags.waiting_senders) {
            if wake {
                self.send_waiters.notify_one(&self.flags.waiting_senders);
            }
            return Ok(msg);
        }
        Err(self.recv_state_when_empty())
    }

    /// Slow path: nothing was written at the head.  The SeqCst `closed` load is
    /// ordered after the SeqCst close, hence after every claim that preceded it, and
    /// the SeqCst index check then sees those claims.
    #[cold]
    fn recv_state_when_empty(&self) -> RecvState {
        if !self.flags.closed.load(SeqCst) {
            RecvState::Empty
        } else if self.queue.is_empty_seqcst() {
            RecvState::Closed
        } else {
            RecvState::Busy
        }
    }
}

/// Outcome of a receive attempt that found no value.
enum RecvState {
    /// No value available on this attempt while the channel is open.
    Empty,
    /// Closed and drained.
    Closed,
    /// Closed, but a write or head transition is still in flight.
    Busy,
}

// ============================================================================
// Sender & Receiver
// ============================================================================

/// The sending half of a channel.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

/// The receiving half of a channel.
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

// SAFETY: `Inner<T>` is `Send + Sync` for `T: Send` (the queue hands values across
// threads with proper synchronisation; everything else is atomics and mutexes).
unsafe impl<T: std::marker::Send> std::marker::Send for Sender<T> {}
unsafe impl<T: std::marker::Send> Sync for Sender<T> {}
unsafe impl<T: std::marker::Send> std::marker::Send for Receiver<T> {}
unsafe impl<T: std::marker::Send> Sync for Receiver<T> {}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.flags.senders.fetch_add(1, Relaxed);
        Sender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.inner.flags.receivers.fetch_add(1, Relaxed);
        Receiver {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.inner.flags.senders.fetch_sub(1, AcqRel) == 1 {
            // Last sender gone: close and wake the receivers.
            self.inner.close();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if self.inner.flags.receivers.fetch_sub(1, AcqRel) == 1 {
            // Last receiver gone: close and wake the senders.
            self.inner.close();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .field("is_closed", &self.is_closed())
            .field("senders", &self.sender_count())
            .field("receivers", &self.receiver_count())
            .finish()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .field("is_closed", &self.is_closed())
            .field("senders", &self.sender_count())
            .field("receivers", &self.receiver_count())
            .finish()
    }
}

// ============================================================================
// Channel Constructors
// ============================================================================

/// Creates an unbounded channel.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner::new(None));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

/// Creates a bounded channel with the specified capacity.
///
/// # Panics
///
/// Panics if `capacity` is zero.
pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    let inner = Arc::new(Inner::new(Some(capacity)));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

// ============================================================================
// Sender Implementation
// ============================================================================

impl<T> Sender<T> {
    /// Attempts to send a message into the channel without awaiting.
    #[inline(always)]
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        self.inner.try_send(msg)
    }

    /// Sends a message into the channel asynchronously.
    ///
    /// On an unbounded channel this completes on the first poll; on a bounded channel
    /// it waits for room.
    #[inline]
    pub fn send(&self, msg: T) -> Send<'_, T> {
        Send {
            sender: self,
            msg: Some(msg),
            waiter_id: None,
        }
    }

    /// Closes the channel.  Returns `true` if this call closed it.
    #[inline]
    pub fn close(&self) -> bool {
        self.inner.close()
    }

    /// Returns `true` if the channel is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Returns the number of messages currently in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.queue.len()
    }

    /// Returns `true` if the channel is currently empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the capacity of the channel, or `None` if unbounded.
    #[inline]
    pub fn capacity(&self) -> Option<usize> {
        self.inner.queue.capacity()
    }

    /// Returns the current number of senders.
    #[inline]
    pub fn sender_count(&self) -> usize {
        self.inner.flags.senders.load(Relaxed)
    }

    /// Returns the current number of receivers.
    #[inline]
    pub fn receiver_count(&self) -> usize {
        self.inner.flags.receivers.load(Relaxed)
    }
}

// ============================================================================
// Receiver Implementation
// ============================================================================

impl<T> Receiver<T> {
    /// Debugging aid (unstable, hidden): a racy snapshot of the queue internals.
    #[doc(hidden)]
    pub fn __debug_dump(&self) -> String {
        self.inner.queue.debug_dump()
    }

    /// Attempts to receive a message from the channel without awaiting.
    #[inline(always)]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }

    /// Receives a message from the channel asynchronously.
    #[inline]
    pub fn recv(&self) -> Recv<'_, T> {
        Recv {
            receiver: self,
            waiter_id: None,
        }
    }

    /// Closes the channel.  Returns `true` if this call closed it.
    #[inline]
    pub fn close(&self) -> bool {
        self.inner.close()
    }

    /// Returns `true` if the channel is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Returns the number of messages currently in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.queue.len()
    }

    /// Returns `true` if the channel is currently empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the capacity of the channel, or `None` if unbounded.
    #[inline]
    pub fn capacity(&self) -> Option<usize> {
        self.inner.queue.capacity()
    }

    /// Returns the current number of senders.
    #[inline]
    pub fn sender_count(&self) -> usize {
        self.inner.flags.senders.load(Relaxed)
    }

    /// Returns the current number of receivers.
    #[inline]
    pub fn receiver_count(&self) -> usize {
        self.inner.flags.receivers.load(Relaxed)
    }
}

// ============================================================================
// Futures: Send & Recv
// ============================================================================

/// Future returned by [`Sender::send`].
pub struct Send<'a, T> {
    sender: &'a Sender<T>,
    msg: Option<T>,
    waiter_id: Option<u64>,
}

impl<T> Send<'_, T> {
    /// `forward`: this future is being cancelled rather than completed, so a wake-up
    /// it may have absorbed is passed on to the next parked sender.
    #[inline(always)]
    fn unregister(&mut self, forward: bool) {
        if self.waiter_id.is_some() {
            let inner = &*self.sender.inner;
            inner.send_waiters.unregister(
                &inner.flags.waiting_senders,
                &mut self.waiter_id,
                forward,
            );
        }
    }
}

impl<T> Drop for Send<'_, T> {
    fn drop(&mut self) {
        self.unregister(true);
    }
}

impl<T> Unpin for Send<'_, T> {}
impl<T> Unpin for Recv<'_, T> {}

impl<T> Future for Send<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let inner = &*this.sender.inner;
        let msg = this.msg.take().expect("Send polled after completion");

        let msg = match inner.try_send(msg) {
            Ok(()) => {
                this.unregister(false);
                return Poll::Ready(Ok(()));
            }
            Err(TrySendError::Closed(msg)) => {
                this.unregister(false);
                return Poll::Ready(Err(SendError(msg)));
            }
            Err(TrySendError::Full(msg)) => msg,
        };

        // Bounded channel is full: register, publish the flag to consumers, then
        // re-check so a pop that raced with the registration cannot be missed.
        inner.send_waiters.register(
            &inner.flags.waiting_senders,
            &inner.flags.next_waiter_id,
            &mut this.waiter_id,
            cx.waker(),
        );
        inner.queue.sender_parking();

        match inner.try_send(msg) {
            Ok(()) => {
                this.unregister(false);
                Poll::Ready(Ok(()))
            }
            Err(TrySendError::Closed(msg)) => {
                this.unregister(false);
                Poll::Ready(Err(SendError(msg)))
            }
            Err(TrySendError::Full(msg)) => {
                this.msg = Some(msg);
                Poll::Pending
            }
        }
    }
}

/// Future returned by [`Receiver::recv`].
pub struct Recv<'a, T> {
    receiver: &'a Receiver<T>,
    waiter_id: Option<u64>,
}

impl<T> Recv<'_, T> {
    /// `forward`: this future is being cancelled rather than completed, so a wake-up
    /// it may have absorbed is passed on to the next parked receiver.
    #[inline(always)]
    fn unregister(&mut self, forward: bool) {
        if self.waiter_id.is_some() {
            let inner = &*self.receiver.inner;
            inner.recv_waiters.unregister(
                &inner.flags.waiting_receivers,
                &mut self.waiter_id,
                forward,
            );
        }
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        self.unregister(true);
    }
}

impl<T> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let inner = &*this.receiver.inner;

        // Fast path: no lock, no waker.
        match inner.try_recv_nonblocking() {
            Ok(msg) => {
                this.unregister(false);
                return Poll::Ready(Ok(msg));
            }
            Err(RecvState::Closed) => {
                this.unregister(false);
                return Poll::Ready(Err(RecvError));
            }
            Err(RecvState::Empty) | Err(RecvState::Busy) => {}
        }

        // Empty: register, publish the flag to producers, then re-check so a push that
        // raced with the registration cannot be missed.
        inner.recv_waiters.register(
            &inner.flags.waiting_receivers,
            &inner.flags.next_waiter_id,
            &mut this.waiter_id,
            cx.waker(),
        );
        let tail = inner.queue.receiver_parking();

        let mut wait = Backoff::new();
        loop {
            let busy = match inner.try_recv_nonblocking() {
                Ok(msg) => {
                    this.unregister(false);
                    return Poll::Ready(Ok(msg));
                }
                Err(RecvState::Closed) => {
                    this.unregister(false);
                    return Poll::Ready(Err(RecvError));
                }
                Err(RecvState::Busy) => true,
                Err(RecvState::Empty) => inner.queue.claims_below(tail),
            };
            if !busy {
                // Truly empty as of the snapshot: every later claim will see our flag.
                return Poll::Pending;
            }
            // A write claimed before our registration, or a paused head transition,
            // may complete without waking us. Poll briefly, then return the thread
            // to the executor and ask to be polled again if the peer is still paused.
            if !wait.snooze_bounded() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }
}
