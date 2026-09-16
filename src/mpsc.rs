//! Channels with many senders and exactly one receiver.
//!
//! Receiving requires exclusive access. The receiver cannot be cloned, allowing
//! the queue to omit arbitration and completion flags between consumers. Senders
//! have the same API as the general channel. Memory retention and close-race
//! semantics are the same as the general channel.
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let (tx, mut rx) = rapidfire::mpsc::bounded(256);
//! tx.send(7).await.unwrap();
//! tx.send(8).await.unwrap();
//! let mut batch = Vec::with_capacity(32);
//! assert_eq!(rx.recv_many(&mut batch, 32).await.unwrap(), 2);
//! assert_eq!(batch, [7, 8]);
//! # });
//! ```
//!
//! Receivers cannot be cloned or polled concurrently:
//!
//! ```compile_fail
//! let (_, rx) = rapidfire::mpsc::unbounded::<u64>();
//! let second = rx.clone();
//! ```
//!
//! ```compile_fail
//! let (_, mut rx) = rapidfire::mpsc::unbounded::<u64>();
//! let first = rx.recv();
//! let second = rx.recv();
//! drop((first, second));
//! ```

use crate::queue::Backoff;
pub use crate::Sender;
use crate::{RecvError, RecvState, TryRecvError};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Creates a channel with unlimited capacity and one receiver.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = crate::unbounded();
    (tx, Receiver { inner: rx })
}

/// Creates a channel with the given capacity and one receiver.
///
/// Panics if `capacity` is zero.
pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = crate::bounded(capacity);
    (tx, Receiver { inner: rx })
}

/// The exclusive receiving half of an MPSC channel.
///
/// This type is not cloneable. Every receive needs `&mut self`, including the
/// entire lifetime of an outstanding receive future. The inner MPMC receiver is
/// private and is used only for lifecycle and non-receiving inspection methods.
pub struct Receiver<T> {
    inner: crate::Receiver<T>,
}

impl<T> Receiver<T> {
    #[inline(always)]
    fn try_recv_nonblocking(&mut self) -> Result<T, RecvState> {
        let inner = &self.inner.inner;
        // SAFETY: only constructors above create this private wrapper. No API
        // exposes/clones its inner receiver, and all receiving borrows are exclusive.
        if let Some((value, wake)) = unsafe { inner.queue.pop_single(&inner.flags.waiting_senders) }
        {
            if wake {
                inner.send_waiters.notify_one(&inner.flags.waiting_senders);
            }
            Ok(value)
        } else {
            Err(inner.recv_state_when_empty())
        }
    }

    /// Attempts to receive a message without awaiting.
    #[inline(always)]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut backoff = Backoff::new();
        loop {
            match self.try_recv_nonblocking() {
                Ok(value) => return Ok(value),
                Err(RecvState::Empty) => return Err(TryRecvError::Empty),
                Err(RecvState::Closed) => return Err(TryRecvError::Closed),
                Err(RecvState::Busy) => backoff.snooze(),
            }
        }
    }

    /// Receives one message asynchronously. Cancelling the future consumes nothing.
    #[inline]
    pub fn recv(&mut self) -> Recv<'_, T> {
        Recv {
            receiver: self,
            waiter_id: None,
        }
    }

    /// Appends up to `limit` ready messages to `buffer`, awaiting only the first.
    ///
    /// Returns `Ok(0)` immediately for a zero limit. Otherwise returns an error
    /// only if the channel is closed and drained before taking any messages.
    /// Never waits for a full batch. Existing buffer contents are preserved.
    /// Cancelling this future while pending consumes no messages.
    pub async fn recv_many(
        &mut self,
        buffer: &mut Vec<T>,
        limit: usize,
    ) -> Result<usize, RecvError> {
        if limit == 0 {
            return Ok(0);
        }
        let mut count = self.try_recv_many_ready(buffer, limit);
        if count == 0 {
            buffer.push(self.recv().await?);
            count = 1;
        }
        while count < limit {
            let received = self.try_recv_many_ready(buffer, limit - count);
            if received == 0 {
                break;
            }
            count += received;
        }
        Ok(count)
    }

    fn try_recv_many_ready(&mut self, buffer: &mut Vec<T>, limit: usize) -> usize {
        let inner = &self.inner.inner;
        // SAFETY: the same exclusive receiver as try_recv_nonblocking. Notify
        // immediately, before the next call could allocate or unwind.
        let (count, wake) = unsafe {
            inner
                .queue
                .pop_single_batch(buffer, limit, &inner.flags.waiting_senders)
        };
        if wake != 0 {
            inner
                .send_waiters
                .notify_many(&inner.flags.waiting_senders, wake);
        }
        count
    }

    /// Closes the channel, retaining already queued messages for draining.
    pub fn close(&self) -> bool {
        self.inner.close()
    }
    /// Reports whether the channel is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
    /// Returns the number of queued messages (including pending publications).
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    /// Reports whether the channel currently contains no messages.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    /// Returns the configured capacity, or `None` for an unbounded channel.
    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }
    /// Returns the number of sender handles.
    pub fn sender_count(&self) -> usize {
        self.inner.sender_count()
    }
}

/// A pending exclusive receive, created by [`Receiver::recv`].
pub struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
    waiter_id: Option<u64>,
}

impl<T> Unpin for Recv<'_, T> {}

impl<T> Recv<'_, T> {
    fn unregister(&mut self, forward: bool) {
        let inner = &self.receiver.inner.inner;
        if self.waiter_id.is_some() {
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
        match this.receiver.try_recv_nonblocking() {
            Ok(value) => {
                this.unregister(false);
                return Poll::Ready(Ok(value));
            }
            Err(RecvState::Closed) => {
                this.unregister(false);
                return Poll::Ready(Err(RecvError));
            }
            Err(_) => {}
        }
        let inner = &this.receiver.inner.inner;
        inner.recv_waiters.register(
            &inner.flags.waiting_receivers,
            &inner.flags.next_waiter_id,
            &mut this.waiter_id,
            cx.waker(),
        );
        let tail = inner.queue.receiver_parking();
        let mut wait = Backoff::new();
        loop {
            let busy = match this.receiver.try_recv_nonblocking() {
                Ok(value) => {
                    this.unregister(false);
                    return Poll::Ready(Ok(value));
                }
                Err(RecvState::Closed) => {
                    this.unregister(false);
                    return Poll::Ready(Err(RecvError));
                }
                Err(RecvState::Busy) => true,
                Err(RecvState::Empty) => this.receiver.inner.inner.queue.claims_below(tail),
            };
            if !busy {
                return Poll::Pending;
            }
            if !wait.snooze_bounded() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn single_receiver_recycles_and_drops_values() {
        for cap in [1, 3, 25, 4096, usize::MAX] {
            let (tx, mut rx) = if cap == usize::MAX {
                unbounded()
            } else {
                bounded(cap)
            };
            for i in 0..1024 {
                tx.try_send(Box::new(i)).unwrap();
                assert_eq!(*rx.try_recv().unwrap(), i);
            }
            tx.try_send(Box::new(1024)).unwrap();
            drop(tx);
            assert_eq!(*rx.try_recv().unwrap(), 1024);
            assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
        }
    }

    #[test]
    fn batch_zero_partial_and_close() {
        futures::executor::block_on(async {
            let (tx, mut rx) = bounded(3);
            let mut got = vec![99];
            assert_eq!(rx.recv_many(&mut got, 0).await, Ok(0));
            tx.try_send(1).unwrap();
            tx.try_send(2).unwrap();
            assert_eq!(rx.recv_many(&mut got, 10).await, Ok(2));
            assert_eq!(got, [99, 1, 2]);
            drop(tx);
            assert_eq!(rx.recv_many(&mut got, 10).await, Err(RecvError));
        });
    }

    #[test]
    fn batch_recycling_and_channel_drop_destroy_each_value_once() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        struct Item(Arc<AtomicUsize>);
        impl Drop for Item {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = unbounded();
        for _ in 0..133 {
            tx.try_send(Item(drops.clone())).unwrap();
        }
        let mut batch = Vec::with_capacity(32);
        for _ in 0..4 {
            assert_eq!(
                futures::executor::block_on(rx.recv_many(&mut batch, 32)),
                Ok(32)
            );
            batch.clear();
        }
        assert_eq!(drops.load(Ordering::Relaxed), 128);
        drop(rx);
        drop(tx);
        assert_eq!(drops.load(Ordering::Relaxed), 133);
    }
}
