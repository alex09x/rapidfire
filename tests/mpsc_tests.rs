#![cfg(not(feature = "loom"))]
use rapidfire::{mpsc, RecvError, TryRecvError};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

#[test]
fn mpsc_threads_fifo_exactly_once() {
    for cap in [1, 3, 25, 4096, 0] {
        for producers in [2, 4, 8] {
            let (tx, mut rx) = if cap == 0 {
                mpsc::unbounded()
            } else {
                mpsc::bounded(cap)
            };
            std::thread::scope(|scope| {
                for p in 0..producers {
                    let tx = tx.clone();
                    scope.spawn(move || {
                        futures::executor::block_on(async move {
                            for i in 0..1003 {
                                tx.send((p, i)).await.unwrap();
                            }
                        })
                    });
                }
                drop(tx);
                let mut next = vec![0; producers];
                let mut batch = Vec::new();
                futures::executor::block_on(async {
                    while rx.recv_many(&mut batch, 32).await.is_ok() {
                        for (p, i) in batch.drain(..) {
                            assert_eq!(next[p], i);
                            next[p] += 1;
                        }
                    }
                });
                assert_eq!(next, vec![1003; producers]);
            });
        }
    }
}

struct Counter(AtomicUsize);
impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn mpsc_cancelled_recv_and_batch_leave_messages() {
    for cap in [0, 1, 3] {
        let (tx, mut rx) = if cap == 0 {
            mpsc::unbounded()
        } else {
            mpsc::bounded(cap)
        };
        let counter = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);
        let mut future = Box::pin(rx.recv());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        tx.try_send(7).unwrap();
        assert!(counter.0.load(Ordering::Relaxed) > 0);
        drop(future);
        assert_eq!(rx.try_recv(), Ok(7));
        let mut out = vec![99];
        let mut future = Box::pin(rx.recv_many(&mut out, 32));
        assert!(future.as_mut().poll(&mut cx).is_pending());
        tx.try_send(8).unwrap();
        drop(future);
        assert_eq!(out, [99]);
        assert_eq!(rx.try_recv(), Ok(8));
        let mut future = Box::pin(rx.recv());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        drop(tx);
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(Err(RecvError)));
    }
}

#[test]
fn mpsc_batch_wakes_all_senders_for_freed_capacity() {
    let (tx, mut rx) = mpsc::bounded(3);
    for i in 0..3 {
        tx.try_send(i).unwrap();
    }
    let counter = Arc::new(Counter(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&waker);
    let mut pending: Vec<_> = (3..6).map(|i| Box::pin(tx.send(i))).collect();
    for p in &mut pending {
        assert!(p.as_mut().poll(&mut cx).is_pending());
    }
    let mut out = Vec::new();
    assert_eq!(
        futures::executor::block_on(rx.recv_many(&mut out, 3)),
        Ok(3)
    );
    assert_eq!(counter.0.load(Ordering::Relaxed), 3);
    for p in &mut pending {
        assert_eq!(p.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    }
    drop(pending);
    drop(tx);
    assert_eq!(
        futures::executor::block_on(rx.recv_many(&mut out, 100)),
        Ok(3)
    );
    assert_eq!(out, [0, 1, 2, 3, 4, 5]);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}

#[test]
fn mpsc_receiver_drop_closes_pending_sends() {
    let (tx, mut rx) = mpsc::bounded(1);
    tx.try_send(1).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    tx.try_send(2).unwrap();
    let counter = Arc::new(Counter(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&waker);
    let mut pending = Box::pin(tx.send(3));
    assert!(pending.as_mut().poll(&mut cx).is_pending());
    drop(rx);
    assert!(counter.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        pending.as_mut().poll(&mut cx),
        Poll::Ready(Err(rapidfire::SendError(3)))
    );
    assert_eq!(tx.receiver_count(), 0);
}
