#![cfg(not(feature = "loom"))]

use rapidfire::mpsc;
use rapidfire::{RecvError, SendError, TryRecvError};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

/// Payload type for tracking exact drop counts across allocation, batching, and recycling.
#[derive(Debug)]
struct TrackedDrop {
    id: usize,
    counter: Arc<AtomicUsize>,
}

impl TrackedDrop {
    fn new(id: usize, counter: &Arc<AtomicUsize>) -> Self {
        Self {
            id,
            counter: Arc::clone(counter),
        }
    }
}

impl Drop for TrackedDrop {
    fn drop(&mut self) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

/// A waker implementation that counts wake notifications.
struct WakeCounter(AtomicUsize);

impl WakeCounter {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicUsize::new(0)))
    }

    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Verifies non-Copy payload drop correctness across multiple block boundaries,
/// partial batches, and block recycling.
#[test]
fn test_non_copy_drop_counts_across_batching_and_recycling() {
    for cap in [0, 20, 63, 64, 128] {
        let (tx, mut rx) = if cap == 0 {
            mpsc::unbounded()
        } else {
            mpsc::bounded(cap)
        };
        let drop_counter = Arc::new(AtomicUsize::new(0));
        let total = 300;

        let counter_producer = Arc::clone(&drop_counter);
        let producer = std::thread::spawn(move || {
            futures::executor::block_on(async {
                for i in 0..total {
                    let item = TrackedDrop::new(i, &counter_producer);
                    tx.send(item).await.unwrap();
                }
            });
        });

        let mut received = Vec::new();
        let batch_limits = [1, 7, 20, 63, 64, 100, 15];
        let mut limit_idx = 0;
        let mut next_expected_id = 0;

        futures::executor::block_on(async {
            while next_expected_id < total {
                let limit = batch_limits[limit_idx % batch_limits.len()];
                limit_idx += 1;
                let mut batch = Vec::new();
                let n = rx.recv_many(&mut batch, limit).await.unwrap();
                assert_eq!(n, batch.len());

                let drops_before = drop_counter.load(Ordering::SeqCst);
                for item in batch {
                    assert_eq!(item.id, next_expected_id);
                    next_expected_id += 1;
                    received.push(item);
                }
                assert_eq!(drop_counter.load(Ordering::SeqCst), drops_before);

                // Partially drain to test incremental drop behavior
                if received.len() > 60 {
                    let drops_before_drain = drop_counter.load(Ordering::SeqCst);
                    let drained_count = received.drain(..30).count();
                    assert_eq!(
                        drop_counter.load(Ordering::SeqCst),
                        drops_before_drain + drained_count
                    );
                }
            }
        });

        producer.join().unwrap();
        drop(received);
        assert_eq!(drop_counter.load(Ordering::SeqCst), total);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Closed);
    }
}

/// Verifies that dropping the receiver with unread items in multi-block chains
/// cleans up and drops every queued value without leaking.
#[test]
fn test_non_copy_drop_counts_on_receiver_drop_with_unread_items() {
    for cap in [0, 200, 256] {
        let (tx, mut rx) = if cap == 0 {
            mpsc::unbounded()
        } else {
            mpsc::bounded(cap)
        };
        let drop_counter = Arc::new(AtomicUsize::new(0));
        let total = 200;

        for i in 0..total {
            tx.try_send(TrackedDrop::new(i, &drop_counter)).unwrap();
        }

        let mut buf = Vec::new();
        futures::executor::block_on(async {
            rx.recv_many(&mut buf, 25).await.unwrap();
            rx.recv_many(&mut buf, 20).await.unwrap();
        });
        assert_eq!(buf.len(), 45);
        assert_eq!(drop_counter.load(Ordering::SeqCst), 0);

        drop(buf);
        assert_eq!(drop_counter.load(Ordering::SeqCst), 45);

        // Dropping rx and tx must drop all remaining 155 unread items
        drop(rx);
        drop(tx);
        assert_eq!(drop_counter.load(Ordering::SeqCst), total);
    }
}

/// Verifies that cancelling selected or pending bounded send futures forwards
/// wakeups to remaining parked senders and preserves capacity and drop counts.
#[test]
fn test_cancelling_woken_send_future_forwards_wakeup() {
    let (tx, mut rx) = mpsc::bounded(2);
    let drop_counter = Arc::new(AtomicUsize::new(0));

    // Fill capacity
    tx.try_send(TrackedDrop::new(0, &drop_counter)).unwrap();
    tx.try_send(TrackedDrop::new(1, &drop_counter)).unwrap();

    let mut s2 = Box::pin(tx.send(TrackedDrop::new(2, &drop_counter)));
    let mut s3 = Box::pin(tx.send(TrackedDrop::new(3, &drop_counter)));
    let mut s4 = Box::pin(tx.send(TrackedDrop::new(4, &drop_counter)));
    let mut s5 = Box::pin(tx.send(TrackedDrop::new(5, &drop_counter)));

    let w2 = WakeCounter::new();
    let w3 = WakeCounter::new();
    let w4 = WakeCounter::new();
    let w5 = WakeCounter::new();

    let waker2 = Waker::from(w2.clone());
    let waker3 = Waker::from(w3.clone());
    let waker4 = Waker::from(w4.clone());
    let waker5 = Waker::from(w5.clone());

    let mut cx2 = Context::from_waker(&waker2);
    let mut cx3 = Context::from_waker(&waker3);
    let mut cx4 = Context::from_waker(&waker4);
    let mut cx5 = Context::from_waker(&waker5);

    // Register all four send futures in send_waiters
    assert!(s2.as_mut().poll(&mut cx2).is_pending());
    assert!(s3.as_mut().poll(&mut cx3).is_pending());
    assert!(s4.as_mut().poll(&mut cx4).is_pending());
    assert!(s5.as_mut().poll(&mut cx5).is_pending());

    assert_eq!(w2.count(), 0);
    assert_eq!(w3.count(), 0);
    assert_eq!(w4.count(), 0);
    assert_eq!(w5.count(), 0);

    // Free 2 slots via batch receive
    let mut buf = Vec::new();
    let n = futures::executor::block_on(rx.recv_many(&mut buf, 2)).unwrap();
    assert_eq!(n, 2);
    assert_eq!(buf[0].id, 0);
    assert_eq!(buf[1].id, 1);

    // Two senders were woken: s2 and s3
    assert_eq!(w2.count(), 1);
    assert_eq!(w3.count(), 1);
    assert_eq!(w4.count(), 0);
    assert_eq!(w5.count(), 0);

    // Cancel s2 (dropped without completing). It absorbed a wakeup, so it must forward it to s4!
    drop(s2);
    assert_eq!(w4.count(), 1);
    assert_eq!(w5.count(), 0);

    // Cancel s5 (unwoken at tail). It unregisters without forwarding.
    drop(s5);
    assert_eq!(w5.count(), 0);

    // s3 and s4 complete into the freed slots
    assert!(matches!(s3.as_mut().poll(&mut cx3), Poll::Ready(Ok(()))));
    assert!(matches!(s4.as_mut().poll(&mut cx4), Poll::Ready(Ok(()))));

    let n2 = futures::executor::block_on(rx.recv_many(&mut buf, 2)).unwrap();
    assert_eq!(n2, 2);
    assert_eq!(buf[2].id, 3);
    assert_eq!(buf[3].id, 4);

    drop(buf);
    drop(s3);
    drop(s4);
    drop(rx);
    drop(tx);
    // All 6 items created (0, 1, 2, 3, 4, 5) are exactly dropped
    assert_eq!(drop_counter.load(Ordering::SeqCst), 6);
}

/// Verifies pending recv_many behavior across channel close, sender drop, and receiver drop.
#[test]
fn test_pending_recv_many_followed_by_close_and_drop() {
    // Empty channel, pending recv_many, all senders dropped
    {
        let (tx, mut rx) = mpsc::bounded::<usize>(3);
        let w = WakeCounter::new();
        let waker = Waker::from(w.clone());
        let mut cx = Context::from_waker(&waker);
        let mut buf = vec![999];
        let mut fut = Box::pin(rx.recv_many(&mut buf, 5));

        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(w.count(), 0);

        drop(tx);
        assert!(w.count() > 0);
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(Err(RecvError)));
        drop(fut);
        assert_eq!(buf, vec![999]);
    }

    // Empty channel, pending recv_many, messages sent then tx.close()
    {
        let (tx, mut rx) = mpsc::bounded::<usize>(3);
        let w = WakeCounter::new();
        let waker = Waker::from(w.clone());
        let mut cx = Context::from_waker(&waker);
        let mut buf = Vec::new();
        let mut fut = Box::pin(rx.recv_many(&mut buf, 5));

        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);

        tx.try_send(10).unwrap();
        tx.try_send(20).unwrap();
        tx.close();

        assert!(w.count() > 0);
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(Ok(2)));
        drop(fut);
        assert_eq!(buf, vec![10, 20]);

        // Next call reports closed channel
        let mut next_buf = Vec::new();
        let mut fut2 = Box::pin(rx.recv_many(&mut next_buf, 5));
        assert_eq!(fut2.as_mut().poll(&mut cx), Poll::Ready(Err(RecvError)));
        drop(fut2);
        assert!(next_buf.is_empty());
    }

    // Full bounded channel, pending send, receiver dropped
    {
        let (tx, rx) = mpsc::bounded::<usize>(1);
        tx.try_send(1).unwrap();
        let w = WakeCounter::new();
        let waker = Waker::from(w.clone());
        let mut cx = Context::from_waker(&waker);
        let mut send_fut = Box::pin(tx.send(2));
        assert_eq!(send_fut.as_mut().poll(&mut cx), Poll::Pending);

        drop(rx);
        assert!(w.count() > 0);
        assert_eq!(
            send_fut.as_mut().poll(&mut cx),
            Poll::Ready(Err(SendError(2)))
        );
        assert_eq!(tx.receiver_count(), 0);
        assert!(tx.is_closed());
    }

    // recv_many with limit 0 returns Ok(0) immediately
    {
        let (tx, mut rx) = mpsc::unbounded::<usize>();
        let mut buf = vec![1, 2, 3];
        assert_eq!(
            futures::executor::block_on(rx.recv_many(&mut buf, 0)),
            Ok(0)
        );
        assert_eq!(buf, vec![1, 2, 3]);
        drop(tx);
    }
}

/// Verifies that repeated waker updates and cancellations correctly replace wakers
/// without leaking waiters or stale wake counts.
#[test]
fn test_repeated_waker_replacement_and_cancellation() {
    // Receiver waker replacement
    {
        let (tx, mut rx) = mpsc::unbounded::<usize>();
        let mut fut = Box::pin(rx.recv());

        let w1 = WakeCounter::new();
        let w2 = WakeCounter::new();
        let w3 = WakeCounter::new();

        let waker1 = Waker::from(w1.clone());
        let waker2 = Waker::from(w2.clone());
        let waker3 = Waker::from(w3.clone());

        let mut cx1 = Context::from_waker(&waker1);
        let mut cx2 = Context::from_waker(&waker2);
        let mut cx3 = Context::from_waker(&waker3);

        assert_eq!(fut.as_mut().poll(&mut cx1), Poll::Pending);
        assert_eq!(fut.as_mut().poll(&mut cx2), Poll::Pending);
        assert_eq!(fut.as_mut().poll(&mut cx3), Poll::Pending);

        tx.try_send(777).unwrap();

        // Only the newest waker was notified
        assert_eq!(w1.count(), 0);
        assert_eq!(w2.count(), 0);
        assert_eq!(w3.count(), 1);

        assert_eq!(fut.as_mut().poll(&mut cx3), Poll::Ready(Ok(777)));
    }

    // Sender waker replacement
    {
        let (tx, mut rx) = mpsc::bounded::<usize>(1);
        tx.try_send(1).unwrap();

        let mut send_fut = Box::pin(tx.send(2));
        let w1 = WakeCounter::new();
        let w2 = WakeCounter::new();
        let w3 = WakeCounter::new();

        let waker1 = Waker::from(w1.clone());
        let waker2 = Waker::from(w2.clone());
        let waker3 = Waker::from(w3.clone());

        let mut cx1 = Context::from_waker(&waker1);
        let mut cx2 = Context::from_waker(&waker2);
        let mut cx3 = Context::from_waker(&waker3);

        assert_eq!(send_fut.as_mut().poll(&mut cx1), Poll::Pending);
        assert_eq!(send_fut.as_mut().poll(&mut cx2), Poll::Pending);
        assert_eq!(send_fut.as_mut().poll(&mut cx3), Poll::Pending);

        assert_eq!(rx.try_recv(), Ok(1));

        assert_eq!(w1.count(), 0);
        assert_eq!(w2.count(), 0);
        assert_eq!(w3.count(), 1);

        assert_eq!(send_fut.as_mut().poll(&mut cx3), Poll::Ready(Ok(())));
        assert_eq!(rx.try_recv(), Ok(2));
    }

    // Repeated poll and cancel cycle
    {
        let (tx, mut rx) = mpsc::bounded::<usize>(1);
        for _ in 0..200 {
            let w = WakeCounter::new();
            let waker = Waker::from(w);
            let mut cx = Context::from_waker(&waker);
            let mut fut = Box::pin(rx.recv());
            assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);
            drop(fut);
        }
        tx.try_send(42).unwrap();
        assert_eq!(rx.try_recv(), Ok(42));
    }
}

/// Verifies that exclusive Receiver ownership can be transferred across threads
/// while concurrent producers send, preserving FIFO order and preventing data races.
#[test]
fn test_exclusive_receiver_transfer_between_threads() {
    let (tx, rx) = mpsc::bounded::<(usize, usize)>(16);
    let num_producers = 4;
    let items_per_producer = 250;
    let total_items = num_producers * items_per_producer;

    let (pass1_tx, pass1_rx) = std::sync::mpsc::channel();
    let (pass2_tx, pass2_rx) = std::sync::mpsc::channel();

    let mut producer_handles = Vec::new();
    for p in 0..num_producers {
        let p_tx = tx.clone();
        producer_handles.push(std::thread::spawn(move || {
            futures::executor::block_on(async move {
                for i in 0..items_per_producer {
                    p_tx.send((p, i)).await.unwrap();
                }
            });
        }));
    }
    drop(tx);

    let c1 = std::thread::spawn(move || {
        let mut rx = rx;
        let mut received = Vec::new();
        futures::executor::block_on(async {
            while received.len() < 200 {
                let mut batch = Vec::new();
                let _ = rx.recv_many(&mut batch, 20).await.unwrap();
                received.extend(batch);
            }
        });
        pass1_tx.send(rx).unwrap();
        received
    });

    let c2 = std::thread::spawn(move || {
        let mut rx = pass1_rx.recv().unwrap();
        let mut received = Vec::new();
        futures::executor::block_on(async {
            while received.len() < 300 {
                if received.len() % 2 == 0 {
                    let mut batch = Vec::new();
                    let _ = rx.recv_many(&mut batch, 15).await.unwrap();
                    received.extend(batch);
                } else {
                    received.push(rx.recv().await.unwrap());
                }
            }
        });
        pass2_tx.send(rx).unwrap();
        received
    });

    let c3 = std::thread::spawn(move || {
        let mut rx = pass2_rx.recv().unwrap();
        let mut received = Vec::new();
        futures::executor::block_on(async {
            let mut batch = Vec::new();
            while rx.recv_many(&mut batch, 32).await.is_ok() {
                received.append(&mut batch);
            }
        });
        received
    });

    for h in producer_handles {
        h.join().unwrap();
    }

    let mut all_received = c1.join().unwrap();
    all_received.extend(c2.join().unwrap());
    all_received.extend(c3.join().unwrap());

    assert_eq!(all_received.len(), total_items);

    let mut next_expected = vec![0; num_producers];
    for (p, seq) in all_received {
        assert_eq!(seq, next_expected[p], "FIFO violation on producer {}", p);
        next_expected[p] += 1;
    }
    assert_eq!(next_expected, vec![items_per_producer; num_producers]);
}

/// Stress test combining concurrent selective send cancellations with batch receives.
#[test]
fn test_adversarial_concurrent_send_cancellation_and_batch_drain() {
    let (tx, mut rx) = mpsc::bounded::<TrackedDrop>(3);
    let drop_counter = Arc::new(AtomicUsize::new(0));
    let num_senders = 4;
    let target_sends_per_sender = 50;
    let created = Arc::new(AtomicUsize::new(0));

    let mut sender_handles = Vec::new();
    for s in 0..num_senders {
        let s_tx = tx.clone();
        let counter = Arc::clone(&drop_counter);
        let created = created.clone();
        sender_handles.push(std::thread::spawn(move || {
            let mut completed = 0;
            let mut seq = 0;
            while completed < target_sends_per_sender {
                created.fetch_add(1, Ordering::SeqCst);
                let item = TrackedDrop::new(s * 1000 + seq, &counter);
                seq += 1;
                let send_res = futures::executor::block_on(async {
                    let send_fut = s_tx.send(item);
                    futures::pin_mut!(send_fut);
                    if seq % 3 == 0 {
                        let mut polled = false;
                        let cancel_fut = futures::future::poll_fn(|cx| {
                            if polled {
                                Poll::Ready(())
                            } else {
                                polled = true;
                                cx.waker().wake_by_ref();
                                Poll::Pending
                            }
                        });
                        futures::pin_mut!(cancel_fut);
                        match futures::future::select(send_fut, cancel_fut).await {
                            futures::future::Either::Left((res, _)) => {
                                res.unwrap();
                                true
                            }
                            futures::future::Either::Right(((), send_fut)) => {
                                drop(send_fut);
                                false
                            }
                        }
                    } else {
                        send_fut.await.unwrap();
                        true
                    }
                });
                if send_res {
                    completed += 1;
                }
            }
        }));
    }
    drop(tx);

    let total_expected = num_senders * target_sends_per_sender;
    let mut received = Vec::new();

    futures::executor::block_on(async {
        let mut batch = Vec::new();
        while received.len() < total_expected {
            let limit = (received.len() % 5) + 1;
            let _ = rx.recv_many(&mut batch, limit).await.unwrap();
            received.append(&mut batch);
        }
    });

    for h in sender_handles {
        h.join().unwrap();
    }

    assert_eq!(received.len(), total_expected);
    drop(received);
    drop(rx);
    assert_eq!(
        drop_counter.load(Ordering::SeqCst),
        created.load(Ordering::SeqCst)
    );
}
