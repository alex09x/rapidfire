use rapidfire::{bounded, unbounded, RecvError, SendError, TryRecvError, TrySendError};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_unbounded_basic_send_recv() {
    let (tx, rx) = unbounded::<i32>();
    assert!(rx.is_empty());
    assert_eq!(rx.len(), 0);

    tx.send(42).await.unwrap();
    assert!(!rx.is_empty());
    assert_eq!(rx.len(), 1);

    let val = rx.recv().await.unwrap();
    assert_eq!(val, 42);
    assert!(rx.is_empty());
    assert_eq!(rx.len(), 0);
}

#[test]
fn test_try_send_try_recv() {
    let (tx, rx) = unbounded::<String>();

    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    assert!(TryRecvError::Empty.is_empty());
    assert!(!TryRecvError::Empty.is_closed());

    tx.try_send("hello".to_string()).unwrap();
    assert_eq!(rx.len(), 1);

    let val = rx.try_recv().unwrap();
    assert_eq!(val, "hello");
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[tokio::test]
async fn test_close_and_drain() {
    let (tx, rx) = unbounded::<usize>();
    for i in 0..5 {
        tx.send(i).await.unwrap();
    }

    assert!(tx.close());
    assert!(!tx.close()); // Second close returns false
    assert!(tx.is_closed());
    assert!(rx.is_closed());

    // Messages can still be drained after close
    for i in 0..5 {
        assert_eq!(rx.recv().await.unwrap(), i);
    }

    // Now that it's empty and closed, recv returns RecvError
    assert_eq!(rx.recv().await, Err(RecvError));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    assert!(TryRecvError::Closed.is_closed());
    assert!(!TryRecvError::Closed.is_empty());

    // Sending on closed returns error
    assert_eq!(tx.send(100).await, Err(SendError(100)));
    assert_eq!(tx.try_send(100), Err(TrySendError::Closed(100)));
}

#[tokio::test]
async fn test_sender_drop_closes_channel() {
    let (tx, rx) = unbounded::<i32>();
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    drop(tx);

    assert!(rx.is_closed());
    assert_eq!(rx.recv().await.unwrap(), 1);
    assert_eq!(rx.recv().await.unwrap(), 2);
    assert_eq!(rx.recv().await, Err(RecvError));
}

#[tokio::test]
async fn test_receiver_drop_closes_channel() {
    let (tx, rx) = unbounded::<i32>();
    drop(rx);

    assert!(tx.is_closed());
    assert_eq!(tx.send(1).await, Err(SendError(1)));
    assert_eq!(tx.try_send(2), Err(TrySendError::Closed(2)));
}

#[test]
fn test_counts_and_clones() {
    let (tx1, rx1) = unbounded::<i32>();
    assert_eq!(tx1.sender_count(), 1);
    assert_eq!(tx1.receiver_count(), 1);
    assert_eq!(rx1.sender_count(), 1);
    assert_eq!(rx1.receiver_count(), 1);
    assert_eq!(tx1.capacity(), None);
    assert_eq!(rx1.capacity(), None);

    let tx2 = tx1.clone();
    assert_eq!(tx1.sender_count(), 2);

    let rx2 = rx1.clone();
    assert_eq!(rx1.receiver_count(), 2);

    drop(tx1);
    assert_eq!(tx2.sender_count(), 1);
    assert!(!tx2.is_closed());

    drop(rx1);
    assert_eq!(rx2.receiver_count(), 1);
    assert!(!rx2.is_closed());
}

#[tokio::test]
async fn test_bounded_channel() {
    let (tx, rx) = bounded::<i32>(2);
    assert_eq!(tx.capacity(), Some(2));
    assert_eq!(rx.capacity(), Some(2));

    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();

    // 3rd should be full
    let err = tx.try_send(3).unwrap_err();
    assert!(err.is_full());
    assert!(!err.is_closed());
    assert_eq!(err.into_inner(), 3);

    // Spawn task waiting on send
    let tx_clone = tx.clone();
    let send_handle = tokio::spawn(async move {
        tx_clone.send(3).await.unwrap();
    });

    // Give sender task time to reach pending
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Popping one item should unblock the sender
    assert_eq!(rx.recv().await.unwrap(), 1);
    send_handle.await.unwrap();

    assert_eq!(rx.recv().await.unwrap(), 2);
    assert_eq!(rx.recv().await.unwrap(), 3);
}

#[tokio::test]
async fn test_bounded_channel_close_while_sending() {
    let (tx, rx) = bounded::<i32>(1);
    tx.send(1).await.unwrap();

    let tx_clone = tx.clone();
    let send_handle = tokio::spawn(async move { tx_clone.send(2).await });

    tokio::time::sleep(Duration::from_millis(20)).await;
    rx.close();

    let res = send_handle.await.unwrap();
    assert_eq!(res, Err(SendError(2)));
}

#[test]
#[should_panic(expected = "capacity must be greater than zero")]
fn test_bounded_zero_capacity_panics() {
    let _ = bounded::<i32>(0);
}

#[tokio::test]
async fn test_select_cancellation_safety() {
    let (tx, rx) = unbounded::<i32>();

    // Test select timeout (future dropped while waiting)
    tokio::select! {
        _ = rx.recv() => {
            panic!("should not receive anything");
        }
        _ = tokio::time::sleep(Duration::from_millis(20)) => {}
    }

    // Now send and receive normally
    tx.send(999).await.unwrap();
    assert_eq!(rx.recv().await.unwrap(), 999);
}

#[test]
fn test_debug_and_display() {
    let (tx, rx) = unbounded::<i32>();
    let debug_tx = format!("{:?}", tx);
    assert!(debug_tx.contains("Sender"));
    assert!(debug_tx.contains("senders: 1"));

    let debug_rx = format!("{:?}", rx);
    assert!(debug_rx.contains("Receiver"));
    assert!(debug_rx.contains("receivers: 1"));

    let send_err = SendError(123);
    assert_eq!(send_err.into_inner(), 123);
    assert_eq!(format!("{}", SendError(123)), "sending on a closed channel");
    assert_eq!(format!("{:?}", SendError(123)), "SendError");

    let recv_err = RecvError;
    assert_eq!(
        format!("{}", recv_err),
        "receiving on an empty and closed channel"
    );

    let try_send_full = TrySendError::Full(456);
    assert_eq!(try_send_full.into_inner(), 456);
    assert_eq!(
        format!("{}", TrySendError::Full(456)),
        "sending on a full channel"
    );
    assert_eq!(
        format!("{:?}", TrySendError::Full(456)),
        "TrySendError::Full"
    );

    let try_send_closed = TrySendError::Closed(789);
    assert_eq!(try_send_closed.into_inner(), 789);
    assert_eq!(
        format!("{}", TrySendError::Closed(789)),
        "sending on a closed channel"
    );
    assert_eq!(
        format!("{:?}", TrySendError::Closed(789)),
        "TrySendError::Closed"
    );

    let try_recv_empty = TryRecvError::Empty;
    assert_eq!(
        format!("{}", try_recv_empty),
        "receiving on an empty channel"
    );

    let try_recv_closed = TryRecvError::Closed;
    assert_eq!(
        format!("{}", try_recv_closed),
        "receiving on an empty and closed channel"
    );
}

#[tokio::test]
async fn test_mpsc_concurrent_producers() {
    let (tx, rx) = unbounded::<usize>();
    const PRODUCERS: usize = 8;
    const MSGS_PER_PRODUCER: usize = 1000;
    const TOTAL: usize = PRODUCERS * MSGS_PER_PRODUCER;

    for p in 0..PRODUCERS {
        let tx_c = tx.clone();
        tokio::spawn(async move {
            for i in 0..MSGS_PER_PRODUCER {
                tx_c.send(p * 10000 + i).await.unwrap();
            }
        });
    }
    drop(tx); // Drop original tx so rx knows when all are done

    let mut count = 0;
    while let Ok(_msg) = rx.recv().await {
        count += 1;
    }
    assert_eq!(count, TOTAL);
}

#[tokio::test]
async fn test_mpmc_concurrent_producers_and_consumers() {
    let (tx, rx) = unbounded::<usize>();
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    const MSGS_PER_PRODUCER: usize = 2500;
    const TOTAL: usize = PRODUCERS * MSGS_PER_PRODUCER;

    let received_sum = Arc::new(AtomicUsize::new(0));
    let received_count = Arc::new(AtomicUsize::new(0));

    let mut consumer_handles = Vec::new();
    for _ in 0..CONSUMERS {
        let rx_c = rx.clone();
        let sum_c = received_sum.clone();
        let count_c = received_count.clone();
        consumer_handles.push(tokio::spawn(async move {
            while let Ok(msg) = rx_c.recv().await {
                sum_c.fetch_add(msg, Ordering::Relaxed);
                count_c.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    drop(rx); // Drop original rx

    let mut expected_sum = 0;
    for p in 0..PRODUCERS {
        let tx_c = tx.clone();
        for i in 0..MSGS_PER_PRODUCER {
            expected_sum += p * 10000 + i;
        }
        tokio::spawn(async move {
            for i in 0..MSGS_PER_PRODUCER {
                tx_c.send(p * 10000 + i).await.unwrap();
            }
        });
    }
    drop(tx); // Drop original tx

    for h in consumer_handles {
        h.await.unwrap();
    }

    assert_eq!(received_count.load(Ordering::Relaxed), TOTAL);
    assert_eq!(received_sum.load(Ordering::Relaxed), expected_sum);
}

#[tokio::test]
async fn test_tx_is_empty() {
    let (tx, rx) = unbounded::<i32>();
    assert!(tx.is_empty());
    assert_eq!(tx.len(), 0);
    tx.send(10).await.unwrap();
    assert!(!tx.is_empty());
    assert_eq!(tx.len(), 1);
    let _ = rx.recv().await.unwrap();
    assert!(tx.is_empty());
}

#[tokio::test]
async fn test_close_wakes_waiting_receivers() {
    let (tx, rx) = unbounded::<i32>();
    let rx_handle = tokio::spawn(async move { rx.recv().await });

    tokio::time::sleep(Duration::from_millis(25)).await;
    tx.close();

    let res = rx_handle.await.unwrap();
    assert_eq!(res, Err(RecvError));
}

#[tokio::test]
async fn test_bounded_send_select_cancellation_safety() {
    let (tx, rx) = bounded::<i32>(1);
    tx.send(1).await.unwrap();

    // Try sending into full channel with a timeout
    let tx_clone = tx.clone();
    tokio::select! {
        _ = tx_clone.send(2) => {
            panic!("should not send successfully");
        }
        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
    }

    // Space is cleared now
    assert_eq!(rx.recv().await.unwrap(), 1);
    tx.send(3).await.unwrap();
    assert_eq!(rx.recv().await.unwrap(), 3);
}

#[tokio::test]
async fn test_bounded_send_unblocks_when_space_opens() {
    let (tx, rx) = bounded::<i32>(1);
    tx.send(10).await.unwrap();

    let tx_clone = tx.clone();
    let send_h = tokio::spawn(async move { tx_clone.send(20).await });

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(rx.recv().await.unwrap(), 10);

    let res = send_h.await.unwrap();
    assert_eq!(res, Ok(()));
    assert_eq!(rx.recv().await.unwrap(), 20);
}

#[allow(dead_code)]
struct DummyWaker(usize);

#[allow(clippy::manual_noop_waker)]
impl std::task::Wake for DummyWaker {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn test_manual_poll_recv_re_registration_and_receive() {
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    let (tx, rx) = unbounded::<i32>();
    let waker1 = Waker::from(Arc::new(DummyWaker(1)));
    let mut cx1 = Context::from_waker(&waker1);

    let mut recv_fut = rx.recv();

    // 1st poll: registers waker and returns Pending
    assert_eq!(Pin::new(&mut recv_fut).poll(&mut cx1), Poll::Pending);

    // 2nd poll with different waker: updates waker
    let waker2 = Waker::from(Arc::new(DummyWaker(2)));
    let mut cx2 = Context::from_waker(&waker2);
    assert_eq!(Pin::new(&mut recv_fut).poll(&mut cx2), Poll::Pending);

    // Send item
    tx.try_send(42).unwrap();

    // 3rd poll: receives item
    assert_eq!(Pin::new(&mut recv_fut).poll(&mut cx2), Poll::Ready(Ok(42)));
}

#[test]
fn test_manual_poll_recv_closed_while_pending() {
    use futures::task::noop_waker;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let (tx, rx) = unbounded::<i32>();
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let mut recv_fut = rx.recv();
    assert_eq!(Pin::new(&mut recv_fut).poll(&mut cx), Poll::Pending);

    tx.close();
    assert_eq!(
        Pin::new(&mut recv_fut).poll(&mut cx),
        Poll::Ready(Err(RecvError))
    );
}

#[test]
fn test_manual_poll_send_re_registration_and_send() {
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    let (tx, rx) = bounded::<i32>(1);
    tx.try_send(10).unwrap();

    let waker1 = Waker::from(Arc::new(DummyWaker(1)));
    let mut cx1 = Context::from_waker(&waker1);

    let mut send_fut = tx.send(20);

    // 1st poll: registers waker and returns Pending
    assert_eq!(Pin::new(&mut send_fut).poll(&mut cx1), Poll::Pending);

    // 2nd poll with different waker: updates waker
    let waker2 = Waker::from(Arc::new(DummyWaker(2)));
    let mut cx2 = Context::from_waker(&waker2);
    assert_eq!(Pin::new(&mut send_fut).poll(&mut cx2), Poll::Pending);

    // Clear space
    assert_eq!(rx.try_recv().unwrap(), 10);

    // 3rd poll: completes send
    assert_eq!(Pin::new(&mut send_fut).poll(&mut cx2), Poll::Ready(Ok(())));
    assert_eq!(rx.try_recv().unwrap(), 20);
}

#[test]
fn test_manual_poll_send_closed_while_pending() {
    use futures::task::noop_waker;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let (tx, rx) = bounded::<i32>(1);
    tx.try_send(1).unwrap();

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let mut send_fut = tx.send(2);
    assert_eq!(Pin::new(&mut send_fut).poll(&mut cx), Poll::Pending);

    rx.close();
    assert_eq!(
        Pin::new(&mut send_fut).poll(&mut cx),
        Poll::Ready(Err(SendError(2)))
    );
}

/// A receiver that was woken by a send and then cancelled (e.g. a `select!` timeout
/// firing on the same tick) must pass the wake-up on to the next parked receiver.
#[tokio::test]
async fn cancelled_notified_receiver_forwards_wakeup() {
    use futures::task::noop_waker;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let (tx, rx) = unbounded::<i32>();
    let rx2 = rx.clone();

    // R1 parks first (front of the waiter list), polled by hand so it can be dropped
    // without ever being polled again.
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut r1 = rx.recv();
    assert_eq!(Pin::new(&mut r1).poll(&mut cx), Poll::Pending);

    // R2 parks behind it.
    let r2 = tokio::spawn(async move { rx2.recv().await });
    tokio::time::sleep(Duration::from_millis(30)).await;

    // The send wakes R1 (the front entry); R1 is then cancelled.
    tx.try_send(7).unwrap();
    drop(r1);

    let got = tokio::time::timeout(Duration::from_millis(500), r2)
        .await
        .expect("R2 must be woken by the forwarded notification")
        .unwrap();
    assert_eq!(got, Ok(7));
}

/// Bounded counterpart: a sender woken by a pop and then cancelled must pass the
/// wake-up on to the next parked sender.
#[tokio::test]
async fn cancelled_notified_sender_forwards_wakeup() {
    use futures::task::noop_waker;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let (tx, rx) = bounded::<i32>(1);
    tx.try_send(1).unwrap();
    let tx2 = tx.clone();

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut s1 = tx.send(2);
    assert_eq!(Pin::new(&mut s1).poll(&mut cx), Poll::Pending);

    let s2 = tokio::spawn(async move { tx2.send(3).await });
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert_eq!(rx.try_recv(), Ok(1)); // wakes S1 (front)
    drop(s1); // cancelled: must forward to S2

    tokio::time::timeout(Duration::from_millis(500), s2)
        .await
        .expect("S2 must be woken by the forwarded notification")
        .unwrap()
        .unwrap();
    assert_eq!(rx.try_recv(), Ok(3));
}

// ============================================================================
// Coverage & Stress Tests
// ============================================================================

// Target: Verify Sender and Receiver implement Send and Sync when T: Send.
#[test]
fn test_send_sync_static_assertions() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<rapidfire::Sender<usize>>();
    assert_sync::<rapidfire::Sender<usize>>();
    assert_send::<rapidfire::Receiver<usize>>();
    assert_sync::<rapidfire::Receiver<usize>>();
}

// Target: Exercise Debug, Display, std::error::Error, and methods for all error types.
#[test]
fn test_all_error_types_debug_display_and_traits() {
    use std::error::Error;

    // SendError
    let se1 = SendError("err_val");
    let se2 = se1;
    assert_eq!(se1, se2);
    assert_eq!(format!("{:?}", se1), "SendError");
    assert_eq!(format!("{}", se1), "sending on a closed channel");
    assert!(se1.source().is_none());
    assert_eq!(se1.into_inner(), "err_val");

    // RecvError
    let re1 = RecvError;
    let re2 = re1;
    assert_eq!(re1, re2);
    assert_eq!(format!("{:?}", re1), "RecvError");
    assert_eq!(
        format!("{}", re1),
        "receiving on an empty and closed channel"
    );
    assert!(re1.source().is_none());

    // TrySendError::Full
    let tse_full = TrySendError::Full(100);
    let tse_full2 = tse_full;
    assert_eq!(tse_full, tse_full2);
    assert!(tse_full.is_full());
    assert!(!tse_full.is_closed());
    assert_eq!(format!("{:?}", tse_full), "TrySendError::Full");
    assert_eq!(format!("{}", tse_full), "sending on a full channel");
    assert!(tse_full.source().is_none());
    assert_eq!(tse_full.into_inner(), 100);

    // TrySendError::Closed
    let tse_closed = TrySendError::Closed(200);
    let tse_closed2 = tse_closed;
    assert_eq!(tse_closed, tse_closed2);
    assert!(!tse_closed.is_full());
    assert!(tse_closed.is_closed());
    assert_eq!(format!("{:?}", tse_closed), "TrySendError::Closed");
    assert_eq!(format!("{}", tse_closed), "sending on a closed channel");
    assert!(tse_closed.source().is_none());
    assert_eq!(tse_closed.into_inner(), 200);

    assert_ne!(TrySendError::Full(10), TrySendError::Closed(10));

    // TryRecvError::Empty
    let tre_empty = TryRecvError::Empty;
    let tre_empty2 = tre_empty;
    assert_eq!(tre_empty, tre_empty2);
    assert!(tre_empty.is_empty());
    assert!(!tre_empty.is_closed());
    assert_eq!(format!("{:?}", tre_empty), "Empty");
    assert_eq!(format!("{}", tre_empty), "receiving on an empty channel");
    assert!(tre_empty.source().is_none());

    // TryRecvError::Closed
    let tre_closed = TryRecvError::Closed;
    let tre_closed2 = tre_closed;
    assert_eq!(tre_closed, tre_closed2);
    assert!(!tre_closed.is_empty());
    assert!(tre_closed.is_closed());
    assert_eq!(format!("{:?}", tre_closed), "Closed");
    assert_eq!(
        format!("{}", tre_closed),
        "receiving on an empty and closed channel"
    );
    assert!(tre_closed.source().is_none());

    assert_ne!(TryRecvError::Empty, TryRecvError::Closed);
}

// Target: Exercise fmt::Debug on Sender and Receiver across unbounded and bounded variants.
#[test]
fn test_sender_receiver_debug_formatting() {
    let (tx_unb, rx_unb) = unbounded::<i32>();
    let dbg_tx = format!("{:?}", tx_unb);
    let dbg_rx = format!("{:?}", rx_unb);
    assert!(dbg_tx.starts_with("Sender"));
    assert!(dbg_tx.contains("capacity: None"));
    assert!(dbg_rx.starts_with("Receiver"));
    assert!(dbg_rx.contains("capacity: None"));

    let (tx_b, rx_b) = bounded::<i32>(42);
    let dbg_tx_b = format!("{:?}", tx_b);
    let dbg_rx_b = format!("{:?}", rx_b);
    assert!(dbg_tx_b.contains("capacity: Some(42)"));
    assert!(dbg_rx_b.contains("capacity: Some(42)"));
}

// Target: Verify Send future panics with 'Send polled after completion' if polled after ready.
#[test]
#[should_panic(expected = "Send polled after completion")]
fn test_send_future_polled_after_completion_panics() {
    use futures::task::noop_waker;
    use std::pin::Pin;
    use std::task::Context;

    let (tx, _rx) = unbounded::<i32>();
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let mut send_fut = tx.send(10);
    assert_eq!(
        Pin::new(&mut send_fut).poll(&mut cx),
        std::task::Poll::Ready(Ok(()))
    );
    let _ = Pin::new(&mut send_fut).poll(&mut cx);
}

// Target: Verify Recv future behavior when polled after completion (returns Pending when empty, Err when closed).
#[test]
fn test_recv_future_re_polled_after_completion() {
    use futures::task::noop_waker;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let (tx, rx) = unbounded::<i32>();
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    tx.try_send(42).unwrap();
    let mut recv_fut = rx.recv();
    assert_eq!(Pin::new(&mut recv_fut).poll(&mut cx), Poll::Ready(Ok(42)));

    // Polling again on empty channel returns Pending
    assert_eq!(Pin::new(&mut recv_fut).poll(&mut cx), Poll::Pending);

    // Close channel and poll again: returns Ready(Err(RecvError))
    tx.close();
    assert_eq!(
        Pin::new(&mut recv_fut).poll(&mut cx),
        Poll::Ready(Err(RecvError))
    );
}

// Target: Exercise WaiterList will_wake branches: same waker (no-op) and different waker (update).
#[test]
fn test_send_and_recv_waker_will_wake_dedup() {
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    // 1. Recv waker dedup
    let (tx, rx) = unbounded::<i32>();
    let waker1 = Waker::from(Arc::new(DummyWaker(10)));
    let mut cx1 = Context::from_waker(&waker1);
    let mut recv_fut = rx.recv();

    // Initial poll registers waker1
    assert_eq!(Pin::new(&mut recv_fut).poll(&mut cx1), Poll::Pending);

    // Poll with same waker: entry.1.will_wake(waker) is true, does not re-clone
    let waker1_clone = waker1.clone();
    let mut cx1_clone = Context::from_waker(&waker1_clone);
    assert_eq!(Pin::new(&mut recv_fut).poll(&mut cx1_clone), Poll::Pending);

    // Poll with different waker: will_wake is false, updates entry
    let waker2 = Waker::from(Arc::new(DummyWaker(20)));
    let mut cx2 = Context::from_waker(&waker2);
    assert_eq!(Pin::new(&mut recv_fut).poll(&mut cx2), Poll::Pending);

    tx.try_send(99).unwrap();
    assert_eq!(Pin::new(&mut recv_fut).poll(&mut cx2), Poll::Ready(Ok(99)));

    // 2. Send waker dedup
    let (tx_b, rx_b) = bounded::<i32>(1);
    tx_b.try_send(1).unwrap();
    let mut send_fut = tx_b.send(2);

    assert_eq!(Pin::new(&mut send_fut).poll(&mut cx1), Poll::Pending);
    assert_eq!(Pin::new(&mut send_fut).poll(&mut cx1_clone), Poll::Pending);
    assert_eq!(Pin::new(&mut send_fut).poll(&mut cx2), Poll::Pending);

    assert_eq!(rx_b.try_recv().unwrap(), 1);
    assert_eq!(Pin::new(&mut send_fut).poll(&mut cx2), Poll::Ready(Ok(())));
    assert_eq!(rx_b.try_recv().unwrap(), 2);
}

// Target: Exercise re-registration when waiter_id is Some(id) but entry was popped by notify_one.
#[test]
fn test_waiter_re_registration_after_notify_popped_entry() {
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    // Recv side:
    let (tx, rx) = unbounded::<i32>();
    let rx2 = rx.clone();
    let waker1 = Waker::from(Arc::new(DummyWaker(100)));
    let mut cx1 = Context::from_waker(&waker1);

    let mut r = rx.recv();
    assert_eq!(Pin::new(&mut r).poll(&mut cx1), Poll::Pending);

    // tx sends, waking r (popping r's waiter entry from list)
    tx.try_send(1).unwrap();

    // rx2 races and takes the value before r polls
    assert_eq!(rx2.try_recv().unwrap(), 1);

    // r polls again on empty channel: r has waiter_id=Some(id), but is not in list
    // It must re-register its id into the list and return Pending
    assert_eq!(Pin::new(&mut r).poll(&mut cx1), Poll::Pending);

    // Send another value, r receives it
    tx.try_send(2).unwrap();
    assert_eq!(Pin::new(&mut r).poll(&mut cx1), Poll::Ready(Ok(2)));

    // Send side:
    let (tx_b, rx_b) = bounded::<i32>(1);
    let tx_b2 = tx_b.clone();
    tx_b.try_send(10).unwrap();

    let mut s = tx_b.send(20);
    assert_eq!(Pin::new(&mut s).poll(&mut cx1), Poll::Pending);

    // rx_b pops, waking s (popping s's entry from send_waiters list)
    assert_eq!(rx_b.try_recv().unwrap(), 10);

    // tx_b2 races and takes the available slot
    tx_b2.try_send(30).unwrap();

    // s polls again on full channel: s has waiter_id=Some(id), but not in list
    // Re-registers into list and returns Pending
    assert_eq!(Pin::new(&mut s).poll(&mut cx1), Poll::Pending);

    // Clear slot, s completes
    assert_eq!(rx_b.try_recv().unwrap(), 30);
    assert_eq!(Pin::new(&mut s).poll(&mut cx1), Poll::Ready(Ok(())));
    assert_eq!(rx_b.try_recv().unwrap(), 20);
}

// Target: Wake-up forwarding through a chain of multiple cancelled waiters for both receivers and senders.
#[test]
fn test_cancellation_forwarding_chain_multiple_waiters() {
    use futures::task::noop_waker;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    // 1. Receivers chain
    let (tx, rx) = unbounded::<i32>();
    let rx4 = rx.clone();

    let mut r1 = rx.recv();
    assert_eq!(Pin::new(&mut r1).poll(&mut cx), Poll::Pending);
    let mut r2 = rx.recv();
    assert_eq!(Pin::new(&mut r2).poll(&mut cx), Poll::Pending);
    let mut r3 = rx.recv();
    assert_eq!(Pin::new(&mut r3).poll(&mut cx), Poll::Pending);
    let mut r4 = rx4.recv();
    assert_eq!(Pin::new(&mut r4).poll(&mut cx), Poll::Pending);

    // All 4 are parked in order: R1, R2, R3, R4.
    // Send 1 item: wakes R1
    tx.try_send(777).unwrap();
    // Drop R1 -> forwards to R2 -> drop R2 -> forwards to R3 -> drop R3 -> forwards to R4
    drop(r1);
    drop(r2);
    drop(r3);

    // R4 receives the item
    assert_eq!(Pin::new(&mut r4).poll(&mut cx), Poll::Ready(Ok(777)));

    // 2. Senders chain
    let (tx_b, rx_b) = bounded::<i32>(1);
    tx_b.try_send(1).unwrap();
    let tx_b4 = tx_b.clone();

    let mut s1 = tx_b.send(10);
    assert_eq!(Pin::new(&mut s1).poll(&mut cx), Poll::Pending);
    let mut s2 = tx_b.send(20);
    assert_eq!(Pin::new(&mut s2).poll(&mut cx), Poll::Pending);
    let mut s3 = tx_b.send(30);
    assert_eq!(Pin::new(&mut s3).poll(&mut cx), Poll::Pending);
    let mut s4 = tx_b4.send(40);
    assert_eq!(Pin::new(&mut s4).poll(&mut cx), Poll::Pending);

    // Pop 1 item: wakes S1
    assert_eq!(rx_b.try_recv().unwrap(), 1);
    // Drop S1 -> forwards to S2 -> drop S2 -> forwards to S3 -> drop S3 -> forwards to S4
    drop(s1);
    drop(s2);
    drop(s3);

    // S4 completes send
    assert_eq!(Pin::new(&mut s4).poll(&mut cx), Poll::Ready(Ok(())));
    assert_eq!(rx_b.try_recv().unwrap(), 40);
}

// Target: Cancellation forwarding when channel is closed while waiters are parked.
#[test]
fn test_cancellation_with_close_racing() {
    use futures::task::noop_waker;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    // Receivers:
    let (tx, rx) = unbounded::<i32>();
    let mut r1 = rx.recv();
    assert_eq!(Pin::new(&mut r1).poll(&mut cx), Poll::Pending);
    let mut r2 = rx.recv();
    assert_eq!(Pin::new(&mut r2).poll(&mut cx), Poll::Pending);

    // Send item 1 -> wakes R1
    tx.try_send(1).unwrap();
    // Close channel
    tx.close();
    // Drop R1 (forwards notification to R2)
    drop(r1);

    // R2 receives item 1
    assert_eq!(Pin::new(&mut r2).poll(&mut cx), Poll::Ready(Ok(1)));

    // Senders:
    let (tx_b, rx_b) = bounded::<i32>(1);
    tx_b.try_send(1).unwrap();
    let mut s1 = tx_b.send(2);
    assert_eq!(Pin::new(&mut s1).poll(&mut cx), Poll::Pending);
    let mut s2 = tx_b.send(3);
    assert_eq!(Pin::new(&mut s2).poll(&mut cx), Poll::Pending);

    // Close channel: wakes all waiters
    rx_b.close();
    drop(s1);

    assert_eq!(
        Pin::new(&mut s2).poll(&mut cx),
        Poll::Ready(Err(SendError(3)))
    );
}

// Target: Exercise notify_all paths when closing from Sender and Receiver with parked waiters.
#[tokio::test]
async fn test_close_from_sender_and_receiver_all_notify_all_paths() {
    // 1. tx.close() wakes all parked receivers
    let (tx1, rx1) = unbounded::<i32>();
    let mut r_handles = Vec::new();
    for _ in 0..4 {
        let rx = rx1.clone();
        r_handles.push(tokio::spawn(async move { rx.recv().await }));
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(tx1.close());
    for h in r_handles {
        assert_eq!(h.await.unwrap(), Err(RecvError));
    }

    // 2. tx.close() wakes all parked senders
    let (tx2, _rx2) = bounded::<i32>(1);
    tx2.try_send(1).unwrap();
    let mut s_handles = Vec::new();
    for i in 0..4 {
        let tx = tx2.clone();
        s_handles.push(tokio::spawn(async move { tx.send(i + 10).await }));
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(tx2.close());
    for (i, h) in s_handles.into_iter().enumerate() {
        assert_eq!(h.await.unwrap(), Err(SendError(i as i32 + 10)));
    }

    // 3. rx.close() wakes all parked receivers
    let (_tx3, rx3) = unbounded::<i32>();
    let mut r_handles3 = Vec::new();
    for _ in 0..4 {
        let rx = rx3.clone();
        r_handles3.push(tokio::spawn(async move { rx.recv().await }));
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(rx3.close());
    for h in r_handles3 {
        assert_eq!(h.await.unwrap(), Err(RecvError));
    }

    // 4. rx.close() wakes all parked senders
    let (tx4, rx4) = bounded::<i32>(1);
    tx4.try_send(1).unwrap();
    let mut s_handles4 = Vec::new();
    for i in 0..4 {
        let tx = tx4.clone();
        s_handles4.push(tokio::spawn(async move { tx.send(i + 20).await }));
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(rx4.close());
    for (i, h) in s_handles4.into_iter().enumerate() {
        assert_eq!(h.await.unwrap(), Err(SendError(i as i32 + 20)));
    }
}

// Target: Verify try_recv drains all items after close, then persistently returns Closed.
#[test]
fn test_try_recv_closed_drain_loop() {
    let (tx, rx) = unbounded::<usize>();
    const COUNT: usize = 200;
    for i in 0..COUNT {
        tx.try_send(i).unwrap();
    }
    assert!(tx.close());
    assert!(tx.is_closed());
    assert!(rx.is_closed());

    for i in 0..COUNT {
        assert_eq!(rx.try_recv(), Ok(i));
    }

    for _ in 0..5 {
        assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
        assert!(rx.is_empty());
        assert_eq!(rx.len(), 0);
    }
}

// Target: Receiver::__debug_dump on empty, partially filled, multi-block, and drained queues.
#[test]
fn test_receiver_debug_dump_variants() {
    let (tx, rx) = unbounded::<usize>();

    // Empty queue
    let d_empty = rx.__debug_dump();
    assert!(d_empty.contains("head=0"));
    assert!(d_empty.contains("tail=0"));

    // Partially filled queue
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    let d_partial = rx.__debug_dump();
    assert!(d_partial.contains("tail=2"));

    // Multi-block queue (200 items spans multiple blocks regardless of block size)
    for i in 3..200 {
        tx.try_send(i).unwrap();
    }
    let d_multi = rx.__debug_dump();
    assert!(d_multi.contains("chain:"));

    // Drained queue
    for _ in 0..199 {
        let _ = rx.try_recv().unwrap();
    }
    let d_drained = rx.__debug_dump();
    assert!(d_drained.contains("chain:"));
}

// Target: is_empty, len, capacity, sender_count, and receiver_count under concurrency.
#[test]
fn test_concurrency_len_empty_counts_capacity() {
    use std::thread;

    let (tx, rx) = bounded::<usize>(50);
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);

    let mut handles = Vec::new();
    // Producer thread
    {
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..2000 {
                loop {
                    if tx.try_send(i).is_ok() {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }));
    }
    // Consumer thread
    {
        let rx = rx.clone();
        handles.push(thread::spawn(move || {
            let mut count = 0;
            while count < 2000 {
                if rx.try_recv().is_ok() {
                    count += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        }));
    }
    // Inspector thread checking properties concurrently
    {
        let tx = tx.clone();
        let rx = rx.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                assert_eq!(tx.capacity(), Some(50));
                assert_eq!(rx.capacity(), Some(50));
                let _ = tx.sender_count();
                let _ = rx.receiver_count();
                let len = tx.len();
                let _is_empty = tx.is_empty();
                assert!(len <= 50);
                std::hint::spin_loop();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    assert!(rx.is_empty());
    assert_eq!(rx.len(), 0);
}

// Target: Dropping unread values of a Drop-counting type across several blocks.
#[test]
fn test_dropping_channel_unread_values_multi_block() {
    struct DropItem(Arc<AtomicUsize>);
    impl Drop for DropItem {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    const TOTAL: usize = 350;
    const READ: usize = 70;

    {
        let (tx, rx) = unbounded::<DropItem>();
        for _ in 0..TOTAL {
            tx.try_send(DropItem(drops.clone())).unwrap();
        }
        for _ in 0..READ {
            let item = rx.try_recv().unwrap();
            drop(item);
        }
        assert_eq!(drops.load(Ordering::SeqCst), READ);
        // Dropping tx and rx drops the remaining TOTAL - READ items
    }
    assert_eq!(drops.load(Ordering::SeqCst), TOTAL);

    // Bounded channel drop test
    let drops_b = Arc::new(AtomicUsize::new(0));
    {
        let (tx_b, _rx_b) = bounded::<DropItem>(100);
        for _ in 0..80 {
            tx_b.try_send(DropItem(drops_b.clone())).unwrap();
        }
    }
    assert_eq!(drops_b.load(Ordering::SeqCst), 80);
}

// Target: Zero-sized types and large types ([u64; 32]) across block boundaries.
#[test]
fn test_zero_sized_and_large_types() {
    // Zero-sized type ()
    let (tx_z, rx_z) = unbounded::<()>();
    for _ in 0..300 {
        tx_z.try_send(()).unwrap();
    }
    assert_eq!(tx_z.len(), 300);
    for _ in 0..300 {
        assert_eq!(rx_z.try_recv().unwrap(), ());
    }
    assert!(rx_z.is_empty());

    // Bounded zero-sized type
    let (tx_zb, rx_zb) = bounded::<()>(5);
    for _ in 0..5 {
        tx_zb.try_send(()).unwrap();
    }
    assert!(tx_zb.try_send(()).is_err());
    for _ in 0..5 {
        assert_eq!(rx_zb.try_recv().unwrap(), ());
    }

    // Large type [u64; 32] (256 bytes per element)
    type Big = [u64; 32];
    let (tx_b, rx_b) = unbounded::<Big>();
    for i in 0..150u64 {
        let mut val = [0u64; 32];
        val[0] = i;
        val[31] = i * 10;
        tx_b.try_send(val).unwrap();
    }
    for i in 0..150u64 {
        let val = rx_b.try_recv().unwrap();
        assert_eq!(val[0], i);
        assert_eq!(val[31], i * 10);
    }
}

// Target: Block pool slow paths (take_block_slow, recycle_slow) by growing and shrinking backlog.
#[test]
fn test_block_pool_slow_paths() {
    let (tx, rx) = unbounded::<usize>();
    const COUNT: usize = 20_000;

    for round in 0..3 {
        for i in 0..COUNT {
            tx.try_send(round * 1_000_000 + i).unwrap();
        }
        for i in 0..COUNT {
            assert_eq!(rx.try_recv().unwrap(), round * 1_000_000 + i);
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }
}

// Target: claim_bounded cached-head refresh and sentinel skip under contention.
#[test]
fn test_bounded_claim_sentinel_and_cached_head() {
    use std::thread;

    // 1. Deterministic cached-head refresh
    let (tx, rx) = bounded::<usize>(2);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert!(tx.try_send(3).is_err()); // Full; cached head updated to 0

    // Pop 1: head moves to 1, but tx.cached is still 0
    assert_eq!(rx.try_recv().unwrap(), 1);

    // Next send: cache says full (tail 2 - cached 0 >= 2), confirms against real head (1),
    // refreshes cached head to 1, and succeeds!
    assert!(tx.try_send(3).is_ok());

    // Next send: tail 3 - cached 1 >= 2 => full; confirms against real head (1) => returns Err
    assert!(tx.try_send(4).is_err());
    assert_eq!(rx.try_recv().unwrap(), 2);
    assert_eq!(rx.try_recv().unwrap(), 3);

    // 2. Sentinel-skip CAS under multi-threaded contention
    let (tx_m, rx_m) = bounded::<usize>(70);
    let tx_m = Arc::new(tx_m);
    let mut handles = Vec::new();
    for p in 0..8 {
        let tx = tx_m.clone();
        handles.push(thread::spawn(move || {
            for i in 0..500 {
                loop {
                    if tx.try_send(p * 10_000 + i).is_ok() {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }));
    }

    let mut received = 0;
    while received < 8 * 500 {
        if rx_m.try_recv().is_ok() {
            received += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(received, 4000);
}

// Target: Contended CAS mode for producers via shared Arc<Sender> and clones, then countdown to fetch_add.
#[test]
fn test_contended_cas_mode_shared_arc_and_clones() {
    use std::thread;

    let (tx, rx) = unbounded::<usize>();
    let shared_tx = Arc::new(tx);
    let mut handles = Vec::new();

    // 8 threads pushing through ONE shared Arc<Sender>
    for p in 0..8 {
        let tx = shared_tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                tx.try_send(p * 10_000 + i).unwrap();
            }
        }));
    }

    // 8 threads pushing through cloned Senders
    for p in 8..16 {
        let tx = (*shared_tx).clone();
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                tx.try_send(p * 10_000 + i).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Now push 100 uncontended messages from single thread to count down contended window back to fetch_add
    for i in 0..100 {
        shared_tx.try_send(999_000 + i).unwrap();
    }

    let mut count = 0;
    while rx.try_recv().is_ok() {
        count += 1;
    }
    assert_eq!(count, 16 * 1000 + 100);
}

// Target: 8 producers x 8 consumers with std::thread, exact receipt and per-producer FIFO.
#[test]
fn test_threads_unbounded_and_bounded_mpmc_fifo() {
    use std::thread;

    const PRODUCERS: usize = 8;
    const CONSUMERS: usize = 8;
    const PER_PRODUCER: usize = 1500;
    const TOTAL: usize = PRODUCERS * PER_PRODUCER;

    // 1. Unbounded
    {
        let (tx, rx) = unbounded::<u64>();
        let rx = Arc::new(rx);
        let mut prod_handles = Vec::new();
        for p in 0..PRODUCERS {
            let tx_c = tx.clone();
            prod_handles.push(thread::spawn(move || {
                for seq in 0..PER_PRODUCER {
                    let msg = ((p as u64) << 32) | (seq as u64);
                    tx_c.try_send(msg).unwrap();
                }
            }));
        }
        drop(tx);

        let mut cons_handles = Vec::new();
        for _ in 0..CONSUMERS {
            let rx_c = rx.clone();
            cons_handles.push(thread::spawn(move || {
                let mut seen = Vec::new();
                let mut last_seen = [None; PRODUCERS];
                while let Ok(msg) = rx_c.try_recv() {
                    let p = (msg >> 32) as usize;
                    let seq = msg & 0xFFFF_FFFF;
                    if let Some(prev) = last_seen[p] {
                        assert!(
                            seq > prev,
                            "Consumer saw out-of-order seq {} after {} for producer {}",
                            seq,
                            prev,
                            p
                        );
                    }
                    last_seen[p] = Some(seq);
                    seen.push(msg);
                }
                seen
            }));
        }

        for h in prod_handles {
            h.join().unwrap();
        }

        // Collect all messages across all consumers
        let mut all_msgs = Vec::with_capacity(TOTAL);
        for h in cons_handles {
            all_msgs.extend(h.join().unwrap());
        }
        while let Ok(msg) = rx.try_recv() {
            all_msgs.push(msg);
        }
        assert_eq!(all_msgs.len(), TOTAL);

        // Verify every value was received exactly once
        all_msgs.sort_unstable();
        for p in 0..PRODUCERS {
            for seq in 0..PER_PRODUCER {
                let expected = ((p as u64) << 32) | (seq as u64);
                assert_eq!(all_msgs[p * PER_PRODUCER + seq], expected);
            }
        }
    }

    // 2. Bounded (cap = 16)
    {
        let (tx, rx) = bounded::<u64>(16);
        let tx = Arc::new(tx);
        let rx = Arc::new(rx);
        let received_count = Arc::new(AtomicUsize::new(0));

        let mut prod_handles = Vec::new();
        for p in 0..PRODUCERS {
            let tx = tx.clone();
            prod_handles.push(thread::spawn(move || {
                for seq in 0..PER_PRODUCER {
                    let msg = ((p as u64) << 32) | (seq as u64);
                    loop {
                        if tx.try_send(msg).is_ok() {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            }));
        }

        let mut cons_handles = Vec::new();
        for _ in 0..CONSUMERS {
            let rx = rx.clone();
            let rc = received_count.clone();
            cons_handles.push(thread::spawn(move || {
                let mut seen = Vec::new();
                let mut last_seen = [None; PRODUCERS];
                loop {
                    if let Ok(msg) = rx.try_recv() {
                        let p = (msg >> 32) as usize;
                        let seq = msg & 0xFFFF_FFFF;
                        if let Some(prev) = last_seen[p] {
                            assert!(
                                seq > prev,
                                "Bounded consumer saw out-of-order seq {} after {} for producer {}",
                                seq,
                                prev,
                                p
                            );
                        }
                        last_seen[p] = Some(seq);
                        seen.push(msg);
                        if rc.fetch_add(1, Ordering::Relaxed) + 1 == TOTAL {
                            break;
                        }
                    } else if rc.load(Ordering::Relaxed) == TOTAL {
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                }
                seen
            }));
        }

        for h in prod_handles {
            h.join().unwrap();
        }

        let mut all_msgs = Vec::with_capacity(TOTAL);
        for h in cons_handles {
            all_msgs.extend(h.join().unwrap());
        }
        assert_eq!(all_msgs.len(), TOTAL);

        // Verify every value was received exactly once
        all_msgs.sort_unstable();
        for p in 0..PRODUCERS {
            for seq in 0..PER_PRODUCER {
                let expected = ((p as u64) << 32) | (seq as u64);
                assert_eq!(all_msgs[p * PER_PRODUCER + seq], expected);
            }
        }
    }
}

// Simple xorshift PRNG for deterministic pseudo-random sequences
struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0xDEAD_BEEF_CAFE_BABE
        } else {
            seed
        })
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

// Target: Tokio multi_thread test with 64 spawned producer tasks and 4 consumers awaiting async send/recv.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tokio_multi_thread_64_producers_4_consumers_async() {
    const PRODUCERS: usize = 64;
    const CONSUMERS: usize = 4;
    const PER_PRODUCER: usize = 150;
    const TOTAL: usize = PRODUCERS * PER_PRODUCER;

    let (tx, rx) = bounded::<u64>(32);

    let mut cons_handles = Vec::new();
    for _ in 0..CONSUMERS {
        let rx = rx.clone();
        cons_handles.push(tokio::spawn(async move {
            let mut seen = Vec::new();
            let mut last_seen = vec![None; PRODUCERS];
            let mut rng = XorShift64::new(0x9E37_79B9_7F4A_7C15);
            while let Ok(msg) = rx.recv().await {
                let p = (msg >> 32) as usize;
                let seq = msg & 0xFFFF_FFFF;
                if let Some(prev) = last_seen[p] {
                    assert!(
                        seq > prev,
                        "Async consumer saw out-of-order seq {} after {} for producer {}",
                        seq,
                        prev,
                        p
                    );
                }
                last_seen[p] = Some(seq);
                seen.push(msg);
                if rng.next().is_multiple_of(5) {
                    tokio::task::yield_now().await;
                }
            }
            seen
        }));
    }
    drop(rx); // Drop original rx so consumers exit when senders drop

    let mut prod_handles = Vec::new();
    for p in 0..PRODUCERS {
        let tx = tx.clone();
        prod_handles.push(tokio::spawn(async move {
            let mut rng = XorShift64::new((p as u64 + 1).wrapping_mul(0xBF58_476D_1CE4_E5B9));
            for seq in 0..PER_PRODUCER {
                let msg = ((p as u64) << 32) | (seq as u64);
                tx.send(msg).await.unwrap();
                if rng.next().is_multiple_of(7) {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }
    drop(tx); // Drop original tx

    for h in prod_handles {
        h.await.unwrap();
    }

    let mut all_msgs = Vec::with_capacity(TOTAL);
    for h in cons_handles {
        all_msgs.extend(h.await.unwrap());
    }
    assert_eq!(all_msgs.len(), TOTAL);

    // Verify every value was received exactly once
    all_msgs.sort_unstable();
    for p in 0..PRODUCERS {
        for seq in 0..PER_PRODUCER {
            let expected = ((p as u64) << 32) | (seq as u64);
            assert_eq!(all_msgs[p * PER_PRODUCER + seq], expected);
        }
    }
}

// Target: tokio::select! cancellation storm with many recv futures dropped while values flow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_select_cancellation_storm() {
    const TOTAL: usize = 1500;
    let (tx, rx) = unbounded::<usize>();

    // Producer task pushing values
    let prod_h = tokio::spawn(async move {
        for i in 0..TOTAL {
            tx.send(i).await.unwrap();
            if i % 50 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    // 4 consumer tasks frequently cancelling recv futures via select!
    let received = Arc::new(std::sync::Mutex::new(Vec::with_capacity(TOTAL)));
    let done = Arc::new(tokio::sync::Notify::new());
    let mut cons_handles = Vec::new();

    for c in 0..4 {
        let rx = rx.clone();
        let rec = received.clone();
        let done = done.clone();
        cons_handles.push(tokio::spawn(async move {
            let mut rng = XorShift64::new(0x1234_5678_9ABC_DEF0 + c as u64 * 100);
            loop {
                tokio::select! {
                    biased;
                    res = rx.recv() => {
                        match res {
                            Ok(val) => {
                                let mut l = rec.lock().unwrap();
                                l.push(val);
                                if l.len() == TOTAL {
                                    done.notify_waiters();
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    _ = tokio::task::yield_now(), if rng.next().is_multiple_of(2) => {
                        // Cancellation: recv() dropped before completion
                    }
                }
                if rec.lock().unwrap().len() >= TOTAL {
                    break;
                }
            }
        }));
    }

    // Wait for all messages or timeout
    tokio::time::timeout(Duration::from_secs(10), done.notified())
        .await
        .expect("Cancellation storm should deliver all messages without deadlock");

    prod_h.await.unwrap();
    for h in cons_handles {
        h.abort();
    }

    let mut all = received.lock().unwrap().clone();
    all.sort_unstable();
    assert_eq!(all.len(), TOTAL);
    for (i, &v) in all.iter().enumerate() {
        assert_eq!(v, i);
    }
}

// Target: Bounded backpressure end-to-end with capacities 1, 2, 63, 64, 65, and 1000.
#[tokio::test]
async fn test_bounded_backpressure_all_capacities() {
    const CAPACITIES: &[usize] = &[1, 2, 63, 64, 65, 1000];

    for &cap in CAPACITIES {
        let (tx, rx) = bounded::<usize>(cap);
        assert_eq!(tx.capacity(), Some(cap));
        assert_eq!(rx.capacity(), Some(cap));

        for i in 0..cap {
            tx.try_send(i).unwrap();
        }
        assert_eq!(tx.len(), cap);
        assert_eq!(tx.try_send(cap), Err(TrySendError::Full(cap)));

        // Extra send in spawned task
        let tx_clone = tx.clone();
        let send_extra = tokio::spawn(async move {
            tx_clone.send(cap).await.unwrap();
        });

        // Pop 1 item
        let got = rx.recv().await.unwrap();
        assert_eq!(got, 0);

        // Bounded sender should complete
        tokio::time::timeout(Duration::from_secs(2), send_extra)
            .await
            .expect("send_extra must complete")
            .unwrap();

        // Drain remaining cap items (1..=cap)
        for expected in 1..=cap {
            let val = rx.recv().await.unwrap();
            assert_eq!(val, expected);
        }
        assert!(rx.is_empty());
        assert_eq!(rx.len(), 0);
    }
}

// Target: Edge cases for manual polling of Send and Recv futures (pre-closed, dropped without poll, etc.)
#[test]
fn test_futures_manual_poll_edge_cases() {
    use futures::task::noop_waker;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    // 1. Send on channel closed before poll
    let (tx, rx) = bounded::<i32>(1);
    tx.try_send(1).unwrap();
    rx.close();
    let mut send_fut = tx.send(2);
    assert_eq!(
        Pin::new(&mut send_fut).poll(&mut cx),
        Poll::Ready(Err(SendError(2)))
    );

    // 2. Recv on channel closed before poll
    let (tx2, rx2) = unbounded::<i32>();
    tx2.close();
    let mut recv_fut = rx2.recv();
    assert_eq!(
        Pin::new(&mut recv_fut).poll(&mut cx),
        Poll::Ready(Err(RecvError))
    );

    // 3. Drop Send and Recv without ever polling
    let (tx3, rx3) = bounded::<i32>(1);
    let s_unpolled = tx3.send(1);
    drop(s_unpolled);
    let r_unpolled = rx3.recv();
    drop(r_unpolled);

    // 4. Drop Send and Recv while still parked in list (not yet notified)
    let (tx4, _rx4) = bounded::<i32>(1);
    tx4.try_send(1).unwrap();
    let mut s_parked = tx4.send(2);
    assert_eq!(Pin::new(&mut s_parked).poll(&mut cx), Poll::Pending);
    drop(s_parked); // unregisters from list (pos is Some)

    let (_tx5, rx5) = unbounded::<i32>();
    let mut r_parked = rx5.recv();
    assert_eq!(Pin::new(&mut r_parked).poll(&mut cx), Poll::Pending);
    drop(r_parked); // unregisters from list (pos is Some)
}
