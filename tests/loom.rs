//! Model-checked interleavings of the core protocol.
//!
//! Run with:
//!   cargo test --release --features loom --test loom
//!
//! With the `loom` feature blocks hold 3 values, so every test below crosses at least one
//! block boundary and exercises the sentinel transition and block recycling.
#![cfg(feature = "loom")]

use loom::future::block_on;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;
use rapidfire::{bounded, unbounded, RecvError, TryRecvError};

fn model<F: Fn() + Sync + Send + 'static>(f: F) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(f);
}

#[test]
fn mpsc_single_receiver_unbounded_recycles() {
    model(|| {
        let (tx, mut rx) = rapidfire::mpsc::unbounded();
        let sender = thread::spawn(move || {
            for i in 0..5 {
                tx.try_send(i).unwrap();
            }
        });
        for i in 0..5 {
            assert_eq!(block_on(rx.recv()), Ok(i));
        }
        sender.join().unwrap();
        assert_eq!(block_on(rx.recv()), Err(RecvError));
    });
}

#[test]
fn mpsc_bounded_two_waiting_senders() {
    // Three actors with parked futures grow the state space rapidly. Keep this
    // additional model at two preemptions; other queue models use three.
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, mut rx) = rapidfire::mpsc::bounded(1);
        tx.try_send(0).unwrap();
        let tx2 = tx.clone();
        let a = thread::spawn(move || block_on(tx.send(1)).unwrap());
        let b = thread::spawn(move || block_on(tx2.send(2)).unwrap());
        assert_eq!(block_on(rx.recv()), Ok(0));
        let mut got = [block_on(rx.recv()).unwrap(), block_on(rx.recv()).unwrap()];
        got.sort_unstable();
        assert_eq!(got, [1, 2]);
        a.join().unwrap();
        b.join().unwrap();
        assert_eq!(block_on(rx.recv()), Err(RecvError));
    });
}

#[test]
fn general_bounded_two_waiting_senders() {
    // Three actors with parked futures grow the state space rapidly. Keep this
    // additional model at two preemptions; other queue models use three.
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let (tx, rx) = rapidfire::bounded(1);
        tx.try_send(0).unwrap();
        let tx2 = tx.clone();
        let a = thread::spawn(move || block_on(tx.send(1)).unwrap());
        let b = thread::spawn(move || block_on(tx2.send(2)).unwrap());
        assert_eq!(block_on(rx.recv()), Ok(0));
        let mut got = [block_on(rx.recv()).unwrap(), block_on(rx.recv()).unwrap()];
        got.sort_unstable();
        assert_eq!(got, [1, 2]);
        a.join().unwrap();
        b.join().unwrap();
        assert_eq!(block_on(rx.recv()), Err(RecvError));
    });
}

#[test]
fn mpsc_batch_wakes_sender_at_block_transition() {
    model(|| {
        let (tx, mut rx) = rapidfire::mpsc::bounded(3);
        for i in 0..3 {
            tx.try_send(i).unwrap();
        }
        let sender = thread::spawn(move || block_on(tx.send(3)).unwrap());
        let mut got = Vec::new();
        assert_eq!(block_on(rx.recv_many(&mut got, 3)), Ok(3));
        assert_eq!(got, [0, 1, 2]);
        assert_eq!(block_on(rx.recv()), Ok(3));
        sender.join().unwrap();
        assert_eq!(block_on(rx.recv()), Err(RecvError));
    });
}

#[test]
fn mpsc_recv_is_woken_by_send_or_close() {
    model(|| {
        let (tx, mut rx) = rapidfire::mpsc::unbounded();
        let receiver = thread::spawn(move || {
            assert_eq!(block_on(rx.recv()), Ok(7));
            assert_eq!(block_on(rx.recv()), Err(RecvError));
        });
        tx.try_send(7).unwrap();
        drop(tx);
        receiver.join().unwrap();
    });
}

#[test]
fn two_producers_one_consumer_cross_block() {
    model(|| {
        let (tx, rx) = unbounded::<usize>();
        let tx2 = tx.clone();
        let a = thread::spawn(move || {
            tx.try_send(1).unwrap();
            tx.try_send(2).unwrap();
        });
        let b = thread::spawn(move || {
            tx2.try_send(10).unwrap();
            tx2.try_send(20).unwrap();
        });
        a.join().unwrap();
        b.join().unwrap();
        let mut got = Vec::new();
        while let Ok(v) = rx.try_recv() {
            got.push(v);
        }
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 10, 20]);
    });
}

#[test]
fn producer_and_consumer_concurrent_fifo() {
    model(|| {
        let (tx, rx) = unbounded::<usize>();
        let p = thread::spawn(move || {
            for i in 0..5 {
                tx.try_send(i).unwrap();
            }
        });
        let mut next = 0;
        loop {
            match rx.try_recv() {
                Ok(v) => {
                    assert_eq!(v, next);
                    next += 1;
                    if next == 5 {
                        break;
                    }
                }
                Err(TryRecvError::Empty) => thread::yield_now(),
                Err(TryRecvError::Closed) => panic!("closed early"),
            }
        }
        p.join().unwrap();
    });
}

#[test]
fn two_consumers_take_each_value_once() {
    model(|| {
        let (tx, rx) = unbounded::<usize>();
        for i in 0..4 {
            tx.try_send(i).unwrap();
        }
        let rx2 = rx.clone();
        let sum = Arc::new(AtomicUsize::new(0));
        let count = Arc::new(AtomicUsize::new(0));
        let (s2, c2) = (sum.clone(), count.clone());
        let c = thread::spawn(move || {
            while let Ok(v) = rx2.try_recv() {
                s2.fetch_add(v, Ordering::Relaxed);
                c2.fetch_add(1, Ordering::Relaxed);
            }
        });
        while let Ok(v) = rx.try_recv() {
            sum.fetch_add(v, Ordering::Relaxed);
            count.fetch_add(1, Ordering::Relaxed);
        }
        c.join().unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 4);
        assert_eq!(sum.load(Ordering::Relaxed), 0 + 1 + 2 + 3);
    });
}

#[test]
fn overlapping_consumers_recycle_blocks_without_losing_values() {
    model(|| {
        let (tx, rx) = unbounded::<usize>();
        for i in 0..4 {
            tx.try_send(i).unwrap();
        }
        let rx2 = rx.clone();
        let other =
            thread::spawn(move || [block_on(rx2.recv()).unwrap(), block_on(rx2.recv()).unwrap()]);
        let mut got = vec![block_on(rx.recv()).unwrap(), block_on(rx.recv()).unwrap()];
        // The second block transition may reclaim a block retired by the other
        // reader, or leave it alone if that reader has not finished its value copy.
        tx.try_send(4).unwrap();
        tx.try_send(5).unwrap();
        got.push(block_on(rx.recv()).unwrap());
        got.push(block_on(rx.recv()).unwrap());
        got.extend(other.join().unwrap());
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 3, 4, 5]);
    });
}

#[test]
fn async_recv_is_woken_by_send() {
    model(|| {
        let (tx, rx) = unbounded::<usize>();
        let r = thread::spawn(move || block_on(async move { rx.recv().await }));
        tx.try_send(7).unwrap();
        assert_eq!(r.join().unwrap(), Ok(7));
    });
}

#[test]
fn async_recv_is_woken_by_close() {
    model(|| {
        let (tx, rx) = unbounded::<usize>();
        let r = thread::spawn(move || block_on(async move { rx.recv().await }));
        drop(tx);
        assert_eq!(r.join().unwrap(), Err(RecvError));
    });
}

#[test]
fn value_sent_before_close_is_never_lost() {
    model(|| {
        let (tx, rx) = unbounded::<usize>();
        let r = thread::spawn(move || {
            block_on(async move {
                let first = rx.recv().await;
                let second = rx.recv().await;
                (first, second)
            })
        });
        tx.try_send(1).unwrap();
        drop(tx);
        assert_eq!(r.join().unwrap(), (Ok(1), Err(RecvError)));
    });
}

#[test]
fn bounded_sender_is_woken_by_recv() {
    model(|| {
        let (tx, rx) = bounded::<usize>(1);
        tx.try_send(1).unwrap();
        let s = thread::spawn(move || block_on(async move { tx.send(2).await }));
        assert_eq!(block_on(async { rx.recv().await }), Ok(1));
        assert_eq!(s.join().unwrap(), Ok(()));
        assert_eq!(rx.try_recv(), Ok(2));
    });
}

/// A parked bounded sender must be woken by a pop that happens across a block
/// transition (the transition's index update must extend the release sequence the
/// sender started with its parking RMW).  With 3-slot blocks the third pop is the
/// transition.
#[test]
fn bounded_sender_is_woken_across_block_transition() {
    model(|| {
        let (tx, rx) = bounded::<usize>(1);
        // Fill and drain twice so the queue sits on the last slot of the first block.
        tx.try_send(1).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        tx.try_send(2).unwrap();
        assert_eq!(rx.try_recv(), Ok(2));
        tx.try_send(3).unwrap(); // occupies slot 2, the last of the block
        let s = thread::spawn(move || block_on(async move { tx.send(4).await }));
        assert_eq!(block_on(async { rx.recv().await }), Ok(3)); // transition pop
        assert_eq!(s.join().unwrap(), Ok(()));
        assert_eq!(block_on(async { rx.recv().await }), Ok(4));
    });
}
