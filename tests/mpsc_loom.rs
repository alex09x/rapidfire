#![cfg(feature = "loom")]

use loom::future::block_on;
use loom::thread;
use rapidfire::mpsc;
use rapidfire::RecvError;
use std::future::Future;
use std::task::Context;

fn model<F: Fn() + Sync + Send + 'static>(f: F) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(f);
}

/// A notified sender is cancelled after a batch frees capacity. Another
/// sender must receive the forwarded notification or observe the free slot.
#[test]
fn mpsc_loom_batch_capacity_release_cancellation() {
    model(|| {
        let (tx, mut rx) = mpsc::bounded(1);
        tx.try_send(0).unwrap();
        let waker = futures::task::noop_waker();
        let mut cancelled = Box::pin(tx.send(1));
        assert!(cancelled
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        let tx2 = tx.clone();
        let sender = thread::spawn(move || block_on(tx2.send(2)).unwrap());
        let mut got = Vec::new();
        assert_eq!(block_on(rx.recv_many(&mut got, 1)), Ok(1));
        assert_eq!(got, [0]);
        drop(cancelled);
        drop(tx);
        assert_eq!(block_on(rx.recv()), Ok(2));
        sender.join().unwrap();
        assert_eq!(block_on(rx.recv()), Err(RecvError));
    });
}

/// Model 2: Channel close racing with send and pending recv_many.
///
/// Exercises recv_many awaiting ready messages while a sender sends and/or closes
/// the channel, ensuring partial batches or clean close errors are observed
/// without hanging or corrupting the channel.
#[test]
fn mpsc_loom_pending_recv_many_close_race() {
    model(|| {
        let (tx, mut rx) = mpsc::bounded(2);
        let tx1 = tx.clone();
        let tx2 = tx;

        let h1 = thread::spawn(move || tx1.try_send(42));

        let h2 = thread::spawn(move || {
            tx2.close();
        });

        let mut got = Vec::new();
        let res = block_on(rx.recv_many(&mut got, 2));

        let sent = h1.join().unwrap().is_ok();
        h2.join().unwrap();

        match res {
            Ok(n) => {
                assert_eq!(n, 1);
                assert_eq!(got, vec![42]);
            }
            Err(RecvError) => {
                assert!(got.is_empty());
            }
        }
        while let Ok(value) = rx.try_recv() {
            got.push(value);
        }
        assert_eq!(got, if sent { vec![42] } else { vec![] });
        assert_eq!(block_on(rx.recv()), Err(RecvError));
    });
}
