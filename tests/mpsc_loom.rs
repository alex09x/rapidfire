#![cfg(feature = "loom")]

use loom::future::block_on;
use loom::thread;
use rapidfire::mpsc;
use rapidfire::RecvError;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

fn model<F: Fn() + Sync + Send + 'static>(f: F) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(f);
}

/// A future that yields `Poll::Pending` once while waking itself,
/// then yields `Poll::Ready(())` on the second poll.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Model 1: Bounded batch capacity release with concurrent send cancellation.
///
/// Exercises pop_single_batch releasing capacity for waiting senders, where a
/// woken sender drops its send future (cancelling it) and forwards its wakeup
/// to another parked sender without deadlock, data loss, or capacity leak.
#[test]
fn mpsc_loom_batch_capacity_release_cancellation() {
    model(|| {
        let (tx, mut rx) = mpsc::bounded(1);
        tx.try_send(0).unwrap();

        let tx1 = tx.clone();
        let h1 = thread::spawn(move || {
            block_on(async {
                let send_fut = tx1.send(1);
                futures::pin_mut!(send_fut);
                let cancel_fut = YieldOnce(false);
                futures::pin_mut!(cancel_fut);
                match futures::future::select(send_fut, cancel_fut).await {
                    futures::future::Either::Left((res, _)) => {
                        assert!(res.is_ok());
                        true
                    }
                    futures::future::Either::Right(((), send_fut)) => {
                        drop(send_fut);
                        false
                    }
                }
            })
        });

        let tx2 = tx.clone();
        let h2 = thread::spawn(move || {
            block_on(tx2.send(2)).unwrap();
        });

        let mut got = Vec::new();
        assert_eq!(block_on(rx.recv_many(&mut got, 1)), Ok(1));
        assert_eq!(got[0], 0);

        drop(tx);
        while let Ok(value) = block_on(rx.recv()) {
            got.push(value);
        }
        let s1_completed = h1.join().unwrap();
        h2.join().unwrap();
        got.sort_unstable();

        if s1_completed {
            assert_eq!(got, vec![0, 1, 2]);
        } else {
            assert_eq!(got, vec![0, 2]);
        }
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
