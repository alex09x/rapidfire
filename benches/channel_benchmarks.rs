use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rapidfire::unbounded as fast_unbounded;

fn benchmark_spsc(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("spsc_1k");
    const ITEMS: usize = 1000;

    group.bench_function("1_async_channel", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, rx) = async_channel::unbounded::<usize>();
                for i in 0..ITEMS {
                    let _ = tx.try_send(black_box(i));
                }
                let mut sum = 0;
                for _ in 0..ITEMS {
                    sum += rx.recv().await.unwrap();
                }
                black_box(sum);
            });
        });
    });

    group.bench_function("2_flume", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, rx) = flume::unbounded::<usize>();
                for i in 0..ITEMS {
                    let _ = tx.send(black_box(i));
                }
                let mut sum = 0;
                for _ in 0..ITEMS {
                    sum += rx.recv_async().await.unwrap();
                }
                black_box(sum);
            });
        });
    });

    group.bench_function("3_rapidfire", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, rx) = fast_unbounded::<usize>();
                for i in 0..ITEMS {
                    let _ = tx.try_send(black_box(i));
                }
                let mut sum = 0;
                for _ in 0..ITEMS {
                    sum += rx.recv().await.unwrap();
                }
                black_box(sum);
            });
        });
    });

    group.bench_function("4_tokio_mpsc", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
                for i in 0..ITEMS {
                    // `send` is the non-blocking operation for unbounded tokio channels.
                    let _ = tx.send(black_box(i));
                }
                let mut sum = 0;
                for _ in 0..ITEMS {
                    sum += rx.recv().await.unwrap();
                }
                black_box(sum);
            });
        });
    });

    group.finish();
}

fn benchmark_mpsc(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("mpsc_4_producers_1_consumer_1k");
    const PRODUCERS: usize = 4;
    const ITEMS_PER_PRODUCER: usize = 250;
    const TOTAL: usize = PRODUCERS * ITEMS_PER_PRODUCER;

    group.bench_function("1_async_channel", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, rx) = async_channel::unbounded::<usize>();
                for p in 0..PRODUCERS {
                    let tx_c = tx.clone();
                    tokio::spawn(async move {
                        for i in 0..ITEMS_PER_PRODUCER {
                            let _ = tx_c.try_send(black_box(p * 1000 + i));
                        }
                    });
                }
                let mut count = 0;
                while let Ok(msg) = rx.recv().await {
                    black_box(msg);
                    count += 1;
                    if count == TOTAL {
                        break;
                    }
                }
            });
        });
    });

    group.bench_function("2_flume", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, rx) = flume::unbounded::<usize>();
                for p in 0..PRODUCERS {
                    let tx_c = tx.clone();
                    tokio::spawn(async move {
                        for i in 0..ITEMS_PER_PRODUCER {
                            let _ = tx_c.send(black_box(p * 1000 + i));
                        }
                    });
                }
                let mut count = 0;
                while let Ok(msg) = rx.recv_async().await {
                    black_box(msg);
                    count += 1;
                    if count == TOTAL {
                        break;
                    }
                }
            });
        });
    });

    group.bench_function("3_rapidfire", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, rx) = fast_unbounded::<usize>();
                for p in 0..PRODUCERS {
                    let tx_c = tx.clone();
                    tokio::spawn(async move {
                        for i in 0..ITEMS_PER_PRODUCER {
                            let _ = tx_c.try_send(black_box(p * 1000 + i));
                        }
                    });
                }
                let mut count = 0;
                while let Ok(msg) = rx.recv().await {
                    black_box(msg);
                    count += 1;
                    if count == TOTAL {
                        break;
                    }
                }
            });
        });
    });

    group.bench_function("4_tokio_mpsc", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
                for p in 0..PRODUCERS {
                    let tx_c = tx.clone();
                    tokio::spawn(async move {
                        for i in 0..ITEMS_PER_PRODUCER {
                            let _ = tx_c.send(black_box(p * 1000 + i));
                        }
                    });
                }
                let mut count = 0;
                while let Some(msg) = rx.recv().await {
                    black_box(msg);
                    count += 1;
                    if count == TOTAL {
                        break;
                    }
                }
            });
        });
    });

    group.finish();
}

fn benchmark_async_pingpong(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("async_pingpong");
    const ROUNDS: usize = 1000;
    group.throughput(Throughput::Elements(ROUNDS as u64));

    group.bench_function("1_async_channel", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx1, rx1) = async_channel::unbounded::<usize>();
                let (tx2, rx2) = async_channel::unbounded::<usize>();
                let echo = tokio::spawn(async move {
                    for _ in 0..ROUNDS {
                        let v = rx1.recv().await.unwrap();
                        let _ = tx2.send(v).await;
                    }
                });
                let mut sum = 0;
                for i in 0..ROUNDS {
                    let _ = tx1.send(black_box(i)).await;
                    sum += rx2.recv().await.unwrap();
                }
                echo.await.unwrap();
                black_box(sum);
            });
        });
    });

    group.bench_function("2_flume", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx1, rx1) = flume::unbounded::<usize>();
                let (tx2, rx2) = flume::unbounded::<usize>();
                let echo = tokio::spawn(async move {
                    for _ in 0..ROUNDS {
                        let v = rx1.recv_async().await.unwrap();
                        let _ = tx2.send_async(v).await;
                    }
                });
                let mut sum = 0;
                for i in 0..ROUNDS {
                    let _ = tx1.send_async(black_box(i)).await;
                    sum += rx2.recv_async().await.unwrap();
                }
                echo.await.unwrap();
                black_box(sum);
            });
        });
    });

    group.bench_function("3_rapidfire", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx1, rx1) = fast_unbounded::<usize>();
                let (tx2, rx2) = fast_unbounded::<usize>();
                let echo = tokio::spawn(async move {
                    for _ in 0..ROUNDS {
                        let v = rx1.recv().await.unwrap();
                        let _ = tx2.send(v).await;
                    }
                });
                let mut sum = 0;
                for i in 0..ROUNDS {
                    let _ = tx1.send(black_box(i)).await;
                    sum += rx2.recv().await.unwrap();
                }
                echo.await.unwrap();
                black_box(sum);
            });
        });
    });

    group.bench_function("4_tokio_mpsc", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<usize>();
                let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel::<usize>();
                let echo = tokio::spawn(async move {
                    for _ in 0..ROUNDS {
                        let v = rx1.recv().await.unwrap();
                        let _ = tx2.send(v);
                    }
                });
                let mut sum = 0;
                for i in 0..ROUNDS {
                    let _ = tx1.send(black_box(i));
                    sum += rx2.recv().await.unwrap();
                }
                echo.await.unwrap();
                black_box(sum);
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_spsc,
    benchmark_mpsc,
    benchmark_async_pingpong
);
criterion_main!(benches);
