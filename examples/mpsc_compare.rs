//! A/B comparison of the general receiver and an exclusive receiver.
use rapidfire::{mpsc, RecvError, Sender, TryRecvError};
use std::future::Future;
use std::hint::{black_box, spin_loop};
use std::sync::{Arc, Barrier};
use std::time::Instant;

trait Receive<T>: Send {
    fn try_recv(&mut self) -> Result<T, TryRecvError>;
    fn recv(&mut self) -> impl Future<Output = Result<T, RecvError>> + Send;
    fn many(
        &mut self,
        out: &mut Vec<T>,
        limit: usize,
    ) -> impl Future<Output = Result<usize, RecvError>> + Send;
}
impl<T: Send> Receive<T> for rapidfire::Receiver<T> {
    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        rapidfire::Receiver::try_recv(self)
    }
    fn recv(&mut self) -> impl Future<Output = Result<T, RecvError>> + Send {
        rapidfire::Receiver::recv(self)
    }
    async fn many(&mut self, out: &mut Vec<T>, limit: usize) -> Result<usize, RecvError> {
        out.push(self.recv().await?);
        let mut n = 1;
        while n < limit {
            match self.try_recv() {
                Ok(v) => {
                    out.push(v);
                    n += 1;
                }
                Err(_) => break,
            }
        }
        Ok(n)
    }
}
impl<T: Send> Receive<T> for mpsc::Receiver<T> {
    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        mpsc::Receiver::try_recv(self)
    }
    fn recv(&mut self) -> impl Future<Output = Result<T, RecvError>> + Send {
        mpsc::Receiver::recv(self)
    }
    fn many(
        &mut self,
        out: &mut Vec<T>,
        limit: usize,
    ) -> impl Future<Output = Result<usize, RecvError>> + Send {
        self.recv_many(out, limit)
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Msg<const PAD: usize> {
    seq: u64,
    stamp: u64,
    body: [u8; PAD],
}
impl<const PAD: usize> Msg<PAD> {
    fn new(seq: usize) -> Self {
        Self {
            seq: seq as u64,
            stamp: 0,
            body: [0; PAD],
        }
    }
}

fn pin_error(message: impl std::fmt::Display) -> ! {
    // A panic in one worker would leave other workers waiting at the start gate.
    eprintln!("invalid benchmark CPU placement: {message}");
    std::process::exit(2);
}

fn pin(slot: usize) {
    let cpus = std::env::var("MPSC_PIN").unwrap_or_default();
    if cpus.is_empty() {
        return;
    }
    let cpu: usize = cpus
        .split(',')
        .nth(slot)
        .unwrap_or_else(|| pin_error("pin list must cover every worker"))
        .parse()
        .unwrap_or_else(|_| pin_error("invalid CPU ID"));
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn sched_setaffinity(pid: i32, size: usize, mask: *const u64) -> i32;
        }
        let mut mask = [0u64; 16];
        if cpu >= 1024 {
            pin_error("CPU ID must be less than 1024");
        }
        mask[cpu / 64] |= 1 << (cpu % 64);
        if sched_setaffinity(0, 128, mask.as_ptr()) != 0 {
            pin_error(format!(
                "affinity failed: {}",
                std::io::Error::last_os_error()
            ));
        }
    }
    #[cfg(target_os = "macos")]
    unsafe {
        unsafe extern "C" {
            fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
        }
        let _ = cpu; // macOS has no CPU affinity API; request QoS only.
        let code = pthread_set_qos_class_self_np(0x21, 0);
        if code != 0 {
            pin_error(format!(
                "QoS request failed: {}",
                std::io::Error::from_raw_os_error(code)
            ));
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = cpu;
    }
}

fn sync_run<const PAD: usize, R: Receive<Msg<PAD>>>(
    tx: Sender<Msg<PAD>>,
    mut rx: R,
    producers: usize,
    n: usize,
) -> f64 {
    let each = n.div_ceil(producers);
    let actual = each * producers;
    let ready = Arc::new(Barrier::new(producers + 2));
    let start = Arc::new(Barrier::new(producers + 2));
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for p in 0..producers {
            let tx = tx.clone();
            let ready = ready.clone();
            let start = start.clone();
            handles.push(scope.spawn(move || {
                pin(p);
                ready.wait();
                start.wait();
                for i in 0..each {
                    let mut msg = Msg::new(p * each + i);
                    loop {
                        match tx.try_send(msg) {
                            Ok(()) => break,
                            Err(e) => {
                                msg = e.into_inner();
                                spin_loop();
                            }
                        }
                    }
                }
            }));
        }
        drop(tx);
        let ready_r = ready.clone();
        let start_r = start.clone();
        let consumer = scope.spawn(move || {
            pin(producers);
            ready_r.wait();
            start_r.wait();
            let mut sum = 0u64;
            for _ in 0..actual {
                let v = loop {
                    match rx.try_recv() {
                        Ok(v) => break v,
                        Err(TryRecvError::Empty) => spin_loop(),
                        Err(_) => panic!("closed early"),
                    }
                };
                sum = sum.wrapping_add(black_box(v).seq);
            }
            (Instant::now(), sum)
        });
        ready.wait();
        let clock = Instant::now();
        start.wait();
        let (end, sum) = consumer.join().unwrap();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(sum, (actual as u64) * (actual as u64 - 1) / 2);
        (end - clock).as_nanos() as f64 / actual as f64
    })
}

fn async_run<const PAD: usize, R: Receive<Msg<PAD>> + 'static>(
    tx: Sender<Msg<PAD>>,
    mut rx: R,
    producers: usize,
    n: usize,
    batch: usize,
) -> f64 {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let worker = AtomicUsize::new(0);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .on_thread_start(move || pin(worker.fetch_add(1, Ordering::Relaxed)))
        .build()
        .unwrap();
    let each = n.div_ceil(producers);
    let actual = each * producers;
    runtime.block_on(async move {
        let ready = Arc::new(tokio::sync::Barrier::new(producers + 2));
        let start = Arc::new(tokio::sync::Barrier::new(producers + 2));
        let mut handles = Vec::new();
        for p in 0..producers {
            let tx = tx.clone();
            let ready = ready.clone();
            let start = start.clone();
            handles.push(tokio::spawn(async move {
                ready.wait().await;
                start.wait().await;
                for i in 0..each {
                    tx.send(Msg::new(p * each + i)).await.unwrap();
                }
            }));
        }
        drop(tx);
        let ready_r = ready.clone();
        let start_r = start.clone();
        let consumer = tokio::spawn(async move {
            ready_r.wait().await;
            start_r.wait().await;
            let mut sum = 0u64;
            let mut count = 0;
            let mut values = Vec::with_capacity(batch);
            while count < actual {
                if batch == 1 {
                    sum += black_box(rx.recv().await.unwrap()).seq;
                    count += 1;
                } else {
                    let got = rx
                        .many(&mut values, batch.min(actual - count))
                        .await
                        .unwrap();
                    for v in values.drain(..) {
                        sum += black_box(v).seq;
                    }
                    count += got;
                }
            }
            (Instant::now(), sum)
        });
        ready.wait().await;
        let clock = Instant::now();
        start.wait().await;
        let (end, sum) = consumer.await.unwrap();
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(sum, (actual as u64) * (actual as u64 - 1) / 2);
        (end - clock).as_nanos() as f64 / actual as f64
    })
}

fn run<const PAD: usize>(n: usize, runs: usize, ps: &[usize], modes: &[&str]) {
    for &p in ps {
        for capacity in [0, 25, 4096] {
            for &mode in modes {
                for batch in [1, 32] {
                    if mode == "sync" && batch != 1 {
                        continue;
                    }
                    let mut results = [Vec::new(), Vec::new()];
                    for round in 0..runs + 1 {
                        for order in 0..2 {
                            let variant = (round + order) % 2;
                            let ns = if variant == 0 {
                                let (tx, rx) = if capacity == 0 {
                                    rapidfire::unbounded()
                                } else {
                                    rapidfire::bounded(capacity)
                                };
                                if mode == "sync" {
                                    sync_run::<PAD, _>(tx, rx, p, n)
                                } else {
                                    async_run::<PAD, _>(tx, rx, p, n, batch)
                                }
                            } else {
                                let (tx, rx) = if capacity == 0 {
                                    mpsc::unbounded()
                                } else {
                                    mpsc::bounded(capacity)
                                };
                                if mode == "sync" {
                                    sync_run::<PAD, _>(tx, rx, p, n)
                                } else {
                                    async_run::<PAD, _>(tx, rx, p, n, batch)
                                }
                            };
                            if round > 0 {
                                results[variant].push(ns);
                            }
                        }
                    }
                    for (variant, samples) in results.iter().enumerate() {
                        let mut sorted = samples.clone();
                        sorted.sort_by(f64::total_cmp);
                        println!("{{\"variant\":\"{}\",\"mode\":\"{}\",\"producers\":{},\"capacity\":{},\"bytes\":{},\"batch\":{},\"ns_per_message\":{},\"samples\":{:?}}}",
                if variant==0 {"mpmc"} else {"mpsc"},mode,p,capacity,std::mem::size_of::<Msg<PAD>>(),batch,sorted[sorted.len()/2],samples);
                    }
                }
            }
        }
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let n = args.get(1).map_or(100003, |s| s.parse().unwrap());
    let runs = args.get(2).map_or(3, |s| s.parse().unwrap());
    let ps: Vec<usize> = args
        .get(3)
        .map_or("4,8", String::as_str)
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    let modes: Vec<&str> = args
        .get(4)
        .map_or("sync,async", String::as_str)
        .split(',')
        .collect();
    let bytes: usize = args.get(5).map_or(64, |s| s.parse().unwrap());
    if modes == ["latency"] {
        latency_matrix(n, runs, &ps);
        return;
    }
    match bytes {
        64 => run::<48>(n, runs, &ps, &modes),
        256 => run::<240>(n, runs, &ps, &modes),
        1024 => run::<1008>(n, runs, &ps, &modes),
        _ => panic!("payload must be 64/256/1024"),
    }
}

// A separate paced workload. Latency is timestamp-before-send to consumer
// inspection, including capacity waits, runtime scheduling and measurement cost.
// Each producer offers bursts of 16 values at 1 ms intervals; it does not wait
// for a response. This is not a throughput measurement or a network benchmark.
fn latency_run<R: Receive<Msg<48>> + 'static>(
    tx: Sender<Msg<48>>,
    mut rx: R,
    producers: usize,
    n: usize,
    batch: usize,
) -> [u64; 3] {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let worker = AtomicUsize::new(0);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .on_thread_start(move || pin(worker.fetch_add(1, Ordering::Relaxed)))
        .build()
        .unwrap();
    let each = n.div_ceil(producers);
    let actual = each * producers;
    runtime.block_on(async move {
        let start = Arc::new(tokio::sync::Barrier::new(producers + 1));
        let epoch = Instant::now();
        let mut handles = Vec::new();
        for p in 0..producers {
            let tx = tx.clone();
            let start = start.clone();
            handles.push(tokio::spawn(async move {
                start.wait().await;
                let mut timer = tokio::time::interval(std::time::Duration::from_millis(1));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                for i in 0..each {
                    if i % 16 == 0 {
                        timer.tick().await;
                    }
                    let mut msg = Msg::new(p * each + i);
                    msg.stamp = epoch.elapsed().as_nanos() as u64;
                    tx.send(msg).await.unwrap();
                }
            }));
        }
        drop(tx);
        let consumer = tokio::spawn(async move {
            let mut delays = Vec::with_capacity(actual);
            let mut buffer = Vec::with_capacity(batch);
            let mut sum = 0;
            start.wait().await;
            while delays.len() < actual {
                if batch == 1 {
                    buffer.push(rx.recv().await.unwrap());
                } else {
                    rx.many(&mut buffer, batch.min(actual - delays.len()))
                        .await
                        .unwrap();
                }
                for msg in buffer.drain(..) {
                    delays.push((epoch.elapsed().as_nanos() as u64).saturating_sub(msg.stamp));
                    sum += black_box(msg).seq;
                }
            }
            assert_eq!(sum, (actual as u64) * (actual as u64 - 1) / 2);
            delays.sort_unstable();
            [
                delays[actual / 2],
                delays[(actual - 1) * 99 / 100],
                delays[actual - 1],
            ]
        });
        for h in handles {
            h.await.unwrap();
        }
        consumer.await.unwrap()
    })
}

fn latency_matrix(n: usize, runs: usize, ps: &[usize]) {
    for &p in ps {
        for capacity in [0, 25, 4096] {
            for batch in [1, 32] {
                let mut results = [Vec::new(), Vec::new()];
                for round in 0..runs + 1 {
                    for order in 0..2 {
                        let variant = (round + order) % 2;
                        let values = if variant == 0 {
                            let (tx, rx) = if capacity == 0 {
                                rapidfire::unbounded()
                            } else {
                                rapidfire::bounded(capacity)
                            };
                            latency_run(tx, rx, p, n, batch)
                        } else {
                            let (tx, rx) = if capacity == 0 {
                                mpsc::unbounded()
                            } else {
                                mpsc::bounded(capacity)
                            };
                            latency_run(tx, rx, p, n, batch)
                        };
                        if round > 0 {
                            results[variant].push(values);
                        }
                    }
                }
                for (variant, samples) in results.iter().enumerate() {
                    println!("{{\"variant\":\"{}\",\"mode\":\"latency\",\"producers\":{},\"capacity\":{},\"bytes\":64,\"batch\":{},\"samples_p50_p99_max_ns\":{:?}}}",
                if variant == 0 { "mpmc" } else { "mpsc" }, p, capacity, batch, samples);
                }
            }
        }
    }
}
