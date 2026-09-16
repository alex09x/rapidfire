//! Quick hot-path microbenchmark used while tuning; the real harness lives in benches/.
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

fn ns(d: std::time::Duration, ops: usize) -> f64 {
    d.as_nanos() as f64 / ops as f64
}

/// macOS: ask for a performance core (QoS user-interactive); no-op elsewhere.
#[cfg(target_os = "macos")]
fn want_p_core() {
    extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }
    const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
    // SAFETY: plain libc call on the current thread.
    unsafe {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    }
}
#[cfg(not(target_os = "macos"))]
fn want_p_core() {}

trait Q: 'static {
    type Tx: Clone + Send + 'static;
    type Rx: Send + 'static;

    const MULTI_CONSUMER: bool;

    fn channel() -> (Self::Tx, Self::Rx);
    fn clone_rx(rx: &Self::Rx) -> Option<Self::Rx>;
    fn push(tx: &Self::Tx, v: u64);
    fn pop(rx: &mut Self::Rx) -> Option<u64>;
    fn dump(rx: &Self::Rx) -> String {
        let _ = rx;
        String::new()
    }
}

struct Fast;
impl Q for Fast {
    type Tx = rapidfire::Sender<u64>;
    type Rx = rapidfire::Receiver<u64>;

    const MULTI_CONSUMER: bool = true;

    fn channel() -> (Self::Tx, Self::Rx) {
        rapidfire::unbounded()
    }
    fn clone_rx(rx: &Self::Rx) -> Option<Self::Rx> {
        Some(rx.clone())
    }
    #[inline(always)]
    fn push(tx: &Self::Tx, v: u64) {
        tx.try_send(v).ok().unwrap();
    }
    #[inline(always)]
    fn pop(rx: &mut Self::Rx) -> Option<u64> {
        rx.try_recv().ok()
    }
    fn dump(rx: &Self::Rx) -> String {
        rx.__debug_dump()
    }
}

struct Seg;
impl Q for Seg {
    type Tx = Arc<crossbeam_queue::SegQueue<u64>>;
    type Rx = Arc<crossbeam_queue::SegQueue<u64>>;

    const MULTI_CONSUMER: bool = true;

    fn channel() -> (Self::Tx, Self::Rx) {
        let q = Arc::new(crossbeam_queue::SegQueue::new());
        (q.clone(), q)
    }
    fn clone_rx(rx: &Self::Rx) -> Option<Self::Rx> {
        Some(rx.clone())
    }
    #[inline(always)]
    fn push(tx: &Self::Tx, v: u64) {
        tx.push(v);
    }
    #[inline(always)]
    fn pop(rx: &mut Self::Rx) -> Option<u64> {
        rx.pop()
    }
}

struct Tokio;
impl Q for Tokio {
    type Tx = tokio::sync::mpsc::UnboundedSender<u64>;
    type Rx = tokio::sync::mpsc::UnboundedReceiver<u64>;

    const MULTI_CONSUMER: bool = false;

    fn channel() -> (Self::Tx, Self::Rx) {
        tokio::sync::mpsc::unbounded_channel()
    }
    fn clone_rx(_rx: &Self::Rx) -> Option<Self::Rx> {
        None
    }
    #[inline(always)]
    fn push(tx: &Self::Tx, v: u64) {
        tx.send(v).unwrap();
    }
    #[inline(always)]
    fn pop(rx: &mut Self::Rx) -> Option<u64> {
        rx.try_recv().ok()
    }
}

fn expected_sum(n: usize) -> u64 {
    let n = n as u128;
    (n * n.saturating_sub(1) / 2) as u64
}

fn st_push_pop<T: Q>(n: usize) -> f64 {
    let (tx, mut rx) = T::channel();
    let t = Instant::now();
    let mut s = 0u64;
    for i in 0..n as u64 {
        T::push(&tx, i);
        s = s.wrapping_add(T::pop(&mut rx).unwrap());
    }
    let elapsed = t.elapsed();
    assert_eq!(std::hint::black_box(s), expected_sum(n));
    ns(elapsed, n)
}

fn burst<T: Q>(n: usize, b: usize) -> f64 {
    let (tx, mut rx) = T::channel();
    let bursts = (n / b).max(1);
    let total = bursts * b;
    let t = Instant::now();
    let mut s = 0u64;
    let mut next = 0u64;
    for _ in 0..bursts {
        for _ in 0..b {
            T::push(&tx, next);
            next += 1;
        }
        for _ in 0..b {
            s = s.wrapping_add(T::pop(&mut rx).unwrap());
        }
    }
    let elapsed = t.elapsed();
    assert_eq!(std::hint::black_box(s), expected_sum(total));
    ns(elapsed, total)
}

fn mp_sc<T: Q>(n: usize, producers: usize, consumers: usize) -> f64 {
    if consumers > 1 && !T::MULTI_CONSUMER {
        panic!("mp_sc called with multiple consumers on a single-consumer channel");
    }
    let (tx, rx) = T::channel();
    let mut rxs = Vec::with_capacity(consumers);
    if consumers == 1 {
        rxs.push(rx);
    } else {
        for _ in 0..consumers {
            rxs.push(T::clone_rx(&rx).expect("clone_rx required for multi-consumer"));
        }
    }

    let per = (n / producers).max(1);
    let total = per * producers;
    let base_quota = total / consumers;
    let rem = total % consumers;

    // Wait until workers are ready, then time the start gate and full workload.
    let ready = Arc::new(Barrier::new(producers + consumers + 1));
    let start_gate = Arc::new(Barrier::new(producers + consumers + 1));

    let mut prod_hs = Vec::with_capacity(producers);
    for p in 0..producers {
        let tx_c = tx.clone();
        let ready_b = ready.clone();
        let start_b = start_gate.clone();
        prod_hs.push(thread::spawn(move || {
            want_p_core();
            ready_b.wait();
            start_b.wait();
            for i in 0..per as u64 {
                T::push(&tx_c, (p * per) as u64 + i);
            }
        }));
    }

    let mut cons_hs = Vec::with_capacity(consumers);
    for (c, mut rx_c) in rxs.into_iter().enumerate() {
        let ready_b = ready.clone();
        let start_b = start_gate.clone();
        let quota = base_quota + if c < rem { 1 } else { 0 };
        cons_hs.push(thread::spawn(move || {
            want_p_core();
            ready_b.wait();
            start_b.wait();
            let mut s = 0u64;
            let mut idle = 0u64;
            let mut last = Instant::now();
            for received in 0..quota {
                loop {
                    if let Some(v) = T::pop(&mut rx_c) {
                        s = s.wrapping_add(v);
                        idle = 0;
                        break;
                    }
                    idle += 1;
                    if idle.is_multiple_of(4096) && last.elapsed().as_secs() >= 3 {
                        eprintln!(
                            "STALL consumer={c} received={received}/{quota} : {}",
                            T::dump(&rx_c)
                        );
                        last = Instant::now();
                    }
                    std::hint::spin_loop();
                }
            }
            std::hint::black_box(s);
            s
        }));
    }

    ready.wait();
    let t = Instant::now();
    start_gate.wait();
    let mut total_sum = 0u64;
    for h in cons_hs {
        total_sum = total_sum.wrapping_add(h.join().unwrap());
    }
    let elapsed = t.elapsed();
    for h in prod_hs {
        h.join().unwrap();
    }
    assert_eq!(std::hint::black_box(total_sum), expected_sum(total));
    ns(elapsed, total)
}

fn pingpong<T: Q>(rounds: usize) -> f64 {
    let (tx_a, mut rx_a) = T::channel();
    let (tx_b, mut rx_b) = T::channel();
    // Wait until workers are ready, then time the start gate and full workload.
    let ready = Arc::new(Barrier::new(3));
    let start_gate = Arc::new(Barrier::new(3));

    let ready_e = ready.clone();
    let start_e = start_gate.clone();
    let echo = thread::spawn(move || {
        want_p_core();
        ready_e.wait();
        start_e.wait();
        for _ in 0..rounds {
            loop {
                if let Some(v) = T::pop(&mut rx_a) {
                    T::push(&tx_b, v);
                    break;
                }
                std::hint::spin_loop();
            }
        }
    });

    let ready_i = ready.clone();
    let start_i = start_gate.clone();
    let initiator = thread::spawn(move || {
        want_p_core();
        ready_i.wait();
        start_i.wait();
        let mut sum = 0u64;
        for i in 0..rounds as u64 {
            T::push(&tx_a, i);
            loop {
                if let Some(v) = T::pop(&mut rx_b) {
                    assert_eq!(v, i);
                    sum = sum.wrapping_add(v);
                    break;
                }
                std::hint::spin_loop();
            }
        }
        sum
    });

    ready.wait();
    let t = Instant::now();
    start_gate.wait();
    let sum = initiator.join().unwrap();
    let e = t.elapsed();
    echo.join().unwrap();
    std::hint::black_box(sum);
    ns(e, rounds)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn run<T: Q>(name: &str, n: usize, runs: usize, sc: &str) {
    let r = |f: &dyn Fn() -> f64| median((0..runs).map(|_| f()).collect());
    let want = |s: &str| sc.is_empty() || sc.split(',').any(|x| s.contains(x));
    if want("st_push_pop") {
        println!(
            "{:<8} st_push_pop {:>7.2} ns",
            name,
            r(&|| st_push_pop::<T>(n))
        );
    }
    if want("burst_1k") {
        println!(
            "{:<8} burst_1k    {:>7.2} ns",
            name,
            r(&|| burst::<T>(n, 1000))
        );
    }
    if want("spsc") {
        println!(
            "{:<8} spsc        {:>7.2} ns",
            name,
            r(&|| mp_sc::<T>(n, 1, 1))
        );
    }
    if want("mpsc_4p") {
        println!(
            "{:<8} mpsc_4p     {:>7.2} ns",
            name,
            r(&|| mp_sc::<T>(n, 4, 1))
        );
    }
    if want("mpmc_4p4c") {
        if T::MULTI_CONSUMER {
            println!(
                "{:<8} mpmc_4p4c   {:>7.2} ns",
                name,
                r(&|| mp_sc::<T>(n, 4, 4))
            );
        } else {
            println!("{:<8} mpmc_4p4c   n/a", name);
        }
    }
    if want("pingpong") {
        println!(
            "{:<8} pingpong    {:>7.2} ns/rt",
            name,
            r(&|| pingpong::<T>(n / 20))
        );
    }
}

fn main() {
    want_p_core();
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4_000_000);
    let runs: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let which = std::env::args().nth(3).unwrap_or_default();
    let sc = std::env::args().nth(4).unwrap_or_default();
    if which.is_empty() || which.contains("rapidfire") {
        run::<Fast>("rapidfire", n, runs, &sc);
    }
    if which.is_empty() || which.contains("seg") {
        run::<Seg>("segq", n, runs, &sc);
    }
    if which.is_empty() || which.contains("tokio") {
        run::<Tokio>("tokio", n, runs, &sc);
    }
}
