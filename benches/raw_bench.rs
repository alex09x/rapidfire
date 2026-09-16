//! Synchronous, thread-based cross-implementation channel benchmark harness.
//!
//! This target is compiled by the default libtest harness (`Cargo.toml` cannot be
//! extended with a `harness = false` entry), so the actual work is gated behind
//! environment variables and never runs during a normal `cargo test`:
//!
//! ```text
//! FCRS_RAW_BENCH=1      cargo test --release --bench raw_bench -- --nocapture
//! FCRS_RAW_BENCH_FULL=1 cargo test --release --bench raw_bench -- --nocapture
//! ```
//!
//! Extra options can be passed through `FCRS_RAW_BENCH_ARGS` (whitespace separated):
//!
//! ```text
//! --quick             reduce item count and run count by ~10x
//! --filter=<substr>   only run scenarios/impls whose name contains <substr>
//! --json              additionally print one JSON object per result line
//! --pin=<c0,c1,...>   Linux only: pin the k-th worker thread of a scenario to core c_k
//! --n=<items>         override the item count
//! ```

use crossbeam_queue::{ArrayQueue, SegQueue};
use std::hint::spin_loop;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

// ============================================================================
// CPU pinning
// ============================================================================

#[cfg(target_os = "linux")]
extern "C" {
    fn sched_setaffinity(pid: i32, cpusetsize: usize, mask: *const u64) -> i32;
}

/// `cpu_set_t` is 128 bytes on Linux.
#[cfg(target_os = "linux")]
const CPU_SET_WORDS: usize = 16;

/// Pins the calling thread to `core`. No-op on every non-Linux platform.
#[cfg(target_os = "linux")]
fn pin_to_core(core: usize) {
    let word = core / 64;
    if word >= CPU_SET_WORDS {
        return;
    }
    let mut mask = [0u64; CPU_SET_WORDS];
    mask[word] = 1u64 << (core % 64);
    // SAFETY: `mask` is a live, properly aligned [u64; 16] of exactly the size we
    // pass as `cpusetsize` (128 bytes = cpu_set_t); pid 0 means "the calling thread".
    // The kernel only reads `cpusetsize` bytes from the pointer.
    unsafe {
        sched_setaffinity(0, CPU_SET_WORDS * 8, mask.as_ptr());
    }
}

/// macOS has no affinity API; the closest thing is asking for a performance core
/// via the user-interactive QoS class.
#[cfg(target_os = "macos")]
fn pin_to_core(_core: usize) {
    extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }
    const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
    // SAFETY: plain libc call affecting only the calling thread.
    unsafe {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn pin_to_core(_core: usize) {}

/// Pins the calling thread if a core was configured for worker slot `k`.
fn pin_worker(pin: &[usize], k: usize) {
    if let Some(&core) = pin.get(k) {
        pin_to_core(core);
    }
}

// ============================================================================
// The common channel abstraction
// ============================================================================

/// Uniform façade over every benchmarked channel implementation so that all of
/// them are driven by exactly the same scenario code.
trait Chan: 'static {
    type Tx: Send + 'static;
    type Rx: Send + 'static;

    const NAME: &'static str;
    /// `false` when the receiving half cannot be shared by several consumers.
    const MULTI_CONSUMER: bool;

    /// `None` when the implementation has no unbounded flavour.
    fn unbounded() -> Option<(Self::Tx, Self::Rx)>;
    /// `None` when the implementation has no bounded flavour.
    fn bounded(cap: usize) -> Option<(Self::Tx, Self::Rx)>;
    fn clone_tx(tx: &Self::Tx) -> Self::Tx;
    /// `None` when the receiver is not cloneable.
    fn clone_rx(rx: &Self::Rx) -> Option<Self::Rx>;
    /// `Err` means full or closed; the caller spins.
    fn try_send(tx: &Self::Tx, v: u64) -> Result<(), u64>;
    fn try_recv(rx: &mut Self::Rx) -> Option<u64>;
}

// ---------------------------------------------------------------- rapidfire

struct Fast;

impl Chan for Fast {
    type Tx = rapidfire::Sender<u64>;
    type Rx = rapidfire::Receiver<u64>;

    const NAME: &'static str = "rapidfire";
    const MULTI_CONSUMER: bool = true;

    fn unbounded() -> Option<(Self::Tx, Self::Rx)> {
        Some(rapidfire::unbounded())
    }
    fn bounded(cap: usize) -> Option<(Self::Tx, Self::Rx)> {
        Some(rapidfire::bounded(cap))
    }
    fn clone_tx(tx: &Self::Tx) -> Self::Tx {
        tx.clone()
    }
    fn clone_rx(rx: &Self::Rx) -> Option<Self::Rx> {
        Some(rx.clone())
    }
    fn try_send(tx: &Self::Tx, v: u64) -> Result<(), u64> {
        tx.try_send(v).map_err(|e| e.into_inner())
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<u64> {
        rx.try_recv().ok()
    }
}

// ------------------------------------------------------------ crossbeam SegQueue

struct CrossbeamSeg;

impl Chan for CrossbeamSeg {
    type Tx = Arc<SegQueue<u64>>;
    type Rx = Arc<SegQueue<u64>>;

    const NAME: &'static str = "crossbeam_seg";
    const MULTI_CONSUMER: bool = true;

    fn unbounded() -> Option<(Self::Tx, Self::Rx)> {
        let q = Arc::new(SegQueue::new());
        Some((q.clone(), q))
    }
    fn bounded(_cap: usize) -> Option<(Self::Tx, Self::Rx)> {
        None
    }
    fn clone_tx(tx: &Self::Tx) -> Self::Tx {
        tx.clone()
    }
    fn clone_rx(rx: &Self::Rx) -> Option<Self::Rx> {
        Some(rx.clone())
    }
    fn try_send(tx: &Self::Tx, v: u64) -> Result<(), u64> {
        tx.push(v);
        Ok(())
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<u64> {
        rx.pop()
    }
}

// ---------------------------------------------------------- crossbeam ArrayQueue

struct CrossbeamArray;

impl Chan for CrossbeamArray {
    type Tx = Arc<ArrayQueue<u64>>;
    type Rx = Arc<ArrayQueue<u64>>;

    const NAME: &'static str = "crossbeam_array";
    const MULTI_CONSUMER: bool = true;

    fn unbounded() -> Option<(Self::Tx, Self::Rx)> {
        None
    }
    fn bounded(cap: usize) -> Option<(Self::Tx, Self::Rx)> {
        let q = Arc::new(ArrayQueue::new(cap));
        Some((q.clone(), q))
    }
    fn clone_tx(tx: &Self::Tx) -> Self::Tx {
        tx.clone()
    }
    fn clone_rx(rx: &Self::Rx) -> Option<Self::Rx> {
        Some(rx.clone())
    }
    fn try_send(tx: &Self::Tx, v: u64) -> Result<(), u64> {
        tx.push(v)
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<u64> {
        rx.pop()
    }
}

// ---------------------------------------------------------------- std::sync::mpsc

struct StdMpsc;

impl Chan for StdMpsc {
    type Tx = std::sync::mpsc::Sender<u64>;
    type Rx = std::sync::mpsc::Receiver<u64>;

    const NAME: &'static str = "std_mpsc";
    const MULTI_CONSUMER: bool = false;

    fn unbounded() -> Option<(Self::Tx, Self::Rx)> {
        Some(std::sync::mpsc::channel())
    }
    fn bounded(_cap: usize) -> Option<(Self::Tx, Self::Rx)> {
        // `sync_channel` yields a `SyncSender`, a different type than `Sender`.
        None
    }
    fn clone_tx(tx: &Self::Tx) -> Self::Tx {
        tx.clone()
    }
    fn clone_rx(_rx: &Self::Rx) -> Option<Self::Rx> {
        None
    }
    fn try_send(tx: &Self::Tx, v: u64) -> Result<(), u64> {
        tx.send(v).map_err(|e| e.0)
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<u64> {
        rx.try_recv().ok()
    }
}

// ------------------------------------------------------------------------- flume

struct Flume;

impl Chan for Flume {
    type Tx = flume::Sender<u64>;
    type Rx = flume::Receiver<u64>;

    const NAME: &'static str = "flume";
    const MULTI_CONSUMER: bool = true;

    fn unbounded() -> Option<(Self::Tx, Self::Rx)> {
        Some(flume::unbounded())
    }
    fn bounded(cap: usize) -> Option<(Self::Tx, Self::Rx)> {
        Some(flume::bounded(cap))
    }
    fn clone_tx(tx: &Self::Tx) -> Self::Tx {
        tx.clone()
    }
    fn clone_rx(rx: &Self::Rx) -> Option<Self::Rx> {
        Some(rx.clone())
    }
    fn try_send(tx: &Self::Tx, v: u64) -> Result<(), u64> {
        tx.try_send(v).map_err(|e| e.into_inner())
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<u64> {
        rx.try_recv().ok()
    }
}

// ----------------------------------------------------------------- async-channel

struct AsyncChannel;

impl Chan for AsyncChannel {
    type Tx = async_channel::Sender<u64>;
    type Rx = async_channel::Receiver<u64>;

    const NAME: &'static str = "async_channel";
    const MULTI_CONSUMER: bool = true;

    fn unbounded() -> Option<(Self::Tx, Self::Rx)> {
        Some(async_channel::unbounded())
    }
    fn bounded(cap: usize) -> Option<(Self::Tx, Self::Rx)> {
        Some(async_channel::bounded(cap))
    }
    fn clone_tx(tx: &Self::Tx) -> Self::Tx {
        tx.clone()
    }
    fn clone_rx(rx: &Self::Rx) -> Option<Self::Rx> {
        Some(rx.clone())
    }
    fn try_send(tx: &Self::Tx, v: u64) -> Result<(), u64> {
        tx.try_send(v).map_err(|e| e.into_inner())
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<u64> {
        rx.try_recv().ok()
    }
}

// -------------------------------------------------------------- tokio::sync::mpsc

enum TokioTx {
    Unbounded(tokio::sync::mpsc::UnboundedSender<u64>),
    Bounded(tokio::sync::mpsc::Sender<u64>),
}

enum TokioRx {
    Unbounded(tokio::sync::mpsc::UnboundedReceiver<u64>),
    Bounded(tokio::sync::mpsc::Receiver<u64>),
}

struct TokioMpsc;

impl Chan for TokioMpsc {
    type Tx = TokioTx;
    type Rx = TokioRx;

    const NAME: &'static str = "tokio_mpsc";
    const MULTI_CONSUMER: bool = false;

    fn unbounded() -> Option<(Self::Tx, Self::Rx)> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Some((TokioTx::Unbounded(tx), TokioRx::Unbounded(rx)))
    }
    fn bounded(cap: usize) -> Option<(Self::Tx, Self::Rx)> {
        let (tx, rx) = tokio::sync::mpsc::channel(cap);
        Some((TokioTx::Bounded(tx), TokioRx::Bounded(rx)))
    }
    fn clone_tx(tx: &Self::Tx) -> Self::Tx {
        match tx {
            TokioTx::Unbounded(t) => TokioTx::Unbounded(t.clone()),
            TokioTx::Bounded(t) => TokioTx::Bounded(t.clone()),
        }
    }
    fn clone_rx(_rx: &Self::Rx) -> Option<Self::Rx> {
        None
    }
    fn try_send(tx: &Self::Tx, v: u64) -> Result<(), u64> {
        match tx {
            // For the unbounded flavour `send` itself is the non-blocking op.
            TokioTx::Unbounded(t) => t.send(v).map_err(|e| e.0),
            TokioTx::Bounded(t) => t.try_send(v).map_err(|e| e.into_inner()),
        }
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<u64> {
        match rx {
            TokioRx::Unbounded(r) => r.try_recv().ok(),
            TokioRx::Bounded(r) => r.try_recv().ok(),
        }
    }
}

// ============================================================================
// Scenario helpers
// ============================================================================

/// Sum of `0..count`.
fn expected_sum(count: u64) -> u64 {
    if count == 0 {
        0
    } else {
        count * (count - 1) / 2
    }
}

fn check_sum(got: u64, want: u64) {
    assert_eq!(
        got, want,
        "checksum mismatch: received sum differs from sent"
    );
}

/// Spins until an item is available.
fn recv_spin<C: Chan>(rx: &mut C::Rx) -> u64 {
    loop {
        if let Some(v) = C::try_recv(rx) {
            return v;
        }
        spin_loop();
    }
}

/// Spins until the item could be pushed.
fn send_spin<C: Chan>(tx: &C::Tx, v: u64) {
    let mut v = v;
    while let Err(back) = C::try_send(tx, v) {
        v = back;
        spin_loop();
    }
}

fn ns_per_op(elapsed_ns: f64, ops: usize) -> Option<f64> {
    Some(elapsed_ns / ops as f64)
}

// ============================================================================
// Scenarios
// ============================================================================

fn sc_st_push_pop<C: Chan>(n: usize, _pin: &[usize]) -> Option<f64> {
    let (tx, mut rx) = C::unbounded()?;
    let mut sum = 0u64;
    let start = Instant::now();
    for i in 0..n as u64 {
        send_spin::<C>(&tx, i);
        sum += recv_spin::<C>(&mut rx);
    }
    let elapsed = start.elapsed().as_nanos() as f64;
    check_sum(sum, expected_sum(n as u64));
    ns_per_op(elapsed, n)
}

fn sc_st_burst_1k<C: Chan>(n: usize, _pin: &[usize]) -> Option<f64> {
    const BURST: usize = 1000;
    let (tx, mut rx) = C::unbounded()?;
    let bursts = (n / BURST).max(1);
    let total = bursts * BURST;
    let mut next = 0u64;
    let mut sum = 0u64;
    let start = Instant::now();
    for _ in 0..bursts {
        for _ in 0..BURST {
            send_spin::<C>(&tx, next);
            next += 1;
        }
        for _ in 0..BURST {
            sum += recv_spin::<C>(&mut rx);
        }
    }
    let elapsed = start.elapsed().as_nanos() as f64;
    check_sum(sum, expected_sum(total as u64));
    ns_per_op(elapsed, total)
}

fn spsc_inner<C: Chan>(n: usize, pin: &[usize], cap: Option<usize>) -> Option<f64> {
    let (tx, mut rx) = match cap {
        Some(c) => C::bounded(c)?,
        None => C::unbounded()?,
    };
    // Wait until workers are ready, then time the start gate and full workload.
    let ready = Arc::new(Barrier::new(3));
    let start_gate = Arc::new(Barrier::new(3));

    let ready_p = ready.clone();
    let start_p = start_gate.clone();
    let pin_p: Vec<usize> = pin.to_vec();
    let producer = thread::spawn(move || {
        pin_worker(&pin_p, 0);
        ready_p.wait();
        start_p.wait();
        for i in 0..n as u64 {
            send_spin::<C>(&tx, i);
        }
    });

    let ready_c = ready.clone();
    let start_c = start_gate.clone();
    let pin_c: Vec<usize> = pin.to_vec();
    let consumer = thread::spawn(move || {
        pin_worker(&pin_c, 1);
        ready_c.wait();
        start_c.wait();
        let mut sum = 0u64;
        for _ in 0..n {
            sum += recv_spin::<C>(&mut rx);
        }
        sum
    });

    ready.wait();
    let start = Instant::now();
    start_gate.wait();
    let sum = consumer.join().expect("consumer thread panicked");
    let elapsed = start.elapsed().as_nanos() as f64;
    producer.join().expect("producer thread panicked");
    check_sum(sum, expected_sum(n as u64));
    ns_per_op(elapsed, n)
}

fn sc_spsc<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    spsc_inner::<C>(n, pin, None)
}

fn sc_bounded_spsc_1024<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    spsc_inner::<C>(n, pin, Some(1024))
}

fn mpsc_inner<C: Chan>(
    n: usize,
    pin: &[usize],
    producers: usize,
    cap: Option<usize>,
) -> Option<f64> {
    let (tx, mut rx) = match cap {
        None => C::unbounded()?,
        Some(c) => C::bounded(c)?,
    };
    let per = (n / producers).max(1);
    let total = per * producers;
    // Wait until workers are ready, then time the start gate and full workload.
    let ready = Arc::new(Barrier::new(producers + 2));
    let start_gate = Arc::new(Barrier::new(producers + 2));

    let mut prod_handles = Vec::with_capacity(producers);
    for p in 0..producers {
        let tx_c = C::clone_tx(&tx);
        let ready_p = ready.clone();
        let start_p = start_gate.clone();
        let pin_p: Vec<usize> = pin.to_vec();
        prod_handles.push(thread::spawn(move || {
            pin_worker(&pin_p, p);
            ready_p.wait();
            start_p.wait();
            let base = (p * per) as u64;
            for j in 0..per as u64 {
                send_spin::<C>(&tx_c, base + j);
            }
        }));
    }

    let ready_c = ready.clone();
    let start_c = start_gate.clone();
    let pin_c: Vec<usize> = pin.to_vec();
    let consumer = thread::spawn(move || {
        pin_worker(&pin_c, producers);
        ready_c.wait();
        start_c.wait();
        let mut sum = 0u64;
        for _ in 0..total {
            sum += recv_spin::<C>(&mut rx);
        }
        sum
    });

    ready.wait();
    let start = Instant::now();
    start_gate.wait();
    let sum = consumer.join().expect("consumer thread panicked");
    let elapsed = start.elapsed().as_nanos() as f64;
    for h in prod_handles {
        h.join().expect("producer thread panicked");
    }
    check_sum(sum, expected_sum(total as u64));
    ns_per_op(elapsed, total)
}

fn sc_mpsc_4p<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    mpsc_inner::<C>(n, pin, 4, None)
}

fn sc_bounded_mpsc_4p_1024<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    mpsc_inner::<C>(n, pin, 4, Some(1024))
}

fn sc_mpsc_8p<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    mpsc_inner::<C>(n, pin, 8, None)
}

fn sc_bounded_mpsc_8p_1024<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    mpsc_inner::<C>(n, pin, 8, Some(1024))
}

/// Dozens of WebSocket readers feeding one writer (market-data collectors run 10–40 per exchange).
fn sc_mpsc_32p<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    mpsc_inner::<C>(n, pin, 32, None)
}

fn sc_bounded_mpsc_32p_1024<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    mpsc_inner::<C>(n, pin, 32, Some(1024))
}

fn sc_mpmc_4p4c<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    mpmc_inner::<C>(n, pin, None)
}

fn sc_bounded_mpmc_4p4c_1024<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    mpmc_inner::<C>(n, pin, Some(1024))
}

fn mpmc_inner<C: Chan>(n: usize, pin: &[usize], cap: Option<usize>) -> Option<f64> {
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    if !C::MULTI_CONSUMER {
        return None;
    }
    let (tx, rx) = match cap {
        None => C::unbounded()?,
        Some(c) => C::bounded(c)?,
    };

    // All receiver handles are materialised before any thread is spawned so that
    // a non-cloneable receiver simply reports "not supported".
    let mut rxs = Vec::with_capacity(CONSUMERS);
    for _ in 0..CONSUMERS {
        rxs.push(C::clone_rx(&rx)?);
    }
    drop(rx);

    let per = (n / PRODUCERS).max(1);
    let total = per * PRODUCERS;

    // Local completion accounting: each consumer has a fixed quota summing to the exact total.
    // Handles totals not divisible by CONSUMERS by giving +1 to the first remainder consumers.
    // No shared AtomicUsize RMW is performed per message.
    let base_quota = total / CONSUMERS;
    let rem = total % CONSUMERS;

    // Wait until workers are ready, then time the start gate and full workload.
    let ready = Arc::new(Barrier::new(PRODUCERS + CONSUMERS + 1));
    let start_gate = Arc::new(Barrier::new(PRODUCERS + CONSUMERS + 1));

    let mut prod_handles = Vec::with_capacity(PRODUCERS);
    for p in 0..PRODUCERS {
        let tx_c = C::clone_tx(&tx);
        let ready_p = ready.clone();
        let start_p = start_gate.clone();
        let pin_p: Vec<usize> = pin.to_vec();
        prod_handles.push(thread::spawn(move || {
            pin_worker(&pin_p, p);
            ready_p.wait();
            start_p.wait();
            let base = (p * per) as u64;
            for j in 0..per as u64 {
                send_spin::<C>(&tx_c, base + j);
            }
        }));
    }

    let mut cons_handles = Vec::with_capacity(CONSUMERS);
    for (k, mut rx_c) in rxs.into_iter().enumerate() {
        let ready_c = ready.clone();
        let start_c = start_gate.clone();
        let pin_c: Vec<usize> = pin.to_vec();
        let my_quota = base_quota + if k < rem { 1 } else { 0 };
        cons_handles.push(thread::spawn(move || {
            pin_worker(&pin_c, PRODUCERS + k);
            ready_c.wait();
            start_c.wait();
            let mut sum = 0u64;
            for _ in 0..my_quota {
                sum += recv_spin::<C>(&mut rx_c);
            }
            sum
        }));
    }

    ready.wait();
    let start = Instant::now();
    start_gate.wait();
    let mut sum = 0u64;
    for h in cons_handles {
        sum += h.join().expect("consumer thread panicked");
    }
    let elapsed = start.elapsed().as_nanos() as f64;
    for h in prod_handles {
        h.join().expect("producer thread panicked");
    }
    check_sum(sum, expected_sum(total as u64));
    ns_per_op(elapsed, total)
}

fn sc_pingpong<C: Chan>(n: usize, pin: &[usize]) -> Option<f64> {
    let rounds = (n / 20).max(1);
    let (tx_a, mut rx_a) = C::unbounded()?;
    let (tx_b, mut rx_b) = C::unbounded()?;
    // Wait until workers are ready, then time the start gate and full workload.
    let ready = Arc::new(Barrier::new(3));
    let start_gate = Arc::new(Barrier::new(3));

    let ready_e = ready.clone();
    let start_e = start_gate.clone();
    let pin_e: Vec<usize> = pin.to_vec();
    let echo = thread::spawn(move || {
        pin_worker(&pin_e, 1);
        ready_e.wait();
        start_e.wait();
        for _ in 0..rounds {
            let v = recv_spin::<C>(&mut rx_a);
            send_spin::<C>(&tx_b, v);
        }
    });

    let ready_i = ready.clone();
    let start_i = start_gate.clone();
    let pin_i: Vec<usize> = pin.to_vec();
    let initiator = thread::spawn(move || {
        pin_worker(&pin_i, 0);
        ready_i.wait();
        start_i.wait();
        let mut sum = 0u64;
        for i in 0..rounds as u64 {
            send_spin::<C>(&tx_a, i);
            sum += recv_spin::<C>(&mut rx_b);
        }
        sum
    });

    ready.wait();
    let start = Instant::now();
    start_gate.wait();
    let sum = initiator.join().expect("initiator thread panicked");
    let elapsed = start.elapsed().as_nanos() as f64;
    echo.join().expect("echo thread panicked");
    check_sum(sum, expected_sum(rounds as u64));
    ns_per_op(elapsed, rounds)
}

// ============================================================================
// Driver
// ============================================================================

struct Cfg {
    n: usize,
    runs: usize,
    quick: bool,
    filter: Option<String>,
    json: bool,
    pin: Vec<usize>,
}

impl Cfg {
    fn selected(&self, scenario: &str, imp: &str) -> bool {
        match &self.filter {
            Some(f) => scenario.contains(f.as_str()) || imp.contains(f.as_str()),
            None => true,
        }
    }
}

fn parse_cfg(args: &[String]) -> Cfg {
    let mut all: Vec<String> = args.to_vec();
    if let Ok(extra) = std::env::var("FCRS_RAW_BENCH_ARGS") {
        all.extend(extra.split_whitespace().map(str::to_string));
    }

    let mut cfg = Cfg {
        n: 4_000_000,
        runs: 5,
        quick: false,
        filter: None,
        json: false,
        pin: Vec::new(),
    };
    let mut n_override: Option<usize> = None;

    for arg in &all {
        if arg == "--quick" {
            cfg.quick = true;
        } else if arg == "--json" {
            cfg.json = true;
        } else if let Some(v) = arg.strip_prefix("--filter=") {
            cfg.filter = Some(v.to_string());
        } else if let Some(v) = arg.strip_prefix("--pin=") {
            cfg.pin = v
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .filter_map(|s| s.parse::<usize>().ok())
                .collect();
        } else if let Some(v) = arg.strip_prefix("--n=") {
            n_override = v.parse::<usize>().ok();
        }
    }

    if cfg.quick {
        cfg.n = 400_000;
        cfg.runs = 2;
    }
    if let Some(n) = n_override {
        cfg.n = n.max(1000);
    }
    cfg
}

struct Row {
    order: usize,
    scenario: &'static str,
    imp: &'static str,
    ns: Option<f64>,
    runs: usize,
}

type ScenarioFn = fn(usize, &[usize]) -> Option<f64>;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).expect("timings are never NaN"));
    let mid = v.len() / 2;
    if v.len().is_multiple_of(2) {
        (v[mid - 1] + v[mid]) / 2.0
    } else {
        v[mid]
    }
}

fn measure(
    cfg: &Cfg,
    f: ScenarioFn,
    order: usize,
    scenario: &'static str,
    imp: &'static str,
) -> Row {
    // One warmup run, also used to detect "not supported".
    if f(cfg.n, &cfg.pin).is_none() {
        return Row {
            order,
            scenario,
            imp,
            ns: None,
            runs: 0,
        };
    }
    let mut samples = Vec::with_capacity(cfg.runs);
    for _ in 0..cfg.runs {
        if let Some(ns) = f(cfg.n, &cfg.pin) {
            samples.push(ns);
        }
    }
    if samples.is_empty() {
        return Row {
            order,
            scenario,
            imp,
            ns: None,
            runs: 0,
        };
    }
    let runs = samples.len();
    Row {
        order,
        scenario,
        imp,
        ns: Some(median(samples)),
        runs,
    }
}

fn bench_impl<C: Chan>(cfg: &Cfg, out: &mut Vec<Row>) {
    let scenarios: [(&'static str, ScenarioFn); 13] = [
        ("st_push_pop", sc_st_push_pop::<C>),
        ("st_burst_1k", sc_st_burst_1k::<C>),
        ("spsc", sc_spsc::<C>),
        ("mpsc_4p", sc_mpsc_4p::<C>),
        ("mpsc_8p", sc_mpsc_8p::<C>),
        ("mpmc_4p4c", sc_mpmc_4p4c::<C>),
        ("pingpong", sc_pingpong::<C>),
        ("bounded_spsc_1024", sc_bounded_spsc_1024::<C>),
        ("bounded_mpsc_4p_1024", sc_bounded_mpsc_4p_1024::<C>),
        ("bounded_mpsc_8p_1024", sc_bounded_mpsc_8p_1024::<C>),
        ("bounded_mpmc_4p4c_1024", sc_bounded_mpmc_4p4c_1024::<C>),
        ("mpsc_32p", sc_mpsc_32p::<C>),
        ("bounded_mpsc_32p_1024", sc_bounded_mpsc_32p_1024::<C>),
    ];
    for (order, (name, f)) in scenarios.into_iter().enumerate() {
        if !cfg.selected(name, C::NAME) {
            continue;
        }
        out.push(measure(cfg, f, order, name, C::NAME));
    }
}

fn print_table(cfg: &Cfg, rows: &[Row]) {
    println!(
        "\nrapidfire raw bench | n={} runs={} (median of {} + 1 warmup){} | pin={}",
        cfg.n,
        cfg.runs,
        cfg.runs,
        if cfg.quick { " quick" } else { "" },
        if cfg.pin.is_empty() {
            "none".to_string()
        } else {
            cfg.pin
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(",")
        }
    );
    println!(
        "{:<18} | {:<15} | {:>12} | {:>10} | {:>4}",
        "scenario", "impl", "ns/op", "Mops/s", "runs"
    );
    println!(
        "{:-<18}-+-{:-<15}-+-{:->12}-+-{:->10}-+-{:->4}",
        "", "", "", "", ""
    );
    let mut prev: Option<&str> = None;
    for r in rows {
        if let Some(p) = prev {
            if p != r.scenario {
                println!(
                    "{:-<18}-+-{:-<15}-+-{:->12}-+-{:->10}-+-{:->4}",
                    "", "", "", "", ""
                );
            }
        }
        prev = Some(r.scenario);
        match r.ns {
            Some(ns) => println!(
                "{:<18} | {:<15} | {:>12.2} | {:>10.2} | {:>4}",
                r.scenario,
                r.imp,
                ns,
                1000.0 / ns,
                r.runs
            ),
            None => println!(
                "{:<18} | {:<15} | {:>12} | {:>10} | {:>4}",
                r.scenario, r.imp, "n/a", "n/a", "-"
            ),
        }
    }
    println!();
}

fn print_json(rows: &[Row]) {
    for r in rows {
        match r.ns {
            Some(ns) => println!(
                "{{\"scenario\":\"{}\",\"impl\":\"{}\",\"ns_per_op\":{:.3},\"mops\":{:.3},\"runs\":{}}}",
                r.scenario,
                r.imp,
                ns,
                1000.0 / ns,
                r.runs
            ),
            None => println!(
                "{{\"scenario\":\"{}\",\"impl\":\"{}\",\"ns_per_op\":null,\"mops\":null,\"runs\":{}}}",
                r.scenario, r.imp, r.runs
            ),
        }
    }
}

/// Runs the whole raw benchmark suite.
pub fn run(args: &[String]) {
    let cfg = parse_cfg(args);
    let mut rows: Vec<Row> = Vec::new();

    bench_impl::<Fast>(&cfg, &mut rows);
    bench_impl::<CrossbeamSeg>(&cfg, &mut rows);
    bench_impl::<CrossbeamArray>(&cfg, &mut rows);
    bench_impl::<StdMpsc>(&cfg, &mut rows);
    bench_impl::<Flume>(&cfg, &mut rows);
    bench_impl::<AsyncChannel>(&cfg, &mut rows);
    bench_impl::<TokioMpsc>(&cfg, &mut rows);

    // Stable sort keeps the implementation order inside each scenario block.
    rows.sort_by_key(|r| r.order);

    print_table(&cfg, &rows);
    if cfg.json {
        print_json(&rows);
    }
}

#[test]
fn raw_bench_quick() {
    if std::env::var("FCRS_RAW_BENCH").is_ok() {
        run(&["--quick".to_string()]);
    }
}

#[test]
fn raw_bench_full() {
    if std::env::var("FCRS_RAW_BENCH_FULL").is_ok() {
        run(&[]);
    }
}
