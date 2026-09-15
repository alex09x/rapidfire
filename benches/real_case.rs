#![allow(clippy::manual_async_fn)]
//! Trading-bot shaped cross-implementation channel benchmark.
//!
//! Where `raw_bench.rs` measures the abstract micro-topologies with a `u64`
//! payload, this target measures the shapes real trading bots actually run: a
//! WebSocket reader task feeding a bot loop, a couple of auxiliary senders
//! feeding the same loop, and a command/response round trip against an executor
//! task. The payload is a `#[repr(C)]` struct of 64 / 256 / 1024 bytes, i.e. a
//! parsed market-data message rather than a machine word.
//!
//! Every topology moves `n` messages one way and reports ns per message, except
//! `request_response`, which is a latency and runs `n / 10` round trips and
//! reports ns per round trip.
//!
//! Like `raw_bench.rs` this target is compiled by the default libtest harness,
//! so the work is gated behind an environment variable and never runs during a
//! normal `cargo test`:
//!
//! ```text
//! FCRS_REAL_BENCH=1 cargo test --release --bench real_case -- --nocapture
//! ```
//!
//! Extra options can be passed through `FCRS_REAL_BENCH_ARGS` (whitespace
//! separated):
//!
//! ```text
//! --quick             200_000 messages and 2 runs instead of 2_000_000 and 5
//! --filter=<substr>   only run rows whose topology / mode / impl contains <substr>
//! --json              additionally print one JSON object per result line
//! --pin=<c0,c1,...>   Linux only: pin the k-th worker of a scenario to core c_k
//! --n=<messages>      override the message count
//! ```

use std::future::Future;
use std::hint::spin_loop;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

/// Worker threads of the tokio runtime used by every `async` row.
const ASYNC_WORKERS: usize = 4;

/// Capacity the bot's command/response channels are configured with.
const CAP_BOT: usize = 4096;

/// Capacity of the FIX/CTS style streams: tiny, so the producer is throttled by
/// the consumer almost all of the time.
const CAP_STREAM: usize = 25;

/// `request_response` measures a latency, not a throughput, so it runs `n` over
/// this divisor round trips instead of `n` one-way messages. Each round trip
/// costs two hand-offs plus two full wake-ups, which is one to two orders of
/// magnitude more than a pipelined message.
const ROUNDTRIP_DIV: usize = 10;

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
// Payload
// ============================================================================

/// What every scenario needs from the message it moves: a sequence number for
/// the consumer-side checksum and a known wire size.
trait Payload: Copy + Send + 'static {
    /// `size_of::<Self>()`, reported in the table.
    const BYTES: usize;
    fn new(seq: u64) -> Self;
    fn seq(&self) -> u64;
}

/// A parsed market-data message: a sequence number plus `PAD` bytes of body.
#[repr(C)]
#[derive(Clone, Copy)]
struct Msg<const PAD: usize> {
    seq: u64,
    body: [u8; PAD],
}

impl<const PAD: usize> Payload for Msg<PAD> {
    const BYTES: usize = std::mem::size_of::<Self>();

    fn new(seq: u64) -> Self {
        Msg {
            seq,
            body: [0u8; PAD],
        }
    }

    fn seq(&self) -> u64 {
        // The first body byte is folded in so the consumer really touches the
        // payload instead of only its header. It is always zero, so the
        // checksum is exactly the sequence number.
        self.seq ^ self.body[0] as u64
    }
}

type Msg64 = Msg<56>;
type Msg256 = Msg<248>;
type Msg1024 = Msg<1016>;

// ============================================================================
// The common channel abstraction
// ============================================================================

/// Uniform façade over every benchmarked channel implementation, in both the
/// blocking (`try_*` plus a spin) and the async flavour, so that all of them are
/// driven by exactly the same scenario code.
///
/// Every topology here has a single consumer per channel, so the receiving half
/// is owned by one worker and taken by `&mut` — that is what `tokio::sync::mpsc`
/// needs and what the others are happy with.
trait Chan<T: Payload>: 'static {
    type Tx: Clone + Send + Sync + 'static;
    type Rx: Send + 'static;

    const NAME: &'static str;

    /// `None` capacity means the unbounded flavour.
    fn channel(cap: Option<usize>) -> (Self::Tx, Self::Rx);

    /// `Err` gives the message back: full or closed; the caller spins.
    fn try_send(tx: &Self::Tx, v: T) -> Result<(), T>;
    fn try_recv(rx: &mut Self::Rx) -> Option<T>;

    fn send_async<'a>(tx: &'a Self::Tx, v: T) -> impl Future<Output = ()> + Send + 'a;
    fn recv_async<'a>(rx: &'a mut Self::Rx) -> impl Future<Output = Option<T>> + Send + 'a;
}

// ---------------------------------------------------------------- rapidfire

struct Fast;

impl<T: Payload> Chan<T> for Fast {
    type Tx = rapidfire::Sender<T>;
    type Rx = rapidfire::Receiver<T>;

    const NAME: &'static str = "rapidfire";

    fn channel(cap: Option<usize>) -> (Self::Tx, Self::Rx) {
        match cap {
            Some(c) => rapidfire::bounded(c),
            None => rapidfire::unbounded(),
        }
    }
    fn try_send(tx: &Self::Tx, v: T) -> Result<(), T> {
        tx.try_send(v).map_err(|e| e.into_inner())
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<T> {
        rx.try_recv().ok()
    }
    fn send_async<'a>(tx: &'a Self::Tx, v: T) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let _ = tx.send(v).await;
        }
    }
    fn recv_async<'a>(rx: &'a mut Self::Rx) -> impl Future<Output = Option<T>> + Send + 'a {
        async move { rx.recv().await.ok() }
    }
}

// ----------------------------------------------------------------- async-channel

struct AsyncCh;

impl<T: Payload> Chan<T> for AsyncCh {
    type Tx = async_channel::Sender<T>;
    type Rx = async_channel::Receiver<T>;

    const NAME: &'static str = "async_channel";

    fn channel(cap: Option<usize>) -> (Self::Tx, Self::Rx) {
        match cap {
            Some(c) => async_channel::bounded(c),
            None => async_channel::unbounded(),
        }
    }
    fn try_send(tx: &Self::Tx, v: T) -> Result<(), T> {
        tx.try_send(v).map_err(|e| e.into_inner())
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<T> {
        rx.try_recv().ok()
    }
    fn send_async<'a>(tx: &'a Self::Tx, v: T) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let _ = tx.send(v).await;
        }
    }
    fn recv_async<'a>(rx: &'a mut Self::Rx) -> impl Future<Output = Option<T>> + Send + 'a {
        async move { rx.recv().await.ok() }
    }
}

// ------------------------------------------------------------------------- flume

struct FlumeCh;

impl<T: Payload> Chan<T> for FlumeCh {
    type Tx = flume::Sender<T>;
    type Rx = flume::Receiver<T>;

    const NAME: &'static str = "flume";

    fn channel(cap: Option<usize>) -> (Self::Tx, Self::Rx) {
        match cap {
            Some(c) => flume::bounded(c),
            None => flume::unbounded(),
        }
    }
    fn try_send(tx: &Self::Tx, v: T) -> Result<(), T> {
        tx.try_send(v).map_err(|e| e.into_inner())
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<T> {
        rx.try_recv().ok()
    }
    fn send_async<'a>(tx: &'a Self::Tx, v: T) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let _ = tx.send_async(v).await;
        }
    }
    fn recv_async<'a>(rx: &'a mut Self::Rx) -> impl Future<Output = Option<T>> + Send + 'a {
        async move { rx.recv_async().await.ok() }
    }
}

// -------------------------------------------------------------- tokio::sync::mpsc

enum TokioTx<T> {
    Bounded(tokio::sync::mpsc::Sender<T>),
    Unbounded(tokio::sync::mpsc::UnboundedSender<T>),
}

// A manual impl: `derive(Clone)` would demand `T: Clone`, which the channel does
// not need.
impl<T> Clone for TokioTx<T> {
    fn clone(&self) -> Self {
        match self {
            TokioTx::Bounded(t) => TokioTx::Bounded(t.clone()),
            TokioTx::Unbounded(t) => TokioTx::Unbounded(t.clone()),
        }
    }
}

enum TokioRx<T> {
    Bounded(tokio::sync::mpsc::Receiver<T>),
    Unbounded(tokio::sync::mpsc::UnboundedReceiver<T>),
}

struct TokioMpsc;

impl<T: Payload> Chan<T> for TokioMpsc {
    type Tx = TokioTx<T>;
    type Rx = TokioRx<T>;

    const NAME: &'static str = "tokio_mpsc";

    fn channel(cap: Option<usize>) -> (Self::Tx, Self::Rx) {
        match cap {
            Some(c) => {
                let (tx, rx) = tokio::sync::mpsc::channel(c);
                (TokioTx::Bounded(tx), TokioRx::Bounded(rx))
            }
            None => {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                (TokioTx::Unbounded(tx), TokioRx::Unbounded(rx))
            }
        }
    }
    fn try_send(tx: &Self::Tx, v: T) -> Result<(), T> {
        match tx {
            TokioTx::Bounded(t) => t.try_send(v).map_err(|e| e.into_inner()),
            // For the unbounded flavour `send` itself is the non-blocking op.
            TokioTx::Unbounded(t) => t.send(v).map_err(|e| e.0),
        }
    }
    fn try_recv(rx: &mut Self::Rx) -> Option<T> {
        match rx {
            TokioRx::Bounded(r) => r.try_recv().ok(),
            TokioRx::Unbounded(r) => r.try_recv().ok(),
        }
    }
    fn send_async<'a>(tx: &'a Self::Tx, v: T) -> impl Future<Output = ()> + Send + 'a {
        async move {
            match tx {
                TokioTx::Bounded(t) => {
                    let _ = t.send(v).await;
                }
                TokioTx::Unbounded(t) => {
                    let _ = t.send(v);
                }
            }
        }
    }
    fn recv_async<'a>(rx: &'a mut Self::Rx) -> impl Future<Output = Option<T>> + Send + 'a {
        async move {
            match rx {
                TokioRx::Bounded(r) => r.recv().await,
                TokioRx::Unbounded(r) => r.recv().await,
            }
        }
    }
}

// ============================================================================
// Scenario helpers
// ============================================================================

/// Sum of `0..count`, the checksum every consumer must reproduce.
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
        "checksum mismatch: received sequence numbers differ from sent"
    );
}

/// Spins until the item could be pushed.
fn send_spin<T: Payload, C: Chan<T>>(tx: &C::Tx, v: T) {
    let mut v = v;
    while let Err(back) = C::try_send(tx, v) {
        v = back;
        spin_loop();
    }
}

/// Spins until an item is available.
fn recv_spin<T: Payload, C: Chan<T>>(rx: &mut C::Rx) -> T {
    loop {
        if let Some(v) = C::try_recv(rx) {
            return v;
        }
        spin_loop();
    }
}

/// One timed repetition of a scenario.
#[derive(Clone, Copy)]
struct Sample {
    ns: f64,
    /// Messages (or round trips, for `request_response`) the timing covers.
    ops: usize,
}

// ============================================================================
// Scenarios
// ============================================================================

/// `producers` senders feeding one bot loop over a channel of capacity `cap`,
/// std threads, spinning on `Full` / `Empty`. All threads are created and gated
/// on a `Barrier` before the clock starts.
fn sync_pipeline<T: Payload, C: Chan<T>>(
    ctx: &Ctx<'_>,
    cap: Option<usize>,
    producers: usize,
) -> Sample {
    let per = (ctx.cfg.n / producers).max(1);
    let total = per * producers;
    let (tx, mut rx) = C::channel(cap);
    let barrier = Arc::new(Barrier::new(producers + 2));

    let mut prod_handles = Vec::with_capacity(producers);
    for p in 0..producers {
        let tx_c = tx.clone();
        let b = barrier.clone();
        let pin: Vec<usize> = ctx.cfg.pin.clone();
        prod_handles.push(thread::spawn(move || {
            pin_worker(&pin, p);
            b.wait();
            let base = (p * per) as u64;
            for j in 0..per as u64 {
                send_spin::<T, C>(&tx_c, T::new(base + j));
            }
        }));
    }

    let b = barrier.clone();
    let pin: Vec<usize> = ctx.cfg.pin.clone();
    let consumer = thread::spawn(move || {
        pin_worker(&pin, producers);
        b.wait();
        let mut sum = 0u64;
        for _ in 0..total {
            sum = sum.wrapping_add(recv_spin::<T, C>(&mut rx).seq());
        }
        sum
    });

    barrier.wait();
    let start = Instant::now();
    let sum = consumer.join().expect("consumer thread panicked");
    let elapsed = start.elapsed().as_nanos() as f64;
    for h in prod_handles {
        h.join().expect("producer thread panicked");
    }
    check_sum(sum, expected_sum(total as u64));
    Sample {
        ns: elapsed / total as f64,
        ops: total,
    }
}

/// The same topology on a 4-worker tokio runtime, using the async `send`/`recv`
/// of each implementation. Tasks are spawned and gated on an async barrier
/// before the clock starts.
fn async_pipeline<T: Payload, C: Chan<T>>(
    ctx: &Ctx<'_>,
    cap: Option<usize>,
    producers: usize,
) -> Sample {
    let per = (ctx.cfg.n / producers).max(1);
    let total = per * producers;

    ctx.rt.block_on(async move {
        let (tx, mut rx) = C::channel(cap);
        let barrier = Arc::new(tokio::sync::Barrier::new(producers + 2));

        let mut prod_handles = Vec::with_capacity(producers);
        for p in 0..producers {
            let tx_c = tx.clone();
            let b = barrier.clone();
            prod_handles.push(tokio::spawn(async move {
                b.wait().await;
                let base = (p * per) as u64;
                for j in 0..per as u64 {
                    C::send_async(&tx_c, T::new(base + j)).await;
                }
            }));
        }

        let b = barrier.clone();
        let consumer = tokio::spawn(async move {
            b.wait().await;
            let mut sum = 0u64;
            for _ in 0..total {
                match C::recv_async(&mut rx).await {
                    Some(v) => sum = sum.wrapping_add(v.seq()),
                    None => break,
                }
            }
            sum
        });

        barrier.wait().await;
        let start = Instant::now();
        let sum = consumer.await.expect("consumer task panicked");
        let elapsed = start.elapsed().as_nanos() as f64;
        for h in prod_handles {
            h.await.expect("producer task panicked");
        }
        check_sum(sum, expected_sum(total as u64));
        Sample {
            ns: elapsed / total as f64,
            ops: total,
        }
    })
}

/// Bot thread sends a command and waits for the answer on a second channel; the
/// executor thread echoes it back. Timing covers whole round trips.
fn sync_request_response<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    let rounds = (ctx.cfg.n / ROUNDTRIP_DIV).max(1);
    let (tx_req, mut rx_req) = C::channel(Some(CAP_BOT));
    let (tx_rsp, mut rx_rsp) = C::channel(Some(CAP_BOT));
    let barrier = Arc::new(Barrier::new(3));

    let b = barrier.clone();
    let pin: Vec<usize> = ctx.cfg.pin.clone();
    let executor = thread::spawn(move || {
        pin_worker(&pin, 1);
        b.wait();
        for _ in 0..rounds {
            let v = recv_spin::<T, C>(&mut rx_req);
            send_spin::<T, C>(&tx_rsp, v);
        }
    });

    let b = barrier.clone();
    let pin: Vec<usize> = ctx.cfg.pin.clone();
    let bot = thread::spawn(move || {
        pin_worker(&pin, 0);
        b.wait();
        let mut sum = 0u64;
        for i in 0..rounds as u64 {
            send_spin::<T, C>(&tx_req, T::new(i));
            sum = sum.wrapping_add(recv_spin::<T, C>(&mut rx_rsp).seq());
        }
        sum
    });

    barrier.wait();
    let start = Instant::now();
    let sum = bot.join().expect("bot thread panicked");
    let elapsed = start.elapsed().as_nanos() as f64;
    executor.join().expect("executor thread panicked");
    check_sum(sum, expected_sum(rounds as u64));
    Sample {
        ns: elapsed / rounds as f64,
        ops: rounds,
    }
}

/// The same round trip between two tokio tasks.
fn async_request_response<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    let rounds = (ctx.cfg.n / ROUNDTRIP_DIV).max(1);

    ctx.rt.block_on(async move {
        let (tx_req, mut rx_req) = C::channel(Some(CAP_BOT));
        let (tx_rsp, mut rx_rsp) = C::channel(Some(CAP_BOT));
        let barrier = Arc::new(tokio::sync::Barrier::new(3));

        let b = barrier.clone();
        let executor = tokio::spawn(async move {
            b.wait().await;
            for _ in 0..rounds {
                match C::recv_async(&mut rx_req).await {
                    Some(v) => C::send_async(&tx_rsp, v).await,
                    None => break,
                }
            }
        });

        let b = barrier.clone();
        let bot = tokio::spawn(async move {
            b.wait().await;
            let mut sum = 0u64;
            for i in 0..rounds as u64 {
                C::send_async(&tx_req, T::new(i)).await;
                match C::recv_async(&mut rx_rsp).await {
                    Some(v) => sum = sum.wrapping_add(v.seq()),
                    None => break,
                }
            }
            sum
        });

        barrier.wait().await;
        let start = Instant::now();
        let sum = bot.await.expect("bot task panicked");
        let elapsed = start.elapsed().as_nanos() as f64;
        executor.await.expect("executor task panicked");
        check_sum(sum, expected_sum(rounds as u64));
        Sample {
            ns: elapsed / rounds as f64,
            ops: rounds,
        }
    })
}

// ---------------------------------------------------------------- the ten rows

fn sync_ws_bot_4096<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    sync_pipeline::<T, C>(ctx, Some(CAP_BOT), 1)
}
fn async_ws_bot_4096<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    async_pipeline::<T, C>(ctx, Some(CAP_BOT), 1)
}
fn sync_ws_bot_25<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    sync_pipeline::<T, C>(ctx, Some(CAP_STREAM), 1)
}
fn async_ws_bot_25<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    async_pipeline::<T, C>(ctx, Some(CAP_STREAM), 1)
}
fn sync_ws_bot_unbounded<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    sync_pipeline::<T, C>(ctx, None, 1)
}
fn async_ws_bot_unbounded<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    async_pipeline::<T, C>(ctx, None, 1)
}
fn sync_two_readers<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    sync_pipeline::<T, C>(ctx, Some(CAP_BOT), 2)
}
fn async_two_readers<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    async_pipeline::<T, C>(ctx, Some(CAP_BOT), 2)
}

/// The market-data collector shape: one reader task per WebSocket connection (up to 40 per
/// exchange), all cloning one unbounded sender that feeds a single
/// stream-writer task.
const COLLECTOR_READERS: usize = 40;

fn sync_collector_readers<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    sync_pipeline::<T, C>(ctx, None, COLLECTOR_READERS)
}
fn async_collector_readers<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>) -> Sample {
    async_pipeline::<T, C>(ctx, None, COLLECTOR_READERS)
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
    fn selected(&self, topology: &str, mode: &str, imp: &str) -> bool {
        match &self.filter {
            Some(f) => {
                topology.contains(f.as_str())
                    || mode.contains(f.as_str())
                    || imp.contains(f.as_str())
            }
            None => true,
        }
    }
}

struct Ctx<'a> {
    cfg: &'a Cfg,
    rt: &'a tokio::runtime::Runtime,
}

fn parse_cfg(args: &[String]) -> Cfg {
    let mut all: Vec<String> = args.to_vec();
    if let Ok(extra) = std::env::var("FCRS_REAL_BENCH_ARGS") {
        all.extend(extra.split_whitespace().map(str::to_string));
    }

    let mut cfg = Cfg {
        n: 2_000_000,
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
        cfg.n = 200_000;
        cfg.runs = 2;
    }
    if let Some(n) = n_override {
        cfg.n = n.max(1000);
    }
    cfg
}

/// One tokio runtime for the whole suite: building it is never part of a timed
/// region, and its worker threads are pinned once, like the sync workers.
fn build_runtime(pin: &[usize]) -> tokio::runtime::Runtime {
    let pin: Vec<usize> = pin.to_vec();
    let next = Arc::new(AtomicUsize::new(0));
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(ASYNC_WORKERS)
        .enable_all()
        .on_thread_start(move || {
            let k = next.fetch_add(1, Ordering::Relaxed);
            pin_worker(&pin, k);
        })
        .build()
        .expect("tokio multi_thread runtime")
}

struct Row {
    topo: &'static str,
    topo_idx: usize,
    bytes: usize,
    mode: &'static str,
    mode_idx: usize,
    imp: &'static str,
    imp_idx: usize,
    ns: f64,
    ops: usize,
    runs: usize,
}

type ScenarioFn = fn(&Ctx<'_>) -> Sample;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).expect("timings are never NaN"));
    let mid = v.len() / 2;
    if v.len().is_multiple_of(2) {
        (v[mid - 1] + v[mid]) / 2.0
    } else {
        v[mid]
    }
}

fn bench_impl<T: Payload, C: Chan<T>>(ctx: &Ctx<'_>, imp_idx: usize, out: &mut Vec<Row>) {
    let scenarios: [(&'static str, &'static str, ScenarioFn); 12] = [
        (
            "ws_to_bot_spsc_bounded4096",
            "sync",
            sync_ws_bot_4096::<T, C>,
        ),
        (
            "ws_to_bot_spsc_bounded4096",
            "async",
            async_ws_bot_4096::<T, C>,
        ),
        ("ws_to_bot_spsc_bounded25", "sync", sync_ws_bot_25::<T, C>),
        ("ws_to_bot_spsc_bounded25", "async", async_ws_bot_25::<T, C>),
        (
            "ws_to_bot_spsc_unbounded",
            "sync",
            sync_ws_bot_unbounded::<T, C>,
        ),
        (
            "ws_to_bot_spsc_unbounded",
            "async",
            async_ws_bot_unbounded::<T, C>,
        ),
        (
            "two_readers_to_bot_bounded4096",
            "sync",
            sync_two_readers::<T, C>,
        ),
        (
            "two_readers_to_bot_bounded4096",
            "async",
            async_two_readers::<T, C>,
        ),
        (
            "collector_40_readers_to_writer_unbounded",
            "sync",
            sync_collector_readers::<T, C>,
        ),
        (
            "collector_40_readers_to_writer_unbounded",
            "async",
            async_collector_readers::<T, C>,
        ),
        (
            "request_response_bounded4096",
            "sync",
            sync_request_response::<T, C>,
        ),
        (
            "request_response_bounded4096",
            "async",
            async_request_response::<T, C>,
        ),
    ];

    for (k, (topo, mode, f)) in scenarios.into_iter().enumerate() {
        if !ctx.cfg.selected(topo, mode, C::NAME) {
            continue;
        }
        // One warmup repetition, then `runs` measured ones; the median is kept.
        let warm = f(ctx);
        let mut samples = Vec::with_capacity(ctx.cfg.runs);
        for _ in 0..ctx.cfg.runs {
            samples.push(f(ctx).ns);
        }
        out.push(Row {
            topo,
            topo_idx: k / 2,
            bytes: T::BYTES,
            mode,
            mode_idx: k % 2,
            imp: C::NAME,
            imp_idx,
            ns: median(samples),
            ops: warm.ops,
            runs: ctx.cfg.runs,
        });
    }
}

fn bench_payload<T: Payload>(ctx: &Ctx<'_>, out: &mut Vec<Row>) {
    bench_impl::<T, Fast>(ctx, 0, out);
    bench_impl::<T, AsyncCh>(ctx, 1, out);
    bench_impl::<T, TokioMpsc>(ctx, 2, out);
    bench_impl::<T, FlumeCh>(ctx, 3, out);
}

fn print_table(cfg: &Cfg, rows: &[Row]) {
    println!(
        "\nrapidfire real-case bench | n={} runs={} (median of {} + 1 warmup){} | async workers={} | pin={}",
        cfg.n,
        cfg.runs,
        cfg.runs,
        if cfg.quick { " quick" } else { "" },
        ASYNC_WORKERS,
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
        "request_response rows are n/{} round trips (two messages each) per op; \
         every other row is n one-way messages.",
        ROUNDTRIP_DIV
    );
    println!(
        "{:<30} | {:>5} | {:<5} | {:<13} | {:>10} | {:>8}",
        "topology", "bytes", "mode", "impl", "ns/op", "Mops"
    );
    println!(
        "{:-<30}-+-{:->5}-+-{:-<5}-+-{:-<13}-+-{:->10}-+-{:->8}",
        "", "", "", "", "", ""
    );
    let mut prev: Option<(usize, usize)> = None;
    for r in rows {
        if let Some(p) = prev {
            if p != (r.topo_idx, r.bytes) {
                println!(
                    "{:-<30}-+-{:->5}-+-{:-<5}-+-{:-<13}-+-{:->10}-+-{:->8}",
                    "", "", "", "", "", ""
                );
            }
        }
        prev = Some((r.topo_idx, r.bytes));
        println!(
            "{:<30} | {:>5} | {:<5} | {:<13} | {:>10.2} | {:>8.2}",
            r.topo,
            r.bytes,
            r.mode,
            r.imp,
            r.ns,
            1000.0 / r.ns
        );
    }
    println!();
}

fn print_json(rows: &[Row]) {
    for r in rows {
        println!(
            "{{\"topology\":\"{}\",\"payload_bytes\":{},\"mode\":\"{}\",\"impl\":\"{}\",\"ns_per_op\":{:.3},\"mops\":{:.3},\"ops\":{},\"runs\":{}}}",
            r.topo,
            r.bytes,
            r.mode,
            r.imp,
            r.ns,
            1000.0 / r.ns,
            r.ops,
            r.runs
        );
    }
}

/// Runs the whole real-case benchmark suite.
pub fn run(args: &[String]) {
    let cfg = parse_cfg(args);
    let rt = build_runtime(&cfg.pin);
    let ctx = Ctx { cfg: &cfg, rt: &rt };

    let mut rows: Vec<Row> = Vec::new();
    bench_payload::<Msg64>(&ctx, &mut rows);
    bench_payload::<Msg256>(&ctx, &mut rows);
    bench_payload::<Msg1024>(&ctx, &mut rows);

    rows.sort_by_key(|r| (r.topo_idx, r.bytes, r.mode_idx, r.imp_idx));

    print_table(&cfg, &rows);
    if cfg.json {
        print_json(&rows);
    }
}

#[test]
fn real_case_bench() {
    if std::env::var("FCRS_REAL_BENCH").is_ok() {
        run(&[]);
    }
}
