//! Quick hot-path microbenchmark used while tuning; the real harness lives in benches/.
use std::sync::atomic::{AtomicUsize, Ordering};
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

trait Q: Send + Sync + 'static {
    fn new() -> Self;
    fn push(&self, v: u64);
    fn pop(&self) -> Option<u64>;
    fn dump(&self) -> String {
        String::new()
    }
}

struct Fast(rapidfire::Sender<u64>, rapidfire::Receiver<u64>);
impl Q for Fast {
    fn new() -> Self {
        let (t, r) = rapidfire::unbounded();
        Fast(t, r)
    }
    #[inline(always)]
    fn push(&self, v: u64) {
        self.0.try_send(v).ok().unwrap();
    }
    #[inline(always)]
    fn pop(&self) -> Option<u64> {
        self.1.try_recv().ok()
    }
    fn dump(&self) -> String {
        self.1.__debug_dump()
    }
}

struct Seg(crossbeam_queue::SegQueue<u64>);
impl Q for Seg {
    fn new() -> Self {
        Seg(crossbeam_queue::SegQueue::new())
    }
    #[inline(always)]
    fn push(&self, v: u64) {
        self.0.push(v)
    }
    #[inline(always)]
    fn pop(&self) -> Option<u64> {
        self.0.pop()
    }
}

struct Tokio(
    tokio::sync::mpsc::UnboundedSender<u64>,
    std::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<u64>>,
);
impl Q for Tokio {
    fn new() -> Self {
        let (t, r) = tokio::sync::mpsc::unbounded_channel();
        Tokio(t, std::sync::Mutex::new(r))
    }
    #[inline(always)]
    fn push(&self, v: u64) {
        self.0.send(v).unwrap()
    }
    #[inline(always)]
    fn pop(&self) -> Option<u64> {
        self.1.lock().unwrap().try_recv().ok()
    }
}

fn st_push_pop<T: Q>(n: usize) -> f64 {
    let q = T::new();
    let t = Instant::now();
    let mut s = 0u64;
    for i in 0..n as u64 {
        q.push(i);
        s = s.wrapping_add(q.pop().unwrap());
    }
    std::hint::black_box(s);
    ns(t.elapsed(), n)
}

fn burst<T: Q>(n: usize, b: usize) -> f64 {
    let q = T::new();
    let t = Instant::now();
    let mut s = 0u64;
    for _ in 0..n / b {
        for i in 0..b as u64 {
            q.push(i);
        }
        for _ in 0..b {
            s = s.wrapping_add(q.pop().unwrap());
        }
    }
    std::hint::black_box(s);
    ns(t.elapsed(), n)
}

fn mp_sc<T: Q>(n: usize, producers: usize, consumers: usize) -> f64 {
    let q = Arc::new(T::new());
    let barrier = Arc::new(Barrier::new(producers + consumers + 1));
    let done = Arc::new(AtomicUsize::new(0));
    let mut hs = Vec::new();
    for p in 0..producers {
        let q = q.clone();
        let barrier = barrier.clone();
        let per = n / producers;
        hs.push(thread::spawn(move || {
            want_p_core();
            barrier.wait();
            for i in 0..per as u64 {
                q.push((p as u64) << 40 | i);
            }
        }));
    }
    for _ in 0..consumers {
        let q = q.clone();
        let barrier = barrier.clone();
        let done = done.clone();
        hs.push(thread::spawn(move || {
            want_p_core();
            barrier.wait();
            let mut s = 0u64;
            let mut idle = 0u64;
            let mut last = Instant::now();
            loop {
                match q.pop() {
                    Some(v) => {
                        s = s.wrapping_add(v);
                        idle = 0;
                        if done.fetch_add(1, Ordering::Relaxed) + 1 == n {
                            break;
                        }
                    }
                    None => {
                        if done.load(Ordering::Relaxed) >= n {
                            break;
                        }
                        idle += 1;
                        if idle.is_multiple_of(4096) && last.elapsed().as_secs() >= 3 {
                            eprintln!(
                                "STALL received={} : {}",
                                done.load(Ordering::Relaxed),
                                q.dump()
                            );
                            last = Instant::now();
                        }
                        std::hint::spin_loop();
                    }
                }
            }
            std::hint::black_box(s);
        }));
    }
    barrier.wait();
    let t = Instant::now();
    for h in hs {
        h.join().unwrap();
    }
    ns(t.elapsed(), n)
}

fn pingpong<T: Q>(rounds: usize) -> f64 {
    let a = Arc::new(T::new());
    let b = Arc::new(T::new());
    let barrier = Arc::new(Barrier::new(2));
    let (a2, b2, bar2) = (a.clone(), b.clone(), barrier.clone());
    let h = thread::spawn(move || {
        want_p_core();
        bar2.wait();
        for _ in 0..rounds {
            loop {
                if let Some(v) = a2.pop() {
                    b2.push(v);
                    break;
                }
                std::hint::spin_loop();
            }
        }
    });
    barrier.wait();
    let t = Instant::now();
    for i in 0..rounds as u64 {
        a.push(i);
        loop {
            if let Some(v) = b.pop() {
                assert_eq!(v, i);
                break;
            }
            std::hint::spin_loop();
        }
    }
    let e = t.elapsed();
    h.join().unwrap();
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
        println!(
            "{:<8} mpmc_4p4c   {:>7.2} ns",
            name,
            r(&|| mp_sc::<T>(n, 4, 4))
        );
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
