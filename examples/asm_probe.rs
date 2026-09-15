//! Isolated, never-inlined entry points around the hot paths, so the generated code can
//! be read with `objdump -d` / `otool -tv` without the surrounding benchmark noise:
//!
//! ```text
//! cargo build --release --example asm_probe
//! objdump -d --no-show-raw-insn target/release/examples/asm_probe | awk '/<.*probe_try_send.*>:/,/ret/'
//! ```
use rapidfire::{bounded, unbounded, Receiver, Sender};

#[inline(never)]
pub fn probe_try_send(tx: &Sender<u64>, v: u64) -> bool {
    tx.try_send(v).is_ok()
}

#[inline(never)]
pub fn probe_try_recv(rx: &Receiver<u64>) -> Option<u64> {
    rx.try_recv().ok()
}

#[inline(never)]
pub fn probe_try_send_bounded(tx: &Sender<u64>, v: u64) -> bool {
    tx.try_send(v).is_ok()
}

fn main() {
    let (tx, rx) = unbounded::<u64>();
    let (btx, brx) = bounded::<u64>(1024);
    let mut sum = 0u64;
    for i in 0..1_000_000u64 {
        probe_try_send(&tx, i);
        sum = sum.wrapping_add(probe_try_recv(&rx).unwrap_or(0));
        probe_try_send_bounded(&btx, i);
        sum = sum.wrapping_add(brx.try_recv().unwrap_or(0));
    }
    println!("{sum}");
}
