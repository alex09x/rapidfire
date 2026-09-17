# rapidfire

[![crates.io](https://img.shields.io/crates/v/rapidfire.svg)](https://crates.io/crates/rapidfire)
[![docs.rs](https://docs.rs/rapidfire/badge.svg)](https://docs.rs/rapidfire)
[![CI](https://github.com/alex09x/rapidfire/actions/workflows/ci.yml/badge.svg)](https://github.com/alex09x/rapidfire/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Async **MPMC and MPSC channels** for Rust, built for WebSocket fan-in, trading bots
and other message pipelines. The default build depends only on `std`.

Version **0.2.0** adds an exclusive MPSC receiver, ready-message batching and a
shared lost-wakeup fix. See the [changelog](CHANGELOG.md).

## Choose a channel

| API | Senders | Receivers | Receive access | Batching |
|:--|:--|:--|:--|:--|
| `rapidfire::{bounded, unbounded}` | Cloneable | Cloneable (MPMC) | `&self` | Drain with `try_recv` |
| `rapidfire::mpsc::{bounded, unbounded}` | Cloneable | One, not cloneable (MPSC) | `&mut self` | `recv_many` |

Both provide bounded back-pressure and unbounded queues, async `send`/`recv`,
nonblocking `try_send`/`try_recv`, explicit `close`, queue length and sender counts.
They work with executors that poll standard Rust futures; Tokio is used below.
The existing MPMC API is unchanged.

## Quick start

```toml
[dependencies]
rapidfire = "0.2"
```

### Many producers, one receiver

```rust
use rapidfire::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::bounded::<u64>(4096);

    for producer in 0..4 {
        let tx = tx.clone();
        tokio::spawn(async move {
            for sequence in 0..100 {
                tx.send(producer * 100 + sequence).await.unwrap();
            }
        });
    }
    drop(tx); // The receiver finishes after all producer handles are dropped.

    let mut batch = Vec::with_capacity(32);
    while let Ok(count) = rx.recv_many(&mut batch, 32).await {
        assert!(count > 0);
        for value in batch.drain(..) {
            // Process the message here.
            println!("{value}");
        }
    }
    Ok(())
}
```

`Sender` can be cloned freely. The MPSC receiver has no `Clone` implementation;
`recv`, `try_recv` and `recv_many` require an exclusive mutable borrow. A pending
receive keeps that borrow, so Rust rejects two concurrent receives on one receiver.
The receiver can be moved to another task or thread.

`recv_many(&mut buffer, limit)` appends up to `limit` ready messages, waiting only
for the first message. It never waits to fill a batch. Reuse the buffer and clear
or drain it after processing. A zero limit returns `Ok(0)` immediately. Otherwise,
a closed and drained channel returns `Err(RecvError)`. Cancelling a pending receive
consumes no messages.

### Multiple receivers

```rust
let (tx, rx) = rapidfire::unbounded::<u64>();
let second_sender = tx.clone();
let second_receiver = rx.clone();
tx.try_send(42).unwrap();
assert_eq!(second_receiver.try_recv().unwrap(), 42);
```

Cloned receivers compete for messages; each message goes to one receiver.

## Performance

Use the [0.2.0 fleet report](benches/FLEET-0.2.0.md) for current measurements,
CPU placement, workload parameters and individual samples. It covers the previous
12 machines and includes both rapidfire APIs plus the other channel implementations.
The MPSC comparison separately measures sync 4/8 producers and async 4/40 producers,
64/256/1024-byte messages, bounded capacities 25/4096, unbounded queues, and receive
limits 1/32. Paced delivery latency records p50, p99 and maximum delay.

Selected results with 1 KiB messages, four Tokio workers and receive limit 32:

| CPU (report machine) | Workload | MPMC, ns/message | MPSC, ns/message | Result |
|:--|:--|--:|--:|:--|
| Ryzen 7950X (#3) | 40 producers, bounded(4096) | 249.58 | 57.22 | 4.36× faster |
| Ryzen 9950X (#8) | 40 producers, bounded(4096) | 155.17 | 49.53 | 3.13× faster |
| Apple M3 Pro (#12) | 40 producers, bounded(4096) | 82.23 | 72.95 | 1.13× faster |
| Ryzen 9950X (#8) | 4 producers, unbounded | 46.74 | 245.56 | 5.25× slower |

These are amortized throughput costs, measured on shared machines. The report
includes every machine, all workloads and separate paced latency measurements.

**MPSC is an opt-in performance tradeoff.** A single receiver avoids arbitration
between consumers, and batching amortizes capacity updates and sender notifications.
Producers still contend on a shared tail. Scheduling, queue occupancy and recycling
can outweigh the saved work; some workloads become slower. Lower throughput cost
does not imply lower p99 latency.

The [initial MPSC study](benches/MPSC.md) contains the earlier single-machine A/B
experiment. The [0.1.0 tables](benches/RESULTS.md) are a historical snapshot with
known harness limitations; their ratios are not measurements from the corrected
harness. Earlier changes are documented in the [optimization report](benches/OPTIMIZATION.md).

### Reproduce the matrix

Select nine physical CPU IDs for MPSC and a complete CPU pool for the wider
comparison harnesses. Put physical cores before their SMT siblings. Every requested
affinity call is checked; the harness fails if the list misses a worker. macOS uses
user-interactive QoS and remains unpinned.

```sh
python3 benches/run_matrix.py --source-commit "$(git rev-parse HEAD)" \
  --output /tmp/rapidfire-results --cpus 0,1,2,3,4,5,6,7,8 \
  --comparison-cpus 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
```

The runner uses Rust 1.97.1, `-C target-cpu=native`, fat LTO and one codegen unit.
It runs correctness tests, the MPSC throughput/latency matrix and both comparative
harnesses. Keep the output directory to retain the source hashes, commands, load
readings, logs and samples. Run one matrix at a time per machine.

## How it works

The queue stores messages in linked blocks with 63 usable slots. Producer and
consumer indices are separately padded to 128 bytes. Producers reserve a position,
write a value and publish its slot state with a release store. Unbounded producers
use `fetch_add` on the uncontended path and CAS retries under contention; bounded
producers reserve with a capacity check and CAS.

General MPMC consumers check publication before advancing the head with CAS.
Per-block reader-completion flags prevent recycling a block while another consumer
still reads it. The exclusive MPSC receiver omits coordination with other consumers.
Bounded MPSC receives retain the atomic read-modify-write needed by the sender
parking protocol; unbounded receives can publish the new head with a store.

A receive batch publishes freed capacity once per chunk within a block and wakes
up to the corresponding number of blocked senders. On this development branch,
waiters and the recycling pool use atomic ownership slots instead of mutexes.
Waiter scans rotate through reusable slots; waiter notification order is not a
FIFO guarantee. A cancellation forwards an absorbed notification even when the
original notifier is paused. Blocks and extra slot pages remain allocated until
channel destruction, retaining high-water memory.

Removing internal mutexes does not make every API operation formally lock-free:
a producer paused after reserving a message or installing a block can still delay
other operations. Allocation and user-supplied waker callbacks have their own
progress properties. See [queue.rs](src/queue.rs) and [waiters.rs](src/waiters.rs)
for the ownership and synchronization protocols.

Pending `Send` and `Recv` futures unregister their waiters on cancellation and
forward notifications when needed. Version 0.2.0 also fixes a completing operation
absorbing a later notification that belongs to another waiter. Deterministic
regressions cover both the send and receive races.

A documented close race remains: a send that observed the channel open may succeed
after a concurrent close has already made a receiver report `Closed`; the value is
dropped with the channel.

## Verification

- `cargo test`: queue, integration and compile-fail documentation tests.
- `RUSTFLAGS="--cfg fcrs_small_blocks" cargo test`: three-slot blocks to stress
  transitions, recycling and back-pressure.
- `cargo test --release --features loom --lib --test loom --test mpsc_loom`: bounded
  concurrency models, including atomic waiter and pool ownership. Two default
  wakeup searches cap permutations at 1,000,000;
  `RAPIDFIRE_LOOM_EXTENDED=1` removes that cap, retaining the preemption bounds.
- `RUSTFLAGS="--cfg fcrs_small_blocks" MIRIFLAGS="-Zmiri-strict-provenance"
  cargo +nightly miri test --lib -- --skip threads_`: unsafe-core checks.
- GitHub Actions runs native tests on Linux x86-64, Linux ARM64 and macOS, with
  separate Loom and Miri jobs on Linux. Hosted-runner benchmark numbers are indicative.

## License

Licensed under either [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT License](LICENSE-MIT), at your option.
