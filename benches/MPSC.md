# Exclusive MPSC and receive batches — 2026-09-16

`rapidfire::mpsc::{bounded, unbounded}` adds an exclusive receiving half to the
existing producer/block queue. The receiver cannot be cloned; receiving requires
`&mut self`, including the full lifetime of a pending future. Existing MPMC APIs
and the published 0.1.0 benchmark tables are unchanged.

## What changed

- Single receives omit consumer CAS retries, per-slot read-completion stores and
  the scan for other readers before recycling a block.
- Bounded receives retain an AcqRel read-modify-write on head. It acquires the
  release sequence from parking senders; replacing it with a store would allow
  lost wakeups. Unbounded senders never park, so unbounded head updates use Release
  stores. Producer claims and receiver-parking synchronization are unchanged.
- `recv_many` copies a ready prefix into a reusable caller buffer and publishes
  freed capacity once per chunk within a block. It wakes up to the number of freed
  slots under one waiter-list lock. Reserving buffer space before moving values
  preserves ownership if allocation fails; notification precedes the next chunk.
- Receive cancellation, close/drain behavior, block retention and the shared
  sender API follow the general channel. Pool and waiter mutexes still exist.

The new tests also exposed a lost-wakeup bug in the shared send/receive futures:
a successful operation could still have its old waiter entry queued and absorb a
notification intended for a later value or freed slot. The fix removes the old
registration before the next attempt; successful register/recheck attempts forward
any notification they may have absorbed. Two deterministic public-API regressions
cover both send and receive, in addition to the Loom models. This fixes the general
MPMC channel too; the benchmark compares both variants with that fix applied.

No guarantee of uniformly faster execution follows from removing instructions.
The sender still contends on one shared tail, and making a receiver faster changes
queue occupancy, recycling frequency and how often tasks park.

## Throughput comparison

Candidate: `da9775b`, based on `e972335`. Both receivers and the identical sender
implementation, including the notification fix below, are compiled into the **same binary**. MPMC batch mode performs one
`recv().await` followed by `try_recv`; MPSC batch mode uses native `recv_many`.
Thus batch-to-batch rows compare equal message limits, not one-message receiving
against a batch of 32.

Host: AMD Ryzen 9 7950X, Linux 6.8.0-136-generic, rustc 1.97.1, fat LTO, one codegen
unit, `-C target-cpu=native`. Synchronous 4→1 pins all five workers to CPUs 0–4;
8→1 pins all nine to 0–8 and crosses CCDs. Async runs use four pinned Tokio workers
on CPUs 0–3; all producer and receiver tasks run on those workers. Affinity calls
are checked for failure. No worker silently falls outside the pin list.

Each result is the median of three batch medians. Each batch contains one warm-up
and five measurements per variant, alternating variant order. Runs request 500,003
messages, rounded up to equal producer quotas. Timing starts before releasing
ready workers and ends when the receiver has consumed all messages. Thread/runtime
creation and channel destruction are excluded. Channels start empty; allocations
needed as their backlog grows are included. Count and checksum checks passed.

Selected results, **amortized ns/message**, lower is better:

| Topology | Capacity | Payload | Receive limit | General MPMC | Exclusive MPSC | Time change |
|:--|--:|--:|--:|--:|--:|--:|
| Async 4→1 | 25 | 64 B | 32 | 65.55 | 29.81 | -54.5% |
| Async 40→1 | 4096 | 64 B | 32 | 63.38 | 22.64 | -64.3% |
| Async 40→1 | 4096 | 256 B | 32 | 63.99 | 34.00 | -46.9% |
| Async 40→1 | 4096 | 1024 B | 32 | 254.65 | 56.41 | -77.8% |
| Async 4→1 | Unbounded | 64 B | 32 | 16.11 | 48.83 | +203.2% |
| Async 4→1 | Unbounded | 1024 B | 32 | 118.13 | 415.72 | +251.9% |
| Async 40→1 | Unbounded | 256 B | 32 | 57.02 | 86.20 | +51.2% |
| Sync 4→1 | Unbounded | 64 B | 1 | 15.92 | 12.32 | -22.6% |
| Sync 4→1 | Unbounded | 256 B | 1 | 21.62 | 29.24 | +35.3% |
| Sync 8→1 | Unbounded | 64 B | 1 | 62.59 | 109.24 | +74.5% |

All **54 paired scenarios**, all 15 measured samples per variant, and the batch
medians are in [mpsc-2026-09-16.json](mpsc-2026-09-16.json). This is evidence from
one machine and workload, not a confidence interval or a universal ranking.
Unbounded async screening runs were especially sensitive to scheduling/occupancy;
the stored final matrix is the comparison used here. Large improvements in some
bounded batch rows do not erase the substantial regressions in the last rows.

## Paced delivery latency

A separate workload timestamps each message immediately before `send().await` and
records elapsed time when the receiver inspects it. Each producer offers bursts of
16 messages every 1 ms, skipping missed timer ticks. Both sides run as tasks on the
same four pinned Tokio workers. Each run requests 16,003 messages, rounded to equal
producer quotas. One warm-up precedes five interleaved measured runs per variant.

This includes capacity waits, scheduling and measurement overhead. It is not an
isolated atomic-operation latency or an end-to-end network measurement. Values
below are medians of the five per-run p99s, **microseconds**:

| Topology | Capacity | Receive limit | General MPMC p99 | Exclusive MPSC p99 |
|:--|--:|--:|--:|--:|
| Async 4→1 | Unbounded | 32 | 1.513 | 1.212 |
| Async 40→1 | 25 | 32 | 16.270 | 8.005 |
| Async 40→1 | 4096 | 32 | 4.368 | 8.426 |
| Async 40→1 | Unbounded | 1 | 14.307 | 19.396 |

Full per-run p50/p99/max values, including worse cases, are in the JSON. Lower
throughput cost alone would not justify claiming lower p99; these are separately
instrumented observations for the stated burst pattern. In particular, the 40→1
bounded(4096) batch row has lower throughput cost but a worse p99 in this run.
Earlier screening runs varied substantially, so no general latency improvement
is claimed.

## Reproduce

Choose CPU IDs from the host topology. For sync 8→1, all nine entries are required.
The example's positional arguments are message count, measured runs, producer
counts, modes and payload bytes. Capacity 0 (unbounded), 25 and 4096 are all tested.

```sh
RUSTFLAGS='-C target-cpu=native' cargo build --release --locked --example mpsc_compare

# Repeat the throughput commands three times, retaining each JSONL output.
MPSC_PIN=0,1,2,3,4,5,6,7,8 target/release/examples/mpsc_compare 500003 5 4,8 sync 64
MPSC_PIN=0,1,2,3 target/release/examples/mpsc_compare 500003 5 4,40 async 64
# Repeat with payload arguments 256 and 1024.

MPSC_PIN=0,1,2,3 target/release/examples/mpsc_compare 16003 5 4,40 latency 64
```

On macOS the example does not pin CPUs; do not label those runs as pinned. Async
batch limits are 1 and 32. Sync rows use single-message reads. Latency payloads are
64 bytes. Use positive message, run and producer counts.

## Validation

- 74 native tests and four doctests passed on macOS ARM, Linux x86-64 and Linux
  ARM. The Linux tests also passed with three-slot blocks; Linux ARM release
  checks covered the queue stress tests, MPSC integration tests and raw-harness
  smoke test.
- All 16 Loom models passed. Models use preemption bounds of two or three. The two
  three-actor wakeup models cap the default search at 1,000,000 permutations each;
  both still fail with the original deadlock on pre-fix `318df93`. Set
  `RAPIDFIRE_LOOM_EXTENDED=1` to remove that cap for a longer bounded search. This
  is bounded model checking, not an exhaustive proof of every execution.
- Miri with strict provenance and three-slot blocks passed all 13 non-threaded
  unit tests. Compile-fail doctests enforce the non-cloneable receiver and exclusive
  receive borrow. Formatting and Clippy checks passed.
- A clean rebuild reproduced the measured benchmark binary's SHA-256, recorded in
  the JSON. Negative controls rebuilt the pre-fix crate separately before running
  the deterministic lost-wakeup regression and both failing Loom scenarios.

Coverage includes per-producer FIFO, exact delivery and drop counts, unread values
at destruction, block recycling, batch wake cardinality, cancelled/notified futures,
waker replacement, close/drain, and transferring the exclusive receiver between
threads. Two deterministic public-API regressions cover a completing send or receive
absorbing a later notification.
