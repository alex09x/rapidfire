---
title: "rapidfire: the channel between a market feed and a decision"
description: "A trading process spends its microsecond budget on the network and on parsing; the hand-off between threads is one of the few costs fully under our control. How we built a lock-free channel for it, what broke on a 128-core ARM box, what the sampler said about assembly, and the numbers from twelve machines."
pubDate: 2026-09-16
tag: "design note"
draft: false
---

Published article: [rapidfire: the channel between a market feed and a decision](https://prod.codes/blog/rapidfire-channel-between-feed-and-decision/).
This upstream draft retains the original 0.1.0 measurements. Subsequent channel and
harness changes have a separate [before/after report](../benches/OPTIMIZATION.md).

An exchange publishes an order-book update over a WebSocket. A reader task decrypts the frame,
parses it, stamps it, and hands it to the strategy thread. The strategy decides, and an order
goes out. In high-frequency trading that whole chain is the product: whoever reacts to the same
update first gets the fill, and the budget from packet to order is tens of microseconds. Most of
it is not ours to spend. The exchange's matching engine, the network and TLS eat the bulk;
parsing takes what it takes. The hand-off between the reader and the strategy is different: it
is entirely in-process, it happens on every message, and a channel that costs a hundred
nanoseconds plus a syscall to wake the consumer is a visible slice of the budget. We wrote our
own. It is now open source as [rapidfire](https://github.com/alex09x/rapidfire)
(`rapidfire = "0.1"` on crates.io), and this note is about what it took.

![Where the channel sits: exchange feeds, reader tasks, one channel, one pinned strategy thread, the order gateway](img/pipeline.svg)

The principles are the usual ones for this kind of code, and all of them are about the hot path
only. No lock: a lock is a syscall waiting to happen. No allocation per message. No syscall to
hand a message over; the consumer only parks when the queue is empty, and it is the parking
that costs, not the message. Keep the producer's and the consumer's data on different cache
lines, because every line that crosses cores is a round trip of about a hundred nanoseconds.
And measure the tail, not the mean, on the machine you actually run on.

## What the queue looks like

The channel is an intrusive linked list of blocks. Each block holds 63 value slots; the
64th index position is a sentinel used during block transitions and holds no value.
Every slot carries a state word tagged with the lap number, so a slot reused on the next
pass around the pool cannot be mistaken for a written one.

Producer tail and consumer head are separately padded to 128 bytes; each block also keeps
reader-completion marks in a separately padded header. An uncontended unbounded producer
reserves a slot with `fetch_add`; under contention this path uses CAS retries with backoff,
which reduced adjacent-slot contention in our measurements. Bounded senders reserve with a
capacity check and CAS. After reservation, the producer writes the value and publishes the
slot state with a release store. A consumer checks that state before CAS-advancing the head,
so it never claims an unwritten slot. Blocks stay allocated while the channel is live and
return to a spare slot or pool only after their readers finish. Recycling retains high-water
memory until the channel is dropped.

![Queue layout: linked 63-slot blocks, padded head and tail, per-block read flags](img/queue-layout.svg)

The async layer is a mutex-protected waiter list used for registration and notification.
After registering, a parking receiver does `fetch_add(0, AcqRel)` on the tail index. Later
producer claims acquire that release sequence, so a relaxed load after the claim sees the
registration. If the snapshot includes a claimed but unwritten slot, the receiver polls and
yields instead of sleeping through its publication. Bounded senders use the corresponding
protocol on the head index. Every index mutation must be a read-modify-write to preserve the
release sequence. A plain `store` on the head broke it, and the Loom model caught the bug.

## What broke on the way

Pre-release builds ran fine on a workstation and crashed within minutes on a 128-core
Neoverse-N1 box: a lagging block walker followed a `prev` link into a
block that had been recycled and had its links cleared. The fix is a rule, not a patch: a
block's `start` position is published last with a release store, links are never nulled on
recycle, and a walker validates every block it lands on by its `start`. The second failure was
a deadlock in the four-by-four MPMC test on the same machine. Consumers claimed a slot first
and waited for it to be written; under load the head passed an unwritten slot, the blocks
behind it were recycled, and a producer walking backwards from the tail to its slot was
stranded. Claim-first consumers were removed; a parked receiver polls busy slots and yields instead.

Two bugs came from review rather than from a machine. A delegated review task (we run
reviews as isolated !prod tasks, so the reviewer starts from the exact commit and can build
probes) found a lost wake-up: a `recv()` future cancelled right after it was chosen by
`notify_one` took the notification with it. Cancellation now forwards the wake-up to the next
waiter, and the case is a regression test. The same review flagged the send-versus-close race
shared with tokio and async-channel; we documented it instead of paying a fence for it.
One optimisation was reverted for correctness rather than speed: throttling how often a
bounded producer re-reads the head to check for space saved a line transfer per message and
made `try_send` return `Full` on a queue that was not full. Exactness won.

## What the sampler said about assembly

We expected to end up with inline assembly. We did not, and the reason is worth the detour.
`examples/asm_probe.rs` wraps `try_send` and `try_recv` in never-inlined functions so
`objdump` shows just the hot path. On Zen 4 an unbounded `try_send` is one `lock xadd` (a
`lock cmpxchg` once contention has been seen) and about twenty ordinary instructions; on
AArch64 it is one LSE atomic, `ldapr` loads and a single `stlr`. We measured three alternative
publish sequences: inline assembly using `dmb ishst` plus a plain store, a Rust
`fence(Release)` plus a plain store, and an atomic RMW publish. All three were slower
than the compiler's `stlr`.
They stay in the tree behind `--cfg` switches as evidence.

`perf` then explained where the remaining time is. In the single-producer case both rapidfire
and crossbeam's `SegQueue` miss L1 equally often, because the slot line has to cross cores
either way; rapidfire is faster because it executes 43 percent fewer instructions and half
the locked operations per message. On the N1 the producer spends 86 percent of its samples on
the instruction after `ldaddal`: the atomic has release semantics and waits for the previous
slot's store to reach the coherence point. That is the cross-core transfer of the slot line,
the physical floor. In the four-by-four MPMC case three quarters of all samples on both
machines are in the `pause` and `isb` back-off loops: contention on two index words, which
no instruction selection fixes. The compiler had not made a mistake anywhere we looked; the
work left is choosing which cache lines cross cores, and that is a data-layout decision.

## The numbers, and where we lose

The harness compares rapidfire with crossbeam-queue, std mpsc, flume, async-channel and
tokio's mpsc: medians of five runs, with the requested CPU lists recorded in the tables,
rustc 1.97.1 with `target-cpu=native`, on twelve machines from a Haswell Xeon and a Zen 2
desktop to Zen 5, a 128-core N1 and an M3 Pro.[^1] The topology we care most about is many
WebSocket readers feeding one writer, because that is what a market-data collector is.
The SPSC and collector numbers are amortized elapsed time per message under a stream,
not the latency of an individual hand-off or an end-to-end trading pipeline.

These are the original 0.1.0 measurements, retained as a historical snapshot. The raw harness
added a Tokio-only receive mutex and a shared per-message completion counter in MPMC tests,
and the harnesses started timing after releasing workers. Pin lists assigned only the listed
workers; additional workers were unpinned. Later corrections to the harness mean the original
ratios should not be interpreted as measurements from the corrected setup.

![Amortized time per message, SPSC unbounded, Ryzen 9 7950X](img/spsc-zen4.svg)

![Forty reader tasks feeding one writer on tokio, Ryzen 9 7900, by payload size](img/collector-async-zen4.svg)

The losses are in the tables too. On the M3 Pro, unpinned, crossbeam's `SegQueue` is 12
percent faster in the single-producer case and `ArrayQueue` wins the bounded one; Apple's
cores do not reward the lower instruction count the way Zen does. Many producers hammering a
small bounded channel that is routinely full spin on the consumer's head line, where
`ArrayQueue` polls its slot instead. A short pin list in an eight-producer run leaves some
workers unpinned, so placement and scheduling can dominate that comparison. None of these is
the shape our bots run, and all of them are written down in `benches/RESULTS.md` next to the wins.

The rule we would give anyone doing this: benchmark the topology you run, on the hardware you
run it on, and let the sampler decide whether assembly is the next step. For us it said no,
twice.

[^1]: Library versions in the comparison: crossbeam-queue 0.3.14, flume 0.11.1, async-channel
    2.5.0, tokio 1.53.1. The full matrix (bounded and unbounded, SPSC, MPSC 4, 8 and 32 producers,
    MPMC, ping-pong) and the perf profiles are in the
    [original RESULTS table](https://github.com/alex09x/rapidfire/blob/60966c1512b24e710af2f99d1b3382f10e9e218d/benches/RESULTS.md).
