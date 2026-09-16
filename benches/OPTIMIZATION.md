# Channel optimization check — 2026-09-15

The changes remove two consumer waits on another consumer's progress and avoid a
redundant waiter-list lock after a notification. Blocks with unfinished readers
are retired and reused only after those readers finish. Bounded backoff stays out
of the successful receive path's instruction footprint.

This improves several async workloads. It is not a uniform throughput improvement:
some synchronous 256-byte workloads are slower. No new per-message atomic RMW or
dependency was added. A paused reader can keep an extra block allocated; blocks
remain allocated until the channel is dropped, as before. Pool and waiter mutexes
still exist, so this does not establish a hard bound on every API call's latency.

## Comparison

Baseline: `cf40507` (original channel with the corrected benchmark harness).
Candidate: `7b2a867`. Both use identical harness sources and separate Cargo target
directories. The old published tables in [RESULTS.md](RESULTS.md) are unchanged.

Machine: AMD Ryzen 9 7950X, Linux 6.8.0-136-generic, rustc 1.97.1. Release profile:
`opt-level=3`, fat LTO, one codegen unit, `-C target-cpu=native`. Each value below is
the median of three interleaved batch medians; each batch contains a warm-up and
five measured runs. Raw runs request 1,000,003 messages; real-case runs request
500,003. Multi-producer rows use the harness's rounded actual count. All checksums
passed. The JSON contains all 49 scenarios and all three samples for each version:
[optimization-2026-09-15.json](optimization-2026-09-15.json).

Examples below include improvements, neutral results and regressions. Throughput
rows report amortized ns per message; request/response reports ns per round trip.
These are observations from one machine, not confidence intervals or p99 latency.

| Scenario | Payload | Before | After | Time change |
|:--|--:|--:|--:|--:|
| Async SPSC, bounded(4096) | 256 B | 43.54 | 35.67 | -18.1% |
| Async SPSC, bounded(4096) | 1024 B | 111.01 | 100.37 | -9.6% |
| Async SPSC, bounded(25) | 1024 B | 130.58 | 115.05 | -11.9% |
| Async request/response, bounded(4096), round trip | 256 B | 285.38 | 223.83 | -21.6% |
| Async request/response, bounded(4096), round trip | 1024 B | 405.60 | 406.55 | +0.2% |
| Sync SPSC, unbounded | 64 B | 13.45 | 12.56 | -6.6% |
| Sync SPSC, unbounded | 256 B | 34.17 | 36.98 | +8.2% |
| Sync two producers to one consumer, bounded(4096) | 256 B | 31.80 | 35.20 | +10.7% |
| Raw MPMC 4→4, bounded(1024) | 8 B | 9.29 | 9.06 | -2.5% |
| Raw ping-pong, round trip | 8 B | 80.67 | 80.62 | -0.1% |

The raw MPMC batch ranges overlap (baseline 8.49–10.32, candidate 7.87–9.74 ns),
so these final measurements do not establish a throughput win there. The separate
paused-reader tests establish the progress improvement.

### CPU placement limitation

The main comparison uses `--pin=0,1,2,3,4,5,6,7`, eight physical cores of one CCD.
The harness leaves workers beyond the pin list unpinned, and its single-thread raw
rows are also unpinned. Async rows use four pinned runtime workers. The 8-producer,
32-producer and synchronous 40-producer rows therefore do not have fully controlled
placement; their numbers must not be interpreted as a comparison on eight cores.

For example, bounded MPSC 8→1 appeared to regress from 10.61 to 53.96 ns in the
eight-entry runs. A focused rerun assigned all nine workers a CPU (`0..8`):
59.84 → 59.75 ns, with batch ranges 57.51–60.38 and 58.91–59.78. This shows why a
short pin list cannot support a performance claim for that topology. The follow-up
samples are included in the JSON.

## Reproduce

Run on a quiet Linux machine. Choose a CPU list from that machine's topology;
one entry is needed for every synchronous worker whose placement matters. Use
separate build directories even though the crate name and version are identical.

```sh
bench_dir=$(mktemp -d)
git worktree add --detach "$bench_dir/before" cf40507
git worktree add --detach "$bench_dir/after" 7b2a867

for bench_variant in before after; do
    RUSTFLAGS='-C target-cpu=native' \
    CARGO_TARGET_DIR="$bench_dir/target-$bench_variant" \
    cargo +1.97.1 test --release --locked \
        --manifest-path "$bench_dir/$bench_variant/Cargo.toml" \
        --no-run --bench raw_bench --bench real_case
done

# Repeat in interleaved before/after order, preserving each run's JSON output.
bench_variant=before
RUSTFLAGS='-C target-cpu=native' \
CARGO_TARGET_DIR="$bench_dir/target-$bench_variant" \
FCRS_RAW_BENCH_FULL=1 \
FCRS_RAW_BENCH_ARGS='--n=1000003 --filter=rapidfire --json --pin=0,1,2,3,4,5,6,7' \
cargo +1.97.1 test --release --locked \
    --manifest-path "$bench_dir/$bench_variant/Cargo.toml" \
    --bench raw_bench -- raw_bench_full --exact --nocapture --test-threads=1

RUSTFLAGS='-C target-cpu=native' \
CARGO_TARGET_DIR="$bench_dir/target-$bench_variant" \
FCRS_REAL_BENCH=1 \
FCRS_REAL_BENCH_ARGS='--n=500003 --filter=rapidfire --json --pin=0,1,2,3,4,5,6,7' \
cargo +1.97.1 test --release --locked \
    --manifest-path "$bench_dir/$bench_variant/Cargo.toml" \
    --bench real_case -- real_case_bench --exact --nocapture --test-threads=1
```

## Correctness checks

- 12 unit and 46 integration tests passed on the final Linux x86-64 build.
- The final Linux ARM core passed all 12 tests with both normal and 3-slot blocks.
- All 9 Loom models passed, including overlapping consumers and block recycling.
- All 10 non-threaded core tests passed Miri with 3-slot blocks and strict
  provenance. Native tests cover the two threaded core tests.
- Clippy with warnings denied, formatting, the doc test and benchmark checksums
  passed. The corrected harnesses and diagnostic example also passed the 4,003-item
  smoke run, which exercises message-count rounding.

The new deterministic tests pause a reader after claiming a slot, verify that its
block is not reused early, then exercise reclamation and destruction. A second
test pauses the head transition and verifies that a peer returns to its caller
and can still detect the queued value. These checks cover the waits removed by
this change; benchmark medians alone cannot demonstrate that property.
