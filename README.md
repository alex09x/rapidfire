# rapidfire

[![crates.io](https://img.shields.io/crates/v/rapidfire.svg)](https://crates.io/crates/rapidfire)
[![docs.rs](https://docs.rs/rapidfire/badge.svg)](https://docs.rs/rapidfire)
[![CI](https://github.com/alex09x/rapidfire/actions/workflows/ci.yml/badge.svg)](https://github.com/alex09x/rapidfire/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

An ultra low-latency, lock-free, **zero-dependency** async MPMC channel for Rust, built for
trading bots, WebSocket fan-in and other latency-critical pipelines where a lost message is
unacceptable and every nanosecond on the hot path counts.

The core is an in-crate lock-free block queue; the crate depends on nothing but `std`.

## Key features

- ⚡ **Fastest path measured**: 5 ns per push+pop and 5 ns per SPSC message on Zen 4;
  18 ns per SPSC message on Neoverse-N1 (see the tables below).
- 🧱 **Own lock-free block queue**: 63-slot blocks, one CAS (or one wait-free `fetch_add`)
  per claim, per-slot state words tagged with the lap number, blocks recycled through a
  pool so steady-state traffic never touches the allocator.
- 🧭 **Cache-line discipline (128 bytes)**: producers never read the consumers' line and
  consumers never read the producers' line; the only lines that cross cores per message
  are the slot lines carrying the values. Read marks live on a consumer-owned line.
- 💤 **Zero-cost sleep, no `SeqCst` on the hot path**: while messages flow nothing is
  locked and no waker is cloned; a producer does one `Relaxed` load after its claim. The
  party that parks pays with one RMW on the other side's index (release-sequence
  hand-off, model-checked with loom).
- 🔄 **Cancellation safe**: dropping a `Recv`/`Send` future (e.g. in `tokio::select!`)
  removes its waker; counters never drift.
- 🔌 **Drop-in API**: `unbounded`, `bounded`, `send`, `recv`, `try_send`, `try_recv`,
  `close`, `len`, counts — the same shape as `async-channel`.

## Benchmarks

Two harnesses live in `benches/`: `raw_bench.rs` (synchronous, thread-based, pinned, the
same code driving every implementation, `u64` payloads) and `real_case.rs` (the message
shapes and topologies of the trading bots this crate was written for, both as std threads
and as tokio tasks). `channel_benchmarks.rs` adds criterion/tokio async groups. All numbers
are medians of 5 runs after a warm-up; the full set for every machine and scenario, the
perf profiles and the notes on the cells we lose are in
[`benches/RESULTS.md`](benches/RESULTS.md).

These tables preserve the original 0.1.0 measurements. The current harness removes a
Tokio-only receiver mutex and the MPMC per-message completion counter, and starts timing
before releasing workers. See the methodology note in `benches/RESULTS.md`; use the current
harness for new comparisons rather than treating the historical ratios as corrected runs.

Toolchain and library versions: rustc 1.97.1 (8bab26f4f 2026-07-14), `-C target-cpu=native`,
`lto = "fat"`, `codegen-units = 1`; compared against crossbeam-queue 0.3.14 (`SegQueue`,
`ArrayQueue`), flume 0.11.1, async-channel 2.5.0, tokio 1.53.1 (`sync::mpsc`) and
`std::sync::mpsc` of the same toolchain.

### Machines

| # | machine | pinning |
|--:|:--|:--|
| 1 | AMD Ryzen 9 7900 · Zen 4 · 12C/24T · DDR5-4800 · Linux 6.8 lowlatency | one 6-core CCD |
| 2 | AMD Ryzen 9 7900 · Zen 4 · 12C/24T · DDR5-4800 · Linux 7.0 | one 6-core CCD |
| 3 | AMD Ryzen 9 7950X · Zen 4 · 16C/32T · DDR5-4800 · Linux 6.8 | 8 cores, one CCD |
| 4 | AMD Ryzen 9 7950X · Zen 4 · 16C/32T · Linux 6.8 | 8 cores, one CCD |
| 5 | AMD Ryzen 9 7950X · Zen 4 · 16C/32T · DDR5-3600 · Linux 5.15 (loaded box, load ≈ 18) | 8 cores, one CCD |
| 6 | AMD Ryzen 9 7950X3D · Zen 4 + 3D V-cache · 16C/32T · DDR5-3600 · Linux 5.15 | 8 cores, V-cache CCD |
| 7 | AMD Ryzen 9 7950X3D · Zen 4 + 3D V-cache · 16C/32T · Linux 6.8 | 8 cores, V-cache CCD |
| 8 | AMD Ryzen 9 9950X · Zen 5 · 16C/32T · DDR5-3600 · Linux 6.8 (loaded box, load ≈ 3–15) | 8 cores, one CCD |
| 9 | AMD Ryzen 9 3950X · Zen 2 · 16C/32T · Linux 5.19 | 8 cores, two CCXs |
| 10 | Intel Xeon E5-2699 v3 · Haswell · 2×18C/36T · DDR4 · Linux 5.15 | 8 cores, one socket |
| 11 | Ampere Altra · 128× Neoverse-N1 · ARMv8.2 (LSE) · DDR4-3200 · Linux 6.8 | 8 cores, one NUMA node |
| 12 | Apple M3 Pro · 5P+6E · LPDDR5 · macOS 26 | unpinned, QoS user-interactive |

### Raw harness, ns per message (lower is better)

**SPSC 1→1, unbounded**

| # | **rapidfire** | crossbeam SegQueue | std mpsc | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | **5.4** | 14.2 | 13.8 | 51.6 | 33.4 | 70.5 |
| 2 | **5.8** | 14.2 | 15.6 | 70.8 | 36.0 | 125.1 |
| 3 | **5.4** | 14.5 | 14.8 | 104.7 | 40.7 | 106.0 |
| 4 | **5.7** | 13.7 | 14.0 | 101.7 | 41.4 | 103.5 |
| 5 | **5.4** | 14.3 | 21.0 | 34.7 | 44.5 | 93.8 |
| 6 | **5.6** | 13.7 | 13.8 | 63.9 | 38.2 | 118.2 |
| 7 | **5.6** | 14.5 | 15.4 | 74.7 | 37.3 | 117.4 |
| 8 | **5.9** | 38.6 | 39.0 | 78.3 | 58.6 | 139.8 |
| 9 | **8.4** | 60.1 | 58.2 | 98.4 | 103.4 | 157.3 |
| 10 | **16.0** | 27.9 | 42.6 | 110.1 | 133.0 | 312.6 |
| 11 | **19.2** | 25.5 | 27.1 | 475.8 | 94.4 | 151.7 |
| 12 | 8.6 | **7.6** | 10.7 | 21.1 | 89.3 | 181.4 |

**SPSC 1→1, bounded(1024)**

| # | **rapidfire** | crossbeam ArrayQueue | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|
| 1 | **14.1** | 15.3 | 80.9 | 52.2 | 53.5 |
| 2 | **16.7** | 31.7 | 86.0 | 65.6 | 56.4 |
| 3 | **5.8** | 23.6 | 89.7 | 63.3 | 58.3 |
| 4 | **6.0** | 29.9 | 77.1 | 62.4 | 58.2 |
| 5 | **5.6** | 31.7 | 102.6 | 64.1 | 49.9 |
| 6 | **6.8** | 29.8 | 92.3 | 64.8 | 57.3 |
| 7 | **6.2** | 27.1 | 91.1 | 65.0 | 54.8 |
| 8 | **6.0** | 7.6 | 73.9 | 79.1 | 63.4 |
| 9 | 10.6 | **10.5** | 170.8 | 127.6 | 143.3 |
| 10 | **21.8** | 25.9 | 222.2 | 179.7 | 228.8 |
| 11 | **17.5** | 22.3 | 448.9 | 105.0 | 173.4 |
| 12 | 11.6 | **6.1** | 26.5 | 80.1 | 187.3 |

**MPSC 4→1, unbounded**

| # | **rapidfire** | crossbeam SegQueue | std mpsc | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | **6.9** | 7.3 | 50.6 | 77.4 | 92.9 | 107.3 |
| 2 | **7.0** | 8.5 | 55.2 | 58.6 | 93.4 | 112.0 |
| 3 | **6.7** | 6.8 | 52.4 | 82.1 | 94.8 | 105.5 |
| 4 | 6.6 | **6.2** | 50.3 | 80.5 | 94.6 | 102.1 |
| 5 | **6.3** | 6.8 | 52.1 | 19.8 | 62.1 | 106.6 |
| 6 | **6.7** | 7.5 | 50.7 | 63.4 | 91.9 | 110.6 |
| 7 | **6.6** | 7.7 | 49.4 | 63.4 | 122.8 | 109.5 |
| 8 | **9.0** | 45.9 | 54.4 | 93.7 | 117.6 | 115.3 |
| 9 | 22.5 | **17.4** | 51.4 | 129.5 | 176.1 | 120.0 |
| 10 | **63.0** | 183.5 | 145.2 | 216.8 | 279.9 | 254.1 |
| 11 | 48.9 | **37.3** | 236.0 | 409.8 | 297.8 | 217.7 |
| 12 | **9.2** | 9.5 | 81.3 | 31.5 | 104.8 | 89.0 |

**MPSC 4→1, bounded(1024)**

| # | **rapidfire** | crossbeam ArrayQueue | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|
| 1 | **8.4** | 12.9 | 254.7 | 123.0 | 160.6 |
| 2 | **8.3** | 19.9 | 194.2 | 136.8 | 154.8 |
| 3 | **8.2** | 13.2 | 269.0 | 106.8 | 150.0 |
| 4 | **8.0** | 13.8 | 241.8 | 112.7 | 144.9 |
| 5 | **7.2** | 13.2 | 262.3 | 102.4 | 165.1 |
| 6 | **8.3** | 13.0 | 258.7 | 110.6 | 159.4 |
| 7 | **7.6** | 12.3 | 236.6 | 109.1 | 165.6 |
| 8 | **11.8** | 24.5 | 290.9 | 116.5 | 144.7 |
| 9 | **52.9** | 129.4 | 380.5 | 242.2 | 226.7 |
| 10 | 128.0 | **100.2** | 759.0 | 270.9 | 378.5 |
| 11 | 75.9 | **68.9** | 372.4 | 300.5 | 284.3 |
| 12 | 14.4 | **10.5** | 101.3 | 132.3 | 157.2 |

**MPMC 4→4, unbounded**

| # | **rapidfire** | crossbeam SegQueue | flume | async-channel |
|--:|--:|--:|--:|--:|
| 1 | 28.9 | **19.0** | 251.3 | 184.3 |
| 2 | 23.3 | **13.2** | 184.8 | 147.0 |
| 3 | **7.5** | 8.3 | 175.4 | 129.7 |
| 4 | **7.4** | 8.1 | 166.3 | 119.7 |
| 5 | **7.4** | 7.9 | 160.7 | 60.0 |
| 6 | **7.1** | 9.6 | 122.7 | 117.5 |
| 7 | **7.8** | 9.9 | 117.6 | 93.4 |
| 8 | **11.8** | 13.7 | 166.9 | 140.9 |
| 9 | 20.4 | **20.1** | 201.8 | 212.8 |
| 10 | **99.8** | 188.5 | 404.3 | 313.9 |
| 11 | **36.0** | 71.7 | 448.4 | 296.6 |
| 12 | **15.4** | 25.2 | 49.6 | 67.5 |

**MPSC 32→1, unbounded** (market-data collectors: 10–40 WebSocket readers feeding one
writer; 32 producer threads on 12–18 cores, so this is oversubscribed like production)

| # | **rapidfire** | crossbeam SegQueue | std mpsc | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | **62.2** | 87.7 | 81.9 | 151.6 | 250.2 | 255.9 |
| 2 | **45.9** | 120.2 | 89.8 | 119.5 | 304.4 | 217.4 |
| 3 | **61.7** | 109.7 | 90.6 | 175.3 | 276.8 | 150.7 |
| 4 | 91.8 | 148.6 | **85.0** | 158.5 | 260.0 | 126.2 |
| 5 | 90.2 | **74.5** | 95.2 | 82.2 | 234.5 | 144.0 |
| 6 | 92.6 | 146.8 | **89.3** | 157.4 | 263.0 | 136.3 |
| 7 | 107.7 | 154.4 | **84.9** | 165.5 | 240.6 | 144.4 |
| 8 | **63.3** | 123.1 | 72.5 | 183.6 | 271.7 | 134.2 |
| 9 | 72.2 | 115.1 | **47.5** | 236.5 | 371.3 | 140.4 |
| 10 | 310.4 | 345.8 | 395.0 | **208.0** | 367.9 | 233.7 |
| 11 | 629.8 | 1615.1 | 1307.9 | 285.2 | 368.5 | **245.9** |
| 12 | **10.5** | 37.5 | 295.6 | 31.6 | 146.7 | 972.9 |

**MPMC 4→4, bounded(1024)**

| # | **rapidfire** | crossbeam ArrayQueue | flume | async-channel |
|--:|--:|--:|--:|--:|
| 1 | **41.1** | 205.6 | 251.7 | 253.1 |
| 2 | **30.2** | 204.1 | 188.1 | 195.4 |
| 3 | **31.4** | 86.9 | 179.6 | 111.9 |
| 4 | **30.2** | 84.2 | 144.9 | 102.2 |
| 5 | **30.4** | 79.0 | 182.5 | 92.0 |
| 6 | **34.1** | 89.8 | 156.4 | 97.6 |
| 7 | **32.2** | 87.1 | 136.5 | 97.6 |
| 8 | **38.4** | 86.5 | 180.1 | 140.1 |
| 9 | **42.2** | 220.7 | 391.5 | 298.0 |
| 10 | **125.9** | 172.6 | 388.3 | 353.1 |
| 11 | **53.2** | 73.0 | 540.4 | 297.4 |
| 12 | 86.2 | **51.6** | 66.8 | 87.7 |

**Ping-pong round trip (two channels, two threads)**

| # | **rapidfire** | crossbeam SegQueue | std mpsc | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | **92.1** | 119.7 | 119.4 | 224.0 | 142.8 | 249.6 |
| 2 | **83.9** | 110.6 | 108.1 | 206.4 | 134.9 | 241.4 |
| 3 | **81.1** | 113.9 | 116.3 | 221.5 | 142.4 | 249.9 |
| 4 | **81.3** | 110.6 | 112.6 | 221.1 | 134.9 | 229.0 |
| 5 | **78.6** | 111.6 | 126.5 | 243.5 | 166.1 | 234.0 |
| 6 | **82.1** | 114.6 | 120.3 | 222.9 | 136.4 | 235.2 |
| 7 | **82.3** | 119.4 | 118.4 | 224.6 | 141.7 | 235.2 |
| 8 | **70.7** | 120.0 | 109.1 | 175.0 | 144.4 | 240.4 |
| 9 | **95.8** | 160.4 | 175.0 | 354.9 | 172.3 | 309.9 |
| 10 | **209.8** | 370.5 | 363.4 | 1637.5 | 479.4 | 730.2 |
| 11 | **141.0** | 291.9 | 303.5 | 871.1 | 488.9 | 697.8 |
| 12 | **86.3** | 178.6 | 183.4 | 371.2 | 238.5 | 443.6 |

**1 thread, push+pop pair**

| # | **rapidfire** | crossbeam SegQueue | std mpsc | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | **5.3** | 8.1 | 8.3 | 6.5 | 22.1 | 12.8 |
| 2 | **5.1** | 7.7 | 7.7 | 6.3 | 20.9 | 12.9 |
| 3 | **5.4** | 9.0 | 9.7 | 7.6 | 25.1 | 14.3 |
| 4 | **5.3** | 8.0 | 8.1 | 6.4 | 21.2 | 12.8 |
| 5 | **5.1** | 8.1 | 13.4 | 6.7 | 33.0 | 16.6 |
| 6 | **6.1** | 8.7 | 8.7 | 7.1 | 22.3 | 13.5 |
| 7 | **4.9** | 8.0 | 7.9 | 6.2 | 20.3 | 12.3 |
| 8 | **9.4** | 20.3 | 19.8 | 16.5 | 57.1 | 30.2 |
| 9 | **14.9** | 28.2 | 29.3 | 24.0 | 82.1 | 42.8 |
| 10 | **21.2** | 38.0 | 39.1 | 33.1 | 101.1 | 54.7 |
| 11 | **21.1** | 34.6 | 39.6 | 37.8 | 101.1 | 57.5 |
| 12 | **5.5** | 6.5 | 9.7 | 12.0 | 27.0 | 11.7 |

**rapidfire versus the best other implementation in each cell** (×, >1 means rapidfire is
faster; the 8→1 rows and the burst row are in `RESULTS.md`)

| # | 1 thread, push+pop pair | 1 thread, burst of 1000 | SPSC 1→1, unbounded | SPSC 1→1, bounded(1024) | MPSC 4→1, unbounded | MPSC 4→1, bounded(1024) | MPSC 8→1, unbounded | MPSC 8→1, bounded(1024) | MPMC 4→4, unbounded | MPMC 4→4, bounded(1024) | MPSC 32→1, unbounded | MPSC 32→1, bounded(1024) | ping-pong round trip |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1.23 | 1.15 | 2.53 | 1.08 | 1.05 | 1.53 | 0.48 | 2.79 | 0.66 | 5.00 | 1.32 | 0.49 | 1.30 |
| 2 | 1.24 | 1.16 | 2.44 | 1.90 | 1.22 | 2.40 | 0.45 | 2.00 | 0.57 | 6.23 | 1.95 | 0.80 | 1.29 |
| 3 | 1.40 | 1.23 | 2.69 | 4.05 | 1.03 | 1.61 | 0.48 | 4.68 | 1.11 | 2.76 | 1.47 | 1.10 | 1.40 |
| 4 | 1.20 | 1.11 | 2.41 | 4.97 | 0.94 | 1.72 | 0.52 | 0.94 | 1.10 | 2.79 | 0.93 | 1.08 | 1.36 |
| 5 | 1.32 | 1.20 | 2.64 | 5.63 | 1.08 | 1.84 | 0.53 | 1.00 | 1.08 | 2.60 | 0.83 | 1.25 | 1.42 |
| 6 | 1.16 | 1.12 | 2.46 | 4.41 | 1.13 | 1.57 | 2.07 | 3.23 | 1.36 | 2.63 | 0.96 | 1.11 | 1.40 |
| 7 | 1.26 | 1.16 | 2.59 | 4.35 | 1.18 | 1.63 | 0.66 | 3.48 | 1.27 | 2.70 | 0.79 | 1.21 | 1.44 |
| 8 | 1.76 | 1.91 | 6.51 | 1.25 | 5.08 | 2.08 | 2.67 | 1.15 | 1.17 | 2.26 | 1.15 | 1.26 | 1.54 |
| 9 | 1.61 | 1.77 | 6.91 | 0.99 | 0.77 | 2.45 | 1.93 | 3.31 | 0.99 | 5.23 | 0.66 | 1.81 | 1.67 |
| 10 | 1.56 | 1.45 | 1.75 | 1.18 | 2.31 | 0.78 | 1.00 | 0.67 | 1.89 | 1.37 | 0.67 | 0.81 | 1.73 |
| 11 | 1.64 | 1.64 | 1.33 | 1.27 | 0.76 | 0.91 | 2.24 | 1.36 | 1.99 | 1.37 | 0.39 | 0.64 | 2.07 |
| 12 | 1.19 | 1.22 | 0.88 | 0.53 | 1.03 | 0.73 | 2.07 | 0.24 | 1.64 | 0.60 | 3.01 | 0.14 | 2.07 |

### Real bot topologies

**AMD Ryzen 9 7900 (Zen 4), one 6-core CCD (40 readers: all 12 cores) — std threads, spinning** (ns per message; round trip for command/response; lower is better)

| topology | bytes | **rapidfire** | async-channel | tokio mpsc | flume |
|:--|--:|--:|--:|--:|--:|
| WS reader → bot, bounded(4096) | 64 | 39 | **32** | 64 | 165 |
| WS reader → bot, bounded(4096) | 256 | **34** | 43 | 74 | 152 |
| WS reader → bot, bounded(4096) | 1024 | 86 | **85** | 88 | 202 |
| WS reader → bot, bounded(25) | 64 | **47** | 73 | 72 | 151 |
| WS reader → bot, bounded(25) | 256 | **49** | 88 | 84 | 156 |
| WS reader → bot, bounded(25) | 1024 | **89** | 105 | 108 | 200 |
| WS reader → bot, unbounded | 64 | **13** | 24 | 70 | 81 |
| WS reader → bot, unbounded | 256 | **15** | 39 | 62 | 126 |
| WS reader → bot, unbounded | 1024 | 86 | **71** | 91 | 345 |
| 2 readers → bot, bounded(4096) | 64 | **36** | 102 | 109 | 325 |
| 2 readers → bot, bounded(4096) | 256 | **29** | 106 | 114 | 304 |
| 2 readers → bot, bounded(4096) | 1024 | 87 | **65** | 87 | 275 |
| 40 WS readers → writer, unbounded (market-data collector) | 64 | **99** | 267 | 262 | 281 |
| 40 WS readers → writer, unbounded (market-data collector) | 256 | **150** | 325 | 281 | 484 |
| 40 WS readers → writer, unbounded (market-data collector) | 1024 | **241** | 356 | 398 | 802 |
| command/response round trip, bounded(4096) | 64 | **102** | 179 | 279 | 285 |
| command/response round trip, bounded(4096) | 256 | **132** | 219 | 327 | 304 |
| command/response round trip, bounded(4096) | 1024 | **246** | 384 | 998 | 360 |

**AMD Ryzen 9 7900 (Zen 4), one 6-core CCD (40 readers: all 12 cores) — tokio tasks, 4 workers, async send/recv** (ns per message; round trip for command/response; lower is better)

| topology | bytes | **rapidfire** | async-channel | tokio mpsc | flume |
|:--|--:|--:|--:|--:|--:|
| WS reader → bot, bounded(4096) | 64 | **34** | 40 | 56 | 107 |
| WS reader → bot, bounded(4096) | 256 | 87 | **51** | 74 | 95 |
| WS reader → bot, bounded(4096) | 1024 | 121 | **107** | 126 | 113 |
| WS reader → bot, bounded(25) | 64 | **33** | 76 | 52 | 49 |
| WS reader → bot, bounded(25) | 256 | **62** | 100 | 72 | 68 |
| WS reader → bot, bounded(25) | 1024 | **124** | 181 | 138 | 149 |
| WS reader → bot, unbounded | 64 | 48 | 82 | **48** | 55 |
| WS reader → bot, unbounded | 256 | **65** | 132 | 83 | 138 |
| WS reader → bot, unbounded | 1024 | 225 | 550 | **197** | 490 |
| 2 readers → bot, bounded(4096) | 64 | **24** | 80 | 88 | 122 |
| 2 readers → bot, bounded(4096) | 256 | **43** | 55 | 89 | 82 |
| 2 readers → bot, bounded(4096) | 1024 | **68** | 107 | 94 | 115 |
| 40 WS readers → writer, unbounded (market-data collector) | 64 | **43** | 92 | 104 | 132 |
| 40 WS readers → writer, unbounded (market-data collector) | 256 | **58** | 71 | 135 | 215 |
| 40 WS readers → writer, unbounded (market-data collector) | 1024 | **127** | 189 | 367 | 642 |
| command/response round trip, bounded(4096) | 64 | **192** | 364 | 255 | 256 |
| command/response round trip, bounded(4096) | 256 | **253** | 487 | 350 | 353 |
| command/response round trip, bounded(4096) | 1024 | **458** | 741 | 479 | 546 |

The same tables for the Ryzen 9 7950X, the Neoverse-N1 box and the M3 Pro are in
`RESULTS.md`, together with the explanation of the cells where async-channel or
`ArrayQueue` is ahead and what is planned for them.

## How it works

`src/queue.rs` documents the protocol in full; the short version:

- Positions are absolute indices; a block serves one *lap* of 64 positions, the last of
  which is a sentinel that never holds a value and marks a block transition.
- A producer claims a position (wait-free `fetch_add` while no contention has been seen,
  CAS otherwise — with several producers CAS serialises the claims so adjacent-slot
  writes do not fight over one cache line, which is 4× faster than `fetch_add` on both
  Zen 4 and N1), locates its block by walking `prev` links back from `tail.block`,
  writes the value and publishes the slot with a Release store.
- A consumer looks at the head slot's lap-tagged state and claims it with a CAS only once
  it is written, so it never waits on a producer and the head never moves past an
  unwritten slot. The consumer that takes the last slot moves the head to the next block
  and recycles the old block if its other readers have finished. A still-busy block is retired
  without waiting and reclaimed by an installing producer once all readers are done.
  Waiting for a paused head transition also has a bounded spin budget, so an async
  receiver can return control to its executor.
- Blocks are never freed while the channel lives (a spare slot plus a pool), which is
  what makes stale block pointers safe to inspect. Memory stays at the channel's
  high-water mark until it is dropped.
- Sleeping: a receiver registers its waker, bumps a counter and does `fetch_add(0)` on
  the tail index; every later claim synchronises with it, so producers only need a
  `Relaxed` load of the counter after their claim. A slot that was claimed before the
  registration but not yet written makes the receiver poll briefly and then yield to its
  executor instead of sleeping.

## Generated code

`examples/asm_probe.rs` wraps `try_send`/`try_recv` in never-inlined functions so the hot
paths can be read with `objdump`. On x86-64 (Zen 4) an unbounded `try_send` is one
`lock xadd` (or one `lock cmpxchg` once producer contention has been observed) plus about
twenty ordinary instructions: the closed byte, the capacity word, the contention word, the
tail block and its start position, the value store and a plain `mov` to publish the slot.
`try_recv` is one `lock cmpxchg` plus about twenty instructions. On AArch64 the same paths
are one LSE atomic (`ldaddal`/`casal`) with `ldapr` loads and one `stlr`. Everything that
is not on that path (contended claims, the bounded capacity check, the block walk, the
block transition) is out of line, so the inlined code at a call site stays small. Three
alternative publish sequences were measured and rejected (`dmb ishst` + plain store,
`fence(Release)` + plain store, and an RMW publish); they remain behind `--cfg` switches.
`perf` profiles on Zen 4 and Neoverse-N1 (in `RESULTS.md`) show the remaining time in the
locked claim and in the slot-line transfers between cores, not in the generated code.

## Verification

- `cargo test` — 12 unit, 46 integration (std threads and tokio: MPMC stress with every
  value received exactly once, bounded back-pressure across block boundaries, cancellation
  storms in `select!`, close while parked, Drop accounting, zero-sized and large payloads,
  waker re-registration, Debug/Display) and doc tests; also run with
  `RUSTFLAGS="--cfg fcrs_small_blocks"` (3-slot blocks) so every test crosses block
  boundaries and recycles blocks constantly. The 0.1.0 `cargo llvm-cov` run reported
  97.8 % of lines and 98.3 % of functions of the core covered.
- `cargo test --release --features loom --test loom` — loom model checks of the
  producer/consumer protocol, block transitions, close and the sleep/wake hand-off.
- `cargo +nightly miri test --lib` (with the small-block cfg) — no undefined behaviour in
  the unsafe core.
- GitHub Actions (`.github/workflows/ci.yml`) runs fmt, clippy, the test suite with both
  block sizes, loom and miri on x86-64 Linux, AArch64 Linux and macOS, plus a manual
  benchmark job (virtual machines: indicative numbers only).
- Multi-hour hang/crash hunts on the 128-core ARM box are what found and fixed the two
  bugs in the first drafts (a null `prev` on a recycled block; consumers passing an
  unwritten slot stranding a pre-empted producer's block lookup).
- An independent adversarial review of the memory-ordering arguments found two more,
  both now covered by regression tests: a `Recv`/`Send` future cancelled right after
  being woken swallowed the wake-up (it is now forwarded to the next parked waiter), and
  the block transition's index update was a plain store, which ends the release
  sequence a parked bounded sender relies on (it is an RMW now).

Known, documented race (shared with tokio's and async-channel's channels): a send that
observed the channel open and then races with `close()` may succeed after the receivers
already reported `Closed`; that value is dropped with the channel.

## Quick start

```toml
[dependencies]
rapidfire = "0.1"
```

```rust
use rapidfire::unbounded;

#[tokio::main]
async fn main() {
    let (tx, rx) = unbounded::<String>();

    tokio::spawn(async move {
        tx.send("BTC-USDT orderbook update".to_string()).await.unwrap();
    });

    if let Ok(msg) = rx.recv().await {
        println!("Received: {}", msg);
    }
}
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT License](LICENSE-MIT) at your option.
