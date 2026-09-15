# Benchmark results (rapidfire 0.1.0)

All numbers were produced by the harnesses in this directory on the machines listed
below: `raw_bench.rs` (`u64` payloads, 4 M messages per run, median of 5 runs after a
warm-up) and `real_case.rs` (trading-bot topologies). Threads are pinned to the cores
named in the "pinning" column (Linux `sched_setaffinity`; macOS has no affinity API, so the
benchmark threads only ask for performance cores). The bold value is the best in its row.

Toolchain and library versions: rustc 1.97.1 (8bab26f4f 2026-07-14), `-C target-cpu=native`,
`lto = "fat"`, `codegen-units = 1`; compared against crossbeam-queue 0.3.14 (`SegQueue`,
`ArrayQueue`), flume 0.11.1, async-channel 2.5.0, tokio 1.53.1 (`sync::mpsc`) and
`std::sync::mpsc` of the same toolchain.

Two machines were busy with production work during the runs (marked "loaded box"); their
numbers are indicative only. The 8→1 scenarios run nine threads on eight (or six) pinned
cores and are therefore oversubscribed on every machine; see the notes below.

## Raw harness (`raw_bench.rs`)

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

**1 thread, push+pop pair** — ns per operation, median of 5 runs, lower is better

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

**1 thread, burst of 1000** — ns per operation, median of 5 runs, lower is better

| # | **rapidfire** | crossbeam SegQueue | std mpsc | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | **5.9** | 6.9 | 7.3 | 6.8 | 20.5 | 13.2 |
| 2 | **5.7** | 6.7 | 7.1 | 6.6 | 19.7 | 12.8 |
| 3 | **6.1** | 7.5 | 8.5 | 7.7 | 23.0 | 14.3 |
| 4 | **6.0** | 6.8 | 7.2 | 6.7 | 20.6 | 13.2 |
| 5 | **5.7** | 6.9 | 13.5 | 12.6 | 26.2 | 19.2 |
| 6 | **6.6** | 7.4 | 8.3 | 7.6 | 21.0 | 14.2 |
| 7 | **5.4** | 6.5 | 7.1 | 6.3 | 19.5 | 12.6 |
| 8 | **8.6** | 16.5 | 16.7 | 16.5 | 52.4 | 30.4 |
| 9 | **13.3** | 23.6 | 24.2 | 24.3 | 77.6 | 42.5 |
| 10 | **22.9** | 34.1 | 35.3 | 33.1 | 97.1 | 55.8 |
| 11 | **20.1** | 32.8 | 36.4 | 36.5 | 98.2 | 57.5 |
| 12 | **8.0** | 9.8 | 10.8 | 12.3 | 27.2 | 14.6 |

**SPSC 1→1, unbounded** — ns per operation, median of 5 runs, lower is better

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

**SPSC 1→1, bounded(1024)** — ns per operation, median of 5 runs, lower is better

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

**MPSC 4→1, unbounded** — ns per operation, median of 5 runs, lower is better

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

**MPSC 4→1, bounded(1024)** — ns per operation, median of 5 runs, lower is better

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

**MPSC 8→1, unbounded** — ns per operation, median of 5 runs, lower is better

| # | **rapidfire** | crossbeam SegQueue | std mpsc | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | 32.8 | **15.6** | 70.6 | 147.4 | 217.8 | 180.8 |
| 2 | 34.0 | **15.1** | 78.1 | 97.8 | 229.6 | 183.2 |
| 3 | 15.3 | **7.4** | 31.9 | 94.1 | 128.8 | 173.1 |
| 4 | 30.0 | **15.7** | 31.0 | 104.1 | 154.6 | 157.8 |
| 5 | 29.0 | **15.3** | 30.4 | 68.1 | 62.0 | 164.1 |
| 6 | **8.2** | 16.9 | 33.2 | 91.2 | 147.0 | 187.1 |
| 7 | 26.3 | **17.4** | 32.0 | 67.0 | 156.7 | 178.9 |
| 8 | **10.9** | 98.0 | 29.1 | 96.9 | 166.1 | 180.8 |
| 9 | **33.1** | 118.4 | 64.0 | 155.4 | 331.3 | 152.2 |
| 10 | 193.1 | 287.5 | **192.3** | 271.2 | 323.0 | 213.2 |
| 11 | **65.6** | 146.8 | 440.4 | 406.5 | 384.4 | 232.6 |
| 12 | **14.8** | 38.9 | 250.7 | 30.6 | 142.7 | 265.5 |

**MPSC 8→1, bounded(1024)** — ns per operation, median of 5 runs, lower is better

| # | **rapidfire** | crossbeam ArrayQueue | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|
| 1 | **52.7** | 147.4 | 519.6 | 272.2 | 231.8 |
| 2 | **54.8** | 109.8 | 466.6 | 265.8 | 195.7 |
| 3 | **12.1** | 56.8 | 996.8 | 173.3 | 178.7 |
| 4 | 57.7 | **54.1** | 439.4 | 262.1 | 181.4 |
| 5 | 56.0 | **55.9** | 598.4 | 152.0 | 173.3 |
| 6 | **56.3** | 186.9 | 514.9 | 279.7 | 181.7 |
| 7 | **51.1** | 178.1 | 361.1 | 283.1 | 184.4 |
| 8 | **44.5** | 51.2 | 289.8 | 222.7 | 247.9 |
| 9 | **72.4** | 239.4 | 504.7 | 393.9 | 303.3 |
| 10 | 227.6 | **153.2** | 1290.8 | 333.9 | 543.7 |
| 11 | **157.4** | 214.8 | 489.6 | 394.0 | 588.9 |
| 12 | 68.6 | **16.3** | 241.8 | 261.8 | 549.3 |

**MPMC 4→4, unbounded** — ns per operation, median of 5 runs, lower is better

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

**MPMC 4→4, bounded(1024)** — ns per operation, median of 5 runs, lower is better

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

**MPSC 32→1, unbounded** — ns per operation, median of 5 runs, lower is better

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

**MPSC 32→1, bounded(1024)** — ns per operation, median of 5 runs, lower is better

| # | **rapidfire** | crossbeam ArrayQueue | flume | async-channel | tokio mpsc |
|--:|--:|--:|--:|--:|--:|
| 1 | 440.4 | **216.6** | 2420.5 | 330.0 | 538.0 |
| 2 | 247.0 | **197.1** | 1748.4 | 336.2 | 552.5 |
| 3 | **264.3** | 291.3 | 2540.3 | 383.5 | 422.3 |
| 4 | **274.5** | 297.5 | 2395.8 | 339.4 | 422.8 |
| 5 | **256.7** | 322.2 | 1979.3 | 350.4 | 694.9 |
| 6 | **228.2** | 254.0 | 2484.6 | 286.2 | 459.7 |
| 7 | **205.6** | 248.6 | 2233.7 | 306.6 | 405.2 |
| 8 | **167.4** | 211.2 | 3404.2 | 330.7 | 452.4 |
| 9 | **146.3** | 264.5 | 2985.0 | 485.2 | 380.1 |
| 10 | 421.5 | 508.1 | 5657.7 | **343.5** | 914.8 |
| 11 | 1368.0 | **879.7** | 1985.4 | 971.2 | 1266.1 |
| 12 | 222.2 | **32.0** | 894.6 | 850.7 | 1703.1 |

**ping-pong round trip** — ns per operation, median of 5 runs, lower is better

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

**rapidfire versus the best other implementation in each cell** (×, >1 means rapidfire is faster)

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


Notes on the raw harness:

- `std mpsc` and `tokio mpsc` have a single receiver, so MPMC is n/a for them; the
  `ArrayQueue` rows only exist for the bounded scenarios; `SegQueue` is unbounded only.
- MPSC 8→1 is oversubscribed (nine threads on eight or six pinned cores). rapidfire's
  consumer never waits on a producer: when a producer is pre-empted mid-push the consumer
  returns `Empty` and the benchmark spins without yielding, while `SegQueue`'s consumer
  claims the slot and its wait loop yields the CPU to the pre-empted producer. With one
  core per thread (12 cores on the Ryzen 9 7900) the two tie at ≈27 ns.
- In the bounded multi-producer scenarios a producer that finds the queue full spins on
  `try_send`; every retry re-reads the consumer's head index, which costs the consumer a
  cache-line transfer per pop. `ArrayQueue` (a Vyukov ring) polls the slot instead of the
  index and wins those cells; `send().await` parks and does not have the problem. A
  ring-buffer bounded variant is the planned fix.
- The Apple M2 Ultra development machine is not listed: it carried a load average of
  15–40 from other work during every attempt.

## Real bot topologies (`real_case.rs`)

The trading bots this channel is written for use: a WebSocket reader task feeding a bot
loop through `bounded(4096)` command/response channels, an unbounded message channel, and
FIX/CTS style streams with `bounded(20..25)`; message structs are 70–300 bytes. The
harness reproduces those shapes with 64 / 256 / 1024-byte `#[repr(C)]` payloads, once
with std threads (spinning on empty/full) and once as tokio tasks on a 4-worker runtime
using the async `send`/`recv`. 500 000 messages per run (50 000 round trips for
command/response), median of 5 runs.

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

**AMD Ryzen 9 7950X (Zen 4), 8 cores of one CCD (40 readers: 16 cores) — std threads, spinning** (ns per message; round trip for command/response; lower is better)

| topology | bytes | **rapidfire** | async-channel | tokio mpsc | flume |
|:--|--:|--:|--:|--:|--:|
| WS reader → bot, bounded(4096) | 64 | **18** | 43 | 62 | 154 |
| WS reader → bot, bounded(4096) | 256 | **40** | 46 | 68 | 181 |
| WS reader → bot, bounded(4096) | 1024 | 87 | **55** | 95 | 196 |
| WS reader → bot, bounded(25) | 64 | **39** | 68 | 73 | 129 |
| WS reader → bot, bounded(25) | 256 | **34** | 83 | 75 | 180 |
| WS reader → bot, bounded(25) | 1024 | 87 | **77** | 100 | 191 |
| WS reader → bot, unbounded | 64 | **10** | 23 | 105 | 81 |
| WS reader → bot, unbounded | 256 | 40 | **39** | 86 | 143 |
| WS reader → bot, unbounded | 1024 | 89 | **78** | 98 | 316 |
| 2 readers → bot, bounded(4096) | 64 | **18** | 96 | 107 | 342 |
| 2 readers → bot, bounded(4096) | 256 | **37** | 100 | 107 | 306 |
| 2 readers → bot, bounded(4096) | 1024 | 87 | 87 | **86** | 276 |
| 40 WS readers → writer, unbounded (market-data collector) | 64 | **94** | 285 | 195 | 309 |
| 40 WS readers → writer, unbounded (market-data collector) | 256 | 164 | 315 | **161** | 499 |
| 40 WS readers → writer, unbounded (market-data collector) | 1024 | **213** | 317 | 339 | 655 |
| command/response round trip, bounded(4096) | 64 | **85** | 163 | 262 | 240 |
| command/response round trip, bounded(4096) | 256 | **107** | 196 | 310 | 308 |
| command/response round trip, bounded(4096) | 1024 | **252** | 346 | 824 | 340 |

**AMD Ryzen 9 7950X (Zen 4), 8 cores of one CCD (40 readers: 16 cores) — tokio tasks, 4 workers, async send/recv** (ns per message; round trip for command/response; lower is better)

| topology | bytes | **rapidfire** | async-channel | tokio mpsc | flume |
|:--|--:|--:|--:|--:|--:|
| WS reader → bot, bounded(4096) | 64 | **27** | 58 | 64 | 95 |
| WS reader → bot, bounded(4096) | 256 | **31** | 40 | 73 | 126 |
| WS reader → bot, bounded(4096) | 1024 | 120 | **99** | 122 | 112 |
| WS reader → bot, bounded(25) | 64 | **47** | 72 | 64 | 52 |
| WS reader → bot, bounded(25) | 256 | **61** | 83 | 80 | 69 |
| WS reader → bot, bounded(25) | 1024 | **115** | 164 | 137 | 148 |
| WS reader → bot, unbounded | 64 | 57 | 73 | **55** | 60 |
| WS reader → bot, unbounded | 256 | **69** | 157 | 86 | 126 |
| WS reader → bot, unbounded | 1024 | **203** | 485 | 223 | 439 |
| 2 readers → bot, bounded(4096) | 64 | 53 | **34** | 87 | 104 |
| 2 readers → bot, bounded(4096) | 256 | 38 | **37** | 90 | 102 |
| 2 readers → bot, bounded(4096) | 1024 | **76** | 112 | 90 | 119 |
| 40 WS readers → writer, unbounded (market-data collector) | 64 | **30** | 116 | 124 | 152 |
| 40 WS readers → writer, unbounded (market-data collector) | 256 | **44** | 105 | 162 | 241 |
| 40 WS readers → writer, unbounded (market-data collector) | 1024 | **134** | 183 | 324 | 671 |
| command/response round trip, bounded(4096) | 64 | **189** | 304 | 195 | 265 |
| command/response round trip, bounded(4096) | 256 | **240** | 366 | 266 | 277 |
| command/response round trip, bounded(4096) | 1024 | **397** | 607 | 439 | 589 |

**Ampere Altra 128× Neoverse-N1 (ARM64), 8 cores (40 readers: 40 cores) — std threads, spinning** (ns per message; round trip for command/response; lower is better)

| topology | bytes | **rapidfire** | async-channel | tokio mpsc | flume |
|:--|--:|--:|--:|--:|--:|
| WS reader → bot, bounded(4096) | 64 | **48** | 96 | 199 | 378 |
| WS reader → bot, bounded(4096) | 256 | **98** | 136 | 182 | 390 |
| WS reader → bot, bounded(4096) | 1024 | **128** | 303 | 228 | 565 |
| WS reader → bot, bounded(25) | 64 | **73** | 110 | 202 | 398 |
| WS reader → bot, bounded(25) | 256 | **96** | 127 | 191 | 413 |
| WS reader → bot, bounded(25) | 1024 | **128** | 205 | 230 | 492 |
| WS reader → bot, unbounded | 64 | **61** | 78 | 219 | 207 |
| WS reader → bot, unbounded | 256 | **99** | 144 | 220 | 396 |
| WS reader → bot, unbounded | 1024 | **127** | 166 | 234 | 740 |
| 2 readers → bot, bounded(4096) | 64 | **87** | 195 | 256 | 573 |
| 2 readers → bot, bounded(4096) | 256 | **143** | 217 | 232 | 622 |
| 2 readers → bot, bounded(4096) | 1024 | **136** | 258 | 309 | 802 |
| 40 WS readers → writer, unbounded (market-data collector) | 64 | 897 | 413 | **245** | 463 |
| 40 WS readers → writer, unbounded (market-data collector) | 256 | 876 | 428 | **260** | 604 |
| 40 WS readers → writer, unbounded (market-data collector) | 1024 | 778 | 430 | **400** | 1317 |
| command/response round trip, bounded(4096) | 64 | **158** | 385 | 776 | 985 |
| command/response round trip, bounded(4096) | 256 | **242** | 503 | 956 | 1116 |
| command/response round trip, bounded(4096) | 1024 | **497** | 802 | 2132 | 1678 |

**Ampere Altra 128× Neoverse-N1 (ARM64), 8 cores (40 readers: 40 cores) — tokio tasks, 4 workers, async send/recv** (ns per message; round trip for command/response; lower is better)

| topology | bytes | **rapidfire** | async-channel | tokio mpsc | flume |
|:--|--:|--:|--:|--:|--:|
| WS reader → bot, bounded(4096) | 64 | **39** | 159 | 181 | 230 |
| WS reader → bot, bounded(4096) | 256 | **129** | 174 | 231 | 339 |
| WS reader → bot, bounded(4096) | 1024 | **343** | 421 | 382 | 374 |
| WS reader → bot, bounded(25) | 64 | **81** | 190 | 147 | 114 |
| WS reader → bot, bounded(25) | 256 | **127** | 229 | 190 | 150 |
| WS reader → bot, bounded(25) | 1024 | **347** | 454 | 355 | 411 |
| WS reader → bot, unbounded | 64 | **80** | 177 | 116 | 95 |
| WS reader → bot, unbounded | 256 | **194** | 261 | 230 | 236 |
| WS reader → bot, unbounded | 1024 | **414** | 794 | 465 | 707 |
| 2 readers → bot, bounded(4096) | 64 | **39** | 151 | 230 | 353 |
| 2 readers → bot, bounded(4096) | 256 | **135** | 219 | 257 | 458 |
| 2 readers → bot, bounded(4096) | 1024 | **197** | 348 | 341 | 523 |
| 40 WS readers → writer, unbounded (market-data collector) | 64 | **70** | 271 | 205 | 309 |
| 40 WS readers → writer, unbounded (market-data collector) | 256 | **178** | 258 | 268 | 418 |
| 40 WS readers → writer, unbounded (market-data collector) | 1024 | 475 | **371** | 683 | 1065 |
| command/response round trip, bounded(4096) | 64 | **594** | 960 | 696 | 690 |
| command/response round trip, bounded(4096) | 256 | **781** | 1196 | 813 | 825 |
| command/response round trip, bounded(4096) | 1024 | **1060** | 1897 | 1285 | 1559 |

**Apple M3 Pro, unpinned — std threads, spinning** (ns per message; round trip for command/response; lower is better)

| topology | bytes | **rapidfire** | async-channel | tokio mpsc | flume |
|:--|--:|--:|--:|--:|--:|
| WS reader → bot, bounded(4096) | 64 | **29** | 77 | 173 | 29 |
| WS reader → bot, bounded(4096) | 256 | 73 | 131 | 145 | **58** |
| WS reader → bot, bounded(4096) | 1024 | 153 | **81** | 127 | 647 |
| WS reader → bot, bounded(25) | 64 | **30** | 86 | 175 | 249 |
| WS reader → bot, bounded(25) | 256 | **74** | 127 | 153 | 257 |
| WS reader → bot, bounded(25) | 1024 | **210** | 269 | 211 | 769 |
| WS reader → bot, unbounded | 64 | **29** | 64 | 177 | 29 |
| WS reader → bot, unbounded | 256 | **74** | 95 | 121 | 81 |
| WS reader → bot, unbounded | 1024 | **63** | 81 | 124 | 265 |
| 2 readers → bot, bounded(4096) | 64 | **31** | 127 | 124 | 78 |
| 2 readers → bot, bounded(4096) | 256 | **79** | 141 | 136 | 212 |
| 2 readers → bot, bounded(4096) | 1024 | **135** | 186 | 139 | 1136 |
| 40 WS readers → writer, unbounded (market-data collector) | 64 | **13** | 173 | 583 | 47 |
| 40 WS readers → writer, unbounded (market-data collector) | 256 | **35** | 208 | 1008 | 112 |
| 40 WS readers → writer, unbounded (market-data collector) | 1024 | **91** | 280 | 1228 | 414 |
| command/response round trip, bounded(4096) | 64 | **125** | 250 | 437 | 437 |
| command/response round trip, bounded(4096) | 256 | **201** | 282 | 1074 | 1198 |
| command/response round trip, bounded(4096) | 1024 | 381 | **376** | 1884 | 2724 |

**Apple M3 Pro, unpinned — tokio tasks, 4 workers, async send/recv** (ns per message; round trip for command/response; lower is better)

| topology | bytes | **rapidfire** | async-channel | tokio mpsc | flume |
|:--|--:|--:|--:|--:|--:|
| WS reader → bot, bounded(4096) | 64 | **19** | 65 | 112 | 49 |
| WS reader → bot, bounded(4096) | 256 | **45** | 58 | 88 | 64 |
| WS reader → bot, bounded(4096) | 1024 | **91** | 118 | 161 | 208 |
| WS reader → bot, bounded(25) | 64 | **34** | 60 | 47 | 45 |
| WS reader → bot, bounded(25) | 256 | **59** | 102 | 79 | 78 |
| WS reader → bot, bounded(25) | 1024 | **153** | 256 | 177 | 192 |
| WS reader → bot, unbounded | 64 | **17** | 40 | 111 | 33 |
| WS reader → bot, unbounded | 256 | 66 | 74 | **49** | 87 |
| WS reader → bot, unbounded | 1024 | 179 | 199 | **149** | 385 |
| 2 readers → bot, bounded(4096) | 64 | **12** | 95 | 118 | 112 |
| 2 readers → bot, bounded(4096) | 256 | **30** | 54 | 144 | 109 |
| 2 readers → bot, bounded(4096) | 1024 | **68** | 119 | 156 | 452 |
| 40 WS readers → writer, unbounded (market-data collector) | 64 | **15** | 73 | 109 | 74 |
| 40 WS readers → writer, unbounded (market-data collector) | 256 | **31** | 112 | 106 | 161 |
| 40 WS readers → writer, unbounded (market-data collector) | 1024 | **95** | 204 | 169 | 531 |
| command/response round trip, bounded(4096) | 64 | **154** | 326 | 170 | 236 |
| command/response round trip, bounded(4096) | 256 | **227** | 377 | 272 | 369 |
| command/response round trip, bounded(4096) | 1024 | **509** | 677 | 523 | 644 |


Notes on the real-case harness:

- In the synchronous bounded cases a producer that finds the channel full spins on
  `try_send` (see above); a real sender parks (`send().await`) instead.
- In the async cells with 256–1024-byte payloads async-channel is ahead on Zen 4 once the
  consumer keeps catching up with the producer and parks often: rapidfire's park/unpark
  goes through a mutex-protected waker list. A single-waiter atomic fast path (the way
  tokio's `AtomicWaker` works) is the planned fix.

## Where the time goes (perf)

`perf stat` and `perf record`/`annotate` on the `quick` example, 2 M messages, one run,
rapidfire versus crossbeam `SegQueue` (the closest competitor), same pinned cores.
No inline assembly is used in the shipped build: the hot paths are the compiler's own
atomics, checked instruction by instruction with `objdump` (`examples/asm_probe.rs`).

**AMD Ryzen 9 7950X (Zen 4), 8 cores of one CCD**

| scenario | impl | ns/msg | cycles | instructions | IPC | L1d load misses |
|:--|:--|--:|--:|--:|--:|--:|
| SPSC | rapidfire | 4.6 | 98 M | 213 M | 2.2 | 2.3 M |
| SPSC | SegQueue | 15.2 | 218 M | 372 M | 1.7 | 2.3 M |
| MPSC 4→1 | rapidfire | 5.3 | 246 M | 222 M | 0.9 | 1.5 M |
| MPSC 4→1 | SegQueue | 7.3 | 322 M | 331 M | 1.0 | 1.9 M |
| MPMC 4→4 | rapidfire | 7.2 | 611 M | 328 M | 0.5 | 3.2 M |
| MPMC 4→4 | SegQueue | 8.2 | 559 M | 365 M | 0.7 | 2.5 M |

**Ampere Altra (Neoverse-N1), 8 cores of one NUMA node**

| scenario | impl | ns/msg | cycles | instructions | IPC | L2 refills |
|:--|:--|--:|--:|--:|--:|--:|
| SPSC | rapidfire | 25.4 | 306 M | 214 M | 0.7 | 1.2 M |
| SPSC | SegQueue | 46.2 | 377 M | 372 M | 1.0 | 1.0 M |
| MPSC 4→1 | rapidfire | 34.5 | 822 M | 282 M | 0.3 | 2.0 M |
| MPSC 4→1 | SegQueue | 37.3 | 882 M | 328 M | 0.4 | 2.7 M |
| MPMC 4→4 | rapidfire | 35.5 | 1 165 M | 424 M | 0.4 | 2.2 M |
| MPMC 4→4 | SegQueue | 116.5 | 3 137 M | 540 M | 0.2 | 11.1 M |

What the samples say:

- **SPSC**: the two queues miss the cache equally often (the slot lines must cross cores
  either way); rapidfire simply executes 43 % fewer instructions and half the locked
  operations per message, hence 2.2 instructions per cycle on Zen 4. The hottest
  instruction on the consumer is the `lock cmpxchg` that claims the head (18 % of its
  samples), followed by the load of the slot state (6 %); on the producer the samples are
  spread over ordinary loads and branches, the `lock xadd` claim is under 4 %. On the N1
  the producer sits 86 % of its time on the instruction after `ldaddal`: the atomic has
  release semantics and waits for the previous slot's store to reach the coherence point,
  i.e. the cross-core transfer of the slot line, which is the physical floor.
- **MPMC 4→4**: on both machines about three quarters of all samples are in spin loops
  (`pause`/`isb` back-off) — consumers waiting for the head CAS to succeed and producers
  in the contended CAS claim. That is contention on the two index words, not code
  quality; the queue is at the hardware's serialization limit for four producers and four
  consumers on one index each. SegQueue on the N1 is three times worse in the same
  scenario because its consumers also read the tail line on every pop.
- Nothing in the profiles points at a compiler mistake: no spills in the hot loop, no
  redundant fences, one atomic per claim.
