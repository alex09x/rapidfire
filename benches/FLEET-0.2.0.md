# 0.2.0: twelve-machine benchmark matrix

All results below are newly measured with the corrected harness. The original
0.1.0 tables remain in [RESULTS.md](RESULTS.md) as a historical snapshot.
Individual measured samples, exact CPU lists, toolchain, source hashes, command
timings and host load readings are in [fleet-0.2.0.json](fleet-0.2.0.json).

Measured on 2026-09-16 at source
[`04726d2`](https://github.com/alex09x/rapidfire/tree/04726d2eb5f1ebac3e47176c8242d70e972e0bb6).

## Machines

| # | CPU | OS/kernel | RAM, GiB | MPSC CPUs | Comparison CPU pool |
|--:|:--|:--|--:|:--|:--|
| 1 | AMD Ryzen 9 7900 12-Core Processor | Linux 6.8.0-90-lowlatency | 61.9 | 0,1,2,3,4,5,6,7,8 | 24 logical CPUs |
| 2 | AMD Ryzen 9 7900 12-Core Processor | Linux 7.0.0-29-generic | 60.9 | 0,1,2,3,4,5,6,7,8 | 24 logical CPUs |
| 3 | AMD Ryzen 9 7950X 16-Core Processor | Linux 6.8.0-136-generic | 61.9 | 0,1,2,3,4,5,6,7,8 | 32 logical CPUs |
| 4 | AMD Ryzen 9 7950X 16-Core Processor | Linux 6.8.0-1008-nvidia | 124.9 | 0,1,2,3,4,5,6,7,8 | 32 logical CPUs |
| 5 | AMD Ryzen 9 7950X 16-Core Processor | Linux 5.15.0-187-generic | 124.9 | 0,1,2,3,4,5,6,7,8 | 32 logical CPUs |
| 6 | AMD Ryzen 9 7950X3D 16-Core Processor | Linux 5.15.0-187-generic | 125.0 | 0,1,2,3,4,5,6,7,8 | 32 logical CPUs |
| 7 | AMD Ryzen 9 7950X3D 16-Core Processor | Linux 6.8.0-136-generic | 125.0 | 0,1,2,3,4,5,6,7,8 | 32 logical CPUs |
| 8 | AMD Ryzen 9 9950X 16-Core Processor | Linux 6.8.0-111-generic | 186.4 | 0,1,2,3,4,5,6,7,8 | 32 logical CPUs |
| 9 | AMD Ryzen 9 3950X 16-Core Processor | Linux 5.19.0-46-generic | 125.7 | 0,1,2,3,4,5,6,7,8 | 32 logical CPUs |
| 10 | Intel(R) Xeon(R) CPU E5-2699 v3 @ 2.30GHz | Linux 5.15.0-177-generic | 125.8 | 0,1,2,3,4,5,6,7,8 | 72 logical CPUs |
| 11 | Neoverse-N1 | Linux 6.8.0-137-generic | 250.9 | 0,1,2,3,4,5,6,7,8 | 128 logical CPUs |
| 12 | Apple M3 Pro | Darwin 25.6.0 | 18.0 | 0,1,2,3,4,5,6,7,8 | 11 logical CPUs |

Linux affinity is checked. MPSC sync 4→1 uses the first five physical CPUs,
sync 8→1 uses all nine, and async tasks use four pinned Tokio workers.
Nine-core placements cross cache clusters on Ryzen CPUs; no single-CCD claim is made.
The wider harnesses assign physical cores before SMT siblings and wrap around the
full listed pool when workers outnumber CPUs. Every worker has an explicit entry.
The raw harness single-thread push/pop and burst microbenchmarks use the unpinned
calling thread; the worker-affinity list does not apply to those two rows.
On macOS all three harnesses request user-interactive QoS; CPU IDs do not pin threads.
These are live shared machines. Load can change during a run and is recorded.

## MPSC throughput

Both APIs use the same fixed sender in the same binary. MPMC batches use one
`recv().await` followed by `try_recv`; MPSC batches use native `recv_many`.
The limit is identical on both sides. Each throughput command uses 500,003
requested messages, rounded up to equal producer quotas, one warm-up and five
interleaved measurements per variant. Three independent batches are retained;
the tables report the median of the three batch medians, in amortized ns/message.
Timing starts before ready workers are released and ends at the final receive.
Backlog allocations are included; thread/runtime creation and channel destruction are excluded.

### Sync 4→1, unbounded, 64 B, one message

| # | General MPMC | Exclusive MPSC | MPSC / MPMC |
|--:|--:|--:|--:|
| 1 | 13.90 | 26.28 | 1.89× |
| 2 | 15.26 | 13.43 | 0.88× |
| 3 | 15.96 | 12.01 | 0.75× |
| 4 | 15.13 | 12.00 | 0.79× |
| 5 | 15.92 | 14.02 | 0.88× |
| 6 | 16.23 | 14.05 | 0.87× |
| 7 | 16.10 | 14.80 | 0.92× |
| 8 | 19.13 | 21.26 | 1.11× |
| 9 | 54.02 | 57.23 | 1.06× |
| 10 | 95.50 | 113.95 | 1.19× |
| 11 | 92.38 | 89.59 | 0.97× |
| 12 | 29.55 | 29.72 | 1.01× |

### Async 40→1, bounded(4096), 64 B, receive limit 32

| # | General MPMC | Exclusive MPSC | MPSC / MPMC |
|--:|--:|--:|--:|
| 1 | 15.69 | 19.95 | 1.27× |
| 2 | 53.78 | 22.00 | 0.41× |
| 3 | 62.71 | 22.88 | 0.36× |
| 4 | 58.89 | 21.36 | 0.36× |
| 5 | 58.11 | 22.07 | 0.38× |
| 6 | 59.17 | 22.45 | 0.38× |
| 7 | 62.25 | 22.72 | 0.37× |
| 8 | 15.78 | 15.43 | 0.98× |
| 9 | 35.61 | 37.91 | 1.06× |
| 10 | 134.91 | 127.14 | 0.94× |
| 11 | 94.89 | 102.25 | 1.08× |
| 12 | 13.56 | 14.45 | 1.07× |

### Async 40→1, bounded(4096), 1024 B, receive limit 32

| # | General MPMC | Exclusive MPSC | MPSC / MPMC |
|--:|--:|--:|--:|
| 1 | 224.39 | 53.86 | 0.24× |
| 2 | 237.72 | 57.51 | 0.24× |
| 3 | 249.58 | 57.22 | 0.23× |
| 4 | 251.65 | 55.27 | 0.22× |
| 5 | 190.95 | 113.02 | 0.59× |
| 6 | 299.53 | 60.19 | 0.20× |
| 7 | 278.81 | 61.59 | 0.22× |
| 8 | 155.17 | 49.53 | 0.32× |
| 9 | 391.76 | 69.77 | 0.18× |
| 10 | 740.45 | 154.36 | 0.21× |
| 11 | 746.82 | 138.33 | 0.19× |
| 12 | 82.23 | 72.95 | 0.89× |

### Async 4→1, unbounded, 1024 B, receive limit 32

| # | General MPMC | Exclusive MPSC | MPSC / MPMC |
|--:|--:|--:|--:|
| 1 | 473.19 | 122.76 | 0.26× |
| 2 | 107.07 | 265.28 | 2.48× |
| 3 | 115.12 | 423.12 | 3.68× |
| 4 | 460.41 | 164.51 | 0.36× |
| 5 | 451.16 | 221.49 | 0.49× |
| 6 | 73.77 | 295.00 | 4.00× |
| 7 | 488.33 | 119.07 | 0.24× |
| 8 | 46.74 | 245.56 | 5.25× |
| 9 | 155.47 | 67.27 | 0.43× |
| 10 | 776.18 | 739.50 | 0.95× |
| 11 | 214.94 | 595.35 | 2.77× |
| 12 | 151.65 | 140.30 | 0.93× |

A ratio below 1 means lower time for MPSC; above 1 means a regression.
The JSON contains every one of the 54 paired throughput scenarios per machine.
Do not infer uniform improvement or statistical confidence from selected rows.

## Paced delivery latency

Each producer offers bursts of 16 messages every 1 ms and skips missed ticks.
Timestamps span the attempt to send through consumer inspection, including
capacity waits, scheduling and measurement overhead. Runs request 16,003 messages
rounded up to equal quotas; one warm-up precedes five interleaved runs per variant.
The table uses medians of the five per-run p99s, in microseconds, for async 40→1,
64 B, bounded(4096), receive limit 32. Raw p50/p99/max values for all 12 paired
latency scenarios are retained; throughput is not an individual handoff latency.

| # | General MPMC p99, µs | Exclusive MPSC p99, µs |
|--:|--:|--:|
| 1 | 15.089 | 14.827 |
| 2 | 10.700 | 17.873 |
| 3 | 6.743 | 2.785 |
| 4 | 7.063 | 10.700 |
| 5 | 10.740 | 33.293 |
| 6 | 10.500 | 23.233 |
| 7 | 17.162 | 9.999 |
| 8 | 23.744 | 27.382 |
| 9 | 4.200 | 4.541 |
| 10 | 84.204 | 156.590 |
| 11 | 195.634 | 168.355 |
| 12 | 263.875 | 207.042 |

## Common competitor harnesses

The raw harness requests 4,000,000 u64 messages and the real-case harness requests
2,000,000 messages with 64/256/1024-byte payloads. Each row has one warm-up and five
measurements. Request/response uses n/10 round trips; other rows count messages.
All 104 raw rows (including unsupported entries) and 180 real-case rows per machine
are in the JSON. `rapidfire` is general MPMC; `rapidfire_mpsc` is the exclusive API.
Raw MPMC rows are unsupported for the exclusive receiver, std mpsc and Tokio mpsc.
Implementations run sequentially within these harnesses; time-varying host load
can affect comparisons. The older Tokio-only receive mutex and shared per-message
completion counter are absent. Timing begins before worker release.

### Raw SPSC, unbounded (u64)

Amortized ns/message; lower is better.

| # | rapidfire | rapidfire_mpsc | crossbeam_seg | std_mpsc | flume | async_channel | tokio_mpsc |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 5.40 | 5.58 | 12.92 | 13.83 | 94.01 | 33.63 | 107.07 |
| 2 | 5.32 | 5.31 | 16.87 | 13.82 | 55.24 | 34.86 | 109.91 |
| 3 | 5.60 | 5.91 | 13.98 | 15.43 | 83.84 | 42.76 | 117.67 |
| 4 | 5.30 | 5.47 | 14.19 | 13.91 | 78.86 | 38.95 | 112.81 |
| 5 | 5.30 | 8.51 | 27.93 | 29.06 | 67.89 | 32.52 | 111.61 |
| 6 | 6.89 | 5.43 | 13.44 | 14.62 | 72.33 | 40.38 | 117.24 |
| 7 | 5.57 | 5.64 | 16.26 | 16.99 | 100.17 | 37.86 | 115.95 |
| 8 | 9.51 | 10.11 | 37.90 | 37.28 | 71.42 | 58.63 | 135.64 |
| 9 | 8.65 | 12.01 | 62.48 | 61.02 | 120.03 | 107.86 | 159.70 |
| 10 | 22.47 | 32.84 | 33.29 | 32.21 | 138.15 | 139.91 | 317.11 |
| 11 | 17.56 | 38.57 | 25.56 | 26.76 | 161.63 | 98.12 | 210.30 |
| 12 | 8.49 | 8.44 | 6.99 | 10.99 | 21.38 | 67.60 | 173.55 |

### Raw MPSC 4→1, unbounded (u64)

Amortized ns/message; lower is better.

| # | rapidfire | rapidfire_mpsc | crossbeam_seg | std_mpsc | flume | async_channel | tokio_mpsc |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 7.24 | 7.01 | 7.32 | 49.28 | 68.71 | 88.35 | 106.73 |
| 2 | 7.09 | 6.67 | 10.87 | 50.03 | 62.14 | 113.22 | 108.17 |
| 3 | 6.91 | 7.39 | 9.98 | 54.02 | 84.49 | 95.25 | 108.64 |
| 4 | 6.63 | 7.62 | 8.39 | 50.22 | 62.66 | 86.64 | 107.95 |
| 5 | 6.35 | 10.63 | 27.79 | 36.38 | 38.97 | 64.87 | 105.94 |
| 6 | 6.93 | 7.06 | 11.62 | 51.43 | 73.30 | 89.77 | 111.36 |
| 7 | 7.02 | 7.58 | 10.42 | 50.82 | 70.97 | 90.71 | 105.40 |
| 8 | 10.35 | 11.24 | 41.65 | 52.68 | 88.97 | 108.08 | 118.35 |
| 9 | 23.20 | 28.10 | 18.24 | 54.54 | 190.90 | 179.67 | 113.63 |
| 10 | 118.02 | 73.82 | 133.88 | 184.67 | 152.67 | 312.60 | 229.33 |
| 11 | 43.76 | 46.00 | 37.50 | 239.97 | 302.48 | 299.00 | 226.59 |
| 12 | 9.18 | 9.27 | 9.27 | 82.77 | 32.79 | 84.27 | 92.02 |

### Raw MPSC 4→1, bounded(1024), u64

Amortized ns/message; lower is better.

| # | rapidfire | rapidfire_mpsc | crossbeam_array | flume | async_channel | tokio_mpsc |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | 7.45 | 7.63 | 11.07 | 240.34 | 113.82 | 153.78 |
| 2 | 7.72 | 7.88 | 19.15 | 216.49 | 121.72 | 158.77 |
| 3 | 7.59 | 8.07 | 15.81 | 240.00 | 129.53 | 152.57 |
| 4 | 6.90 | 7.63 | 14.28 | 296.32 | 120.76 | 136.41 |
| 5 | 7.02 | 14.98 | 15.13 | 224.14 | 117.61 | 139.69 |
| 6 | 7.67 | 7.44 | 16.38 | 243.62 | 123.05 | 157.04 |
| 7 | 7.65 | 7.99 | 18.11 | 245.75 | 124.87 | 149.78 |
| 8 | 10.74 | 14.00 | 25.55 | 299.59 | 117.47 | 154.54 |
| 9 | 56.60 | 58.27 | 119.93 | 397.02 | 241.29 | 206.66 |
| 10 | 137.36 | 101.11 | 141.24 | 746.74 | 300.71 | 324.92 |
| 11 | 70.43 | 65.54 | 52.30 | 341.03 | 295.34 | 247.75 |
| 12 | 14.40 | 15.64 | 13.95 | 107.08 | 127.59 | 145.47 |

### Async 40 readers → one writer, unbounded, 64 B

One message per receive, four Tokio workers; amortized ns/message.

| # | rapidfire | rapidfire_mpsc | async_channel | tokio_mpsc | flume |
|--:|--:|--:|--:|--:|--:|
| 1 | 36.51 | 27.19 | 96.35 | 113.72 | 129.33 |
| 2 | 31.73 | 39.49 | 85.84 | 108.75 | 103.39 |
| 3 | 25.90 | 21.23 | 112.45 | 107.30 | 138.91 |
| 4 | 22.66 | 21.44 | 110.57 | 105.46 | 122.06 |
| 5 | 21.64 | 31.09 | 95.81 | 114.95 | 88.30 |
| 6 | 24.23 | 24.99 | 110.16 | 105.11 | 128.21 |
| 7 | 22.10 | 24.20 | 105.79 | 105.38 | 121.66 |
| 8 | 21.37 | 17.85 | 111.90 | 108.11 | 96.22 |
| 9 | 34.29 | 34.57 | 173.53 | 124.55 | 167.19 |
| 10 | 142.29 | 103.77 | 247.43 | 242.78 | 240.74 |
| 11 | 66.52 | 74.80 | 244.48 | 193.43 | 253.43 |
| 12 | 16.08 | 13.02 | 68.13 | 109.80 | 75.59 |

### Async 40 readers → one writer, unbounded, 1024 B

One message per receive, four Tokio workers; amortized ns/message.

| # | rapidfire | rapidfire_mpsc | async_channel | tokio_mpsc | flume |
|--:|--:|--:|--:|--:|--:|
| 1 | 150.46 | 128.20 | 189.28 | 201.76 | 587.29 |
| 2 | 350.03 | 189.42 | 224.67 | 437.00 | 629.81 |
| 3 | 128.69 | 128.21 | 215.94 | 193.68 | 638.72 |
| 4 | 128.88 | 133.08 | 210.56 | 190.18 | 545.75 |
| 5 | 138.83 | 131.18 | 319.59 | 304.67 | 752.04 |
| 6 | 151.25 | 137.11 | 206.31 | 201.75 | 674.57 |
| 7 | 122.75 | 135.97 | 209.44 | 193.01 | 653.81 |
| 8 | 142.93 | 168.75 | 210.62 | 197.07 | 396.43 |
| 9 | 189.25 | 214.40 | 257.03 | 242.28 | 668.34 |
| 10 | 266.41 | 231.41 | 487.76 | 380.48 | 2187.29 |
| 11 | 222.00 | 300.39 | 446.29 | 400.89 | 955.11 |
| 12 | 115.04 | 119.66 | 174.71 | 167.70 | 532.39 |

## Correctness and reproduction

All twelve machines passed 74 native tests plus four doctests, and 74 release
tests with three-slot blocks. The first pass exposed an existing bounded-length
snapshot bug on two machines; it was fixed before the reported final matrix.
Interrupted and pre-fix measurement runs are excluded from this report.

Run `python3 benches/run_matrix.py --help` for the reproducible command. Benchmark builds
use rustc 1.97.1, `-C target-cpu=native`, fat LTO and one codegen unit. The source
commit and individual file hashes are recorded with every host result.
