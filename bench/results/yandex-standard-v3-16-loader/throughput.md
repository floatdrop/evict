# Throughput

Millions of operations per second that all goroutines together get done on one cache,
with as many goroutines as CPUs. A number is the median of the runs, and the brackets
behind it hold the slowest and the fastest run. It is in bold if even the slowest run of
that cache beat the fastest of every other one; where the ranges overlap, or where there
are fewer than three runs to go by, the row has no winner. Benchmarks in which a cache can
miss have a second table, with the share of the reads that it answered.

Every cache is set up the way its library suggests, anew for every number of CPUs. See
throughput_test.go for what the benchmarks do, and throughput.txt for the output this
was made from.

Measured on Intel Xeon Processor (Icelake) (linux/amd64) with go1.27.3, otter v2.4.0, golang-lru v2.0.7.

### ReadThroughLoader, capacity=16K

![Throughput of ReadThroughLoader, capacity=16K by number of CPUs](throughput-readthroughloader-capacity-16k.svg)

| CPUs | otter |
|---:|---:|
| 1 | 2.5 (2.4–2.5) |
| 2 | 4.2 (4.0–4.2) |
| 4 | 8.1 (7.2–8.3) |
| 8 | 9.1 (8.8–9.8) |

### ReadThroughLoader, capacity=1M

![Throughput of ReadThroughLoader, capacity=1M by number of CPUs](throughput-readthroughloader-capacity-1m.svg)

| CPUs | otter |
|---:|---:|
| 1 | 1.4 (1.3–1.4) |
| 2 | 2.3 (2.2–2.4) |
| 4 | 4.6 (4.5–4.8) |
| 8 | 6.5 (6.0–6.7) |

### ReadThrough, capacity=16K

![Throughput of ReadThrough, capacity=16K by number of CPUs](throughput-readthrough-capacity-16k.svg)

| CPUs | otter |
|---:|---:|
| 1 | 3.9 (3.8–4.0) |
| 2 | 6.1 (5.7–6.4) |
| 4 | 9.1 (8.5–14.3) |
| 8 | 10.1 (9.7–12.5) |

Share of the reads that were answered, in percent:

| CPUs | otter |
|---:|---:|
| 1 | 86.9 |
| 2 | 87.1 |
| 4 | 86.6 |
| 8 | 87.4 |

### ReadThrough, capacity=1M

![Throughput of ReadThrough, capacity=1M by number of CPUs](throughput-readthrough-capacity-1m.svg)

| CPUs | otter |
|---:|---:|
| 1 | 1.6 (1.5–1.6) |
| 2 | 2.8 (2.6–2.9) |
| 4 | 6.3 (5.6–6.5) |
| 8 | 6.6 (6.2–7.1) |

Share of the reads that were answered, in percent:

| CPUs | otter |
|---:|---:|
| 1 | 89.5 |
| 2 | 89.4 |
| 4 | 89.5 |
| 8 | 89.6 |
