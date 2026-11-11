# Hit ratios and shards

A cache of this module is split into shards, each of which has its share of the capacity
and evicts on its own: a shard that is full evicts although another one may have room.
This is what that does to the hit ratio. The numbers are differences in percentage
points: the hit ratio of a cache with the shards it has by default, less that of the same
cache in one piece, which is the one that [hitratio.md](hitratio.md) reports. A positive
number means that the cache did better in shards.

The caches were created as on a machine with 8 CPUs. That gives a cache of less than 512
entries one shard, and a larger one the largest power of two of them that leaves each with
256 entries or more, up to 64. Which key goes to which shard is up to a hash that is seeded at random,
so the hit ratio of a cache in shards differs from run to run. The number is the
difference for the median of 11 replays, and the brackets hold the differences for the
worst and for the best of them.

`evict/tinylfu` differs from run to run in one piece as well: which keys share counters in
its sketch is up to the same hash. What its replays in shards are compared with is the
median of as many replays in one piece, so its brackets are never empty, not even where a
cache is too small to be split.

Produced by `go run ./cmd/hitratio -sharding -out results` with go1.27.3, golang-lru v2.0.7, otter v2.4.0.

### Zipf

Synthetic: a million requests whose popularity follows a Zipf distribution (s = 1.0001, v = 10) over 50 million keys: 1,000,000 requests for 456,052 keys.
Trace: generated; the parameters are those of otter's benchmarks.

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 500 | 0.00 | 0.00 | 0.00 | 0.00 | −0.20 (−0.48 to +0.15) |
| 1,000 | 0.00 (0.00 to +0.01) | −0.02 (−0.03 to 0.00) | +0.05 (+0.01 to +0.08) | 0.00 (−0.01 to +0.01) | −0.08 (−0.31 to −0.01) |
| 2,000 | +0.01 (0.00 to +0.01) | −0.02 (−0.03 to −0.01) | −0.02 (−0.03 to +0.01) | 0.00 (−0.01 to +0.01) | −0.09 (−0.13 to −0.01) |
| 5,000 | 0.00 (−0.01 to 0.00) | −0.02 (−0.03 to 0.00) | +0.01 (0.00 to +0.02) | +0.01 (−0.01 to +0.02) | 0.00 (−0.02 to +0.01) |
| 10,000 | 0.00 (−0.01 to 0.00) | 0.00 (−0.02 to +0.01) | 0.00 (−0.01 to +0.01) | −0.01 (−0.02 to 0.00) | +0.04 (0.00 to +0.06) |
| 20,000 | 0.00 (−0.01 to 0.00) | +0.01 (−0.01 to +0.03) | 0.00 (−0.01 to +0.01) | −0.01 (−0.02 to +0.01) | +0.10 (+0.07 to +0.11) |
| 40,000 | 0.00 (−0.01 to 0.00) | 0.00 (0.00 to +0.01) | 0.00 (−0.01 to +0.01) | 0.00 (−0.01 to 0.00) | +0.11 (+0.10 to +0.13) |
| 80,000 | 0.00 | 0.00 (−0.01 to 0.00) | 0.00 (−0.01 to 0.00) | 0.00 (0.00 to +0.01) | +0.08 (+0.05 to +0.09) |

### S3

Disk reads of a search engine: 16,407,702 requests for 1,689,882 keys.
Trace: Megiddo and Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache", FAST '03.

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 0.00 | −0.01 (−0.02 to −0.01) | 0.00 (0.00 to +0.01) | 0.00 (−0.01 to 0.00) | −0.57 (−0.60 to −0.56) |
| 200,000 | 0.00 | 0.00 (0.00 to +0.01) | 0.00 (0.00 to +0.01) | 0.00 (0.00 to +0.01) | −0.34 (−0.37 to −0.34) |
| 300,000 | 0.00 | −0.01 (−0.01 to 0.00) | 0.00 (−0.01 to 0.00) | +0.01 | −0.30 (−0.31 to −0.27) |
| 400,000 | +0.01 | −0.02 (−0.03 to −0.02) | +0.01 (0.00 to +0.01) | +0.01 (0.00 to +0.01) | −0.02 (−0.04 to −0.01) |
| 500,000 | 0.00 (−0.01 to 0.00) | +0.01 | +0.01 (0.00 to +0.01) | 0.00 (0.00 to +0.01) | +0.14 (+0.13 to +0.16) |
| 600,000 | −0.01 (−0.01 to 0.00) | +0.04 (+0.03 to +0.04) | +0.02 (+0.01 to +0.03) | 0.00 | +0.12 (+0.11 to +0.13) |
| 700,000 | −0.01 (−0.01 to 0.00) | −0.01 (−0.02 to −0.01) | +0.01 | +0.02 | +0.18 (+0.17 to +0.21) |
| 800,000 | −0.01 (−0.01 to 0.00) | 0.00 (−0.01 to 0.00) | −0.03 (−0.03 to −0.02) | −0.01 (−0.02 to −0.01) | +0.19 (+0.19 to +0.21) |

### DS1

Disk reads of a database server that runs an ERP application, over seven days: 43,704,979 requests for 10,516,352 keys.
Trace: Megiddo and Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache", FAST '03.

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 1,000,000 | 0.00 | +0.63 (+0.40 to +1.17) | 0.00 | −0.02 | −1.12 (−1.18 to −1.03) |
| 2,000,000 | 0.00 | 0.00 (−0.01 to 0.00) | +0.03 (+0.03 to +0.04) | 0.00 | −0.57 (−0.67 to −0.55) |
| 3,000,000 | 0.00 | 0.00 | 0.00 | +0.03 (+0.02 to +0.03) | −0.10 (−0.12 to −0.06) |
| 4,000,000 | 0.00 | +0.03 (+0.02 to +0.04) | 0.00 | 0.00 (−0.01 to 0.00) | −0.49 (−0.56 to −0.38) |
| 5,000,000 | +0.02 (+0.01 to +0.04) | 0.00 | 0.00 | 0.00 | −0.33 (−0.45 to −0.23) |
| 6,000,000 | −0.09 (−0.12 to −0.07) | 0.00 | −0.02 | +0.01 (0.00 to +0.01) | −0.40 (−0.52 to −0.33) |
| 7,000,000 | 0.00 | −0.01 | 0.00 | 0.00 | −0.09 (−0.14 to −0.05) |
| 8,000,000 | 0.00 | 0.00 | −0.01 (−0.02 to −0.01) | +0.01 | +0.08 (+0.03 to +0.10) |

### OLTP

References to a CODASYL database, over one hour: 914,145 requests for 186,880 keys.
Trace: Megiddo and Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache", FAST '03.

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 250 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 (−0.80 to +0.25) |
| 500 | 0.00 | 0.00 | 0.00 | 0.00 | +0.02 (−1.00 to +0.20) |
| 750 | −0.01 (−0.03 to 0.00) | −0.02 (−0.05 to +0.01) | +3.05 (+2.41 to +3.22) | −0.05 (−0.07 to −0.03) | −0.01 (−0.06 to +0.01) |
| 1,000 | −0.02 (−0.03 to 0.00) | −0.01 (−0.03 to +0.01) | +2.65 (+2.39 to +2.98) | −0.03 (−0.03 to −0.02) | −0.02 (−0.04 to +0.03) |
| 1,250 | 0.00 (−0.05 to 0.00) | −0.03 (−0.05 to 0.00) | +4.35 (+4.26 to +4.42) | −0.06 (−0.07 to −0.03) | −0.01 (−0.02 to +0.01) |
| 1,500 | −0.01 (−0.04 to 0.00) | −0.01 (−0.02 to 0.00) | +4.33 (+4.29 to +4.41) | −0.04 (−0.06 to −0.02) | +0.02 (−0.02 to +0.03) |
| 1,750 | −0.02 (−0.03 to −0.01) | −0.01 (−0.03 to −0.01) | +4.09 (+4.04 to +4.22) | −0.05 (−0.06 to −0.03) | +0.01 (−0.02 to +0.02) |
| 2,000 | 0.00 (−0.01 to +0.01) | −0.03 (−0.04 to −0.01) | +3.11 (+3.05 to +3.20) | −0.02 (−0.03 to −0.01) | +0.02 (+0.01 to +0.06) |

### P8

Disk reads of a Windows NT workstation: 42,243,785 requests for 977,545 keys.
Trace: Megiddo and Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache", FAST '03.

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 10,000 | +0.01 (0.00 to +0.01) | −0.03 (−0.04 to −0.01) | +0.25 (+0.23 to +0.28) | 0.00 (−0.01 to 0.00) | −0.27 (−0.71 to −0.25) |
| 20,000 | +0.01 (+0.01 to +0.02) | −0.07 (−0.07 to −0.06) | +0.27 (+0.20 to +0.31) | +0.01 (0.00 to +0.01) | −0.57 (−0.58 to −0.44) |
| 30,000 | 0.00 (0.00 to +0.01) | −0.06 (−0.06 to −0.05) | +0.53 (+0.48 to +0.57) | +0.06 (+0.06 to +0.07) | −0.30 (−0.31 to −0.28) |
| 40,000 | −0.01 | +0.02 (+0.01 to +0.02) | +0.55 (+0.48 to +0.63) | +0.03 | −0.39 (−0.42 to −0.38) |
| 50,000 | +0.03 | −0.10 (−0.10 to −0.09) | +0.48 (+0.20 to +0.56) | +0.04 (+0.04 to +0.05) | −0.20 (−0.23 to −0.18) |
| 60,000 | 0.00 (−0.01 to 0.00) | −0.03 (−0.03 to −0.02) | +1.24 (+1.18 to +1.49) | +0.01 (0.00 to +0.01) | −0.14 (−0.16 to −0.12) |
| 70,000 | −0.03 (−0.03 to −0.02) | −0.01 (−0.01 to 0.00) | +1.05 (+0.94 to +1.09) | −0.01 (−0.01 to 0.00) | −0.25 (−0.26 to −0.23) |
| 80,000 | −0.01 (−0.02 to −0.01) | −0.05 (−0.06 to −0.04) | +0.31 (+0.22 to +0.45) | 0.00 (0.00 to +0.01) | +0.02 (+0.01 to +0.04) |

### Glimpse

Block reads of the text search tool glimpse, which scans its files in loops: 6,015 requests for 2,529 keys.
Trace: Jiang and Zhang, "LIRS: An Efficient Low Inter-reference Recency Set Replacement Policy to Improve Buffer Cache Performance", SIGMETRICS '02.

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 250 | 0.00 | 0.00 | 0.00 | 0.00 | −1.08 (−2.41 to +1.40) |
| 500 | 0.00 | 0.00 | 0.00 | 0.00 | +0.10 (−0.13 to +0.33) |
| 750 | 0.00 (0.00 to +0.02) | −0.05 (−4.64 to −0.05) | 0.00 | +0.05 | −0.55 (−1.25 to +0.05) |
| 1,000 | 0.00 | −0.03 (−0.03 to +0.62) | 0.00 | 0.00 (0.00 to +0.03) | +0.28 (−0.12 to +0.43) |
| 1,250 | 0.00 | −0.13 (−0.30 to −0.03) | 0.00 | −0.08 (−0.18 to +0.02) | +0.02 (−0.02 to +0.23) |
| 1,500 | −1.26 (−1.43 to 0.00) | −0.07 (−0.07 to +0.86) | 0.00 (−2.83 to 0.00) | 0.00 (−0.47 to 0.00) | −0.10 (−0.27 to 0.00) |
| 1,750 | −0.07 (−0.18 to 0.00) | 0.00 | −0.10 (−0.27 to 0.00) | +0.12 (+0.03 to +0.20) | 0.00 (0.00 to +0.02) |
| 2,000 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### Mixed

Three workloads in a row: the vault service of a node of the Corda ledger, a program that reads its blocks in a loop, and the vault service once more: 776,904 requests for 91,479 keys.
Trace: R3, by way of the simulator of Caffeine, and Jiang and Zhang, the authors of LIRS; strung together as in otter's benchmarks.

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 250 | 0.00 | 0.00 | 0.00 | 0.00 | +0.20 (−0.46 to +0.97) |
| 500 | 0.00 | 0.00 | 0.00 | 0.00 | −0.92 (−2.18 to +0.54) |
| 750 | 0.00 | −0.08 (−0.09 to −0.08) | +1.72 (+1.30 to +1.95) | 0.00 | −2.57 (−4.64 to +0.86) |
| 1,000 | 0.00 | −0.12 | +1.95 (+1.45 to +2.52) | 0.00 | 0.00 (−0.07 to +0.03) |
| 1,250 | 0.00 | 0.00 | +7.90 (+7.52 to +40.71) | 0.00 | +0.05 (−0.01 to +0.09) |
| 1,500 | 0.00 | 0.00 | +24.30 (+8.23 to +40.85) | 0.00 | +0.05 (−1.55 to +0.10) |
| 1,750 | 0.00 | 0.00 | +24.80 (+8.25 to +57.09) | 0.00 | +0.10 (−0.63 to +0.16) |
| 2,000 | 0.00 | 0.00 | +40.62 (+24.59 to +57.48) | 0.00 | +0.27 (+0.20 to +0.30) |

### Scarab

Lookups of an e-commerce recommendation service, over one hour: 1,245,184 requests for 258,368 keys.
Trace: Scarab Research, by way of the simulator of Caffeine.

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.00 (−0.02 to +0.01) | −0.03 (−0.09 to −0.01) | +0.06 (+0.02 to +0.11) | 0.00 (−0.01 to +0.03) | −0.06 (−0.11 to −0.03) |
| 5,000 | −0.02 (−0.03 to 0.00) | −0.04 (−0.06 to −0.02) | +0.03 (+0.01 to +0.09) | −0.03 (−0.05 to −0.02) | −0.14 (−0.17 to −0.13) |
| 10,000 | 0.00 (−0.01 to +0.01) | −0.03 (−0.05 to −0.02) | +0.08 (+0.04 to +0.13) | −0.01 (−0.03 to 0.00) | −0.02 (−0.05 to 0.00) |
| 25,000 | −0.01 (−0.03 to 0.00) | −0.01 (−0.03 to 0.00) | +0.11 (+0.09 to +0.14) | −0.02 (−0.03 to −0.01) | +0.13 (+0.10 to +0.15) |
| 50,000 | −0.02 (−0.03 to −0.02) | −0.02 (−0.03 to −0.01) | +0.02 (0.00 to +0.04) | −0.01 (−0.02 to −0.01) | +0.22 (+0.20 to +0.24) |
| 75,000 | −0.01 (−0.01 to 0.00) | −0.01 (−0.01 to 0.00) | +0.02 (0.00 to +0.02) | −0.01 (−0.02 to 0.00) | +0.15 (+0.13 to +0.16) |
| 100,000 | −0.01 (−0.01 to 0.00) | +0.01 (0.00 to +0.01) | +0.01 (−0.01 to +0.02) | −0.01 (−0.03 to −0.01) | +0.07 (+0.06 to +0.08) |
| 125,000 | −0.01 (−0.01 to 0.00) | 0.00 (0.00 to +0.01) | −0.02 (−0.02 to −0.01) | −0.01 (−0.02 to −0.01) | +0.10 (+0.07 to +0.11) |

### Twitter

Requests to an in-memory key-value cache at Twitter (cluster 45, a sample of one key in a hundred): 2,245,093 requests for 654,573 keys.
Trace: Yang, Yue and Rashmi, "A large scale analysis of hundreds of in-memory cache clusters at Twitter", OSDI '20; in the format of libCacheSim, https://github.com/cacheMon/cache_dataset (CC BY 4.0).

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.00 (−0.01 to 0.00) | 0.00 (−0.01 to 0.00) | +0.01 (0.00 to +0.03) | 0.00 | −0.02 (−0.06 to +0.01) |
| 2,500 | 0.00 | −0.01 (−0.02 to 0.00) | −0.01 (−0.02 to 0.00) | 0.00 (0.00 to +0.01) | −0.02 (−0.03 to +0.07) |
| 5,000 | 0.00 (−0.01 to 0.00) | −0.01 (−0.02 to +0.01) | +0.01 (0.00 to +0.02) | −0.01 (−0.02 to 0.00) | −0.01 (−0.03 to 0.00) |
| 10,000 | 0.00 | −0.01 (−0.02 to −0.01) | −0.02 (−0.03 to −0.01) | −0.01 (−0.02 to −0.01) | 0.00 (−0.01 to +0.01) |
| 25,000 | 0.00 (−0.01 to 0.00) | 0.00 (−0.01 to +0.01) | −0.01 (−0.02 to 0.00) | 0.00 | 0.00 (−0.01 to 0.00) |
| 50,000 | 0.00 (−0.01 to 0.00) | 0.00 (0.00 to +0.01) | −0.01 (−0.02 to 0.00) | −0.02 (−0.03 to −0.02) | 0.00 (−0.01 to +0.01) |
| 100,000 | 0.00 (−0.01 to 0.00) | 0.00 | 0.00 (−0.01 to 0.00) | 0.00 (−0.01 to 0.00) | +0.03 (+0.03 to +0.04) |
| 200,000 | 0.00 | 0.00 | 0.00 (−0.01 to 0.00) | 0.00 | +0.01 |

### MSR

Block I/O of a hardware monitoring server at Microsoft Research Cambridge (volume hm_0): 3,993,316 requests for 439,187 keys.
Trace: Narayanan, Donnelly and Rowstron, "Write Off-Loading: Practical Power Management for Enterprise Storage", FAST '08; in the format of libCacheSim, https://github.com/cacheMon/cache_dataset (CC BY 4.0).

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 1,000 | −0.01 (−0.02 to 0.00) | 0.00 (0.00 to +0.01) | +0.21 (+0.13 to +0.33) | −0.01 (−0.01 to 0.00) | −0.20 (−0.42 to 0.00) |
| 2,500 | +0.03 (+0.02 to +0.06) | +0.08 (+0.03 to +0.12) | −0.03 (−0.12 to +0.01) | +0.01 (−0.01 to +0.03) | −0.13 (−0.25 to −0.04) |
| 5,000 | 0.00 (−0.01 to 0.00) | −0.03 (−0.04 to −0.02) | +0.12 (+0.10 to +0.14) | −0.01 (−0.02 to −0.01) | −0.04 (−0.06 to −0.03) |
| 10,000 | +0.01 | −0.02 (−0.04 to 0.00) | +0.16 (+0.13 to +0.20) | −0.11 (−0.13 to −0.11) | −0.06 (−0.07 to −0.04) |
| 25,000 | −0.01 (−0.02 to −0.01) | −0.05 (−0.06 to −0.04) | +0.01 (−0.01 to +0.03) | 0.00 (−0.01 to +0.01) | −0.07 (−0.09 to −0.06) |
| 50,000 | +0.01 (0.00 to +0.01) | +0.03 (+0.02 to +0.04) | +0.06 (+0.03 to +0.08) | +0.01 (+0.01 to +0.02) | −0.01 (−0.02 to 0.00) |
| 100,000 | 0.00 (0.00 to +0.01) | −0.03 (−0.03 to −0.02) | +0.04 (+0.04 to +0.05) | 0.00 (0.00 to +0.01) | +0.08 (+0.07 to +0.09) |
| 200,000 | −0.01 (−0.01 to 0.00) | +0.02 | +0.01 (0.00 to +0.01) | 0.00 | −0.01 (−0.02 to 0.00) |

### CloudPhysics

Block I/O of a virtual machine (trace w105): 3,375,344 requests for 654,126 keys.
Trace: Waldspurger, Park, Garthwaite and Ahmad, "Efficient MRC Construction with SHARDS", FAST '15; in the format of libCacheSim, https://github.com/cacheMon/cache_dataset (CC BY 4.0).

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.00 (−0.02 to +0.01) | 0.00 (−0.01 to +0.01) | −0.15 (−0.22 to −0.07) | 0.00 (−0.01 to 0.00) | −0.01 (−0.09 to 0.00) |
| 2,500 | 0.00 (−0.01 to 0.00) | 0.00 | −0.03 (−0.08 to −0.01) | 0.00 (0.00 to +0.01) | 0.00 (−0.05 to +0.03) |
| 5,000 | 0.00 | −0.01 (−0.02 to 0.00) | +0.09 (+0.08 to +0.11) | −0.01 (−0.01 to 0.00) | +0.07 (+0.04 to +0.09) |
| 10,000 | −0.01 (−0.02 to 0.00) | 0.00 (−0.04 to +0.02) | −0.01 (−0.02 to 0.00) | −0.09 (−0.16 to −0.06) | −0.06 (−0.09 to +0.02) |
| 25,000 | +0.01 (0.00 to +0.01) | −0.07 (−0.10 to −0.07) | −0.28 (−0.46 to −0.19) | −0.07 (−0.09 to −0.06) | −0.15 (−0.26 to −0.09) |
| 50,000 | 0.00 (−0.01 to 0.00) | −0.97 (−1.19 to −0.75) | +0.37 (+0.33 to +0.44) | −0.07 (−0.09 to −0.06) | −0.15 (−0.18 to −0.02) |
| 100,000 | +0.02 (+0.02 to +0.03) | +0.03 (+0.01 to +0.05) | 0.00 (−0.01 to 0.00) | −0.01 (−0.03 to −0.01) | −0.03 (−0.15 to +0.07) |
| 200,000 | −0.01 (−0.01 to 0.00) | −0.01 (−0.02 to 0.00) | 0.00 (0.00 to +0.01) | +0.08 (+0.06 to +0.10) | +0.03 (−0.02 to +0.12) |
