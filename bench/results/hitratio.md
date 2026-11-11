# Hit ratios

The share of requests, in percent, that a cache of a given capacity answers when a
trace is replayed against it: every key of the trace is looked up in turn and stored if
it is missing. The column `optimal` is not a cache but the bound that no cache can
beat: what one would score that knew the requests to come.

The caches of this module run in one piece here, with a single shard, and otter evicts on
the spot rather than in the background, as it does in its own simulator. The caches of
this module and those of golang-lru then evict the same way every time, and their numbers
are the same on every run. Otter's are not: it admits some entries at random, which moves
its hit ratio from one replay to the next, in this run by up to 3.8 percentage points (otter on P8 with 20,000 entries).
Its number is the median of 11 replays, which is also what the charts show, and the column
next to it has the lowest and the highest of them. That is not all the range there is:
of further replays, about 17 in a hundred would come out below it or above it.

A number is in bold if the cache did better in that row than every other one, on each of
their replays. Where the range of otter reaches the best of the others, the row has no
winner.

A program does not get the caches of this module in one piece unless it asks for that: by
default they are split into shards. What difference that makes is in
[sharding.md](sharding.md). For most of the numbers here it is none to speak of, but not
for all: `evict/sieve` in particular does far better in shards on the traces on which it
does worst here.

Produced by `go run ./cmd/hitratio -out results` with go1.27.3, golang-lru v2.0.7, otter v2.4.0.

## At a glance

The hit ratio of every cache on every trace, averaged over the capacities at which the
trace was replayed. Averages hide a lot: a cache that leads here may trail at the capacity
that matters to you, so look at the trace that resembles your workload, below.

| trace | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | golang-lru/2q | optimal |
|:---|---:|---:|---:|---:|---:|---:|---:|
| Zipf | 29.97 | 36.53 | 37.55 | **38.25** | 37.86 | 37.61 | 44.07 |
| S3 | 23.33 | 32.78 | 35.54 | 38.90 | **44.57** | 33.15 | 59.37 |
| DS1 | 23.70 | 28.88 | 38.87 | 37.05 | **46.52** | 34.47 | 50.21 |
| OLTP | 32.40 | 39.07 | 30.60 | 39.11 | 38.92 | 37.60 | 52.77 |
| P8 | 24.10 | 32.11 | 27.66 | 35.31 | 36.22 | 31.45 | 53.53 |
| Glimpse | 21.81 | 27.40 | 30.14 | 43.24 | **45.21** | 33.20 | 47.81 |
| Mixed | 44.11 | 56.80 | 0.43 | 43.84 | **61.62** | 35.97 | 64.22 |
| Scarab | 59.10 | 61.00 | 60.53 | **61.60** | 58.97 | 61.14 | 70.30 |
| Twitter | 58.37 | 59.72 | 58.99 | **60.32** | 59.28 | 58.80 | 65.46 |
| MSR | 64.40 | 66.31 | 66.16 | **67.22** | 66.56 | 66.57 | 73.60 |
| CloudPhysics | 20.90 | 24.41 | 24.70 | **27.59** | 26.73 | 25.59 | 34.23 |

## Trace by trace

### Zipf

Synthetic: a million requests whose popularity follows a Zipf distribution (s = 1.0001, v = 10) over 50 million keys: 1,000,000 requests for 456,052 keys.
Trace: generated; the parameters are those of otter's benchmarks.

![Hit ratios on the Zipf trace by cache capacity](hitratio-zipf.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 500 | 12.25 | 22.98 | 23.77 | **24.41** | 23.25 | 21.81–23.53 | 22.93 | 29.40 |
| 1,000 | 16.96 | 27.25 | 27.88 | **28.70** | 27.54 | 26.85–27.90 | 27.22 | 33.81 |
| 2,000 | 21.78 | 31.41 | 32.06 | **32.88** | 31.41 | 30.10–32.04 | 31.58 | 38.17 |
| 5,000 | 28.22 | 36.51 | 36.90 | **38.00** | 37.17 | 36.90–37.68 | 37.08 | 43.80 |
| 10,000 | 33.04 | 39.66 | 40.34 | 41.39 | 41.45 | 41.06–41.58 | 40.88 | 47.79 |
| 20,000 | 37.83 | 42.30 | 43.49 | 44.32 | 44.67 | 44.19–44.83 | 44.30 | 51.29 |
| 40,000 | 42.58 | 44.82 | 46.54 | 46.90 | **47.54** | 47.28–47.60 | 47.16 | 53.91 |
| 80,000 | 47.11 | 47.35 | 49.39 | 49.40 | **49.84** | 49.74–49.88 | 49.73 | 54.39 |

### S3

Disk reads of a search engine: 16,407,702 requests for 1,689,882 keys.
Trace: Megiddo and Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache", FAST '03.

![Hit ratios on the S3 trace by cache capacity](hitratio-s3.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 2.33 | 9.15 | 10.12 | **12.34** | 11.73 | 11.36–11.89 | 10.35 | 25.42 |
| 200,000 | 4.63 | 16.86 | 17.93 | 22.52 | **23.01** | 22.97–23.06 | 18.94 | 39.79 |
| 300,000 | 7.59 | 25.01 | 23.94 | 29.81 | **33.49** | 33.17–33.56 | 26.36 | 50.92 |
| 400,000 | 12.04 | 30.87 | 29.36 | 34.62 | **42.83** | 42.69–42.88 | 30.87 | 59.96 |
| 500,000 | 22.77 | 34.20 | 37.51 | 41.42 | **51.16** | 51.08–51.18 | 33.99 | 67.09 |
| 600,000 | 34.63 | 39.59 | 46.27 | 49.21 | **58.90** | 58.76–58.97 | 39.01 | 72.97 |
| 700,000 | 46.04 | 49.20 | 55.40 | 56.97 | **65.11** | 65.06–65.13 | 48.12 | 77.57 |
| 800,000 | 56.60 | 57.38 | 63.76 | 64.31 | **70.31** | 70.26–70.36 | 57.55 | 81.27 |

### DS1

Disk reads of a database server that runs an ERP application, over seven days: 43,704,979 requests for 10,516,352 keys.
Trace: Megiddo and Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache", FAST '03.

![Hit ratios on the DS1 trace by cache capacity](hitratio-ds1.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000,000 | 3.09 | 5.51 | 6.12 | 14.09 | 13.98 | 13.82–14.12 | 7.08 | 20.19 |
| 2,000,000 | 10.74 | 21.70 | 23.32 | 25.32 | **28.28** | 28.20–28.36 | 19.59 | 31.79 |
| 3,000,000 | 18.59 | 18.14 | 37.73 | 30.85 | **39.82** | 39.77–39.97 | 33.65 | 41.23 |
| 4,000,000 | 20.24 | 23.81 | 39.07 | 31.96 | **45.42** | 45.27–45.79 | 31.31 | 48.09 |
| 5,000,000 | 21.03 | 25.57 | 40.66 | 37.31 | **51.63** | 51.43–51.70 | 37.89 | 54.96 |
| 6,000,000 | 33.95 | 37.21 | 49.81 | 40.03 | **58.29** | 58.12–58.42 | 39.94 | 61.82 |
| 7,000,000 | 38.90 | 45.88 | 56.65 | 51.97 | **64.40** | 64.28–64.59 | 48.96 | 68.69 |
| 8,000,000 | 43.03 | 53.22 | 57.57 | 64.91 | **70.35** | 70.29–70.62 | 57.38 | 74.93 |

### OLTP

References to a CODASYL database, over one hour: 914,145 requests for 186,880 keys.
Trace: Megiddo and Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache", FAST '03.

![Hit ratios on the OLTP trace by cache capacity](hitratio-oltp.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 250 | 16.47 | 22.70 | 11.81 | 21.65 | **24.26** | 23.75–24.59 | 20.23 | 38.47 |
| 500 | 23.45 | 33.28 | 20.62 | 32.40 | 33.30 | 32.40–33.64 | 30.37 | 46.43 |
| 750 | 28.28 | **37.99** | 27.46 | 37.70 | 37.64 | 36.36–37.86 | 36.10 | 50.67 |
| 1,000 | 32.83 | 40.53 | 32.56 | **40.82** | 39.81 | 39.63–40.03 | 39.24 | 53.62 |
| 1,250 | 36.21 | 42.38 | 35.18 | **42.91** | 41.85 | 41.00–42.57 | 41.37 | 55.84 |
| 1,500 | 38.70 | 43.88 | 37.10 | **44.53** | 43.66 | 43.44–43.88 | 43.09 | 57.62 |
| 1,750 | 40.79 | 45.32 | 38.91 | **45.88** | 44.91 | 44.88–44.98 | 44.51 | 59.13 |
| 2,000 | 42.47 | 46.51 | 41.19 | **46.99** | 45.93 | 45.86–46.39 | 45.87 | 60.40 |

### P8

Disk reads of a Windows NT workstation: 42,243,785 requests for 977,545 keys.
Trace: Megiddo and Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache", FAST '03.

![Hit ratios on the P8 trace by cache capacity](hitratio-p8.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | 3.34 | 9.76 | 6.19 | **12.29** | 10.81 | 10.29–11.85 | 9.39 | 26.22 |
| 20,000 | 9.48 | 18.17 | 14.48 | 21.20 | 20.35 | 17.83–21.59 | 17.74 | 38.69 |
| 30,000 | 15.49 | 25.16 | 21.17 | 28.23 | 29.12 | 27.03–29.72 | 24.73 | 47.37 |
| 40,000 | 21.63 | 30.94 | 27.55 | 34.43 | 35.26 | 32.77–36.20 | 30.59 | 53.94 |
| 50,000 | 27.51 | 36.56 | 33.33 | 39.62 | **41.49** | 40.51–42.39 | 35.81 | 59.29 |
| 60,000 | 33.13 | 41.02 | 36.01 | 44.67 | **47.29** | 47.00–47.46 | 40.36 | 63.77 |
| 70,000 | 38.75 | 45.42 | 39.12 | 49.02 | **51.42** | 49.87–51.85 | 44.53 | 67.72 |
| 80,000 | 43.45 | 49.81 | 43.39 | 53.01 | 53.98 | 52.70–54.42 | 48.42 | 71.21 |

### Glimpse

Block reads of the text search tool glimpse, which scans its files in loops: 6,015 requests for 2,529 keys.
Trace: Jiang and Zhang, "LIRS: An Efficient Low Inter-reference Recency Set Replacement Policy to Improve Buffer Cache Performance", SIGMETRICS '02.

![Hit ratios on the Glimpse trace by cache capacity](hitratio-glimpse.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 250 | 0.91 | 1.00 | 1.38 | 15.83 | 15.46 | 14.35–15.89 | 1.38 | 17.71 |
| 500 | 0.95 | 1.18 | 1.38 | 30.54 | **33.28** | 33.00–33.47 | 1.40 | 34.33 |
| 750 | 1.15 | 28.96 | 1.41 | 39.09 | **43.51** | 41.53–43.59 | 29.14 | 46.13 |
| 1,000 | 11.21 | 30.77 | 31.34 | 44.34 | **50.44** | 49.64–50.82 | 30.86 | 53.15 |
| 1,250 | 21.25 | 31.21 | 41.31 | 50.42 | **51.90** | 51.85–52.44 | 41.26 | 57.31 |
| 1,500 | 36.56 | 36.08 | 51.85 | 53.00 | **53.55** | 53.55–53.60 | 49.09 | 57.96 |
| 1,750 | 45.04 | 42.08 | 55.06 | 55.33 | **55.58** | 55.58–55.59 | 55.06 | 57.96 |
| 2,000 | 57.41 | 47.90 | 57.41 | 57.41 | **57.96** | 57.96–57.96 | 57.41 | 57.96 |

### Mixed

Three workloads in a row: the vault service of a node of the Corda ledger, a program that reads its blocks in a loop, and the vault service once more: 776,904 requests for 91,479 keys.
Trace: R3, by way of the simulator of Caffeine, and Jiang and Zhang, the authors of LIRS; strung together as in otter's benchmarks.

![Hit ratios on the Mixed trace by cache capacity](hitratio-mixed.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 250 | 11.64 | 11.64 | 0.10 | 9.50 | **25.43** | 24.97–25.96 | 11.64 | 27.70 |
| 500 | 11.64 | 11.64 | 0.19 | 11.70 | **41.56** | 41.11–41.90 | 11.64 | 43.76 |
| 750 | 11.64 | 49.80 | 0.29 | 11.73 | **55.56** | 54.98–56.00 | 11.64 | 59.82 |
| 1,000 | 11.64 | **74.96** | 0.39 | 11.76 | 72.85 | 72.16–73.22 | 11.64 | 75.87 |
| 1,250 | 76.58 | 76.58 | 0.48 | 76.44 | 74.69 | 74.46–74.78 | 11.64 | 76.61 |
| 1,500 | 76.58 | 76.58 | 0.58 | 76.49 | 74.41 | 74.36–74.45 | 76.50 | 76.64 |
| 1,750 | 76.58 | 76.58 | 0.67 | 76.54 | 73.86 | 73.48–73.97 | 76.51 | 76.68 |
| 2,000 | 76.58 | 76.58 | 0.77 | 76.58 | 74.60 | 74.53–74.64 | 76.51 | 76.71 |

### Scarab

Lookups of an e-commerce recommendation service, over one hour: 1,245,184 requests for 258,368 keys.
Trace: Scarab Research, by way of the simulator of Caffeine.

![Hit ratios on the Scarab trace by cache capacity](hitratio-scarab.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 26.96 | 35.10 | 33.28 | **35.22** | 34.65 | 34.13–34.95 | 32.53 | 44.91 |
| 5,000 | 41.20 | 47.24 | 45.90 | **47.75** | 46.41 | 46.31–47.02 | 45.44 | 59.72 |
| 10,000 | 48.65 | 52.64 | 51.51 | **53.51** | 52.36 | 51.98–52.41 | 51.55 | 66.57 |
| 25,000 | 60.13 | 61.92 | 60.56 | **62.41** | 59.73 | 59.62–59.77 | 61.81 | 74.79 |
| 50,000 | 69.15 | 69.01 | 68.43 | 69.47 | 64.77 | 64.69–64.87 | **70.07** | 78.63 |
| 75,000 | 73.51 | 72.37 | 72.74 | 72.99 | 68.18 | 68.11–68.26 | **74.05** | 79.25 |
| 100,000 | 75.91 | 74.26 | 74.80 | 75.03 | 71.55 | 71.50–71.63 | **76.23** | 79.25 |
| 125,000 | 77.27 | 75.44 | 77.04 | 76.40 | 74.10 | 74.04–74.19 | **77.47** | 79.25 |

### Twitter

Requests to an in-memory key-value cache at Twitter (cluster 45, a sample of one key in a hundred): 2,245,093 requests for 654,573 keys.
Trace: Yang, Yue and Rashmi, "A large scale analysis of hundreds of in-memory cache clusters at Twitter", OSDI '20; in the format of libCacheSim, https://github.com/cacheMon/cache_dataset (CC BY 4.0).

![Hit ratios on the Twitter trace by cache capacity](hitratio-twitter.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 50.28 | 53.20 | 51.36 | **53.52** | 52.99 | 52.65–53.29 | 51.02 | 58.65 |
| 2,500 | 53.12 | 55.49 | 54.16 | **55.78** | 55.43 | 55.16–55.52 | 53.86 | 60.92 |
| 5,000 | 55.49 | 57.37 | 56.03 | **57.55** | 57.03 | 56.86–57.07 | 55.86 | 62.54 |
| 10,000 | 57.49 | 59.07 | 57.94 | **59.13** | 57.81 | 57.32–58.42 | 57.77 | 64.20 |
| 25,000 | 59.69 | 60.98 | 60.16 | **61.29** | 59.51 | 59.49–59.54 | 60.02 | 66.83 |
| 50,000 | 61.37 | 62.12 | 61.88 | **63.13** | 62.00 | 61.97–62.03 | 61.69 | 68.82 |
| 100,000 | 63.39 | 63.72 | 63.89 | **65.00** | 63.10 | 63.08–63.12 | 63.79 | 70.84 |
| 200,000 | 66.15 | 65.82 | 66.48 | **67.17** | 66.37 | 66.33–66.40 | 66.36 | 70.84 |

### MSR

Block I/O of a hardware monitoring server at Microsoft Research Cambridge (volume hm_0): 3,993,316 requests for 439,187 keys.
Trace: Narayanan, Donnelly and Rowstron, "Write Off-Loading: Practical Power Management for Enterprise Storage", FAST '08; in the format of libCacheSim, https://github.com/cacheMon/cache_dataset (CC BY 4.0).

![Hit ratios on the MSR trace by cache capacity](hitratio-msr.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 50.47 | 53.78 | 52.78 | **53.99** | 53.30 | 52.96–53.51 | 53.79 | 59.02 |
| 2,500 | 54.23 | 56.31 | 56.67 | **57.43** | 57.25 | 56.58–57.34 | 56.88 | 62.94 |
| 5,000 | 57.56 | 60.33 | 59.63 | **60.59** | 59.98 | 59.74–60.15 | 60.00 | 66.07 |
| 10,000 | 60.08 | 62.62 | 62.01 | **63.46** | 63.01 | 62.92–63.18 | 62.31 | 69.96 |
| 25,000 | 64.63 | 67.40 | 66.80 | **67.91** | 67.43 | 67.38–67.47 | 67.26 | 75.78 |
| 50,000 | 69.08 | 70.81 | 71.01 | **72.17** | 71.19 | 71.16–71.25 | 71.52 | 80.75 |
| 100,000 | 76.11 | 76.90 | 76.66 | **77.99** | 77.12 | 77.07–77.18 | 77.06 | 85.54 |
| 200,000 | 83.03 | 82.36 | 83.70 | **84.24** | 83.16 | 83.11–83.21 | 83.71 | 88.76 |

### CloudPhysics

Block I/O of a virtual machine (trace w105): 3,375,344 requests for 654,126 keys.
Trace: Waldspurger, Park, Garthwaite and Ahmad, "Efficient MRC Construction with SHARDS", FAST '15; in the format of libCacheSim, https://github.com/cacheMon/cache_dataset (CC BY 4.0).

![Hit ratios on the CloudPhysics trace by cache capacity](hitratio-cloudphysics.svg)

| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | otter, range | golang-lru/2q | optimal |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 12.53 | 13.56 | 13.14 | **13.65** | 13.25 | 13.12–13.43 | 13.32 | 16.11 |
| 2,500 | 13.59 | 14.31 | 14.23 | **14.44** | 14.27 | 14.13–14.29 | 14.26 | 18.45 |
| 5,000 | 14.46 | 15.09 | 14.83 | 15.22 | 14.91 | 14.84–15.23 | 15.05 | 21.04 |
| 10,000 | 15.75 | 17.41 | 16.03 | **17.84** | 16.63 | 16.45–16.86 | 16.44 | 25.25 |
| 25,000 | 21.01 | 25.11 | 26.46 | **27.62** | 25.23 | 24.22–25.66 | 25.55 | 34.01 |
| 50,000 | 23.28 | 31.65 | 29.16 | **33.87** | 31.62 | 30.90–31.91 | 32.33 | 42.28 |
| 100,000 | 28.48 | 37.99 | 36.83 | **43.28** | 39.76 | 39.46–40.29 | 40.49 | 50.92 |
| 200,000 | 38.09 | 40.14 | 46.93 | 54.81 | **58.19** | 58.01–58.75 | 47.26 | 65.73 |
