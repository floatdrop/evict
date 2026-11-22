# Benchmarks

Three questions about a cache, three benchmarks:

| Question | Run | Result |
|:---|:---|:---|
| How many requests does it answer? | `make bench-hitratio` | [results/hitratio.md](results/hitratio.md), [results/sharding.md](results/sharding.md) |
| How fast is it, alone and with every CPU at work? | `make bench-throughput` | [results/throughput.md](results/throughput.md), [on 16 cores](results/yandex-standard-v3-32/throughput.md) |
| What does it cost in memory, and what does it cost the garbage collector? | `make bench-memory` | [results/memory.md](results/memory.md) |

`make bench` runs all of them. The targets are in the Makefile at the root of the repository;
each is a `go` command that can just as well be run from this directory.

A fourth question is only about `evict/s3fifo`: what would it answer with other numbers
for its queues? `make bench-tuning` replays the traces with 84 settings and writes
[results/tuning.md](results/tuning.md); see [Tuning S3-FIFO](#tuning-s3-fifo) below.

The benchmarks are a Go module of their own, so that the caches do not depend on the
libraries they are compared with.

## Results

### Hit ratio

Percent of requests answered when a trace is replayed, averaged over eight cache sizes per
trace. Ten traces are public recordings of real systems; `optimal` is the upper bound. Otter
and `tinylfu` vary between replays (by up to 4.1 and 5.6 points), so theirs are medians of 11.
The best in each row is <ins>underlined</ins>. In OLTP, Glimpse and CloudPhysics no cache beat
the others on every replay.

| trace | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu | otter | optimal |
|:---|---:|---:|---:|---:|---:|---:|---:|
| Zipf | 29.97 | 36.53 | 37.55 | <ins>38.25</ins> | 37.66 | 37.90 | 44.07 |
| S3 | 23.33 | 32.78 | 35.54 | 38.90 | <ins>44.83</ins> | 44.57 | 59.37 |
| DS1 | 23.70 | 28.88 | 38.87 | 37.05 | <ins>47.07</ins> | 46.54 | 50.21 |
| OLTP | 32.40 | 39.07 | 30.60 | <ins>39.11</ins> | 38.76 | 38.97 | 52.77 |
| P8 | 24.10 | 32.11 | 27.66 | 35.31 | <ins>37.86</ins> | 36.20 | 53.53 |
| Glimpse | 21.81 | 27.40 | 30.14 | 43.24 | 45.20 | <ins>45.28</ins> | 47.81 |
| Mixed | 44.11 | 56.80 | 0.43 | 43.84 | 60.32 | <ins>61.61</ins> | 64.22 |
| Scarab | 59.10 | 61.00 | 60.53 | <ins>61.60</ins> | 60.31 | 58.99 | 70.30 |
| Twitter | 58.37 | 59.72 | 58.99 | <ins>60.32</ins> | 59.59 | 59.29 | 65.46 |
| MSR | 64.40 | 66.31 | 66.16 | <ins>67.22</ins> | 67.06 | 66.54 | 73.60 |
| CloudPhysics | 20.90 | 24.41 | 24.70 | 27.59 | <ins>27.80</ins> | 26.77 | 34.23 |

- Of the 88 trace-and-size rows, `s3fifo` wins 32 outright, `tinylfu` 25, hashicorp's 2Q 4,
  `twoq` 2 and otter 1; 24 are too close to call.
- `s3fifo` leads on Zipf, OLTP, Scarab, Twitter and MSR, though by less than a point.
- `tinylfu` and otter lead on the traces with loops and on the ARC disk traces (S3, DS1,
  Glimpse, Mixed), by 2 to 18 points over `s3fifo`.
- How far each cache falls behind the best one in its worst row: `tinylfu` 5.3 points, otter
  5.9, `twoq` 32, `s3fifo` and `lru` 63, `sieve` 76.
- `tinylfu` has the best average (47.9% of requests, against 47.5% for otter and 44.8% for
  `s3fifo`). It was designed on these same traces, though; on 14 others, in a simulation not
  in this repository, it was level with S3-FIFO and W-TinyLFU. It trails `lru` by up to 4
  points on Mixed at large sizes.

These caches ran in one piece. Default sharding moves most numbers by a tenth of a point or
less; it helps `sieve` a lot on its bad traces and costs `tinylfu` up to 2.6 points
([sharding.md](results/sharding.md)). Every capacity, with a chart per trace, is in
[hitratio.md](results/hitratio.md).

### Throughput

Millions of operations per second on one CPU and on eight (`1 → 8`), against a cache of
16,384 string entries unless noted. Medians of five runs on an Apple M3 Max with Go 1.27.3;
[throughput.md](results/throughput.md) has two and four CPUs and the spread.

| Benchmark | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu | otter |
|:---|---:|---:|---:|---:|---:|---:|
| `Get`: reads, all of them hits | 40.2 → 34.7 | 47.3 → 44.9 | 40.1 → 317.8 | 40.7 → 308.7 | 38.9 → 307.0 | 26.1 → 204.7 |
| `Mixed`: the same, 10% of them writes | 40.3 → 32.4 | 46.4 → 38.9 | 35.7 → 71.6 | 36.6 → 64.2 | 35.3 → 61.4 | 19.7 → 80.3 |
| `Set`: writes that replace | 38.1 → 24.1 | 44.0 → 26.9 | 22.0 → 11.8 | 22.4 → 12.6 | 22.5 → 11.6 | 8.7 → 15.3 |
| `Evict`: writes that evict | 17.8 → 31.6 | 16.9 → 30.3 | 14.4 → 12.0 | 9.6 → 10.3 | 8.4 → 9.5 | 3.4 → 5.6 |
| `ReadThrough`: read, store on a miss | 31.3 → 35.3 | 30.6 → 35.9 | 31.3 → 55.1 | 23.8 → 42.2 | 22.7 → 46.0 | 10.9 → 22.8 |
| `ReadThrough`, a million entries | 7.2 → 28.6 | 7.6 → 27.3 | 7.3 → 41.1 | 6.8 → 35.5 | 6.8 → 37.3 | 4.0 → 17.2 |

- Lock-free reads scale: on 8 CPUs `sieve`, `s3fifo` and `tinylfu` read 50–55% faster than
  otter, while `lru` and `twoq` slow down past 4 CPUs.
- On one CPU all five caches beat otter at everything, by 1.5 to 5 times.
- `lru` and `twoq` change entries in place and write fastest. The lock-free caches move a
  replaced entry to a fresh node so that readers need no lock, and their writes cost 1.2 to
  3.3 times as much.
- Otter wins on 8 CPUs when 10–25% of the operations overwrite entries.
- Writes do not scale with CPUs: each takes its shard's lock and pulls the lines it changes
  from the CPU that wrote them last, and in `Set` a tenth of the writes go to one key. These
  caches are made for workloads that mostly read, so they leave out what otter does about it,
  queueing the work of its policy and applying it in batches.
- Otter's own numbers, from Linux on x86, show its writes scaling five-fold from one CPU to
  eight, against less than two-fold here. On the Xeon below they scale three-fold.

#### On 16 cores of an Intel Xeon

The same benchmarks on one CPU and on sixteen (`1 → 16`), on a virtual machine in Yandex
Cloud with 16 cores of an Intel Xeon (Ice Lake), one thread of each. Medians of five runs
on Linux with Go 1.27.3, made with [yandex-cloud.sh](yandex-cloud.sh);
[throughput.md](results/yandex-standard-v3-32/throughput.md) has 2, 4 and 8 CPUs and the
spread.

| Benchmark | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu | otter |
|:---|---:|---:|---:|---:|---:|---:|
| `Get`: reads, all of them hits | 22.6 → 22.6 | 25.5 → 30.8 | 23.3 → 343.4 | 23.9 → 341.5 | 21.6 → 300.6 | 14.8 → 236.0 |
| `Mixed`: the same, 10% of them writes | 21.8 → 21.7 | 24.3 → 27.7 | 20.3 → 76.0 | 20.7 → 76.3 | 18.6 → 73.0 | 10.9 → 88.2 |
| `Set`: writes that replace | 21.3 → 18.7 | 23.5 → 20.4 | 12.2 → 9.9 | 12.4 → 10.8 | 12.0 → 10.6 | 4.1 → 13.5 |
| `Evict`: writes that evict | 9.8 → 31.7 | 8.6 → 28.6 | 7.3 → 24.5 | 4.6 → 19.5 | 3.9 → 16.0 | 1.3 → 2.0 |
| `ReadThrough`: read, store on a miss | 14.9 → 26.2 | 14.1 → 24.9 | 14.4 → 93.2 | 11.2 → 75.4 | 10.3 → 62.1 | 4.8 → 12.6 |
| `ReadThrough`, a million entries | 4.6 → 33.8 | 4.5 → 32.2 | 4.8 → 69.4 | 4.3 → 58.8 | 3.9 → 53.8 | 2.4 → 9.8 |

- Reads scale with the cores, otter's too: from one CPU to 16, `Get` gets 14.7 times as fast
  with `sieve` and 15.9 times with otter. On 16 CPUs `sieve`, `s3fifo` and `tinylfu` read
  27–46% faster than otter. `lru` and `twoq` get about as much done on 16 CPUs as on one.
- One CPU of the Xeon gets 41–46% less done in `Get` than one of the M3 Max. On it the five
  caches beat otter at everything, by 1.5 to 7.5 times.
- Otter's `Set` gets 3.0 times as fast from one CPU to 8 and 3.3 times to 16. That of the
  lock-free caches stays near 10 million a second from 8 CPUs on, so from there otter writes
  faster than they do. `lru` and `twoq` write faster than otter on every number of CPUs.
- On 16 CPUs otter wins when 10–25% of the operations overwrite entries, by 16% with 10% and
  by 24% with 25%. On 8 the lock-free caches beat it in every run with 10%, and are level
  with it with 25%.
- Evicting scales for the caches of this module and not for otter: from one CPU to 16,
  `Evict` gets 3.2 to 4.2 times as fast with them, while otter gets no further than 2.6
  million a second. On the M3 Max it got at most 13% faster for the lock-free caches, and
  slower for `sieve`.
- With `ReadThrough` and a million entries the lock-free caches do 2.8–3.6 times as much as
  otter on 8 CPUs and 5.5–7.1 times as much on 16. Otter gets little more done on 16 CPUs
  than on 4 (9.8 million a second against 8.9).

### Memory

Bytes per entry with `uint64` keys and values, over caches of 10,000 to 10 million entries;
[memory.md](results/memory.md) has every size, and strings.

| | Bytes per entry | of which the GC scans | One GC cycle, 1M entries | 10M entries |
|:---|---:|---:|---:|---:|
| evict/lru | 56–63 | 0–2 | 0.2 ms | 0.2 ms |
| evict/sieve | 64–81 | 0–2 | 0.3 ms | 0.3 ms |
| evict/twoq | 84–93 | 0–1 | 0.3 ms | 0.2 ms |
| evict/s3fifo | 82–100 | 0–2 | 0.5 ms | 0.2 ms |
| evict/tinylfu | 76–95 | 0–2 | 0.4 ms | 0.2 ms |
| otter | 83–95 | 40–50 | 65 ms | 530 ms |
| golang-lru | 110–127 | 94–111 | 66 ms | 719 ms |
| golang-lru/2q | 156–183 | 136–163 | 101 ms | 1,157 ms |

The garbage collector's work grows with caches built from pointers and stays near zero for
these. That holds for keys and values without pointers; what strings cost the collector
was not measured.

## What is compared

| Name | What it is |
|:---|:---|
| `evict/lru`, `evict/twoq`, `evict/sieve`, `evict/s3fifo`, `evict/tinylfu` | The caches of this module. |
| `otter` | [otter](https://github.com/maypok86/otter) v2, the state of the art among Go caches. It evicts with adaptive W-TinyLFU, reads without locks, and writes through buffers that a background task drains. |
| `golang-lru`, `golang-lru/2q` | [hashicorp/golang-lru](https://github.com/hashicorp/golang-lru) v2: an LRU and a 2Q cache behind a single lock, the way most caches used to be built. They are here for perspective, and `golang-lru` also as a check: the tests require `evict/lru` to agree with it on every single request of a trace. Its 2Q is not the same algorithm as `evict/twoq`: it promotes an entry that is read again while it is still recent, where the 2Q of the paper waits for it to come back after it has been evicted. |
| `optimal` | Not a cache but the bound on hit ratios: what a cache would score that knew the requests to come ([Belady's algorithm](https://en.wikipedia.org/wiki/Cache_replacement_policies#Belady's_algorithm), with the option of not storing a key at all). |

Otter is created the way its own benchmarks create it, which includes telling it the
capacity up front (`InitialCapacity`), so that it does not have to grow while it is measured.

## Hit ratios

`go run ./cmd/hitratio` replays traces of real workloads: every key of a trace is looked up
in turn and stored if it is missing. The share of lookups that are answered is the hit ratio.
For the traces that otter's benchmarks replay as well, the capacities are the same as there.

So that the eviction policy is all that is compared, the caches of this module run in one
piece, with a single shard, and otter is told to evict on the spot instead of in the
background, as it is in its own simulator.

The traces are downloaded on first use, into the user's cache directory unless `-dir` says
otherwise, and each file is checked against the SHA-256 that
[dataset/dataset.go](dataset/dataset.go) has on record for it. A run on another machine
therefore replays the very same requests, and the numbers for golang-lru and for the caches
of this module but `evict/tinylfu` come out the same to the last digit.

Otter's do not. It admits some entries at random, and its hit ratio moves from one replay
to the next because of it: by several percentage points at some capacities, and hardly at
all at others. Nor do those of `evict/tinylfu`, whose sketch has keys share counters by a
hash that is seeded at random, so that which keys it confuses differs from one replay to the
next. A cache like that is replayed eleven times (`-runs`), and the report has the median and
the range. A cache is only called the winner of a row if it beat the others on every
replay.

| Dataset | Workload | Requests | Keys | Download |
|:---|:---|---:|---:|---:|
| `zipf` | Synthetic: Zipf-distributed popularity (s = 1.0001, v = 10) | 1,000,000 | 456,052 | generated |
| `s3` | Disk reads of a search engine | 16,407,702 | 1,689,882 | 50.0 MB |
| `ds1` | Disk reads of a database server running an ERP application, seven days | 43,704,979 | 10,516,352 | 7.4 MB |
| `oltp` | References to a CODASYL database, one hour | 914,145 | 186,880 | 2.2 MB |
| `p8` | Disk reads of a Windows NT workstation | 42,243,785 | 977,545 | 15.0 MB |
| `glimpse` | Block reads of the text search tool glimpse, which scans in loops | 6,015 | 2,529 | 6 kB |
| `mixed` | The vault service of a Corda node, then a program reading blocks in a loop, then the vault service again | 776,904 | 91,479 | 822 kB |
| `scarab` | Lookups of an e-commerce recommendation service, one hour | 1,245,184 | 258,368 | 3.1 MB |
| `twitter` | Requests to an in-memory key-value cache at Twitter (cluster 45, 1% of keys) | 2,245,093 | 654,573 | 17.9 MB |
| `msr` | Block I/O of a server at Microsoft Research Cambridge (volume hm_0) | 3,993,316 | 439,187 | 18.7 MB |
| `cloudphysics` | Block I/O of a virtual machine (trace w105) | 3,375,344 | 654,126 | 11.7 MB |

Where the traces come from, and whom to credit for them:

- `s3`, `ds1`, `oltp`, `p8`: Megiddo and Modha, "ARC: A Self-Tuning, Low Overhead Replacement
  Cache", FAST '03.
- `glimpse`, and the loop in `mixed`: Jiang and Zhang, "LIRS: An Efficient Low Inter-reference
  Recency Set Replacement Policy to Improve Buffer Cache Performance", SIGMETRICS '02.
- `scarab`: Scarab Research, published with the simulator of
  [Caffeine](https://github.com/ben-manes/caffeine).
- The vault service in `mixed`: R3, the makers of Corda, published with the simulator of
  Caffeine as well.
- `twitter`: Yang, Yue and Rashmi, "A large scale analysis of hundreds of in-memory cache
  clusters at Twitter", OSDI '20.
- `msr`: Narayanan, Donnelly and Rowstron, "Write Off-Loading: Practical Power Management for
  Enterprise Storage", FAST '08.
- `cloudphysics`: Waldspurger, Park, Garthwaite and Ahmad, "Efficient MRC Construction with
  SHARDS", FAST '15.

All but the last three are fetched from otter's repository at a fixed commit, which keeps the
files that otter's benchmarks replay; `zipf` is generated, with the parameters that otter
uses, and `mixed` strings its files together the way otter's benchmarks do. The last three
are in the `oracleGeneral` format of [libCacheSim](https://github.com/1a1a11a/libCacheSim)
and come from the [cache_dataset](https://github.com/cacheMon/cache_dataset) collection,
which is published under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), by way
of Carnegie Mellon's Parallel Data Lab or, failing that, the collection's public bucket.

Otter's benchmarks also have a configuration for a trace called P3, which its documentation
does not show. It is not replayed here.

### Shards

A program does not use the caches of this module in one piece: by default they are split
into shards, and a shard that is full evicts although another one may have room.
`go run ./cmd/hitratio -sharding` measures what that does to the hit ratio, and
[results/sharding.md](results/sharding.md) has the answer for every trace: the difference
between a cache with its default shards and the same cache in one piece. Which key goes to
which shard depends on a hash that is seeded at random, so these numbers vary as well, and
are reported with their range.

`-caches evict/sieve,evict/sieve@16,evict/sieve@256` puts caches with a number of shards of
one's choosing next to each other.

### Tuning S3-FIFO

`evict/s3fifo` has four options that change the numbers of the paper: the share of the
small queue, the number of ghosts, the reads that it takes to move up from the small queue,
and the times a ghost has to come back. `go run ./cmd/tune` replays every trace with each of
84 combinations of them, in one piece, and reports what the cache answers as it is, with
the one setting that is best for all traces, and with the setting that is best for each.

To find the setting for a workload of your own, record its keys and replay the recording:

```
go run ./cmd/tune -trace requests.txt -format lirs -capacities 10000,100000 -out tuned
```

A recording in the format `lirs` is a text file with a number for every request, one per
line; hash your keys to numbers if they are something else. The other formats are those of
the published traces: `arc`, `oraclegeneral` and `bigendian`. The file may be compressed
with gzip, zstd or xz. The report names the options to pass to `s3fifo.New`.

## Throughput

`go test -bench . -cpu 1,2,4,8` runs [throughput_test.go](throughput_test.go). Every
benchmark has as many goroutines as CPUs working on one cache. Keys and values are strings.
Unless it says otherwise below, the popularity of the keys follows a Zipf distribution, so
a few of them get most of the requests, as in real workloads; that is what makes goroutines
get in each other's way. Every goroutine draws its requests on its own, one independent of
the next, so a workload never repeats itself and does not change with the number of
goroutines or with the length of a run.

The cache is created the way its library suggests, for the number of CPUs at hand. Before
the clock starts it is put to use: it has been full, and it has seen sixteen requests of
the workload for every entry it has room for. A cache that has never been full is faster
than that for some policies, which then have nothing to sort yet.

Sixteen requests for every entry do not teach a policy all there is to learn about a
workload: a cache that tells popular keys from the others, as all but the LRU caches do,
goes on to answer a little more for hundreds of requests per entry. With 16,384 entries, a
second of a benchmark is hundreds to thousands of requests per entry, so most of what it
measures comes after that. With a million entries it is not: the share of reads that
`ReadThrough` reports for those caches is that of a cache which is still learning, and a
few tenths of a percentage point higher where a cache got through more requests in its
second, as it does with more CPUs. Otter's share moves with the number of CPUs in the small
cache as well, between 86.9% with two CPUs and 87.6% with eight. That is not the warm-up, and why it does was not looked
into.

| Benchmark | What every goroutine does |
|:---|:---|
| `Get` | Reads keys that are all cached: there are half as many as the cache has room for, and the rest of the room is taken by keys that nobody asks for. |
| `Set` | Replaces the values of the same keys. |
| `Mixed` | Reads them, and one time in a hundred, in ten or in four replaces a value instead. |
| `Evict` | Writes keys from a set four times the size of the cache, each as likely as any other, so that three writes in four add an entry and evict another. |
| `ReadThrough` | Reads a key from a set four times the size of the cache and stores it if it is missing. This is what a cache in front of a database does, and the benchmark in which the eviction policy has a say: fewer misses mean fewer writes. The hit ratio is reported next to the throughput. |

`make bench-throughput` runs all of this five times from start to end, rather than each
benchmark five times in a row, so that a noisy minute on the machine does not land on one
cache alone. `go run ./cmd/benchtable < results/throughput.txt` turns the output into the
tables of `results/throughput.md`: the median of the runs in millions of operations per
second, with the slowest and the fastest run next to it. A cache is called the winner of a
row only if its slowest run beat the fastest of every other cache, and only if there are
at least three runs to go by.

Take the absolute numbers with a grain of salt.

- A benchmark that does nothing but call a cache overstates the differences that a program
  would see, and contention of the kind that it creates is rare in practice. The shape is
  what carries over: what gets faster with more CPUs, what gets slower, and by how much.
- `Get` is the best case of the caches that read without a lock, and of SIEVE and S3-FIFO in
  particular: nothing is evicted, and reading an entry that has been read before writes
  nothing at all.
- The results in this repository are from two machines: an Apple laptop, and 16 cores of an
  Intel Xeon in Yandex Cloud. In the results that otter publishes, which are from Linux on
  an x86 CPU, its writes get more than five times as fast between one CPU and eight (from 8
  to 45 million a second); on the Xeon here they get three times as fast, and on the laptop
  less than twice. How the caches compare on another machine may well differ again. The
  `Benchmarks` workflow in `.github/workflows` runs the suite on one of GitHub's Linux
  runners, with four CPUs. [yandex-cloud.sh](yandex-cloud.sh) rents a virtual machine in
  Yandex Cloud, runs the benchmarks on one thread of each of its cores (16 by default), and
  deletes it again; the comment at its top has the settings.

## Memory

`go run ./cmd/memory` creates a cache of every kind at a range of capacities, fills it, and
reports the growth of the live heap per entry of capacity. For the garbage collector it
reports how much of that the collector has to look through for pointers, which hardly
differs from run to run, and the CPU time that a collection takes while the cache is alive,
which does: it is given as the least and the most of seven collections.

## Layout

| Path | What is there |
|:---|:---|
| `trace/` | Readers for the formats that traces are published in. |
| `dataset/` | The list of traces, with their checksums, and the code that fetches them. |
| `caches/` | The caches under one interface. |
| `replay/` | The replay of a trace against a cache, and the optimal cache. |
| `report/` | The tables and the charts. |
| `cmd/hitratio`, `cmd/tune`, `cmd/memory`, `cmd/benchtable` | The commands. |
| `throughput_test.go` | The throughput benchmarks. |
| `results/` | What the benchmarks measured the last time they were run for this repository. |
