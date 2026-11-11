# Benchmarks

Three questions about a cache, three benchmarks:

| Question | Run | Result |
|:---|:---|:---|
| How many requests does it answer? | `make bench-hitratio` | [results/hitratio.md](results/hitratio.md), [results/sharding.md](results/sharding.md) |
| How fast is it, alone and with every CPU at work? | `make bench-throughput` | [results/throughput.md](results/throughput.md) |
| What does it cost in memory, and what does it cost the garbage collector? | `make bench-memory` | [results/memory.md](results/memory.md) |

`make bench` runs all of them. The targets are in the Makefile at the root of the repository;
each is a `go` command that can just as well be run from this directory.

The benchmarks are a Go module of their own, so that the caches do not depend on the
libraries they are compared with.

## What is compared

| Name | What it is |
|:---|:---|
| `evict/lru`, `evict/twoq`, `evict/sieve`, `evict/s3fifo` | The caches of this module. |
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
therefore replays the very same requests, and the numbers for the caches of this module
and for golang-lru come out the same to the last digit.

Otter's do not. It admits some entries at random, and its hit ratio moves from one replay
to the next because of it: by several percentage points at some capacities, and hardly at
all at others. A cache like that is replayed eleven times (`-runs`), and the report has the
median and the range. A cache is only called the winner of a row if it beat the others on
every replay.

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
cache as well, from 87.0% to 87.6%. That is not the warm-up, and why it does was not looked
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
- The results in this repository are from one machine, an Apple laptop. In the results that
  otter publishes, which are from Linux on an x86 CPU, its writes get more than five times
  as fast between one CPU and eight (from 8 to 45 million a second). Compare that with what
  they do in the `Set` table here before concluding anything about writes: how the caches
  compare on such a machine may well differ. The `Benchmarks` workflow in
  `.github/workflows` runs the suite on one of GitHub's Linux runners for a second opinion.

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
| `cmd/hitratio`, `cmd/memory`, `cmd/benchtable` | The commands. |
| `throughput_test.go` | The throughput benchmarks. |
| `results/` | What the benchmarks measured the last time they were run for this repository. |
