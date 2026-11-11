# evict

[![Go Reference](https://pkg.go.dev/badge/github.com/floatdrop/evict.svg)](https://pkg.go.dev/github.com/floatdrop/evict)
[![CI](https://github.com/floatdrop/evict/actions/workflows/ci.yml/badge.svg)](https://github.com/floatdrop/evict/actions/workflows/ci.yml)

Fixed-capacity in-memory caches for Go.

| Package | Policy | Reads |
|:---|:---|:---|
| [`lru`](https://pkg.go.dev/github.com/floatdrop/evict/lru) | least recently used | under a shard lock |
| [`twoq`](https://pkg.go.dev/github.com/floatdrop/evict/twoq) | [2Q](http://www.vldb.org/conf/1994/P439.PDF): new entries kept apart from proven ones | under a shard lock |
| [`sieve`](https://pkg.go.dev/github.com/floatdrop/evict/sieve) | [SIEVE](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo): a queue with lazy promotion | lock-free |
| [`s3fifo`](https://pkg.go.dev/github.com/floatdrop/evict/s3fifo) | [S3-FIFO](https://dl.acm.org/doi/10.1145/3600006.3613147): new entries prove themselves in a small queue | lock-free |
| [`tinylfu`](https://pkg.go.dev/github.com/floatdrop/evict/tinylfu) | S3-FIFO's queues with [TinyLFU](https://arxiv.org/abs/1512.00727) admission: a new entry must have been asked for more often than the one it would evict | lock-free |

All of them

- allocate nothing after `New`: entries live in arrays, linked by 32-bit indices;
- give the garbage collector nothing to scan when keys and values hold no pointers;
- return what they dropped: `Set` hands back the replaced or evicted entry;
- are safe for concurrent use;
- depend only on the standard library.

```
go get github.com/floatdrop/evict
```

Requires Go 1.26 or later.

## Usage

```go
package main

import (
	"fmt"

	"github.com/floatdrop/evict/tinylfu"
)

func main() {
	cache := tinylfu.New[string, int](10_000)

	cache.Set("answer", 42)

	if value, ok := cache.Get("answer"); ok {
		fmt.Println(value)
	}

	// Set hands back what it pushed out: the previous entry for the key, or
	// the entry that was evicted to make room.
	if key, value, displaced := cache.Set("answer", 43); displaced {
		fmt.Println("dropped", key, value)
	}

	for key, value := range cache.All() {
		fmt.Println(key, value)
	}
}
```

Every package implements [`evict.Cache`](https://pkg.go.dev/github.com/floatdrop/evict#Cache):

| Method | |
|:---|:---|
| `Get(key) (value, ok)` | Looks up an entry and counts it as used. |
| `Peek(key) (value, ok)` | Looks up an entry without counting it. |
| `Set(key, value) (oldKey, oldValue, displaced)` | Stores an entry; returns the one it pushed out. |
| `Delete(key) (value, ok)` | Removes an entry. |
| `Len()`, `Cap()` | The number of entries, and the capacity. |
| `Clear()` | Removes every entry. |
| `All()` | Iterates over the entries. |

Every package takes `WithShards`. `twoq` and `s3fifo` also have options for tuning their
queues ([what that is worth for `s3fifo`](bench/results/tuning.md)); `tinylfu` tunes its
own.

## Choosing a policy

| | Use it when | Watch out for |
|:---|:---|:---|
| `tinylfu` | **in doubt**: the workload is unknown, or has loops or scans, and is read far more than written | Evicting writes are 5–10% slower than in `s3fifo`, and with 1% writes on 8 CPUs it was 22% slower, for a reason not yet found. Hit ratios vary between runs by up to 5.6 points. |
| `s3fifo` | you want the same results on every run, or the workload has no loops | Falls far behind on looping workloads: on Mixed, at some sizes, it hits as rarely as `lru`. Writes cost about twice what they do in `lru`. |
| `sieve` | reading through has to be as fast as possible, and you have checked your workload | It can collapse: on Mixed it answers under 1% of requests where `lru` answers 44%; see [when not to use it](https://pkg.go.dev/github.com/floatdrop/evict/sieve#hdr-When_not_to_use_it). |
| `twoq` | a quarter or more of the operations are writes, or one goroutine uses the cache | Reads take a lock; more memory than `lru`. |
| `lru` | memory counts most, or you want behavior everyone knows | The lowest hit ratio on 9 of the 11 traces; reads take a lock. |

`tinylfu` has the best average hit ratio here (47.9% of requests, against 47.5% for otter
and 44.8% for `s3fifo`) and is never more than 5.3 points behind the best cache. It was
designed on these same traces, though; on 14 others, in a simulation not in this
repository, it was level with S3-FIFO and W-TinyLFU. It trails `lru` by up to 4 points on
Mixed at large sizes; [its docs](https://pkg.go.dev/github.com/floatdrop/evict/tinylfu) say
where else it does badly.

Use [otter](https://github.com/maypok86/otter) instead if you need expiry, weights, loading or
statistics, or if many CPUs overwrite a tenth to a quarter of what they read.

## Benchmarks

[`bench`](bench) is a separate module that compares these caches with otter v2 and hashicorp's
`golang-lru`. [bench/README.md](bench/README.md) explains the method, `make bench` reruns it,
and [bench/results](bench/results) has the full reports.

### Hit ratio

Percent of requests answered when a trace is replayed, averaged over eight cache sizes per
trace. Ten traces are public recordings of real systems; `optimal` is the upper bound. Otter
and `tinylfu` vary between replays (by up to 4.1 and 5.6 points), so theirs are medians of 11;
**bold** means the cache beat every other on every replay.

| trace | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu | otter | golang-lru/2q | optimal |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|
| Zipf | 29.97 | 36.53 | 37.55 | **38.25** | 37.66 | 37.90 | 37.61 | 44.07 |
| S3 | 23.33 | 32.78 | 35.54 | 38.90 | **44.83** | 44.57 | 33.15 | 59.37 |
| DS1 | 23.70 | 28.88 | 38.87 | 37.05 | **47.07** | 46.54 | 34.47 | 50.21 |
| OLTP | 32.40 | 39.07 | 30.60 | 39.11 | 38.76 | 38.97 | 37.60 | 52.77 |
| P8 | 24.10 | 32.11 | 27.66 | 35.31 | **37.86** | 36.20 | 31.45 | 53.53 |
| Glimpse | 21.81 | 27.40 | 30.14 | 43.24 | 45.20 | 45.28 | 33.20 | 47.81 |
| Mixed | 44.11 | 56.80 | 0.43 | 43.84 | 60.32 | **61.61** | 35.97 | 64.22 |
| Scarab | 59.10 | 61.00 | 60.53 | **61.60** | 60.31 | 58.99 | 61.14 | 70.30 |
| Twitter | 58.37 | 59.72 | 58.99 | **60.32** | 59.59 | 59.29 | 58.80 | 65.46 |
| MSR | 64.40 | 66.31 | 66.16 | **67.22** | 67.06 | 66.54 | 66.57 | 73.60 |
| CloudPhysics | 20.90 | 24.41 | 24.70 | 27.59 | 27.80 | 26.77 | 25.59 | 34.23 |

- Of the 88 trace-and-size rows, `s3fifo` wins 32 outright, `tinylfu` 25, the 2Q caches 6
  and otter 1; 24 are too close to call.
- `s3fifo` leads on Zipf, OLTP, Scarab, Twitter and MSR, though by less than a point.
- `tinylfu` and otter lead on the traces with loops and on the ARC disk traces (S3, DS1,
  Glimpse, Mixed), by 2 to 18 points over `s3fifo`.
- How far each cache falls behind the best one in its worst row: `tinylfu` 5.3 points, otter
  5.9, `twoq` 32, `s3fifo` and `lru` 63, `sieve` 76.

These caches ran in one piece. Default sharding moves most numbers by a tenth of a point or
less; it helps `sieve` a lot on its bad traces and costs `tinylfu` up to 2.6 points
([sharding.md](bench/results/sharding.md)).

### Throughput

Millions of operations per second on one CPU and on eight (`1 → 8`), against a cache of
16,384 string entries unless noted. Medians of five runs on an Apple M3 Max with Go 1.27.3;
[throughput.md](bench/results/throughput.md) has two and four CPUs and the spread.

| Benchmark | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu | otter | golang-lru |
|:---|---:|---:|---:|---:|---:|---:|---:|
| `Get`: reads, all of them hits | 39.7 → 35.9 | 46.7 → 44.4 | 35.6 → 265.8 | 36.1 → 274.7 | 35.0 → 258.1 | 27.2 → 202.3 | 34.2 → 4.3 |
| `Mixed`: the same, 10% of them writes | 37.6 → 34.0 | 42.5 → 42.8 | 32.9 → 68.6 | 33.4 → 69.9 | 33.1 → 62.6 | 21.7 → 80.8 | 33.5 → 4.4 |
| `Set`: writes that replace | 36.5 → 24.9 | 43.3 → 28.4 | 22.4 → 13.8 | 22.5 → 13.6 | 22.2 → 12.8 | 8.9 → 15.9 | 32.1 → 4.3 |
| `Evict`: writes that evict | 16.5 → 31.3 | 16.9 → 30.9 | 13.4 → 12.0 | 8.6 → 10.5 | 8.2 → 9.8 | 3.6 → 5.9 | 7.5 → 3.3 |
| `ReadThrough`: read, store on a miss | 30.3 → 34.1 | 31.9 → 33.7 | 28.6 → 53.3 | 22.1 → 43.1 | 21.7 → 46.0 | 12.7 → 23.6 | 16.7 → 3.6 |
| `ReadThrough`, a million entries | 8.8 → 30.1 | 8.4 → 28.7 | 8.9 → 41.5 | 7.2 → 36.8 | 7.5 → 37.8 | 4.3 → 17.9 | 6.3 → 2.9 |

![Throughput of ReadThrough with a million entries by number of CPUs](bench/results/throughput-readthrough-capacity-1m.svg)

- Lock-free reads scale: on 8 CPUs `sieve`, `s3fifo` and `tinylfu` read 28–36% faster than
  otter, while `lru` and `twoq` slow down past 4 CPUs.
- On one CPU all five caches beat otter at everything, by 1.3 to 5 times.
- `lru` and `twoq` change entries in place and write fastest. The lock-free caches move a
  replaced entry to a fresh node so that readers need no lock, and their writes cost 1.2 to
  3.4 times as much.
- Otter wins on 8 CPUs when 10–25% of the operations overwrite entries.
- These are numbers from one machine. Otter's own, from Linux on x86, show its writes
  scaling five-fold from one CPU to eight, against less than two-fold here.

### Memory

Bytes per entry with `uint64` keys and values, over caches of 10,000 to 10 million entries;
[memory.md](bench/results/memory.md) has every size, and strings.

| | Bytes per entry | of which the GC scans | One GC cycle, 1M entries | 10M entries |
|:---|---:|---:|---:|---:|
| evict/lru | 46–58 | 0–1 | 0.2 ms | 0.2 ms |
| evict/sieve | 54–72 | 0–2 | 0.4 ms | 0.2 ms |
| evict/twoq | 74–82 | 0–1 | 0.4 ms | 0.2 ms |
| evict/s3fifo | 68–90 | 0–2 | 0.3 ms | 0.2 ms |
| evict/tinylfu | 67–82 | 0–2 | 0.4 ms | 0.2 ms |
| otter | 83–94 | 40–50 | 64 ms | 634 ms |
| golang-lru | 110–127 | 93–111 | 65 ms | 740 ms |
| golang-lru/2q | 156–183 | 136–163 | 97 ms | 1,167 ms |

Sizes swing with capacity because indexes come in powers of two. The garbage collector's
work does not: it grows with caches built from pointers and stays near zero for these. That
holds for keys and values without pointers; what strings cost the collector was not
measured.

## How it works

- **Arrays, not pointers.** Each shard allocates its nodes once and links them by 32-bit
  index. An open-addressing index maps a 32-bit hash to a node, so a lookup compares hashes
  before it touches an entry.
- **Shards.** The upper half of a key's hash picks a shard, which writers lock on its own;
  the lower half indexes the key within it. Each shard evicts from its own share of the
  capacity. A cache under 512 entries is one shard; a larger one has up to eight per CPU, of
  256 entries or more. `WithShards(1)` gives a policy's textbook behavior.
- **Lock-free reads** (`sieve`, `s3fifo`, `tinylfu`). A reader pins the node it reads in a
  slot of its own and checks that the index still points there. Writers never change a node
  that a reader can reach: a new value goes into a fresh node, and a node is reused only
  once no slot pins it. A read writes only to a cache line its CPU has to itself. Removed
  entries stay referenced until their shard collects them in a batch (an eighth of its
  capacity, 1,024 at most); `Clear` lets go of everything. The slots take 2–4 KiB per CPU,
  so for very many small caches, use `lru`.
- **Admission** (`tinylfu`). Each shard keeps a count-min sketch of how often keys are
  asked for, cached or not; only writers update it, picking up the reads counted on
  entries. Once the shard is full, a new entry gets into the main queue only if the sketch
  rates its key above the entry that queue would evict next. The size of the small queue
  is tuned by hill climbing, as in Caffeine.
- **Tests.** Every cache runs one conformance suite, is checked against a simple model of
  its policy step by step, and is fuzzed against that model. Lock-free reads are tested
  under the race detector. `lru` must agree with hashicorp's `golang-lru` on every request
  of a trace.

## What it does not do

No expiry, no weights or sizes in bytes, no loading functions, no statistics. If you need
those, use [otter](https://github.com/maypok86/otter).

## License

[MIT](LICENSE)
