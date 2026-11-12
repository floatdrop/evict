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

### Batches

`sieve`, `s3fifo` and `tinylfu` also have `GetMany(keys, values, found)`, which does what
`Get` does for each key. For up to 32 keys at a time it loads the memory that the lookups
start with before it looks any key up, so the CPU fetches it in parallel.

| Time per key, `GetMany` against a `Get` loop | 1 CPU | 8 CPUs |
|:---|---:|---:|
| 1M `int` entries, 32 keys per call | −48 to −62% | −47 to −53% |
| 1M `int` entries, 8 keys per call | −36 to −49% | −27 to −34% |
| 1M entries with 23-byte string keys, 32 keys per call | −66 to −68% | −56 to −58% |
| Zipf read-through with room for 1M, 32 keys per call | −49 to −55% | −48 to −51% |
| 16K `int` entries, 32 keys per call | −2 to +3% | +1 to +9% |
| 1M `int` entries, 1 key per call | +40 to +86% | +32 to +80% |

- Ranges over the three caches of medians of three runs of `BenchmarkGetMany` in each
  package, on an Apple M3 Max, keys picked at random. The times themselves vary more
  between runs than these ratios.
- For a single key, use `Get`.
- `stats.Wrap` passes `GetMany` on to the cache it wraps, and counts a hit or a miss per key.

### Statistics

[`stats.Wrap`](https://pkg.go.dev/github.com/floatdrop/evict/stats) counts the hits, misses
and evictions of any of them:

```go
cache := stats.Wrap(tinylfu.New[string, int](10_000))

s := cache.Stats()
fmt.Println(s.HitRatio(), s.Evictions, s.Len, s.Cap)
```

The counts only go up; `s.Sub(prev)` gives those of an interval. A `Get` that hits took
10–14% longer through the wrapper (`tinylfu`, one and eight CPUs, Apple M3 Max), and the
counters take 2–4 KiB per CPU.

## Choosing a policy

| | Use it when | Watch out for |
|:---|:---|:---|
| `tinylfu` | **in doubt**: the workload is unknown, or has loops or scans, and is read far more than written | Evicting writes are 8–13% slower than in `s3fifo`. Hit ratios vary between runs by up to 5.6 points. |
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

Use [otter](https://github.com/maypok86/otter) instead if you need expiry, weights or loading,
or if many CPUs overwrite a tenth to a quarter of what they read.

## Benchmarks

[`bench`](bench) is a separate module that compares these caches with otter v2 and hashicorp's
`golang-lru`. [bench/README.md](bench/README.md) explains the method, `make bench` reruns it,
and [bench/results](bench/results) has the full reports.

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

These caches ran in one piece. Default sharding moves most numbers by a tenth of a point or
less; it helps `sieve` a lot on its bad traces and costs `tinylfu` up to 2.6 points
([sharding.md](bench/results/sharding.md)).

### Throughput

Millions of operations per second on one CPU and on eight (`1 → 8`), against a cache of
16,384 string entries unless noted. Medians of five runs on an Apple M3 Max with Go 1.27.3;
[throughput.md](bench/results/throughput.md) has two and four CPUs and the spread.

| Benchmark | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu | otter |
|:---|---:|---:|---:|---:|---:|---:|
| `Get`: reads, all of them hits | 40.2 → 34.7 | 47.3 → 44.9 | 40.1 → 317.8 | 40.7 → 308.7 | 38.9 → 307.0 | 26.1 → 204.7 |
| `Mixed`: the same, 10% of them writes | 40.3 → 32.4 | 46.4 → 38.9 | 35.7 → 71.6 | 36.6 → 64.2 | 35.3 → 61.4 | 19.7 → 80.3 |
| `Set`: writes that replace | 38.1 → 24.1 | 44.0 → 26.9 | 22.0 → 11.8 | 22.4 → 12.6 | 22.5 → 11.6 | 8.7 → 15.3 |
| `Evict`: writes that evict | 17.8 → 31.6 | 16.9 → 30.3 | 14.4 → 12.0 | 9.6 → 10.3 | 8.4 → 9.5 | 3.4 → 5.6 |
| `ReadThrough`: read, store on a miss | 31.3 → 35.3 | 30.6 → 35.9 | 31.3 → 55.1 | 23.8 → 42.2 | 22.7 → 46.0 | 10.9 → 22.8 |
| `ReadThrough`, a million entries | 7.2 → 28.6 | 7.6 → 27.3 | 7.3 → 41.1 | 6.8 → 35.5 | 6.8 → 37.3 | 4.0 → 17.2 |

![Throughput of ReadThrough with a million entries by number of CPUs](bench/results/throughput-readthrough-capacity-1m.svg)

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
- These are numbers from one machine. Otter's own, from Linux on x86, show its writes
  scaling five-fold from one CPU to eight, against less than two-fold here.

### Memory

Bytes per entry with `uint64` keys and values, over caches of 10,000 to 10 million entries;
[memory.md](bench/results/memory.md) has every size, and strings.

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

## How it works

- **Arrays, not pointers.** Each shard allocates its nodes once and links them by 32-bit
  index. An open-addressing index with three slots per entry maps a 32-bit hash to a node,
  so a lookup compares hashes before it touches an entry.
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

No expiry, no weights or sizes in bytes, no loading functions. If you need those, use
[otter](https://github.com/maypok86/otter).

## License

[MIT](LICENSE)
