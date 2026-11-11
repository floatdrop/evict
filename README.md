# evict

[![Go Reference](https://pkg.go.dev/badge/github.com/floatdrop/evict.svg)](https://pkg.go.dev/github.com/floatdrop/evict)
[![CI](https://github.com/floatdrop/evict/actions/workflows/ci.yml/badge.svg)](https://github.com/floatdrop/evict/actions/workflows/ci.yml)

Fixed-capacity in-memory caches for Go: one API, four eviction policies.

| Package | Evicts | Reads |
|:---|:---|:---|
| [`lru`](https://pkg.go.dev/github.com/floatdrop/evict/lru) | the least recently used entry | take the lock of a shard |
| [`twoq`](https://pkg.go.dev/github.com/floatdrop/evict/twoq) | with [2Q](http://www.vldb.org/conf/1994/P439.PDF), which keeps new entries apart from proven ones | take the lock of a shard |
| [`sieve`](https://pkg.go.dev/github.com/floatdrop/evict/sieve) | with [SIEVE](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo), a queue with lazy promotion | take no lock |
| [`s3fifo`](https://pkg.go.dev/github.com/floatdrop/evict/s3fifo) | with [S3-FIFO](https://dl.acm.org/doi/10.1145/3600006.3613147), which makes new entries prove themselves in a small queue first | take no lock |

All of them

- **allocate nothing** once they are created: entries live in arrays that are set up front and
  are addressed by 32-bit indices;
- **leave the garbage collector alone**: a cache of a million integers is a handful of
  pointer-free arrays, which the collector does not look into;
- **tell you what they dropped**: `Set` returns the entry it replaced or evicted, so that
  whatever the entry held on to can be released;
- **are safe for concurrent use**: entries are spread over shards that are locked one by one,
  and `sieve` and `s3fifo` read without a lock at all;
- depend on nothing but the standard library.

```
go get github.com/floatdrop/evict
```

Requires Go 1.26 or later.

## Usage

```go
package main

import (
	"fmt"

	"github.com/floatdrop/evict/s3fifo"
)

func main() {
	cache := s3fifo.New[string, int](10_000)

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

The four packages have the same methods, which are also the interface
[`evict.Cache`](https://pkg.go.dev/github.com/floatdrop/evict#Cache):

| Method | |
|:---|:---|
| `Get(key) (value, ok)` | Looks an entry up and tells the eviction policy that it was used. |
| `Peek(key) (value, ok)` | Looks an entry up without telling the policy. |
| `Set(key, value) (oldKey, oldValue, displaced)` | Stores an entry and returns the one that left the cache as a result. |
| `Delete(key) (value, ok)` | Removes an entry. |
| `Len()`, `Cap()` | The number of entries, and the number there is room for. |
| `Clear()` | Removes every entry. |
| `All()` | Iterates over the entries. |

`New` takes options. All four packages have `WithShards`; `twoq` also has `WithRecentRatio`
and `WithGhostRatio` for the sizes of its queues.

## Choosing a policy

**If in doubt, use `s3fifo`.** Of the four it answers the most requests on nine of the eleven
traces that the benchmarks replay, its reads take no lock, and it never does much worse
than `lru`: about two points at the most, where `sieve` can lose everything. It has a weak spot
of its own, though. On the trace called Mixed, at some capacities, it answers what `lru`
answers while `twoq` and otter answer four to six times as much.

| | Use it when | Mind |
|:---|:---|:---|
| `s3fifo` | you do not know the workload, or it is read far more than it is written | Writes cost about twice what they do in `lru`, and at worst three times. A cache of fewer than ten entries per shard is too small for it. |
| `sieve` | several CPUs read through the cache and it has to be as fast as can be, in less memory than `s3fifo` takes, and you have measured that your workload is one it copes with | It can fail outright. On one of the traces a SIEVE cache in one piece answers less than 1% of the requests where LRU answers 44%; see [when not to use it](https://pkg.go.dev/github.com/floatdrop/evict/sieve#hdr-When_not_to_use_it). On the traces here its hit ratio is ahead of that of `s3fifo` on one only. |
| `twoq` | a quarter or more of the operations are writes, or one goroutine at a time uses the cache | It takes more memory than `lru` and `sieve`, because it keeps a node for every key it remembers. Reads take a lock. |
| `lru` | memory is what counts, there are a great many small caches, or you want the behavior that everyone knows | The lowest hit ratio of the four on nine traces of eleven. Reads take a lock. |

And when to use [otter](https://github.com/maypok86/otter) instead: when you need what
these caches do not have (expiry, weights, loading, statistics); when the worst case matters
more than the usual one, since otter is never more than six points behind the best cache
on these traces and each of the four here is thirty or more behind somewhere; or when eight
CPUs read a cache while a tenth to a quarter of their operations overwrite entries. (For
writes alone, `twoq` and `lru` stay ahead.) The numbers are below.

## Benchmarks

The benchmarks are in [`bench`](bench), a module of its own, and compare the four caches
with otter v2, the state of the art among Go caches, and with hashicorp's `golang-lru`, a
cache behind a single lock. [bench/README.md](bench/README.md) says what is measured and
how; `make bench` runs it all again. What follows is a summary of
[bench/results](bench/results).

### Hit ratio

The share of requests, in percent, that a cache answers when a trace is replayed against
it, averaged over the eight capacities at which each trace is replayed. Ten of the traces
are public recordings of real systems, checked against a SHA-256 before they are used;
`optimal` is the bound that no cache can beat. Otter's hit ratio differs from one replay to
the next, by up to 3.8 points, so it is replayed eleven times: its number is the median,
and a number is in bold only if the cache leads whichever of those replays it is compared
with.

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

- **`s3fifo` and otter share the wins.** Of the 88 combinations of trace and capacity,
  `s3fifo` wins 37 outright and otter 31. Six go to one of the 2Q caches, and 14 have no
  clear winner.
- **`s3fifo` is ahead** on Zipf and on the traces of a recommendation service, a key-value
  cache and block storage (Scarab, Twitter, MSR, CloudPhysics), by 0.4 to 2.6 points on
  average and by no more than 4.8 at any capacity.
- **Otter is ahead by more where it is ahead**: on two of the disk traces of the ARC paper
  and on the two traces with loops in them (S3, DS1, Glimpse, Mixed), by 2 to 18 points
  over `s3fifo` on average, and by 61 at the capacity where `s3fifo` does worst. Against the
  best of the four on each of those traces, which is `sieve` on DS1 and `twoq` on Mixed, it
  is 2 to 8 points. On OLTP and P8 the two are within a point of each other.
- **`sieve` is close behind `s3fifo` where it works**: about a point on Zipf, Scarab,
  Twitter and MSR, about three on S3 and CloudPhysics, and ahead of it on DS1. Where it
  does not work it is far behind: 8 to 13 points on OLTP, P8 and Glimpse, and on Mixed it
  answers next to nothing.
- **`twoq` is ahead of `lru` on the average of every trace**, though not at every capacity:
  `lru` leads in 10 of the 88 combinations. It is the best of the four on Mixed.
- **Otter has no bad trace.** In none of the 88 combinations is it more than 5.9 points
  behind the best cache. Each of the four here is somewhere: `twoq` by 32 points, `s3fifo`
  and `lru` by 63, `sieve` by 76.
- Averages hide a lot. [hitratio.md](bench/results/hitratio.md) has every capacity, with a
  chart for each trace.

These numbers are for caches in one piece, so that the policy is all that is compared. A
program gets shards by default, and [sharding.md](bench/results/sharding.md) has what that
changes to the median of eleven runs: less than a tenth of a point for `lru`, about a
tenth at the most for `s3fifo`, the same for `twoq` but for two cases of half a point and
of a point. For `sieve` it changes a great deal on two of the traces it does badly on, for
the better: 3 to 4 points on OLTP and anything from 2 to 41 on Mixed, depending on the
capacity. On P8 it is a point at the most, and on Glimpse nothing. It was measured for a machine with eight CPUs; one with more gives a cache of
some size more and smaller shards.

### Throughput

Millions of operations per second with one CPU and with eight (`1 → 8`), each CPU running
a goroutine against the same cache of 16,384 string entries unless it says otherwise. The
numbers are medians of five runs on an Apple M3 Max with Go 1.27.3;
[throughput.md](bench/results/throughput.md) has two and four CPUs as well, the range of
the runs, and which differences are beyond noise.

| Benchmark | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | otter | golang-lru |
|:---|---:|---:|---:|---:|---:|---:|
| `Get`: reads, all of them hits | 37.0 → 36.4 | 48.3 → 47.3 | 35.0 → 278.0 | 36.2 → 276.6 | 26.6 → 205.3 | 33.6 → 4.1 |
| `Mixed`: the same, 10% of them writes | 37.3 → 32.2 | 44.5 → 41.5 | 33.0 → 67.7 | 33.0 → 67.8 | 21.4 → 81.9 | 33.6 → 4.2 |
| `Set`: writes that replace | 35.9 → 25.3 | 43.4 → 29.3 | 21.9 → 12.9 | 21.7 → 13.1 | 8.6 → 15.7 | 31.7 → 4.2 |
| `Evict`: writes that evict | 16.5 → 31.4 | 16.8 → 31.4 | 13.3 → 12.0 | 8.5 → 10.5 | 3.5 → 5.8 | 7.4 → 3.2 |
| `ReadThrough`: read, store on a miss | 30.1 → 34.5 | 31.6 → 34.8 | 28.5 → 52.1 | 21.9 → 43.2 | 12.4 → 23.6 | 16.5 → 3.5 |
| `ReadThrough`, a million entries | 8.9 → 29.4 | 7.8 → 28.6 | 8.4 → 41.6 | 7.3 → 37.0 | 4.1 → 17.7 | 6.2 → 2.8 |

![Throughput of ReadThrough with a million entries by number of CPUs](bench/results/throughput-readthrough-capacity-1m.svg)

- **Reads scale in `sieve` and `s3fifo`**, as they do in otter: with eight CPUs both read
  about a third faster than otter does. `lru` and `twoq` read under a lock, and with eight
  CPUs they are slower than with four.
- **With one CPU all four are faster than otter** at everything measured, by a factor of
  1.3 to 5.
- **Reading through is what `sieve` and `s3fifo` are best at.** `ReadThrough` is the
  benchmark that comes closest to a cache in front of a database. From two CPUs on `sieve`
  is the fastest in it, and from four on `s3fifo` is the second; with one CPU it is `twoq`
  or `lru`. With eight, `sieve` and `s3fifo` do about twice what otter does.
- **Writes are where `lru` and `twoq` are at home.** They change an entry in place. `sieve`
  and `s3fifo` move a replaced entry to a fresh node so that readers need no lock. A write
  that replaces costs them 1.6 to 2.5 times as much, and up to three times with four CPUs;
  one that evicts costs 1.2 to 3 times as much. Their replacing writes are fastest with one
  CPU, and every cache here writes slower with eight CPUs than with four.
- **Otter wins when eight CPUs keep overwriting the entries that they read**: by a fifth
  over `sieve` and `s3fifo` with 10% writes, and by nearly a quarter over `twoq` with 25%.
  With four CPUs the fastest of the caches here are still well ahead, though `lru` is
  behind otter already with 10% writes.
- **One machine is one machine.** In the numbers that otter publishes, from Linux on x86,
  its writes get five times as fast between one CPU and eight; here they do not get twice
  as fast. How the caches compare on such a machine may well differ, and the `Benchmarks`
  workflow of this repository measures on GitHub's Linux runners for a second opinion.
  A benchmark that does nothing but call a cache also overstates every difference that a
  program would see.

### Memory and the garbage collector

What a cache takes for every entry it has room for, with `uint64` keys and values, which
are 16 of the bytes themselves. The ranges are over caches of ten thousand to ten million
entries. [memory.md](bench/results/memory.md) has every size, and strings.

| | Bytes per entry | of which the collector has to scan | CPU time of one collection, a million entries | ten million |
|:---|---:|---:|---:|---:|
| evict/lru | 46–58 | 0–1 | 0.2 ms | 0.2 ms |
| evict/sieve | 54–71 | 0–2 | 0.3 ms | 0.2 ms |
| evict/twoq | 74–82 | 0–1 | 0.3 ms | 0.2 ms |
| evict/s3fifo | 68–90 | 0–2 | 0.3 ms | 0.2 ms |
| otter | 84–95 | 41–50 | 64 ms | 631 ms |
| golang-lru | 110–127 | 94–111 | 65 ms | 736 ms |
| golang-lru/2q | 156–183 | 136–163 | 99 ms | 1,229 ms |

The bytes per entry go up and down with the capacity, because the index of a cache has a
power of two of slots, and the ranges overlap: at 100,000 entries `s3fifo` takes 90 bytes
and otter 84. What does not overlap is the work for the garbage collector. A cache that is
made of pointers gives it more to do the larger it is, and a cache of this module gives it
next to nothing at any size. That is for keys and values that hold no pointers. If they do,
as strings do, the collector has to go through the entries to follow them, which was not
measured here.

## How it works

**Entries live in arrays.** A shard allocates its nodes once, as one array, and links them into
queues by 32-bit index instead of by pointer. Keys are found through an open-addressing index
of 64-bit words, each a 32-bit hash and a node index, so a lookup compares a hash before it
touches a node. Nothing is allocated after `New`, and for keys and values without pointers
the garbage collector has nothing to look at.

**Shards.** A cache is split into shards by the upper half of a key's hash; the lower half is
what the shard indexes the key under. Writers lock one shard. Each shard has its share of the
capacity and evicts on its own, so the capacity of a cache with several shards is an upper
bound that it rarely reaches exactly. By default a cache of fewer than 512 entries is one
shard, and a larger one has up to eight per CPU, of 256 entries or more. `WithShards(1)`
gives the textbook behavior of a policy.

**Reads without a lock** (`sieve`, `s3fifo`). The index is made of atomic words. A reader
that finds its key says which node it is about to read by writing the node's number into a
slot of its own, checks that the index still points there, and reads. A writer never changes
a node that readers can reach: replacing a value moves the entry to a fresh node, and a node
that has left the index is reused only after a look at the slots shows that nobody has it.
A read thus costs two writes to a cache line that the reading CPU has to itself, and nothing
that all CPUs share. Both policies were chosen for this: reading an entry sets a mark or
counts to three, and writes nothing once that is done.

Two consequences are worth knowing:

- An entry that was removed or replaced stays referenced for a short while, until its shard
  has collected enough of them to make the look at the slots worthwhile: an eighth of the
  shard's capacity, and never more than 1024 entries. `Clear` lets go of everything before
  it returns.
- A cache has a table of those slots, of 2 to 4 KiB per CPU. For a great many small caches,
  use `lru`.

**Tests.** Every cache passes the same conformance suite, is compared operation by
operation against a simple model of its policy, and is fuzzed against that model. The
lock-free path is tested for values that tear, keys that go missing while they are present,
and readers that are held up, under the race detector. `evict/lru` must agree with
hashicorp's `golang-lru` on every request of a trace.

## What it does not do

The caches hold a fixed number of entries, and that is all they do. There are no expiry
times, no weights or sizes in bytes, no loading functions and no statistics. If you need
those, [otter](https://github.com/maypok86/otter) has them.

## License

[MIT](LICENSE)
