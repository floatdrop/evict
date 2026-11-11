// Package bench measures the throughput of the caches of this module next to
// that of otter. For hit ratios see cmd/hitratio, and for memory cmd/memory.
package bench

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/evict/bench/caches"
)

// The benchmarks run on every implementation, including the ones that are
// there for perspective: a cache behind a single lock shows what the others
// are up against.
var kinds = caches.All[string, string]()

const (
	// small is the capacity of the caches that fit into the CPU's own
	// caches, and large that of the ones that do not.
	small = 1 << 14
	large = 1 << 20
)

// A workload is a set of keys and a way of picking one of them for the next
// request. Every request is picked on its own, whatever came before it: a
// workload has no end and never repeats itself, and it is the same workload
// however long a benchmark runs and however many goroutines take part.
type workload struct {
	capacity int

	// names holds the keys one after the other, each of them width bytes
	// long. A key is a piece of it: getting at one takes no more than
	// looking at its bytes does, which a cache has to do anyway.
	names string
	keys  int
	// filler holds twice as many keys as the cache has room for, none of
	// which is ever requested, for the workloads whose keys do not fill
	// the cache. See fill.
	filler string

	// table makes the popularity of the keys follow a Zipf distribution, as
	// that of real keys tends to: a few of them get most of the requests.
	// Without a table every key is as likely as any other.
	table []alias
}

// alias is a row of the table that Walker's alias method picks from: the row
// is chosen at random, and then it is either the key with the number of the
// row or the other key that the row names, depending on a second random
// number. Picking a key takes one look at the table that way, however many
// keys there are.
type alias struct {
	// threshold is what the second number, of 32 bits, has to stay below
	// for the key of the row itself to be picked.
	threshold uint32
	other     uint32
}

// width is the length of a key: that of "user:profile:1000000000".
const width = 23

// newWorkload returns a workload for a cache of the given capacity with the
// given number of keys. If skew is not zero, key k is requested with a
// probability proportional to (1 + k) ** -skew.
func newWorkload(capacity, keys int, skew float64) *workload {
	// Keys of a length that is common for keys: longer than a number,
	// shorter than a URL.
	write := func(prefix string, n int) string {
		var b strings.Builder
		b.Grow(n * width)
		for i := range n {
			b.WriteString(prefix)
			b.WriteString(strconv.Itoa(1_000_000_000 + i))
		}
		return b.String()
	}

	w := &workload{capacity: capacity, names: write("user:profile:", keys), keys: keys}
	if keys < capacity {
		w.filler = write("user:session:", 2*capacity)
	}
	if skew != 0 {
		weights := make([]float64, keys)
		for k := range weights {
			weights[k] = math.Pow(float64(1+k), -skew)
		}
		w.table = aliases(weights)
	}
	return w
}

// aliases builds the table for picking a number with a probability that is
// proportional to its weight.
func aliases(weights []float64) []alias {
	n := len(weights)
	sum := 0.0
	for _, weight := range weights {
		sum += weight
	}

	// A row has room for a probability of 1/n. The numbers that are less
	// likely than that leave room in their rows, which the ones that are
	// more likely fill up, one row at a time.
	share := make([]float64, n)
	var under, over []uint32
	for i, weight := range weights {
		share[i] = weight / sum * float64(n)
		if share[i] < 1 {
			under = append(under, uint32(i))
		} else {
			over = append(over, uint32(i))
		}
	}

	table := make([]alias, n)
	for i := range table {
		table[i] = alias{threshold: math.MaxUint32, other: uint32(i)}
	}
	for len(under) > 0 && len(over) > 0 {
		u, o := under[len(under)-1], over[len(over)-1]
		under = under[:len(under)-1]

		table[u] = alias{threshold: uint32(share[u] * (1 << 32)), other: o}
		share[o] -= 1 - share[u]
		if share[o] < 1 {
			over = over[:len(over)-1]
			under = append(under, o)
		}
	}
	return table
}

// name returns the key with the given number.
func (w *workload) name(key uint32) string {
	at := int(key) * width
	return w.names[at : at+width]
}

// A source hands out the requests of a workload to one goroutine.
//
// It picks them a few hundred at a time. Picking a key means looking at a
// table that does not fit into the CPU's caches, and in a row of such looks
// the CPU does not have to wait for one before it starts the next.
type source struct {
	w     *workload
	state uint64
	next  int
	// buf holds requests that have been picked: the number of a key in the
	// lower half, and random bits for whoever has a use for them in the
	// upper.
	buf [256]uint64
}

// sources counts the sources that have been made, so that each of them picks
// differently.
var sources atomic.Uint64

func (w *workload) source() *source {
	// The random numbers of all sources are one long row of numbers, and the
	// state of a source is its place in the row. Two sources that start next
	// to each other there hand out the same requests, one a step behind the
	// other. So the place where a source starts is a random number itself,
	// made from the number of the source: far from where the others start.
	seed := source{state: sources.Add(1)}
	s := &source{w: w, state: seed.random()}
	s.next = len(s.buf)
	return s
}

// stride is how far the state of a source moves with every random number.
const stride = 0x9e3779b97f4a7c15

// random returns 64 random bits (SplitMix64).
func (s *source) random() uint64 {
	s.state += stride
	z := s.state
	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
	z = (z ^ z>>27) * 0x94d049bb133111eb
	return z ^ z>>31
}

// request returns the number of the key of the next request, and 32 random
// bits to go with it.
func (s *source) request() (key, bits uint32) {
	if s.next == len(s.buf) {
		s.pick()
	}
	r := s.buf[s.next]
	s.next++
	return uint32(r), uint32(r >> 32)
}

func (s *source) pick() {
	keys, table := uint64(s.w.keys), s.w.table
	for i := range s.buf {
		r := s.random()
		// The upper half of r, scaled to the number of keys, is a row.
		key := uint32(r >> 32 * keys >> 32)
		if table != nil {
			if row := table[key]; r&math.MaxUint32 >= uint64(row.threshold) {
				key = row.other
			}
		}
		s.buf[i] = s.random()&^math.MaxUint32 | uint64(key)
	}
	s.next = 0
}

// Workloads take a while to make up and are the same for every benchmark
// that uses them.
var (
	// Half as many keys as the cache has room for: every one of them fits,
	// whichever shard it falls into. Reads hit, writes replace.
	fitting = sync.OnceValue(func() *workload { return newWorkload(small, small/2, 1.01) })
	// Four times as many keys as the cache has room for: writes evict.
	crowded = sync.OnceValue(func() *workload { return newWorkload(small, 4*small, 1.01) })
	// The same, with a cache that is too large for the CPU's caches.
	crowdedLarge = sync.OnceValue(func() *workload { return newWorkload(large, 4*large, 1.01) })
	// Four times as many keys as the cache has room for, none more popular
	// than another: three requests in four are for a key that is not cached.
	churning = sync.OnceValue(func() *workload { return newWorkload(small, 4*small, 0) })
)

// run benchmarks op on a cache of every kind. The cache is created with the
// number of CPUs that the benchmark runs on, as it would be in a program, and
// it has seen plenty of the workload before the clock starts; see fill. If
// counted is set, op says whether it found its key, and the share of the
// requests that did is reported as well.
func run(b *testing.B, w *workload, counted bool, op func(c caches.Cache[string, string], key string, bits uint32) (hit bool)) {
	b.Helper()

	for _, kind := range kinds {
		// The testing package calls the function below several times, with
		// more iterations each time, and again for every number of CPUs.
		// The cache is set up on the first call for a number of CPUs only:
		// filling a large cache takes longer than measuring it.
		var (
			c     caches.Cache[string, string]
			procs int
		)
		b.Run("cache="+kind.Name, func(b *testing.B) {
			switch {
			case b.N == 1:
				// The testing package is only finding out whether the
				// benchmark runs at all, with whatever number of CPUs the
				// last one left behind. Any cache will do for that.
				c, procs = kind.New(w.capacity), 0
			case c == nil || procs != runtime.GOMAXPROCS(0):
				c = nil // Let go of the old one before making the new one.
				c, procs = fill(kind, w), runtime.GOMAXPROCS(0)
			}

			var hits atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			start := time.Now()
			b.RunParallel(func(pb *testing.PB) {
				src, h := w.source(), 0
				for pb.Next() {
					key, bits := src.request()
					if op(c, w.name(key), bits) {
						h++
					}
				}
				hits.Add(int64(h))
			})
			b.ReportMetric(float64(b.N)/time.Since(start).Seconds()/1e6, "Mops/s")
			if counted {
				b.ReportMetric(100*float64(hits.Load())/float64(b.N), "hit%")
			}
		})
	}
}

// fill returns a cache of the given kind that has been in use for a while.
//
// A cache that has never been full is a good deal faster with some policies
// than it will ever be again: a policy that sorts entries by what became of
// them has sorted nothing yet. So a cache that the keys of the workload do
// not fill is first filled with other keys, twice over. Then it gets sixteen
// requests of the workload for every entry it has room for.
//
// That is enough for the cache to be full of the workload, and not enough for
// a policy to have learned all there is to learn about it: a key that the
// cache is undecided about has come up once or twice by then. A cache that
// tells popular keys from the others goes on to answer a little more, for
// hundreds of requests per entry. A benchmark of a small cache gets through
// that many in a fraction of its time and measures what comes after. One of
// a cache with a million entries does not: its hit ratio is a few tenths of
// a percentage point higher after three times as many requests, and so
// depends a little on how many requests the benchmark got through.
func fill(kind caches.Kind[string, string], w *workload) caches.Cache[string, string] {
	c := kind.New(w.capacity)
	settle := func() {
		// A cache that leaves work for later gets to finish it.
		if s, ok := c.(caches.Settler); ok {
			s.Settle()
		}
	}

	for at := 0; at < len(w.filler); at += width {
		key := w.filler[at : at+width]
		c.Set(key, key)
	}
	src := w.source()
	for range 16 * w.capacity {
		key, _ := src.request()
		c.Set(w.name(key), w.name(key))
	}
	settle()

	// Where every key of the workload fits, every key is to be found: what
	// the cache dropped while it was full is put back until nothing is
	// missing.
	for missing := w.filler != ""; missing; {
		missing = false
		for key := range w.keys {
			name := w.name(uint32(key))
			if _, ok := c.Get(name); !ok {
				c.Set(name, name)
				missing = true
			}
		}
		settle()
	}

	runtime.GC() // None pays for the garbage of another.
	return c
}

// BenchmarkGet reads keys that are all cached, which the hit ratio it reports
// confirms. Run it with -cpu 1,2,4,8 to see how a cache copes with more
// goroutines reading at once.
func BenchmarkGet(b *testing.B) {
	run(b, fitting(), true, func(c caches.Cache[string, string], key string, _ uint32) bool {
		_, ok := c.Get(key)
		return ok
	})
}

// BenchmarkSet replaces the values of keys that are all cached.
func BenchmarkSet(b *testing.B) {
	run(b, fitting(), false, func(c caches.Cache[string, string], key string, _ uint32) bool {
		c.Set(key, key)
		return false
	})
}

// BenchmarkMixed reads keys that are all cached and, a given share of the
// time, replaces a value instead.
func BenchmarkMixed(b *testing.B) {
	for _, writes := range []uint32{1, 10, 25} {
		b.Run(fmt.Sprintf("writes=%d%%", writes), func(b *testing.B) {
			run(b, fitting(), false, func(c caches.Cache[string, string], key string, bits uint32) bool {
				if bits%100 < writes {
					c.Set(key, key)
				} else {
					c.Get(key)
				}
				return false
			})
		})
	}
}

// BenchmarkEvict writes keys of which the cache has room for a quarter only,
// all equally likely, so that three writes in four add an entry and evict
// another.
func BenchmarkEvict(b *testing.B) {
	run(b, churning(), false, func(c caches.Cache[string, string], key string, _ uint32) bool {
		c.Set(key, key)
		return false
	})
}

// BenchmarkReadThrough is what a cache in front of a slow lookup sees: every
// key is read and, if it is missing, stored. The keys come from a set four
// times the size of the cache, so storing a key evicts another. How often
// that happens is up to the eviction policy, which is why the hit ratio is
// reported as well.
func BenchmarkReadThrough(b *testing.B) {
	for _, tt := range []struct {
		name string
		w    func() *workload
	}{{"capacity=16K", crowded}, {"capacity=1M", crowdedLarge}} {
		b.Run(tt.name, func(b *testing.B) {
			run(b, tt.w(), true, func(c caches.Cache[string, string], key string, _ uint32) bool {
				if _, ok := c.Get(key); ok {
					return true
				}
				c.Set(key, key)
				return false
			})
		})
	}
}
