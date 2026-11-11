package bench

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

// frequencies draws n requests from a source of the workload and returns how
// often each key came up, as a share of n.
func frequencies(t *testing.T, w *workload, n int) []float64 {
	t.Helper()

	counts := make([]float64, w.keys)
	src := w.source()
	for range n {
		key, _ := src.request()
		if int(key) >= w.keys {
			t.Fatalf("request for key %d of %d", key, w.keys)
		}
		counts[key] += 1 / float64(n)
	}
	return counts
}

func TestAliases(t *testing.T) {
	t.Parallel()

	// Whatever the weights, a number comes up as often as its weight says.
	for _, weights := range [][]float64{
		{1},
		{1, 1},
		{1, 2, 3, 4},
		{100, 1, 1, 1, 1, 1},
		{0, 5, 0, 5},
		{1e-9, 1, 1e9},
	} {
		w := &workload{keys: len(weights), table: aliases(weights)}
		sum := 0.0
		for _, weight := range weights {
			sum += weight
		}
		for key, got := range frequencies(t, w, 1_000_000) {
			if want := weights[key] / sum; math.Abs(got-want) > 0.003 {
				t.Errorf("weights %v: number %d came up %.4f of the time, want %.4f", weights, key, got, want)
			}
		}
	}
}

func TestWorkload(t *testing.T) {
	t.Parallel()

	// Keys are all different, of one length, and where name says.
	w := newWorkload(64, 1000, 1.01)
	seen := map[string]bool{}
	for key := range w.keys {
		name := w.name(uint32(key))
		if len(name) != width || !strings.HasPrefix(name, "user:profile:") || seen[name] {
			t.Fatalf("key %d is %q", key, name)
		}
		if n, err := strconv.Atoi(strings.TrimPrefix(name, "user:profile:")); err != nil || n != 1_000_000_000+key {
			t.Fatalf("key %d is %q", key, name)
		}
		seen[name] = true
	}
	if w.filler != "" {
		t.Error("a workload with more keys than the cache has room for has filler")
	}

	// Their popularity is the Zipf distribution that was asked for.
	sum := 0.0
	for k := range w.keys {
		sum += math.Pow(float64(1+k), -1.01)
	}
	got := frequencies(t, w, 2_000_000)
	for _, key := range []int{0, 1, 2, 9, 99} {
		if want := math.Pow(float64(1+key), -1.01) / sum; math.Abs(got[key]-want) > 0.05*want+0.0002 {
			t.Errorf("key %d gets %.5f of the requests, want %.5f", key, got[key], want)
		}
	}
	tail, wantTail := 0.0, 0.0
	for key := 100; key < w.keys; key++ {
		tail += got[key]
		wantTail += math.Pow(float64(1+key), -1.01) / sum
	}
	if math.Abs(tail-wantTail) > 0.005 {
		t.Errorf("the keys from 100 on get %.4f of the requests, want %.4f", tail, wantTail)
	}

	// Without skew, no key is more popular than another.
	flat := newWorkload(64, 256, 0)
	for key, share := range frequencies(t, flat, 2_000_000) {
		if math.Abs(share-1.0/256) > 0.0005 {
			t.Errorf("key %d of the flat workload gets %.5f of the requests, want %.5f", key, share, 1.0/256)
		}
	}

	// Two sources do not hand out the same requests, and the bits that
	// come with a request are not always the same.
	a, b := w.source(), w.source()
	same, ones := 0, 0
	for range 1000 {
		ka, bits := a.request()
		kb, _ := b.request()
		if ka == kb {
			same++
		}
		ones += int(bits % 2)
	}
	if same > 300 || ones < 400 || ones > 600 {
		t.Errorf("two sources agree on %d requests of 1000, and %d of 1000 random bits are set", same, ones)
	}
}

// TestSourcesGoTheirOwnWays checks that no source hands out the requests of
// another one a little earlier or a little later. Goroutines that follow each
// other through the same requests find in the cache what the one ahead of
// them has just put there, and the more of them there are, the more hits a
// benchmark counts that no cache has earned.
func TestSourcesGoTheirOwnWays(t *testing.T) {
	t.Parallel()

	// A million keys, all equally likely: two requests are for the same key
	// by chance once in a million times.
	w := &workload{keys: 1 << 20}
	const (
		requests = 4096
		reach    = 64 // how far behind a source may be and still be found out
	)

	// As many sources as the benchmarks have goroutines, and then some, made
	// one after the other as the goroutines of a benchmark make them.
	streams := make([][]uint32, 16)
	starts := make([]uint64, len(streams))
	for i := range streams {
		src := w.source()
		starts[i] = src.state
		streams[i] = make([]uint32, requests)
		for at := range streams[i] {
			streams[i][at], _ = src.request()
		}
	}

	// A source that is further behind than that is not found out by its
	// requests, but by where it started: the random numbers of all sources
	// are one row of numbers, in which a source moves on by one stride at a
	// time. The number of strides from one start to another is their
	// difference divided by the stride, which among numbers of 64 bits is a
	// multiplication with the inverse of the stride. No benchmark gets
	// through as many requests as there must be strides between two starts.
	inverse := uint64(stride) // Newton's method doubles the bits that are right.
	for range 6 {
		inverse *= 2 - stride*inverse
	}
	if stride*inverse != 1 {
		t.Fatalf("%#x is not the inverse of the stride", inverse)
	}
	for i, a := range starts {
		for j, b := range starts[:i] {
			strides := (a - b) * inverse
			if apart := min(strides, -strides); apart < 1<<40 {
				t.Errorf("source %d starts %d random numbers from where source %d does", i, apart, j)
			}
		}
	}

	for i, ahead := range streams {
		for j, behind := range streams {
			if i == j {
				continue
			}
			for lag := range reach {
				same := 0
				for at := range requests - lag {
					if ahead[at+lag] == behind[at] {
						same++
					}
				}
				if same > requests/100 {
					t.Fatalf("source %d hands out %d of the %d requests of source %d, %d requests later",
						j, same, requests-lag, i, lag)
				}
			}
		}
	}
}

// TestFill checks the state in which a benchmark finds its cache.
func TestFill(t *testing.T) {
	t.Parallel()

	// A workload whose keys fit leaves every one of them in the cache, and
	// the cache has been full on the way there.
	w := newWorkload(1024, 512, 1.01)
	if len(w.filler) != 2*1024*width {
		t.Fatalf("the filler has %d bytes, want %d keys", len(w.filler), 2*1024)
	}
	for _, kind := range kinds {
		c := fill(kind, w)
		for key := range w.keys {
			if _, ok := c.Get(w.name(uint32(key))); !ok {
				t.Errorf("%s: key %d is missing after fill", kind.Name, key)
				break
			}
		}
		// The rest of the room is taken by what the cache was filled
		// with before.
		filler := 0
		for at := 0; at < len(w.filler); at += width {
			if _, ok := c.Get(w.filler[at : at+width]); ok {
				filler++
			}
		}
		if room := w.capacity - w.keys; filler < room/2 || filler > room {
			t.Errorf("%s: %d keys of the filler are cached after fill, want up to %d and not many fewer", kind.Name, filler, room)
		}
	}

	// One whose keys do not fit leaves the cache full of them.
	w = newWorkload(1024, 4096, 1.01)
	for _, kind := range kinds {
		c := fill(kind, w)
		cached := 0
		for key := range w.keys {
			if _, ok := c.Get(w.name(uint32(key))); ok {
				cached++
			}
		}
		// A cache in shards does not fill every one of them to the brim.
		if cached < w.capacity*9/10 || cached > w.capacity {
			t.Errorf("%s: %d keys are cached after fill, want about %d", kind.Name, cached, w.capacity)
		}
	}
}
