package replay_test

import (
	"math/rand/v2"
	"testing"

	"github.com/floatdrop/evict/bench/caches"
	"github.com/floatdrop/evict/bench/replay"
	"github.com/floatdrop/evict/bench/trace"
)

func TestHits(t *testing.T) {
	t.Parallel()

	keys := []uint64{1, 2, 1, 3, 1, 2, 4, 1}
	lru, _ := caches.Find[uint64, uint64]("evict/lru")

	// With room for two entries, an LRU cache answers the second and the
	// third request for 1; by the time 2 and the last 1 come around again,
	// they have been evicted.
	if got := replay.Hits(lru.Exact(2), keys); got != 2 {
		t.Errorf("Hits with capacity 2 = %d, want 2", got)
	}
	if got := replay.Hits(lru.Exact(4), keys); got != 4 {
		t.Errorf("Hits with capacity 4 = %d, want every repeated request, 4", got)
	}
	if got := replay.Hits(lru.Exact(0), keys); got != 0 {
		t.Errorf("Hits with capacity 0 = %d, want 0", got)
	}
}

func TestOptimal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keys     []uint64
		capacity int
		want     int
	}{
		{"no requests", nil, 3, 0},
		{"no capacity", []uint64{1, 1, 1}, 0, 0},
		{"one key", []uint64{1, 1, 1}, 1, 2},
		{"all different", []uint64{1, 2, 3, 4}, 2, 0},
		// With one place, keeping 1 over 2 pays off twice; LRU would get nothing.
		{"knows what is coming", []uint64{1, 2, 1, 3, 1}, 1, 2},
		// 1, 2 and 3 are stored. 4 and 5 are not, because each of them comes
		// back later than everything in the cache, which answers the two
		// further requests for 1 and for 2 and the one for 3.
		{"textbook", []uint64{1, 2, 3, 4, 1, 2, 5, 1, 2, 3, 4, 5}, 3, 5},
		{"everything fits", []uint64{1, 2, 3, 1, 2, 3, 3, 2, 1}, 3, 6},
	}
	for _, tt := range tests {
		if got := replay.Optimal(tt.keys, replay.Foresee(tt.keys), tt.capacity); got != tt.want {
			t.Errorf("%s: Optimal = %d, want %d", tt.name, got, tt.want)
		}
	}
}

// bruteForce finds the largest number of hits that any sequence of evictions
// achieves, by trying all of them. A key need not be stored.
func bruteForce(keys []uint64, capacity int, cache map[uint64]bool) int {
	if len(keys) == 0 {
		return 0
	}
	key, rest := keys[0], keys[1:]
	if cache[key] {
		return 1 + bruteForce(rest, capacity, cache)
	}

	best := bruteForce(rest, capacity, cache) // Leave the key out.
	if capacity == 0 {
		return best
	}
	cache[key] = true
	if len(cache) <= capacity {
		best = max(best, bruteForce(rest, capacity, cache))
	} else {
		for victim := range cache {
			if victim == key {
				continue
			}
			delete(cache, victim)
			best = max(best, bruteForce(rest, capacity, cache))
			cache[victim] = true
		}
	}
	delete(cache, key)
	return best
}

func TestOptimalIsOptimal(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(3, 4))
	for range 300 {
		keys := make([]uint64, 1+rng.IntN(11))
		for i := range keys {
			keys[i] = uint64(rng.IntN(5))
		}
		capacity := rng.IntN(4)

		got, want := replay.Optimal(keys, replay.Foresee(keys), capacity), bruteForce(keys, capacity, map[uint64]bool{})
		if got != want {
			t.Fatalf("Optimal(%v, %d) = %d, but %d hits are possible", keys, capacity, got, want)
		}
	}
}

// TestBound checks on a longer trace that no cache beats the optimum, and
// that more capacity never hurts it.
func TestBound(t *testing.T) {
	t.Parallel()

	keys := trace.Zipf(200_000, 1.0001, 10, 100_000)
	future := replay.Foresee(keys)
	previous := 0
	for _, capacity := range []int{10, 100, 1000, 10_000} {
		optimal := replay.Optimal(keys, future, capacity)
		if optimal < previous {
			t.Errorf("capacity %d: the optimum is %d hits, less than the %d of a smaller cache", capacity, optimal, previous)
		}
		previous = optimal

		for _, kind := range caches.All[uint64, uint64]() {
			if hits := replay.Hits(kind.Exact(capacity), keys); hits > optimal || hits == 0 {
				t.Errorf("capacity %d: %s has %d hits, the optimum is %d", capacity, kind.Name, hits, optimal)
			}
		}
	}
}

// TestSameAsReference checks the LRU cache of this module against another
// implementation of the same policy: the two must agree on every request.
func TestSameAsReference(t *testing.T) {
	t.Parallel()

	ours, _ := caches.Find[uint64, uint64]("evict/lru")
	theirs, _ := caches.Find[uint64, uint64]("golang-lru")

	keys := trace.Zipf(500_000, 1.0001, 10, 200_000)
	for _, capacity := range []int{1, 2, 10, 100, 1000, 10_000, 50_000} {
		a, b := ours.Exact(capacity), theirs.Exact(capacity)
		for i, key := range keys {
			_, hitA := a.Get(key)
			_, hitB := b.Get(key)
			if hitA != hitB {
				t.Fatalf("capacity %d, request %d for key %d: evict/lru says %t, golang-lru says %t",
					capacity, i, key, hitA, hitB)
			}
			if !hitA {
				a.Set(key, key)
				b.Set(key, key)
			}
		}
	}
}

// TestSameEveryTime checks that the caches of this module, in one piece,
// answer the same requests of a trace on every replay. They hash their keys
// with a seed that is new every time, and what they keep must not depend on
// it: not even by way of two keys that happen to share a hash, which among
// this many is bound to occur.
func TestSameEveryTime(t *testing.T) {
	t.Parallel()

	keys := trace.Zipf(1_000_000, 1.0001, 10, 50_000_000)
	const capacity = 100_000
	for _, name := range []string{"evict/lru", "evict/twoq", "evict/sieve", "evict/s3fifo"} {
		kind, ok := caches.Find[uint64, uint64](name)
		if !ok || kind.Varies {
			t.Fatalf("%s is not a cache that is the same every time", name)
		}
		first := replay.Hits(kind.Exact(capacity), keys)
		for range 2 {
			if again := replay.Hits(kind.Exact(capacity), keys); again != first {
				t.Errorf("%s answered %d requests on one replay and %d on another", name, first, again)
			}
		}
	}
}

func TestForesee(t *testing.T) {
	t.Parallel()

	const never = 1<<31 - 1
	got := replay.Foresee([]uint64{7, 8, 7, 7, 9, 8})
	want := replay.Future{2, 5, 3, never, never, never}
	if len(got) != len(want) {
		t.Fatalf("Foresee = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Foresee = %v, want %v", got, want)
			break
		}
	}
	if got := replay.Foresee(nil); len(got) != 0 {
		t.Errorf("Foresee of no requests = %v", got)
	}

	// A future goes with one trace only.
	defer func() {
		if recover() == nil {
			t.Error("Optimal took the future of another trace")
		}
	}()
	replay.Optimal([]uint64{1, 2, 3}, replay.Foresee([]uint64{1, 2}), 1)
}
