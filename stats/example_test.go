package stats_test

import (
	"fmt"

	"github.com/floatdrop/evict/lru"
	"github.com/floatdrop/evict/stats"
)

func Example() {
	cache := stats.Wrap(lru.New[string, int](2))

	cache.Set("a", 1)
	cache.Set("b", 2)
	cache.Get("a")    // A hit.
	cache.Get("c")    // A miss,
	cache.Set("c", 3) // and "b" is evicted for "c".

	s := cache.Stats()
	fmt.Printf("%d hits, %d misses, hit ratio %.2f\n", s.Hits, s.Misses, s.HitRatio())
	fmt.Printf("%d evicted, %d of %d entries\n", s.Evictions, s.Len, s.Cap)

	// Rates are counts over an interval: subtract the snapshot taken at its
	// start.
	cache.Get("c")
	cache.Get("c")
	fmt.Printf("since then: %d hits\n", cache.Stats().Sub(s).Hits)

	// Output:
	// 1 hits, 1 misses, hit ratio 0.50
	// 1 evicted, 2 of 2 entries
	// since then: 2 hits
}
