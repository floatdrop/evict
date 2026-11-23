package expire_test

import (
	"fmt"
	"time"

	"github.com/floatdrop/evict/expire"
	"github.com/floatdrop/evict/stats"
	"github.com/floatdrop/evict/tinylfu"
)

func Example() {
	// The cache holds expire.Entry values, which are the values with the
	// time at which they expire.
	cache := expire.Wrap(tinylfu.New[string, expire.Entry[int]](10_000), time.Minute)

	cache.Set("a", 1)                   // Expires in a minute,
	cache.SetWithTTL("b", 2, time.Hour) // this one in an hour,
	cache.SetWithTTL("c", 3, 0)         // and this one already has.

	for _, key := range []string{"a", "b", "c"} {
		value, ok := cache.Get(key)
		fmt.Println(key, value, ok)
	}

	// Output:
	// a 1 true
	// b 2 true
	// c 0 false
}

// Wrapped in stats, a cache counts a key whose entry has expired as a miss.
func Example_stats() {
	expiring := expire.Wrap(tinylfu.New[string, expire.Entry[int]](10_000), time.Minute)
	cache := stats.Wrap(expiring)

	cache.Set("a", 1)
	cache.Set("b", 2)
	expiring.SetWithTTL("b", 3, 0) // Its time is up.

	cache.Get("a")
	cache.Get("b")

	s := cache.Stats()
	fmt.Println(s.Hits, "hit,", s.Misses, "miss")

	// Output:
	// 1 hit, 1 miss
}
