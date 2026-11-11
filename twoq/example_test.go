package twoq_test

import (
	"fmt"

	"github.com/floatdrop/evict/twoq"
)

func Example() {
	cache := twoq.New[string, int](4)

	// "home" is requested, drops out of the cache, and is requested again.
	// That is what makes an entry frequent.
	cache.Set("home", 1)
	for _, page := range []string{"a", "b", "c", "d"} {
		cache.Set(page, 0)
	}
	_, ok := cache.Get("home")
	fmt.Println("home is cached:", ok)
	cache.Set("home", 1)

	// A scan over pages that are requested once now leaves it alone.
	for _, page := range []string{"e", "f", "g", "h", "i", "j"} {
		cache.Set(page, 0)
	}
	_, ok = cache.Get("home")
	fmt.Println("home is cached:", ok)

	// Output:
	// home is cached: false
	// home is cached: true
}

func ExampleWithRecentRatio() {
	// Half of the cache is for entries that have yet to prove themselves,
	// and the keys of as many evicted entries are remembered as there are
	// entries.
	cache := twoq.New[int, string](1000,
		twoq.WithRecentRatio(0.5),
		twoq.WithGhostRatio(1),
	)

	cache.Set(1, "one")
	fmt.Println(cache.Get(1))

	// Output:
	// one true
}
