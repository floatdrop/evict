package tinylfu_test

import (
	"fmt"

	"github.com/floatdrop/evict/tinylfu"
)

func Example() {
	cache := tinylfu.New[string, int](100)

	// "home" is asked for again and again, while pages that nobody comes
	// back to pass through the cache and leave it alone.
	cache.Set("home", 1)
	for i := range 1000 {
		cache.Get("home")
		cache.Set(fmt.Sprint("page ", i), i)
	}

	_, ok := cache.Get("home")
	fmt.Println("home is cached:", ok)
	_, ok = cache.Get("page 0")
	fmt.Println("page 0 is cached:", ok)

	// Output:
	// home is cached: true
	// page 0 is cached: false
}

// Set returns the entry that it pushed out of the cache, which is the place
// to release whatever that entry was holding on to.
func ExampleCache_Set() {
	cache := tinylfu.New[string, string](1)

	cache.Set("a", "first")

	if key, value, displaced := cache.Set("a", "second"); displaced {
		fmt.Printf("replaced %s=%s\n", key, value)
	}
	if key, value, displaced := cache.Set("b", "third"); displaced {
		fmt.Printf("evicted %s=%s\n", key, value)
	}

	// Output:
	// replaced a=first
	// evicted a=second
}
