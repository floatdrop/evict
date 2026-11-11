package s3fifo_test

import (
	"fmt"

	"github.com/floatdrop/evict/s3fifo"
)

func Example() {
	cache := s3fifo.New[string, int](100)

	// "home" is asked for again and again.
	cache.Set("home", 1)
	cache.Get("home")
	cache.Get("home")

	// A burst of pages that nobody comes back to passes through the cache
	// and leaves it alone.
	for i := range 1000 {
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
	cache := s3fifo.New[string, string](1)

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

// A larger small queue gives a new entry longer to be asked for again.
func ExampleWithSmallRatio() {
	for _, ratio := range []float64{0.1, 0.5} {
		cache := s3fifo.New[int, string](10, s3fifo.WithShards(1), s3fifo.WithSmallRatio(ratio))
		for key := 1; key <= 10; key++ {
			cache.Set(key, "")
		}

		cache.Set(100, "new")
		// Three more entries arrive before anyone asks for it again.
		for key := 11; key <= 13; key++ {
			cache.Set(key, "")
		}

		_, ok := cache.Get(100)
		fmt.Printf("with %2.0f%% for the small queue it is still there: %t\n", 100*ratio, ok)
	}

	// Output:
	// with 10% for the small queue it is still there: false
	// with 50% for the small queue it is still there: true
}
