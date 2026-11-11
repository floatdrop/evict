package sieve_test

import (
	"fmt"

	"github.com/floatdrop/evict/sieve"
)

func Example() {
	// A cache in one piece evicts in exactly the order that SIEVE defines.
	cache := sieve.New[string, int](3, sieve.WithShards(1))

	cache.Set("a", 1)
	cache.Set("b", 2)
	cache.Set("c", 3)

	// "a" is the oldest entry, but it has been read since it was added.
	cache.Get("a")

	// So it is spared, and the next oldest entry goes in its place.
	key, _, _ := cache.Set("d", 4)
	fmt.Println("evicted", key)

	_, ok := cache.Get("a")
	fmt.Println("a is cached:", ok)

	// Output:
	// evicted b
	// a is cached: true
}

// Set returns the entry that it pushed out of the cache, which is the place
// to release whatever that entry was holding on to.
func ExampleCache_Set() {
	cache := sieve.New[string, string](1)

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
