package lru_test

import (
	"fmt"

	"github.com/floatdrop/evict/lru"
)

func Example() {
	cache := lru.New[string, int](2)

	cache.Set("a", 1)
	cache.Set("b", 2)

	cache.Get("a")    // "a" is now more recently used than "b",
	cache.Set("c", 3) // so "b" is the one to go.

	_, ok := cache.Get("b")
	fmt.Println(ok)

	if value, ok := cache.Get("a"); ok {
		fmt.Println(value)
	}

	// Output:
	// false
	// 1
}

// Set returns the entry that it pushed out of the cache, which is the place
// to release whatever that entry was holding on to.
func ExampleCache_Set() {
	cache := lru.New[string, string](1)

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

func ExampleCache_All() {
	// A cache in one piece yields its entries from the most to the least
	// recently used.
	cache := lru.New[string, int](3, lru.WithShards(1))

	cache.Set("a", 1)
	cache.Set("b", 2)
	cache.Set("c", 3)
	cache.Get("a")

	for key, value := range cache.All() {
		fmt.Println(key, value)
	}

	// Output:
	// a 1
	// c 3
	// b 2
}

func ExampleWithShards() {
	// Sixteen shards, each with its own lock and room for 4096 entries.
	cache := lru.New[int, string](1<<16, lru.WithShards(16))

	cache.Set(42, "answer")
	fmt.Println(cache.Get(42))
	fmt.Println(cache.Cap())

	// Output:
	// answer true
	// 65536
}
