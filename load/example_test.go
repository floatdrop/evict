package load_test

import (
	"context"
	"fmt"

	"github.com/floatdrop/evict/load"
	"github.com/floatdrop/evict/tinylfu"
)

func Example() {
	// db stands for where the values come from, such as a database.
	db := map[string]int{"a": 1}
	cache := load.Wrap(tinylfu.New[string, int](10_000), func(_ context.Context, key string) (int, error) {
		v, ok := db[key]
		if !ok {
			return 0, fmt.Errorf("no %q in the database", key)
		}
		return v, nil
	})
	ctx := context.Background()

	v, err := cache.Fetch(ctx, "a") // Loads a and stores it.
	fmt.Println(v, err)
	_, err = cache.Fetch(ctx, "b") // An error is not stored.
	fmt.Println(err)

	// After a change to the source, delete the key from the cache: a load of
	// it that is under way then does not store what it read before.
	db["a"] = 10
	cache.Delete("a")
	v, _ = cache.Fetch(ctx, "a")
	fmt.Println(v)
	// Output:
	// 1 <nil>
	// no "b" in the database
	// 10
}
