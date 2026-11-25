// Package probe uses a loading cache the way a program does: with key and
// value types of its own, and without importing anything but the loader, not
// even a cache of this module. The test two directories up compiles it and
// looks at what the compiler made of it.
package probe

import (
	"context"

	"github.com/floatdrop/evict/load"
)

// Key is a key type that only this package instantiates the cache with.
type Key struct{ Tenant, ID string }

// Value is a value type that only this package instantiates the cache with.
type Value struct{ Size, Hits int }

// Use keeps the compiler from discarding the methods of the cache.
func Use(ctx context.Context, c *load.Cache[Key, Value], k Key, v Value) (Value, error) {
	c.Set(k, v)
	c.SetIfAbsent(k, v)
	c.Peek(k)
	c.Get(k)
	c.Delete(k)
	return c.Fetch(ctx, k)
}
