// Package probe uses a counted cache the way a program does: with key and
// value types of its own, and without importing anything but the counter, not
// even a cache of this module. The test two directories up compiles it and
// looks at what the compiler made of it.
package probe

import "github.com/floatdrop/evict/stats"

// Key is a key type that only this package instantiates the cache with.
type Key struct{ Tenant, ID string }

// Value is a value type that only this package instantiates the cache with.
type Value struct{ Size, Hits int }

// Use keeps the compiler from discarding the methods of the cache.
func Use(c *stats.Cache[Key, Value], k Key, v Value) (Value, bool) {
	c.Set(k, v)
	c.Peek(k)
	c.Delete(k)
	return c.Get(k)
}
