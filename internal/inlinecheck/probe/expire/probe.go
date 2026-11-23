// Package probe uses a cache whose entries expire the way a program does: with
// key and value types of its own, and without importing anything but the
// wrapper and a cache to wrap. The test two directories up compiles it and
// looks at what the compiler made of it.
package probe

import (
	"time"

	"github.com/floatdrop/evict/expire"
)

// Key is a key type that only this package instantiates the cache with.
type Key struct{ Tenant, ID string }

// Value is a value type that only this package instantiates the cache with.
type Value struct{ Size, Hits int }

// Use keeps the compiler from discarding the methods of the cache.
func Use(c *expire.Cache[Key, Value], k Key, v Value) (Value, bool) {
	c.Set(k, v)
	c.SetWithTTL(k, v, time.Second)
	c.Peek(k)
	c.Delete(k)
	var values [2]Value
	var found [2]bool
	c.GetMany([]Key{k, k}, values[:], found[:])
	return c.Get(k)
}
