// Package probe uses a counted cache through statswrap, a generic type of
// another package that instantiates nothing. The test two directories up
// compiles it and looks at what the compiler made of it.
package probe

import wrap "github.com/floatdrop/evict/internal/inlinecheck/probe/statswrap"

// Key is a key type that only this package instantiates the cache with.
type Key struct{ Tenant, ID string }

// Value is a value type that only this package instantiates the cache with.
type Value struct{ Size, Hits int }

// Use keeps the compiler from discarding the methods of the cache.
func Use(c wrap.Cache[Key, Value], k Key, v Value) (Value, bool) {
	c.Set(k, v)
	var values [2]Value
	var found [2]bool
	c.GetMany([]Key{k, k}, values[:], found[:])
	return c.Get(k)
}
