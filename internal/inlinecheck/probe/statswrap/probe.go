// Package probe is a generic type that wraps a counted cache and instantiates
// nothing, like a program's own cache type might. The package that uses it,
// statswrapuser, is where its methods and those of the counted cache are
// compiled, knowing only what this package has passed on, which is next to
// nothing.
package probe

import "github.com/floatdrop/evict/stats"

// Cache wraps a counted cache.
type Cache[K comparable, V any] struct{ C *stats.Cache[K, V] }

// Get reads from the counted cache.
func (c Cache[K, V]) Get(key K) (V, bool) { return c.C.Get(key) }

// GetMany reads several keys from the counted cache.
func (c Cache[K, V]) GetMany(keys []K, values []V, found []bool) { c.C.GetMany(keys, values, found) }

// Set writes to the counted cache.
func (c Cache[K, V]) Set(key K, value V) { c.C.Set(key, value) }
