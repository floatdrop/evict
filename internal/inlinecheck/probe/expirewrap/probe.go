// Package probe is a generic type that wraps a cache whose entries expire, and
// instantiates nothing, like a program's own cache type might. The package that
// uses it, expirewrapuser, is where its methods and those of the wrapper are
// compiled, knowing only what this package has passed on, which is next to
// nothing.
package probe

import "github.com/floatdrop/evict/expire"

// Cache wraps a cache whose entries expire.
type Cache[K comparable, V any] struct{ C *expire.Cache[K, V] }

// Get reads from the cache.
func (c Cache[K, V]) Get(key K) (V, bool) { return c.C.Get(key) }

// GetMany reads several keys from the cache.
func (c Cache[K, V]) GetMany(keys []K, values []V, found []bool) { c.C.GetMany(keys, values, found) }

// Peek reads from the cache without recording the access.
func (c Cache[K, V]) Peek(key K) (V, bool) { return c.C.Peek(key) }

// Set writes to the cache.
func (c Cache[K, V]) Set(key K, value V) { c.C.Set(key, value) }
