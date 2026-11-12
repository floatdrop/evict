// Package wrap holds the caches that read without a lock in a generic type of
// its own, as a program may, and instantiates nothing. Package wrapuser does,
// so the methods of the caches are compiled there, by a compiler that knows
// only what this package has passed on, which is little.
package wrap

import (
	"github.com/floatdrop/evict/s3fifo"
	"github.com/floatdrop/evict/sieve"
	"github.com/floatdrop/evict/tinylfu"
)

// Caches holds a cache of each kind.
type Caches[K comparable, V any] struct {
	Sieve   *sieve.Cache[K, V]
	S3FIFO  *s3fifo.Cache[K, V]
	TinyLFU *tinylfu.Cache[K, V]
}

// Read gets and peeks at key in every cache, and returns how often it was
// found.
func (c Caches[K, V]) Read(key K) (found int) {
	for _, ok := range []bool{
		has(c.Sieve.Get(key)), has(c.Sieve.Peek(key)),
		has(c.S3FIFO.Get(key)), has(c.S3FIFO.Peek(key)),
		has(c.TinyLFU.Get(key)), has(c.TinyLFU.Peek(key)),
	} {
		if ok {
			found++
		}
	}
	return found
}

// ReadMany looks up keys in every cache, all of them together, and returns
// how often they were found.
func (c Caches[K, V]) ReadMany(keys []K) (found int) {
	values, ok := make([]V, len(keys)), make([]bool, len(keys))
	for _, getMany := range []func([]K, []V, []bool){c.Sieve.GetMany, c.S3FIFO.GetMany, c.TinyLFU.GetMany} {
		getMany(keys, values, ok)
		for _, ok := range ok {
			if ok {
				found++
			}
		}
	}
	return found
}

func has[V any](_ V, ok bool) bool { return ok }
