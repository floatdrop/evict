package load

import (
	"context"
	"hash/maphash"

	"github.com/floatdrop/evict"
)

// WrapNotify is Wrap with functions that are called whenever a load starts,
// before the Fetch that starts it waits, and whenever one is over, once its
// value has been stored or not. Either may be nil.
func WrapNotify[K comparable, V any](cache evict.Cache[K, V], load func(context.Context, K) (V, error), started, finished func()) *Cache[K, V] {
	c := Wrap(cache, load)
	c.started, c.finished = started, finished
	return c
}

// Loading returns how many loads are in progress whose values are still to be
// stored.
func Loading[K comparable, V any](c *Cache[K, V]) int {
	n := 0
	for i := range c.stripes {
		s := &c.stripes[i]
		s.mu.Lock()
		n += len(s.loads)
		s.mu.Unlock()
	}
	return n
}

// Waiting returns how many Fetch calls wait for the load of key whose value
// is still to be stored, or -1 if there is no such load.
func Waiting[K comparable, V any](c *Cache[K, V], key K) int {
	s := c.stripe(maphash.Comparable(c.seed, key))
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.loads[key]; l != nil {
		return l.waiters
	}
	return -1
}

// Held reports whether the stripe of key is locked. A goroutine that holds the
// lock gets true as well: the lock is not reentrant.
func Held[K comparable, V any](c *Cache[K, V], key K) bool {
	s := c.stripe(maphash.Comparable(c.seed, key))
	if s.mu.TryLock() {
		s.mu.Unlock()
		return false
	}
	return true
}

// ErrGoexit is what a load returns if its loader calls runtime.Goexit.
var ErrGoexit = errGoexit
