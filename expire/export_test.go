package expire

import (
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/floatdrop/evict"
)

// Clock is a fake clock for tests. It starts at zero, and has the time that
// readers look at lag behind the time itself only when told to.
type Clock struct{ c *clock }

// NewClock returns a fake clock.
func NewClock() Clock {
	return Clock{&clock{fixed: true, begin: func() {}}}
}

// Advance moves the time on by d, and the copy that readers look at with it.
func (c Clock) Advance(d time.Duration) {
	atomic.AddInt64(&c.c.fake, int64(d))
	atomic.AddInt64(&c.c.now, int64(d))
}

// Lag sets the copy of the time that readers look at to d behind the time, as
// if the timer that updates it were late, but no further back than zero, where
// it starts.
func (c Clock) Lag(d time.Duration) {
	atomic.StoreInt64(&c.c.now, max(atomic.LoadInt64(&c.c.fake)-int64(d), 0))
}

// WrapWith is Wrap with a fake clock.
func WrapWith[K comparable, V any](cache evict.Cache[K, Entry[V]], ttl time.Duration, clock Clock) *Cache[K, V] {
	return wrap(cache, ttl, clock.c)
}

// NowOffset is where the time that readers look at lies in a clock.
const NowOffset = unsafe.Offsetof(clock{}.now)

// FakeOffset is where the first field after the line of now lies in a clock.
const FakeOffset = unsafe.Offsetof(clock{}.fake)

// SystemNow returns the time of the last tick of the clock that Wrap uses.
func SystemNow() time.Duration { return time.Duration(atomic.LoadInt64(&system.now)) }

// SystemRead returns the time by the clock that Wrap uses.
func SystemRead() time.Duration { return time.Duration(system.read()) }

// NewRealClock returns a clock that reads the time like the one Wrap uses,
// but has no timer: the copy of the time that readers look at stays where
// SetCopy puts it.
func NewRealClock() Clock {
	return Clock{&clock{epoch: time.Now(), begin: func() {}}}
}

// SetCopy sets the copy of the time that readers look at.
func (c Clock) SetCopy(d time.Duration) { atomic.StoreInt64(&c.c.now, int64(d)) }

// Read returns the time by the clock.
func (c Clock) Read() time.Duration { return time.Duration(c.c.read()) }

// PooledBatch takes a batch that GetMany has put back from the pool of c, and
// reports whether it holds nothing but zero entries. got is false if the pool
// had none to hand, which may happen when the goroutine has moved to another
// CPU since it called GetMany.
func PooledBatch[K, V comparable](c *Cache[K, V]) (clean, got bool) {
	b, _ := c.batches.Get().(*batch[V])
	if b == nil {
		return false, false
	}
	defer c.batches.Put(b)
	for _, e := range b {
		if e != (Entry[V]{}) {
			return false, true
		}
	}
	return true, true
}

// DeadlineOf returns the deadline of the entry of key, as the wrapped cache
// holds it, or -1 if it has none.
func DeadlineOf[K comparable, V any](c *Cache[K, V], key K) int64 {
	e, ok := c.cache.Peek(key)
	if !ok {
		return -1
	}
	return e.deadline
}
