// Package caches puts the caches that the benchmarks compare behind one
// interface.
package caches

import (
	lru2 "github.com/hashicorp/golang-lru/v2"
	"github.com/maypok86/otter/v2"

	"github.com/floatdrop/evict/lru"
	"github.com/floatdrop/evict/s3fifo"
	"github.com/floatdrop/evict/sieve"
	"github.com/floatdrop/evict/twoq"
)

// Cache is the least that the benchmarks need of a cache.
type Cache[K comparable, V any] interface {
	// Get returns the value of key, if the cache has it.
	Get(key K) (V, bool)
	// Set stores value under key.
	Set(key K, value V)
}

// Settler is implemented by the caches that leave work for later, such as
// evicting what a write has pushed out.
type Settler interface {
	// Settle does the work that is pending.
	Settle()
}

// Kind is a cache implementation.
type Kind[K comparable, V any] struct {
	// Name is how reports refer to the implementation.
	Name string

	// New returns a cache the way a program would set it up: with the
	// defaults of the library.
	New func(capacity int) Cache[K, V]

	// Exact returns a cache that evicts as its policy says when it is used
	// by one goroutine: in one piece rather than in shards, and without
	// work that is left for later. That is the setup for comparing hit
	// ratios.
	Exact func(capacity int) Cache[K, V]

	// Shards returns a cache with the given number of shards, for the
	// implementations that let one choose. It is nil for the others.
	Shards func(capacity, shards int) Cache[K, V]

	// Varies marks an implementation that does not evict the same way every
	// time, even when it is set up with Exact: one replay of a trace says
	// little about it.
	Varies bool
}

// All returns the implementations: first the caches of this module, then
// otter, then the ones that are there for perspective.
func All[K comparable, V any]() []Kind[K, V] {
	return []Kind[K, V]{
		{
			Name: "evict/lru",
			New: func(capacity int) Cache[K, V] {
				return lruCache[K, V]{lru.New[K, V](capacity)}
			},
			Exact: func(capacity int) Cache[K, V] {
				return lruCache[K, V]{lru.New[K, V](capacity, lru.WithShards(1))}
			},
			Shards: func(capacity, shards int) Cache[K, V] {
				return lruCache[K, V]{lru.New[K, V](capacity, lru.WithShards(shards))}
			},
		},
		{
			Name: "evict/twoq",
			New: func(capacity int) Cache[K, V] {
				return twoqCache[K, V]{twoq.New[K, V](capacity)}
			},
			Exact: func(capacity int) Cache[K, V] {
				return twoqCache[K, V]{twoq.New[K, V](capacity, twoq.WithShards(1))}
			},
			Shards: func(capacity, shards int) Cache[K, V] {
				return twoqCache[K, V]{twoq.New[K, V](capacity, twoq.WithShards(shards))}
			},
		},
		{
			Name: "evict/sieve",
			New: func(capacity int) Cache[K, V] {
				return sieveCache[K, V]{sieve.New[K, V](capacity)}
			},
			Exact: func(capacity int) Cache[K, V] {
				return sieveCache[K, V]{sieve.New[K, V](capacity, sieve.WithShards(1))}
			},
			Shards: func(capacity, shards int) Cache[K, V] {
				return sieveCache[K, V]{sieve.New[K, V](capacity, sieve.WithShards(shards))}
			},
		},
		{
			Name: "evict/s3fifo",
			New: func(capacity int) Cache[K, V] {
				return s3fifoCache[K, V]{s3fifo.New[K, V](capacity)}
			},
			Exact: func(capacity int) Cache[K, V] {
				return s3fifoCache[K, V]{s3fifo.New[K, V](capacity, s3fifo.WithShards(1))}
			},
			Shards: func(capacity, shards int) Cache[K, V] {
				return s3fifoCache[K, V]{s3fifo.New[K, V](capacity, s3fifo.WithShards(shards))}
			},
		},
		{
			Name: "otter",
			// Setting the initial capacity is not a default, but it is what
			// otter's own benchmarks do, and it spares otter from growing
			// its table while it is being measured.
			New: func(capacity int) Cache[K, V] {
				return otterCache[K, V]{otter.Must(&otter.Options[K, V]{
					MaximumSize:     capacity,
					InitialCapacity: capacity,
				})}
			},
			Exact: func(capacity int) Cache[K, V] {
				return otterCache[K, V]{otter.Must(&otter.Options[K, V]{
					MaximumSize:     capacity,
					InitialCapacity: capacity,
					// Otter evicts in the background by default. Doing it
					// on the spot is what its own simulator does.
					Executor: func(fn func()) { fn() },
				})}
			},
			// Otter admits some of the entries at random.
			Varies: true,
		},
		{
			Name:  "golang-lru",
			New:   newGolangLRU[K, V],
			Exact: newGolangLRU[K, V],
		},
		{
			Name:  "golang-lru/2q",
			New:   newGolang2Q[K, V],
			Exact: newGolang2Q[K, V],
		},
	}
}

// Find returns the implementation with the given name.
func Find[K comparable, V any](name string) (Kind[K, V], bool) {
	for _, kind := range All[K, V]() {
		if kind.Name == name {
			return kind, true
		}
	}
	return Kind[K, V]{}, false
}

type lruCache[K comparable, V any] struct{ c *lru.Cache[K, V] }

func (c lruCache[K, V]) Get(key K) (V, bool) { return c.c.Get(key) }
func (c lruCache[K, V]) Set(key K, value V)  { c.c.Set(key, value) }

type twoqCache[K comparable, V any] struct{ c *twoq.Cache[K, V] }

func (c twoqCache[K, V]) Get(key K) (V, bool) { return c.c.Get(key) }
func (c twoqCache[K, V]) Set(key K, value V)  { c.c.Set(key, value) }

type sieveCache[K comparable, V any] struct{ c *sieve.Cache[K, V] }

func (c sieveCache[K, V]) Get(key K) (V, bool) { return c.c.Get(key) }
func (c sieveCache[K, V]) Set(key K, value V)  { c.c.Set(key, value) }

type s3fifoCache[K comparable, V any] struct{ c *s3fifo.Cache[K, V] }

func (c s3fifoCache[K, V]) Get(key K) (V, bool) { return c.c.Get(key) }
func (c s3fifoCache[K, V]) Set(key K, value V)  { c.c.Set(key, value) }

type otterCache[K comparable, V any] struct{ c *otter.Cache[K, V] }

func (c otterCache[K, V]) Get(key K) (V, bool) { return c.c.GetIfPresent(key) }
func (c otterCache[K, V]) Set(key K, value V)  { c.c.Set(key, value) }
func (c otterCache[K, V]) Settle()             { c.c.CleanUp() }

type golangLRU[K comparable, V any] struct{ c *lru2.Cache[K, V] }

func newGolangLRU[K comparable, V any](capacity int) Cache[K, V] {
	c, err := lru2.New[K, V](capacity)
	if err != nil {
		panic(err)
	}
	return golangLRU[K, V]{c}
}

func (c golangLRU[K, V]) Get(key K) (V, bool) { return c.c.Get(key) }
func (c golangLRU[K, V]) Set(key K, value V)  { c.c.Add(key, value) }

type golang2Q[K comparable, V any] struct{ c *lru2.TwoQueueCache[K, V] }

func newGolang2Q[K comparable, V any](capacity int) Cache[K, V] {
	c, err := lru2.New2Q[K, V](capacity)
	if err != nil {
		panic(err)
	}
	return golang2Q[K, V]{c}
}

func (c golang2Q[K, V]) Get(key K) (V, bool) { return c.c.Get(key) }
func (c golang2Q[K, V]) Set(key K, value V)  { c.c.Add(key, value) }
