// Package replay measures how many requests of a trace a cache can answer.
package replay

import "github.com/floatdrop/evict/bench/caches"

// Hits replays a trace against a cache the way a program uses a cache: it
// asks for every key in turn and stores the ones that are missing. It returns
// the number of requests that the cache answered.
func Hits(c caches.Cache[uint64, uint64], keys []uint64) int {
	hits := 0
	for _, key := range keys {
		if _, ok := c.Get(key); ok {
			hits++
		} else {
			c.Set(key, key)
		}
	}
	return hits
}

// never is the position of the next request for a key that is not requested
// again.
const never = int32(1<<31 - 1)

// Future tells of every request of a trace when its key is requested next:
// the position of that request, or one past anything a trace can hold if
// there is none. It is what [Optimal] knows that a cache does not. Working it
// out takes a while and as much memory as half the trace, so it is done once
// for a trace and not once for every capacity.
type Future []int32

// Foresee returns the future of a trace.
func Foresee(keys []uint64) Future {
	if len(keys) >= int(never) {
		panic("replay: the trace is too long")
	}
	next := make(Future, len(keys))
	last := make(map[uint64]int32)
	for i := len(keys) - 1; i >= 0; i-- {
		if j, ok := last[keys[i]]; ok {
			next[i] = j
		} else {
			next[i] = never
		}
		last[keys[i]] = int32(i)
	}
	return next
}

// Optimal returns the number of requests that the best possible cache of the
// given capacity answers: one that knows the requests to come, which next
// must be for these keys. When it is full, it gives up the key that will not
// be needed for the longest time, as Belady's algorithm does. That may be the
// key it has just been asked for, which it then does not store at all. No
// cache that learns of a request only when it arrives can do better.
func Optimal(keys []uint64, next Future, capacity int) int {
	if len(next) != len(keys) {
		panic("replay: the future is that of another trace")
	}
	if capacity <= 0 {
		return 0
	}

	// The cache is a heap with the entry on top whose next request is
	// furthest away.
	h := heap{position: make(map[uint64]int32, capacity)}
	hits := 0
	for i, key := range keys {
		switch pos, ok := h.position[key]; {
		case ok:
			hits++
			h.update(pos, next[i])
		case next[i] == never:
			// Not worth a place in the cache.
		case len(h.entries) < capacity:
			h.push(key, next[i])
		case next[i] < h.entries[0].next:
			h.pop()
			h.push(key, next[i])
		}
	}
	return hits
}

// heap is a binary max-heap of cache entries by the time of their next
// request, which also knows where each key is.
type heap struct {
	entries  []entry
	position map[uint64]int32
}

type entry struct {
	key  uint64
	next int32
}

func (h *heap) push(key uint64, next int32) {
	h.entries = append(h.entries, entry{key, next})
	h.up(int32(len(h.entries) - 1))
}

// pop removes the top of the heap.
func (h *heap) pop() {
	last := int32(len(h.entries) - 1)
	delete(h.position, h.entries[0].key)
	h.entries[0] = h.entries[last]
	h.entries = h.entries[:last]
	if last > 0 {
		h.down(0)
	}
}

// update gives the entry at pos a later next request.
func (h *heap) update(pos, next int32) {
	h.entries[pos].next = next
	h.up(pos)
}

func (h *heap) up(pos int32) {
	e := h.entries[pos]
	for pos > 0 {
		parent := (pos - 1) / 2
		if h.entries[parent].next >= e.next {
			break
		}
		h.entries[pos] = h.entries[parent]
		h.position[h.entries[pos].key] = pos
		pos = parent
	}
	h.entries[pos] = e
	h.position[e.key] = pos
}

func (h *heap) down(pos int32) {
	e := h.entries[pos]
	n := int32(len(h.entries))
	for {
		child := 2*pos + 1
		if child >= n {
			break
		}
		if right := child + 1; right < n && h.entries[right].next > h.entries[child].next {
			child = right
		}
		if e.next >= h.entries[child].next {
			break
		}
		h.entries[pos] = h.entries[child]
		h.position[h.entries[pos].key] = pos
		pos = child
	}
	h.entries[pos] = e
	h.position[e.key] = pos
}
