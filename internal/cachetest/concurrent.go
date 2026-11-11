package cachetest

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Wide is a value of several words that says which key it belongs to and
// vouches for itself. A value that was read while it was being written, or
// that turns up under the wrong key, does not get past [Wide.Valid].
type Wide struct {
	Key, Version uint64
	Text         string
	Ref          *[2]uint64
	Sum          uint64
}

// texts are strings of different lengths, so that the pointer of one does not
// go with the length of another.
var texts = func() []string {
	all := make([]string, 13)
	for i := range all {
		all[i] = strings.Repeat(strconv.Itoa(i%10), 1+3*i)
	}
	return all
}()

const golden = 0x9e3779b97f4a7c15

// NewWide returns the value for a version of a key.
func NewWide(key, version uint64) Wide {
	text := texts[(key+version)%uint64(len(texts))]
	return Wide{
		Key:     key,
		Version: version,
		Text:    text,
		Ref:     &[2]uint64{version, key},
		Sum:     (key^version)*golden + uint64(len(text)),
	}
}

// Valid reports whether the value is one that NewWide made for key.
func (w Wide) Valid(key uint64) bool {
	text := texts[(w.Key+w.Version)%uint64(len(texts))]
	return w.Key == key && w.Text == text && w.Ref != nil && *w.Ref == [2]uint64{w.Version, w.Key} &&
		w.Sum == (w.Key^w.Version)*golden+uint64(len(text))
}

// Versions checks that a cache which is read and written by many goroutines
// at once gives every one of them answers that make sense:
//
//   - A value is whole and belongs to the key it is stored under.
//   - Whoever is alone in writing a key gets back what it wrote last, or
//     nothing, and never what it has deleted.
//   - Whoever reads a key twice does not get an older value the second time.
//
// Each goroutine is alone in writing some of the keys, and all of them read
// all of the keys.
func Versions(t *testing.T, newCache Factory[string, Wide]) {
	t.Helper()

	steps := 20_000
	if testing.Short() {
		steps = 2_000
	}

	for _, tt := range []struct{ capacity, shards, goroutines, keysEach int }{
		{1, 1, 8, 2},
		{3, 1, 8, 2},
		{8, 2, 8, 3},
		{40, 1, 8, 8},
		{64, 4, 16, 8},
		{2048, 0, 8, 64}, // Rarely evicts.
	} {
		name := fmt.Sprintf("capacity=%d/shards=%d/goroutines=%d", tt.capacity, tt.shards, tt.goroutines)
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := newCache(tt.capacity, tt.shards)
			keys := make([]string, tt.goroutines*tt.keysEach)
			for k := range keys {
				keys[k] = "key-" + strconv.Itoa(k)
			}

			var (
				wg     sync.WaitGroup
				failed atomic.Bool
			)
			fail := func(format string, args ...any) {
				if failed.CompareAndSwap(false, true) {
					t.Errorf(format, args...)
				}
			}
			for g := range tt.goroutines {
				wg.Go(func() {
					rng := rand.New(rand.NewPCG(uint64(g), 11))
					// The goroutine writes the keys g, g+goroutines, and so
					// on. written is the version it wrote last for each of
					// them, or zero if it has deleted the key, and seen is
					// the newest version of every key it has come across.
					written := make([]uint64, tt.keysEach)
					seen := make([]uint64, len(keys))
					version := uint64(0)

					for range steps {
						if failed.Load() {
							return
						}
						mine := rng.IntN(tt.keysEach)
						own := uint64(mine*tt.goroutines + g)
						pick := uint64(rng.IntN(len(keys)))

						switch op := rng.IntN(100); {
						case op < 40:
							v, ok := c.Get(keys[pick])
							if !ok {
								break
							}
							if !v.Valid(pick) || v.Version < seen[pick] {
								fail("Get(%d) = %+v after version %d was seen", pick, v, seen[pick])
							}
							seen[pick] = v.Version
						case op < 45:
							v, ok := c.Peek(keys[pick])
							if !ok {
								break
							}
							if !v.Valid(pick) || v.Version < seen[pick] {
								fail("Peek(%d) = %+v after version %d was seen", pick, v, seen[pick])
							}
							seen[pick] = v.Version
						case op < 65:
							if v, ok := c.Get(keys[own]); ok && (!v.Valid(own) || v.Version != written[mine]) {
								fail("Get(%d) = %+v, but the only one who writes the key wrote version %d last", own, v, written[mine])
							}
						case op < 89:
							version++
							k, v, displaced := c.Set(keys[own], NewWide(own, version))
							switch {
							case !displaced:
							case k == keys[own] && (!v.Valid(own) || v.Version != written[mine]):
								fail("Set(%d) replaced %+v, but version %d was written last", own, v, written[mine])
							case k != keys[own]:
								other, err := strconv.Atoi(strings.TrimPrefix(k, "key-"))
								if err != nil || !v.Valid(uint64(other)) {
									fail("Set(%d) evicted %q=%+v", own, k, v)
								}
							}
							written[mine] = version
						case op < 99:
							if v, ok := c.Delete(keys[own]); ok && (!v.Valid(own) || v.Version != written[mine]) {
								fail("Delete(%d) = %+v, but version %d was written last", own, v, written[mine])
							}
							written[mine] = 0
						case rng.IntN(4) == 0:
							c.Clear()
						default:
							n := 0
							for k, v := range c.All() {
								key, err := strconv.Atoi(strings.TrimPrefix(k, "key-"))
								if err != nil || !v.Valid(uint64(key)) {
									fail("All yields %q=%+v", k, v)
								}
								n++
							}
							if n > tt.capacity {
								fail("All yields %d entries with capacity %d", n, tt.capacity)
							}
						}
					}
				})
			}
			wg.Wait()
		})
	}
}

// Present checks that a cache does not miss an entry that is there, however
// much the entries around it come and go.
//
// The cache has room for every key, so nothing is evicted. Writers delete
// keys and put them back, and say when: the counter of a key is odd while it
// may be missing. A reader that finds a key missing whose counter was even
// before, and is the same after, has been told something that never was.
func Present(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	const (
		writers = 2
		readers = 6
	)
	lookups := 200_000
	if testing.Short() {
		lookups = 20_000
	}

	// The smallest caches have the smallest indexes, which are the ones
	// that a reader gets through while they change under it.
	for _, keys := range []int{1, 2, 6, 48, 500} {
		t.Run(fmt.Sprintf("keys=%d", keys), func(t *testing.T) {
			t.Parallel()

			c := newCache(keys, 1)
			present := make([]atomic.Uint32, keys)
			for key := range keys {
				c.Set(key, valueOf(key))
			}

			var (
				reading, writing sync.WaitGroup
				stop             atomic.Bool
			)
			for w := range writers {
				writing.Go(func() {
					rng := rand.New(rand.NewPCG(uint64(w), 13))
					for !stop.Load() {
						// A writer keeps to the keys that are its own.
						key := rng.IntN(keys)/writers*writers + w
						if key >= keys {
							runtime.Gosched() // It has none among so few.
							continue
						}
						if rng.IntN(3) == 0 {
							c.Set(key, valueOf(key)) // Replaces the entry.
							continue
						}

						present[key].Add(1)
						c.Delete(key)
						if rng.IntN(8) == 0 {
							runtime.Gosched()
						}
						c.Set(key, valueOf(key))
						present[key].Add(1)
					}
				})
			}
			for r := range readers {
				reading.Go(func() {
					rng := rand.New(rand.NewPCG(uint64(r), 17))
					for range lookups {
						key := rng.IntN(keys)
						before := present[key].Load()

						var (
							v  int
							ok bool
						)
						if r%3 == 0 {
							v, ok = c.Peek(key)
						} else {
							v, ok = c.Get(key)
						}

						switch {
						case ok && v != valueOf(key):
							t.Errorf("key %d has the value %d, want %d", key, v, valueOf(key))
							return
						case !ok && before&1 == 0 && present[key].Load() == before:
							t.Errorf("key %d was reported missing while it was there", key)
							return
						}
					}
				})
			}
			reading.Wait()
			stop.Store(true)
			writing.Wait()

			if got := c.Len(); got != keys {
				t.Errorf("Len() = %d in the end, want %d", got, keys)
			}
		})
	}
}

// ClearAtOnce checks that a shard is cleared in one go as far as anyone can
// tell: once a reader has found a key gone, no reader finds another key that
// was to go with it.
//
// One goroutine fills a cache of a single shard and clears it, over and over,
// and says when it is filling. A reader that finds one key missing and then
// another one present, without anything having been added in between, has
// seen the cache half cleared.
func ClearAtOnce(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	const (
		keys    = 2048
		readers = 4
	)
	rounds := 300
	if testing.Short() {
		rounds = 50
	}

	c := newCache(keys, 1)
	// filling is odd while keys are being added, and goes up by two with
	// every round.
	var (
		filling atomic.Uint64
		stop    atomic.Bool
		wg      sync.WaitGroup
	)
	filling.Store(1)

	for r := range readers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(r), 19))
			for !stop.Load() {
				first, second := rng.IntN(keys), rng.IntN(keys)

				before := filling.Load()
				_, found := c.Get(first)
				_, late := c.Get(second)
				if !found && late && before&1 == 0 && filling.Load() == before {
					t.Errorf("key %d was gone, and then key %d was still there, although nothing was added", first, second)
					return
				}
			}
		})
	}

	for range rounds {
		for key := range keys {
			c.Set(key, valueOf(key))
		}
		filling.Add(1)
		runtime.Gosched()
		c.Clear()
		filling.Add(1)
	}
	stop.Store(true)
	wg.Wait()
}
