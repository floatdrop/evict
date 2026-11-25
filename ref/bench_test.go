package ref_test

import (
	"math/rand/v2"
	"testing"

	"github.com/floatdrop/evict/ref"
	"github.com/floatdrop/evict/tinylfu"
)

const benchCapacity = 1 << 14

// BenchmarkGet reads entries that are all there, on every CPU at once: with
// the Get of tinylfu, and with ref.Get and Release, spread over every key or
// all of one key, whose count every reader then writes.
func BenchmarkGet(b *testing.B) {
	direct := tinylfu.New[int, int](benchCapacity)
	counted := tinylfu.New[int, *ref.Ref[int]](benchCapacity)
	for key := range benchCapacity {
		direct.Set(key, key)
		counted.Set(key, ref.New(key, nil))
	}

	b.Run("spread-direct", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			for pb.Next() {
				direct.Get(i & (benchCapacity - 1))
				i++
			}
		})
	})

	b.Run("spread-ref", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			for pb.Next() {
				if r, _, ok := ref.Get(counted, i&(benchCapacity-1)); ok {
					r.Release()
				}
				i++
			}
		})
	})

	b.Run("one-direct", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				direct.Get(0)
			}
		})
	})

	b.Run("one-ref", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if r, _, ok := ref.Get(counted, 0); ok {
					r.Release()
				}
			}
		})
	})
}

// BenchmarkNew makes a Ref and drops it, which releases it.
func BenchmarkNew(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		ref.New(1, nil).Drop()
	}
}
