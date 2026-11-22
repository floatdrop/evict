package bench

import (
	"context"
	"testing"

	"github.com/maypok86/otter/v2"

	"github.com/floatdrop/evict/bench/caches"
)

// otterLoading is otter set up as caches.All sets it up, read through its
// loader: Get with a Loader, which loads a missing key once however many
// goroutines ask for it, and stores it.
type otterLoading struct {
	c      *otter.Cache[string, string]
	loader otter.Loader[string, string]
}

func (o otterLoading) Get(key string) (string, bool) { return o.c.GetIfPresent(key) }
func (o otterLoading) Set(key, value string)         { o.c.Set(key, value) }
func (o otterLoading) Settle()                       { o.c.CleanUp() }

// BenchmarkReadThroughLoader is BenchmarkReadThrough for otter alone, through
// its loader rather than GetIfPresent and Set.
func BenchmarkReadThroughLoader(b *testing.B) {
	saved := kinds
	defer func() { kinds = saved }()
	kinds = []caches.Kind[string, string]{{
		Name: "otter",
		New: func(capacity int) caches.Cache[string, string] {
			return otterLoading{
				c: otter.Must(&otter.Options[string, string]{
					MaximumSize:     capacity,
					InitialCapacity: capacity,
				}),
				loader: otter.LoaderFunc[string, string](func(_ context.Context, key string) (string, error) {
					return key, nil
				}),
			}
		},
	}}
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		w    func() *workload
	}{{"capacity=16K", crowded}, {"capacity=1M", crowdedLarge}} {
		b.Run(tt.name, func(b *testing.B) {
			run(b, tt.w(), false, func(c caches.Cache[string, string], key string, _ uint32) bool {
				o := c.(otterLoading)
				_, _ = o.c.Get(ctx, key, o.loader)
				return false
			})
		})
	}
}
