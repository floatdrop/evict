// Command memory measures what a cache costs in memory for every entry it
// has room for, and what a full cache costs the garbage collector, which has
// to look at everything in the heap that may hold a pointer.
//
//	go run ./cmd/memory
//
// writes results/memory.md.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"

	"github.com/floatdrop/evict/bench/caches"
	"github.com/floatdrop/evict/bench/report"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "memory:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("memory", flag.ContinueOnError)
	out := flags.String("out", "results", "`directory` to write memory.md to")
	sizes := flags.String("capacities", "10000,100000,1000000,10000000", "comma-separated `capacities` to measure")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	var capacities []int
	for size := range strings.SplitSeq(*sizes, ",") {
		n, err := strconv.Atoi(size)
		if err != nil || n <= 0 {
			return fmt.Errorf("%q is not a capacity", size)
		}
		capacities = append(capacities, n)
	}
	// Better to find out now than after everything has been measured.
	if err := os.MkdirAll(*out, 0o750); err != nil {
		return err
	}

	var b strings.Builder
	fmt.Fprintf(&b, `# Memory

What a cache takes up for every entry it has room for, in bytes. A cache is set up the
way its library suggests and written to until it is full, with four times as many keys
as it has room for. The live heap is measured before and after.

Produced by `+"`%s`"+` with %s on %s/%s.

## Integers

Keys and values are `+"`uint64`"+`. The 16 bytes they take are part of the number, since the
caches of this module store them in place.

`, strings.TrimSpace("go run ./cmd/memory "+strings.Join(args, " ")), runtime.Version(), runtime.GOOS, runtime.GOARCH)
	numbers := measure(&b, capacities, true, func(i int) uint64 { return uint64(i) }, func(i int) uint64 { return uint64(i) })

	b.WriteString(`
## Strings

Keys and values are strings that exist before the cache does, so the number is the
overhead of the cache alone, including the 32 bytes of the two string headers.

`)
	names := make([]string, 4*slices.Max(capacities))
	for i := range names {
		names[i] = fmt.Sprintf("%032d", i)
	}
	measure(&b, capacities, false, func(i int) string { return names[i] }, func(i int) string { return names[i] })

	b.WriteString(`
## Garbage collection

What a full cache of integers gives the garbage collector to do. The collector does not
look into memory that holds no pointers, which is all there is to a cache of this module
when its keys and values have none.

The first table has the part of the heap that the collector has to look through because
of the cache, in bytes for every entry the cache has room for. It hardly differs from
one run to the next.

`)
	table(&b, capacities, numbers, func(m measurement) string {
		return strconv.FormatFloat(m.scan, 'f', 1, 64)
	})

	b.WriteString(`
The second has the CPU time that one collection takes while the cache is alive and
little else is, in milliseconds: the least and the most of seven collections. It is the
time of all the CPUs that took part, so it does not shrink with their number as the wait
for a collection does. Whatever else the machine is doing adds to it, which is why the
lower number is the one to go by.

`)
	table(&b, capacities, numbers, func(m measurement) string {
		return fmt.Sprintf("%.2f–%.2f", m.gcMin*1000, m.gcMax*1000)
	})

	if err := os.WriteFile(filepath.Join(*out, "memory.md"), []byte(b.String()), 0o644); err != nil { //nolint:gosec // A report is for everyone to read.
		return err
	}
	_, err := io.WriteString(stdout, b.String())
	return err
}

type measurement struct {
	bytes float64 // of live heap, per entry of capacity
	scan  float64 // of heap that the collector looks through, per entry of capacity

	// CPU seconds of a collection with the cache alive: the least and the
	// most of several.
	gcMin, gcMax float64
}

// measure fills a cache of every kind at every capacity, writes a table of
// the bytes per entry, and returns all it has measured, by kind and capacity.
// Collections are timed only if asked for: it takes a while.
func measure[K comparable, V any](b *strings.Builder, capacities []int, timeGC bool, key func(i int) K, value func(i int) V) [][]measurement {
	kinds := caches.All[K, V]()
	results := make([][]measurement, len(kinds))
	for i, kind := range kinds {
		for _, capacity := range capacities {
			m := measureOne(kind, capacity, timeGC, key, value)
			results[i] = append(results[i], m)
			fmt.Fprintf(os.Stderr, "%s at %d: %.1f bytes per entry\n", kind.Name, capacity, m.bytes)
		}
	}

	table(b, capacities, results, func(m measurement) string {
		return strconv.FormatFloat(m.bytes, 'f', 1, 64)
	})
	return results
}

func measureOne[K comparable, V any](kind caches.Kind[K, V], capacity int, timeGC bool, key func(i int) K, value func(i int) V) measurement {
	before := readHeap()

	c := kind.New(capacity)
	for i := range 4 * capacity {
		c.Set(key(i), value(i))
	}
	// A cache that evicts in the background gets to finish, so that it is
	// measured at the size it was asked to have.
	if s, ok := c.(caches.Settler); ok {
		s.Settle()
	}

	after := readHeap()
	m := measurement{
		bytes: float64(after.live-before.live) / float64(capacity),
		scan:  float64(after.scan-before.scan) / float64(capacity),
	}

	if timeGC {
		times := make([]float64, 7)
		for i := range times {
			start := gcCPU()
			runtime.GC()
			times[i] = gcCPU() - start
		}
		m.gcMin, m.gcMax = slices.Min(times), slices.Max(times)
	}

	runtime.KeepAlive(c)
	return m
}

// heap is what the runtime says about the heap after a collection.
type heap struct {
	live int64 // Bytes in use.
	scan int64 // Bytes that the collector has to look through for pointers.
}

func readHeap() heap {
	// The first collection finds what is unreachable; the second one makes
	// sure that the numbers have caught up.
	runtime.GC()
	runtime.GC()

	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/gc/scan/heap:bytes"},
	}
	metrics.Read(samples)
	return heap{live: int64(samples[0].Value.Uint64()), scan: int64(samples[1].Value.Uint64())}
}

// gcCPU returns the CPU time that the garbage collector has used so far, in
// seconds, as far as the runtime can tell.
func gcCPU() float64 {
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
	metrics.Read(samples)
	return samples[0].Value.Float64()
}

// table writes one row for every kind of cache and one column for every
// capacity.
func table(b *strings.Builder, capacities []int, results [][]measurement, cell func(measurement) string) {
	b.WriteString("| cache |")
	for _, capacity := range capacities {
		fmt.Fprintf(b, " %s |", report.Thousands(capacity))
	}
	b.WriteString("\n|:---|")
	b.WriteString(strings.Repeat("---:|", len(capacities)))
	b.WriteString("\n")

	for i, kind := range caches.All[int, int]() {
		fmt.Fprintf(b, "| %s |", kind.Name)
		for _, m := range results[i] {
			fmt.Fprintf(b, " %s |", cell(m))
		}
		b.WriteString("\n")
	}
}
