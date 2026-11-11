package main

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

// The output of two runs of the benchmarks, the second of which was cut
// short. The numbers of CPUs are not in order, one cache was too fast to be
// timed once, and another one was not measured on a single CPU at all.
const output = `go: go1.27.3
otter: v2.4.0
golang-lru: v2.0.7
goos: darwin
goarch: arm64
pkg: github.com/floatdrop/evict/bench
cpu: Apple M3 Max
BenchmarkGet/cache=evict/lru-8       	 5000000	        40.00 ns/op	        25.00 Mops/s	       0 B/op	       0 allocs/op
BenchmarkGet/cache=evict/lru         	 5000000	        20.00 ns/op	        50.00 Mops/s	       0 B/op	       0 allocs/op
BenchmarkGet/cache=otter             	 5000000	        40.00 ns/op	        25.00 Mops/s	       0 B/op	       0 allocs/op
BenchmarkGet/cache=otter-8           	 5000000	         5.00 ns/op	       200.0 Mops/s	       0 B/op	       0 allocs/op
BenchmarkGet/cache=slow-8            	 5000000	       500.00 ns/op	         2.00 Mops/s	       100.0 hit%	       0 B/op	       0 allocs/op
BenchmarkReadThrough/capacity=16K/cache=evict/sieve-8   	 100	  20.0 ns/op	  50.50 Mops/s	  91.44 hit%	 0 B/op	 0 allocs/op
BenchmarkReadThrough/capacity=16K/cache=otter-8         	 100	  40.0 ns/op	  25.25 Mops/s	  89.21 hit%	 3 B/op	 0 allocs/op
PASS
ok  	github.com/floatdrop/evict/bench	12.3s
go: go1.27.3
goos: darwin
goarch: arm64
pkg: github.com/floatdrop/evict/bench
cpu: Apple M3 Max
BenchmarkGet/cache=evict/lru         	 5000000	        25.00 ns/op	        40.00 Mops/s	       0 B/op	       0 allocs/op
BenchmarkGet/cache=evict/lru-8       	 5000000	        50.00 ns/op	        20.00 Mops/s	       0 B/op	       0 allocs/op
BenchmarkGet/cache=otter             	 5000000	        40.00 ns/op	        25.00 Mops/s	       0 B/op	       0 allocs/op
BenchmarkGet/cache=otter-8           	       1	         0.00 ns/op	        +Inf Mops/s	       0 B/op	       0 allocs/op
BenchmarkGet/cache=evict/lru         	 5000000	        10.00 ns/op	       100.00 Mops/s	       0 B/op	       0 allocs/op
BenchmarkGet/cache=evict/lru         	       1	         0.00 ns/op	         NaN Mops/s	       0 B/op	       0 allocs/op
BenchmarkReadThrough/capacity=16K/cache=evict/sieve-8   	 100	  20.0 ns/op	  49.50 Mops/s	  91.04 hit%	 0 B/op	 0 allocs/op
BenchmarkReadThrough/capacity=16K/cache=otter-8         	 100	  40.0 ns/op	  49.50 Mops/s	  88.21 hit%	 3 B/op	 0 allocs/op
`

func TestRender(t *testing.T) {
	t.Parallel()

	// With two or three runs of a cache, none of them has won anything.
	// The only cache that says how many reads it answered says all of
	// them; that is not known of the others.
	const want = `### Get

| CPUs | evict/lru | otter | slow |
|---:|---:|---:|---:|
| 1 | 50.0 (40.0–100.0) | 25.0 | |
| 8 | 22.5 (20.0–25.0) | 200.0 | 2.0 |

Share of the reads that were answered, in percent:

| CPUs | evict/lru | otter | slow |
|---:|---:|---:|---:|
| 1 | | | |
| 8 | | | 100.0 |

### ReadThrough, capacity=16K

| CPUs | evict/sieve | otter |
|---:|---:|---:|
| 8 | 50.0 (49.5–50.5) | 37.4 (25.2–49.5) |

Share of the reads that were answered, in percent:

| CPUs | evict/sieve | otter |
|---:|---:|---:|
| 8 | 91.2 | 88.7 |
`
	tables, err := parse(strings.NewReader(output))
	if err != nil {
		t.Fatal(err)
	}
	if got := render(tables); got != want {
		t.Errorf("render wrote:\n%s\nwant:\n%s", got, want)
	}
}

func TestChart(t *testing.T) {
	t.Parallel()

	tables, err := parse(strings.NewReader(output))
	if err != nil {
		t.Fatal(err)
	}

	c := tables[0].chart()
	if c.Title != "Get" || !slices.Equal(c.X, []string{"1", "8"}) || len(c.Series) != 3 {
		t.Fatalf("chart = %+v", c)
	}
	if s := c.Series[0]; s.Name != "evict/lru" || !slices.Equal(s.Values, []float64{50, 22.5}) {
		t.Errorf("first series = %+v, want the medians of evict/lru", s)
	}
	// A cache that was not measured on every number of CPUs leaves a gap,
	// and a run that could not be timed leaves no trace.
	if s := c.Series[2]; s.Name != "slow" || !math.IsNaN(s.Values[0]) || s.Values[1] != 2 {
		t.Errorf("third series = %+v, want a gap and then 2", s)
	}
	svg, err := c.SVG()
	if err != nil || strings.Contains(svg, "NaN") || strings.Contains(svg, "Inf") {
		t.Errorf("SVG: %v\n%s", err, svg)
	}

	c = tables[1].chart()
	if c.Title != "ReadThrough, capacity=16K" || len(c.X) != 1 {
		t.Fatalf("chart = %+v", c)
	}

	for name, want := range map[string]string{
		"Get":                      "get",
		"Mixed/writes=10%":         "mixed-writes-10",
		"ReadThrough/capacity=16K": "readthrough-capacity-16k",
		"//":                       "",
	} {
		if got := slug(name); got != want {
			t.Errorf("slug(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestPreamble(t *testing.T) {
	t.Parallel()

	got := preamble(output)
	if !strings.Contains(got, "Measured on Apple M3 Max (darwin/arm64) with go1.27.3, otter v2.4.0, golang-lru v2.0.7.\n") {
		t.Errorf("preamble does not say where the numbers are from:\n%s", got)
	}
	if got := preamble("PASS\n"); !strings.Contains(got, "Measured on an unnamed CPU (?/?) with an unnamed version of Go.\n") {
		t.Errorf("preamble of output that says nothing about itself:\n%s", got)
	}
}

func TestParseErrors(t *testing.T) {
	t.Parallel()

	tables, err := parse(strings.NewReader("no benchmarks here\n"))
	if err != nil || len(tables) != 0 {
		t.Errorf("parse = %v, %v, want nothing", tables, err)
	}

	_, err = parse(strings.NewReader("BenchmarkGet/cache=otter-8  100  x ns/op\n"))
	if err == nil {
		t.Error("parse accepted a value that is not a number")
	}
}

func TestMedian(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		values []float64
		want   float64
	}{
		{nil, 0}, {[]float64{3}, 3}, {[]float64{3, 1}, 2}, {[]float64{5, 1, 3}, 3}, {[]float64{4, 1, 3, 2}, 2.5},
	} {
		if got := median(tt.values); got != tt.want {
			t.Errorf("median(%v) = %v, want %v", tt.values, got, tt.want)
		}
	}
}

// runs returns the output of a number of runs of one benchmark on two caches,
// with the given throughput and share of hits for each of them in each run.
func runs(fast, slow, fastHits, slowHits []float64) string {
	var b strings.Builder
	for i := range fast {
		fmt.Fprintf(&b, "BenchmarkGet/cache=fast-4  100  1.0 ns/op  %v Mops/s  %v hit%%\n", fast[i], fastHits[i])
		fmt.Fprintf(&b, "BenchmarkGet/cache=slow-4  100  1.0 ns/op  %v Mops/s  %v hit%%\n", slow[i], slowHits[i])
	}
	return b.String()
}

func TestWinner(t *testing.T) {
	t.Parallel()

	all := []float64{100, 100, 100}
	for _, tt := range []struct {
		name       string
		fast, slow []float64
		want       string
	}{
		{"three runs apart", []float64{50, 60, 55}, []float64{40, 45, 49}, "| 4 | **55.0** (50.0–60.0) | 45.0 (40.0–49.0) |"},
		{"ranges that touch", []float64{50, 60, 55}, []float64{40, 45, 50}, "| 4 | 55.0 (50.0–60.0) | 45.0 (40.0–50.0) |"},
		{"ranges that overlap", []float64{50, 60, 55}, []float64{40, 45, 58}, "| 4 | 55.0 (50.0–60.0) | 45.0 (40.0–58.0) |"},
		{"the same every time", []float64{50, 50, 50}, []float64{40, 40, 40}, "| 4 | **50.0** | 40.0 |"},
		{"two runs", []float64{50, 60}, []float64{10, 11}, "| 4 | 55.0 (50.0–60.0) | 10.5 (10.0–11.0) |"},
	} {
		tables, err := parse(strings.NewReader(runs(tt.fast, tt.slow, all[:len(tt.fast)], all[:len(tt.fast)])))
		if err != nil {
			t.Fatal(err)
		}
		if got := render(tables); !strings.Contains(got, tt.want+"\n") {
			t.Errorf("%s: render wrote:\n%s\nwant the row %s", tt.name, got, tt.want)
		}
	}

	// A cache that is alone in a row has beaten nobody.
	tables, err := parse(strings.NewReader(strings.Repeat("BenchmarkSet/cache=only-2  100  1.0 ns/op  7.0 Mops/s\n", 5)))
	if err != nil {
		t.Fatal(err)
	}
	if got := render(tables); !strings.Contains(got, "| 2 | 7.0 |\n") {
		t.Errorf("render of a single cache wrote:\n%s", got)
	}
}

func TestHits(t *testing.T) {
	t.Parallel()

	speed := []float64{50, 50, 50}
	for _, tt := range []struct {
		name               string
		fastHits, slowHits []float64
		want               string
	}{
		{"all of them", []float64{100, 100, 100}, []float64{100, 99.96, 100}, "\nEvery read was answered, by every cache.\n"},
		{"one run short", []float64{100, 100, 100}, []float64{100, 99.9, 100}, "\n| 4 | 100.0 | 100.0 |\n"},
		{"some", []float64{90, 91, 92}, []float64{80, 82, 81}, "\n| 4 | 91.0 | 81.0 |\n"},
	} {
		tables, err := parse(strings.NewReader(runs(speed, speed, tt.fastHits, tt.slowHits)))
		if err != nil {
			t.Fatal(err)
		}
		if got := render(tables); !strings.Contains(got, tt.want) {
			t.Errorf("%s: render wrote:\n%s\nwant %q in it", tt.name, got, tt.want)
		}
	}
}
