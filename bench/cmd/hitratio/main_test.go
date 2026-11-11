package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/floatdrop/evict/bench/dataset"
	"github.com/floatdrop/evict/bench/report"
	"github.com/floatdrop/evict/bench/trace"
)

// read returns the content of a file that a run has written.
func read(t *testing.T, dir, name string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// The tests replay a trace that is made up on the spot and short, so that they
// need no download and little time.
func TestMain(m *testing.M) {
	catalog = func() []dataset.Dataset {
		return []dataset.Dataset{dataset.Generated(
			"small", "Small", "a few requests with a skew", "made up",
			[]int{100, 1000, 4000},
			func() []uint64 { return trace.Zipf(60_000, 1.0001, 10, 20_000) },
		)}
	}
	os.Exit(m.Run())
}

func TestRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	args := []string{"-caches", "evict/lru,evict/sieve@4,otter,optimal", "-runs", "3", "-out", dir, "-dir", t.TempDir()}
	if err := run(t.Context(), args); err != nil {
		t.Fatal(err)
	}

	md := read(t, dir, "hitratio.md")
	for _, want := range []string{
		"# Hit ratios\n",
		"the medians of 3 replays",
		"in this run by up to ",
		"Produced by `go run ./cmd/hitratio -caches evict/lru,evict/sieve@4,otter,optimal -runs 3 ",
		"## At a glance\n",
		"| trace | evict/lru | evict/sieve@4 | otter | optimal |\n",
		"\n| Small | ",
		"## Trace by trace\n",
		"### Small\n",
		"A few requests with a skew: 60,000 requests for ",
		"![Hit ratios on the Small trace by cache capacity](hitratio-small.svg)",
		// The caches that vary have a column for their range.
		"| capacity | evict/lru | evict/sieve@4 | evict/sieve@4, range | otter | otter, range | optimal |\n",
		"\n| 100 | ",
		"\n| 4,000 | ",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("hitratio.md lacks %q:\n%s", want, md)
		}
	}

	csv := read(t, dir, "hitratio.csv")
	if !strings.HasPrefix(csv, "dataset,capacity,cache,requests,hits,hit_ratio,min_hit_ratio,max_hit_ratio\nsmall,100,evict/lru,60000,") {
		t.Errorf("hitratio.csv begins:\n%.300s", csv)
	}
	if lines := strings.Count(csv, "\n"); lines != 1+3*4 {
		t.Errorf("hitratio.csv has %d lines, want a header and one for each of 4 caches at 3 capacities", lines)
	}

	if svg := read(t, dir, "hitratio-small.svg"); !strings.Contains(svg, "<svg ") || !strings.Contains(svg, ">evict/sieve@4</text>") {
		t.Errorf("hitratio-small.svg is not a chart of the caches:\n%.300s", svg)
	}

	// A cache that evicts the same way every time scores the same again.
	again := t.TempDir()
	if err := run(t.Context(), []string{"-caches", "evict/lru,evict/s3fifo,golang-lru/2q,optimal", "-out", again}); err != nil {
		t.Fatal(err)
	}
	first := read(t, again, "hitratio.csv")
	if err := run(t.Context(), []string{"-caches", "evict/lru,evict/s3fifo,golang-lru/2q,optimal", "-out", again}); err != nil {
		t.Fatal(err)
	}
	if second := read(t, again, "hitratio.csv"); second != first {
		t.Errorf("two runs differ:\n%s\n%s", first, second)
	}
	for line := range strings.Lines(first) {
		if fields := strings.Split(strings.TrimSpace(line), ","); fields[0] == "small" && fields[6] != fields[7] {
			t.Errorf("a cache that does not vary has a range: %s", line)
		}
	}
}

// TestSharding is not parallel: the report is made with a number of CPUs of
// its own choosing.
func TestSharding(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))

	dir := t.TempDir()
	if err := run(t.Context(), []string{"-sharding", "-runs", "3", "-out", dir, "-dir", t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if got := runtime.GOMAXPROCS(0); got != sharedCPUs {
		t.Errorf("the caches were split for %d CPUs, want %d", got, sharedCPUs)
	}

	md := read(t, dir, "sharding.md")
	for _, want := range []string{
		"# Hit ratios and shards\n",
		"as on a machine with 8 CPUs",
		"up to 64.",
		"### Small\n",
		"| capacity | evict/lru | evict/twoq | evict/sieve | evict/s3fifo | evict/tinylfu |\n",
		// A cache of 100 entries is in one piece either way. The last one
		// is not the same on every replay even so.
		"| 100 | 0.00 | 0.00 | 0.00 | 0.00 | ",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("sharding.md lacks %q:\n%s", want, md)
		}
	}
	if strings.Contains(md, "![") {
		t.Error("sharding.md refers to a chart, and there is none")
	}
	// From 512 entries on there are shards, and they leave a mark.
	if !strings.Contains(md, " to ") {
		t.Errorf("no cache varies in shards:\n%s", md)
	}
	if csv := read(t, dir, "sharding.csv"); !strings.Contains(csv, "\nsmall,4000,evict/s3fifo in shards,60000,") {
		t.Errorf("sharding.csv lacks the caches in shards:\n%.400s", csv)
	}
}

func TestRunErrors(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"-datasets", "nope"}, `there is no dataset "nope"`},
		{[]string{"-caches", "nope"}, `there is no cache "nope"`},
		{[]string{"-caches", "otter@4"}, `"otter@4" does not name a cache with a number of shards`},
		{[]string{"-caches", "evict/lru@0"}, "does not name a cache with a number of shards"},
		{[]string{"-caches", "evict/lru@x"}, "does not name a cache with a number of shards"},
		{[]string{"-caches", "evict/lru@3"}, "a number of shards that is a power of two"},
		{[]string{"-out", "/dev/null/nowhere"}, "not a directory"},
		{[]string{"-runs", "0"}, "-runs is 0"},
		{[]string{"-sharding", "-caches", "otter"}, "-sharding picks the caches itself"},
		{[]string{"-caches", "evict/lru@1,evict/lru@2,evict/lru@4,evict/lru@8,evict/lru@16,evict/lru@32,evict/lru@64,evict/lru@128,evict/lru@256"}, "9 caches are more than the 8 that a chart can tell apart"},
		{[]string{"-no-such-flag"}, "flag provided but not defined"},
	} {
		args := append([]string{"-datasets", "small", "-out", t.TempDir()}, tt.args...)
		if err := run(t.Context(), args); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("run(%q) = %v, want an error with %q", tt.args, err, tt.want)
		}
	}

	for _, args := range [][]string{{"-help"}, {"-list"}} {
		if err := run(t.Context(), args); err != nil {
			t.Errorf("run(%q): %v", args, err)
		}
	}
}

func TestWidest(t *testing.T) {
	t.Parallel()

	tables := []report.Table{
		{
			Title: "First", Requests: 1000, Caches: []string{"steady", "dice"},
			Rows: []report.Row{
				{Capacity: 10, Cells: []report.Cell{{Hits: 5, Min: 5, Max: 5}, {Hits: 50, Min: 40, Max: 61}}},
				{Capacity: 2500, Cells: []report.Cell{{Hits: 5, Min: 5, Max: 5}, {Hits: 50, Min: 20, Max: 68}}},
			},
		},
		{
			Title: "Second", Requests: 100, Caches: []string{"steady", "dice"},
			Rows: []report.Row{{Capacity: 10, Cells: []report.Cell{{Hits: 5, Min: 5, Max: 5}, {Hits: 50, Min: 49, Max: 51}}}},
		},
	}
	if got, want := widest(tables), "by up to 4.8 percentage points (dice on First with 2,500 entries)"; got != want {
		t.Errorf("widest = %q, want %q", got, want)
	}
	if got := widest(tables[1:]); got != "by up to 2.0 percentage points (dice on Second with 10 entries)" {
		t.Errorf("widest of the second table = %q", got)
	}
	tables[1].Rows[0].Cells[1] = report.Cell{Hits: 50, Min: 50, Max: 50}
	if got := widest(tables[1:]); got != "by nothing" {
		t.Errorf("widest without a cache that varies = %q", got)
	}
}

func TestOutside(t *testing.T) {
	t.Parallel()

	// One replay says nothing about the range, and two leave a third of the
	// others below them and a third above.
	for runs, want := range map[int]int{1: 100, 2: 67, 11: 17, 39: 5, 199: 1} {
		if got := outside(runs); got != want {
			t.Errorf("outside(%d) = %d, want %d", runs, got, want)
		}
	}
}

func TestSize(t *testing.T) {
	t.Parallel()

	for bytes, want := range map[int64]string{0: "generated", 5770: "6 kB", 822_156: "822 kB", 50_020_220: "50.0 MB"} {
		if got := size(bytes); got != want {
			t.Errorf("size(%d) = %q, want %q", bytes, got, want)
		}
	}
}
