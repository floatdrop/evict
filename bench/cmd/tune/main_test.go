package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/floatdrop/evict/bench/caches"
	"github.com/floatdrop/evict/bench/dataset"
	"github.com/floatdrop/evict/bench/replay"
	"github.com/floatdrop/evict/bench/trace"
)

// requests is the trace that the tests tune for: short, so that eighty-odd
// settings are replayed in no time, and with a skew, so that they differ.
func requests() []uint64 { return trace.Zipf(40_000, 1.0001, 10, 20_000) }

// The tests replay traces that are made up on the spot, so that they need no
// download.
func TestMain(m *testing.M) {
	catalog = func() []dataset.Dataset {
		return []dataset.Dataset{
			dataset.Generated("small", "Small", "a few requests with a skew", "made up", []int{100, 1000}, requests),
			dataset.Generated("loop", "Loop", "the same keys over and over", "made up", []int{50, 200}, func() []uint64 {
				keys := make([]uint64, 30_000)
				for i := range keys {
					keys[i] = uint64(i % 300)
				}
				return keys
			}),
		}
	}
	os.Exit(m.Run())
}

func read(t *testing.T, dir, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestGrid(t *testing.T) {
	t.Parallel()

	settings := grid()
	if len(settings) != 7*3*2*2 {
		t.Fatalf("the grid has %d settings, want %d", len(settings), 7*3*2*2)
	}
	if settings[0] != standard {
		t.Errorf("the grid begins with %v, want the standard setting", settings[0])
	}
	seen := map[setting]bool{}
	for i, s := range settings {
		if seen[s] {
			t.Errorf("the setting %v is in the grid twice", s)
		}
		seen[s] = true
		// Fewer ghosts come before more, so that they win a tie.
		if i > 0 && s.ghosts < settings[i-1].ghosts {
			t.Errorf("%v comes after %v", s, settings[i-1])
		}
	}
	if got, want := standard.String(), "small 0.1, ghosts 0.9, promotion 2, returns 1"; got != want {
		t.Errorf("the standard setting is written %q, want %q", got, want)
	}
}

// TestStandardSetting checks that the first setting of the grid is the cache
// as New makes it: what the report calls "as it is".
func TestStandardSetting(t *testing.T) {
	t.Parallel()

	keys := requests()
	plain, _ := caches.Find[uint64, uint64]("evict/s3fifo")
	for _, capacity := range []int{10, 100, 1000} {
		if got, want := replay.Hits(standard.cache(capacity), keys), replay.Hits(plain.Exact(capacity), keys); got != want {
			t.Errorf("capacity %d: the standard setting answers %d requests, the cache as it is %d", capacity, got, want)
		}
	}
	// And the others are not: they are told apart by what they answer.
	different := 0
	for _, s := range grid()[1:] {
		if replay.Hits(s.cache(100), keys) != replay.Hits(standard.cache(100), keys) {
			different++
		}
	}
	if different < len(grid())/2 {
		t.Errorf("only %d of %d settings answer the trace differently from the standard one", different, len(grid())-1)
	}
}

func TestRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := run(t.Context(), []string{"-out", dir}); err != nil {
		t.Fatal(err)
	}

	// Every setting at every capacity of every trace is in the CSV, and the
	// first one of each is the cache as it is.
	csv := strings.Split(strings.TrimSpace(read(t, dir, "tuning.csv")), "\n")
	if csv[0] != "dataset,capacity,small_ratio,ghost_ratio,promotion_reads,ghost_returns,requests,hits" {
		t.Errorf("tuning.csv begins with %q", csv[0])
	}
	if got, want := len(csv)-1, 2*2*len(grid()); got != want {
		t.Fatalf("tuning.csv has %d rows, want %d", got, want)
	}
	keys := requests()
	plain, _ := caches.Find[uint64, uint64]("evict/s3fifo")
	best := map[string]int{}
	for _, line := range csv[1:] {
		f := strings.Split(line, ",")
		hits, err := strconv.Atoi(f[7])
		if err != nil {
			t.Fatal(line)
		}
		if f[0] == "small" && f[2] == "0.1" && f[3] == "0.9" && f[4] == "2" && f[5] == "1" {
			capacity, _ := strconv.Atoi(f[1])
			if want := replay.Hits(plain.Exact(capacity), keys); hits != want || f[6] != "40000" {
				t.Errorf("%s: want %d hits of 40000 requests", line, want)
			}
		}
		best[f[0]+"@"+f[1]] = max(best[f[0]+"@"+f[1]], hits)
	}

	md := read(t, dir, "tuning.md")
	for _, want := range []string{
		"# Tuning S3-FIFO\n",
		"with 84 settings",
		"Produced by `go run ./cmd/tune -out " + dir + "` with go",
		"| trace | as it is | one setting for all | best setting for the trace | best setting for each capacity | the best setting for the trace is |\n",
		"\n| Small | ", "\n| Loop | ", "\n| **mean** | ",
		"\nThe one setting that does best for all of these traces together is: small ",
		"\n### Small\n\n40,000 requests.\n\n| capacity | as it is | best | with |\n",
		"\n### Loop\n\n30,000 requests.\n",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("tuning.md lacks %q:\n%s", want, md)
		}
	}
	// The best of a capacity in the report is the best in the CSV.
	for at, hits := range best {
		name, capacity, _ := strings.Cut(at, "@")
		requests := map[string]int{"small": 40_000, "loop": 30_000}[name]
		n, _ := strconv.Atoi(capacity)
		cell := fmt.Sprintf("| %s | ", thousands(n))
		want := fmt.Sprintf(" | %.2f | small ", 100*float64(hits)/float64(requests))
		found := false
		for line := range strings.Lines(md) {
			found = found || strings.HasPrefix(line, cell) && strings.Contains(line, want)
		}
		if !found {
			t.Errorf("tuning.md has no row for %s with the best hit ratio%s", at, want)
		}
	}

	// The settings are there to be told apart: on a loop that is larger than
	// the cache, the standard one is not the best.
	_, loop, _ := strings.Cut(md, "\n### Loop")
	if strings.Count(loop, standard.String()) == 2 {
		t.Errorf("the standard setting is the best for a loop at both capacities:\n%s", loop)
	}
}

func thousands(n int) string {
	s := strconv.Itoa(n)
	if len(s) > 3 {
		s = s[:len(s)-3] + "," + s[len(s)-3:]
	}
	return s
}

// TestRecording tunes for a file of requests, plain and compressed.
func TestRecording(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	for _, key := range requests() {
		fmt.Fprintln(&b, key)
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "requests.txt")
	if err := os.WriteFile(file, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "out")
	if err := run(t.Context(), []string{"-trace", file, "-capacities", "1000, 100,100", "-out", out}); err != nil {
		t.Fatal(err)
	}
	md := read(t, out, "tuning.md")
	for _, want := range []string{
		"| trace | as it is | best setting for the trace | best setting for each capacity | the best setting for the trace is |\n",
		"\n| requests.txt | ",
		"\n### requests.txt\n\n40,000 requests.\n",
		"\n| 100 | ", "\n| 1,000 | ",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("tuning.md lacks %q:\n%s", want, md)
		}
	}
	if strings.Contains(md, "one setting for all") || strings.Contains(md, "**mean**") {
		t.Errorf("the report on one trace compares it with others:\n%s", md)
	}
	if rows := strings.Count(read(t, out, "tuning.csv"), "\n"); rows != 1+2*len(grid()) {
		t.Errorf("tuning.csv has %d lines, want a header and %d rows", rows, 2*len(grid()))
	}

	// The same answers as from the generated trace.
	first := read(t, out, "tuning.csv")
	other := filepath.Join(dir, "datasets")
	if err := run(t.Context(), []string{"-datasets", "small", "-out", other}); err != nil {
		t.Fatal(err)
	}
	if got := strings.ReplaceAll(read(t, other, "tuning.csv"), "small,", "requests.txt,"); got != first {
		t.Errorf("a trace from a file is tuned for differently than the same trace made up:\n%.300s\n%.300s", first, got)
	}
}

func TestRunErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	file := filepath.Join(dir, "requests.txt")
	if err := os.WriteFile(file, []byte("1\n2\n1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"-datasets", "nothing"}, `there is no dataset "nothing"`},
		{[]string{"-capacities", "10"}, "-capacities goes with -trace"},
		{[]string{"-trace", file}, "-trace needs -capacities"},
		{[]string{"-trace", file, "-capacities", "10", "-format", "csv"}, `there is no format "csv"`},
		{[]string{"-trace", file, "-capacities", "10,x"}, `-capacities has "x"`},
		{[]string{"-trace", file, "-capacities", "0"}, `-capacities has "0"`},
		{[]string{"-trace", filepath.Join(dir, "missing.txt"), "-capacities", "10"}, "no such file"},
		{[]string{"-trace", empty, "-capacities", "10"}, "holds no requests"},
		{[]string{"-no-such-flag"}, "flag provided but not defined"},
	} {
		err := run(t.Context(), append(tt.args, "-out", filepath.Join(dir, "out")))
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("run(%q) = %v, want an error with %q", tt.args, err, tt.want)
		}
	}
	if err := run(t.Context(), []string{"-h"}); err != nil {
		t.Errorf("run(-h) = %v", err)
	}
}
