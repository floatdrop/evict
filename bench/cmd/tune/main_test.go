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
	"github.com/floatdrop/evict/s3fifo"
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

// TestSettingsReachTheCache checks that every part of a setting gets to the
// cache as the option that it stands for.
func TestSettingsReachTheCache(t *testing.T) {
	t.Parallel()

	const capacity = 100
	keys := requests()
	hits := func(options ...s3fifo.Option) int {
		return replay.Hits(tuned{s3fifo.New[uint64, uint64](capacity, append(options, s3fifo.WithShards(1))...)}, keys)
	}
	plain := hits()

	for _, tt := range []struct {
		setting setting
		option  s3fifo.Option
	}{
		{setting{0.3, 0.9, 2, 1}, s3fifo.WithSmallRatio(0.3)},
		{setting{0.1, 6, 2, 1}, s3fifo.WithGhostRatio(6)},
		{setting{0.1, 0.9, 1, 1}, s3fifo.WithPromotionReads(1)},
		{setting{0.1, 0.9, 2, 2}, s3fifo.WithGhostReturns(2)},
	} {
		got, want := replay.Hits(tt.setting.cache(capacity), keys), hits(tt.option)
		if got != want {
			t.Errorf("%v: answered %d requests, a cache with that one option answers %d", tt.setting, got, want)
		}
		// The check is only worth something if the option makes a difference.
		if want == plain {
			t.Errorf("%v: the option changes nothing about the %d requests that the cache answers as it is", tt.setting, plain)
		}
	}
}

// TestReport checks what the report makes of results: the averages, which
// settings it calls the best, and which one it names when several are.
func TestReport(t *testing.T) {
	t.Parallel()

	a, b := setting{0.05, 0.9, 2, 1}, setting{0.2, 3, 1, 2}
	settings := []setting{standard, a, b}
	results := []result{
		{
			workload: workload{name: "x", title: "X", capacities: []int{10, 20, 30}},
			requests: 1000,
			// One setting is the best at 10, another at 20, and at 30
			// the two are as good as each other.
			hits: [][]int{{100, 350, 300}, {500, 400, 600}, {700, 800, 800}},
		},
		{
			workload: workload{name: "y", title: "Y", capacities: []int{5}},
			requests: 2000,
			// Nothing beats the standard setting.
			hits: [][]int{{1000, 1000, 400}},
		},
	}

	var out strings.Builder
	if err := markdown(&out, "go run ./cmd/tune", settings, results); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"Here it is replayed with 3 settings:",
		"| trace | as it is | one setting for all | best setting for the trace | best setting for each capacity | the best setting for the trace is |",
		// 1300, 1550, 1700 and 350+600+800 of 3000 requests.
		"| X | 43.33 | 51.67 | 56.67 | 58.33 | " + b.String() + " |",
		"| Y | 50.00 | 50.00 | 50.00 | 50.00 | " + standard.String() + " |",
		"| **mean** | 46.67 | 50.83 | 53.33 | 54.17 | |",
		// The best for both together is the best for neither.
		"The one setting that does best for all of these traces together is: " + a.String() + ".",
		"### X\n\n1,000 requests.",
		"| 10 | 10.00 | 35.00 | " + a.String() + " |",
		"| 20 | 50.00 | 60.00 | " + b.String() + " |",
		// Of two that are as good, the one that comes first in the grid.
		"| 30 | 70.00 | 80.00 | " + a.String() + " |",
		"### Y\n\n2,000 requests.",
		// And the standard one before any other.
		"| 5 | 50.00 | 50.00 | " + standard.String() + " |",
	} {
		if !strings.Contains(out.String(), line) {
			t.Errorf("the report lacks the line %q:\n%s", line, out.String())
		}
	}

	// A report on one trace has nothing to say about all of them.
	out.Reset()
	if err := markdown(&out, "go run ./cmd/tune", settings, results[:1]); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"| trace | as it is | best setting for the trace | best setting for each capacity | the best setting for the trace is |",
		"| X | 43.33 | 56.67 | 58.33 | " + b.String() + " |",
	} {
		if !strings.Contains(out.String(), line) {
			t.Errorf("the report on one trace lacks the line %q:\n%s", line, out.String())
		}
	}
	for _, not := range []string{"one setting for all", "**mean**", "does best for all"} {
		if strings.Contains(out.String(), not) {
			t.Errorf("the report on one trace has %q in it:\n%s", not, out.String())
		}
	}

	// The table of everything has a line for every setting at every capacity.
	out.Reset()
	if err := csv(&out, settings, results); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1+3*3+3 || lines[0] != "dataset,capacity,small_ratio,ghost_ratio,promotion_reads,ghost_returns,requests,hits" ||
		lines[5] != "x,20,0.05,0.9,2,1,1000,400" || lines[12] != "y,5,0.2,3,1,2,2000,400" {
		t.Errorf("the table is:\n%s", out.String())
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
		{[]string{file}, `there is nothing to do with "` + file + `"`},
		{[]string{"-trace", file, "-capacities", "10", "-datasets", "small"}, "-datasets and -trace do not go together"},
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
