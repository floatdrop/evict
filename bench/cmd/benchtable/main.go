// Command benchtable turns the output of the throughput benchmarks into
// Markdown tables, one for each benchmark: a row for every number of CPUs and
// a column for every cache, with the median of the runs in millions of
// operations per second, and the slowest and the fastest of them.
//
//	go test -run '^$' -bench . -cpu 1,2,4,8 | go run ./cmd/benchtable
//
// The output of several runs of the benchmarks, one after the other, is what
// it is meant for: runs that are apart in time say more about what to expect
// than repetitions within one run.
package main

import (
	"bufio"
	"bytes"
	"cmp"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/floatdrop/evict/bench/report"
)

func main() {
	charts := flag.String("charts", "", "`directory` to write a chart of every table to; the tables then show them")
	flag.Parse()

	if err := run(*charts); err != nil {
		fmt.Fprintln(os.Stderr, "benchtable:", err)
		os.Exit(1)
	}
}

func run(charts string) error {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	tables, err := parse(bytes.NewReader(input))
	if err != nil {
		return err
	}

	if charts != "" {
		if err := os.MkdirAll(charts, 0o750); err != nil {
			return err
		}
		for _, t := range tables {
			svg, err := t.chart().SVG()
			if err != nil {
				return err
			}
			t.image = "throughput-" + slug(t.name) + ".svg"
			if err := os.WriteFile(filepath.Join(charts, t.image), []byte(svg), 0o644); err != nil { //nolint:gosec // A report is for everyone to read.
				return err
			}
		}
	}

	fmt.Print(preamble(string(input)), render(tables))
	return nil
}

// preamble returns the head of the report: what the numbers are and where
// they were measured, which the output of the benchmarks says in lines such
// as "cpu: Apple M3 Max".
func preamble(output string) string {
	setting := map[string]string{}
	for line := range strings.Lines(output) {
		if name, value, ok := strings.Cut(strings.TrimSpace(line), ": "); ok && !strings.Contains(name, " ") {
			setting[name] = value
		}
	}

	var b strings.Builder
	b.WriteString(`# Throughput

Millions of operations per second that all goroutines together get done on one cache,
with as many goroutines as CPUs. A number is the median of the runs, and the brackets
behind it hold the slowest and the fastest run. It is in bold if even the slowest run of
that cache beat the fastest of every other one; where the ranges overlap, or where there
are fewer than three runs to go by, the row has no winner. Benchmarks in which a cache can
miss have a second table, with the share of the reads that it answered.

Every cache is set up the way its library suggests, anew for every number of CPUs. See
throughput_test.go for what the benchmarks do, and throughput.txt for the output this
was made from.

`)
	fmt.Fprintf(&b, "Measured on %s (%s/%s) with %s", cmp.Or(setting["cpu"], "an unnamed CPU"),
		cmp.Or(setting["goos"], "?"), cmp.Or(setting["goarch"], "?"), cmp.Or(setting["go"], "an unnamed version of Go"))
	for _, lib := range []string{"otter", "golang-lru"} {
		if version := setting[lib]; version != "" {
			fmt.Fprintf(&b, ", %s %s", lib, version)
		}
	}
	b.WriteString(".\n\n")
	return b.String()
}

// A table holds the results of one benchmark.
type table struct {
	name   string
	image  string   // The file name of its chart, if it has one.
	caches []string // In the order of their first appearance.
	cpus   []int    // In ascending order.
	// The samples of every cache at every number of CPUs.
	cells map[cell]*samples
}

type cell struct {
	cache string
	cpus  int
}

type samples struct {
	mops []float64
	hits []float64 // Hit ratios in percent, for the benchmarks that report them.
}

// A benchmark line looks like this:
//
//	BenchmarkMixed/writes=10%/cache=evict/sieve-8   1000   12.3 ns/op   81.2 Mops/s   0 B/op
var line = regexp.MustCompile(`^Benchmark(\S+?)/cache=(\S+?)(?:-(\d+))?\s+\d+\s+(.*)$`)

func parse(r io.Reader) ([]*table, error) {
	var tables []*table
	byName := map[string]*table{}

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		m := line.FindStringSubmatch(scanner.Text())
		if m == nil {
			continue
		}
		name, cache := m[1], m[2]
		cpus := 1
		if m[3] != "" {
			cpus, _ = strconv.Atoi(m[3])
		}

		t := byName[name]
		if t == nil {
			t = &table{name: name, cells: map[cell]*samples{}}
			byName[name] = t
			tables = append(tables, t)
		}
		if !slices.Contains(t.caches, cache) {
			t.caches = append(t.caches, cache)
		}
		if !slices.Contains(t.cpus, cpus) {
			t.cpus = append(t.cpus, cpus)
			slices.Sort(t.cpus)
		}
		s := t.cells[cell{cache, cpus}]
		if s == nil {
			s = &samples{}
			t.cells[cell{cache, cpus}] = s
		}

		// The rest of the line is pairs of a value and its unit.
		fields := strings.Fields(m[4])
		for i := 0; i+1 < len(fields); i += 2 {
			value, err := strconv.ParseFloat(fields[i], 64)
			if err != nil {
				return nil, fmt.Errorf("%q: %w", scanner.Text(), err)
			}
			if math.IsNaN(value) || math.IsInf(value, 0) {
				continue // A run that was too short to be timed says nothing.
			}
			switch fields[i+1] {
			case "Mops/s":
				s.mops = append(s.mops, value)
			case "hit%":
				s.hits = append(s.hits, value)
			}
		}
	}
	return tables, scanner.Err()
}

func render(tables []*table) string {
	var b strings.Builder
	for i, t := range tables {
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "### %s\n\n", title(t.name))
		if t.image != "" {
			fmt.Fprintf(&b, "![Throughput of %s by number of CPUs](%s)\n\n", title(t.name), t.image)
		}

		t.grid(&b, func(cpus int, cache string) string {
			s := t.cells[cell{cache, cpus}]
			if s == nil || len(s.mops) == 0 {
				return ""
			}
			text := strconv.FormatFloat(median(s.mops), 'f', 1, 64)
			if t.winner(cpus) == cache {
				text = "**" + text + "**"
			}
			if lo, hi := slices.Min(s.mops), slices.Max(s.mops); lo != hi {
				text += fmt.Sprintf(" (%.1f–%.1f)", lo, hi)
			}
			return text
		})

		switch lowest, ok := t.fewestHits(); {
		case !ok:
			continue
		case lowest >= 99.95:
			b.WriteString("\nEvery read was answered, by every cache.\n")
			continue
		}
		b.WriteString("\nShare of the reads that were answered, in percent:\n\n")
		t.grid(&b, func(cpus int, cache string) string {
			s := t.cells[cell{cache, cpus}]
			if s == nil || len(s.hits) == 0 {
				return ""
			}
			return strconv.FormatFloat(median(s.hits), 'f', 1, 64)
		})
	}
	return b.String()
}

// grid writes a table with a row for every number of CPUs and a column for
// every cache.
func (t *table) grid(b *strings.Builder, text func(cpus int, cache string) string) {
	b.WriteString("| CPUs |")
	for _, cache := range t.caches {
		fmt.Fprintf(b, " %s |", cache)
	}
	b.WriteString("\n|---:|")
	b.WriteString(strings.Repeat("---:|", len(t.caches)))
	b.WriteString("\n")

	for _, cpus := range t.cpus {
		fmt.Fprintf(b, "| %d |", cpus)
		for _, cache := range t.caches {
			if cell := text(cpus, cache); cell != "" {
				fmt.Fprintf(b, " %s |", cell)
			} else {
				b.WriteString(" |")
			}
		}
		b.WriteString("\n")
	}
}

// fewestHits returns the lowest share of reads that a cache answered in any
// run of the benchmark, in percent. A run that does not say counts as one
// that answered nothing, if other runs do say. It reports false if the
// benchmark does not count the reads that are answered at all.
func (t *table) fewestHits() (lowest float64, ok bool) {
	lowest = math.Inf(1)
	silent := false
	for _, s := range t.cells {
		for _, hits := range s.hits {
			lowest, ok = min(lowest, hits), true
		}
		silent = silent || len(s.hits) < len(s.mops)
	}
	if ok && silent {
		lowest = 0
	}
	return lowest, ok
}

// minRuns is the number of runs that every cache in a row must have for one
// of them to be called the winner. With fewer, a range says little about what
// the next run would bring.
const minRuns = 3

// winner returns the cache whose slowest run at a number of CPUs was faster
// than the fastest run of every other cache, or nothing if there is none, or
// if there are too few runs or caches to tell.
func (t *table) winner(cpus int) string {
	best, floor, measured := "", 0.0, 0
	for _, cache := range t.caches {
		s := t.cells[cell{cache, cpus}]
		if s == nil || len(s.mops) == 0 {
			continue
		}
		if len(s.mops) < minRuns {
			return ""
		}
		measured++
		if slices.Min(s.mops) > floor {
			best, floor = cache, slices.Min(s.mops)
		}
	}
	if measured < 2 {
		return ""
	}
	for _, cache := range t.caches {
		if s := t.cells[cell{cache, cpus}]; cache != best && s != nil && len(s.mops) > 0 && slices.Max(s.mops) >= floor {
			return ""
		}
	}
	return best
}

// median returns the middle one of the values, or the mean of the two in the
// middle. It is zero if there are none.
func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := slices.Sorted(slices.Values(values))
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

// slug turns the name of a benchmark into a part of a file name:
// "Mixed/writes=10%" becomes "mixed-writes-10".
func slug(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case 'a' <= r && r <= 'z', '0' <= r && r <= '9':
			b.WriteRune(r)
		case b.Len() > 0 && !strings.HasSuffix(b.String(), "-"):
			b.WriteByte('-')
		}
	}
	return strings.TrimSuffix(b.String(), "-")
}

// chart returns the table as a line chart of throughput by number of CPUs.
func (t *table) chart() report.Chart {
	c := report.Chart{
		Title:  title(t.name),
		Unit:   "Million operations per second",
		XTitle: "CPUs",
	}
	for _, cpus := range t.cpus {
		c.X = append(c.X, strconv.Itoa(cpus))
	}
	for _, cache := range t.caches {
		s := report.Series{Name: cache}
		for _, cpus := range t.cpus {
			value := math.NaN()
			if cell := t.cells[cell{cache, cpus}]; cell != nil && len(cell.mops) > 0 {
				value = median(cell.mops)
			}
			s.Values = append(s.Values, value)
		}
		c.Series = append(c.Series, s)
	}
	return c
}

// title returns the name of a benchmark as a heading: "Mixed, writes=10%".
func title(name string) string {
	return strings.ReplaceAll(name, "/", ", ")
}
