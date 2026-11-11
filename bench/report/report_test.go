package report

import (
	"strings"
	"testing"
)

// exact returns the cells of caches that score the same on every replay.
func exact(hits ...int) []Cell {
	cells := make([]Cell, len(hits))
	for i, h := range hits {
		cells[i] = Cell{h, h, h}
	}
	return cells
}

var tables = []Table{
	{
		Name:     "small",
		Title:    "Small",
		Workload: "a handful of requests",
		Origin:   "made up",
		Requests: 2000,
		Unique:   1500,
		Caches:   []string{"a", "b", Bound},
		Rows: []Row{
			{Capacity: 10, Cells: exact(100, 250, 400)},
			{Capacity: 1000, Cells: exact(500, 500, 2000)},
		},
	},
	{
		Name:     "varying",
		Title:    "Varying",
		Workload: "one cache that varies",
		Origin:   "made up",
		Requests: 1000,
		Unique:   10,
		Caches:   []string{"a", "dice", Bound},
		Rows: []Row{
			// The dice win on every throw.
			{Capacity: 1, Cells: []Cell{{100, 100, 100}, {150, 101, 180}, {900, 900, 900}}},
			// Their worst throw ties with a.
			{Capacity: 2, Cells: []Cell{{100, 100, 100}, {150, 100, 180}, {900, 900, 900}}},
			// a wins over their best.
			{Capacity: 3, Cells: []Cell{{200, 200, 200}, {150, 100, 199}, {900, 900, 900}}},
			// Their best throw ties with a.
			{Capacity: 4, Cells: []Cell{{200, 200, 200}, {150, 100, 200}, {900, 900, 900}}},
		},
	},
	{
		Name:   "empty",
		Title:  "Empty",
		Caches: []string{"a"},
		Rows:   []Row{{Capacity: 1, Cells: exact(0)}},
	},
}

func TestMarkdown(t *testing.T) {
	t.Parallel()

	const want = `### Small

A handful of requests: 2,000 requests for 1,500 keys.
Trace: made up.

| capacity | a | b | optimal |
|---:|---:|---:|---:|
| 10 | 5.00 | **12.50** | 20.00 |
| 1,000 | 25.00 | 25.00 | 100.00 |

### Varying

One cache that varies: 1,000 requests for 10 keys.
Trace: made up.

| capacity | a | dice | dice, range | optimal |
|---:|---:|---:|---:|---:|
| 1 | 10.00 | **15.00** | 10.10–18.00 | 90.00 |
| 2 | 10.00 | 15.00 | 10.00–18.00 | 90.00 |
| 3 | **20.00** | 15.00 | 10.00–19.90 | 90.00 |
| 4 | 20.00 | 15.00 | 10.00–20.00 | 90.00 |

### Empty

: 0 requests for 0 keys.
Trace: .

| capacity | a |
|---:|---:|
| 1 | - |
`
	// A cache that is alone in a table wins nothing, however well it does.
	alone := Table{Title: "Alone", Requests: 10, Caches: []string{"a", Bound}, Rows: []Row{{Capacity: 1, Cells: exact(5, 9)}}}
	var single strings.Builder
	if err := Markdown(&single, []Table{alone}); err != nil || !strings.HasSuffix(single.String(), "| 1 | 50.00 | 90.00 |\n") {
		t.Errorf("Markdown of a table with one cache wrote:\n%s\n%v", single.String(), err)
	}

	var b strings.Builder
	if err := Markdown(&b, tables); err != nil {
		t.Fatal(err)
	}
	if got := b.String(); got != want {
		t.Errorf("Markdown wrote:\n%s\nwant:\n%s", got, want)
	}
}

func TestSummary(t *testing.T) {
	t.Parallel()

	// Two traces with the same caches. On the first, the dice average 15,
	// 15, 15 and 15 percent, between 10 and 19.5, against 10, 10, 20 and 20
	// for a: nobody wins. On the second, b wins by a hair.
	second := Table{
		Name:     "second",
		Title:    "Second",
		Requests: 200,
		Caches:   []string{"a", "dice", Bound},
		Rows: []Row{
			{Capacity: 1, Cells: []Cell{{50, 50, 50}, {60, 52, 70}, {100, 100, 100}}},
			{Capacity: 2, Cells: []Cell{{51, 51, 51}, {60, 50, 70}, {120, 120, 120}}},
		},
	}
	const want = `| trace | a | dice | optimal |
|:---|---:|---:|---:|
| Varying | 15.00 | 15.00 | 90.00 |
| Second | 25.25 | **30.00** | 55.00 |
`
	var b strings.Builder
	if err := Summary(&b, []Table{tables[1], second}); err != nil {
		t.Fatal(err)
	}
	if got := b.String(); got != want {
		t.Errorf("Summary wrote:\n%s\nwant:\n%s", got, want)
	}

	// Tables with different caches do not go into one.
	if err := Summary(&b, tables[:2]); err == nil || !strings.Contains(err.Error(), "other caches") {
		t.Errorf("Summary of tables with different caches: %v", err)
	}
	b.Reset()
	if err := Summary(&b, nil); err != nil || b.Len() != 0 {
		t.Errorf("Summary of no tables wrote %q, %v", b.String(), err)
	}
	// A trace without requests has no ratios.
	b.Reset()
	if err := Summary(&b, tables[2:]); err != nil || !strings.HasSuffix(b.String(), "| Empty | - |\n") {
		t.Errorf("Summary of an empty trace wrote %q, %v", b.String(), err)
	}
}

func TestDifferences(t *testing.T) {
	t.Parallel()

	pairs := []Table{{
		Name:     "pairs",
		Title:    "Pairs",
		Workload: "two caches and a variant of each",
		Origin:   "made up",
		Requests: 10_000,
		Unique:   100,
		Caches:   []string{"a", "a in shards", "b", "b in shards"},
		Rows: []Row{
			{Capacity: 10, Cells: []Cell{{1000, 1000, 1000}, {1000, 1000, 1000}, {2000, 2000, 2000}, {2000, 2000, 2000}}},
			{Capacity: 1000, Cells: []Cell{{1000, 1000, 1000}, {1012, 995, 1031}, {2000, 2000, 2000}, {1950, 1900, 1999}}},
			{Capacity: 2000, Cells: []Cell{{1000, 1000, 1000}, {1250, 1250, 1250}, {2000, 2000, 2000}, {2000, 2000, 2002}}},
			// A range that the two decimals do not show is left out.
			{Capacity: 3000, Cells: []Cell{{5000, 5000, 5000}, {4992, 4991, 4992}, {2000, 2000, 2000}, {2000, 2000, 2000}}},
		},
	}}
	const want = `### Pairs

Two caches and a variant of each: 10,000 requests for 100 keys.
Trace: made up.

| capacity | a | b |
|---:|---:|---:|
| 10 | 0.00 | 0.00 |
| 1,000 | +0.12 (−0.05 to +0.31) | −0.50 (−1.00 to −0.01) |
| 2,000 | +2.50 | 0.00 (0.00 to +0.02) |
| 3,000 | −0.08 (−0.09 to −0.08) | 0.00 |
`
	var b strings.Builder
	if err := Differences(&b, pairs); err != nil {
		t.Fatal(err)
	}
	if got := b.String(); got != want {
		t.Errorf("Differences wrote:\n%s\nwant:\n%s", got, want)
	}

	// Without a variant for every cache there is nothing to compare.
	if err := Differences(&b, tables[:1]); err == nil || !strings.Contains(err.Error(), "do not pair up") {
		t.Errorf("Differences of a table with three caches: %v", err)
	}
}

func TestSummarize(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		hits []int
		want Cell
	}{
		{[]int{7}, Cell{7, 7, 7}},
		{[]int{9, 1, 5}, Cell{5, 1, 9}},
		{[]int{4, 4, 4, 4}, Cell{4, 4, 4}},
		{[]int{8, 2, 6, 4, 10}, Cell{6, 2, 10}},
		{[]int{3, 1, 2, 5}, Cell{2, 1, 5}}, // Halfway between the two in the middle, rounded down.
		{[]int{8, 2}, Cell{5, 2, 8}},
	} {
		if got := Summarize(tt.hits); got != tt.want {
			t.Errorf("Summarize = %+v, want %+v", got, tt.want)
		}
	}
}

func TestCSV(t *testing.T) {
	t.Parallel()

	const want = `dataset,capacity,cache,requests,hits,hit_ratio,min_hit_ratio,max_hit_ratio
small,10,a,2000,100,0.050000,0.050000,0.050000
small,10,b,2000,250,0.125000,0.125000,0.125000
small,10,optimal,2000,400,0.200000,0.200000,0.200000
small,1000,a,2000,500,0.250000,0.250000,0.250000
small,1000,b,2000,500,0.250000,0.250000,0.250000
small,1000,optimal,2000,2000,1.000000,1.000000,1.000000
varying,1,a,1000,100,0.100000,0.100000,0.100000
varying,1,dice,1000,150,0.150000,0.101000,0.180000
`
	var b strings.Builder
	if err := CSV(&b, tables); err != nil {
		t.Fatal(err)
	}
	if got := b.String(); !strings.HasPrefix(got, want) || !strings.HasSuffix(got, "empty,1,a,0,0,0.000000,0.000000,0.000000\n") {
		t.Errorf("CSV wrote:\n%s\nwant it to begin with:\n%s", got, want)
	}
}

func TestThousands(t *testing.T) {
	t.Parallel()

	for n, want := range map[int]string{
		0: "0", 7: "7", 999: "999", 1000: "1,000", 12345: "12,345", 123456: "123,456",
		1234567: "1,234,567", -1: "-1", -1000: "-1,000", -123456: "-123,456",
	} {
		if got := Thousands(n); got != want {
			t.Errorf("Thousands(%d) = %q, want %q", n, got, want)
		}
	}
}
