package report

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"testing"
)

// parse checks that the chart is well-formed XML and returns how often each
// element with a class occurs, by class.
func parse(t *testing.T, svg string) map[string]int {
	t.Helper()

	classes := map[string]int{}
	decoder := xml.NewDecoder(strings.NewReader(svg))
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return classes
		}
		if err != nil {
			t.Fatalf("the chart is not well-formed: %v\n%s", err, svg)
		}
		if start, ok := token.(xml.StartElement); ok {
			for _, attr := range start.Attr {
				if attr.Name.Local == "class" {
					for class := range strings.FieldsSeq(attr.Value) {
						classes[class]++
					}
				}
			}
		}
	}
}

func TestChartSVG(t *testing.T) {
	t.Parallel()

	c := Chart{
		Title:  `S3 & "friends" <sic>`,
		Unit:   "Hit ratio, %",
		XTitle: "Cache capacity, entries",
		X:      []string{"100K", "200K", "300K"},
		Series: []Series{
			{Name: "evict/sieve", Values: []float64{10, 20, 30}},
			{Name: "otter", Values: []float64{12, math.NaN(), 31}},
			{Name: "something new", Values: []float64{5, 6, 7}},
			{Name: Bound, Values: []float64{20, 30, 40}},
		},
	}
	svg, err := c.SVG()
	if err != nil {
		t.Fatal(err)
	}
	classes := parse(t, svg)

	// A series keeps its color and shape: sieve has the third, otter the
	// fifth, and a newcomer gets the first that is free. Each has a line
	// in the legend and one in the plot, and a marker in the legend and
	// one for every value it has.
	for class, want := range map[string]int{
		"s2": 2, "m2": 1 + 3,
		"s4": 2, "m4": 1 + 2,
		"s0": 2, "m0": 1 + 3,
		"ref":    2, // The bound is a plain gray line.
		"legend": 4,
		"title":  1,
	} {
		if classes[class] != want {
			t.Errorf("%d elements of class %q, want %d", classes[class], class, want)
		}
	}

	for _, want := range []string{
		"S3 &amp; &#34;friends&#34; &lt;sic&gt;", // Text is escaped.
		"prefers-color-scheme: dark",
		"<title>otter at 300K: 31.00</title>",
		">100K</text>", ">300K</text>",
		">0</text>", ">40</text>", // The axis covers all values in round numbers.
	} {
		if !strings.Contains(svg, want) {
			t.Errorf("the chart lacks %q", want)
		}
	}
	// The gap in otter's values breaks its line in two: the pen is set
	// down at the first value and again at the third.
	line := regexp.MustCompile(`<path class="line s4" d="(M52,[0-9.]+M620,[0-9.]+)"/>`)
	if !line.MatchString(svg) {
		t.Errorf("otter's line is not two points with a gap between them:\n%s", svg)
	}
	if strings.Contains(svg, "NaN") {
		t.Error("the chart draws a value that is not a number")
	}
}

func TestChartErrors(t *testing.T) {
	t.Parallel()

	c := Chart{Title: "short", X: []string{"1", "2"}, Series: []Series{{Name: "a", Values: []float64{1}}}}
	if _, err := c.SVG(); err == nil || !strings.Contains(err.Error(), "1 values for 2 positions") {
		t.Errorf("SVG with a series that is too short: %v", err)
	}

	c = Chart{Title: "crowded", X: []string{"1"}}
	for i := range 9 {
		c.Series = append(c.Series, Series{Name: strings.Repeat("x", i+1), Values: []float64{1}})
	}
	if _, err := c.SVG(); err == nil || !strings.Contains(err.Error(), "more series than there are colors") {
		t.Errorf("SVG with nine series: %v", err)
	}

	// Charts with next to nothing in them are drawn all the same.
	for _, c := range []Chart{
		{Title: "empty"},
		{Title: "one point", X: []string{"1"}, Series: []Series{{Name: "a", Values: []float64{3}}}},
		{Title: "no numbers", X: []string{"1"}, Series: []Series{{Name: "a", Values: []float64{math.NaN()}}}},
		{Title: "flat", X: []string{"1", "2"}, Series: []Series{{Name: "a", Values: []float64{0, 0}}}},
	} {
		svg, err := c.SVG()
		if err != nil {
			t.Errorf("%s: %v", c.Title, err)
			continue
		}
		parse(t, svg)
		if strings.Contains(svg, "NaN") || strings.Contains(svg, "Inf") {
			t.Errorf("%s: the chart has a coordinate that is not a number", c.Title)
		}
	}
}

func TestScale(t *testing.T) {
	t.Parallel()

	tests := []struct {
		values       []float64
		lo, hi, step float64
	}{
		{[]float64{11.8, 60.4}, 10, 70, 10},
		{[]float64{0.91, 57.96}, 0, 60, 20},
		{[]float64{50.3, 66.2}, 50, 70, 5},
		{[]float64{2, 425}, 0, 500, 100},
		{[]float64{0.21, 0.29}, 0.2, 0.3, 0.02},
		{[]float64{7, 7}, 6, 8, 0.5},
	}
	for _, tt := range tests {
		c := Chart{X: make([]string, len(tt.values)), Series: []Series{{Values: tt.values}}}
		lo, hi, step := c.scale()
		if math.Abs(lo-tt.lo) > 1e-9 || math.Abs(hi-tt.hi) > 1e-9 || math.Abs(step-tt.step) > 1e-9 {
			t.Errorf("scale of %v = %v to %v in steps of %v, want %v to %v in steps of %v",
				tt.values, lo, hi, step, tt.lo, tt.hi, tt.step)
		}
	}
}

func TestCompact(t *testing.T) {
	t.Parallel()

	for n, want := range map[int]string{
		0: "0", 250: "250", 999: "999", 1000: "1K", 1250: "1.25K", 2500: "2.5K",
		100_000: "100K", 125_000: "125K", 1_000_000: "1M", 8_000_000: "8M", 2_500_000: "2.5M",
	} {
		if got := Compact(n); got != want {
			t.Errorf("Compact(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestTableChart(t *testing.T) {
	t.Parallel()

	c := tables[0].Chart()
	if c.Title != "Small" || len(c.X) != 2 || c.X[1] != "1K" || len(c.Series) != 3 {
		t.Fatalf("chart = %+v", c)
	}
	if got := c.Series[1].Values; got[0] != 12.5 || got[1] != 25 {
		t.Errorf("the hit ratios of the second cache are %v, want 12.5 and 25", got)
	}

	// A cache that varies is drawn at its median.
	if got := tables[1].Chart().Series[1].Values[0]; got != 15 {
		t.Errorf("the varying cache is drawn at %v, want its median, 15", got)
	}
}

// TestLegendFits checks that a chart with as many series as there are colors
// keeps its legend within its bounds, by putting it in rows.
func TestLegendFits(t *testing.T) {
	t.Parallel()

	c := Chart{Title: "crowded", X: []string{"1", "2"}}
	for _, name := range []string{
		"evict/lru", "evict/twoq", "evict/sieve", "evict/s3fifo", "otter", "golang-lru", "golang-lru/2q", "something new", Bound,
	} {
		c.Series = append(c.Series, Series{Name: name, Values: []float64{1, 2}})
	}
	at, row, rows := c.place()
	if rows < 2 {
		t.Fatalf("the legend of nine series is in %d row", rows)
	}
	for i, s := range c.Series {
		if end := at[i] + legendWidth(s.Name); at[i] < margin || end > width-right {
			t.Errorf("the legend entry of %q runs from %v to %v, outside the chart", s.Name, at[i], end)
		}
		if i > 0 && row[i] == row[i-1] && at[i] < at[i-1]+legendWidth(c.Series[i-1].Name) {
			t.Errorf("the legend entry of %q runs into the one before it", s.Name)
		}
		if i > 0 && (row[i] < row[i-1] || row[i] > row[i-1]+1) {
			t.Errorf("the legend entry of %q is in row %d, after one in row %d", s.Name, row[i], row[i-1])
		}
	}

	svg, err := c.SVG()
	if err != nil {
		t.Fatal(err)
	}
	parse(t, svg)
	// The chart is taller by the rows that the legend takes beyond the
	// first, and the plot keeps its size.
	want := fmt.Sprintf(`viewBox="0 0 640 %d"`, 360+legendRow*(rows-1))
	if !strings.Contains(svg, want) {
		t.Errorf("the chart lacks %s", want)
	}

	// A handful of series fit in one row, as before.
	c.Series = c.Series[:5]
	if _, _, rows := c.place(); rows != 1 {
		t.Errorf("the legend of five series is in %d rows", rows)
	}
	if svg, _ := c.SVG(); !strings.Contains(svg, `viewBox="0 0 640 360"`) {
		t.Error("a chart with a legend of one row is not 360 high")
	}
}
