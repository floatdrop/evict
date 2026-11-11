package report

import (
	"fmt"
	"html"
	"math"
	"strconv"
	"strings"
)

// Chart is a line chart: a line for each series over positions that all of
// them share and that have an order, such as the capacities of a cache.
type Chart struct {
	// Title names what was measured, and Unit says in what.
	Title, Unit string
	// XTitle says what the positions are, and X labels each of them.
	XTitle string
	X      []string

	Series []Series
}

// Series is one line of a [Chart]. The series with the name [Bound] is drawn
// as a thin gray line without markers: it is there to compare the others
// with.
type Series struct {
	Name string
	// Values holds a value for every position of the chart, or NaN where
	// there is none.
	Values []float64
}

// reference is the slot of a series that is drawn as a reference line.
const reference = -1

// The colors and shapes of the series. A series keeps its color and its shape
// wherever it appears, so that a reader who has learned them once does not
// have to learn them again; a name that is not listed takes the first ones
// that are free.
//
// The colors are the categorical palette of the data visualization guidelines
// this was built to, in the order that keeps neighbors apart for readers with
// impaired color vision. The shapes are there so that color is never the only
// thing to tell two lines apart.
var (
	slots = map[string]int{
		"evict/lru":     0,
		"evict/twoq":    1,
		"evict/sieve":   2,
		"evict/s3fifo":  3,
		"otter":         4,
		"golang-lru":    5,
		"golang-lru/2q": 6,
	}
	light = [...]string{"#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"}
	dark  = [...]string{"#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"}
)

// The size of a chart and of the area in it that the lines are drawn in. A
// chart whose legend takes more than one row is taller by that much.
const (
	width        = 640
	left, right  = 52, 20
	bottom       = 52
	plotW, plotH = width - left - right, 224

	margin    = 20 // Where the title, the unit and the legend begin.
	legendTop = 66 // The middle of the first row of the legend.
	legendRow = 20
)

// MaxSeries is the number of series that a chart can tell apart, not counting
// the one that is drawn as a reference line.
const MaxSeries = len(light)

// place lays out the legend: it returns where each entry begins and in which
// row it is, and the number of rows.
func (c Chart) place() (at []float64, row []int, rows int) {
	x := float64(margin)
	for _, s := range c.Series {
		w := legendWidth(s.Name)
		if x > margin && x+w > width-right {
			x = margin
			rows++
		}
		at, row = append(at, x), append(row, rows)
		x += w + 18
	}
	return at, row, rows + 1
}

// legendWidth returns the room that the legend entry of a series takes up:
// the piece of line with the marker, and the name.
func legendWidth(name string) float64 { return 30 + textWidth(name) }

// SVG renders the chart. The result follows the color scheme of whoever looks
// at it, light or dark.
func (c Chart) SVG() (string, error) {
	assigned, err := c.assign()
	if err != nil {
		return "", err
	}
	lo, hi, step := c.scale()

	legendAt, legendIn, rows := c.place()
	top := legendTop + legendRow*rows - 2
	height := top + plotH + bottom

	x := func(i int) float64 {
		if len(c.X) < 2 {
			return left + plotW/2
		}
		return left + float64(i)*plotW/float64(len(c.X)-1)
	}
	y := func(v float64) float64 {
		return float64(top) + plotH - (v-lo)/(hi-lo)*plotH
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img" aria-labelledby="title desc">`+"\n",
		width, height, width, height)
	fmt.Fprintf(&b, "<title id=\"title\">%s</title>\n", html.EscapeString(c.Title))
	fmt.Fprintf(&b, "<desc id=\"desc\">%s</desc>\n", html.EscapeString(c.describe()))
	b.WriteString(c.style(assigned))
	fmt.Fprintf(&b, `<rect class="surface" x="0.5" y="0.5" width="%d" height="%d" rx="8"/>`+"\n", width-1, height-1)

	fmt.Fprintf(&b, `<text class="title" x="%d" y="28">%s</text>`+"\n", margin, html.EscapeString(c.Title))
	fmt.Fprintf(&b, `<text class="unit" x="%d" y="46">%s</text>`+"\n", margin, html.EscapeString(c.Unit))

	// The legend: a short piece of line with the marker of the series on
	// it, and the name next to it in ordinary ink.
	for i, s := range c.Series {
		slot, at, mid := assigned[i], legendAt[i], float64(legendTop+legendRow*legendIn[i])
		fmt.Fprintf(&b, `<path class="%s" d="M%s,%s h24"/>`, lineClass(slot), num(at), num(mid))
		b.WriteString(marker(slot, at+12, mid))
		fmt.Fprintf(&b, `<text class="legend" x="%s" y="%s">%s</text>`+"\n", num(at+30), num(mid+4), html.EscapeString(s.Name))
	}

	// Grid lines and their values. The lowest one is the axis.
	for v := lo; v <= hi+step/2; v += step {
		class := "grid"
		if v == lo {
			class = "axis"
		}
		fmt.Fprintf(&b, `<path class="%s" d="M%d,%s h%d"/>`, class, left, num(y(v)), plotW)
		fmt.Fprintf(&b, `<text class="tick" x="%d" y="%s" text-anchor="end">%s</text>`+"\n", left-8, num(y(v)+4), formatTick(v, step))
	}
	for i, label := range c.X {
		fmt.Fprintf(&b, `<text class="tick" x="%s" y="%d" text-anchor="middle">%s</text>`+"\n",
			num(x(i)), top+plotH+18, html.EscapeString(label))
	}
	fmt.Fprintf(&b, `<text class="tick" x="%d" y="%d" text-anchor="middle">%s</text>`+"\n",
		left+plotW/2, height-12, html.EscapeString(c.XTitle))

	// The lines, and then the markers on top of all of them. A marker has a
	// ring in the color of the surface that sets it off from the lines it
	// sits on, and says what it stands for when it is pointed at.
	for i, s := range c.Series {
		var d strings.Builder
		pen := "M"
		for j, v := range s.Values {
			if math.IsNaN(v) {
				pen = "M"
				continue
			}
			fmt.Fprintf(&d, "%s%s,%s", pen, num(x(j)), num(y(v)))
			pen = "L"
		}
		fmt.Fprintf(&b, `<path class="%s" d="%s"/>`+"\n", lineClass(assigned[i]), d.String())
	}
	for i, s := range c.Series {
		for j, v := range s.Values {
			if math.IsNaN(v) || assigned[i] == reference {
				continue
			}
			fmt.Fprintf(&b, "<g><title>%s at %s: %s</title>%s</g>\n",
				html.EscapeString(s.Name), html.EscapeString(c.X[j]), strconv.FormatFloat(v, 'f', 2, 64),
				marker(assigned[i], x(j), y(v)))
		}
	}

	b.WriteString("</svg>\n")
	return b.String(), nil
}

// assign returns the slot, which is a color and a shape, of every series.
func (c Chart) assign() ([]int, error) {
	assigned := make([]int, len(c.Series))
	taken := map[int]bool{}
	for i, s := range c.Series {
		if len(s.Values) != len(c.X) {
			return nil, fmt.Errorf("chart %q: series %q has %d values for %d positions", c.Title, s.Name, len(s.Values), len(c.X))
		}
		const unassigned = -2
		switch slot, ok := slots[s.Name]; {
		case s.Name == Bound:
			assigned[i] = reference
		case ok:
			assigned[i] = slot
			taken[slot] = true
		default:
			assigned[i] = unassigned
		}
	}
	for i := range assigned {
		if assigned[i] >= reference {
			continue
		}
		slot := 0
		for taken[slot] {
			slot++
		}
		if slot >= len(light) {
			return nil, fmt.Errorf("chart %q has more series than there are colors to tell them apart", c.Title)
		}
		assigned[i] = slot
		taken[slot] = true
	}
	return assigned, nil
}

// scale returns the range of the vertical axis and the distance between its
// grid lines: round numbers that leave the lines a little room.
func (c Chart) scale() (lo, hi, step float64) {
	lo, hi = math.Inf(1), math.Inf(-1)
	for _, s := range c.Series {
		for _, v := range s.Values {
			if !math.IsNaN(v) {
				lo, hi = min(lo, v), max(hi, v)
			}
		}
	}
	if lo > hi {
		return 0, 1, 1 // Nothing to show.
	}
	if lo == hi {
		lo, hi = lo-1, hi+1
	}

	// About five grid lines, a 1, 2 or 5 times a power of ten apart.
	rough := (hi - lo) / 5
	magnitude := math.Pow(10, math.Floor(math.Log10(rough)))
	step = 10 * magnitude
	for _, m := range []float64{1, 2, 5} {
		if m*magnitude >= rough {
			step = m * magnitude
			break
		}
	}
	lo = math.Floor(lo/step) * step
	hi = math.Ceil(hi/step) * step
	return lo, hi, step
}

// describe puts the chart into words, for readers who cannot see it.
func (c Chart) describe() string {
	names := make([]string, len(c.Series))
	for i, s := range c.Series {
		names[i] = s.Name
	}
	return fmt.Sprintf("Line chart of %s by %s for %s. The numbers are in the table that goes with it.",
		c.Unit, c.XTitle, strings.Join(names, ", "))
}

// ink holds the colors of everything in a chart that is not data.
type ink struct {
	surface, border, primary, secondary, muted, grid, axis string
}

var (
	lightInk = ink{"#fcfcfb", "rgba(11,11,11,0.10)", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"}
	darkInk  = ink{"#1a1a19", "rgba(255,255,255,0.10)", "#f0efec", "#c3c2b7", "#898781", "#2c2c2a", "#383835"}
)

// style returns the style sheet of the chart: the colors for a light surface,
// and the ones for a dark surface for whoever prefers that. The colors are
// spelled out in both, since not everything that displays an SVG file knows
// about variables in style sheets.
func (c Chart) style(assigned []int) string {
	colors := func(b *strings.Builder, indent string, in ink, palette [8]string) {
		fmt.Fprintf(b, "%s.surface { fill: %s; stroke: %s; }\n", indent, in.surface, in.border)
		fmt.Fprintf(b, "%s.title { fill: %s; }\n", indent, in.primary)
		fmt.Fprintf(b, "%s.unit, .legend { fill: %s; }\n", indent, in.secondary)
		fmt.Fprintf(b, "%s.tick { fill: %s; }\n", indent, in.muted)
		fmt.Fprintf(b, "%s.grid { stroke: %s; }\n", indent, in.grid)
		fmt.Fprintf(b, "%s.axis { stroke: %s; }\n", indent, in.axis)
		fmt.Fprintf(b, "%s.ref { stroke: %s; }\n", indent, in.muted)
		fmt.Fprintf(b, "%s.mark { stroke: %s; }\n", indent, in.surface)
		for _, slot := range assigned {
			if slot != reference {
				fmt.Fprintf(b, "%s.s%d { stroke: %s; } .m%d { fill: %s; }\n", indent, slot, palette[slot], slot, palette[slot])
			}
		}
	}

	var b strings.Builder
	b.WriteString("<style>\n")
	b.WriteString("text { font-family: system-ui, -apple-system, \"Segoe UI\", sans-serif; }\n")
	b.WriteString(".title { font-size: 15px; font-weight: 600; }\n")
	b.WriteString(".unit, .legend { font-size: 12px; }\n")
	b.WriteString(".tick { font-size: 11px; font-variant-numeric: tabular-nums; }\n")
	b.WriteString(".grid, .axis { stroke-width: 1; }\n")
	b.WriteString(".line { fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }\n")
	b.WriteString(".ref { fill: none; stroke-width: 1.5; stroke-linejoin: round; stroke-linecap: round; }\n")
	b.WriteString(".mark { stroke-width: 2; paint-order: stroke; }\n")
	colors(&b, "", lightInk, light)
	b.WriteString("@media (prefers-color-scheme: dark) {\n")
	colors(&b, "  ", darkInk, dark)
	b.WriteString("}\n</style>\n")
	return b.String()
}

// lineClass returns the classes of the line of a slot.
func lineClass(slot int) string {
	if slot == reference {
		return "ref"
	}
	return "line s" + strconv.Itoa(slot)
}

// marker draws the shape of a slot around a point. The shapes take up about
// as much room as a circle that is eight pixels across. A reference line has
// none.
func marker(slot int, x, y float64) string {
	if slot == reference {
		return ""
	}

	path := func(points ...float64) string {
		var d strings.Builder
		for i := 0; i < len(points); i += 2 {
			pen := "L"
			if i == 0 {
				pen = "M"
			}
			fmt.Fprintf(&d, "%s%s,%s", pen, num(x+points[i]), num(y+points[i+1]))
		}
		return fmt.Sprintf(`<path class="mark m%d" d="%sZ"/>`, slot, d.String())
	}

	switch slot % 8 {
	case 0: // Circle.
		return fmt.Sprintf(`<circle class="mark m%d" cx=%q cy=%q r="4"/>`, slot, num(x), num(y))
	case 1: // Square.
		return path(-3.6, -3.6, 3.6, -3.6, 3.6, 3.6, -3.6, 3.6)
	case 2: // Triangle.
		return path(0, -4.8, 4.6, 3.4, -4.6, 3.4)
	case 3: // Diamond.
		return path(0, -5, 5, 0, 0, 5, -5, 0)
	case 4: // Triangle, pointing down.
		return path(0, 4.8, 4.6, -3.4, -4.6, -3.4)
	case 5: // Pentagon.
		return path(0, -4.6, 4.4, -1.4, 2.7, 3.7, -2.7, 3.7, -4.4, -1.4)
	case 6: // Hexagon.
		return path(-2.3, -4, 2.3, -4, 4.6, 0, 2.3, 4, -2.3, 4, -4.6, 0)
	default: // A square on its side, cut off at the corners.
		return path(-2, -4.4, 2, -4.4, 4.4, -2, 4.4, 2, 2, 4.4, -2, 4.4, -4.4, 2, -4.4, -2)
	}
}

// textWidth estimates the width of the name of a series in a legend, in
// pixels. A chart is rendered without a browser at hand to measure the text,
// so the estimate errs on the side of leaving room: it only has to keep the
// entries from running into each other and off the chart.
func textWidth(s string) float64 {
	return 7 * float64(len(s))
}

// num formats a coordinate: one decimal is more than a screen can show.
func num(v float64) string {
	return strconv.FormatFloat(math.Round(v*10)/10, 'f', -1, 64)
}

// formatTick formats the value of a grid line with as many decimals as the
// distance between the lines calls for.
func formatTick(v, step float64) string {
	decimals := 0
	if step < 1 {
		decimals = int(math.Ceil(-math.Log10(step)))
	}
	return strconv.FormatFloat(v, 'f', decimals, 64)
}

// Compact formats n the way an axis has room for: 250, 2.5K, 100K, 8M.
func Compact(n int) string {
	switch {
	case n >= 1_000_000:
		return strconv.FormatFloat(float64(n)/1e6, 'f', -1, 64) + "M"
	case n >= 1000:
		return strconv.FormatFloat(float64(n)/1e3, 'f', -1, 64) + "K"
	default:
		return strconv.Itoa(n)
	}
}
