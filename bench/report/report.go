// Package report writes up the results of replaying traces against caches.
package report

import (
	"encoding/csv"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// Table holds the hits that a number of caches scored on one trace at a
// number of capacities.
type Table struct {
	// Title and Workload describe the trace, and Origin credits it.
	Title, Workload, Origin string
	// Name identifies the trace in the CSV output.
	Name string

	// Requests and Unique are the number of requests in the trace and the
	// number of different keys among them.
	Requests, Unique int

	// Caches are the names of the caches, one for each column.
	Caches []string
	Rows   []Row

	// Image is the file name of a chart of the table, if there is one.
	// Markdown shows it above the table.
	Image string
}

// Chart returns the table as a line chart of hit ratios by capacity.
func (t Table) Chart() Chart {
	c := Chart{
		Title:  t.Title,
		Unit:   "Hit ratio, %",
		XTitle: "Cache capacity, entries",
	}
	for _, row := range t.Rows {
		c.X = append(c.X, Compact(row.Capacity))
	}
	for i, name := range t.Caches {
		s := Series{Name: name}
		for _, row := range t.Rows {
			s.Values = append(s.Values, t.percent(row.Cells[i].Hits))
		}
		c.Series = append(c.Series, s)
	}
	return c
}

// percent returns hits as a percentage of the requests of the table.
func (t Table) percent(hits int) float64 {
	return 100 * float64(hits) / float64(max(t.Requests, 1))
}

// varies reports whether the cache in column i scored differently from one
// replay to the next at any capacity.
func (t Table) varies(i int) bool {
	for _, row := range t.Rows {
		if row.Cells[i].Min != row.Cells[i].Max {
			return true
		}
	}
	return false
}

// Row holds the results for one capacity.
type Row struct {
	Capacity int
	// Cells are the results of each of the caches of the table.
	Cells []Cell
}

// Cell says how many requests a cache answered. A cache that does not evict
// the same way every time was replayed more than once: Hits is the median of
// its replays, and Min and Max are the worst and the best of them. For the
// others, the three are the same.
type Cell struct {
	Hits, Min, Max int
}

// Summarize returns the cell for the hits of a number of replays, of which
// there must be at least one. It sorts them. The median of an even number of
// replays is halfway between the two in the middle.
func Summarize(hits []int) Cell {
	slices.Sort(hits)
	mid := len(hits) / 2
	median := hits[mid]
	if len(hits)%2 == 0 {
		median = (hits[mid-1] + hits[mid]) / 2
	}
	return Cell{Hits: median, Min: hits[0], Max: hits[len(hits)-1]}
}

// Bound is the name of the column with the best that any cache could do. It
// is left out when the winner of a row is picked.
const Bound = "optimal"

// winner returns the column of the cache that did better in a row than every
// other one, or -1 if none did: a cache whose worst replay ties with the best
// replay of another, or falls behind it, has not won.
func (t Table) winner(row Row) int {
	best, caches := -1, 0
	for i, cell := range row.Cells {
		if t.Caches[i] == Bound {
			continue
		}
		caches++
		if best < 0 || cell.Min > row.Cells[best].Min {
			best = i
		}
	}
	if caches < 2 {
		return -1 // A cache that is on its own has beaten nobody.
	}
	for i, cell := range row.Cells {
		if i != best && t.Caches[i] != Bound && cell.Max >= row.Cells[best].Min {
			return -1
		}
	}
	return best
}

// Summary writes one table for all the traces as Markdown: a row for each of
// them, with the hit ratio of every cache averaged over the capacities at
// which the trace was replayed. The tables must have the same caches.
//
// A cache is the winner of a row if the average of its worst replays is above
// the average of the best replays of every other cache.
func Summary(w io.Writer, tables []Table) error {
	if len(tables) == 0 {
		return nil
	}
	caches := tables[0].Caches

	var b strings.Builder
	b.WriteString("| trace |")
	for _, name := range caches {
		fmt.Fprintf(&b, " %s |", name)
	}
	b.WriteString("\n|:---|")
	b.WriteString(strings.Repeat("---:|", len(caches)))
	b.WriteString("\n")

	for _, t := range tables {
		if !slices.Equal(t.Caches, caches) {
			return fmt.Errorf("table %q has other caches than table %q", t.Name, tables[0].Name)
		}
		// The sums over the capacities stand in for the averages: they
		// compare the same way.
		row := Row{Cells: make([]Cell, len(caches))}
		for _, r := range t.Rows {
			for i, cell := range r.Cells {
				row.Cells[i].Hits += cell.Hits
				row.Cells[i].Min += cell.Min
				row.Cells[i].Max += cell.Max
			}
		}
		winner := t.winner(row)

		fmt.Fprintf(&b, "| %s |", t.Title)
		for i, cell := range row.Cells {
			text := "-"
			if n := t.Requests * len(t.Rows); n > 0 {
				text = strconv.FormatFloat(100*float64(cell.Hits)/float64(n), 'f', 2, 64)
				if i == winner {
					text = "**" + text + "**"
				}
			}
			fmt.Fprintf(&b, " %s |", text)
		}
		b.WriteString("\n")
	}

	_, err := io.WriteString(w, b.String())
	return err
}

// Markdown writes the tables as Markdown, each under a heading of its own.
// The hit ratios are in percent. The one of a cache that beat all the others
// in a row is set in bold. A cache that varies from replay to replay has a
// second column, with the range of its hit ratios.
func Markdown(w io.Writer, tables []Table) error {
	var b strings.Builder
	for i, t := range tables {
		if i > 0 {
			b.WriteString("\n")
		}
		t.heading(&b)
		if t.Image != "" {
			fmt.Fprintf(&b, "![Hit ratios on the %s trace by cache capacity](%s)\n\n", t.Title, t.Image)
		}

		columns := 0
		b.WriteString("| capacity |")
		for i, name := range t.Caches {
			fmt.Fprintf(&b, " %s |", name)
			columns++
			if t.varies(i) {
				fmt.Fprintf(&b, " %s, range |", name)
				columns++
			}
		}
		b.WriteString("\n|---:|")
		b.WriteString(strings.Repeat("---:|", columns))
		b.WriteString("\n")

		for _, row := range t.Rows {
			winner := t.winner(row)

			fmt.Fprintf(&b, "| %s |", Thousands(row.Capacity))
			for i, cell := range row.Cells {
				text := t.ratio(cell.Hits)
				if i == winner && t.Requests > 0 {
					text = "**" + text + "**"
				}
				fmt.Fprintf(&b, " %s |", text)
				if t.varies(i) {
					fmt.Fprintf(&b, " %s–%s |", t.ratio(cell.Min), t.ratio(cell.Max))
				}
			}
			b.WriteString("\n")
		}
	}

	_, err := io.WriteString(w, b.String())
	return err
}

// heading writes the title of the table and what there is to say about its
// trace.
func (t Table) heading(b *strings.Builder) {
	fmt.Fprintf(b, "### %s\n\n", t.Title)
	fmt.Fprintf(b, "%s: %s requests for %s keys.\n", capitalize(t.Workload), Thousands(t.Requests), Thousands(t.Unique))
	fmt.Fprintf(b, "Trace: %s.\n\n", t.Origin)
}

// Differences writes tables whose caches come in pairs, a cache and a variant
// of it, as Markdown: for every pair, by how many percentage points the hit
// ratio of the variant differs from that of the cache. If the variant varies
// from replay to replay, the difference is that of its median, and the range
// of the differences follows in brackets.
func Differences(w io.Writer, tables []Table) error {
	var b strings.Builder
	for i, t := range tables {
		if len(t.Caches)%2 != 0 {
			return fmt.Errorf("table %q has %d caches, which do not pair up", t.Name, len(t.Caches))
		}
		if i > 0 {
			b.WriteString("\n")
		}
		t.heading(&b)

		b.WriteString("| capacity |")
		for i := 0; i < len(t.Caches); i += 2 {
			fmt.Fprintf(&b, " %s |", t.Caches[i])
		}
		b.WriteString("\n|---:|")
		b.WriteString(strings.Repeat("---:|", len(t.Caches)/2))
		b.WriteString("\n")

		for _, row := range t.Rows {
			fmt.Fprintf(&b, "| %s |", Thousands(row.Capacity))
			for i := 0; i < len(row.Cells); i += 2 {
				base, variant := row.Cells[i].Hits, row.Cells[i+1]
				text := t.points(variant.Hits - base)
				// A range that is no wider than the last digit shown says
				// nothing that the number has not said.
				if lo, hi := t.points(variant.Min-base), t.points(variant.Max-base); lo != hi {
					text += " (" + lo + " to " + hi + ")"
				}
				fmt.Fprintf(&b, " %s |", text)
			}
			b.WriteString("\n")
		}
	}

	_, err := io.WriteString(w, b.String())
	return err
}

// points formats a difference in hits as percentage points, with its sign.
func (t Table) points(hits int) string {
	if t.Requests == 0 {
		return "-"
	}
	text := strconv.FormatFloat(t.percent(hits), 'f', 2, 64)
	switch {
	case text == "0.00" || text == "-0.00":
		return "0.00"
	case hits > 0:
		return "+" + text
	default:
		return "−" + text[1:] // A minus sign, not a hyphen.
	}
}

// CSV writes one line for every cache at every capacity of every table. The
// ratios are fractions of one: that of the median replay, and those of the
// worst and the best.
func CSV(w io.Writer, tables []Table) error {
	out := csv.NewWriter(w)
	if err := out.Write([]string{"dataset", "capacity", "cache", "requests", "hits", "hit_ratio", "min_hit_ratio", "max_hit_ratio"}); err != nil {
		return err
	}
	for _, t := range tables {
		fraction := func(hits int) string {
			return strconv.FormatFloat(float64(hits)/float64(max(t.Requests, 1)), 'f', 6, 64)
		}
		for _, row := range t.Rows {
			for i, cell := range row.Cells {
				err := out.Write([]string{
					t.Name,
					strconv.Itoa(row.Capacity),
					t.Caches[i],
					strconv.Itoa(t.Requests),
					strconv.Itoa(cell.Hits),
					fraction(cell.Hits),
					fraction(cell.Min),
					fraction(cell.Max),
				})
				if err != nil {
					return err
				}
			}
		}
	}
	out.Flush()
	return out.Error()
}

// ratio formats hits as a percentage of the requests of the table.
func (t Table) ratio(hits int) string {
	if t.Requests == 0 {
		return "-"
	}
	return strconv.FormatFloat(t.percent(hits), 'f', 2, 64)
}

// Thousands formats n with commas between groups of three digits.
func Thousands(n int) string {
	s := strconv.Itoa(n)
	sign := ""
	if n < 0 {
		sign, s = "-", s[1:]
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return sign + s
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
