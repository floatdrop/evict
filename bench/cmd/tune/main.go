// Command tune replays traces against evict/s3fifo with one setting after the
// other and reports which of them serves each trace best.
//
// S3-FIFO runs its queues by the numbers that its paper recommends, and the
// options of package s3fifo change them. What the right numbers are depends on
// the workload. This command finds them for a recording of one:
//
//	go run ./cmd/tune -trace requests.txt -format lirs -capacities 1000,10000
//
// Without -trace it goes through the traces that the benchmarks replay.
package main

import (
	"bufio"
	"cmp"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"

	"github.com/floatdrop/evict/bench/dataset"
	"github.com/floatdrop/evict/bench/replay"
	"github.com/floatdrop/evict/bench/report"
	"github.com/floatdrop/evict/bench/trace"
	"github.com/floatdrop/evict/s3fifo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	// The first interrupt asks the replays to wind down; a second one ends
	// the program on the spot.
	go func() {
		<-ctx.Done()
		stop()
	}()

	err := run(ctx, os.Args[1:])
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "tune:", err)
		os.Exit(1)
	}
}

// A setting is a way of setting up an S3-FIFO cache.
type setting struct {
	small     float64 // share of the capacity for the small queue
	ghosts    float64 // ghosts as a share of the capacity
	promotion int     // reads in the small queue that move an entry up
	returns   int     // times a ghost has to come back to go to the main queue
}

// standard is the cache as the paper has it and as New makes it.
var standard = setting{small: 0.1, ghosts: 0.9, promotion: 2, returns: 1}

func (s setting) String() string {
	return fmt.Sprintf("small %s, ghosts %s, promotion %d, returns %d",
		strconv.FormatFloat(s.small, 'g', -1, 64), strconv.FormatFloat(s.ghosts, 'g', -1, 64), s.promotion, s.returns)
}

// cache returns a cache with the setting, in one piece: what is compared is
// the policy.
func (s setting) cache(capacity int) tuned {
	return tuned{s3fifo.New[uint64, uint64](capacity,
		s3fifo.WithShards(1),
		s3fifo.WithSmallRatio(s.small),
		s3fifo.WithGhostRatio(s.ghosts),
		s3fifo.WithPromotionReads(s.promotion),
		s3fifo.WithGhostReturns(s.returns),
	)}
}

type tuned struct{ c *s3fifo.Cache[uint64, uint64] }

func (t tuned) Get(key uint64) (uint64, bool) { return t.c.Get(key) }
func (t tuned) Set(key, value uint64)         { t.c.Set(key, value) }

// grid returns the settings that are tried, the standard one first. They are
// the values that the S4-FIFO paper lets the same knobs take, less its skip
// region, which this cache does not have. Where two settings do equally
// well, the one that comes first here is reported: the standard one before
// all others, and fewer ghosts, which cost memory, before more.
func grid() []setting {
	settings := []setting{standard}
	for _, ghosts := range []float64{0.9, 3, 6} {
		for _, returns := range []int{1, 2} {
			for _, promotion := range []int{2, 1} {
				for _, small := range []float64{0.1, 0.05, 0.2, 0.3, 0.5, 0.7, 0.9} {
					if s := (setting{small, ghosts, promotion, returns}); s != standard {
						settings = append(settings, s)
					}
				}
			}
		}
	}
	return settings
}

// A workload is a trace to tune for, and the capacities to tune at.
type workload struct {
	name, title string
	capacities  []int
	load        func(ctx context.Context) ([]uint64, error)
}

// A result has what every setting answered of a workload: hits[i][j] is the
// number of requests that setting j answered at capacity i.
type result struct {
	workload
	requests int
	hits     [][]int
}

// catalog returns the datasets there are to choose from. The tests have
// their own.
var catalog = dataset.All

func run(ctx context.Context, args []string) error {
	defaultDir := filepath.Join(os.TempDir(), "evict-bench")
	if dir, err := os.UserCacheDir(); err == nil {
		defaultDir = filepath.Join(dir, "evict-bench")
	}

	flags := flag.NewFlagSet("tune", flag.ContinueOnError)
	var (
		datasetNames = flags.String("datasets", "all", "comma-separated `names` of the datasets to tune for, as hitratio -list has them")
		file         = flags.String("trace", "", "`file` with a recording of requests to tune for, instead of the datasets; it may be compressed (.gz, .zst, .xz)")
		format       = flags.String("format", "lirs", "format of -trace: lirs (a number per line), arc, oraclegeneral or bigendian")
		capacities   = flags.String("capacities", "", "comma-separated cache `sizes` to tune at; needed with -trace")
		dir          = flags.String("dir", defaultDir, "`directory` to keep the downloaded traces in")
		out          = flags.String("out", "results", "`directory` to write the report to")
		parallel     = flags.Int("parallel", min(runtime.GOMAXPROCS(0), 8), "`number` of replays to run at the same time; every one of them holds a cache in memory, of up to 170 bytes for every entry")
	)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	var workloads []workload
	if *file != "" {
		w, err := recording(*file, *format, *capacities)
		if err != nil {
			return err
		}
		workloads = []workload{w}
	} else {
		if *capacities != "" {
			return errors.New("-capacities goes with -trace; the datasets have their own")
		}
		all := catalog()
		for _, d := range all {
			if *datasetNames != "all" && !slices.Contains(strings.Split(*datasetNames, ","), d.Name) {
				continue
			}
			workloads = append(workloads, workload{
				name: d.Name, title: d.Title, capacities: d.Capacities,
				load: func(ctx context.Context) ([]uint64, error) {
					if err := d.Fetch(ctx, *dir); err != nil {
						return nil, err
					}
					return d.Load(ctx, *dir)
				},
			})
		}
		if *datasetNames != "all" {
			for name := range strings.SplitSeq(*datasetNames, ",") {
				if !slices.ContainsFunc(all, func(d dataset.Dataset) bool { return d.Name == name }) {
					return fmt.Errorf("there is no dataset %q; see hitratio -list", name)
				}
			}
		}
	}

	// Whatever might go wrong with the report had better go wrong before
	// the replays, which take a while.
	if err := os.MkdirAll(*out, 0o750); err != nil {
		return err
	}

	settings := grid()
	var results []result
	for _, w := range workloads {
		r, err := tune(ctx, w, settings, max(*parallel, 1))
		if err != nil {
			return err
		}
		results = append(results, r)
	}

	command := "go run ./cmd/tune " + strings.Join(args, " ")
	if err := writeFile(filepath.Join(*out, "tuning.md"), func(w io.Writer) error {
		return markdown(w, strings.TrimSpace(command), settings, results)
	}); err != nil {
		return err
	}
	return writeFile(filepath.Join(*out, "tuning.csv"), func(w io.Writer) error {
		return csv(w, settings, results)
	})
}

// recording returns the workload that is a file of requests.
func recording(file, format, capacities string) (workload, error) {
	formats := map[string]trace.Format{
		"lirs": trace.LIRS, "arc": trace.ARC, "oraclegeneral": trace.OracleGeneral, "bigendian": trace.BigEndian,
	}
	f, ok := formats[strings.ToLower(format)]
	if !ok {
		return workload{}, fmt.Errorf("there is no format %q; it is lirs, arc, oraclegeneral or bigendian", format)
	}
	if capacities == "" {
		return workload{}, errors.New("-trace needs -capacities: the cache sizes to tune at")
	}
	var sizes []int
	for field := range strings.SplitSeq(capacities, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || n < 1 {
			return workload{}, fmt.Errorf("-capacities has %q, want sizes of one entry or more", field)
		}
		sizes = append(sizes, n)
	}
	slices.Sort(sizes)
	sizes = slices.Compact(sizes)

	name := filepath.Base(file)
	return workload{name: name, title: name, capacities: sizes, load: func(context.Context) ([]uint64, error) {
		in, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		defer in.Close() //nolint:errcheck // The file is only read.

		var r io.Reader = bufio.NewReaderSize(in, 1<<20)
		switch strings.ToLower(filepath.Ext(file)) {
		case ".gz":
			r, err = gzip.NewReader(r)
		case ".zst":
			var z *zstd.Decoder
			if z, err = zstd.NewReader(r); err == nil {
				defer z.Close()
				r = z
			}
		case ".xz":
			r, err = xz.NewReader(r)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		keys, err := trace.Read(r, f)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		return keys, nil
	}}, nil
}

// tune replays the workload against a cache with every setting at every
// capacity.
func tune(ctx context.Context, w workload, settings []setting, parallel int) (result, error) {
	start := time.Now()
	fmt.Fprintf(os.Stderr, "%s: loading\n", w.name)
	keys, err := w.load(ctx)
	if err != nil {
		return result{}, err
	}
	if len(keys) == 0 {
		return result{}, fmt.Errorf("%s holds no requests", w.name)
	}
	fmt.Fprintf(os.Stderr, "%s: %s requests, loaded in %s\n", w.name, report.Thousands(len(keys)), time.Since(start).Round(time.Millisecond))

	r := result{workload: w, requests: len(keys), hits: make([][]int, len(w.capacities))}
	var wg sync.WaitGroup
	slots := make(chan struct{}, parallel)
	for i, capacity := range w.capacities {
		r.hits[i] = make([]int, len(settings))
		for j, s := range settings {
			wg.Go(func() {
				slots <- struct{}{}
				defer func() { <-slots }()
				if ctx.Err() != nil {
					return
				}
				r.hits[i][j] = replay.Hits(s.cache(capacity), keys)
			})
		}
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return result{}, err
	}
	fmt.Fprintf(os.Stderr, "%s: %d settings at %d capacities in %s\n", w.name, len(settings), len(w.capacities), time.Since(start).Round(time.Second))
	return r, nil
}

// total returns what setting j answered of the workload at all capacities
// together: the settings compare by it as they do by their average hit ratio.
func (r result) total(j int) int {
	sum := 0
	for i := range r.hits {
		sum += r.hits[i][j]
	}
	return sum
}

// best returns the setting that answered the most at all capacities together.
func (r result) best() int {
	return firstMax(len(r.hits[0]), r.total)
}

// bestAt returns the setting that answered the most at capacity i.
func (r result) bestAt(i int) int {
	return firstMax(len(r.hits[i]), func(j int) int { return r.hits[i][j] })
}

// percent returns hits, summed over as many capacities as given, as the
// average hit ratio in percent.
func (r result) percent(hits, capacities int) float64 {
	return 100 * float64(hits) / float64(r.requests*capacities)
}

// firstMax returns the first of n indices at which score is the highest.
func firstMax[T cmp.Ordered](n int, score func(int) T) int {
	best := 0
	for j := 1; j < n; j++ {
		if score(j) > score(best) {
			best = j
		}
	}
	return best
}

// forAll returns the one setting that does best on all workloads: the one
// with the highest hit ratio on average, every workload counting the same.
func forAll(results []result) int {
	return firstMax(len(results[0].hits[0]), func(j int) float64 {
		sum := 0.0
		for _, r := range results {
			sum += r.percent(r.total(j), len(r.capacities))
		}
		return sum
	})
}

func markdown(w io.Writer, command string, settings []setting, results []result) error {
	var b strings.Builder
	fmt.Fprintf(&b, `# Tuning S3-FIFO

What `+"`evict/s3fifo`"+` answers of a trace as it is, and what it would answer with other
numbers for its queues. The cache is as the S3-FIFO paper has it unless its options say
otherwise: a tenth of the room for the small queue, nine tenths as many ghosts as entries,
two reads to move up from the small queue, and a ghost that goes to the main queue the
first time it comes back. Here it is replayed with %d settings: the small queue at 5%%, 10%%,
20%%, 30%%, 50%%, 70%% and 90%% of the capacity, 0.9, 3 and 6 times as many ghosts as entries,
one or two reads to move up, and one or two returns of a ghost.

The caches are in one piece. A setting is written as the options that make it:
`+"`small`"+` is WithSmallRatio, `+"`ghosts`"+` WithGhostRatio, `+"`promotion`"+` WithPromotionReads and
`+"`returns`"+` WithGhostReturns. Where settings do equally well, the standard one is named
before the others, and one with fewer ghosts before one with more: a ghost takes about
sixteen bytes.

Produced by `+"`%s`"+` with %s.

## At a glance

The hit ratio in percent, averaged over the capacities at which a trace was replayed.

`, len(settings), command, runtime.Version())

	one := forAll(results)
	several := len(results) > 1
	b.WriteString("| trace | as it is |")
	if several {
		b.WriteString(" one setting for all |")
	}
	b.WriteString(" best setting for the trace | best setting for each capacity | the best setting for the trace is |\n|:---|---:|")
	if several {
		b.WriteString("---:|")
	}
	b.WriteString("---:|---:|:---|\n")

	var sums [4]float64
	for _, r := range results {
		n := len(r.capacities)
		best := r.best()
		each := 0
		for i := range r.hits {
			each += r.hits[i][r.bestAt(i)]
		}
		cols := [4]float64{r.percent(r.total(0), n), r.percent(r.total(one), n), r.percent(r.total(best), n), r.percent(each, n)}
		for k, v := range cols {
			sums[k] += v
		}
		fmt.Fprintf(&b, "| %s | %.2f |", r.title, cols[0])
		if several {
			fmt.Fprintf(&b, " %.2f |", cols[1])
		}
		fmt.Fprintf(&b, " %.2f | %.2f | %s |\n", cols[2], cols[3], settings[best])
	}
	if several {
		n := float64(len(results))
		fmt.Fprintf(&b, "| **mean** | %.2f | %.2f | %.2f | %.2f | |\n", sums[0]/n, sums[1]/n, sums[2]/n, sums[3]/n)
		fmt.Fprintf(&b, "\nThe one setting that does best for all of these traces together is: %s.\n", settings[one])
	}

	b.WriteString("\n## Trace by trace\n")
	for _, r := range results {
		fmt.Fprintf(&b, "\n### %s\n\n%s requests.\n\n", r.title, report.Thousands(r.requests))
		b.WriteString("| capacity | as it is | best | with |\n|---:|---:|---:|:---|\n")
		for i, capacity := range r.capacities {
			best := r.bestAt(i)
			fmt.Fprintf(&b, "| %s | %.2f | %.2f | %s |\n", report.Thousands(capacity),
				r.percent(r.hits[i][0], 1), r.percent(r.hits[i][best], 1), settings[best])
		}
	}

	_, err := io.WriteString(w, b.String())
	return err
}

func csv(w io.Writer, settings []setting, results []result) error {
	var b strings.Builder
	b.WriteString("dataset,capacity,small_ratio,ghost_ratio,promotion_reads,ghost_returns,requests,hits\n")
	for _, r := range results {
		for i, capacity := range r.capacities {
			for j, s := range settings {
				fmt.Fprintf(&b, "%s,%d,%s,%s,%d,%d,%d,%d\n", r.name, capacity,
					strconv.FormatFloat(s.small, 'g', -1, 64), strconv.FormatFloat(s.ghosts, 'g', -1, 64),
					s.promotion, s.returns, r.requests, r.hits[i][j])
			}
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func writeFile(name string, write func(io.Writer) error) (err error) {
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	return write(f)
}
