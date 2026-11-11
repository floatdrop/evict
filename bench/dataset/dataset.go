// Package dataset knows the public cache traces that the benchmarks replay,
// and fetches them. Every file is pinned by its SHA-256, so a benchmark runs
// on the same requests wherever and whenever it is run.
package dataset

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"

	"github.com/floatdrop/evict/bench/trace"
)

// Compression is the way a trace file is compressed.
type Compression int

// The compressions that a [Dataset] may use.
const (
	Gzip Compression = iota + 1
	Zstd
	XZ
)

// Dataset describes a trace: where to get it, how to read it, and the cache
// sizes at which it is worth replaying.
type Dataset struct {
	// Name identifies the dataset on the command line and in file names.
	Name string
	// Title is the name of the dataset in reports.
	Title string
	// Workload says what produced the requests.
	Workload string
	// Origin credits the people who collected the trace.
	Origin string

	// URLs are the places to download the file from, each of which serves
	// the same bytes. They are tried in order.
	URLs []string
	// SHA256 is the checksum of the file, in hexadecimal.
	SHA256 string
	// Size is the length of the file in bytes. A download that turns out
	// longer is given up.
	Size int64

	Compression Compression
	Format      trace.Format

	// Parts are the traces that the dataset strings together, for one that
	// has no file of its own. A trace may be in there more than once.
	Parts []Dataset

	// Capacities are the cache sizes to replay the trace against, in
	// entries.
	Capacities []int

	// generate makes up the requests of a dataset that is not downloaded.
	generate func() []uint64
}

// Download returns the number of bytes that the dataset takes to download.
func (d Dataset) Download() int64 {
	size := d.Size
	seen := map[string]bool{}
	for _, part := range d.Parts {
		if !seen[part.SHA256] {
			size += part.Download()
		}
		seen[part.SHA256] = true
	}
	return size
}

// Generated returns a dataset whose requests are made up by a function rather
// than downloaded. The function must return the same requests every time.
func Generated(name, title, workload, origin string, capacities []int, generate func() []uint64) Dataset {
	return Dataset{Name: name, Title: title, Workload: workload, Origin: origin, Capacities: capacities, generate: generate}
}

// The files of the traces that ship with otter's own benchmarks come from
// otter's repository, at a fixed commit.
const otter = "https://raw.githubusercontent.com/maypok86/otter/73e5472e8d8a6047589e48b37daaf952a390bc74/benchmarks/simulator/trace/"

// The traces of the libCacheSim project are served by Carnegie Mellon's
// Parallel Data Lab and from a public bucket.
const (
	cmu    = "https://ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/cacheDatasets/"
	bucket = "https://cache-datasets.s3.amazonaws.com/"
)

const (
	arcPaper = `Megiddo and Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache", FAST '03`
	cacheSim = "in the format of libCacheSim, https://github.com/cacheMon/cache_dataset (CC BY 4.0)"
)

// The two traces that otter's benchmarks string together as "Mixed".
var (
	corda = Dataset{
		Name:        "corda",
		URLs:        []string{otter + "corda/trace_vaultservice.gz"},
		SHA256:      "bc2c118fe0a56b877fc21d35ad2a46b19d2f28b593553b8aa36a788cc169b21c",
		Size:        804308,
		Compression: Gzip,
		Format:      trace.BigEndian,
	}
	loop = Dataset{
		Name:        "loop",
		URLs:        []string{otter + "lirs/loop.gz"},
		SHA256:      "465c1cc6bb08993034fd3597106cb0edc55b25a5984a17a12018f30a06ba60e9",
		Size:        17848,
		Compression: Gzip,
		Format:      trace.LIRS,
	}
)

// All returns the datasets, in the order in which reports list them.
func All() []Dataset {
	return []Dataset{
		{
			Name:     "zipf",
			Title:    "Zipf",
			Workload: "synthetic: a million requests whose popularity follows a Zipf distribution (s = 1.0001, v = 10) over 50 million keys",
			Origin:   "generated; the parameters are those of otter's benchmarks",
			generate: func() []uint64 {
				return trace.Zipf(1_000_000, 1.0001, 10, 50_000_000)
			},
			Capacities: []int{500, 1000, 2000, 5000, 10_000, 20_000, 40_000, 80_000},
		},
		{
			Name:        "s3",
			Title:       "S3",
			Workload:    "disk reads of a search engine",
			Origin:      arcPaper,
			URLs:        []string{otter + "arc/s3.gz"},
			SHA256:      "ac802e9252c79c26eb3a919277d80bf8b6fb65348bbc6e491c5434bd4bbc8bb6",
			Size:        50020220,
			Compression: Gzip,
			Format:      trace.ARC,
			Capacities:  []int{100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 700_000, 800_000},
		},
		{
			Name:        "ds1",
			Title:       "DS1",
			Workload:    "disk reads of a database server that runs an ERP application, over seven days",
			Origin:      arcPaper,
			URLs:        []string{otter + "arc/ds1.gz"},
			SHA256:      "1c4eb8811b9840a6a006b5c6560e0a93e2dc978fae274378fb48ce2969e78366",
			Size:        7356498,
			Compression: Gzip,
			Format:      trace.ARC,
			Capacities:  []int{1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000, 7_000_000, 8_000_000},
		},
		{
			Name:        "oltp",
			Title:       "OLTP",
			Workload:    "references to a CODASYL database, over one hour",
			Origin:      arcPaper,
			URLs:        []string{otter + "arc/oltp.gz"},
			SHA256:      "9599b313a5662e72734fb499b3513759144d7f80847134f29638567b6a5ab151",
			Size:        2246479,
			Compression: Gzip,
			Format:      trace.ARC,
			Capacities:  []int{250, 500, 750, 1000, 1250, 1500, 1750, 2000},
		},
		{
			Name:        "p8",
			Title:       "P8",
			Workload:    "disk reads of a Windows NT workstation",
			Origin:      arcPaper,
			URLs:        []string{otter + "arc/p8.gz"},
			SHA256:      "fa3f54c0ccce081e1ba86578e967567d370319911102c045233da69bda17b29a",
			Size:        15020297,
			Compression: Gzip,
			Format:      trace.ARC,
			Capacities:  []int{10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000},
		},
		{
			Name:        "glimpse",
			Title:       "Glimpse",
			Workload:    "block reads of the text search tool glimpse, which scans its files in loops",
			Origin:      `Jiang and Zhang, "LIRS: An Efficient Low Inter-reference Recency Set Replacement Policy to Improve Buffer Cache Performance", SIGMETRICS '02`,
			URLs:        []string{otter + "lirs/gli.gz"},
			SHA256:      "c300a0bff8149373bdbb7df3fa440e621aba920b0ec6bcc7a936f360f782511c",
			Size:        5770,
			Compression: Gzip,
			Format:      trace.LIRS,
			Capacities:  []int{250, 500, 750, 1000, 1250, 1500, 1750, 2000},
		},
		{
			Name:  "mixed",
			Title: "Mixed",
			Workload: "three workloads in a row: the vault service of a node of the Corda ledger, " +
				"a program that reads its blocks in a loop, and the vault service once more",
			Origin: "R3, by way of the simulator of Caffeine, and Jiang and Zhang, the authors of LIRS; " +
				"strung together as in otter's benchmarks",
			Parts:      []Dataset{corda, loop, corda},
			Capacities: []int{250, 500, 750, 1000, 1250, 1500, 1750, 2000},
		},
		{
			Name:        "scarab",
			Title:       "Scarab",
			Workload:    "lookups of an e-commerce recommendation service, over one hour",
			Origin:      "Scarab Research, by way of the simulator of Caffeine",
			URLs:        []string{otter + "scarab/scarab-recs.trace.xz"},
			SHA256:      "abf86a7eef62e1e5ed93afe318168bb72a575c6d552e9904426711039c05ede3",
			Size:        3075100,
			Compression: XZ,
			Format:      trace.BigEndian,
			Capacities:  []int{1000, 5000, 10_000, 25_000, 50_000, 75_000, 100_000, 125_000},
		},
		{
			Name:     "twitter",
			Title:    "Twitter",
			Workload: "requests to an in-memory key-value cache at Twitter (cluster 45, a sample of one key in a hundred)",
			Origin:   `Yang, Yue and Rashmi, "A large scale analysis of hundreds of in-memory cache clusters at Twitter", OSDI '20; ` + cacheSim,
			URLs: []string{
				cmu + "twitter/sample100/cluster45.oracleGeneral.sample100.zst",
				bucket + "cache_trace_twitter_memcache/oracleGeneral/sample100/cluster45.oracleGeneral.sample100.zst",
			},
			SHA256:      "423445a07d8d3b9f40e19f4974c5477e80bcb9df8d3251abaad6969d192cd84f",
			Size:        17875473,
			Compression: Zstd,
			Format:      trace.OracleGeneral,
			Capacities:  []int{1000, 2500, 5000, 10_000, 25_000, 50_000, 100_000, 200_000},
		},
		{
			Name:     "msr",
			Title:    "MSR",
			Workload: "block I/O of a hardware monitoring server at Microsoft Research Cambridge (volume hm_0)",
			Origin:   `Narayanan, Donnelly and Rowstron, "Write Off-Loading: Practical Power Management for Enterprise Storage", FAST '08; ` + cacheSim,
			URLs: []string{
				cmu + "msr/msr_hm_0.oracleGeneral.zst",
				bucket + "cache_dataset_oracleGeneral/2007_msr/msr_hm_0.oracleGeneral.zst",
			},
			SHA256:      "5fc99e798d5e06f61ad97019298ea7226d1afff9ef577e87b447c63a4480de17",
			Size:        18681259,
			Compression: Zstd,
			Format:      trace.OracleGeneral,
			Capacities:  []int{1000, 2500, 5000, 10_000, 25_000, 50_000, 100_000, 200_000},
		},
		{
			Name:     "cloudphysics",
			Title:    "CloudPhysics",
			Workload: "block I/O of a virtual machine (trace w105)",
			Origin:   `Waldspurger, Park, Garthwaite and Ahmad, "Efficient MRC Construction with SHARDS", FAST '15; ` + cacheSim,
			URLs: []string{
				cmu + "cloudphysics/w105.oracleGeneral.bin.zst",
				bucket + "cache_dataset_oracleGeneral/2015_cloudphysics/w105.oracleGeneral.bin.zst",
			},
			SHA256:      "f7a1c17a5842d0a6376aa73ac2af4aa41fc7c5253ddcc2ee31ccdf1650c77175",
			Size:        11731963,
			Compression: Zstd,
			Format:      trace.OracleGeneral,
			Capacities:  []int{1000, 2500, 5000, 10_000, 25_000, 50_000, 100_000, 200_000},
		},
	}
}

// Find returns the dataset with the given name.
func Find(name string) (Dataset, bool) {
	for _, d := range All() {
		if d.Name == name {
			return d, true
		}
	}
	return Dataset{}, false
}

// Fetch makes sure that the files of the dataset are in dir and intact, and
// downloads the ones that are not.
func (d Dataset) Fetch(ctx context.Context, dir string) error {
	if d.generate != nil {
		return nil
	}
	for _, part := range d.Parts {
		if err := part.Fetch(ctx, dir); err != nil {
			return fmt.Errorf("dataset %s: %w", d.Name, err)
		}
	}
	if len(d.Parts) > 0 {
		return nil
	}
	if _, err := d.fetch(ctx, dir); err != nil {
		return fmt.Errorf("dataset %s: %w", d.Name, err)
	}
	return nil
}

// Load returns the requests of the dataset. The file is downloaded into dir
// unless it is there already, and its checksum is verified either way.
func (d Dataset) Load(ctx context.Context, dir string) ([]uint64, error) {
	if d.generate != nil {
		return d.generate(), nil
	}
	if len(d.Parts) > 0 {
		var keys []uint64
		for _, part := range d.Parts {
			more, err := part.Load(ctx, dir)
			if err != nil {
				return nil, fmt.Errorf("dataset %s: %w", d.Name, err)
			}
			keys = append(keys, more...)
		}
		return keys, nil
	}

	file, err := d.fetch(ctx, dir)
	if err != nil {
		return nil, fmt.Errorf("dataset %s: %w", d.Name, err)
	}

	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("dataset %s: %w", d.Name, err)
	}
	defer f.Close() //nolint:errcheck // The file is only read.

	r, err := d.decompress(f)
	if err != nil {
		return nil, fmt.Errorf("dataset %s: decompressing %s: %w", d.Name, file, err)
	}
	keys, err := trace.Read(r, d.Format)
	if err != nil {
		return nil, fmt.Errorf("dataset %s: %w", d.Name, err)
	}
	return keys, nil
}

func (d Dataset) decompress(r io.Reader) (io.Reader, error) {
	switch d.Compression {
	case Gzip:
		return gzip.NewReader(r)
	case Zstd:
		return zstd.NewReader(r)
	case XZ:
		return xz.NewReader(r)
	default:
		return nil, fmt.Errorf("unknown compression %d", int(d.Compression))
	}
}

// fetch makes sure that the file of the dataset is in dir, intact, and
// returns its path.
func (d Dataset) fetch(ctx context.Context, dir string) (string, error) {
	if len(d.URLs) == 0 {
		return "", errors.New("no place to download it from")
	}
	file := filepath.Join(dir, d.Name+"-"+path.Base(d.URLs[0]))

	// A file that is there but is not what it should be was cut short or
	// tampered with. It is replaced.
	if err := verify(file, d.SHA256); err == nil {
		return file, nil
	} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errChecksum) {
		return "", err
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	var errs []error
	for _, url := range d.URLs {
		err := download(ctx, url, file, d.SHA256, d.Size, patience)
		if err == nil {
			return file, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", url, err))
		if ctx.Err() != nil {
			break
		}
	}
	return "", errors.Join(errs...)
}

var (
	errChecksum = errors.New("checksum mismatch")
	errStalled  = errors.New("the download has stalled")
)

// patience is how long a download may go without a byte arriving.
const patience = time.Minute

// verify checks that the file has the given SHA-256.
func verify(file, want string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // The file is only read.

	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return err
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != want {
		return fmt.Errorf("%w: %s has SHA-256 %s, want %s", errChecksum, file, got, want)
	}
	return nil
}

// client gives a server a while to start answering, and the download as long
// as the context allows.
var client = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ResponseHeaderTimeout: 30 * time.Second,
	},
}

// progress is the body of a response that calls off the download when
// nothing has arrived for too long.
type progress struct {
	body     io.Reader
	timer    *time.Timer
	patience time.Duration
}

func (p progress) Read(b []byte) (int, error) {
	n, err := p.body.Read(b)
	if n > 0 {
		p.timer.Reset(p.patience)
	}
	return n, err
}

// download stores what url serves in file, provided it has the given SHA-256
// and is no longer than size, if a size is given. The file appears under its
// name only once it is complete and verified. A server that sends nothing for
// as long as wait says is given up on.
func download(ctx context.Context, url, file, want string, size int64, wait time.Duration) (err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	timer := time.AfterFunc(wait, func() { cancel(errStalled) })
	defer timer.Stop()
	defer func() {
		// Whatever the transport made of it, say what went wrong.
		if cause := context.Cause(ctx); err != nil && errors.Is(cause, errStalled) {
			err = cause
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck // Nothing to do about it.

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("the server answered %s", resp.Status)
	}

	tmp, err := os.CreateTemp(filepath.Dir(file), filepath.Base(file)+".*.part")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// The file may be closed already, and it may be gone; neither
			// is worth reporting on top of what went wrong.
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()

	// One byte over the size is enough to know that this is another file.
	var body io.Reader = progress{resp.Body, timer, wait}
	if size > 0 {
		body = io.LimitReader(body, size+1)
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, hash), body)
	if err != nil {
		return err
	}
	if size > 0 && n > size {
		return fmt.Errorf("%w: the download is longer than the %d bytes of the file", errChecksum, size)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != want {
		return fmt.Errorf("%w: the download has SHA-256 %s, want %s", errChecksum, got, want)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), file)
}
