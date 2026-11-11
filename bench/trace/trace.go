// Package trace reads the file formats that cache traces are published in. A
// trace is a sequence of keys, one for every request that a cache received.
package trace

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
)

// Format is the way a trace is laid out in a file.
type Format int

// The formats that [Read] understands.
const (
	// ARC is the text format of the traces published with the ARC paper.
	// A line holds four numbers, of which the first two matter: a request
	// for a run of blocks, given by its first block and their number. The
	// run is one key after the other.
	ARC Format = iota + 1

	// LIRS is the text format of the traces published with the LIRS paper:
	// one block number per line. Empty lines and lines that start with an
	// asterisk carry no request.
	LIRS

	// OracleGeneral is the binary format of the libCacheSim project. A
	// request is a record of 24 bytes in little-endian byte order: a
	// timestamp of 4 bytes, the key of 8 bytes, a size of 4 bytes and the
	// time of the next request for the same key in 8 bytes.
	OracleGeneral

	// BigEndian is a plain sequence of keys of 8 bytes each in big-endian
	// byte order, as written by Java programs.
	BigEndian
)

func (f Format) String() string {
	switch f {
	case ARC:
		return "ARC"
	case LIRS:
		return "LIRS"
	case OracleGeneral:
		return "oracleGeneral"
	case BigEndian:
		return "big-endian"
	default:
		return fmt.Sprintf("Format(%d)", int(f))
	}
}

// Read reads a trace in the given format to its end and returns its keys.
func Read(r io.Reader, format Format) ([]uint64, error) {
	switch format {
	case ARC:
		return readLines(r, parseARC)
	case LIRS:
		return readLines(r, parseLIRS)
	case OracleGeneral:
		return readRecords(r, 24, func(record []byte) uint64 {
			return binary.LittleEndian.Uint64(record[4:])
		})
	case BigEndian:
		return readRecords(r, 8, binary.BigEndian.Uint64)
	default:
		return nil, fmt.Errorf("trace: unknown format %d", int(format))
	}
}

// readLines reads a text trace. The parse function appends the keys that a
// line stands for.
func readLines(r io.Reader, parse func(keys []uint64, line []byte) ([]uint64, error)) ([]uint64, error) {
	var keys []uint64

	scanner := bufio.NewScanner(r)
	n := 1
	for ; scanner.Scan(); n++ {
		var err error
		if keys, err = parse(keys, bytes.TrimSpace(scanner.Bytes())); err != nil {
			return nil, fmt.Errorf("trace: line %d: %w", n, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("trace: line %d: %w", n, err)
	}
	return keys, nil
}

// maxRun is the longest run of blocks that a line of an ARC trace may ask
// for. The published traces stay below 1000; a line that asks for more is
// taken to be damaged rather than allowed to fill up the memory.
const maxRun = 1 << 20

// parseARC reads a line of an ARC trace: the number of the first block of a
// run and the number of blocks in it, followed by two numbers that are of no
// use here.
func parseARC(keys []uint64, line []byte) ([]uint64, error) {
	if len(line) == 0 {
		return keys, nil
	}
	fields := bytes.Fields(line)
	if len(fields) < 2 || len(fields) > 4 {
		return nil, fmt.Errorf("%d fields, want the start and the length of a run and up to two more", len(fields))
	}
	var numbers [4]uint64
	for i, field := range fields {
		n, rest, err := leadingNumber(field)
		if err != nil {
			return nil, err
		}
		if len(rest) != 0 {
			return nil, fmt.Errorf("unexpected %q after a number", rest)
		}
		numbers[i] = n
	}

	start, count := numbers[0], numbers[1]
	if count > maxRun {
		return nil, fmt.Errorf("a run of %d blocks", count)
	}
	if count > 0 && start > math.MaxUint64-(count-1) {
		return nil, fmt.Errorf("a run of %d blocks from %d on leaves the range of block numbers", count, start)
	}
	for i := range count {
		keys = append(keys, start+i)
	}
	return keys, nil
}

func parseLIRS(keys []uint64, line []byte) ([]uint64, error) {
	if len(line) == 0 || line[0] == '*' {
		return keys, nil
	}
	key, rest, err := leadingNumber(line)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("unexpected %q after the block number", rest)
	}
	return append(keys, key), nil
}

var errNoNumber = errors.New("expected a number")

// leadingNumber parses the decimal number that b starts with and returns it
// along with what follows.
func leadingNumber(b []byte) (n uint64, rest []byte, err error) {
	i := 0
	for ; i < len(b) && '0' <= b[i] && b[i] <= '9'; i++ {
		digit := uint64(b[i] - '0')
		if n > (1<<64-1-digit)/10 {
			return 0, nil, fmt.Errorf("number %q is out of range", b)
		}
		n = n*10 + digit
	}
	if i == 0 {
		return 0, nil, errNoNumber
	}
	return n, b[i:], nil
}

// readRecords reads a binary trace of records of a fixed size.
func readRecords(r io.Reader, size int, key func(record []byte) uint64) ([]uint64, error) {
	var keys []uint64

	br := bufio.NewReaderSize(r, 1<<20)
	record := make([]byte, size)
	for {
		switch _, err := io.ReadFull(br, record); {
		case err == nil:
			keys = append(keys, key(record))
		case errors.Is(err, io.EOF):
			return keys, nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			return nil, fmt.Errorf("trace: the last of %d records is cut short", len(keys)+1)
		default:
			return nil, fmt.Errorf("trace: %w", err)
		}
	}
}

// Zipf returns a trace of n requests in which the popularity of the keys
// follows a Zipf distribution: key k is requested with a probability
// proportional to (v + k) ** -s, for k in [0, imax]. The trace is the same
// every time.
func Zipf(n int, s, v float64, imax uint64) []uint64 {
	zipf := rand.NewZipf(rand.New(rand.NewPCG(1, 1)), s, v, imax)

	keys := make([]uint64, n)
	for i := range keys {
		keys[i] = zipf.Uint64()
	}
	return keys
}

// Unique returns the number of different keys in a trace.
func Unique(keys []uint64) int {
	seen := make(map[uint64]struct{}, len(keys)/8)
	for _, key := range keys {
		seen[key] = struct{}{}
	}
	return len(seen)
}
