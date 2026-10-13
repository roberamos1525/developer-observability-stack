package tsdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const (
	recSeries  byte = 1
	recSamples byte = 2
	maxWALRec       = 16 << 20
)

type walSeries struct {
	ref  uint64
	lset labels.Labels
}

type walSample struct {
	ref uint64
	t   int64
	v   float64
}

// wal is a segmented write-ahead log of series and sample records.
// Each record is framed as uvarint(len) | type+payload | crc32c.
type wal struct {
	dir string
	mu  sync.Mutex
	seq int
	f   *os.File
	w   *bufio.Writer
	buf []byte
}

func walSegName(seq int) string { return fmt.Sprintf("%08d.wal", seq) }

func listWALSegments(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var seqs []int
	for _, e := range entries {
		if n, err := strconv.Atoi(strings.TrimSuffix(e.Name(), ".wal")); err == nil && strings.HasSuffix(e.Name(), ".wal") {
			seqs = append(seqs, n)
		}
	}
	sort.Ints(seqs)
	return seqs, nil
}

// openWAL replays every segment through the callbacks and then opens a new
// segment for writing. A corrupt tail in the last segment is truncated.
func openWAL(dir string, onSeries func(walSeries), onSamples func([]walSample)) (*wal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	seqs, err := listWALSegments(dir)
	if err != nil {
		return nil, err
	}
	for i, seq := range seqs {
		if err := replaySegment(filepath.Join(dir, walSegName(seq)), i == len(seqs)-1, onSeries, onSamples); err != nil {
			return nil, err
		}
	}
	w := &wal{dir: dir}
	next := 1
	if len(seqs) > 0 {
		next = seqs[len(seqs)-1] + 1
	}
	if err := w.openSegment(next); err != nil {
		return nil, err
	}
	return w, nil
}

func replaySegment(path string, last bool, onSeries func(walSeries), onSamples func([]walSample)) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 256<<10)
	var off int64
	for {
		n, err := binary.ReadUvarint(r)
		if errors.Is(err, io.EOF) {
			return nil
		}
		var rec []byte
		if err == nil && n > 0 && n <= maxWALRec {
			rec = make([]byte, n+4)
			_, err = io.ReadFull(r, rec)
		} else if err == nil {
			err = errors.New("bad record length")
		}
		if err == nil && crc32.Checksum(rec[:n], castagnoli) != binary.LittleEndian.Uint32(rec[n:]) {
			err = errors.New("checksum mismatch")
		}
		if err == nil {
			err = decodeWALRecord(rec[:n], onSeries, onSamples)
		}
		if err != nil {
			if !last {
				return fmt.Errorf("wal segment %s corrupt at offset %d: %w", path, off, err)
			}
			// Torn write at the end of the newest segment: drop it.
			if terr := f.Truncate(off); terr != nil {
				return terr
			}
			return f.Sync()
		}
		off += int64(uvarintSize(n)) + int64(n) + 4
	}
}

func uvarintSize(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func decodeWALRecord(rec []byte, onSeries func(walSeries), onSamples func([]walSample)) error {
	if len(rec) == 0 {
		return errors.New("empty record")
	}
	d := &byteDecoder{b: rec[1:]}
	switch rec[0] {
	case recSeries:
		n := d.uvarint()
		for i := uint64(0); i < n && d.err == nil; i++ {
			ref := d.uvarint()
			nl := d.uvarint()
			if nl > labels.MaxLabels+1 {
				return errors.New("too many labels")
			}
			ls := make(labels.Labels, 0, nl)
			for j := uint64(0); j < nl && d.err == nil; j++ {
				ls = append(ls, labels.Label{Name: d.str(), Value: d.str()})
			}
			if d.err == nil {
				onSeries(walSeries{ref, ls})
			}
		}
	case recSamples:
		n := d.uvarint()
		if n > maxWALRec {
			return errors.New("bad sample count")
		}
		out := make([]walSample, 0, n)
		for i := uint64(0); i < n && d.err == nil; i++ {
			out = append(out, walSample{ref: d.uvarint(), t: d.varint(), v: math.Float64frombits(d.u64())})
		}
		if d.err == nil {
			onSamples(out)
		}
	default:
		return fmt.Errorf("unknown record type %d", rec[0])
	}
	return d.err
}

func (w *wal) openSegment(seq int) error {
	f, err := os.OpenFile(filepath.Join(w.dir, walSegName(seq)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.f, w.w, w.seq = f, bufio.NewWriterSize(f, 128<<10), seq
	return nil
}

func (w *wal) writeRecord(rec []byte) error {
	w.buf = binary.AppendUvarint(w.buf[:0], uint64(len(rec)))
	w.buf = append(w.buf, rec...)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, crc32.Checksum(rec, castagnoli))
	_, err := w.w.Write(w.buf)
	return err
}

func encodeSeriesRecord(series []walSeries) []byte {
	b := []byte{recSeries}
	b = binary.AppendUvarint(b, uint64(len(series)))
	for _, s := range series {
		b = binary.AppendUvarint(b, s.ref)
		b = binary.AppendUvarint(b, uint64(len(s.lset)))
		for _, l := range s.lset {
			b = appendStr(b, l.Name)
			b = appendStr(b, l.Value)
		}
	}
	return b
}

func encodeSamplesRecord(samples []walSample) []byte {
	b := make([]byte, 0, 1+len(samples)*14)
	b = append(b, recSamples)
	b = binary.AppendUvarint(b, uint64(len(samples)))
	for _, s := range samples {
		b = binary.AppendUvarint(b, s.ref)
		b = binary.AppendVarint(b, s.t)
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(s.v))
	}
	return b
}

// log appends records for new series and accepted samples and flushes the
// buffer to the OS (fsync happens in sync()).
func (w *wal) log(series []walSeries, samples []walSample) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errors.New("wal closed")
	}
	if len(series) > 0 {
		if err := w.writeRecord(encodeSeriesRecord(series)); err != nil {
			return err
		}
	}
	const batch = 10_000
	for len(samples) > 0 {
		n := min(batch, len(samples))
		if err := w.writeRecord(encodeSamplesRecord(samples[:n])); err != nil {
			return err
		}
		samples = samples[n:]
	}
	return w.w.Flush()
}

func (w *wal) sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// checkpoint starts a new segment containing the full current head state
// and deletes all older segments.
func (w *wal) checkpoint(series []walSeries, samples []walSample) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	oldSeq := w.seq
	if err := w.openSegment(oldSeq + 1); err != nil {
		return err
	}
	if len(series) > 0 {
		if err := w.writeRecord(encodeSeriesRecord(series)); err != nil {
			return err
		}
	}
	for len(samples) > 0 {
		n := min(10_000, len(samples))
		if err := w.writeRecord(encodeSamplesRecord(samples[:n])); err != nil {
			return err
		}
		samples = samples[n:]
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	seqs, err := listWALSegments(w.dir)
	if err != nil {
		return err
	}
	for _, s := range seqs {
		if s <= oldSeq {
			if err := os.Remove(filepath.Join(w.dir, walSegName(s))); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := errors.Join(w.w.Flush(), w.f.Sync(), w.f.Close())
	w.f = nil
	return err
}

// size returns the total on-disk size of the WAL.
func (w *wal) size() int64 {
	seqs, _ := listWALSegments(w.dir)
	var total int64
	for _, s := range seqs {
		if st, err := os.Stat(filepath.Join(w.dir, walSegName(s))); err == nil {
			total += st.Size()
		}
	}
	return total
}

func appendStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

type byteDecoder struct {
	b   []byte
	err error
}

func (d *byteDecoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errors.New("bad uvarint")
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *byteDecoder) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errors.New("bad varint")
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *byteDecoder) u64() uint64 {
	if d.err != nil {
		return 0
	}
	if len(d.b) < 8 {
		d.err = errors.New("short read")
		return 0
	}
	v := binary.LittleEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

func (d *byteDecoder) str() string {
	n := d.uvarint()
	if d.err != nil {
		return ""
	}
	if n > uint64(len(d.b)) {
		d.err = errors.New("short string")
		return ""
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s
}

func (d *byteDecoder) bytes() []byte {
	n := d.uvarint()
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.b)) {
		d.err = errors.New("short bytes")
		return nil
	}
	out := d.b[:n]
	d.b = d.b[n:]
	return out
}
