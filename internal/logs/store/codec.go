// Package store persists log entries in time-partitioned, append-only
// segments, each paired with an inverted index (message terms plus
// field=value postings), and executes parsed queries against them.
//
// On-disk layout (per tenant):
//
//	<dir>/p-<partitionStartUnixSeconds>/<segmentID>.log   framed entry records
//	<dir>/p-<partitionStartUnixSeconds>/<segmentID>.idx   sealed segment index
//
// A segment without a valid .idx is recovered on open by scanning its .log
// (truncating a torn tail) and rebuilding the index.
package store

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/robertramos/developer-observability-stack/internal/logs"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// errCorrupt marks undecodable on-disk data.
var errCorrupt = errors.New("logstore: corrupt record")

// maxRecordSize bounds a single framed record when reading.
const maxRecordSize = 4 << 20

// encodeEntry appends the binary form of e to dst.
func encodeEntry(dst []byte, e *logs.Entry) []byte {
	dst = binary.AppendVarint(dst, e.Timestamp)
	dst = appendString(dst, e.Level)
	dst = appendString(dst, e.Message)
	dst = binary.AppendUvarint(dst, uint64(len(e.Fields)))
	for _, k := range e.SortedFieldKeys() {
		dst = appendString(dst, k)
		dst = appendString(dst, e.Fields[k])
	}
	return dst
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) str() string {
	n := d.uvarint()
	if d.err != nil {
		return ""
	}
	if n > uint64(len(d.b)) {
		d.err = errCorrupt
		return ""
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s
}

func decodeEntry(b []byte) (logs.Entry, error) {
	d := decoder{b: b}
	e := logs.Entry{Timestamp: d.varint(), Level: d.str(), Message: d.str()}
	n := d.uvarint()
	if d.err == nil && n > logs.MaxFields*2 {
		return e, errCorrupt
	}
	if n > 0 && d.err == nil {
		e.Fields = make(map[string]string, n)
		for i := uint64(0); i < n && d.err == nil; i++ {
			k := d.str()
			e.Fields[k] = d.str()
		}
	}
	return e, d.err
}

// frame wraps payload as uvarint(len) | payload | crc32c(payload).
func frame(dst, payload []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	dst = append(dst, payload...)
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(payload, castagnoli))
}

// readFrame reads one framed record from r. It returns the payload and the
// total number of bytes consumed. io.EOF means a clean end of stream; any
// other error indicates a torn or corrupt tail.
func readFrame(r *bufio.Reader, buf []byte) ([]byte, int, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, io.EOF
		}
		return nil, 0, errCorrupt
	}
	if n > maxRecordSize {
		return nil, 0, errCorrupt
	}
	hdr := uvarintLen(n)
	if cap(buf) < int(n)+4 {
		buf = make([]byte, int(n)+4)
	}
	buf = buf[:int(n)+4]
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, 0, errCorrupt
	}
	payload := buf[:n]
	if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(buf[n:]) {
		return nil, 0, errCorrupt
	}
	return payload, hdr + int(n) + 4, nil
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
