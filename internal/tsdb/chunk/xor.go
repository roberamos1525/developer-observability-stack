package chunk

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/bits"
)

// MaxSamples is the largest number of samples a chunk can hold (the count
// is stored in a 16-bit header).
const MaxSamples = math.MaxUint16

// ErrFull is returned when appending to a chunk at MaxSamples.
var ErrFull = errors.New("chunk: full")

// ErrCorrupt is returned for undecodable chunk bytes.
var ErrCorrupt = errors.New("chunk: corrupt data")

// XOR is a Gorilla-compressed chunk of (timestamp int64, value float64)
// samples. Layout: 2-byte big-endian sample count, then the bit stream.
//
// Sample 0: varint timestamp, raw 64-bit value.
// Sample 1: varint timestamp delta, XOR-encoded value.
// Sample n: delta-of-delta in one of five buckets
//
//	'0'                 dod == 0
//	'10'   + 14 bits    dod in [-8191, 8192]
//	'110'  + 17 bits    dod in [-65535, 65536]
//	'1110' + 20 bits    dod in [-524287, 524288]
//	'1111' + 64 bits    anything else
//
// and the value XORed with its predecessor:
//
//	'0'                                         identical value
//	'10' + meaningful bits                      fits the previous window
//	'11' + 5b leading + 6b length + bits        new window
//
// Timestamp arithmetic wraps like Go int64 arithmetic, so any int64
// sequence (including decreasing timestamps) round-trips exactly; values
// round-trip bit-for-bit, including NaN payloads and infinities.
type XOR struct {
	b bstream
}

// NewXOR returns an empty chunk.
func NewXOR() *XOR {
	return &XOR{b: bstream{stream: make([]byte, 2, 128)}}
}

// FromBytes wraps encoded chunk bytes (copied).
func FromBytes(b []byte) (*XOR, error) {
	if len(b) < 2 {
		return nil, ErrCorrupt
	}
	return &XOR{b: bstream{stream: append([]byte(nil), b...)}}, nil
}

// Bytes returns the encoded chunk. The slice must not be modified.
func (c *XOR) Bytes() []byte { return c.b.bytes() }

// NumSamples returns the number of samples in the chunk.
func (c *XOR) NumSamples() int { return int(binary.BigEndian.Uint16(c.b.stream)) }

// Size returns the encoded size in bytes.
func (c *XOR) Size() int { return len(c.b.stream) }

// Appender returns an appender positioned after the last sample. For a
// non-empty chunk the encoder state is rebuilt by re-encoding its samples
// into a fresh stream, which is exact because encoding is deterministic.
func (c *XOR) Appender() (*Appender, error) {
	if c.NumSamples() == 0 {
		return &Appender{c: c, leading: 0xff}, nil
	}
	fresh := NewXOR()
	a := &Appender{c: fresh, leading: 0xff}
	it := c.Iterator()
	for it.Next() {
		if err := a.Append(it.At()); err != nil {
			return nil, err
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}
	c.b = fresh.b
	a.c = c
	return a, nil
}

// Appender appends samples to an XOR chunk.
type Appender struct {
	c        *XOR
	t        int64
	v        float64
	tDelta   int64
	leading  uint8
	trailing uint8
}

// Append adds a sample. Callers enforce ordering; the encoding itself
// accepts arbitrary timestamps.
func (a *Appender) Append(t int64, v float64) error {
	num := a.c.NumSamples()
	if num >= MaxSamples {
		return ErrFull
	}
	b := &a.c.b
	switch num {
	case 0:
		var buf [binary.MaxVarintLen64]byte
		for _, x := range buf[:binary.PutVarint(buf[:], t)] {
			b.writeByte(x)
		}
		b.writeBits(math.Float64bits(v), 64)
	case 1:
		tDelta := t - a.t
		var buf [binary.MaxVarintLen64]byte
		for _, x := range buf[:binary.PutVarint(buf[:], tDelta)] {
			b.writeByte(x)
		}
		a.writeValue(v)
		a.tDelta = tDelta
	default:
		tDelta := t - a.t
		dod := tDelta - a.tDelta
		switch {
		case dod == 0:
			b.writeBit(false)
		case bitRange(dod, 14):
			b.writeBits(0b10, 2)
			b.writeBits(uint64(dod), 14)
		case bitRange(dod, 17):
			b.writeBits(0b110, 3)
			b.writeBits(uint64(dod), 17)
		case bitRange(dod, 20):
			b.writeBits(0b1110, 4)
			b.writeBits(uint64(dod), 20)
		default:
			b.writeBits(0b1111, 4)
			b.writeBits(uint64(dod), 64)
		}
		a.writeValue(v)
		a.tDelta = tDelta
	}
	a.t, a.v = t, v
	binary.BigEndian.PutUint16(b.stream, uint16(num+1))
	return nil
}

// bitRange reports whether x fits the signed nbits bucket.
func bitRange(x int64, nbits uint8) bool {
	return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}

func (a *Appender) writeValue(v float64) {
	b := &a.c.b
	delta := math.Float64bits(v) ^ math.Float64bits(a.v)
	if delta == 0 {
		b.writeBit(false)
		return
	}
	b.writeBit(true)
	leading := uint8(bits.LeadingZeros64(delta))
	trailing := uint8(bits.TrailingZeros64(delta))
	if leading >= 32 {
		leading = 31 // stored in 5 bits
	}
	if a.leading != 0xff && leading >= a.leading && trailing >= a.trailing {
		b.writeBit(false)
		b.writeBits(delta>>a.trailing, int(64-a.leading-a.trailing))
		return
	}
	a.leading, a.trailing = leading, trailing
	b.writeBit(true)
	b.writeBits(uint64(leading), 5)
	sig := 64 - leading - trailing
	// 64 significant bits do not fit 6 bits; 0 is used as its encoding
	// since a zero-length window is impossible for a non-zero delta.
	b.writeBits(uint64(sig&63), 6)
	b.writeBits(delta>>trailing, int(sig))
}

// Iterator decodes samples in order.
type Iterator struct {
	br       bstreamReader
	numTotal int
	numRead  int
	t        int64
	v        float64
	tDelta   int64
	leading  uint8
	trailing uint8
	err      error
}

// Iterator returns a fresh iterator over the chunk.
func (c *XOR) Iterator() *Iterator {
	return &Iterator{br: newReader(c.b.stream[2:]), numTotal: c.NumSamples()}
}

// At returns the current sample.
func (it *Iterator) At() (int64, float64) { return it.t, it.v }

// Err returns the decoding error, if any.
func (it *Iterator) Err() error { return it.err }

// Next advances to the next sample.
func (it *Iterator) Next() bool {
	if it.err != nil || it.numRead >= it.numTotal {
		return false
	}
	switch it.numRead {
	case 0:
		t, err := binary.ReadVarint(&it.br)
		if err != nil {
			return it.fail()
		}
		v, err := it.br.readBits(64)
		if err != nil {
			return it.fail()
		}
		it.t, it.v = t, math.Float64frombits(v)
	case 1:
		d, err := binary.ReadVarint(&it.br)
		if err != nil {
			return it.fail()
		}
		it.tDelta = d
		it.t += d
		if !it.readValue() {
			return false
		}
	default:
		var prefix uint8
		for i := 0; i < 4; i++ {
			bit, err := it.br.readBit()
			if err != nil {
				return it.fail()
			}
			if !bit {
				break
			}
			prefix++
		}
		var sz uint8
		switch prefix {
		case 0:
		case 1:
			sz = 14
		case 2:
			sz = 17
		case 3:
			sz = 20
		case 4:
			sz = 64
		}
		var dod int64
		if sz > 0 {
			raw, err := it.br.readBits(sz)
			if err != nil {
				return it.fail()
			}
			if sz == 64 {
				dod = int64(raw)
			} else {
				if raw > 1<<(sz-1) { // sign-extend
					raw -= 1 << sz
				}
				dod = int64(raw)
			}
		}
		it.tDelta += dod
		it.t += it.tDelta
		if !it.readValue() {
			return false
		}
	}
	it.numRead++
	return true
}

func (it *Iterator) readValue() bool {
	bit, err := it.br.readBit()
	if err != nil {
		return it.fail()
	}
	if !bit {
		return true // same value
	}
	bit, err = it.br.readBit()
	if err != nil {
		return it.fail()
	}
	if bit {
		lead, err := it.br.readBits(5)
		if err != nil {
			return it.fail()
		}
		sig, err := it.br.readBits(6)
		if err != nil {
			return it.fail()
		}
		if sig == 0 {
			sig = 64
		}
		if lead+sig > 64 {
			return it.fail()
		}
		it.leading = uint8(lead)
		it.trailing = uint8(64 - lead - sig)
	}
	sig := 64 - it.leading - it.trailing
	raw, err := it.br.readBits(sig)
	if err != nil {
		return it.fail()
	}
	it.v = math.Float64frombits(math.Float64bits(it.v) ^ raw<<it.trailing)
	return true
}

func (it *Iterator) fail() bool {
	if it.err == nil {
		it.err = ErrCorrupt
	}
	return false
}

var _ io.ByteReader = (*bstreamReader)(nil)
