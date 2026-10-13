// Package chunk implements Gorilla-style compressed sample chunks:
// delta-of-delta encoded timestamps and XOR encoded float64 values
// (Pelkonen et al., "Gorilla: A Fast, Scalable, In-Memory Time Series
// Database", VLDB 2015), with the variable-width timestamp buckets used by
// Prometheus.
package chunk

import "io"

// bstream is an append-only bit stream.
type bstream struct {
	stream []byte
	count  uint8 // free bits in the last byte
}

func (b *bstream) bytes() []byte { return b.stream }

func (b *bstream) writeBit(bit bool) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}
	i := len(b.stream) - 1
	if bit {
		b.stream[i] |= 1 << (b.count - 1)
	}
	b.count--
}

func (b *bstream) writeByte(byt byte) {
	if b.count == 0 {
		b.stream = append(b.stream, byt)
		return
	}
	i := len(b.stream) - 1
	b.stream[i] |= byt >> (8 - b.count)
	b.stream = append(b.stream, byt<<b.count)
}

// writeBits writes the low nbits of u, most significant first.
func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= 64 - uint(nbits)
	for nbits >= 8 {
		b.writeByte(byte(u >> 56))
		u <<= 8
		nbits -= 8
	}
	for nbits > 0 {
		b.writeBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

// bstreamReader reads bits from a byte slice.
type bstreamReader struct {
	stream []byte
	pos    int // next byte to load into buffer
	buffer uint64
	valid  uint8 // number of valid bits in buffer (right-aligned)
}

func newReader(b []byte) bstreamReader { return bstreamReader{stream: b} }

func (r *bstreamReader) loadNext() bool {
	if r.pos >= len(r.stream) {
		return false
	}
	// Load up to 8 bytes at once.
	n := 0
	var buf uint64
	for n < 8 && r.pos < len(r.stream) {
		buf = buf<<8 | uint64(r.stream[r.pos])
		r.pos++
		n++
	}
	r.buffer = buf
	r.valid = uint8(n * 8)
	return true
}

func (r *bstreamReader) readBit() (bool, error) {
	if r.valid == 0 && !r.loadNext() {
		return false, io.EOF
	}
	r.valid--
	return (r.buffer>>r.valid)&1 == 1, nil
}

func (r *bstreamReader) readBits(nbits uint8) (uint64, error) {
	var v uint64
	for nbits > 0 {
		if r.valid == 0 && !r.loadNext() {
			return 0, io.EOF
		}
		take := nbits
		if take > r.valid {
			take = r.valid
		}
		shift := r.valid - take
		mask := uint64(1)<<take - 1
		if take == 64 {
			mask = ^uint64(0)
		}
		v = v<<take | (r.buffer>>shift)&mask
		r.valid -= take
		nbits -= take
	}
	return v, nil
}

func (r *bstreamReader) ReadByte() (byte, error) {
	v, err := r.readBits(8)
	return byte(v), err
}
