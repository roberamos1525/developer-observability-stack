// Package postings implements set algebra over sorted, duplicate-free
// uint32 posting lists (entry ordinals within a log segment) and their
// compact delta-varint serialisation.
package postings

import (
	"encoding/binary"
	"errors"
)

// List is a strictly increasing sequence of ordinals.
type List []uint32

// Intersect returns a ∩ b.
func Intersect(a, b List) List {
	if len(a) > len(b) {
		a, b = b, a
	}
	out := make(List, 0, len(a))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			// Galloping would help for very skewed sizes; a linear merge
			// is adequate for segment-sized lists.
			j++
		}
	}
	return out
}

// Union returns a ∪ b.
func Union(a, b List) List {
	out := make(List, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// UnionAll merges many lists.
func UnionAll(lists []List) List {
	switch len(lists) {
	case 0:
		return nil
	case 1:
		return lists[0]
	}
	mid := len(lists) / 2
	return Union(UnionAll(lists[:mid]), UnionAll(lists[mid:]))
}

// Difference returns a \ b.
func Difference(a, b List) List {
	out := make(List, 0, len(a))
	j := 0
	for _, v := range a {
		for j < len(b) && b[j] < v {
			j++
		}
		if j < len(b) && b[j] == v {
			continue
		}
		out = append(out, v)
	}
	return out
}

// Range returns [0, n).
func Range(n uint32) List {
	out := make(List, n)
	for i := range out {
		out[i] = uint32(i)
	}
	return out
}

// Contains reports whether v is in l (binary search).
func (l List) Contains(v uint32) bool {
	lo, hi := 0, len(l)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if l[m] < v {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo < len(l) && l[lo] == v
}

// Encode appends the delta-varint encoding of l to dst.
func Encode(dst []byte, l List) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(l)))
	prev := uint32(0)
	for i, v := range l {
		if i == 0 {
			dst = binary.AppendUvarint(dst, uint64(v))
		} else {
			dst = binary.AppendUvarint(dst, uint64(v-prev))
		}
		prev = v
	}
	return dst
}

// ErrCorrupt reports an undecodable posting list.
var ErrCorrupt = errors.New("postings: corrupt encoding")

// Decode reads one list from b and returns it with the bytes consumed.
func Decode(b []byte) (List, int, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)) {
		return nil, 0, ErrCorrupt
	}
	off := k
	out := make(List, n)
	var prev uint64
	for i := uint64(0); i < n; i++ {
		d, k := binary.Uvarint(b[off:])
		if k <= 0 {
			return nil, 0, ErrCorrupt
		}
		off += k
		if i == 0 {
			prev = d
		} else {
			if d == 0 {
				return nil, 0, ErrCorrupt
			}
			prev += d
		}
		if prev > 0xFFFFFFFF {
			return nil, 0, ErrCorrupt
		}
		out[i] = uint32(prev)
	}
	return out, off, nil
}
