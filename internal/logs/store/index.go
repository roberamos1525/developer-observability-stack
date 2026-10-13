package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strings"

	"github.com/robertramos/developer-observability-stack/internal/logs"
	"github.com/robertramos/developer-observability-stack/internal/logs/postings"
	"github.com/robertramos/developer-observability-stack/internal/logs/query"
)

// segIndex is the inverted index of one segment. While a segment is active
// it is mutated by add(); after sealing it is immutable and has sorted key
// slices for prefix lookups.
type segIndex struct {
	n       uint32
	offsets []int64 // record start offset per ordinal
	ts      []int64 // timestamp per ordinal, lets queries skip reads
	minTs   int64
	maxTs   int64
	terms   map[string]postings.List
	fields  map[string]map[string]postings.List

	sortedTerms  []string
	sortedValues map[string][]string
}

func newSegIndex() *segIndex {
	return &segIndex{terms: map[string]postings.List{}, fields: map[string]map[string]postings.List{}}
}

func (ix *segIndex) add(e *logs.Entry, offset int64) {
	id := ix.n
	ix.n++
	ix.offsets = append(ix.offsets, offset)
	ix.ts = append(ix.ts, e.Timestamp)
	if id == 0 || e.Timestamp < ix.minTs {
		ix.minTs = e.Timestamp
	}
	if id == 0 || e.Timestamp > ix.maxTs {
		ix.maxTs = e.Timestamp
	}
	for _, t := range logs.Tokenize(e.Message) {
		l := ix.terms[t]
		if len(l) == 0 || l[len(l)-1] != id { // dedupe repeated tokens
			ix.terms[t] = append(l, id)
		}
	}
	if e.Level != "" {
		ix.addField("level", e.Level, id)
	}
	for k, v := range e.Fields {
		ix.addField(k, v, id)
	}
}

func (ix *segIndex) addField(k, v string, id uint32) {
	vals := ix.fields[k]
	if vals == nil {
		vals = map[string]postings.List{}
		ix.fields[k] = vals
	}
	key := query.IndexKey(v)
	vals[key] = append(vals[key], id)
}

// freeze prepares sorted key slices for prefix scans.
func (ix *segIndex) freeze() {
	ix.sortedTerms = sortedKeys(ix.terms)
	ix.sortedValues = make(map[string][]string, len(ix.fields))
	for f, vals := range ix.fields {
		ix.sortedValues[f] = sortedKeys(vals)
	}
}

func sortedKeys(m map[string]postings.List) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (ix *segIndex) Len() uint32 { return ix.n }

func (ix *segIndex) Term(t string) postings.List { return ix.terms[t] }

func (ix *segIndex) TermPrefix(p string) postings.List {
	return prefixUnion(ix.terms, ix.sortedTerms, p)
}

func (ix *segIndex) Field(name, value string) postings.List {
	return ix.fields[name][value]
}

func (ix *segIndex) FieldPrefix(name, p string) postings.List {
	vals := ix.fields[name]
	if vals == nil {
		return nil
	}
	return prefixUnion(vals, ix.sortedValues[name], p)
}

func prefixUnion(m map[string]postings.List, sorted []string, p string) postings.List {
	var lists []postings.List
	if sorted != nil {
		for i := sort.SearchStrings(sorted, p); i < len(sorted) && strings.HasPrefix(sorted[i], p); i++ {
			lists = append(lists, m[sorted[i]])
		}
	} else {
		for k, l := range m {
			if strings.HasPrefix(k, p) {
				lists = append(lists, l)
			}
		}
	}
	return postings.UnionAll(lists)
}

// FieldNames returns the indexed field names with their distinct value counts.
func (ix *segIndex) FieldNames() map[string]int {
	out := make(map[string]int, len(ix.fields))
	for f, vals := range ix.fields {
		out[f] = len(vals)
	}
	return out
}

const idxMagic = "OBSLIDX1"

// marshal serialises the index. Layout: magic | n | minTs | maxTs |
// offsets (delta uvarint) | terms | fields | crc32c(all previous bytes).
func (ix *segIndex) marshal() []byte {
	b := []byte(idxMagic)
	b = binary.AppendUvarint(b, uint64(ix.n))
	b = binary.AppendVarint(b, ix.minTs)
	b = binary.AppendVarint(b, ix.maxTs)
	var prev int64
	for _, off := range ix.offsets {
		b = binary.AppendUvarint(b, uint64(off-prev))
		prev = off
	}
	for _, ts := range ix.ts {
		b = binary.AppendVarint(b, ts-ix.minTs)
	}
	terms := sortedKeys(ix.terms)
	b = binary.AppendUvarint(b, uint64(len(terms)))
	for _, t := range terms {
		b = appendString(b, t)
		b = postings.Encode(b, ix.terms[t])
	}
	names := make([]string, 0, len(ix.fields))
	for f := range ix.fields {
		names = append(names, f)
	}
	sort.Strings(names)
	b = binary.AppendUvarint(b, uint64(len(names)))
	for _, f := range names {
		b = appendString(b, f)
		vals := sortedKeys(ix.fields[f])
		b = binary.AppendUvarint(b, uint64(len(vals)))
		for _, v := range vals {
			b = appendString(b, v)
			b = postings.Encode(b, ix.fields[f][v])
		}
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func unmarshalIndex(b []byte) (*segIndex, error) {
	if len(b) < len(idxMagic)+4 || string(b[:len(idxMagic)]) != idxMagic {
		return nil, fmt.Errorf("index: bad magic")
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return nil, fmt.Errorf("index: checksum mismatch")
	}
	d := decoder{b: body[len(idxMagic):]}
	ix := newSegIndex()
	n := d.uvarint()
	if n > 1<<31 {
		return nil, errCorrupt
	}
	ix.n = uint32(n)
	ix.minTs, ix.maxTs = d.varint(), d.varint()
	ix.offsets = make([]int64, 0, n)
	var off int64
	for i := uint64(0); i < n && d.err == nil; i++ {
		off += int64(d.uvarint())
		ix.offsets = append(ix.offsets, off)
	}
	ix.ts = make([]int64, 0, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		ix.ts = append(ix.ts, ix.minTs+d.varint())
	}
	readList := func() postings.List {
		if d.err != nil {
			return nil
		}
		l, k, err := postings.Decode(d.b)
		if err != nil {
			d.err = err
			return nil
		}
		d.b = d.b[k:]
		for _, id := range l {
			if id >= ix.n {
				d.err = errCorrupt
			}
		}
		return l
	}
	nt := d.uvarint()
	for i := uint64(0); i < nt && d.err == nil; i++ {
		t := d.str()
		ix.terms[t] = readList()
	}
	nf := d.uvarint()
	for i := uint64(0); i < nf && d.err == nil; i++ {
		f := d.str()
		nv := d.uvarint()
		vals := make(map[string]postings.List, min(nv, 1<<16))
		for j := uint64(0); j < nv && d.err == nil; j++ {
			v := d.str()
			vals[v] = readList()
		}
		ix.fields[f] = vals
	}
	if d.err != nil {
		return nil, d.err
	}
	ix.freeze()
	return ix, nil
}

// writeFileAtomic writes data to path via a synced temp file and rename.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
