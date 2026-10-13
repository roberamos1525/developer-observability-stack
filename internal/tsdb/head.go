// Package tsdb is an embedded time series database: an in-memory head of
// Gorilla-compressed chunks protected by a write-ahead log, immutable
// on-disk blocks cut from the head, downsampled blocks, and retention.
package tsdb

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/robertramos/developer-observability-stack/internal/tsdb/chunk"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// Sample is a timestamped value; T is Unix milliseconds.
type Sample struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

// Series is a label set with samples in increasing time order.
type Series struct {
	Labels  labels.Labels
	Samples []Sample
}

// LabeledSample is the unit of ingestion.
type LabeledSample struct {
	Labels labels.Labels
	T      int64
	V      float64
}

// Append errors.
var (
	ErrOutOfOrder = errors.New("out of order sample")
	ErrDuplicate  = errors.New("duplicate sample with different value")
	ErrTooOld     = errors.New("sample older than the newest persisted block")
)

type memChunk struct {
	c          *chunk.XOR
	app        *chunk.Appender
	minT, maxT int64
}

type memSeries struct {
	mu     sync.Mutex
	ref    uint64
	lset   labels.Labels
	chunks []*memChunk
	lastT  int64
	lastV  float64
	has    bool
}

// append adds a sample; chunks are cut at blockDur-aligned boundaries so
// head truncation after a block cut is exact at chunk granularity.
func (s *memSeries) append(t int64, v float64, maxSamples int, blockDur int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.has {
		if t < s.lastT {
			return false, ErrOutOfOrder
		}
		if t == s.lastT {
			if math.Float64bits(v) == math.Float64bits(s.lastV) {
				return false, nil // idempotent re-delivery
			}
			return false, ErrDuplicate
		}
	}
	var hc *memChunk
	if n := len(s.chunks); n > 0 {
		hc = s.chunks[n-1]
		if hc.c.NumSamples() >= maxSamples || floorDiv(t, blockDur) != floorDiv(hc.minT, blockDur) {
			hc = nil
		}
	}
	if hc == nil {
		c := chunk.NewXOR()
		app, err := c.Appender()
		if err != nil {
			return false, err
		}
		hc = &memChunk{c: c, app: app, minT: t}
		s.chunks = append(s.chunks, hc)
	}
	if err := hc.app.Append(t, v); err != nil {
		return false, err
	}
	hc.maxT = t
	s.lastT, s.lastV, s.has = t, v, true
	return true, nil
}

func (s *memSeries) samples(mint, maxt int64) []Sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Sample
	for _, c := range s.chunks {
		if c.maxT < mint || c.minT > maxt {
			continue
		}
		out = appendChunkSamples(out, c.c, mint, maxt)
	}
	return out
}

func appendChunkSamples(out []Sample, c *chunk.XOR, mint, maxt int64) []Sample {
	it := c.Iterator()
	for it.Next() {
		t, v := it.At()
		if t < mint {
			continue
		}
		if t > maxt {
			break
		}
		out = append(out, Sample{t, v})
	}
	return out
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// Head is the mutable in-memory part of the database.
type Head struct {
	mu       sync.RWMutex
	series   map[string]*memSeries
	byRef    map[uint64]*memSeries
	postings map[string]map[string][]uint64
	nextRef  uint64

	chunkMaxSamples int
	blockDur        int64
	minT, maxT      int64
	empty           bool
}

func newHead(chunkMaxSamples int, blockDur int64) *Head {
	return &Head{
		series: map[string]*memSeries{}, byRef: map[uint64]*memSeries{},
		postings: map[string]map[string][]uint64{}, nextRef: 1,
		chunkMaxSamples: chunkMaxSamples, blockDur: blockDur, empty: true,
	}
}

// getOrCreate returns the series for lset, creating it if needed.
func (h *Head) getOrCreate(lset labels.Labels) (*memSeries, bool) {
	key := lset.Key()
	h.mu.RLock()
	s := h.series[key]
	h.mu.RUnlock()
	if s != nil {
		return s, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if s = h.series[key]; s != nil {
		return s, false
	}
	s = h.createLocked(h.nextRef, lset)
	return s, true
}

func (h *Head) createLocked(ref uint64, lset labels.Labels) *memSeries {
	s := &memSeries{ref: ref, lset: lset}
	h.series[lset.Key()] = s
	h.byRef[ref] = s
	if ref >= h.nextRef {
		h.nextRef = ref + 1
	}
	for _, l := range lset {
		vals := h.postings[l.Name]
		if vals == nil {
			vals = map[string][]uint64{}
			h.postings[l.Name] = vals
		}
		vals[l.Value] = insertSorted(vals[l.Value], ref)
	}
	return s
}

func insertSorted(l []uint64, v uint64) []uint64 {
	i := sort.Search(len(l), func(i int) bool { return l[i] >= v })
	if i < len(l) && l[i] == v {
		return l
	}
	l = append(l, 0)
	copy(l[i+1:], l[i:])
	l[i] = v
	return l
}

func (h *Head) observe(t int64) {
	h.mu.Lock()
	if h.empty || t < h.minT {
		h.minT = t
	}
	if h.empty || t > h.maxT {
		h.maxT = t
	}
	h.empty = false
	h.mu.Unlock()
}

// bounds returns the time range covered by the head.
func (h *Head) bounds() (int64, int64, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.minT, h.maxT, !h.empty
}

// selectSeries returns series matching ms with samples in [mint, maxt].
func (h *Head) selectSeries(ms []*labels.Matcher, mint, maxt int64) []Series {
	h.mu.RLock()
	refs := postingsForMatchers(h.postings, func() []uint64 { return sortedRefs(h.byRef) }, ms)
	cands := make([]*memSeries, 0, len(refs))
	for _, r := range refs {
		if s := h.byRef[r]; s != nil && labels.MatchesAll(s.lset, ms) {
			cands = append(cands, s)
		}
	}
	h.mu.RUnlock()
	out := make([]Series, 0, len(cands))
	for _, s := range cands {
		if smp := s.samples(mint, maxt); len(smp) > 0 {
			out = append(out, Series{Labels: s.lset, Samples: smp})
		}
	}
	return out
}

func sortedRefs[T any](m map[uint64]T) []uint64 {
	out := make([]uint64, 0, len(m))
	for r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// truncateBefore drops chunks entirely before t and series left empty.
// It returns the number of series removed.
func (h *Head) truncateBefore(t int64) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	removed := 0
	newMin, first := int64(0), true
	for key, s := range h.series {
		s.mu.Lock()
		keep := s.chunks[:0]
		for _, c := range s.chunks {
			if c.maxT >= t {
				keep = append(keep, c)
				if first || c.minT < newMin {
					newMin, first = c.minT, false
				}
			}
		}
		for i := len(keep); i < len(s.chunks); i++ {
			s.chunks[i] = nil
		}
		s.chunks = keep
		empty := len(keep) == 0
		s.mu.Unlock()
		if empty {
			delete(h.series, key)
			delete(h.byRef, s.ref)
			removed++
		}
	}
	if removed > 0 {
		for name, vals := range h.postings {
			for v, refs := range vals {
				kept := refs[:0]
				for _, r := range refs {
					if _, ok := h.byRef[r]; ok {
						kept = append(kept, r)
					}
				}
				if len(kept) == 0 {
					delete(vals, v)
				} else {
					vals[v] = kept
				}
			}
			if len(vals) == 0 {
				delete(h.postings, name)
			}
		}
	}
	if first {
		h.empty = true
		h.minT, h.maxT = 0, 0
	} else {
		h.minT = newMin
	}
	return removed
}

// numSeries returns the number of live series.
func (h *Head) numSeries() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.series)
}

// forEach calls fn for every series (snapshot).
func (h *Head) forEach(fn func(s *memSeries)) {
	h.mu.RLock()
	all := make([]*memSeries, 0, len(h.byRef))
	for _, r := range sortedRefs(h.byRef) {
		all = append(all, h.byRef[r])
	}
	h.mu.RUnlock()
	for _, s := range all {
		fn(s)
	}
}

// postingsForMatchers narrows candidates using the inverted index. Matchers
// that accept the empty string cannot be resolved through the index (they
// also match series lacking the label) and are left to the final filter.
func postingsForMatchers(idx map[string]map[string][]uint64, all func() []uint64, ms []*labels.Matcher) []uint64 {
	var result []uint64
	narrowed := false
	for _, m := range ms {
		if m.Matches("") {
			continue
		}
		var union []uint64
		if m.Type == labels.MatchEqual {
			union = idx[m.Name][m.Value]
		} else {
			for v, refs := range idx[m.Name] {
				if m.Matches(v) {
					union = append(union, refs...)
				}
			}
			union = sortDedupe(union)
		}
		if !narrowed {
			result, narrowed = union, true
		} else {
			result = intersectSorted(result, union)
		}
		if len(result) == 0 {
			return nil
		}
	}
	if !narrowed {
		return all()
	}
	return result
}

func sortDedupe(l []uint64) []uint64 {
	sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
	out := l[:0]
	for i, v := range l {
		if i == 0 || v != l[i-1] {
			out = append(out, v)
		}
	}
	return out
}

func intersectSorted(a, b []uint64) []uint64 {
	out := make([]uint64, 0, min(len(a), len(b)))
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
			j++
		}
	}
	return out
}
