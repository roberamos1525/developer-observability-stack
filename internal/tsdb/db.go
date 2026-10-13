package tsdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// Options configure a DB. Durations are converted to milliseconds.
type Options struct {
	Dir                  string
	BlockDuration        time.Duration
	ChunkMaxSamples      int
	RawRetention         time.Duration
	DownsampleResolution time.Duration
	DownsampleAfter      time.Duration
	DownsampledRetention time.Duration
	Logger               *slog.Logger
}

func (o *Options) defaults() {
	if o.BlockDuration <= 0 {
		o.BlockDuration = 2 * time.Hour
	}
	if o.ChunkMaxSamples <= 0 {
		o.ChunkMaxSamples = 120
	}
	if o.RawRetention <= 0 {
		o.RawRetention = 15 * 24 * time.Hour
	}
	if o.DownsampleResolution <= 0 {
		o.DownsampleResolution = 5 * time.Minute
	}
	if o.DownsampledRetention <= 0 {
		o.DownsampledRetention = 90 * 24 * time.Hour
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
}

// DB is a tenant's time series database.
type DB struct {
	opts     Options
	blockDur int64

	appendMu sync.Mutex // serialises Append against block cuts
	head     *Head
	wal      *wal

	mu       sync.RWMutex
	blocks   []*Block // raw, sorted by MinT
	dsBlocks []*Block // downsampled, sorted by MinT
	closed   bool
}

// AppendStats summarises one Append call.
type AppendStats struct {
	Accepted   int `json:"accepted"`
	Duplicates int `json:"duplicates"`
	OutOfOrder int `json:"out_of_order"`
	TooOld     int `json:"too_old"`
	Invalid    int `json:"invalid"`
	// FirstError describes the first rejected sample, if any.
	FirstError string `json:"first_error,omitempty"`
}

// Rejected returns the number of rejected samples.
func (s AppendStats) Rejected() int { return s.Duplicates + s.OutOfOrder + s.TooOld + s.Invalid }

// Open opens the database, loading blocks and replaying the WAL.
func Open(opts Options) (*DB, error) {
	opts.defaults()
	db := &DB{opts: opts, blockDur: opts.BlockDuration.Milliseconds()}
	db.head = newHead(opts.ChunkMaxSamples, db.blockDur)
	for _, d := range []string{"blocks", "wal"} {
		if err := os.MkdirAll(filepath.Join(opts.Dir, d), 0o755); err != nil {
			return nil, err
		}
	}
	if err := db.loadBlocks(); err != nil {
		return nil, err
	}
	minValid := db.minValidTime()
	replayed := 0
	w, err := openWAL(filepath.Join(opts.Dir, "wal"),
		func(s walSeries) {
			db.head.mu.Lock()
			if _, exists := db.head.byRef[s.ref]; !exists {
				db.head.createLocked(s.ref, s.lset)
			}
			db.head.mu.Unlock()
		},
		func(samples []walSample) {
			for _, s := range samples {
				if s.t < minValid {
					continue
				}
				db.head.mu.RLock()
				ms := db.head.byRef[s.ref]
				db.head.mu.RUnlock()
				if ms == nil {
					continue
				}
				if ok, _ := ms.append(s.t, s.v, opts.ChunkMaxSamples, db.blockDur); ok {
					db.head.observe(s.t)
					replayed++
				}
			}
		})
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	db.wal = w
	db.head.dropEmptySeries()
	if replayed > 0 {
		opts.Logger.Info("replayed metrics WAL", "dir", opts.Dir, "samples", replayed, "series", db.head.numSeries())
	}
	return db, nil
}

// dropEmptySeries removes series that received no samples (e.g. all of
// their WAL samples were already persisted in blocks).
func (h *Head) dropEmptySeries() {
	h.mu.Lock()
	var empty bool
	for _, s := range h.series {
		if len(s.chunks) == 0 {
			empty = true
			break
		}
	}
	h.mu.Unlock()
	if empty {
		// truncateBefore with the minimum time drops only empty series.
		h.truncateBefore(math.MinInt64)
	}
}

func (db *DB) loadBlocks() error {
	dir := filepath.Join(db.opts.Dir, "blocks")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if strings.HasSuffix(e.Name(), ".tmp") {
			os.RemoveAll(path) // interrupted write
			continue
		}
		b, err := openBlock(path)
		if err != nil {
			db.opts.Logger.Error("skipping unreadable block", "dir", path, "err", err)
			continue
		}
		if b.meta.Resolution == 0 {
			db.blocks = append(db.blocks, b)
		} else {
			db.dsBlocks = append(db.dsBlocks, b)
		}
	}
	sortBlocks(db.blocks)
	sortBlocks(db.dsBlocks)
	return nil
}

func sortBlocks(bs []*Block) {
	sort.Slice(bs, func(i, j int) bool { return bs[i].meta.MinT < bs[j].meta.MinT })
}

// minValidTime is the first timestamp the head may accept: samples
// earlier than the newest raw block's end would overlap immutable data.
func (db *DB) minValidTime() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(db.blocks) == 0 {
		return math.MinInt64
	}
	maxT := int64(math.MinInt64)
	for _, b := range db.blocks {
		maxT = max(maxT, b.meta.MaxT)
	}
	return maxT
}

// Append ingests samples. Invalid or out-of-order samples are counted and
// skipped; the error return is reserved for storage failures.
func (db *DB) Append(samples []LabeledSample) (AppendStats, error) {
	var st AppendStats
	db.appendMu.Lock()
	defer db.appendMu.Unlock()
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return st, errors.New("tsdb: closed")
	}
	minValid := db.minValidTime()
	reject := func(counter *int, err error, s LabeledSample) {
		*counter++
		if st.FirstError == "" {
			st.FirstError = fmt.Sprintf("%s @%d: %v", s.Labels, s.T, err)
		}
	}
	var newSeries []walSeries
	accepted := make([]walSample, 0, len(samples))
	minT, maxT := int64(math.MaxInt64), int64(math.MinInt64)
	for _, s := range samples {
		if err := s.Labels.Validate(); err != nil {
			reject(&st.Invalid, err, s)
			continue
		}
		if s.T < minValid {
			reject(&st.TooOld, ErrTooOld, s)
			continue
		}
		ms, created := db.head.getOrCreate(s.Labels)
		if created {
			newSeries = append(newSeries, walSeries{ms.ref, s.Labels})
		}
		ok, err := ms.append(s.T, s.V, db.opts.ChunkMaxSamples, db.blockDur)
		switch {
		case errors.Is(err, ErrOutOfOrder):
			reject(&st.OutOfOrder, err, s)
		case errors.Is(err, ErrDuplicate):
			reject(&st.Duplicates, err, s)
		case err != nil:
			return st, err
		case ok:
			st.Accepted++
			accepted = append(accepted, walSample{ms.ref, s.T, s.V})
			minT, maxT = min(minT, s.T), max(maxT, s.T)
		}
	}
	if st.Accepted > 0 {
		db.head.observe(minT)
		db.head.observe(maxT)
	}
	if len(newSeries) > 0 || len(accepted) > 0 {
		if err := db.wal.log(newSeries, accepted); err != nil {
			return st, fmt.Errorf("wal: %w", err)
		}
	}
	return st, nil
}

// Select returns series matching ms with samples in [mint, maxt] (ms).
// Raw blocks and the head are merged; time ranges no longer covered by raw
// data are served from downsampled blocks using their "last" aggregate.
func (db *DB) Select(ctx context.Context, ms []*labels.Matcher, mint, maxt int64) ([]Series, error) {
	if len(ms) == 0 {
		return nil, errors.New("at least one matcher is required")
	}
	db.mu.RLock()
	blocks := append([]*Block(nil), db.blocks...)
	ds := append([]*Block(nil), db.dsBlocks...)
	db.mu.RUnlock()

	rawStart := int64(math.MaxInt64)
	if len(blocks) > 0 {
		rawStart = blocks[0].meta.MinT
	}
	if hmin, _, ok := db.head.bounds(); ok {
		rawStart = min(rawStart, hmin)
	}
	merged := map[string]*Series{}
	var order []string
	add := func(series []Series) {
		for _, s := range series {
			k := s.Labels.Key()
			if m, ok := merged[k]; ok {
				m.Samples = append(m.Samples, s.Samples...)
				continue
			}
			cp := s
			merged[k] = &cp
			order = append(order, k)
		}
	}
	if mint < rawStart {
		for _, b := range ds {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			add(b.selectSeries(ms, mint, min(maxt, rawStart-1), AggrLast))
		}
	}
	for _, b := range blocks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		add(b.selectSeries(ms, mint, maxt, AggrRaw))
	}
	add(db.head.selectSeries(ms, mint, maxt))
	out := make([]Series, 0, len(order))
	for _, k := range order {
		s := merged[k]
		s.Samples = sortDedupeSamples(s.Samples)
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Labels.Key() < out[j].Labels.Key() })
	return out, nil
}

func sortDedupeSamples(s []Sample) []Sample {
	less := func(i, j int) bool { return s[i].T < s[j].T }
	if !sort.SliceIsSorted(s, less) {
		sort.SliceStable(s, less)
	}
	out := s[:0]
	for i, x := range s {
		if i > 0 && x.T == s[i-1].T {
			continue
		}
		out = append(out, x)
	}
	return out
}

// SelectDownsampled returns one aggregate (count, sum, min, max, last, or
// the derived avg) from downsampled blocks only.
func (db *DB) SelectDownsampled(ctx context.Context, ms []*labels.Matcher, mint, maxt int64, aggr string) ([]Series, error) {
	if len(ms) == 0 {
		return nil, errors.New("at least one matcher is required")
	}
	db.mu.RLock()
	ds := append([]*Block(nil), db.dsBlocks...)
	db.mu.RUnlock()
	collect := func(a string) map[string]*Series {
		out := map[string]*Series{}
		for _, b := range ds {
			for _, s := range b.selectSeries(ms, mint, maxt, a) {
				k := s.Labels.Key()
				if m, ok := out[k]; ok {
					m.Samples = append(m.Samples, s.Samples...)
				} else {
					cp := s
					out[k] = &cp
				}
			}
		}
		return out
	}
	var res map[string]*Series
	switch aggr {
	case AggrCount, AggrSum, AggrMin, AggrMax, AggrLast:
		res = collect(aggr)
	case "avg":
		sums, counts := collect(AggrSum), collect(AggrCount)
		res = sums
		for k, s := range sums {
			c := counts[k]
			if c == nil || len(c.Samples) != len(s.Samples) {
				delete(res, k)
				continue
			}
			for i := range s.Samples {
				s.Samples[i].V /= c.Samples[i].V
			}
		}
	default:
		return nil, fmt.Errorf("unknown aggregate %q", aggr)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]Series, 0, len(res))
	for _, s := range res {
		s.Samples = sortDedupeSamples(s.Samples)
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Labels.Key() < out[j].Labels.Key() })
	return out, nil
}

// LabelNames returns all label names.
func (db *DB) LabelNames() []string {
	set := map[string]struct{}{}
	db.head.mu.RLock()
	for n := range db.head.postings {
		set[n] = struct{}{}
	}
	db.head.mu.RUnlock()
	db.mu.RLock()
	for _, b := range append(append([]*Block(nil), db.blocks...), db.dsBlocks...) {
		b.labelNames(set)
	}
	db.mu.RUnlock()
	return sortedSet(set)
}

// LabelValues returns all values of a label.
func (db *DB) LabelValues(name string) []string {
	set := map[string]struct{}{}
	db.head.mu.RLock()
	for v := range db.head.postings[name] {
		set[v] = struct{}{}
	}
	db.head.mu.RUnlock()
	db.mu.RLock()
	for _, b := range append(append([]*Block(nil), db.blocks...), db.dsBlocks...) {
		b.labelValues(name, set)
	}
	db.mu.RUnlock()
	return sortedSet(set)
}

func sortedSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// SeriesLabels lists distinct label sets matching ms with data in range.
func (db *DB) SeriesLabels(ctx context.Context, ms []*labels.Matcher, mint, maxt int64) ([]labels.Labels, error) {
	series, err := db.Select(ctx, ms, mint, maxt)
	if err != nil {
		return nil, err
	}
	out := make([]labels.Labels, len(series))
	for i, s := range series {
		out[i] = s.Labels
	}
	return out, nil
}

// Stats describes the database.
type Stats struct {
	HeadSeries        int         `json:"head_series"`
	HeadMinT          int64       `json:"head_min_t"`
	HeadMaxT          int64       `json:"head_max_t"`
	WALBytes          int64       `json:"wal_bytes"`
	Blocks            []BlockMeta `json:"blocks"`
	DownsampledBlocks []BlockMeta `json:"downsampled_blocks"`
}

// Stats returns database statistics.
func (db *DB) Stats() Stats {
	minT, maxT, _ := db.head.bounds()
	st := Stats{HeadSeries: db.head.numSeries(), HeadMinT: minT, HeadMaxT: maxT, WALBytes: db.wal.size()}
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, b := range db.blocks {
		st.Blocks = append(st.Blocks, b.meta)
	}
	for _, b := range db.dsBlocks {
		st.DownsampledBlocks = append(st.DownsampledBlocks, b.meta)
	}
	return st
}

// Sync fsyncs the WAL.
func (db *DB) Sync() error { return db.wal.sync() }

// Close flushes and closes the database.
func (db *DB) Close() error {
	db.appendMu.Lock()
	defer db.appendMu.Unlock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()
	return db.wal.close()
}
