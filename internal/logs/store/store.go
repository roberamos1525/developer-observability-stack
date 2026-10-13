package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/logs"
)

// Options configure a Store.
type Options struct {
	Dir                string
	PartitionDuration  time.Duration
	SegmentMaxBytes    int64
	SegmentMaxEntries  int
	MaxActiveSegments  int
	CompactMinSegments int
	Logger             *slog.Logger
}

func (o *Options) defaults() {
	if o.PartitionDuration <= 0 {
		o.PartitionDuration = time.Hour
	}
	if o.SegmentMaxBytes <= 0 {
		o.SegmentMaxBytes = 32 << 20
	}
	if o.SegmentMaxEntries <= 0 {
		o.SegmentMaxEntries = 200_000
	}
	if o.MaxActiveSegments <= 0 {
		o.MaxActiveSegments = 8
	}
	if o.CompactMinSegments < 2 {
		o.CompactMinSegments = 4
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
}

type partition struct {
	start, end int64
	dir        string
	segments   []*segment // ordered by id; guarded by Store.mu
	active     *segment   // guarded by Store.appendMu
	lastWrite  time.Time
}

// Store is a tenant's log storage.
type Store struct {
	opts       Options
	mu         sync.RWMutex // guards partitions and partition.segments
	partitions map[int64]*partition
	appendMu   sync.Mutex // serialises writers, compaction swaps and retention
	nextID     atomic.Uint64
	closed     bool
}

// Open opens (or creates) a store, recovering unsealed segments.
func Open(opts Options) (*Store, error) {
	opts.defaults()
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{opts: opts, partitions: map[int64]*partition{}}
	dirs, err := os.ReadDir(opts.Dir)
	if err != nil {
		return nil, err
	}
	for _, d := range dirs {
		if !d.IsDir() || !strings.HasPrefix(d.Name(), "p-") {
			continue
		}
		sec, err := strconv.ParseInt(strings.TrimPrefix(d.Name(), "p-"), 10, 64)
		if err != nil {
			continue
		}
		p, err := s.openPartition(sec*int64(time.Second), filepath.Join(opts.Dir, d.Name()))
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("open partition %s: %w", d.Name(), err)
		}
		s.partitions[p.start] = p
	}
	return s, nil
}

func (s *Store) openPartition(start int64, dir string) (*partition, error) {
	p := &partition{start: start, end: start + int64(s.opts.PartitionDuration), dir: dir}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		name := f.Name()
		if strings.HasSuffix(name, ".tmp") {
			os.Remove(filepath.Join(dir, name)) // interrupted index write
			continue
		}
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(name, ".log"), 16, 64)
		if err != nil {
			continue
		}
		seg, recovered, err := openSegment(dir, id)
		if err != nil {
			return nil, err
		}
		if recovered {
			s.opts.Logger.Info("recovered log segment", "segment", seg.base, "entries", seg.ix.n)
		}
		if seg.ix.n == 0 {
			seg.remove()
			continue
		}
		p.segments = append(p.segments, seg)
		for {
			cur := s.nextID.Load()
			if id <= cur || s.nextID.CompareAndSwap(cur, id) {
				break
			}
		}
	}
	sort.Slice(p.segments, func(i, j int) bool { return p.segments[i].id < p.segments[j].id })
	return p, nil
}

func (s *Store) partitionStart(ts int64) int64 {
	d := int64(s.opts.PartitionDuration)
	return ts - ts%d
}

// Append persists entries. Entries are routed to the partition covering
// their timestamp; each partition has at most one active segment.
func (s *Store) Append(entries []logs.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	if s.isClosed() {
		return errors.New("logstore: closed")
	}
	touched := map[*segment]bool{}
	var scratch []byte
	now := time.Now()
	for i := range entries {
		e := &entries[i]
		p, err := s.partitionFor(e.Timestamp)
		if err != nil {
			return err
		}
		seg, err := s.activeSegment(p)
		if err != nil {
			return err
		}
		if scratch, err = seg.append(e, scratch); err != nil {
			return err
		}
		p.lastWrite = now
		touched[seg] = true
	}
	var errs []error
	for seg := range touched {
		errs = append(errs, seg.flush())
	}
	errs = append(errs, s.limitActive())
	return errors.Join(errs...)
}

func (s *Store) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func (s *Store) partitionFor(ts int64) (*partition, error) {
	start := s.partitionStart(ts)
	s.mu.RLock()
	p := s.partitions[start]
	s.mu.RUnlock()
	if p != nil {
		return p, nil
	}
	dir := filepath.Join(s.opts.Dir, fmt.Sprintf("p-%d", start/int64(time.Second)))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p = &partition{start: start, end: start + int64(s.opts.PartitionDuration), dir: dir}
	s.mu.Lock()
	s.partitions[start] = p
	s.mu.Unlock()
	return p, nil
}

// activeSegment returns p's writable segment, rolling it when full.
// Caller holds appendMu.
func (s *Store) activeSegment(p *partition) (*segment, error) {
	if a := p.active; a != nil {
		n, size, _, _, _ := a.stats()
		if size < s.opts.SegmentMaxBytes && int(n) < s.opts.SegmentMaxEntries {
			return a, nil
		}
		if err := a.seal(); err != nil {
			return nil, err
		}
		p.active = nil
	}
	seg, err := createSegment(p.dir, s.nextID.Add(1))
	if err != nil {
		return nil, err
	}
	p.active = seg
	s.mu.Lock()
	p.segments = append(p.segments, seg)
	s.mu.Unlock()
	return seg, nil
}

// limitActive seals the least recently written active segments when too
// many partitions are open for writing (e.g. a burst of late data).
func (s *Store) limitActive() error {
	var open []*partition
	s.mu.RLock()
	for _, p := range s.partitions {
		if p.active != nil {
			open = append(open, p)
		}
	}
	s.mu.RUnlock()
	if len(open) <= s.opts.MaxActiveSegments {
		return nil
	}
	sort.Slice(open, func(i, j int) bool { return open[i].lastWrite.Before(open[j].lastWrite) })
	var errs []error
	for _, p := range open[:len(open)-s.opts.MaxActiveSegments] {
		errs = append(errs, p.active.seal())
		p.active = nil
	}
	return errors.Join(errs...)
}

// SealIdle seals active segments of partitions that have ended, so they
// become eligible for compaction. Returns the number sealed.
func (s *Store) SealIdle(now time.Time) (int, error) {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	n := 0
	var errs []error
	s.mu.RLock()
	parts := s.sortedPartitions()
	s.mu.RUnlock()
	for _, p := range parts {
		if p.active != nil && p.end <= now.UnixNano() && now.Sub(p.lastWrite) > time.Second {
			errs = append(errs, p.active.seal())
			p.active = nil
			n++
		}
	}
	return n, errors.Join(errs...)
}

func (s *Store) sortedPartitions() []*partition {
	out := make([]*partition, 0, len(s.partitions))
	for _, p := range s.partitions {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].start < out[j].start })
	return out
}

// ApplyRetention deletes partitions that ended at or before cutoff
// (Unix ns). Returns the number of partitions removed.
func (s *Store) ApplyRetention(cutoff int64) (int, error) {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.mu.Lock()
	var doomed []*partition
	for start, p := range s.partitions {
		if p.end <= cutoff {
			doomed = append(doomed, p)
			delete(s.partitions, start)
		}
	}
	s.mu.Unlock()
	for _, p := range doomed {
		for _, seg := range p.segments {
			seg.remove()
		}
		p.active = nil
		// Succeeds once no reader holds a segment open; otherwise the
		// last segment release removes the directory.
		os.Remove(p.dir)
	}
	return len(doomed), nil
}

// Compact merges small sealed segments within closed partitions into
// time-ordered segments of up to SegmentMaxBytes. Returns the number of
// segments eliminated.
func (s *Store) Compact(ctx context.Context) (int, error) {
	s.mu.RLock()
	parts := s.sortedPartitions()
	s.mu.RUnlock()
	eliminated := 0
	for _, p := range parts {
		if err := ctx.Err(); err != nil {
			return eliminated, err
		}
		n, err := s.compactPartition(p)
		eliminated += n
		if err != nil {
			return eliminated, err
		}
	}
	return eliminated, nil
}

func (s *Store) compactPartition(p *partition) (int, error) {
	s.mu.RLock()
	var small []*segment
	var total int64
	for _, seg := range p.segments {
		_, size, _, _, sealed := seg.stats()
		if sealed && size < s.opts.SegmentMaxBytes/2 && total+size <= s.opts.SegmentMaxBytes {
			small = append(small, seg)
			total += size
		}
	}
	s.mu.RUnlock()
	if len(small) < s.opts.CompactMinSegments {
		return 0, nil
	}
	var all []logs.Entry
	for _, seg := range small {
		if !seg.acquire() {
			return 0, nil // concurrently removed by retention
		}
		seg.mu.RLock()
		for id := uint32(0); id < seg.ix.n; id++ {
			e, err := seg.readAt(id)
			if err != nil {
				seg.mu.RUnlock()
				seg.release()
				return 0, fmt.Errorf("compact read %s: %w", seg.base, err)
			}
			all = append(all, e)
		}
		seg.mu.RUnlock()
		seg.release()
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].Timestamp < all[j].Timestamp })
	merged, err := createSegment(p.dir, s.nextID.Add(1))
	if err != nil {
		return 0, err
	}
	var scratch []byte
	for i := range all {
		if scratch, err = merged.append(&all[i], scratch); err != nil {
			merged.remove()
			return 0, err
		}
	}
	if err := merged.seal(); err != nil {
		merged.remove()
		return 0, err
	}
	s.appendMu.Lock()
	s.mu.Lock()
	if _, live := s.partitions[p.start]; !live {
		s.mu.Unlock()
		s.appendMu.Unlock()
		merged.remove()
		return 0, nil
	}
	drop := map[*segment]bool{}
	for _, seg := range small {
		drop[seg] = true
	}
	kept := []*segment{merged}
	for _, seg := range p.segments {
		if !drop[seg] {
			kept = append(kept, seg)
		}
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].id < kept[j].id })
	p.segments = kept
	s.mu.Unlock()
	s.appendMu.Unlock()
	for _, seg := range small {
		seg.remove()
	}
	s.opts.Logger.Debug("compacted log partition", "partition", p.dir, "merged", len(small), "entries", len(all))
	return len(small) - 1, nil
}

// Stats summarises the store.
type Stats struct {
	Partitions     int   `json:"partitions"`
	Segments       int   `json:"segments"`
	ActiveSegments int   `json:"active_segments"`
	Entries        int64 `json:"entries"`
	Bytes          int64 `json:"bytes"`
	OldestTs       int64 `json:"oldest_ts,omitempty"`
	NewestTs       int64 `json:"newest_ts,omitempty"`
}

// Stats returns storage statistics.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := Stats{Partitions: len(s.partitions)}
	for _, p := range s.partitions {
		for _, seg := range p.segments {
			n, size, minTs, maxTs, sealed := seg.stats()
			st.Segments++
			if !sealed {
				st.ActiveSegments++
			}
			st.Entries += int64(n)
			st.Bytes += size
			if n > 0 && (st.OldestTs == 0 || minTs < st.OldestTs) {
				st.OldestTs = minTs
			}
			if maxTs > st.NewestTs {
				st.NewestTs = maxTs
			}
		}
	}
	return st
}

// FieldInfo describes an indexed field.
type FieldInfo struct {
	Name string `json:"name"`
	// MaxCardinality is the highest distinct-value count seen in any one
	// segment (a cheap lower bound of the global cardinality).
	MaxCardinality int `json:"max_cardinality"`
}

// Fields lists indexed field names across all segments.
func (s *Store) Fields() []FieldInfo {
	agg := map[string]int{}
	s.mu.RLock()
	for _, p := range s.partitions {
		for _, seg := range p.segments {
			seg.mu.RLock()
			for f, n := range seg.ix.FieldNames() {
				agg[f] = max(agg[f], n)
			}
			seg.mu.RUnlock()
		}
	}
	s.mu.RUnlock()
	out := make([]FieldInfo, 0, len(agg))
	for f, n := range agg {
		out = append(out, FieldInfo{f, n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Close seals active segments and closes all files.
func (s *Store) Close() error {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	parts := s.sortedPartitions()
	s.mu.Unlock()
	var errs []error
	for _, p := range parts {
		if p.active != nil {
			errs = append(errs, p.active.seal())
			p.active = nil
		}
		for _, seg := range p.segments {
			errs = append(errs, seg.closeFiles())
		}
	}
	return errors.Join(errs...)
}
