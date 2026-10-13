package traces

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Options configure a Store.
type Options struct {
	Dir               string
	PartitionDuration time.Duration
	MaxSpansPerTrace  int
	Logger            *slog.Logger
}

type trace struct {
	spans    []*Span
	ids      map[string]bool
	minStart int64
	maxEnd   int64
}

type partFile struct {
	start int64
	path  string
	f     *os.File
	w     *bufio.Writer
}

// Store keeps spans in memory, indexed by trace, and persists them as
// NDJSON files partitioned by span start time. On open, partition files
// are replayed (a torn last line is skipped).
type Store struct {
	opts Options
	mu   sync.RWMutex

	traces     map[string]*trace
	parts      map[int64]*partFile
	operations map[string]map[string]int // service -> operation -> span count
	spanCount  int
	closed     bool
}

// AppendResult reports how many spans were stored or rejected.
type AppendResult struct {
	Accepted   int
	Duplicates int
	Dropped    int // over MaxSpansPerTrace
	Invalid    int
}

// Open opens or creates a trace store.
func Open(opts Options) (*Store, error) {
	if opts.PartitionDuration <= 0 {
		opts.PartitionDuration = time.Hour
	}
	if opts.MaxSpansPerTrace <= 0 {
		opts.MaxSpansPerTrace = 10_000
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{opts: opts, traces: map[string]*trace{}, parts: map[int64]*partFile{}, operations: map[string]map[string]int{}}
	files, err := filepath.Glob(filepath.Join(opts.Dir, "spans-*.ndjson"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	for _, path := range files {
		if err := s.replay(path); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) replay(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 8<<20)
	bad := 0
	for sc.Scan() {
		var sp Span
		if err := json.Unmarshal(sc.Bytes(), &sp); err != nil || sp.Validate() != nil {
			bad++
			continue
		}
		s.index(&sp)
	}
	if bad > 0 {
		s.opts.Logger.Warn("skipped unreadable span records", "file", path, "count", bad)
	}
	return sc.Err()
}

func (s *Store) partitionStart(ns int64) int64 {
	d := int64(s.opts.PartitionDuration)
	return ns - ns%d
}

// index adds a span to the in-memory indexes. Caller holds s.mu (or is
// single-threaded during replay). Returns false for duplicates/overflow.
func (s *Store) index(sp *Span) (ok bool, dup bool) {
	t := s.traces[sp.TraceID]
	if t == nil {
		t = &trace{ids: map[string]bool{}, minStart: sp.StartUnixNano, maxEnd: sp.EndUnixNano}
		s.traces[sp.TraceID] = t
	}
	if t.ids[sp.SpanID] {
		return false, true
	}
	if len(t.spans) >= s.opts.MaxSpansPerTrace {
		return false, false
	}
	t.ids[sp.SpanID] = true
	t.spans = append(t.spans, sp)
	t.minStart = min(t.minStart, sp.StartUnixNano)
	t.maxEnd = max(t.maxEnd, sp.EndUnixNano)
	ops := s.operations[sp.Service]
	if ops == nil {
		ops = map[string]int{}
		s.operations[sp.Service] = ops
	}
	ops[sp.Name]++
	s.spanCount++
	return true, false
}

// Append validates and stores spans.
func (s *Store) Append(spans []Span) (AppendResult, error) {
	var res AppendResult
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return res, errors.New("trace store closed")
	}
	touched := map[*partFile]bool{}
	for i := range spans {
		sp := spans[i]
		if sp.Validate() != nil {
			res.Invalid++ // the API validates first; this guards replays and direct callers
			continue
		}
		ok, dup := s.index(&sp)
		if !ok {
			if dup {
				res.Duplicates++
			} else {
				res.Dropped++
			}
			continue
		}
		pf, err := s.partFor(sp.StartUnixNano)
		if err != nil {
			return res, err
		}
		line, err := json.Marshal(&sp)
		if err != nil {
			return res, err
		}
		pf.w.Write(line)
		if err := pf.w.WriteByte('\n'); err != nil {
			return res, err
		}
		touched[pf] = true
		res.Accepted++
	}
	for pf := range touched {
		if err := pf.w.Flush(); err != nil {
			return res, err
		}
	}
	return res, nil
}

func (s *Store) partFor(ns int64) (*partFile, error) {
	start := s.partitionStart(ns)
	if pf := s.parts[start]; pf != nil {
		return pf, nil
	}
	path := filepath.Join(s.opts.Dir, fmt.Sprintf("spans-%020d.ndjson", start/int64(time.Second)))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	pf := &partFile{start: start, path: path, f: f, w: bufio.NewWriterSize(f, 64<<10)}
	s.parts[start] = pf
	return pf, nil
}

// Trace returns all spans of a trace ordered by start time.
func (s *Store) Trace(traceID string) ([]Span, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := s.traces[traceID]
	if t == nil {
		return nil, false
	}
	out := make([]Span, len(t.spans))
	for i, sp := range t.spans {
		out[i] = *sp
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].StartUnixNano < out[j].StartUnixNano })
	return out, true
}

// Services returns service names with their operations and span counts.
func (s *Store) Services() map[string]map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]map[string]int, len(s.operations))
	for svc, ops := range s.operations {
		cp := make(map[string]int, len(ops))
		for k, v := range ops {
			cp[k] = v
		}
		out[svc] = cp
	}
	return out
}

// Stats summarises the store.
type Stats struct {
	Traces     int `json:"traces"`
	Spans      int `json:"spans"`
	Partitions int `json:"partitions"`
}

// Stats returns store statistics.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	files, _ := filepath.Glob(filepath.Join(s.opts.Dir, "spans-*.ndjson"))
	return Stats{Traces: len(s.traces), Spans: s.spanCount, Partitions: len(files)}
}

// ApplyRetention drops traces whose last span ended before cutoff (Unix
// ns) and deletes partition files that ended before cutoff.
func (s *Store) ApplyRetention(cutoff int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for id, t := range s.traces {
		if t.maxEnd < cutoff {
			for _, sp := range t.spans {
				if ops := s.operations[sp.Service]; ops != nil {
					if ops[sp.Name]--; ops[sp.Name] <= 0 {
						delete(ops, sp.Name)
					}
					if len(ops) == 0 {
						delete(s.operations, sp.Service)
					}
				}
			}
			s.spanCount -= len(t.spans)
			delete(s.traces, id)
			removed++
		}
	}
	files, err := filepath.Glob(filepath.Join(s.opts.Dir, "spans-*.ndjson"))
	if err != nil {
		return removed, err
	}
	var errs []error
	for _, path := range files {
		sec, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "spans-"), ".ndjson"), 10, 64)
		if err != nil {
			continue
		}
		start := sec * int64(time.Second)
		if start+int64(s.opts.PartitionDuration) > cutoff {
			continue
		}
		if pf := s.parts[start]; pf != nil {
			errs = append(errs, pf.w.Flush(), pf.f.Close())
			delete(s.parts, start)
		}
		errs = append(errs, os.Remove(path))
	}
	return removed, errors.Join(errs...)
}

// Close flushes and closes partition files.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	for _, pf := range s.parts {
		errs = append(errs, pf.w.Flush(), pf.f.Sync(), pf.f.Close())
	}
	return errors.Join(errs...)
}
