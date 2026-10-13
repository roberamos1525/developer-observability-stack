package store

import (
	"container/heap"
	"context"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/errs"
	"github.com/robertramos/developer-observability-stack/internal/logs"
	"github.com/robertramos/developer-observability-stack/internal/logs/query"
)

// SearchRequest is an executable query. Start/End are inclusive Unix ns
// bounds (0 = unbounded) after merging API parameters with query stages.
type SearchRequest struct {
	Query *query.Query
	Start int64
	End   int64
	Limit int
	// MaxGroups bounds count-by cardinality.
	MaxGroups int
}

// GroupCount is one row of a count/count-by result.
type GroupCount struct {
	Labels map[string]string `json:"labels"`
	Count  int64             `json:"count"`
}

// SearchStats describe the work a query performed.
type SearchStats struct {
	PartitionsScanned int `json:"partitions_scanned"`
	SegmentsScanned   int `json:"segments_scanned"`
	SegmentsPruned    int `json:"segments_pruned"`
	EntriesRead       int `json:"entries_read"`
	CandidateEntries  int `json:"candidate_entries"`
	Matched           int `json:"matched"`
}

// SearchResult holds entries (search mode) or groups (count mode).
type SearchResult struct {
	Entries []logs.Entry `json:"entries,omitempty"`
	Groups  []GroupCount `json:"groups,omitempty"`
	Stats   SearchStats  `json:"stats"`
	// Truncated reports that further matching entries may exist beyond Limit.
	Truncated bool `json:"truncated"`
}

// entryHeap keeps the best `limit` entries. The root is the worst kept
// entry: the oldest for Desc ordering, the newest for Asc.
type entryHeap struct {
	items []logs.Entry
	asc   bool
}

func (h *entryHeap) Len() int { return len(h.items) }
func (h *entryHeap) Less(i, j int) bool {
	if h.asc {
		return h.items[i].Timestamp > h.items[j].Timestamp
	}
	return h.items[i].Timestamp < h.items[j].Timestamp
}
func (h *entryHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *entryHeap) Push(x any)    { h.items = append(h.items, x.(logs.Entry)) }
func (h *entryHeap) Pop() any {
	n := len(h.items) - 1
	x := h.items[n]
	h.items = h.items[:n]
	return x
}

// better reports whether ts would displace the heap's worst entry.
func (h *entryHeap) better(ts int64) bool {
	worst := h.items[0].Timestamp
	if h.asc {
		return ts < worst
	}
	return ts > worst
}

type segRef struct {
	seg  *segment
	part *partition
}

// Search executes req.
func (s *Store) Search(ctx context.Context, req SearchRequest) (*SearchResult, error) {
	if req.Query == nil {
		return nil, errs.Invalid("query is required")
	}
	start, end := req.Start, req.End
	if end == 0 {
		end = math.MaxInt64
	}
	if start > end {
		return nil, errs.Invalid("start must not be after end")
	}
	if req.Query.Count {
		return s.count(ctx, req, start, end)
	}
	if req.Limit <= 0 {
		return nil, errs.Invalid("limit must be positive")
	}
	asc := req.Query.Order == query.Asc
	refs, parts := s.snapshot(start, end)
	defer releaseAll(refs)
	sortRefs(refs, asc)

	res := &SearchResult{Stats: SearchStats{PartitionsScanned: parts}}
	h := &entryHeap{asc: asc}
	skipped := 0
	var tc query.TokenCache
	for _, r := range refs {
		seg := r.seg
		if err := ctx.Err(); err != nil {
			return nil, errs.Wrap(errs.KindTimeout, err, "log search cancelled")
		}
		if err := seg.flush(); err != nil {
			return nil, err
		}
		seg.mu.RLock()
		minTs, maxTs := seg.ix.minTs, seg.ix.maxTs
		full := h.Len() >= req.Limit
		if seg.ix.n == 0 || maxTs < start || minTs > end ||
			full && (!asc && maxTs < h.items[0].Timestamp || asc && minTs > h.items[0].Timestamp) {
			seg.mu.RUnlock()
			res.Stats.SegmentsPruned++
			continue
		}
		res.Stats.SegmentsScanned++
		plan := query.Plan(req.Query.Root, seg.ix)
		ids := plan.List(seg.ix)
		res.Stats.CandidateEntries += len(ids)
		for i, id := range ids {
			if i&1023 == 1023 {
				if err := ctx.Err(); err != nil {
					seg.mu.RUnlock()
					return nil, errs.Wrap(errs.KindTimeout, err, "log search cancelled")
				}
			}
			ts := seg.ix.ts[id]
			if ts < start || ts > end {
				continue
			}
			if h.Len() >= req.Limit && !h.better(ts) {
				skipped++ // cannot enter the top-K; it may or may not match
				continue
			}
			e, err := seg.readAt(id)
			if err != nil {
				seg.mu.RUnlock()
				return nil, errs.Wrap(errs.KindInternal, err, "read log entry")
			}
			res.Stats.EntriesRead++
			if !plan.Exact && !req.Query.Root.Match(&e, &tc) {
				continue
			}
			res.Stats.Matched++
			if h.Len() < req.Limit {
				heap.Push(h, e)
			} else {
				h.items[0] = e
				heap.Fix(h, 0)
				res.Truncated = true
			}
		}
		seg.mu.RUnlock()
	}
	if skipped > 0 {
		res.Truncated = true
	}
	out := h.items
	sort.Slice(out, func(i, j int) bool {
		if asc {
			return out[i].Timestamp < out[j].Timestamp
		}
		return out[i].Timestamp > out[j].Timestamp
	})
	res.Entries = out
	return res, nil
}

func (s *Store) count(ctx context.Context, req SearchRequest, start, end int64) (*SearchResult, error) {
	maxGroups := req.MaxGroups
	if maxGroups <= 0 {
		maxGroups = 10_000
	}
	refs, parts := s.snapshot(start, end)
	defer releaseAll(refs)
	res := &SearchResult{Stats: SearchStats{PartitionsScanned: parts}}
	by := req.Query.CountBy
	groups := map[string]*GroupCount{}
	var tc query.TokenCache
	var sb strings.Builder
	for _, r := range refs {
		seg := r.seg
		if err := ctx.Err(); err != nil {
			return nil, errs.Wrap(errs.KindTimeout, err, "log search cancelled")
		}
		if err := seg.flush(); err != nil {
			return nil, err
		}
		seg.mu.RLock()
		if seg.ix.n == 0 || seg.ix.maxTs < start || seg.ix.minTs > end {
			seg.mu.RUnlock()
			res.Stats.SegmentsPruned++
			continue
		}
		res.Stats.SegmentsScanned++
		plan := query.Plan(req.Query.Root, seg.ix)
		ids := plan.List(seg.ix)
		res.Stats.CandidateEntries += len(ids)
		for i, id := range ids {
			if i&1023 == 1023 && ctx.Err() != nil {
				seg.mu.RUnlock()
				return nil, errs.Wrap(errs.KindTimeout, ctx.Err(), "log count cancelled")
			}
			ts := seg.ix.ts[id]
			if ts < start || ts > end {
				continue
			}
			// Exact plans without group-by need no entry reads at all.
			if plan.Exact && len(by) == 0 {
				res.Stats.Matched++
				continue
			}
			e, err := seg.readAt(id)
			if err != nil {
				seg.mu.RUnlock()
				return nil, errs.Wrap(errs.KindInternal, err, "read log entry")
			}
			res.Stats.EntriesRead++
			if !plan.Exact && !req.Query.Root.Match(&e, &tc) {
				continue
			}
			res.Stats.Matched++
			if len(by) == 0 {
				continue
			}
			sb.Reset()
			for _, f := range by {
				v, _ := e.Field(f)
				sb.WriteString(v)
				sb.WriteByte(0)
			}
			key := sb.String()
			g := groups[key]
			if g == nil {
				if len(groups) >= maxGroups {
					seg.mu.RUnlock()
					return nil, errs.Invalid("count by produced more than %d groups", maxGroups)
				}
				labels := make(map[string]string, len(by))
				for _, f := range by {
					labels[f], _ = e.Field(f)
				}
				g = &GroupCount{Labels: labels}
				groups[key] = g
			}
			g.Count++
		}
		seg.mu.RUnlock()
	}
	if len(by) == 0 {
		res.Groups = []GroupCount{{Labels: map[string]string{}, Count: int64(res.Stats.Matched)}}
		return res, nil
	}
	for _, g := range groups {
		res.Groups = append(res.Groups, *g)
	}
	sort.Slice(res.Groups, func(i, j int) bool {
		a, b := res.Groups[i], res.Groups[j]
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		return groupKey(a.Labels, by) < groupKey(b.Labels, by)
	})
	return res, nil
}

func groupKey(l map[string]string, by []string) string {
	parts := make([]string, len(by))
	for i, f := range by {
		parts[i] = l[f]
	}
	return strings.Join(parts, "\x1f")
}

// snapshot pins every segment of the partitions overlapping [start,end].
func (s *Store) snapshot(start, end int64) ([]segRef, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var refs []segRef
	parts := 0
	for _, p := range s.partitions {
		if p.end <= start || p.start > end {
			continue
		}
		parts++
		for _, seg := range p.segments {
			if seg.acquire() {
				refs = append(refs, segRef{seg, p})
			}
		}
	}
	return refs, parts
}

func releaseAll(refs []segRef) {
	for _, r := range refs {
		r.seg.release()
	}
}

// sortRefs orders segments so the most promising come first, maximising
// the effect of top-K pruning.
func sortRefs(refs []segRef, asc bool) {
	keys := make([]int64, len(refs))
	for i, r := range refs {
		_, _, minTs, maxTs, _ := r.seg.stats()
		if asc {
			keys[i] = minTs
		} else {
			keys[i] = -maxTs
		}
	}
	sort.Sort(refSorter{refs, keys})
}

type refSorter struct {
	refs []segRef
	keys []int64
}

func (r refSorter) Len() int           { return len(r.refs) }
func (r refSorter) Less(i, j int) bool { return r.keys[i] < r.keys[j] }
func (r refSorter) Swap(i, j int) {
	r.refs[i], r.refs[j] = r.refs[j], r.refs[i]
	r.keys[i], r.keys[j] = r.keys[j], r.keys[i]
}

// ResolveRange merges API-level bounds with the query's since/between
// stages. now anchors relative ranges.
func ResolveRange(q *query.Query, start, end int64, now time.Time) (int64, int64) {
	if q.Start != 0 {
		start = max(start, q.Start)
	}
	if q.End != 0 {
		if end == 0 {
			end = q.End
		} else {
			end = min(end, q.End)
		}
	}
	if q.Since > 0 {
		start = max(start, now.Add(-q.Since).UnixNano())
	}
	return start, end
}
