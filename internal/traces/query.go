package traces

import (
	"sort"
	"strconv"
	"time"
)

// SearchQuery selects traces containing at least one span that satisfies
// every span-level criterion.
type SearchQuery struct {
	Service     string
	Operation   string
	MinDuration time.Duration
	MaxDuration time.Duration
	Attributes  map[string]string
	ErrorsOnly  bool
	Start       int64 // trace start lower bound, Unix ns (0 = none)
	End         int64 // trace start upper bound, Unix ns (0 = none)
	Limit       int
}

// Summary describes a trace in search results.
type Summary struct {
	TraceID       string   `json:"traceId"`
	RootService   string   `json:"rootService"`
	RootOperation string   `json:"rootOperation"`
	StartUnixNano int64    `json:"startTimeUnixNano"`
	DurationMs    float64  `json:"durationMs"`
	SpanCount     int      `json:"spanCount"`
	ErrorCount    int      `json:"errorCount"`
	Services      []string `json:"services"`
}

func (q *SearchQuery) spanMatches(sp *Span) bool {
	if q.Service != "" && sp.Service != q.Service {
		return false
	}
	if q.Operation != "" && sp.Name != q.Operation {
		return false
	}
	d := time.Duration(sp.DurationNano())
	if q.MinDuration > 0 && d < q.MinDuration || q.MaxDuration > 0 && d > q.MaxDuration {
		return false
	}
	if q.ErrorsOnly && !sp.IsError() {
		return false
	}
	for k, want := range q.Attributes {
		if v, ok := sp.AttrString(k); !ok || v != want {
			return false
		}
	}
	return true
}

// Search returns matching traces, newest first.
func (s *Store) Search(q SearchQuery) []Summary {
	if q.Limit <= 0 {
		q.Limit = 20
	}
	s.mu.RLock()
	var out []Summary
	for id, t := range s.traces {
		if q.Start != 0 && t.minStart < q.Start || q.End != 0 && t.minStart > q.End {
			continue
		}
		matched := false
		for _, sp := range t.spans {
			if q.spanMatches(sp) {
				matched = true
				break
			}
		}
		if matched {
			out = append(out, summarize(id, t))
		}
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartUnixNano != out[j].StartUnixNano {
			return out[i].StartUnixNano > out[j].StartUnixNano
		}
		return out[i].TraceID < out[j].TraceID
	})
	if len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out
}

func summarize(id string, t *trace) Summary {
	sum := Summary{TraceID: id, StartUnixNano: t.minStart, SpanCount: len(t.spans),
		DurationMs: float64(t.maxEnd-t.minStart) / 1e6}
	ids := t.ids
	svcs := map[string]bool{}
	var root *Span
	for _, sp := range t.spans {
		svcs[sp.Service] = true
		if sp.IsError() {
			sum.ErrorCount++
		}
		isRoot := sp.ParentSpanID == "" || !ids[sp.ParentSpanID]
		if isRoot && (root == nil || sp.StartUnixNano < root.StartUnixNano) {
			root = sp
		}
	}
	if root != nil {
		sum.RootService, sum.RootOperation = root.Service, root.Name
	}
	for svc := range svcs {
		sum.Services = append(sum.Services, svc)
	}
	sort.Strings(sum.Services)
	return sum
}

// Node is a span within an assembled tree.
type Node struct {
	Span       Span    `json:"span"`
	Depth      int     `json:"depth"`
	SelfTimeMs float64 `json:"selfTimeMs"`
	Children   []*Node `json:"children,omitempty"`
}

// Tree is an assembled trace.
type Tree struct {
	TraceID    string   `json:"traceId"`
	Roots      []*Node  `json:"roots"`
	SpanCount  int      `json:"spanCount"`
	DurationMs float64  `json:"durationMs"`
	Services   []string `json:"services"`
	Warnings   []string `json:"warnings,omitempty"`
}

// BuildTree links spans by parent ID. Spans whose parent is missing become
// additional roots (a warning is recorded); spans unreachable from any
// root (parent cycles) are also promoted to roots. Children are ordered by
// start time and each node's self time excludes time covered by children.
func BuildTree(traceID string, spans []Span) *Tree {
	tr := &Tree{TraceID: traceID, SpanCount: len(spans)}
	nodes := make(map[string]*Node, len(spans))
	svcs := map[string]bool{}
	var minStart, maxEnd int64
	for i := range spans {
		sp := spans[i]
		nodes[sp.SpanID] = &Node{Span: sp}
		svcs[sp.Service] = true
		if i == 0 || sp.StartUnixNano < minStart {
			minStart = sp.StartUnixNano
		}
		maxEnd = max(maxEnd, sp.EndUnixNano)
	}
	missing := 0
	for i := range spans {
		n := nodes[spans[i].SpanID]
		pid := n.Span.ParentSpanID
		if pid == "" {
			tr.Roots = append(tr.Roots, n)
			continue
		}
		parent := nodes[pid]
		if parent == nil {
			missing++
			tr.Roots = append(tr.Roots, n)
			continue
		}
		parent.Children = append(parent.Children, n)
	}
	if missing > 0 {
		tr.Warnings = append(tr.Warnings, pluralize(missing, "span references a missing parent", "spans reference missing parents"))
	}
	visited := map[*Node]bool{}
	var walk func(n *Node, depth int)
	walk = func(n *Node, depth int) {
		visited[n] = true
		n.Depth = depth
		sortNodes(n.Children)
		kept := n.Children[:0]
		for _, c := range n.Children {
			if !visited[c] { // drops back-edges of parent cycles
				kept = append(kept, c)
				walk(c, depth+1)
			}
		}
		n.Children = kept
		n.SelfTimeMs = float64(selfTime(n)) / 1e6
	}
	sortNodes(tr.Roots)
	for _, r := range tr.Roots {
		walk(r, 0)
	}
	cycles := 0
	for i := range spans {
		if n := nodes[spans[i].SpanID]; !visited[n] {
			cycles++
			tr.Roots = append(tr.Roots, n)
			walk(n, 0)
		}
	}
	if cycles > 0 {
		tr.Warnings = append(tr.Warnings, pluralize(cycles, "parent cycle was broken", "parent cycles were broken"))
	}
	for svc := range svcs {
		tr.Services = append(tr.Services, svc)
	}
	sort.Strings(tr.Services)
	tr.DurationMs = float64(maxEnd-minStart) / 1e6
	return tr
}

func sortNodes(ns []*Node) {
	sort.SliceStable(ns, func(i, j int) bool { return ns[i].Span.StartUnixNano < ns[j].Span.StartUnixNano })
}

// selfTime is the span duration minus the union of child intervals
// clipped to the span.
func selfTime(n *Node) int64 {
	start, end := n.Span.StartUnixNano, n.Span.EndUnixNano
	covered := int64(0)
	cur, curEnd := int64(-1), int64(-1)
	for _, c := range n.Children { // sorted by start
		cs, ce := max(c.Span.StartUnixNano, start), min(c.Span.EndUnixNano, end)
		if ce <= cs {
			continue
		}
		if cur < 0 || cs > curEnd {
			if cur >= 0 {
				covered += curEnd - cur
			}
			cur, curEnd = cs, ce
		} else if ce > curEnd {
			curEnd = ce
		}
	}
	if cur >= 0 {
		covered += curEnd - cur
	}
	return (end - start) - covered
}

func pluralize(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + many
}

// Edge is a call relationship between two services.
type Edge struct {
	Parent     string `json:"parent"`
	Child      string `json:"child"`
	CallCount  int    `json:"callCount"`
	ErrorCount int    `json:"errorCount"`
}

// Dependencies builds the service graph for traces starting in [start,end]
// (Unix ns, 0 = unbounded). An edge is counted for every parent/child span
// pair crossing a service boundary, and for client/producer spans that
// name a peer.service without an instrumented child.
func (s *Store) Dependencies(start, end int64) []Edge {
	type key struct{ parent, child string }
	edges := map[key]*Edge{}
	add := func(p, c string, isErr bool) {
		k := key{p, c}
		e := edges[k]
		if e == nil {
			e = &Edge{Parent: p, Child: c}
			edges[k] = e
		}
		e.CallCount++
		if isErr {
			e.ErrorCount++
		}
	}
	s.mu.RLock()
	for _, t := range s.traces {
		if start != 0 && t.minStart < start || end != 0 && t.minStart > end {
			continue
		}
		byID := make(map[string]*Span, len(t.spans))
		hasRemoteChild := map[string]bool{}
		for _, sp := range t.spans {
			byID[sp.SpanID] = sp
		}
		for _, sp := range t.spans {
			if p := byID[sp.ParentSpanID]; p != nil && p.Service != sp.Service {
				add(p.Service, sp.Service, sp.IsError())
				hasRemoteChild[p.SpanID] = true
			}
		}
		for _, sp := range t.spans {
			if (sp.Kind == KindClient || sp.Kind == KindProducer) && !hasRemoteChild[sp.SpanID] {
				if peer, ok := sp.AttrString("peer.service"); ok && peer != "" && peer != sp.Service {
					add(sp.Service, peer, sp.IsError())
				}
			}
		}
	}
	s.mu.RUnlock()
	out := make([]Edge, 0, len(edges))
	for _, e := range edges {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Parent != out[j].Parent {
			return out[i].Parent < out[j].Parent
		}
		return out[i].Child < out[j].Child
	})
	return out
}
