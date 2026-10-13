package traces

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var t0 = time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC).UnixNano()

func id(prefix string, n, width int) string {
	return fmt.Sprintf("%s%0*x", prefix, width-len(prefix), n)
}

func span(trace, sid, parent int, svc, name string, startMs, durMs int64, kind SpanKind) Span {
	s := Span{
		TraceID: id("a", trace, 32), SpanID: id("b", sid, 16), Service: svc, Name: name, Kind: kind,
		StartUnixNano: t0 + startMs*1e6, EndUnixNano: t0 + (startMs+durMs)*1e6,
		Attributes: map[string]any{"env": "prod"},
	}
	if parent > 0 {
		s.ParentSpanID = id("b", parent, 16)
	}
	return s
}

// sampleTrace: frontend -> api -> (db client, cache client with peer.service)
func sampleTrace(n int, startMs int64, apiErr bool) []Span {
	api := span(n, n*10+2, n*10+1, "api", "POST /orders", startMs+5, 80, KindServer)
	if apiErr {
		api.Status = Status{Code: StatusError, Message: "boom"}
	}
	db := span(n, n*10+3, n*10+2, "db", "INSERT orders", startMs+10, 30, KindServer)
	cache := span(n, n*10+4, n*10+2, "api", "GET cache", startMs+50, 10, KindClient)
	cache.Attributes["peer.service"] = "redis"
	return []Span{
		span(n, n*10+1, 0, "frontend", "GET /", startMs, 100, KindServer),
		api, db, cache,
	}
}

func openStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(Options{Dir: dir, PartitionDuration: time.Hour, MaxSpansPerTrace: 5})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAppendLookupAndPersistence(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	res, err := s.Append(append(sampleTrace(1, 0, false), sampleTrace(1, 0, false)[0]))
	if err != nil || res.Accepted != 4 || res.Duplicates != 1 {
		t.Fatalf("append %+v %v", res, err)
	}
	bad := span(2, 1, 0, "x", "", 0, 1, KindServer)
	res, _ = s.Append([]Span{bad})
	if res.Invalid != 1 {
		t.Fatalf("invalid span accepted: %+v", res)
	}
	over := []Span{}
	for i := 0; i < 7; i++ {
		over = append(over, span(3, 100+i, 0, "batch", "job", int64(i), 1, KindInternal))
	}
	res, _ = s.Append(over)
	if res.Accepted != 5 || res.Dropped != 2 {
		t.Fatalf("max spans per trace: %+v", res)
	}
	got, ok := s.Trace(id("a", 1, 32))
	if !ok || len(got) != 4 || got[0].Name != "GET /" {
		t.Fatalf("trace lookup: %v %v", ok, got)
	}
	s.Close()

	s2 := openStore(t, dir)
	defer s2.Close()
	if st := s2.Stats(); st.Traces != 2 || st.Spans != 9 {
		t.Fatalf("after reopen %+v", st)
	}
	if ops := s2.Services()["api"]; ops["POST /orders"] != 1 || ops["GET cache"] != 1 {
		t.Fatalf("operations index %v", ops)
	}
}

func TestReplaySkipsCorruptLines(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir)
	s.Append(sampleTrace(1, 0, false))
	s.Close()
	files, _ := filepath.Glob(filepath.Join(dir, "*.ndjson"))
	f, _ := os.OpenFile(files[0], os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString(`{"traceId":"torn`)
	f.Close()
	s2 := openStore(t, dir)
	defer s2.Close()
	if st := s2.Stats(); st.Spans != 4 {
		t.Fatalf("spans after replay %d", st.Spans)
	}
}

func TestSearch(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	for i := 1; i <= 6; i++ {
		s.Append(sampleTrace(i, int64(i)*1000, i%3 == 0))
	}
	cases := []struct {
		q    SearchQuery
		want int
	}{
		{SearchQuery{Service: "api"}, 6},
		{SearchQuery{Service: "api", Operation: "POST /orders", ErrorsOnly: true}, 2},
		{SearchQuery{MinDuration: 90 * time.Millisecond}, 6},
		{SearchQuery{Service: "db", MinDuration: 31 * time.Millisecond}, 0},
		{SearchQuery{Service: "db", MaxDuration: 30 * time.Millisecond}, 6},
		{SearchQuery{Attributes: map[string]string{"peer.service": "redis"}}, 6},
		{SearchQuery{Attributes: map[string]string{"env": "dev"}}, 0},
		{SearchQuery{Start: t0 + 3000*1e6, End: t0 + 4000*1e6}, 2},
		{SearchQuery{Limit: 2}, 2},
	}
	for i, c := range cases {
		if got := s.Search(c.q); len(got) != c.want {
			t.Errorf("case %d: got %d traces, want %d", i, len(got), c.want)
		}
	}
	res := s.Search(SearchQuery{Service: "frontend", Limit: 10})
	if res[0].TraceID != id("a", 6, 32) || res[0].RootService != "frontend" || res[0].SpanCount != 4 || res[0].ErrorCount != 1 {
		t.Fatalf("newest-first summary %+v", res[0])
	}
	if len(res[0].Services) != 3 || res[0].DurationMs != 100 {
		t.Fatalf("summary services/duration %+v", res[0])
	}
}

func TestBuildTree(t *testing.T) {
	spans := sampleTrace(1, 0, false)
	tree := BuildTree(spans[0].TraceID, spans)
	if len(tree.Roots) != 1 || len(tree.Warnings) != 0 {
		t.Fatalf("roots %d warnings %v", len(tree.Roots), tree.Warnings)
	}
	root := tree.Roots[0]
	api := root.Children[0]
	if api.Depth != 1 || len(api.Children) != 2 || api.Children[0].Span.Name != "INSERT orders" {
		t.Fatalf("api node %+v", api)
	}
	// frontend 100ms with child 5..85 => self 20ms; api 80ms with
	// children covering 30+10ms => self 40ms.
	if root.SelfTimeMs != 20 || api.SelfTimeMs != 40 {
		t.Fatalf("self time root=%v api=%v", root.SelfTimeMs, api.SelfTimeMs)
	}
	if tree.DurationMs != 100 || len(tree.Services) != 3 {
		t.Fatalf("tree %+v", tree)
	}
	// Orphan and a parent cycle.
	orphan := span(1, 50, 49, "api", "orphan", 1, 1, KindInternal)
	c1 := span(1, 60, 61, "api", "c1", 1, 1, KindInternal)
	c2 := span(1, 61, 60, "api", "c2", 1, 1, KindInternal)
	tree = BuildTree(spans[0].TraceID, append(spans, orphan, c1, c2))
	if len(tree.Roots) != 3 || len(tree.Warnings) != 2 {
		t.Fatalf("roots=%d warnings=%v", len(tree.Roots), tree.Warnings)
	}
}

func TestSelfTimeOverlappingChildren(t *testing.T) {
	parent := &Node{Span: Span{StartUnixNano: 0, EndUnixNano: 100}}
	for _, iv := range [][2]int64{{10, 40}, {30, 50}, {90, 150}, {-20, 5}} {
		parent.Children = append(parent.Children, &Node{Span: Span{StartUnixNano: iv[0], EndUnixNano: iv[1]}})
	}
	sortNodes(parent.Children)
	// covered: [0,5] + [10,50] + [90,100] = 5+40+10 = 55
	if got := selfTime(parent); got != 45 {
		t.Fatalf("self time %d", got)
	}
}

func TestDependenciesAndRetention(t *testing.T) {
	s := openStore(t, t.TempDir())
	defer s.Close()
	s.Append(sampleTrace(1, 0, true))
	s.Append(sampleTrace(2, 2*3600*1000, false)) // two hours later, another partition
	deps := s.Dependencies(0, 0)
	want := map[string][2]int{"frontend->api": {2, 1}, "api->db": {2, 0}, "api->redis": {2, 0}}
	if len(deps) != 3 {
		t.Fatalf("edges %+v", deps)
	}
	for _, e := range deps {
		w := want[e.Parent+"->"+e.Child]
		if e.CallCount != w[0] || e.ErrorCount != w[1] {
			t.Errorf("edge %+v want %v", e, w)
		}
	}
	if d := s.Dependencies(t0+3600*1e9, 0); len(d) != 3 || d[0].CallCount != 1 {
		t.Fatalf("time-bounded dependencies %+v", d)
	}
	removed, err := s.ApplyRetention(t0 + 3600*1e9)
	if err != nil || removed != 1 {
		t.Fatalf("retention removed %d, %v", removed, err)
	}
	if st := s.Stats(); st.Traces != 1 || st.Partitions != 1 || st.Spans != 4 {
		t.Fatalf("after retention %+v", st)
	}
	if _, ok := s.Trace(id("a", 1, 32)); ok {
		t.Fatal("expired trace still readable")
	}
	if _, ok := s.Services()["frontend"]; !ok {
		t.Fatal("services of remaining traces must stay indexed")
	}
}

func TestValidID(t *testing.T) {
	cases := map[string]bool{
		"0123456789abcdef": true, "0000000000000000": false, "0123456789ABCDEF": false,
		"0123456789abcde": false, "zz23456789abcdef": false,
	}
	for in, want := range cases {
		if ValidID(in, 8) != want {
			t.Errorf("ValidID(%q) != %v", in, want)
		}
	}
	if KindClient.String() != "client" || SpanKind(9).String() != "unspecified" {
		t.Fatal("kind strings")
	}
}
