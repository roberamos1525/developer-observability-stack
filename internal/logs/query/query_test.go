package query

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/logs"
	"github.com/robertramos/developer-observability-stack/internal/logs/postings"
)

func TestParseStructure(t *testing.T) {
	cases := map[string]string{
		`error`:                              `error`,
		`level:error service:api`:            `(level="error" AND service="api")`,
		`a OR b c`:                           `(a OR (b AND c))`,
		`(a OR b) AND NOT c`:                 `((a OR b) AND NOT c)`,
		`-timeout "connection refused"`:      `(NOT timeout AND "connection refused")`,
		`status>=500 latency_ms<-1.5`:        `(status>="500" AND latency_ms<"-1.5")`,
		`host=~"web-[0-9]+" user!~"bot.*"`:   `(host=~"web-[0-9]+" AND user!~"bot.*")`,
		`service:pay* conn*`:                 `(service="pay*" AND conn*)`,
		`message:"disk full"`:                `"disk full"`,
		`trace_id:*`:                         `trace_id="*"`,
		`level:WARNING`:                      `level="warn"`,
		`connection-refused`:                 `"connection-refused"`,
		`a && b || c`:                        `((a AND b) OR c)`,
		``:                                   `*`,
		`| count by level`:                   `*`,
		`user!=alice | limit 5 | sort asc`:   `user!="alice"`,
		`x | since 15m | between 1 2`:        `x`,
		`env:"prod eu" | count by env, host`: `env="prod eu"`,
	}
	for in, want := range cases {
		q, err := Parse(in)
		if err != nil {
			t.Errorf("Parse(%q): %v", in, err)
			continue
		}
		if got := q.Root.String(); got != want {
			t.Errorf("Parse(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestParseStages(t *testing.T) {
	q, err := Parse(`level:error | limit 25 | sort asc | since 1h30m`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Limit != 25 || q.Order != Asc || q.Since != 90*time.Minute {
		t.Fatalf("%+v", q)
	}
	q, err = Parse(`| between "2026-01-01T00:00:00Z" "2026-01-02T00:00:00Z" | count by service, level`)
	if err != nil {
		t.Fatal(err)
	}
	if !q.Count || strings.Join(q.CountBy, ",") != "service,level" {
		t.Fatalf("%+v", q)
	}
	if q.End-q.Start != int64(24*time.Hour) {
		t.Fatal("between range")
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		`(a OR b`, `a AND`, `level:`, `"unterminated`, `a | limit x`, `a | limit 0`,
		`a | sort sideways`, `a | since -5m`, `a | frobnicate`, `x=~"("`, `a !b`,
		`bad$field:1`, `| between 2 1`, `***`, `ab*c* d`, `"multi word"* `, `message>3`,
		strings.Repeat("(", MaxDepth+2) + "a" + strings.Repeat(")", MaxDepth+2),
		strings.Repeat("a ", MaxQueryLength),
	}
	for _, in := range bad {
		if _, err := Parse(in); err == nil {
			t.Errorf("Parse(%q) should fail", in)
		} else {
			var se *SyntaxError
			if !errors.As(err, &se) {
				t.Errorf("Parse(%q) returned %T, want *SyntaxError", in, err)
			}
		}
	}
}

func entry(level, msg string, fields ...string) logs.Entry {
	e := logs.Entry{Timestamp: 1, Level: level, Message: msg, Fields: map[string]string{}}
	for i := 0; i+1 < len(fields); i += 2 {
		e.Fields[fields[i]] = fields[i+1]
	}
	return e
}

func TestMatchSemantics(t *testing.T) {
	e := entry("error", "Connection refused by upstream db-1 after 3 retries",
		"service", "checkout", "status", "502", "latency_ms", "1250.5", "region", "eu-west")
	yes := []string{
		`refused`, `REFUSED`, `"connection refused"`, `"refused by upstream"`, `upstream db`,
		`level:error`, `level:err`, `service:checkout`, `service:check*`, `status>=500`, `status<600`,
		`latency_ms>1000`, `region=~"^eu-"`, `user!=bob`, `NOT timeout`, `-timeout`,
		`retr*`, `region:*`, `status:502 OR missing`, `(foo OR refused) AND service:checkout`,
		`message:"db 1"`, `region>"eu"`,
	}
	no := []string{
		`timeout`, `"refused connection"`, `level:warn`, `service:pay*`, `status<500`,
		`region!~"eu"`, `user:*`, `NOT refused`, `latency_ms<=1000`, `refus`, `status:50`,
	}
	var tc TokenCache
	for _, s := range yes {
		q, err := Parse(s)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		if !q.Root.Match(&e, &tc) {
			t.Errorf("%q should match", s)
		}
	}
	for _, s := range no {
		q, err := Parse(s)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		if q.Root.Match(&e, &tc) {
			t.Errorf("%q should not match", s)
		}
	}
}

// memIndex is a straightforward reference index used to cross-check the
// planner against brute-force matching.
type memIndex struct {
	entries []logs.Entry
	terms   map[string]postings.List
	fields  map[string]map[string]postings.List
}

func buildIndex(entries []logs.Entry) *memIndex {
	ix := &memIndex{entries: entries, terms: map[string]postings.List{}, fields: map[string]map[string]postings.List{}}
	for i, e := range entries {
		id := uint32(i)
		seen := map[string]bool{}
		for _, t := range logs.Tokenize(e.Message) {
			if !seen[t] {
				seen[t] = true
				ix.terms[t] = append(ix.terms[t], id)
			}
		}
		add := func(k, v string) {
			if ix.fields[k] == nil {
				ix.fields[k] = map[string]postings.List{}
			}
			ix.fields[k][IndexKey(v)] = append(ix.fields[k][IndexKey(v)], id)
		}
		if e.Level != "" {
			add("level", e.Level)
		}
		for k, v := range e.Fields {
			add(k, v)
		}
	}
	return ix
}

func (ix *memIndex) Len() uint32                       { return uint32(len(ix.entries)) }
func (ix *memIndex) Term(t string) postings.List       { return ix.terms[t] }
func (ix *memIndex) Field(f, v string) postings.List   { return ix.fields[f][v] }
func (ix *memIndex) TermPrefix(p string) postings.List { return prefix(ix.terms, p) }
func (ix *memIndex) FieldPrefix(f, p string) postings.List {
	return prefix(ix.fields[f], p)
}

func prefix(m map[string]postings.List, p string) postings.List {
	var ls []postings.List
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if strings.HasPrefix(k, p) {
			ls = append(ls, m[k])
		}
	}
	return postings.UnionAll(ls)
}

// TestPlanAgreesWithMatch is a randomized property test: for random
// entries and random queries the planner's candidate set must contain
// every matching entry, and when it claims exactness it must equal the
// matching set.
func TestPlanAgreesWithMatch(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	words := []string{"alpha", "beta", "gamma", "delta", "error", "timeout", "retry", "db"}
	levels := []string{"", "info", "warn", "error"}
	services := []string{"api", "web", "worker", "apigw"}
	var entries []logs.Entry
	for i := 0; i < 300; i++ {
		n := 1 + rng.Intn(6)
		msg := make([]string, n)
		for j := range msg {
			msg[j] = words[rng.Intn(len(words))]
		}
		e := entry(levels[rng.Intn(len(levels))], strings.Join(msg, " "))
		if rng.Intn(4) > 0 {
			e.Fields["service"] = services[rng.Intn(len(services))]
		}
		e.Fields["code"] = fmt.Sprint(200 + 100*rng.Intn(4))
		entries = append(entries, e)
	}
	ix := buildIndex(entries)
	atoms := []string{
		"alpha", "bet*", `"db timeout"`, `"error retry db"`, "level:error", "level:warn", "service:api",
		"service:api*", "service!=web", "code>=400", "code:500", `service=~"^w"`, "service:*", "missing",
	}
	var gen func(depth int) string
	gen = func(depth int) string {
		if depth == 0 || rng.Intn(3) == 0 {
			return atoms[rng.Intn(len(atoms))]
		}
		switch rng.Intn(4) {
		case 0:
			return "(" + gen(depth-1) + " OR " + gen(depth-1) + ")"
		case 1:
			return "NOT " + gen(depth-1)
		default:
			return "(" + gen(depth-1) + " " + gen(depth-1) + ")"
		}
	}
	var tc TokenCache
	for iter := 0; iter < 400; iter++ {
		qs := gen(3)
		q, err := Parse(qs)
		if err != nil {
			t.Fatalf("%s: %v", qs, err)
		}
		c := Plan(q.Root, ix)
		cand := map[uint32]bool{}
		for _, id := range c.List(ix) {
			cand[id] = true
		}
		for i := range entries {
			m := q.Root.Match(&entries[i], &tc)
			if m && !cand[uint32(i)] {
				t.Fatalf("query %s: entry %d matches but is not a candidate", qs, i)
			}
			if c.Exact && cand[uint32(i)] && !m {
				t.Fatalf("query %s: plan claims exact but entry %d does not match", qs, i)
			}
		}
	}
}

func TestIndexKeyTruncation(t *testing.T) {
	long := strings.Repeat("é", MaxIndexedValueLen)
	k := IndexKey(long)
	if len(k) > MaxIndexedValueLen+1 || !strings.HasSuffix(k, string(rune(0))) {
		t.Fatalf("truncated key malformed (len %d)", len(k))
	}
	if IndexKey("short") != "short" {
		t.Fatal("short values are stored verbatim")
	}
	e := entry("", "m", "big", long)
	ix := buildIndex([]logs.Entry{e})
	q, _ := Parse(`big:"` + long + `"`)
	c := Plan(q.Root, ix)
	if !c.All || c.Exact {
		t.Fatal("equality on values longer than the index key must fall back to verification")
	}
	var tc TokenCache
	if !q.Root.Match(&e, &tc) {
		t.Fatal("long value equality must match on verification")
	}
}
