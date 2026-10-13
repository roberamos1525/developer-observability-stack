package query

import (
	"testing"

	"github.com/robertramos/developer-observability-stack/internal/logs"
)

// FuzzParse checks that arbitrary input never panics the lexer, parser,
// planner or matcher, and that accepted queries are internally consistent.
func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		`level:error AND service:api "connection refused"`, `-timeout | limit 5 | sort asc`,
		`(a OR b) NOT c`, `status>=500 user!~"bot.*"`, `| count by level, service`, `x:"\"quoted\"\\"`,
		`a || b && !c`, `field:* | between 1 2`, `((((`, `"`, `|`, `a:`, `-`, `a | since 5m`,
	} {
		f.Add(seed)
	}
	entry := logs.Entry{Timestamp: 1, Level: "error", Message: "connection refused by api",
		Fields: map[string]string{"service": "api", "status": "502", "user": "bot-7"}}
	ix := buildIndex([]logs.Entry{entry})
	f.Fuzz(func(t *testing.T, input string) {
		q, err := Parse(input)
		if err != nil {
			return
		}
		if q.Root == nil {
			t.Fatal("accepted query without root")
		}
		var tc TokenCache
		matched := q.Root.Match(&entry, &tc)
		c := Plan(q.Root, ix)
		ids := c.List(ix)
		if matched && len(ids) == 0 {
			t.Fatalf("%q matches the entry but the planner excluded it", input)
		}
		if c.Exact && len(ids) == 1 && !matched {
			t.Fatalf("%q: exact plan includes a non-matching entry", input)
		}
		_ = q.Root.String()
	})
}
