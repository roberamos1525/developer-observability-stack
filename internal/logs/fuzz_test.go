package logs

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// FuzzParsers feeds arbitrary bytes to the JSON/NDJSON and syslog parsers:
// they must never panic, and every accepted entry must satisfy the
// storage invariants.
func FuzzParsers(f *testing.F) {
	for _, seed := range []string{
		`{"message":"x","level":"WARN","nested":{"a":[1,2]}}`, `[{"ts":1700000000}]`, "{\"msg\":1}\n{}\n",
		`<34>1 2026-01-01T00:00:00Z h a p m [id k="v\"]"] msg`, `<13>Jan  1 00:00:00 host app[1]: hi`, "<999>", `<1>1 - - - - - [x`,
	} {
		f.Add(seed)
	}
	check := func(t *testing.T, es []Entry) {
		for _, e := range es {
			if e.Timestamp <= 0 || len(e.Message) > MaxMessageBytes || !utf8.ValidString(e.Message) || len(e.Fields) > MaxFields {
				t.Fatalf("invariant violated: %+v", e)
			}
			for k := range e.Fields {
				if ValidateFieldKey(k) != nil {
					t.Fatalf("invalid field key %q accepted", k)
				}
			}
		}
	}
	f.Fuzz(func(t *testing.T, input string) {
		res, _ := ParseJSON([]byte(input), testNow)
		check(t, res.Entries)
		res, _ = ParseSyslog(strings.NewReader(input), testNow)
		check(t, res.Entries)
	})
}
