package promtext

import (
	"strings"
	"testing"
)

// FuzzParse checks the exposition parser never panics and that accepted
// samples always carry a valid metric name.
func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		"# TYPE a counter\na_total{x=\"1\"} 1 2\n", "a{b=\"\\\"\n\\\\\"} +Inf\n", "# HELP a h\n\n",
		"h_bucket{le=\"+Inf\"} 1\n", "a{,} 1", "a{b=\"c\",} NaN 1", "# EOF\nx 1",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		fams, err := Parse(strings.NewReader(in))
		if err != nil {
			return
		}
		for _, fam := range fams {
			for _, s := range fam.Samples {
				if err := s.Labels.Validate(); err != nil {
					t.Fatalf("accepted invalid sample %v: %v", s.Labels, err)
				}
			}
		}
	})
}
