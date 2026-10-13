// Package labels implements sorted label sets and label matchers shared by
// the TSDB, the PromQL engine, scraping and alerting.
package labels

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// MetricName is the reserved label holding the metric name.
const MetricName = "__name__"

// Label is a name/value pair.
type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Labels is a set of labels sorted by name with unique names.
type Labels []Label

// New builds a sorted label set, dropping empty values. A later duplicate
// name overrides an earlier one.
func New(ls ...Label) Labels {
	m := make(map[string]string, len(ls))
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return FromMap(m)
}

// FromMap builds a label set from a map, dropping empty values.
func FromMap(m map[string]string) Labels {
	out := make(Labels, 0, len(m))
	for k, v := range m {
		if v != "" {
			out = append(out, Label{k, v})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// FromStrings builds labels from alternating name, value arguments.
func FromStrings(kv ...string) Labels {
	if len(kv)%2 != 0 {
		panic("labels.FromStrings: odd argument count")
	}
	ls := make([]Label, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		ls = append(ls, Label{kv[i], kv[i+1]})
	}
	return New(ls...)
}

// Get returns the value of name or "".
func (ls Labels) Get(name string) string {
	i := sort.Search(len(ls), func(i int) bool { return ls[i].Name >= name })
	if i < len(ls) && ls[i].Name == name {
		return ls[i].Value
	}
	return ""
}

// Has reports whether name is present.
func (ls Labels) Has(name string) bool { return ls.Get(name) != "" }

// Map returns the labels as a map.
func (ls Labels) Map() map[string]string {
	m := make(map[string]string, len(ls))
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}

// String renders the Prometheus text form: name{a="b", ...}.
func (ls Labels) String() string {
	var b strings.Builder
	name := ls.Get(MetricName)
	b.WriteString(name)
	b.WriteByte('{')
	first := true
	for _, l := range ls {
		if l.Name == MetricName {
			continue
		}
		if !first {
			b.WriteString(", ")
		}
		first = false
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(l.Value))
	}
	b.WriteByte('}')
	return b.String()
}

// Key returns an unambiguous identity string (names and values cannot
// contain the 0xff separator byte as valid UTF-8).
func (ls Labels) Key() string {
	var b strings.Builder
	for _, l := range ls {
		b.WriteString(l.Name)
		b.WriteByte(0xff)
		b.WriteString(l.Value)
		b.WriteByte(0xff)
	}
	return b.String()
}

// Hash returns a 64-bit FNV-1a hash of Key().
func (ls Labels) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(ls.Key()))
	return h.Sum64()
}

// Equal reports label set equality.
func Equal(a, b Labels) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Copy returns an independent copy.
func (ls Labels) Copy() Labels { return append(Labels(nil), ls...) }

// Without returns ls minus the named labels.
func (ls Labels) Without(names ...string) Labels {
	out := make(Labels, 0, len(ls))
outer:
	for _, l := range ls {
		for _, n := range names {
			if l.Name == n {
				continue outer
			}
		}
		out = append(out, l)
	}
	return out
}

// Only returns the subset of ls with the given names.
func (ls Labels) Only(names ...string) Labels {
	out := make(Labels, 0, len(names))
	for _, l := range ls {
		for _, n := range names {
			if l.Name == n {
				out = append(out, l)
				break
			}
		}
	}
	return out
}

// DropMetricName removes __name__.
func (ls Labels) DropMetricName() Labels { return ls.Without(MetricName) }

// With returns ls with name set to value (removed when value is empty).
func (ls Labels) With(name, value string) Labels {
	m := ls.Map()
	m[name] = value
	return FromMap(m)
}

var (
	metricNameRe = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelNameRe  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// ValidMetricName reports Prometheus metric name validity.
func ValidMetricName(s string) bool { return metricNameRe.MatchString(s) }

// ValidLabelName reports Prometheus label name validity.
func ValidLabelName(s string) bool { return labelNameRe.MatchString(s) }

// Limits enforced by Validate.
const (
	MaxLabels        = 64
	MaxLabelValueLen = 2048
)

// Validate checks a series' label set.
func (ls Labels) Validate() error {
	name := ls.Get(MetricName)
	if name == "" {
		return fmt.Errorf("missing metric name")
	}
	if !ValidMetricName(name) {
		return fmt.Errorf("invalid metric name %q", name)
	}
	if len(ls) > MaxLabels {
		return fmt.Errorf("too many labels (%d > %d)", len(ls), MaxLabels)
	}
	for i, l := range ls {
		if l.Name != MetricName && !ValidLabelName(l.Name) {
			return fmt.Errorf("invalid label name %q", l.Name)
		}
		if len(l.Value) > MaxLabelValueLen {
			return fmt.Errorf("label %q value too long", l.Name)
		}
		if i > 0 && ls[i-1].Name >= l.Name {
			return fmt.Errorf("labels not sorted or duplicated at %q", l.Name)
		}
	}
	return nil
}

// SanitizeName maps arbitrary strings (e.g. OTLP attribute keys) to valid
// label names by replacing invalid characters with '_'.
func SanitizeName(s string, metric bool) string {
	if s == "" {
		return "_"
	}
	var b strings.Builder
	for i, r := range s {
		ok := r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9' || metric && r == ':'
		if ok {
			b.WriteRune(r)
		} else if i == 0 && r >= '0' && r <= '9' {
			b.WriteByte('_')
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// MatchType is the kind of a label matcher.
type MatchType int

const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (t MatchType) String() string {
	return [...]string{"=", "!=", "=~", "!~"}[t]
}

// Matcher matches a label value.
type Matcher struct {
	Type  MatchType
	Name  string
	Value string
	re    *regexp.Regexp
}

// NewMatcher builds a matcher; regexes are fully anchored as in PromQL.
func NewMatcher(t MatchType, name, value string) (*Matcher, error) {
	m := &Matcher{Type: t, Name: name, Value: value}
	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return nil, fmt.Errorf("invalid regex %q: %w", value, err)
		}
		m.re = re
	}
	return m, nil
}

// MustMatcher is NewMatcher that panics; for tests and constants.
func MustMatcher(t MatchType, name, value string) *Matcher {
	m, err := NewMatcher(t, name, value)
	if err != nil {
		panic(err)
	}
	return m
}

// Matches reports whether v satisfies the matcher.
func (m *Matcher) Matches(v string) bool {
	switch m.Type {
	case MatchEqual:
		return v == m.Value
	case MatchNotEqual:
		return v != m.Value
	case MatchRegexp:
		return m.re.MatchString(v)
	default:
		return !m.re.MatchString(v)
	}
}

func (m *Matcher) String() string { return m.Name + m.Type.String() + strconv.Quote(m.Value) }

// MatchesAll reports whether ls satisfies every matcher (missing labels
// have the empty value).
func MatchesAll(ls Labels, ms []*Matcher) bool {
	for _, m := range ms {
		if !m.Matches(ls.Get(m.Name)) {
			return false
		}
	}
	return true
}
