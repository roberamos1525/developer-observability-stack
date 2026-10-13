// Package promtext parses the Prometheus text exposition format (0.0.4),
// as served by /metrics endpoints, into metric families.
package promtext

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// Type is a metric family type.
type Type string

const (
	Counter   Type = "counter"
	Gauge     Type = "gauge"
	Histogram Type = "histogram"
	Summary   Type = "summary"
	Untyped   Type = "untyped"
)

// Sample is one exposed sample. Labels include __name__.
type Sample struct {
	Labels      labels.Labels
	Value       float64
	TimestampMs *int64
}

// Family groups samples that share a metric family name.
type Family struct {
	Name    string
	Type    Type
	Help    string
	Samples []Sample
}

// ParseError reports a malformed line.
type ParseError struct {
	Line int
	Msg  string
}

func (e *ParseError) Error() string { return fmt.Sprintf("line %d: %s", e.Line, e.Msg) }

// MaxLineBytes bounds a single exposition line.
const MaxLineBytes = 1 << 20

// Parse reads an exposition document. Families are returned in order of
// first appearance.
func Parse(r io.Reader) ([]*Family, error) {
	p := &parser{byName: map[string]*Family{}}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), MaxLineBytes)
	for sc.Scan() {
		p.line++
		if err := p.parseLine(sc.Text()); err != nil {
			return nil, err
		}
		if p.eof {
			break
		}
	}
	if err := sc.Err(); err != nil {
		return nil, &ParseError{p.line + 1, err.Error()}
	}
	for _, f := range p.families {
		if f.Type == Histogram {
			for _, s := range f.Samples {
				if strings.HasSuffix(s.Labels.Get(labels.MetricName), "_bucket") && !s.Labels.Has("le") {
					return nil, &ParseError{0, fmt.Sprintf("histogram %s has a bucket without le label", f.Name)}
				}
			}
		}
	}
	return p.families, nil
}

type parser struct {
	line     int
	families []*Family
	byName   map[string]*Family
	eof      bool
}

func (p *parser) fail(format string, args ...any) error {
	return &ParseError{p.line, fmt.Sprintf(format, args...)}
}

func (p *parser) family(name string) *Family {
	if f := p.byName[name]; f != nil {
		return f
	}
	f := &Family{Name: name, Type: Untyped}
	p.byName[name] = f
	p.families = append(p.families, f)
	return f
}

func (p *parser) parseLine(line string) error {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil
	}
	if line[0] == '#' {
		return p.parseComment(line)
	}
	return p.parseSample(line)
}

func (p *parser) parseComment(line string) error {
	fields := strings.Fields(line[1:])
	if len(fields) == 1 && fields[0] == "EOF" {
		p.eof = true // OpenMetrics terminator
		return nil
	}
	if len(fields) < 2 || (fields[0] != "HELP" && fields[0] != "TYPE") {
		return nil // ordinary comment
	}
	name := fields[1]
	if !labels.ValidMetricName(name) {
		return p.fail("invalid metric name %q", name)
	}
	f := p.family(name)
	if fields[0] == "HELP" {
		idx := strings.Index(line, name) + len(name)
		f.Help = unescapeHelp(strings.TrimSpace(line[idx:]))
		return nil
	}
	if len(fields) != 3 {
		return p.fail("TYPE line needs exactly one type")
	}
	if len(f.Samples) > 0 {
		return p.fail("TYPE for %s after its samples", name)
	}
	if f.Type != Untyped {
		return p.fail("duplicate TYPE for %s", name)
	}
	switch t := Type(fields[2]); t {
	case Counter, Gauge, Histogram, Summary, Untyped:
		f.Type = t
	default:
		return p.fail("unknown metric type %q", fields[2])
	}
	return nil
}

func unescapeHelp(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
				i++
				continue
			case '\\':
				b.WriteByte('\\')
				i++
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// familyName maps a sample name to its family, stripping the histogram and
// summary suffixes when a matching typed family exists.
func (p *parser) familyName(name string) string {
	for _, suffix := range []string{"_bucket", "_sum", "_count", "_created", "_total"} {
		if base, ok := strings.CutSuffix(name, suffix); ok {
			if f := p.byName[base]; f != nil && f.Type != Untyped {
				return base
			}
		}
	}
	return name
}

func (p *parser) parseSample(line string) error {
	i := 0
	for i < len(line) && line[i] != '{' && line[i] != ' ' && line[i] != '\t' {
		i++
	}
	name := line[:i]
	if !labels.ValidMetricName(name) {
		return p.fail("invalid metric name %q", name)
	}
	ls := []labels.Label{{Name: labels.MetricName, Value: name}}
	rest := line[i:]
	if strings.HasPrefix(rest, "{") {
		var err error
		var parsed []labels.Label
		parsed, rest, err = parseLabels(rest[1:])
		if err != nil {
			return p.fail("%v", err)
		}
		ls = append(ls, parsed...)
	}
	fields := strings.Fields(rest)
	if len(fields) < 1 || len(fields) > 2 {
		return p.fail("expected value [timestamp] after metric")
	}
	v, err := parseFloat(fields[0])
	if err != nil {
		return p.fail("invalid value %q", fields[0])
	}
	s := Sample{Value: v}
	if len(fields) == 2 {
		ts, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return p.fail("invalid timestamp %q", fields[1])
		}
		s.TimestampMs = &ts
	}
	if len(ls) > labels.MaxLabels {
		return p.fail("too many labels (%d > %d)", len(ls), labels.MaxLabels)
	}
	seen := map[string]bool{}
	for _, l := range ls {
		if seen[l.Name] {
			return p.fail("duplicate label %q", l.Name)
		}
		if len(l.Value) > labels.MaxLabelValueLen {
			return p.fail("label %q value exceeds %d bytes", l.Name, labels.MaxLabelValueLen)
		}
		seen[l.Name] = true
	}
	s.Labels = labels.New(ls...)
	f := p.family(p.familyName(name))
	f.Samples = append(f.Samples, s)
	return nil
}

func parseFloat(s string) (float64, error) {
	switch s {
	case "+Inf", "Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(s, 64)
}

// parseLabels parses `a="x",b="y"}` and returns the remainder after '}'.
func parseLabels(s string) ([]labels.Label, string, error) {
	var out []labels.Label
	i := 0
	for {
		for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
		if i < len(s) && s[i] == '}' {
			return out, s[i+1:], nil
		}
		start := i
		for i < len(s) && s[i] != '=' && s[i] != ' ' && s[i] != '}' {
			i++
		}
		name := s[start:i]
		if !labels.ValidLabelName(name) {
			return nil, "", fmt.Errorf("invalid label name %q", name)
		}
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) || s[i] != '=' {
			return nil, "", fmt.Errorf("expected '=' after label %q", name)
		}
		i++
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) || s[i] != '"' {
			return nil, "", fmt.Errorf("label %q value must be quoted", name)
		}
		i++
		var val strings.Builder
		closed := false
		for i < len(s) {
			c := s[i]
			if c == '\\' && i+1 < len(s) {
				switch s[i+1] {
				case 'n':
					val.WriteByte('\n')
				case '"', '\\':
					val.WriteByte(s[i+1])
				default:
					return nil, "", fmt.Errorf("invalid escape in label %q", name)
				}
				i += 2
				continue
			}
			if c == '"' {
				closed = true
				i++
				break
			}
			val.WriteByte(c)
			i++
		}
		if !closed {
			return nil, "", fmt.Errorf("unterminated value for label %q", name)
		}
		out = append(out, labels.Label{Name: name, Value: val.String()})
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i < len(s) && s[i] == ',' {
			i++
			continue
		}
		if i < len(s) && s[i] == '}' {
			return out, s[i+1:], nil
		}
		return nil, "", fmt.Errorf("expected ',' or '}' after label %q", name)
	}
}
