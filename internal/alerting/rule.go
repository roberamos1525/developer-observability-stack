// Package alerting evaluates metric (PromQL) and log-count rules, drives a
// per-alert state machine (inactive -> pending -> firing -> resolved),
// applies silences, and dispatches grouped, de-duplicated notifications
// to webhook and log channels.
package alerting

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"text/template"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	logquery "github.com/robertramos/developer-observability-stack/internal/logs/query"
	"github.com/robertramos/developer-observability-stack/internal/promql"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// RuleType selects the data source of a rule.
type RuleType string

const (
	RuleMetric   RuleType = "metric"
	RuleLogCount RuleType = "log_count"
)

// Rule is an alerting rule definition.
type Rule struct {
	Name        string            `json:"name"`
	Type        RuleType          `json:"type"`
	Expr        string            `json:"expr,omitempty"`      // metric: PromQL returning a vector
	Query       string            `json:"query,omitempty"`     // log_count: log query
	Window      config.Duration   `json:"window,omitempty"`    // log_count: look-back window
	Op          string            `json:"op,omitempty"`        // log_count: comparison
	Threshold   float64           `json:"threshold,omitempty"` // log_count: compared with the count
	For         config.Duration   `json:"for,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Channels    []string          `json:"channels,omitempty"`
	GroupBy     []string          `json:"group_by,omitempty"`
}

var ruleNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_:.-]{0,127}$`)

// Validate checks the rule and compiles its query to surface syntax errors.
func (r *Rule) Validate(channels map[string]bool) error {
	if !ruleNameRe.MatchString(r.Name) {
		return fmt.Errorf("rule name must match %s", ruleNameRe)
	}
	switch r.Type {
	case RuleMetric:
		expr, err := promql.Parse(r.Expr)
		if err != nil {
			return fmt.Errorf("expr: %w", err)
		}
		if expr.Type() != promql.TypeVector {
			return fmt.Errorf("expr must return an instant vector, got %s", expr.Type())
		}
	case RuleLogCount:
		q, err := logquery.Parse(r.Query)
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}
		if q.Since != 0 || q.Start != 0 {
			return fmt.Errorf("query must not contain time stages; use window")
		}
		if r.Window.D() < time.Second || r.Window.D() > 7*24*time.Hour {
			return fmt.Errorf("window must be between 1s and 7d")
		}
		if _, ok := compare(r.Op, 0, 0); !ok {
			return fmt.Errorf("op must be one of > >= < <= == !=")
		}
	default:
		return fmt.Errorf("type must be %q or %q", RuleMetric, RuleLogCount)
	}
	if r.For < 0 || r.For.D() > 24*time.Hour {
		return fmt.Errorf("for must be between 0 and 24h")
	}
	for k := range r.Labels {
		if !labels.ValidLabelName(k) {
			return fmt.Errorf("invalid label name %q", k)
		}
	}
	for _, g := range r.GroupBy {
		if !labels.ValidLabelName(g) {
			return fmt.Errorf("invalid group_by label %q", g)
		}
	}
	for _, c := range r.Channels {
		if !channels[c] {
			return fmt.Errorf("unknown channel %q", c)
		}
	}
	for k, v := range r.Annotations {
		if _, err := parseTemplate(k, v); err != nil {
			return fmt.Errorf("annotation %q: %w", k, err)
		}
	}
	return nil
}

func compare(op string, a, b float64) (bool, bool) {
	switch op {
	case ">":
		return a > b, true
	case ">=":
		return a >= b, true
	case "<":
		return a < b, true
	case "<=":
		return a <= b, true
	case "==":
		return a == b, true
	case "!=":
		return a != b, true
	}
	return false, false
}

// templates use Prometheus-style variables: {{ $value }}, {{ $labels.x }}.
const tmplHeader = "{{$value := .Value}}{{$labels := .Labels}}"

func parseTemplate(name, text string) (*template.Template, error) {
	return template.New(name).Option("missingkey=zero").Parse(tmplHeader + text)
}

func expand(name, text string, ls map[string]string, value float64) string {
	t, err := parseTemplate(name, text)
	if err != nil {
		return text
	}
	var buf bytes.Buffer
	data := struct {
		Value  string
		Labels map[string]string
	}{strconv.FormatFloat(value, 'g', 6, 64), ls}
	if err := t.Execute(&buf, data); err != nil {
		return text
	}
	return buf.String()
}

// State is the alert lifecycle state.
type State int

const (
	StateInactive State = iota
	StatePending
	StateFiring
	StateResolved
)

func (s State) String() string {
	return [...]string{"inactive", "pending", "firing", "resolved"}[s]
}

func (s State) MarshalJSON() ([]byte, error) { return []byte(strconv.Quote(s.String())), nil }

// Alert is one alert instance (a rule plus a label set).
type Alert struct {
	Rule        string            `json:"rule"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Fingerprint string            `json:"fingerprint"`
	State       State             `json:"state"`
	Value       float64           `json:"value"`
	ActiveAt    time.Time         `json:"activeAt,omitzero"`
	FiredAt     time.Time         `json:"firedAt,omitzero"`
	ResolvedAt  time.Time         `json:"resolvedAt,omitzero"`
	LastEval    time.Time         `json:"lastEval"`
	Silenced    bool              `json:"silenced"`
}

// Transition is the result of one state machine step.
type Transition int

const (
	NoChange Transition = iota
	ToPending
	ToFiring
	ToResolved
	ToInactive
)

// Step advances the state machine for one evaluation. active reports
// whether the rule condition holds for this alert at time now.
//
//	inactive --active--> pending (or firing when for == 0)
//	pending  --active for >= for--> firing
//	pending  --inactive--> inactive
//	firing   --inactive--> resolved
//	resolved --active--> pending/firing (a new episode)
//	resolved --inactive for >= retention--> inactive (forgotten)
func Step(a *Alert, active bool, now time.Time, forDur, resolvedRetention time.Duration) Transition {
	a.LastEval = now
	switch a.State {
	case StateInactive, StateResolved:
		if active {
			a.ActiveAt, a.FiredAt, a.ResolvedAt = now, time.Time{}, time.Time{}
			if forDur <= 0 {
				a.State, a.FiredAt = StateFiring, now
				return ToFiring
			}
			a.State = StatePending
			return ToPending
		}
		if a.State == StateResolved && now.Sub(a.ResolvedAt) >= resolvedRetention {
			a.State = StateInactive
			return ToInactive
		}
	case StatePending:
		if !active {
			a.State = StateInactive
			return ToInactive
		}
		if now.Sub(a.ActiveAt) >= forDur {
			a.State, a.FiredAt = StateFiring, now
			return ToFiring
		}
	case StateFiring:
		if !active {
			a.State, a.ResolvedAt = StateResolved, now
			return ToResolved
		}
	}
	return NoChange
}
