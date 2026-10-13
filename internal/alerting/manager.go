package alerting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/errs"
	"github.com/robertramos/developer-observability-stack/internal/selfmon"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// Result is one series/group produced by a rule query.
type Result struct {
	Labels map[string]string
	Value  float64
}

// Querier executes rule queries against a tenant's data.
type Querier interface {
	QueryMetric(ctx context.Context, tenant, expr string, ts time.Time) ([]Result, error)
	CountLogs(ctx context.Context, tenant, query string, start, end time.Time) ([]Result, error)
}

// Options configure a Manager.
type Options struct {
	StatePath         string
	ResolvedRetention time.Duration
	Dispatch          DispatchOptions
	Channels          map[string]Channel
	Querier           Querier
	Metrics           *selfmon.Metrics
	Logger            *slog.Logger
	Now               func() time.Time
}

type ruleState struct {
	rule         Rule
	alerts       map[string]*Alert
	lastEval     time.Time
	lastDuration time.Duration
	lastErr      string
}

type tenantState struct {
	rules    map[string]*ruleState
	silences map[string]*Silence
}

// Manager owns rules, alert state and silences for all tenants.
type Manager struct {
	opts       Options
	mu         sync.Mutex
	tenants    map[string]*tenantState
	dispatcher *Dispatcher
	channelSet map[string]bool
}

// NewManager loads persisted rules and silences.
func NewManager(opts Options) (*Manager, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	m := &Manager{opts: opts, tenants: map[string]*tenantState{}, channelSet: map[string]bool{}}
	for name := range opts.Channels {
		m.channelSet[name] = true
	}
	m.dispatcher = NewDispatcher(opts.Dispatch, opts.Channels, opts.Metrics, opts.Logger)
	m.dispatcher.silenced = m.isSilenced
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

type persisted struct {
	Tenants map[string]struct {
		Rules    []Rule     `json:"rules"`
		Silences []*Silence `json:"silences"`
	} `json:"tenants"`
}

func (m *Manager) load() error {
	if m.opts.StatePath == "" {
		return nil
	}
	raw, err := os.ReadFile(m.opts.StatePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var p persisted
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("alerting state %s: %w", m.opts.StatePath, err)
	}
	for tenant, t := range p.Tenants {
		ts := m.tenant(tenant)
		for _, r := range t.Rules {
			if err := r.Validate(m.channelSet); err != nil {
				m.opts.Logger.Warn("skipping invalid persisted rule", "tenant", tenant, "rule", r.Name, "err", err)
				continue
			}
			ts.rules[r.Name] = &ruleState{rule: r, alerts: map[string]*Alert{}}
		}
		for _, s := range t.Silences {
			if err := s.Validate(); err == nil {
				ts.silences[s.ID] = s
			}
		}
	}
	return nil
}

// save persists state; caller holds m.mu.
func (m *Manager) save() error {
	if m.opts.StatePath == "" {
		return nil
	}
	p := persisted{Tenants: map[string]struct {
		Rules    []Rule     `json:"rules"`
		Silences []*Silence `json:"silences"`
	}{}}
	for id, ts := range m.tenants {
		entry := p.Tenants[id]
		for _, rs := range ts.rules {
			entry.Rules = append(entry.Rules, rs.rule)
		}
		sort.Slice(entry.Rules, func(i, j int) bool { return entry.Rules[i].Name < entry.Rules[j].Name })
		for _, s := range ts.silences {
			entry.Silences = append(entry.Silences, s)
		}
		p.Tenants[id] = entry
	}
	raw, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.opts.StatePath), 0o755); err != nil {
		return err
	}
	tmp := m.opts.StatePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.opts.StatePath)
}

func (m *Manager) tenant(id string) *tenantState {
	ts := m.tenants[id]
	if ts == nil {
		ts = &tenantState{rules: map[string]*ruleState{}, silences: map[string]*Silence{}}
		m.tenants[id] = ts
	}
	return ts
}

// PutRule creates or replaces a rule. Existing alert state is kept when
// the rule is replaced.
func (m *Manager) PutRule(tenant string, r Rule) error {
	if err := r.Validate(m.channelSet); err != nil {
		return errs.Invalid("invalid rule: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ts := m.tenant(tenant)
	if rs := ts.rules[r.Name]; rs != nil {
		rs.rule = r
	} else {
		if len(ts.rules) >= 1000 {
			return errs.Invalid("rule limit (1000) reached")
		}
		ts.rules[r.Name] = &ruleState{rule: r, alerts: map[string]*Alert{}}
	}
	return m.save()
}

// DeleteRule removes a rule and its alerts.
func (m *Manager) DeleteRule(tenant, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts := m.tenants[tenant]
	if ts == nil || ts.rules[name] == nil {
		return errs.NotFound("rule %q not found", name)
	}
	delete(ts.rules, name)
	return m.save()
}

// RuleStatus describes a rule and its evaluation health.
type RuleStatus struct {
	Rule           Rule      `json:"rule"`
	LastEvaluation time.Time `json:"lastEvaluation"`
	LastDurationMs float64   `json:"lastDurationMs"`
	LastError      string    `json:"lastError,omitempty"`
	ActiveAlerts   int       `json:"activeAlerts"`
}

// Rules lists a tenant's rules.
func (m *Manager) Rules(tenant string) []RuleStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []RuleStatus
	if ts := m.tenants[tenant]; ts != nil {
		for _, rs := range ts.rules {
			out = append(out, RuleStatus{Rule: rs.rule, LastEvaluation: rs.lastEval,
				LastDurationMs: float64(rs.lastDuration.Microseconds()) / 1000, LastError: rs.lastErr, ActiveAlerts: len(rs.alerts)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Rule.Name < out[j].Rule.Name })
	return out
}

// Alerts lists a tenant's pending, firing and recently resolved alerts.
func (m *Manager) Alerts(tenant string) []Alert {
	now := m.opts.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Alert
	if ts := m.tenants[tenant]; ts != nil {
		for _, rs := range ts.rules {
			for _, a := range rs.alerts {
				cp := *a
				cp.Silenced = m.silencedLocked(ts, a.Labels, now)
				out = append(out, cp)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Rule != out[j].Rule {
			return out[i].Rule < out[j].Rule
		}
		return out[i].Fingerprint < out[j].Fingerprint
	})
	return out
}

// AddSilence validates and stores a silence, returning its ID.
func (m *Manager) AddSilence(tenant string, s Silence) (Silence, error) {
	if s.StartsAt.IsZero() {
		s.StartsAt = m.opts.Now()
	}
	if err := s.Validate(); err != nil {
		return s, errs.Invalid("invalid silence: %v", err)
	}
	s.ID = newID()
	m.mu.Lock()
	defer m.mu.Unlock()
	ts := m.tenant(tenant)
	if len(ts.silences) >= 1000 {
		return s, errs.Invalid("silence limit (1000) reached")
	}
	ts.silences[s.ID] = &s
	return s, m.save()
}

// ExpireSilence ends a silence immediately.
func (m *Manager) ExpireSilence(tenant, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts := m.tenants[tenant]
	if ts == nil || ts.silences[id] == nil {
		return errs.NotFound("silence %q not found", id)
	}
	s := ts.silences[id]
	if now := m.opts.Now(); s.EndsAt.After(now) {
		s.EndsAt = now
		if s.StartsAt.After(now) {
			s.StartsAt = now.Add(-time.Nanosecond)
		}
	}
	return m.save()
}

// SilenceView adds the computed status.
type SilenceView struct {
	*Silence
	Status string `json:"status"`
}

// Silences lists a tenant's silences, dropping ones expired for over a day.
func (m *Manager) Silences(tenant string) []SilenceView {
	now := m.opts.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []SilenceView
	if ts := m.tenants[tenant]; ts != nil {
		for id, s := range ts.silences {
			if now.Sub(s.EndsAt) > 24*time.Hour {
				delete(ts.silences, id)
				continue
			}
			out = append(out, SilenceView{s, s.Status(now)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartsAt.Before(out[j].StartsAt) })
	return out
}

func (m *Manager) isSilenced(tenant string, ls map[string]string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts := m.tenants[tenant]
	return ts != nil && m.silencedLocked(ts, ls, now)
}

func (m *Manager) silencedLocked(ts *tenantState, ls map[string]string, now time.Time) bool {
	for _, s := range ts.silences {
		if s.Active(now) && s.Matches(ls) {
			return true
		}
	}
	return false
}

type evalTask struct {
	tenant string
	rule   Rule
}

// Evaluate runs every rule once at now and flushes due notifications.
func (m *Manager) Evaluate(ctx context.Context, now time.Time) {
	m.mu.Lock()
	var tasks []evalTask
	for tenant, ts := range m.tenants {
		for _, rs := range ts.rules {
			tasks = append(tasks, evalTask{tenant, rs.rule})
		}
	}
	m.mu.Unlock()
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].tenant+"/"+tasks[i].rule.Name < tasks[j].tenant+"/"+tasks[j].rule.Name
	})
	for _, t := range tasks {
		if ctx.Err() != nil {
			return
		}
		m.evalRule(ctx, t, now)
	}
	m.dispatcher.Flush(ctx, now)
}

func (m *Manager) evalRule(ctx context.Context, t evalTask, now time.Time) {
	start := time.Now()
	results, err := m.query(ctx, t, now)
	took := time.Since(start)
	if m.opts.Metrics != nil {
		res := "success"
		if err != nil {
			res = "error"
		}
		m.opts.Metrics.AlertEvaluations.WithLabelValues(res).Inc()
	}
	m.mu.Lock()
	ts := m.tenants[t.tenant]
	rs := ts.rules[t.rule.Name]
	if rs == nil { // deleted meanwhile
		m.mu.Unlock()
		return
	}
	rs.lastEval, rs.lastDuration = now, took
	if err != nil {
		// Keep alert state unchanged on query failure rather than
		// resolving everything.
		rs.lastErr = err.Error()
		m.mu.Unlock()
		m.opts.Logger.Warn("rule evaluation failed", "tenant", t.tenant, "rule", t.rule.Name, "err", err)
		return
	}
	rs.lastErr = ""
	r := rs.rule
	activeNow := map[string]Result{}
	for _, res := range results {
		ls := map[string]string{}
		for k, v := range res.Labels {
			if k != labels.MetricName {
				ls[k] = v
			}
		}
		for k, v := range r.Labels {
			ls[k] = v
		}
		ls["alertname"] = r.Name
		fp := strconv.FormatUint(labels.FromMap(ls).Hash(), 16)
		activeNow[fp] = Result{Labels: ls, Value: res.Value}
	}
	for fp, res := range activeNow {
		if rs.alerts[fp] == nil {
			rs.alerts[fp] = &Alert{Rule: r.Name, Labels: res.Labels, Fingerprint: fp}
		}
	}
	var changed []*Alert
	for fp, a := range rs.alerts {
		res, active := activeNow[fp]
		if active {
			a.Value = res.Value
			a.Annotations = map[string]string{}
			for k, v := range r.Annotations {
				a.Annotations[k] = expand(k, v, a.Labels, a.Value)
			}
		}
		tr := Step(a, active, now, r.For.D(), m.opts.ResolvedRetention)
		if tr != NoChange && m.opts.Metrics != nil {
			to := [...]string{"", "pending", "firing", "resolved", "inactive"}[tr]
			m.opts.Metrics.AlertTransitions.WithLabelValues(to).Inc()
		}
		if a.State == StateInactive {
			delete(rs.alerts, fp)
			continue
		}
		if a.State == StateFiring || tr == ToResolved {
			changed = append(changed, a)
		}
	}
	m.mu.Unlock()
	m.dispatcher.Update(t.tenant, &r, changed, now)
}

func (m *Manager) query(ctx context.Context, t evalTask, now time.Time) ([]Result, error) {
	if m.opts.Querier == nil {
		return nil, errors.New("no querier configured")
	}
	r := t.rule
	if r.Type == RuleMetric {
		return m.opts.Querier.QueryMetric(ctx, t.tenant, r.Expr, now)
	}
	groups, err := m.opts.Querier.CountLogs(ctx, t.tenant, r.Query, now.Add(-r.Window.D()), now)
	if err != nil {
		return nil, err
	}
	var out []Result
	for _, g := range groups {
		if ok, _ := compare(r.Op, g.Value, r.Threshold); ok {
			out = append(out, g)
		}
	}
	return out, nil
}

// Run evaluates rules every interval until ctx is cancelled.
func (m *Manager) Run(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			m.Evaluate(ctx, m.opts.Now())
		}
	}
}

// Channels lists configured channel names.
func (m *Manager) Channels() []string {
	out := make([]string, 0, len(m.channelSet))
	for c := range m.channelSet {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
