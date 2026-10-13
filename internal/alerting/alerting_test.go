package alerting

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/selfmon"
)

var now0 = time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)

func TestStateMachine(t *testing.T) {
	type step struct {
		at     time.Duration
		active bool
		want   State
		tr     Transition
	}
	cases := map[string]struct {
		forDur time.Duration
		steps  []step
	}{
		"pending then firing then resolved then forgotten": {time.Minute, []step{
			{0, true, StatePending, ToPending},
			{30 * time.Second, true, StatePending, NoChange},
			{60 * time.Second, true, StateFiring, ToFiring},
			{90 * time.Second, true, StateFiring, NoChange},
			{120 * time.Second, false, StateResolved, ToResolved},
			{150 * time.Second, false, StateResolved, NoChange},
			{180 * time.Second, false, StateInactive, ToInactive}, // resolved retention 1m
		}},
		"pending flaps back to inactive": {time.Minute, []step{
			{0, true, StatePending, ToPending},
			{30 * time.Second, false, StateInactive, ToInactive},
		}},
		"for zero fires immediately and re-fires after resolve": {0, []step{
			{0, true, StateFiring, ToFiring},
			{10 * time.Second, false, StateResolved, ToResolved},
			{20 * time.Second, true, StateFiring, ToFiring},
		}},
		"resolved re-enters pending": {time.Minute, []step{
			{0, true, StatePending, ToPending},
			{time.Minute, true, StateFiring, ToFiring},
			{2 * time.Minute, false, StateResolved, ToResolved},
			{2*time.Minute + time.Second, true, StatePending, ToPending},
		}},
		"inactive stays inactive": {time.Minute, []step{{0, false, StateInactive, NoChange}}},
	}
	for name, c := range cases {
		a := &Alert{}
		for i, s := range c.steps {
			tr := Step(a, s.active, now0.Add(s.at), c.forDur, time.Minute)
			if a.State != s.want || tr != s.tr {
				t.Fatalf("%s step %d: state=%s tr=%d, want %s/%d", name, i, a.State, tr, s.want, s.tr)
			}
		}
	}
	a := &Alert{}
	Step(a, true, now0, time.Minute, 0)
	Step(a, true, now0.Add(time.Minute), time.Minute, 0)
	if !a.FiredAt.Equal(now0.Add(time.Minute)) || !a.ActiveAt.Equal(now0) {
		t.Fatalf("timestamps %+v", a)
	}
}

func TestRuleValidation(t *testing.T) {
	ch := map[string]bool{"log": true}
	good := []Rule{
		{Name: "HighErrorRate", Type: RuleMetric, Expr: `rate(errors_total[5m]) > 1`, For: config.Duration(time.Minute), Channels: []string{"log"}},
		{Name: "ErrorBurst", Type: RuleLogCount, Query: "level:error | count by service", Window: config.Duration(5 * time.Minute), Op: ">", Threshold: 10,
			Annotations: map[string]string{"summary": "{{ $labels.service }} logged {{ $value }} errors"}},
	}
	for _, r := range good {
		if err := r.Validate(ch); err != nil {
			t.Errorf("%s: %v", r.Name, err)
		}
	}
	bad := []Rule{
		{Name: "bad name!", Type: RuleMetric, Expr: "up"},
		{Name: "x", Type: "other"},
		{Name: "x", Type: RuleMetric, Expr: "rate(up)"},
		{Name: "x", Type: RuleMetric, Expr: "1 + 1"},
		{Name: "x", Type: RuleLogCount, Query: "(", Window: config.Duration(time.Minute), Op: ">"},
		{Name: "x", Type: RuleLogCount, Query: "a | since 5m", Window: config.Duration(time.Minute), Op: ">"},
		{Name: "x", Type: RuleLogCount, Query: "a", Window: 0, Op: ">"},
		{Name: "x", Type: RuleLogCount, Query: "a", Window: config.Duration(time.Minute), Op: "~"},
		{Name: "x", Type: RuleMetric, Expr: "up", Channels: []string{"pager"}},
		{Name: "x", Type: RuleMetric, Expr: "up", Labels: map[string]string{"bad-label": "1"}},
		{Name: "x", Type: RuleMetric, Expr: "up", Annotations: map[string]string{"a": "{{ .Broken"}},
		{Name: "x", Type: RuleMetric, Expr: "up", For: config.Duration(48 * time.Hour)},
	}
	for i, r := range bad {
		if r.Validate(ch) == nil {
			t.Errorf("bad rule %d accepted", i)
		}
	}
	if got := expand("s", "{{ $labels.service }} at {{ $value }}", map[string]string{"service": "api"}, 12.5); got != "api at 12.5" {
		t.Fatalf("template expansion %q", got)
	}
}

func TestSilences(t *testing.T) {
	no := false
	s := Silence{Matchers: []Matcher{{Name: "alertname", Value: "High.*", IsRegex: true}, {Name: "env", Value: "dev", IsEqual: &no}},
		StartsAt: now0, EndsAt: now0.Add(time.Hour), CreatedBy: "oncall"}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	if !s.Matches(map[string]string{"alertname": "HighLatency", "env": "prod"}) || s.Matches(map[string]string{"alertname": "HighLatency", "env": "dev"}) {
		t.Fatal("matcher semantics")
	}
	if !s.Active(now0) || s.Active(now0.Add(time.Hour)) || s.Status(now0.Add(-time.Second)) != "pending" || s.Status(now0.Add(2*time.Hour)) != "expired" {
		t.Fatal("activity window")
	}
	bad := []Silence{
		{CreatedBy: "x", StartsAt: now0, EndsAt: now0.Add(time.Hour)},
		{Matchers: []Matcher{{Name: "a", Value: ".*", IsRegex: true}}, CreatedBy: "x", StartsAt: now0, EndsAt: now0.Add(time.Hour)},
		{Matchers: []Matcher{{Name: "a", Value: "b"}}, CreatedBy: "x", StartsAt: now0, EndsAt: now0},
		{Matchers: []Matcher{{Name: "a", Value: "("}}, CreatedBy: "x", StartsAt: now0, EndsAt: now0.Add(time.Hour)},
		{Matchers: []Matcher{{Name: "a", Value: "(", IsRegex: true}}, CreatedBy: "x", StartsAt: now0, EndsAt: now0.Add(time.Hour)},
		{Matchers: []Matcher{{Name: "a", Value: "b"}}, StartsAt: now0, EndsAt: now0.Add(time.Hour)},
	}
	for i, b := range bad {
		if i == 3 {
			continue // "(" is a valid literal (non-regex) value
		}
		if b.Validate() == nil {
			t.Errorf("bad silence %d accepted", i)
		}
	}
}

type recorder struct {
	mu   sync.Mutex
	sent []Notification
}

func (r *recorder) Name() string { return "rec" }
func (r *recorder) Send(_ context.Context, n Notification) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, n)
	return nil
}
func (r *recorder) count() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.sent) }
func (r *recorder) last() Notification {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sent[len(r.sent)-1]
}

func TestDispatcherGroupingDedupAndRepeat(t *testing.T) {
	rec := &recorder{}
	d := NewDispatcher(DispatchOptions{GroupWait: 10 * time.Second, GroupInterval: time.Minute, RepeatInterval: time.Hour},
		map[string]Channel{"rec": rec}, nil, slog.New(slog.DiscardHandler))
	rule := &Rule{Name: "HighLatency", GroupBy: []string{"alertname", "cluster"}}
	firing := func(fp, cluster string) *Alert {
		return &Alert{Rule: rule.Name, Fingerprint: fp, State: StateFiring, FiredAt: now0,
			Labels: map[string]string{"alertname": rule.Name, "cluster": cluster, "pod": fp}}
	}
	d.Update("t1", rule, []*Alert{firing("a", "eu"), firing("b", "eu"), firing("c", "us")}, now0)
	if n := d.Flush(context.Background(), now0.Add(5*time.Second)); n != 0 {
		t.Fatal("group_wait not respected")
	}
	if n := d.Flush(context.Background(), now0.Add(10*time.Second)); n != 2 {
		t.Fatalf("expected one notification per group (2), got %d", n)
	}
	for _, n := range rec.sent {
		if n.GroupLabels["cluster"] == "eu" && len(n.Alerts) != 2 {
			t.Fatalf("eu group should carry 2 alerts: %+v", n)
		}
	}
	// Unchanged state: de-duplicated until the repeat interval.
	d.Update("t1", rule, []*Alert{firing("a", "eu"), firing("b", "eu")}, now0.Add(2*time.Minute))
	if n := d.Flush(context.Background(), now0.Add(2*time.Minute)); n != 0 {
		t.Fatal("identical group content must not be re-sent")
	}
	if n := d.Flush(context.Background(), now0.Add(time.Hour+11*time.Second)); n != 2 {
		t.Fatalf("repeat interval re-notification, got %d", n)
	}
	// A resolved alert changes the group: sent after group_interval, once.
	res := firing("a", "eu")
	res.State, res.ResolvedAt = StateResolved, now0.Add(2*time.Hour)
	d.Update("t1", rule, []*Alert{res}, now0.Add(2*time.Hour))
	before := rec.count()
	d.Flush(context.Background(), now0.Add(2*time.Hour))
	if rec.count() != before+1 {
		t.Fatal("changed group must notify")
	}
	last := rec.last()
	if last.Status != "firing" || len(last.Alerts) != 2 {
		t.Fatalf("group with one firing and one resolved alert: %+v", last)
	}
	// Silenced alerts are not delivered.
	d.silenced = func(_ string, ls map[string]string, _ time.Time) bool { return ls["cluster"] == "us" }
	d.Update("t1", rule, []*Alert{firing("d", "us")}, now0.Add(3*time.Hour))
	before = rec.count()
	d.Flush(context.Background(), now0.Add(4*time.Hour))
	for _, n := range rec.sent[before:] {
		if n.GroupLabels["cluster"] == "us" {
			t.Fatal("silenced group was notified")
		}
	}
	if d.Groups() != 2 {
		t.Fatalf("groups %d", d.Groups())
	}
}

func TestWebhookSignatureAndRetries(t *testing.T) {
	var calls atomic.Int32
	var got Notification
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.Header.Get(SignatureHeader) != Sign([]byte("s3cret"), body) {
			t.Error("bad signature")
		}
		json.Unmarshal(body, &got)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	env := map[string]string{"HOOK_SECRET": "s3cret"}
	chans, err := BuildChannels([]config.ChannelConfig{
		{Name: "hook", Type: "webhook", URL: srv.URL, SecretEnv: "HOOK_SECRET", MaxRetries: 3},
		{Name: "log", Type: "log"},
	}, func(k string) (string, bool) { v, ok := env[k]; return v, ok }, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	hook := chans["hook"].(*WebhookChannel)
	hook.backoff = time.Millisecond
	n := Notification{Version: "1", Tenant: "t", Status: "firing", Alerts: []NotifiedAlert{{Fingerprint: "x"}}}
	if err := hook.Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || got.Tenant != "t" {
		t.Fatalf("calls=%d got=%+v", calls.Load(), got)
	}
	if err := chans["log"].Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	clientErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer clientErr.Close()
	calls.Store(0)
	hook.url = clientErr.URL
	if err := hook.Send(context.Background(), n); err == nil || calls.Load() != 1 {
		t.Fatalf("4xx must fail without retry (calls=%d)", calls.Load())
	}
	if _, err := BuildChannels([]config.ChannelConfig{{Name: "h", Type: "webhook", URL: srv.URL, SecretEnv: "MISSING"}},
		func(string) (string, bool) { return "", false }, nil); err == nil {
		t.Fatal("missing secret env must fail")
	}
}

type fakeQuerier struct {
	mu      sync.Mutex
	metric  []Result
	logs    []Result
	failing bool
}

func (f *fakeQuerier) QueryMetric(context.Context, string, string, time.Time) ([]Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return nil, io.ErrUnexpectedEOF
	}
	return f.metric, nil
}

func (f *fakeQuerier) CountLogs(context.Context, string, string, time.Time, time.Time) ([]Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logs, nil
}

func TestManagerEndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alerting.json")
	q := &fakeQuerier{}
	rec := &recorder{}
	clock := now0
	opts := Options{StatePath: path, ResolvedRetention: time.Minute, Querier: q, Metrics: selfmon.New(),
		Channels: map[string]Channel{"rec": rec}, Now: func() time.Time { return clock },
		Dispatch: DispatchOptions{GroupWait: 0, GroupInterval: time.Second, RepeatInterval: time.Hour}}
	m, err := NewManager(opts)
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(m.PutRule("acme", Rule{Name: "HighErrors", Type: RuleMetric, Expr: `errors > 5`, For: config.Duration(30 * time.Second),
		Labels: map[string]string{"severity": "page"}, Annotations: map[string]string{"summary": "{{ $labels.job }}={{ $value }}"}}))
	must(m.PutRule("acme", Rule{Name: "ErrorLogs", Type: RuleLogCount, Query: "level:error | count by service",
		Window: config.Duration(5 * time.Minute), Op: ">=", Threshold: 3}))
	if err := m.PutRule("acme", Rule{Name: "bad", Type: RuleMetric, Expr: "("}); err == nil {
		t.Fatal("invalid rule accepted")
	}

	q.metric = []Result{{Labels: map[string]string{"__name__": "errors", "job": "api"}, Value: 9}}
	q.logs = []Result{{Labels: map[string]string{"service": "api"}, Value: 4}, {Labels: map[string]string{"service": "web"}, Value: 1}}
	m.Evaluate(context.Background(), clock)
	alerts := m.Alerts("acme")
	if len(alerts) != 2 {
		t.Fatalf("alerts %+v", alerts)
	}
	states := map[string]State{}
	for _, a := range alerts {
		states[a.Rule] = a.State
	}
	if states["HighErrors"] != StatePending || states["ErrorLogs"] != StateFiring {
		t.Fatalf("states %v", states)
	}
	if rec.count() != 1 {
		t.Fatalf("log rule (for=0) should notify immediately, sent %d", rec.count())
	}
	clock = clock.Add(30 * time.Second)
	m.Evaluate(context.Background(), clock)
	if rec.count() != 2 {
		t.Fatalf("metric rule should fire after its for duration, sent %d", rec.count())
	}
	n := rec.last()
	if n.Alerts[0].Labels["severity"] != "page" || n.Alerts[0].Annotations["summary"] != "api=9" || n.Alerts[0].Labels["__name__"] != "" {
		t.Fatalf("labels/annotations %+v", n.Alerts[0])
	}
	// Query failures keep state.
	q.failing = true
	clock = clock.Add(time.Second)
	m.Evaluate(context.Background(), clock)
	if rs := m.Rules("acme"); rs[0].LastError == "" && rs[1].LastError == "" {
		t.Fatal("evaluation error not recorded")
	}
	q.failing = false
	// Silence the metric alert, then resolve both conditions.
	sil, err := m.AddSilence("acme", Silence{Matchers: []Matcher{{Name: "alertname", Value: "HighErrors"}},
		EndsAt: clock.Add(time.Hour), CreatedBy: "test"})
	must(err)
	for _, a := range m.Alerts("acme") {
		if a.Rule == "HighErrors" && !a.Silenced {
			t.Fatal("alert should be marked silenced")
		}
	}
	q.metric, q.logs = nil, nil
	clock = clock.Add(2 * time.Second)
	before := rec.count()
	m.Evaluate(context.Background(), clock)
	for _, n := range rec.sent[before:] {
		if n.Status != "resolved" || n.Alerts[0].Labels["alertname"] != "ErrorLogs" {
			t.Fatalf("only the unsilenced alert should resolve-notify: %+v", n)
		}
	}
	clock = clock.Add(2 * time.Minute)
	m.Evaluate(context.Background(), clock)
	if len(m.Alerts("acme")) != 0 {
		t.Fatalf("resolved alerts must be forgotten after retention: %+v", m.Alerts("acme"))
	}
	must(m.ExpireSilence("acme", sil.ID))
	if v := m.Silences("acme"); len(v) != 1 || v[0].Status != "expired" {
		t.Fatalf("silences %+v", v)
	}
	if err := m.ExpireSilence("acme", "nope"); err == nil {
		t.Fatal("unknown silence")
	}

	// Rules and silences survive a restart.
	m2, err := NewManager(opts)
	must(err)
	if len(m2.Rules("acme")) != 2 || len(m2.Silences("acme")) != 1 || len(m2.Rules("other")) != 0 {
		t.Fatal("state not persisted")
	}
	must(m2.DeleteRule("acme", "ErrorLogs"))
	if err := m2.DeleteRule("acme", "ErrorLogs"); err == nil {
		t.Fatal("double delete must fail")
	}
	if strings.Join(m2.Channels(), ",") != "rec" {
		t.Fatal("channels")
	}
}
