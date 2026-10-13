package alerting

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/selfmon"
)

// NotifiedAlert is an alert as delivered to channels.
type NotifiedAlert struct {
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations,omitempty"`
	StartsAt    time.Time         `json:"startsAt"`
	EndsAt      time.Time         `json:"endsAt,omitzero"`
	Value       float64           `json:"value"`
	Fingerprint string            `json:"fingerprint"`
}

// Notification is one grouped delivery (Alertmanager-like payload).
type Notification struct {
	Version     string            `json:"version"`
	Tenant      string            `json:"tenant"`
	GroupKey    string            `json:"groupKey"`
	Status      string            `json:"status"`
	Receiver    string            `json:"receiver"`
	GroupLabels map[string]string `json:"groupLabels"`
	Alerts      []NotifiedAlert   `json:"alerts"`
}

// Channel delivers notifications.
type Channel interface {
	Name() string
	Send(ctx context.Context, n Notification) error
}

// LogChannel writes notifications to the structured log.
type LogChannel struct {
	name string
	log  *slog.Logger
}

func (c *LogChannel) Name() string { return c.name }

func (c *LogChannel) Send(_ context.Context, n Notification) error {
	names := make([]string, len(n.Alerts))
	for i, a := range n.Alerts {
		names[i] = a.Labels["alertname"] + "/" + a.Status
	}
	c.log.Info("alert notification", "channel", c.name, "tenant", n.Tenant, "group", n.GroupKey,
		"status", n.Status, "alerts", strings.Join(names, ","))
	return nil
}

// WebhookChannel POSTs JSON notifications, optionally signed with
// HMAC-SHA256 (header X-Obs-Signature: sha256=<hex>), retrying network
// errors, 429 and 5xx responses with exponential backoff.
type WebhookChannel struct {
	name       string
	url        string
	secret     []byte
	client     *http.Client
	maxRetries int
	backoff    time.Duration
}

func (c *WebhookChannel) Name() string { return c.name }

// SignatureHeader carries the payload HMAC.
const SignatureHeader = "X-Obs-Signature"

// Sign returns the signature header value for body.
func Sign(secret, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

func (c *WebhookChannel) Send(ctx context.Context, n Notification) error {
	body, err := json.Marshal(n)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(c.backoff << (attempt - 1)):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "developer-observability-stack/alerting")
		if len(c.secret) > 0 {
			req.Header.Set(SignatureHeader, Sign(c.secret, body))
		}
		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("webhook %s returned %s", c.name, resp.Status)
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
			return lastErr // client errors are not retried
		}
	}
	return lastErr
}

// BuildChannels instantiates configured channels. Webhook secrets are read
// from the environment variable named by secret_env.
func BuildChannels(cfgs []config.ChannelConfig, lookup func(string) (string, bool), log *slog.Logger) (map[string]Channel, error) {
	out := map[string]Channel{}
	for _, c := range cfgs {
		switch c.Type {
		case "log":
			out[c.Name] = &LogChannel{name: c.Name, log: log}
		case "webhook":
			timeout := c.Timeout.D()
			if timeout <= 0 {
				timeout = 5 * time.Second
			}
			ch := &WebhookChannel{name: c.Name, url: c.URL, client: &http.Client{Timeout: timeout},
				maxRetries: c.MaxRetries, backoff: 200 * time.Millisecond}
			if c.SecretEnv != "" {
				v, ok := lookup(c.SecretEnv)
				if !ok || v == "" {
					return nil, fmt.Errorf("channel %s: environment variable %s is not set", c.Name, c.SecretEnv)
				}
				ch.secret = []byte(v)
			}
			out[c.Name] = ch
		default:
			return nil, fmt.Errorf("channel %s: unknown type %q", c.Name, c.Type)
		}
	}
	return out, nil
}

// DispatchOptions control grouping timing.
type DispatchOptions struct {
	GroupWait      time.Duration
	GroupInterval  time.Duration
	RepeatInterval time.Duration
}

type alertGroup struct {
	key      string
	tenant   string
	labels   map[string]string
	channels []string
	alerts   map[string]*Alert // by fingerprint
	created  time.Time
	lastSent time.Time
	lastHash uint64
	sentOnce bool
}

// Dispatcher groups alerts and decides when to notify:
//   - a new group waits GroupWait before its first notification;
//   - a changed group (new/resolved alerts) notifies at most every GroupInterval;
//   - an unchanged firing group re-notifies every RepeatInterval;
//   - identical content is never re-sent earlier (de-duplication);
//   - silenced alerts are excluded from notifications.
type Dispatcher struct {
	mu       sync.Mutex
	opts     DispatchOptions
	channels map[string]Channel
	groups   map[string]*alertGroup
	silenced func(tenant string, ls map[string]string, now time.Time) bool
	metrics  *selfmon.Metrics
	log      *slog.Logger
}

// NewDispatcher creates a dispatcher.
func NewDispatcher(opts DispatchOptions, channels map[string]Channel, m *selfmon.Metrics, log *slog.Logger) *Dispatcher {
	return &Dispatcher{opts: opts, channels: channels, groups: map[string]*alertGroup{}, metrics: m, log: log,
		silenced: func(string, map[string]string, time.Time) bool { return false }}
}

func groupKey(tenant string, r *Rule, ls map[string]string) (string, map[string]string) {
	by := r.GroupBy
	if len(by) == 0 {
		by = []string{"alertname"}
	}
	gl := map[string]string{}
	parts := []string{tenant}
	for _, k := range by {
		gl[k] = ls[k]
		parts = append(parts, k+"="+ls[k])
	}
	return strings.Join(parts, "|"), gl
}

// Update records the latest firing/resolved alerts of a rule.
func (d *Dispatcher) Update(tenant string, r *Rule, alerts []*Alert, now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	channels := r.Channels
	if len(channels) == 0 {
		for name := range d.channels {
			channels = append(channels, name)
		}
		sort.Strings(channels)
	}
	for _, a := range alerts {
		if a.State != StateFiring && a.State != StateResolved {
			continue
		}
		key, gl := groupKey(tenant, r, a.Labels)
		g := d.groups[key]
		if g == nil {
			if a.State == StateResolved {
				continue // never notified as firing; nothing to resolve
			}
			g = &alertGroup{key: key, tenant: tenant, labels: gl, alerts: map[string]*Alert{}, created: now}
			d.groups[key] = g
		}
		g.channels = channels
		cp := *a
		g.alerts[a.Fingerprint] = &cp
	}
}

func (g *alertGroup) snapshot(now time.Time, silenced func(string, map[string]string, time.Time) bool) ([]NotifiedAlert, uint64, bool) {
	var out []NotifiedAlert
	firing := false
	for _, a := range g.alerts {
		if silenced(g.tenant, a.Labels, now) {
			continue
		}
		na := NotifiedAlert{Status: a.State.String(), Labels: a.Labels, Annotations: a.Annotations,
			StartsAt: a.FiredAt, Value: a.Value, Fingerprint: a.Fingerprint}
		if a.State == StateResolved {
			na.EndsAt = a.ResolvedAt
		} else {
			firing = true
		}
		out = append(out, na)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Fingerprint < out[j].Fingerprint })
	h := fnv.New64a()
	for _, a := range out {
		h.Write([]byte(a.Fingerprint + a.Status))
	}
	return out, h.Sum64(), firing
}

// Flush sends due notifications. It returns the number of notifications
// attempted (one per group and channel).
func (d *Dispatcher) Flush(ctx context.Context, now time.Time) int {
	type job struct {
		g *alertGroup
		n Notification
	}
	var jobs []job
	d.mu.Lock()
	for key, g := range d.groups {
		alerts, hash, firing := g.snapshot(now, d.silenced)
		due := false
		switch {
		case len(alerts) == 0:
			// Everything is silenced: resolved alerts will never be sent.
			for fp, a := range g.alerts {
				if a.State == StateResolved {
					delete(g.alerts, fp)
				}
			}
		case !g.sentOnce:
			due = now.Sub(g.created) >= d.opts.GroupWait
		case hash != g.lastHash:
			due = now.Sub(g.lastSent) >= d.opts.GroupInterval
		case firing:
			due = now.Sub(g.lastSent) >= d.opts.RepeatInterval
		}
		if due {
			status := "resolved"
			if firing {
				status = "firing"
			}
			for _, ch := range g.channels {
				jobs = append(jobs, job{g, Notification{Version: "1", Tenant: g.tenant, GroupKey: key, Status: status,
					Receiver: ch, GroupLabels: g.labels, Alerts: alerts}})
			}
			g.sentOnce, g.lastSent, g.lastHash = true, now, hash
			for fp, a := range g.alerts {
				if a.State == StateResolved {
					delete(g.alerts, fp) // resolved alerts are delivered once
				}
			}
		}
		if len(g.alerts) == 0 {
			delete(d.groups, key)
		}
	}
	d.mu.Unlock()
	for _, j := range jobs {
		ch := d.channels[j.n.Receiver]
		if ch == nil {
			continue
		}
		err := ch.Send(ctx, j.n)
		result := "success"
		if err != nil {
			result = "failure"
			d.log.Error("notification failed", "channel", j.n.Receiver, "group", j.n.GroupKey, "err", err)
		}
		if d.metrics != nil {
			d.metrics.Notifications.WithLabelValues(j.n.Receiver, result).Inc()
		}
	}
	return len(jobs)
}

// Groups returns the number of active notification groups.
func (d *Dispatcher) Groups() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.groups)
}
