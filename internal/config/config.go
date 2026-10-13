// Package config loads, defaults and validates the server configuration.
//
// Precedence (lowest to highest): built-in defaults, an optional JSON config
// file, then OBS_* environment variables. Secrets (API keys, webhook HMAC
// secrets) are expected to come from the environment.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Duration is a time.Duration that marshals to/from Go duration strings.
type Duration time.Duration

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		var n int64
		if err2 := json.Unmarshal(b, &n); err2 != nil {
			return fmt.Errorf("duration must be a string like \"5m\" or integer nanoseconds")
		}
		*d = Duration(n)
		return nil
	}
	v, err := ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// ParseDuration extends time.ParseDuration with day ("d") and week ("w") units.
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty duration")
	}
	var total time.Duration
	rest := s
	for rest != "" {
		i := 0
		for i < len(rest) && (rest[i] >= '0' && rest[i] <= '9' || rest[i] == '.') {
			i++
		}
		if i == 0 {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		j := i
		for j < len(rest) && (rest[j] < '0' || rest[j] > '9') && rest[j] != '.' {
			j++
		}
		num, unit := rest[:i], rest[i:j]
		var mult time.Duration
		switch unit {
		case "d":
			mult = 24 * time.Hour
		case "w":
			mult = 7 * 24 * time.Hour
		default:
			d, err := time.ParseDuration(num + unit)
			if err != nil {
				return 0, fmt.Errorf("invalid duration %q", s)
			}
			total += d
			rest = rest[j:]
			continue
		}
		f, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		total += time.Duration(f * float64(mult))
		rest = rest[j:]
	}
	return total, nil
}

// Scope names accepted for API keys.
const (
	ScopeRead  = "read"
	ScopeWrite = "write"
	ScopeAdmin = "admin"
)

// APIKey binds a key to a tenant and a set of scopes. Either Key (plaintext,
// normally injected via OBS_API_KEYS) or KeySHA256 (hex digest, safe to keep
// in a config file) must be set.
type APIKey struct {
	Tenant    string   `json:"tenant"`
	Key       string   `json:"key,omitempty"`
	KeySHA256 string   `json:"key_sha256,omitempty"`
	Scopes    []string `json:"scopes"`
}

type AuthConfig struct {
	Enabled       bool     `json:"enabled"`
	DefaultTenant string   `json:"default_tenant"`
	Keys          []APIKey `json:"keys"`
}

type IngestConfig struct {
	Workers       int      `json:"workers"`
	QueueCapacity int      `json:"queue_capacity"`
	BatchSize     int      `json:"batch_size"`
	FlushInterval Duration `json:"flush_interval"`
	SubmitTimeout Duration `json:"submit_timeout"`
}

type LogsConfig struct {
	PartitionDuration  Duration `json:"partition_duration"`
	SegmentMaxBytes    int64    `json:"segment_max_bytes"`
	SegmentMaxEntries  int      `json:"segment_max_entries"`
	Retention          Duration `json:"retention"`
	CompactMinSegments int      `json:"compact_min_segments"`
	DefaultQueryLimit  int      `json:"default_query_limit"`
	MaxQueryLimit      int      `json:"max_query_limit"`
}

type ScrapeTarget struct {
	Tenant string            `json:"tenant"`
	Job    string            `json:"job"`
	URL    string            `json:"url"`
	Labels map[string]string `json:"labels,omitempty"`
}

type ScrapeConfig struct {
	Interval     Duration       `json:"interval"`
	Timeout      Duration       `json:"timeout"`
	MaxBodyBytes int64          `json:"max_body_bytes"`
	Targets      []ScrapeTarget `json:"targets"`
}

type MetricsConfig struct {
	BlockDuration        Duration     `json:"block_duration"`
	ChunkMaxSamples      int          `json:"chunk_max_samples"`
	RawRetention         Duration     `json:"raw_retention"`
	DownsampleResolution Duration     `json:"downsample_resolution"`
	DownsampleAfter      Duration     `json:"downsample_after"`
	DownsampledRetention Duration     `json:"downsampled_retention"`
	LookbackDelta        Duration     `json:"lookback_delta"`
	QueryTimeout         Duration     `json:"query_timeout"`
	MaxSamplesPerQuery   int          `json:"max_samples_per_query"`
	MaxRangeSteps        int          `json:"max_range_steps"`
	Scrape               ScrapeConfig `json:"scrape"`
}

type TracesConfig struct {
	PartitionDuration Duration `json:"partition_duration"`
	Retention         Duration `json:"retention"`
	MaxSpansPerTrace  int      `json:"max_spans_per_trace"`
	MaxSearchResults  int      `json:"max_search_results"`
}

// ChannelConfig describes a notification channel. Secrets are referenced
// by environment variable name, never stored inline.
type ChannelConfig struct {
	Name       string   `json:"name"`
	Type       string   `json:"type"`
	URL        string   `json:"url,omitempty"`
	SecretEnv  string   `json:"secret_env,omitempty"`
	Timeout    Duration `json:"timeout,omitempty"`
	MaxRetries int      `json:"max_retries,omitempty"`
}

type AlertingConfig struct {
	EvaluationInterval Duration        `json:"evaluation_interval"`
	GroupWait          Duration        `json:"group_wait"`
	GroupInterval      Duration        `json:"group_interval"`
	RepeatInterval     Duration        `json:"repeat_interval"`
	ResolvedRetention  Duration        `json:"resolved_retention"`
	Channels           []ChannelConfig `json:"channels"`
}

type RetentionConfig struct {
	Interval Duration `json:"interval"`
}

// Config is the complete server configuration.
type Config struct {
	ListenAddr           string          `json:"listen_addr"`
	DataDir              string          `json:"data_dir"`
	LogLevel             string          `json:"log_level"`
	LogFormat            string          `json:"log_format"`
	MaxBodyBytes         int64           `json:"max_body_bytes"`
	MaxDecompressedBytes int64           `json:"max_decompressed_bytes"`
	ShutdownTimeout      Duration        `json:"shutdown_timeout"`
	Auth                 AuthConfig      `json:"auth"`
	Ingest               IngestConfig    `json:"ingest"`
	Logs                 LogsConfig      `json:"logs"`
	Metrics              MetricsConfig   `json:"metrics"`
	Traces               TracesConfig    `json:"traces"`
	Alerting             AlertingConfig  `json:"alerting"`
	Retention            RetentionConfig `json:"retention"`
}

// Default returns a configuration with safe, local-development defaults.
func Default() Config {
	return Config{
		ListenAddr:           "127.0.0.1:8428",
		DataDir:              "./var/data",
		LogLevel:             "info",
		LogFormat:            "json",
		MaxBodyBytes:         8 << 20,
		MaxDecompressedBytes: 32 << 20,
		ShutdownTimeout:      Duration(15 * time.Second),
		Auth:                 AuthConfig{Enabled: true, DefaultTenant: "default"},
		Ingest: IngestConfig{
			Workers:       4,
			QueueCapacity: 1024,
			BatchSize:     2000,
			FlushInterval: Duration(200 * time.Millisecond),
			SubmitTimeout: Duration(250 * time.Millisecond),
		},
		Logs: LogsConfig{
			PartitionDuration:  Duration(time.Hour),
			SegmentMaxBytes:    32 << 20,
			SegmentMaxEntries:  200_000,
			Retention:          Duration(7 * 24 * time.Hour),
			CompactMinSegments: 4,
			DefaultQueryLimit:  100,
			MaxQueryLimit:      5000,
		},
		Metrics: MetricsConfig{
			BlockDuration:        Duration(2 * time.Hour),
			ChunkMaxSamples:      120,
			RawRetention:         Duration(15 * 24 * time.Hour),
			DownsampleResolution: Duration(5 * time.Minute),
			DownsampleAfter:      Duration(24 * time.Hour),
			DownsampledRetention: Duration(90 * 24 * time.Hour),
			LookbackDelta:        Duration(5 * time.Minute),
			QueryTimeout:         Duration(30 * time.Second),
			MaxSamplesPerQuery:   5_000_000,
			MaxRangeSteps:        11_000,
			Scrape: ScrapeConfig{
				Interval:     Duration(15 * time.Second),
				Timeout:      Duration(10 * time.Second),
				MaxBodyBytes: 16 << 20,
			},
		},
		Traces: TracesConfig{
			PartitionDuration: Duration(time.Hour),
			Retention:         Duration(3 * 24 * time.Hour),
			MaxSpansPerTrace:  10_000,
			MaxSearchResults:  1000,
		},
		Alerting: AlertingConfig{
			EvaluationInterval: Duration(15 * time.Second),
			GroupWait:          Duration(10 * time.Second),
			GroupInterval:      Duration(1 * time.Minute),
			RepeatInterval:     Duration(4 * time.Hour),
			ResolvedRetention:  Duration(15 * time.Minute),
			Channels:           []ChannelConfig{{Name: "log", Type: "log"}},
		},
		Retention: RetentionConfig{Interval: Duration(1 * time.Minute)},
	}
}

// Load builds a Config from defaults, the optional file at path and the
// process environment, then validates it.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("read config: %w", err)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&cfg); err != nil {
			return cfg, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	if err := ApplyEnv(&cfg, os.LookupEnv); err != nil {
		return cfg, err
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// LookupFunc matches os.LookupEnv so tests can inject an environment.
type LookupFunc func(string) (string, bool)

// ApplyEnv overrides cfg fields from OBS_* environment variables.
func ApplyEnv(cfg *Config, lookup LookupFunc) error {
	var errs []error
	str := func(name string, dst *string) {
		if v, ok := lookup(name); ok && v != "" {
			*dst = v
		}
	}
	dur := func(name string, dst *Duration) {
		if v, ok := lookup(name); ok && v != "" {
			d, err := ParseDuration(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
				return
			}
			*dst = Duration(d)
		}
	}
	integer := func(name string, dst *int) {
		if v, ok := lookup(name); ok && v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", name, err))
				return
			}
			*dst = n
		}
	}
	str("OBS_LISTEN_ADDR", &cfg.ListenAddr)
	str("OBS_DATA_DIR", &cfg.DataDir)
	str("OBS_LOG_LEVEL", &cfg.LogLevel)
	str("OBS_LOG_FORMAT", &cfg.LogFormat)
	str("OBS_DEFAULT_TENANT", &cfg.Auth.DefaultTenant)
	if v, ok := lookup("OBS_AUTH_ENABLED"); ok && v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("OBS_AUTH_ENABLED: %w", err))
		} else {
			cfg.Auth.Enabled = b
		}
	}
	if v, ok := lookup("OBS_API_KEYS"); ok && v != "" {
		keys, err := ParseAPIKeys(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("OBS_API_KEYS: %w", err))
		} else {
			cfg.Auth.Keys = append(cfg.Auth.Keys, keys...)
		}
	}
	integer("OBS_INGEST_WORKERS", &cfg.Ingest.Workers)
	integer("OBS_INGEST_QUEUE_CAPACITY", &cfg.Ingest.QueueCapacity)
	dur("OBS_LOGS_RETENTION", &cfg.Logs.Retention)
	dur("OBS_METRICS_RAW_RETENTION", &cfg.Metrics.RawRetention)
	dur("OBS_METRICS_DOWNSAMPLED_RETENTION", &cfg.Metrics.DownsampledRetention)
	dur("OBS_TRACES_RETENTION", &cfg.Traces.Retention)
	dur("OBS_ALERT_EVAL_INTERVAL", &cfg.Alerting.EvaluationInterval)
	dur("OBS_RETENTION_INTERVAL", &cfg.Retention.Interval)
	dur("OBS_SCRAPE_INTERVAL", &cfg.Metrics.Scrape.Interval)
	return errors.Join(errs...)
}

// ParseAPIKeys parses "tenant:key:scope+scope;tenant2:key2:read".
// Scopes default to read+write when omitted.
func ParseAPIKeys(s string) ([]APIKey, error) {
	var out []APIKey
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Split(part, ":")
		if len(fields) < 2 || len(fields) > 3 {
			return nil, fmt.Errorf("entry %q must be tenant:key[:scopes]", redact(part))
		}
		k := APIKey{Tenant: fields[0], Key: fields[1], Scopes: []string{ScopeRead, ScopeWrite}}
		if len(fields) == 3 && fields[2] != "" {
			k.Scopes = strings.Split(fields[2], "+")
		}
		out = append(out, k)
	}
	return out, nil
}

func redact(s string) string {
	if i := strings.Index(s, ":"); i >= 0 {
		return s[:i] + ":***"
	}
	return "***"
}

var (
	tenantRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	sha256Re = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// ValidTenantID reports whether id is an acceptable tenant identifier. The
// identifier is used as a directory name, so the alphabet is restricted.
func ValidTenantID(id string) bool { return tenantRe.MatchString(id) }

// Validate checks the configuration for internal consistency.
func (c *Config) Validate() error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if c.ListenAddr == "" {
		fail("listen_addr is required")
	}
	if c.DataDir == "" {
		fail("data_dir is required")
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		fail("log_level must be debug|info|warn|error, got %q", c.LogLevel)
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		fail("log_format must be json|text")
	}
	if c.MaxBodyBytes <= 0 || c.MaxDecompressedBytes < c.MaxBodyBytes {
		fail("max_body_bytes must be > 0 and max_decompressed_bytes >= max_body_bytes")
	}
	if !ValidTenantID(c.Auth.DefaultTenant) {
		fail("auth.default_tenant %q is not a valid tenant id", c.Auth.DefaultTenant)
	}
	if c.Auth.Enabled && len(c.Auth.Keys) == 0 {
		fail("auth.enabled requires at least one API key (set OBS_API_KEYS)")
	}
	for i, k := range c.Auth.Keys {
		if !ValidTenantID(k.Tenant) {
			fail("auth.keys[%d]: invalid tenant %q", i, k.Tenant)
		}
		if (k.Key == "") == (k.KeySHA256 == "") {
			fail("auth.keys[%d]: exactly one of key or key_sha256 is required", i)
		}
		if k.Key != "" && len(k.Key) < 8 {
			fail("auth.keys[%d]: key must be at least 8 characters", i)
		}
		if k.KeySHA256 != "" && !sha256Re.MatchString(k.KeySHA256) {
			fail("auth.keys[%d]: key_sha256 must be 64 lowercase hex characters", i)
		}
		if len(k.Scopes) == 0 {
			fail("auth.keys[%d]: at least one scope is required", i)
		}
		for _, s := range k.Scopes {
			if s != ScopeRead && s != ScopeWrite && s != ScopeAdmin {
				fail("auth.keys[%d]: unknown scope %q", i, s)
			}
		}
	}
	in := c.Ingest
	if in.Workers < 1 || in.Workers > 256 {
		fail("ingest.workers must be in [1,256]")
	}
	if in.QueueCapacity < 1 || in.BatchSize < 1 {
		fail("ingest.queue_capacity and ingest.batch_size must be positive")
	}
	if in.FlushInterval <= 0 || in.SubmitTimeout < 0 {
		fail("ingest.flush_interval must be positive and submit_timeout non-negative")
	}
	l := c.Logs
	if l.PartitionDuration.D() < time.Minute {
		fail("logs.partition_duration must be >= 1m")
	}
	if l.SegmentMaxBytes < 1<<16 || l.SegmentMaxEntries < 1 {
		fail("logs.segment_max_bytes must be >= 64KiB and segment_max_entries >= 1")
	}
	if l.Retention.D() < l.PartitionDuration.D() {
		fail("logs.retention must be >= logs.partition_duration")
	}
	if l.CompactMinSegments < 2 {
		fail("logs.compact_min_segments must be >= 2")
	}
	if l.DefaultQueryLimit < 1 || l.MaxQueryLimit < l.DefaultQueryLimit {
		fail("logs query limits must satisfy 1 <= default <= max")
	}
	m := c.Metrics
	if m.BlockDuration.D() < time.Minute {
		fail("metrics.block_duration must be >= 1m")
	}
	if m.ChunkMaxSamples < 2 || m.ChunkMaxSamples > 65535 {
		fail("metrics.chunk_max_samples must be in [2,65535]")
	}
	if m.RawRetention.D() < m.BlockDuration.D() {
		fail("metrics.raw_retention must be >= metrics.block_duration")
	}
	if m.DownsampleResolution.D() < time.Second {
		fail("metrics.downsample_resolution must be >= 1s")
	}
	if m.DownsampledRetention.D() < m.RawRetention.D() {
		fail("metrics.downsampled_retention must be >= metrics.raw_retention")
	}
	if m.LookbackDelta.D() <= 0 || m.QueryTimeout.D() <= 0 {
		fail("metrics.lookback_delta and metrics.query_timeout must be positive")
	}
	if m.MaxSamplesPerQuery < 1 || m.MaxRangeSteps < 1 {
		fail("metrics.max_samples_per_query and max_range_steps must be positive")
	}
	if m.Scrape.Interval.D() < time.Second || m.Scrape.Timeout <= 0 || m.Scrape.Timeout > m.Scrape.Interval {
		fail("metrics.scrape: interval must be >= 1s and 0 < timeout <= interval")
	}
	for i, t := range m.Scrape.Targets {
		if !ValidTenantID(t.Tenant) || t.Job == "" {
			fail("metrics.scrape.targets[%d]: tenant and job are required", i)
		}
		u, err := url.Parse(t.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			fail("metrics.scrape.targets[%d]: url must be an absolute http(s) URL", i)
		}
	}
	t := c.Traces
	if t.PartitionDuration.D() < time.Minute || t.Retention.D() < t.PartitionDuration.D() {
		fail("traces: partition_duration must be >= 1m and retention >= partition_duration")
	}
	if t.MaxSpansPerTrace < 1 || t.MaxSearchResults < 1 {
		fail("traces: max_spans_per_trace and max_search_results must be positive")
	}
	a := c.Alerting
	if a.EvaluationInterval.D() < 100*time.Millisecond {
		fail("alerting.evaluation_interval must be >= 100ms")
	}
	if a.GroupWait < 0 || a.GroupInterval <= 0 || a.RepeatInterval < a.GroupInterval {
		fail("alerting: group_wait >= 0, group_interval > 0 and repeat_interval >= group_interval required")
	}
	if a.ResolvedRetention < 0 {
		fail("alerting.resolved_retention must be >= 0")
	}
	names := map[string]bool{}
	for i, ch := range a.Channels {
		if ch.Name == "" || names[ch.Name] {
			fail("alerting.channels[%d]: name must be non-empty and unique", i)
		}
		names[ch.Name] = true
		switch ch.Type {
		case "log":
		case "webhook":
			u, err := url.Parse(ch.URL)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				fail("alerting.channels[%d]: webhook url must be an absolute http(s) URL", i)
			}
			if ch.MaxRetries < 0 || ch.MaxRetries > 10 {
				fail("alerting.channels[%d]: max_retries must be in [0,10]", i)
			}
		default:
			fail("alerting.channels[%d]: unknown type %q", i, ch.Type)
		}
	}
	if c.Retention.Interval.D() < time.Second {
		fail("retention.interval must be >= 1s")
	}
	return errors.Join(errs...)
}
