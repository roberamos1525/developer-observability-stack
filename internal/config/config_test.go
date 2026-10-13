package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) LookupFunc {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestDefaultsNeedAKeyWhenAuthEnabled(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "OBS_API_KEYS") {
		t.Fatalf("expected missing key error, got %v", err)
	}
	cfg.Auth.Enabled = false
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults with auth disabled must validate: %v", err)
	}
}

func TestLoadFileAndEnvPrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.json")
	os.WriteFile(path, []byte(`{
		"listen_addr": "0.0.0.0:9000",
		"logs": {"retention": "3d", "partition_duration": "30m"},
		"metrics": {"scrape": {"interval": "30s", "timeout": "5s",
			"targets": [{"tenant": "acme", "job": "node", "url": "http://127.0.0.1:9100/metrics"}]}},
		"alerting": {"channels": [{"name": "hook", "type": "webhook", "url": "http://127.0.0.1:9999/hook", "max_retries": 2}]}
	}`), 0o644)
	t.Setenv("OBS_API_KEYS", "acme:supersecretkey:read+write+admin;beta:anotherkey1")
	t.Setenv("OBS_LISTEN_ADDR", "127.0.0.1:7000")
	t.Setenv("OBS_TRACES_RETENTION", "2d")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != "127.0.0.1:7000" {
		t.Fatal("env must override file")
	}
	if cfg.Logs.Retention.D() != 72*time.Hour || cfg.Logs.PartitionDuration.D() != 30*time.Minute {
		t.Fatalf("file values not applied: %+v", cfg.Logs)
	}
	if cfg.Traces.Retention.D() != 48*time.Hour || len(cfg.Auth.Keys) != 2 || len(cfg.Auth.Keys[0].Scopes) != 3 {
		t.Fatalf("env values not applied: %+v %+v", cfg.Traces, cfg.Auth.Keys)
	}
	if len(cfg.Metrics.Scrape.Targets) != 1 || cfg.Alerting.Channels[0].Name != "hook" {
		t.Fatal("nested file values")
	}
	if cfg.Ingest.Workers != Default().Ingest.Workers {
		t.Fatal("unset values keep defaults")
	}
}

func TestLoadRejectsUnknownFieldsAndBadEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cfg.json")
	os.WriteFile(path, []byte(`{"listen_adr": "x"}`), 0o644)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field must fail: %v", err)
	}
	cfg := Default()
	err := ApplyEnv(&cfg, env(map[string]string{"OBS_LOGS_RETENTION": "soon", "OBS_INGEST_WORKERS": "many", "OBS_AUTH_ENABLED": "maybe", "OBS_API_KEYS": "nokey"}))
	if err == nil || strings.Count(err.Error(), "\n") < 3 {
		t.Fatalf("all env errors must be reported: %v", err)
	}
	if strings.Contains(err.Error(), "nokey") && !strings.Contains(err.Error(), "***") {
		t.Fatal("key material must be redacted in errors")
	}
}

func TestValidateCatchesInconsistencies(t *testing.T) {
	mutations := map[string]func(*Config){
		"log level":   func(c *Config) { c.LogLevel = "verbose" },
		"body limits": func(c *Config) { c.MaxDecompressedBytes = c.MaxBodyBytes - 1 },
		"tenant":      func(c *Config) { c.Auth.DefaultTenant = "Bad Tenant" },
		"short key":   func(c *Config) { c.Auth.Keys = []APIKey{{Tenant: "a", Key: "short", Scopes: []string{"read"}}} },
		"both key forms": func(c *Config) {
			c.Auth.Keys = []APIKey{{Tenant: "a", Key: "longenough", KeySHA256: strings.Repeat("a", 64), Scopes: []string{"read"}}}
		},
		"bad scope":       func(c *Config) { c.Auth.Keys = []APIKey{{Tenant: "a", Key: "longenough", Scopes: []string{"root"}}} },
		"bad digest":      func(c *Config) { c.Auth.Keys = []APIKey{{Tenant: "a", KeySHA256: "xyz", Scopes: []string{"read"}}} },
		"workers":         func(c *Config) { c.Ingest.Workers = 0 },
		"retention":       func(c *Config) { c.Logs.Retention = Duration(time.Minute) },
		"query limits":    func(c *Config) { c.Logs.MaxQueryLimit = 1 },
		"chunk size":      func(c *Config) { c.Metrics.ChunkMaxSamples = 1 },
		"ds retention":    func(c *Config) { c.Metrics.DownsampledRetention = Duration(time.Hour) },
		"scrape timeout":  func(c *Config) { c.Metrics.Scrape.Timeout = Duration(time.Hour) },
		"scrape target":   func(c *Config) { c.Metrics.Scrape.Targets = []ScrapeTarget{{Tenant: "a", Job: "j", URL: "ftp://x"}} },
		"repeat interval": func(c *Config) { c.Alerting.RepeatInterval = Duration(time.Second) },
		"dup channel": func(c *Config) {
			c.Alerting.Channels = append(c.Alerting.Channels, ChannelConfig{Name: "log", Type: "log"})
		},
		"webhook url":  func(c *Config) { c.Alerting.Channels = []ChannelConfig{{Name: "w", Type: "webhook", URL: "not a url"}} },
		"channel type": func(c *Config) { c.Alerting.Channels = []ChannelConfig{{Name: "w", Type: "pager"}} },
	}
	for name, mut := range mutations {
		cfg := Default()
		cfg.Auth.Enabled = false
		mut(&cfg)
		if cfg.Validate() == nil {
			t.Errorf("%s: invalid config accepted", name)
		}
	}
}

func TestParseDurationAndKeys(t *testing.T) {
	cases := map[string]time.Duration{"90s": 90 * time.Second, "1h30m": 90 * time.Minute, "2d": 48 * time.Hour,
		"1w": 7 * 24 * time.Hour, "1.5d": 36 * time.Hour, "500ms": 500 * time.Millisecond}
	for in, want := range cases {
		if got, err := ParseDuration(in); err != nil || got != want {
			t.Errorf("ParseDuration(%q) = %v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "d", "5x", "abc"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Errorf("ParseDuration(%q) should fail", bad)
		}
	}
	keys, err := ParseAPIKeys("a:k1longkey; b:k2longkey:read ;")
	if err != nil || len(keys) != 2 || keys[1].Scopes[0] != "read" || len(keys[0].Scopes) != 2 {
		t.Fatalf("keys %+v %v", keys, err)
	}
	var d Duration
	if err := d.UnmarshalJSON([]byte(`"15m"`)); err != nil || d.D() != 15*time.Minute {
		t.Fatal("Duration JSON string")
	}
	if err := d.UnmarshalJSON([]byte(`1000`)); err != nil || d.D() != time.Microsecond {
		t.Fatal("Duration JSON integer nanoseconds")
	}
	if b, _ := d.MarshalJSON(); string(b) != `"1µs"` {
		t.Fatalf("marshal %s", b)
	}
	if !ValidTenantID("team-a_1") || ValidTenantID("../etc") || ValidTenantID("") {
		t.Fatal("tenant id validation")
	}
}
