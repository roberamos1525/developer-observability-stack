package tenant

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/errs"
	"github.com/robertramos/developer-observability-stack/internal/logs"
)

func testCfg(t *testing.T) config.Config {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	return cfg
}

func TestRegistryLifecycle(t *testing.T) {
	cfg := testCfg(t)
	r := NewRegistry(cfg, slog.New(slog.DiscardHandler))
	a, err := r.Get("acme")
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := r.Get("acme"); again != a {
		t.Fatal("tenants must be cached")
	}
	if _, err := r.Get("../escape"); !errs.Is(err, errs.KindInvalid) {
		t.Fatalf("invalid id must be rejected: %v", err)
	}
	for _, sub := range []string{"logs", "metrics", "traces"} {
		if _, err := os.Stat(filepath.Join(cfg.DataDir, "tenants", "acme", sub)); err != nil {
			t.Fatalf("%s dir missing: %v", sub, err)
		}
	}
	if err := a.Logs.Append([]logs.Entry{{Timestamp: time.Now().UnixNano(), Message: "persist me"}}); err != nil {
		t.Fatal(err)
	}
	r.Get("beta")
	if l := r.List(); len(l) != 2 || l[0].ID != "acme" || l[1].ID != "beta" {
		t.Fatalf("list %v", l)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get("acme"); !errs.Is(err, errs.KindUnavailable) {
		t.Fatal("Get after Close must fail")
	}

	// A new registry discovers tenants on disk.
	r2 := NewRegistry(cfg, slog.New(slog.DiscardHandler))
	defer r2.Close()
	if err := r2.OpenExisting(); err != nil {
		t.Fatal(err)
	}
	if len(r2.List()) != 2 {
		t.Fatal("existing tenants not reopened")
	}
	if st := r2.List()[0].Logs.Stats(); st.Entries != 1 {
		t.Fatalf("data not reopened: %+v", st)
	}
}

func TestRegistryOpenExistingWithoutDir(t *testing.T) {
	r := NewRegistry(testCfg(t), slog.New(slog.DiscardHandler))
	defer r.Close()
	if err := r.OpenExisting(); err != nil || len(r.List()) != 0 {
		t.Fatalf("fresh data dir: %v", err)
	}
}

func TestRegistryTenantLimit(t *testing.T) {
	r := NewRegistry(testCfg(t), slog.New(slog.DiscardHandler))
	defer r.Close()
	// Fill the registry with placeholder entries rather than opening 256
	// real tenants; the limit check only counts map entries.
	for i := 0; i < MaxTenants; i++ {
		r.tenants[string(rune('a'+i%26))+string(rune('0'+i/26))] = &Tenant{}
	}
	if _, err := r.Get("one-more"); !errs.Is(err, errs.KindUnavailable) {
		t.Fatalf("expected tenant limit error, got %v", err)
	}
	r.tenants = map[string]*Tenant{}
}
