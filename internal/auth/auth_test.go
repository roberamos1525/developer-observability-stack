package auth

import (
	"net/http/httptest"
	"testing"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/errs"
)

func TestAuthenticate(t *testing.T) {
	a, err := New(config.AuthConfig{Enabled: true, Keys: []config.APIKey{
		{Tenant: "acme", Key: "acme-writer-key", Scopes: []string{"write"}},
		{Tenant: "acme", KeySHA256: HashKey("acme-admin-key"), Scopes: []string{"admin"}},
		{Tenant: "beta", Key: "beta-reader-key", Scopes: []string{"read"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		header, value string
		tenant        string
		read, write   bool
		kind          errs.Kind
	}{
		{"Authorization", "Bearer acme-writer-key", "acme", false, true, 0},
		{"Authorization", "bearer acme-admin-key", "acme", true, true, 0},
		{"X-API-Key", "beta-reader-key", "beta", true, false, 0},
		{"Authorization", "Bearer wrong", "", false, false, errs.KindUnauthorized},
		{"Authorization", "Basic abc", "", false, false, errs.KindUnauthorized},
		{"", "", "", false, false, errs.KindUnauthorized},
	}
	for i, c := range cases {
		r := httptest.NewRequest("GET", "/", nil)
		if c.header != "" {
			r.Header.Set(c.header, c.value)
		}
		p, err := a.Authenticate(r)
		if c.kind != 0 {
			if errs.KindOf(err) != c.kind {
				t.Errorf("case %d: err %v", i, err)
			}
			continue
		}
		if err != nil || p.Tenant != c.tenant || p.Has(ScopeRead) != c.read || p.Has(ScopeWrite) != c.write {
			t.Errorf("case %d: %+v %v", i, p, err)
		}
		if len(p.KeyID) != 8 {
			t.Errorf("case %d: key id %q", i, p.KeyID)
		}
	}
	if _, err := New(config.AuthConfig{Keys: []config.APIKey{{Tenant: "a", Key: "samekey12"}, {Tenant: "b", Key: "samekey12"}}}); err == nil {
		t.Fatal("duplicate keys must be rejected")
	}
}

func TestAuthDisabledUsesTenantHeader(t *testing.T) {
	a, _ := New(config.AuthConfig{Enabled: false, DefaultTenant: "default"})
	r := httptest.NewRequest("GET", "/", nil)
	p, err := a.Authenticate(r)
	if err != nil || p.Tenant != "default" || !p.Has(ScopeAdmin) {
		t.Fatalf("%+v %v", p, err)
	}
	r.Header.Set(TenantHeader, "team-b")
	if p, _ := a.Authenticate(r); p.Tenant != "team-b" {
		t.Fatal("tenant header ignored")
	}
	r.Header.Set(TenantHeader, "../../etc")
	if _, err := a.Authenticate(r); errs.KindOf(err) != errs.KindInvalid {
		t.Fatal("invalid tenant header must be rejected")
	}
	if a.Enabled() {
		t.Fatal("Enabled")
	}
	ctx := WithPrincipal(r.Context(), Principal{Tenant: "x"})
	if got, ok := FromContext(ctx); !ok || got.Tenant != "x" {
		t.Fatal("context round trip")
	}
}
