// Package auth implements per-tenant API key authentication.
//
// Keys are never kept in plaintext in memory: each configured key is reduced
// to its SHA-256 digest and requests are authenticated by hashing the
// presented key and looking up the digest. Each key maps to exactly one
// tenant and a set of scopes (read, write, admin).
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/errs"
)

// Scope is a permission granted to an API key.
type Scope string

const (
	ScopeRead  Scope = config.ScopeRead
	ScopeWrite Scope = config.ScopeWrite
	ScopeAdmin Scope = config.ScopeAdmin
)

// TenantHeader selects the tenant when authentication is disabled.
const TenantHeader = "X-Tenant-ID"

// Principal is the authenticated caller.
type Principal struct {
	Tenant string
	KeyID  string // first 8 hex chars of the key digest, safe to log
	scopes map[Scope]bool
}

// Has reports whether the principal holds scope s. Admin implies every scope.
func (p Principal) Has(s Scope) bool { return p.scopes[s] || p.scopes[ScopeAdmin] }

// Scopes lists the granted scopes.
func (p Principal) Scopes() []string {
	out := make([]string, 0, len(p.scopes))
	for _, s := range []Scope{ScopeRead, ScopeWrite, ScopeAdmin} {
		if p.scopes[s] {
			out = append(out, string(s))
		}
	}
	return out
}

type keyEntry struct {
	digest    [32]byte
	principal Principal
}

// Authenticator validates API keys.
type Authenticator struct {
	enabled       bool
	defaultTenant string
	keys          map[[32]byte]keyEntry
}

// New builds an Authenticator from configuration.
func New(cfg config.AuthConfig) (*Authenticator, error) {
	a := &Authenticator{enabled: cfg.Enabled, defaultTenant: cfg.DefaultTenant, keys: map[[32]byte]keyEntry{}}
	for i, k := range cfg.Keys {
		var digest [32]byte
		if k.Key != "" {
			digest = sha256.Sum256([]byte(k.Key))
		} else {
			b, err := hex.DecodeString(k.KeySHA256)
			if err != nil || len(b) != 32 {
				return nil, fmt.Errorf("auth key %d: invalid key_sha256", i)
			}
			copy(digest[:], b)
		}
		if _, dup := a.keys[digest]; dup {
			return nil, fmt.Errorf("auth key %d: duplicate key", i)
		}
		scopes := map[Scope]bool{}
		for _, s := range k.Scopes {
			scopes[Scope(s)] = true
		}
		a.keys[digest] = keyEntry{digest: digest, principal: Principal{
			Tenant: k.Tenant,
			KeyID:  hex.EncodeToString(digest[:4]),
			scopes: scopes,
		}}
	}
	return a, nil
}

// Enabled reports whether authentication is enforced.
func (a *Authenticator) Enabled() bool { return a.enabled }

// ExtractKey returns the API key from Authorization: Bearer or X-API-Key.
func ExtractKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
			return strings.TrimSpace(h[7:])
		}
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}

// Authenticate resolves the request's principal.
func (a *Authenticator) Authenticate(r *http.Request) (Principal, error) {
	if !a.enabled {
		tenant := a.defaultTenant
		if t := r.Header.Get(TenantHeader); t != "" {
			if !config.ValidTenantID(t) {
				return Principal{}, errs.Invalid("invalid %s header", TenantHeader)
			}
			tenant = t
		}
		return Principal{Tenant: tenant, KeyID: "anonymous", scopes: map[Scope]bool{ScopeAdmin: true}}, nil
	}
	key := ExtractKey(r)
	if key == "" {
		return Principal{}, errs.New(errs.KindUnauthorized, "missing API key")
	}
	digest := sha256.Sum256([]byte(key))
	entry, ok := a.keys[digest]
	// The map lookup is keyed by digest, so timing does not depend on the key
	// prefix; the explicit compare guards against any future lookup change.
	if !ok || subtle.ConstantTimeCompare(entry.digest[:], digest[:]) != 1 {
		return Principal{}, errs.New(errs.KindUnauthorized, "invalid API key")
	}
	return entry.principal, nil
}

type ctxKey struct{}

// WithPrincipal stores p in ctx.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// FromContext returns the principal stored in ctx.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// HashKey returns the hex SHA-256 digest of a key, for config files.
func HashKey(key string) string {
	d := sha256.Sum256([]byte(key))
	return hex.EncodeToString(d[:])
}
