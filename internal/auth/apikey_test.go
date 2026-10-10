package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// errAPIKeyMiss is a sentinel error the fake verifier returns on a miss.
var errAPIKeyMiss = errors.New("api key verification failed")

// fakeAPIKeyVerifier is a stub APIKeyVerifier (F-25) for middleware tests: it
// reports success only for the configured (username, apiKey) pair.
type fakeAPIKeyVerifier struct {
	username string
	apiKey   string
	user     User
}

func (f *fakeAPIKeyVerifier) Verify(ctx context.Context, username, apiKey string) (User, error) {
	if username == f.username && apiKey == f.apiKey {
		return f.user, nil
	}
	return User{}, errAPIKeyMiss
}

// TestMiddlewareAPIKeyWhenOIDCDisabled verifies that the API-key branch of the
// middleware is active even when OIDC authentication is disabled (the default
// local setup, which has no identity provider): a `Bearer <username>:cdrom-…`
// credential is verified against the key directory and, on success, the owner
// is placed in the context, while a request that is not an API-key credential
// passes through unchanged (synthetic admin).
func TestMiddlewareAPIKeyWhenOIDCDisabled(t *testing.T) {
	// OIDC is disabled (nil Provider) but API-key auth is enabled.
	a := &Auth{}
	a.SetAPIKeyVerifier(&fakeAPIKeyVerifier{
		username: "ada@example.com",
		apiKey:   "cdrom-" + "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		user:     User{Subject: "ada@example.com", Email: "ada@example.com", Roles: []string{"admin"}},
	}, true)
	if a.Enabled() {
		t.Fatal("OIDC should be disabled")
	}
	if !a.APIKeyEnabled() {
		t.Fatal("API-key auth should be enabled")
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := UserFromContext(r.Context())
		if u.Subject != "" {
			_, _ = w.Write([]byte("user:" + u.Subject))
			return
		}
		_, _ = w.Write([]byte("anonymous"))
	})
	h := a.Middleware(next)

	// A valid API-key credential authenticates the owner.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer ada@example.com:cdrom-a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid api key: status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "user:ada@example.com" {
		t.Errorf("valid api key: body = %q, want user:ada@example.com", rec.Body.String())
	}

	// A request that is not an API-key credential passes through unchanged
	// (synthetic admin) when OIDC is disabled.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("non-api-key request: status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "anonymous" {
		t.Errorf("non-api-key request: body = %q, want anonymous", rec.Body.String())
	}
}

// TestMiddlewareAPIKeyMissWhenOIDCDisabled verifies that a `Bearer
// <username>:cdrom-…` credential that fails verification is rejected with 401
// even when OIDC is disabled (the key directory is the authority, not a
// passthrough).
func TestMiddlewareAPIKeyMissWhenOIDCDisabled(t *testing.T) {
	a := &Auth{}
	a.SetAPIKeyVerifier(&fakeAPIKeyVerifier{
		username: "ada@example.com",
		apiKey:   "cdrom-" + "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		user:     User{Subject: "ada@example.com"},
	}, true)

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("should not reach"))
	})
	h := a.Middleware(next)

	// A wrong key (right shape, wrong value) is rejected.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer ada@example.com:cdrom-wrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong api key: status = %d, want 401", rec.Code)
	}
}
