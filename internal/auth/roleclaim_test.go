package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cdrom/internal/config"
)

// signTokenWithClaims signs a token with the fake IdP's key, merging the
// standard claims with any extra claims (e.g. a "groups" claim for role
// mapping tests).
func signTokenWithClaims(t *testing.T, idp *fakeIDP, subject, audience string, extra map[string]any) string {
	t.Helper()
	claims := map[string]any{
		"iss":   idp.issuer,
		"sub":   subject,
		"aud":   audience,
		"exp":   time.Now().Add(time.Hour).Unix(),
		"iat":   time.Now().Unix(),
		"name":  "Test User",
		"email": "test@example.com",
	}
	for k, v := range extra {
		claims[k] = v
	}
	return signJWT(t, idp.key, idp.kid, claims)
}

// TestRoleClaimMapping verifies that when auth.roles.role_claim is set and a
// token carries that claim, the claim values are mapped to roles via
// auth.roles.role_mappings and appear on the authenticated user.
func TestRoleClaimMapping(t *testing.T) {
	idp := newFakeIDP(t)
	cfg := testAuthConfig(idp)
	cfg.Roles = config.RolesConfig{
		RoleClaim: "groups",
		RoleMappings: map[string]string{
			"platform-ops": "operator",
			"read-only":    "viewer",
		},
	}
	a, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}

	var got User
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = UserFromContext(r.Context())
	})
	h := a.Middleware(next)

	// Token carries a "groups" claim with two values that map to roles.
	token := signTokenWithClaims(t, idp, "u1", "test-client", map[string]any{
		"groups": []string{"platform-ops", "read-only"},
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if !got.HasRole("operator") {
		t.Errorf("user roles = %v, want operator (mapped from platform-ops)", got.Roles)
	}
	if !got.HasRole("viewer") {
		t.Errorf("user roles = %v, want viewer (mapped from read-only)", got.Roles)
	}
}

// TestRoleClaimAsNames verifies that when role_claim_as_names is true, each
// value of the role claim is treated as a role name directly, even without a
// RoleMappings entry.
func TestRoleClaimAsNames(t *testing.T) {
	idp := newFakeIDP(t)
	cfg := testAuthConfig(idp)
	cfg.Roles = config.RolesConfig{
		RoleClaim:        "groups",
		RoleClaimAsNames: true,
	}
	a, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}

	var got User
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = UserFromContext(r.Context())
	})
	h := a.Middleware(next)

	token := signTokenWithClaims(t, idp, "u1", "test-client", map[string]any{
		"groups": []string{"operator", "viewer"},
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if !got.HasRole("operator") {
		t.Errorf("user roles = %v, want operator (claim value as name)", got.Roles)
	}
	if !got.HasRole("viewer") {
		t.Errorf("user roles = %v, want viewer (claim value as name)", got.Roles)
	}
}

// TestRoleClaimUnmappedIgnored verifies that when RoleClaimAsNames is false
// and a claim value has no RoleMappings entry, it is ignored (no role is
// added).
func TestRoleClaimUnmappedIgnored(t *testing.T) {
	idp := newFakeIDP(t)
	cfg := testAuthConfig(idp)
	cfg.Roles = config.RolesConfig{
		RoleClaim:    "groups",
		RoleMappings: map[string]string{"known-group": "operator"},
	}
	a, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}

	var got User
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = UserFromContext(r.Context())
	})
	h := a.Middleware(next)

	// "unknown-group" has no mapping and RoleClaimAsNames is false, so it
	// should be ignored.
	token := signTokenWithClaims(t, idp, "u1", "test-client", map[string]any{
		"groups": []string{"known-group", "unknown-group"},
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if !got.HasRole("operator") {
		t.Errorf("user roles = %v, want operator (mapped from known-group)", got.Roles)
	}
	if got.HasRole("unknown-group") {
		t.Errorf("user roles = %v, must not contain unknown-group (unmapped, RoleClaimAsNames=false)", got.Roles)
	}
}

// TestRoleClaimDisabled verifies that when RoleClaim is empty, claim mapping
// is disabled and no roles are derived from any claim.
func TestRoleClaimDisabled(t *testing.T) {
	idp := newFakeIDP(t)
	cfg := testAuthConfig(idp)
	// No Roles config at all.
	a, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}

	var got User
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = UserFromContext(r.Context())
	})
	h := a.Middleware(next)

	token := signTokenWithClaims(t, idp, "u1", "test-client", map[string]any{
		"groups": []string{"operator"},
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if len(got.Roles) != 0 {
		t.Errorf("user roles = %v, want empty (claim mapping disabled)", got.Roles)
	}
}

// TestRoleClaimCoexistsWithTokenRoles verifies that token-stamped roles (from
// the standard "roles" claim) and claim-mapped roles coexist on the user.
func TestRoleClaimCoexistsWithTokenRoles(t *testing.T) {
	idp := newFakeIDP(t)
	cfg := testAuthConfig(idp)
	cfg.Roles = config.RolesConfig{
		RoleClaim:    "groups",
		RoleMappings: map[string]string{"ops": "operator"},
	}
	a, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}

	var got User
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = UserFromContext(r.Context())
	})
	h := a.Middleware(next)

	// Token has both a "roles" claim (stamped by the IdP) and a "groups"
	// claim (mapped via config).
	token := signTokenWithClaims(t, idp, "u1", "test-client", map[string]any{
		"roles":  []string{"admin"},
		"groups": []string{"ops"},
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if !got.HasRole("admin") {
		t.Errorf("user roles = %v, want admin (from token roles claim)", got.Roles)
	}
	if !got.HasRole("operator") {
		t.Errorf("user roles = %v, want operator (mapped from groups claim)", got.Roles)
	}
}

// TestRoleClaimScalar verifies that a scalar (non-list) role claim value is
// handled correctly.
func TestRoleClaimScalar(t *testing.T) {
	idp := newFakeIDP(t)
	cfg := testAuthConfig(idp)
	cfg.Roles = config.RolesConfig{
		RoleClaim:    "department",
		RoleMappings: map[string]string{"engineering": "operator"},
	}
	a, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}

	var got User
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = UserFromContext(r.Context())
	})
	h := a.Middleware(next)

	// "department" is a scalar string claim, not a list.
	token := signTokenWithClaims(t, idp, "u1", "test-client", map[string]any{
		"department": "engineering",
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if !got.HasRole("operator") {
		t.Errorf("user roles = %v, want operator (mapped from scalar department claim)", got.Roles)
	}
}
