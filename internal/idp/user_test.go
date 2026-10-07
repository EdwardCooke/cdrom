package idp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	oidc "github.com/coreos/go-oidc/v3/oidc"
)

// newTestUserServer starts an IdP with an in-memory user store and returns the
// test server, the user store (so tests can inspect state), and the key
// manager.
func newTestUserServer(t *testing.T) (*httptest.Server, *MemoryUserStore, *KeyManager) {
	t.Helper()
	cfg := testConfig(time.Hour, 30*time.Minute)
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	users := NewMemoryUserStore()
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger()).WithUsers(users)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, users, km
}

// postJSON posts a JSON body to url and returns the response status and body.
func postJSON(t *testing.T, url string, body any) (int, []byte) {
	t.Helper()
	data, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", strings.NewReader(string(data)))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// TestUserRegisterAndLogin drives the full username/password flow: register a
// user, log in, and verify the returned token with go-oidc (the same library
// the API uses). It checks the token's subject, email, and roles claims.
func TestUserRegisterAndLogin(t *testing.T) {
	ts, users, _ := newTestUserServer(t)
	ctx := context.Background()

	// The API would do this at startup: OIDC discovery against the issuer.
	provider, err := oidc.NewProvider(ctx, ts.URL)
	if err != nil {
		t.Fatalf("oidc discovery: %v", err)
	}
	verifier := provider.Verifier(&oidc.Config{SkipClientIDCheck: true})

	// Register the first user (it should be given the admin role).
	code, body := postJSON(t, ts.URL+"/register", map[string]any{
		"first_name": "Ada",
		"last_name":  "Lovelace",
		"email":      "ada@example.com",
		"password":   "s3cret",
	})
	if code != http.StatusCreated {
		t.Fatalf("register status = %d, want 201 (body %s)", code, body)
	}
	var reg struct {
		ID    string   `json:"id"`
		Email string   `json:"email"`
		Roles []string `json:"roles"`
	}
	if err := json.Unmarshal(body, &reg); err != nil {
		t.Fatalf("decode register: %v", err)
	}
	if reg.Email != "ada@example.com" {
		t.Errorf("register email = %q, want ada@example.com", reg.Email)
	}
	if !hasRole(reg.Roles, "admin") {
		t.Errorf("first user roles = %v, want to include admin", reg.Roles)
	}

	// Log in with the correct password.
	code, body = postJSON(t, ts.URL+"/login", map[string]any{
		"email":    "ada@example.com",
		"password": "s3cret",
	})
	if code != http.StatusOK {
		t.Fatalf("login status = %d, want 200 (body %s)", code, body)
	}
	var login struct {
		AccessToken string   `json:"access_token"`
		IDToken     string   `json:"id_token"`
		Email       string   `json:"email"`
		Roles       []string `json:"roles"`
	}
	if err := json.Unmarshal(body, &login); err != nil {
		t.Fatalf("decode login: %v", err)
	}
	if login.AccessToken == "" {
		t.Fatal("login returned no access_token")
	}
	if !hasRole(login.Roles, "admin") {
		t.Errorf("login roles = %v, want to include admin", login.Roles)
	}

	// Verify the token exactly as the API would.
	idt, err := verifier.Verify(ctx, login.AccessToken)
	if err != nil {
		t.Fatalf("verify token: %v", err)
	}
	if idt.Subject != reg.ID {
		t.Errorf("token subject = %q, want %q", idt.Subject, reg.ID)
	}
	var claims struct {
		Email string   `json:"email"`
		Name  string   `json:"name"`
		Roles []string `json:"roles"`
	}
	if err := idt.Claims(&claims); err != nil {
		t.Fatalf("claims: %v", err)
	}
	if claims.Email != "ada@example.com" {
		t.Errorf("token email = %q, want ada@example.com", claims.Email)
	}
	if !hasRole(claims.Roles, "admin") {
		t.Errorf("token roles = %v, want to include admin", claims.Roles)
	}

	// The user is stored with a password hash, not the plaintext.
	stored, err := users.GetByEmail(ctx, "ada@example.com")
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if stored.PasswordHash == "" {
		t.Error("stored user has no password hash")
	}
	if stored.PasswordHash == "s3cret" {
		t.Error("stored user password is the plaintext, not a hash")
	}
}

// TestUserLoginWrongPassword checks that a login with a bad password is
// rejected (401) and that an unknown user is rejected the same way.
func TestUserLoginWrongPassword(t *testing.T) {
	ts, _, _ := newTestUserServer(t)

	postJSON(t, ts.URL+"/register", map[string]any{
		"first_name": "Bob", "last_name": "Jones",
		"email": "bob@example.com", "password": "right",
	})

	// Wrong password.
	code, _ := postJSON(t, ts.URL+"/login", map[string]any{
		"email": "bob@example.com", "password": "wrong",
	})
	if code != http.StatusUnauthorized {
		t.Errorf("wrong password status = %d, want 401", code)
	}

	// Unknown user.
	code, _ = postJSON(t, ts.URL+"/login", map[string]any{
		"email": "nobody@example.com", "password": "x",
	})
	if code != http.StatusUnauthorized {
		t.Errorf("unknown user status = %d, want 401", code)
	}
}

// TestUserRegisterDuplicate checks that registering a user with an existing
// email is rejected (409).
func TestUserRegisterDuplicate(t *testing.T) {
	ts, _, _ := newTestUserServer(t)

	body := map[string]any{
		"first_name": "Ada", "last_name": "L",
		"email": "ada@example.com", "password": "s3cret",
	}
	if code, _ := postJSON(t, ts.URL+"/register", body); code != http.StatusCreated {
		t.Fatalf("first register status = %d, want 201", code)
	}
	if code, _ := postJSON(t, ts.URL+"/register", body); code != http.StatusConflict {
		t.Errorf("duplicate register status = %d, want 409", code)
	}
}

// TestUserRolesManagement checks the user/role management endpoints: list,
// update roles, and delete.
func TestUserRolesManagement(t *testing.T) {
	ts, _, _ := newTestUserServer(t)

	// Create two users (first is admin, second is user).
	postJSON(t, ts.URL+"/register", map[string]any{
		"first_name": "Ada", "last_name": "L",
		"email": "ada@example.com", "password": "s3cret",
	})
	code, body := postJSON(t, ts.URL+"/register", map[string]any{
		"first_name": "Bob", "last_name": "J",
		"email": "bob@example.com", "password": "pw",
	})
	if code != http.StatusCreated {
		t.Fatalf("register bob status = %d, want 201 (body %s)", code, body)
	}
	var bob struct {
		ID    string   `json:"id"`
		Roles []string `json:"roles"`
	}
	if err := json.Unmarshal(body, &bob); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !hasRole(bob.Roles, "user") {
		t.Errorf("second user roles = %v, want to include user", bob.Roles)
	}

	// List users.
	resp, err := http.Get(ts.URL + "/users")
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list users status = %d, want 200", resp.StatusCode)
	}
	var listed []struct {
		Email string `json:"email"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed) != 2 {
		t.Errorf("list users returned %d users, want 2", len(listed))
	}

	// Update bob's roles to admin.
	req, _ := http.NewRequest("PUT", ts.URL+"/users/"+bob.ID,
		strings.NewReader(`{"roles":["admin"]}`))
	req.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("update user: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("update user status = %d, want 200", resp2.StatusCode)
	}
	var updated struct {
		Roles []string `json:"roles"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&updated); err != nil {
		t.Fatalf("decode update: %v", err)
	}
	if !hasRole(updated.Roles, "admin") {
		t.Errorf("updated roles = %v, want to include admin", updated.Roles)
	}

	// Delete bob.
	delReq, _ := http.NewRequest("DELETE", ts.URL+"/users/"+bob.ID, nil)
	resp3, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("delete user: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("delete user status = %d, want 200", resp3.StatusCode)
	}

	// A second delete is a 404.
	delReq2, _ := http.NewRequest("DELETE", ts.URL+"/users/"+bob.ID, nil)
	resp4, err := http.DefaultClient.Do(delReq2)
	if err != nil {
		t.Fatalf("delete user (again): %v", err)
	}
	defer resp4.Body.Close()
	if resp4.StatusCode != http.StatusNotFound {
		t.Errorf("second delete status = %d, want 404", resp4.StatusCode)
	}
}

// TestUserLoginAudienceOverride checks that a login with an explicit audience
// stamps the token's aud with that audience — this is how the API requests a
// token whose aud matches the audience its OIDC verifier checks (its
// client_id or token_audience), so the API accepts the login token.
func TestUserLoginAudienceOverride(t *testing.T) {
	ts, _, _ := newTestUserServer(t)
	ctx := context.Background()

	provider, err := oidc.NewProvider(ctx, ts.URL)
	if err != nil {
		t.Fatalf("oidc discovery: %v", err)
	}
	// The API's verifier checks aud against its client_id ("cdrom-ui").
	verifier := provider.Verifier(&oidc.Config{ClientID: "cdrom-ui"})

	postJSON(t, ts.URL+"/register", map[string]any{
		"first_name": "Ada", "last_name": "L",
		"email": "ada@example.com", "password": "s3cret",
	})

	// Login requesting the audience the API's verifier checks.
	code, body := postJSON(t, ts.URL+"/login", map[string]any{
		"email": "ada@example.com", "password": "s3cret", "audience": "cdrom-ui",
	})
	if code != http.StatusOK {
		t.Fatalf("login status = %d, want 200 (body %s)", code, body)
	}
	var login struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &login); err != nil {
		t.Fatalf("decode login: %v", err)
	}

	// The token verifies against the API's verifier (aud == client_id).
	idt, err := verifier.Verify(ctx, login.AccessToken)
	if err != nil {
		t.Fatalf("verify token: %v", err)
	}
	if len(idt.Audience) != 1 || idt.Audience[0] != "cdrom-ui" {
		t.Errorf("token aud = %v, want [cdrom-ui]", idt.Audience)
	}
}

// TestUserEndpointsDisabledWithoutStore checks that the user endpoints respond
// 501 when the IdP has no user store configured.
func TestUserEndpointsDisabledWithoutStore(t *testing.T) {
	cfg := testConfig(time.Hour, 30*time.Minute)
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger()) // no user store
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	if code, _ := postJSON(t, ts.URL+"/login", map[string]any{"email": "a", "password": "b"}); code != http.StatusNotImplemented {
		t.Errorf("login status = %d, want 501", code)
	}
	if code, _ := postJSON(t, ts.URL+"/register", map[string]any{"email": "a", "password": "b"}); code != http.StatusNotImplemented {
		t.Errorf("register status = %d, want 501", code)
	}
}

// hasRole reports whether roles contains role.
func hasRole(roles []string, role string) bool {
	for _, r := range roles {
		if r == role {
			return true
		}
	}
	return false
}
