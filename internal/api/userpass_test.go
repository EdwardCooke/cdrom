package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cdrom/internal/auth"
)

// fakeUserPass is a stub UserPassClient that returns canned responses and
// records the requests the API forwarded to the IdP.
type fakeUserPass struct {
	loginResult    *LoginResult
	loginErr       error
	registerResult *UserResult
	registerErr    error
	users          []UserResult
	listErr        error
	createResult   *UserResult
	createErr      error
	updateResult   *UserResult
	updateErr      error
	deleteErr      error

	loginEmail, loginPassword string
	registered                RegisterRequest
	created                   RegisterRequest
	updatedID                 string
	updated                   UpdateUserRequest
	deletedID                 string
}

func (f *fakeUserPass) Login(_ context.Context, email, password string) (*LoginResult, error) {
	f.loginEmail, f.loginPassword = email, password
	if f.loginErr != nil {
		return nil, f.loginErr
	}
	return f.loginResult, nil
}

func (f *fakeUserPass) Register(_ context.Context, req RegisterRequest) (*UserResult, error) {
	f.registered = req
	if f.registerErr != nil {
		return nil, f.registerErr
	}
	return f.registerResult, nil
}

func (f *fakeUserPass) ListUsers(_ context.Context) ([]UserResult, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.users, nil
}

func (f *fakeUserPass) CreateUser(_ context.Context, req RegisterRequest) (*UserResult, error) {
	f.created = req
	if f.createErr != nil {
		return nil, f.createErr
	}
	return f.createResult, nil
}

func (f *fakeUserPass) UpdateUser(_ context.Context, id string, req UpdateUserRequest) (*UserResult, error) {
	f.updatedID, f.updated = id, req
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return f.updateResult, nil
}

func (f *fakeUserPass) DeleteUser(_ context.Context, id string) error {
	f.deletedID = id
	return f.deleteErr
}

// userPassServer returns an API test server with the given userpass client
// attached. When user is non-nil, every request is served with that user in
// the context (simulating an authenticated caller).
func userPassServer(t *testing.T, up UserPassClient, user auth.User) *httptest.Server {
	t.Helper()
	srv := New(Clients{}, nil)
	if up != nil {
		srv.SetUserPassClient(up)
	}
	handler := srv.Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user.Subject != "" {
			r = r.WithContext(auth.WithUser(r.Context(), user))
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func postJSONBody(t *testing.T, url, body string) (int, []byte) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// TestAPILogin checks that /api/login proxies to the IdP and returns the
// token, and that a failed login (401 from the IdP) is surfaced as a 401.
func TestAPILogin(t *testing.T) {
	fake := &fakeUserPass{loginResult: &LoginResult{
		AccessToken: "tok", TokenType: "Bearer", ExpiresIn: 3600,
		Email: "ada@example.com", Roles: []string{"admin"},
	}}
	ts := userPassServer(t, fake, auth.User{})

	code, body := postJSONBody(t, ts.URL+"/api/login", `{"email":"ada@example.com","password":"s3cret"}`)
	if code != http.StatusOK {
		t.Fatalf("login status = %d, want 200 (body %s)", code, body)
	}
	if fake.loginEmail != "ada@example.com" || fake.loginPassword != "s3cret" {
		t.Errorf("forwarded creds = %q/%q, want ada@example.com/s3cret", fake.loginEmail, fake.loginPassword)
	}
	var out LoginResult
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.AccessToken != "tok" {
		t.Errorf("access_token = %q, want tok", out.AccessToken)
	}

	// A failed login (the IdP returns 401) is surfaced as a 401.
	fake2 := &fakeUserPass{loginErr: &userPassError{status: http.StatusUnauthorized, body: "invalid credentials"}}
	ts2 := userPassServer(t, fake2, auth.User{})
	if code, _ := postJSONBody(t, ts2.URL+"/api/login", `{"email":"a","password":"b"}`); code != http.StatusUnauthorized {
		t.Errorf("failed login status = %d, want 401", code)
	}
}

// TestAPIRegister checks that /api/register proxies to the IdP and returns the
// created user, and that a duplicate (409 from the IdP) is surfaced as a 409.
func TestAPIRegister(t *testing.T) {
	fake := &fakeUserPass{registerResult: &UserResult{ID: "u1", Email: "ada@example.com", Roles: []string{"admin"}}}
	ts := userPassServer(t, fake, auth.User{})

	code, body := postJSONBody(t, ts.URL+"/api/register",
		`{"first_name":"Ada","last_name":"L","email":"ada@example.com","password":"s3cret"}`)
	if code != http.StatusCreated {
		t.Fatalf("register status = %d, want 201 (body %s)", code, body)
	}
	if fake.registered.Email != "ada@example.com" || fake.registered.Password != "s3cret" {
		t.Errorf("forwarded register = %+v, want ada@example.com/s3cret", fake.registered)
	}
	var out UserResult
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.ID != "u1" {
		t.Errorf("id = %q, want u1", out.ID)
	}

	// A duplicate (the IdP returns 409) is surfaced as a 409.
	fake2 := &fakeUserPass{registerErr: &userPassError{status: http.StatusConflict, body: "exists"}}
	ts2 := userPassServer(t, fake2, auth.User{})
	if code, _ := postJSONBody(t, ts2.URL+"/api/register", `{"email":"a","password":"b"}`); code != http.StatusConflict {
		t.Errorf("duplicate register status = %d, want 409", code)
	}
}

// TestAPIUserManagement checks the user/role management endpoints: they require
// an authenticated admin caller, and an admin's requests are proxied to the
// IdP.
func TestAPIUserManagement(t *testing.T) {
	admin := auth.User{Subject: "admin-1", Roles: []string{"admin"}}
	nonAdmin := auth.User{Subject: "user-1", Roles: []string{"user"}}

	// A non-admin cannot list users (403).
	fake := &fakeUserPass{users: []UserResult{{ID: "u1"}}}
	ts := userPassServer(t, fake, nonAdmin)
	if code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/users", ""); code != http.StatusForbidden {
		t.Errorf("non-admin list status = %d, want 403", code)
	}

	// An unauthenticated caller cannot list users (401).
	tsNoAuth := userPassServer(t, fake, auth.User{})
	if code, _ := doRequest(t, http.MethodGet, tsNoAuth.URL+"/api/users", ""); code != http.StatusUnauthorized {
		t.Errorf("unauthenticated list status = %d, want 401", code)
	}

	// An admin can list users.
	tsAdmin := userPassServer(t, fake, admin)
	code, body := doRequest(t, http.MethodGet, tsAdmin.URL+"/api/users", "")
	if code != http.StatusOK {
		t.Fatalf("admin list status = %d, want 200 (body %s)", code, body)
	}
	var users []UserResult
	if err := json.Unmarshal(body, &users); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(users) != 1 || users[0].ID != "u1" {
		t.Errorf("list = %+v, want one user u1", users)
	}

	// An admin can update a user's roles.
	fake2 := &fakeUserPass{updateResult: &UserResult{ID: "u2", Roles: []string{"admin"}}}
	tsUpdate := userPassServer(t, fake2, admin)
	code, _ = doRequest(t, http.MethodPut, tsUpdate.URL+"/api/users/u2", `{"roles":["admin"]}`)
	if code != http.StatusOK {
		t.Fatalf("admin update status = %d, want 200", code)
	}
	if fake2.updatedID != "u2" || !hasRoleStr(fake2.updated.Roles, "admin") {
		t.Errorf("forwarded update = %q %+v, want u2 [admin]", fake2.updatedID, fake2.updated)
	}

	// An admin can delete a user.
	fake3 := &fakeUserPass{}
	tsDelete := userPassServer(t, fake3, admin)
	code, _ = doRequest(t, http.MethodDelete, tsDelete.URL+"/api/users/u2", "")
	if code != http.StatusOK {
		t.Fatalf("admin delete status = %d, want 200", code)
	}
	if fake3.deletedID != "u2" {
		t.Errorf("forwarded delete = %q, want u2", fake3.deletedID)
	}
}

// TestAPIUserPassDisabled checks that the userpass endpoints respond 501 when
// username/password auth is not enabled (no userpass client attached).
func TestAPIUserPassDisabled(t *testing.T) {
	ts := userPassServer(t, nil, auth.User{})
	if code, _ := postJSONBody(t, ts.URL+"/api/login", `{"email":"a","password":"b"}`); code != http.StatusNotImplemented {
		t.Errorf("login status = %d, want 501", code)
	}
	if code, _ := postJSONBody(t, ts.URL+"/api/register", `{"email":"a","password":"b"}`); code != http.StatusNotImplemented {
		t.Errorf("register status = %d, want 501", code)
	}
	if code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/users", ""); code != http.StatusNotImplemented {
		t.Errorf("list users status = %d, want 501", code)
	}
}

// doRequest performs a request with an optional JSON body and returns the
// status code and body.
func doRequest(t *testing.T, method, url, body string) (int, []byte) {
	t.Helper()
	var reader *bytes.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request %s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// hasRoleStr reports whether roles contains role.
func hasRoleStr(roles []string, role string) bool {
	for _, r := range roles {
		if r == role {
			return true
		}
	}
	return false
}
