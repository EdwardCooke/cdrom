package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cdrom/internal/auth"
	"cdrom/internal/authz"
)

// fakeAPIKey is a stub APIKeyClient that returns canned responses and records
// the requests the API forwarded to the IdP.
type fakeAPIKey struct {
	createResult    *APIKeyResult
	createPlaintext string
	createErr       error
	getResult       *APIKeyResult
	getErr          error
	keys            []APIKeyResult
	listErr         error
	updateResult    *APIKeyResult
	updateErr       error
	rotateResult    *APIKeyResult
	rotatePlaintext string
	rotateErr       error
	deleteErr       error
	verifyResult    auth.User
	verifyErr       error
	resetErr        error

	createdOwner, createdDesc, createdExpires string
	createdScope                              []uint
	createdID                                 string
	listOwner                                 string
	updatedID                                 string
	updated                                   UpdateAPIKeyRequest
	rotatedID                                 string
	deletedID                                 string
	resetUserID                               string
	verifiedUser, verifiedKey                 string
}

func (f *fakeAPIKey) CreateAPIKey(_ context.Context, ownerEmail, description, expiresIn string, pipelineScope []uint) (*APIKeyResult, string, error) {
	f.createdOwner, f.createdDesc, f.createdExpires, f.createdScope = ownerEmail, description, expiresIn, pipelineScope
	if f.createErr != nil {
		return nil, "", f.createErr
	}
	if f.createResult == nil {
		f.createResult = &APIKeyResult{ID: "k1", OwnerID: "u1", KeyPrefix: "cdrom-ab"}
	}
	if f.createPlaintext == "" {
		f.createPlaintext = "cdrom-" + "a1b2c3d4"
	}
	return f.createResult, f.createPlaintext, nil
}

func (f *fakeAPIKey) GetAPIKey(_ context.Context, id string) (*APIKeyResult, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.getResult != nil {
		return f.getResult, nil
	}
	// Resolve the key's owner from the directory so ownership checks are
	// accurate (k1 belongs to u1, k2 to u2).
	for _, k := range f.keys {
		if k.ID == id {
			out := k
			return &out, nil
		}
	}
	return &APIKeyResult{ID: id, OwnerID: "u1", KeyPrefix: "cdrom-ab"}, nil
}

func (f *fakeAPIKey) ListAPIKeys(_ context.Context, ownerID string) ([]APIKeyResult, error) {
	f.listOwner = ownerID
	if f.listErr != nil {
		return nil, f.listErr
	}
	if ownerID != "" {
		out := make([]APIKeyResult, 0, len(f.keys))
		for _, k := range f.keys {
			if k.OwnerID == ownerID {
				out = append(out, k)
			}
		}
		return out, nil
	}
	return f.keys, nil
}

func (f *fakeAPIKey) UpdateAPIKey(_ context.Context, id string, req UpdateAPIKeyRequest) (*APIKeyResult, error) {
	f.updatedID, f.updated = id, req
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	if f.updateResult != nil {
		return f.updateResult, nil
	}
	return &APIKeyResult{ID: id, OwnerID: "u1", Description: req.Description, KeyPrefix: "cdrom-ab"}, nil
}

func (f *fakeAPIKey) RotateAPIKey(_ context.Context, id string) (*APIKeyResult, string, error) {
	f.rotatedID = id
	if f.rotateErr != nil {
		return nil, "", f.rotateErr
	}
	if f.rotateResult == nil {
		f.rotateResult = &APIKeyResult{ID: id, OwnerID: "u1", KeyPrefix: "cdrom-xy"}
	}
	if f.rotatePlaintext == "" {
		f.rotatePlaintext = "cdrom-" + "newkey"
	}
	return f.rotateResult, f.rotatePlaintext, nil
}

func (f *fakeAPIKey) DeleteAPIKey(_ context.Context, id string) error {
	f.deletedID = id
	return f.deleteErr
}

func (f *fakeAPIKey) VerifyAPIKey(_ context.Context, username, apiKey string) (auth.User, error) {
	f.verifiedUser, f.verifiedKey = username, apiKey
	if f.verifyErr != nil {
		return auth.User{}, f.verifyErr
	}
	if f.verifyResult.Email == "" {
		f.verifyResult = auth.User{Subject: "u1", Email: username, Roles: []string{"user"}}
	}
	return f.verifyResult, nil
}

func (f *fakeAPIKey) ResetAPIKeyLockout(_ context.Context, userID string) error {
	f.resetUserID = userID
	return f.resetErr
}

// apiKeyServer returns an API test server with the given API-key client
// attached. When user is non-nil, every request is served with that user in
// the context (simulating an authenticated caller).
func apiKeyServer(t *testing.T, ak APIKeyClient, user auth.User) *httptest.Server {
	t.Helper()
	srv := New(Clients{}, nil)
	if ak != nil {
		srv.SetAPIKeyClient(ak)
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

// TestAPIKeyDisabled checks that the /api/api-keys endpoints respond 501 when
// API-key authentication is not enabled (no API-key client attached).
func TestAPIKeyDisabled(t *testing.T) {
	ts := apiKeyServer(t, nil, auth.User{})
	if code, _ := postJSONBody(t, ts.URL+"/api/api-keys", `{"description":"x"}`); code != http.StatusNotImplemented {
		t.Errorf("create status = %d, want 501", code)
	}
	if code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/api-keys", ""); code != http.StatusNotImplemented {
		t.Errorf("list status = %d, want 501", code)
	}
	if code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/api-keys/k1", ""); code != http.StatusNotImplemented {
		t.Errorf("get status = %d, want 501", code)
	}
	if code, _ := doRequest(t, http.MethodPut, ts.URL+"/api/api-keys/k1", `{"description":"x"}`); code != http.StatusNotImplemented {
		t.Errorf("update status = %d, want 501", code)
	}
	if code, _ := doRequest(t, http.MethodPost, ts.URL+"/api/api-keys/k1/rotate", ""); code != http.StatusNotImplemented {
		t.Errorf("rotate status = %d, want 501", code)
	}
	if code, _ := doRequest(t, http.MethodDelete, ts.URL+"/api/api-keys/k1", ""); code != http.StatusNotImplemented {
		t.Errorf("delete status = %d, want 501", code)
	}
	if code, _ := postJSONBody(t, ts.URL+"/api/api-keys/lockout/reset", `{"user_id":"u1"}`); code != http.StatusNotImplemented {
		t.Errorf("reset status = %d, want 501", code)
	}
}

// TestAPIKeyCRUD checks the API-key management endpoints: create returns the
// plaintext exactly once, list/get/update/rotate/delete proxy to the IdP, and
// the owner's email is filled in from the user directory.
func TestAPIKeyCRUD(t *testing.T) {
	fake := &fakeAPIKey{
		keys: []APIKeyResult{
			{ID: "k1", OwnerID: "u1", Description: "ci", KeyPrefix: "cdrom-ab"},
			{ID: "k2", OwnerID: "u2", Description: "other", KeyPrefix: "cdrom-xy"},
		},
	}
	// Attach a userpass client so the owner's email can be filled in.
	srv := New(Clients{}, nil)
	srv.SetAPIKeyClient(fake)
	srv.SetUserPassClient(&fakeUserPass{users: []UserResult{
		{ID: "u1", Email: "ada@example.com"},
		{ID: "u2", Email: "bob@example.com"},
	}})
	handler := srv.Handler()
	// Serve every request as the user u1 (ada@example.com) so the own-keys
	// list is filtered to the caller's own keys.
	testUser := auth.User{Subject: "u1", Email: "ada@example.com"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(auth.WithUser(r.Context(), testUser))
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)

	// Create a key: the plaintext is returned exactly once. No user is in the
	// context (RBAC off), so the owner is taken from the body.
	code, body := postJSONBody(t, ts.URL+"/api/api-keys", `{"owner_email":"ada@example.com","description":"ci key","expires_in":"24h","pipeline_scope":[1,2]}`)
	if code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201 (body %s)", code, body)
	}
	var created struct {
		Key       APIKeyResult `json:"key"`
		Plaintext string       `json:"plaintext"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	if created.Plaintext == "" {
		t.Errorf("create plaintext = empty, want a cdrom- key")
	}

	// List all keys (no owner filter, since RBAC is off the caller is a
	// synthetic admin).
	code, body = doRequest(t, http.MethodGet, ts.URL+"/api/api-keys", "")
	if code != http.StatusOK {
		t.Fatalf("list status = %d, want 200 (body %s)", code, body)
	}
	var listed []APIKeyResult
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	// The own-keys list returns only the caller's (u1's) keys.
	if len(listed) != 1 || listed[0].ID != "k1" {
		t.Errorf("list = %+v, want only k1 (the caller's own key)", listed)
	}
	// The owner's email is filled in from the user directory.
	if len(listed) > 0 && listed[0].OwnerEmail != "ada@example.com" {
		t.Errorf("list[0].owner_email = %q, want ada@example.com", listed[0].OwnerEmail)
	}

	// Get a single key.
	code, body = doRequest(t, http.MethodGet, ts.URL+"/api/api-keys/k1", "")
	if code != http.StatusOK {
		t.Fatalf("get status = %d, want 200 (body %s)", code, body)
	}
	var got APIKeyResult
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if got.ID != "k1" {
		t.Errorf("get id = %q, want k1", got.ID)
	}

	// Update a key (renew): only the fields present are applied.
	code, body = doRequest(t, http.MethodPut, ts.URL+"/api/api-keys/k1", `{"description":"new desc"}`)
	if code != http.StatusOK {
		t.Fatalf("update status = %d, want 200 (body %s)", code, body)
	}
	if fake.updatedID != "k1" {
		t.Errorf("updated id = %q, want k1", fake.updatedID)
	}
	if !fake.updated.HasDescription {
		t.Errorf("update HasDescription = false, want true")
	}
	if fake.updated.HasExpiresAt || fake.updated.HasScope {
		t.Errorf("update HasExpiresAt/HasScope = %v/%v, want false/false", fake.updated.HasExpiresAt, fake.updated.HasScope)
	}

	// Rotate a key: the new plaintext is returned exactly once.
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/api-keys/k1/rotate", "")
	if code != http.StatusOK {
		t.Fatalf("rotate status = %d, want 200 (body %s)", code, body)
	}
	var rotated struct {
		Key       APIKeyResult `json:"key"`
		Plaintext string       `json:"plaintext"`
	}
	if err := json.Unmarshal(body, &rotated); err != nil {
		t.Fatalf("decode rotate: %v", err)
	}
	if rotated.Plaintext == "" {
		t.Errorf("rotate plaintext = empty, want a cdrom- key")
	}
	if fake.rotatedID != "k1" {
		t.Errorf("rotated id = %q, want k1", fake.rotatedID)
	}

	// Delete a key.
	code, _ = doRequest(t, http.MethodDelete, ts.URL+"/api/api-keys/k1", "")
	if code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", code)
	}
	if fake.deletedID != "k1" {
		t.Errorf("deleted id = %q, want k1", fake.deletedID)
	}

	// Reset a user's API-key lockout.
	code, _ = postJSONBody(t, ts.URL+"/api/api-keys/lockout/reset", `{"user_id":"u1"}`)
	if code != http.StatusOK {
		t.Fatalf("reset status = %d, want 200", code)
	}
	if fake.resetUserID != "u1" {
		t.Errorf("reset user = %q, want u1", fake.resetUserID)
	}
}

// apiKeyRBACServer builds an API server with RBAC enabled over the built-in
// roles and the given API-key client, wrapped by a handler that injects the
// given user into the request context (simulating the auth middleware).
func apiKeyRBACServer(t *testing.T, user auth.User, ak APIKeyClient) http.Handler {
	t.Helper()
	srv := New(Clients{Database: &fakeRBACDatabase{}, Scheduler: &fakeScheduler{}}, nil)
	srv.SetAuthz(authz.New(&testRoleSource{roles: builtinTestRoles()}), true)
	if ak != nil {
		srv.SetAPIKeyClient(ak)
	}
	handler := srv.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(auth.WithUser(r.Context(), user))
		handler.ServeHTTP(w, r)
	})
}

// TestAPIKeyRBAC checks the API-key endpoints under RBAC: a viewer cannot
// manage keys at all, a user (who holds api-keys.can-manage for self-service)
// can manage only their own keys and sees only their own on list, and an
// admin (who holds api-keys.can-manage-all and api-keys.can-list-all) can
// manage and list every key in the system.
func TestAPIKeyRBAC(t *testing.T) {
	fake := &fakeAPIKey{
		keys: []APIKeyResult{
			{ID: "k1", OwnerID: "u1", Description: "mine", KeyPrefix: "cdrom-ab"},
			{ID: "k2", OwnerID: "u2", Description: "theirs", KeyPrefix: "cdrom-xy"},
		},
	}

	// A viewer cannot list keys (403).
	viewerHandler := apiKeyRBACServer(t, auth.User{Subject: "viewer-1", Roles: []string{authz.RoleViewer}}, fake)
	if code := rbacDoRequest(t, viewerHandler, http.MethodGet, "/api/api-keys", ""); code != http.StatusForbidden {
		t.Errorf("viewer list status = %d, want 403", code)
	}

	// A user (self-service) can list only their own keys.
	user := auth.User{Subject: "u1", Email: "u1@example.com", Roles: []string{authz.RoleUser}}
	userHandler := apiKeyRBACServer(t, user, fake)
	code, body := rbacDoRequestWithBody(t, userHandler, http.MethodGet, "/api/api-keys", "")
	if code != http.StatusOK {
		t.Fatalf("user list status = %d, want 200 (body %s)", code, body)
	}
	var listed []APIKeyResult
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != "k1" {
		t.Errorf("user list = %+v, want only k1 (own key)", listed)
	}

	// A user can manage their own key (k1, which they own).
	if code := rbacDoRequest(t, userHandler, http.MethodPut, "/api/api-keys/k1", `{"description":"mine"}`); code != http.StatusOK {
		t.Errorf("user update own key status = %d, want 200", code)
	}

	// A user cannot get another user's key (403): acting on a non-own key
	// requires the api-keys.can-manage-all permission, which the user role
	// does not grant.
	if code := rbacDoRequest(t, userHandler, http.MethodGet, "/api/api-keys/k2", ""); code != http.StatusForbidden {
		t.Errorf("user get other key status = %d, want 403", code)
	}
	// A user cannot create a key for another user (403).
	if code := rbacDoRequest(t, userHandler, http.MethodPost, "/api/api-keys", `{"owner_email":"u2@example.com","description":"x"}`); code != http.StatusForbidden {
		t.Errorf("user create-for-other status = %d, want 403", code)
	}
	// A user cannot update/rotate/delete another user's key (403).
	if code := rbacDoRequest(t, userHandler, http.MethodPut, "/api/api-keys/k2", `{"description":"x"}`); code != http.StatusForbidden {
		t.Errorf("user update other key status = %d, want 403", code)
	}
	if code := rbacDoRequest(t, userHandler, http.MethodPost, "/api/api-keys/k2/rotate", ""); code != http.StatusForbidden {
		t.Errorf("user rotate other key status = %d, want 403", code)
	}
	if code := rbacDoRequest(t, userHandler, http.MethodDelete, "/api/api-keys/k2", ""); code != http.StatusForbidden {
		t.Errorf("user delete other key status = %d, want 403", code)
	}

	// A user cannot list every key (403): the list-all permission is not in
	// the user role.
	if code := rbacDoRequest(t, userHandler, http.MethodGet, "/api/api-keys/all", ""); code != http.StatusForbidden {
		t.Errorf("user list-all status = %d, want 403", code)
	}

	// An admin's own-keys list shows only their own keys (admin-1 owns none).
	adminHandler := apiKeyRBACServer(t, auth.User{Subject: "admin-1", Roles: []string{authz.RoleAdmin}}, fake)
	code, body = rbacDoRequestWithBody(t, adminHandler, http.MethodGet, "/api/api-keys", "")
	if code != http.StatusOK {
		t.Fatalf("admin own-list status = %d, want 200 (body %s)", code, body)
	}
	listed = nil
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatalf("decode admin own-list: %v", err)
	}
	if len(listed) != 0 {
		t.Errorf("admin own-list = %d keys, want 0 (admin owns none)", len(listed))
	}

	// An admin can list every key in the system via the privileged endpoint.
	code, body = rbacDoRequestWithBody(t, adminHandler, http.MethodGet, "/api/api-keys/all", "")
	if code != http.StatusOK {
		t.Fatalf("admin list-all status = %d, want 200 (body %s)", code, body)
	}
	listed = nil
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatalf("decode admin list-all: %v", err)
	}
	if len(listed) != 2 {
		t.Errorf("admin list-all = %d keys, want 2 (all keys)", len(listed))
	}

	// An admin can get any key.
	if code := rbacDoRequest(t, adminHandler, http.MethodGet, "/api/api-keys/k2", ""); code != http.StatusOK {
		t.Errorf("admin get other key status = %d, want 200", code)
	}
	// An admin can create a key for another user (api-keys.can-manage-all).
	if code := rbacDoRequest(t, adminHandler, http.MethodPost, "/api/api-keys", `{"owner_email":"u2@example.com","description":"x"}`); code != http.StatusCreated {
		t.Errorf("admin create-for-other status = %d, want 201", code)
	}
	// An admin can update/rotate/delete another user's key.
	if code := rbacDoRequest(t, adminHandler, http.MethodPut, "/api/api-keys/k2", `{"description":"x"}`); code != http.StatusOK {
		t.Errorf("admin update other key status = %d, want 200", code)
	}
	if code := rbacDoRequest(t, adminHandler, http.MethodPost, "/api/api-keys/k2/rotate", ""); code != http.StatusOK {
		t.Errorf("admin rotate other key status = %d, want 200", code)
	}
	if code := rbacDoRequest(t, adminHandler, http.MethodDelete, "/api/api-keys/k2", ""); code != http.StatusOK {
		t.Errorf("admin delete other key status = %d, want 200", code)
	}
}

// rbacDoRequestWithBody performs a request against a handler and returns the
// status code and body.
func rbacDoRequestWithBody(t *testing.T, handler http.Handler, method, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}
