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
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fakeAgentCred is a stub AgentCredentialClient that returns canned responses
// and records the requests the API forwarded to the IdP.
type fakeAgentCred struct {
	issueCred  *dbpb.AgentCredential
	issueJWT   string
	issueErr   error
	listCreds  []*dbpb.AgentCredential
	listErr    error
	renewCred  *dbpb.AgentCredential
	renewErr   error
	rotateCred *dbpb.AgentCredential
	rotateJWT  string
	rotateErr  error
	revokeCred *dbpb.AgentCredential
	revokeErr  error

	issuedName, issuedBy, issuedDesc string
	listedName                       string
	renewedID, rotatedID, revokedID  string
	rotatedBy, rotatedDesc           string
	revokedReason                    string
}

func (f *fakeAgentCred) IssueAgentCredential(_ context.Context, name, issuedBy, desc string) (*dbpb.AgentCredential, string, error) {
	f.issuedName, f.issuedBy, f.issuedDesc = name, issuedBy, desc
	if f.issueErr != nil {
		return nil, "", f.issueErr
	}
	if f.issueCred == nil {
		f.issueCred = &dbpb.AgentCredential{Id: "cred-1", AgentName: name}
	}
	if f.issueJWT == "" {
		f.issueJWT = "eyJhbGciOiJSUzI1NiJ9.fake.jwt"
	}
	return f.issueCred, f.issueJWT, nil
}

func (f *fakeAgentCred) GetAgentCredential(_ context.Context, id string) (*dbpb.AgentCredential, error) {
	return &dbpb.AgentCredential{Id: id, AgentName: "edge-box-1"}, nil
}

func (f *fakeAgentCred) ListAgentCredentials(_ context.Context, agentName string) ([]*dbpb.AgentCredential, error) {
	f.listedName = agentName
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.listCreds, nil
}

func (f *fakeAgentCred) RenewAgentCredential(_ context.Context, id string) (*dbpb.AgentCredential, error) {
	f.renewedID = id
	if f.renewErr != nil {
		return nil, f.renewErr
	}
	if f.renewCred == nil {
		f.renewCred = &dbpb.AgentCredential{Id: id, AgentName: "edge-box-1"}
	}
	return f.renewCred, nil
}

func (f *fakeAgentCred) RotateAgentCredential(_ context.Context, id, issuedBy, desc string) (*dbpb.AgentCredential, string, error) {
	f.rotatedID, f.rotatedBy, f.rotatedDesc = id, issuedBy, desc
	if f.rotateErr != nil {
		return nil, "", f.rotateErr
	}
	if f.rotateCred == nil {
		f.rotateCred = &dbpb.AgentCredential{Id: "cred-2", AgentName: "edge-box-1", ReplacedById: id}
	}
	if f.rotateJWT == "" {
		f.rotateJWT = "eyJhbGciOiJSUzI1NiJ9.successor.jwt"
	}
	return f.rotateCred, f.rotateJWT, nil
}

func (f *fakeAgentCred) RevokeAgentCredential(_ context.Context, id, reason string) (*dbpb.AgentCredential, error) {
	f.revokedID, f.revokedReason = id, reason
	if f.revokeErr != nil {
		return nil, f.revokeErr
	}
	if f.revokeCred == nil {
		f.revokeCred = &dbpb.AgentCredential{Id: id, AgentName: "edge-box-1"}
	}
	return f.revokeCred, nil
}

// credServer builds an API test server with the agent-credential client
// attached (nil = disabled) and the given user injected into the request
// context (the F-25 apiKeyServer pattern).
func credServer(t *testing.T, fc AgentCredentialClient, user auth.User) *httptest.Server {
	t.Helper()
	srv := New(Clients{}, nil)
	if fc != nil {
		srv.SetAgentCredentialClient(fc)
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

// doGetJSON performs a GET and returns the status code + decoded body.
func doGetJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	code, body := doRequest(t, http.MethodGet, url, "")
	var m map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &m); err != nil {
			t.Fatalf("unmarshal %q: %v", body, err)
		}
	}
	return code, m
}

// TestAgentCredentialDisabled checks the 501 degrade when the IdP is not
// wired (no client attached) — the F-28.4 S4 convention.
func TestAgentCredentialDisabled(t *testing.T) {
	ts := credServer(t, nil, auth.User{Subject: "admin", Roles: []string{authz.RoleAdmin}})
	urls := []struct {
		method, path, body string
	}{
		{http.MethodPost, "/api/agents/edge-box-1/credentials", "{}"},
		{http.MethodGet, "/api/agents/edge-box-1/credentials", ""},
		{http.MethodPost, "/api/agents/edge-box-1/credentials/cred-1/renew", ""},
		{http.MethodPost, "/api/agents/edge-box-1/credentials/cred-1/rotate", "{}"},
		{http.MethodDelete, "/api/agents/edge-box-1/credentials/cred-1", "{}"},
	}
	for _, u := range urls {
		// doRequest honors the METHOD (postJSONBody would force POST — the
		// DELETE case would 405 against the POST-only siblings).
		code, _ := doRequest(t, u.method, ts.URL+u.path, u.body)
		if code != http.StatusNotImplemented {
			t.Errorf("%s %s status = %d, want 501", u.method, u.path, code)
		}
	}
}

// TestAgentCredentialWalkthrough is the Slice 2 "curl walkthrough": issue →
// list → renew → rotate → revoke, asserting the plaintext JWT appears EXACTLY
// ONCE per issue/rotate (in the response body) and never in list.
func TestAgentCredentialWalkthrough(t *testing.T) {
	fake := &fakeAgentCred{
		listCreds: []*dbpb.AgentCredential{{Id: "cred-1", AgentName: "edge-box-1"}},
	}
	admin := auth.User{Subject: "admin", Roles: []string{authz.RoleAdmin}}
	ts := credServer(t, fake, admin)
	const name = "edge-box-1"

	// Issue: 201, body carries credential + jwt (plaintext once).
	code, body := postJSONBody(t, ts.URL+"/api/agents/"+name+"/credentials", `{"description":"deploy host"}`)
	if code != http.StatusCreated {
		t.Fatalf("issue status = %d, want 201", code)
	}
	var issueResp struct {
		JWT string `json:"jwt"`
	}
	if err := json.Unmarshal(body, &issueResp); err != nil {
		t.Fatalf("issue body: %v", err)
	}
	if issueResp.JWT != "eyJhbGciOiJSUzI1NiJ9.fake.jwt" {
		t.Errorf("issue jwt = %q, want the fake plaintext", issueResp.JWT)
	}
	if fake.issuedName != name || fake.issuedBy != "admin" || fake.issuedDesc != "deploy host" {
		t.Errorf("issue forwarded (name=%q, by=%q, desc=%q), want (%q, admin, deploy host)",
			fake.issuedName, fake.issuedBy, fake.issuedDesc, name)
	}

	// List: 200, metadata only — the body must NOT contain the JWT.
	lcode, lbody := doRequest(t, http.MethodGet, ts.URL+"/api/agents/"+name+"/credentials", "")
	if lcode != http.StatusOK {
		t.Fatalf("list status = %d, want 200", lcode)
	}
	if strings.Contains(string(lbody), "fake.jwt") || strings.Contains(string(lbody), "successor.jwt") {
		t.Errorf("list leaked JWT material: %s", lbody)
	}

	// Renew: 200, the SAME credential id, no JWT in the body.
	rcode, rbody := doRequest(t, http.MethodPost, ts.URL+"/api/agents/"+name+"/credentials/cred-1/renew", "")
	if rcode != http.StatusOK {
		t.Fatalf("renew status = %d, want 200", rcode)
	}
	if strings.Contains(string(rbody), ".jwt") {
		t.Errorf("renew leaked JWT material: %s", rbody)
	}
	if fake.renewedID != "cred-1" {
		t.Errorf("renew forwarded id = %q, want cred-1", fake.renewedID)
	}

	// Rotate: 200, successor credential + a NEW plaintext jwt (once).
	zcode, zbody := postJSONBody(t, ts.URL+"/api/agents/"+name+"/credentials/cred-1/rotate", ``)
	if zcode != http.StatusOK {
		t.Fatalf("rotate status = %d, want 200", zcode)
	}
	var rotateResp struct {
		JWT string `json:"jwt"`
	}
	if err := json.Unmarshal(zbody, &rotateResp); err != nil {
		t.Fatalf("rotate body: %v", err)
	}
	if rotateResp.JWT != "eyJhbGciOiJSUzI1NiJ9.successor.jwt" {
		t.Errorf("rotate jwt = %q, want the successor plaintext", rotateResp.JWT)
	}
	if fake.rotatedID != "cred-1" || fake.rotatedBy != "admin" {
		t.Errorf("rotate forwarded (id=%q, by=%q), want (cred-1, admin)", fake.rotatedID, fake.rotatedBy)
	}

	// Revoke: 200, reason forwarded.
	vcode, _ := doRequest(t, http.MethodDelete, ts.URL+"/api/agents/"+name+"/credentials/cred-1", `{"reason":"lost laptop"}`)
	if vcode != http.StatusOK {
		t.Fatalf("revoke status = %d, want 200", vcode)
	}
	if fake.revokedID != "cred-1" || fake.revokedReason != "lost laptop" {
		t.Errorf("revoke forwarded (id=%q, reason=%q), want (cred-1, lost laptop)", fake.revokedID, fake.revokedReason)
	}
}

// credRBACServer builds an API test server with RBAC ENGAGED (the F-14
// engine over the built-in roles) and the agent-credential client attached —
// the apiKeyRBACServer pattern.
func credRBACServer(t *testing.T, user auth.User, fc AgentCredentialClient) http.Handler {
	t.Helper()
	srv := New(Clients{Database: &fakeRBACDatabase{}, Scheduler: &fakeScheduler{}}, nil)
	srv.SetAuthz(authz.New(&testRoleSource{roles: builtinTestRoles()}), true)
	if fc != nil {
		srv.SetAgentCredentialClient(fc)
	}
	handler := srv.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(auth.WithUser(r.Context(), user))
		handler.ServeHTTP(w, r)
	})
}

// TestAgentCredentialRBAC checks the permission split: viewers can list but
// are DENIED the manage operations (issue/renew/rotate/revoke); an admin
// passes them.
func TestAgentCredentialRBAC(t *testing.T) {
	const name = "edge-box-1"
	viewer := auth.User{Subject: "viewer-1", Roles: []string{authz.RoleViewer}}
	admin := auth.User{Subject: "admin-1", Roles: []string{authz.RoleAdmin}}

	serve := func(handler http.Handler) *httptest.Server {
		ts := httptest.NewServer(handler)
		t.Cleanup(ts.Close)
		return ts
	}

	// Viewer may list (agents.can-view is granted to the viewer role)…
	vts := serve(credRBACServer(t, viewer, &fakeAgentCred{}))
	if code, _ := doRequest(t, http.MethodGet, vts.URL+"/api/agents/"+name+"/credentials", ""); code != http.StatusOK {
		t.Errorf("viewer list status = %d, want 200", code)
	}
	// …but is denied every manage operation (agents.can-manage).
	denied := []struct {
		method, path, body string
	}{
		{http.MethodPost, "/api/agents/" + name + "/credentials", "{}"},
		{http.MethodPost, "/api/agents/" + name + "/credentials/cred-1/renew", ""},
		{http.MethodPost, "/api/agents/" + name + "/credentials/cred-1/rotate", "{}"},
		{http.MethodDelete, "/api/agents/" + name + "/credentials/cred-1", "{}"},
	}
	for _, d := range denied {
		code, _ := doRequest(t, d.method, vts.URL+d.path, d.body)
		if code != http.StatusForbidden {
			t.Errorf("viewer %s %s status = %d, want 403", d.method, d.path, code)
		}
	}

	// Admin passes the manage operations (agents.can-manage via AllPermissions).
	ats := serve(credRBACServer(t, admin, &fakeAgentCred{}))
	if code, _ := postJSONBody(t, ats.URL+"/api/agents/"+name+"/credentials", "{}"); code != http.StatusCreated {
		t.Errorf("admin issue status = %d, want 201", code)
	}
	if code, _ := doRequest(t, http.MethodPost, ats.URL+"/api/agents/"+name+"/credentials/cred-1/renew", ""); code != http.StatusOK {
		t.Errorf("admin renew status = %d, want 200", code)
	}
}
