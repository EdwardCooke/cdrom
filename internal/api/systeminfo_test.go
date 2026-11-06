// systeminfo_test.go — F-28.4 Slice 4 (API side): the restricted-write lock on
// PUT /api/users/{id} and the GET /api/system/info aggregator.
//
// Covers: (a) a SCIM-managed user's identity fields are refused (409) EVEN on
// a value-preserving no-op (detected by presence, not value), while roles-only
// payloads and DELETE pass, and JIT/local users stay editable; and (b)
// /api/system/info is segmented, reflects the API's own flags + the IdP's and
// Artifacts' Describe RPCs, and yields a NULL segment (still 200) for a
// degraded/unwired component.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/auth"
	"cdrom/internal/config"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// unavailableErr models a peer that is down/unreachable (the aggregated
// segment must collapse to null on ANY error).
func unavailableErr() error { return errors.New("connection refused") }

// ---------------------------------------------------------------------------
// Restricted-write lock (PUT /api/users/{id})
// ---------------------------------------------------------------------------

// putJSONBody issues a PUT with a JSON body and returns the status code.
func putJSONBody(t *testing.T, url string, body []byte) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", url, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// fakeGetIDPUserDB is a minimal DatabaseClient serving GetIDPUser (the
// by-ID locator the user update/delete handlers use — one row, never the
// whole directory). Embedding the nil interface makes every other method a
// panic-if-called sentinel.
type fakeGetIDPUserDB struct {
	dbpb.DatabaseClient
	user *dbpb.IDPUser
	err  error
}

func (f *fakeGetIDPUserDB) GetIDPUser(_ context.Context, _ *dbpb.GetIDPUserRequest, _ ...grpc.CallOption) (*dbpb.IDPUser, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.user, nil
}

// restrictedWriteServer is userPassServer with a Database client injected
// (the update/delete handlers fetch the target row by ID through it).
func restrictedWriteServer(t *testing.T, up UserPassClient, db dbpb.DatabaseClient, user auth.User) *httptest.Server {
	t.Helper()
	srv := New(Clients{Database: db}, nil)
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

// TestUpdateUserRestrictedWrites verifies the SCIM-managed identity-field lock:
// any identity-field touch (detected by PRESENCE, so value-preserving no-ops
// trip it too) is 409 WITHOUT contacting the IdP; roles-only payloads pass;
// JIT (provisioned, no externalId) and local users remain fully editable; and
// the by-ID fetch's error semantics (NotFound proceeds, other errors abort).
func TestUpdateUserRestrictedWrites(t *testing.T) {
	const id = "42"
	admin := auth.User{Subject: "admin", Roles: []string{adminRole}}
	persona := func(username string, provisioned bool, externalID string) *dbpb.IDPUser {
		return &dbpb.IDPUser{Id: id, Username: username, Provisioned: provisioned, ExternalId: externalID, Roles: []string{"user"}}
	}
	scimManaged := persona("corp.user", true, "ext-1")
	jit := persona("jit.user", true, "")
	local := persona("local.user", false, "")

	put := func(db dbpb.DatabaseClient, payload UpdateUserRequest, idpUpdateErr error) (int, *fakeUserPass) {
		t.Helper()
		// Wrap the IdP error the way the REAL client does (a userPassError
		// carrying the gRPC code) so the handler's status mapping engages.
		var wrapped error
		if idpUpdateErr != nil {
			wrapped = &userPassError{code: status.Code(idpUpdateErr), msg: status.Convert(idpUpdateErr).Message()}
		}
		fake := &fakeUserPass{updateResult: &UserResult{ID: id, Username: "someone", Roles: []string{"user"}}, updateErr: wrapped}
		ts := restrictedWriteServer(t, fake, db, admin)
		body, _ := json.Marshal(payload)
		return putJSONBody(t, ts.URL+"/api/users/"+id, body), fake
	}

	// SCIM-managed: identity-field touch ⇒ 409 (even a no-op value), and the
	// IdP is NEVER CONTACTED (the lock precedes the forward).
	for name, payload := range map[string]UpdateUserRequest{
		"email(no-op)":    {Email: "whatever"},
		"username(no-op)": {},
		"firstName":       {FirstName: "Renamed"},
		"password":        {Password: "hunter2"},
	} {
		payload := payload
		if name == "username(no-op)" {
			payload.Username = "corp.user" // the CURRENT value — a no-op
		}
		code, fake := put(&fakeGetIDPUserDB{user: scimManaged}, payload, nil)
		if code != http.StatusConflict {
			t.Errorf("scim %s = %d, want 409", name, code)
		}
		if fake.updatedID != "" {
			t.Errorf("scim %s: the IdP WAS contacted (updatedID=%q), want the lock to short-circuit", name, fake.updatedID)
		}
	}
	// SCIM-managed: ROLES-ONLY payload passes (200, forwarded).
	if code, fake := put(&fakeGetIDPUserDB{user: scimManaged}, UpdateUserRequest{Roles: []string{"operator"}}, nil); code != http.StatusOK {
		t.Errorf("scim roles-only = %d, want 200", code)
	} else if fake.updatedID != id {
		t.Errorf("scim roles-only: not forwarded to the IdP (updatedID=%q)", fake.updatedID)
	}
	// JIT (provisioned, no externalId): identity fields EDITABLE (200).
	if code, _ := put(&fakeGetIDPUserDB{user: jit}, UpdateUserRequest{Email: "jit@x"}, nil); code != http.StatusOK {
		t.Errorf("jit email = %d, want 200 (fully editable)", code)
	}
	// Local (not provisioned): identity fields EDITABLE (200).
	if code, _ := put(&fakeGetIDPUserDB{user: local}, UpdateUserRequest{Email: "local@x"}, nil); code != http.StatusOK {
		t.Errorf("local email = %d, want 200 (fully editable)", code)
	}
	// Row VANISHED racing us (NotFound): proceed to the IdP (it 404s it).
	notFoundDB := &fakeGetIDPUserDB{err: status.Error(codes.NotFound, "no such user")}
	if code, _ := put(notFoundDB, UpdateUserRequest{Email: "x@y"}, status.Error(codes.NotFound, "no such user")); code != http.StatusNotFound {
		t.Errorf("vanishing row = %d, want 404 (the IdP's verdict)", code)
	}
	// Infrastructure error on the fetch: ABORT (≥500), never a blind forward.
	boomDB := &fakeGetIDPUserDB{err: status.Error(codes.Internal, "db exploded")}
	if code, fake := put(boomDB, UpdateUserRequest{Email: "x@y"}, nil); code < 500 {
		t.Errorf("fetch-error = %d, want ≥500 (aborted)", code)
	} else if fake.updatedID != "" {
		t.Errorf("fetch-error: the IdP was contacted anyway, want aborted before the forward")
	}

	// DELETE remains AVAILABLE for a SCIM-managed user (the lock is on
	// identity-field EDITS, not removal) and fetches by ID too.
	delFake := &fakeUserPass{}
	delTs := restrictedWriteServer(t, delFake, &fakeGetIDPUserDB{user: scimManaged}, admin)
	req, _ := http.NewRequest(http.MethodDelete, delTs.URL+"/api/users/"+id, nil)
	dresp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	dresp.Body.Close()
	if dresp.StatusCode != http.StatusOK {
		t.Errorf("DELETE scim-managed = %d, want 200 (unaffected)", dresp.StatusCode)
	}
	if delFake.deletedID != id {
		t.Errorf("DELETE not forwarded to the IdP (deletedID=%q)", delFake.deletedID)
	}
}

// ---------------------------------------------------------------------------
// GET /api/system/info
// ---------------------------------------------------------------------------

// fakeIdpDesc is a minimal idpSystemDescriber.
type fakeIdpDesc struct {
	resp *idppb.DescribeSystemResponse
	err  error
}

func (f *fakeIdpDesc) DescribeSystem(_ context.Context, _ *idppb.DescribeSystemRequest, _ ...grpc.CallOption) (*idppb.DescribeSystemResponse, error) {
	return f.resp, f.err
}

// fakeArtifactsClient satisfies the Artifacts client interface minimally: it
// embeds the (nil) interface for the unused methods and overrides Describe.
type fakeArtifactsClient struct {
	artifactspb.ArtifactsClient
	resp *artifactspb.DescribeResponse
	err  error
}

func (f *fakeArtifactsClient) Describe(_ context.Context, _ *artifactspb.DescribeRequest, _ ...grpc.CallOption) (*artifactspb.DescribeResponse, error) {
	return f.resp, f.err
}

func systemInfoServer(t *testing.T, cfg *config.Config, doubler idpSystemDescriber, arts artifactspb.ArtifactsClient) *httptest.Server {
	t.Helper()
	srv := New(Clients{Artifacts: arts}, nil)
	if cfg != nil {
		srv.SetConfig(cfg)
	}
	if doubler != nil {
		srv.SetIdPSystemDescriber(doubler)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func getSystemInfo(t *testing.T, ts *httptest.Server) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(ts.URL + "/api/system/info")
	if err != nil {
		t.Fatalf("GET /api/system/info: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	doc := map[string]any{}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("decode system info %q: %v", string(b), err)
	}
	return resp.StatusCode, doc
}

// TestSystemInfoShape verifies the happy path: the endpoint is unauthenticated,
// 200, and carries the API's own flags plus the IdP's and Artifacts' segments.
func TestSystemInfoShape(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Enabled = true
	cfg.Auth.UserPassEnabled = true
	cfg.Auth.APIKey.Enabled = true
	cfg.Auth.ServiceAccount.Enabled = false

	ts := systemInfoServer(t, cfg,
		&fakeIdpDesc{resp: &idppb.DescribeSystemResponse{ScimEnabled: true, LocalLoginEnabled: true, BreakGlassEnabled: true}},
		&fakeArtifactsClient{resp: &artifactspb.DescribeResponse{StoreKind: "filesystem"}})

	code, doc := getSystemInfo(t, ts)
	if code != http.StatusOK {
		t.Fatalf("system info = %d, want 200", code)
	}
	authSeg, _ := doc["auth"].(map[string]any)
	if authSeg == nil {
		t.Fatalf("missing auth segment: %v", doc)
	}
	if authSeg["oidc"] != true || authSeg["userpass"] != true || authSeg["api_keys"] != true || authSeg["service_accounts"] != false {
		t.Errorf("auth segment = %v, want oidc/userpass/api_keys true, service_accounts false", authSeg)
	}
	idpSeg, _ := doc["idp"].(map[string]any)
	if idpSeg == nil {
		t.Fatalf("missing idp segment: %v", doc)
	}
	if idpSeg["scim"] != true || idpSeg["local_login"] != true || idpSeg["break_glass"] != true {
		t.Errorf("idp segment = %v, want all true", idpSeg)
	}
	artsSeg, _ := doc["artifacts"].(map[string]any)
	if artsSeg == nil || artsSeg["store_kind"] != "filesystem" {
		t.Errorf("artifacts segment = %v, want store_kind filesystem", artsSeg)
	}
}

// TestSystemInfoDegradedComponents verifies a degraded (errored) or unwired
// component yields a NULL segment while the endpoint still returns 200.
func TestSystemInfoDegradedComponents(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Enabled = true

	// Errored IdP + errored Artifacts ⇒ both segments null.
	ts := systemInfoServer(t, cfg,
		&fakeIdpDesc{err: unavailableErr()},
		&fakeArtifactsClient{err: unavailableErr()})
	code, doc := getSystemInfo(t, ts)
	if code != http.StatusOK {
		t.Fatalf("degraded system info = %d, want 200", code)
	}
	if doc["idp"] != nil {
		t.Errorf("idp segment = %v, want null (degraded)", doc["idp"])
	}
	if doc["artifacts"] != nil {
		t.Errorf("artifacts segment = %v, want null (degraded)", doc["artifacts"])
	}
	// The API's own segment is ALWAYS present (never degraded).
	if _, ok := doc["auth"].(map[string]any); !ok {
		t.Errorf("auth segment missing even when peers degrade: %v", doc)
	}

	// Unwired (nil) IdP + nil Artifacts ⇒ both segments null too.
	ts2 := systemInfoServer(t, cfg, nil, nil)
	_, doc2 := getSystemInfo(t, ts2)
	if doc2["idp"] != nil || doc2["artifacts"] != nil {
		t.Errorf("unwired segments = idp:%v artifacts:%v, want null", doc2["idp"], doc2["artifacts"])
	}
}

// TestSystemInfoNonSecret scans the happy-path body for forbidden material:
// no addresses, keys, peppers, or DSNs may leak (the whitelist is capability
// only).
func TestSystemInfoNonSecret(t *testing.T) {
	cfg := &config.Config{}
	cfg.Auth.Enabled = true
	ts := systemInfoServer(t, cfg,
		&fakeIdpDesc{resp: &idppb.DescribeSystemResponse{LocalLoginEnabled: true, BreakGlassEnabled: true}},
		&fakeArtifactsClient{resp: &artifactspb.DescribeResponse{StoreKind: "filesystem"}})
	resp, err := http.Get(ts.URL + "/api/system/info")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	text := string(b)
	for _, needle := range []string{"127.0.0.1", ":7104", "dsn=", "postgresql://", "pepper", "private_key", "cdrom-"} {
		if strings.Contains(strings.ToLower(text), strings.ToLower(needle)) {
			t.Errorf("system info leaked %q: %s", needle, text)
		}
	}
}
