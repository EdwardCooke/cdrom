// F-28.5 Slice 4 — the read-only /api/groups admin surface.
//
// Coverage: the guard matrix (denied / allowed / synthetic-admin), the
// enriched SHAPE (list + get, resolved-member nulls, derived_roles, stable
// field names), and COMPONENT DEGRADATION (IdP unwired ⇒ 503; transport
// error ⇒ 503; unknown id ⇒ 404). The IdP is the group directory's home, so
// the API is a thin bridge (cf. /api/users).
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/auth"
	"cdrom/internal/authz"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// fakeGroupSvc is a controllable groupDetailsService (embedding the nil
// IdPClient so any unstubbed method is not called).
type fakeGroupSvc struct {
	idppb.IdPClient
	listFn func() (*idppb.ListGroupDetailsResponse, error)
	getFn  func(id string) (*idppb.GroupDetail, error)
}

func (f *fakeGroupSvc) ListGroupDetails(_ context.Context, _ *idppb.ListGroupDetailsRequest, _ ...grpc.CallOption) (*idppb.ListGroupDetailsResponse, error) {
	if f.listFn == nil {
		return &idppb.ListGroupDetailsResponse{}, nil
	}
	return f.listFn()
}

func (f *fakeGroupSvc) GetGroupDetails(_ context.Context, in *idppb.GetGroupDetailsRequest, _ ...grpc.CallOption) (*idppb.GroupDetail, error) {
	if f.getFn == nil {
		return nil, status.Error(codes.NotFound, "no such group")
	}
	return f.getFn(in.GetId())
}

// groupsTestServer builds an API server with RBAC ENABLED and the given
// group service, wrapping the handler to inject the given user (simulating
// the auth middleware).
func groupsTestServer(t *testing.T, user auth.User, svc groupDetailsService) *httptest.Server {
	t.Helper()
	srv := New(Clients{}, nil)
	srv.SetAuthz(authz.New(&testRoleSource{roles: builtinTestRoles()}), true)
	if svc != nil {
		srv.SetGroupDetailsService(svc)
	}
	handler := srv.Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(auth.WithUser(r.Context(), user))
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func strPtr(s string) *string { return &s }

// sampleGroupDetail is a representative enriched document for shape legs.
func sampleGroupDetail() *idppb.GroupDetail {
	return &idppb.GroupDetail{
		Id:           "1",
		DisplayName:  "Pod",
		Description:  "the pod team",
		ExternalId:   "ext-pod",
		MemberValues: []string{"ext-alice", "ghost"},
		Members: []*idppb.ResolvedGroupMember{
			{Value: "ext-alice", Username: strPtr("alice")},
			{Value: "ghost", Username: nil}, // unresolvable ⇒ null
		},
		DerivedRoles: []string{"viewer"},
		CreatedAt:    "2026-11-01T00:00:00Z",
		UpdatedAt:    "2026-11-02T00:00:00Z",
	}
}

// TestGroupsGuardMatrix proves the users.can-manage guard: a VIEWER is
// denied (403), an ADMIN is allowed (200).
func TestGroupsGuardMatrix(t *testing.T) {
	svc := &fakeGroupSvc{listFn: func() (*idppb.ListGroupDetailsResponse, error) {
		return &idppb.ListGroupDetailsResponse{Groups: []*idppb.GroupDetail{sampleGroupDetail()}}, nil
	}}

	viewerTs := groupsTestServer(t, auth.User{Subject: "v", Roles: []string{authz.RoleViewer}}, svc)
	if code, _ := doRequest(t, http.MethodGet, viewerTs.URL+"/api/groups", ""); code != http.StatusForbidden {
		t.Errorf("viewer GET /api/groups = %d, want 403", code)
	}
	adminTs := groupsTestServer(t, auth.User{Subject: "a", Roles: []string{authz.RoleAdmin}}, svc)
	if code, _ := doRequest(t, http.MethodGet, adminTs.URL+"/api/groups", ""); code != http.StatusOK {
		t.Errorf("admin GET /api/groups = %d, want 200", code)
	}
}

// TestGroupsSyntheticAdmin proves the degenerate mode (RBAC off): an
// admin-role user is allowed.
func TestGroupsSyntheticAdmin(t *testing.T) {
	srv := New(Clients{}, nil)
	// No SetAuthz ⇒ rbacEnabled false ⇒ requireGroupAccess falls back to the
	// admin-role check.
	srv.SetGroupDetailsService(&fakeGroupSvc{listFn: func() (*idppb.ListGroupDetailsResponse, error) {
		return &idppb.ListGroupDetailsResponse{Groups: []*idppb.GroupDetail{sampleGroupDetail()}}, nil
	}})
	handler := srv.Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(auth.WithUser(r.Context(), auth.User{Subject: "root", Roles: []string{authz.RoleAdmin}}))
		handler.ServeHTTP(w, r)
	}))
	defer ts.Close()
	if code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/groups", ""); code != http.StatusOK {
		t.Errorf("synthetic-admin GET /api/groups = %d, want 200", code)
	}
}

// TestGroupsShape proves the UI contract: stable field names, the resolved
// member nulls, and derived_roles.
func TestGroupsShape(t *testing.T) {
	svc := &fakeGroupSvc{
		listFn: func() (*idppb.ListGroupDetailsResponse, error) {
			return &idppb.ListGroupDetailsResponse{Groups: []*idppb.GroupDetail{sampleGroupDetail()}}, nil
		},
		getFn: func(id string) (*idppb.GroupDetail, error) {
			if id == "1" {
				return sampleGroupDetail(), nil
			}
			return nil, status.Error(codes.NotFound, "no such group")
		},
	}
	ts := groupsTestServer(t, auth.User{Subject: "a", Roles: []string{authz.RoleAdmin}}, svc)

	// LIST.
	code, body := doRequest(t, http.MethodGet, ts.URL+"/api/groups", "")
	if code != http.StatusOK {
		t.Fatalf("list = %d, want 200", code)
	}
	var items []groupDetailDTO
	if err := json.Unmarshal(body, &items); err != nil {
		t.Fatalf("list body not an array: %v (%s)", err, body)
	}
	if len(items) != 1 {
		t.Fatalf("list len = %d, want 1", len(items))
	}
	it := items[0]
	if it.ID != "1" || it.DisplayName != "Pod" || it.Description != "the pod team" || it.ExternalID != "ext-pod" {
		t.Errorf("scalar fields wrong: %+v", it)
	}
	if len(it.MemberValues) != 2 || it.MemberValues[0] != "ext-alice" || it.MemberValues[1] != "ghost" {
		t.Errorf("member_values = %v", it.MemberValues)
	}
	if len(it.Members) != 2 {
		t.Fatalf("members len = %d, want 2", len(it.Members))
	}
	if it.Members[0].Username == nil || *it.Members[0].Username != "alice" {
		t.Errorf("members[0].username = %v, want alice", it.Members[0].Username)
	}
	if it.Members[1].Username != nil {
		t.Errorf("members[1].username = %v, want null (unresolvable)", *it.Members[1].Username)
	}
	if len(it.DerivedRoles) != 1 || it.DerivedRoles[0] != "viewer" {
		t.Errorf("derived_roles = %v, want [viewer]", it.DerivedRoles)
	}
	if it.CreatedAt == "" || it.UpdatedAt == "" {
		t.Errorf("timestamps missing: %+v", it)
	}

	// GET (single object).
	code, body = doRequest(t, http.MethodGet, ts.URL+"/api/groups/1", "")
	if code != http.StatusOK {
		t.Fatalf("get = %d, want 200", code)
	}
	var one groupDetailDTO
	if err := json.Unmarshal(body, &one); err != nil {
		t.Fatalf("get body not an object: %v (%s)", err, body)
	}
	if one.ID != "1" || one.DisplayName != "Pod" {
		t.Errorf("get fields wrong: %+v", one)
	}
}

// TestGroupsDegradation proves the component-degradation contract: IdP
// unwired ⇒ 503; transport error ⇒ 503; unknown id ⇒ 404.
func TestGroupsDegradation(t *testing.T) {
	admin := auth.User{Subject: "a", Roles: []string{authz.RoleAdmin}}

	// (a) Unwired (nil service).
	nilTs := groupsTestServer(t, admin, nil)
	if code, _ := doRequest(t, http.MethodGet, nilTs.URL+"/api/groups", ""); code != http.StatusServiceUnavailable {
		t.Errorf("unwired list = %d, want 503", code)
	}
	if code, _ := doRequest(t, http.MethodGet, nilTs.URL+"/api/groups/1", ""); code != http.StatusServiceUnavailable {
		t.Errorf("unwired get = %d, want 503", code)
	}

	// (b) Transport error (IdP unreachable).
	errTs := groupsTestServer(t, admin, &fakeGroupSvc{
		listFn: func() (*idppb.ListGroupDetailsResponse, error) {
			return nil, status.Error(codes.Unavailable, "io: connection refused")
		},
		getFn: func(string) (*idppb.GroupDetail, error) {
			return nil, status.Error(codes.Unavailable, "io: connection refused")
		},
	})
	if code, _ := doRequest(t, http.MethodGet, errTs.URL+"/api/groups", ""); code != http.StatusServiceUnavailable {
		t.Errorf("error list = %d, want 503", code)
	}
	if code, _ := doRequest(t, http.MethodGet, errTs.URL+"/api/groups/9", ""); code != http.StatusServiceUnavailable {
		t.Errorf("error get = %d, want 503", code)
	}

	// (c) Unknown id ⇒ 404.
	notFoundTs := groupsTestServer(t, admin, &fakeGroupSvc{
		getFn: func(string) (*idppb.GroupDetail, error) {
			return nil, status.Error(codes.NotFound, "no such group")
		},
	})
	if code, _ := doRequest(t, http.MethodGet, notFoundTs.URL+"/api/groups/999", ""); code != http.StatusNotFound {
		t.Errorf("unknown id get = %d, want 404", code)
	}
}

// TestGroupsReadOnly proves NO mutating routes exist: POST/PUT/PATCH/DELETE
// on the group paths are not handled (405 MethodNotAllowed from the mux, or
// 404 if unrouted) — never a mutation.
func TestGroupsReadOnly(t *testing.T) {
	svc := &fakeGroupSvc{listFn: func() (*idppb.ListGroupDetailsResponse, error) {
		return &idppb.ListGroupDetailsResponse{}, nil
	}}
	ts := groupsTestServer(t, auth.User{Subject: "a", Roles: []string{authz.RoleAdmin}}, svc)
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/api/groups"},
		{http.MethodPut, "/api/groups/1"},
		{http.MethodPatch, "/api/groups/1"},
		{http.MethodDelete, "/api/groups/1"},
	} {
		req, _ := http.NewRequest(tc.method, ts.URL+tc.path, bytes.NewReader(nil))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		// The mux only registered GET on these paths, so a non-GET is a
		// 405 (allowed methods advertised) — proof the verbs are not wired.
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s %s = %d, want 405 (route is GET-only)", tc.method, tc.path, resp.StatusCode)
		}
	}
}
