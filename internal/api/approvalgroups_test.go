package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"cdrom/internal/auth"
	"cdrom/internal/authz"
	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// fakeApprovalDB is a stub DatabaseClient for the F-27 approval tests: it
// serves the handful of RPCs resolveApproval / isAuthorizedApprover exercise
// (gate introspection, group membership, observed claims, the vote ledger) and
// records CastApprovalVote calls. Embedding the (nil) interface makes any
// UNSTUBBED method fault loudly if a test accidentally reaches it.
type fakeApprovalDB struct {
	dbpb.DatabaseClient
	groupMembers map[string][]string
	observed     map[string][]string
	voters       []string // per-gate distinct approved actors
	casted       int
}

func (f *fakeApprovalDB) GetJob(_ context.Context, in *dbpb.GetJobRequest, _ ...grpc.CallOption) (*dbpb.Job, error) {
	return &dbpb.Job{Id: in.GetId()}, nil
}
func (f *fakeApprovalDB) EffectiveApprovalGroupMembers(_ context.Context, in *dbpb.EffectiveApprovalGroupMembersRequest, _ ...grpc.CallOption) (*dbpb.EffectiveApprovalGroupMembersResponse, error) {
	return &dbpb.EffectiveApprovalGroupMembersResponse{Members: f.groupMembers[in.GetName()]}, nil
}
func (f *fakeApprovalDB) ObservedClaimsFor(_ context.Context, in *dbpb.ObservedClaimsForRequest, _ ...grpc.CallOption) (*dbpb.ObservedClaimsForResponse, error) {
	key := in.GetPrincipalSubject() + "|" + in.GetClaimName()
	return &dbpb.ObservedClaimsForResponse{Values: f.observed[key]}, nil
}
func (f *fakeApprovalDB) ListApprovalVotes(_ context.Context, _ *dbpb.ListApprovalVotesRequest, _ ...grpc.CallOption) (*dbpb.ListApprovalVotesResponse, error) {
	resp := &dbpb.ListApprovalVotesResponse{}
	for _, a := range f.voters {
		resp.Votes = append(resp.Votes, &dbpb.ApprovalVote{Actor: a, Decision: "approved"})
	}
	return resp, nil
}
func (f *fakeApprovalDB) CastApprovalVote(_ context.Context, _ *dbpb.CastApprovalVoteRequest, _ ...grpc.CallOption) (*dbpb.CastApprovalVoteResponse, error) {
	f.casted++
	return &dbpb.CastApprovalVoteResponse{Recorded: true}, nil
}
func (f *fakeApprovalDB) RecordObservedClaim(_ context.Context, _ *dbpb.RecordObservedClaimRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// approvalTestServer builds an RBAC-enforcing API server (operator role, so
// jobs.can-approve is granted) over a fake scheduler (whose GetJob returns a
// job advertising the GIVEN gate references + quorum) and the given fake DB,
// with the F-14 role-claim mapping configured (claim "team" → role "lead").
// The handler injects the given user into the request context (standing in
// for the auth middleware).
func approvalTestServer(t *testing.T, user auth.User, db *fakeApprovalDB, gateRefs []string, quorum int32) http.Handler {
	t.Helper()
	svc := New(Clients{Database: db, Scheduler: &fakeScheduler{job: &schedpb.Job{
		Id: 1, Status: dbpb.JobStatus_JOB_STATUS_AWAITING_APPROVAL,
		ApprovalApprovers: gateRefs, ApprovalQuorum: quorum,
	}}}, nil)
	svc.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.SetConfig(&config.Config{Auth: config.AuthConfig{Roles: config.RolesConfig{
		RoleClaim: "team", RoleMappings: map[string]string{"lead": "lead"},
	}}})
	svc.SetAuthz(authz.New(&staticRoleSource{roles: []authz.Role{{
		Name: authz.RoleOperator, BuiltIn: true,
		Permissions: []string{authz.PermJobsApprove, authz.PermJobsReject},
	}}}), true)
	handler := svc.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r.WithContext(auth.WithUser(r.Context(), user)))
	})
}

// staticRoleSource is a trivial authz.RoleSource for the approval tests.
type staticRoleSource struct {
	roles    []authz.Role
	bindings []authz.Binding
}

func (s *staticRoleSource) ListRoles(context.Context) ([]authz.Role, error) { return s.roles, nil }
func (s *staticRoleSource) ListBindings(context.Context) ([]authz.Binding, error) {
	return s.bindings, nil
}

// TestResolveApprovalAuthorizationMatrix walks the F-27 authorized-set rule:
// a decision is accepted only when the actor holds the F-14 permission AND is
// in the gate's authorized set (group member ∪ claim-derived member ∪ role
// holder ∪ named individual). The fake DB emulates the Database service's
// EFFECTIVE-membership oracle (manual ∪ claim-derived already united); the
// claim→membership linkage itself is covered by the database-service test.
func TestResolveApprovalAuthorizationMatrix(t *testing.T) {
	op := []string{authz.RoleOperator}
	cases := []struct {
		name     string
		refs     []string
		user     auth.User
		groupMem map[string][]string
		wantCode int
	}{
		{name: "no refs → F-13 baseline (any permitted actor)", refs: nil, user: auth.User{Subject: "anyone", Roles: op}, wantCode: http.StatusOK},
		{name: "group member allowed", refs: []string{"group:eng"}, user: auth.User{Subject: "alice", Roles: op}, groupMem: map[string][]string{"eng": {"alice"}}, wantCode: http.StatusOK},
		{name: "group non-member denied", refs: []string{"group:eng"}, user: auth.User{Subject: "mallory", Roles: op}, groupMem: map[string][]string{"eng": {"alice"}}, wantCode: http.StatusForbidden},
		{name: "bare individual allowed", refs: []string{"bob"}, user: auth.User{Subject: "bob", Roles: op}, wantCode: http.StatusOK},
		{name: "bare individual denied", refs: []string{"bob"}, user: auth.User{Subject: "erin", Roles: op}, wantCode: http.StatusForbidden},
		{name: "role holder allowed (live token claim → mapped role)", refs: []string{"role:lead"}, user: auth.User{Subject: "cara", Roles: op, Claims: map[string][]string{"team": {"lead"}}}, wantCode: http.StatusOK},
		{name: "role non-holder denied", refs: []string{"role:lead"}, user: auth.User{Subject: "dan", Roles: op}, wantCode: http.StatusForbidden},
		{name: "claim-derived group member allowed", refs: []string{"group:payroll"}, user: auth.User{Subject: "ella", Roles: op}, groupMem: map[string][]string{"payroll": {"ella"}}, wantCode: http.StatusOK},
		{name: "mix: authorized via one of several refs", refs: []string{"group:nobody", "role:none", "zed"}, user: auth.User{Subject: "zed", Roles: op}, groupMem: map[string][]string{"nobody": {}}, wantCode: http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &fakeApprovalDB{groupMembers: tc.groupMem}
			handler := approvalTestServer(t, tc.user, db, tc.refs, 1)
			req := httptest.NewRequest(http.MethodPost, "/api/jobs/1/approve", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d (%s), want %d", rec.Code, rec.Body.String(), tc.wantCode)
			}
		})
	}
}

// TestResolveApprovalRoleFromObservedClaims verifies the FALLBACK role
// enrichment (F-27): a principal whose LIVE token carries no claims (e.g. an
// API-key principal) is still recognized as holding a role when their OBSERVED
// claims (recorded at a prior sign-in) map to it.
func TestResolveApprovalRoleFromObservedClaims(t *testing.T) {
	db := &fakeApprovalDB{
		observed: map[string][]string{"fran|team": {"lead"}},
	}
	// fran's live token has no claims, but fran was observed with team=lead.
	handler := approvalTestServer(t, auth.User{Subject: "fran", Roles: []string{authz.RoleOperator}}, db, []string{"role:lead"}, 1)
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/1/approve", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d (%s), want 200 (observed-claim role)", rec.Code, rec.Body.String())
	}
}

// TestApplyApprovalDefaults verifies the pipeline-level default approvers /
// quorum merge (F-27): an approval step that sets NEITHER inherits both; a step
// that sets one inherits the other; a step that sets both keeps its own; a
// non-approval step is untouched.
func TestApplyApprovalDefaults(t *testing.T) {
	req := &pipelineRequest{
		Approvers: []string{"group:eng"},
		Quorum:    2,
		Jobs: []pipelineJobRequest{
			{Key: "inherit-both", Spec: &jobSpecRequest{Steps: []jobStepRequest{{Type: "approval"}}}},
			{Key: "override-approvers", Spec: &jobSpecRequest{Steps: []jobStepRequest{{Type: "approval", Params: map[string]*paramValueRequest{"approvers": {Strings: []string{"solo"}}}}}}},
			{Key: "shell-no-touch", Spec: &jobSpecRequest{Steps: []jobStepRequest{{Type: "shell", Params: map[string]*paramValueRequest{"command": {String: "echo hi"}}}}}},
		},
	}
	applyApprovalDefaults(req)

	get := func(key string) *jobStepRequest {
		for i := range req.Jobs {
			if req.Jobs[i].Key == key {
				return &req.Jobs[i].Spec.Steps[0]
			}
		}
		t.Fatalf("job %q not found", key)
		return nil
	}

	ib := get("inherit-both")
	if len(ib.Params["approvers"].Strings) != 1 || ib.Params["approvers"].Strings[0] != "group:eng" {
		t.Errorf("inherit-both approvers = %+v, want [group:eng]", ib.Params["approvers"])
	}
	if ib.Params["quorum"].String != "2" {
		t.Errorf("inherit-both quorum = %q, want \"2\"", ib.Params["quorum"].String)
	}

	oav := get("override-approvers")
	if len(oav.Params["approvers"].Strings) != 1 || oav.Params["approvers"].Strings[0] != "solo" {
		t.Errorf("override-approvers approvers = %+v, want [solo] (unchanged)", oav.Params["approvers"])
	}
	if oav.Params["quorum"] == nil || oav.Params["quorum"].String != "2" {
		t.Errorf("override-approvers quorum = %+v, want inherited \"2\"", oav.Params["quorum"])
	}

	sh := get("shell-no-touch")
	if sh.Params["approvers"] != nil || sh.Params["quorum"] != nil {
		t.Errorf("shell step gained approval params: %+v", sh.Params)
	}
}
