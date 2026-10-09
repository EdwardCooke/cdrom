package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"cdrom/internal/audit"
	"cdrom/internal/auth"
	"cdrom/internal/authz"
	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fakeAuditDB is a stub DatabaseClient that records the audit events appended
// to it (via AppendAuditEvent) and returns canned pipeline/run responses for
// the handlers the audit wiring tests exercise. It embeds the interface (nil)
// so any unstubbed method is not called by these tests.
type fakeAuditDB struct {
	dbpb.DatabaseClient
	appended []*dbpb.AppendAuditEventRequest
}

func (f *fakeAuditDB) AppendAuditEvent(ctx context.Context, in *dbpb.AppendAuditEventRequest, opts ...grpc.CallOption) (*dbpb.AuditEvent, error) {
	f.appended = append(f.appended, in)
	return &dbpb.AuditEvent{Id: int64(len(f.appended))}, nil
}

func (f *fakeAuditDB) ListAuditEvents(ctx context.Context, in *dbpb.ListAuditEventsRequest, opts ...grpc.CallOption) (*dbpb.ListAuditEventsResponse, error) {
	out := &dbpb.ListAuditEventsResponse{}
	for i, e := range f.appended {
		out.Events = append(out.Events, &dbpb.AuditEvent{
			Id:          int64(i + 1),
			Actor:       e.Actor,
			ActorKind:   e.ActorKind,
			Action:      e.Action,
			TargetKind:  e.TargetKind,
			TargetId:    e.TargetId,
			TargetName:  e.TargetName,
			PipelineId:  e.PipelineId,
			RunId:       e.RunId,
			Outcome:     e.Outcome,
			SourceIp:    e.SourceIp,
			OldValue:    e.OldValue,
			NewValue:    e.NewValue,
			Details:     e.Details,
		})
	}
	return out, nil
}

func (f *fakeAuditDB) PruneAuditEvents(ctx context.Context, in *dbpb.PruneAuditEventsRequest, opts ...grpc.CallOption) (*dbpb.PruneAuditEventsResponse, error) {
	return &dbpb.PruneAuditEventsResponse{Pruned: 0}, nil
}

func (f *fakeAuditDB) CreatePipeline(ctx context.Context, in *dbpb.CreatePipelineRequest, opts ...grpc.CallOption) (*dbpb.Pipeline, error) {
	return &dbpb.Pipeline{Id: 1, Name: in.GetName(), Description: in.GetDescription()}, nil
}

func (f *fakeAuditDB) GetPipeline(ctx context.Context, in *dbpb.GetPipelineRequest, opts ...grpc.CallOption) (*dbpb.Pipeline, error) {
	return &dbpb.Pipeline{Id: in.GetId(), Name: "old"}, nil
}

func (f *fakeAuditDB) UpdatePipeline(ctx context.Context, in *dbpb.UpdatePipelineRequest, opts ...grpc.CallOption) (*dbpb.Pipeline, error) {
	return &dbpb.Pipeline{Id: in.GetId(), Name: "new"}, nil
}

func (f *fakeAuditDB) CreateRole(ctx context.Context, in *dbpb.CreateRoleRequest, opts ...grpc.CallOption) (*dbpb.Role, error) {
	return &dbpb.Role{Name: in.GetName(), Permissions: in.GetPermissions()}, nil
}

func (f *fakeAuditDB) GetRole(ctx context.Context, in *dbpb.GetRoleRequest, opts ...grpc.CallOption) (*dbpb.Role, error) {
	return &dbpb.Role{Name: in.GetName()}, nil
}

func (f *fakeAuditDB) ListRoles(ctx context.Context, in *dbpb.ListRolesRequest, opts ...grpc.CallOption) (*dbpb.ListRolesResponse, error) {
	return &dbpb.ListRolesResponse{}, nil
}

func (f *fakeAuditDB) ListRoleBindings(ctx context.Context, in *dbpb.ListRoleBindingsRequest, opts ...grpc.CallOption) (*dbpb.ListRoleBindingsResponse, error) {
	return &dbpb.ListRoleBindingsResponse{}, nil
}

func (f *fakeAuditDB) AddRoleBinding(ctx context.Context, in *dbpb.AddRoleBindingRequest, opts ...grpc.CallOption) (*dbpb.RoleBinding, error) {
	return &dbpb.RoleBinding{Id: 1, RoleName: in.GetRoleName(), PrincipalId: in.GetPrincipalId()}, nil
}

func (f *fakeAuditDB) DeleteRoleBinding(ctx context.Context, in *dbpb.DeleteRoleBindingRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeAuditDB) PublishRoleChange(ctx context.Context, in *dbpb.PublishRoleChangeRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	return &dbpb.PublishEventResponse{Id: 1}, nil
}

// auditTestServer builds an API server with an audit recorder attached (writing
// the local audit log to a temp file and the durable store to a fakeAuditDB),
// and returns the handler plus the fake db (to inspect appended events) and a
// way to inject the authenticated user.
func auditTestServer(t *testing.T, user auth.User) (http.Handler, *fakeAuditDB) {
	t.Helper()
	db := &fakeAuditDB{}
	recorder, err := NewAuditRecorder(config.AuditConfig{File: filepath.Join(t.TempDir(), "audit.log"), Format: "json"}, db, nil)
	if err != nil {
		t.Fatalf("NewAuditRecorder: %v", err)
	}
	t.Cleanup(func() { _ = recorder.Close() })

	srv := New(Clients{Database: db, Scheduler: &fakeScheduler{}}, nil)
	srv.SetAudit(recorder)
	// Enable RBAC so the actor is the authenticated user (not "anonymous") and
	// the permission checks are enforced. The role catalog grants the "test"
	// role every permission, so a user with that role can perform any audited
	// action; a user with no roles is denied (the forbidden test).
	srv.SetAuthz(authzEngineForTest(t), true)

	handler := srv.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(auth.WithUser(r.Context(), user))
		handler.ServeHTTP(w, r)
	}), db
}

// authzEngineForTest builds an authorization engine whose role catalog grants
// every permission to a single "test" role. A user whose token carries the
// "test" role can perform any audited action; a user with no roles is denied
// everything (deny-by-default).
func authzEngineForTest(t *testing.T) *authz.Engine {
	t.Helper()
	source := &testRoleSource{
		roles: []authz.Role{{
			Name:        "test",
			Permissions: authz.AllPermissions,
		}},
	}
	return authz.New(source)
}

// auditTestRequest runs a request against the handler and returns the status
// code.
func auditTestRequest(t *testing.T, handler http.Handler, method, path, body string) int {
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
	return rec.Code
}

// TestAuditPipelineCreate verifies that creating a pipeline records an audit
// event (to the durable store) with the actor, action, target, and new value.
func TestAuditPipelineCreate(t *testing.T) {
	handler, db := auditTestServer(t, auth.User{Subject: "alice", Roles: []string{"test"}})

	code := auditTestRequest(t, handler, http.MethodPost, "/api/pipelines", `{"name":"build","description":"ci"}`)
	if code != http.StatusCreated {
		t.Fatalf("POST /api/pipelines = %d, want 201", code)
	}
	if len(db.appended) != 1 {
		t.Fatalf("appended events = %d, want 1", len(db.appended))
	}
	e := db.appended[0]
	if e.Actor != "alice" || e.ActorKind != audit.ActorKindUser {
		t.Errorf("actor = %q/%q, want alice/user", e.Actor, e.ActorKind)
	}
	if e.Action != audit.ActionPipelineCreate || e.TargetKind != audit.TargetPipeline {
		t.Errorf("action/target = %q/%q, want %s/%s", e.Action, e.TargetKind, audit.ActionPipelineCreate, audit.TargetPipeline)
	}
	if e.TargetName != "build" || e.PipelineId != 1 {
		t.Errorf("target = %q pipeline=%d, want build/1", e.TargetName, e.PipelineId)
	}
	if e.Outcome != audit.OutcomeSuccess {
		t.Errorf("outcome = %q, want %s", e.Outcome, audit.OutcomeSuccess)
	}
	if e.NewValue == "" || !strings.Contains(e.NewValue, `"name":"build"`) {
		t.Errorf("new_value = %q, want a document naming the pipeline", e.NewValue)
	}
}

// TestAuditPipelineUpdate verifies that updating a pipeline records an audit
// event carrying both the old and new value documents.
func TestAuditPipelineUpdate(t *testing.T) {
	handler, db := auditTestServer(t, auth.User{Subject: "alice", Roles: []string{"test"}})

	code := auditTestRequest(t, handler, http.MethodPut, "/api/pipelines/1", `{"name":"build2"}`)
	if code != http.StatusOK {
		t.Fatalf("PUT /api/pipelines/1 = %d, want 200", code)
	}
	if len(db.appended) != 1 {
		t.Fatalf("appended events = %d, want 1", len(db.appended))
	}
	e := db.appended[0]
	if e.Action != audit.ActionPipelineUpdate {
		t.Errorf("action = %q, want %s", e.Action, audit.ActionPipelineUpdate)
	}
	if e.OldValue == "" || e.NewValue == "" {
		t.Errorf("old/new value = %q/%q, want both set", e.OldValue, e.NewValue)
	}
}

// TestAuditRunTrigger verifies that triggering a run records an audit event
// with the run as the target and the trigger in the details.
func TestAuditRunTrigger(t *testing.T) {
	handler, db := auditTestServer(t, auth.User{Subject: "alice", Roles: []string{"test"}})

	code := auditTestRequest(t, handler, http.MethodPost, "/api/pipelines/1/runs", `{"trigger":"manual"}`)
	if code != http.StatusCreated {
		t.Fatalf("POST /api/pipelines/1/runs = %d, want 201", code)
	}
	if len(db.appended) != 1 {
		t.Fatalf("appended events = %d, want 1", len(db.appended))
	}
	e := db.appended[0]
	if e.Action != audit.ActionRunTrigger || e.TargetKind != audit.TargetRun {
		t.Errorf("action/target = %q/%q, want %s/%s", e.Action, e.TargetKind, audit.ActionRunTrigger, audit.TargetRun)
	}
	if e.PipelineId != 1 {
		t.Errorf("pipeline_id = %d, want 1", e.PipelineId)
	}
	if !strings.Contains(e.Details, `"trigger":"manual"`) {
		t.Errorf("details = %q, want the trigger name", e.Details)
	}
}

// TestAuditRoleCreate verifies that creating a role records an audit event.
func TestAuditRoleCreate(t *testing.T) {
	handler, db := auditTestServer(t, auth.User{Subject: "alice", Roles: []string{"test"}})

	code := auditTestRequest(t, handler, http.MethodPost, "/api/roles", `{"name":"auditor","permissions":["audit.can-view"]}`)
	if code != http.StatusCreated {
		t.Fatalf("POST /api/roles = %d, want 201", code)
	}
	if len(db.appended) != 1 {
		t.Fatalf("appended events = %d, want 1", len(db.appended))
	}
	e := db.appended[0]
	if e.Action != audit.ActionRoleCreate || e.TargetKind != audit.TargetRole {
		t.Errorf("action/target = %q/%q, want %s/%s", e.Action, e.TargetKind, audit.ActionRoleCreate, audit.TargetRole)
	}
	if e.TargetId != "auditor" {
		t.Errorf("target_id = %q, want auditor", e.TargetId)
	}
}

// TestAuditBindingAdd verifies that adding a role binding records an audit
// event with the binding's fields in the new value.
func TestAuditBindingAdd(t *testing.T) {
	handler, db := auditTestServer(t, auth.User{Subject: "alice", Roles: []string{"test"}})

	// The caller (holding the "test" role) binds that same role to another
	// principal — the delegation rule requires the caller to hold the role
	// being granted.
	code := auditTestRequest(t, handler, http.MethodPost, "/api/role-bindings", `{"principal_kind":"user","principal_id":"bob","role_name":"test"}`)
	if code != http.StatusCreated {
		t.Fatalf("POST /api/role-bindings = %d, want 201", code)
	}
	if len(db.appended) != 1 {
		t.Fatalf("appended events = %d, want 1", len(db.appended))
	}
	e := db.appended[0]
	if e.Action != audit.ActionBindingAdd || e.TargetKind != audit.TargetRoleBinding {
		t.Errorf("action/target = %q/%q, want %s/%s", e.Action, e.TargetKind, audit.ActionBindingAdd, audit.TargetRoleBinding)
	}
	if !strings.Contains(e.NewValue, `"role_name":"test"`) {
		t.Errorf("new_value = %q, want the binding's role", e.NewValue)
	}
}

// TestAuditListEndpoint verifies GET /api/audit requires the audit.can-view
// permission and returns the recorded events.
func TestAuditListEndpoint(t *testing.T) {
	// A user with audit.can-view can list the audit log.
	handler, db := auditTestServer(t, auth.User{Subject: "alice", Roles: []string{"test"}})
	// Seed an event by creating a pipeline.
	if code := auditTestRequest(t, handler, http.MethodPost, "/api/pipelines", `{"name":"build"}`); code != http.StatusCreated {
		t.Fatalf("POST /api/pipelines = %d, want 201", code)
	}
	code := auditTestRequest(t, handler, http.MethodGet, "/api/audit", "")
	if code != http.StatusOK {
		t.Fatalf("GET /api/audit = %d, want 200", code)
	}
	if len(db.appended) != 1 {
		t.Fatalf("appended events = %d, want 1", len(db.appended))
	}
}

// TestAuditListEndpointForbidden verifies a user without audit.can-view cannot
// list the audit log.
func TestAuditListEndpointForbidden(t *testing.T) {
	// A user with no roles is denied audit.can-view (deny-by-default).
	handler, _ := auditTestServer(t, auth.User{Subject: "bob"})
	code := auditTestRequest(t, handler, http.MethodGet, "/api/audit", "")
	if code != http.StatusForbidden {
		t.Errorf("GET /api/audit (no permission) = %d, want 403", code)
	}
}

// TestAuditRecorderLocalFile verifies NewAuditRecorder writes to the local
// audit file (separate from the default log) in the configured format.
func TestAuditRecorderLocalFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	recorder, err := NewAuditRecorder(config.AuditConfig{File: path, Format: "json"}, nil, nil)
	if err != nil {
		t.Fatalf("NewAuditRecorder: %v", err)
	}
	defer recorder.Close()
	recorder.Record(audit.Event{Actor: "alice", Action: audit.ActionLogin, TargetKind: audit.TargetLogin, Outcome: audit.OutcomeSuccess})

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit file: %v", err)
	}
	if !strings.Contains(string(data), `"actor":"alice"`) {
		t.Errorf("audit file = %q, want the record", string(data))
	}
}

// TestAuditRecorderStdout verifies a "-" file routes the local log to stdout
// (no file is created) and does not error.
func TestAuditRecorderStdout(t *testing.T) {
	recorder, err := NewAuditRecorder(config.AuditConfig{File: "-"}, nil, nil)
	if err != nil {
		t.Fatalf("NewAuditRecorder: %v", err)
	}
	defer recorder.Close()
	recorder.Record(audit.Event{Actor: "alice", Action: audit.ActionLogin, Outcome: audit.OutcomeSuccess})
}

// TestAuditRotationConfig verifies the config's rotation string maps to the
// audit package's Rotation type.
func TestAuditRotationConfig(t *testing.T) {
	if got := auditRotation("hourly"); got != audit.RotationHourly {
		t.Errorf("auditRotation(hourly) = %q, want %q", got, audit.RotationHourly)
	}
	if got := auditRotation("daily"); got != audit.RotationDaily {
		t.Errorf("auditRotation(daily) = %q, want %q", got, audit.RotationDaily)
	}
	if got := auditRotation(""); got != audit.RotationDaily {
		t.Errorf("auditRotation(empty) = %q, want %q (default)", got, audit.RotationDaily)
	}
}
