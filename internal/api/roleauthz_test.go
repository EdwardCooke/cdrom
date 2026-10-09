package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"cdrom/internal/auth"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// testRoleSource is an in-memory authz.RoleSource for RBAC tests.
type testRoleSource struct {
	roles    []authz.Role
	bindings []authz.Binding
}

func (s *testRoleSource) ListRoles(context.Context) ([]authz.Role, error) {
	return s.roles, nil
}

func (s *testRoleSource) ListBindings(context.Context) ([]authz.Binding, error) {
	return s.bindings, nil
}

// builtinTestRoles returns the built-in role definitions as authz.Roles.
func builtinTestRoles() []authz.Role {
	roles := make([]authz.Role, 0, len(authz.BuiltinRoles()))
	for _, def := range authz.BuiltinRoles() {
		roles = append(roles, authz.Role{
			Name:        def.Name,
			Description: def.Description,
			BuiltIn:     def.BuiltIn,
			Permissions: def.Permissions,
			Includes:    def.Includes,
		})
	}
	return roles
}

// fakeRBACDatabase is a stub DatabaseClient that returns empty responses for the
// read/create RPCs the RBAC tests exercise. It embeds the interface (nil) so
// any unstubbed method is not called by these tests.
type fakeRBACDatabase struct {
	dbpb.DatabaseClient
}

func (f *fakeRBACDatabase) ListPipelines(ctx context.Context, in *dbpb.ListPipelinesRequest, opts ...grpc.CallOption) (*dbpb.ListPipelinesResponse, error) {
	return &dbpb.ListPipelinesResponse{}, nil
}
func (f *fakeRBACDatabase) CreatePipeline(ctx context.Context, in *dbpb.CreatePipelineRequest, opts ...grpc.CallOption) (*dbpb.Pipeline, error) {
	return &dbpb.Pipeline{Id: 1, Name: in.GetName()}, nil
}
func (f *fakeRBACDatabase) ListRoles(ctx context.Context, in *dbpb.ListRolesRequest, opts ...grpc.CallOption) (*dbpb.ListRolesResponse, error) {
	return &dbpb.ListRolesResponse{}, nil
}
func (f *fakeRBACDatabase) ListRoleBindings(ctx context.Context, in *dbpb.ListRoleBindingsRequest, opts ...grpc.CallOption) (*dbpb.ListRoleBindingsResponse, error) {
	return &dbpb.ListRoleBindingsResponse{}, nil
}
func (f *fakeRBACDatabase) GetRole(ctx context.Context, in *dbpb.GetRoleRequest, opts ...grpc.CallOption) (*dbpb.Role, error) {
	return &dbpb.Role{Name: in.GetName()}, nil
}
func (f *fakeRBACDatabase) CreateRole(ctx context.Context, in *dbpb.CreateRoleRequest, opts ...grpc.CallOption) (*dbpb.Role, error) {
	return &dbpb.Role{Name: in.GetName()}, nil
}
func (f *fakeRBACDatabase) UpdateRole(ctx context.Context, in *dbpb.UpdateRoleRequest, opts ...grpc.CallOption) (*dbpb.Role, error) {
	return &dbpb.Role{Name: in.GetName()}, nil
}
func (f *fakeRBACDatabase) DeleteRole(ctx context.Context, in *dbpb.DeleteRoleRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (f *fakeRBACDatabase) AddRoleBinding(ctx context.Context, in *dbpb.AddRoleBindingRequest, opts ...grpc.CallOption) (*dbpb.RoleBinding, error) {
	return &dbpb.RoleBinding{Id: 1}, nil
}
func (f *fakeRBACDatabase) DeleteRoleBinding(ctx context.Context, in *dbpb.DeleteRoleBindingRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (f *fakeRBACDatabase) PublishRoleChange(ctx context.Context, in *dbpb.PublishRoleChangeRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	return &dbpb.PublishEventResponse{Id: 1}, nil
}

// rbacTestServer builds an API server with RBAC enabled over a fake role
// source, wrapped by a handler that injects the given user into the request
// context (simulating the auth middleware).
func rbacTestServer(t *testing.T, user auth.User, roles []authz.Role, bindings []authz.Binding) http.Handler {
	t.Helper()
	srv := New(Clients{Database: &fakeRBACDatabase{}, Scheduler: &fakeScheduler{}}, nil)
	srv.SetAuthz(authz.New(&testRoleSource{roles: roles, bindings: bindings}), true)
	handler := srv.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(auth.WithUser(r.Context(), user))
		handler.ServeHTTP(w, r)
	})
}

func rbacDoRequest(t *testing.T, handler http.Handler, method, path, body string) int {
	t.Helper()
	// Always provide a non-nil (possibly empty) body so handlers that decode
	// the request body do not see a nil r.Body.
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

func TestRBACViewerCannotMutate(t *testing.T) {
	handler := rbacTestServer(t, auth.User{Subject: "viewer-1", Roles: []string{authz.RoleViewer}}, builtinTestRoles(), nil)

	// A viewer can list pipelines (pipelines.can-view).
	if code := rbacDoRequest(t, handler, http.MethodGet, "/api/pipelines", ""); code != http.StatusOK {
		t.Errorf("viewer GET /api/pipelines = %d, want 200", code)
	}
	// A viewer cannot create a pipeline (pipelines.can-create).
	if code := rbacDoRequest(t, handler, http.MethodPost, "/api/pipelines", `{"name":"x"}`); code != http.StatusForbidden {
		t.Errorf("viewer POST /api/pipelines = %d, want 403", code)
	}
	// A viewer cannot trigger a run (runs.can-trigger).
	if code := rbacDoRequest(t, handler, http.MethodPost, "/api/pipelines/1/runs", ""); code != http.StatusForbidden {
		t.Errorf("viewer POST /api/pipelines/1/runs = %d, want 403", code)
	}
	// A viewer cannot approve a job (jobs.can-approve).
	if code := rbacDoRequest(t, handler, http.MethodPost, "/api/jobs/1/approve", ""); code != http.StatusForbidden {
		t.Errorf("viewer POST /api/jobs/1/approve = %d, want 403", code)
	}
	// A viewer cannot manage roles (roles.can-manage).
	if code := rbacDoRequest(t, handler, http.MethodGet, "/api/roles", ""); code != http.StatusForbidden {
		t.Errorf("viewer GET /api/roles = %d, want 403", code)
	}
}

func TestRBACOperatorCanRun(t *testing.T) {
	handler := rbacTestServer(t, auth.User{Subject: "op-1", Roles: []string{authz.RoleOperator}}, builtinTestRoles(), nil)

	// An operator can trigger a run (runs.can-trigger).
	if code := rbacDoRequest(t, handler, http.MethodPost, "/api/pipelines/1/runs", ""); code != http.StatusCreated {
		t.Errorf("operator POST /api/pipelines/1/runs = %d, want 201", code)
	}
	// An operator can approve a job (jobs.can-approve).
	if code := rbacDoRequest(t, handler, http.MethodPost, "/api/jobs/1/approve", ""); code != http.StatusOK {
		t.Errorf("operator POST /api/jobs/1/approve = %d, want 200", code)
	}
	// An operator cannot edit a pipeline (pipelines.can-edit).
	if code := rbacDoRequest(t, handler, http.MethodPut, "/api/pipelines/1", `{"name":"x"}`); code != http.StatusForbidden {
		t.Errorf("operator PUT /api/pipelines/1 = %d, want 403", code)
	}
	// An operator cannot manage roles (roles.can-manage).
	if code := rbacDoRequest(t, handler, http.MethodGet, "/api/roles", ""); code != http.StatusForbidden {
		t.Errorf("operator GET /api/roles = %d, want 403", code)
	}
}

func TestRBACAdminCanDoEverything(t *testing.T) {
	handler := rbacTestServer(t, auth.User{Subject: "admin-1", Roles: []string{authz.RoleAdmin}}, builtinTestRoles(), nil)

	// An admin can create a pipeline.
	if code := rbacDoRequest(t, handler, http.MethodPost, "/api/pipelines", `{"name":"x"}`); code != http.StatusCreated {
		t.Errorf("admin POST /api/pipelines = %d, want 201", code)
	}
	// An admin can manage roles.
	if code := rbacDoRequest(t, handler, http.MethodGet, "/api/roles", ""); code != http.StatusOK {
		t.Errorf("admin GET /api/roles = %d, want 200", code)
	}
	// An admin can manage role bindings.
	if code := rbacDoRequest(t, handler, http.MethodGet, "/api/role-bindings", ""); code != http.StatusOK {
		t.Errorf("admin GET /api/role-bindings = %d, want 200", code)
	}
	// An admin can view their permissions.
	if code := rbacDoRequest(t, handler, http.MethodGet, "/api/me/permissions", ""); code != http.StatusOK {
		t.Errorf("admin GET /api/me/permissions = %d, want 200", code)
	}
}

func TestRBACDenyByDefault(t *testing.T) {
	// A principal with no roles can do nothing.
	handler := rbacTestServer(t, auth.User{Subject: "nobody"}, builtinTestRoles(), nil)
	if code := rbacDoRequest(t, handler, http.MethodGet, "/api/pipelines", ""); code != http.StatusForbidden {
		t.Errorf("nobody GET /api/pipelines = %d, want 403 (deny-by-default)", code)
	}
}

func TestRBACPipelineScopedBinding(t *testing.T) {
	// A principal with operator scoped to pipeline 10 can trigger on 10 but not 20.
	handler := rbacTestServer(t, auth.User{Subject: "scoped-1"}, builtinTestRoles(), []authz.Binding{
		{PrincipalKind: "user", PrincipalID: "scoped-1", RoleName: authz.RoleOperator, PipelineID: 10},
	})
	if code := rbacDoRequest(t, handler, http.MethodPost, "/api/pipelines/10/runs", ""); code != http.StatusCreated {
		t.Errorf("scoped operator POST /api/pipelines/10/runs = %d, want 201", code)
	}
	if code := rbacDoRequest(t, handler, http.MethodPost, "/api/pipelines/20/runs", ""); code != http.StatusForbidden {
		t.Errorf("scoped operator POST /api/pipelines/20/runs = %d, want 403", code)
	}
}

func TestRBACDisabledActsAsAdmin(t *testing.T) {
	// When RBAC is not enabled (authentication disabled), requests act as a
	// synthetic admin: everything is allowed.
	srv := New(Clients{Database: &fakeRBACDatabase{}, Scheduler: &fakeScheduler{}}, nil)
	// No SetAuthz call: rbacEnabled is false.
	handler := srv.Handler()
	req := httptest.NewRequest(http.MethodGet, "/api/pipelines", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("RBAC disabled GET /api/pipelines = %d, want 200 (synthetic admin)", rec.Code)
	}
}
