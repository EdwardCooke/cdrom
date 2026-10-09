package authz

import (
	"context"
	"testing"
)

// fakeSource is a RoleSource backed by in-memory roles and bindings.
type fakeSource struct {
	roles    []Role
	bindings []Binding
}

func (f *fakeSource) ListRoles(context.Context) ([]Role, error) {
	return f.roles, nil
}

func (f *fakeSource) ListBindings(context.Context) ([]Binding, error) {
	return f.bindings, nil
}

// builtinCatalog returns a RoleSource with the built-in roles and no bindings.
func builtinCatalog() *fakeSource {
	roles := make([]Role, 0, len(BuiltinRoles()))
	for _, def := range BuiltinRoles() {
		roles = append(roles, Role{
			Name:        def.Name,
			Description: def.Description,
			BuiltIn:     def.BuiltIn,
			Permissions: def.Permissions,
			Includes:    def.Includes,
		})
	}
	return &fakeSource{roles: roles}
}

func user(id string, roles ...string) Principal {
	return Principal{Kind: "user", ID: id, TokenRoles: roles}
}

func TestCheckBuiltInRoles(t *testing.T) {
	ctx := context.Background()
	e := New(builtinCatalog())

	// An admin can do everything.
	for _, perm := range AllPermissions {
		if ok, _ := e.Check(ctx, user("a", RoleAdmin), perm, Resource{}); !ok {
			t.Errorf("admin: %s = false, want true", perm)
		}
	}

	// A viewer can read but not trigger/cancel/approve/edit.
	if ok, _ := e.Check(ctx, user("v", RoleViewer), PermPipelinesView, Resource{}); !ok {
		t.Errorf("viewer: pipelines.can-view = false, want true")
	}
	for _, perm := range []string{PermRunsTrigger, PermRunsCancel, PermJobsApprove, PermPipelinesEdit, PermSecretsManage} {
		if ok, _ := e.Check(ctx, user("v", RoleViewer), perm, Resource{}); ok {
			t.Errorf("viewer: %s = true, want false", perm)
		}
	}

	// An operator can trigger/cancel/approve but not edit or manage secrets.
	if ok, _ := e.Check(ctx, user("o", RoleOperator), PermRunsTrigger, Resource{}); !ok {
		t.Errorf("operator: runs.can-trigger = false, want true")
	}
	if ok, _ := e.Check(ctx, user("o", RoleOperator), PermJobsApprove, Resource{}); !ok {
		t.Errorf("operator: jobs.can-approve = false, want true")
	}
	for _, perm := range []string{PermPipelinesEdit, PermSecretsManage, PermRolesManage} {
		if ok, _ := e.Check(ctx, user("o", RoleOperator), perm, Resource{}); ok {
			t.Errorf("operator: %s = true, want false", perm)
		}
	}

	// A principal with no roles can do nothing (deny-by-default).
	for _, perm := range AllPermissions {
		if ok, _ := e.Check(ctx, user("nobody"), perm, Resource{}); ok {
			t.Errorf("nobody: %s = true, want false (deny-by-default)", perm)
		}
	}
}

func TestCheckPipelineScoping(t *testing.T) {
	ctx := context.Background()
	src := builtinCatalog()
	// Bind "operator" to user "sc" scoped to pipeline 10 only.
	src.bindings = []Binding{
		{PrincipalKind: "user", PrincipalID: "sc", RoleName: RoleOperator, PipelineID: 10},
	}
	e := New(src)

	// A scoped operator can trigger on pipeline 10 but not pipeline 20.
	if ok, _ := e.Check(ctx, user("sc"), PermRunsTrigger, Resource{PipelineID: 10}); !ok {
		t.Errorf("scoped operator: trigger on pipeline 10 = false, want true")
	}
	if ok, _ := e.Check(ctx, user("sc"), PermRunsTrigger, Resource{PipelineID: 20}); ok {
		t.Errorf("scoped operator: trigger on pipeline 20 = true, want false")
	}
	// A scoped binding never grants platform-wide permissions.
	if ok, _ := e.Check(ctx, user("sc"), PermRolesManage, Resource{}); ok {
		t.Errorf("scoped operator: roles.can-manage = true, want false (platform-wide never granted by a scoped binding)")
	}
	// A scoped operator can still view (a resource-scoped permission) on any
	// pipeline it is scoped to.
	if ok, _ := e.Check(ctx, user("sc"), PermPipelinesView, Resource{PipelineID: 10}); !ok {
		t.Errorf("scoped operator: view on pipeline 10 = false, want true")
	}
}

func TestCheckUnscopedCoversAllPipelines(t *testing.T) {
	ctx := context.Background()
	src := builtinCatalog()
	// An unscoped (platform-wide) operator binding.
	src.bindings = []Binding{
		{PrincipalKind: "user", PrincipalID: "uw", RoleName: RoleOperator},
	}
	e := New(src)
	// An unscoped grant covers every pipeline.
	for _, pid := range []int64{1, 2, 100} {
		if ok, _ := e.Check(ctx, user("uw"), PermRunsTrigger, Resource{PipelineID: pid}); !ok {
			t.Errorf("unscoped operator: trigger on pipeline %d = false, want true", pid)
		}
	}
}

func TestRoleComposition(t *testing.T) {
	ctx := context.Background()
	src := builtinCatalog()
	// A custom role that includes "operator" and adds secrets.can-manage.
	src.roles = append(src.roles, Role{
		Name:        "senior-operator",
		Permissions: []string{PermSecretsManage},
		Includes:    []string{RoleOperator},
	})
	e := New(src)

	// The composed role inherits operator's permissions plus its own.
	if ok, _ := e.Check(ctx, user("s", "senior-operator"), PermRunsTrigger, Resource{}); !ok {
		t.Errorf("senior-operator: runs.can-trigger (inherited) = false, want true")
	}
	if ok, _ := e.Check(ctx, user("s", "senior-operator"), PermSecretsManage, Resource{}); !ok {
		t.Errorf("senior-operator: secrets.can-manage (own) = false, want true")
	}
	// It does not gain permissions neither it nor operator have.
	if ok, _ := e.Check(ctx, user("s", "senior-operator"), PermRolesManage, Resource{}); ok {
		t.Errorf("senior-operator: roles.can-manage = true, want false")
	}
}

func TestRoleCompositionTransitive(t *testing.T) {
	ctx := context.Background()
	src := builtinCatalog()
	src.roles = append(src.roles,
		Role{Name: "a", Permissions: []string{PermSecretsManage}},
		Role{Name: "b", Includes: []string{"a"}},
		Role{Name: "c", Includes: []string{"b"}},
	)
	e := New(src)
	// "c" includes "b" which includes "a": c inherits a's permission.
	if ok, _ := e.Check(ctx, user("x", "c"), PermSecretsManage, Resource{}); !ok {
		t.Errorf("transitive composition: c -> b -> a secrets.can-manage = false, want true")
	}
}

func TestRoleCompositionCycle(t *testing.T) {
	ctx := context.Background()
	src := builtinCatalog()
	// A cycle must not cause infinite recursion.
	src.roles = append(src.roles,
		Role{Name: "x", Permissions: []string{PermSecretsManage}, Includes: []string{"y"}},
		Role{Name: "y", Includes: []string{"x"}},
	)
	e := New(src)
	if ok, _ := e.Check(ctx, user("x", "x"), PermSecretsManage, Resource{}); !ok {
		t.Errorf("cycle: x secrets.can-manage = false, want true")
	}
}

func TestPermissionsFor(t *testing.T) {
	ctx := context.Background()
	src := builtinCatalog()
	src.bindings = []Binding{
		// Unscoped viewer (platform-wide view).
		{PrincipalKind: "user", PrincipalID: "p", RoleName: RoleViewer},
		// Scoped operator on pipeline 5.
		{PrincipalKind: "user", PrincipalID: "p", RoleName: RoleOperator, PipelineID: 5},
	}
	e := New(src)
	set, err := e.PermissionsFor(ctx, user("p"))
	if err != nil {
		t.Fatalf("PermissionsFor: %v", err)
	}
	// Platform-wide: viewer's permissions (unscoped).
	if !set.PlatformWide[PermPipelinesView] {
		t.Errorf("platform-wide: pipelines.can-view missing")
	}
	// Scoped: operator's resource-scoped permissions on pipeline 5.
	if !set.Scoped[5][PermRunsTrigger] {
		t.Errorf("scoped[5]: runs.can-trigger missing")
	}
	// A platform-wide permission is not granted by a scoped binding.
	if _, ok := set.Scoped[5][PermRolesManage]; ok {
		t.Errorf("scoped[5]: roles.can-manage present, want absent")
	}
}

func TestCanGrantDelegation(t *testing.T) {
	ctx := context.Background()
	src := builtinCatalog()
	e := New(src)

	// An admin can grant the admin role (it holds it, unscoped), at any scope.
	if ok, _ := e.CanGrant(ctx, user("a", RoleAdmin), RoleAdmin, 0); !ok {
		t.Errorf("admin granting admin = false, want true")
	}
	if ok, _ := e.CanGrant(ctx, user("a", RoleAdmin), RoleAdmin, 10); !ok {
		t.Errorf("admin granting scoped admin = false, want true")
	}
	// A principal can grant a role only if it holds that role: the admin role
	// does not hold operator, so it cannot grant operator (even though it has
	// roles.can-assign).
	if ok, _ := e.CanGrant(ctx, user("a", RoleAdmin), RoleOperator, 0); ok {
		t.Errorf("admin granting operator = true, want false (does not hold operator)")
	}

	// A non-admin cannot grant (no roles.can-assign).
	if ok, _ := e.CanGrant(ctx, user("v", RoleViewer), RoleViewer, 0); ok {
		t.Errorf("viewer granting viewer = true, want false (no roles.can-assign)")
	}

	// A principal with roles.can-assign but not the role being granted cannot
	// grant it (e.g. an admin-less "assigner" cannot grant admin).
	srcAssigner := builtinCatalog()
	srcAssigner.roles = append(srcAssigner.roles, Role{Name: "assigner", Permissions: []string{PermRolesAssign}})
	eAssigner := New(srcAssigner)
	if ok, _ := eAssigner.CanGrant(ctx, user("x", "assigner"), RoleAdmin, 0); ok {
		t.Errorf("assigner granting admin = true, want false (does not hold admin)")
	}

	// A principal that directly holds both roles.can-assign and the role can
	// grant it. (Delegation counts only directly-held roles — a role reached
	// by composition does not let a principal delegate it.)
	srcGrant := builtinCatalog()
	srcGrant.roles = append(srcGrant.roles, Role{Name: "grant-operator", Permissions: []string{PermRolesAssign}})
	eGrant := New(srcGrant)
	if ok, _ := eGrant.CanGrant(ctx, user("g", "grant-operator", RoleOperator), RoleOperator, 0); !ok {
		t.Errorf("grant-operator+operator granting operator = false, want true")
	}
	// A principal that only *includes* operator (does not directly hold it)
	// cannot grant it, even with roles.can-assign.
	if ok, _ := eGrant.CanGrant(ctx, user("g", "grant-operator"), RoleOperator, 0); ok {
		t.Errorf("grant-operator (no direct operator) granting operator = true, want false")
	}

	// A pipeline-scoped operator cannot grant platform-wide operator.
	src2 := builtinCatalog()
	src2.bindings = []Binding{
		{PrincipalKind: "user", PrincipalID: "so", RoleName: RoleOperator, PipelineID: 10},
	}
	// The scoped operator also needs roles.can-assign to even attempt a grant;
	// give it via a custom role so we can test the scope rule in isolation.
	src2.roles = append(src2.roles, Role{Name: "scoped-assigner", Permissions: []string{PermRolesAssign, PermRunsTrigger}})
	src2.bindings = append(src2.bindings, Binding{PrincipalKind: "user", PrincipalID: "so", RoleName: "scoped-assigner"})
	e2 := New(src2)
	// Can grant scoped operator on the same pipeline (holds operator scoped to 10).
	if ok, _ := e2.CanGrant(ctx, user("so"), RoleOperator, 10); !ok {
		t.Errorf("scoped operator granting scoped operator (same pipeline) = false, want true")
	}
	// Cannot grant platform-wide operator (does not hold it unscoped).
	if ok, _ := e2.CanGrant(ctx, user("so"), RoleOperator, 0); ok {
		t.Errorf("scoped operator granting platform-wide operator = true, want false")
	}
	// Cannot grant operator on a different pipeline.
	if ok, _ := e2.CanGrant(ctx, user("so"), RoleOperator, 20); ok {
		t.Errorf("scoped operator granting operator on another pipeline = true, want false")
	}
}

func TestInvalidate(t *testing.T) {
	ctx := context.Background()
	src := builtinCatalog()
	e := New(src)
	// Prime the cache.
	if ok, _ := e.Check(ctx, user("a", RoleAdmin), PermRunsTrigger, Resource{}); !ok {
		t.Fatal("expected admin to be allowed before invalidation")
	}
	// Mutate the source (simulate a role change) and invalidate.
	src.roles = nil // remove all roles
	e.Invalidate()
	// After invalidation, the admin no longer has any roles (deny-by-default).
	if ok, _ := e.Check(ctx, user("a", RoleAdmin), PermRunsTrigger, Resource{}); ok {
		t.Errorf("after invalidation: admin trigger = true, want false (cache was invalidated)")
	}
}
