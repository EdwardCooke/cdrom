package database

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TestSeedBuiltInRoles verifies that seeding inserts the built-in roles and is
// idempotent (a second call does not duplicate or overwrite them).
func TestSeedBuiltInRoles(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	dbPath := t.TempDir() + "/seed.db"
	db, err := Open(Config{Backend: BackendSQLite, SQLitePath: dbPath}, logger)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer Close(db)
	if err := Migrate(db, logger); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	s := NewServer(db)
	ctx := context.Background()

	if err := s.SeedBuiltInRoles(ctx); err != nil {
		t.Fatalf("SeedBuiltInRoles: %v", err)
	}
	// All four built-in roles exist and are marked built-in.
	for _, name := range []string{"admin", "operator", "viewer", "user"} {
		role, err := s.GetRole(ctx, &dbpb.GetRoleRequest{Name: name})
		if err != nil {
			t.Fatalf("GetRole(%s): %v", name, err)
		}
		if !role.GetBuiltIn() {
			t.Errorf("role %s: built_in = false, want true", name)
		}
	}
	// Idempotent: seeding again does not error or duplicate.
	if err := s.SeedBuiltInRoles(ctx); err != nil {
		t.Fatalf("second SeedBuiltInRoles: %v", err)
	}
	resp, err := s.ListRoles(ctx, &dbpb.ListRolesRequest{})
	if err != nil {
		t.Fatalf("ListRoles: %v", err)
	}
	if got := len(resp.GetRoles()); got != 4 {
		t.Errorf("ListRoles after re-seed = %d roles, want 4 (no duplicates)", got)
	}
}

func TestRoleCRUD(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// Create a custom role.
	role, err := client.CreateRole(ctx, &dbpb.CreateRoleRequest{
		Name:        "pipeline-owner",
		Description: "owns a pipeline",
		Permissions: []string{"pipelines.can-view", "pipelines.can-edit", "runs.can-trigger"},
	})
	if err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if role.GetBuiltIn() {
		t.Errorf("custom role built_in = true, want false")
	}

	// A custom role cannot take a built-in name.
	if _, err := client.CreateRole(ctx, &dbpb.CreateRoleRequest{Name: "admin"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateRole(admin) code = %v, want InvalidArgument", status.Code(err))
	}
	// A custom role cannot take an existing name.
	if _, err := client.CreateRole(ctx, &dbpb.CreateRoleRequest{Name: "pipeline-owner"}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateRole(dup) code = %v, want AlreadyExists", status.Code(err))
	}
	// An unknown permission is rejected.
	if _, err := client.CreateRole(ctx, &dbpb.CreateRoleRequest{Name: "bad", Permissions: []string{"nope.can-do"}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateRole(unknown perm) code = %v, want InvalidArgument", status.Code(err))
	}

	// Get the role back.
	got, err := client.GetRole(ctx, &dbpb.GetRoleRequest{Name: "pipeline-owner"})
	if err != nil {
		t.Fatalf("GetRole: %v", err)
	}
	if len(got.GetPermissions()) != 3 {
		t.Errorf("GetRole permissions = %d, want 3", len(got.GetPermissions()))
	}

	// Update the custom role's permission set.
	updated, err := client.UpdateRole(ctx, &dbpb.UpdateRoleRequest{
		Name:        "pipeline-owner",
		Permissions: []string{"pipelines.can-view", "runs.can-trigger", "runs.can-cancel"},
	})
	if err != nil {
		t.Fatalf("UpdateRole: %v", err)
	}
	if len(updated.GetPermissions()) != 3 {
		t.Errorf("UpdateRole permissions = %d, want 3", len(updated.GetPermissions()))
	}

	// A built-in role cannot be edited.
	if _, err := client.UpdateRole(ctx, &dbpb.UpdateRoleRequest{Name: "admin", Permissions: []string{"pipelines.can-view"}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("UpdateRole(admin) code = %v, want InvalidArgument", status.Code(err))
	}
	// A built-in role cannot be deleted.
	if _, err := client.DeleteRole(ctx, &dbpb.DeleteRoleRequest{Name: "admin"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("DeleteRole(admin) code = %v, want InvalidArgument", status.Code(err))
	}

	// Delete the custom role.
	if _, err := client.DeleteRole(ctx, &dbpb.DeleteRoleRequest{Name: "pipeline-owner"}); err != nil {
		t.Fatalf("DeleteRole: %v", err)
	}
	if _, err := client.GetRole(ctx, &dbpb.GetRoleRequest{Name: "pipeline-owner"}); status.Code(err) != codes.NotFound {
		t.Errorf("GetRole(deleted) code = %v, want NotFound", status.Code(err))
	}
}

func TestRoleBindingCRUD(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// Add an unscoped binding.
	binding, err := client.AddRoleBinding(ctx, &dbpb.AddRoleBindingRequest{
		PrincipalKind: "user",
		PrincipalId:   "42",
		RoleName:      "operator",
	})
	if err != nil {
		t.Fatalf("AddRoleBinding: %v", err)
	}
	if binding.GetPipelineId() != 0 {
		t.Errorf("unscoped binding pipeline_id = %d, want 0", binding.GetPipelineId())
	}

	// Idempotent: adding the same binding again returns the existing one.
	again, err := client.AddRoleBinding(ctx, &dbpb.AddRoleBindingRequest{
		PrincipalKind: "user",
		PrincipalId:   "42",
		RoleName:      "operator",
	})
	if err != nil {
		t.Fatalf("AddRoleBinding (idempotent): %v", err)
	}
	if again.GetId() != binding.GetId() {
		t.Errorf("idempotent binding id = %d, want %d", again.GetId(), binding.GetId())
	}

	// Add a pipeline-scoped binding.
	scoped, err := client.AddRoleBinding(ctx, &dbpb.AddRoleBindingRequest{
		PrincipalKind: "user",
		PrincipalId:   "42",
		RoleName:      "operator",
		PipelineId:    7,
	})
	if err != nil {
		t.Fatalf("AddRoleBinding (scoped): %v", err)
	}
	if scoped.GetPipelineId() != 7 {
		t.Errorf("scoped binding pipeline_id = %d, want 7", scoped.GetPipelineId())
	}

	// List bindings for the principal (both unscoped and scoped).
	resp, err := client.ListRoleBindings(ctx, &dbpb.ListRoleBindingsRequest{PrincipalId: "42"})
	if err != nil {
		t.Fatalf("ListRoleBindings: %v", err)
	}
	if got := len(resp.GetBindings()); got != 2 {
		t.Errorf("ListRoleBindings = %d, want 2", got)
	}

	// Filter by role.
	byRole, err := client.ListRoleBindings(ctx, &dbpb.ListRoleBindingsRequest{RoleName: "operator"})
	if err != nil {
		t.Fatalf("ListRoleBindings (role): %v", err)
	}
	if got := len(byRole.GetBindings()); got != 2 {
		t.Errorf("ListRoleBindings(role) = %d, want 2", got)
	}

	// Delete the scoped binding.
	if _, err := client.DeleteRoleBinding(ctx, &dbpb.DeleteRoleBindingRequest{Id: scoped.GetId()}); err != nil {
		t.Fatalf("DeleteRoleBinding: %v", err)
	}
	after, err := client.ListRoleBindings(ctx, &dbpb.ListRoleBindingsRequest{PrincipalId: "42"})
	if err != nil {
		t.Fatalf("ListRoleBindings (after delete): %v", err)
	}
	if got := len(after.GetBindings()); got != 1 {
		t.Errorf("ListRoleBindings after delete = %d, want 1", got)
	}

	// Deleting a missing binding is NotFound.
	if _, err := client.DeleteRoleBinding(ctx, &dbpb.DeleteRoleBindingRequest{Id: 999999}); status.Code(err) != codes.NotFound {
		t.Errorf("DeleteRoleBinding(missing) code = %v, want NotFound", status.Code(err))
	}
}

func TestPublishRoleChange(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	resp, err := client.PublishRoleChange(ctx, &dbpb.PublishRoleChangeRequest{})
	if err != nil {
		t.Fatalf("PublishRoleChange: %v", err)
	}
	if resp.GetId() == 0 {
		t.Errorf("PublishRoleChange id = 0, want non-zero")
	}
}
