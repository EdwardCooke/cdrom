package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"cdrom/internal/audit"
	"cdrom/internal/auth"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// dbRoleSource is the API's implementation of authz.RoleSource: it reads the
// role catalog (roles and bindings) from the Database service, which owns the
// rows so every API replica sees the same catalog. The authorization engine
// (internal/authz) caches the catalog locally and invalidates it on
// role_change events (F-23), so this source is consulted only on a cache miss.
type dbRoleSource struct {
	db dbpb.DatabaseClient
}

// NewDBRoleSource returns an authz.RoleSource that reads roles and bindings
// from the Database service.
func NewDBRoleSource(db dbpb.DatabaseClient) authz.RoleSource {
	return &dbRoleSource{db: db}
}

func (s *dbRoleSource) ListRoles(ctx context.Context) ([]authz.Role, error) {
	resp, err := s.db.ListRoles(ctx, &dbpb.ListRolesRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]authz.Role, 0, len(resp.GetRoles()))
	for _, r := range resp.GetRoles() {
		out = append(out, authz.Role{
			Name:        r.GetName(),
			Description: r.GetDescription(),
			BuiltIn:     r.GetBuiltIn(),
			Permissions: r.GetPermissions(),
			Includes:    r.GetIncludes(),
		})
	}
	return out, nil
}

func (s *dbRoleSource) ListBindings(ctx context.Context) ([]authz.Binding, error) {
	resp, err := s.db.ListRoleBindings(ctx, &dbpb.ListRoleBindingsRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]authz.Binding, 0, len(resp.GetBindings()))
	for _, b := range resp.GetBindings() {
		out = append(out, authz.Binding{
			PrincipalKind: b.GetPrincipalKind(),
			PrincipalID:   b.GetPrincipalId(),
			RoleName:      b.GetRoleName(),
			PipelineID:    b.GetPipelineId(),
		})
	}
	return out, nil
}

// principalFromContext returns the principal to authorize: the authenticated
// user (its kind is "user", or "service-account" when the principal
// authenticated with a service-account key (F-26), and its id is the token's
// subject) when authentication is enabled, or a synthetic admin principal
// when it is not (a local run with authentication disabled acts as an admin,
// so the API stays open). The principal's kind and id are how the engine looks
// up the principal's stored role bindings, so a service-account principal is
// authorized by its own role bindings, not a human user's.
func (s *Server) principalFromContext(ctx context.Context) authz.Principal {
	if s.rbacEnabled {
		user := auth.UserFromContext(ctx)
		kind := user.Kind
		if kind == "" {
			kind = "user"
		}
		return authz.Principal{Kind: kind, ID: user.Subject, TokenRoles: user.Roles}
	}
	// Authentication disabled: act as a synthetic admin so the API stays open.
	return authz.Principal{Kind: "user", ID: "anonymous", TokenRoles: []string{authz.RoleAdmin}}
}

// requirePermission enforces role-based access control (F-14) on a request:
// it checks that the caller's principal is allowed to perform permission on
// the resource, and writes a 403 (or 500 on an engine error) and returns
// false when the caller is not allowed. When RBAC is not enabled (authentication
// disabled) it always allows (the caller acts as a synthetic admin).
//
// When the caller authenticated with an API key (F-25) that carries a
// pipeline scope, the caller's resource-scoped permissions are additionally
// limited to that scope: a request for a pipeline outside the key's scope is
// denied even if the owner's roles would otherwise allow it. A key with an
// empty scope is not limited.
func (s *Server) requirePermission(w http.ResponseWriter, r *http.Request, permission string, res authz.Resource) bool {
	if !s.rbacEnabled || s.authz == nil {
		return true
	}
	// API-key pipeline scope (F-25): a key scoped to a set of pipelines limits
	// the caller's resource-scoped permissions to those pipelines. A
	// platform-wide permission (or a request for a non-pipeline resource) is
	// unaffected by the scope.
	if user := auth.UserFromContext(r.Context()); len(user.PipelineScope) > 0 && res.PipelineID != 0 {
		if !pipelineInScope(user.PipelineScope, uint(res.PipelineID)) {
			httpError(w, http.StatusForbidden, "permission denied: %s (outside the api key's pipeline scope)", permission)
			return false
		}
	}
	principal := s.principalFromContext(r.Context())
	ok, err := s.authz.Check(r.Context(), principal, permission, res)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "authorization: %v", err)
		return false
	}
	if !ok {
		httpError(w, http.StatusForbidden, "permission denied: %s", permission)
		return false
	}
	return true
}

// pipelineInScope reports whether pipeline is in the key's pipeline scope.
func pipelineInScope(scope []uint, pipeline uint) bool {
	for _, p := range scope {
		if p == pipeline {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Roles (F-14, RBAC)
// ---------------------------------------------------------------------------

// roleRequest is the JSON form of a role create/update: a name, description,
// a permission set, and optional included roles (composition).
type roleRequest struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Permissions []string `json:"permissions,omitempty"`
	Includes    []string `json:"includes,omitempty"`
}

// listRoles lists every role (built-in and custom). It requires
// roles.can-manage (viewing the role catalog is part of managing roles).
func (s *Server) listRoles(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermRolesManage, authz.Resource{}) {
		return
	}
	resp, err := s.clients.Database.ListRoles(r.Context(), &dbpb.ListRolesRequest{})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(resp.GetRoles()))
}

// getRole fetches a single role by name. It requires roles.can-manage.
func (s *Server) getRole(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermRolesManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "role name is required")
		return
	}
	role, err := s.clients.Database.GetRole(r.Context(), &dbpb.GetRoleRequest{Name: name})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, role)
}

// createRole creates a custom role. It requires roles.can-manage.
func (s *Server) createRole(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermRolesManage, authz.Resource{}) {
		return
	}
	var req roleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Name == "" {
		httpError(w, http.StatusBadRequest, "role name is required")
		return
	}
	role, err := s.clients.Database.CreateRole(r.Context(), &dbpb.CreateRoleRequest{
		Name:        req.Name,
		Description: req.Description,
		Permissions: req.Permissions,
		Includes:    req.Includes,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionRoleCreate,
		TargetKind: audit.TargetRole,
		TargetID:   role.GetName(),
		TargetName: role.GetName(),
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditJSON(map[string]any{"description": role.GetDescription(), "permissions": role.GetPermissions(), "includes": role.GetIncludes()}),
	})
	s.publishRoleChange()
	writeJSON(w, http.StatusCreated, role)
}

// updateRole edits a custom role. It requires roles.can-manage. Built-in
// roles are rejected by the database service.
func (s *Server) updateRole(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermRolesManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "role name is required")
		return
	}
	var req roleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	// Fetch the role's current state so the audit event can record what
	// changed (old value vs new value, F-15). A failure to fetch it does not
	// block the update.
	var oldRole *dbpb.Role
	if old, err := s.clients.Database.GetRole(r.Context(), &dbpb.GetRoleRequest{Name: name}); err == nil {
		oldRole = old
	}
	role, err := s.clients.Database.UpdateRole(r.Context(), &dbpb.UpdateRoleRequest{
		Name:        name,
		Description: req.Description,
		Permissions: req.Permissions,
		Includes:    req.Includes,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionRoleUpdate,
		TargetKind: audit.TargetRole,
		TargetID:   role.GetName(),
		TargetName: role.GetName(),
		Outcome:    audit.OutcomeSuccess,
		OldValue:   auditRoleValue(oldRole),
		NewValue:   auditRoleValue(role),
	})
	s.publishRoleChange()
	writeJSON(w, http.StatusOK, role)
}

// deleteRole removes a custom role. It requires roles.can-manage. Built-in
// roles are rejected by the database service.
func (s *Server) deleteRole(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermRolesManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "role name is required")
		return
	}
	// Fetch the role's current state so the audit event can record what was
	// removed (F-15). A failure to fetch it does not block the delete.
	var oldRole *dbpb.Role
	if old, err := s.clients.Database.GetRole(r.Context(), &dbpb.GetRoleRequest{Name: name}); err == nil {
		oldRole = old
	}
	if _, err := s.clients.Database.DeleteRole(r.Context(), &dbpb.DeleteRoleRequest{Name: name}); err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionRoleDelete,
		TargetKind: audit.TargetRole,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
		OldValue:   auditRoleValue(oldRole),
	})
	s.publishRoleChange()
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// ---------------------------------------------------------------------------
// Role bindings (F-14, RBAC)
// ---------------------------------------------------------------------------

// bindingRequest is the JSON form of a role binding: the principal (kind and
// id), the role to bind, and an optional pipeline scope (0 means unscoped /
// platform-wide).
type bindingRequest struct {
	PrincipalKind string `json:"principal_kind"`
	PrincipalID   string `json:"principal_id"`
	RoleName      string `json:"role_name"`
	PipelineID    int64  `json:"pipeline_id,omitempty"`
}

// listBindings lists role bindings, optionally filtered by principal and/or
// role (query params principal_kind, principal_id, role_name). It requires
// roles.can-assign.
func (s *Server) listBindings(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermRolesAssign, authz.Resource{}) {
		return
	}
	query := r.URL.Query()
	resp, err := s.clients.Database.ListRoleBindings(r.Context(), &dbpb.ListRoleBindingsRequest{
		PrincipalKind: query.Get("principal_kind"),
		PrincipalId:   query.Get("principal_id"),
		RoleName:      query.Get("role_name"),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(resp.GetBindings()))
}

// addBinding attaches a role to a principal, optionally scoped to a pipeline.
// It requires roles.can-assign, and the delegation rule: the caller may bind
// a role only if the caller already holds that role (unscoped, or with a
// scope at least as wide as the one being granted). This is what keeps an
// operator from promoting someone to admin.
func (s *Server) addBinding(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermRolesAssign, authz.Resource{}) {
		return
	}
	var req bindingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.RoleName == "" {
		httpError(w, http.StatusBadRequest, "role_name is required")
		return
	}
	// Delegation rule: the caller must hold the role being granted, with a
	// scope at least as wide as the one being granted.
	if s.rbacEnabled && s.authz != nil {
		caller := s.principalFromContext(r.Context())
		ok, err := s.authz.CanGrant(r.Context(), caller, req.RoleName, req.PipelineID)
		if err != nil {
			httpError(w, http.StatusInternalServerError, "authorization: %v", err)
			return
		}
		if !ok {
			httpError(w, http.StatusForbidden, "cannot grant role %q: caller does not hold it with sufficient scope", req.RoleName)
			return
		}
	}
	kind := req.PrincipalKind
	if kind == "" {
		kind = "user"
	}
	binding, err := s.clients.Database.AddRoleBinding(r.Context(), &dbpb.AddRoleBindingRequest{
		PrincipalKind: kind,
		PrincipalId:   req.PrincipalID,
		RoleName:      req.RoleName,
		PipelineId:    req.PipelineID,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionBindingAdd,
		TargetKind: audit.TargetRoleBinding,
		TargetID:   strconv.FormatInt(binding.GetId(), 10),
		TargetName: binding.GetRoleName(),
		PipelineID: binding.GetPipelineId(),
		Outcome:    audit.OutcomeSuccess,
		NewValue: auditJSON(map[string]any{
			"principal_kind": binding.GetPrincipalKind(),
			"principal_id":   binding.GetPrincipalId(),
			"role_name":      binding.GetRoleName(),
			"pipeline_id":    binding.GetPipelineId(),
		}),
	})
	s.publishRoleChange()
	writeJSON(w, http.StatusCreated, binding)
}

// deleteBinding removes a role binding by id. It requires roles.can-assign.
func (s *Server) deleteBinding(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermRolesAssign, authz.Resource{}) {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid binding id %q", r.PathValue("id"))
		return
	}
	// Fetch the binding's current state so the audit event can record what
	// was removed (F-15). A failure to fetch it does not block the delete.
	var oldBinding *dbpb.RoleBinding
	if resp, err := s.clients.Database.ListRoleBindings(r.Context(), &dbpb.ListRoleBindingsRequest{}); err == nil {
		for _, b := range resp.GetBindings() {
			if b.GetId() == id {
				oldBinding = b
				break
			}
		}
	}
	if _, err := s.clients.Database.DeleteRoleBinding(r.Context(), &dbpb.DeleteRoleBindingRequest{Id: id}); err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionBindingDelete,
		TargetKind: audit.TargetRoleBinding,
		TargetID:   strconv.FormatInt(id, 10),
		TargetName: oldBinding.GetRoleName(),
		PipelineID: oldBinding.GetPipelineId(),
		Outcome:    audit.OutcomeSuccess,
		OldValue: auditJSON(map[string]any{
			"principal_kind": oldBinding.GetPrincipalKind(),
			"principal_id":   oldBinding.GetPrincipalId(),
			"role_name":      oldBinding.GetRoleName(),
			"pipeline_id":    oldBinding.GetPipelineId(),
		}),
	})
	s.publishRoleChange()
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// ---------------------------------------------------------------------------
// My permissions (F-14, RBAC)
// ---------------------------------------------------------------------------

// myPermissions returns the caller's effective permissions, split by scope:
// the permissions granted platform-wide and, per pipeline, the resource-scoped
// permissions granted for that pipeline. The UI uses it to hide/disable
// actions the caller cannot use instead of surfacing 403s.
func (s *Server) myPermissions(w http.ResponseWriter, r *http.Request) {
	if !s.rbacEnabled || s.authz == nil {
		// RBAC not enforced: the caller acts as a synthetic admin.
		writeJSON(w, http.StatusOK, map[string]any{
			"platform_wide": authz.AllPermissions,
			"scoped":        map[string][]string{},
		})
		return
	}
	principal := s.principalFromContext(r.Context())
	set, err := s.authz.PermissionsFor(r.Context(), principal)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "authorization: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, permissionSetResponse(set))
}

// permissionSetResponse renders a PermissionSet as JSON: the platform-wide
// permissions as a list, and the per-pipeline scoped permissions as a map of
// pipeline id (string) to permission list.
func permissionSetResponse(set *authz.PermissionSet) map[string]any {
	platform := make([]string, 0, len(set.PlatformWide))
	for p := range set.PlatformWide {
		platform = append(platform, p)
	}
	scoped := map[string][]string{}
	for pid, perms := range set.Scoped {
		list := make([]string, 0, len(perms))
		for p := range perms {
			list = append(list, p)
		}
		scoped[strconv.FormatInt(pid, 10)] = list
	}
	return map[string]any{
		"platform_wide": platform,
		"scoped":        scoped,
	}
}

// publishRoleChange appends a role_change event to the shared event log (F-14,
// F-23) so every API replica invalidates its local authorization cache and the
// change takes effect without a restart. It is a no-op when the Database
// client is nil (tests).
func (s *Server) publishRoleChange() {
	if s.clients.Database == nil {
		return
	}
	_, _ = s.clients.Database.PublishRoleChange(context.Background(), &dbpb.PublishRoleChangeRequest{})
}
