// Package authz implements the authorization engine for Cdrom (F-14, RBAC).
//
// Authorization is deny-by-default: a principal can do exactly what its bound
// roles permit, nothing else. A principal's effective permissions are the
// union of the permissions of every role bound to it — via the token's
// roles claim, a configured claim mapping, or a stored binding — expanded
// through role composition (a role that includes another inherits its
// permissions), then filtered by resource scope per request.
//
// The engine is stateless with respect to the role catalog: it reads roles
// and bindings through a RoleSource (implemented by the API over the Database
// service) and caches the results locally, invalidating the cache when a
// role/binding change event arrives on the shared event log (F-23). This
// makes a grant take effect on every API replica within the tail cadence
// without a restart.
package authz

// Permission names (F-14). Every operation in the system is a permission; the
// catalog is namespaced (<area>.can-<action>) and extensible — every feature
// registers its permissions here (F-26 adds service-accounts.*, F-19 adds
// artifacts.can-promote, …).
const (
	PermPipelinesView    = "pipelines.can-view"
	PermPipelinesCreate  = "pipelines.can-create"
	PermPipelinesEdit    = "pipelines.can-edit"
	PermPipelinesDelete  = "pipelines.can-delete"
	PermRunsTrigger      = "runs.can-trigger"
	PermRunsCancel       = "runs.can-cancel"
	PermJobsApprove      = "jobs.can-approve"
	PermJobsReject       = "jobs.can-reject"
	PermSecretsView      = "secrets.can-view"
	PermSecretsManage    = "secrets.can-manage"
	PermRolesManage      = "roles.can-manage"
	PermRolesAssign      = "roles.can-assign"
	PermUsersManage      = "users.can-manage"
	PermAPIKeysManage    = "api-keys.can-manage"
	PermAPIKeysListAll   = "api-keys.can-list-all"
	PermAPIKeysManageAll = "api-keys.can-manage-all"
	PermAuditView        = "audit.can-view"
	PermWorkersView      = "workers.can-view"
)

// AllPermissions is the complete catalog of permissions the platform knows
// about. The admin role grants every one of these.
var AllPermissions = []string{
	PermPipelinesView,
	PermPipelinesCreate,
	PermPipelinesEdit,
	PermPipelinesDelete,
	PermRunsTrigger,
	PermRunsCancel,
	PermJobsApprove,
	PermJobsReject,
	PermSecretsView,
	PermSecretsManage,
	PermRolesManage,
	PermRolesAssign,
	PermUsersManage,
	PermAPIKeysManage,
	PermAPIKeysListAll,
	PermAPIKeysManageAll,
	PermAuditView,
	PermWorkersView,
}

// permissionCatalog is the set of known permission names, used to validate a
// custom role's permission set (a role that names an unknown permission is
// rejected at create/update time).
var permissionCatalog = func() map[string]bool {
	m := make(map[string]bool, len(AllPermissions))
	for _, p := range AllPermissions {
		m[p] = true
	}
	return m
}()

// IsKnownPermission reports whether p is a permission in the catalog.
func IsKnownPermission(p string) bool {
	return permissionCatalog[p]
}

// isResourceScoped reports whether the permission operates on a specific
// pipeline resource (and so can be granted via a pipeline-scoped binding).
// Resource-scoped permissions are the ones that act on a pipeline and its
// runs, jobs, and secrets. Platform-wide permissions (pipeline creation,
// role/user management, API keys, audit, workers) are only granted by
// unscoped bindings.
func isResourceScoped(p string) bool {
	switch p {
	case PermPipelinesView, PermPipelinesEdit, PermPipelinesDelete,
		PermRunsTrigger, PermRunsCancel,
		PermJobsApprove, PermJobsReject,
		PermSecretsView, PermSecretsManage:
		return true
	default:
		return false
	}
}

// Built-in role names. Built-in roles ship with the platform and cannot be
// deleted or have their permission sets edited; only custom roles can.
const (
	RoleAdmin    = "admin"
	RoleOperator = "operator"
	RoleViewer   = "viewer"
	RoleUser     = "user"
)

// RoleDefinition is a role's definition: its name, description, built-in
// flag, the permissions it grants directly, and the roles it includes
// (composition). It is the form the built-in roles are seeded from and the
// form a custom role is created/updated with.
type RoleDefinition struct {
	Name        string
	Description string
	BuiltIn     bool
	Permissions []string
	Includes    []string
}

// BuiltinRoles returns the built-in role definitions, keyed by name. The
// database service seeds these into the role catalog on startup so every API
// replica sees the same built-in roles.
//
//   - admin: every permission (the platform administrator).
//   - operator: view + trigger + cancel + approve/reject (runs a platform).
//   - viewer: read-only access to everything.
//   - user: the default role of a registered user — viewer plus self-service
//     (manage own API keys).
func BuiltinRoles() map[string]RoleDefinition {
	return map[string]RoleDefinition{
		RoleAdmin: {
			Name:        RoleAdmin,
			Description: "Platform administrator: every permission.",
			BuiltIn:     true,
			Permissions: AllPermissions,
		},
		RoleOperator: {
			Name:        RoleOperator,
			Description: "Runs a platform: view, trigger and cancel runs, approve/reject gated jobs.",
			BuiltIn:     true,
			Permissions: []string{
				PermPipelinesView,
				PermRunsTrigger,
				PermRunsCancel,
				PermJobsApprove,
				PermJobsReject,
				PermWorkersView,
			},
		},
		RoleViewer: {
			Name:        RoleViewer,
			Description: "Read-only access to pipelines, runs, jobs, logs, artifacts, and workers.",
			BuiltIn:     true,
			Permissions: []string{
				PermPipelinesView,
				PermSecretsView,
				PermWorkersView,
			},
		},
		RoleUser: {
			Name:        RoleUser,
			Description: "Default role of a registered user: viewer plus self-service (own API keys).",
			BuiltIn:     true,
			Permissions: []string{
				PermPipelinesView,
				PermSecretsView,
				PermWorkersView,
				PermAPIKeysManage,
			},
		},
	}
}
