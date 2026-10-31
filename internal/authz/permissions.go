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
// registers its permissions here (F-26 adds service-accounts.*, F-21.6 adds
// packages.can-view/can-publish/can-promote, …).
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
	// Service-account management permissions (F-26). Roles contain granular
	// permissions; these are permission names, not hard-coded roles.
	PermServiceAccountsView    = "service-accounts.can-view"
	PermServiceAccountsCreate  = "service-accounts.can-create"
	PermServiceAccountsEdit    = "service-accounts.can-edit"
	PermServiceAccountsRotate  = "service-accounts.can-rotate-keys"
	PermServiceAccountsDisable = "service-accounts.can-disable"
	PermServiceAccountsEnable  = "service-accounts.can-enable"
	PermServiceAccountsDelete  = "service-accounts.can-delete"
	PermServiceAccountsAssign  = "service-accounts.can-assign-roles"
	PermServiceAccountsRemove  = "service-accounts.can-remove-roles"
	// Concurrency-limit management permissions (F-16). The concurrency limits
	// (global / per-group / per-key caps, stored as regex-based rules in the
	// database) are platform-wide: can-view reads them, can-manage edits them.
	PermConcurrencyLimitsView   = "concurrency-limits.can-view"
	PermConcurrencyLimitsManage = "concurrency-limits.can-manage"
	// Environment management permissions (F-17). Environments (named deployment
	// targets that carry approval + automation policy) are platform-wide:
	// can-view reads them, can-manage edits them. Proceeding a run held by a
	// `manual` environment requires runs.can-proceed.
	PermEnvironmentsView   = "environments.can-view"
	PermEnvironmentsManage = "environments.can-manage"
	PermRunsProceed        = "runs.can-proceed"
	// Worker-group management permissions (F-29). Worker groups (named groups
	// of long-lived workers that carry labels) are platform-wide: can-view reads
	// a group and its labels, can-manage creates/updates/deletes a group and its
	// labels.
	PermWorkerGroupsView   = "worker-groups.can-view"
	PermWorkerGroupsManage = "worker-groups.can-manage"
	// SSH known-host management permissions (F-21.2.4.1). The runtime-managed
	// SSH host keys the source-fetcher uses to verify a git-ssh server's host
	// key are platform-wide: can-view reads them, can-manage adds/removes them.
	PermSSHKnownHostsView   = "ssh-known-hosts.can-view"
	PermSSHKnownHostsManage = "ssh-known-hosts.can-manage"
	// Source-credential management permissions (F-21.3). The named,
	// database-stored credentials that authenticate a fetch of a private
	// source are platform-wide: can-view reads them (secret material redacted),
	// can-manage creates/updates/deletes them.
	PermSourceCredentialsView   = "source-credentials.can-view"
	PermSourceCredentialsManage = "source-credentials.can-manage"
	// Package registry permissions (F-21.6). The package registry (the
	// artifacts store's reserved "packages" namespace) is platform-wide:
	// can-view browses/downloads published versions, can-publish uploads a
	// version (POST /api/packages), can-promote moves an artifact between
	// namespaces (POST /api/artifacts/promote — the former F-19 operation).
	// All are denied by default (grant them to a role to enable).
	PermPackagesView    = "packages.can-view"
	PermPackagesPublish = "packages.can-publish"
	PermPackagesPromote = "packages.can-promote"
	// Restoration permission (F-22): initiating a RESUMPTION of a pipeline —
	// spawning a run that re-executes the suffix of the pipeline's latest
	// successful run from a named cut point (POST /api/pipelines/{id}/restart,
	// and the armed rollback step handler's API mediation). Pipeline-scoped,
	// denied by default (grant it to a role to enable manual restoration).
	PermRunsRestore = "runs.can-restore"
	// Approval-group management permissions (F-27). Approval groups (named
	// sets of users an approval step can reference) are platform-wide:
	// can-view reads a group and its members, can-manage creates/updates/
	// deletes a group and its memberships. Denied by default (grant them to a
	// role to enable group administration); the ability to DECIDE a gate is
	// governed separately by jobs.can-approve / jobs.can-reject plus the
	// gate's approver references.
	PermApprovalsView   = "approvals.can-view"
	PermApprovalsManage = "approvals.can-manage"
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
	PermServiceAccountsView,
	PermServiceAccountsCreate,
	PermServiceAccountsEdit,
	PermServiceAccountsRotate,
	PermServiceAccountsDisable,
	PermServiceAccountsEnable,
	PermServiceAccountsDelete,
	PermServiceAccountsAssign,
	PermServiceAccountsRemove,
	PermConcurrencyLimitsView,
	PermConcurrencyLimitsManage,
	PermEnvironmentsView,
	PermEnvironmentsManage,
	PermRunsProceed,
	PermWorkerGroupsView,
	PermWorkerGroupsManage,
	PermSSHKnownHostsView,
	PermSSHKnownHostsManage,
	PermSourceCredentialsView,
	PermSourceCredentialsManage,
	PermPackagesView,
	PermPackagesPublish,
	PermPackagesPromote,
	PermRunsRestore,
	PermApprovalsView,
	PermApprovalsManage,
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
		PermRunsTrigger, PermRunsCancel, PermRunsRestore,
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
				PermConcurrencyLimitsView,
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
				PermConcurrencyLimitsView,
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
				PermConcurrencyLimitsView,
			},
		},
	}
}
