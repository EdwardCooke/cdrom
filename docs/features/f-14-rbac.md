# F-14 · Roles & permissions (RBAC)


> Phase 3 — Safety & governance · [Feature index](../Features.md)


**What.** A full role-based access control layer: a catalog of fine-grained
**permissions** (every operation in the system is a permission), **roles**
(built-in plus user-defined **custom roles**, each a named set of permissions),
and **bindings** that attach roles to principals (users, service accounts) —
optionally scoped to individual pipelines. Authorization is deny-by-default: a
principal can do exactly what its bound roles permit, nothing else.

Roles reach a principal through several independent binding mechanisms, which
compose (effective permissions are the union of all of them):

- **User-directory roles** (F-24) — the built-in IdP stamps the user's
  registered roles onto the token's `roles` claim; the API reads them from the
  verified token. No per-request database lookup.
- **JWT claim mapping** — for tokens from *any* IdP (including external
  enterprise IdPs), a configurable claim (e.g. `groups`, `roles`, `scope`) is
  read and its values mapped to role names, so group membership in the
  corporate IdP drives Cdrom roles without Cdrom's user directory knowing the
  user at all.
- **Direct bindings** — role bindings stored in the Database service and
  managed via the API; the mechanism for scoped grants (e.g. "operator on
  pipeline X") and the only mechanism F-26 service accounts use.
- **API keys** (F-25) — a key acts as its owner with the owner's permissions
  further limited to the key's pipeline scope.

**Why.** A shared CD platform must distinguish who may do what: a viewer
should not trigger a production deploy, a pipeline owner should manage their
pipeline without being a platform admin, and an enterprise should drive roles
from its existing IdP groups instead of re-managing membership in Cdrom.

**The permission model.**

- **Permissions** are fine-grained, namespaced names
  (`<area>.can-<action>`). The catalog is extensible — every feature
  registers its permissions here (F-26 adds `service-accounts.*`, F-19 adds
  `artifacts.can-promote`, …):

  | Permission | Allows |
  |------------|--------|
  | `pipelines.can-view` | List/read pipelines, versions, runs, jobs, logs, artifacts |
  | `pipelines.can-create` | Create pipelines |
  | `pipelines.can-edit` | Edit a pipeline's definition (jobs, steps, triggers, params) |
  | `pipelines.can-delete` | Delete a pipeline |
  | `runs.can-trigger` | Trigger a run (manually or via webhook) |
  | `runs.can-cancel` | Cancel a running run/job |
  | `jobs.can-approve` | Approve a gated job (F-13) |
  | `jobs.can-reject` | Reject a gated job (F-13) |
  | `secrets.can-view` | See which secrets a pipeline declares (names only, never values) |
  | `secrets.can-manage` | Create/update/delete secret values |
  | `roles.can-manage` | Create/edit/delete custom roles |
  | `roles.can-assign` | Grant/remove role bindings (subject to the delegation rules) |
  | `users.can-manage` | Manage users and their roles (F-24) |
  | `api-keys.can-manage` | Create/edit/rotate API keys (F-25) |
  | `audit.can-view` | Query the audit log (F-15) |
  | `workers.can-view` | View worker/agent status and the pending queue (F-16) |

- **Roles** are named sets of permissions. **Built-in roles** ship with the
  platform and cannot be deleted or have their permission sets edited:
  - `admin` — every permission (the platform administrator).
  - `operator` — view + trigger + cancel + approve/reject (runs a platform).
  - `viewer` — read-only access to everything.
  - `user` — the default role of a registered user (F-24): `viewer` plus
    self-service (manage own API keys, own profile).
  **Custom roles** are created by a principal with `roles.can-manage`: a name,
  a description, and an explicit set of permissions (e.g. `pipeline-owner` =
  view + edit + trigger + cancel + approve; `secret-keeper` = view + manage
  secrets). A role may also **include** other roles (composition), so
  `senior-operator` can be `operator` + `secrets.can-manage` without
  duplicating the list.

- **Bindings** attach a role to a principal, optionally **scoped to a
  pipeline**: an unscoped binding grants the role's permissions
  platform-wide; a pipeline-scoped binding grants them only for that pipeline
  (a scoped `runs.can-trigger` triggers runs of pipeline X but not pipeline
  Y). Scoping applies to resource-scoped permissions (pipelines, runs, jobs,
  secrets); platform-wide permissions (role/user management, audit) are only
  granted by unscoped bindings.

**Delegation rules.** Granting a role is itself a permission
(`roles.can-assign`), and it is bounded: a caller may bind a role to a
principal only if the caller already holds that role (unscoped, or with a
scope at least as wide as the one being granted). An operator cannot promote
someone to admin, and a pipeline-scoped operator cannot grant platform-wide
operator. Removing a binding requires `roles.can-assign` on the role being
removed. `admin` is grantable only by an `admin`.

**Nice-to-haves** (build after the core works):
- **Role templates / config-as-code** — export/import role definitions as YAML
  (pairs with F-21) so role sets are reviewable in source control.
- **Per-pipeline owner shortcut** — creating a pipeline optionally binds the
  creator a scoped `pipeline-owner` custom role on it.
- **Permission introspection** — `GET /api/me/permissions` returns the
  caller's effective permissions (per scope) so the UI hides/disables actions
  the caller can't use instead of surfacing 403s.
- **Role health view** — a UI screen listing roles, who holds each, and which
  permissions each grants; flags roles that grant admin-level power.
- **Just-in-time elevation** — a time-boxed elevation request ("grant me
  operator for 1 hour", approved by an admin, auto-expiring binding) for
  break-glass access.

**Scope.**
- `internal/models` — `Role` (name, description, built-in flag, permissions
  JSON, included-role names) and `RoleBinding` (principal kind
  `user`/`service-account`, principal id, role name, optional pipeline scope)
  registered in `All()`.
- `proto/cdrom/db/v1/db.proto` + `internal/services/database` — role and
  binding CRUD RPCs (create/get/list/update/delete roles; add/list/remove
  bindings).
- `internal/authz` (new package) — the authorization engine:
  `Check(ctx, principal, permission, resource) (bool, error)` and
  `PermissionsFor(ctx, principal)`. Resolves a principal's roles from (a) the
  token's `roles` claim, (b) the configured claim mapping, and (c) stored
  bindings; expands role composition; filters by resource scope;
  deny-by-default. Results are cached and invalidated on role/binding
  mutation (via the shared event log, F-23, so every API replica sees the
  change).
- `internal/auth` — `User` already carries `Roles` from the token's `roles`
  claim; add the claim-mapping step (read the configured claim, map values to
  role names, append to `User.Roles`).
- `internal/config` — an `auth.roles` section: `role_claim` (the claim to
  read, e.g. `groups`; empty = claim mapping disabled) and `role_mappings`
  (claim value → role name; unmapped values are ignored unless
  `role_claim_as_names: true`, in which case each value is itself a role
  name).
- `internal/api/server.go` — enforce `authz.Check` on the relevant endpoints
  before acting; new management endpoints: `GET/POST /api/roles`,
  `GET/PUT/DELETE /api/roles/{name}`, `GET/POST /api/role-bindings`,
  `DELETE /api/role-bindings/{id}`, `GET /api/me/permissions`.
- `internal/api` (gRPC surface) — user-role authorization applies to the
  UI-facing surface; the gRPC target surface keeps job-token auth, and the
  IdP's API-only gRPC surface stays reachable only from the API via mTLS.
- `ui/` — role management (custom role CRUD, permission checkboxes, role
  inclusion), binding management (principal + role + optional pipeline
  scope), and a "my permissions" view; hide/disable actions the caller lacks.

**Acceptance criteria.**
- [x] A `viewer` can read pipelines/runs/logs but cannot trigger, cancel,
      approve, or edit anything (403 on each).
- [x] An `operator` can trigger and cancel runs and approve/reject gated jobs
      (F-13), but cannot edit pipelines or manage secrets.
- [x] An `admin` can do everything, including manage roles, users, and
      secrets.
- [x] A principal with `roles.can-manage` can create a custom role with an
      arbitrary permission set; a principal bound to it gets exactly those
      permissions.
- [x] A custom role that includes another role inherits its permissions
      (composition); editing the included role changes the composed
      permissions (the engine re-reads roles from the Database service on
      invalidation, so a change to an included role is picked up).
- [x] A pipeline-scoped binding grants the role's resource-scoped permissions
      only for that pipeline (trigger on pipeline X succeeds, on pipeline Y
      403s); platform-wide permissions are never granted by a scoped binding.
- [x] With `auth.roles.role_claim` set, a token whose claim carries a mapped
      value is authorized as the mapped role — including tokens from an
      external IdP that Cdrom's user directory knows nothing about.
- [x] Delegation: a non-admin cannot grant a role they do not hold, cannot
      grant a wider scope than they hold, and `admin` is grantable only by an
      admin. (A role reached only by composition does not let a principal
      delegate it — only directly-held roles count.)
- [x] Deny-by-default: a principal with no roles (and no mapped claims) can
      do nothing (not even list pipelines).
- [x] Authorization is enforced server-side on the HTTP (UI) surface; the
      gRPC target surface keeps job-token auth (a separate, job-scoped
      mechanism), and the authz engine is wired into the gRPC server so
      user-facing gRPC operations can be authorized. UI visibility is not
      authorization.
- [ ] The acting principal is recorded on the action (feeds F-15); role and
      binding mutations are themselves audited. *(Partially: role/binding
      mutations emit a `role_change` event on the shared event log (F-23) as
      the invalidation + audit hook; the queryable audit log is F-15.)*
- [x] A role or binding change takes effect on every API replica without a
      restart (cache invalidation via the event log).
- [x] `GET /api/me/permissions` returns the caller's effective permissions
      per scope. *(The endpoint is implemented; the UI consuming it to
      disable actions is deferred — this feature was implemented
      backend-only.)*
- [x] Built-in roles cannot be deleted or have their permission sets edited;
      custom roles can.

**Implementation notes (backend-only).** This feature was implemented
backend-only: the React UI (`ui/`) is a stub, so the role/binding management
screens and the "my permissions" view are not built. The API-key (F-25) and
service-account (F-26) binding mechanisms are not wired in yet; the
`RoleBinding` model already carries a `principal_kind`
(`user`/`service-account`) so they plug in without schema change. When
authentication is disabled, requests act as a synthetic admin (the RBAC
engine is still wired in but bypassed).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **Deny-by-default, union of bindings.** A principal's effective permission
  set is the union of the permissions of every role bound to it (via token
  claim, claim mapping, or stored binding), expanded through role
  composition, then filtered by resource scope per request. No implicit
  permissions; no deny rules in v1 — a principal who should not have a
  permission simply is not bound to the role that grants it.
- **Roles live in the Database service; evaluation happens in the API.** Role
  and binding rows are owned by the Database service (like the IdP's user
  directory, F-24) so every API replica sees the same data; each API replica
  evaluates locally with a short-lived cache invalidated by role/binding
  mutation events on the shared event log (F-23), so a grant takes effect
  within ~50 ms on every pod.
- **The token's `roles` claim is the fast path; stored bindings and claim
  mapping are the general path.** For users known to the built-in IdP, roles
  are stamped onto the token at login (F-24) and need no DB lookup per
  request. Stored bindings and claim mapping cover the rest: external-IdP
  users, service accounts (F-26), and scoped grants. The mechanisms compose —
  a principal can have token-stamped and bound roles at once.
- **Claim mapping is config, not data.** `auth.roles.role_claim` +
  `role_mappings` are deployment config (env/config file), so an enterprise
  points Cdrom at its IdP's group claim without per-group database rows; the
  mapping is static and reviewed with the rest of the config.
- **Scoping is per-pipeline only in v1.** The resource dimension is the
  pipeline (which subsumes its runs, jobs, and secrets); worker groups and
  environments (F-17) can be added as further scope dimensions later.
  Platform-wide permissions (role/user management, audit) ignore scope.
- **API keys and service accounts plug in without new machinery.** A key's
  effective permissions are the owner's role-derived permissions ∩ the key's
  pipeline scope (F-25); a service account's are its bound roles' permissions
  (F-26). Both are just principals with a role set.
