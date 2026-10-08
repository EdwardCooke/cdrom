# F-28 · SCIM user provisioning (automatic user management from an external IdP)


> Phase 6 — Authentication · [Feature index](../Features.md)


**What.** A **SCIM 2.0** (RFC 7643/7644) service-provider endpoint on the
built-in IdP, so an external identity provider (Azure AD / Entra ID, Okta,
Google Workspace, …) can **provision, update, and deprovision** Cdrom users
and groups automatically. When a person is added to the corporate directory
they appear in Cdrom with the right roles; when they leave they are
deactivated/deprovisioned — no manual user management in Cdrom, no drift
between the corporate directory and Cdrom. How a user record comes into
existence depends on whether SCIM is enabled: with SCIM **enabled** the
corporate IdP is the sole source of users (no auto-provisioning on login);
with SCIM **disabled** the IdP auto-provisions users just-in-time on their
first OIDC login (see **Provisioning modes** below).

**Why.** In an enterprise, users and their group memberships are owned by the
corporate IdP (e.g. Azure AD). Re-keying that into Cdrom by hand (F-24
`register`, F-14 role bindings) is slow, error-prone, and drifts. SCIM is the
standard, well-supported protocol for exactly this: the corporate IdP pushes
user/group lifecycle events to Cdrom's IdP, which maintains the user directory
(the same directory F-24/F-14 use for roles and authorization). Cdrom's
built-in IdP authenticates via the corporate IdP's OIDC tokens (F-14 claim
mapping) while also receiving SCIM provisioning — authentication and
provisioning both flow from the same corporate IdP.

**The model.**
- **SCIM service-provider surface** on the IdP's HTTP server (alongside the
  existing OIDC surface: discovery, JWKS, `/auth`, `/token`), under a
  configurable base path (default `/scim/v2`):
  - `GET /scim/v2/ServiceProviderConfig`, `GET /scim/v2/Schemas`,
    `GET /scim/v2/ResourceTypes` — metadata.
  - `GET/POST /scim/v2/Users`, `GET/PUT/PATCH/DELETE /scim/v2/Users/{id}` —
    user lifecycle.
  - `GET/POST /scim/v2/Groups`, `GET/PUT/PATCH/DELETE /scim/v2/Groups/{id}` —
    group lifecycle.
  - Standard SCIM semantics: `ListResponse` with pagination (`startIndex` /
    `count`, `totalResults`), filter support (a subset: `userName eq …`,
    `active eq …`, `externalId eq …`), and ETag/`If-Match` optimistic
    concurrency (nice-to-have).
- **SCIM → Cdrom user mapping** (RFC 7643 User schema → `IDPUser`):
  - `userName` → `Email` (the unique login identifier).
  - `name.givenName` / `name.familyName` → `FirstName` / `LastName`.
  - `active` (boolean) → the user's active/deactivated state.
  - `externalId` → stored as the corporate directory's stable ID (e.g. the
    Azure object ID), used to match re-provisioning.
  - `groups` (value refs) → role bindings (via a group→role mapping) and/or
    F-27 approval-group memberships.
  - `meta.created` / `meta.lastModified` → `CreatedAt` / `UpdatedAt`.
- **Operations:**
  - **Create** (`POST /Users`) — provision a new user (default role, e.g.
    `user`), or, if the `userName`/`externalId` matches a soft-deleted user,
    **re-provision** (clear the tombstone, reactivate) — the "re-hire" case.
  - **Update** (`PUT`/`PATCH /Users/{id}`) — update profile, active state,
    and group/role memberships.
  - **Deactivate** (`PATCH active:false`) — the user can no longer
    authenticate (login and token verification reject them) but the record is
    retained; `active:true` restores access. The "on leave" case.
  - **Deprovision** (`DELETE /Users/{id}`) — **soft delete** (tombstone): the
    user is removed from the active directory and can no longer authenticate,
    but the row is retained (never hard-deleted).

**Provisioning modes (JIT vs. SCIM).** How a user record comes into existence
depends on `idp.scim.enabled`, and the two mechanisms are mutually exclusive
(there is always exactly one source of user records):
- **SCIM enabled** — the corporate IdP is the source of truth and provisions
  users via SCIM. The IdP does **not** auto-provision on OIDC login: a user
  must already exist (SCIM-provisioned) to authenticate, and an OIDC login for
  an unknown user is rejected. This stops un-provisioned "shadow" users from
  reaching Cdrom.
- **SCIM disabled** (the default) — there is no corporate directory pushing
  users, so the IdP **auto-provisions on first OIDC login** (just-in-time):
  when a user authenticates via the OIDC flow for the first time and no record
  exists, the IdP creates one from the OIDC claims (email, name) with the
  default role; subsequent logins reuse the record. This is the simple
  local-dev / plain-OIDC mode where users are not pre-created.

**Soft delete (the core requirement).** Users are **never hard-deleted** by
SCIM (or by the admin `DELETE /api/users`, F-24). A deprovisioned user is
tombstoned: the `IDPUser` row is retained with a `deleted_at` / `active=false`
marker and its credentials invalidated. This is deliberate:
- **Audit tracing (F-15).** Audit events, approval decisions (F-13), and run
  triggers (F-07/F-09) record the actor by the user's stable subject/ID.
  Because a user is never hard-deleted, that ID remains permanently resolvable
  to a name/email, so "who did this" stays answerable long after the person
  has left. A hard delete would leave dangling actor references.
- **Stable identity.** The user's ID (the token subject) is stable across
  deprovision/re-provision cycles, so historical actions always tie back to
  the same principal.
- **Re-provisioning is safe.** Re-creating a tombstoned user (same
  `userName`/`externalId`) reactivates the existing row rather than creating a
  duplicate, preserving the stable ID and all historical attribution.

**Nice-to-haves** (build after the core works):
- **SCIM Groups → roles / approval groups** — a configurable group→role
  mapping (SCIM group name → F-14 role) so corporate group membership drives
  Cdrom roles; optionally map groups to F-27 approval groups.
- **OAuth2 client-credentials** — authenticate the SCIM caller with a standard
  OAuth2 client-credentials token from the corporate IdP (instead of a static
  bearer credential), for environments that already issue machine tokens.
- **Bulk + ETag** — SCIM `Bulk` operations and ETag/`If-Match` optimistic
  concurrency for large directories.
- **Provisioning health** — a `GET /api/scim/status` (or UI view) showing the
  last successful sync, user counts (active / deactivated / deprovisioned),
  and recent provisioning events.

**Scope.**
- `internal/models` — `IDPUser` gains an `Active` (bool), a soft-delete marker
  (`DeletedAt` / `Deleted`), an `ExternalID` (the corporate directory's stable
  ID), and a `Provisioned` flag (SCIM-managed vs. locally registered); the new
  fields are picked up by `All()` / `AutoMigrate`.
- `proto/cdrom/db/v1/db.proto` + `internal/services/database` — the user RPCs
  gain soft-delete semantics: `DeleteIDPUser` becomes a **soft delete**
  (tombstone, never hard-delete); `ListIDPUsers` gains an `include_deleted`
  flag (for audit joins); a `ReactivateIDPUser` (clear tombstone) for
  re-provisioning; the `IDPUser` message carries `active`, `deleted_at`,
  `external_id`, and `provisioned`.
- `proto/cdrom/idp/v1/idp.proto` + `internal/idp` — the IdP's gRPC API-only
  surface gains SCIM operations (`SCIMCreateUser` / `SCIMUpdateUser` /
  `SCIMDeleteUser` (soft) / `SCIMListUsers`, plus group operations) that the
  SCIM HTTP handler calls; the `UserStore` interface gains `Deactivate` /
  `Reactivate` / `SoftDelete` (and `List` takes an `includeDeleted` option).
- `internal/idp` (new `scim.go`) — the SCIM 2.0 HTTP handler on the IdP's
  existing HTTP server: the `/scim/v2/*` routes, SCIM request/response
  (de)serialization, filter + pagination, the SCIM→`UserStore` mapping, and
  SCIM error mapping (404/409/4xx per RFC 7644).
- `internal/idp` (auth) — SCIM caller authentication: the SCIM surface requires
  a Bearer credential (a dedicated SCIM service token, or an OAuth2
  client-credentials token — see nice-to-haves); unauthenticated/invalid →
  401. The SCIM surface is reachable only with that credential (it is not part
  of the public OIDC surface).
- `internal/idp` (OIDC login path) — when `idp.scim.enabled` is false, the
  OIDC authorization/token flow auto-provisions a user on first login
  (just-in-time: create the `IDPUser` from the token's claims with the default
  role); when true, the flow requires the user to already exist
  (SCIM-provisioned) and rejects an unknown user (no JIT).
- `internal/config` — an `idp.scim` section: `enabled` (default `false`; when
  false the IdP auto-provisions users just-in-time on first OIDC login, when
  true it does not — see Provisioning modes), `base_path` (default
  `/scim/v2`), the SCIM service credential (a bearer token, or a reference to
  a service account / API key from F-25/F-26), and optional
  `group_role_mappings` (SCIM group name → F-14 role name).
- `internal/api` — the admin `DELETE /api/users` (F-24) becomes a **soft
  delete** (tombstone) rather than a hard delete, so the "never hard-delete"
  invariant holds for both SCIM and manual management; `GET /api/users` can
  include deactivated/deprovisioned users for audit.
- `internal/idp` + F-15 — every SCIM operation (create/update/deactivate /
  deprovision/reactivate, group changes) writes an **audit event** (actor =
  the SCIM service, target user, operation, outcome), so provisioning history
  is traceable.
- `ui/` — a user view that shows active / deactivated / deprovisioned
  (soft-deleted) states and, when SCIM is enabled, marks SCIM-managed users
  (no manual edit of a provisioned user's identity; edits are driven by the
  corporate IdP).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **SCIM lives on the built-in IdP, not the API.** The user directory is owned
  by the IdP (via the Database service, F-24), and SCIM is an
  identity-provisioning protocol; the corporate IdP points its SCIM
  integration at Cdrom's IdP endpoint. The IdP's existing HTTP server hosts
  the new `/scim/v2/*` surface next to the OIDC surface. The API is not in the
  SCIM path.
- **JIT auto-provisioning is the complement of SCIM.** When SCIM is disabled,
  the IdP auto-provisions a user on their first OIDC login (just-in-time, from
  the token's claims, with the default role) so users need not be pre-created.
  When SCIM is enabled, JIT is off: the corporate IdP is the sole source of
  user records (via SCIM), and an OIDC login for a user with no
  SCIM-provisioned record is rejected. This guarantees that in a SCIM-managed
  deployment only users the corporate directory has provisioned can sign in.
- **Soft delete is the only delete.** Neither SCIM `DELETE` nor the admin
  `DELETE /api/users` hard-deletes a user. Both tombstone the row (retain the
  record, invalidate credentials). This keeps every historical actor reference
  (audit F-15, approvals F-13, triggers F-07/F-09) permanently resolvable, and
  makes re-provisioning (re-hire) a clean reactivation of the same stable-ID
  row. A hard delete, if ever needed for erasure, is a separate, explicit,
  admin-only operation distinct from deprovisioning (and would zero
  credentials the way F-26 does for service accounts) — it is not part of the
  normal lifecycle.
- **`active` vs. deprovisioned are distinct states.** `active:false` (SCIM
  deactivate) = the user is temporarily off (on leave): retained, reactivatable,
  credentials suspended. Deprovisioned (SCIM delete) = tombstoned: removed
  from the active directory, reactivatable only by re-provisioning. Both
  retain the row; both block authentication.
- **SCIM-managed users are not hand-edited.** A user provisioned via SCIM
  (`provisioned=true`) has its identity (email, name, active state, group/role
  memberships) driven by the corporate IdP; manual edits to those fields are
  rejected (or overridden on the next sync) so the corporate directory stays
  the source of truth. Locally-registered users (F-24) are managed manually
  and are not touched by SCIM.
- **The SCIM caller is a privileged, authenticated service.** The SCIM surface
  is protected by a dedicated credential (a SCIM service token, or an OAuth2
  client-credentials token, or a F-26 service account scoped to user
  management) — it is not public and not part of the OIDC sign-in surface.
  Only the corporate IdP (holding that credential) can provision users.
- **Groups map to roles (and optionally approval groups).** A SCIM group's
  membership is applied via a configurable group→role mapping (F-14) so
  corporate group membership drives Cdrom authorization; the same mechanism
  can target F-27 approval groups. This pairs with F-14's JWT claim mapping:
  the corporate IdP both provisions (SCIM) and authenticates (OIDC group
  claims), and the two stay consistent.

**Acceptance criteria.**
- [ ] With `idp.scim.enabled: true`, the IdP serves the SCIM 2.0 surface
      (`ServiceProviderConfig`, `Schemas`, `Users`, `Groups`) at the
      configured base path.
- [ ] With `idp.scim.enabled: false`, a user who has never logged in is
      auto-provisioned on their first OIDC login (a user record is created
      from the OIDC claims with the default role); subsequent logins reuse it.
- [ ] With `idp.scim.enabled: true`, an OIDC login for a user with no
      SCIM-provisioned record is rejected (no JIT auto-provisioning); only
      SCIM-provisioned users can authenticate.
- [ ] The SCIM surface rejects unauthenticated or invalid-credential callers
      (401); only the configured SCIM service credential is accepted.
- [ ] `POST /scim/v2/Users` provisions a new user (mapped from the SCIM User
      schema) with the default role; the user can then authenticate.
- [ ] `PATCH /scim/v2/Users/{id}` with `active:false` deactivates the user
      (login and token verification reject them) while retaining the record;
      `active:true` restores access.
- [ ] `DELETE /scim/v2/Users/{id}` **soft-deletes** (tombstones) the user: the
      row is retained, the user can no longer authenticate, and the user's ID
      remains resolvable.
- [ ] A soft-deleted user's past actions remain attributable: audit events
      (F-15), approval decisions (F-13), and run triggers (F-07/F-09) that
      named the user by ID still resolve to the user's name/email after
      deprovisioning.
- [ ] Re-provisioning a tombstoned user (same `userName`/`externalId`)
      reactivates the existing row (same stable ID) rather than creating a
      duplicate.
- [ ] The admin `DELETE /api/users` (F-24) is a soft delete (no hard delete of
      a user by any path in the normal lifecycle).
- [ ] SCIM Groups are mapped to F-14 roles (via the configured group→role
      mapping) and/or F-27 approval groups; a user's group memberships update
      their roles on sync.
- [ ] Every SCIM operation writes an audit event (actor = SCIM service, target
      user, operation, outcome).
- [ ] SCIM-managed users (`provisioned=true`) are not hand-editable in the
      UI/API for identity fields; the corporate IdP is the source of truth.
- [ ] Idempotency: a duplicate `POST /Users` for an existing `userName`
      returns the existing user (200) or 409, never a duplicate row; a
      `DELETE` of an unknown user is a 404.
