# F-28 · SCIM user provisioning (automatic user management from an external IdP)


> Phase 6 — Authentication · [Feature index](../Features.md)


**Overview.** F-28 is large, so it is decomposed into independently-buildable
sub-features, each with its own spec file. This file is the **overview** and
holds the shared motivation, model, and design decisions; the sub-features
carry the detailed scope and acceptance criteria for each piece.

| Sub-feature | Spec | Scope | Depends on |
|-------------|------|-------|------------|
| **F-28.1** SCIM metadata & service configuration | [f-28.1](f-28.1-scim-metadata-and-config.md) | Mounting the SCIM 2.0 surface on the IdP's HTTP server: `ServiceProviderConfig`, `Schemas`, `ResourceTypes` + the `idp.scim` configuration object (enablement, base path, page bound, group→role mappings) | — |
| **F-28.2** SCIM caller authentication | [f-28.2](f-28.2-scim-caller-authentication.md) | Protecting the SCIM surface with a rotated pool of database-held SCIM API keys (Argon2ID-encoded at rest with the pepper prepended, shown only at creation, optional expiration, zero-downtime rotation via the API server's `/api/scim/keys` admin endpoints); 401 semantics | F-28.1 |
| **F-28.3** User lifecycle foundations (soft delete) | [f-28.3](f-28.3-user-lifecycle-foundation.md) | `IDPUser` model deltas (active, tombstone, external ID, provenance), soft-delete DB/admin RPC semantics, active-vs-deprovisioned states, surviving audit attribution | F-24, F-14, F-15 |
| **F-28.4** SCIM user management | [f-28.4](f-28.4-scim-user-management.md) | The `/Users` lifecycle (create/re-provision, update, activate/deactivate, delete/deprovision, list with pagination + filter subset), the SCIM→directory mapping, and the JIT-vs-SCIM provisioning-mode switch on the OIDC login path | F-28.1, F-28.2, F-28.3 |
| **F-28.5** SCIM group management | [f-28.5](f-28.5-scim-group-management.md) | The `/Groups` lifecycle, group storage, and applying group→role (F-14) / group→approval-group (F-27) mappings on sync | F-28.4 |

**Suggested build order:** F-28.1 → F-28.2 → F-28.3 → F-28.4 → F-28.5.
F-28.3 is also worthwhile standing alone: it gives the existing user
directory (F-24) soft-delete semantics regardless of whether SCIM is ever
turned on.

---

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
first OIDC login (F-28.4, **Provisioning modes**).

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
  configurable base path (default `/scim/v2`) — F-28.1 mounts it:
  - `GET /scim/v2/ServiceProviderConfig`, `GET /scim/v2/Schemas`,
    `GET /scim/v2/ResourceTypes` — metadata (F-28.1).
  - `GET/POST /scim/v2/Users`, `GET/PUT/PATCH/DELETE /scim/v2/Users/{id}` —
    user lifecycle (F-28.4).
  - `GET/POST /scim/v2/Groups`, `GET/PUT/PATCH/DELETE /scim/v2/Groups/{id}` —
    group lifecycle (F-28.5).
  - Standard SCIM semantics: `ListResponse` with pagination (`startIndex` /
    `count`, `totalResults`), a filter subset (`userName eq …`, `active eq …`,
    `externalId eq …`), and ETag/`If-Match` optimistic concurrency
    (nice-to-have).
- **Caller authentication** — the whole surface sits behind a rotated pool
  of SCIM API keys (F-28.2).
- **SCIM → Cdrom user mapping** (RFC 7643 User schema → `IDPUser`) — F-28.4:
  - `userName` → `Email` (the unique login identifier).
  - `name.givenName` / `name.familyName` → `FirstName` / `LastName`.
  - `active` (boolean) → the user's active/deactivated state.
  - `externalId` → stored as the corporate directory's stable ID (e.g. the
    Azure object ID), used to match re-provisioning.
  - `groups` (value refs) → role bindings (via a group→role mapping) and/or
    F-27 approval-group memberships (F-28.5).
  - `meta.created` / `meta.lastModified` → `CreatedAt` / `UpdatedAt`.
- **Lifecycle states** — F-28.3 establishes the four-way distinction the
  operations manipulate: active, deactivated (`active:false`, "on leave"),
  deprovisioned (tombstoned, "gone"), and re-provisioned (tombstone cleared,
  same stable ID, "re-hire").

**Nice-to-haves** (build after the core works):
- **Bulk + ETag** (F-28.4) — SCIM `Bulk` operations and ETag/`If-Match`
  optimistic concurrency for large directories; the F-28.1 metadata
  advertisements flip on when these land.
- **Provisioning health** (F-28.4) — a `GET /api/scim/status` (or UI view)
  showing the last successful sync, user counts (active / deactivated /
  deprovisioned), and recent provisioning events.
- **Groups → F-27 approval groups** (F-28.5) — the group→role mapping is core;
  additionally targeting F-27 approval groups is a stretch.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **SCIM lives on the built-in IdP, not the API.** The user directory is owned
  by the IdP (via the Database service, F-24), and SCIM is an
  identity-provisioning protocol; the corporate IdP points its SCIM
  integration at Cdrom's IdP endpoint. The IdP's existing HTTP server hosts
  the new `/scim/v2/*` surface next to the OIDC surface. The API is not in the
  SCIM path.
- **JIT auto-provisioning is the complement of SCIM** (F-28.4). When SCIM is
  disabled, the IdP auto-provisions a user on their first OIDC login
  (just-in-time, from the token's claims, with the default role) so users need
  not be pre-created. When SCIM is enabled, JIT is off: the corporate IdP is
  the sole source of user records (via SCIM), and an OIDC login for a user
  with no SCIM-provisioned record is rejected. This guarantees that in a
  SCIM-managed deployment only users the corporate directory has provisioned
  can sign in.
- **Soft delete is the only delete** (F-28.3). Neither SCIM `DELETE` nor the
  admin `DELETE /api/users` hard-deletes a user. Both tombstone the row
  (retain the record, invalidate credentials). This keeps every historical
  actor reference (audit F-15, approvals F-13, triggers F-07/F-09) permanently
  resolvable, and makes re-provisioning (re-hire) a clean reactivation of the
  same stable-ID row. A hard delete, if ever needed for erasure, is a
  separate, explicit, admin-only operation distinct from deprovisioning (and
  would zero credentials the way F-26 does for service accounts) — it is not
  part of the normal lifecycle.
- **`active` vs. deprovisioned are distinct states** (F-28.3). `active:false`
  (SCIM deactivate) = the user is temporarily off (on leave): retained,
  reactivatable, credentials suspended. Deprovisioned (SCIM delete) =
  tombstoned: removed from the active directory, reactivatable only by
  re-provisioning. Both retain the row; both block authentication.
- **SCIM-managed users are not hand-edited** (F-28.4). A user provisioned via
  SCIM (`provisioned=true`) has its identity (email, name, active state,
  group/role memberships) driven by the corporate IdP; manual edits to those
  fields are rejected (or overridden on the next sync) so the corporate
  directory stays the source of truth. Locally-registered users (F-24) are
  managed manually and are not touched by SCIM.
- **The SCIM caller is a privileged, authenticated service** (F-28.2). The
  SCIM surface is protected by a rotated pool of SCIM API keys —
  database-held, hashed at rest, administered through the API server, and
  completely separate from F-26 service accounts — so it is not public and
  not part of the OIDC sign-in surface. Only the corporate IdP (possessing a
  valid key) can provision users.
- **Groups map to roles (and optionally approval groups)** (F-28.5). A SCIM
  group's membership is applied via a configurable group→role mapping (F-14)
  so corporate group membership drives Cdrom authorization; the same
  mechanism can target F-27 approval groups. This pairs with F-14's JWT claim
  mapping: the corporate IdP both provisions (SCIM) and authenticates (OIDC
  group claims), and the two stay consistent.
