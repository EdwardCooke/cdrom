# F-26 · Service accounts


> Phase 6 — Authentication · [Feature index](../Features.md)


**What.** A service account is a non-human identity for automation, independent
of any user's login or employment lifecycle. It has assignable F-14 roles and
**exactly two API-key slots**, numbered `1` and `2`, both usable concurrently
and individually rotatable. Creation returns both plaintext keys; rotation
returns only the replacement key for the selected slot. The Database service
generates and hashes the keys; plaintext is never persisted and is returned
only in these successful mutation responses for one-time display in the UI.
An account can be temporarily **disabled** to stop access, or permanently
**soft-deleted**, retaining its identity while zeroing out both key slots.

**Why.** Integrations should not depend on a human's personal API key. Two
independent credentials allow a client to switch to the other slot before
rotating a key, avoiding downtime. Separate management permissions let an
operator rotate credentials or disable access without also being able to
create identities, delete them, or grant privileges.

**Account model.**
- `ServiceAccount`: immutable ID, unique immutable login name (separate from
  the human email namespace), editable display name and description, role
  memberships, `disabled`, nullable `deleted_at`, creation/update timestamps,
  creator/updater principal IDs, and a revision for concurrent mutations.
  An account has no password or interactive OIDC login.
- `ServiceAccountKey`: account ID, slot (`1` or `2`), hash, non-secret display
  prefix, generation/rotation timestamp, and a generation counter.
  Enforce a unique `(service_account_id, slot)` and valid slot values. Every
  account retains two slot records; callers cannot add a third or delete one.
  Public DTOs expose slot metadata, never hashes.
- Roles determine the account's own permissions, not the creator's. An
  account with no roles has no authorized operations. Role bindings use the
  same F-14 roles and resource scopes as human principals; no implicit admin
  or default privileged role is assigned.
- `deleted_at` is an irreversible tombstone, not a hard delete or a generic
  soft-delete mechanism that hides the row from verification. Keep the name
  reserved after deletion so a new identity cannot impersonate the old one.
  Retain identity, role history, and audit references for attribution.

**Key lifecycle and storage.**
- Create the account, its role bindings, and both freshly generated keys in
  one Database-service transaction. Use Go `crypto/rand` to generate each
  independent secret (`cdrom-sa-` plus 64 uniformly random alphanumeric
  characters); fail the whole operation if randomness, hashing, or storage
  fails. A service-account-specific prefix distinguishes F-26 from F-25.
- Store only SHA-512 hashes of these high-entropy keys and non-secret
  metadata. Compare hashes in constant time. If a pepper is configured, use
  HMAC-SHA-512 and keep the pepper out of the database; all Database replicas
  must share it. Never return stored hashes to the API, IdP, or UI.
- Unlike F-25, which proposes generation/hashing in the IdP, **F-26 key
  generation, hashing, and verification belong to the Database service**.
  The IdP coordinates identity operations over gRPC; it does not generate
  these secrets or read their hashes. All persistence remains DB-owned.
- Rotating slot `1` or `2` atomically replaces only that slot's hash and
  metadata. The old value stops authenticating as soon as the transaction
  commits; the other slot remains valid and unchanged. Never rotate both
  slots implicitly. Use account revision checks to reject conflicting
  rotations or lifecycle mutations rather than silently overwriting them.
- Disable preserves both hashes and role bindings but rejects both keys.
  Enable restores access with the same keys and current roles. Rotation is
  allowed while disabled to support incident recovery, but does not enable
  the account.
- Delete atomically marks `deleted_at`, sets `disabled`, and **zeros out both
  slots**: clear hashes, prefixes, and any credential-bearing fields to
  empty values, leaving no usable verifier. Empty hashes must never match a
  credential. Keep only non-secret lifecycle metadata. There is no restore,
  enable, edit, role mutation, or rotation after deletion; repeated delete
  is idempotent.
- Plaintext keys exist only transiently for verification and committed
  create/rotate responses. Never write them to logs, traces, audit events,
  event streams, caches, or database records. Return secrets only after
  commit. If a response is lost, keys cannot be retrieved; an authorized
  caller must rotate the affected slot, not replay a stored plaintext result.
  Require TLS outside local development, including internal gRPC hops.

**Management permissions (F-14).** Roles contain granular permissions; the
following are permission names, not hard-coded roles:

| Permission | Allows |
|------------|--------|
| `service-accounts.can-view` | List/read account and slot metadata, including deleted accounts when explicitly requested |
| `service-accounts.can-create` | Create an account and receive its two initial keys |
| `service-accounts.can-edit` | Change display name/description only |
| `service-accounts.can-rotate-keys` | Rotate either individual slot and receive that replacement key |
| `service-accounts.can-disable` | Temporarily disable an account |
| `service-accounts.can-enable` | Re-enable a disabled, non-deleted account |
| `service-accounts.can-delete` | Permanently soft-delete an account and clear both key slots |
| `service-accounts.can-assign-roles` | Add authorized role bindings |
| `service-accounts.can-remove-roles` | Remove authorized role bindings |

Enforce each permission server-side, including on direct gRPC management
calls; UI visibility is not authorization. Resource-scoped permissions limit
which accounts the caller can manage. Editing metadata cannot change roles,
status, or keys. Creation with roles additionally requires
`can-assign-roles`; creating without roles requires only `can-create`.
Assignment must satisfy F-14 delegation rules: possessing
`can-assign-roles` alone must not let a caller grant privileges or scopes they
are not authorized to delegate. Removal requires `can-remove-roles`, not
assignment permission. Service accounts can manage accounts only when
explicitly granted these permissions and scopes; self-management must not
bypass delegation checks. Authorize before generating or returning secrets.

**Authentication and revocation.**
- Accept `Authorization: Bearer <login-name>:cdrom-sa-...` on the UI-facing
  HTTP API through the existing API-key authentication branch. Resolve a
  distinct service-account principal (`service-account:<id>`) with its
  current role bindings; never treat it as a human user or a job token.
  F-26 does not replace worker/agent job-token authentication.
- API middleware delegates verification to the IdP, which calls the
  Database service to check account state and either slot's hash. Read state
  and roles consistently on every request; do not exchange these keys for
  long-lived JWTs or cache positive verification in a way that delays
  revocation. Subsequent requests after committed disable/delete/rotation
  or role removal must observe the change on every API replica.
- Unknown identity, wrong key, disabled account, or deleted account returns
  the same generic 401; an authenticated principal lacking a permission gets
  403. Database/IdP outages fail closed with an explicit service error,
  never an authenticated fallback.
- Apply F-25-style throttling/lockout to the service-account identity across
  both slots, with state separate from human accounts. Lockout is not
  administrative disable; successful verification cannot undo disable.
  Existing WebSocket/stream access must be revalidated and closed when
  access is revoked; an already committed action is not rolled back.

**Scope and API contract.**
- `internal/models` — account, role bindings, two key slots, and lockout
  state; register entities in `All()` and migrate via GORM `AutoMigrate`.
- `proto/cdrom/db/v1/db.proto` + `internal/services/database` — transactional
  create, metadata update, role assignment/removal, disable/enable, delete,
  per-slot rotation, metadata queries, and credential verification RPCs.
  Keep internal hash-bearing models separate from public protobuf messages.
- `proto/cdrom/idp/v1/idp.proto` + `internal/idp` — matching identity and
  management RPCs backed only by the Database service. Accept trusted caller
  context from the authenticated API and enforce the same permission and
  delegation checks; transport trust alone is not management authorization.
- `internal/auth` + `internal/api` — service-account principal support and
  thin HTTP-to-IdP proxies:

| Endpoint | Response / behavior |
|----------|---------------------|
| `POST /api/service-accounts` | 201; account metadata plus `keys: [{slot: 1, key: "..."}, {slot: 2, key: "..."}]` |
| `GET /api/service-accounts` | Metadata only; deleted accounts excluded unless explicitly requested |
| `GET /api/service-accounts/{id}` | Metadata and both slots' non-secret metadata only |
| `PATCH /api/service-accounts/{id}` | Update display name/description only; reject protected fields |
| `POST /api/service-accounts/{id}/keys/{slot}/rotate` | Selected slot metadata plus its plaintext `key` once; invalid slot is 400 |
| `POST /api/service-accounts/{id}/disable` | Disabled account metadata; no secrets |
| `POST /api/service-accounts/{id}/enable` | Enabled account metadata; no secrets |
| `POST /api/service-accounts/{id}/roles` | Assign validated role bindings; no secrets |
| `DELETE /api/service-accounts/{id}/roles/{role_id}` | Remove the specified role binding; no secrets |
| `DELETE /api/service-accounts/{id}` | 204 after tombstone and key clearing commit; never hard-delete |

Mutations use revision checks (stale revisions return 409), with idempotent
delete for an already-deleted account. Other mutations on a tombstone return
410. Create/rotate responses use `Cache-Control: no-store`; GET/list/edit,
errors, disable/enable, role changes, and delete never return plaintext keys.
- `ui/` — list/detail and role management screens, distinct active/disabled/
  deleted states, two individually labeled rotation controls, and confirmation
  for destructive operations. Display/copy both keys after create and only
  the selected replacement after rotate, with a one-time-display warning.
  Do not persist secrets in browser storage, URLs, analytics, or client logs;
  clear transient UI state on dismissal/navigation. Gate each control by its
  corresponding permission, including enable separately from disable.
- F-15 audit events — actor kind/ID, target account, action, key slot when
  applicable, role changes, timestamp, and outcome. Record lifecycle and
  management actions without keys or hashes; preserve attribution after
  deletion.

**Acceptance criteria.**
- [ ] Creating an account persists two independent hashed keys atomically
      and returns exactly two plaintext keys once; both authenticate as the
      same service-account principal with its assigned roles.
- [ ] Account GET/list and every non-key mutation expose metadata only;
      stored hashes and plaintext keys never appear in logs/audit/events.
- [ ] Rotating either slot returns only its new key, invalidates only its old
      key, and leaves the other slot unchanged; slots outside `1`/`2` fail.
- [ ] Disable rejects both keys on all replicas and closes existing live
      access; enable restores the unchanged keys. Rotation while disabled
      does not restore access.
- [ ] Delete retains the account and both slot rows, sets the tombstone, and
      empties both hashes/prefixes atomically. Both keys fail immediately;
      deleted accounts cannot be restored or mutated and names cannot be reused.
- [ ] Each management permission is tested independently on HTTP and gRPC;
      `can-edit` cannot mutate roles/keys/status, and create-with-roles also
      requires assignment permission. Unauthorized calls return no secrets.
- [ ] Role assignment/removal uses F-14 delegation/resource checks and
      affects subsequent authorization without waiting for token expiry.
- [ ] Concurrent rotation/disable/delete tests reject stale revisions and
      prove that deletion cannot race into retaining or recreating a key.
- [ ] SQLite integration tests verify creation rollback, hash-only storage,
      zeroed deletion, constant-time verifier use, lockout isolation,
      cross-replica revocation, and fail-closed dependency errors.
- [ ] UI tests verify permission-specific controls, slot-specific rotation,
      one-time key display, transient secret cleanup, and all lifecycle states.
