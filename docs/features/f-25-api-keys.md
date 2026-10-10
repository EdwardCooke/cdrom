# F-25 · API keys


> Phase 6 — Authentication · [Feature index](../Features.md)


**What.** A user can create one or more **API keys** — long-lived credentials
that authenticate them to the API in place of a JWT. A key is a string
`cdrom-` followed by 64 random alphanumeric characters; only its hash is
stored, and the plaintext is shown to the user exactly once (at creation and
at each rotation). A key carries a **description**, an **expiration date**
(at most one year in the future), and a **pipeline scope** (a set of pipeline
IDs, or all pipelines). The key's effective permissions are the owner's own
permissions **limited to** the key's pipeline scope. A key is presented to the
API as `Authorization: Bearer <username>:<apikey>` (the username is the
user's email) and is accepted anywhere a JWT would be. API keys are available
to any user who has the permission to create one, whether they signed in via
OIDC or username/password (internal); a user with the appropriate role (e.g.
`admin`) can create a key **for another user**. A user can **edit/renew** a
key (change its description, expiration, or pipeline scope) without changing
the key's secret, and can **rotate** it (a new secret is generated and the old
one stops working). A user is **locked out of API-key access** after a
configurable number of failed key attempts — a mechanism that is separate from
(and independent of) any password-attempt lockout.

**Why.** UI sign-in (OIDC or password) is interactive and browser-oriented.
Scripts, CI systems, and other non-interactive clients need a way to
authenticate to the API without driving a full OIDC flow, while still being
bound to a specific user and a limited set of pipelines. API keys provide
that: a per-user, per-pipeline-scoped, expiring, revocable credential that
plugs into the same authorization the user already has.

**Scope.**
- `internal/models` — an `IDPAPIKey` entity (owner user ID, description, key
  hash, key prefix, expiration, pipeline scope) registered in `All()`; per-user
  API-key lockout state on `IDPUser` (`APIKeyFailedCount`,
  `APIKeyLockedUntil`).
- `proto/cdrom/db/v1/db.proto` + `internal/services/database` — an `IDPAPIKey`
  message + `CreateAPIKey` / `GetAPIKey` / `ListAPIKeys` / `UpdateAPIKey` /
  `RotateAPIKey` / `DeleteAPIKey` / `VerifyAPIKey` RPCs. The Database service
  stores the key hash opaquely and never sees the plaintext.
- `proto/cdrom/idp/v1/idp.proto` + `internal/idp` — the IdP's gRPC API-only
  surface gains the API-key operations: `CreateAPIKey` (generate the
  `cdrom-…` secret, hash it, store it, return the plaintext once),
  `GetAPIKey` / `ListAPIKeys` (metadata + prefix, never the plaintext),
  `UpdateAPIKey` (edit description/expiration/pipeline scope without changing
  the secret — "renew"), `RotateAPIKey` (generate a new secret, return it
  once), `DeleteAPIKey`, and `VerifyAPIKey` (check the presented
  `username:apikey` against the stored hash, expiration, and lockout; on a
  miss, increment the user's failure counter and lock them out at the
  configured max). The RPCs are `Unimplemented` when no user store is
  attached. The key directory (hashes, metadata, lockout state) is
  DB-backed, so multiple IdP replicas share it.
- `internal/config` — an `auth.api_key` section: `enabled` (default `true`),
  `max_failures` (the failure count that triggers lockout),
  `lockout_duration` (how long a lockout lasts; `0` = until reset), and an
  optional `pepper` (mixed into the key hash).
- `internal/api` — an `APIKeyClient` (a gRPC proxy to the IdP, like
  `UserPassClient`) and the `/api/api-keys` handlers. Managing **one's own**
  keys requires `api-keys.can-manage` (granted to the built-in user role for
  self-service); acting on **another user's** keys (creating a key for them,
  or getting/editing/rotating/deleting their key) requires the explicit
  `api-keys.can-manage-all` permission, which the admin role grants by
  default. The handlers: `POST /api/api-keys` (create for self, or for
  another user when the caller holds `api-keys.can-manage-all`),
  `GET /api/api-keys` (list the **caller's own** keys),
  `GET /api/api-keys/all` (list **every key in the system** — requires
  `api-keys.can-list-all`, which the admin role grants), `GET /api/api-keys/{id}`,
  `PUT /api/api-keys/{id}` (edit/renew), `POST /api/api-keys/{id}/rotate`,
  `DELETE /api/api-keys/{id}`, and `POST /api/api-keys/lockout/reset` (an
  admin clears a user's API-key lockout). The create and rotate responses
  return the plaintext key once.
- `internal/auth` — the middleware accepts `Authorization: Bearer
  <username>:<apikey>` as an alternative to a JWT: when the bearer value is of
  the form `<username>:cdrom-…` it calls the IdP's `VerifyAPIKey` and, on
  success, establishes the authenticated user (with the user's roles) plus the
  key's pipeline scope in the request context; a miss is a 401. The pipeline
  scope is carried on `auth.User` (e.g. a `PipelineScope` field) so
  authorization can limit the caller to the key's pipelines.

**Acceptance criteria.**
- [x] A user (whether they signed in via OIDC or username/password) can create
      an API key; the response returns the plaintext `cdrom-…` key once, and
      the stored record holds only its hash.
- [x] API keys are available to any user with the permission to create one, on
      both the OIDC and the username/password (internal) sign-in paths.
- [x] A user with the `api-keys.can-manage-all` permission (e.g. `admin`) can
      create an API key for another user; the key belongs to that user and its
      effective permissions are that user's, limited to the key's pipeline
      scope. A user without the permission can only create keys for themselves.
- [x] A user can have any number of API keys.
- [x] A key's expiration date is validated to be at most one year in the
      future; an expired key is rejected.
- [x] `Authorization: Bearer <username>:<apikey>` authenticates the caller as
      the key's owner on any `/api/*` route that accepts a JWT; a JWT and an
      API key are both accepted (both sign-in methods coexist).
- [x] The key's effective permissions are the owner's permissions limited to
      the key's pipeline scope (a key scoped to pipeline X cannot act on
      pipeline Y, even if the owner could).
- [x] A key can be edited/renewed (description, expiration, pipeline scope)
      without changing its secret — the same `cdrom-…` value keeps working.
- [x] A key can be rotated: a new `cdrom-…` secret is generated and returned
      once, and the previous secret stops working.
- [x] The plaintext key is never returned by list/get (only the prefix and
      metadata); it is returned only by create and rotate.
- [x] After the configured number of failed API-key attempts for a user, that
      user is locked out of API-key access (for the configured duration, or
      until reset); password sign-in is unaffected.
- [x] API-key lockout is a separate mechanism from password-attempt lockout
      (distinct counters/state).
- [x] `auth.api_key.enabled: false` disables API-key authentication and the
      `/api/api-keys` endpoints (they respond 501).
- [x] `GET /api/api-keys` returns only the caller's own keys. A user with the
      `api-keys.can-list-all` permission (e.g. `admin`) can call
      `GET /api/api-keys/all` to get a list of **all** API keys in the system;
      any other caller is denied (403). An admin can also reset a user's
      API-key lockout (`POST /api/api-keys/lockout/reset`).
- [x] A user can manage (get/edit/rotate/delete) only their own keys. Acting
      on another user's key requires the `api-keys.can-manage-all` permission
      (granted to `admin` by default); a user without it is denied (403).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **The key is `cdrom-` + 64 random alphanumeric characters; only its hash is
  stored.** The plaintext is generated by the IdP at create/rotate and returned
  to the caller exactly once. The stored value is a SHA-512 hash of the key
  (optionally mixed with a configured pepper), so a database leak does not
  reveal usable keys. Because the key is high-entropy, a fast hash (not
  bcrypt) is used so per-request verification stays cheap.
- **The key is presented as `Authorization: Bearer <username>:<apikey>`.** The
  middleware distinguishes it from a JWT by shape: a bearer value of the form
  `<username>:cdrom-…` is an API-key credential, anything else is a JWT. The
  username is the user's email (the same unique identifier as password login).
  This keeps a single `Authorization` header for all three credential types
  (OIDC JWT, password-login JWT, API key).
- **The real logic lives in the IdP; the API is a thin proxy.** Mirroring
  F-24, the API's `APIKeyClient` forwards create/get/list/update/rotate/delete
  /verify to the IdP's gRPC surface (over mTLS when TLS is configured), so
  only the API can manage or verify keys. The key directory (hashes, metadata,
  lockout state) is persisted through the Database service, so multiple IdP
  replicas share it.
- **Effective permissions = owner's permissions ∩ key's pipeline scope.** The
  key's pipeline scope (a set of pipeline IDs; an empty set means all
  pipelines) is intersected with the owner's own role-based permissions (F-14).
  A key can only narrow, never widen, what the owner can do. The scope is
  carried on the authenticated principal so authorization checks apply it. Until
  F-14 lands, the owner's "permissions" are their coarse role (admin/user) and
  the key's pipeline scope is the additional narrowing.
- **Edit/renew vs rotate.** `UpdateAPIKey` changes the description, expiration,
  and/or pipeline scope without touching the key's secret (the same `cdrom-…`
  value keeps working) — this is "renew". `RotateAPIKey` generates a brand-new
  secret (a new hash), returns it once, and invalidates the previous one. Both
  are supported.
- **API-key lockout is per-user and distinct from password lockout.** Failed
  `VerifyAPIKey` attempts (a wrong key, or a key whose owner does not match the
  presented username) increment the user's `APIKeyFailedCount`; at the
  configured `max_failures` the user's `APIKeyLockedUntil` is set (for
  `lockout_duration`, or permanently when it is `0`) and the counter resets. A
  successful verification clears the counter. While locked, every API-key
  verification for that user fails (401) until the lockout expires or is reset.
  This state is separate from any password-attempt lockout, so brute-forcing
  API keys does not lock a user out of password sign-in, and vice versa. An
  admin can reset a user's API-key lockout.
- **New fields / RPCs.** `IDPAPIKey` model + `IDPUser` lockout fields (db);
  `IDPAPIKey` message + `CreateAPIKey` / `GetAPIKey` / `ListAPIKeys` /
  `UpdateAPIKey` / `RotateAPIKey` / `DeleteAPIKey` / `VerifyAPIKey` RPCs (db);
  the matching IdP gRPC RPCs (idp); the `auth.api_key` config section; the
  `/api/api-keys` HTTP handlers; and the API-key branch of the auth middleware
  (auth).
