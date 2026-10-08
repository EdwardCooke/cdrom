# F-24 · Username/password authentication


> Phase 6 — Authentication · [Feature index](../Features.md)


**What.** A password-based sign-in path alongside OIDC, so the UI can be
exercised in local development without a full OIDC client. The API exposes
unauthenticated `POST /api/login` and `POST /api/register` (plus admin-only
`/api/users` for role management) that **proxy to the IdP over gRPC** (the
IdP's API-only surface, over mTLS when TLS is configured), where the real logic
lives: the IdP verifies the password against a stored bcrypt hash and, on
success, mints an OIDC token for the user (stamped with the user's roles). The
API is a pure proxy — it never sees the password hash or verifies credentials
itself. The returned token is the same OIDC token the API already verifies on
every other request (against the IdP's JWKS).

**Why.** The OIDC authorization-code + PKCE flow is the right production path
but is awkward to drive from a browser-less local setup. A password login
(`register` the first user, then `login`) gives a one-command way to get a
token for the UI and curl. It is **enabled by default** for local development
and can be turned off (`auth.userpass_enabled: false`) when an external
identity provider is used.

**Scope.**
- `proto/cdrom/db/v1/db.proto` + `internal/services/database` — `IDPUser`
  table (GORM `AutoMigrate`) + `CreateUser` / `GetIDPUser` / `ListIDPUsers` /
  `UpdateIDPUser` / `DeleteIDPUser` RPCs. The user directory (profiles,
  bcrypt password hashes, roles) is persisted through the Database service, so
  multiple IdP replicas share it. F-28 makes `DeleteIDPUser` a soft delete
  (tombstone, never hard-delete) and adds SCIM provisioning of this directory
  from an external IdP.
- `proto/cdrom/idp/v1/idp.proto` + `internal/idp` — the IdP's gRPC API-only
  surface (`GRPCServer`): `Register` (hash the password with bcrypt, store the
  user; the first user becomes an `admin`, later users get the default `user`
  role), `Login` (verify the password, mint an OIDC token stamped with the
  user's roles), and `ListUsers` / `CreateUser` / `UpdateUser` / `DeleteUser`
  (role management). The RPCs are disabled (`Unimplemented`) when no user store
  is attached. The user store is DB-backed + in-memory.
- `internal/config` — `auth.userpass_enabled` (default `true`;
  `CDROM_AUTH_USERPASS_ENABLED`).
- `internal/api` — `UserPassClient` (a gRPC proxy to the IdP) and the
  `/api/login`, `/api/register`, and `/api/users` handlers. The login/register
  endpoints are exempted from the auth middleware (reachable without a token);
  the user-management endpoints require an authenticated caller with the
  `admin` role.
- `internal/auth` — `User.Roles` + `User.HasRole` (roles are read from the
  token's `roles` claim); `MiddlewareExempt` so the sign-in entry points stay
  reachable while the rest of `/api/*` still requires a token.

**Acceptance criteria.**
- [x] `POST /api/register` with `first_name`/`last_name`/`email`/`password`
      creates a user (the password is stored only as a bcrypt hash, never the
      plaintext); the first user registered is given the `admin` role, later
      users the default `user` role.
- [x] `POST /api/login` with valid credentials returns an OIDC `access_token`
      (plus the user's profile and roles) that the API verifies against the
      IdP's JWKS; a wrong password or unknown user is a 401.
- [x] The token's `roles` claim carries the user's roles and is surfaced via
      `auth.User.Roles`.
- [x] `GET/POST/PUT/DELETE /api/users` manage users and roles and require an
      authenticated caller with the `admin` role (403 for a non-admin, 401 for
      an unauthenticated caller).
- [x] `auth.userpass_enabled: false` disables the endpoints (they respond 501)
      and the API works with no IdP user store.
- [x] The user directory is shared across IdP replicas via the Database
      service (like the IdP's signing keys and auth codes).
