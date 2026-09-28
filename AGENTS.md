# Cdrom — Project Context

> **Living document.** Keep this file up to date as the project evolves. When an architectural
> decision is made or changed, update the relevant section here so every agent (human or AI)
> starts from the same shared context.

## Project Overview

Cdrom (continuous delivery runtime, orchestrator and manager) is a pipeline
tool: it defines, schedules, and executes pipeline jobs across distributed
execution targets.

## Architecture

N-tier architecture with the following layers (top to bottom):

1. **UI layer** — React (the only non-Go component)
2. **API / controller layer** — Go. The **single control plane**. It exposes
   the HTTP API consumed by the UI *and* a gRPC API consumed by execution
   targets (workers, agents) and the scheduler. It holds the workers'
   WatchJobs streams, relays job assignments pushed by the scheduler, and
   proxies artifact traffic to the artifacts service. No business logic
   belongs here.
3. **Service layer** — Go services, one per concern. Each service is a
   **standalone process** exposing a **gRPC** interface (protos in `proto/`,
   generated code in `internal/gen/`):
   - **Database** (`cmd/db`) — owns the storage backend (SQLite/PostgreSQL);
     the only component allowed to touch it. All other components read/write
     data through its gRPC API.
   - **Scheduler** (`cmd/scheduler`) — manages job lifecycle and dispatch.
     When a job targets a worker group it pushes it to the API (DispatchJob),
     which fans it out to the live workers. Persists state through the
     Database service.
   - **Artifacts** (`cmd/artifacts`) — job artifact storage and retrieval
     (filesystem-backed, streamed uploads/downloads).
   - **IdP** (`cmd/idp`) — a local OIDC identity provider that acts as a JWT
     issuer for the API's OIDC authentication. It is an HTTP service (not
     gRPC) serving the discovery doc, JWKS, authorization, and token
     endpoints, and it rotates its RSA signing key automatically (see
     Authentication & Live Events).
   - More services will be added as concerns emerge.
4. **Execution layer** — where jobs actually run (see Execution Model below).

**Topology rule:** execution targets (workers, agents) talk **only** to the
API. The API talks to everything else (database, scheduler, artifacts). There
is no central logs service — every process logs locally to stdout or a file
(see Logging below).

### gRPC Conventions

- All inter-component communication is gRPC. When a TLS CA is configured
  (see `TLSConfig` in `internal/config`), every connection uses **mutual
  TLS**: servers present their certificate and require clients to present one
  signed by the shared CA; clients verify the server against the CA. When no
  CA is configured, connections fall back to plaintext (suitable for local
  development without certs).
- mTLS credentials are built in `internal/grpcutil` (`ServerCreds` for
  servers, `ClientCreds` for clients) from the `tls` section of the config.
- Proto definitions live in `proto/cdrom/<service>/v1/`; generated Go code
  is committed under `internal/gen/` (regenerate with `make proto`).
- Service implementations live in `internal/services/<service>/server.go` and
  implement the generated `*Server` interface (embedding `Unimplemented*Server`).
- Shared gRPC plumbing (client dial, serve-until-signal, TLS credentials) is
  in `internal/grpcutil`.
- All service addresses and TLS certificate paths are configured via the
  config file / environment variables (see `internal/config`); defaults target
  a local development setup on localhost.

### Execution Model

Two kinds of execution targets, both of which talk **only** to the API:

- **Long-lived workers** — resident processes on deployment targets. Workers
  are **targetable by group**: a job can be dispatched to all (or a selection
  of) workers in a named group. A worker registers with the API and holds a
  WatchJobs stream open; the API pushes JobAssignments down the stream.
- **Ephemeral agents** — short-lived processes that spin up in **Kubernetes**
  when a one-off job runs, execute it, and terminate. An agent fetches its
  job from the API and reports status back to the API.

**Job execution spec.** A job carries a declarative `JobSpec`: an ordered
list of steps. Each step is **agnostic about how it runs**: a `type` field
selects a step handler, and the handler reads everything it needs from the
common fields (`workdir`, `env`, `timeout`) and from `params`. Handler-specific
settings live in `params` (a map of name → value, where a value is either a
scalar string or a list of strings), not in the step's own fields, so a new
step type can be added without changing the spec schema. The built-in
**`shell`** handler (also the default when `type` is empty) runs a command —
directly, or through a user-chosen shell — reading its `command`, `args`, and
`shell` from `params`. The spec is a **per-run snapshot** stored on the `Job`
row (a JSON `text` column via GORM's `serializer:json`), so a pipeline edit
never changes what a past run did. The canonical spec type is
`cdrom.db.v1.JobSpec`; the API and scheduler protos reference it rather than
redefining it.

Both targets execute the spec through the shared `internal/executor`
(`executor.Execute(ctx, spec, logger)`): steps run in order and the job fails
on the first step that errors. A job with no spec is a no-op that succeeds.

**Step types & handlers.** The executor (`internal/executor`) is the generic
dispatch engine: it selects an `executor.StepHandler` by the step's `type`
(empty → the handler registered under `executor.DefaultType`) and runs it. The
concrete handlers live in `internal/stephandlers`; the built-in **`shell`**
handler registers itself under `executor.DefaultType` at package init, so a
step with no type runs through it. A target or plugin adds new step types with
`executor.RegisterStepType(name, handler)` (e.g. `ansible`, `terraform`,
`argo`). A step whose type is not registered on the target fails the job with
a clear error. `params` carries handler-specific configuration so a new step
type can be added without changing the spec schema. The worker and agent
blank-import `internal/stephandlers` so the built-in handlers are registered
before any job runs. This is the seam for the later plugin architecture:
writing a new step type is a matter of registering a handler, not changing the
executor core.

**Shell contract (shell handler):** the shell handler reads its `command`
(string param), `args` (list param), and `shell` (string param) from the
step's `params`. The command is executed **directly by the target OS — no
implicit shell**. This is what makes a spec portable across Windows and Linux
(no `&&`, pipes, globbing, or `$VAR` expansion). A step that needs shell
behavior must invoke a shell explicitly (`sh -c ...` on Linux, `cmd /c ...` on
Windows). Step stdout/stderr are inherited from the target's own stdout/stderr
(local logging); streaming output to the API is a later feature (F-02). A
per-step `timeout` is enforced by the target via a derived context; a
scheduler-side watchdog for dead targets is a later feature (F-03).

**Shell override (shell handler):** a step may set the `shell` param to run
through a user-chosen interpreter instead of executing `command` directly.
When `shell` is set the target executes `<shell> <args> <command>` — `args`
are the shell's own flags and `command` is passed as the final argument (e.g.
`shell: "pwsh"`, `args: ["-NoProfile", "-Command"]`,
`command: "Get-ChildItem"`). This is the portable way to opt a step into shell
behavior with a specific shell rather than a platform default. When `shell` is
empty the step runs `command` directly, preserving the no-implicit-shell
contract.

### Logging

There is no central logs service. Every process logs locally via
`internal/logging` to **stdout** by default, or to the file named by
`CDROM_LOG_FILE` when that variable is set (append mode). Format and level
are still controlled by `CDROM_LOG_FORMAT` and `CDROM_LOG_LEVEL`.

## Tech Stack & Constraints

- **Everything except the frontend is Go** (API, services, workers, ephemeral agents).
- **Frontend is React.**
- **Database:**
  - **SQLite** for local development.
  - **PostgreSQL** for deployments.
  - **ORM: GORM** (`gorm.io/gorm`) with `gorm.io/driver/postgres` and
    **`github.com/glebarez/sqlite`** — the pure-Go GORM SQLite driver. The official
    `gorm.io/driver/sqlite` requires cgo and breaks Windows cross-compiles, which is
    not acceptable per the cross-platform requirements below.
  - **Migrations: `AutoMigrate`** — schema is derived from the GORM models in
    `internal/models` (register new entities in `models.All()`). No hand-written SQL
    or migration scripts.
  - All persistence goes through the database service, which abstracts the backend so
    the driver can be swapped. Code outside the database service must never talk to a
    specific driver directly.

## Cross-Platform Requirements

- Both long-lived workers **and** ephemeral agents must run on **Windows and Linux**.
- Use portable Go (`filepath`, `os` env handling); avoid hard-coding path separators,
  Unix-only syscalls, or shell command assumptions without a Windows equivalent.
- Windows path/line-ending handling is a known failure mode — review code touching
  file paths for portability.
- Job steps are type-agnostic: the built-in `shell` handler executes a command
  directly (no implicit shell) — see the shell contract in the Execution Model.
  Never rely on shell features in a step's `command`/`args` params; invoke a
  shell explicitly when shell behavior is required, either by naming the shell
  as the `command` param or by setting the step's `shell` param (e.g. `pwsh`).
  New step types (e.g. `ansible`, `terraform`) are registered handlers and must
  also be portable across Windows and Linux.

## Repository Layout

Standard Go layout; the frontend lives outside the Go module:

```
cmd/
  api/        API/controller service entrypoint (HTTP for UI + gRPC for workers/agents/scheduler)
  db/         database service entrypoint (gRPC)
  scheduler/  scheduler service entrypoint (gRPC)
  artifacts/  artifacts service entrypoint (gRPC)
  worker/     long-lived worker entrypoint
  agent/      ephemeral Kubernetes agent entrypoint
internal/
  api/        API/controller layer: HTTP routing + gRPC control-plane server (worker hub, artifact proxy, event hub + /api/ws WebSocket)
  auth/       OIDC authentication (provider, PKCE, signed session cookie, endpoints, middleware)
  config/     shared configuration loading for all binaries
  models/     GORM entities (single source of truth for the schema)
  gen/        generated gRPC/protobuf Go code (committed; `make proto`)
  grpcutil/   shared gRPC plumbing (client dial, serve-until-signal, TLS credentials)
  logging/    centralized logging (stdout or CDROM_LOG_FILE)
  executor/   shared job-step executor: the generic dispatch engine (selects a
              StepHandler by step type, enforces per-step timeout; used by worker + agent)
  stephandlers/ built-in step handlers (the "shell" handler, registered under
              executor.DefaultType); a target or plugin adds more via
              executor.RegisterStepType
  services/
    data/     pipeline/job data service
    artifacts/ job artifact storage & retrieval (gRPC server)
    scheduler/ job lifecycle + dispatch (gRPC server)
    database/ storage backend abstraction + gRPC server (SQLite / PostgreSQL)
  worker/     long-lived worker implementation
  agent/      ephemeral agent implementation
  idp/        local OIDC identity provider (JWT issuer, auto key rotation;
              signing keys + auth codes persisted through the Database service)
proto/        protobuf definitions (proto/cdrom/<service>/v1/)
ui/           React frontend (separate from the Go module)
  tests/      UI tests
docs/         documentation
scripts/      build/release scripts (genproto.sh regenerates gRPC code)
```

Rules:
- All shared Go code goes under `internal/` — no `pkg/` or importable top-level packages.
- `cmd/*` packages contain only thin `main` entrypoints; real logic lives in `internal/`.
- The UI is not part of the Go module; it has its own tooling under `ui/`.

## Build & Tooling

- `Makefile` provides: `make build` (host binaries → `./bin`), `make proto`
  (regenerate gRPC code), `make test`, `make vet`, `make fmt`, `make clean`.
- Build with `go build ./...`; verify with `go vet ./...`.
- **gRPC codegen:** `make proto` runs `scripts/genproto.sh`, which invokes
  `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc`. Generated code is
  committed under `internal/gen/` — regenerate after editing any `.proto`.
- **Cross-compile check:** `GOOS=windows GOARCH=amd64 go build ./cmd/...` must stay
  green — worker and agent ship for Windows. Never add cgo dependencies.

## Running Locally

All service addresses are configured via environment variables (see
`internal/config`); the defaults below target a local development setup where
every service runs on localhost.

Every binary also accepts a `--config-file <path>` flag pointing at a YAML
file (see `config.example.yaml` for the schema). Precedence, lowest to
highest: built-in defaults → config file → environment variables.

| Service     | Default address | Env var (listen) | Env var (address of) |
|-------------|-----------------|------------------|----------------------|
| db          | 127.0.0.1:7101  | `CDROM_LISTEN_ADDR` | — |
| scheduler   | 127.0.0.1:7102  | `CDROM_LISTEN_ADDR` | `CDROM_DB_ADDR`, `CDROM_API_ADDR` |
| artifacts   | 127.0.0.1:7103  | `CDROM_LISTEN_ADDR` | — |
| api (gRPC)  | 127.0.0.1:7105  | `CDROM_LISTEN_ADDR` | db, scheduler, artifacts |
| api (HTTP)  | 127.0.0.1:8080  | `CDROM_API_HTTP_ADDR` | — |
| idp         | 127.0.0.1:7104  | `CDROM_LISTEN_ADDR` | — |
| worker      | —               | —                | `CDROM_API_ADDR` |
| agent       | —               | —                | `CDROM_API_ADDR` |

Other notable variables: `CDROM_DB_BACKEND` (`sqlite`|`postgres`),
`CDROM_DB_SQLITE_PATH`, `CDROM_DB_POSTGRES_DSN`,
`CDROM_ARTIFACTS_ROOT`, `CDROM_WORKER_NAME`, `CDROM_WORKER_GROUP`,
`CDROM_AGENT_JOB_ID`, `CDROM_API_ADDR`, `CDROM_API_HTTP_ADDR`, the
mTLS certificate paths `CDROM_TLS_CA_FILE`, `CDROM_TLS_CERT_FILE`,
`CDROM_TLS_KEY_FILE` (equivalents of the `tls` section in the config file),
and the auth variables `CDROM_AUTH_ENABLED` (`true`/`1`),
`CDROM_AUTH_ISSUER`, `CDROM_AUTH_CLIENT_ID`, `CDROM_AUTH_REDIRECT_URL`,
`CDROM_AUTH_COOKIE_SECRET`, `CDROM_AUTH_COOKIE_DOMAIN` (equivalents of the
`auth` section in the config file), and the IdP variables `CDROM_IDP_ISSUER`,
`CDROM_IDP_KEY_LIFETIME`, `CDROM_IDP_ROTATE_BEFORE`,
`CDROM_IDP_TOKEN_LIFETIME`, `CDROM_IDP_CHECK_INTERVAL` (equivalents of the
`idp` section in the config file), and the job-token auth variables
`CDROM_GRPC_AUTH_ENABLED` (`true`/`1`), `CDROM_GRPC_AUTH_IDP_ADDR`,
`CDROM_GRPC_AUTH_AUDIENCES` (comma-separated; equivalents of the
`grpc_auth` section in the config file).

Logging is centralized in `internal/logging`: every binary calls
`logging.New()`. `CDROM_LOG_FORMAT` selects the handler — `json` (default)
or `text` for raw text — `CDROM_LOG_LEVEL` sets the minimum level
(`debug`, `info` (default), `warn`, `error`), and `CDROM_LOG_FILE` names a
file to write logs to (append mode) instead of stdout. Callers needing a
custom handler use `logging.NewHandler(writer)` or
`logging.NewWithHandler(handler)`.

A minimal local run (plaintext, no certs):

```sh
make build
./bin/db          # owns the SQLite file (cdrom.db) and runs migrations
./bin/scheduler   # dials db and the api
./bin/artifacts   # stores files under ./artifacts
./bin/api         # gRPC on :7105 + HTTP on :8080, dials db/scheduler/artifacts
./bin/worker      # registers with the api, watches for jobs
```

To exercise the full OIDC login flow locally, also run the local identity
provider and point the API at it:

```sh
./bin/idp         # OIDC IdP / JWT issuer on :7104 (auto key rotation)
# in the API's config: auth.enabled: true, auth.issuer: http://127.0.0.1:7104,
#   auth.client_id: cdrom-ui,
#   auth.redirect_url: http://127.0.0.1:8080/api/auth/callback,
#   auth.cookie_secret: <random>
```

To run with mTLS, generate a local certificate set and point every binary at
it (the same config file works for services and clients):

```sh
make certs        # writes certs/ca.crt, certs/cert.crt, certs/cert.key
# add to your config file (see config.example.yaml):
#   tls:
#     ca_file:   certs/ca.crt
#     cert_file: certs/cert.crt
#     key_file:  certs/cert.key
./bin/db --config-file config.yaml
# ... and so on for the other services, api, and worker
```

## Authentication & Live Events

The API's UI-facing HTTP surface (and its WebSocket endpoint) supports OIDC
authentication, implemented in `internal/auth`.

- **Disabled by default.** With no `auth` config the API is open — no identity
  provider is required and a local run just works. Enable it with
  `auth.enabled: true` (or `CDROM_AUTH_ENABLED=true`); `issuer`,
  `client_id`, `redirect_url`, and `cookie_secret` are then required and
  validated at startup.
- **Flow:** OIDC authorization-code grant with PKCE (via
  `github.com/coreos/go-oidc/v3`). `GET /api/auth/login` bounces the browser to
  the identity provider (the PKCE verifier is packed into the OIDC `state`
  value, so no server-side pre-auth store is needed); `GET /api/auth/callback`
  exchanges the code, validates the ID token, and sets a signed session cookie
  (`cdrom_session`, HMAC-SHA256, HttpOnly, 12h); `GET /api/auth/logout`
  clears it. The session cookie is marked Secure when TLS is configured.
- **Middleware:** when auth is enabled, every `/api/*` request (including the
  WebSocket upgrade) must carry a valid session cookie or it gets a 401 JSON
  response; the authenticated user is available to handlers via
  `auth.UserFromContext`. When disabled the middleware is a passthrough.
- **Live events:** `GET /api/ws` is a WebSocket endpoint (gorilla/websocket).
  On connect the client receives a state snapshot (jobs + workers); afterwards
  it receives one message per event. Event types: `job_status` (job id +
  status) and `worker` (worker name/group + action `registered` /
  `deregistered` / `watching`). Events are published from the gRPC server
  (worker lifecycle, job status reports) and the HTTP handlers (job submit /
  cancel) through a shared `EventHub` (`internal/api/events.go`); slow
  subscribers have events dropped and resync from the snapshot on reconnect.
- **Local OIDC IdP (`cmd/idp`):** a standalone HTTP service that acts as a
  JWT issuer so a local run can exercise the full OIDC flow without an
  external identity provider. It serves `/.well-known/openid-configuration`,
  `/jwks`, `/auth` (authorization-code + PKCE), and `/token` (mints RS256 ID
  tokens). It signs with an RSA key and **rotates it automatically** when the
  key is within `idp.rotate_before` of its expiry (checked every
  `idp.check_interval`); the JWKS keeps serving predecessor keys until they
  expire, so clients keep verifying tokens signed with an older key during the
  overlap window. The issuer is derived from the request host, so it works on
  any port. To use it, point the API's `auth.issuer` at the IdP's address
  (default `127.0.0.1:7104`) and set `auth.enabled: true`.
  - **State lives in the Database service.** The IdP persists its signing
    keyring *and* its OIDC authorization codes through the Database service
    (`db_address`), not on its local filesystem. This makes the IdP
    horizontally scalable: multiple IdP replicas share the same keys and codes
    and can run behind a load balancer. The IdP therefore requires the db
    service to be running (start `./bin/db` before `./bin/idp`).

### Job Tokens (gRPC surface)

Separate from the UI's OIDC session, the API's **gRPC surface** (workers and
agents) supports **job-token authentication**, implemented in
`internal/api/jobsauth.go`.

- **Disabled by default.** With no `grpc_auth` config the API accepts gRPC
calls without a job token. Enable it with `grpc_auth.enabled: true` (or
`CDROM_GRPC_AUTH_ENABLED=true`); `idp_address` and `audiences` are then
required and validated at startup.
- **Minting:** the API is the **only** caller of the IdP's `job_token` grant
on `/token`. It authenticates to the IdP with its **mTLS client
certificate** (the same `tls` section used for gRPC) — there is no shared
secret. When the IdP serves TLS (its `tls` section is set) the `job_token`
grant rejects any TLS client that did not present a client certificate, so
only a CA-signed client (the API) can mint; in plaintext mode the grant is
open. The API mints one short-lived RS256 token per job: on dispatch it is
delivered to workers inside the `JobAssignment` (gRPC field `token`), and on
`GetJob` it is returned in the `Job` for ephemeral agents. Claims: `iss`,
`sub: "job:<jobID>"`, `aud` (the configured audiences), `exp`, `iat`,
`job_id`, `pipeline_id`, `target_group`, `token_type: "job"`.
- **Exchange:** a job can request a **new token for a different audience**
(e.g. an outside resource it must call) via the `ExchangeJobToken` RPC. The
caller presents its existing job token (scoped to the job) and a target
`audience`; the API mints a fresh token from the IdP stamped for that
audience. Any audience is accepted — the IdP stamps whatever is requested.
- **Verification:** when enabled, `ReportJobStatus` and the artifact RPCs
(`UploadArtifact`, `DownloadArtifact`, `GetArtifact`, `ListArtifacts` with a
`job_id`, `DeleteArtifact`) require a `Bearer <token>` in the gRPC
`authorization` metadata. The API verifies the token against the IdP's JWKS
(OIDC discovery against `grpc_auth.idp_address`), checks that one of the
configured audiences is present, and rejects the call unless the token's
`job_id` matches the job the call targets (missing/invalid token →
`Unauthenticated`, wrong job → `PermissionDenied`). Workers and agents
present the token via `grpcutil.WithBearerToken`.
- **Local run:** to exercise it, run `./bin/idp` with `idp.audiences` set,
and point the API's `grpc_auth` at the same IdP with matching `audiences`.
To exercise the mTLS mint path, run both with the `tls` section set (see
`make certs`) so the API presents its client certificate to the IdP.

## Conventions

*Fill in as they are established.*
  The database service routes GORM logs through slog via the adapter in `internal/services/database/logger.go`.
- Errors: wrap with `fmt.Errorf("context: %w", err)`; no panics across service boundaries.
- Testing: integration-style tests use SQLite via `database.Open` with a path under
  `t.TempDir()` (or `:memory:`) — never require a live Postgres. initialize a `slog` JSON handler).
- Variables names should make sense except for where there are already go based idioms like err and ok.
- TODO: error handling strategy
- TODO: testing conventions (unit vs integration, sqlite for tests?)

## Open Decisions

*Track questions that are not yet answered.*

- Go module path is the placeholder `cdrom`; update `go.mod` and all imports when the
  repo is hosted (e.g. `github.com/<owner>/cdrom`).
