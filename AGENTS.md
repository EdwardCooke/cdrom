# Cdrom — Project Context

> **Living document.** Keep this file up to date as the project evolves; when an
> architectural decision changes, update the relevant section so every agent
> (human or AI) starts from the same shared context.
>
> **Deep detail lives in `docs/`:** `docs/Architecture.md` (system design),
> `docs/features/` (one file per feature, F-01…F-28, each with acceptance
> criteria), `docs/Features.md` (the summary index: baseline, per-phase feature
> list, build order, checklist), and `docs/HighAvailability.md` (the F-23
> control plane). This file is the concise orientation + conventions; consult
> the docs for the full spec of any feature.

## Project Overview

Cdrom (continuous delivery runtime, orchestrator and manager) is a pipeline
tool: it defines, schedules, and executes pipeline jobs across distributed
execution targets. Everything except the React frontend is Go.

## Architecture

N-tier architecture, top to bottom:

1. **UI layer** — React (the only non-Go component).
2. **API / controller layer** — Go. The **control plane** (one or more
   replicas, F-23). Exposes the HTTP API to the UI *and* a gRPC API to execution
   targets (workers, agents). Holds workers' `WatchJobs` streams, tails the
   shared event log (F-23) to fan job assignments / cancellations / status out
   to local workers and UI clients, and proxies artifact traffic. No business
   logic lives here.
3. **Service layer** — standalone Go services, one per concern, each a gRPC
   server (protos in `proto/`, generated code in `internal/gen/`):
   - **Database** (`cmd/db`) — owns the storage backend (SQLite/PostgreSQL); the
     only component that touches it. Everyone else reads/writes through its gRPC API.
   - **Scheduler** (`cmd/scheduler`) — job lifecycle + dispatch. Talks **only**
     to the Database service (F-23): it appends "assignment" events to the shared
     event log that every API pod tails and fans out to live workers. Runs the
     background loops below on exactly one replica via **leader election** (a
     portable lease on the Database service): watchdog (F-03), retry (F-04),
     dependency resolver (F-06/F-08), run-status (F-07), job-status (fan-out),
     and cron + event triggers (F-09). Also `CreateRun` (F-07) and `TriggerRun` (F-09).
   - **Artifacts** (`cmd/artifacts`) — a general-purpose **namespaced file store**
     (filesystem-backed, streamed). A job's artifacts/logs use the job id as the
     namespace; it also stores streamed job logs. The store is an interface
     (`internal/services/artifacts/store.go`); the kind is configurable
     (`CDROM_ARTIFACTS_STORE`, default `filesystem`) so S3 / Azure Blob can be added later.
   - **IdP** (`cmd/idp`) — a local OIDC identity provider / JWT issuer. It serves
     the standard OIDC surface over **HTTP** (discovery, JWKS, `/auth`, the
     `authorization_code` token grant — a browser-based protocol that cannot be
     gRPC) and its **API-only** surface over **gRPC** (job-token minting and user
     management), which the API dials over mTLS so only the API can reach it.
     It auto-rotates its RSA key. Its state (keys + auth codes + users) lives in
     the Database service.
4. **Execution layer** — where jobs actually run (see Execution Model).

**Topology rule:** execution targets (workers, agents) talk **only** to the API.
The API talks to everything else (database, scheduler, artifacts). The scheduler
talks **only** to the Database service — job delivery to workers flows through
the shared event log that every API pod tails. There is no central logs service;
every process logs locally (see Logging).

### gRPC Conventions

- All inter-component communication is gRPC. With a TLS CA configured
  (`TLSConfig` in `internal/config`) every connection uses **mutual TLS**;
  without a CA it falls back to plaintext (local dev).
- mTLS creds are built in `internal/grpcutil` (`ServerCreds` / `ClientCreds`).
- Protos in `proto/cdrom/<service>/v1/`; generated code is committed under
  `internal/gen/` (regenerate with `make proto`).
- Service impls live in `internal/services/<service>/server.go` and implement
  the generated `*Server` interface (embedding `Unimplemented*Server`).
- Addresses + TLS paths come from config / env vars (see `internal/config`);
  defaults target localhost.

### Execution Model

Two execution targets, both talking **only** to the API:

- **Long-lived workers** — resident processes, targetable **by group**. A worker
  registers with the API and holds a `WatchJobs` stream open; the API pushes
  `JobAssignment`s down it.
- **Ephemeral agents** — short-lived processes spun up in Kubernetes for a
  one-off job; they fetch the job from the API and report status back.

**Job spec.** A job carries a declarative `JobSpec`: an ordered list of steps.
Each step is agnostic about how it runs — a `type` field selects a step handler,
and the handler reads everything it needs from the common fields (`workdir`,
`env`, `timeout`) plus `params` (a name→value map; a value is a scalar string or
a list of strings). Handler-specific settings live in `params`, so a new step
type needs no schema change. The spec is a **per-run snapshot** stored on the
`Job` row (JSON via GORM `serializer:json`), so editing a pipeline never changes
what a past run did. Canonical type: `cdrom.db.v1.JobSpec`.

Both targets run the spec through the shared `internal/executor`
(`executor.Execute(ctx, spec, logger)`): steps run in order, the job fails on the
first step that errors, and a job with no spec is a no-op that succeeds.

**Step handlers.** The executor is a generic dispatch engine: it selects an
`executor.StepHandler` by the step's `type` (empty → `executor.DefaultType`) and
runs it. Built-in handlers live in `internal/stephandlers`: **`shell`**
(registered under `executor.DefaultType`), **`token_exchange`**, and
**`approval`** (F-13). A target or plugin adds new types with
`executor.RegisterStepType(name, handler)` (e.g. `ansible`, `terraform`). A step
whose type is unregistered fails the job with a clear error. Worker and agent
blank-import `internal/stephandlers` so built-ins register before any job runs —
this is the seam for the later plugin architecture.

**Shell contract.** The `shell` handler reads `command` (string), `args` (list),
and `shell` (string) from `params`. The command is executed **directly by the
target OS — no implicit shell** (no `&&`, pipes, globbing, or `$VAR` expansion),
which is what makes a spec portable across Windows and Linux. A step that needs
shell behavior must invoke a shell explicitly (`sh -c …` on Linux, `cmd /c …` on
Windows) — either by naming the shell as `command` or by setting the `shell`
param (e.g. `shell: "pwsh"`, `args: ["-NoProfile","-Command"]`). Step
stdout/stderr go to the target's local logs and, when a log sink is set, are
streamed to the API in near-real-time (F-02, `internal/logstream`); the API
persists them to the artifacts service and fans them out to the UI as `job_log`
events.

**Secrets (F-12).** Pipelines declare named secrets (like `params`); values are
encrypted with a single AES-256-GCM key held in the API's config and stored
opaquely (ciphertext) on the pipeline, its version snapshots, and each run's job
instances. The API is the **only** component that can decrypt: it decrypts at
dispatch and hands plaintext to the target via the API `Job.secrets` map (never
persisted), which the executor exposes to specs as `{{ .secrets.name }}`.
Nonces come from a DB-backed counter (`NextSecretNonce`), so all API replicas
share one key + one sequence. The API also redacts secret values from job logs
before they reach the artifacts store / UI. An authenticated user can encrypt a
value directly via `POST /api/secrets/encrypt` (body `{"value": "…"}` →
`{"ciphertext": "…"}`), which returns the ciphertext a pipeline's secret
declaration stores.

**Feature index.** The detailed spec, acceptance criteria, and RPC/field changes
for each feature are in `docs/features/` (one file per feature; `docs/Features.md`
is the summary index). Summary:

| ID | Feature |
|----|---------|
| F-01 | Job execution spec (steps, `type`→handler, `params`, per-run snapshot) |
| F-02 | Job log streaming (target→API→artifacts→UI `job_log`, replayable) |
| F-03 | Timeouts (job + per-step; `timed_out` status; scheduler watchdog) |
| F-04 | Retry & re-run (`max_attempts`/`backoff`; retry loop; `POST /api/jobs/{id}/rerun`) |
| F-05 | Cancellation (persist + signal target to stop; late reports can't clobber terminal status) |
| F-06 | Skip / step `condition` / `ignore_failed` / step `outputs` / job `depends_on` (dependency resolver) |
| F-07 | Pipeline runs (`PipelineRun` + per-run job instances; atomic `CreateRun`; derived run status) |
| F-08 | Job dependencies DAG (`needs` by stable `key`; validated at save; reuses F-06 resolver) |
| F-09 | Triggers (cron / webhook / event; atomic `TriggerRun`; cron + event loops; webhook endpoint) |
| F-10 | Parameters & variables (pipeline `Parameter`s; run values; Go-template interpolation) |
| F-11 | Pipeline versioning (immutable `PipelineVersion` snapshots; a run binds to the version active at start; run against a specific version) |
| F-12 | Secrets management (pipeline-level named secrets; AES-256-GCM with a configured key; DB-backed nonce counter; API decrypts at dispatch; log redaction) |
| F-13 | Approval gates (`approval` step; `awaiting_approval` status; `POST /api/jobs/{id}/approve` / `.../reject`; decision + actor + reason persisted) |
| F-14 | Roles & permissions (RBAC) (fine-grained permissions, built-in + custom roles with composition, user/service-account bindings optionally scoped per pipeline; deny-by-default; roles live in the Database service, evaluated in the API via `internal/authz` with a cache invalidated by `role_change` events on the shared event log; JWT claim mapping via `auth.roles`; delegation rules; synthetic admin when auth is off; backend-only — UI screens deferred) |
| F-15…F-22 | Roadmap: audit log, queueing, environments, notifications, artifact promotion, observability, config-as-code, post-deploy verification (not yet implemented) |
| F-23 | High availability (shared event log, hybrid push+pull dispatch, leader election, cross-pod logs — see `docs/HighAvailability.md`) |
| F-24 | Username/password authentication (unauthenticated `POST /api/login` + `POST /api/register` proxied to the IdP over gRPC; bcrypt password hashes + roles in the Database service; admin role management via `/api/users`; enabled by default for local dev) |
| F-25 | API keys (per-user `cdrom-`+64-char credentials, hashed at rest, presented as `Bearer <username>:<apikey>`; description + ≤1-year expiration + per-pipeline scope; edit/renew without changing the secret, or rotate; effective permissions = owner's ∩ key's scope; a separate per-user API-key lockout; real logic in the IdP, proxied by the API) |

| F-26 | Roadmap: service accounts (role-assignable non-human identities; exactly two individually rotatable API keys generated and hashed by the Database service, returned once on create/rotate for UI display; disable/enable; permanent soft deletion with both key slots cleared; granular create/edit/rotate/disable/enable/delete/assign-role/remove-role management permissions; not yet implemented) |
| F-27 | Roadmap: approval groups (named sets of users an `approval` step (F-13) references via an `approvers` param instead of individual users; a decision is accepted only from a member of a referenced group / named individual who also holds the F-14 approve/reject permission; membership resolved at decision time; optional role references and N-of-M quorum; not yet implemented) |
| F-28 | Roadmap: SCIM 2.0 user provisioning (a service-provider endpoint on the built-in IdP so an external IdP like Azure AD/Entra ID provisions, updates, and deprovisions users and groups into the IdP's user directory; SCIM Groups map to F-14 roles / F-27 approval groups; **users are soft-deleted, never hard-deleted**, so audit (F-15) / approval (F-13) / trigger (F-07/F-09) actor references stay resolvable after deprovisioning; provisioning is mutually exclusive — with SCIM **enabled** the corporate IdP is the sole source of users (no auto-provisioning on login), with SCIM **disabled** the IdP auto-provisions users just-in-time on first OIDC login; not yet implemented) |

Cross-cutting execution concepts (full detail in `docs/features/` /
`docs/Architecture.md`):

- **Fan-out (worker groups).** A job targeting a group runs on **every** worker
  in it; each worker's run is a separate `JobExecution`. The job row's status is
  *derived* by the scheduler **job-status loop** from the per-worker outcomes of
  **alive** workers, per the job's `failure_mode` (`all` default / `best_effort`
  / `any`). Worker restarts abandon stale executions; the job-level watchdog skips group jobs.
- **Step barrier.** A group job with `step_barrier` set makes its workers
  synchronize at each step boundary (report completion, wait for every alive
  worker) via `ReportStepCompletion` / `CheckStepBarrier`; a dead worker is dropped.
- **Token exchange handler.** The `token_exchange` step handler requests a new
  job token for a different `audience` (via the executor's `TokenExchange`,
  backed by the API's `ExchangeJobToken`) and writes it to a step output.
- **Approval gate (F-13).** An `approval` step pauses the job: the handler
  renders its `message` param (same interpolation as conditions — secrets, step
  outputs, run params, upstream job outputs, job identity, env) and reports the
  job `awaiting_approval` with the message via the API (`internal/approval`
  gate), then polls `CheckApproval` until a decision lands. The UI sees the
  pending gate + message on `GET /api/jobs/{id}` and decides via
  `POST /api/jobs/{id}/approve` / `POST /api/jobs/{id}/reject` (optional
  `reason` body); the decision (actor, timestamp, reason) is persisted on the
  job row. Approved → the step succeeds and the job resumes; rejected → the
  step fails the job; a timeout or cancellation ends the job as usual. The
  gate is job-level: one approval unblocks every worker of a fan-out job.

### Logging

No central logs service. Every process logs locally via `internal/logging` to
**stdout** by default, or to the file named by `CDROM_LOG_FILE` (append mode).
`CDROM_LOG_FORMAT` selects the handler (`json` default / `text`); `CDROM_LOG_LEVEL`
sets the minimum level. Custom handlers via `logging.NewHandler` /
`logging.NewWithHandler`.

## Tech Stack & Constraints

- **Everything except the frontend is Go**; the frontend is React.
- **Database:** SQLite for local dev, PostgreSQL for deployments.
- **ORM: GORM** with `gorm.io/driver/postgres` and **`github.com/glebarez/sqlite`**
  (pure-Go; the official `gorm.io/driver/sqlite` needs cgo and breaks Windows cross-compiles).
- **Migrations: `AutoMigrate`** — schema is derived from the GORM models in
  `internal/models` (register new entities in `models.All()`). No hand-written SQL.
- All persistence goes through the database service; code outside it must never
  talk to a specific driver directly.

## Cross-Platform Requirements

- Both workers **and** agents must run on **Windows and Linux**.
- Use portable Go (`filepath`, `os` env handling); no hard-coded path separators,
  Unix-only syscalls, or shell assumptions without a Windows equivalent. Windows
  path/line-ending handling is a known failure mode.
- Job steps are type-agnostic and the `shell` handler runs a command directly
  (no implicit shell) — never rely on shell features in `command`/`args`; invoke
  a shell explicitly when needed. New step types must also be portable.

## Repository Layout

Standard Go layout; the frontend lives outside the Go module.

```
cmd/
  api/        API/controller entrypoint (HTTP for UI + gRPC for targets/scheduler)
  db/         database service entrypoint (gRPC)
  scheduler/  scheduler service entrypoint (gRPC)
  artifacts/  artifacts service entrypoint (gRPC)
  idp/        local OIDC IdP entrypoint (HTTP OIDC surface + gRPC API-only surface)
  worker/     long-lived worker entrypoint
  agent/      ephemeral Kubernetes agent entrypoint
internal/
  api/        API/controller: HTTP routing + gRPC control-plane server (worker hub,
              artifact proxy, StreamJobLogs persistence + job_log fan-out, event hub + /api/ws)
  auth/       OIDC auth (provider, PKCE, Bearer verification, discovery, middleware)
  authz/      RBAC engine (F-14): permission catalog, role/binding resolution,
              composition, resource scoping, deny-by-default Check/PermissionsFor
  config/     shared config loading for all binaries
  models/     GORM entities (single source of truth for the schema)
  gen/        generated gRPC/protobuf Go code (committed; `make proto`)
  grpcutil/   shared gRPC plumbing (client dial, serve-until-signal, TLS creds)
  logging/    centralized logging (stdout or CDROM_LOG_FILE)
  logstream/  executor.LogSink streaming a job's step output to the API (worker + agent)
  tokenexchange/ shared executor.TokenExchange via the API's ExchangeJobToken (worker + agent)
  approval/   executor.Approval gate via the API (worker + agent): report
              awaiting_approval + poll CheckApproval (F-13)
  target/     shared execution layer for worker + agent (run context, status reporting,
              post-run status decision; worker supplies the StepBarrier, agent runs unbarriered)
  executor/   shared job-step executor: dispatch engine (handler by type, job/step timeouts,
              LogSink, StepStatusReporter, StepBarrier, TokenExchange, RunInfo; spec
              interpolation F-10, step condition F-06, step outputs, token exchange)
  stephandlers/ built-in handlers: `shell` (DefaultType) + `token_exchange` +
              `approval` (F-13); extend via executor.RegisterStepType
  secrets/    secret store: `Store` interface + built-in AES-256-GCM store; nonces
              come from the Database service's `NextSecretNonce` (F-12)
  services/
    data/     pipeline/job data service
    artifacts/ namespaced file store (artifacts + job logs; gRPC server; store interface)
    scheduler/ job lifecycle + dispatch + background loops (watchdog F-03, retry F-04,
              dependency resolver F-06/F-08, run-status F-07, job-status fan-out,
              cron + event triggers F-09) + CreateRun/TriggerRun
    database/ storage backend abstraction + gRPC server (SQLite / PostgreSQL)
  worker/     long-lived worker (run context + status via internal/target; StepBarrier)
  agent/      ephemeral agent (run context + status via internal/target; unbarriered)
  idp/        local OIDC IdP (JWT issuer, auto key rotation; HTTP OIDC surface +
              gRPC API-only surface for job-token minting + user management; state via Database service)
proto/        protobuf definitions (proto/cdrom/<service>/v1/)
ui/           React frontend (separate from the Go module) + ui/tests/
docs/         documentation (Architecture.md, Features.md summary index,
              features/ per-feature specs, HighAvailability.md)
scripts/      build/release scripts (genproto.sh regenerates gRPC code)
```

Rules:
- All shared Go code goes under `internal/` — no `pkg/` or importable top-level packages.
- `cmd/*` packages contain only thin `main` entrypoints; real logic lives in `internal/`.
- The UI is not part of the Go module; it has its own tooling under `ui/`.

## Build & Tooling

- `Makefile`: `make build` (host binaries → `./bin`), `make proto` (regenerate
  gRPC code), `make test`, `make vet`, `make fmt`, `make clean`, `make certs`.
- Build with `go build ./...`; verify with `go vet ./...`.
- **gRPC codegen:** `make proto` runs `scripts/genproto.sh` (protoc +
  protoc-gen-go + protoc-gen-go-grpc). Generated code is committed under
  `internal/gen/` — regenerate after editing any `.proto`.
- **Cross-compile check:** `GOOS=windows GOARCH=amd64 go build ./cmd/...` must
  stay green — worker and agent ship for Windows. Never add cgo dependencies.

## Running Locally

All service addresses come from env vars (see `internal/config`); defaults target
a localhost dev setup. Every binary also accepts `--config-file <path>` (see
`config.example.yaml`). Precedence, lowest→highest: built-in defaults → config
file → env vars.

| Service      | Default address | Listen env var        |
|--------------|-----------------|-----------------------|
| db           | 127.0.0.1:7101  | `CDROM_LISTEN_ADDR`   |
| idp (HTTP)   | 127.0.0.1:7104  | `CDROM_LISTEN_ADDR`   |
| idp (gRPC)   | 127.0.0.1:7106  | `CDROM_IDP_GRPC_ADDR` |
| scheduler    | 127.0.0.1:7102  | `CDROM_LISTEN_ADDR`   |
| artifacts    | 127.0.0.1:7103  | `CDROM_LISTEN_ADDR`   |
| api (gRPC)   | 127.0.0.1:7105  | `CDROM_LISTEN_ADDR`   |
| api (HTTP)   | 127.0.0.1:8080  | `CDROM_API_HTTP_ADDR` |
| worker/agent | —               | `CDROM_API_ADDR`      |

Other notable vars: `CDROM_DB_BACKEND` (`sqlite`|`postgres`), `CDROM_DB_SQLITE_PATH`,
`CDROM_DB_POSTGRES_DSN`, `CDROM_ARTIFACTS_ROOT`, `CDROM_ARTIFACTS_STORE`,
`CDROM_WORKER_NAME`, `CDROM_WORKER_GROUP`, `CDROM_AGENT_JOB_ID`, `CDROM_AGENT_NAME`,
the mTLS paths `CDROM_TLS_CA_FILE`/`CDROM_TLS_CERT_FILE`/`CDROM_TLS_KEY_FILE`, the
auth vars `CDROM_AUTH_*` (incl. `CDROM_AUTH_USERPASS_ENABLED`, username/password
auth, default on; and the RBAC claim-mapping vars `CDROM_AUTH_ROLE_CLAIM`,
`CDROM_AUTH_ROLE_MAPPINGS` (comma-separated `claimValue=roleName`), and
`CDROM_AUTH_ROLE_CLAIM_AS_NAMES`, F-14), the IdP vars `CDROM_IDP_*` (incl. `CDROM_IDP_GRPC_ADDR`, the
IdP's gRPC listen address; the API dials it via `CDROM_IDP_GRPC_ADDR`), the
job-token auth vars `CDROM_GRPC_AUTH_*`, and the secrets vars
`CDROM_SECRETS_KIND` / `CDROM_SECRETS_KEY` (base64 32-byte AES-256-GCM key;
when unset, secrets are disabled).

A minimal local run (plaintext, no certs):

```sh
make build
./bin/db          # owns the SQLite file (cdrom.db) and runs migrations
./bin/scheduler   # dials db; runs background loops only while it holds the leader lease
./bin/artifacts   # stores files under ./artifacts
./bin/api         # gRPC :7105 + HTTP :8080, dials db/scheduler/artifacts
./bin/worker      # registers with the api, watches for jobs
```

To exercise authentication, also run `./bin/idp` (OIDC IdP / JWT issuer: HTTP
OIDC surface on :7104 + gRPC API-only surface on :7106, auto key rotation). For
**username/password** sign-in (enabled by default), point
the API's `auth.issuer` at it (`http://127.0.0.1:7104`), then register the first
user (`POST /api/register` — it becomes an `admin`) and sign in with
`POST /api/login`. For the full **OIDC** authorization-code flow, additionally
enable it (`auth.enabled: true`, `auth.issuer: http://127.0.0.1:7104`,
`auth.client_id: cdrom-ui`, `auth.redirect_url: http://127.0.0.1:8080/api/auth/callback`).
For mTLS, run `make certs` and point every binary at the cert set via the `tls`
section of the config file.

## Authentication & Live Events

The API's UI-facing HTTP surface (and its WebSocket endpoint) supports OIDC auth,
implemented in `internal/auth`.

- **Disabled by default.** With no `auth` config the API is open. Enable with
  `auth.enabled: true` (`CDROM_AUTH_ENABLED=true`); `issuer`, `client_id`, and
  `redirect_url` are then required.
- **Flow:** the API is a pure token verifier — it does not run sign-in. A client
  performs OIDC authorization-code + PKCE against the IdP and presents the
  resulting token as `Authorization: Bearer <token>`. `GET /api/auth/oidc`
  advertises the IdP's discovery doc (404 means auth is disabled). The API
  verifies each token against the IdP's JWKS (`github.com/coreos/go-oidc/v3`) and,
  when `token_audience` is set, checks `aud` against it (else `client_id`).
- **Middleware:** when enabled, every `/api/*` request (incl. the WebSocket
  upgrade) must carry a valid Bearer token or it gets a 401; the user is
  available via `auth.UserFromContext`. When disabled it's a passthrough.
- **Live events:** `GET /api/ws` (gorilla/websocket). On connect the client gets
  a state snapshot (jobs + workers + runs); afterwards one message per event.
  Event types: `job_status`, `worker` (registered/deregistered/watching),
  `job_log` (F-02), and `run_status` (F-07). Events are published from the gRPC
  server and HTTP handlers through a shared `EventHub` (`internal/api/events.go`);
  slow subscribers drop events and resync from the snapshot on reconnect.
- **Local IdP (`cmd/idp`):** a standalone OIDC IdP / JWT issuer. Its **HTTP**
  surface serves the standard OIDC protocol (discovery doc, JWKS, `/auth`
  authorization-code + PKCE, and the `authorization_code` token grant minting
  RS256 tokens); its **gRPC** surface serves the API-only operations (job-token
  minting and user management), which the API dials over mTLS. It auto-rotates
  its RSA key near expiry and keeps serving predecessor keys until they expire.
  Its state (signing keys + auth codes + the user directory for
  username/password auth) lives in the Database service, so multiple IdP
  replicas can share keys and users behind a load balancer — it requires
  `./bin/db` to be running.

### Username/password authentication

Alongside OIDC, the API supports username/password sign-in for easy local
testing of the UI. **Enabled by default** (`auth.userpass_enabled: true`,
`CDROM_AUTH_USERPASS_ENABLED`); set it to `false` to disable (e.g. when using an
external identity provider). All the real logic lives in the **IdP**; the API is
a thin proxy (the API's gRPC/HTTP surface never sees the password hash or
verifies credentials itself). The API reaches the IdP's user operations over
**gRPC** (the IdP's API-only surface, over mTLS when TLS is configured), so only
the API can register users, log them in, or manage roles.

- **Unauthenticated endpoints** (reachable without a token even when OIDC auth
  is on — they are exempted from the auth middleware): `POST /api/login`
  (body `{"email","password"}` → the IdP verifies the password and returns an
  OIDC `access_token` stamped with the user's roles, plus the user's profile)
  and `POST /api/register` (body `{"first_name","last_name","email","password"}`
  → the IdP hashes the password (bcrypt) and stores the user; the **first** user
  registered becomes an `admin`, later users get the default `user` role).
- **Role management** (require an authenticated caller with the `admin` role):
  `GET /api/users`, `POST /api/users` (create with explicit roles),
  `PUT /api/users/{id}` (update profile/roles/password), `DELETE /api/users/{id}`.
  The API proxies these to the IdP's gRPC user-management RPCs.
- **Tokens:** a login returns the same OIDC token the API verifies on every
  other request (against the IdP's JWKS); the user's roles are stamped onto the
  token and surfaced via `auth.User.Roles` (`User.HasRole`).
- The user directory (profiles, bcrypt password hashes, roles) is persisted
  through the Database service (`internal/models.IDPUser`), like the IdP's keys
  and auth codes, so multiple IdP replicas share it.

### Roles & permissions (RBAC, F-14)

A deny-by-default authorization layer. A principal's effective permissions are
the union of the permissions of every role it holds, expanded through role
composition, then filtered by resource scope per request. Roles reach a
principal through three mechanisms that compose:

- **Token `roles` claim** — the built-in IdP stamps the user's registered roles
  (F-24) onto the token; the API reads them from the verified token (the fast
  path, no per-request DB lookup).
- **JWT claim mapping** — `auth.roles.role_claim` names a token claim (e.g.
  `groups`) whose values are mapped to role names via `auth.roles.role_mappings`
  (or treated as role names directly when `role_claim_as_names` is set), so an
  external enterprise IdP's group membership drives Cdrom roles.
- **Stored bindings** — `RoleBinding` rows in the Database service attach a role
  to a principal, optionally scoped to a single pipeline (a scoped binding grants
  the role's resource-scoped permissions only for that pipeline; platform-wide
  permissions are granted only by unscoped bindings).

- **Engine.** `internal/authz` resolves a principal's roles, expands composition,
  and answers `Check(ctx, principal, permission, resource)` /
  `PermissionsFor(ctx, principal)`. Roles and bindings are owned by the Database
  service (seeded built-in roles `admin`/`operator`/`viewer`/`user` on startup);
  each API replica evaluates locally with a cache that is invalidated by
  `role_change` events on the shared event log (F-23), so a grant takes effect on
  every pod without a restart.
- **Enforcement.** The API enforces permissions on the UI-facing HTTP surface
  (pipelines, runs, jobs, approvals, workers, logs, secrets, role/binding
  management). The gRPC target surface keeps job-token auth (a separate,
  job-scoped mechanism). **When authentication is disabled, requests act as a
  synthetic admin** (the engine is wired in but bypassed).
- **Delegation.** Granting a role requires `roles.can-assign` *and* that the
  caller directly holds the role (a role reached only by composition does not
  count) with a scope at least as wide as the one being granted; `admin` is
  grantable only by an `admin`.
- **Endpoints.** `GET/POST /api/roles`, `GET/PUT/DELETE /api/roles/{name}`,
  `GET/POST /api/role-bindings`, `DELETE /api/role-bindings/{id}`, and
  `GET /api/me/permissions` (the caller's effective permissions per scope).
  Backend-only: the React UI role/binding screens are not built.

### Job Tokens (gRPC surface)

Separate from the UI's OIDC token, the API's **gRPC surface** (workers/agents)
supports **job-token auth** (`internal/api/jobsauth.go`).

- **Disabled by default.** Enable with `grpc_auth.enabled: true`
  (`CDROM_GRPC_AUTH_ENABLED=true`); `idp_address` and `audiences` are then required.
- **Minting:** the API is the **only** caller of the IdP's `MintJobToken` gRPC
  RPC, authenticating with its **mTLS client certificate** (no shared secret), so
  only a CA-signed client (the API) can mint job tokens. It mints one RS256
  token per job (claims: `sub: "job:<jobID>"`, `aud`, `job_id`, `pipeline_id`,
  `target_group`, `token_type: "job"`, plus the run's trigger context when
  trigger-fired). The main token's `exp` is a long placeholder (a week) —
  validity is gated on the job's live status, not `exp`.
- **Exchange:** a job requests a new token for a different `audience` via
  `ExchangeJobToken` (presents its own token); the exchanged token has a short
  lifetime (`expires_in`, else `grpc_auth.exchanged_token_lifetime`, default 15m)
  and re-stamps the trigger context.
- **Verification:** when enabled, `ReportJobStatus` and the artifact RPCs require
  a `Bearer <token>`. The API verifies it against the IdP's JWKS, checks an
  audience, and rejects unless the token's `job_id` matches the targeted job
  (artifact namespaces parse back to a job id). It **skips `exp`** and instead
  rejects tokens for a job in a terminal state — so a token is invalidated the
  moment the job stops (missing/invalid → `Unauthenticated`; wrong/finished job →
  `PermissionDenied`). Targets present the token via `grpcutil.WithBearerToken`.

## Conventions

- The database service routes GORM logs through slog via the adapter in
  `internal/services/database/logger.go`.
- Errors: wrap with `fmt.Errorf("context: %w", err)`; no panics across service boundaries.
- Testing: integration-style tests use SQLite via `database.Open` with a path
  under `t.TempDir()` (or `:memory:`) — never require a live Postgres.
- Variable names should make sense, except for Go idioms like `err` and `ok`.
- TODO: error handling strategy.
- TODO: testing conventions (unit vs integration, SQLite for tests?).

## Open Decisions

- Go module path is the placeholder `cdrom`; update `go.mod` and all imports when
  the repo is hosted (e.g. `github.com/<owner>/cdrom`).
- **Event-log retention (F-23, remaining).** The HA control plane (F-23) is
  implemented (shared event log, hybrid push+pull dispatch, scheduler leader
  election, cross-pod near-live logs — see `docs/HighAvailability.md`). The one
  open item is retention: the `events` table is an unpruned append-only log;
  partitioning by `created_at` + a retention schedule is a deployment concern to
  add when the log's growth warrants it.
