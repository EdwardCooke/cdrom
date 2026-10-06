# Cdrom — Project Context

> **Living document.** Keep this file up to date as the project evolves; when an
> architectural decision changes, update the relevant section so every agent
> (human or AI) starts from the same shared context.
>
> **Deep detail lives in `docs/`:** `docs/Architecture.md` (system design),
> `docs/Features.md` (per-feature specs F-01…F-23 with acceptance criteria), and
> `docs/HighAvailability.md` (the F-23 control plane). This file is the concise
> orientation + conventions; consult the docs for the full spec of any feature.

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
   - **IdP** (`cmd/idp`) — a local OIDC identity provider / JWT issuer (HTTP, not
     gRPC) for the API's OIDC auth and job-token minting; auto-rotates its RSA key.
     Its state (keys + auth codes) lives in the Database service.
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
(registered under `executor.DefaultType`) and **`token_exchange`**. A target or
plugin adds new types with `executor.RegisterStepType(name, handler)` (e.g.
`ansible`, `terraform`). A step whose type is unregistered fails the job with a
clear error. Worker and agent blank-import `internal/stephandlers` so built-ins
register before any job runs — this is the seam for the later plugin architecture.

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
before they reach the artifacts store / UI.

**Feature index.** The detailed spec, acceptance criteria, and RPC/field changes
for each feature are in `docs/Features.md`. Summary:

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
| F-13…F-22 | Roadmap: approval gates, RBAC, audit log, queueing, environments, notifications, artifact promotion, observability, config-as-code, post-deploy verification (not yet implemented) |
| F-23 | High availability (shared event log, hybrid push+pull dispatch, leader election, cross-pod logs — see `docs/HighAvailability.md`) |

Cross-cutting execution concepts (full detail in `docs/Features.md` /
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
  idp/        local OIDC IdP entrypoint (HTTP)
  worker/     long-lived worker entrypoint
  agent/      ephemeral Kubernetes agent entrypoint
internal/
  api/        API/controller: HTTP routing + gRPC control-plane server (worker hub,
              artifact proxy, StreamJobLogs persistence + job_log fan-out, event hub + /api/ws)
  auth/       OIDC auth (provider, PKCE, Bearer verification, discovery, middleware)
  config/     shared config loading for all binaries
  models/     GORM entities (single source of truth for the schema)
  gen/        generated gRPC/protobuf Go code (committed; `make proto`)
  grpcutil/   shared gRPC plumbing (client dial, serve-until-signal, TLS creds)
  logging/    centralized logging (stdout or CDROM_LOG_FILE)
  logstream/  executor.LogSink streaming a job's step output to the API (worker + agent)
  tokenexchange/ shared executor.TokenExchange via the API's ExchangeJobToken (worker + agent)
  target/     shared execution layer for worker + agent (run context, status reporting,
              post-run status decision; worker supplies the StepBarrier, agent runs unbarriered)
  executor/   shared job-step executor: dispatch engine (handler by type, job/step timeouts,
              LogSink, StepStatusReporter, StepBarrier, TokenExchange, RunInfo; spec
              interpolation F-10, step condition F-06, step outputs, token exchange)
  stephandlers/ built-in handlers: `shell` (DefaultType) + `token_exchange`; extend via
              executor.RegisterStepType
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
  idp/        local OIDC IdP (JWT issuer, auto key rotation; state via Database service)
proto/        protobuf definitions (proto/cdrom/<service>/v1/)
ui/           React frontend (separate from the Go module) + ui/tests/
docs/         documentation (Architecture.md, Features.md, HighAvailability.md)
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
| idp          | 127.0.0.1:7104  | `CDROM_LISTEN_ADDR`   |
| scheduler    | 127.0.0.1:7102  | `CDROM_LISTEN_ADDR`   |
| artifacts    | 127.0.0.1:7103  | `CDROM_LISTEN_ADDR`   |
| api (gRPC)   | 127.0.0.1:7105  | `CDROM_LISTEN_ADDR`   |
| api (HTTP)   | 127.0.0.1:8080  | `CDROM_API_HTTP_ADDR` |
| worker/agent | —               | `CDROM_API_ADDR`      |

Other notable vars: `CDROM_DB_BACKEND` (`sqlite`|`postgres`), `CDROM_DB_SQLITE_PATH`,
`CDROM_DB_POSTGRES_DSN`, `CDROM_ARTIFACTS_ROOT`, `CDROM_ARTIFACTS_STORE`,
`CDROM_WORKER_NAME`, `CDROM_WORKER_GROUP`, `CDROM_AGENT_JOB_ID`, `CDROM_AGENT_NAME`,
the mTLS paths `CDROM_TLS_CA_FILE`/`CDROM_TLS_CERT_FILE`/`CDROM_TLS_KEY_FILE`, the
auth vars `CDROM_AUTH_*`, the IdP vars `CDROM_IDP_*`, the job-token auth vars
`CDROM_GRPC_AUTH_*`, and the secrets vars `CDROM_SECRETS_KIND` / `CDROM_SECRETS_KEY`
(base64 32-byte AES-256-GCM key; when unset, secrets are disabled).

A minimal local run (plaintext, no certs):

```sh
make build
./bin/db          # owns the SQLite file (cdrom.db) and runs migrations
./bin/scheduler   # dials db; runs background loops only while it holds the leader lease
./bin/artifacts   # stores files under ./artifacts
./bin/api         # gRPC :7105 + HTTP :8080, dials db/scheduler/artifacts
./bin/worker      # registers with the api, watches for jobs
```

To exercise the full OIDC login flow, also run `./bin/idp` (OIDC IdP / JWT issuer
on :7104, auto key rotation) and point the API's `auth` config at it
(`auth.enabled: true`, `auth.issuer: http://127.0.0.1:7104`,
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
- **Local IdP (`cmd/idp`):** a standalone HTTP OIDC IdP / JWT issuer (discovery
  doc, JWKS, `/auth` authorization-code + PKCE, `/token` minting RS256 tokens).
  It auto-rotates its RSA key near expiry and keeps serving predecessor keys
  until they expire. Its state (signing keys + auth codes) lives in the Database
  service, so multiple IdP replicas can share keys behind a load balancer — it
  requires `./bin/db` to be running.

### Job Tokens (gRPC surface)

Separate from the UI's OIDC token, the API's **gRPC surface** (workers/agents)
supports **job-token auth** (`internal/api/jobsauth.go`).

- **Disabled by default.** Enable with `grpc_auth.enabled: true`
  (`CDROM_GRPC_AUTH_ENABLED=true`); `idp_address` and `audiences` are then required.
- **Minting:** the API is the **only** caller of the IdP's `job_token` grant,
  authenticating with its **mTLS client certificate** (no shared secret). It mints
  one RS256 token per job (claims: `sub: "job:<jobID>"`, `aud`, `job_id`,
  `pipeline_id`, `target_group`, `token_type: "job"`, plus the run's trigger
  context when trigger-fired). The main token's `exp` is a long placeholder (a
  week) — validity is gated on the job's live status, not `exp`.
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
