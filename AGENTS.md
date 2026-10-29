# Cdrom — Project Context

> **Living document.** Keep this file up to date as the project evolves; when an
> architectural decision changes, update the relevant section so every agent
> (human or AI) starts from the same shared context. Deep detail lives in
> `docs/`: `docs/Architecture.md` (system design + configuration reference),
> `docs/Features.md` (feature index, build order, checklist),
> `docs/features/` (one spec per feature, F-01…F-29, with acceptance
> criteria), `docs/HighAvailability.md` (the F-23 control plane), and
> `docs/Monitoring.md` (metric scrape targets). This file is the concise
> orientation + conventions; consult the docs for the full spec of any feature.

## Project Overview

Cdrom (continuous delivery runtime, orchestrator and manager) is a pipeline
tool: it defines, schedules, and executes pipeline jobs across distributed
execution targets. Everything except the React frontend is Go.

## Architecture

Four layers, top to bottom: **UI** (React) → **API/control plane** (Go, one or
more replicas) → **services** (standalone gRPC servers) → **execution targets**
(workers/agents). Details, diagrams, and the RPC inventory are in
`docs/Architecture.md`.

- **API / control plane** (`cmd/api`) — the only component execution targets
  talk to. Serves the HTTP API to the UI (:8080, incl. `/api/ws` live events)
  and a gRPC API to workers/agents/the scheduler (:7105). Holds workers'
  `WatchJobs` streams, tails the shared event log (F-23) to fan assignments /
  cancellations / status out to local workers and UI clients, proxies artifact
  traffic, and is the only component that can decrypt secrets (F-12). No
  business logic lives here.
- **Services** (one gRPC server each; protos in `proto/`, generated code in
  `internal/gen/`):
  - **Database** (`cmd/db`, :7101) — owns the storage backend (SQLite/PostgreSQL);
    the only component that touches it. Everyone else reads/writes through its
    gRPC API.
  - **Scheduler** (`cmd/scheduler`, :7102) — job lifecycle + dispatch. Talks
    **only** to the Database service: it appends "assignment" events to the
    shared event log that every API pod tails and fans out to live workers
    (hybrid push+pull, F-23). Background loops (watchdog F-03, retry F-04,
    dependency resolver F-06/F-08, run-status F-07, job-status fan-out,
    cron + event triggers F-09, concurrency admission F-16) run on exactly one
    replica via **leader election** (a portable lease on the Database service).
    Also `CreateRun` (F-07) and `TriggerRun` (F-09).
  - **Artifacts** (`cmd/artifacts`, :7103) — a general-purpose **namespaced
    file store** (filesystem-backed, streamed; job id is the namespace for a
    job's artifacts/logs). Store kind is configurable
    (`CDROM_ARTIFACTS_STORE`, default `filesystem`) so S3/Azure Blob can be
    added later.
  - **IdP** (`cmd/idp`, HTTP :7104 + gRPC :7106) — local OIDC identity
    provider / JWT issuer. Standard OIDC surface over HTTP (browser protocol,
    cannot be gRPC); API-only surface over gRPC (job-token minting, user/API-key/
    service-account management), dialed by the API over mTLS so only the API
    can reach it. State (keys, auth codes, users) lives in the Database service.
  - **Pipeline Source** (`cmd/pipelinesource`, :7107) — the source-fetcher
    service (F-21.2): fetches pipeline-definition files from `git`/`http`/`file`
    references, keeps a shallow bare-object-store git cache (go-git, no external
    `git` binary), pins SSH host keys, and serves fetched bytes to the API.
- **Execution layer** — long-lived **workers** (resident, targetable by group
  and environment; hold a `WatchJobs` stream *and* poll as the authoritative
  delivery path, F-23) and ephemeral **agents** (spun up in Kubernetes for a
  one-off job; `GetJob` → run → report). Both run on Windows and Linux.

**Topology rule:** execution targets talk **only** to the API. The API talks
to everything else. The scheduler talks **only** to the Database service — job
delivery flows through the shared event log. There is no central logs service;
every process logs locally.

### gRPC conventions

- Inter-component comms are gRPC. With a TLS CA configured every connection
  uses **mutual TLS** (creds in `internal/grpcutil`); without one it falls back
  to plaintext (local dev).
- Protos in `proto/cdrom/<service>/v1/`; generated code committed under
  `internal/gen/` (**regenerate with `make proto`** after editing any `.proto`).
- Service impls live in `internal/services/<service>/server.go` implementing
  the generated `*Server` interface (embed `Unimplemented*Server`).

### Execution model

A job carries a declarative `JobSpec`: an ordered list of steps. Each step's
`type` selects a handler (empty → `executor.DefaultType`); handler-specific
settings live in `params` (scalar-string or string-list values), so a new step
type needs no schema change. The spec is a **per-run snapshot** on the `Job`
row (GORM `serializer:json`), so editing a pipeline never changes what a past
run did. Both targets run the spec through the shared `internal/executor`
(steps in order; fail on the first errored step; no spec = no-op success).

Built-in handlers (`internal/stephandlers`): **`shell`** (default),
**`token_exchange`**, **`approval`** (F-13), **`email`** and **`webhook`**
(F-18 — the target renders the fields and calls the API's `SendEmail`/
`SendWebhook` RPCs; the API does the actual send), and the artifact handlers
(F-21.6): **`download_artifact`** (copies a named artifact onto the target —
four-tier resolution: run snapshot → run namespace → job namespace → the
public `packages` registry), **`extract`** (untar / gunzip-tar / unzip,
std-lib only, magic-byte detection, path-slip + symlink refusals), and
**`publish_artifact`** (multiline glob collection — `**` recursion via the
vendored, patched `filepathx`, `-`-negated exclusions — zipped with structure
preserved, entry names sanitized for Windows compatibility with
collision-counter disambiguation, uploaded into the job's own namespace).
New types register via `executor.RegisterStepType(name, handler)`; worker and
agent blank-import `internal/stephandlers` so built-ins register first — the
future plugin seam.

**Shell contract** (`shell` handler reads `command`, `args`, `shell` from
`params`):

- No `shell` param and no `args` → `command` is a one-line shell command run
  through the target OS's **default shell** (`bash -c` on Linux when bash is on
  the PATH, else `sh -c`; `pwsh -Command` on Windows when PowerShell Core is on
  the PATH, else `powershell -Command`). This backs the file format's
  `shell: <string>` shorthand (F-21.1.2).
- `args` set (invocation constructed explicitly) → the command runs **directly,
  no implicit shell** (no `&&`, pipes, globs, `$VAR` expansion) — the portable
  case.
- `shell` set → the target runs `<shell> <args> <command>` (opt into a
  specific shell, e.g. `shell: "pwsh"`, `args: ["-NoProfile", "-Command"]`).

Never rely on shell features in a step's `command`/`args` unless the step is
meant to run through a shell. Step stdout/stderr go to the target's local logs
and, when a log sink is set, stream to the API in near-real-time (F-02,
`internal/logstream`); the API persists them to the artifacts service and fans
them out to the UI as `job_log` events.

**Secrets (F-12).** Named secrets declared on pipelines; values encrypted with
a single AES-256-GCM key in the API's config (`CDROM_SECRETS_KEY`), stored
opaquely on the pipeline/version/job rows. The API alone decrypts — at dispatch
it hands plaintext to the target via the `Job.secrets` map (never persisted),
available to specs as `{{ .secrets.name }}`; nonces come from a DB-backed
counter so all API replicas share one key + sequence. The API redacts secret
values from job logs. `POST /api/secrets/encrypt` returns ciphertext for a
declared value.

**Cross-cutting execution concepts** (specs in `docs/features/`):

- **Fan-out (worker groups).** A job targeting a group runs on **every** worker
  in it; each worker's run is a separate `JobExecution`. The job row's status
  is *derived* by the scheduler job-status loop from alive workers' outcomes,
  per the job's `failure_mode` (`all` default / `best_effort` / `any`).
- **Step barrier.** A group job with `step_barrier` synchronizes workers at
  each step boundary (`ReportStepCompletion` / `CheckStepBarrier`); a dead
  worker is dropped.
- **`once` step field (F-01).** Runs on at most one target of a fan-out job
  (first to claim a durable slot in the Database service); others record
  `skipped`. Keeps a notification step from firing once per worker.
- **Approval gate (F-13).** An `approval` step pauses the job
  (`awaiting_approval`), renders its `message` against the condition context,
  and polls `CheckApproval`; decided via `POST /api/jobs/{id}/approve|reject`.
  Gate is job-level: one approval unblocks every worker of a fan-out job.
- **Concurrency admission (F-16).** Before dispatch the scheduler admits the
  job durably (`AcquireJobSlots`): one slot per concurrency key (keys are
  OR'd — blocked while any is held), the target group's cap, and the global
  cap. Limits are **regex-based rules in the database**
  (`GET/POST/PUT/DELETE /api/concurrency-limits`), most-specific-match wins,
  unmatched keys default to 1 (= a lock). Blocked jobs stay `pending` for the
  leader-gated admission loop — the single dispatch path. Queue visible via
  `GET /api/queue` + a `queue` WS event. Keys are **templatable** against run
  params/identity.
- **Environments (F-17).** Policy-bearing targets (`dev`/`staging`/`prod`/…)
  carrying `requires_approval` (inserts an F-13 gate) and an
  `automatic`/`manual` automation policy; a `manual` environment holds a run
  (`awaiting_proceed`) until `POST /api/runs/{id}/proceed` (or per-job),
  overriding any trigger and the job's own setting. Managed via
  `GET/POST/PUT/DELETE /api/environments[/{name}]`.
- **Labeling (F-29).** Normalized key/value labels (a `Label` table, indexed
  on `(key,value)`) on pipelines, jobs, environments, worker groups, and
  workers. A run snapshots the combined labels of its context
  (`worker > worker group > environment > job > pipeline` on collisions).
  Worker groups are first-class rows managed via
  `GET/POST/PUT/DELETE /api/worker-groups[/{name}]`.

### Logging & metrics

- **Logging** — no central service. Every process logs locally via
  `internal/logging` to stdout or `CDROM_LOG_FILE` (append); `CDROM_LOG_FORMAT`
  (`json` default / `text`), `CDROM_LOG_LEVEL`.
- **Audit (F-15)** — append-only who/what/where/outcome/change record via
  `internal/audit` (rotating file or stdout, separately from the default log;
  plus a durable copy in the Database service queried via `GET /api/audit`);
  secret values are never stored (name only, redacted).
- **Metrics (F-20)** — every component serves Prometheus `/metrics` +
  `/healthz` on a **dedicated port** (`CDROM_METRICS_ADDR`, 9100–9105+, not
  behind auth; `CDROM_METRICS_ENABLED=false` disables). Plumbing + per-component
  sets in `internal/metrics`; all metrics prefixed `cdrom_`, low-cardinality
  labels only. Scrape targets: `docs/Monitoring.md`; catalog:
  `docs/features/f-20-observability.md`.

## Feature index

Full descriptions, status, build order, and checklist: `docs/Features.md`.
One spec per feature under `docs/features/`. Done unless marked *(planned)*:

| ID | Feature | Spec |
|----|---------|------|
| F-01 | Job execution spec (steps, `type`→handler, `params`, per-run snapshot) | [f-01](docs/features/f-01-job-execution-spec.md) |
| F-02 | Job log streaming (target→API→artifacts→UI `job_log`, replayable) | [f-02](docs/features/f-02-job-log-streaming.md) |
| F-03 | Timeouts (job + per-step; `timed_out`; scheduler watchdog) | [f-03](docs/features/f-03-job-timeouts.md) |
| F-04 | Retry & re-run (`max_attempts`/`backoff`; `POST /api/jobs/{id}/rerun`) | [f-04](docs/features/f-04-retry-rerun.md) |
| F-05 | Cancellation (signal target; late reports can't clobber terminal status) | [f-05](docs/features/f-05-cancellation-propagation.md) |
| F-06 | Skip / step `condition` / `ignore_failed` / step `outputs` / `depends_on` | [f-06](docs/features/f-06-skipped-status.md) |
| F-07 | Pipeline runs (`PipelineRun` + per-run job instances; atomic `CreateRun`) | [f-07](docs/features/f-07-pipeline-runs.md) |
| F-08 | Job dependencies DAG (`needs` by stable `key`; validated at save) | [f-08](docs/features/f-08-job-dependencies.md) |
| F-09 | Triggers (cron / webhook / event; atomic `TriggerRun`) | [f-09](docs/features/f-09-triggers.md) |
| F-10 | Parameters & variables (run values; Go-template interpolation) | [f-10](docs/features/f-10-parameters-variables.md) |
| F-11 | Pipeline versioning (immutable snapshots; run binds to active-at-start) | [f-11](docs/features/f-11-pipeline-versioning.md) |
| F-12 | Secrets management (AES-256-GCM; API decrypts at dispatch; log redaction) | [f-12](docs/features/f-12-secrets-management.md) |
| F-13 | Approval gates (`approval` step; `awaiting_approval`; decide endpoints) | [f-13](docs/features/f-13-approval-gates.md) |
| F-14 | Roles & permissions (deny-by-default RBAC; `internal/authz`) | [f-14](docs/features/f-14-rbac.md) |
| F-15 | Audit log (local rotating file + durable DB copy; `GET /api/audit`) | [f-15](docs/features/f-15-audit-log.md) |
| F-16 | Concurrency control & queueing (regex-rule limits; concurrency keys) | [f-16](docs/features/f-16-concurrency-queueing.md) |
| F-17 | Environments & deployment targets (policy-bearing targets) | [f-17](docs/features/f-17-environments.md) |
| F-18 | Notifications (`email`/`webhook` step handlers; API-side send) | [f-18](docs/features/f-18-notifications.md) |
| ~~F-19~~ | ~~Artifact promotion~~ — **absorbed into F-21.6** (publishing + promotion ride with artifact packages) | [f-19](docs/features/f-19-artifact-promotion.md) |
| F-20 | Observability & metrics (Prometheus; DORA indicators) | [f-20](docs/features/f-20-observability.md) |
| F-21 | Config as code (F-21.1…F-21.7; F-21.1–F-21.6 done, F-21.7 planned) | [f-21](docs/features/f-21-config-as-code.md) |
| F-22 | Post-deploy verification & rollback *(planned)* | [f-22](docs/features/f-22-verification-rollback.md) |
| F-23 | High availability (event log, hybrid dispatch, leader election) | [f-23](docs/features/f-23-high-availability.md) · [HA design](docs/HighAvailability.md) |
| F-24 | Username/password authentication (login/register proxied to IdP) | [f-24](docs/features/f-24-username-password-auth.md) |
| F-25 | API keys (`cdrom-`+64-char; per-pipeline scope; rotate/reset) | [f-25](docs/features/f-25-api-keys.md) |
| F-26 | Service accounts (non-human identities; two rotatable key slots) | [f-26](docs/features/f-26-service-accounts.md) |
| F-27 | Approval groups *(planned)* | [f-27](docs/features/f-27-approval-groups.md) |
| F-28 | SCIM 2.0 user provisioning *(planned)* | [f-28](docs/features/f-28-scim-provisioning.md) |
| F-29 | Labeling (normalized `Label` table; run label snapshot) | [f-29](docs/features/f-29-tagging.md) |

## Tech Stack & Constraints

- **Everything except the frontend is Go**; the frontend is React.
- **Database:** SQLite for local dev, PostgreSQL for deployments.
- **ORM: GORM** with `gorm.io/driver/postgres` and **`github.com/glebarez/sqlite`**
  (pure-Go; the official sqlite driver needs cgo and breaks Windows
  cross-compiles). **Never add cgo dependencies.**
- **Migrations: `AutoMigrate`** — schema is derived from the GORM models in
  `internal/models` (register new entities in `models.All()`). No hand-written
  SQL.
- All persistence goes through the database service; code outside it must
  never talk to a specific driver directly.

## Cross-Platform Requirements

- Both workers **and** agents must run on **Windows and Linux**.
- Portable Go only (`filepath`, `os` env handling); no hard-coded path
  separators, Unix-only syscalls, or shell assumptions without a Windows
  equivalent. Windows path/line-ending handling is a known failure mode.
- New step types must be portable (see the shell contract above).

## Repository Layout

Standard Go layout; the frontend lives outside the Go module.

```
cmd/            thin mains: api, db, scheduler, artifacts, idp, pipelinesource, worker, agent
internal/
  api/          API/controller: HTTP routing + gRPC control plane (worker hub,
                artifact proxy, StreamJobLogs persistence + job_log fan-out,
                event hub + /api/ws, job-token auth)
  auth/         OIDC/Bearer verification (JWKS, discovery ad, middleware)
  authz/        RBAC engine (F-14): permission catalog, role/binding resolution,
                composition, resource scoping, deny-by-default Check/PermissionsFor
  config/       shared config loading for all binaries
  models/       GORM entities (single source of truth for the schema)
  gen/          generated gRPC/protobuf Go code (committed; `make proto`)
  grpcutil/     shared gRPC plumbing (client dial, serve-until-signal, mTLS creds,
                bearer presentation)
  logging/      centralized logging (stdout or CDROM_LOG_FILE)
  metrics/      Prometheus plumbing (F-20) + per-component metric sets
  logstream/    streams a job's step output to the API (worker + agent)
  tokenexchange/ executor.TokenExchange via the API's ExchangeJobToken
  approval/     approval gate (F-13): report awaiting_approval + poll CheckApproval
  target/       shared execution layer for worker + agent (run context, status
                reporting, post-run decision; worker supplies the StepBarrier)
  executor/     shared job-step executor: dispatch engine (handler by type,
                job/step timeouts, LogSink, StepStatusReporter, StepBarrier,
                TokenExchange; spec interpolation F-10, step condition F-06,
                step outputs)
  stephandlers/ built-in handlers: `shell` (DefaultType), `token_exchange`,
                `approval` (F-13), `email` + `webhook` (F-18); extend via
                executor.RegisterStepType
  secrets/      secret store: `Store` interface + AES-256-GCM builtin; nonces
                from the Database service's `NextSecretNonce` (F-12)
  audit/        audit log (F-15): rotating file/stdout writer, Recorder, Pruner,
                Redactor
  notify/       notification rendering/support (F-18)
  webhook/      webhook trigger support (F-09)
  pipelinefile/ config-as-code file format + compiler (F-21.1)
  sourceref/    `<type>:<spec>` source-reference parsing (F-21.2.2)
  services/
    database/   storage backend abstraction + gRPC server (SQLite / PostgreSQL)
    scheduler/  job lifecycle + dispatch + background loops (watchdog F-03,
                retry F-04, dependency resolver F-06/F-08, run-status F-07,
                job-status fan-out, cron + event triggers F-09, admission F-16)
                + CreateRun/TriggerRun
    artifacts/  namespaced file store (artifacts + job logs; gRPC server;
                pluggable store interface)
    pipelinesource/ source-fetcher service (F-21.2): git/http/file fetch
                clients, shallow bare-object-store git cache, strict SSH
                host-key pinning
  worker/       long-lived worker (registration, WatchJobs + poll, StepBarrier)
  agent/        ephemeral agent (GetJob → run → report; unbarriered)
  idp/          local OIDC IdP (JWT issuer, auto key rotation; users, keys,
                codes via the Database service)
proto/          protobuf definitions (proto/cdrom/<service>/v1/)
ui/             React frontend (own tooling) + ui/tests/
docs/           Architecture.md, Features.md, features/, HighAvailability.md,
                Monitoring.md
scripts/        genproto.sh, gencerts.sh, update-known-hosts.sh (SSH host keys
                for F-21.2.4.1); e2e.sh boots the stack once and runs
                scripts/e2e/NN-*.sh in order (each runnable standalone; shared
                helpers in scripts/e2e/lib.sh)
```

Rules:
- All shared Go code goes under `internal/` — no `pkg/` or importable
  top-level packages.
- `cmd/*` packages contain only thin `main` entrypoints; real logic lives in
  `internal/`.
- The UI is not part of the Go module.

## Build & Tooling

- `Makefile`: `make build` (host binaries → `./bin`, incl. the e2e
  `fetchclient` helper), `make proto` (regen gRPC code), `make test`,
  `make vet`, `make fmt`, `make e2e` (boot stack + run `scripts/e2e/*.sh`),
  `make certs` (local mTLS set → `./certs`), `make clean`.
- Verify with `go vet ./...`; build with `go build ./...`.
- **Cross-compile check:** `GOOS=windows GOARCH=amd64 go build ./cmd/...` must
  stay green — worker and agent ship for Windows.
- Regenerate `internal/gen/` after editing any `.proto` (`make proto`).

## Running Locally

Addresses come from env vars (precedence: built-in defaults → config file →
env vars; every binary accepts `--config-file <path>`, see
`config.example.yaml`). Full reference: `docs/Architecture.md` §11.

| Service         | Default address | Listen env var        |
|-----------------|-----------------|-----------------------|
| db              | 127.0.0.1:7101  | `CDROM_LISTEN_ADDR`   |
| scheduler       | 127.0.0.1:7102  | `CDROM_LISTEN_ADDR`   |
| artifacts       | 127.0.0.1:7103  | `CDROM_LISTEN_ADDR`   |
| idp (HTTP)      | 127.0.0.1:7104  | `CDROM_LISTEN_ADDR`   |
| api (gRPC)      | 127.0.0.1:7105  | `CDROM_LISTEN_ADDR`   |
| idp (gRPC)      | 127.0.0.1:7106  | `CDROM_IDP_GRPC_ADDR` |
| pipelinesource  | 127.0.0.1:7107  | `CDROM_LISTEN_ADDR`   |
| api (HTTP)      | 127.0.0.1:8080  | `CDROM_API_HTTP_ADDR` |
| worker/agent    | —               | `CDROM_API_ADDR`      |
| metrics         | 127.0.0.1:91xx  | `CDROM_METRICS_ADDR`  |

Notable vars (complete list in `docs/Architecture.md` §11): `CDROM_DB_BACKEND`
(`sqlite`|`postgres`), `CDROM_DB_SQLITE_PATH` / `CDROM_DB_POSTGRES_DSN`,
`CDROM_ARTIFACTS_ROOT` / `_STORE`, `CDROM_WORKER_{NAME,GROUP,ENVIRONMENTS,TAGS}`,
`CDROM_AGENT_{JOB_ID,NAME}`, `CDROM_WORK_ROOT` (worker/agent staged-working-area
base dir, F-21.6; default OS temp dir — the target warns at startup when it
resolves to a tmpfs mount), `CDROM_TLS_{CA,CERT,KEY}_FILE`, `CDROM_AUTH_*`
(OIDC enablement, userpass, API keys, service accounts, RBAC claim mapping),
`CDROM_IDP_*`, `CDROM_GRPC_AUTH_*` (job-token auth), `CDROM_SECRETS_{KIND,KEY}`,
`CDROM_PIPELINE_SOURCE_*` (F-21.2/F-21.5), `CDROM_AUDIT_*` (F-15),
`CDROM_EMAIL_*` + `CDROM_WEBHOOK_TIMEOUT{,_MAX}` (F-18), `CDROM_METRICS_*`
(F-20), `CDROM_LOG_{FILE,FORMAT,LEVEL}`.

Minimal local run (plaintext, no certs):

```sh
make build
./bin/db          # owns the SQLite file (cdrom.db), runs migrations
./bin/scheduler   # dials db; loops run only while it holds the leader lease
./bin/artifacts   # stores files under ./artifacts
./bin/api         # gRPC :7105 + HTTP :8080
./bin/worker      # registers with the api, watches for jobs
```

For auth also run `./bin/idp` and point the API's `auth.issuer` at
`http://127.0.0.1:7104`. Username/password sign-in is on by default: register
the first user (`POST /api/register` → `admin`) and sign in with
`POST /api/login`. For the full OIDC authorization-code flow set
`auth.enabled: true` + `client_id`/`redirect_url`. For mTLS run `make certs`
and configure the `tls` section.

## Authentication Overview

Mechanisms per surface (deep dive: `docs/Architecture.md` §6 + the F-14/F-24/
F-25/F-26 specs):

- **UI (HTTP):** OIDC — the API is a pure Bearer-token verifier against the
  IdP's JWKS; disabled by default (passthrough, requests act as a synthetic
  admin). Alongside it, username/password (`POST /api/login` /
  `/api/register`, enabled by default) and per-user **API keys**
  (`Bearer <username>:<apikey>`, F-25) and **service accounts** (F-26) are
  handled by the IdP; the API proxies.
- **RBAC (F-14):** deny-by-default; effective permissions = union of the
  principal's roles (token `roles` claim ⊕ JWT claim mapping ⊕ stored
  bindings, composed transitively), filtered by resource scope. Evaluated in
  the API (`internal/authz`) with a cache invalidated by `role_change` events
  on the shared event log. Delegation: granting a role requires
  `roles.can-assign` + direct possession of the role at ≥ the granted scope.
- **Targets (gRPC):** job tokens (RS256, minted by the API via the IdP over
  mTLS; one per job, `sub: "job:<id>"`). Validity is gated on the job's live
  status, not `exp`; `ExchangeJobToken` mints short-lived tokens for other
  audiences (used by the `token_exchange` step). Disabled by default
  (`CDROM_GRPC_AUTH_ENABLED`).

## Live Events

`GET /api/ws` (gorilla/websocket). Connect → state snapshot (jobs + workers +
runs + …); thereafter one message per event: `job_status`, `worker`,
`job_log` (F-02), `run_status` (F-07), `queue`, `environment`,
`concurrency_limits`, `role_change`, `source_credential`, …. Published from
the gRPC server and HTTP handlers through a shared `EventHub`
(`internal/api/events.go`); in HA mode the hub rides the shared event log
(every pod tails it). Slow subscribers drop events and resync from the
snapshot on reconnect.

## Conventions

- Error handling: wrap with `fmt.Errorf("context: %w", err)`; no panics across
  service boundaries.
- Tests: integration-style tests use SQLite via `database.Open` with a path
  under `t.TempDir()` (or `:memory:`) — never require a live Postgres. End-to-
  end scenarios live in `scripts/e2e/NN-name.sh` (run via `make e2e` or
  standalone); add a new scenario as the next NN number.
- The database service routes GORM logs through slog
  (`internal/services/database/logger.go`).
- Variable names should make sense, except Go idioms like `err` and `ok`.
- Docs discipline: when landing a feature, fold lasting design decisions back
  into this file / `docs/Architecture.md` and tick the checklist in
  `docs/Features.md`.

## Open Decisions

- Go module path is the placeholder `cdrom`; update `go.mod` and all imports
  when the repo is hosted (e.g. `github.com/<owner>/cdrom`).
- **Event-log retention (F-23, remaining).** The `events` table is an
  unpruned append-only log; partitioning by `created_at` + a retention
  schedule is a deployment concern to add when growth warrants it.
