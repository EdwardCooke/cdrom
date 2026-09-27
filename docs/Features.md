# Cdrom — Feature Roadmap

This document catalogs the common features of a continuous delivery (CD)
platform that Cdrom should implement. It is written as an **implementation
roadmap**: each feature is self-contained, scoped to the concrete components in
this codebase, and given testable acceptance criteria so it can be picked up
and built in isolation.

> **How to use this doc.** Work top-to-bottom through the phases. Each feature
> lists the components it touches and the acceptance criteria that define
> "done". When a feature is implemented, mark it `[x]` in the checklist at the
> bottom and note any design decisions that should be folded back into
> `AGENTS.md` / `Architecture.md`.

---

## Current state (what already exists)

Before adding features, note the baseline that is already built and working:

| Area | Status |
|------|--------|
| Pipeline model | `Pipeline` (name, description, ordered `Jobs`) — CRUD via DB service |
| Job model | `Job` (status, `target_group`, start/finish timestamps) |
| Job lifecycle | `pending → running → succeeded / failed / cancelled` |
| Dispatch | `target_group` set → fan out to live workers; empty → ephemeral K8s agent |
| Execution targets | Long-lived **workers** (register, `WatchJobs` stream, heartbeat) and ephemeral **agents** (`GetJob` → run → report) |
| Job execution | **Real** — a job carries a `JobSpec` (ordered steps; each step's `type` selects a handler, defaulting to the built-in `shell` handler that runs a command with args, workdir, env, per-step timeout, and an optional shell override); the worker/agent run the steps in order via `internal/executor` and fail on the first step that errors |
| Job tokens | Minted per job by the API (via IdP), audience exchange, verified on status/artifact RPCs |
| UI auth | OIDC authorization-code + PKCE, signed session cookie |
| Artifacts | Streamed upload/download, per-job, proxied through the API |
| Live events | WebSocket `/api/ws` — `job_status` and `worker` events |
| Transport | gRPC everywhere, optional mutual TLS |

The biggest remaining gaps: **a job's execution is not observable** (no log
streaming, F-02) and **jobs are not orchestrated** (no runs, DAG, or
triggers). Everything below builds toward orchestrating real jobs, then
governing them.

---

## Phase 1 — Core execution

Make a job actually do work, and make its execution observable and controllable.
These are the foundation every other feature depends on.

### F-01 · Job execution spec

**What.** A job carries a declarative spec describing the work to perform: an
ordered list of **steps**. Each step is agnostic about how it runs: a `type`
field selects a step handler, and the remaining fields (`command`, `args`,
`workdir`, `env`, `timeout`, `shell`, `params`) are interpreted by that
handler. The built-in `shell` handler (default when `type` is empty) runs a
command, directly or through a user-chosen shell. The worker/agent executes
the steps in order and the job fails on the first step that errors.

**Why.** CD jobs are fundamentally "run these commands in this environment".
Without a spec, a job is just a label. This is the single most important
missing piece.

**Scope.**
- `internal/models` — add a `JobSpec` (or embed steps in `Job`): steps with
  `command`, `args`, `workdir`, `env`, `timeout`. Decide whether the spec is
  stored on the `Job` row (per-run snapshot) or referenced from the `Pipeline`
  (shared definition). **Recommendation:** store a snapshot on the run so a
  pipeline edit never changes what a past run did.
- `proto/cdrom/db/v1/db.proto` + `proto/cdrom/api/v1/api.proto` — carry the
  spec to the execution target.
- `internal/worker/worker.go`, `internal/agent/agent.go` — replace the
  `runJob` stub with a real executor that runs each step (portable across
  Windows + Linux — see cross-platform constraints).
- `internal/api/server.go` — accept the spec on `POST /api/jobs`.

**Acceptance criteria.**
- [x] A job submitted with a multi-step spec runs the steps in order on a
      worker and on an agent.
- [x] A failing step marks the job `failed` and stops subsequent steps.
- [x] Step environment variables are available to the command.
- [x] The spec is portable: the same job spec executes on a Windows worker and
      a Linux worker (no shell-specific assumptions, or an explicit shell
      contract is documented).
- [x] A step's `shell` override runs the step through a user-chosen shell
      (`<shell> <args> <command>`), e.g. `pwsh` on Windows.
- [x] A step's `type` selects its handler: the built-in `shell` handler runs
      by default, a target can register new types via
      `executor.RegisterStepType`, and an unregistered type fails the job with
      a clear error.
- [x] `GOOS=windows GOARCH=amd64 go build ./cmd/...` stays green.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- The spec is a **per-run snapshot** stored on the `Job` row (a JSON `text`
  column via GORM's `serializer:json`), so a pipeline edit never changes what
  a past run did.
- The canonical spec type is `cdrom.db.v1.JobSpec`; the API and scheduler
  protos reference it rather than redefining it.
- Execution is shared by worker and agent in `internal/executor`
  (`executor.Execute(ctx, spec, logger)`).
- **Step types & handlers:** the executor (`internal/executor`) is the generic
  dispatch engine; a step's `type` selects an `executor.StepHandler` (empty →
  the handler registered under `executor.DefaultType`). The concrete handlers
  live in `internal/stephandlers`; the built-in `shell` handler registers
  itself under `executor.DefaultType` at package init. A target or plugin adds
  new types with `executor.RegisterStepType(name, handler)` (e.g. `ansible`,
  `terraform`, `argo`). An unregistered type fails the job with a clear error.
  `params` carries handler-specific configuration so a new type needs no
  spec-schema change. The worker and agent blank-import `internal/stephandlers`
  so the built-in handlers are registered before any job runs. This is the
  seam for the later plugin architecture.
- **Shell contract:** a step's command is executed directly by the target OS —
  no implicit shell. Steps needing shell behavior invoke a shell explicitly
  (`sh -c ...` / `cmd /c ...`). Step stdout/stderr are inherited from the
  target (local logging); streaming to the API is F-02.
- **Shell override:** a step may set `shell` to run through a user-chosen
  interpreter; the target executes `<shell> <args> <command>` (e.g.
  `shell: "pwsh"`, `args: ["-NoProfile", "-Command"]`,
  `command: "Get-ChildItem"`). When `shell` is empty the step runs `command`
  directly, preserving the no-implicit-shell contract.
- Per-step `timeout` is enforced by the target via a derived context; a
  scheduler-side watchdog for dead targets is F-03.

### F-02 · Job log streaming

**What.** Execution targets stream job output (stdout/stderr per step) back to
the API in near-real-time; the API persists it and fans it out to the UI over
the existing WebSocket event hub. The UI can tail a running job's logs and
replay finished logs.

**Why.** "Where is it stuck?" is the #1 question in CD. Today all logging is
local to the worker/agent and there is no central view.

**Scope.**
- `proto/cdrom/api/v1/api.proto` — a new streaming RPC (e.g.
  `StreamJobLogs` client-stream, or piggyback on `ReportJobStatus` with a log
  payload). Decide the chunking/flow-control model.
- `internal/api` — accept the stream, buffer/persist (via DB service), publish
  a new `job_log` event type on the `EventHub`.
- `internal/models` — a `JobLog`/`JobLogLine` entity (or a blob per job) so
  logs survive past the run.
- `internal/worker`, `internal/agent` — capture step output and stream it.
- `internal/api/ws.go` — new event type; UI consumes it.

**Acceptance criteria.**
- [ ] While a job runs, its stdout/stderr appears in the UI within ~1s.
- [ ] Logs are attributed to the correct step.
- [ ] After a job finishes, its full log can still be fetched (replay).
- [ ] A slow UI client does not block the worker (backpressure/drop policy
      defined, consistent with the existing EventHub drop-and-resync behavior).

### F-03 · Job timeouts

**What.** A job (and optionally each step) can declare a maximum duration. If
execution exceeds it, the job is marked `failed` (or `timed_out`) and the
running process is terminated.

**Why.** Hung jobs hold workers forever; timeouts are a basic safety valve.

**Scope.**
- `internal/models` / proto — `timeout` on the job spec (and per step).
- `internal/worker`, `internal/agent` — enforce the deadline, cancel the
  running command, report the terminal status.
- `internal/services/scheduler` — a watchdog that reaps jobs that exceed their
  timeout even if the target goes silent (covers a dead worker).

**Acceptance criteria.**
- [ ] A job whose command sleeps past its timeout is terminated and marked
      failed/timed-out.
- [ ] The timeout is enforced by the target *and* by the scheduler watchdog
      (a target that stops reporting is still reaped).
- [ ] A job with no timeout runs unbounded.

### F-04 · Retry & re-run

**What.** A job can declare a retry policy (max attempts, backoff). On failure
the scheduler re-dispatches it up to the limit. Separately, a user can
manually **re-run** any finished job (a fresh attempt, or a new job seeded from
the same spec).

**Why.** Transient failures (network blips, flaky targets) are common; CD
tools always offer retry and "run again".

**Scope.**
- `internal/models` / proto — `retry` policy on the job spec; track
  `attempt` count on the job.
- `internal/services/scheduler` — on a `failed` report, schedule a re-dispatch
  if attempts remain (respecting backoff).
- `internal/api/server.go` — `POST /api/jobs/{id}/rerun`.
- Job status model — decide whether a retry is a new `Job` row or a new attempt
  on the same row (recommend: same row, `attempt` counter, so the pipeline run
  stays coherent).

**Acceptance criteria.**
- [ ] A job with `retry: 2` that fails is re-dispatched up to 2 more times.
- [ ] Backoff delays are applied between attempts.
- [ ] Re-running a finished job produces a new execution with a fresh status.
- [ ] Attempts are visible (UI shows attempt N of M).

### F-05 · Cancellation propagation

**What.** Cancelling a running job signals the execution target to stop the
work (terminate the running command), not just flip the status in the DB.

**Why.** Today `CancelJob` only updates the DB row; the worker keeps running
the command. A real cancel must reach the target.

**Scope.**
- `proto/cdrom/api/v1/api.proto` — a cancel signal to the target (e.g. a
  control message on the `WatchJobs` stream, or a `CancelJob` RPC the target
  watches).
- `internal/api` (worker hub) — deliver the cancel to the worker holding the
  job.
- `internal/worker`, `internal/agent` — on cancel, interrupt the running step
  and report `cancelled`.
- `internal/services/scheduler` — `CancelJob` now also notifies the target.

**Acceptance criteria.**
- [ ] Cancelling a running job stops the command on the target within a bounded
      time.
- [ ] The job's final status is `cancelled` and the target stops work.
- [ ] Cancelling an already-finished job is a no-op (idempotent).

### F-06 · `skipped` job status

**What.** Add a `skipped` status for jobs that never run because a dependency
failed or a condition was not met (pairs with F-08 dependencies).

**Why.** In a DAG, a downstream job of a failed job is "skipped", not
"failed" — the distinction matters for reporting and for `on_failure` logic.

**Scope.**
- `internal/models` + `proto/cdrom/db/v1/db.proto` — new `JobStatusSkipped`.
- `internal/services/scheduler` — mark downstream jobs skipped when an
  upstream fails.
- UI — render the new status.

**Acceptance criteria.**
- [ ] A job whose dependency failed is marked `skipped`, not `failed`.
- [ ] `skipped` is a terminal state.
- [ ] The status is surfaced in the UI and in `job_status` events.

---

## Phase 2 — Pipeline orchestration

Turn a flat list of jobs into a real, triggerable, parameterized pipeline.

### F-07 · Pipeline runs (first-class execution entity)

**What.** Introduce a **PipelineRun** (a.k.a. execution / build): one execution
of a pipeline, owning a set of job instances. Jobs become *instances* of the
pipeline's job definitions, bound to a run. This separates the pipeline
*definition* from each *run* of it.

**Why.** Today a `Job` is both the definition and the execution. You cannot
run the same pipeline twice and compare runs, or see "run #42 of pipeline X".
A run is the unit CD tools report on.

**Scope.**
- `internal/models` — `PipelineRun` (pipeline_id, status, trigger, params,
  started/finished); `Job` gains a `run_id` and becomes a per-run instance.
- `proto/cdrom/db/v1/db.proto` — run + job-instance messages and RPCs.
- `internal/services/scheduler` — submitting a run creates the run + its job
  instances and drives them.
- `internal/api/server.go` — `POST /api/pipelines/{id}/runs`, list/get runs.
- UI — a runs view per pipeline.

**Acceptance criteria.**
- [ ] Triggering a pipeline creates a `PipelineRun` and one job instance per
      job definition.
- [ ] Two runs of the same pipeline are independent and both queryable.
- [ ] A run's overall status is derived from its jobs (succeeded only if all
      non-skipped jobs succeeded).
- [ ] Historical runs are retained and browsable.

### F-08 · Job dependencies (DAG)

**What.** Jobs within a pipeline declare dependencies (`needs`), forming a DAG.
A job starts only when all its dependencies have succeeded. The scheduler
resolves the DAG and dispatches ready jobs in parallel where possible.

**Why.** Real pipelines are graphs (build → test → deploy), not just ordered
lists. Parallelism and correct ordering come from the DAG.

**Scope.**
- `internal/models` / proto — `needs` (list of job keys) on the job definition.
- `internal/services/scheduler` — a DAG resolver: compute in-degrees, dispatch
  jobs whose deps are satisfied, handle failure propagation (→ F-06 skipped).
- Validation — reject cycles at pipeline save time.

**Acceptance criteria.**
- [ ] A job with `needs: [A]` does not start until A succeeds.
- [ ] Independent jobs run in parallel.
- [ ] A cycle in the graph is rejected when the pipeline is saved.
- [ ] Failure of A skips its dependents (F-06).

### F-09 · Triggers

**What.** Ways to start a pipeline run other than a manual click:
- **Schedule (cron)** — run on a cron expression.
- **Webhook** — an external POST (e.g. from a git host) starts a run, passing
  payload (commit, branch) as run parameters.
- **Event** — start when another pipeline/run reaches a state (chaining).

**Why.** CD is driven by events (a push, a timer, an upstream deploy), not
just by humans.

**Scope.**
- `internal/models` / proto — trigger definitions on the pipeline (cron spec,
  webhook secret, event rule).
- `internal/services/scheduler` — a cron scheduler loop; webhook intake; event
  subscription (hook into the EventHub / job-status flow).
- `internal/api/server.go` — `POST /api/pipelines/{id}/webhook` (auth via
  shared secret / job-token audience).
- `internal/config` — any scheduler tuning.

**Acceptance criteria.**
- [ ] A cron-triggered pipeline runs at the scheduled times.
- [ ] A webhook POST with a valid secret starts a run and records the payload
      as run parameters.
- [ ] An event trigger starts a run when the watched condition is met.
- [ ] Trigger source is recorded on the run (manual / cron / webhook / event).

### F-10 · Parameters & variables

**What.** Pipelines and jobs accept **parameters** (typed, with defaults)
supplied at trigger time, and **variables** (pipeline- or run-scoped values)
that can be interpolated into job specs (env, commands). Supports common
interpolation like `{{ run.branch }}` or `{{ params.version }}`.

**Why.** The same pipeline deploys to different places / versions by changing
inputs, not by editing the definition.

**Scope.**
- `internal/models` / proto — parameter + variable definitions; a run stores
  its concrete parameter values.
- A small, safe interpolation/templating helper (shared, in `internal/`) used
  by the executor to render the job spec before running.
- `internal/api/server.go` — accept parameters on run creation / webhook.

**Acceptance criteria.**
- [ ] A parameter with a default is used when not supplied; an explicit value
      overrides it.
- [ ] Interpolation substitutes run/parameter values into env and commands.
- [ ] Unknown/undefined references fail the run clearly (no silent empty
      strings) — or a defined fallback policy is documented.
- [ ] Parameter values are visible on the run (non-secret ones).

### F-11 · Pipeline versioning

**What.** Each change to a pipeline definition produces a new **version**. A
run is bound to the version that was active when it started, so re-running an
old run reproduces the old definition.

**Why.** Reproducibility and audit: "what exactly did run #42 execute?"

**Scope.**
- `internal/models` / proto — a `PipelineVersion` (immutable snapshot of the
  definition) or a version counter + stored spec.
- `internal/services/scheduler` — a run snapshots the current version.
- `internal/api/server.go` — list versions; run against a specific version.

**Acceptance criteria.**
- [ ] Editing a pipeline bumps its version; existing runs keep their original
      version.
- [ ] A run records which version it executed.
- [ ] A run can be re-executed against its original version.

---

## Phase 3 — Safety & governance

Control who can do what, protect secrets, and keep an accountable record.

### F-12 · Secrets management

**What.** A store for secret values (API keys, tokens, credentials) that can be
referenced from job specs and injected into the execution environment. Secrets
are never returned in plaintext to the UI or logged; only their names/metadata
are visible.

**Why.** Every real pipeline needs credentials; they must not live in the
pipeline definition or in logs.

**Scope.**
- `internal/models` — a `Secret` entity (name, scope, encrypted value).
- `proto/cdrom/db/v1/db.proto` — CRUD that writes but never returns the
  plaintext (return a masked reference).
- Encryption at rest (decide: a configured key, or OS keychain for local dev).
- `internal/worker`, `internal/agent` — resolve secret references into the
  process environment at run time (fetched via the API, never stored on disk).
- Interop with F-10 interpolation (`{{ secret.NAME }}`).

**Acceptance criteria.**
- [ ] A secret can be created and referenced by a job; the value is available
      to the command's environment.
- [ ] No API/UI path returns the plaintext secret value.
- [ ] Secret values never appear in job logs (redaction if accidentally echoed).
- [ ] Secrets are encrypted at rest.

### F-13 · Approval gates

**What.** A job (or a stage) can require a **manual approval** before it runs.
The run pauses in an `awaiting_approval` state until an authorized user
approves (or rejects / times out).

**Why.** Production deploys and other high-blast-radius steps are gated behind
a human sign-off.

**Scope.**
- `internal/models` / proto — an approval requirement on the job; an
  `awaiting_approval` status; an approval record (who, when, decision).
- `internal/services/scheduler` — hold the job at the gate; proceed on
  approval, skip/fail on rejection or timeout.
- `internal/api/server.go` — `POST /api/jobs/{id}/approve` / `.../reject`.
- UI — an approval prompt.

**Acceptance criteria.**
- [ ] A gated job does not start until approved.
- [ ] Approval/rejection is recorded (actor + timestamp).
- [ ] An approval timeout (if set) fails/skips the job.
- [ ] Only authorized actors can approve (pairs with F-14).

### F-14 · Roles & permissions (RBAC)

**What.** Define roles (e.g. viewer, operator, admin) and bind users (from the
OIDC identity) to them. Gate actions — trigger, cancel, approve, edit
pipelines, manage secrets — by role.

**Why.** A shared CD platform needs to distinguish who may do what.

**Scope.**
- `internal/models` — `Role`, `User`/membership (map OIDC subject → roles).
- `internal/auth` — expose the authenticated principal (already available via
  `auth.UserFromContext`) to the authorization check.
- An authorization helper (in `internal/`) that the API handlers and gRPC
  surface consult before mutating actions.
- `internal/api/server.go` — enforce on the relevant endpoints.

**Acceptance criteria.**
- [ ] A viewer cannot trigger or cancel a run; an operator can; an admin can
      manage secrets/pipelines.
- [ ] Authorization is enforced server-side on both the HTTP and gRPC surfaces.
- [ ] The acting principal is recorded on the action (feeds F-15).

### F-15 · Audit log

**What.** An append-only record of significant actions: who triggered/cancelled
/approved what, when, and the outcome. Queryable from the UI.

**Why.** Compliance and debugging both need "what happened and who did it".

**Scope.**
- `internal/models` — an `AuditEvent` entity (actor, action, target, time,
  metadata).
- `proto/cdrom/db/v1/db.proto` — append + query.
- Instrument the API handlers (and gRPC surface) to emit audit events for the
  actions in F-07/F-09/F-13/F-14.
- `internal/api/server.go` — `GET /api/audit`.

**Acceptance criteria.**
- [ ] Triggering, cancelling, approving, and secret/pipeline mutations each
      write an audit event with the actor.
- [ ] The log is append-only (no update/delete of past events).
- [ ] Events can be filtered by actor, target, and time range.

### F-16 · Concurrency control & queueing

**What.** Bound how much runs in parallel: max concurrent jobs per worker
group (and/or globally), a visible pending queue, and optional job priority.
Jobs beyond the limit wait their turn.

**Why.** Without limits a burst of jobs overwhelms targets; operators need to
see and manage the backlog.

**Scope.**
- `internal/config` — concurrency limits (per group / global).
- `internal/api` (worker hub) — track in-flight jobs per worker/group; hold
  excess assignments until a slot frees.
- `internal/services/scheduler` — priority ordering of the pending queue.
- UI — a queue view (pending jobs, who's running what).

**Acceptance criteria.**
- [ ] With a limit of N per group, at most N jobs run concurrently on that
      group; the rest stay `pending`.
- [ ] When a job finishes, a queued job is dispatched.
- [ ] Higher-priority jobs are dispatched before lower-priority ones.
- [ ] The UI shows the pending queue and current load per group.

### F-17 · Environments & deployment targets

**What.** First-class **environments** (e.g. dev, staging, prod) that group
targets and carry policy (e.g. prod requires approval — ties to F-13). A job
targets an environment, which resolves to the appropriate worker group / agent
pool.

**Why.** "Deploy to staging" vs "deploy to prod" is the core CD distinction,
and prod usually has stricter gates.

**Scope.**
- `internal/models` / proto — an `Environment` entity (name, target group or
  agent pool, policy flags).
- Job spec — target an environment (in addition to / instead of a raw
  `target_group`).
- `internal/services/scheduler` — resolve environment → dispatch target; apply
  environment policy (approval gate).

**Acceptance criteria.**
- [ ] A job targeting an environment is dispatched to that environment's
      targets.
- [ ] An environment marked "requires approval" inserts an approval gate
      (F-13) before dispatch.
- [ ] Environments are manageable (CRUD) and visible in the UI.

---

## Phase 4 — Delivery & observability

The higher-level CD capabilities that make the platform a complete delivery
tool.

### F-18 · Notifications

**What.** Emit notifications when a run/job reaches a terminal state (or hits
an approval gate): via **webhook** (generic POST) and/or other channels.
Configurable per pipeline.

**Why.** Teams live in chat/issue trackers, not the CD UI.

**Scope.**
- `internal/models` / proto — notification config on the pipeline (webhook URL
  + secret, event filters).
- `internal/services/scheduler` (or a small notifier) — fire webhooks on the
  relevant events (reuse the EventHub).
- Delivery semantics — retry with backoff on failure; never block the run.

**Acceptance criteria.**
- [ ] A configured webhook receives a payload when a run finishes (with status
      and run link).
- [ ] Notification failures are retried and logged but do not fail the run.
- [ ] Event filters (e.g. only notify on failure) are honored.

### F-19 · Artifact promotion

**What.** Promote an artifact produced in one run/environment to another
environment (e.g. the build artifact from staging is what prod deploys), so
environments deploy the *same* artifact rather than rebuilding.

**Why.** "Build once, deploy everywhere" is a core CD guarantee.

**Scope.**
- `proto/cdrom/artifacts/v1/artifacts.proto` — a promote/copy operation
  (source job → target job/environment).
- `internal/services/artifacts` — implement the copy (streamed).
- `internal/api/server.go` — `POST /api/artifacts/promote`.
- Ties to F-17 (environments) and F-07 (runs).

**Acceptance criteria.**
- [ ] An artifact from run A can be promoted to run B / another environment.
- [ ] The promoted artifact is byte-identical (checksum verified).
- [ ] Promotion is authorized (F-14) and audited (F-15).

### F-20 · Observability & metrics

**What.** Surface operational metrics: job duration, success/failure rate,
queue depth, worker utilization, artifact volume. Expose a metrics endpoint
and/or a dashboard in the UI.

**Why.** Operators need to see the health and throughput of the platform.

**Scope.**
- Instrument the scheduler, API, and worker hub with counters/histograms
  (decide: a Prometheus `/metrics` endpoint on the API, and/or UI charts fed
  by the DB).
- `internal/api/server.go` — a metrics endpoint and/or dashboard data routes.
- UI — dashboards (run history, durations, failure trends, worker load).

**Acceptance criteria.**
- [ ] A metrics endpoint exposes job duration, success rate, and queue depth.
- [ ] The UI shows per-pipeline run history with durations and outcomes.
- [ ] Worker load (in-flight vs. limit) is visible.

### F-21 · Config as code

**What.** Define pipelines (jobs, steps, deps, triggers, params) in a
declarative file format (YAML) that can be imported/exported and checked into
source control. The UI editor and the file format are two views of the same
model.

**Why.** Teams want pipeline definitions in version control, reviewable via PR.

**Scope.**
- A schema for the pipeline definition file (documented in `docs/`).
- `internal/api/server.go` — import (validate + create/update pipeline) and
  export (render current definition).
- Validation shared with F-08 (DAG) and F-10 (params).

**Acceptance criteria.**
- [ ] A pipeline defined in YAML can be imported and becomes runnable.
- [ ] Exporting a pipeline reproduces a file that re-imports equivalently
      (round-trip).
- [ ] Invalid definitions are rejected with actionable errors.

### F-22 · Post-deploy verification & rollback

**What.** After a deploy job, run **health checks** (readiness probes / a
verification step) to confirm the release is healthy; if it fails, support
**rollback** to the previous known-good release.

**Why.** Deploying is not done until the system is verified healthy, and a bad
release must be reversible.

**Scope.**
- Job spec — a verification step / health-check definition (pairs with F-01).
- `internal/models` / proto — a "current release" record per environment (what
  is live now, what was previous) to enable rollback.
- `internal/services/scheduler` — run verification after deploy; on failure,
  offer/trigger a rollback run.
- Ties to F-17 (environments).

**Acceptance criteria.**
- [ ] A deploy job can declare a post-deploy verification step that gates the
      run's success.
- [ ] The current release per environment is tracked.
- [ ] A rollback produces a run that restores the previous release.

---

## Suggested build order

The phases are ordered so each builds on the last. A pragmatic first cut:

1. **F-01 → F-02 → F-03** — a job that does real work, with live logs and a
   timeout. This alone turns the placeholder into a usable executor.
2. **F-05 → F-04 → F-06** — control (cancel, retry) and the `skipped` status.
3. **F-07 → F-08** — the pipeline run + DAG, the heart of orchestration.
4. **F-09 → F-10 → F-11** — triggers, parameters, versioning.
5. **F-12 → F-14 → F-13 → F-15** — secrets, RBAC, approvals, audit.
6. **F-16 → F-17** — concurrency and environments.
7. **F-18 → F-20 → F-21 → F-22 → F-19** — notifications, observability,
   config-as-code, verification/rollback, artifact promotion.

---

## Implementation checklist

Tick each feature off as it lands.

- [x] F-01 Job execution spec
- [ ] F-02 Job log streaming
- [ ] F-03 Job timeouts
- [ ] F-04 Retry & re-run
- [ ] F-05 Cancellation propagation
- [ ] F-06 `skipped` job status
- [ ] F-07 Pipeline runs
- [ ] F-08 Job dependencies (DAG)
- [ ] F-09 Triggers (cron / webhook / event)
- [ ] F-10 Parameters & variables
- [ ] F-11 Pipeline versioning
- [ ] F-12 Secrets management
- [ ] F-13 Approval gates
- [ ] F-14 Roles & permissions (RBAC)
- [ ] F-15 Audit log
- [ ] F-16 Concurrency control & queueing
- [ ] F-17 Environments & deployment targets
- [ ] F-18 Notifications
- [ ] F-19 Artifact promotion
- [ ] F-20 Observability & metrics
- [ ] F-21 Config as code
- [ ] F-22 Post-deploy verification & rollback
