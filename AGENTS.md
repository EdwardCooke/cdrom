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
2. **API / controller layer** — Go. The **control plane** (one or more
   replicas, F-23). It exposes the HTTP API consumed by the UI *and* a gRPC
   API consumed by execution targets (workers, agents). It holds the workers'
   WatchJobs streams, tails the shared event log (F-23) to fan job
   assignments, cancellations, and status events out to its local workers and
   UI clients, and proxies artifact traffic to the artifacts service. No
   business logic belongs here.
3. **Service layer** — Go services, one per concern. Each service is a
   **standalone process** exposing a **gRPC** interface (protos in `proto/`,
   generated code in `internal/gen/`):
   - **Database** (`cmd/db`) — owns the storage backend (SQLite/PostgreSQL);
     the only component allowed to touch it. All other components read/write
     data through its gRPC API.
   - **Scheduler** (`cmd/scheduler`) — manages job lifecycle and dispatch.
     It talks **only** to the Database service (F-23): when a job targets a
     worker group it appends an "assignment" event to the shared event log,
     which every API pod tails and fans out to its live workers (the workers'
     ~1 s poll is the authoritative catch-up path). It no longer dials a
     specific API pod. It runs the background loops (below) on exactly
     one replica at a time via **leader election** — a portable lease on the
     Database service (`AcquireLease` / `ReleaseLease` / `GetLease`) with a
     long TTL backstop and a short heartbeat window (the leader heartbeats on
     every renewal, so a follower takes over quickly once the leader's
     heartbeat goes stale) — so only the leader reaps, retries, resolves
     dependencies, re-derives run status, re-derives group jobs' fan-out
     status, and fires cron / event triggers. It
     also **creates pipeline runs** (F-07): `CreateRun`
     asks the Database service to create a `PipelineRun` plus one job instance
     per job definition and then drives the instances, and a background
     **run-status loop** re-derives each in-flight run's overall status from
     its job instances and persists + fans it out to the UI. It also runs a
     background **watchdog** that reaps running jobs that have exceeded their
     declared timeout even if the execution target goes silent (F-03), a
     background **retry loop** that re-dispatches failed jobs per their retry
     policy (F-04), **cancels** a job by persisting the cancellation and
     signalling the execution target to stop the work (F-05), a background
     **dependency resolver** that skips a pending job whose dependency failed
     (or dispatches it once every dependency succeeded, F-06), a background
     **job-status loop** that re-derives each in-flight group job's overall
     status from the per-worker outcomes of its fan-out executions (see
     Fan-out below) and fans the change out to the UI, a background
     **cron trigger loop** that starts a run of a pipeline at each of its
     cron triggers' scheduled times (F-09), and a background **event trigger
     loop** that starts a run of a pipeline when a run of another pipeline
     reaches a status one of its event triggers watches (F-09). It also
     **starts trigger-fired runs** (F-09): `TriggerRun` asks the Database
     service to atomically claim a trigger's fire window (creating a
     `PipelineRun` plus one job instance per job definition only if the
     trigger has not already started a run for the window) and then drives
     the instances.
   - **Artifacts** (`cmd/artifacts`) — a general-purpose, **namespaced file
     store** (filesystem-backed, streamed uploads/downloads). Every file lives
     in an opaque *namespace* that groups related files; the service is
     agnostic about what a namespace means. A job's artifacts and logs use the
     job's id as the namespace, while a deployed release's artifacts might use
     a release identifier — so the same store holds both job-owned and
     deployed/published artifacts. It also provides **job-log storage**: the
     API persists a job's streamed step output here (one file per step,
     `step-<n>.log`, plus a combined `job.log`, under
     `<root>/<namespace>/logs/` where the namespace is the job id) via the
     `AppendLog`/`DownloadLog`/`GetLog`/`ListLogs` RPCs. The store is an
     interface (`internal/services/artifacts/store.go`) with a filesystem
     implementation; the store kind is configurable (`artifacts_store` /
     `CDROM_ARTIFACTS_STORE`, default `filesystem`) so S3 / Azure Blob can be
     added later.
   - **IdP** (`cmd/idp`) — a local OIDC identity provider that acts as a JWT
     issuer for the API's OIDC authentication. It is an HTTP service (not
     gRPC) serving the discovery doc, JWKS, authorization, and token
     endpoints, and it rotates its RSA signing key automatically (see
     Authentication & Live Events).
   - More services will be added as concerns emerge.
4. **Execution layer** — where jobs actually run (see Execution Model below).

**Topology rule:** execution targets (workers, agents) talk **only** to the
API. The API talks to everything else (database, scheduler, artifacts). The
scheduler talks **only** to the Database service (F-23): it never dials a
specific API pod, and job delivery to workers flows through the shared event
log that every API pod tails. There is no central logs service — every
process logs locally to stdout or a file (see Logging below).

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
step with no type runs through it, and the built-in **`token_exchange`**
handler registers itself under `token_exchange` (see the Token exchange
handler below). A target or plugin adds new step types with
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
(local logging) and, when the target has a log sink, also streamed to the API
in near-real-time (F-02, `internal/logstream`); the API persists the output to
the artifacts service and fans it out to the UI as `job_log` events.

**Timeouts (F-03):** a job declares a job-level `timeout` on its `JobSpec`
(the sum of all steps) and each step may declare its own `timeout` (an
individual step); whichever deadline expires first terminates the job. The
executor derives a context for the job-level timeout in `Execute` and a nested
context for a step's timeout in `runStep`, cancelling the running step when a
deadline passes. A job with no job-level and no per-step timeout runs
unbounded. When a deadline expires the executor returns the sentinel
`executor.ErrTimeout` (wrapped with the context); the worker and agent check
`errors.Is(err, executor.ErrTimeout)` and report the new terminal status
`timed_out` (distinct from `failed`) so the UI can tell a hung job apart from
one that ran and errored. As a safety valve for a target that goes silent (a
dead worker, a crashed agent), the scheduler runs a background **watchdog**
(`internal/services/scheduler/watchdog.go`, started from `cmd/scheduler`) that
periodically reaps running jobs past their effective timeout (job-level
timeout, else the longest per-step timeout) via the conditional
`Database.ReapJob` RPC and tells the API (`NotifyJobStatus`) to fan the status
change out to the UI.

**Retry & re-run (F-04):** a job's `JobSpec` may carry a `retry` policy:
`max_attempts` (the number of retries *after* the initial attempt — a job
with `max_attempts: 2` runs at most 3 times; `0`/absent means never retried)
and `backoff` (a duration to wait before each re-dispatch). A retry is a new
attempt on the **same `Job` row** — the `Job` carries an `attempt` counter
(starts at 1) and a denormalized `max_attempts` (copied from the spec at
creation) so the pipeline run stays coherent and the UI can show "attempt N
of M". The scheduler runs a background **retry loop**
(`internal/services/scheduler/retry.go`, started from `cmd/scheduler`) that
periodically asks the database for the jobs that still have retries remaining
(`Database.ListRetriableJobs` — failed jobs with `max_attempts > 0` and
`attempt < max_attempts + 1`, filtered in the database so the loop never pulls
every failed job over the wire) and, for each, claims the next attempt through
the conditional `Database.ClaimJobRetry` RPC (atomically resets the job to
`pending` with `attempt + 1` and a cleared `finished_at`; rejects jobs that
succeeded, were cancelled, or exhausted their budget) and re-dispatches it
through the API — immediately, or after the policy's `backoff` delay.
Separately, a user can **re-run** any finished job (succeeded, failed,
cancelled, or timed out) via `POST /api/jobs/{id}/rerun` (API → scheduler
`RerunJob` → `Database.RerunJob`): the job is reset to `pending` with
`attempt` back to 1 and re-dispatched, producing a fresh execution.
`attempt`/`max_attempts` are carried on the job protos (db, scheduler, api)
and surfaced to the UI through the WebSocket snapshot and `job_status`
events.

**Cancellation propagation (F-05):** cancelling a running job signals the
execution target to stop the work (terminate the running command), not just
flip the status in the database. `POST /api/jobs/{id}/cancel` (API → scheduler
`CancelJob`) persists the cancellation with the conditional
`Database.CancelJob` RPC (marks the job `cancelled` only if it is still
`pending` or `running`, so cancelling a finished job is a no-op) and then
signals the target through the API's `CancelJob` RPC. The API's `WatchJobs`
server stream carries a `WatchMessage` (a oneof of `JobAssignment` and
`JobCancellation`): the API pushes a `JobCancellation` down every live
worker's stream, and a worker that is running the job interrupts the running
step (cancelling the job's context, which terminates the command) and reports
`cancelled`; a cancellation for a job that is not running on that worker is
ignored. An ephemeral agent holds no `WatchJobs` stream, so it observes the
cancellation on the API instead: it polls `GetJob` on a short interval while
the job runs and, when it sees the job is `cancelled`, interrupts the running
step and reports `cancelled`. Signalling the target is best-effort: if it
cannot be reached (no live worker, or the API is down) the job is still marked
`cancelled` in the database, and the target's next status report (or the
scheduler's watchdog) reconciles it. A late target report cannot clobber a
terminal status: `Database.UpdateJob` applies a target's status report only if
the job is still `pending` or `running`, so once a job is `succeeded`,
`failed`, `cancelled`, or `timed_out` a late report (e.g. a target that
finished just as it was cancelled) is ignored.

**Skip propagation & step conditions (F-06):** a job or step can be skipped —
never run — instead of failing, in two ways:

- **Step `condition`:** a step's `condition` param is a Go template
  (`text/template`) rendered against a **condition context** that carries, in
  addition to the step's own `Env` vars (top-level, so `{{ .NAME }}` still
  works), three keys: `steps` (a slice of the job's prior steps, each exposing
  `.Index`, `.Status`, and `.Outputs`), `jobs` (a slice of the job's upstream
  dependencies, each exposing `.ID`, `.Name`, `.Status`, and `.Outputs`), and
  `job` (the job's own identity: `.ID`, `.Name`, `.Status`). The rendered
  output is parsed as a boolean (`strconv.ParseBool`). An empty `condition`
  never skips (the default, unchanged behavior). A `condition` that renders to
  `false` marks the step `skipped` (`executor.StepStatusSkipped`) and the
  executor moves on to the next step without running it or failing the job. A
  `condition` that fails to render or whose output does not parse as a
  boolean is treated as a spec error: it fails the job (the step is not
  skipped), since this is a configuration mistake rather than a legitimate
  "don't run this" signal. The upstream jobs and the job identity are supplied
  by the execution target via the executor's context
  (`executor.ContextWithUpstreamJobs` / `executor.ContextWithJobIdentity`);
  the API populates a job's `upstream_jobs` (fetched best-effort from the
  database for each `depends_on` id) before handing the job to the target.
  Example: `{{ if eq (index .jobs 0).Status "succeeded" }}true{{ else
  }}false{{ end }}`. Each step's terminal outcome (`succeeded`, `failed`,
  `skipped`, or `timed_out`, with an error message when applicable) is
  collected by the execution target via an `executor.StepStatusReporter` set
  in the executor's context (`executor.StepResultCollector`, mirroring the
  `LogSink` context pattern) and attached to the job's final
  `ReportJobStatus` call as `step_results`; the API passes them through to
  `Database.UpdateJob`, which persists them unconditionally (they are
  informational, not part of the terminal-status guard) so the UI can show
  each step's outcome.
- **Step & job `ignore_failed`:** a step may set `ignore_failed` so that a
  failure or timeout is recorded (in `step_results`) but does **not** stop the
  job — the executor moves on to the next step. A job may set `ignore_failed`
  (denormalized from its spec onto the `Job` row at creation) so that, in the
  dependency resolver, a `failed` or `timed_out` dependency counts as
  satisfied: dependents are dispatched rather than skipped. A `cancelled` or
  `skipped` dependency is **never** overridden by `ignore_failed` (a
  cancellation is not a failure a job can opt out of) — it always blocks.
- **Step & job `outputs`:** a step may declare `outputs` — a list of names.
  The executor gives every step a fresh per-step directory, exposed to it via
  the `CDROM_STEP_OUTPUT_DIR` env var (created even for a step that declares no
  outputs, so a step handler that always produces output can write to it); the
  step's command writes one file per declared name into it, and the executor
  reads them back (trimmed) after the step runs. A declared name that was never
  written is recorded as an empty string. The job's `outputs` are the union of its steps' outputs (a later
  step overrides an earlier one on a name collision), aggregated by the
  `StepResultCollector` and reported on the job's final `ReportJobStatus`
  call, where `Database.UpdateJob` persists them (via the same
  `Select`/`Updates` serializer pattern as `step_results`). Downstream steps
  and jobs read these through the condition context (`.steps`/`.jobs`
  `.Outputs`) — this is how a step hands a value (a version string, a build
  id, an artifact path) to later steps and jobs.
- **Job `depends_on` (dependency skip propagation):** a job may declare
  `depends_on` — a list of other job ids it depends on. `SubmitJob` persists
  it on the `Job` row and, unlike a job with no dependencies, does **not**
  dispatch it immediately; it is left `pending`. The scheduler runs a
  background **dependency resolver**
  (`internal/services/scheduler/dependencies.go`, started from
  `cmd/scheduler/main.go`) that periodically lists pending jobs with a
  non-empty `depends_on` and, for each, checks every dependency's status
  (`Database.GetJob`): a `succeeded` dependency is satisfied; a `failed` or
  `timed_out` dependency is satisfied **only if it has `ignore_failed` set**
  (otherwise it blocks); a `cancelled` or `skipped` dependency always blocks
  (skip propagates; a cancellation is not something `ignore_failed` can
  override). If any dependency blocks, the job is marked `skipped`
  (`Database.SkipJob`, conditional: only from `pending`, mirroring
  `CancelJob`'s conditional pattern) and the scheduler fans the status out to
  the UI (`API.NotifyJobStatus`) — this is what makes skip **propagate**
  transitively through a chain of dependent jobs, one resolver tick at a time.
  If every dependency is satisfied, the job is
  released: its `depends_on` is cleared (`Database.UpdateJob` with
  `clear_depends_on`, so the resolver does not reconsider — and re-dispatch —
  it on the next tick) and it is dispatched exactly as `SubmitJob` would have
  dispatched a job with no dependencies (`API.DispatchJob` for a job with a
  `target_group`; a job with an empty `target_group` is simply left `pending`
  for an ephemeral agent, same as today). A dependency still `pending` or
  `running` leaves the job untouched, to be reconsidered on the resolver's
  next tick.

  **F-08 (now implemented) builds on this resolver rather than replacing it.**
  F-06 landed ahead of F-08 in the roadmap, so `Job` gained a minimal
  `depends_on` field (a flat list of raw job ids) resolved by this polling
  loop. F-08 adds `needs` — dependencies expressed as the stable `key`s of
  other jobs in the same pipeline — and cycle validation at pipeline save
  time. The database service resolves a job's `needs` (keys) to `depends_on`
  (ids) when the pipeline is saved, and `Database.CreateRun` remaps a run's
  instances' `depends_on` (definition ids) onto the run's own instance ids,
  so this same resolver dispatches ready jobs in parallel and skips
  dependents of a failed job. See the "Job dependencies (F-08)" section
  below.

**Pipeline runs (F-07):** a **`PipelineRun`** is a first-class entity — one
execution of a pipeline, owning a set of job instances. It carries
`pipeline_id`, an overall `status` (`pending`, `running`, `succeeded`,
`failed`, `cancelled`), a `trigger` (how it was started, e.g. `manual`),
`params` (run parameters, F-10), and `started_at`/`finished_at`. A `Job`
gains a `run_id` and becomes a per-run *instance* of the pipeline's job
definitions, so the pipeline *definition* is separated from each *run* of it.

- **`CreateRun` is atomic in the database service.** `Database.CreateRun`
  creates the run row and, in a single transaction, one job instance per job
  definition in the pipeline. Each instance copies the definition's spec,
  `target_group`, retry policy, and `ignore_failed`, and is bound to the new
  run. Each instance's `depends_on` is **remapped from the definition job ids
  to the new instance ids**, so two runs of the same pipeline are fully
  independent (a dependency in run A never points at a job in run B).
- **The scheduler drives the instances.** `Scheduler.CreateRun` calls
  `Database.CreateRun` and then drives each instance: a job with a
  `target_group` is dispatched to the API (which fans it out to the live
  workers); a job with `depends_on` is left `pending` for the dependency
  resolver (F-06); a job with an empty `target_group` and no dependencies is
  left `pending` for an ephemeral agent.
- **Run status is derived, not reported.** A run has no target of its own, so
  its status is *derived* from its job instances by a background **run-status
  loop** (`internal/services/scheduler/runstatus.go`, started from
  `cmd/scheduler`). It periodically re-derives each in-flight run's status:
  `failed` if any job failed or timed out; `cancelled` if any job was
  cancelled (and none failed); `running` if any job is still pending or
  running; `succeeded` if every job succeeded or was skipped (a run with no
  jobs succeeds). When the derived status changes, the loop persists it
  (`Database.UpdateRun`, stamping `started_at` when the run leaves `pending`
  and `finished_at` when it reaches a terminal status) and fans the change out
  to the UI (`API.NotifyRunStatus`).
- **New RPCs.** `Database.CreateRun` / `GetRun` / `ListRuns` / `UpdateRun`;
  `Scheduler.CreateRun`; `API.NotifyRunStatus`. `Job` (db, scheduler, api) and
  `CreateJobRequest` / `ListJobsRequest` (db) gain a `run_id` so a run's
  instances can be listed and a job can be created directly under a run.
- **New HTTP endpoints.** `POST /api/pipelines/{id}/runs` (trigger a run),
  `GET /api/pipelines/{id}/runs` (list a pipeline's runs, most recent first),
  and `GET /api/runs/{id}` (fetch a single run).
- **New WebSocket event + snapshot entry.** A `run_status` event (run id +
  status) is published when a run is created and whenever its derived status
  changes; the connect snapshot now also carries the current runs.

**Job dependencies (F-08):** jobs within a pipeline declare dependencies as
`needs` — the stable `key`s of other jobs in the same pipeline — forming a
DAG. A job starts only when all its dependencies have succeeded; the
scheduler dispatches ready jobs in parallel where possible.

- **`needs` are keys, not ids.** A job in a pipeline has a `key` (a short
  name, e.g. `build`, `test`), unique within the pipeline. A job's `needs`
  reference other jobs by their `key`, so a pipeline's DAG is stable across
  runs and survives re-creation of a run's job instances. A job that is not
  part of a pipeline (a standalone job) has no `key` and may still use the
  raw-id `depends_on` (F-06).
- **The DAG is validated at save time.** The database service validates a
  pipeline's job graph whenever it is created, updated, or a job is added:
  keys are unique, every `needs` entry references a key that is present, and
  the keyed subgraph has no cycle (Kahn's algorithm). A cycle, an unknown
  key, or a duplicate key rejects the whole save, so a pipeline is never
  persisted in an unrunnable state.
- **`needs` are resolved to `depends_on` at save time.** `needs` is the
  authoring form (keys); the backend stores dependencies as `depends_on` (ids).
  When a pipeline is created, updated, or a job is added, the database service
  resolves each job's `needs` (keys) to the ids of the jobs they reference and
  stores the result in `depends_on`. `Database.CreateRun` then creates one job
  instance per definition and remaps each instance's `depends_on` (definition
  ids) to the ids of the run's own instances. The F-06 dependency resolver
  (which resolves `depends_on` by id) dispatches ready instances in parallel
  and skips dependents of a failed job — so F-08 reuses the F-06 resolver
  rather than introducing a parallel DAG engine. Two runs of the same pipeline
  remain fully independent.
- **Pipeline jobs are first-class definitions.** A pipeline carries its job
  definitions (key, name, target group, spec, needs) — created via
  `POST /api/pipelines` (with a `jobs` list) or `PUT /api/pipelines/{id}`, or
  by submitting a job with a `pipeline_id` (`POST /api/jobs`), which persists
  it as a definition on the pipeline (via the database service) rather than
  dispatching it. A run of the pipeline instantiates those definitions.
- **New fields / RPCs.** `Job` (db, scheduler, api) gains `key` (the stable
  pipeline-scoped identifier); the authoring messages — `CreateJobRequest`
  (db), `SubmitJobRequest` (scheduler), and the new `JobDefinition` message
  (key, name, target_group, spec, needs, id) — carry `needs` (the keys), which
  the database service resolves to `depends_on` (ids) at save time.
  `JobDefinition` is carried on `Pipeline`, `CreatePipelineRequest`, and
  `UpdatePipelineRequest` (a `jobs` field).
  `POST /api/pipelines` and `PUT /api/pipelines/{id}` accept a `jobs` list;
  `POST /api/jobs` with a `pipeline_id` persists a definition.

**Triggers (F-09):** a pipeline may carry **triggers** — ways to start a run
other than a manual click: a **cron** trigger (a standard five-field cron
expression), a **webhook** trigger (an external POST; a trigger may
authenticate the caller with a shared secret and/or by **OIDC claims** — the
caller presents a Bearer token from a configured issuer and the trigger
matches only when the token's claims satisfy the trigger's required claims,
with `*` wildcards and dot-addressed nested claims; when a trigger sets
neither a secret nor an OIDC issuer it is open and any POST starts a run),
and an **event** trigger (start a run when a run of another pipeline reaches a
watched status, i.e. chaining). Triggers are stored on the `Pipeline` (a JSON
column) and validated at save time (unique non-empty names; a cron expression
that parses; an event pipeline + status; a webhook's `oidc_claims` entries
have non-empty claim names).

- **`TriggerRun` is the single, atomic claim path.** All three kinds funnel
  through `Database.TriggerRun` (wrapped by the scheduler's `TriggerRun` RPC):
  it creates a `PipelineRun` (started by the named trigger) plus one job
  instance per job definition — reusing `CreateRun`'s `createRunAndInstances`
  — but only if the trigger has not already started a run for the same fire
  window. The check and the create run in one transaction that locks the
  pipeline row, so a racing replica is serialized: the first claim creates the
  run, the rest see it and no-op. The run records its `trigger` source
  (`cron` / `webhook` / `event`), its `trigger_name`, and, for an event
  trigger, its `source_run_id`. For a webhook trigger it also records the
  caller's token claims as the run's `upstream_claims` (a JSON object that
  preserves each claim's structure — a claim that is an object or a list, e.g.
  GitLab's `user_identities` or `job_config`, stays an object/list), which are
  denormalized onto each of the run's job instances (so a job token can carry
  them, see Job Tokens below). The scheduler's `TriggerRun` RPC drives the
  claimed run's instances exactly like `CreateRun` (and no-ops when an earlier
  claim already started the run).
- **Dedup keeps a trigger from firing twice.** A cron trigger passes a dedup
  window (shorter than the minimum cron period): a run the same trigger started
  within the window suppresses a duplicate, so a racing replica cannot fire the
  same scheduled time twice while the next legitimate fire (a full period
  later) is not suppressed. An event trigger records the source run's id: the
  same source run can never start the same downstream run twice.
- **Cron loop.** A leader-gated background loop
  (`internal/services/scheduler/cron.go`, started from `cmd/scheduler`) ticks
  once a second, lists only the pipelines that carry a cron trigger
  (`ListPipelines` with `trigger_type` set to `cron`), keeps the parsed
  schedule and next fire time per (pipeline, trigger), and — when a trigger is
  due — calls `TriggerRun` to claim the run and advances to the next fire
  time. On a restart it recomputes the next fire as `schedule.Next(now)`, so a
  time that already passed is not re-fired; the database's dedup is the
  authoritative guard against the racing-replica case.
- **Event loop.** A leader-gated background loop
  (`internal/services/scheduler/eventtrigger.go`, started from
  `cmd/scheduler`) ticks every few seconds, lists only the pipelines that
  carry an event trigger (`ListPipelines` with `trigger_type` set to `event`)
  to build its rules, and — for each recently-finished run (a lookback
  window) whose pipeline name and status match an event trigger — calls
  `TriggerRun` to start a run of the triggered pipeline, recording the source
  run's id and the source pipeline's name in the run's params. The loop
  matches a run against a rule by the run's `pipeline_name` (preloaded with
  the run by the database service), so a watched pipeline that carries no
  trigger of its own still matches. An in-memory set of already-fired source
  run ids is an optimization; the database's dedup (by source run id) makes a
  re-fire after a restart a no-op.
- **Webhook endpoint.** `POST /api/pipelines/{id}/webhook` (API) looks up the
  pipeline's webhook triggers and, for each, checks every credential the
  trigger sets: the presented secret (constant-time, via the
  `X-Cdrom-Webhook-Secret` header) and, when the trigger has an
  `oidc_issuer`, the caller's Bearer token (verified against the issuer via
  OIDC discovery + JWKS, cached per issuer in
  `internal/api/webhookoidc.go`, and matched against the trigger's
  `oidc_claims` — a `*` value is a wildcard, any other value must be present
  and equal; nested claims are flattened to dot-addressed keys **for matching
  only** so a trigger can require a nested claim by its dot path). A trigger
  matches when **all** of its set credentials are satisfied; a trigger with
  neither a secret nor an `oidc_issuer` is open and matches any request. On a
  match the handler decodes the request's JSON body (a flat object of string
  values; an empty body is allowed) as run parameters, merged over the
  trigger's static params (the body wins on a collision), and calls the
  scheduler's `TriggerRun` (which atomically dedups it), passing the matched
  token's claims (as a JSON object preserving each claim's structure) as the
  run's `upstream_claims`. A request that matches no trigger is 401; a
  pipeline with no webhook trigger is 404.
- **Job-token trigger claims.** The job tokens the API mints for a run's jobs
  carry the run's trigger context so a downstream application (a step handler,
  an external service the job calls) can make allow/deny decisions: a
  `trigger_name` claim (the trigger's name) and a `trigger_type` claim
  (`cron` / `webhook` / `event`), plus — for a webhook-triggered run — the
  run's `upstream_claims` stamped as `upstream_<claim>` (e.g. `upstream_org`,
  `upstream_user`); a claim that is itself an object or a list (e.g. GitLab's
  `user_identities` or `job_config`) is stamped as that object/list, not
  flattened to a string. The API reads these off the job (denormalized onto
  the job instance at run creation) and passes them to the IdP's `job_token`
  grant, which stamps them onto the RS256 token it signs. `ExchangeJobToken`
  re-stamps the same trigger context onto the exchanged token.
- **New RPCs / fields.** `Database.TriggerRun` / `Scheduler.TriggerRun`;
  `Trigger` message + `TriggerType` enum (db); `triggers` on `Pipeline`,
  `CreatePipelineRequest`, and `UpdatePipelineRequest` (db); `trigger_type`
  on `ListPipelinesRequest` (db, so the cron and event loops fetch only the
  pipelines that carry a trigger of that type); `trigger_name`,
  `source_run_id`, `pipeline_name`, and `upstream_claims` (a
  `google.protobuf.Struct` — a JSON object, so a claim that is an object or a
  list keeps its structure) on `PipelineRun` (db, scheduler);
  `upstream_claims` (a `google.protobuf.Struct`) on `TriggerRunRequest` (db,
  scheduler); `oidc_issuer` and `oidc_claims` (a `map<string, string>` of
  dot-addressed claim name → expected value, used for matching) on `Trigger`
  (db); `trigger_name`, `trigger_type`, and `upstream_claims` (a
  `google.protobuf.Struct`) on `Job` (db, api); and `finished_after` on
  `ListRunsRequest` (db, so the event loop can scan recently-finished runs).
  `POST /api/pipelines/{id}/webhook` (API).

**Fan-out (worker groups):** a job that targets a worker group runs on
**every** worker in the group — not on a single worker that wins a claim.
Each worker's run is a separate **`JobExecution`** (one row per
(job, worker, attempt)); the job row's status is the *derived* overall status,
not a worker's report. This replaces the earlier single-claim work queue
(F-23), where exactly one worker won the job via an atomic `ClaimJob`.

- **Starting an execution.** When a worker receives an assignment (or finds a
  pending job for its group on its ~1 s poll) it calls the API's
  `StartJobExecution`, which records the worker's `JobExecution` for the job's
  current attempt and (best-effort) marks the job `running`. The start is
  allowed even after the first worker has already marked the job `running`
  (fan-out: several workers each start their own execution); it is rejected
  only when the job has reached a terminal state or has an empty `target_group`
  (an ephemeral agent, not a worker). The API returns the full job (spec,
  upstream jobs, and a minted job token) so the worker can execute it.
- **Reporting an outcome.** A worker reports its outcome to *its own
  execution*, not the job row: `ReportJobStatus` carries the worker's name,
  and when the job targets a group the API routes the report to
  `Database.UpdateJobExecution` (status, finish time, step results, outputs)
  instead of `Database.UpdateJob`. A late report for an execution that already
  reached a terminal status is a no-op (the conditional update applies only
  while the execution is `running`), mirroring the job-level terminal-status
  guard (F-05). Agents and non-group jobs still report straight to the job row.
- **Deriving the job status.** The scheduler's background **job-status loop**
  (`internal/services/scheduler/jobstatus.go`, started from `cmd/scheduler`,
  leader-gated) periodically re-derives each in-flight group job's status from
  the per-worker outcomes of the workers that are **alive** (a worker is alive
  when its `last_seen_at` is within ~30 s; workers heartbeat every ~10 s),
  per the job's failure mode. Only the most recent execution per alive worker
  is considered: a worker that restarted and re-ran the job supersedes its
  abandoned (pre-restart) execution, and a worker that is not alive is dropped
  entirely (its in-progress execution does not block the job). When the derived
  status differs from the stored one, the loop persists it (`Database.UpdateJob`,
  stamping `finished_at` when the job first reaches a terminal status) and
  appends a `job_status` event to the shared event log (F-23), which every API
  pod tails and fans out to its UI clients.
- **Failure mode.** A job (or its pipeline, as the default) carries a
  `failure_mode` that controls how the per-worker outcomes combine into the
  job's overall status:
  - **`all`** (default): failed if any worker's run failed or timed out;
    running while any is still running; succeeded only if every worker's run
    succeeded.
  - **`best_effort`**: running while any worker's run is still running;
    succeeded once every worker's run reaches a terminal state (a per-worker
    failure or timeout is recorded but does not fail the job).
  - **`any`**: succeeded as soon as one worker's run succeeds; running while
    none has succeeded and some are still running; failed only if every
    worker's run failed or timed out.
  The mode is set on the `JobSpec` (`failure_mode`) and denormalized onto the
  `Job` row at creation (a job's own mode overrides the pipeline's; a job that
  sets none inherits the pipeline's, defaulting to `all`). It is carried on
  the job protos (db, scheduler, api) and surfaced to the UI.
- **Worker restarts.** When a worker re-registers after a restart, the API
  calls `Database.AbandonWorkerExecutions` to mark the worker's stale
  in-progress executions `failed` (the worker will not resume them), so they
  do not block the job's overall status; the job-status loop then re-derives
  the status from the worker's fresh execution (or, if the worker is down,
  drops it via liveness).
- **Watchdog interaction.** The job-level watchdog (F-03) skips group jobs:
  fan-out workers enforce their own execution timeouts, and the job-status
  loop drops dead workers via liveness, so the job-level watchdog would
  otherwise wrongly reap a group job as `timed_out` while other workers are
  still running.

**Shell override (shell handler):** a step may set the `shell` param to run
through a user-chosen interpreter instead of executing `command` directly.
When `shell` is set the target executes `<shell> <args> <command>` — `args`
are the shell's own flags and `command` is passed as the final argument (e.g.
`shell: "pwsh"`, `args: ["-NoProfile", "-Command"]`,
`command: "Get-ChildItem"`). This is the portable way to opt a step into shell
behavior with a specific shell rather than a platform default. When `shell` is
empty the step runs `command` directly, preserving the no-implicit-shell
contract.

**Token exchange handler:** the built-in **`token_exchange`** step handler
(`internal/stephandlers/token_exchange.go`, registered under `token_exchange`)
lets a running job request a new job token for a different audience — e.g. an
outside resource the job needs to call — and hand the exchanged token to later
steps and jobs. The handler reads its settings from the step's `params`:
`audience` (string, required — the audience the new token should be valid
for), `expires_in` (string, a Go duration such as `15m`; empty means the API's
default exchanged-token lifetime), and `output` (string, the name of the step
output the token is written under, default `token`). It obtains the exchanged
token from the execution target through the executor's `TokenExchange`
(`executor.TokenExchange`, set in the context via
`executor.ContextWithTokenExchange`): the worker and agent set the shared
`internal/tokenexchange` implementation, which calls the API's
`ExchangeJobToken` RPC presenting the job's own token. The executor gives every
step a per-step output directory (carried in the step's env as
`executor.StepOutputDirEnv`), so the handler always writes the exchanged token
into it under the output name; the executor records it as the step's output
only when the step declares that name in its `outputs` list, which is how
downstream steps and jobs read it through the condition context (`.steps`/
`.jobs` `.Outputs`). A step that declares no outputs still performs the
exchange (and writes the token to its output directory) but does not capture
it as a step output. A `token_exchange` step whose `audience` is missing, whose
`expires_in` is not a valid duration, or whose target provides no
`TokenExchange` fails the job.

**Step barrier (cross-worker step synchronization):** a job that fans out to a
worker group can be given the `step_barrier` flag (on its `JobSpec`,
denormalized onto the `Job` row at creation). When set, the job's workers
synchronize at each step boundary: after a worker completes a step it reports
the completion to the API (which records a `StepCompletion` row for the job's
current attempt) and then waits until **every worker alive at the step's start
has completed it** before starting the next step. A worker that dies mid-step
is dropped from the barrier (it is no longer alive — its `last_seen_at` is
stale), so a dead worker cannot wedge the job; this is the step-level
counterpart to the job-status loop dropping dead workers. The barrier only
applies to jobs that target a worker group; a job on a single target (an
ephemeral agent) runs its steps unbarriered.

- **Mechanism.** The executor (`internal/executor`) gains a `StepBarrier`
  interface (mirroring the `LogSink` / `StepStatusReporter` context patterns),
  injected via `executor.ContextWithStepBarrier`. After each step that is
  followed by another step, `Execute` calls `barrier.SyncStep(ctx, stepIndex)`
  (bounded by the job's context, so a job stuck at a barrier times out like
  any other hung job). The worker implements it (`internal/worker/barrier.go`):
  it reports the step completion via the API's `ReportStepCompletion` RPC and
  then polls the API's `CheckStepBarrier` RPC until it is satisfied, the job
  is cancelled, or the job's context is done.
- **`CheckStepBarrier` (Database service).** Returns `cancelled` when the job
  is cancelled (a waiting worker stops and reports the job `cancelled`);
  `satisfied` when every alive worker in the job's group has a `StepCompletion`
  row for the job's current attempt and the step in question (a worker is
  alive when its `last_seen_at` is within the liveness window, mirroring the
  scheduler's `workerAliveThreshold`); otherwise not satisfied. A job with an
  empty target group is vacuously satisfied (no barrier).
- **New RPCs.** `Database.ReportStepCompletion` / `CheckStepBarrier` and
  `API.ReportStepCompletion` / `CheckStepBarrier` (the API forwards to the
  Database service; both API RPCs require a job token when job-token auth is
  enabled, like `ReportJobStatus`). The `Job` protos (db, api) and `JobSpec`
  (db) gain a `step_barrier` flag.
- **Cancellation at the barrier.** When the job is cancelled while a worker is
  waiting, `CheckStepBarrier` reports `cancelled`; the worker's barrier returns
  `executor.ErrCancelled`, and the worker reports the job `cancelled` (distinct
  from a failure). A cancellation delivered on the `WatchJobs` stream while a
  step is *running* is handled by the existing F-05 path (the job's context is
  cancelled, interrupting the running step).

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
  api/        API/controller layer: HTTP routing + gRPC control-plane server (worker hub, artifact proxy, StreamJobLogs persistence + job_log fan-out, event hub + /api/ws WebSocket)
  auth/       OIDC authentication (provider, PKCE, Bearer-token verification, discovery, middleware)
  config/     shared configuration loading for all binaries
  models/     GORM entities (single source of truth for the schema)
  gen/        generated gRPC/protobuf Go code (committed; `make proto`)
  grpcutil/   shared gRPC plumbing (client dial, serve-until-signal, TLS credentials)
  logging/    centralized logging (stdout or CDROM_LOG_FILE)
  logstream/  executor.LogSink that streams a job's step output to the API's
              StreamJobLogs RPC (bounded queue, drop-on-full; used by worker + agent)
  tokenexchange/ executor.TokenExchange backed by the API's ExchangeJobToken
              RPC (presents the job's own token); the single shared
              implementation used by worker + agent so a step handler can
              request a job token for a different audience
  executor/   shared job-step executor: the generic dispatch engine (selects a
              StepHandler by step type, enforces the job-level and per-step
              timeouts, returns ErrTimeout on a deadline, carries an optional
              LogSink, an optional StepStatusReporter, an optional StepBarrier,
              and an optional TokenExchange in the context — evaluating each
              step's `condition` (F-06) to skip it when false, reporting each
              step's terminal status for a caller's StepResultCollector to
              attach to its final status report, synchronizing a job's workers
              at each step boundary via the StepBarrier (cross-worker step
              barrier), and letting a step handler request a job token for a
              different audience via the TokenExchange; used by worker + agent)
  stephandlers/ built-in step handlers (the "shell" handler, registered under
              executor.DefaultType, tees step output to a LogSink when present;
              the "token_exchange" handler, registered under token_exchange,
              requests a job token for a different audience via the executor's
              TokenExchange and writes it to the step's outputs); a target or
              plugin adds more via executor.RegisterStepType
  services/
    data/     pipeline/job data service
    artifacts/ general namespaced file store (artifacts + job logs; gRPC
              server; store interface with a filesystem implementation)
    scheduler/ job lifecycle + dispatch (gRPC server) + CreateRun (F-07) + a
              background run-status loop that re-derives each in-flight run's
              status from its job instances (F-07) + a background watchdog
              that reaps running jobs past their timeout (F-03) + a background
              retry loop that re-dispatches failed jobs per their retry policy
              (F-04) + a background dependency resolver that skips/dispatches
              a pending job once its depends_on dependencies resolve (F-06,
              also the resolver F-08's `needs`-based DAG dispatch reuses) + a
              background job-status loop that re-derives each in-flight group
              job's overall status from its fan-out executions + a background
              cron trigger loop that starts a run at each cron trigger's
              scheduled time (F-09) + a background event trigger loop that
              starts a run when another pipeline's run reaches a watched
              status (F-09) + the TriggerRun RPC that atomically claims a
              trigger's fire window and drives the run's instances (F-09)
    database/ storage backend abstraction + gRPC server (SQLite / PostgreSQL)
  worker/     long-lived worker implementation (implements the executor's
              StepBarrier via the API's ReportStepCompletion/CheckStepBarrier
              RPCs, so a job that fans out to a worker group synchronizes its
              workers at each step boundary; sets the shared
              internal/tokenexchange TokenExchange so a step handler can
              request a job token for a different audience)
  agent/      ephemeral agent implementation (sets the shared
              internal/tokenexchange TokenExchange so a step handler can
              request a job token for a different audience)
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
| scheduler   | 127.0.0.1:7102  | `CDROM_LISTEN_ADDR` | `CDROM_DB_ADDR` |
| artifacts   | 127.0.0.1:7103  | `CDROM_LISTEN_ADDR` | — |
| api (gRPC)  | 127.0.0.1:7105  | `CDROM_LISTEN_ADDR` | db, scheduler, artifacts |
| api (HTTP)  | 127.0.0.1:8080  | `CDROM_API_HTTP_ADDR` | — |
| idp         | 127.0.0.1:7104  | `CDROM_LISTEN_ADDR` | — |
| worker      | —               | —                | `CDROM_API_ADDR` |
| agent       | —               | —                | `CDROM_API_ADDR` |

Other notable variables: `CDROM_DB_BACKEND` (`sqlite`|`postgres`),
`CDROM_DB_SQLITE_PATH`, `CDROM_DB_POSTGRES_DSN`,
`CDROM_ARTIFACTS_ROOT`, `CDROM_ARTIFACTS_STORE` (`filesystem`|`s3`|`azureblob`,
default `filesystem`), `CDROM_WORKER_NAME`, `CDROM_WORKER_GROUP`,
`CDROM_AGENT_JOB_ID`, `CDROM_API_ADDR`, `CDROM_API_HTTP_ADDR`, the
mTLS certificate paths `CDROM_TLS_CA_FILE`, `CDROM_TLS_CERT_FILE`,
`CDROM_TLS_KEY_FILE` (equivalents of the `tls` section in the config file),
and the auth variables `CDROM_AUTH_ENABLED` (`true`/`1`),
`CDROM_AUTH_ISSUER`, `CDROM_AUTH_CLIENT_ID`, `CDROM_AUTH_REDIRECT_URL`,
`CDROM_AUTH_TOKEN_AUDIENCE` (equivalents of the
`auth` section in the config file), and the IdP variables `CDROM_IDP_ISSUER`,
`CDROM_IDP_KEY_LIFETIME`, `CDROM_IDP_ROTATE_BEFORE`,
`CDROM_IDP_TOKEN_LIFETIME`, `CDROM_IDP_CHECK_INTERVAL` (equivalents of the
`idp` section in the config file), and the job-token auth variables
`CDROM_GRPC_AUTH_ENABLED` (`true`/`1`), `CDROM_GRPC_AUTH_IDP_ADDR`,
`CDROM_GRPC_AUTH_AUDIENCES` (comma-separated), and
`CDROM_GRPC_AUTH_EXCHANGED_TOKEN_LIFETIME` (a duration, e.g. `15m`; the
default lifetime of an exchanged job token, defaulting to 15 minutes;
equivalents of the `grpc_auth` section in the config file).

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
./bin/scheduler   # dials db (publishes to the shared event log; runs the
                  # background loops only while it holds the leader lease)
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
#   auth.redirect_url: http://127.0.0.1:8080/api/auth/callback
# Clients sign in against the IdP (authorization-code + PKCE) and then call
# the API with `Authorization: Bearer <access_token>`.
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
  `client_id`, and `redirect_url` are then required and validated at startup.
- **Flow:** the API is a pure token verifier — it does not run the OIDC
  sign-in itself. A client (the UI, curl, an OpenAPI reference) performs the
  OIDC authorization-code + PKCE flow against the identity provider and then
  presents the resulting OAuth token on every request as
  `Authorization: Bearer <token>`. `GET /api/auth/oidc` advertises the IdP's
  discovery document (issuer, authorization/token endpoints, JWKS URI,
  client_id, redirect_uri) so a client can start the flow; a 404 means auth
  is disabled. The API verifies each token against the IdP's JWKS (via
  `github.com/coreos/go-oidc/v3`) and, when `token_audience` is set, checks
  the token's `aud` against it (otherwise against `client_id`).
- **Middleware:** when auth is enabled, every `/api/*` request (including the
  WebSocket upgrade) must carry a valid `Authorization: Bearer <token>` or it
  gets a 401 JSON response; the authenticated user is available to handlers
  via `auth.UserFromContext`. When disabled the middleware is a passthrough.
- **Live events:** `GET /api/ws` is a WebSocket endpoint (gorilla/websocket).
  On connect the client receives a state snapshot (jobs + workers + runs);
  afterwards it receives one message per event. Event types: `job_status`
  (job id + status), `worker` (worker name/group + action `registered` /
  `deregistered` / `watching`), `job_log` (job id + step index + stream
  `stdout`/`stderr` + a chunk of output, F-02), and `run_status` (run id +
  status, F-07). Events are published from the gRPC server (worker lifecycle,
  job status reports, streamed job logs, run status notifications) and the
  HTTP handlers (job submit / cancel, run trigger) through a shared `EventHub`
  (`internal/api/events.go`); slow subscribers have events dropped and resync
  from the snapshot on reconnect.
- **Local OIDC IdP (`cmd/idp`):** a standalone HTTP service that acts as a
  JWT issuer so a local run can exercise the full OIDC flow without an
  external identity provider. It serves `/.well-known/openid-configuration`,
  `/jwks`, `/auth` (authorization-code + PKCE), and `/token` (mints RS256
  tokens: a signed access token — the same claims as the ID token, so clients
  present it as `Authorization: Bearer <token>` — plus the ID token). It
  signs with an RSA key and **rotates it automatically** when the
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

Separate from the UI's OIDC token, the API's **gRPC surface** (workers and
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
open. The API mints one RS256 token per job: on dispatch it is delivered to
workers inside the `JobAssignment` (gRPC field `token`), and on `GetJob` it is
returned in the `Job` for ephemeral agents. Claims: `iss`,
`sub: "job:<jobID>"`, `aud` (the configured audiences), `exp`, `iat`,
`job_id`, `pipeline_id`, `target_group`, `token_type: "job"`, plus — when the
job belongs to a trigger-fired run — `trigger_name` and `trigger_type` and,
for a webhook-triggered run, the run's `upstream_claims` stamped as
`upstream_<claim>` (see the Job-token trigger claims note under Triggers
above). The main job token's `exp` is a **long placeholder (a week)**: the
token is meant to live as long as the job runs (which can be days), so it
effectively never expires — its validity is gated on the job's live status in
the database (see Verification below), not on `exp`.
- **Exchange:** a job can request a **new token for a different audience**
(e.g. an outside resource it must call) via the `ExchangeJobToken` RPC. The
caller presents its existing job token (scoped to the job) and a target
`audience`; the API mints a fresh token from the IdP stamped for that
audience. Any audience is accepted — the IdP stamps whatever is requested.
The exchanged token re-stamps the job's trigger context (`trigger_name`,
`trigger_type`, and `upstream_*` claims) so the downstream audience sees the
same identity that triggered the run. Unlike the main job token, an exchanged
token has a **short lifetime** (a scoped credential for an outside resource):
the request's `expires_in` when set, else the configured default
(`grpc_auth.exchanged_token_lifetime`, defaulting to 15 minutes).
- **Verification:** when enabled, `ReportJobStatus` and the artifact RPCs
(`UploadArtifact`, `DownloadArtifact`, `GetArtifact`, `ListArtifacts` with a
`namespace`, `DeleteArtifact`) require a `Bearer <token>` in the gRPC
`authorization` metadata. The API verifies the token against the IdP's JWKS
(OIDC discovery against `grpc_auth.idp_address`), checks that one of the
configured audiences is present, and rejects the call unless the token's
`job_id` matches the job the call targets — for artifact RPCs the namespace
is parsed back to a job id (a job's artifacts and logs use the job's id as
the namespace) before the check. The API **skips the token's `exp` check**
(the main job token's `exp` is a long placeholder) and instead gates the
token's validity on the job's **live status in the database**: a token for a
job that has reached a terminal state (`succeeded`, `failed`, `cancelled`,
`timed_out`, or `skipped`) is rejected, so a job's token is invalidated the
moment the job stops running (missing/invalid token → `Unauthenticated`,
wrong job or a finished job → `PermissionDenied`). Workers and agents
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
- **Event-log retention (F-23, remaining).** The high-availability control plane
  (F-23) is implemented: a shared **event log** every API pod tails, **hybrid
  push + pull** dispatch (workers poll pending-for-group ~1 s as the
  authoritative path; API pods tail the log at ~50 ms and nudge local workers),
  **scheduler leader election** (a portable lease on the Database service so only
  one replica runs the background loops; ownership and liveness are
  decoupled — a long TTL backstop keeps ownership stable across transient blips
  while a short heartbeat window lets a follower take over quickly once the
  leader's heartbeat goes stale), and **cross-pod near-live logs** (a lean
  `job_log_updated` pointer event + a range read from the shared artifacts
  store). See `docs/HighAvailability.md`. The one open item is
  **event-log retention**: the `events` table is a plain append-only log today
  and is not pruned; partitioning by `created_at` + a retention schedule (drop
  old partitions) is a deployment concern to add when the log's growth warrants
  it.
