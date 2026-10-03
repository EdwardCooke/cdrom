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
| Job execution | **Real** — a job carries a `JobSpec` (ordered steps; each step's `type` selects a handler, defaulting to the built-in `shell` handler that reads its command/args/shell from the step's `params` and runs them with workdir, env, and a per-step timeout); the worker/agent run the steps in order via `internal/executor` and fail on the first step that errors. A job-level `timeout` bounds the whole job and a per-step `timeout` bounds a step; exceeding either terminates the job as `timed_out` (F-03), and a scheduler watchdog reaps jobs whose target goes silent |
| Retry & re-run | A job's `JobSpec` may carry a `retry` policy (`max_attempts` retries after the initial attempt, `backoff` delay); a scheduler retry loop re-dispatches a failed job up to the limit (a new attempt on the same `Job` row, `attempt` counter, F-04). A user can re-run any finished job (`POST /api/jobs/{id}/rerun`) for a fresh attempt; attempts are visible to the UI ("attempt N of M") |
| Cancellation | Cancelling a running job (`POST /api/jobs/{id}/cancel`) signals the execution target to stop the work (F-05): the API delivers a `JobCancellation` down the worker's `WatchJobs` stream (or an agent observes it on its next `GetJob`), the target interrupts the running step and reports `cancelled`; cancelling a finished job is a no-op |
| Job tokens | Minted per job by the API (via IdP), audience exchange, verified on status/artifact RPCs |
| UI auth | OIDC authorization-code + PKCE; the API verifies the OAuth token as a Bearer token (no session cookie) |
| Artifacts | General-purpose, namespaced file store (streamed upload/download); jobs scope files by job id, other callers (e.g. deployed releases) use their own namespaces; proxied through the API |
| Live events | WebSocket `/api/ws` — `job_status`, `worker`, and `job_log` events |
| Job logs | Streamed from the target to the API (`StreamJobLogs`), persisted to the artifacts service (one file per step + `job.log`), fanned out to the UI as `job_log` events, and retrievable via `GET /api/jobs/{id}/logs[/{name}]` |
| Transport | gRPC everywhere, optional mutual TLS |

The biggest remaining gap: **jobs are not orchestrated** (no runs, DAG, or
triggers). Everything below builds toward orchestrating real jobs, then
governing them.

---

## Phase 1 — Core execution

Make a job actually do work, and make its execution observable and controllable.
These are the foundation every other feature depends on.

### F-01 · Job execution spec

**What.** A job carries a declarative spec describing the work to perform: an
ordered list of **steps**. Each step is agnostic about how it runs: a `type`
field selects a step handler, and the handler reads everything it needs from
the common fields (`workdir`, `env`, `timeout`) and from `params`.
Handler-specific settings live in `params` (a map of name → value, where a
value is either a scalar string or a list of strings), not in the step's own
fields, so a new step type can be added without changing the spec schema. The
built-in `shell` handler (default when `type` is empty) runs a command,
directly or through a user-chosen shell, reading its `command`, `args`, and
`shell` from `params`. The worker/agent executes the steps in order and the
job fails on the first step that errors.

**Why.** CD jobs are fundamentally "run these commands in this environment".
Without a spec, a job is just a label. This is the single most important
missing piece.

**Scope.**
- `internal/models` — add a `JobSpec` (or embed steps in `Job`): steps with
  `type`, `workdir`, `env`, `timeout`, and a `params` map carrying
  handler-specific settings. Decide whether the spec is stored on the `Job`
  row (per-run snapshot) or referenced from the `Pipeline` (shared
  definition). **Recommendation:** store a snapshot on the run so a pipeline
  edit never changes what a past run did.
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
- [x] A step's `shell` param runs the step through a user-chosen shell
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
- **Shell contract:** the shell handler reads its `command` (string param),
  `args` (list param), and `shell` (string param) from the step's `params`.
  The command is executed directly by the target OS — no implicit shell. Steps
  needing shell behavior invoke a shell explicitly (`sh -c ...` / `cmd /c ...`).
  Step stdout/stderr are inherited from the target (local logging) and, when a
  log sink is present, also streamed to the API (F-02).
- **Shell override:** a step may set the `shell` param to run through a
  user-chosen interpreter; the target executes `<shell> <args> <command>`
  (e.g. `shell: "pwsh"`, `args: ["-NoProfile", "-Command"]`,
  `command: "Get-ChildItem"`). When `shell` is empty the step runs `command`
  directly, preserving the no-implicit-shell contract.
- Per-step `timeout` is enforced by the target via a derived context; a
  job-level `timeout` bounds the whole job (F-03), and a scheduler-side
  watchdog reaps jobs whose target goes silent (F-03).

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
- `proto/cdrom/artifacts/v1/artifacts.proto` - new store, retrieve and append to log
  endpoints.
- `internal/artifacts/filesystem/` - filesystem based implementation for storing and
  updating artifacts on the local file system. Currently focused on logs, will focus
  on package storage later.
- `cmd/artifacts/main.go` - wire up the filesystem based artifact implementation.
  Implementation type should be configurable to allow for other builtin implementations
  example: S3 or Azure Blob.
**Acceptance criteria.**
- [x] While a job runs, its stdout/stderr appears in the UI within ~1s.
- [x] Logs are attributed to the correct step.
- [x] After a job finishes, its full log can still be fetched (replay).
- [x] A slow UI client does not block the worker (backpressure/drop policy
      defined, consistent with the existing EventHub drop-and-resync behavior).
- [x] The step logs should be stored via the artifact service and updated as
      the step progresses.
- [x] Ability to retrieve the logs for a particular step and job from the api server.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **Transport:** a new `StreamJobLogs` client-stream RPC on the API
  (`proto/cdrom/api/v1/api.proto`). The target opens one stream per job and
  sends a `JobLogChunk` per output chunk; the first chunk of a step's output
  carries `JobLogMetadata` (job_id, step_index, stream) and the rest carry
  data. The API verifies the caller's job token once, up front.
- **Storage:** logs are stored in the **artifacts service** (not the DB), as
  files under `<root>/<namespace>/logs/` where the namespace is the job's id —
  one file per step (`step-<n>.log`) plus a combined `job.log`. New artifacts
  RPCs `AppendLog` (client stream, append-or-create), `DownloadLog` (server
  stream), `GetLog`, and `ListLogs` back them. The artifacts service is a
  general-purpose, namespaced file store: its store is an interface
  (`internal/services/artifacts/store.go`) with a filesystem implementation
  (`filesystem.go`); the store kind is configurable (`artifacts_store`,
  `CDROM_ARTIFACTS_STORE`, default `filesystem`) so S3 / Azure Blob can be
  added later without touching the server.
- **Capture:** the executor takes an optional `executor.LogSink`
  (`WriteStepOutput(stepIndex, stream, data)`) via the context. The built-in
  shell handler tees each step's stdout/stderr to the target's local streams
  *and* to the sink when one is present (via `StdoutPipe`/`StderrPipe`); with
  no sink it inherits `os.Stdout`/`os.Stderr` as before, so local logging is
  unchanged.
- **Sink & backpressure:** `internal/logstream.Sink` buffers chunks in a
  bounded channel (1024) drained by a background goroutine to the gRPC
  stream. When the queue is full (the API is slower than the command produces
  output) chunks are **dropped** — the sink never blocks or fails the job.
  This is consistent with the EventHub drop-and-resync behavior: the
  persisted log (written by the API from the chunks it did receive) is the
  source of truth for replay, and a slow UI client resynchronizes from it on
  reconnect.
- **Resilience (outages & redeploys):** both sides ride out a peer going away
  (a pod restart or scale event in Kubernetes). *Target side:* if the API's
  `StreamJobLogs` stream breaks, the sink reopens a fresh stream (the gRPC
  channel reconnects on its own) and resumes; chunks queued while the API was
  down are resent, and if the API is still down when the queue fills they are
  dropped. *API side:* if the artifacts service is unreachable, the API
  retries persisting each chunk (`appendLogWithRetry`) and, if the outage
  outlasts the retry budget, drops the chunk and continues — an artifacts
  outage never tears down a target's log stream. The gRPC connections
  themselves (API↔artifacts, target↔API) are long-lived `*grpc.ClientConn`s
  that reconnect transparently.
- **Fan-out:** the API publishes a `job_log` event (job id, step index,
  stream, data) on the `EventHub` for each chunk, so the UI can tail a
  running job's logs over the existing `/api/ws` WebSocket.
- **Retrieval:** `GET /api/jobs/{id}/logs` lists a job's log files and
  `GET /api/jobs/{id}/logs/{name}` streams one (a step's output or the
  combined `job.log`) — both proxied through the artifacts service.

### F-03 · Job timeouts

**What.** A job (and optionally each step) can declare a maximum duration. If
execution exceeds it, the job is marked `timed_out` and the running process is
terminated. A job-level timeout bounds the whole job (all steps combined); a
per-step timeout bounds an individual step. When both are set, whichever
expires first terminates the job.

**Why.** Hung jobs hold workers forever; timeouts are a basic safety valve.

**Scope.**
- `internal/models` / proto — `timeout` on the job spec (and per step).
- `internal/worker`, `internal/agent` — enforce the deadline, cancel the
  running command, report the terminal status.
- `internal/services/scheduler` — a watchdog that reaps jobs that exceed their
  timeout even if the target goes silent (covers a dead worker).

**Acceptance criteria.**
- [x] A job whose command sleeps past its timeout is terminated and marked
      failed/timed-out.
- [x] The timeout is enforced by the target *and* by the scheduler watchdog
      (a target that stops reporting is still reaped).
- [x] A job with no timeout runs unbounded.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **Two levels of timeout.** A job declares a job-level `timeout` on its
  `JobSpec` (the sum of all steps) and each step may declare its own `timeout`
  (an individual step). The executor derives a context for the job-level
  timeout in `Execute` and a nested context for a step's timeout in `runStep`;
  whichever deadline expires first cancels the running step. A job with no
  job-level timeout and no per-step timeouts runs unbounded (bounded only by
  the caller's context).
- **`timed_out` is a distinct terminal status.** A new `JOB_STATUS_TIMED_OUT`
  (proto) / `JobStatusTimedOut` (model) status is added alongside the existing
  terminal states. The executor returns a sentinel `executor.ErrTimeout` (wrapped
  with the context) when a deadline expires; the worker and agent check
  `errors.Is(err, executor.ErrTimeout)` and report `timed_out` rather than
  `failed`, so the UI can tell a hung job apart from one that ran and errored.
  The shell handler wraps `executor.ErrTimeout` with a clear message (the
  command name and, for a per-step timeout, its duration).
- **Scheduler watchdog.** The scheduler runs a background watchdog
  (`internal/services/scheduler/watchdog.go`, started from `cmd/scheduler`)
  that periodically lists running jobs and reaps any that have exceeded their
  effective timeout. A job's **effective timeout** is its job-level timeout
  when set, otherwise the longest per-step timeout; a job with neither is never
  reaped. The deadline is measured from the job's `started_at`. Reaping is
  **conditional** (`Database.ReapJob`): it marks the job `timed_out` only if it
  is still `pending` or `running` (an atomic `WHERE status IN (...)` update),
  so a job that already reported a terminal status is left untouched. When a
  job is reaped the watchdog calls the API's `NotifyJobStatus` RPC to fan the
  status change out to the UI over the WebSocket event hub, mirroring the
  `job_status` events a target's report would produce.
- **New RPCs.** `Database.ReapJob` (conditional `timed_out` update, returns
  whether the job was reaped) and `API.NotifyJobStatus` (fan a status change
  out to the UI; does not touch the database — the scheduler already persisted
  it). The job-level `timeout` is carried on `JobSpec` in the db proto and
  round-trips through the model (a `text` JSON column) and the API's
  `POST /api/jobs` body (`spec.timeout`, a duration string).

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
- [x] A job with `retry: 2` that fails is re-dispatched up to 2 more times.
- [x] Backoff delays are applied between attempts.
- [x] Re-running a finished job produces a new execution with a fresh status.
- [x] Attempts are visible (UI shows attempt N of M).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **A retry is a new attempt on the same `Job` row** (not a new `Job`), so the
  pipeline run stays coherent. The job's `attempt` counter (1-based) is
  incremented by the scheduler each time it re-dispatches a failed job, and the
  job is reset to `pending` (the finished timestamp is cleared). `max_attempts`
  is denormalized onto the job (copied from the spec's retry policy at
  creation) so the UI can render "attempt N of M" without the spec.
- **Retry policy on the spec.** A job's `JobSpec` carries an optional `retry`
  policy: `max_attempts` (the number of retries *after* the initial attempt; 0
  means the job is never retried) and `backoff` (the delay before each retry;
  0 means retries are dispatched immediately). A job with `max_attempts 2` runs
  at most 3 times (the initial attempt plus 2 retries).
- **Scheduler retry loop.** The scheduler runs a background **retry loop**
  (`internal/services/scheduler/retry.go`, started from `cmd/scheduler`) that
  periodically asks the database for the jobs that still have retries remaining
  (`Database.ListRetriableJobs` — failed jobs with `max_attempts > 0` and
  `attempt < max_attempts + 1`, filtered in the database so the loop never
  pulls every failed job over the wire) and, for each, atomically claims the
  next attempt (`Database.ClaimJobRetry`) and re-dispatches the job to the API
  (which fans it out to the live workers) after the policy's backoff. A job
  with no retry policy or one that has exhausted its retries is never
  re-dispatched.
- **New RPCs.** `Database.ListRetriableJobs` (returns the failed jobs that
  still have retries remaining, filtered in the database), `Database.ClaimJobRetry`
  (conditional atomic update: increments `attempt` and resets the job to
  `pending` only if it is in a retryable state — pending, running, failed, or
  timed_out — and has not exhausted its budget; returns the claimed attempt and
  whether it was claimed), and `Database.RerunJob` (resets a finished job to
  `pending` with a fresh attempt 1; only terminal-state jobs can be re-run).
  The scheduler's `RerunJob` RPC calls `Database.RerunJob` and then
  re-dispatches the job.
- **Re-run.** `POST /api/jobs/{id}/rerun` (HTTP) → the scheduler's `RerunJob`
  RPC resets a finished job (succeeded, failed, cancelled, or timed_out) to
  `pending` with a fresh attempt and re-dispatches it, so the job runs again
  with the same spec. A job that is still pending or running is rejected
  (`FailedPrecondition`).
- **Attempt visibility.** The `attempt` and `max_attempts` fields are carried
  on the job protos (db, api, scheduler) and surfaced to the UI: the WebSocket
  snapshot includes them per job, and `job_status` events carry them (the
  scheduler passes them on `NotifyJobStatus` for a retried job's reset to
  pending and for a watchdog-reaped job), so the UI can show "attempt N of M".

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
- [x] Cancelling a running job stops the command on the target within a bounded
      time.
- [x] The job's final status is `cancelled` and the target stops work.
- [x] Cancelling an already-finished job is a no-op (idempotent).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **The cancel signal rides the existing `WatchJobs` stream.** The API's
  `WatchJobs` server stream now carries a `WatchMessage` (a oneof of
  `JobAssignment` and `JobCancellation`) instead of a bare `JobAssignment`.
  When the scheduler cancels a job it calls the API's `CancelJob` RPC, which
  pushes a `JobCancellation` down every live worker's stream. A worker that is
  running the job interrupts the running step (cancelling the job's context,
  which terminates the command) and reports `cancelled`; a cancellation for a
  job that is not running on that worker is ignored (the job's status is
  reconciled by the database).
- **Ephemeral agents observe the cancellation on the API.** An agent holds no
  `WatchJobs` stream, so it cannot be signalled directly. Instead it polls the
  API's `GetJob` on a short interval while the job runs; when it sees the job
  is `cancelled` it interrupts the running step and reports `cancelled`. The
  scheduler's `CancelJob` marks the job `cancelled` in the database, which the
  agent's next poll observes.
- **Persistence is conditional and idempotent.** The scheduler's `CancelJob`
  persists the cancellation with a conditional `Database.CancelJob` update
  (marks the job `cancelled` only if it is still `pending` or `running`), so
  cancelling an already-finished job is a no-op. The target is then signalled
  best-effort: if it cannot be reached (no live worker, or the API is down)
  the job is still marked `cancelled` in the database, and the target's next
  status report (or the scheduler's watchdog) reconciles it.
- **A late target report cannot clobber a terminal status.** `Database.UpdateJob`
  is now conditional: a status report from a target is applied only if the job
  is still `pending` or `running`. Once a job is `succeeded`, `failed`,
  `cancelled`, or `timed_out`, a late report (e.g. a target that finished just
  as it was cancelled) is ignored, so the terminal status is authoritative.
  The API's `ReportJobStatus` publishes the job's actual (post-update) status,
  not the requested one, so a no-op report emits no spurious event.
- **New RPCs.** `API.CancelJob` (signal the execution target to stop the work;
  delivers a `JobCancellation` down the worker's `WatchJobs` stream) and
  `Database.CancelJob` (conditional `cancelled` update, returns whether the job
  was cancelled). The scheduler's `CancelJob` RPC calls `Database.CancelJob`
  and then `API.CancelJob`.

### F-06 · `skipped` job and step status

**What.** Add a `skipped` status for jobs and steps that never run because a dependency
failed or a condition was not met (pairs with F-08 dependencies). Conditions
should be implemented using go templates that must render to a value converted to a
boolean. F-06 also lands three supporting mechanisms that make conditions and
dependencies genuinely useful:

- **`ignore_failed`** — a step (or a job) may declare that its failure should
  not stop the job / should not block its dependents.
- **Step & job `outputs`** — a step may produce named values (written to a
  per-step output directory) that are recorded on the step and aggregated onto
  the job, so downstream steps and jobs can read them.
- **Richer condition context** — a step's condition can reference not just its
  own env, but the status/outputs of prior steps in the same job, the
  status/outputs of upstream jobs higher in the pipeline chain, and the job's
  own identity.

**Why.** In a DAG, a downstream job of a failed job is "skipped", not
"failed" — the distinction matters for reporting and for `on_failure` logic.
`ignore_failed` lets a non-critical step (or a best-effort upstream job) fail
without derailing the pipeline; `outputs` let a step hand a value (a version
string, a build id, an artifact path) to later steps and jobs; and the richer
condition context is what lets a step say "only run if the build step produced
`ready == true`" or "only run if the upstream `build` job succeeded".

**Scope.**
- `internal/models` + `proto/cdrom/db/v1/db.proto` — new `JobStatusSkipped` and `StepStatusSkipped`.
- `internal/services/scheduler` — mark downstream jobs skipped when an
  upstream fails.
- UI — render the new status.

**Acceptance criteria.**
- [x] A job whose dependency failed is marked `skipped`, not `failed`.
- [x] `skipped` is a terminal state.
- [x] The status is surfaced in `job_status` events (the UI itself has no
      source yet — see the design decisions below).
- [x] A steps condition is not met is marked `skipped`.
- [x] A step with `ignore_failed` that fails (or times out) is recorded as
      failed but does not stop the job — the next step still runs.
- [x] A step that declares `outputs` writes one file per name into a per-step
      output directory (exposed via `CDROM_STEP_OUTPUT_DIR`); the executor
      reads them back (trimmed) and a declared-but-unwritten name is recorded
      as an empty string.
- [x] A job's `outputs` are the union of its steps' outputs (a later step
      overrides an earlier one on a name collision) and are persisted on the
      job row.
- [x] A step's condition can reference prior steps' status/outputs
      (`.steps`), upstream jobs' status/outputs (`.jobs`), and the job's
      identity (`.job`).
- [x] A dependency that failed or timed out but has `ignore_failed` set counts
      as satisfied (the dependent job is dispatched, not skipped); a cancelled
      or skipped dependency always blocks (skip propagates).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **`depends_on` is a minimal stand-in for F-08's `needs`/DAG, not F-08
  itself.** F-06 lands ahead of F-08 in the roadmap, and F-08 owns the full
  DAG resolver (named `needs`, cycle validation at save time, parallel
  dispatch). To implement F-06's "a downstream job of a failed job is
  skipped" without that full engine, `Job` gained a minimal `depends_on`
  field (`[]int64`/`[]uint`, F-06): a flat list of raw job ids, checked by a
  simple single-level, polling background resolver
  (`internal/services/scheduler/dependencies.go`) rather than a parallel DAG
  engine. This is explicitly scoped to be replaced or subsumed by F-08's
  resolver later (see AGENTS.md's "Skip propagation & step conditions
  (F-06)" section for the full design note).
- **`SubmitJob` withholds dispatch for a job with dependencies.** A job
  submitted with a non-empty `depends_on` is created `pending` but, unlike a
  dependency-free job, is not dispatched immediately; the dependency resolver
  (not `SubmitJob`) decides its fate once its dependencies resolve.
- **The dependency resolver is a background polling loop**, modeled on the
  existing watchdog (F-03) and retry loop (F-04): it periodically lists
  pending jobs with a non-empty `depends_on`, and for each checks every
  dependency's status (`Database.GetJob`). Any dependency in a terminal state
  other than `succeeded` (`failed`, `cancelled`, `timed_out`, or itself
  `skipped`) skips the job (`Database.SkipJob`, conditional: only from
  `pending`, mirroring `CancelJob`'s pattern) — this is what makes skip
  **propagate** transitively through a chain, one tick at a time. Once every
  dependency succeeds, the job is released: its `depends_on` is cleared
  (`Database.UpdateJob` with a new `clear_depends_on` flag, so the resolver
  does not re-dispatch it on a later tick) and it is dispatched exactly as
  `SubmitJob` would have for a dependency-free job.
- **Step `condition` is a Go template rendered against the step's own `Env`**,
  parsed as a boolean (`strconv.ParseBool`). An empty `condition` never skips.
  A `condition` that fails to render or parse as a boolean **fails the job**
  (not skipped) — treated as a spec/configuration error, since the
  acceptance criteria's "must render to a value converted to a boolean"
  implies a malformed condition is a mistake, not a legitimate skip signal.
- **The condition context is richer than the step's own env.** A step's
  condition is rendered against a map that carries, in addition to the step's
  env vars (top-level, so `{{ .NAME }}` still works), three keys: `steps`
  (a slice of prior steps, each exposing `.Index`, `.Status`, and `.Outputs`),
  `jobs` (a slice of the job's upstream dependencies, each exposing `.ID`,
  `.Name`, `.Status`, and `.Outputs`), and `job` (the job's own identity:
  `.ID`, `.Name`, `.Status`). The upstream jobs and the job identity are
  supplied by the execution target via the executor's context
  (`executor.ContextWithUpstreamJobs` / `executor.ContextWithJobIdentity`);
  the API populates a job's `upstream_jobs` (fetched best-effort from the
  database for each `depends_on` id) before handing the job to the target.
  Example: `{{ if eq (index .jobs 0).Status "succeeded" }}true{{ else
  }}false{{ end }}`.
- **`ignore_failed` is a per-step and per-job flag.** A step with
  `ignore_failed` that fails or times out is recorded (in `step_results`) but
  does not stop the job — the executor moves on to the next step. A job with
  `ignore_failed` (denormalized from its spec onto the `Job` row at creation)
  tells the dependency resolver that a `failed`/`timed_out` dependency counts
  as satisfied, so dependents are dispatched rather than skipped. A
  `cancelled` or `skipped` dependency is never overridden by `ignore_failed`
  (a cancellation is not a failure a job can opt out of).
- **Step outputs use a per-step temp directory.** A step that declares
  `outputs` (a list of names) is given a fresh per-step directory, exposed to
  it via the `CDROM_STEP_OUTPUT_DIR` env var; the step's command writes one
  file per declared name into it, and the executor reads them back (trimmed)
  after the step runs. A declared name that was never written is recorded as
  an empty string. The job's `outputs` are the union of its steps' outputs
  (a later step overrides an earlier one on a name collision), aggregated by
  the `StepResultCollector` and reported on the job's final `ReportJobStatus`
  call, where `Database.UpdateJob` persists them (via the same
  `Select`/`Updates` serializer pattern as `step_results`).
- **Per-step outcomes are collected and persisted.** The executor gained a
  `StepStatusReporter` interface (set in its context, mirroring the existing
  `LogSink` pattern) and a `StepResultCollector` helper; the worker and agent
  create one per job, wire it into the executor's context, and attach the
  collected results (`executor.StepResultCollector.ToProto()`) to the job's
  final `ReportJobStatus` call as `step_results`. `Database.UpdateJob`
  persists `step_results` unconditionally (informational, not part of the
  terminal-status guard that protects `status`/timestamps from a late
  report).
- **GORM gotcha: a raw map-based `Update(column, value)` does not invoke a
  field's `serializer:json` tag** — GORM only runs a field's serializer when
  the update value flows through the model's reflected field (i.e.
  `Updates(&Model{Field: value})` with an explicit `Select`), not through a
  `map[string]any`-keyed single-column `Update`. This was caught by a round-trip
  test (`TestUpdateJobStepResultsRoundTrip`) that failed with a SQLite driver
  error until `UpdateJob`'s `step_results` and `clear_depends_on` persistence
  were switched from `.Update("Field", value)` to
  `.Select("Field").Updates(&models.Job{Field: value})`.
- **UI is out of scope for this change**: `ui/` has no source yet (same as
  F-01–F-05), so "render the new status" / "surfaced in the UI" reduces to
  the backend/event-hub work (the `job_status` event's `status` field already
  carries `"skipped"`); a future UI implementation picks this up for free.

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
- [x] Triggering a pipeline creates a `PipelineRun` and one job instance per
      job definition.
- [x] Two runs of the same pipeline are independent and both queryable.
- [x] A run's overall status is derived from its jobs (succeeded only if all
      non-skipped jobs succeeded).
- [x] Historical runs are retained and browsable.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **`PipelineRun` is a first-class entity.** A run is one execution of a
  pipeline: it carries `pipeline_id`, an overall `status` (`pending`,
  `running`, `succeeded`, `failed`, `cancelled`), a `trigger` (how it was
  started, e.g. `manual`), `params` (run parameters, F-10), and
  `started_at`/`finished_at`. A `Job` gains a `run_id` and becomes a
  per-run *instance* of the pipeline's job definitions, so the pipeline
  *definition* is separated from each *run* of it.
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
  (source namespace → target namespace/environment).
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

## Phase 5 — High availability

Making the control plane survive pod churn and scale beyond a single API
instance. The full design (event log, hybrid push/pull dispatch, leader
election, cross-pod live logs, shared artifacts store) is in
[`HighAvailability.md`](HighAvailability.md).

### F-23 · High-availability control plane

**What.** Run multiple API pods and multiple scheduler replicas without losing
job delivery or live events: a shared Postgres **event log** that every API pod
tails, **hybrid push + pull** job dispatch (workers poll pending-for-group ~1 s
as the authoritative path; the event log is the ~50 ms fast path), **scheduler
leader election** so only one replica runs the background loops, and
**cross-pod near-live logs** (a lean `job_log_updated` pointer event + a range
read of the log delta from the shared artifacts store).

**Why.** Today the control plane assumes a single API instance: worker streams,
the UI event hub, and job dispatch are per-pod in-memory state. A worker on pod
B never receives a job dispatched via pod A (stranded `pending` forever), UI
clients only see events from their own pod, and >1 scheduler replica duplicates
dispatches. This makes the platform deployable as a real HA service (e.g. 4 API
pods per cluster, dual cluster).

**Scope.**
- `proto/cdrom/db/v1/db.proto` — `events` table + publish RPCs
  (`PublishAssignment` / `PublishCancel` / `PublishJobStatus` /
  `PublishRunStatus` / `PublishWorkerEvent` / `PublishLogUpdated`),
  `TailEvents(cursor, limit)`, `ClaimJob` (atomic pending→running),
  `ListPendingByGroup`, and leader-election lease RPCs (`AcquireLease` /
  `ReleaseLease`).
- `internal/services/database` — `events` + `leases` tables (GORM
  `AutoMigrate`); publish RPCs; `TailEvents`; `ClaimJob`; `ListPendingByGroup`;
  `AcquireLease` / `ReleaseLease` (a portable compare-and-swap that works on
  both SQLite and PostgreSQL — no Postgres advisory lock).
- `internal/api` — per-pod 50 ms event-log tail loop (cursor + group filter +
  fan-out to local `watchers` / `EventHub`); `job_log_updated` handler (per-
  (job, log) byte cursor + range read + WS fan-out, skipped when the pod has
  no UI subscribers); log-event coalescer (200 ms window) in `handleLogChunk`;
  `ClaimJob` / `ListPendingJobs` RPCs; the one-shot `dispatch()` push is kept
  only as a same-pod fast path — the event log + worker poll are the source of
  truth.
- `internal/worker` — 1 s poll of pending-for-group + atomic `ClaimJob` (the
  claim returns the fresh spec + token); robust reconnect (2 s backoff) on the
  `WatchJobs` stream.
- `internal/services/scheduler` — drop the API push client for dispatch (publish
  to the DB instead); leader election (a portable lease on the Database
  service) gating the watchdog / retry / dependency-resolver / run-status
  loops, so only one replica runs them.
- `proto/cdrom/artifacts/v1/artifacts.proto` + `internal/services/artifacts` —
  `offset` on `DownloadLogRequest` (0 = whole log); `Store.DownloadLog(ctx,
  ns, name, offset)` on the store interface and the filesystem implementation
  (open + `Seek`); S3 / Azure Blob range reads later.
- Deployment: shared artifacts store root (CephFS / NFSv4.1 `ReadWriteMany`
  PVC on every artifacts replica) so one pod can range-read a log another pod
  appended; keep `AppendLog`'s close-per-append (close-to-open consistency).

**Acceptance criteria.**
- [x] A worker connected to API pod B receives a job dispatched while it is
      registered on pod B, even though the scheduler wrote it via the shared
      event log (no scheduler→specific-pod dependency).
- [x] A job whose dispatch missed its worker (worker down at dispatch time) is
      picked up by the worker on its next poll — no job is stranded `pending`
      forever.
- [x] A UI client on pod A sees `job_status` / `run_status` / `worker` events
      produced by activity on pod B (via the shared event log).
- [x] Two scheduler replicas run concurrently: only the leader runs the
      background loops; no duplicate dispatches or duplicate UI events.
- [x] A worker's job log is visible (near-live) in a UI connected to a
      *different* API pod than the one the worker streams to.
- [x] Killing an API pod does not lose in-flight job delivery: the worker
      reconnects to another pod (2 s backoff) and its poll keeps it in step.
- [ ] The `events` table does not bloat: old partitions are dropped on a
      retention schedule. *(Deferred: the table is a plain append-only log
      today; partitioning + retention DDL is a deployment concern to add when
      the log's growth warrants it.)*

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
8. **F-23** — high-availability control plane (shared event log, hybrid
   push/pull dispatch, scheduler leader election, cross-pod live logs). See
   [`HighAvailability.md`](HighAvailability.md).

---

## Implementation checklist

Tick each feature off as it lands.

- [x] F-01 Job execution spec
- [x] F-02 Job log streaming
- [x] F-03 Job timeouts
- [x] F-04 Retry & re-run
- [x] F-05 Cancellation propagation
- [x] F-06 `skipped` job status
- [x] F-07 Pipeline runs
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
- [x] F-23 High-availability control plane
