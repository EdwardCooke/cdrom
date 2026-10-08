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
| Pipeline model | `Pipeline` (name, description, job definitions with `key`/`needs`) — CRUD via DB service; a pipeline's jobs form a DAG (F-08) |
| Job model | `Job` (status, `target_group`, `key`, `depends_on`, start/finish timestamps) |
| Job lifecycle | `pending → running → succeeded / failed / cancelled / timed_out / skipped` |
| Dispatch | `target_group` set → fan out to live workers; empty → ephemeral K8s agent |
| DAG / dependencies | Jobs in a pipeline declare `needs` (other jobs' `key`s), forming a DAG (F-08); the graph is validated at save time (unique keys, known needs, no cycle), and a run's job instances are dispatched in parallel as their dependencies succeed (a failed dependency skips its dependents, F-06) |
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

The biggest remaining gap: a pipeline's definition is not yet versioned
(F-11). Runs (F-07), the job DAG (F-08), and automatic triggers — cron,
webhook, and event (F-09) — are in place; everything below builds toward
governing and automating them.

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
- [x] The built-in `token_exchange` step handler requests a new job token for
      a different audience (via the target's `TokenExchange`, which calls the
      API's `ExchangeJobToken` RPC) and writes the exchanged token to the step's
      output directory, so later steps and jobs can read it through the
      condition context.
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
  itself under `executor.DefaultType` at package init, and the built-in
  `token_exchange` handler registers itself under `token_exchange` (see the
  Token exchange handler below). A target or plugin adds new types with
  `executor.RegisterStepType(name, handler)` (e.g. `ansible`, `terraform`,
  `argo`). An unregistered type fails the job with a clear error.
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
- **Token exchange handler:** the built-in `token_exchange` step handler
  (`internal/stephandlers/token_exchange.go`) lets a running job request a new
  job token for a different audience (e.g. an outside resource the job needs to
  call) and hand the exchanged token to later steps and jobs. It reads
  `audience` (string, required), `expires_in` (string, a Go duration; empty
  means the API's default exchanged-token lifetime), and `output` (string, the
  step-output name the token is written under, default `token`) from the step's
  `params`. It obtains the exchanged token from the execution target through the
  executor's `TokenExchange` (set via `executor.ContextWithTokenExchange`): the
  worker and agent implement it by calling the API's `ExchangeJobToken` RPC,
  presenting the job's own token. The executor gives every step a per-step
  output directory (the step's env carries `executor.StepOutputDirEnv`), so the
  handler always writes the token into it under the output name; the executor
  reads every file in that directory back as a step output (file name = output
  name), so downstream steps and jobs read it through the condition context
  (`.steps`/`.jobs` `.Outputs`). A step with no `audience`, an invalid
  `expires_in`, or a target with no
  `TokenExchange` fails the job.
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
  that periodically lists running (and approval-waiting) jobs and reaps any
  that have exceeded their effective timeout. A job's **effective timeout** is
  its job-level timeout when set, otherwise the longest per-step timeout; a job
  with neither is never reaped. The deadline is measured from the job's
  `started_at`. Reaping is **conditional** (`Database.ReapJob`): it marks the
  job `timed_out` only if it is still `pending`, `running`, or
  `awaiting_approval` (an atomic `WHERE status IN (...)` update),
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
  (marks the job `cancelled` only if it is still `pending`, `running`, or
  `awaiting_approval`), so cancelling an already-finished job is a no-op. The
  target is then signalled
  best-effort: if it cannot be reached (no live worker, or the API is down)
  the job is still marked `cancelled` in the database, and the target's next
  status report (or the scheduler's watchdog) reconciles it.
- **A late target report cannot clobber a terminal status.** `Database.UpdateJob`
  is now conditional: a status report from a target is applied only if the job
  is still `pending`, `running`, or `awaiting_approval`. Once a job is
  `succeeded`, `failed`, `cancelled`, or `timed_out`, a late report (e.g. a
  target that finished just as it was cancelled) is ignored, so the terminal
  status is authoritative.
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
- [x] A step that writes files into its per-step output directory (exposed via
      `CDROM_STEP_OUTPUT_DIR`) has them read back by the executor (trimmed) —
      each file's name is the output's name, and a step needs to declare
      nothing.
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
- **`depends_on` was a minimal stand-in for F-08's `needs`/DAG; F-08 is now
  built on it.** F-06 landed ahead of F-08 in the roadmap, so `Job` gained a
  minimal `depends_on` field (`[]uint`): a flat list of raw job ids, checked
  by a polling background resolver
  (`internal/services/scheduler/dependencies.go`). F-08 (now implemented)
  adds `needs` (dependencies expressed as the stable `key`s of other jobs in
  the same pipeline) and cycle validation at save time. The database service
  resolves a job's `needs` (keys) to `depends_on` (ids) when the pipeline is
  saved, and `Database.CreateRun` remaps a run's instances' `depends_on`
  (definition ids) onto the run's own instance ids, so the same resolver
  dispatches ready jobs in parallel and skips dependents of a failed job. The
  resolver is now the DAG resolver (see F-08 and AGENTS.md's "Skip
  propagation & step conditions (F-06)" section).
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
- **Step outputs use a per-step temp directory.** Every step is given a fresh
  per-step directory, exposed to it via the `CDROM_STEP_OUTPUT_DIR` env var
  (created even for a step that produces no output, so a step handler that
  always produces output can write to it); the step's command writes one file
  per output it produces into it, and the executor reads **every file** in the
  directory back (trimmed) after the step runs — each file's name is the
  output's name, so a step produces an output simply by writing a file and
  needs to declare nothing. A file that is never written simply does not
  appear as an output. The job's `outputs` are the union of its steps' outputs
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
- [x] A job with `needs: [A]` does not start until A succeeds.
- [x] Independent jobs run in parallel.
- [x] A cycle in the graph is rejected when the pipeline is saved.
- [x] Failure of A skips its dependents (F-06).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **`needs` are keys, not ids.** A job in a pipeline has a stable,
  pipeline-scoped `key` (a short name, e.g. `build`, `test`), unique within
  the pipeline. A job's `needs` reference other jobs by their `key`, not by
  their id, so a pipeline's DAG is stable across runs and survives re-creation
  of a run's job instances. A job that is not part of a pipeline (a standalone
  job) has no `key` and may still use the raw-id `depends_on` (F-06).
- **The DAG is validated at save time.** The database service validates a
  pipeline's job graph whenever it is created, updated, or a job is added:
  keys are unique, every `needs` entry references a key that is present, and
  the keyed subgraph has no cycle (Kahn's algorithm). A cycle, an unknown key,
  or a duplicate key rejects the whole save, so a pipeline is never persisted
  in an unrunnable state.
- **`needs` are resolved to `depends_on` at save time.** `needs` is the
  authoring form (keys); the backend stores dependencies as `depends_on`
  (ids). When a pipeline is created, updated, or a job is added, the database
  service resolves each job's `needs` (keys) to the ids of the jobs they
  reference and stores the result in `depends_on`. `Database.CreateRun` then
  creates one job instance per definition and remaps each instance's
  `depends_on` (definition ids) to the ids of the run's own instances. The
  existing F-06 dependency resolver (which resolves `depends_on` by id)
  dispatches ready instances in parallel and skips dependents of a failed job
  — so F-08 reuses the F-06 resolver rather than introducing a parallel DAG
  engine. Two runs of the same pipeline remain fully independent (a run's
  instances depend on each other, not on the other run's instances).
- **Pipeline jobs are first-class definitions.** A pipeline carries its job
  definitions (key, name, target group, spec, needs) — created via
  `POST /api/pipelines` (with a `jobs` list) or `PUT /api/pipelines/{id}`, or
  by submitting a job with a `pipeline_id` (`POST /api/jobs`), which persists
  it as a definition on the pipeline (via the database service) rather than
  dispatching it. A run of the pipeline instantiates those definitions.

### F-09 · Triggers

**What.** Ways to start a pipeline run other than a manual click:
- **Schedule (cron)** — run on a cron expression.
- **Webhook** — an external POST (e.g. from a git host) starts a run, passing
  payload (commit, branch) as run parameters. A webhook trigger can also
  authenticate the caller by **OIDC claims**: the caller presents a Bearer
  token issued by a configured issuer, and the trigger matches only when the
  token's claims satisfy the trigger's required claims (with `*` wildcards and
  dot-addressed nested claims). The matched token's claims are recorded on the
  run and stamped onto the run's job tokens as `upstream_*` claims.
- **Event** — start when another pipeline/run reaches a state (chaining).

**Why.** CD is driven by events (a push, a timer, an upstream deploy), not
just by humans.

**Scope.**
- `internal/models` / proto — trigger definitions on the pipeline (cron spec,
  optional webhook secret, optional webhook OIDC issuer + required claims,
  event rule); trigger source + name + source run on the run; the run's
  `upstream_claims` (the full claims of a webhook caller's token, as a JSON
  object — a claim that is itself an object or a list keeps its structure);
  `trigger_name` / `trigger_type` / `upstream_claims` on the job;
  `pipeline_name` on the run (so the event loop matches a finished run
  against a watched pipeline without fetching every pipeline).
- `internal/services/database` — trigger validation/persistence; the atomic
  `TriggerRun` claim (dedup); `ListPipelines` filtering by trigger type.
- `internal/services/scheduler` — a cron trigger loop; an event trigger loop;
  the `TriggerRun` RPC that drives a claimed run's instances.
- `internal/api/server.go` — `POST /api/pipelines/{id}/webhook` (auth via the
  trigger's shared secret and/or its OIDC claims; open when neither is set).
- `internal/api/webhookoidc.go` — the webhook token verifier (per-issuer OIDC
  discovery + verification), claim flattening (nested claims become
  dot-addressed keys, used only for matching a trigger's `oidc_claims`), and
  the wildcard-aware claim matcher.
- `internal/api/jobsauth.go` — mints/exchanges job tokens stamped with the
  job's `trigger_name` / `trigger_type` and `upstream_*` claims.
- `internal/idp` — the `MintJobToken` gRPC RPC stamps `trigger_name`,
  `trigger_type`, and `upstream_*` claims onto the token it mints.

**Acceptance criteria.**
- [x] A cron-triggered pipeline runs at the scheduled times.
- [x] A webhook POST with a valid secret starts a run and records the payload
      as run parameters.
- [x] A webhook trigger with an empty secret is open: any POST starts a run.
- [x] A webhook trigger with an `oidc_issuer` requires a Bearer token from
      that issuer whose claims satisfy the trigger's `oidc_claims` (a `*`
      value is a wildcard; nested claims are matched dot-addressed); a token
      that fails verification or mismatches a claim is rejected (401).
- [x] The matched webhook token's claims are recorded on the run
      (`upstream_claims`, as a JSON object that preserves a claim's structure
      — an object or a list stays an object/list) and stamped onto the run's
      job tokens as `upstream_*` claims.
- [x] Job tokens carry the run's `trigger_name` and `trigger_type` claims.
- [x] An event trigger starts a run when the watched condition is met.
- [x] Trigger source is recorded on the run (manual / cron / webhook / event).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **Triggers live on the pipeline.** A `Pipeline` carries a `Triggers` list
  (a JSON column via GORM's `serializer:json`), each a `Trigger` with a
  `name` (unique within the pipeline), a `type` (`cron` / `webhook` /
  `event`), and the fields its type needs: a cron trigger a standard
  five-field `cron` expression; a webhook trigger an optional `secret` and an
  optional `oidc_issuer` + `oidc_claims` (see Webhook OIDC below); an event
  trigger an `event_pipeline` (the name of the pipeline to watch) and an
  `event_status` (the run status that fires it). Any trigger may carry
  static `params` merged into the run it starts. Triggers are validated at
  save time (unique non-empty names; a cron expression that parses; an event
  pipeline + status; a webhook's `oidc_claims` entries have non-empty claim
  names), so a pipeline is never persisted with an unrunnable trigger. A
  webhook trigger's secret is optional: when set it is enforced on the
  webhook endpoint, when empty the trigger is open (any POST starts a run).
- **Webhook OIDC claims.** A webhook trigger may set an `oidc_issuer` (the
  issuer URL of the token a caller must present) and `oidc_claims` (a map of
  claim name → required value). On the webhook endpoint the API verifies the
  caller's Bearer token against the issuer (OIDC discovery + JWKS, cached per
  issuer) and matches the token's claims against the trigger's required
  claims: a required value of `*` is a wildcard (it matches any value, or the
  claim being absent), any other value must be present and equal. Nested
  claims (some providers, e.g. GitLab, nest their claim values) are flattened
  to dot-addressed keys (`a.b.c`) **for matching only**, so a trigger can
  require a nested claim by its dot path. When both a `secret` and an
  `oidc_issuer` are set, **both** must be satisfied (AND); a trigger with
  neither is open. The matched token's claims are recorded on the run as
  `upstream_claims` — as a JSON object that preserves each claim's structure
  (a claim that is itself an object or a list, e.g. GitLab's `user_identities`
  or `job_config`, is kept as that object/list, not flattened to a string) —
  and denormalized onto each of the run's job instances, so downstream
  applications can make allow/deny decisions from the identity that triggered
  the run.
- **`TriggerRun` is the single, atomic claim path.** All three trigger kinds
  funnel through `Database.TriggerRun` (and the scheduler's `TriggerRun` RPC
  that wraps it): it creates a `PipelineRun` (started by the named trigger)
  plus one job instance per job definition — reusing `CreateRun`'s
  `createRunAndInstances` — but only if the trigger has not already started a
  run for the same fire window. The check and the create run in one
  transaction that locks the pipeline row, so a racing replica is serialized:
  the first claim creates the run, the rest see it and no-op. The run records
  its `trigger` source (`cron` / `webhook` / `event`), its `trigger_name`,
  and, for an event trigger, its `source_run_id`. For a webhook trigger it
  also records the caller's token claims as the run's `upstream_claims` (a
  JSON object that preserves each claim's structure), which are denormalized
  onto each of the run's job instances (so a job token can carry them, see
  Job-token trigger claims below).
- **Dedup keeps a trigger from firing twice.** A cron trigger passes a dedup
  window (shorter than the minimum cron period): a run the same trigger
  started within the window suppresses a duplicate, so a racing replica cannot
  fire the same scheduled time twice while the next legitimate fire (a full
  period later) is not suppressed. An event trigger records the source run's
  id: the same source run can never start the same downstream run twice.
- **Cron loop.** A leader-gated background loop
  (`internal/services/scheduler/cron.go`, started from `cmd/scheduler`) ticks
  once a second, keeps the parsed schedule and next fire time per
  (pipeline, trigger), and — when a trigger is due — calls `TriggerRun` to
  claim the run and advances to the next fire time. On a restart it recomputes
  the next fire as `schedule.Next(now)`, so a time that already passed is not
  re-fired; the database's dedup is the authoritative guard against the
  racing-replica case. It lists only the pipelines that carry a cron trigger
  (`ListPipelines` with `trigger_type` set to `cron`), so a pipeline with no
  cron trigger is never fetched or considered.
- **Event loop.** A leader-gated background loop
  (`internal/services/scheduler/eventtrigger.go`, started from
  `cmd/scheduler`) ticks every few seconds, lists only the pipelines that
  carry an event trigger (`ListPipelines` with `trigger_type` set to `event`)
  to build its rules, and — for each recently-finished run (a lookback
  window) whose pipeline name and status match an event trigger — calls
  `TriggerRun` to start a run of the triggered pipeline, recording the source
  run's id and the source pipeline's name in the run's params. The run's
  `pipeline_name` (preloaded with the run) is what the loop matches against,
  so a watched pipeline that carries no trigger of its own still matches. An
  in-memory set of already-fired source run ids is an optimization; the
  database's dedup (by source run id) makes a re-fire after a restart a
  no-op.
- **Webhook endpoint.** `POST /api/pipelines/{id}/webhook` (API) looks up the
  pipeline's webhook triggers and, for each, checks every credential the
  trigger sets: the presented secret (constant-time, via the
  `X-Cdrom-Webhook-Secret` header) and, when the trigger has an
  `oidc_issuer`, the caller's Bearer token (verified against the issuer and
  matched against the trigger's `oidc_claims`, see Webhook OIDC claims above).
  A trigger matches when **all** of its set credentials are satisfied; a
  trigger with neither a secret nor an `oidc_issuer` is open and matches any
  request. On a match the handler decodes the request's JSON body (a flat
  object of string values; an empty body is allowed) as run parameters, merged
  over the trigger's static params (the body wins on a collision), and calls
  the scheduler's `TriggerRun` (which atomically dedups it), passing the
  matched token's claims (as a JSON object preserving each claim's structure)
  as the run's `upstream_claims`. A request that matches no trigger is 401; a
  pipeline with no webhook trigger is 404.
- **Job-token trigger claims.** The job tokens the API mints for a run's jobs
  carry the run's trigger context so a downstream application (a step handler,
  an external service the job calls) can make allow/deny decisions: a
  `trigger_name` claim (the trigger's name) and a `trigger_type` claim
  (`cron` / `webhook` / `event`), plus — for a webhook-triggered run — the
  run's `upstream_claims` stamped as `upstream_<claim>` (e.g.
  `upstream_org`, `upstream_user`); a claim that is itself an object or a
  list (e.g. GitLab's `user_identities` or `job_config`) is stamped as that
  object/list, not flattened to a string. The API reads these off the job
  (they are denormalized onto the job instance at run creation) and passes
  them to the IdP's `MintJobToken` gRPC RPC, which stamps them onto the RS256
  token it signs. `ExchangeJobToken` re-stamps the same trigger context onto the
  exchanged token.
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

### F-10 · Parameters & variables

**What.** Pipelines declare **parameters** (a name, an optional default, and an
optional description); a run supplies concrete values for them (at trigger
time, via a webhook, or from a cron/event trigger). Those values are
interpolated into a job's spec (env, command, workdir) using Go templates
(`text/template`), the same mechanism a step's `condition` uses: `{{ .name }}`
or `{{ .params.name }}` renders a parameter's value, and `{{ .run.<field> }}`
exposes the run's identity (id, pipeline id, trigger, trigger name). A
reference to a parameter that has no value (no supplied value and no default)
fails the job.

**Why.** The same pipeline deploys to different places / versions by changing
inputs, not by editing the definition.

**Scope.**
- `internal/models` / proto — a `Parameter` definition on the pipeline; a run
  stores its concrete parameter values, denormalized onto each job instance.
- `internal/executor` — a shared interpolation helper that renders the job
  spec (workdir, env, params) from the run's parameter values before running.
- `internal/api/server.go` — accept parameters on pipeline create/update and
  on run creation / webhook.

**Acceptance criteria.**
- [x] A parameter with a default is used when not supplied; an explicit value
      overrides it.
- [x] Interpolation substitutes run/parameter values into env and commands.
- [x] Unknown/undefined references fail the run clearly (no silent empty
      strings) — or a defined fallback policy is documented.
- [x] Parameter values are visible on the run (non-secret ones).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **Parameters are declared on the pipeline.** A `Parameter` (name, optional
  `default`, optional `description`) is a first-class message on `Pipeline`
  (and on `CreatePipelineRequest` / `UpdatePipelineRequest`), stored as a JSON
  column on the `Pipeline` row. Names must be non-empty and unique within the
  pipeline; the database service validates this at create and update time.
- **A run supplies concrete values; defaults fill the gaps.** A run's
  concrete parameter values are stored on the run (`PipelineRun.Params`, a
  `map<string,string>`). At run creation the database service merges them
  against the pipeline's declared parameters (`runParamsFor`): a supplied
  value wins, else a non-empty default is used, else the parameter is omitted
  (left unset). The resulting map is **denormalized onto each of the run's
  job instances** as `run_params` (a JSON column on the `Job` row), so the
  execution target can interpolate it without a round-trip to the run row —
  the same denormalization pattern the trigger context uses.
- **The executor interpolates the spec with Go templates.** Before running a
  job, the execution target (worker or agent) sets the run's parameter values
  (plus the run's identity: id, pipeline id, trigger, trigger name) on the
  executor's context via `executor.ContextWithRunInfo`. The executor then
  renders each step's `workdir`, each `env` value, and each `params` value
  (string and string-list) as a `text/template` against a data context that
  exposes the top-level parameter values (so `{{ .name }}` works) plus
  `params` (the whole map) and `run` (the run identity). The same `params` /
  `run` keys are also available to a step's `condition` template.
- **Undefined references fail the job.** The interpolation template is parsed
  with `Option("missingkey=error")`, so a reference to a parameter that has no
  value (no supplied value and no default) is a spec error that fails the job
  — there are no silent empty strings. This is the documented fallback policy.
- **New fields / RPCs.** `Parameter` message (db); `params` on `Pipeline`,
  `CreatePipelineRequest`, and `UpdatePipelineRequest` (db); `run_params` on
  `Job` (db, scheduler, api). The API's pipeline create/update requests accept
  a `params` list, and the run-creation path (manual trigger, cron, webhook,
  event) already carries the run's `params`.

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
- [x] Editing a pipeline bumps its version; existing runs keep their original
      version.
- [x] A run records which version it executed.
- [x] A run can be re-executed against its original version.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **A version is an immutable snapshot, not a diff.** Each change to a
  pipeline (create or update) records a new `PipelineVersion` row: a full
  snapshot of the definition at that version — name, description, failure
  mode, the jobs (key, name, target group, needs, and the full `JobSpec`),
  and the parameters. The `Pipeline` row carries a `version` counter that is
  bumped on every update; the snapshots are what make a run reproducible.
  Deleting a pipeline cascades to its snapshots.
- **A run binds to the version that was active when it started.** `CreateRun`
  records the pipeline's current `version` on the run (`PipelineRun.PipelineVersion`)
  and creates the run's job instances from the pipeline's *current* job
  definitions. A run that does not request a specific version therefore always
  executes the definition that was live at the moment it was created.
- **A run can be created against a specific (older) version.** `CreateRun`
  accepts an optional `pipeline_version`. When set, the run's job instances
  are created from that version's snapshot (its jobs, needs, failure mode, and
  parameters) rather than the current definitions, and the run records that
  version. This is what makes a re-run of an old run reproduce the old
  definition. A trigger-fired run (cron / webhook / event) always runs the
  current version. A requested version that does not exist is rejected
  (`InvalidArgument`).
- **New fields / RPCs.** `PipelineVersion` message + `ListPipelineVersions` /
  `GetPipelineVersion` RPCs (db); `version` on `Pipeline` (db);
  `pipeline_version` on `PipelineRun` and `CreateRunRequest` (db, scheduler);
  `pipeline_version` on the API's run-creation request and the `wsRun`
  snapshot, plus `GET /api/pipelines/{id}/versions` and
  `GET /api/pipelines/{id}/versions/{version}`.

---

## Phase 3 — Safety & governance

Control who can do what, protect secrets, and keep an accountable record.

### F-12 · Secrets management

**What.** Pipelines declare **named secrets** (a name and a value), like their
parameters. The API encrypts each value with a configured key when the pipeline
is created or updated, and decrypts it when it hands a job to an execution
target. A job's spec references a secret with a Go template, `{{
.secrets.name }}` (the same mechanism a step's `condition` and F-10 parameter
interpolation use). The plaintext is never persisted, never returned to the UI,
and is redacted from job logs before they are stored or fanned out.

**Why.** Every real pipeline needs credentials; they must not live in the
pipeline definition (in plaintext) or in logs.

**Scope.**
- `internal/config` — a `secrets` section (store `kind` + a base64 AES-256
  `key`); when no key is set the built-in AES store falls back to an all-zero
  key (test / local-dev convenience).
- `internal/secrets` — a `Store` interface (encrypt/decrypt) with a built-in
  AES-256-GCM store; `vault` / `openbao` are reserved first-class kinds (not
  yet implemented). A `NonceSource` supplies a unique nonce per encryption.
- `internal/models` — a `Secret` (name, encrypted value) on `Pipeline` and
  `PipelineVersion`, denormalized onto each run's `Job` instances; a
  `SecretNonce` counter row.
- `proto/cdrom/db/v1/db.proto` — a `Secret` message; `secrets` on `Pipeline`,
  `CreatePipelineRequest`, `UpdatePipelineRequest`, `PipelineVersion`, and
  `Job`; a `NextSecretNonce` RPC. The database service stores the ciphertext
  opaquely and never sees the key.
- `proto/cdrom/api/v1/api.proto` — `secrets` (a name → **plaintext** map) on
  the API's `Job`, populated by the API at dispatch.
- `internal/api` — encrypt on pipeline create/update; decrypt at dispatch
  (`GetJob`, `StartJobExecution`, the dispatch push); redact secret plaintext
  from streamed job logs before persistence/fan-out. An authenticated user can
  also encrypt a value directly via `POST /api/secrets/encrypt` (body
  `{"value": "…"}` → `{"ciphertext": "…"}`), which returns the ciphertext a
  pipeline's secret declaration stores; the plaintext is never persisted or
  returned.
- `internal/executor` / `internal/target` — carry the decrypted secrets into
  the run context so the spec can interpolate `{{ .secrets.name }}`.

**Acceptance criteria.**
- [x] A secret can be created on a pipeline and referenced by a job's spec;
      the value is available to the step (via `{{ .secrets.name }}`).
- [x] No API/UI path returns the plaintext secret value (the UI sees only
      names; the API decrypts only at dispatch, for the target).
- [x] Secret values never appear in job logs (the API redacts the plaintext
      from every streamed chunk before it is persisted or fanned out).
- [x] Secrets are encrypted at rest (AES-256-GCM with a configured key; the
      nonce is a shared, DB-backed counter so no (key, nonce) pair is ever
      reused across API replicas).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **A single key in configuration; the API is the only encryption authority.**
  The built-in store is AES-256-GCM with a 32-byte key from the `secrets`
  config section (`CDROM_SECRETS_KEY`, base64). When no key is configured the
  built-in store falls back to an all-zero key, so secrets can be exercised in
  test / local-dev scenarios without a real key (the zero key provides no real
  security — set a real key in production). The database service stores the
  ciphertext **opaquely** — it never imports the secrets package or holds the
  key, so no single service or config can decrypt a secret on its own.
- **The nonce is a shared counter in the database.** AES-GCM requires a unique
  (key, nonce) pair per encryption. Because every API replica shares one key,
  they must share one nonce sequence: the built-in store draws each nonce from
  a `SecretNonce` counter row via the database service's `NextSecretNonce` RPC
  (an atomic `UPDATE … RETURNING value + 1`), so no two encryptions — even
  across replicas — ever reuse a nonce. The nonce is embedded in the stored
  ciphertext (`base64(nonce ‖ gcm-ciphertext)`), so decryption needs only the
  key.
- **Secrets are declared on the pipeline, like parameters.** A `Secret` (name,
  encrypted value) is a first-class message on `Pipeline` (and on
  `CreatePipelineRequest` / `UpdatePipelineRequest` / `PipelineVersion`),
  stored as a JSON column. Names must be non-empty and unique within the
  pipeline; the database service validates this at create and update time. The
  API encrypts the plaintext the UI sends before it reaches the database.
- **The ciphertext is denormalized onto each run's job instances.** At run
  creation the database service copies the pipeline's (or the version
  snapshot's) secrets onto each `Job` instance, mirroring the F-10
  `run_params` denormalization. A run against a specific version carries that
  version's secrets, so a re-run of an old run uses the old secrets.
- **The API decrypts at dispatch and hands plaintext to the target.** When the
  API hands a job to an execution target (`GetJob` for agents,
  `StartJobExecution` for workers, and the dispatch push), it decrypts the
  job's stored ciphertexts into a name → plaintext map on the API's `Job`
  (`secrets`), which the target puts on the executor's run context. The
  executor exposes it as `{{ .secrets.name }}` (never top-level, so a secret
  can never collide with a same-named parameter). A ciphertext that fails to
  decrypt (the key changed since it was written) is logged and dropped rather
  than failing the dispatch.
- **The API redacts secrets from job logs.** The API is the only component
  that holds the key, so it is the only place that can both know the plaintext
  (to look for it) and remove it. Before a streamed log chunk is persisted to
  the artifacts store or fanned out to the UI, the API replaces every
  occurrence of a secret's plaintext with a redaction marker, so a secret that
  a step echoes to stdout is never stored or shown.
- **An authenticated user can encrypt a value directly.** `POST
  /api/secrets/encrypt` (body `{"value": "…"}`) runs the value through the
  API's secret store and returns `{"ciphertext": "…"}` — the exact ciphertext a
  pipeline's secret declaration stores. It is a POST (it performs an action and
  draws a nonce), sits behind the same OIDC Bearer-token middleware as every
  other `/api/*` route. It never persists the value and never returns the
  plaintext, so it is safe to call from the UI or a script.
- **New fields / RPCs.** `Secret` message + `NextSecretNonce` RPC (db);
  `secrets` on `Pipeline`, `CreatePipelineRequest`, `UpdatePipelineRequest`,
  `PipelineVersion`, and `Job` (db); `secrets` (name → plaintext map) on the
  API's `Job` (api); `POST /api/secrets/encrypt` (HTTP). The `secrets` config
  section and the `internal/secrets` package are new.

### F-13 · Approval gates

**What.** A job can require a **manual approval** before it proceeds past a
point in its steps. The job pauses in an `awaiting_approval` state at the
approval step until an authorized user approves (or rejects / times out).

**Why.** Production deploys and other high-blast-radius steps are gated behind
a human sign-off.

**Scope.**
- `internal/models` / proto — an `awaiting_approval` job status; an approval
  record on the job (decision, actor, reason, requested/decided timestamps).
- `internal/executor` + `internal/stephandlers` — a built-in `approval` step
  handler that pauses the job at the gate.
- `internal/approval` + `internal/target` — the API-backed approval gate the
  execution target uses to report `awaiting_approval` and poll for the decision.
- `internal/api` — `POST /api/jobs/{id}/approve` / `.../reject` (HTTP) and the
  `CheckApproval` gRPC RPC the target polls.
- `internal/services/scheduler` + `internal/services/database` — persist the
  decision (`ResolveApproval`) and classify `awaiting_approval` in the
  run-status / job-status / watchdog loops.
- UI — an approval prompt (reads `awaiting_approval` + `approval_message` from
  `GET /api/jobs/{id}`, calls approve/reject).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- The gate is a **step** (`type: "approval"`), not a job-level flag: a job
  pauses at the approval step and its later steps run only after approval. The
  step's `message` param is a Go template rendered against the step's condition
  context (secrets, prior steps' outputs, run parameters, upstream jobs, job
  identity) — the same interpolation a step's condition (F-06) and the run's
  spec fields (F-10) use — so a message can reference a secret, a prior step's
  output, or the job's identity.
- The execution target reports the job `awaiting_approval` (with the rendered
  message) via the API's `ReportJobStatus`, then polls the API's `CheckApproval`
  until the gate is resolved. An **approval** succeeds the step (the job
  continues); a **rejection** fails the job; a **timeout** (the step's or the
  job's timeout) reports `timed_out`; a **cancellation** reports `cancelled`.
- `awaiting_approval` is a non-terminal, in-flight status: the run-status loop
  keeps the run `running` while a job is at the gate, and the watchdog reaps a
  gate whose target goes silent. Resolving the gate moves the job out of the
  state (approved → `running`, rejected → `failed`).
- The approval record (decision, actor, reason, timestamps) is persisted on the
  job by the API's `ResolveApproval` (via the scheduler → Database service),
  conditionally (only while the job is still `awaiting_approval`), so a late
  decision is a no-op.

**Acceptance criteria.**
- [x] A gated job does not start (past the approval step) until approved.
- [x] Approval/rejection is recorded (actor + timestamp + optional reason).
- [x] An approval timeout (the step's or job's timeout) fails the job
      (`timed_out`).
- [x] Only authorized actors can approve (pairs with F-14; the actor is the
      authenticated user, recorded on the decision). F-27 extends "authorized"
      to named approval groups, so a pipeline can reference a group instead of
      individual users.

### F-14 · Roles & permissions (RBAC)

**What.** A full role-based access control layer: a catalog of fine-grained
**permissions** (every operation in the system is a permission), **roles**
(built-in plus user-defined **custom roles**, each a named set of permissions),
and **bindings** that attach roles to principals (users, service accounts) —
optionally scoped to individual pipelines. Authorization is deny-by-default: a
principal can do exactly what its bound roles permit, nothing else.

Roles reach a principal through several independent binding mechanisms, which
compose (effective permissions are the union of all of them):

- **User-directory roles** (F-24) — the built-in IdP stamps the user's
  registered roles onto the token's `roles` claim; the API reads them from the
  verified token. No per-request database lookup.
- **JWT claim mapping** — for tokens from *any* IdP (including external
  enterprise IdPs), a configurable claim (e.g. `groups`, `roles`, `scope`) is
  read and its values mapped to role names, so group membership in the
  corporate IdP drives Cdrom roles without Cdrom's user directory knowing the
  user at all.
- **Direct bindings** — role bindings stored in the Database service and
  managed via the API; the mechanism for scoped grants (e.g. "operator on
  pipeline X") and the only mechanism F-26 service accounts use.
- **API keys** (F-25) — a key acts as its owner with the owner's permissions
  further limited to the key's pipeline scope.

**Why.** A shared CD platform must distinguish who may do what: a viewer
should not trigger a production deploy, a pipeline owner should manage their
pipeline without being a platform admin, and an enterprise should drive roles
from its existing IdP groups instead of re-managing membership in Cdrom.

**The permission model.**

- **Permissions** are fine-grained, namespaced names
  (`<area>.can-<action>`). The catalog is extensible — every feature
  registers its permissions here (F-26 adds `service-accounts.*`, F-19 adds
  `artifacts.can-promote`, …):

  | Permission | Allows |
  |------------|--------|
  | `pipelines.can-view` | List/read pipelines, versions, runs, jobs, logs, artifacts |
  | `pipelines.can-create` | Create pipelines |
  | `pipelines.can-edit` | Edit a pipeline's definition (jobs, steps, triggers, params) |
  | `pipelines.can-delete` | Delete a pipeline |
  | `runs.can-trigger` | Trigger a run (manually or via webhook) |
  | `runs.can-cancel` | Cancel a running run/job |
  | `jobs.can-approve` | Approve a gated job (F-13) |
  | `jobs.can-reject` | Reject a gated job (F-13) |
  | `secrets.can-view` | See which secrets a pipeline declares (names only, never values) |
  | `secrets.can-manage` | Create/update/delete secret values |
  | `roles.can-manage` | Create/edit/delete custom roles |
  | `roles.can-assign` | Grant/remove role bindings (subject to the delegation rules) |
  | `users.can-manage` | Manage users and their roles (F-24) |
  | `api-keys.can-manage` | Create/edit/rotate API keys (F-25) |
  | `audit.can-view` | Query the audit log (F-15) |
  | `workers.can-view` | View worker/agent status and the pending queue (F-16) |

- **Roles** are named sets of permissions. **Built-in roles** ship with the
  platform and cannot be deleted or have their permission sets edited:
  - `admin` — every permission (the platform administrator).
  - `operator` — view + trigger + cancel + approve/reject (runs a platform).
  - `viewer` — read-only access to everything.
  - `user` — the default role of a registered user (F-24): `viewer` plus
    self-service (manage own API keys, own profile).
  **Custom roles** are created by a principal with `roles.can-manage`: a name,
  a description, and an explicit set of permissions (e.g. `pipeline-owner` =
  view + edit + trigger + cancel + approve; `secret-keeper` = view + manage
  secrets). A role may also **include** other roles (composition), so
  `senior-operator` can be `operator` + `secrets.can-manage` without
  duplicating the list.

- **Bindings** attach a role to a principal, optionally **scoped to a
  pipeline**: an unscoped binding grants the role's permissions
  platform-wide; a pipeline-scoped binding grants them only for that pipeline
  (a scoped `runs.can-trigger` triggers runs of pipeline X but not pipeline
  Y). Scoping applies to resource-scoped permissions (pipelines, runs, jobs,
  secrets); platform-wide permissions (role/user management, audit) are only
  granted by unscoped bindings.

**Delegation rules.** Granting a role is itself a permission
(`roles.can-assign`), and it is bounded: a caller may bind a role to a
principal only if the caller already holds that role (unscoped, or with a
scope at least as wide as the one being granted). An operator cannot promote
someone to admin, and a pipeline-scoped operator cannot grant platform-wide
operator. Removing a binding requires `roles.can-assign` on the role being
removed. `admin` is grantable only by an `admin`.

**Nice-to-haves** (build after the core works):
- **Role templates / config-as-code** — export/import role definitions as YAML
  (pairs with F-21) so role sets are reviewable in source control.
- **Per-pipeline owner shortcut** — creating a pipeline optionally binds the
  creator a scoped `pipeline-owner` custom role on it.
- **Permission introspection** — `GET /api/me/permissions` returns the
  caller's effective permissions (per scope) so the UI hides/disables actions
  the caller can't use instead of surfacing 403s.
- **Role health view** — a UI screen listing roles, who holds each, and which
  permissions each grants; flags roles that grant admin-level power.
- **Just-in-time elevation** — a time-boxed elevation request ("grant me
  operator for 1 hour", approved by an admin, auto-expiring binding) for
  break-glass access.

**Scope.**
- `internal/models` — `Role` (name, description, built-in flag, permissions
  JSON, included-role names) and `RoleBinding` (principal kind
  `user`/`service-account`, principal id, role name, optional pipeline scope)
  registered in `All()`.
- `proto/cdrom/db/v1/db.proto` + `internal/services/database` — role and
  binding CRUD RPCs (create/get/list/update/delete roles; add/list/remove
  bindings).
- `internal/authz` (new package) — the authorization engine:
  `Check(ctx, principal, permission, resource) (bool, error)` and
  `PermissionsFor(ctx, principal)`. Resolves a principal's roles from (a) the
  token's `roles` claim, (b) the configured claim mapping, and (c) stored
  bindings; expands role composition; filters by resource scope;
  deny-by-default. Results are cached and invalidated on role/binding
  mutation (via the shared event log, F-23, so every API replica sees the
  change).
- `internal/auth` — `User` already carries `Roles` from the token's `roles`
  claim; add the claim-mapping step (read the configured claim, map values to
  role names, append to `User.Roles`).
- `internal/config` — an `auth.roles` section: `role_claim` (the claim to
  read, e.g. `groups`; empty = claim mapping disabled) and `role_mappings`
  (claim value → role name; unmapped values are ignored unless
  `role_claim_as_names: true`, in which case each value is itself a role
  name).
- `internal/api/server.go` — enforce `authz.Check` on the relevant endpoints
  before acting; new management endpoints: `GET/POST /api/roles`,
  `GET/PUT/DELETE /api/roles/{name}`, `GET/POST /api/role-bindings`,
  `DELETE /api/role-bindings/{id}`, `GET /api/me/permissions`.
- `internal/api` (gRPC surface) — user-role authorization applies to the
  UI-facing surface; the gRPC target surface keeps job-token auth, and the
  IdP's API-only gRPC surface stays reachable only from the API via mTLS.
- `ui/` — role management (custom role CRUD, permission checkboxes, role
  inclusion), binding management (principal + role + optional pipeline
  scope), and a "my permissions" view; hide/disable actions the caller lacks.

**Acceptance criteria.**
- [ ] A `viewer` can read pipelines/runs/logs but cannot trigger, cancel,
      approve, or edit anything (403 on each).
- [ ] An `operator` can trigger and cancel runs and approve/reject gated jobs
      (F-13), but cannot edit pipelines or manage secrets.
- [ ] An `admin` can do everything, including manage roles, users, and
      secrets.
- [ ] A principal with `roles.can-manage` can create a custom role with an
      arbitrary permission set; a principal bound to it gets exactly those
      permissions.
- [ ] A custom role that includes another role inherits its permissions
      (composition); editing the included role changes the composed
      permissions.
- [ ] A pipeline-scoped binding grants the role's resource-scoped permissions
      only for that pipeline (trigger on pipeline X succeeds, on pipeline Y
      403s); platform-wide permissions are never granted by a scoped binding.
- [ ] With `auth.roles.role_claim` set, a token whose claim carries a mapped
      value is authorized as the mapped role — including tokens from an
      external IdP that Cdrom's user directory knows nothing about.
- [ ] Delegation: a non-admin cannot grant a role they do not hold, cannot
      grant a wider scope than they hold, and `admin` is grantable only by an
      admin.
- [ ] Deny-by-default: a principal with no roles (and no mapped claims) can
      do nothing (not even list pipelines).
- [ ] Authorization is enforced server-side on both the HTTP and gRPC
      surfaces; UI visibility is not authorization.
- [ ] The acting principal is recorded on the action (feeds F-15); role and
      binding mutations are themselves audited.
- [ ] A role or binding change takes effect on every API replica without a
      restart (cache invalidation via the event log).
- [ ] `GET /api/me/permissions` returns the caller's effective permissions
      per scope, and the UI uses it to disable actions the caller lacks.
- [ ] Built-in roles cannot be deleted or have their permission sets edited;
      custom roles can.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **Deny-by-default, union of bindings.** A principal's effective permission
  set is the union of the permissions of every role bound to it (via token
  claim, claim mapping, or stored binding), expanded through role
  composition, then filtered by resource scope per request. No implicit
  permissions; no deny rules in v1 — a principal who should not have a
  permission simply is not bound to the role that grants it.
- **Roles live in the Database service; evaluation happens in the API.** Role
  and binding rows are owned by the Database service (like the IdP's user
  directory, F-24) so every API replica sees the same data; each API replica
  evaluates locally with a short-lived cache invalidated by role/binding
  mutation events on the shared event log (F-23), so a grant takes effect
  within ~50 ms on every pod.
- **The token's `roles` claim is the fast path; stored bindings and claim
  mapping are the general path.** For users known to the built-in IdP, roles
  are stamped onto the token at login (F-24) and need no DB lookup per
  request. Stored bindings and claim mapping cover the rest: external-IdP
  users, service accounts (F-26), and scoped grants. The mechanisms compose —
  a principal can have token-stamped and bound roles at once.
- **Claim mapping is config, not data.** `auth.roles.role_claim` +
  `role_mappings` are deployment config (env/config file), so an enterprise
  points Cdrom at its IdP's group claim without per-group database rows; the
  mapping is static and reviewed with the rest of the config.
- **Scoping is per-pipeline only in v1.** The resource dimension is the
  pipeline (which subsumes its runs, jobs, and secrets); worker groups and
  environments (F-17) can be added as further scope dimensions later.
  Platform-wide permissions (role/user management, audit) ignore scope.
- **API keys and service accounts plug in without new machinery.** A key's
  effective permissions are the owner's role-derived permissions ∩ the key's
  pipeline scope (F-25); a service account's are its bound roles' permissions
  (F-26). Both are just principals with a role set.

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

## Phase 6 — Authentication

### F-24 · Username/password authentication

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

### F-25 · API keys

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
  `UserPassClient`) and the `/api/api-keys` handlers: `POST /api/api-keys`
  (create for self, or for another user when the caller has the appropriate
  role), `GET /api/api-keys` (list the caller's keys, or all when admin),
  `GET /api/api-keys/{id}`, `PUT /api/api-keys/{id}` (edit/renew),
  `POST /api/api-keys/{id}/rotate`, `DELETE /api/api-keys/{id}`. The create
  and rotate responses return the plaintext key once.
- `internal/auth` — the middleware accepts `Authorization: Bearer
  <username>:<apikey>` as an alternative to a JWT: when the bearer value is of
  the form `<username>:cdrom-…` it calls the IdP's `VerifyAPIKey` and, on
  success, establishes the authenticated user (with the user's roles) plus the
  key's pipeline scope in the request context; a miss is a 401. The pipeline
  scope is carried on `auth.User` (e.g. a `PipelineScope` field) so
  authorization can limit the caller to the key's pipelines.

**Acceptance criteria.**
- [ ] A user (whether they signed in via OIDC or username/password) can create
      an API key; the response returns the plaintext `cdrom-…` key once, and
      the stored record holds only its hash.
- [ ] API keys are available to any user with the permission to create one, on
      both the OIDC and the username/password (internal) sign-in paths.
- [ ] A user with the appropriate role (e.g. `admin`) can create an API key for
      another user; the key belongs to that user and its effective permissions
      are that user's, limited to the key's pipeline scope.
- [ ] A user can have any number of API keys.
- [ ] A key's expiration date is validated to be at most one year in the
      future; an expired key is rejected.
- [ ] `Authorization: Bearer <username>:<apikey>` authenticates the caller as
      the key's owner on any `/api/*` route that accepts a JWT; a JWT and an
      API key are both accepted (both sign-in methods coexist).
- [ ] The key's effective permissions are the owner's permissions limited to
      the key's pipeline scope (a key scoped to pipeline X cannot act on
      pipeline Y, even if the owner could).
- [ ] A key can be edited/renewed (description, expiration, pipeline scope)
      without changing its secret — the same `cdrom-…` value keeps working.
- [ ] A key can be rotated: a new `cdrom-…` secret is generated and returned
      once, and the previous secret stops working.
- [ ] The plaintext key is never returned by list/get (only the prefix and
      metadata); it is returned only by create and rotate.
- [ ] After the configured number of failed API-key attempts for a user, that
      user is locked out of API-key access (for the configured duration, or
      until reset); password sign-in is unaffected.
- [ ] API-key lockout is a separate mechanism from password-attempt lockout
      (distinct counters/state).
- [ ] `auth.api_key.enabled: false` disables API-key authentication and the
      `/api/api-keys` endpoints (they respond 501).

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

### F-26 · Service accounts

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

### F-27 · Approval groups (named approver sets)

**What.** Named **approval groups** — sets of users that a pipeline's
`approval` step (F-13) can reference — so a pipeline declares *who may
approve* by group instead of hard-coding individual users. An approval step
gains an `approvers` param: a list of references, each either a **group name**
(e.g. `group:release-managers`) or an **individual user** (email/subject). The
gate is authorized for the union of every member of the referenced groups plus
the named individuals; a decision is accepted only from a user in that set who
also holds the F-14 `jobs.can-approve` / `jobs.can-reject` permission.

**Why.** Today an approval gate is authorized purely by F-14 permission, so a
pipeline can't say "only the release managers may approve this prod deploy"
without making them all admins or enumerating users in the pipeline. Teams
reorganize — people join and leave a team — and a pipeline that names
individuals goes stale. A group is a stable, centrally-managed handle the
pipeline points at; membership changes propagate without touching any pipeline
or version snapshot.

**The model.**
- **ApprovalGroup** — a named set of users: a unique `name`, a `description`,
  and a `members` list (user subjects/emails). It is a set of *people*,
  distinct from an F-14 **role** (a set of *permissions*); the two overlap only
  in that a group's membership can be derived from a role (a nice-to-have
  below). Stored in the Database service (like the IdP's user directory, F-24)
  so every API replica shares the same groups.
- **`approvers` param** on an `approval` step — a list of references. A
  reference is `group:<name>` or a bare user identifier. When the param is
  absent or empty, the gate falls back to F-13 behavior: anyone with the
  relevant F-14 permission may decide (backward compatible).
- **Authorization at decision time** — when a user calls approve/reject, the
  API resolves the gate's authorized set (members of each referenced group ∪
  named individuals) and accepts the decision only if the actor is in that set
  *and* holds the F-14 permission. Membership is resolved when the decision
  arrives, not at dispatch, so adding or removing a member takes effect for
  gates already waiting.

**Nice-to-haves** (build after the core works):
- **Reference a role** — an `approvers` entry of `role:<name>` authorizes
  everyone currently holding that F-14 role, bridging groups and roles without
  maintaining a separate member list.
- **N-of-M quorum** — a `quorum` param (K) makes the gate pass only after K
  *distinct* authorized approvers have approved, for high-blast-radius prod
  gates ("two of the release managers must sign off").
- **Membership from an IdP claim** — a group whose members are auto-derived
  from a claim value (e.g. everyone whose `groups` claim includes
  `release-managers`), so membership tracks the corporate IdP without manual
  edits (pairs with F-14's claim mapping).
- **Pipeline default approvers** — a pipeline-level default `approvers` set
  that an `approval` step inherits unless it overrides, so a team sets its
  approvers once per pipeline rather than per step.
- **UI group management + prompt** — a screen to create/edit groups and their
  members; the approval prompt (F-13) shows which group(s)/individuals the gate
  is waiting on.

**Scope.**
- `internal/models` — an `ApprovalGroup` entity (name, description, members
  JSON) registered in `All()`.
- `proto/cdrom/db/v1/db.proto` + `internal/services/database` — approval-group
  CRUD RPCs (create/get/list/update/delete) and a `ListApprovalGroupMembers`
  (or fold members into the group message) the API uses to resolve a gate's
  authorized set.
- `internal/stephandlers/approval.go` — read the `approvers` param and carry
  the references onto the gate (the handler still reports `awaiting_approval`
  and polls; it does not itself authorize — the API does).
- `internal/approval` + `internal/api` — the gate carries the `approvers`
  references; `resolveApproval` (F-13) resolves the authorized set and rejects
  a decision from an actor outside it (403) or lacking the F-14 permission.
- `internal/services/scheduler` — `ResolveApproval` validates the actor against
  the gate's authorized set (group members ∪ individuals) before persisting the
  decision.
- `internal/api/server.go` — `GET/POST /api/approval-groups`,
  `GET/PUT/DELETE /api/approval-groups/{name}` (gated by an F-14 permission,
  e.g. `roles.can-manage` or a new `approvals.can-manage`); enforce the
  membership + permission check in `resolveApproval`.
- `ui/` — approval-group management (create/edit, member list) and an approval
  prompt that names the waiting group(s)/individuals.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **A group is people; a role is permissions.** F-14 roles grant *what you may
  do*; an approval group names *who may act on a specific gate*. They are
  separate concepts that compose: the actor must be in the gate's authorized
  set (group/individual) **and** hold the F-14 permission. This keeps a
  pipeline's approval policy ("release managers") independent of platform-wide
  RBAC, and lets the same group be referenced by many pipelines.
- **Membership is resolved at decision time, not dispatch time.** The gate
  stores the `approvers` references (group names / individuals), not a frozen
  member list, so a member added or removed mid-flight is reflected the next
  time a decision is checked. This mirrors how F-14 role/binding changes take
  effect on every replica without a restart.
- **Backward compatible with F-13.** No `approvers` param → the existing
  behavior (anyone with the F-14 permission decides). Adding the param is
  opt-in per step, so existing pipelines and version snapshots are unaffected.
- **Groups live in the Database service; the API resolves and enforces.** Group
  rows are owned by the Database service (shared across replicas, F-24-style);
  the API reads members when a decision arrives and is the only place that
  authorizes, so the check is identical on every replica.
- **Quorum (nice-to-have) is a gate property, not a step re-run.** When
  `quorum: K` is set the gate tracks distinct approving actors and reports
  resolved only at K; a rejection by any authorized actor still fails the job
  immediately.

**Acceptance criteria.**
- [ ] A pipeline's `approval` step can reference a named group; a member of
      that group can approve, and a non-member gets 403.
- [ ] `approvers` can mix group names and individual users; the authorized set
      is their union (a member of any referenced group or a named individual
      may decide).
- [ ] With no `approvers` param, behavior is unchanged from F-13 (anyone with
      the F-14 `jobs.can-approve` / `jobs.can-reject` permission decides).
- [ ] A decision requires both group/individual membership **and** the F-14
      permission (a group member without the permission is still rejected).
- [ ] Adding or removing a group member takes effect for a gate already
      `awaiting_approval` (membership resolved at decision time).
- [ ] Approval groups are manageable (create/read/update/delete) via the API
      and UI, gated by an F-14 permission; group mutations are audited (F-15).
- [ ] The UI approval prompt shows which group(s)/individuals the gate is
      waiting on.
- [ ] (Nice-to-have) `role:<name>` in `approvers` authorizes everyone holding
      that role; `quorum: K` passes the gate only after K distinct authorized
      approvers.

### F-28 · SCIM user provisioning (automatic user management from an external IdP)

**What.** A **SCIM 2.0** (RFC 7643/7644) service-provider endpoint on the
built-in IdP, so an external identity provider (Azure AD / Entra ID, Okta,
Google Workspace, …) can **provision, update, and deprovision** Cdrom users
and groups automatically. When a person is added to the corporate directory
they appear in Cdrom with the right roles; when they leave they are
deactivated/deprovisioned — no manual user management in Cdrom, no drift
between the corporate directory and Cdrom. How a user record comes into
existence depends on whether SCIM is enabled: with SCIM **enabled** the
corporate IdP is the sole source of users (no auto-provisioning on login);
with SCIM **disabled** the IdP auto-provisions users just-in-time on their
first OIDC login (see **Provisioning modes** below).

**Why.** In an enterprise, users and their group memberships are owned by the
corporate IdP (e.g. Azure AD). Re-keying that into Cdrom by hand (F-24
`register`, F-14 role bindings) is slow, error-prone, and drifts. SCIM is the
standard, well-supported protocol for exactly this: the corporate IdP pushes
user/group lifecycle events to Cdrom's IdP, which maintains the user directory
(the same directory F-24/F-14 use for roles and authorization). Cdrom's
built-in IdP authenticates via the corporate IdP's OIDC tokens (F-14 claim
mapping) while also receiving SCIM provisioning — authentication and
provisioning both flow from the same corporate IdP.

**The model.**
- **SCIM service-provider surface** on the IdP's HTTP server (alongside the
  existing OIDC surface: discovery, JWKS, `/auth`, `/token`), under a
  configurable base path (default `/scim/v2`):
  - `GET /scim/v2/ServiceProviderConfig`, `GET /scim/v2/Schemas`,
    `GET /scim/v2/ResourceTypes` — metadata.
  - `GET/POST /scim/v2/Users`, `GET/PUT/PATCH/DELETE /scim/v2/Users/{id}` —
    user lifecycle.
  - `GET/POST /scim/v2/Groups`, `GET/PUT/PATCH/DELETE /scim/v2/Groups/{id}` —
    group lifecycle.
  - Standard SCIM semantics: `ListResponse` with pagination (`startIndex` /
    `count`, `totalResults`), filter support (a subset: `userName eq …`,
    `active eq …`, `externalId eq …`), and ETag/`If-Match` optimistic
    concurrency (nice-to-have).
- **SCIM → Cdrom user mapping** (RFC 7643 User schema → `IDPUser`):
  - `userName` → `Email` (the unique login identifier).
  - `name.givenName` / `name.familyName` → `FirstName` / `LastName`.
  - `active` (boolean) → the user's active/deactivated state.
  - `externalId` → stored as the corporate directory's stable ID (e.g. the
    Azure object ID), used to match re-provisioning.
  - `groups` (value refs) → role bindings (via a group→role mapping) and/or
    F-27 approval-group memberships.
  - `meta.created` / `meta.lastModified` → `CreatedAt` / `UpdatedAt`.
- **Operations:**
  - **Create** (`POST /Users`) — provision a new user (default role, e.g.
    `user`), or, if the `userName`/`externalId` matches a soft-deleted user,
    **re-provision** (clear the tombstone, reactivate) — the "re-hire" case.
  - **Update** (`PUT`/`PATCH /Users/{id}`) — update profile, active state,
    and group/role memberships.
  - **Deactivate** (`PATCH active:false`) — the user can no longer
    authenticate (login and token verification reject them) but the record is
    retained; `active:true` restores access. The "on leave" case.
  - **Deprovision** (`DELETE /Users/{id}`) — **soft delete** (tombstone): the
    user is removed from the active directory and can no longer authenticate,
    but the row is retained (never hard-deleted).

**Provisioning modes (JIT vs. SCIM).** How a user record comes into existence
depends on `idp.scim.enabled`, and the two mechanisms are mutually exclusive
(there is always exactly one source of user records):
- **SCIM enabled** — the corporate IdP is the source of truth and provisions
  users via SCIM. The IdP does **not** auto-provision on OIDC login: a user
  must already exist (SCIM-provisioned) to authenticate, and an OIDC login for
  an unknown user is rejected. This stops un-provisioned "shadow" users from
  reaching Cdrom.
- **SCIM disabled** (the default) — there is no corporate directory pushing
  users, so the IdP **auto-provisions on first OIDC login** (just-in-time):
  when a user authenticates via the OIDC flow for the first time and no record
  exists, the IdP creates one from the OIDC claims (email, name) with the
  default role; subsequent logins reuse the record. This is the simple
  local-dev / plain-OIDC mode where users are not pre-created.

**Soft delete (the core requirement).** Users are **never hard-deleted** by
SCIM (or by the admin `DELETE /api/users`, F-24). A deprovisioned user is
tombstoned: the `IDPUser` row is retained with a `deleted_at` / `active=false`
marker and its credentials invalidated. This is deliberate:
- **Audit tracing (F-15).** Audit events, approval decisions (F-13), and run
  triggers (F-07/F-09) record the actor by the user's stable subject/ID.
  Because a user is never hard-deleted, that ID remains permanently resolvable
  to a name/email, so "who did this" stays answerable long after the person
  has left. A hard delete would leave dangling actor references.
- **Stable identity.** The user's ID (the token subject) is stable across
  deprovision/re-provision cycles, so historical actions always tie back to
  the same principal.
- **Re-provisioning is safe.** Re-creating a tombstoned user (same
  `userName`/`externalId`) reactivates the existing row rather than creating a
  duplicate, preserving the stable ID and all historical attribution.

**Nice-to-haves** (build after the core works):
- **SCIM Groups → roles / approval groups** — a configurable group→role
  mapping (SCIM group name → F-14 role) so corporate group membership drives
  Cdrom roles; optionally map groups to F-27 approval groups.
- **OAuth2 client-credentials** — authenticate the SCIM caller with a standard
  OAuth2 client-credentials token from the corporate IdP (instead of a static
  bearer credential), for environments that already issue machine tokens.
- **Bulk + ETag** — SCIM `Bulk` operations and ETag/`If-Match` optimistic
  concurrency for large directories.
- **Provisioning health** — a `GET /api/scim/status` (or UI view) showing the
  last successful sync, user counts (active / deactivated / deprovisioned),
  and recent provisioning events.

**Scope.**
- `internal/models` — `IDPUser` gains an `Active` (bool), a soft-delete marker
  (`DeletedAt` / `Deleted`), an `ExternalID` (the corporate directory's stable
  ID), and a `Provisioned` flag (SCIM-managed vs. locally registered); the new
  fields are picked up by `All()` / `AutoMigrate`.
- `proto/cdrom/db/v1/db.proto` + `internal/services/database` — the user RPCs
  gain soft-delete semantics: `DeleteIDPUser` becomes a **soft delete**
  (tombstone, never hard-delete); `ListIDPUsers` gains an `include_deleted`
  flag (for audit joins); a `ReactivateIDPUser` (clear tombstone) for
  re-provisioning; the `IDPUser` message carries `active`, `deleted_at`,
  `external_id`, and `provisioned`.
- `proto/cdrom/idp/v1/idp.proto` + `internal/idp` — the IdP's gRPC API-only
  surface gains SCIM operations (`SCIMCreateUser` / `SCIMUpdateUser` /
  `SCIMDeleteUser` (soft) / `SCIMListUsers`, plus group operations) that the
  SCIM HTTP handler calls; the `UserStore` interface gains `Deactivate` /
  `Reactivate` / `SoftDelete` (and `List` takes an `includeDeleted` option).
- `internal/idp` (new `scim.go`) — the SCIM 2.0 HTTP handler on the IdP's
  existing HTTP server: the `/scim/v2/*` routes, SCIM request/response
  (de)serialization, filter + pagination, the SCIM→`UserStore` mapping, and
  SCIM error mapping (404/409/4xx per RFC 7644).
- `internal/idp` (auth) — SCIM caller authentication: the SCIM surface requires
  a Bearer credential (a dedicated SCIM service token, or an OAuth2
  client-credentials token — see nice-to-haves); unauthenticated/invalid →
  401. The SCIM surface is reachable only with that credential (it is not part
  of the public OIDC surface).
- `internal/idp` (OIDC login path) — when `idp.scim.enabled` is false, the
  OIDC authorization/token flow auto-provisions a user on first login
  (just-in-time: create the `IDPUser` from the token's claims with the default
  role); when true, the flow requires the user to already exist
  (SCIM-provisioned) and rejects an unknown user (no JIT).
- `internal/config` — an `idp.scim` section: `enabled` (default `false`; when
  false the IdP auto-provisions users just-in-time on first OIDC login, when
  true it does not — see Provisioning modes), `base_path` (default
  `/scim/v2`), the SCIM service credential (a bearer token, or a reference to
  a service account / API key from F-25/F-26), and optional
  `group_role_mappings` (SCIM group name → F-14 role name).
- `internal/api` — the admin `DELETE /api/users` (F-24) becomes a **soft
  delete** (tombstone) rather than a hard delete, so the "never hard-delete"
  invariant holds for both SCIM and manual management; `GET /api/users` can
  include deactivated/deprovisioned users for audit.
- `internal/idp` + F-15 — every SCIM operation (create/update/deactivate /
  deprovision/reactivate, group changes) writes an **audit event** (actor =
  the SCIM service, target user, operation, outcome), so provisioning history
  is traceable.
- `ui/` — a user view that shows active / deactivated / deprovisioned
  (soft-deleted) states and, when SCIM is enabled, marks SCIM-managed users
  (no manual edit of a provisioned user's identity; edits are driven by the
  corporate IdP).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **SCIM lives on the built-in IdP, not the API.** The user directory is owned
  by the IdP (via the Database service, F-24), and SCIM is an
  identity-provisioning protocol; the corporate IdP points its SCIM
  integration at Cdrom's IdP endpoint. The IdP's existing HTTP server hosts
  the new `/scim/v2/*` surface next to the OIDC surface. The API is not in the
  SCIM path.
- **JIT auto-provisioning is the complement of SCIM.** When SCIM is disabled,
  the IdP auto-provisions a user on their first OIDC login (just-in-time, from
  the token's claims, with the default role) so users need not be pre-created.
  When SCIM is enabled, JIT is off: the corporate IdP is the sole source of
  user records (via SCIM), and an OIDC login for a user with no
  SCIM-provisioned record is rejected. This guarantees that in a SCIM-managed
  deployment only users the corporate directory has provisioned can sign in.
- **Soft delete is the only delete.** Neither SCIM `DELETE` nor the admin
  `DELETE /api/users` hard-deletes a user. Both tombstone the row (retain the
  record, invalidate credentials). This keeps every historical actor reference
  (audit F-15, approvals F-13, triggers F-07/F-09) permanently resolvable, and
  makes re-provisioning (re-hire) a clean reactivation of the same stable-ID
  row. A hard delete, if ever needed for erasure, is a separate, explicit,
  admin-only operation distinct from deprovisioning (and would zero
  credentials the way F-26 does for service accounts) — it is not part of the
  normal lifecycle.
- **`active` vs. deprovisioned are distinct states.** `active:false` (SCIM
  deactivate) = the user is temporarily off (on leave): retained, reactivatable,
  credentials suspended. Deprovisioned (SCIM delete) = tombstoned: removed
  from the active directory, reactivatable only by re-provisioning. Both
  retain the row; both block authentication.
- **SCIM-managed users are not hand-edited.** A user provisioned via SCIM
  (`provisioned=true`) has its identity (email, name, active state, group/role
  memberships) driven by the corporate IdP; manual edits to those fields are
  rejected (or overridden on the next sync) so the corporate directory stays
  the source of truth. Locally-registered users (F-24) are managed manually
  and are not touched by SCIM.
- **The SCIM caller is a privileged, authenticated service.** The SCIM surface
  is protected by a dedicated credential (a SCIM service token, or an OAuth2
  client-credentials token, or a F-26 service account scoped to user
  management) — it is not public and not part of the OIDC sign-in surface.
  Only the corporate IdP (holding that credential) can provision users.
- **Groups map to roles (and optionally approval groups).** A SCIM group's
  membership is applied via a configurable group→role mapping (F-14) so
  corporate group membership drives Cdrom authorization; the same mechanism
  can target F-27 approval groups. This pairs with F-14's JWT claim mapping:
  the corporate IdP both provisions (SCIM) and authenticates (OIDC group
  claims), and the two stay consistent.

**Acceptance criteria.**
- [ ] With `idp.scim.enabled: true`, the IdP serves the SCIM 2.0 surface
      (`ServiceProviderConfig`, `Schemas`, `Users`, `Groups`) at the
      configured base path.
- [ ] With `idp.scim.enabled: false`, a user who has never logged in is
      auto-provisioned on their first OIDC login (a user record is created
      from the OIDC claims with the default role); subsequent logins reuse it.
- [ ] With `idp.scim.enabled: true`, an OIDC login for a user with no
      SCIM-provisioned record is rejected (no JIT auto-provisioning); only
      SCIM-provisioned users can authenticate.
- [ ] The SCIM surface rejects unauthenticated or invalid-credential callers
      (401); only the configured SCIM service credential is accepted.
- [ ] `POST /scim/v2/Users` provisions a new user (mapped from the SCIM User
      schema) with the default role; the user can then authenticate.
- [ ] `PATCH /scim/v2/Users/{id}` with `active:false` deactivates the user
      (login and token verification reject them) while retaining the record;
      `active:true` restores access.
- [ ] `DELETE /scim/v2/Users/{id}` **soft-deletes** (tombstones) the user: the
      row is retained, the user can no longer authenticate, and the user's ID
      remains resolvable.
- [ ] A soft-deleted user's past actions remain attributable: audit events
      (F-15), approval decisions (F-13), and run triggers (F-07/F-09) that
      named the user by ID still resolve to the user's name/email after
      deprovisioning.
- [ ] Re-provisioning a tombstoned user (same `userName`/`externalId`)
      reactivates the existing row (same stable ID) rather than creating a
      duplicate.
- [ ] The admin `DELETE /api/users` (F-24) is a soft delete (no hard delete of
      a user by any path in the normal lifecycle).
- [ ] SCIM Groups are mapped to F-14 roles (via the configured group→role
      mapping) and/or F-27 approval groups; a user's group memberships update
      their roles on sync.
- [ ] Every SCIM operation writes an audit event (actor = SCIM service, target
      user, operation, outcome).
- [ ] SCIM-managed users (`provisioned=true`) are not hand-editable in the
      UI/API for identity fields; the corporate IdP is the source of truth.
- [ ] Idempotency: a duplicate `POST /Users` for an existing `userName`
      returns the existing user (200) or 409, never a duplicate row; a
      `DELETE` of an unknown user is a 404.

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
9. **F-24** — username/password authentication (a password sign-in path
   alongside OIDC, proxied to the IdP; enabled by default for local dev).
10. **F-25** — API keys (per-user, per-pipeline-scoped, expiring, rotatable
    credentials presented as `Bearer <username>:<apikey>`; the real logic lives
    in the IdP, proxied by the API; a separate API-key lockout).

11. **F-14 → F-25 → F-26** — service accounts (non-human role-bound
    identities, two individually rotatable Database-generated keys,
    granular management permissions, disable/enable, and irreversible
    soft deletion with both key slots cleared). F-15 supplies audit history.
12. **F-13 → F-14 → F-27** — approval groups (named approver sets an
    `approval` step references instead of individual users; membership
    resolved at decision time; optional role references and N-of-M quorum).
13. **F-24 → F-14 → F-28** — SCIM user provisioning (the corporate IdP
    provisions/deprovisions users and groups into the IdP's user directory;
    users are soft-deleted, never hard-deleted, so audit attribution survives
    deprovisioning; groups map to F-14 roles / F-27 approval groups).

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
- [x] F-08 Job dependencies (DAG)
- [x] F-09 Triggers (cron / webhook / event)
- [x] F-10 Parameters & variables
- [x] F-11 Pipeline versioning
- [x] F-12 Secrets management
- [x] F-13 Approval gates
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
- [x] F-24 Username/password authentication
- [ ] F-25 API keys
- [ ] F-26 Service accounts
- [ ] F-27 Approval groups (named approver sets)
- [ ] F-28 SCIM user provisioning (external IdP; soft-delete only)
