# F-03 · Job timeouts


> Phase 1 — Core execution · [Feature index](../Features.md)


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
