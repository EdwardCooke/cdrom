# F-04 · Retry & re-run


> Phase 1 — Core execution · [Feature index](../Features.md)


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
