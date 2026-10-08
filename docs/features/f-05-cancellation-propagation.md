# F-05 · Cancellation propagation


> Phase 1 — Core execution · [Feature index](../Features.md)


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
