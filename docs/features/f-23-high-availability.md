# F-23 · High-availability control plane


> Phase 5 — High availability · [Feature index](../Features.md)


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
