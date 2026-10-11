# F-16 · Concurrency control & queueing


> Phase 3 — Safety & governance · [Feature index](../Features.md)


**What.** Bound how much runs in parallel: max concurrent jobs per worker
group (and/or globally), a visible pending queue, optional job priority, and
**concurrency keys** — named, cross-run (and cross-pipeline) locks that limit
what may run at the same time. Jobs beyond a limit, or blocked by a held
concurrency key, wait their turn.

**Why.** Without limits a burst of jobs overwhelms targets; operators need to
see and manage the backlog. Concurrency keys additionally serialize work that
contends for a shared external resource (a database, a deploy target, a
license, a single mutable environment) even when that work is spread across
different pipelines and runs.

**Scope.**
- `internal/models` / proto — a `concurrency_keys` list on the pipeline and
  its version snapshot, and on each job definition (the keys are
  **templatable**, rendered against run params/identity at run creation); a
  `ConcurrencyLimit` table (a regex-based limit rule: kind + pattern + limit)
  and the durable `ConcurrencyResource` slot accounting.
- `internal/services/database` — the durable, atomic slot accounting (who
  holds a resource, how many slots are free), the regex-based limit rules
  (loaded from the database, cached, and resolved per resource), and the CRUD
  RPCs for the limit rules, shared by every scheduler replica.
- `internal/services/scheduler` — a concurrency gate on the dispatch path
  (a job is dispatched only when it fits within every limit it is subject to)
  plus a leader-gated admission loop that re-checks blocked pending jobs, and
  priority ordering of the pending queue.
- `internal/api` — `GET/POST/PUT/DELETE /api/concurrency-limits` (gated on
  `concurrency-limits.can-view` / `can-manage`), `GET /api/queue`, and the
  `queue` / `concurrency_limits` WS events.
- UI — a queue view (pending jobs, who's running what, which key is blocking
  a job) and a limits view (the regex-based rules).

**Acceptance criteria.**
- [x] With a limit of N per group, at most N jobs run concurrently on that
      group; the rest stay `pending`.
- [x] When a job finishes, a queued job is dispatched.
- [ ] Higher-priority jobs are dispatched before lower-priority ones.
      *(deferred: priority was out of scope for this implementation; the
      admission loop admits pending jobs in id order.)*
- [x] The UI shows the pending queue and current load per group.
- [x] Two pipelines that share a pipeline-level concurrency key run at most
      one at a time; a pipeline with no key is unaffected and can run at any
      time.
- [x] Two jobs in different pipelines that share a job-level concurrency key
      do not run at the same time.
- [x] A job with multiple keys is blocked if *any* of its keys is held (the
      keys are OR'd), and it holds *all* of its keys while it runs.
- [x] A job blocked only by a held concurrency key stays `pending` and is
      dispatched once every one of its keys is free.
- [x] A job is dispatched only when its group has a free slot *and* none of
      its concurrency keys are held.
- [x] The UI shows which concurrency key(s) are blocking a pending job.
- [x] The concurrency limits (global / per-group / per-key) are stored as
      regex-based rules in the database and can be created, edited, and
      deleted at runtime via `GET/POST/PUT/DELETE /api/concurrency-limits`
      without a redeployment; a change takes effect on the next admission.
- [x] A concrete resource's limit is resolved from the rules by regex: the
      most specific (longest) matching pattern wins, and a concurrency key
      matching no rule defaults to a limit of 1 (a lock).
- [x] Managing the limits is gated on `concurrency-limits.can-view` (list)
      and `concurrency-limits.can-manage` (create/update/delete); admins have
      both by default.
- [x] A pipeline's and a job's concurrency keys are templatable: each is
      rendered as a Go template against the run's parameters and identity when
      the run's job instances are created (e.g. `{{ .params.env }}-db`); a key
      that references a parameter with no value fails run creation.

**Design decisions.**
- **A concurrency key is a named, global lock.** A key is a short string
  (e.g. `prod-db`, `deploy-cluster-a`) that is meaningful across the whole
  system, not scoped to a pipeline or a run. Anything that carries the same
  key — a pipeline run or a job instance, in any pipeline, in any run —
  contends for the same set of slots. This is what lets "pipeline A and B
  share a key, so only one of them runs at a time" and "job A in pipeline 1
  and job B in pipeline 2 share a key, so they never overlap" both fall out of
  one mechanism.
- **Multiple keys are OR'd.** A pipeline or a job may declare **several**
  keys (a list, not a single value). The keys are **OR'd, not AND'd**: a job
  is blocked if *any one* of its keys is held by another running job, and
  while it runs it holds *all* of its keys. So a job with keys `{prod-db,
  prod-cache}` is blocked if `prod-db` is held *or* `prod-cache` is held, and
  it blocks any other job that shares either key. Because a job acquires all
  of its keys atomically (it only dispatches when every one is free), there
  is no partial hold and therefore no deadlock.
- **Two levels, composed.** A pipeline may declare a set of `concurrency_keys`
  (each a named lock, whose limit is resolved from the regex-based limit
  rules, defaulting to 1 when no rule matches): while a run of that pipeline
  is active, no run of *any* pipeline sharing one of those keys may start. A
  job definition may declare its own set of `concurrency_keys`: while a job
  instance holding one of them is active, no other job instance sharing that
  key (in any pipeline or run) may start. A job's effective key set is the
  union of its pipeline's keys and its own keys; it is dispatched only when
  **none** of that set is held, so the two levels compose rather than
  override. A pipeline with no keys and a job with no keys are unconstrained
  by keys.
- **The keys are part of the per-run snapshot.** Like the spec and `needs`, a
  pipeline's keys are captured on its version snapshot (F-11) and a job's keys
  are denormalized onto each run's job instance (F-07), so a run is gated by
  the keys that were active when it started and editing a pipeline never
  changes what an in-flight or past run contends for.
- **Slot accounting is durable and atomic.** The Database service owns a
  per-key slot table (key → how many of the limit are currently held, and by
  which job/run). Acquiring a slot is a single atomic transaction: it reads
  the free count and, if one is available, records the holder and decrements
  it; otherwise the job is left `pending`. A slot is released when its holder
  reaches a terminal state (succeeded, failed, cancelled, timed_out, or
  skipped). Because the accounting lives in the Database service and the
  acquire is transactional, multiple scheduler replicas (F-23) can never
  double-dispatch the last free slot, and a restart re-derives held slots from
  the live job rows rather than from in-memory state.
- **The gate sits on the scheduler's dispatch path.** Before the scheduler
  appends an "assignment" event (or queues a job for an ephemeral agent), it
  checks that none of the job's keys are held and, if so, atomically marks
  them held. If *any* key is held, the job is left `pending` and a background
  loop (alongside the dependency resolver, F-06/F-08) re-checks pending jobs
  whose keys are now all free and dispatches them. This composes with the
  existing per-group limit: a job is dispatched only when its group has a free
  slot *and* none of its keys are held.
- **Keys are additive to the group limit, not a replacement.** The per-worker
  group limit (and any global limit) still bounds how much runs on a target;
  concurrency keys bound how much runs *across* targets and pipelines that
  share a resource. A job can be group-limited, key-blocked, or both.

**Implementation notes.**
- **Slot accounting is a counter, not a holder list.** The Database service
  keeps one `ConcurrencyResource` row per resource (a concurrency key, or the
  synthetic `group:<name>` / `global` resource) whose `Held` counter is the
  number of jobs currently holding a slot on it. Acquiring is a guarded
  atomic increment (`held < limit`); releasing is a guarded decrement
  (`held > 0`). The guard is what makes the last free slot safe against
  concurrent scheduler replicas. (A plain `held - 1` is used rather than
  `GREATEST(held - 1, 0)` because `GREATEST` is not a SQLite function and the
  pure-Go SQLite driver has no such builtin; the guard makes the clamp
  unnecessary.)
- **Limits are regex-based rules in the database, not configuration.** The
  limits that bound how much runs in parallel — the system-wide cap,
  per-worker-group caps, and per-concurrency-key caps — are stored as
  `ConcurrencyLimit` rows (a rule = `kind` + `pattern` + `limit`) in the
  Database service, so they can be edited at runtime via the API
  (`GET/POST/PUT/DELETE /api/concurrency-limits`) without a redeployment. A
  rule's `pattern` is a regular expression matched against the dimension's
  name (a worker group's name or a concurrency key's name; empty for the
  single `global` resource). A concrete resource's effective limit is the
  limit of the **most specific (longest) matching rule** of its kind; a
  resource matching no rule falls back to a default (a concurrency key
  defaults to a limit of 1 — a lock; a group or the global resource defaults
  to unlimited). The Database service caches the rules briefly (invalidated
  immediately on a create/update/delete, and expiring after a short TTL as a
  safety net for changes made on another replica) so the dispatch path does
  not query per job.
- **Keys are denormalized onto the job row.** A job's effective key set is the
  union of its pipeline's keys (from the pipeline or its version snapshot) and
  its own keys (from its spec); the union is stored on the job row at create
  time so the admission gate and the queue view never have to re-read the
  pipeline or spec.
- **The keys are templatable.** A pipeline's and a job's concurrency keys are
  rendered as Go templates (the same `missingkey=error` semantics as spec
  interpolation, F-10) against the run's parameters and identity (run id,
  pipeline id, trigger) when the run's job instances are created, so a key can
  be derived from a run parameter (e.g. `{{ .params.env }}-db`). The pipeline's
  keys are rendered once per run; each job's own keys are rendered per job. A
  key that references a parameter with no value fails run creation (a run is
  never created with a key that could not be resolved).
- **The admission loop is the single dispatch path for a released job.** The
  dependency resolver (F-06/F-08) no longer dispatches a job whose
  dependencies are satisfied; it only clears the job's `depends_on` and leaves
  it pending. The admission loop (leader-gated, like the other background
  loops) is what admits and dispatches it, so every dispatch — direct,
  dependency-released, or retried — passes the concurrency gate exactly once.
- **The queue and the limits are surfaced to the UI.** `GET /api/queue`
  returns the current queue (pending jobs with the resource(s) blocking each,
  per-group and global load, per-key load); the WebSocket snapshot includes it
  and the current concurrency limit rules, and a `queue` event is published
  whenever a job's status changes (the thing that frees or takes a slot) and a
  `concurrency_limits` event is published whenever a limit rule is created,
  updated, or deleted, so the UI's queue and limits views stay live.
- **Priority is deferred.** The admission loop admits pending jobs in id
  order; per-job priority (and priority ordering of the queue) is not
  implemented and is left for a later pass.
