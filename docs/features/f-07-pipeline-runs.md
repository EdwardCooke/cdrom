# F-07 · Pipeline runs (first-class execution entity)


> Phase 2 — Pipeline orchestration · [Feature index](../Features.md)


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
