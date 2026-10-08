# F-08 · Job dependencies (DAG)


> Phase 2 — Pipeline orchestration · [Feature index](../Features.md)


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
