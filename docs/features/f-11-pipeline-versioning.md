# F-11 · Pipeline versioning


> Phase 2 — Pipeline orchestration · [Feature index](../Features.md)


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
