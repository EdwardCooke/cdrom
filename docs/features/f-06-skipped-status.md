# F-06 · `skipped` job and step status


> Phase 1 — Core execution · [Feature index](../Features.md)


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
