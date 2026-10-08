# F-10 · Parameters & variables


> Phase 2 — Pipeline orchestration · [Feature index](../Features.md)


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
