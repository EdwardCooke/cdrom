# F-01 · Job execution spec


> Phase 1 — Core execution · [Feature index](../Features.md)


**What.** A job carries a declarative spec describing the work to perform: an
ordered list of **steps**. Each step is agnostic about how it runs: a `type`
field selects a step handler, and the handler reads everything it needs from
the common fields (`workdir`, `env`, `timeout`) and from `params`.
Handler-specific settings live in `params` (a map of name → value, where a
value is either a scalar string or a list of strings), not in the step's own
fields, so a new step type can be added without changing the spec schema. The
built-in `shell` handler (default when `type` is empty) runs a command,
directly or through a user-chosen shell, reading its `command`, `args`, and
`shell` from `params`. The worker/agent executes the steps in order and the
job fails on the first step that errors.

**Why.** CD jobs are fundamentally "run these commands in this environment".
Without a spec, a job is just a label. This is the single most important
missing piece.

**Scope.**
- `internal/models` — add a `JobSpec` (or embed steps in `Job`): steps with
  `type`, `workdir`, `env`, `timeout`, and a `params` map carrying
  handler-specific settings. Decide whether the spec is stored on the `Job`
  row (per-run snapshot) or referenced from the `Pipeline` (shared
  definition). **Recommendation:** store a snapshot on the run so a pipeline
  edit never changes what a past run did.
- `proto/cdrom/db/v1/db.proto` + `proto/cdrom/api/v1/api.proto` — carry the
  spec to the execution target.
- `internal/worker/worker.go`, `internal/agent/agent.go` — replace the
  `runJob` stub with a real executor that runs each step (portable across
  Windows + Linux — see cross-platform constraints).
- `internal/api/server.go` — accept the spec on `POST /api/jobs`.

**Acceptance criteria.**
- [x] A job submitted with a multi-step spec runs the steps in order on a
      worker and on an agent.
- [x] A failing step marks the job `failed` and stops subsequent steps.
- [x] Step environment variables are available to the command.
- [x] The spec is portable: the same job spec executes on a Windows worker and
      a Linux worker (no shell-specific assumptions, or an explicit shell
      contract is documented).
- [x] A step's `shell` param runs the step through a user-chosen shell
      (`<shell> <args> <command>`), e.g. `pwsh` on Windows.
- [x] A step's `type` selects its handler: the built-in `shell` handler runs
      by default, a target can register new types via
      `executor.RegisterStepType`, and an unregistered type fails the job with
      a clear error.
- [x] The built-in `token_exchange` step handler requests a new job token for
      a different audience (via the target's `TokenExchange`, which calls the
      API's `ExchangeJobToken` RPC) and writes the exchanged token to the step's
      output directory, so later steps and jobs can read it through the
      condition context.
- [x] `GOOS=windows GOARCH=amd64 go build ./cmd/...` stays green.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- The spec is a **per-run snapshot** stored on the `Job` row (a JSON `text`
  column via GORM's `serializer:json`), so a pipeline edit never changes what
  a past run did.
- The canonical spec type is `cdrom.db.v1.JobSpec`; the API and scheduler
  protos reference it rather than redefining it.
- Execution is shared by worker and agent in `internal/executor`
  (`executor.Execute(ctx, spec, logger)`).
- **Step types & handlers:** the executor (`internal/executor`) is the generic
  dispatch engine; a step's `type` selects an `executor.StepHandler` (empty →
  the handler registered under `executor.DefaultType`). The concrete handlers
  live in `internal/stephandlers`; the built-in `shell` handler registers
  itself under `executor.DefaultType` at package init, and the built-in
  `token_exchange` handler registers itself under `token_exchange` (see the
  Token exchange handler below). A target or plugin adds new types with
  `executor.RegisterStepType(name, handler)` (e.g. `ansible`, `terraform`,
  `argo`). An unregistered type fails the job with a clear error.
  `params` carries handler-specific configuration so a new type needs no
  spec-schema change. The worker and agent blank-import `internal/stephandlers`
  so the built-in handlers are registered before any job runs. This is the
  seam for the later plugin architecture.
- **Shell contract:** the shell handler reads its `command` (string param),
  `args` (list param), and `shell` (string param) from the step's `params`.
  The command is executed directly by the target OS — no implicit shell. Steps
  needing shell behavior invoke a shell explicitly (`sh -c ...` / `cmd /c ...`).
  Step stdout/stderr are inherited from the target (local logging) and, when a
  log sink is present, also streamed to the API (F-02).
- **Shell override:** a step may set the `shell` param to run through a
  user-chosen interpreter; the target executes `<shell> <args> <command>`
  (e.g. `shell: "pwsh"`, `args: ["-NoProfile", "-Command"]`,
  `command: "Get-ChildItem"`). When `shell` is empty the step runs `command`
  directly, preserving the no-implicit-shell contract.
- **Token exchange handler:** the built-in `token_exchange` step handler
  (`internal/stephandlers/token_exchange.go`) lets a running job request a new
  job token for a different audience (e.g. an outside resource the job needs to
  call) and hand the exchanged token to later steps and jobs. It reads
  `audience` (string, required), `expires_in` (string, a Go duration; empty
  means the API's default exchanged-token lifetime), and `output` (string, the
  step-output name the token is written under, default `token`) from the step's
  `params`. It obtains the exchanged token from the execution target through the
  executor's `TokenExchange` (set via `executor.ContextWithTokenExchange`): the
  worker and agent implement it by calling the API's `ExchangeJobToken` RPC,
  presenting the job's own token. The executor gives every step a per-step
  output directory (the step's env carries `executor.StepOutputDirEnv`), so the
  handler always writes the token into it under the output name; the executor
  reads every file in that directory back as a step output (file name = output
  name), so downstream steps and jobs read it through the condition context
  (`.steps`/`.jobs` `.Outputs`). A step with no `audience`, an invalid
  `expires_in`, or a target with no
  `TokenExchange` fails the job.
- Per-step `timeout` is enforced by the target via a derived context; a
  job-level `timeout` bounds the whole job (F-03), and a scheduler-side
  watchdog reaps jobs whose target goes silent (F-03).
