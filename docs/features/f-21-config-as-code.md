# F-21 · Config as code


> Phase 4 — Delivery & observability · [Feature index](../Features.md)


**What.** Define pipelines (jobs, steps, deps, triggers, params) in a
declarative file format (YAML) that can be imported/exported and checked into
source control. The UI editor and the file format are two views of the same
model. A pipeline's definition can be authored in the UI **or** live in an
external source: a **git** repository, an **HTTP/HTTPS endpoint**, or a file on
the **local filesystem**. An **externally-sourced** pipeline is pulled from its
source on a schedule, refreshed again when a run is kicked off, and can be
refreshed manually. The actual fetching (git clone/fetch, HTTP GET, or a local
file read) is done by a dedicated **source-fetcher** service — a separate
process that the API dials over gRPC (mutual TLS, like the other services) — so
the API and scheduler never need git, network egress, or the source's
filesystem. The raw YAML is stored in the database alongside a content hash so
the platform can tell when the file changed, and it is retrievable through the
API. **The source itself is not in the file** — where a pipeline is pulled from
is a database concept (a `PipelineSource` row) configured through the API, so
the same file can be checked into git, served over HTTP, or mounted on the
filesystem. The file format is deliberately **not** the database schema: it is
human-authored and references everything **by name** (step name, job name,
variable, vault, worker group) rather than by ordinal, and a **compiler** maps
those names onto the database schema (resolving step names to step indices, job
names to job ids, and worker names to groups) when it stores the definition. The
file follows the **Kubernetes manifest convention**: an `apiVersion` (currently
`cdrom/alphav1`), a `kind` (`Pipeline`, `VariableSet`, `Step`, or `Job` — so the
compiler knows what a document is), a `metadata` block (`name`, `description`,
`labels`, `annotations`), and a `spec` holding the rest of the definition. As
part of F-21 the project's `tags` are renamed to `labels` (the F-29 concept),
and `metadata.annotations` is added for UI searchability.
Reusable units — sets of variables, steps, or entire jobs — are pulled in from
other files with **explicit fields** (`importFrom` on a step or job, `import`
on a variable set), and steps are **reused** across environments: shared steps
live under `spec.steps` and each environment's job references them by name
(`ref`), so environments differ only in their variables and a few local steps.
There is no separate concept of a secret: every value is a variable, and a
variable is **masked** when its value is not a plain string — an `encrypted`
(builtin F-12 ciphertext) or `secretSource` + `key` (from a named
`secretSources` entry) value. The platform tracks which variables are masked
(their values are redacted in logs and hidden in the UI), and a masked
variable is referenced exactly like any other variable
(`{{ .params.<name> }}`). A variable set can import a vault's values with
`import: [{vault: <name>}]`. A job names the environment it targets and the
worker group it runs on (`workerGroup`, defaulting to `spec.workerGroup`).

**Why.** Teams want pipeline definitions in version control, reviewable via PR.
A single source of truth (a git repo, an endpoint, or a mounted file) means the
definition that runs is the one that was reviewed, and the same job can be
deployed to `dev`/`staging`/`prod` by changing variables rather than copying the
definition. Isolating the fetch in its own process keeps git, network egress,
and source credentials out of the API and scheduler.

**Scope.**
- A schema for the pipeline definition file (documented in `docs/`): a
  Kubernetes-style manifest (`apiVersion` / `kind` / `metadata` / `spec`),
  name-based referencing, camelCase mapping keys, steps as name → spec maps
  (shared under `spec.steps` and job-local), variable sets as inline `variables`
  maps and/or `import` from external files or vaults, masked variables (an
  `encrypted` or `secretSource` + `key` value, referenced like any other
  variable), and reusable units (variable sets, steps, jobs) pulled in with
  explicit fields (`importFrom` / `import`).
- A **compiler** (`internal/pipelinefile`) that validates a definition, splices
  the import fields (`importFrom` / `import` / `ref`), and maps the result
  onto the database schema (names → ordinals/ids), shared with F-08 (DAG) and
  F-10 (params) validation.
- `internal/models` — a `PipelineSource` (where a pipeline is pulled from: a
  `kind` of `git` / `http` / `filesystem`, plus the kind's URL / branch / path,
  a refresh `schedule`, and a credential reference), a `PipelineSourceState`
  (last sync time, last source version, last file hash, last error), and a
  `SourceCredential` (a regex-matched credential for git over SSH/HTTPS or an
  HTTP/HTTPS endpoint); the raw YAML + content hash on the `Pipeline` row.
- `cmd/pipelinesource` + `internal/services/pipelinesource` — the **source
  fetcher**: a standalone gRPC service (a new process, like `db` / `scheduler` /
  `artifacts` / `idp`) that performs the actual fetch — git clone/fetch, HTTP
  GET, or a local file read — and returns the file's content, version, and
  hash. It is stateless (no definitions, no credentials, no key at rest) and is
  dialed **only by the API** over gRPC (mutual TLS when a CA is configured).
- `proto/cdrom/pipelinesource/v1/` — the fetcher's gRPC API (`FetchSource`);
  generated code in `internal/gen/` (`make proto`).
- `internal/services/database` — source/credential CRUD, the stored YAML +
  hash, and the compiled pipeline.
- `internal/api` — the refresh orchestrator: it dials the fetcher to pull a
  source, decrypts the source's credential (F-12) and hands the plaintext to
  the fetcher for that one fetch, compares the returned hash to the stored one,
  and re-imports on change. It owns the **manual refresh** endpoint, the
  **refresh-on-run** hook (in the run-creation path), and a **leader-gated
  scheduled refresh** loop (a named lease on the Database service, like the
  scheduler's loops, F-23).
- `internal/api/server.go` — import (validate + create/update pipeline), export
  (render the current definition back to the file format), **refresh** (manual
  re-pull), **get source** (the stored YAML + hash + last sync), and the
  **source-credential** CRUD endpoints.

**Acceptance criteria.**
- [ ] A pipeline defined in YAML can be imported (via the API or from an
      external source) and becomes runnable.
- [ ] A pipeline definition is a Kubernetes-style manifest with an `apiVersion`
      (currently `cdrom/alphav1`), a `kind` (`Pipeline` / `VariableSet` / `Step`
      / `Job`), a `metadata` block, and a `spec`; the compiler dispatches on
      `kind` to know how to interpret a document (and an imported file's `kind`
      tells it how to splice the file in).
- [ ] `metadata` carries `name`, `description`, `labels` (the F-29 key/value
      pairs, renamed from `tags` as part of F-21), and `annotations` (free-form
      key/value pairs used only for UI searchability — they never affect
      behavior); everything else is under `spec`.
- [ ] Exporting a pipeline reproduces a file that re-imports equivalently
      (round-trip): the exported file, re-imported, yields the same compiled
      definition.
- [ ] Invalid definitions are rejected with actionable errors (a clear message
      naming the offending file, key, and reason — e.g. an unknown step
      reference, a cycle, or a parameter with no value).
- [ ] A pipeline can be sourced from a **git** repository, an **HTTP/HTTPS
      endpoint**, or a file on the **local filesystem** (a `PipelineSource`
      `kind`), and each kind is fetched by the source-fetcher service.
- [ ] An externally-sourced pipeline is refreshed on its schedule (cron) and
      again when a run of it is kicked off; the raw YAML and its content hash
      are stored in the database and retrievable via the API.
- [ ] A change in the source (detected by a changed content hash) re-imports the
      pipeline (bumping its version, F-11); an unchanged file is a no-op (no
      version bump, no re-dispatch).
- [ ] A pipeline can be refreshed manually via the API (an explicit re-pull that
      applies the latest file regardless of schedule).
- [ ] The actual fetch (git / HTTP / filesystem) is performed by the separate
      source-fetcher process, which the API dials over gRPC; with a TLS CA
      configured the connection uses mutual TLS, and without one it falls back
      to plaintext (local dev) — the same convention as the other services.
- [ ] The source-fetcher holds no definitions, credentials, or encryption key at
      rest: the API decrypts the source's credential and passes the plaintext to
      the fetcher for a single fetch, which is never persisted or logged.
- [ ] A step's outputs are referenced in the file by a **user-definable step
      name** (not an ordinal); the compiler maps each step name to its step
      index (by declaration order) when storing the `JobSpec`, so the stored
      spec uses ordinals while the file uses names.
- [ ] A pipeline can pull in sets of variables, steps, or entire jobs from other
      files with explicit fields (`importFrom` on a step or job, `import` on a
      variable set); an imported unit is spliced in place and validated as if it
      were inline.
- [ ] Steps can be reused across environments: shared steps under `spec.steps`
      are referenced by a job with `ref`, and each environment is a distinct job
      (unique name) that reuses the shared steps and differs only by its
      variable sets and a few job-local steps.
- [ ] A `shell` step's handler value may be a plain string: `shell: <string>`
      is shorthand for `shell: {command: <string>}` (only the `command` param
      is set, nothing else); a shell step that sets no `shell` param runs its
      command through the target OS's default shell.
- [ ] There is no separate concept of a secret: a variable is `name: value`
      (a plain value), `name: {encrypted: <ciphertext>}` (a builtin F-12
      ciphertext), or `name: {secretSource: <name>, key: <key>}` (a value from
      a named `secretSources` entry); a variable is **masked** when its value
      is not a plain string, the platform tracks which variables are masked
      (their values are redacted in logs and hidden in the UI), and all
      variables — masked or not — are referenced the same way, as
      `{{ .params.<name> }}`.
- [ ] A variable set can import a vault's values with `import: [{vault: <name>}]`
      (every key in the vault, as masked variables) or
      `import: [{vault: <name>, keys: [a, b, c]}]` (a subset of keys).
- [ ] A job's `steps` is a name → spec map that can be pulled in from another
      file with `importFrom`; any steps the job defines locally override the
      imported steps by name (the imported steps are the base, local steps win).
- [ ] An empty step entry in a job's `steps` map (no fields) is shorthand for
      `ref: <same name>` — it implicitly references the shared step of the same
      name; a name with no matching shared step is a validation error.
- [ ] A job can be pulled in from another file with `importFrom: <uri>`; any
      fields set on the local job override the imported job's (like a step's
      `importFrom`).
- [ ] A job names the environment it targets (`environment`, F-17) and the
      worker group it runs on (`workerGroup`, defaulting to `spec.workerGroup`).
- [ ] Variable sets are either inline `variables` maps or `import` from
      external files or vaults; a job references sets by name in its
      `variableSets` list.
- [ ] Mapping keys in the file are camelCase (`concurrencyKeys`, `variableSets`,
      `importFrom`, `secretSources`, …).
- [ ] The pipeline's source (git / http / filesystem) is **not** in the file —
      it is a `PipelineSource` stored in the database and configured through the
      API, so the same file is portable across source kinds.
- [ ] Standard deployment options in the file (timeout, retry, failure mode,
      concurrency keys, environment, automation, approval, needs, triggers,
      params, labels) map onto the database schema.
- [ ] Named, referencable resources are declared in a consistent `spec` section
      and referenced by name — `secretSources` (F-12), and the same pattern for
      variable sets, steps, and jobs.
- [ ] Source credentials (git over SSH/HTTPS, and HTTP/HTTPS endpoint tokens)
      are stored in the database (secret material encrypted, F-12), matched to a
      source's URL by a regex pattern, and updatable through the API; the
      plaintext is never returned to the UI.

---

## The file format

A pipeline definition is a single YAML document (or a set of documents linked
by imports). It is **declarative** and **name-based**: every reusable unit is
given a name, and references point at names, never at positions. The file does
not need to — and generally will not — match the database schema. The compiler
is the only place the two are reconciled.

### The manifest shape (Kubernetes-style)

Every definition is a **Kubernetes-style manifest** with four top-level keys:

| Key | Purpose |
|-----|---------|
| `apiVersion` | The schema version of the file format. Currently `cdrom/alphav1`; bumped as the format evolves through the project's lifecycle. |
| `kind` | What the document is: `Pipeline`, `VariableSet`, `Step`, or `Job`. The compiler dispatches on `kind` to know how to interpret a document (and an imported file's `kind` tells it how to splice the file in). |
| `metadata` | Identity and classification: `name` (required), `description`, `labels` (the F-29 key/value pairs, renamed from `tags` as part of F-21), and `annotations` (free-form key/value pairs used only for UI searchability — they never affect behavior). |
| `spec` | The definition proper: everything else (see below). |

`apiVersion` and `kind` are the only keys the compiler uses to route a
document; `metadata` is the identity block; and **everything else lives under
`spec`**. A `Pipeline` document's `spec` holds the jobs and the pipeline-level
config; a `VariableSet` document's `spec` is a single variable set (its
`variables` and/or `import`); a `Step` document's `spec` is a single step
definition; and a `Job` document's `spec` is a single job spec (its `steps`,
`needs`, `variables`, `environment`, and deployment options). Each file holds
exactly one unit, so a file contributes a reusable unit (`kind: Step` /
`VariableSet` / `Job`) that many pipelines pull in by import.

### The `spec` of a `Pipeline`

A `Pipeline` document's `spec` has these fields (all optional except `jobs`):

| Field | Purpose | Referenced by |
|-------|---------|---------------|
| `steps` | Shared, reusable step definitions (a name → step-spec map) | a job's `steps` (via `ref`) |
| `variableSets` | Named variable sets — inline `variables` maps and/or `import` from external files or vaults (F-10) | a job's `variableSets` list (by name) |
| `secretSources` | Named secret sources (F-12) | a variable's `secretSource` + `key`, or a variable set's `import: [{vault}]` |
| `jobs` | The jobs (a name → job-spec map: `steps`, `needs`, `variables`, `variableSets`, `environment`, `workerGroup`, deployment options) | the pipeline's DAG (`needs`) |
| `workerGroup` | The pipeline's default worker group (F-17); a job's `workerGroup` overrides it | dispatch |
| `failureMode` | Pipeline-level failure mode (F-01) | — |
| `concurrencyKeys` | Pipeline-level concurrency keys (F-16, templatable) | — |
| `params` | Pipeline-level parameters (F-10) | `{{ .params.<name> }}` |
| `triggers` | Pipeline-level triggers (F-09) | — |

The same "declare a named thing in a section, reference it by name everywhere
else" shape is used for every kind of reusable resource. `steps` is the
canonical example; `variableSets`, `secretSources`, and `jobs` follow it.
Pipeline-level config (`failureMode`, `concurrencyKeys`, `params`, `triggers`)
lives in the `spec` alongside the jobs, and the pipeline's default worker group
is a `spec.workerGroup`. Mapping keys are camelCase.

**The source is not in the file.** Where a pipeline is pulled from (a `git` /
`http` / `filesystem` source, its schedule, and its credential) is a **database**
concept — a `PipelineSource` row associated with the pipeline (see
[Externally-sourced pipelines](#externally-sourced-pipelines)) — not part of the
YAML. The file is purely the definition; the source is configured through the
API and stored in the database. This keeps the file portable: the same file can
be checked into git, served over HTTP, or mounted on the filesystem, and the
source that fetches it is a deployment concern, not a definition concern.

### A complete example

```yaml
# pipelines/webapp.yaml
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
  description: Build, test, and deploy the web app
  labels:
    team: platform
    tier: "1"
  annotations:
    owner: platform-team
    review: required
spec:
  # The pipeline's default worker group (F-17). A job's `workerGroup`
  # overrides it for that job.
  workerGroup: webapp-workers

  # Shared, reusable steps (a name -> step-spec map). A step spec is inline, a
  # `ref` to a shared step, or an `importFrom` pull from another file. The
  # step's handler is named by a key (here `shell`), whose value is that type's
  # params (or, for `shell`, a plain string that is shorthand for
  # `{command: <string>}`); common fields (workdir, env, timeout, ...) sit
  # alongside it.
  steps:
    # A plain string for `shell` is shorthand for `shell: {command: <string>}`
    # (only the `command` param is set); the command runs through the target
    # OS's default shell.
    step1:
      shell: echo "Yes"
    build:
      shell:
        command: make build
      workdir: /src
      timeout: 10m
      env:
        GOFLAGS: -mod=vendor
    # Pull a step in from another file (see "Imports and references").
    unit-test:
      importFrom: git://github.com/blah/blah/steps/unit-test.yaml

    # --- Steps that set every option of each built-in handler -------------
    # Each step below sets all of its handler's params plus the common step
    # fields (workdir, env, timeout, condition, ignoreFailed, once), so the
    # example shows the full surface of the file format.
    #
    # A shell step (the default handler) with every option set. The outer
    # `shell` key names the handler type; the inner `shell` key is the
    # handler's param that runs the command through a shell instead of
    # directly (here `sh -c "make build --verbose"`).
    full-shell:
      shell:
        command: "make build --verbose"
        args: ["-c"]
        shell: sh
      workdir: /src
      env:
        GOFLAGS: -mod=vendor
        CI: "true"
      timeout: 10m
      condition: '{{ .params.environment == "prod" }}'
      ignoreFailed: true
      once: true
    # An approval gate (F-13) with every option set. The `message` param is a
    # template rendered against the condition context (prior steps' outputs,
    # upstream jobs, the job's identity, and run params — which include masked
    # variables); the step's `timeout` bounds how long the job may wait at the
    # gate.
    approve-prod:
      approval:
        message: 'Deploy {{ .params.image_tag }} to {{ .params.environment }}?'
      timeout: 1h
      condition: '{{ .params.environment == "prod" }}'
      ignoreFailed: false
      once: true
    # A token-exchange step with every option set. It requests a new job token
    # for a different audience and writes it to a step output (named by
    # `output`) that later steps and jobs can read.
    mint-registry-token:
      token_exchange:
        audience: container-registry
        expires_in: 15m
        output: registry_token
      timeout: 1m
      condition: '{{ .params.push == "true" }}'
      ignoreFailed: false
      once: true
    # An email notification (F-18) with every option set. The to/cc/bcc/
    # subject/body fields are templates rendered against the condition context,
    # so they can reference a prior step's output or a run parameter (masked or
    # not).
    notify-team:
      email:
        to:
          - team@example.com
        cc:
          - leads@example.com
        bcc:
          - archive@example.com
        subject: 'Deployed {{ .params.image_tag }} to {{ .params.environment }}'
        body: |
          Deployed `{{ .params.image_tag }}` to `{{ .params.environment }}`
          (build `{{ index .steps "build" .Outputs.version }}`).
        content_type: markdown
        from: cdrom@example.com
        reply_to: oncall@example.com
      timeout: 30s
      condition: '{{ .params.environment == "prod" }}'
      ignoreFailed: true
      once: true
    # A webhook notification (F-18) with every option set. The url/headers/
    # body are templates rendered against the condition context. The handler's
    # `timeout` param is the HTTP response timeout (clamped by the API); the
    # common `timeout` field is the step's overall timeout.
    notify-slack:
      webhook:
        url: https://hooks.example.com/services/T000/B000/XXXX
        method: POST
        headers:
          - "Content-Type: application/json"
          - "X-Run-Id: {{ .run.ID }}"
        body: |
          {"text": "Deployed {{ .params.image_tag }} to {{ .params.environment }}"}
        content_type: plain
        timeout: 10s
      timeout: 30s
      condition: '{{ .params.environment == "prod" }}'
      ignoreFailed: true
      once: true

  # Named variable sets (F-10). Each has an inline `variables` map and/or an
  # `import` list of external files or vaults (the compiler fetches each file
  # via the source-fetcher, or reads the named vault's values). A set's
  # `variables` support the same value shapes as a job's `variables`: a plain
  # value, an encrypted value (`encrypted`), or a sourced value
  # (`secretSource` + `key`); a variable is masked when its value is not a
  # plain string. A job references sets by name in its `variableSets` list.
  variableSets:
    global:
      variables:
        var1: value1
        var2: value2
        # A variable set's variables take the same value shapes as a job's
        # `variables` (see the `deploy-dev` job): a plain value, an encrypted
        # value, or a sourced value (the latter two are masked).
        region:
          encrypted: <encrypted>          # a masked variable (builtin F-12 ciphertext)
        deploy_token:
          secretSource: prod-secrets      # a masked variable (sourced from a named source)
          key: deploy_token
    dev:
      import:
        - from: git://github.com/blah/blah/variablesets/dev.yaml
        - vault: prod-secrets            # import every value in the vault (masked)
    prod:
      import:
        - from: git://github.com/blah/blah/variablesets/prod.yaml
        - vault: prod-secrets            # import a subset of the vault's keys (masked)
          keys:
            - deploy_token
            - db_password

  # Named secret sources (F-12). A variable's value can be sourced from one by
  # name (with a `key`); a builtin encrypted value uses `encrypted` instead.
  secretSources:
    prod-secrets:
      kind: Vault
      path: /secret/prod
      audience: cdrom

  # The jobs (a name -> job-spec map). Each job's `steps` is a name -> spec map
  # whose entries are `ref`s to shared steps, inline specs (job-local steps), or
  # `importFrom` pulls. A job's `steps` can also be pulled from another file
  # (via the job's `importFrom`); any local steps override the imported steps
  # by name. `needs` names the jobs this job depends on (F-08); it is
  # optional — a job with no `needs` has no dependencies. `variables` is a map
  # of inline variables (a plain value, or a masked value via `encrypted` /
  # `secretSource` + `key`); `variableSets` is a list of named sets to merge in.
  # `environment` names the environment the job targets (F-17); `workerGroup`
  # names the worker group it runs on (defaulting to `spec.workerGroup`).
  jobs:
    build-test:
      # No `needs`: this job has no dependencies.
      steps:
        # Local name differs from the shared step → an explicit `ref`.
        a-build:
          ref: build
        # Local name matches the shared step → an empty entry is shorthand for
        # `ref: <same name>` (it references the shared `unit-test` step).
        unit-test:
        # A job-local step defined inline (not a `ref` to a shared step).
        lint:
          shell:
            command: make
            args: ["lint"]
          workdir: /src
          timeout: 5m
    deploy-dev:
      needs: [build-test]
      environment: dev
      workerGroup: webapp-workers
      variables:
        canary: "true"
        deploy_token:
          secretSource: prod-secrets      # a masked variable (sourced from a named source)
          key: deploy_token
        api_key:
          encrypted: <encrypted>          # a masked variable (builtin F-12 ciphertext)
      variableSets:
        - global
        - dev
      # The whole job is pulled from another file (see "Imports and
      # references"); any fields set here override the imported job's. The
      # local `steps` below override the imported job's steps by name.
      importFrom: git://github.com/blah/blah/jobs/deploy-dev.yaml
      steps:
        # Overrides the `step2` defined in the imported job file.
        step2:
          shell:
            command: |
              echo "deploy to dev (overridden locally)"
    deploy-prod:
      needs: [deploy-dev]
      environment: prod
      workerGroup: prod-workers
      variables: {}
      variableSets:
        - global
        - prod
      concurrencyKeys: ["webapp-prod"]
      steps:
        # Empty entry: shorthand for `ref: step1` (the shared step of the same name).
        step1:
    deploy-super-prod:
      needs: [deploy-prod]
      environment: prod
      # The whole job is pulled from another file; the local fields above
      # (needs, environment, workerGroup) override the imported job's.
      importFrom: git://github.com/blah/blah/somesuperjob.yaml

  # Pipeline-level config lives in the spec alongside the jobs.
  failureMode: all
  concurrencyKeys: ["webapp-{{ .params.environment }}"]
  params:
    - name: image_tag
      description: The image tag to deploy
    - name: environment
      description: The target environment (dev / staging / prod)
    - name: push
      description: Whether to push the built image
      default: "false"
  triggers:
    - name: nightly
      type: cron
      cron: "0 2 * * *"
```

### The referenced files (variable set, step, job)

The files a pipeline pulls in are themselves Kubernetes-style manifests, with a
`kind` that tells the compiler how to read them. Each file holds **exactly one
unit**, and its `spec` is that unit's definition — the same shape as one entry
in the pipeline's corresponding section — so a file contributes a reusable unit
that matches what the pipeline expects:

**A variable set file** (`kind: VariableSet`) — one variable set per file; its
`spec` is the set itself, shaped exactly like an entry under the pipeline's
`spec.variableSets` (an inline `variables` map and/or `import` from files or
vaults). Importing it merges its variables into the named set in the pipeline:

```yaml
# variablesets/dev.yaml
apiVersion: cdrom/alphav1
kind: VariableSet
metadata:
  name: dev
spec:
  variables:
    region: eu-west
    replicas: "2"
  import:
    - vault: prod-secrets
      keys:
        - deploy_token
```

**A step file** (`kind: Step`) — one step per file; its `spec` is the step
definition itself, shaped exactly like an entry under the pipeline's
`spec.steps` (or a job's `steps`). The step takes its name from where the
pipeline references it (the map key), so the file does not name it:

```yaml
# steps/unit-test.yaml
apiVersion: cdrom/alphav1
kind: Step
metadata:
  name: unit-test
spec:
  shell:
    command: make
    args: ["test"]
  workdir: /src
  timeout: 5m
```

**A job file** (`kind: Job`) — one job per file; its `spec` is the job spec
itself, shaped exactly like an entry under the pipeline's `spec.jobs` (its
`steps`, `needs`, `variables`, `variableSets`, `environment`, `workerGroup`,
and deployment options):

```yaml
# jobs/deploy-dev.yaml
apiVersion: cdrom/alphav1
kind: Job
metadata:
  name: deploy-dev
spec:
  steps:
    # Empty entry: shorthand for `ref: step1` (the shared step of the same name).
    step1:
    step2:
      shell:
        command: |
          echo "deploy to dev"
  variables:
    canary: "true"
  variableSets:
    - dev
```

When a pipeline pulls one of these in (a variable set via `import`, a step via
`importFrom`, or a job via `importFrom`), the compiler splices the file's `spec`
into the pipeline's matching unit and validates it as if it were inline: a
`VariableSet` file's variables are merged into the named set that imports them,
and a `Step` / `Job` file's spec becomes the step or job the pipeline names it.
The file's `metadata` is ignored (a file contributes a reusable unit, not a
competing pipeline), and any fields the importing pipeline sets locally override
the imported file's.

### Name-based referencing (not ordinals)

The file references everything by name; the compiler resolves names to the
database's ordinals/ids when it stores the definition. This is what keeps the
file stable under reordering and what makes it reviewable:

- **Steps** are named: a job's `steps` is a name → step-spec map, and a step's
  name (the map key) is its id. A step that wants another step's output
  references it **by that step's name**, not by its position:
  `{{ index .steps "build" .Outputs.version }}`. A job's steps run in the order
  their names appear in the file (the compiler parses the YAML preserving key
  order, so a map still yields a deterministic ordered step list); the compiler
  maps each step name to its 0-based index in that order and rewrites the
  reference to the ordinal form the executor's condition context uses
  (`{{ index .steps 0 }}`). A reference to a step name that is not a *prior*
  step in the order is a validation error (a step cannot read a later step's
  output).
- **Jobs** are named: the `jobs` map's keys are the jobs' names (their stable
  `key`, F-08). A job's `needs` lists the names of the jobs it depends on; it
  is optional — a job with no `needs` has no dependencies. The compiler
  resolves names to the run's job instance ids at run creation (the existing
  F-08 behavior).
- **Workers / groups** are referenced by name (a worker group's name, or a
  worker's host name). The pipeline's default group is `spec.workerGroup`; a
  job's `workerGroup` names the worker group it runs on and overrides the
  default. The compiler stores the resolved name on the job's `target_group`;
  dispatch resolves it to the live workers (F-17).
- **Environments** are referenced by name (F-17); a job's `environment` names
  the environment it targets.
- **Variables** are referenced by name (`{{ .params.<name> }}`, F-10) — and
  this includes **masked** variables: a variable whose value is not a plain
  string (`{encrypted: <ciphertext>}` for a builtin F-12 ciphertext, or
  `{secretSource: <name>, key: <key>}` for a value from a named `secretSources`
  entry) is masked (its value is redacted in logs and hidden in the UI), but it
  is still just a variable and is referenced the same way, as
  `{{ .params.<name> }}`.

The mapping is one-directional and total: every name in the file must resolve to
exactly one target, and the compiler fails the import with an actionable error
naming the unresolvable reference. Once compiled, the stored `JobSpec` is
canonical (ordinal-based) and is what the executor runs — the file's names never
reach the execution target.

### Standard deployment options

The file exposes the platform's standard deployment options using human-friendly
names; the compiler maps each onto the database schema. Field paths are relative
to the manifest root (so `spec.jobs.<name>` is a job under `spec.jobs`). The
mapping is:

| File field | Maps to (DB / feature) |
|------------|------------------------|
| `metadata.name` | the pipeline's name (required) |
| `metadata.labels` / per-resource `labels` | `Label` rows (F-29, renamed from `tags` as part of F-21) |
| `metadata.annotations` | free-form key/value pairs for UI searchability only (never affect behavior) |
| `spec.workerGroup` | the pipeline's default worker group (F-17); a job's `workerGroup` overrides it |
| `spec.failureMode` | `Pipeline.FailureMode` (F-01) |
| `spec.concurrencyKeys` | `Pipeline.ConcurrencyKeys` (F-16, templatable) |
| `spec.params` | `Pipeline.Params` (F-10) |
| `spec.triggers` | `Pipeline.Triggers` (F-09) |
| `spec.jobs.<name>` | `Job.Key` (the name is the key, F-08) |
| `spec.jobs.<name>.needs` (optional) | `Job` `needs` → `DependsOn` (F-08); empty means no dependencies |
| `spec.jobs.<name>.environment` | `Job.Environment` (F-17) |
| `spec.jobs.<name>.workerGroup` | `Job.TargetGroup` (dispatch; the worker group the job runs on, overriding `spec.workerGroup`) |
| `spec.jobs.<name>.variables` | a map of inline variables for that job (F-10); a value is a plain string or a masked value (`{encrypted}` / `{secretSource, key}`, F-12); masked variables are referenced like any other variable (`{{ .params.<name> }}`) |
| `spec.jobs.<name>.variableSets` | a list of named variable sets (declared under `spec.variableSets`) to merge into the job's variables (F-10) |
| `spec.jobs.<name>.importFrom` | pull the whole job from another file; local fields override the imported job's (a local `steps` map overrides the imported job's steps by name) |
| `spec.jobs.<name>.automation` | `Job.Automation` (F-17) |
| `spec.jobs.<name>.approval` | `Job.Approval` (F-17) |
| `spec.jobs.<name>.timeout` | `JobSpec.Timeout` (F-03) |
| `spec.jobs.<name>.retry` (`maxAttempts`, `backoff`) | `JobSpec.Retry` (F-04) |
| `spec.jobs.<name>.ignoreFailed` | `JobSpec.IgnoreFailed` (F-06) |
| `spec.jobs.<name>.stepBarrier` | `JobSpec.StepBarrier` (F-01) |
| `spec.jobs.<name>.concurrencyKeys` | `JobSpec.ConcurrencyKeys` (F-16) |
| `spec.jobs.<name>.steps` | a name → spec map; can be pulled from another file with `importFrom`, with local steps overriding the imported steps by name |
| `spec.jobs.<name>.steps.<step>` | a step (name → spec; see "Step spec shape"); an empty entry is shorthand for `ref: <same name>` |
| `spec.jobs.<name>.steps.<step>.ref` | reference a shared step by name (its full spec is spliced in) |
| `spec.jobs.<name>.steps.<step>.importFrom` | pull the step from another file; local settings override the imported step's |
| `spec.jobs.<name>.steps.<step>.<type>` (e.g. `shell`) | `JobStep.Type` + `JobStep.Params` (F-01); for `shell` the value may be a plain string (shorthand for `{command: <string>}`, run through the OS's default shell) |
| `spec.jobs.<name>.steps.<step>.workdir` / `.env` / `.timeout` | `JobStep` fields (F-01/F-03) |
| `spec.jobs.<name>.steps.<step>.condition` | `JobStep.Condition` (F-06) |
| `spec.jobs.<name>.steps.<step>.ignoreFailed` | `JobStep.IgnoreFailed` (F-06) |
| `spec.jobs.<name>.steps.<step>.once` | `JobStep.Once` (F-01) |

A field the file does not set is left at the database default (the same
defaults the UI path uses), so a minimal file is valid.

**Step spec shape.** A step's value is a map. The common fields (`workdir`,
`env`, `timeout`, `condition`, `ignoreFailed`, `once`) are recognized by name
and lifted onto the `JobStep`. A key that names a registered step handler type
(e.g. `shell`, `token_exchange`, `approval`, `email`, `webhook`) selects that
handler, and its value is the handler's `params`. When no handler-type key is present the step uses the
default handler (`shell`) and the remaining keys are its `params`. So
`shell: {command: …}` (an explicit type) and a bare `{command: …}` (the
default handler) both describe a shell step — the explicit form just names the
type.

The `shell` handler's value may also be a **plain string**: `shell: <string>`
is shorthand for `shell: {command: <string>}` — only the `command` param is
set, nothing else (no `args`, no `shell`). A shell step that sets no `shell`
param (as the string shorthand never does) runs its command through the target
OS's **default shell** (e.g. `sh` on Linux, `cmd` on Windows), so the string
form is the portable way to write a one-line shell command (as `step1` in the
complete example does).

Two fields make a step reusable or external:

- **`ref: <name>`** — the step is a reference to a shared step (declared under
  `spec.steps` or pulled in by another step's `importFrom`); the referenced
  step's full spec is used. When the step's own name matches a shared step's
  name, the `ref` can be omitted: an **empty** step entry (no fields) is
  shorthand for `ref: <same name>`, so a bare `unit-test:` references the shared
  `unit-test` step. A name with no matching shared step is a validation error.
- **`importFrom: <uri>`** — the step is pulled from another file (the compiler
  fetches the file via the source-fetcher). Any settings present on the local
  step **override** the corresponding settings in the imported step, so a step
  can be imported and then tweaked (e.g. change its `workdir` or `timeout`)
  without re-declaring the whole step.

**Masked variables.** There is no separate concept of a secret: every value is
a variable, and a variable is **masked** when its value is not a plain string.
A plain variable is `name: value`; a masked variable is one of two kinds:

- **Builtin** — `name: {encrypted: <ciphertext>}`: a value encrypted with the
  API's secret store (F-12). The ciphertext is what `POST /api/secrets/encrypt`
  returns; the API decrypts it at dispatch and hands the plaintext to the target.
- **Sourced** — `name: {secretSource: <name>, key: <key>}`: a value
  fetched from the named `secretSources` entry (F-12) at the given `key`. The
  API resolves the source to a concrete value at dispatch and hands it to the
  target.

All variables — masked or not — are referenced the same way, as
`{{ .params.<name> }}`, so a step does not care where a value came from or
whether it is masked. The platform tracks which variables are masked (the
compiler records them) and redacts their values in logs and hides them in the
UI; the plaintext of a masked variable is never persisted or returned to the UI.

```yaml
spec:
  jobs:
    deploy-dev:
      variables:
        canary: "true"                              # a plain variable
        api_key:
          encrypted: <encrypted>                    # a masked variable (builtin F-12 ciphertext)
        deploy_token:
          secretSource: prod-secrets                # a masked variable (from a named secret source)
          key: deploy_token
```

### Reusable units and job reuse

Three kinds of unit are reusable and importable: **variable sets**, **steps**,
and **jobs**.

- **Variable sets** (`variableSets`) are named groups of variables. Each has an
  inline `variables` map and/or an `import` list of external files or vaults —
  a set may have both, and its variables are the union of the inline `variables`
  and the imported ones (the inline ones win on a key collision). The compiler
  fetches each file via the source-fetcher and merges them. A job references
  sets by name in its `variableSets`
  list, and carries its own inline variables in a `variables` map. At compile
  time the compiler resolves each named set (fetching its files) and merges the
  sets' variables with the job's inline `variables` into the run's parameters
  for that job's spec interpolation (F-10), with the job's inline variables
  winning over a set's on a key collision.
- **Steps** are named step specs, and they live in two places: **shared** steps
  under `spec.steps` (a name → step-spec map), and **job-local** steps under a
  job's `steps` map. Each entry is inline, a `ref` to a shared step, or an
  `importFrom` pull from another file (see "Imports and references"). A `ref` /
  `importFrom` is expanded in place (its full step spec) at compile time, so a
  job can mix shared and local steps freely. A job's `steps` can also be pulled
  in from another file with `importFrom`; any steps the job defines locally
  override the imported steps by name (the imported steps are the base).
- **Jobs** are named job specs under `spec.jobs` (a `steps` map, an optional
  `needs` list, a `variables` map, a `variableSets` list, an `environment`, a
  `workerGroup`, and the standard deployment options). A job can be pulled in
  from another file with `importFrom` (any fields set on the local job override
  the imported job's, including its `steps` — a local `steps` map overrides the
  imported job's steps by name).

**Job reuse across environments.** Because steps are named and shared, each
environment is its own job that reuses the same shared steps (via `ref`) and
differs only by the variable sets it references (via `variableSets`) and a few
job-local steps. This is the "each environment is very similar and only varies
by variables" case: `deploy-dev` and `deploy-prod` both `ref` `step1` (the
shared step) and differ only by the variable sets in their `variableSets` list
and their local steps. The DAG (`needs`) is expressed over the jobs' names (a
job with no `needs` has no dependencies), so a promotion chain (`deploy-dev →
deploy-prod`) is a normal F-08 dependency.

### Imports and references

Reusable units are pulled in from other files with **explicit fields** (rather
than YAML tags). A field carries either a locator (a source URI or a path
relative to the current file) or a name (a reference to a unit already declared
in the file):

- **`importFrom: <uri>`** (on a step, a job's `steps`, or a job) — pull a step
  definition, a job's steps, or a whole job from another file. On a step, the
  referenced file is a `Step` manifest holding a single step spec; any settings
  present on the local step override the imported step's. On a
  job's `steps`, it pulls the steps from the referenced file as the base; any
  steps the job defines locally override the imported steps by name (the job's
  other fields — `needs`, `variables`, `variableSets`, `environment`,
  `workerGroup`, … — stay local). On a job, it pulls the whole job spec; any
  fields set on the local job override the imported job's, including its
  `steps` (a local `steps` map overrides the imported job's steps by name).
  Example:
  `unit-test: {importFrom: git://github.com/blah/blah/steps/unit-test.yaml}`.
- **`ref: <name>`** (on a step) — reference a step already defined in
  `spec.steps` (or pulled in by another step's `importFrom`) by name. When the
  step's own name matches the referenced step's, the `ref` can be omitted: an
  empty entry (`step1:`) is shorthand for `ref: step1`.
- **`import: [{from: <uri>}, {vault: <name>}, {vault: <name>, keys: […]}, …]`**
  (on a variable set) — pull one or more external files **or vaults** into a
  variable set under `spec.variableSets`. A `{from}` entry fetches a
  `VariableSet` file and merges its variables into the set; a `{vault}` entry
  imports every value in the named `secretSources` entry (as masked variables),
  and a `{vault, keys}` entry imports a subset of its keys. The set's
  variables are the union of its inline `variables` and the imported variables.
  Example: `dev: {import: [{from: git://…/dev.yaml}, {vault: prod-secrets}]}`.
- **`variableSets: [<name>, …]`** (on a job) — reference named variable sets
  (declared under `spec.variableSets`) to merge into the job's variables.
  Example: `variableSets: [global, dev]`.
- The same pattern extends to the other named resources: a secret source can be
  pulled in from another file with `importFrom`.

A locator is resolved against the pipeline's source, exactly like the main file:
for a `git` source, relative to the file's path in the repo (the fetcher fetches
the referenced file from the same repo/branch); for an `http` source, relative
to the file's URL (the fetcher resolves it against the source's base URL); for a
`filesystem` source, relative to the file's directory on the fetcher's host. The
source-fetcher fetches each referenced file and the compiler splices the fetched
unit in place (or merges a variable set's variables into the job's `variables`).
The rules:

- A unit name is unique across the merged namespace (the file's own units plus
  all imported units); a collision is a validation error.
- Imports may be transitive (an imported file may itself use `importFrom` /
  `import` to pull in others); a cycle of imports is a validation error.
- A `ref` must name a step that is defined (inline, shared, or imported) and a
  `variableSets` entry must name a variable set declared under
  `spec.variableSets`; otherwise the import fails with an actionable error
  naming the missing name.
- Only the referenced unit is spliced in; an imported file's `kind` tells the
  compiler how to read it (a `Step` / `VariableSet` / `Job` file contributes a
  reusable unit, not a competing pipeline), and its `metadata` is ignored.

This is what lets a team keep a `shared/` directory of step, variable set, and
job definitions that many pipelines pull in by `importFrom` (steps and jobs) or
`import` (variable sets), and keep each environment's pipeline file small.

### Secret sources (and the named-resource pattern)

`secretSources` is a section of the `spec` of named, referencable secret
sources. Each entry has a `kind` (a secret-store kind, F-12 — `Vault` /
`OpenBao` are reserved kinds; the built-in AES store is the default when no
source is named), a `path` (the source path), and an `audience` (the token
audience used to fetch from the source). A variable references a source **by
name** with a `key` selecting the value within the source (or a variable set
imports the source's values with `import: [{vault: <name>}]`):

```yaml
spec:
  secretSources:
    prod-secrets:
      kind: Vault
      path: /secret/prod
      audience: cdrom

  jobs:
    deploy:
      variables:
        deploy_token:
          secretSource: prod-secrets   # this value comes from this source (masked)
          key: deploy_token
      steps:
        publish:
          shell:
            command: deploy
            args: ["--token", "{{ .params.deploy_token }}"]
```

The source's `kind`/`path`/`audience` are stored opaquely on the pipeline (like
a secret declaration, F-12); the API resolves the named source to a concrete
value at dispatch and hands the resolved value to the target. The variable is
referenced as `{{ .params.<name> }}` (like any other variable) and, being
masked, its value is redacted in logs and hidden in the UI.

**The pattern generalizes.** `secretSources` is the template for how the file
declares any named, referencable resource: a `spec` section that is a map of
`name → settings`, referenced by name elsewhere in the file. `variableSets`,
`steps`, and `jobs` all follow the same shape, so a reader who knows one section
knows them all. New kinds of named resource (e.g. a future `artifacts` or
`notifiers` section) slot into the same pattern without changing the referencing
rules. Reuse and import are expressed the same way everywhere: a `ref` to a
named unit, or an `importFrom` / `import` pull from another file.

---

## Externally-sourced pipelines

A pipeline's definition can live in an external source instead of (or alongside)
the UI. An **externally-sourced** pipeline is one whose `PipelineSource` points
at a source of one of three kinds: a **git** repository, an **HTTP/HTTPS**
endpoint, or a file on the **local filesystem**. All three are fetched by the
source-fetcher service (below) and flow through the same sync routine.

### The source model

- **`PipelineSource`** — where a pipeline is pulled from. It has a `kind`
  (`git` / `http` / `filesystem`) plus the kind's locator, a `schedule` (a cron
  expression for the scheduled refresh; empty means no scheduled refresh), and a
  `credential` (the name of the `SourceCredential` used to authenticate; empty
  means the source is public / needs no credential):
  - `git` — `url` (the git remote), `branch` (the ref to track; default
    `main`), and `path` (the path of the definition file within the repo).
  - `http` — `url` (the endpoint that serves the definition file, e.g. an
    object-store URL or a raw-file endpoint). The fetcher does a GET and uses
    the response body as the file. An optional `headers` map (e.g. an
    `Authorization` header) can be set on the source for endpoints that need it
    in-band; a named `credential` is the preferred way to supply a token.
  - `filesystem` — `path` (an absolute path to the definition file on the
    source-fetcher's host). The file is read directly; no network is involved.
    This is the kind for a definition that is mounted into the fetcher's pod
    (e.g. a ConfigMap or a volume) and is "pulled" by re-reading the file.
- **`PipelineSourceState`** — the last sync's bookkeeping: `last_synced_at`,
  `last_version` (the source's version identifier — the git commit hash, an
  HTTP `ETag`/`Last-Modified`, or the file's mtime — so a no-change can be
  detected cheaply), `file_hash` (the content hash of the file at that version),
  and `last_error` (set when a sync fails, cleared on the next success).

### The source-fetcher service

The actual fetch is done by a dedicated **source-fetcher** service — a separate
process (`cmd/pipelinesource`, logic in `internal/services/pipelinesource`), in
the same shape as the other services (`db`, `scheduler`, `artifacts`, `idp`):

- **A standalone gRPC server.** It exposes one RPC, `FetchSource`, which takes a
  source locator (kind + url/branch/path + optional headers) and a credential
  (the plaintext secret material, when one is needed) and returns the file's
  content, its version identifier, and its content hash. The fetcher is
  **stateless**: it holds no pipeline definitions, no credentials, and no
  encryption key at rest. It only ever performs a single fetch per call.
- **Dialed only by the API, over gRPC.** The API is the only caller (the same
  topology rule as the other services: the API talks to everything else). The
  connection uses **mutual TLS** when a TLS CA is configured (`TLSConfig` in
  `internal/config`; creds built in `internal/grpcutil`) and falls back to
  plaintext without one (local dev) — exactly the convention the other services
  follow. The fetcher's address comes from config / env
  (`CDROM_PIPELINE_SOURCE_ADDR`, default `127.0.0.1:7107`), and it serves its
  own `/metrics` + `/healthz` on a dedicated metrics port (F-20).
- **Why a separate process.** Keeping the fetch out of the API and scheduler
  means neither needs git installed, outbound network egress, or the source's
  filesystem. The fetcher can be run with a network policy that allows only the
  repos/endpoints it must reach, and it can be scaled or replaced independently
  of the control plane. It also keeps the API's process free of long-running
  git operations.
- **Protos.** `proto/cdrom/pipelinesource/v1/` defines `FetchSource` (and the
  `SourceLocator` / `SourceCredential` / `FetchedFile` messages); generated code
  is committed under `internal/gen/` (`make proto`). The service impl lives in
  `internal/services/pipelinesource/server.go` and implements the generated
  `*Server` interface (embedding `Unimplemented*Server`), per the gRPC
  conventions.

### The stored YAML and its hash

When an externally-sourced pipeline is synced, the API asks the source-fetcher
to fetch the definition file at the source's locator and stores, on the
`Pipeline` row:

- **`source_file`** — the raw YAML text, verbatim (so it is retrievable through
  the API and is the exact input the compiler ran).
- **`source_hash`** — the SHA-256 of `source_file` (hex). This is the change
  detector: two syncs that produce the same hash have not changed the
  definition.

The hash is what makes refresh cheap and idempotent. On a sync the API computes
the hash of the freshly-fetched file (the fetcher returns it) and compares it to
the stored `source_hash`:

- **Changed** (hash differs) — the file is re-compiled and the pipeline is
  re-imported (create or update), which bumps the pipeline's version (F-11) and
  stores the new `source_file` / `source_hash` and the source state's
  `last_version`. A run started after the change uses the new definition.
- **Unchanged** (hash matches) — the sync is a no-op: no recompile, no version
  bump, no re-dispatch. Only `last_synced_at` is updated.

This means a scheduled refresh that finds no change is free, and a run-kickoff
refresh that finds no change does not perturb an in-flight pipeline.

### Refresh triggers

An externally-sourced pipeline is refreshed in three ways:

1. **On a schedule** — the API's **source-refresh loop** (leader-gated via a
   named lease on the Database service, like the scheduler's loops, F-23) wakes
   on each source's `schedule` cron and syncs it. A source with no schedule is
   not polled.
2. **When a run is kicked off** — just before a run of an externally-sourced
   pipeline is created (manual, cron, webhook, or event trigger), the API
   performs a **refresh-on-run**: it fetches the source (via the fetcher) and,
   if the file changed, compiles it so the run executes the latest reviewed
   definition. This guarantees "what I just kicked off is what is in the source
   right now" without waiting for the next scheduled tick. A refresh-on-run that
   fails (e.g. the source is unreachable) does **not** block the run: the run
   proceeds on the last successfully-synced definition, and the failure is
   recorded on the source state and surfaced to the UI.
3. **Manually** — `POST /api/pipelines/{id}/source/refresh` forces an
   immediate sync (regardless of schedule) and applies the result. This is the
   "I just pushed, pull it now" path.

All three go through the same sync routine (fetch via the source-fetcher →
hash → compare → recompile if changed), so they are behaviorally identical; only
the trigger differs. A sync is serialized per pipeline (a source is not synced
concurrently by two triggers), so a scheduled tick and a run-kickoff refresh
racing cannot double-apply a change.

### Retrieval through the API

The stored source is readable through the API:

- `GET /api/pipelines/{id}/source` — the `PipelineSource` (kind, url/branch/path,
  schedule, credential name) plus the `PipelineSourceState` (last sync time,
  last version, file hash, last error) and the stored `source_file` (the raw
  YAML). This is how a user sees "what definition is currently loaded and where
  it came from" without checking out the source.
- `GET /api/pipelines/{id}` — the pipeline's **compiled** definition (the same
  shape the UI editor shows), so the UI and the file are two views of the same
  model.

### Import and export

- **Import** — `POST /api/pipelines` (or `PUT /api/pipelines/{id}`) accepts a
  definition in the file format (YAML or its JSON equivalent). The API runs it
  through the compiler (validate + map to the schema) and creates/updates the
  pipeline. A pipeline becomes externally-sourced when a `PipelineSource` is
  attached to it through the source API — the source is a database concept, not
  part of the file.
- **Export** — `GET /api/pipelines/{id}/export` renders the pipeline's current
  compiled definition back into the file format. For an externally-sourced
  pipeline the export reproduces a file equivalent to the stored `source_file`;
  for a UI-authored pipeline it synthesizes a file from the compiled model. The
  round-trip property (export → import → same compiled definition) holds because
  the compiler is a pure function of the file and the exporter is its inverse on
  the fields the file can express.

---

## Source credentials

Fetching a private source (a private git repository, or an HTTP/HTTPS endpoint
that requires a token) requires a credential. Credentials are **stored in the
database** (not in configuration), **matched to a source by a regex**, and
**updatable through the API**. They cover **git over SSH**, **git over
HTTP/HTTPS**, and **HTTP/HTTPS endpoint** authentication. The source-fetcher
never stores a credential: the API decrypts it and passes the plaintext to the
fetcher for a single fetch.

### The credential model

- **`SourceCredential`** — a named credential:
  - `name` (unique; the value a `PipelineSource.credential` references).
  - `kind` — `git-ssh`, `git-https`, or `http`.
  - `pattern` — a regular expression matched against a source's `url`. When a
    fetch needs a credential, the API matches the source's `url` against every
    credential's `pattern`; the **most specific (longest) matching pattern**
    wins (the same resolution rule as F-16's concurrency-limit rules). A source
    whose `url` matches no credential uses no credential (a public source). A
    source that names a `credential` by name uses that specific credential (the
    name is an explicit override of the regex match).
  - The **secret material**, by kind:
    - `git-https` — `username` (may be empty for token-only auth) and `token`
      (a personal access token or bot password). The fetcher authenticates the
      git clone/fetch as `https://<username>:<token>@host/...` (the token is
      never logged).
    - `git-ssh` — `privateKey` (the PEM-encoded private key) and `hostKey` (the
      expected host public key, for host-key verification so a fetch is not
      vulnerable to a MITM). The fetcher uses the key for the SSH handshake and
      verifies the server's host key against `hostKey`.
    - `http` — `token` (a bearer token or API key) and optional `header` (the
      header the token is sent in; default `Authorization: Bearer <token>`). The
      fetcher sends the header on its GET of the endpoint.
  - `description` (optional, human-readable).

The secret material (`token` for `git-https` / `http`, `privateKey` and
`hostKey` for `git-ssh`) is **encrypted with the API's secret store** (F-12)
before it is stored, exactly like a masked variable's value: the API encrypts
on create/update and the database service stores the ciphertext opaquely (it
never sees the key). The plaintext is never returned to the UI — a credential read
back through the API shows its `name`, `kind`, `pattern`, `description`, and
(for `git-https`) `username`, but the token / private key are redacted.
Updating a credential re-encrypts the new secret material; the existing one is
replaced.

### Matching and use

When a source is synced:

1. If the source names a `credential`, that credential is used.
2. Otherwise the source's `url` is matched against every credential's `pattern`;
   the longest matching pattern's credential is used.
3. If neither applies, the fetch runs unauthenticated (a public source).

The chosen credential's secret material is decrypted by the API for that single
fetch and handed to the source-fetcher (over the gRPC call); it is not persisted
anywhere else (the fetcher is stateless) and is not logged. A credential that
fails to authenticate (a rotated token, a revoked key) surfaces as a sync
failure on the source state (`last_error`) and is visible in the UI, so a stale
credential is diagnosable without a repo checkout.

### API surface

- `GET /api/source-credentials` — list credentials (secret material redacted).
- `POST /api/source-credentials` — create a credential (the body carries the
  plaintext secret material, which the API encrypts before storing).
- `GET /api/source-credentials/{name}` — fetch one (redacted).
- `PUT /api/source-credentials/{name}` — update (re-encrypts the secret
  material).
- `DELETE /api/source-credentials/{name}` — delete (rejected if a source still
  references it by name).

Listing requires `source-credentials.can-view`; create/update/delete require
`source-credentials.can-manage` (admins have both). A change publishes a
`source_credential` WS event so the UI stays current. Because the secret
material is encrypted at rest and redacted on read, a holder of `can-view` can
see which sources a credential covers (its `pattern`) but not the token or key
itself.

---

## Design decisions (folded into AGENTS.md / Architecture.md)

- **The file is a Kubernetes-style manifest.** Every definition has an
  `apiVersion` (currently `cdrom/alphav1`), a `kind` (`Pipeline` /
  `VariableSet` / `Step` / `Job`), a `metadata` block (`name`, `description`,
  `labels`, `annotations`), and a `spec` holding the rest. A referenced file
  holds exactly one unit, so its `spec` is that unit's definition (a variable
  set, a step, or a job), not a map of them. The compiler dispatches on
  `kind` to interpret a document (and an imported file's `kind` tells it how to
  splice the file in), and `apiVersion` is bumped as the format evolves through
  the project's lifecycle. `metadata.annotations` is free-form key/value data
  used only for UI searchability (it never affects behavior). As part of F-21
  the project's `tags` are renamed to `labels` (the F-29 concept) across the
  platform — pipelines, jobs, environments, worker groups, and workers — so the
  manifest's `metadata.labels` is the same `Label` set the rest of the platform
  uses.
- **The file is not the schema; the compiler is the bridge.** The YAML is a
  human-authored, name-based format. A dedicated compiler (`internal/pipelinefile`)
  validates a definition and maps it onto the database schema — resolving step
  ids to step indices, job keys to job ids, worker/group names to groups, and
  named sources/variables to their stored forms — producing the canonical
  `JobSpec` / `JobDefinitionSnapshot` the executor runs. The file's names never
  reach an execution target; the stored spec is ordinal-based. Validation is
  shared with F-08 (the job DAG: unique keys, known `needs`, no cycle) and F-10
  (parameters: declared, unique, and resolvable).
- **Everything is referenced by name, never by ordinal.** Steps by their id,
  jobs by their key, workers by group/host name, environments by name, and
  variables (masked or not) by name. The compiler is the single place a name
  becomes an ordinal or id. This is what makes the file stable under reordering
  and reviewable in a PR, and it is the rule every named-resource section
  follows.
- **Named resources use one consistent shape.** A `spec` section that is a map
  of `name → settings`, referenced by name elsewhere. `secretSources` is the
  canonical example; `variableSets`, `steps`, and `jobs` follow it, and any
  future named resource does too. A reader who knows one section knows them all.
  Mapping keys are camelCase (`concurrencyKeys`, `variableSets`, `importFrom`,
  `secretSources`, …).
- **The raw YAML + hash are stored in the database.** An externally-sourced
  pipeline stores `source_file` (the verbatim YAML) and `source_hash` (its
  SHA-256) on the `Pipeline` row, plus a `PipelineSource` (where it came from:
  a `kind` of `git` / `http` / `filesystem` and its locator) and a
  `PipelineSourceState` (last sync / version / error). The hash is the change
  detector: a sync that yields the same hash is a no-op (no recompile, no
  version bump); a changed hash re-imports and bumps the version (F-11). The
  stored file is retrievable through the API (`GET /api/pipelines/{id}/source`).
- **The fetch is a separate process.** The actual fetch (git clone/fetch, HTTP
  GET, or a local file read) is performed by a dedicated **source-fetcher**
  service (`cmd/pipelinesource` / `internal/services/pipelinesource`) — a
  standalone gRPC server in the same shape as the other services. It is
  **stateless** (no definitions, credentials, or key at rest) and is dialed
  **only by the API** over gRPC: mutual TLS when a CA is configured, plaintext
  otherwise (the convention in `internal/grpcutil`). This keeps git, network
  egress, and the source's filesystem out of the API and scheduler, and lets the
  fetcher be network-policy'd to only the sources it must reach.
- **Refresh is one routine, three triggers.** A single sync routine (fetch via
  the source-fetcher → hash → compare → recompile if changed) is driven by (1)
  the API's leader-gated source-refresh loop (a named lease on the Database
  service, like the scheduler's loops, F-23) on each source's cron schedule, (2)
  a refresh-on-run hook just before a run of an externally-sourced pipeline is
  created, and (3) a manual `POST /api/pipelines/{id}/source/refresh`. A sync is
  serialized per pipeline so the triggers cannot race. A refresh-on-run that
  fails does not block the run (it proceeds on the last good definition and
  records the error); a scheduled or manual failure is recorded on the source
  state.
- **Reusable units and job reuse.** Variable sets, steps, and jobs are named,
  reusable units. Steps live in two places — shared steps under `spec.steps`
  and job-local steps under a job's `steps` map — and each entry is inline, a
  `ref` to a shared step, an `importFrom` pull from another file, or empty — an
  empty entry is shorthand for a `ref` to the shared step of the same name (all
  explicit fields, not YAML tags). Variable sets are inline `variables` maps and/or
  `import` from external files or vaults; a job references sets by name in its
  `variableSets` list and carries its own inline variables in a `variables`
  map. A job can itself be pulled in with `importFrom` (local fields override
  the imported job's). A referenced file holds exactly one unit — a
  `VariableSet`, `Step`, or `Job` manifest whose `spec` is that unit's
  definition — so the compiler splices the referenced units in place,
  fetches each referenced file via the source-fetcher, and merges everything
  into one namespace (unique names; transitive imports; no import cycles).
  Because steps are named and shared, each environment is its own job that
  reuses the same shared steps (via `ref`) and differs only by the variable
  sets it references (via `variableSets`) and a few local steps, so "deploy to
  dev/staging/prod" is shared steps plus per-environment jobs and a `needs`
  chain (a job with no `needs` has no dependencies), not copied jobs.
- **Masked variables, not a separate concept of a secret.** Every value is a
  variable: a plain variable is `name: value`, and a variable is **masked**
  when its value is a map — `{encrypted: <ciphertext>}` for a builtin F-12
  ciphertext or `{secretSource: <name>, key: <key>}` for a value from a named
  `secretSources` entry. A variable set can import a vault's values with
  `import: [{vault}]` or `import: [{vault, keys}]`. This keeps one uniform
  `variables` shape (F-10) for plain and masked values alike, and masked
  variables are referenced the same way as any other variable
  (`{{ .params.<name> }}`); the platform tracks which variables are masked
  (redacting their values in logs and hiding them in the UI), and the
  `secretSources` section (renamed from `secretVaults`) is the named-resource
  section that backs the sourced kind.
- **Standard deployment options map onto the schema.** The file's deployment
  fields (timeout, retry, failure mode, concurrency keys, environment,
  automation, approval, needs, triggers, params, labels) map one-to-one onto the
  existing database fields (F-01…F-17, F-29); an unset field falls back to the
  database default, so a minimal file is valid.
- **Source credentials are DB-stored, regex-matched, and API-managed.** A
  `SourceCredential` (kind `git-ssh` / `git-https` / `http`, a `pattern` regex
  over the source URL, and encrypted secret material) is stored in the database
  (secret material encrypted with the F-12 store, opaque to the database
  service) and managed through the API (CRUD, gated on
  `source-credentials.can-view` / `can-manage`). A source is matched to a
  credential by the longest matching `pattern` (or an explicit `credential` name
  override); the credential's secret is decrypted by the API for a single fetch
  and handed to the source-fetcher (which stores nothing) — it is never logged
  or returned to the UI. `git-ssh` credentials carry a private key + host key
  (host-key verified); `git-https` and `http` credentials carry a token (and, for
  `git-https`, an optional username).
- **New fields / RPCs / endpoints.** `PipelineSource`, `PipelineSourceState`,
  and `SourceCredential` models (registered in `models.All()`); `source_file` /
  `source_hash` / `source` on `Pipeline` (db); source + credential CRUD and
  `SyncPipelineSource` / `RefreshPipelineSource` RPCs (db); a new
  `proto/cdrom/pipelinesource/v1/` gRPC API (`FetchSource`) with generated code
  in `internal/gen/` (`make proto`) and a `cmd/pipelinesource` +
  `internal/services/pipelinesource` service (the fetcher, dialed by the API
  over gRPC/mTLS); `GET/POST /api/pipelines[/{id}]/source[/{action}]`,
  `GET /api/pipelines/{id}/export`, and `GET/POST/PUT/DELETE
  /api/source-credentials[/{name}]` (api); a `source-refresh` background loop
  (a named lease on the Database service) and a refresh-on-run hook (api); the
  fetcher's git / HTTP / filesystem clients (a single file at a ref / URL / path,
  authenticated by a `SourceCredential`). File-format fields that are new to the
  schema: the Kubernetes-style manifest envelope (`apiVersion` / `kind` /
  `metadata` / `spec`, with `metadata.labels` and `metadata.annotations`),
  camelCase mapping keys, a `spec.workerGroup` (the default group a job's
  `workerGroup` overrides), a `spec.secretSources` section (renamed from
  `secretVaults`), masked variables (`{encrypted}` / `{secretSource, key}`
  values, F-12, referenced like any other variable), vault imports in variable
  sets (`import: [{vault}]` / `[{vault, keys}]`), `importFrom` on a step, a
  job's `steps`, or a whole job,
  and a plain-string `shell` value (shorthand for `{command: <string>}`, run
  through the OS's default shell); the rest of the file's fields
  (`environment`, `concurrencyKeys`, `workerGroup`, `needs`, `variables`,
  `variableSets`, …) map onto existing `Pipeline` / `Job` / `JobSpec` fields.
  F-21 also renames the platform's `tags` to `labels` (the F-29 concept)
  across the docs.
