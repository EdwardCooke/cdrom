# F-29 · Tagging

> Phase 4 — Organization & metadata · [Feature index](../Features.md)

**What.** **Tags** — free-form **key/value pairs** that can be attached to the
platform's primary resources: **pipelines**, **jobs in a pipeline**,
**environments**, **worker groups**, and **workers**. When a job is kicked off
(a run is created and its job instances are dispatched), the job's run collects
the tags from **every** resource in its context — the pipeline, the job, the
environment it targets, the worker group it targets, and the worker that runs
it — **combines** them into a single set, and **stores** the result on the job
run. The combined set is what the UI, dashboards, and downstream tooling see
for a run: a flat, denormalized snapshot of "everything this run is tagged
with".

Tags are **metadata, not behavior**: they never gate, route, or serialize work
(contrast with F-16 concurrency keys, which are locks, or F-17 environments,
which carry policy). They exist so a run can be searched, filtered, grouped,
and reported on by arbitrary labels — `team=backend`, `cost-center=1234`,
`region=eu-west`, `release=train-2026-10`, etc.

**Why.** As a CD platform grows, operators need to slice their runs by
dimensions that don't fit the fixed schema (pipeline, environment, group,
trigger). Today the only free-form metadata a run carries is its `params`
(F-10), which are *inputs to the job*, not *labels on the run*. Tags give the
platform a first-class, uniform labeling mechanism that:

- **composes** — a run's label set is the union of the labels on every
  resource it touches, so tagging a `prod` environment or a `backend` worker
  group automatically labels every run that goes through it, with no per-pipeline
  edits;
- **is denormalized onto the run** — a run records the tags that were active
  when it started (a snapshot, like F-11's version binding), so relabeling a
  pipeline or environment later never rewrites history;
- **is uniform** — one `map[string]string` shape on every resource, one
  combining rule, one place (the job run) to read the result.

**Scope.**
- `internal/models` — a `Tags` field (`map[string]string`, in-memory only,
  `gorm:"-"`) on `Pipeline`, `PipelineVersion`, `JobDefinitionSnapshot`, `Job`,
  `Environment`, `Worker`, and `JobExecution`; a new `WorkerGroup` entity
  (`name`, `description`, `tags`) — worker groups are currently implicit (a
  worker's `group` string) and become first-class, taggable rows; and a new
  `Tag` entity — the **normalized** storage for every resource's tags: one row
  per key/value pair, keyed by `owner_type` + `owner_id` + `key` (a composite
  unique index) with a separate index on `(key, value)`. The `owner_type`
  distinguishes the resource kind (`pipeline`, `pipeline_version`, `job`,
  `job_execution`, `environment`, `worker`, `worker_group`) so a row's
  `(owner_type, owner_id)` identifies exactly one parent. All registered in
  `models.All()`. The `Tag` table is what makes tags **searchable by key and/or
  value** with a plain indexed SQL query — a JSON column cannot be queried that
  way. The parent's `Tags` map is populated from the `Tag` table on load and
  written back on save by the Database service.
- proto (`cdrom.db.v1`) — `tags` on the `Pipeline`, `PipelineVersion`,
  `JobDefinition`, `Job`, `JobExecution`, `Environment`, and `Worker` messages
  and on the `CreatePipeline`/`UpdatePipeline`, `CreateEnvironment`/
  `UpdateEnvironment`, and `RegisterWorker` requests; new `WorkerGroup`
  message + `CreateWorkerGroup`/`GetWorkerGroup`/`ListWorkerGroups`/
  `UpdateWorkerGroup`/`DeleteWorkerGroup` RPCs. (`cdrom.api.v1`) — `tags` on
  `Worker` and `RegisterWorkerRequest`. (`cdrom.scheduler.v1`) — `tags` on
  `Job` (the combined set, so the scheduler and UI see it).
- `internal/services/database` — persist tags on every resource; **worker-group
  CRUD**; in `createRunAndInstances` compute each job instance's combined tags
  (pipeline + job + environment + worker group) and store them on the instance;
  in `StartJobExecution` merge the assigned worker's tags into the execution's
  tag set.
- `internal/api` — pipeline/job tags flow through the existing
  `POST/PUT /api/pipelines` body; environment tags through the existing
  `POST/PUT /api/environments[/{name}]`; **new worker-group endpoints**
  (`GET /api/worker-groups`, `GET /api/worker-groups/{name}`,
  `PUT /api/worker-groups/{name}`, `DELETE /api/worker-groups/{name}`); worker
  tags are declared at registration (the gRPC `RegisterWorker` carries them).
- `internal/worker` + `cmd/worker` + `internal/config` — a worker declares its
  tags at registration (configurable, like its environments) via
  `CDROM_WORKER_TAGS` / the config file's `worker_tags`.
- `internal/authz` — `worker-groups.can-view` / `worker-groups.can-manage`
  (new); environment tags reuse `environments.can-view` / `can-manage`,
  pipeline/job tags reuse `pipelines.can-view` / `can-edit`.
- UI — a tags editor on the pipeline, job, environment, and worker-group forms;
  a tags display on the run/job detail view (the combined set).

**The tag model.**

- **A tag is a key/value pair.** A resource's tags are a `map[string]string`
  (a set of key/value pairs; a key is unique within a resource, a value is a
  free-form string, possibly empty). Keys must be non-empty; values may be
  anything. The set is stored **normalized in the `Tag` table** — one row per
  key/value pair, keyed by `owner_type` + `owner_id` + `key` — rather than as a
  JSON document on the parent row. This is what makes a resource searchable and
  filterable by tag key and/or value with a plain indexed SQL query (a JSON
  column cannot be queried that way). The parent's in-memory `Tags` map
  (`gorm:"-"`) is the API/UI-facing shape; the Database service loads it from
  the `Tag` table and writes it back on save.
- **Every resource carries the same shape.** `Pipeline`, a pipeline's job
  definitions, `Environment`, `WorkerGroup`, and `Worker` each have a
  `Tags` map (backed by `Tag` rows of the matching `owner_type`). A resource
  with no tags has an empty (or absent) map and no `Tag` rows.

**Where tags are set.**

- **Pipelines and jobs** — set in the pipeline's definition, through the
  existing `POST/PUT /api/pipelines` body: the pipeline carries a `tags` map,
  and each job definition carries its own `tags` map. They are captured in the
  pipeline's version snapshots (F-11) so a run bound to a version uses that
  version's tags.
- **Environments** — set through the existing `POST/PUT
  /api/environments[/{name}]` body (`tags` field), stored in the database
  (the `Environment` table, owned by the Database service). This is the
  "from the upcoming UI" path: the UI edits an environment's tags and the API
  persists them.
- **Worker groups** — a worker group is a first-class, named, taggable row
  (the `WorkerGroup` table). It is managed through **new API endpoints**
  (`GET/PUT/DELETE /api/worker-groups[/{name}]`), again the "from the upcoming
  UI" path: the UI sets a group's tags and the API persists them. A worker
  group that has never been created has no tags (an empty set); the UI (or an
  admin) creates it and sets its tags.
- **Workers** — a worker is a long-lived process, so its tags are part of its
  identity, declared **at registration** (configurable, like its environments
  membership, F-17): `CDROM_WORKER_TAGS` (a `key=value,key2=value2` list) or
  the config file's `worker_tags`. The worker sends them on `RegisterWorker`;
  the API persists them (via the Database service). Re-registration (e.g. after
  a restart) updates them. Workers are *not* tagged from the UI — a worker's
  tags are set where the worker is deployed.

**Combining: "take all of the tags from everything".**

When a job is kicked off, its run's tag set is the **union** of the tags on
every resource in its context. The sources, from least to most specific, are:

1. **Pipeline** — the pipeline's tags (the same for every job in the run).
2. **Job** — the job's own tags (its definition's tags).
3. **Environment** — the tags of the environment the job targets (F-17);
   absent when the job targets a raw group with no environment.
4. **Worker group** — the tags of the worker group the job targets; absent
   when the job runs on an ephemeral agent (no group).
5. **Worker** — the tags of the worker that runs the job; present only once a
   specific worker is assigned (at dispatch).

**Precedence on a key collision.** When two sources set the same key, the
**more specific** source wins: `worker > worker group > environment > job >
pipeline`. So if the pipeline sets `team=platform` and the job sets
`team=backend`, the run carries `team=backend`; if the `prod` environment sets
`release=train-2026-10`, it overrides a pipeline-level `release` tag. Keys set
by only one source pass through unchanged.

**When the combination is computed and stored.**

- **At run creation** (the Database service's `createRunAndInstances`, F-07):
  each job instance's `Tags` is set to the union of the **four static
  sources** — pipeline, job, environment (if targeted), and worker group (if
  targeted) — with the precedence above. These are known the moment the
  instance is created (the job's environment and target group are on its
  definition), so the base combined set is stored on the instance then. This is
  the run's tag snapshot: it records the tags that were active when the run
  started, and is not rewritten if a resource is later relabeled.
- **At dispatch** (the Database service's `StartJobExecution`, when a worker
  claims the job): the assigned **worker's** tags are merged into the job's
  **execution** (`JobExecution.Tags` = the instance's base set + the worker's
  tags, worker winning on collision). A job that fans out to a worker group
  (F-17/fan-out) runs on every worker in the group; each worker's execution
  carries the base set plus *that* worker's tags, so a per-worker label (e.g.
  `rack=12`) is visible on the execution that ran on it. The job *instance*
  keeps the base set (it does not accumulate every worker's tags, which would
  be ambiguous for a fan-out job).

The result: **the job run (instance) carries the combined tags from the
pipeline, the job, the environment, and the worker group; each worker's
execution additionally carries the worker's own tags.** That is "all of the
tags from everything", stored on the job run.

**Worker groups become first-class.**

Today a "worker group" is only a string a worker declares (`Worker.Group`);
there is no row for it, so it cannot carry tags. F-29 adds a `WorkerGroup`
entity (`name` unique, `description`, `tags`) owned by the Database service and
managed through the API. A worker group's tags are looked up by name when a
job's run is created (a job targeting group `G` picks up `G`'s tags). A group
that has no `WorkerGroup` row simply contributes no tags (the job still runs —
group membership is unchanged; the row only adds metadata). Creating a
`WorkerGroup` row does not affect dispatch; it only makes the group taggable.

**Searching & filtering by tag.**

Because tags are normalized rows, a resource can be searched by tag key and/or
value with a plain indexed query. The list endpoints accept a repeated
`tag=key=value` query parameter; multiple parameters are ANDed (a resource
matches only if it carries **every** given tag). A run matches when at least
one of its job instances carries every given tag (the combined set lives on the
instances, not the run row).

- `GET /api/pipelines?tag=key=value[&tag=key2=value2]` — pipelines whose tag
  set contains every given tag.
- `GET /api/pipelines/{id}/runs?tag=key=value[&tag=key2=value2]` — runs of the
  pipeline having a job instance that carries every given tag.

The Database service's `ListPipelines` / `ListRuns` take a `tags` map and
resolve it to the matching owner / run ids before filtering.

**RBAC (F-14).**

- **Worker groups** — a new `worker-groups.can-view` (read a group and its
  tags) and `worker-groups.can-manage` (create/update/delete a group and its
  tags). Admins have both; they are granted to other roles as needed.
- **Environments** — tags reuse the existing `environments.can-view` /
  `environments.can-manage` (F-17): the same permission that manages an
  environment's policy manages its tags.
- **Pipelines & jobs** — tags reuse the existing `pipelines.can-view` /
  `pipelines.can-edit` (F-08/F-11): editing a pipeline's or a job's tags is a
  pipeline edit.
- **Workers** — a worker's tags are declared by the worker itself at
  registration (the gRPC target surface, job-token auth); there is no UI
  permission to set another worker's tags.

**Acceptance criteria.**
- [x] A pipeline, a job in a pipeline, an environment, a worker group, and a
      worker can each carry a set of key/value tags; a resource with no tags
      has an empty set.
- [x] Environment tags are settable through `POST/PUT /api/environments[/{name}]`
      and persisted in the database; worker-group tags are settable through
      `PUT /api/worker-groups/{name}` (and readable through `GET`) and persisted
      in the database — both reachable from the UI without a redeployment.
- [x] A worker declares its tags at registration (`CDROM_WORKER_TAGS` / config
      `worker_tags`); re-registration updates them; they are persisted and
      visible on `GET /api/workers`.
- [x] When a run is created, each job instance's stored `Tags` equal the union
      of the pipeline's, the job's, the targeted environment's (if any), and
      the targeted worker group's (if any) tags, with the more-specific source
      winning on a key collision (`worker group > environment > job >
      pipeline` for the base set).
- [x] When a job is dispatched to a worker, that worker's execution's `Tags`
      equal the instance's base set plus the worker's tags (worker winning on
      collision); a fan-out job's each-worker execution carries that worker's
      tags.
- [x] The combined set is a **snapshot**: relabeling a pipeline, environment,
      or worker group after a run started does not change the tags stored on
      that run's job instances.
- [x] A job that targets no environment and no worker group (an ephemeral
      agent) combines only the pipeline's and the job's tags.
- [x] Pipeline and job tags are captured in a pipeline's version snapshot
      (F-11); a run bound to a specific version uses that version's tags.
- [x] Worker-group management is gated on `worker-groups.can-view` /
      `worker-groups.can-manage` (F-14); environment tags on
      `environments.can-view` / `can-manage`; pipeline/job tags on
      `pipelines.can-view` / `can-edit`.
- [x] Tags are metadata only: they never affect dispatch, gating, or
      concurrency (F-16/F-17 behavior is unchanged by the presence of tags).
- [x] The combined set is visible on the job run (instance) and on each
      execution, and is surfaced to the UI (run/job detail) and in the
      WebSocket snapshot.
- [x] Pipelines and runs are searchable by tag: `GET /api/pipelines?tag=key=value`
      returns only pipelines carrying that tag, and
      `GET /api/pipelines/{id}/runs?tag=key=value` returns only runs having a job
      instance that carries it; multiple `tag=` parameters are ANDed, and a tag
      no resource carries returns an empty set.

**Design decisions (fold into AGENTS.md / Architecture.md when implemented).**
- **Tags are a uniform `map[string]string` on every resource, stored normalized
  in a `Tag` table** (one row per key/value pair, keyed by `owner_type` +
  `owner_id` + `key`), so the same in-memory field shape and the same combining
  code work for pipelines, jobs, environments, worker groups, and workers — and
  the rows are **searchable by key and/or value** with a plain indexed SQL
  query (a JSON column cannot be queried that way). The `owner_type` column
  distinguishes the resource kind, so a row's `(owner_type, owner_id)` pair
  identifies exactly one parent (a pipeline version and a job execution have
  their own `owner_type`s so their ids can never be confused with a pipeline's
  or a job's id). The `Tag` table has no soft-delete column: a tag set is
  replaced by deleting the old rows and inserting the new ones, and a soft
  delete would leave the deleted rows behind and break the unique index the
  next time the same key is set.
- **The combined set is denormalized onto the job run as a snapshot** (computed
  at run creation for the static sources, and at dispatch for the worker), so a
  run is bound to the labels that were active when it started — the same
  "bind to what was true at start" principle as F-11's version binding.
- **Worker groups become first-class rows** so they can carry tags; the row is
  metadata-only (it does not change dispatch, which still keys off the worker's
  declared `group` string), so adding it is non-breaking.
- **Precedence is specificity-ordered** (`worker > group > environment > job >
  pipeline`) so a broad label (a pipeline's `team`) can be overridden by a
  narrow one (a `prod` environment's `release`), which is the intuitive
  "the thing closest to the run wins" rule.
- **Worker tags are declared by the worker at registration** (not from the UI)
  because a worker is a deployed process whose labels belong with its
  deployment — the same reason its group and environments are declared at
  registration (F-17).
