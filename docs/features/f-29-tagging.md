# F-29 · Labeling

> Phase 4 — Organization & metadata · [Feature index](../Features.md)

**What.** **Labels** — free-form **key/value pairs** that can be attached to the
platform's primary resources: **pipelines**, **jobs in a pipeline**,
**environments**, **agent groups**, and **agents**. When a job is kicked off
(a run is created and its job instances are dispatched), the job's run collects
the labels from **every** resource in its context — the pipeline, the job, the
environment it targets, the agent group it targets, and the agent that runs
it — **combines** them into a single set, and **stores** the result on the job
run. The combined set is what the UI, dashboards, and downstream tooling see
for a run: a flat, denormalized snapshot of "everything this run is labeled
with".

Labels are **metadata, not behavior**: they never gate, route, or serialize work
(contrast with F-16 concurrency keys, which are locks, or F-17 environments,
which carry policy). They exist so a run can be searched, filtered, grouped,
and reported on by arbitrary labels — `team=backend`, `cost-center=1234`,
`region=eu-west`, `release=train-2026-10`, etc.

**Why.** As a CD platform grows, operators need to slice their runs by
dimensions that don't fit the fixed schema (pipeline, environment, group,
trigger). Today the only free-form metadata a run carries is its `params`
(F-10), which are *inputs to the job*, not *labels on the run*. Labels give the
platform a first-class, uniform labeling mechanism that:

- **composes** — a run's label set is the union of the labels on every
  resource it touches, so labeling a `prod` environment or a `backend` agent
  group automatically labels every run that goes through it, with no per-pipeline
  edits;
- **is denormalized onto the run** — a run records the labels that were active
  when it started (a snapshot, like F-11's version binding), so relabeling a
  pipeline or environment later never rewrites history;
- **is uniform** — one `map[string]string` shape on every resource, one
  combining rule, one place (the job run) to read the result.

**Scope.**
- `internal/models` — a `Labels` field (`map[string]string`, in-memory only,
  `gorm:"-"`) on `Pipeline`, `PipelineVersion`, `JobDefinitionSnapshot`, `Job`,
  `Environment`, `Agent`, and `JobExecution`; a new `AgentGroup` entity
  (`name`, `description`, `labels`) — agent groups are currently implicit (a
  agent's `group` string) and become first-class, labelable rows; and a new
  `Label` entity — the **normalized** storage for every resource's labels: one
  row per key/value pair, keyed by `owner_type` + `owner_id` + `key` (a
  composite unique index) with a separate index on `(key, value)`. The
  `owner_type` distinguishes the resource kind (`pipeline`, `pipeline_version`,
  `job`, `job_execution`, `environment`, `agent`, `agent_group`) so a row's
  `(owner_type, owner_id)` identifies exactly one parent. All registered in
  `models.All()`. The `Label` table is what makes labels **searchable by key
  and/or value** with a plain indexed SQL query — a JSON column cannot be
  queried that way. The parent's `Labels` map is populated from the `Label`
  table on load and written back on save by the Database service.
- proto (`cdrom.db.v1`) — `labels` on the `Pipeline`, `PipelineVersion`,
  `JobDefinition`, `Job`, `JobExecution`, `Environment`, and `Agent` messages
  and on the `CreatePipeline`/`UpdatePipeline`, `CreateEnvironment`/
  `UpdateEnvironment`, and `RegisterAgent` requests; new `AgentGroup`
  message + `CreateAgentGroup`/`GetAgentGroup`/`ListAgentGroups`/
  `UpdateAgentGroup`/`DeleteAgentGroup` RPCs. (`cdrom.api.v1`) — `labels` on
  `Agent` and `RegisterAgentRequest`. (`cdrom.scheduler.v1`) — `labels` on
  `Job` (the combined set, so the scheduler and UI see it).
- `internal/services/database` — persist labels on every resource; **agent-group
  CRUD**; in `createRunAndInstances` compute each job instance's combined labels
  (pipeline + job + environment + agent group) and store them on the instance;
  in `StartJobExecution` merge the assigned agent's labels into the execution's
  label set.
- `internal/api` — pipeline/job labels flow through the existing
  `POST/PUT /api/pipelines` body; environment labels through the existing
  `POST/PUT /api/environments[/{name}]`; **new agent-group endpoints**
  (`GET /api/agent-groups`, `GET /api/agent-groups/{name}`,
  `PUT /api/agent-groups/{name}`, `DELETE /api/agent-groups/{name}`); agent
  labels are declared at registration (the gRPC `RegisterAgent` carries them).
- `internal/agent` + `cmd/agent` + `internal/config` — a agent declares its
  labels at registration (configurable, like its environments) via
  `CDROM_AGENT_LABELS` / the config file's `agent_labels`.
- `internal/authz` — `agent-groups.can-view` / `agent-groups.can-manage`
  (new); environment labels reuse `environments.can-view` / `can-manage`,
  pipeline/job labels reuse `pipelines.can-view` / `can-edit`.
- UI — a labels editor on the pipeline, job, environment, and agent-group forms;
  a labels display on the run/job detail view (the combined set).

**The label model.**

- **A label is a key/value pair.** A resource's labels are a `map[string]string`
  (a set of key/value pairs; a key is unique within a resource, a value is a
  free-form string, possibly empty). Keys must be non-empty; values may be
  anything. The set is stored **normalized in the `Label` table** — one row per
  key/value pair, keyed by `owner_type` + `owner_id` + `key` — rather than as a
  JSON document on the parent row. This is what makes a resource searchable and
  filterable by label key and/or value with a plain indexed SQL query (a JSON
  column cannot be queried that way). The parent's in-memory `Labels` map
  (`gorm:"-"`) is the API/UI-facing shape; the Database service loads it from
  the `Label` table and writes it back on save.
- **Every resource carries the same shape.** `Pipeline`, a pipeline's job
  definitions, `Environment`, `AgentGroup`, and `Agent` each have a
  `Labels` map (backed by `Label` rows of the matching `owner_type`). A
  resource with no labels has an empty (or absent) map and no `Label` rows.

**Where labels are set.**

- **Pipelines and jobs** — set in the pipeline's definition, through the
  existing `POST/PUT /api/pipelines` body: the pipeline carries a `labels` map,
  and each job definition carries its own `labels` map. They are captured in the
  pipeline's version snapshots (F-11) so a run bound to a version uses that
  version's labels.
- **Environments** — set through the existing `POST/PUT
  /api/environments[/{name}]` body (`labels` field), stored in the database
  (the `Environment` table, owned by the Database service). This is the
  "from the upcoming UI" path: the UI edits an environment's labels and the API
  persists them.
- **Agent groups** — a agent group is a first-class, named, labelable row
  (the `AgentGroup` table). It is managed through **new API endpoints**
  (`GET/PUT/DELETE /api/agent-groups[/{name}]`), again the "from the upcoming
  UI" path: the UI sets a group's labels and the API persists them. A agent
  group that has never been created has no labels (an empty set); the UI (or an
  admin) creates it and sets its labels.
- **Agents** — a agent is a long-lived process, so its labels are part of its
  identity, declared **at registration** (configurable, like its environments
  membership, F-17): `CDROM_AGENT_LABELS` (a `key=value,key2=value2` list) or
  the config file's `agent_labels`. The agent sends them on `RegisterAgent`;
  the API persists them (via the Database service). Re-registration (e.g. after
  a restart) updates them. Agents are *not* labeled from the UI — a agent's
  labels are set where the agent is deployed.

**Combining: "take all of the labels from everything".**

When a job is kicked off, its run's label set is the **union** of the labels on
every resource in its context. The sources, from least to most specific, are:

1. **Pipeline** — the pipeline's labels (the same for every job in the run).
2. **Job** — the job's own labels (its definition's labels).
3. **Environment** — the labels of the environment the job targets (F-17);
   absent when the job targets a raw group with no environment.
4. **Agent group** — the labels of the agent group the job targets; absent
   when the job runs on an ephemeral agent (no group).
5. **Agent** — the labels of the agent that runs the job; present only once a
   specific agent is assigned (at dispatch).

**Precedence on a key collision.** When two sources set the same key, the
**more specific** source wins: `agent > agent group > environment > job >
pipeline`. So if the pipeline sets `team=platform` and the job sets
`team=backend`, the run carries `team=backend`; if the `prod` environment sets
`release=train-2026-10`, it overrides a pipeline-level `release` label. Keys
set by only one source pass through unchanged.

**When the combination is computed and stored.**

- **At run creation** (the Database service's `createRunAndInstances`, F-07):
  each job instance's `Labels` is set to the union of the **four static
  sources** — pipeline, job, environment (if targeted), and agent group (if
  targeted) — with the precedence above. These are known the moment the
  instance is created (the job's environment and target group are on its
  definition), so the base combined set is stored on the instance then. This is
  the run's label snapshot: it records the labels that were active when the run
  started, and is not rewritten if a resource is later relabeled.
- **At dispatch** (the Database service's `StartJobExecution`, when a agent
  claims the job): the assigned **agent's** labels are merged into the job's
  **execution** (`JobExecution.Labels` = the instance's base set + the agent's
  labels, agent winning on collision). A job that fans out to a agent group
  (F-17/fan-out) runs on every agent in the group; each agent's execution
  carries the base set plus *that* agent's labels, so a per-agent label (e.g.
  `rack=12`) is visible on the execution that ran on it. The job *instance*
  keeps the base set (it does not accumulate every agent's labels, which would
  be ambiguous for a fan-out job).

The result: **the job run (instance) carries the combined labels from the
pipeline, the job, the environment, and the agent group; each agent's
execution additionally carries the agent's own labels.** That is "all of the
labels from everything", stored on the job run.

**Agent groups become first-class.**

Today a "agent group" is only a string a agent declares (`Agent.Group`);
there is no row for it, so it cannot carry labels. F-29 adds a `AgentGroup`
entity (`name` unique, `description`, `labels`) owned by the Database service
and managed through the API. A agent group's labels are looked up by name when
a job's run is created (a job targeting group `G` picks up `G`'s labels). A
group that has no `AgentGroup` row simply contributes no labels (the job still
runs — group membership is unchanged; the row only adds metadata). Creating a
`AgentGroup` row does not affect dispatch; it only makes the group labelable.

**Searching & filtering by label.**

Because labels are normalized rows, a resource can be searched by label key
and/or value with a plain indexed query. The list endpoints accept a repeated
`label=key=value` query parameter; multiple parameters are ANDed (a resource
matches only if it carries **every** given label). A run matches when at least
one of its job instances carries every given label (the combined set lives on
the instances, not the run row).

- `GET /api/pipelines?label=key=value[&label=key2=value2]` — pipelines whose
  label set contains every given label.
- `GET /api/pipelines/{id}/runs?label=key=value[&label=key2=value2]` — runs of
  the pipeline having a job instance that carries every given label.

The Database service's `ListPipelines` / `ListRuns` take a `labels` map and
resolve it to the matching owner / run ids before filtering.

**RBAC (F-14).**

- **Agent groups** — a new `agent-groups.can-view` (read a group and its
  labels) and `agent-groups.can-manage` (create/update/delete a group and its
  labels). Admins have both; they are granted to other roles as needed.
- **Environments** — labels reuse the existing `environments.can-view` /
  `environments.can-manage` (F-17): the same permission that manages an
  environment's policy manages its labels.
- **Pipelines & jobs** — labels reuse the existing `pipelines.can-view` /
  `pipelines.can-edit` (F-08/F-11): editing a pipeline's or a job's labels is a
  pipeline edit.
- **Agents** — a agent's labels are declared by the agent itself at
  registration (the gRPC target surface, job-token auth); there is no UI
  permission to set another agent's labels.

**Acceptance criteria.**
- [x] A pipeline, a job in a pipeline, an environment, a agent group, and a
      agent can each carry a set of key/value labels; a resource with no
      labels has an empty set.
- [x] Environment labels are settable through `POST/PUT /api/environments[/{name}]`
      and persisted in the database; agent-group labels are settable through
      `PUT /api/agent-groups/{name}` (and readable through `GET`) and persisted
      in the database — both reachable from the UI without a redeployment.
- [x] A agent declares its labels at registration (`CDROM_AGENT_LABELS` /
      config `agent_labels`); re-registration updates them; they are persisted
      and visible on `GET /api/agents`.
- [x] When a run is created, each job instance's stored `Labels` equal the
      union of the pipeline's, the job's, the targeted environment's (if any),
      and the targeted agent group's (if any) labels, with the more-specific
      source winning on a key collision (`agent group > environment > job >
      pipeline` for the base set).
- [x] When a job is dispatched to a agent, that agent's execution's `Labels`
      equal the instance's base set plus the agent's labels (agent winning on
      collision); a fan-out job's each-agent execution carries that agent's
      labels.
- [x] The combined set is a **snapshot**: relabeling a pipeline, environment,
      or agent group after a run started does not change the labels stored on
      that run's job instances.
- [x] A job that targets no environment and no agent group (an ephemeral
      agent) combines only the pipeline's and the job's labels.
- [x] Pipeline and job labels are captured in a pipeline's version snapshot
      (F-11); a run bound to a specific version uses that version's labels.
- [x] Agent-group management is gated on `agent-groups.can-view` /
      `agent-groups.can-manage` (F-14); environment labels on
      `environments.can-view` / `can-manage`; pipeline/job labels on
      `pipelines.can-view` / `can-edit`.
- [x] Labels are metadata only: they never affect dispatch, gating, or
      concurrency (F-16/F-17 behavior is unchanged by the presence of labels).
- [x] The combined set is visible on the job run (instance) and on each
      execution, and is surfaced to the UI (run/job detail) and in the
      WebSocket snapshot.
- [x] Pipelines and runs are searchable by label: `GET /api/pipelines?label=key=value`
      returns only pipelines carrying that label, and
      `GET /api/pipelines/{id}/runs?label=key=value` returns only runs having a
      job instance that carries it; multiple `label=` parameters are ANDed, and
      a label no resource carries returns an empty set.

**Design decisions (fold into WORKER.md / Architecture.md when implemented).**
- **Labels are a uniform `map[string]string` on every resource, stored
  normalized in a `Label` table** (one row per key/value pair, keyed by
  `owner_type` + `owner_id` + `key`), so the same in-memory field shape and the
  same combining code work for pipelines, jobs, environments, agent groups, and
  agents — and the rows are **searchable by key and/or value** with a plain
  indexed SQL query (a JSON column cannot be queried that way). The `owner_type`
  column distinguishes the resource kind, so a row's `(owner_type, owner_id)`
  pair identifies exactly one parent (a pipeline version and a job execution
  have their own `owner_type`s so their ids can never be confused with a
  pipeline's or a job's id). The `Label` table has no soft-delete column: a
  label set is replaced by deleting the old rows and inserting the new ones, and
  a soft delete would leave the deleted rows behind and break the unique index
  the next time the same key is set.
- **The combined set is denormalized onto the job run as a snapshot** (computed
  at run creation for the static sources, and at dispatch for the agent), so a
  run is bound to the labels that were active when it started — the same
  "bind to what was true at start" principle as F-11's version binding.
- **Agent groups become first-class rows** so they can carry labels; the row is
  metadata-only (it does not change dispatch, which still keys off the agent's
  declared `group` string), so adding it is non-breaking.
- **Precedence is specificity-ordered** (`agent > group > environment > job >
  pipeline`) so a broad label (a pipeline's `team`) can be overridden by a
  narrow one (a `prod` environment's `release`), which is the intuitive
  "the thing closest to the run wins" rule.
- **Agent labels are declared by the agent at registration** (not from the
  UI) because a agent is a deployed process whose labels belong with its
  deployment — the same reason its group and environments are declared at
  registration (F-17).
