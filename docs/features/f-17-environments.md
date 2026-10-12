# F-17 · Environments & deployment targets


> Phase 3 — Safety & governance · [Feature index](../Features.md)


**What.** First-class **environments** (e.g. `dev`, `staging`, `prod`, and
custom ones like `prod-auto`) that group execution targets and carry the
**deployment policy** that governs how work reaches them. An environment is
the unit of "where does this run, and how strictly is it gated":

- **Targets** — an environment resolves to the worker group(s) / agent pool
  that serve it. A **worker declares which environments it belongs to** (it
  may belong to several), and a job targeting an environment is dispatched to
  the workers that registered for it.
- **Approval policy** — an environment can require that a deploy to it be
  **approved** before it runs (ties to F-13). This is how an admin makes
  `prod` require a human sign-off while `dev` deploys freely.
- **Automation policy** — an environment can be **`automatic`** (a run
  proceeds on its own once its triggers and dependencies are satisfied) or
  **`manual`** (a run pauses and waits for an explicit API call to proceed).
  This is how an admin makes `prod` never deploy on its own while `dev` (or a
  `prod-auto`) does.

Environments are **managed in the database and through the API** (CRUD), not
in configuration, so policy can change at runtime without a redeployment.

**Why.** "Deploy to staging" vs "deploy to prod" is the core CD distinction,
and prod usually has stricter gates. Today a job targets a raw `target_group`
with no notion of *what that group means* or *how carefully it should be
treated*. Environments give that meaning a home: the same `dev`/`prod`
distinction drives both **where** a job runs (worker membership) and **how**
it is gated (approval + automation), and it lets an admin express "prod is
never automatic, but `prod-auto` is" as data rather than as a pipeline edit.

**Scope.**
- `internal/models` / proto — an `Environment` entity (name, description,
  approval policy, automation policy, target group / agent pool) and a
  worker↔environment membership (a worker's environment list); registered in
  `models.All()`.
- `internal/services/database` — environment CRUD + worker-environment
  membership RPCs; environment policy resolution used by the scheduler.
- `internal/services/scheduler` — resolve an environment → its target
  workers; apply the environment's **approval** policy (insert an F-13
  approval gate before dispatch) and its **automation** policy (hold a
  manual-environment run for an explicit proceed call).
- `internal/api` — environment CRUD endpoints; a **proceed** endpoint that
  releases a run held by a manual environment; worker registration that
  carries the worker's environment list; RBAC gating (F-14).
- `internal/worker` + `cmd/worker` + `internal/config` — a worker declares
  the environments it belongs to (configurable, multiple) and sends them on
  registration.
- UI — an environments view (policy + member workers) and a "proceed" action
  for a run held by a manual environment.

**The environment model.**

- **`Environment`** — a named environment. Fields: `name` (unique; e.g.
  `dev`, `staging`, `prod`, `prod-auto`), `description`, `target_group` (the
  worker group that serves it; empty means the environment is served by
  ephemeral agents), `requires_approval` (bool), and `automation`
  (`automatic` / `manual`). The approval and automation policies are the
  environment's **default** gates; a job may override them (see below).
- **Worker membership** — a worker declares the set of environments it
  belongs to. Membership is **many-to-many**: one worker can serve several
  environments (a `prod-auto` worker can also serve `staging`), and an
  environment can be served by many workers. A job targeting environment
  `E` is dispatched to the **live workers that registered `E`** (intersected
  with the environment's `target_group` when one is set). A worker that
  belongs to no environment is reachable only by its raw `target_group`
  (the pre-F-17 behavior).
- **Policy precedence.** For a job targeting an environment, the effective
  policy is:
  1. **Approval** — the environment's `requires_approval` applies **unless
     the job overrides it** (a job may set `approval: none` to skip the gate
     on an environment that requires it, or `approval: required` to add a
     gate on one that does not). The gate, when present, is an F-13
     `approval` step inserted before the job's own steps.
  2. **Automation** — the environment's `automation` policy is **authoritative
     when set** (it is the admin's statement of how that environment is
     governed). A job's own automation setting is used **only when the
     environment has no automation policy configured** (both `automatic` and
     `manual` are valid in that case — the job decides). When the environment
     is `manual`, it **overrides** the job's setting *and* the run's triggers:
     a run that reaches the environment's job with all its triggers and
     dependencies satisfied still **pauses** and waits for an explicit
     proceed call. When the environment is `automatic`, the previous
     environment is irrelevant and the run proceeds on its own.

**Automation: `automatic` vs `manual`.**

- **`automatic`** — once a run's job has satisfied its triggers (F-09) and
  dependencies (F-06/F-08), it is dispatched to the environment's workers and
  runs. The environment a job *previously* ran in (e.g. a promotion from
  `dev` to `staging`) does not matter; the job goes straight to the new
  environment. This is the policy for `dev` and for a `prod-auto` environment
  that an admin wants to deploy to without a human.
- **`manual`** — a run whose job targets a `manual` environment **pauses**
  (a non-terminal, in-flight state, like F-13's `awaiting_approval`) once all
  of its triggers and dependencies are satisfied, and **waits for an explicit
  API call** (`POST /api/runs/{id}/proceed` / per-job `POST
  /api/jobs/{id}/proceed`) before it is dispatched. A **manual environment
  overrides any trigger that is set** (cron, webhook, event — F-09): even a
  cron- or webhook-fired run is held at the manual environment until someone
  proceeds it. This is the policy for `prod`: it is *never* deployed to
  automatically, no matter what fired the run.
- **The previous-environment rule.** The pause on a `manual` environment is
  what makes "the previous environment was different" meaningful: when a job
  moves from one environment to a different one and the destination is
  `manual`, the run pauses and requires the proceed call. When the destination
  is `automatic`, the previous environment is ignored and the run proceeds.
  (A job that stays in the same environment is not a "promotion" and is not
  additionally gated by this rule — only the environment's own policy
  applies.)

**Worker environment membership (configurable, multiple).**

- A worker declares the environments it belongs to at registration. This is
  **configurable** (via `CDROM_WORKER_ENVIRONMENTS`, a comma-separated list,
  or the config file's `worker_environments`) and a worker may list **multiple**
  environments. The worker sends the list on `RegisterWorker`; the API
  persists it (via the Database service) and uses it to route environment
  jobs. Re-registering (e.g. after a restart) updates the membership.
- Because membership is per-worker and many-to-many, an admin can shape the
  topology freely: a dedicated `prod` fleet (workers that belong only to
  `prod`), a shared fleet (workers that belong to `staging` *and*
  `prod-auto`), or a single all-purpose worker that belongs to every
  environment. The same worker that serves `dev` can also serve `prod-auto`
  without any pipeline change.

**Environment configuration (database + API).**

- Environments are **stored in the database** (the `Environment` table, owned
  by the Database service) and **managed through the API** —
  `GET/POST /api/environments`, `GET/PUT/DELETE /api/environments/{name}` —
  so an admin can create `prod-auto`, flip `prod` to `manual`, or add a
  worker to an environment at runtime, with no redeployment. A change
  publishes an `environment` WS event (and is in the WS snapshot) so the UI
  stays current.
- **RBAC (F-14).** Viewing environments requires `environments.can-view`;
  creating/updating/deleting them requires `environments.can-manage` (admins
  have both). **Proceeding** a run held by a manual environment requires
  `runs.can-proceed` (admins have it; it is granted to other roles by an
  admin who wants them to be able to release a manual deploy). The
  environment's approval gate, when it fires, is approved/rejected by whoever
  holds `jobs.can-approve` / `jobs.can-reject` (F-13/F-14) — so an admin can
  give a role the ability to approve `prod` deploys without making it an
  admin.

**Acceptance criteria.**
- [x] A job targeting an environment is dispatched to the live workers that
      registered that environment (intersected with the environment's
      `target_group` when set); a worker belonging to several environments
      receives jobs for each of them.
- [x] A worker can declare multiple environments (configurable via
      `CDROM_WORKER_ENVIRONMENTS` / config), and re-registration updates its
      membership.
- [x] An environment with `requires_approval` inserts an F-13 approval gate
      before dispatch; the job pauses in `awaiting_approval` until approved
      (or rejected / timed out / cancelled). A job's `approval` override
      (`none` / `required`) takes precedence over the environment's flag.
- [x] An environment with `automation: manual` holds a run (a non-terminal
      in-flight state) once its triggers and dependencies are satisfied, and
      releases it only on an explicit proceed API call; a `manual` environment
      overrides any trigger (cron / webhook / event) that started the run.
- [x] An environment with `automation: automatic` (or one with no automation
      policy, where the job's own setting is used) dispatches the run on its
      own; the previous environment is ignored.
- [x] When the environment has no automation policy configured, both
      `automatic` and `manual` job settings are honored (the job decides).
- [x] Environments are manageable (CRUD) via the API and stored in the
      database; a change publishes an `environment` WS event and is visible in
      the UI.
- [x] Environment management is gated on `environments.can-view` /
      `environments.can-manage` (F-14); proceeding a held run is gated on
      `runs.can-proceed`; an admin can grant these to other roles.
- [x] A job held by a `manual` environment that is never proceeded is reaped
      by the watchdog (F-03) if it is held past its timeout (a held job has no
      `started_at`, so the watchdog uses its creation time as the deadline
      reference); a held job with no timeout is never reaped.
- [x] A run whose job is held (`awaiting_proceed`) is derived as `running`
      (not `succeeded`) by the run-status loop (F-07), and reaches a terminal
      status only when the job is proceeded (and completes) or is reaped /
      cancelled.
- [x] The authoritative routing gate: a worker that does not serve the job's
      environment (it is not a member of the environment, or it is outside the
      environment's `target_group` when set) is rejected when it starts the
      job (`StartJobExecution`), so a best-effort push to the wrong worker
      cannot run the job.
- [x] Proceeding a held run / job records an audit event (F-15) with the actor
      and the number of jobs proceeded.

**Design decisions (fold into AGENTS.md / Architecture.md when implemented).**
- **An environment is a policy-bearing target, not just a label.** It names a
  set of workers (via membership + optional `target_group`) *and* carries the
  two gates that govern a deploy to it: `requires_approval` (an F-13 gate)
  and `automation` (`automatic` / `manual`). This is what lets "prod is never
  automatic, but `prod-auto` is" be expressed as data.
- **Worker membership is the routing mechanism.** A job targeting environment
  `E` is dispatched to the live workers that registered `E` (∩ `E.target_group`
  when set). Membership is many-to-many and declared by the worker at
  registration (`CDROM_WORKER_ENVIRONMENTS`), so the topology is shaped by
  worker configuration, not by pipeline edits. A worker in no environment is
  reachable only by its raw `target_group` (pre-F-17 behavior preserved).
- **Automation precedence: environment policy is authoritative when set.**
  The environment's `automation` policy wins over a job's setting and over the
  run's triggers; a job's automation setting is consulted only when the
  environment has none configured. A `manual` environment therefore always
  holds a run (overriding cron / webhook / event triggers) until an explicit
  proceed call; an `automatic` environment always proceeds (the previous
  environment is irrelevant).
- **The manual hold is a first-class in-flight state.** A run held by a
  `manual` environment is a non-terminal, in-flight status (analogous to
  F-13's `awaiting_approval`): the run-status loop keeps the run `running`
  while it is held, and the watchdog reaps it if it is held past its timeout.
  Proceeding (an API call, gated on `runs.can-proceed`) releases it to
  dispatch; the decision (actor, timestamp) is recorded for audit (F-15).
- **Approval reuses F-13.** An environment's `requires_approval` is enforced
  by inserting an `approval` step (F-13) before the job's steps; the gate is
  job-level (one approval unblocks every worker of a fan-out job) and is
  approved/rejected by whoever holds `jobs.can-approve` / `jobs.can-reject`.
- **Policy is data, managed via the API.** Environments live in the database
  (`Environment` table) and are CRUD'd through `GET/POST/PUT/DELETE
  /api/environments[/{name}]`; a change publishes an `environment` WS event.
  No redeployment is needed to add `prod-auto` or flip `prod` to `manual`.
- **RBAC hooks (F-14).** New permissions: `environments.can-view`,
  `environments.can-manage`, and `runs.can-proceed`. Admins hold all of them;
  an admin can grant `runs.can-proceed` (or the approve/reject permissions) to
  a custom role so that role can release a manual deploy or approve a
  `prod` gate without being an admin.
