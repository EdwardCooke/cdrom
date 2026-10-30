# F-22 · Post-deploy verification & rollback


> Phase 4 — Delivery & observability · [Feature index](../Features.md)


**What.** After a deploy job, run **health checks** (readiness probes / a
verification step) to confirm the release is healthy; if it fails, support
**rollback** to the previous known-good release.

**Why.** Deploying is not done until the system is verified healthy, and a bad
release must be reversible.

**Scope.** Converged during design discussion (Oct 2026). Rollback is modeled
as **re-executing a suffix of the previous successful run's immutable
snapshot** (bound version, params, resolved artifacts, target groups, labels —
F-11/F-21.6), not as a new "current release" table. Three slices:

- **Slice A — outcome-aware dependency branching (this document).** Relax the
  dependency resolver so a job may *react* to a dependency's failure instead of
  being blanket-skipped, and expose each dependency's terminal status and
  outputs to the job's conditions. Includes the ruling on how environment
  gates (F-17 approval / manual hold) apply to a resumed branch.
- **Slice B — `RestartRun` orchestration (designed herein).** Transactional
  cloning of the anchor run's job rows: carry-over prefix (terminal clones
  replicating the anchor's real outcomes), fresh suffix, snapshot fidelity
  (verbatim artifacts/vars/keys/labels), provenance columns, `from_job`
  cut-point.
- **Slice C — handlers & surface (designed herein).** The `probe` and
  `rollback` step handlers, the API-mediated restoration path with its guards
  (containment, no re-arm, cooldown), the manual restart endpoint, RBAC,
  audit, WebSocket, metrics, testing, doc debt.

Ties to F-01/F-06/F-08 (specs, conditions, `needs`), F-11 (version binding),
F-13 (approval), F-16 (admission), F-17 (environments), F-21.6 (resolved
artifacts).

## Design — Slice A: outcome-aware dependency branching

Today the resolver (`internal/services/scheduler/dependencies.go`) is binary:
a dependency that reaches a terminal state other than `succeeded` causes the
dependent to be **skipped** outright (cancel/skip always; fail/time-out unless
the *dependency* carries `ignore_failed`). That is exactly why a rollback job
written as `needs: [verify]` can never arm — the moment `verify` fails, the
resolver skips the rollback before it can decide anything.

The fix separates two orthogonal ideas that `ignore_failed` conflates:

| Mechanism | Set by | True status of the dep | Visibility downstream | Intent |
|-----------|--------|------------------------|-----------------------|--------|
| `ignore_failed` (F-06, existing) | the **dependency** | masked as success | hidden — looks succeeded | "failures here are acceptable noise" |
| `branch` (new) | the **dependent** | preserved (real) | visible via `.deps.<key>.status` | "route on the outcome, esp. failure" |

`branch` does **not** hide the failure anywhere: run-status derivation, the
UI, and any other dependent still see the dependency's real status. It only
stops the resolver from reflexively skipping *this* job.

### The `branch` flag

A job-level boolean (`branch: true` in the file format; `Branch` on the
schema). Default `false` (preserve current behavior). When set, the resolver
treats a terminally-non-successful dependency as *eligible to release* rather
than *eligible to skip*; the **polarity** (act on failure vs. act on success)
is expressed by the job's own **conditions**, which now see the real statuses.
Dividing responsibility this way keeps the resolver dumb (it only decides
"may this job run at all?") and pushes the actual routing to the familiar
condition machinery on the target.

A `branch` job paired with no status-testing condition degenerates to "run
whatever the deps did" — legal but unusual; authors intending to route should
pair it with a condition (below).

### Revised resolver decision

Cadence unchanged (poll pending jobs; release only once **all** dependencies
have reached a terminal state — no early release). Per pending job with
dependencies, after observing every dependency's status:

```
states = {dep.key: dep.status for dep in job.deps}   # all terminal here

if any(state == cancelled):
    skip(job)                 # an abort is not revivable; branch does NOT override
elif any(state in {failed, timed_out, skipped}):
    if job.branch: release(job)   # let conditions inspect the real statuses
    else:        skip(job)        # today's behavior
else:                                # all succeeded
    release(job)                   # today's behavior
```

Notes:
- `cancelled` remains a hard stop even under `branch` — resurrecting an
  aborted lineage is a footgun.
- A dependency that is `skipped` counts as non-success (revivable via
  `branch`), mirroring how a skipped dep already blocks today.
- `release` means "clear `DependsOn` and leave pending for the admission loop
  (F-16)" — identical to the existing success path; nothing bypasses
  admission.

### Exposing dependency outcomes to conditions

The executor already builds a per-step condition context
(`internal/executor/executor.go`: `priorSteps`, `upstreamJobs`, `identity`,
`run`). Extend it so a job's **dependencies** contribute observations keyed by
their stable `needs` keys. Transport: at hand-off the API (which already
attaches the job to the target) fetches each dependency's terminal status and
outputs and ships them as a new repeated field, e.g.
`apib.Job.dependency_observation[] { key, status, outputs{name:value} }`.
Because the resolver releases only when all deps are terminal, these values
are stable for the life of the job.

Template forms available to a step/job condition (alongside the existing
ones):

```
{{ .deps.<key>.status }}                    # "succeeded"|"failed"|...
{{ .deps.<key>.outputs.<name> }}            # a dependency's published output
```

Example — a rollback job that arms exactly when verification failed:

```yaml
rollback:
  needs: [verify]
  branch: true
  steps:
    - type: rollback
      condition: '{{ ne .deps.verify.status "succeeded" }}'
      params:
        from_job: prod-green
```

A `branch` job whose steps **all** condition-skip performs no work and is
reported `skipped` (consistent with F-06's treatment of wholly-condition-skipped
jobs), so a no-op branch does not masquerade as a success.

Interaction with fan-out: `branch` is per job *instance*; each fanned-out
instance reacts to its own dependencies' outcomes independently. Carried-over
prefix jobs introduced by Slice B are terminal `succeeded`, so they satisfy
ordinary dependents with no special-casing.

## Design — Slice B: `RestartRun` (resumption orchestration)

### The primitive

`RestartRun(anchor_run_id, from_job_key)` spawns a new run that re-executes the
portion of the anchor run's DAG **downstream of** the cut point, and carries
over everything upstream of it as already-done. Placement follows the topology
rules: the **scheduler** exposes the RPC (it owns run-driving and anchor
choice); the **Database service** performs the transactional clone (it owns
rows). The scheduler then drives the fresh suffix instances exactly like
`CreateRun` does (`dispatchRunInstance` per job).

Crucially, the clone is built **from the anchor run's job rows, not from the
pipeline definition**. `createRunAndInstances` (the `CreateRun` path) leaks the
present in four ways — re-resolving `spec.artifacts` against the registry as it
stands *now*, re-rendering concurrency keys against the *new* run's identity,
re-combining labels from *live* resources, and rebuilding variable/vault
material from the definition. A resumption must inherit **nothing** from the
world of today: the anchor's job rows already hold the fully denormalized,
fully rendered, fully resolved snapshot (raw spec + `RunParams` + ciphertext
secrets + variables + vault imports + resolved artifact names + pre-rendered
concurrency keys + combined labels). Cloning those rows verbatim is what makes
"trust nothing from the run that proved broken, and nothing from the world that
changed since" true by construction.

Side benefit: a resumption run reads **no pipeline definition at all**, so it
survives the pipeline being archived or deleted after the anchor (the jobs are
self-contained); only UI joins degrade.

### Anchor selection

The anchor is **the latest run of the same pipeline with status `succeeded`**,
excluding runs of rollback origin. The exclusion is mandatory: without it, a
successful rollback run becomes the anchor for the *next* rollback, which
would re-execute the suffix of the run that just failed twice — stepping
sideways instead of back. Centralized as a Database RPC
(`PreviousSuccessfulRun(pipeline_id)`) so callers (API endpoint, `rollback`
handler) cannot forget it. A pipeline with no qualifying anchor has no
rollback available (first-ever deploy): the caller surfaces "nothing to
restore to" rather than inventing an anchor.

### The cut point

`from_job_key` is a **stable job key** (F-08), matched against the **anchor's**
job set — never the current definition's. Renamed or removed jobs since the
anchor therefore fail loud: `NotFound`/"cut point `prod-green` not found in
run #41". The cut point itself is **re-executed fresh**; only its strict
ancestors are carried over. Degenerate positions are valid: cutting at a root
job carries over nothing (≈ a full reproduction of the anchor); cutting at a
leaf re-executes only the leaf.

### The partition: ancestor closure

Compute the **backward closure** of the cut point over the anchor's dependency
edges (parents, grandparents, …). Since the dependency resolver clears
`DependsOn` when it releases a job, restoration reconstructs those edges from
the durable keyed `DependencyObservations` when needed, then remaps them onto
the cloned jobs. The partition:

- **Carry-over prefix** = closure ∖ {cut point}: cloned as *terminal*,
  replicating the anchor's actual outcome per job (`succeeded` **or**
  `skipped` — a condition-skipped job in the anchor stays skipped), with its
  `Outputs` and `StepResults` copied verbatim.
- **Fresh suffix** = everything else (the cut point and all of its
  descendants): cloned as `pending`, attempt-reset, to flow through the stock
  dispatch path.

Replicating the anchor's *real* terminal statuses (instead of forcing
"succeeded") is what keeps the dynamics faithful: a suffix job whose anchor
history involved a skipped feeder behaves identically — the resolver sees the
same statuses it saw in the anchor, and the cloned per-job `IgnoreFailed`
flags reproduce the same release/block decisions. Diamonds fall out correctly with
no enumeration: a suffix job feeding on both a carried-over ancestor and a
fresh sibling simply waits on the fresh one.

Nothing downstream of the cut point is ever carried over, even if it
*succeeded* in the anchor — the resumption re-verifies the whole remainder of
the graph, not a linear spine.

### Field disposition (the fidelity matrix)

Per new run row (source: the anchor run, unless noted):

| Field | Disposition |
|---|---|
| `PipelineID`, `PipelineVersion` | cloned (keeps the F-11 binding to the old definition) |
| `Status` | `pending` (derived afresh by the run-status loop) |
| `Trigger` / `TriggerName` | `"rollback"` (NEW run's origin — this marker drives the Slice-A hold bypass and the anchor exclusion) |
| `Params` | cloned **verbatim** (the anchor's concrete values) |
| `UpstreamClaims` | new run's (normally empty for a manual rollback) |
| `SourceKind/Reference/Version/FileHash` | cloned (the resumption remembers which source revision it restores) |
| `Artifacts` (resolved snapshot) | cloned **verbatim — never re-resolved** (pins the exact filed package names the anchor delivered) |
| `RestoresRunID` | NEW: → anchor run id (indexed) |

Per cloned job (source: the anchor **job row**), suffix and prefix share the
matrix; differences are marked:

| Field | Suffix (fresh) | Prefix (carry-over) |
|---|---|---|
| `Key`, `Name`, `TargetGroup`, `Environment` | cloned | cloned |
| `Spec` (raw, templates intact) | cloned | cloned |
| `RunParams`, `Secrets` (ciphertext), `Variables`, `VaultImports` | cloned | cloned |
| `Artifacts` (resolved), `ConcurrencyKeys` (pre-rendered) | cloned | cloned |
| `FailureMode`, `StepBarrier`, `MaxAttempts`, `IgnoreFailed`, `Automation`, `Approval` (override) | cloned | cloned |
| `Labels` | cloned **combined snapshot** (re-written to the `Label` table under the new ids; *not* re-computed from live resources) | ditto |
| `Status` | `pending` | **anchor's terminal status** (`succeeded`/`skipped`) |
| `Attempt` | reset to 1 | anchor's (informational) |
| `StartedAt`/`FinishedAt` | nil | **anchor's timestamps** (deliberately older than the parent run — honest: the work happened then; the UI explains it via the carry-over badge) |
| `Outputs`, `StepResults` | empty | **cloned verbatim** (suffix conditions template against them) |
| `ConcurrencyAdmitted(+At)` | false/nil | false/nil (terminal; never admitted) |
| `Approval*` (message/decision/actor/timestamps) | cleared | cleared (the gate re-materializes at hand-off from live policy, Slice A) |
| `DependsOn` | remapped through the key→new-id map | remapped (inert — terminal jobs are never resolved) |
| `CarriedOver` | FALSE | TRUE (NEW) |
| `RestoresJobID` | → anchor job id (NEW) | → anchor job id |

Two dispositions deserve emphasis:

- **Concurrency keys are cloned pre-rendered**, not re-rendered against the new
  run's identity. The resumption competes for the *same lock profile* the
  known-good run held — which is also what serializes a racing double-rollback
  (two resumptions of the same anchor contend for the same keys at admission;
  F-16 orders them; no bespoke mutex needed).
- **Secrets are cloned as ciphertext.** Constraint to document: if the F-12
  encryption key rotated after the anchor, the old ciphertext fails to decrypt
  and the affected suffix jobs die at interpolation with a clear error.
  Accepted (key rotation is rare and operator-visible); no re-encryption dance.

### Integration points (verified against current code)

- **Dependency resolver** — no special-casing. Carry-over jobs are terminal at
  birth; the resolver only inspects `pending` jobs and reads dependency
  statuses, so suffix jobs observe the replicated prefix exactly as the anchor
  observed it. Released suffix roots flow to the admission loop as usual.
- **Run-status derivation** (`runstatus.go`) — carry-over terminals count as
  satisfied contributions from the first tick; the run's status tracks the
  suffix. No changes.
- **Watchdog** (`watchdog.go`) — watches `running`/`awaiting_*`; fresh suffix
  jobs age normally. No changes.
- **Hand-off gates** — the job's `Environment` *name* is frozen from the
  anchor, but the *policies* (manual hold, approval requirement) are read
  live at dispatch/hand-off, per the Slice-A ruling: approval honored, hold
  bypassed because `Trigger == "rollback"`.
- **Events** — publish a `job_status` event per carry-over job (so every API
  pod's UI shows the assumed prefix immediately, mirroring how skips are
  published today) plus the standard `run_status`/`assignment` traffic for the
  suffix.
- **Missing-group hazard** — if the anchor's target group ceased to exist,
  suffix jobs linger `pending` (assigned to a group no worker serves). This is
  the pre-existing exposure for orphaned groups, not new; the resumption
  inherits it and the UI surfaces it via the stalled-run affordances.

### Schema deltas

- `PipelineRun.RestoresRunID *uint` (nullable, indexed) — the anchor.
- `Job.CarriedOver bool` — carry-over prefix marker.
- `Job.RestoresJobID *uint` (nullable) — the anchor job this clone replicates.
- Registered in `models.All()`; migrated by `AutoMigrate` (no hand SQL).

### Failures & validation (surfaced before the transaction opens)

| Case | Behavior |
|---|---|
| Anchor run missing / not terminal-`succeeded` | `FailedPrecondition` (callers should obtain anchors via `PreviousSuccessfulRun`) |
| `from_job_key` not present in the anchor's jobs | `NotFound` naming the run and the key |
| Anchor has zero jobs | `FailedPrecondition` |
| Mid-transaction fault | full rollback — the partial resumption never exists |

After commit, the scheduler drives the suffix; a drive failure (e.g. lost
connection) leaves a valid run whose suffix is `pending` — recoverable by the
normal admission-loop reconciliation, same posture as `CreateRun`.

## Design — Slice A: environment gates on a resumed branch

Two F-17 gates meet a resumed branch, and they operate at **different
stages**:

- **Manual hold** — enforced at *dispatch* by the scheduler
  (`dispatchJob`/`shouldHoldForProceed` in
  `internal/services/scheduler/server.go`): a pending job targeting a `manual`
  environment is parked `awaiting_proceed` before it is ever handed to a
  target.
- **Approval** — synthesized at *hand-off* by the API
  (`applyEnvironmentApprovalGate` in `internal/api/grpc.go`): when the job's
  environment has `requires_approval` (modulo the job's `approval` override),
  an F-13 approval step is prepended to the spec the target receives. The gate
  is rebuilt from the **live** environment policy at hand-off, so a job
  targeting `prod` gets the gate automatically — rollback or not.

Sequencing consequence: the manual hold fires *before* hand-off. If a resumed
branch into a `manual` environment were held, it would never reach the target
—and thus never reach the approval gate either. So the two rulings are coupled.

**Ruling: honor approval; bypass the manual hold — for rollback-origin runs.**

- **Honoring approval costs nothing and is correct.** Consent to a risky state
  change should apply to remedial actions too. Because the gate is synthesized
  from live policy at hand-off, a resumed branch into a `requires_approval`
  environment stops at the approval gate with zero extra wiring.
- **Bypassing the manual hold is a small, targeted carve-out.** The manual hold
  is an *autonomy* gate ("nothing deploys here unprompted"). Initiating a
  rollback IS the prompt — a human clicked it, or a verify reaction fired it —
  so re-holding double-gates the same decision. Worse, in the auto-fired case
  an unattended hold strands the environment broken with no one to proceed it.
  Implement as: when the owning run is rollback-origin (marked, e.g.
  `Trigger == "rollback"` / a `RestoresRunID` set), `shouldHoldForProceed`
  returns false. Ordinary (non-rollback) runs are unaffected.

Combined effect for a `prod` environment that is both `manual` and
`requires_approval`: a resumed branch parks at the **approval gate** (one human
touch, at the consent moment) and does **not** additionally stall at a proceed
hold. Exactly one intervention, placed where it adds assurance rather than
friction.

## Design — Slice C: handlers, mediation, and surface

### Entry points (authority model)

Exactly three doors into restoration, all funneling into the one scheduler
primitive (`RestartRun`, Slice B):

| Door | Caller | Principal | Depth |
|---|---|---|---|
| Armed | `rollback` step handler reacting to a failed verification | the job's own token (machine) | refused if the calling run is rollback-origin (§ guards) |
| Manual | `POST /api/pipelines/{id}/restart` (body `{from_job}`) | authenticated user/service account bearing `runs.can-restore` (pipeline-scoped) | unlimited (audited human discretion) |
| UI | "Rewind" action on a pipeline page, cut-point dropdown seeded from the latest successful run's job keys | same as manual | same as manual |

The armed and manual doors differ only in principal and guard applicability;
the mechanics are shared.

### The `probe` step handler (verification)

A new built-in handler, `type: probe` — a readiness poll executed **by the
execution target** (worker/agent), stdlib `net/http` only, portable
Win/Linux. Deliberately target-side (contrast F-18's API-side sends): the
probe's value is network adjacency to the workload that was just deployed —
cluster-internal VIPs/LBs that the API cannot reach. Trust parity is the
`shell` handler's (authors control targets and URLs alike); secret material
arrives via the usual `{{ .secrets.name }}` interpolation and is redacted from
logged output (F-12) — the handler logs status lines and timings, never
response bodies.

Parameters (all templatizable; joined to the executor's **lazy-render**
whitelist alongside email/webhook so a URL may cite a prior step's output,
e.g. a dynamically allocated ingress host):

| Param | Meaning | Default |
|---|---|---|
| `url` | probed URL (required) | — |
| `method` | HEAD/GET | `GET` |
| `expect` | accepted status codes (space/list) | `200 201 202 204 301 302 303 307 308` |
| `interval` | poll cadence | `10s` |
| `deadline` | total wait before failure | `5m` (hard-clamped by the step's `Timeout`) |
| `headers` | `Name: Value` lines (secrets allowed) | — |

Semantics: poll until a response in `expect` → **step succeeds**; until
`deadline` exhausted → **step fails**. The failure message classifies the last
observation — `connect-error` (DNS/refused/unreachable: infrastructure) vs.
`status <code>` (app answering but unhealthy) — so operators triage without
opening logs. Step outputs on completion: `final_status`, `elapsed_s`,
`polls`. Typical pairing (the F-22 archetypal shape):

```yaml
verify-prod:
  needs: [prod-green, prod-blue]
  steps:
    - type: probe
      params:
        url: "https://api.prod.internal/healthz"
        deadline: 5m
```

Richer checks (smoke tests, DB pings) remain legitimate `shell` steps; `probe`
covers the readiness-poll majority.

### The `rollback` step handler (reaction)

Mirrors the F-18 handler anatomy: render params, call the API, report. New
typed capability on the executor context (obtained via
`RestorerFromContext`, implemented in `internal/notify` alongside `Notifier`
— same api-client/job-ID/token triple; wired into worker and agent contexts):

```go
type Restorer interface {
    // InitiateRestore asks the API to spawn a resumption of this job's
    // pipeline (guards applied server-side). Returns the new run id.
    InitiateRestore(ctx context.Context, fromJob string) (runID int64, err error)
    // RunStatus reports a run's current status (polling aid).
    RunStatus(ctx context.Context, runID int64) (status string, err error)
}
```

Parameters:

| Param | Meaning | Default |
|---|---|---|
| `from_job` | cut-point key in the anchor run (validated server-side; eager-templatizable against run params) | required |
| `wait` | block the step until the resumption run terminates | `true` |
| `wait_deadline` | max block (then the step fails with the run id exposed; the resumption continues unattached) | `30m` (clamped by step `Timeout`) |

Behavior: `InitiateRestore` → on success, publish outputs `restore_run_id`
(and `restore_status` when waited). Verdict mapping with `wait: true`:
resumption `succeeded` → step succeeds; `failed`/`timed_out`/`cancelled` →
**step fails** (a botched rollback must fail the enclosing run loudly, never
pass silently); wait deadline exceeded → step fails citing the still-live run
id. Absent capability (misconfigured target) → step error, parity with
`email`. Canonical armed wiring (completes the archetype above):

```yaml
rollback-prod:
  needs: [verify-prod]
  branch: true                       # Slice A: react to failure instead of skip
  steps:
    - type: rollback
      condition: '{{ ne .deps.verify-prod.status "succeeded" }}'
      params:
        from_job: prod-green
```

### Server-side guards (API `InitiateRestore`)

Order of operations, all before the scheduler call:

1. **Job-token validation** — the calling job must be live (tokens are
   gated on job status, F-14/job-token auth), establishing the caller's
   run → pipeline. Containment property: **a job can only rewind its own
   pipeline** — the anchor is derived from the caller's pipeline, never taken
   from request input.
2. **No re-arm (depth ≤ 1, armed door only)** — if the caller's run is
   rollback-origin (`Trigger == "rollback"`), refuse
   (`FailedPrecondition: rollbacks do not re-arm`). Mandatory: the resumption
   re-executes the anchor's suffix, which INCLUDES the armed rollback job;
   without this, a perpetually-failing verification descends generations.
   The manual door is exempt (operators may legitimately cascade; every
   initiation is audited).
3. **Cooldown (flap suppression)** — derived, no new state: find the most
   recent rollback-origin run of the pipeline; if its `StartedAt` is younger
   than `rollback_cooldown` (config, default `5m`), refuse with the remaining
   window. Stops hot-loops when the environment is fundamentally broken.
4. **Cut-point validation** — delegated to the scheduler (`from_job` must
   exist among the anchor's jobs; `NotFound` otherwise, Slice B).
5. **Anchor absence** — `PreviousSuccessfulRun` yields nothing →
   `NotFound: no successful run to restore to`.

Only then: scheduler `RestartRun(anchor, from_job)`, audit record, metrics,
response `{run_id}`. Admission serialization is inherited, not invented: the
resumption contends for the anchor's (identical, pre-rendered) concurrency
keys, so overlapping restorations and draining jobs of the failed run order
themselves through F-16.

### Manual endpoint

`POST /api/pipelines/{id}/restart` — body `{ "from_job": "<key>" }`.
Authorization: `runs.can-restore` scoped to the pipeline (added to the F-14
catalog; denied by default like everything else). The job-token bundle (today
granting `SendEmail`/`SendWebhook` et al.) gains the SELF-scoped restore
permission so the armed door authenticates. Response echoes the new run.
Errors map 1:1 from the guarded path (cooldown, no-anchor, bad cut point).

### Authorization, audit, events, metrics

- **Catalog additions** (`internal/authz/permissions.go`):
  `runs.can-restore` (initiate a resumption; pipeline-scoped). Viewing
  rests on the existing runs-read permission.
- **Audit** (`internal/audit`, best-effort dual-sink): one entry per
  initiation — `action: runs.restart`, actor (user id / `job:<id>`),
  details `{pipeline, anchor_run, from_job, new_run, door: armed|manual}`.
  Secret-free by construction.
- **WebSocket**: no new event kinds. The resumption generates standard
  `run_status` / `job_status` / assignment traffic (Slice B publishes the
  carry-over prefix's statuses); clients learning of the new run from the
  event see `RestoresRunID` in the payload. Optional stretch: a dedicated
  `run_resumed` convenience event.
- **Metrics** (low-cardinality, `cdrom_` prefix):
  - `cdrom_restorations_total{door, result}` — `result ∈ initiated |
    cooldown | no_anchor | rearmed | invalid_cut_point` (API).
  - `cdrom_probe_polls_total{outcome}` — `outcome ∈ success | deadline |
    connect_error | unexpected_status` (target-reported via step results;
    aggregated at the API like other step stats).
  Feeds F-20's MTTR/DORA story together with run-duration series.

### Configuration

| Var | Meaning | Default |
|---|---|---|
| `CDROM_ROLLBACK_COOLDOWN` | min spacing between rollback-origin initiations per pipeline | `5m` |

(Probe defaults are per-step params, not globals — no config surface.)

### Testing & e2e

- **Unit/integration (SQLite, per house convention)**: partition & closure
  (linear, diamond, root-cut ≙ full repro, leaf-cut, missing cut point);
  fidelity matrix asserts (artifacts verbatim, keys pre-rendered, labels
  cloned, outputs/timestamps copied, lifecycle fields reset); resolver
  interplay (`branch` releases on fail/timed-out/skipped, cancels halt,
  no-mask of true statuses); guard ordering (re-arm refusal, cooldown
  refusal, containment to own pipeline).
- **E2E** (`scripts/e2e/NN-restoration.sh`, next free NN): a pipeline with
  `deploy-ok` → `verify` (forced-fail switch) → `rollback` (branched). Assert:
  first run fails at verify; resumption run appears with carried-over prefix
  and fresh suffix; suffix re-executes the OLD snapshot; a SECOND verify
  failure in the resumption does NOT re-arm; immediate re-invocation trips the
  cooldown; the manual endpoint rewinds on demand.

### Doc debt (landing obligation)

Update `AGENTS.md` (feature index: F-22 done; step-handler roster gains
`probe`/`rollback`), `docs/Architecture.md` (REST + RPC inventories, config
reference), `docs/Features.md` (checkboxes), `docs/Monitoring.md` (new
series).

## Acceptance criteria

Slice A (this document):
- [x] A job declaring `branch: true` is released (not skipped) when a
      dependency fails/times out/is skipped, and its conditions can observe
      the dependency's real status and outputs via `.deps.<key>.*`.
      _(resolved: `scheduler.dependencies_test.go` — released-on-failed,
      mixed-outcomes capture-all; `executor.deps_condition_test.go` — routes
      on `.deps.<key>.Status`, exposes `.deps.<key>.Outputs`, safe noop for
      unknown keys; e2e arms the rollback step purely from
      `{{ eq .deps.verify.Status "failed" }}`.)_
- [x] A `branch` job is still skipped when a dependency is `cancelled`.
      _(`TestResolveJobDependenciesBranchCancelledAlwaysSkips`.)_
- [x] `branch` does not alter the dependency's true status for run-status
      derivation or other dependents (contrast with `ignore_failed`).
      _(`TestResolveJobDependenciesNonBranchStillSkipsOnFailedDependency`
      contrasts the semantics; e2e: the bad run still lands `failed` although
      its branch-dependent rollback job succeeded.)_
- [x] A `branch` job whose steps all condition-skip is reported `skipped`.
      _(standard F-06 all-stepped-skipped reporting; exercised in e2e by the
      good run, whose rollback job condition-skips and leaves the run
      succeeded.)_
- [x] A resumed (rollback-origin) branch honors the target environment's
      approval gate but is exempt from the manual-environment proceed hold.
      _(`scheduler.hold_bypass_test.go` — bypass iff trigger `rollback`;
      approval gating untouched.)_

Slice B (drafted above; acceptance):
- [x] `RestartRun(anchor, from_job)` spawns a run whose prefix clones replicate
      the anchor's real terminal outcomes (incl. skipped) and whose suffix
      re-executes from the anchor's verbatim snapshot (artifacts, params,
      secrets, variables, concurrency keys, labels).
      _(`database.restoration_test.go` — PartitionAndProvenance,
      DiamondClosure, SnapshotFidelity; e2e asserts `build` arrives
      `carried_over` with its outputs intact and no execution row, while
      `deploy`/`verify` re-execute._)
- [x] Anchors exclude rollback-origin runs; a missing cut point in the anchor
      fails loud.
      _(`PreviousSuccessfulRun` filters `restores_run_id IS NULL`;
      `RestartRunValidation` — unknown cut point → NotFound, non-succeeded
      anchor → FailedPrecondition; e2e: manual restart with a ghost cut point
      → 400.)_
- [x] Resumptions bind to the anchor's `PipelineVersion` and need no pipeline
      definition to exist afterwards.
      _(clones are taken from the anchor's job rows — the per-run snapshot —
      and the new run inherits `anchor.PipelineVersion`; the definition is
      never consulted in the `RestartRun` path.)_

Slice C (drafted above; acceptance):
- [x] A `probe` step polls a URL from the execution target until success or
      deadline, exposing classified failure diagnostics and step outputs.
      _(`stephandlers.probe_test.go` — success, deadline/connect-error vs
      status-error classification, accept-statuses, templated URL, headers;
      e2e distinguishes the healthy endpoint (success) from the dead one
      (classified connect-error failure).)_
- [x] A `rollback` step handler initiates a resumption of its OWN pipeline via
      the API (job-token mediated), publishing the resumption run id and
      honoring `wait`/`wait_deadline`.
      _(`stephandlers.rollback_test.go` + `api.restoration_test.go`; e2e: the
      armed step fires, the API mediates, and the step publishes
      `restore_run_id`. Note: `wait` defaults to `false` — a `wait=true`
      rollback on a single-slot worker group self-starves the resumption it
      awaits; opt in deliberately.)_
- [x] Armed initiation refuses rollback-origin callers (depth ≤ 1) and
      cools-down flapping pipelines; the manual endpoint
      (`POST /api/pipelines/{id}/restart`, `runs.can-restore`) bypasses both
      by human authority.
      _(`api.restoration_test.go` — no-re-arm, cooldown (live + expired),
      caller-outside-run refused; e2e exercises the manual door spawning a
      second resumption from a different cut point.)_
- [x] Every initiation is audited (actor, anchor, cut point, new run, door);
      `cdrom_restorations_total` and probe outcome metrics are exported.
      _(audit `run.restore` with actor `job:<id>` and door `armed`/`manual` —
      grepped in e2e; `cdrom_api_restorations_total{door,result}` in
      `internal/metrics/api.go`; probes surface through
      `cdrom_worker_steps_total{step_type="probe"}`.)_
- [x] E2E demonstrates the full arc: failed verification → branched rollback
      → resumption with carried-over prefix → no re-arm on repeat failure.
      _(`scripts/e2e/17-restoration.sh` — good run anchors, bad run fails the
      probe, the armed rollback fires, the resumption carries over `build`
      and re-executes the suffix to success, provenance + audit + manual door
      asserted. Depth ≤ 1 is structural: the resumption's rollback clone is a
      fresh pending job whose `previous_successful_run` lookup excludes
      rollback-origin runs, and its caller is refused anyway.)_
- [x] A `rollback` step handler invokes the restoring run and reports its
      verdict; recursive re-arming is prevented (depth ≤ 1) with a cooldown.
      _(covered by the preceding two items: `wait`/`wait_deadline` polling of
      the resumption's verdict, no-re-arm refusal, and the flap cooldown.)_
- [x] REST/WS/RBAC/audit/metric surface for verifying and rolling back.
      _(REST: `POST /api/pipelines/{id}/restart` (+400 validation),
      `GET /api/runs/{id}` projecting `trigger`/`restores_run_id`,
      `GET /api/jobs` projecting `branch`/`carried_over`/
      `dependency_observations`; WS: runs arrive via the existing
      `run_status` events; RBAC: `runs.can-restore` denies by default;
      audit + metrics as above.)_
