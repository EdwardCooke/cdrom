# F-31 — Deferred refactors & known bugs

Tracking sheet for work consciously parked during the F-30 epic: items that
are understood, mitigated where worth it, and deliberately NOT tackled in
the F-30 slices (they would enlarge blast radius without advancing the
topology goals). Nothing here blocks F-30.4 Slices 2–3.

## Items

### 1. Pipeline definitions deserve their own table (parked 2026-11-10)

**Today.** F-08 stores pipeline *definitions* as ordinary `Job` rows
(`run_id IS NULL AND pipeline_id IS NOT NULL`); triggering a run clones them
into concrete run-instance rows. Definitions are never work items, yet they
carry `status = pending` perpetually, so they were offered to every agent's
poll path (`ListPendingByGroup` returns all pending rows matching
group-or-environment) and DECLINED by `StartJobExecution`'s definition-row
guard — a wasted RPC + a guaranteed decline, per agent, per second, per
deployed pipeline.

**Interim mitigation (landed 2026-11-10).** The exclusion moved UP to the
QUERY: `ListPendingByGroup` now adds
`WHERE (run_id IS NOT NULL OR pipeline_id IS NULL)` — definition rows never
reach the pull path at all. `StartJobExecution`'s definition-row guard
remains as the authoritative edge-case cover (other start origins). Cost of
the mitigation: essentially zero; benefit: the agent's steady-state poll
noise scales with real work items only. Documented in the handler + the
`db.proto` RPC comment.

**Future story (THIS ITEM).** Give definitions a home of their own — a
dedicated table (say `pipeline_definitions` / `job_definitions`) instead of
piggybacking on `jobs`. Scope sketch, deliberately unbuilt:

- Schema migration (AutoMigrate additions; populate from existing rows;
  retire the dual-use of `Job` for definitions — no backwards compat per
  project policy, but the MIGRATION itself must preserve installed systems).
- Versioning interplay (F-11): a pipeline version's frozen job list is
  currently expressed over definition-shaped `Job` rows; the clone-on-run
  path (`CreateRun`) would rewrite against the new tables.
- Surface ripple: `GET /api/pipelines/{id}` job preview, the UI's pipeline
  editor, label owners (F-29), and the config-as-code importer (F-21) all
  treat definitions as jobs today.
- Acceptance idea: a definition row no longer EXISTS in `jobs`; the poll
  path needs no special casing (the WHERE clause disappears); e2e 15
  (external sourcing) + 01/07 (runs) stay green untouched.

**LANDED (2026-11-14).** Definitions now live in their own table
(`job_definitions`, model `JobDefinition` — pipeline-scoped, unique
`(pipeline_id, key)` LIVE identity via the supplemental
`uk_job_definition_live_identity` index, labels homed under owner type
`job_definition`). Evidence:

- **ACCEPTANCE MET.** A definition row no longer exists in `jobs` — pinned
  by `TestJobDefinitionsLiveInTheirOwnTable` (creates a pipeline with
  definitions; asserts the row in `job_definitions` AND zero
  definition-shaped rows in `jobs`). The poll path's special casing is
  GONE: `ListPendingByGroup`'s `WHERE (run_id IS NOT NULL OR pipeline_id IS
  NULL)` and `StartJobExecution`'s definition-row guard are both retired
  (comments mark the gravesites).
- **No legacy migration (DECIDED 2026-11-14).** There is no installed
  system to preserve; per the no-backwards-compatibility stance the split
  ships as-is — a promotion migration was authored and removed on
  direction.
- **Surface redirected.** Create/Update/Delete pipeline, Get/List pipeline,
  the F-11 snapshot builders, the run-clone path (`runDefinitionFor`
  bridges definitions into the cloner's traditional Job-shaped form via
  `pseudoJobFromDefinition` — the split is a STORAGE matter, not a
  clone-semantics matter), and `CreateJob`'s add-definition branch (which
  now forges a `JobDefinition` row and responds in the job's wire form).
  `GetJob` round-trips a definition id transparently (jobs first, then
  definitions — pinned by `TestGetJobRoundTripServesDefinitions`), so the
  API's `POST /api/jobs` (pipeline-linked) contract regresses nothing.
- **Bonus invariant ENFORCED.** The legacy world permitted duplicate
  definition keys per pipeline (documented F-08 uniqueness, unenforced);
  the new table's live-unique index makes it a save-time error.
- **Certified.** Full `make e2e` green on the sealed tree (archive
  `20261114-053917-shared`, EXIT=0 — the seal on the FINAL, migration-trimmed
  tree; an intermediate seal `20261114-050448-shared` preceded the trim) —
  e2e 15 (external sourcing) + 01/07 (runs) untouched; scenario 02's wait
  hardened to a wide-budget fixed string (a knife-edge 30-ms miss on a
  cold-boot run exposed the flat 20 s default; the property under test —
  REST status 6 — is unchanged).

(The INTERIM MITIGATION's query clause and the start guard described above
were the two special-casings; both are obsolete with the split.)

### 2. F-16 "phantom" premature concurrency-slot release — ROOT CAUSED & FIXED (2026-11-11)

**History.** Parked 2026-11-10 as a load-triggered Heisenbug: a RUNNING job's
concurrency slot seemingly released while it still ran (a queued job on the
SAME key admitted seconds after being held), three sightings, never
instrumented. Tripwire: `scripts/e2e/12-metrics.sh` §2d's hard assertion
(`queued.finished_at >= holder.started_at + 60 s`).

**ROOT CAUSE (witnessed 2026-11-11, quiet standalone run, two rounds).** The
slot was never released early — **the queue job was started WITHOUT EVER
PAYING FOR IT.** The toll was charged only on the SCHEDULER's dispatch path
(`tryAdmit` → `AcquireJobSlots`); the agent's PULL path
(`ListPendingByGroup` offers every pending job → `StartJobExecution`) knew
nothing of admission, and `StartJobExecution` never checked
`concurrency_admitted` — it happily created the execution and flipped the
job to running. Witnessed rows: queue jobs started 1–6 s into the holder's
60 s sleep with `concurrency_admitted=0, admitted_at=NULL`, while the
scheduler's own log showed it had HELD them (`blocked_by=[key]`). The
"scheduling" was the agent's 1 s poll sniping the job. (Collateral casualty:
the tripwire itself had been theater — the API's object-form `Timestamp`
marshaling shattered the one-liner parsers, and the resulting empty-operands
comparison ERRORED, which the enclosing `if` swallowed as false; repaired
with a strict `ts_epoch` evaluator that refuses unusable timestamps.)

**FIX (landed 2026-11-11).** The toll moved INTO the start:
`StartJobExecution` charges the job's slots inside its own transaction
(shared core `acquireSlotsTx`, idempotent when already admitted — the
scheduler's pre-admission or a fan-out sibling pays once), so NO origin
(scheduler push, agent pull, rerun) can start a job unadmitted. A blocked
start declines `Started:false` (the job stays pending; the admission loop's
push delivers promptly on release, and the agent's poll retries harmlessly).
The duplicate-episode guard was pulled into the same transaction (atomic
with the insert). Regression-locked by
`TestStartJobExecutionPaysTheConcurrencyToll` (holder pays → sharer declined
while held → sharer admitted after release). Doctrine corrected in
AGENTS.md (the admission loop is no longer "the single dispatch path").

**Residual caveat.** True premature RELEASE (a slot freed while the holder
still runs) remains theoretically possible (release-path idempotence is not
yet proven under adversarial races) — but every sighting to date is explained
by the uncharged-admission flaw above. The 12-metrics tripwire stays armed;
a future breach with `concurrency_admitted=1` on the violator would indict
the release path instead.

### 3. Shell steps honour cancellation LAZILY (grandchildren hold the pipe) — PARKED (2026-11-11)

**Symptom (witnessed 2026-11-11, F-30.4 Slice 2 e2e authoring).** Cancelling
a running `shell` step whose command forks descendants (e.g.
`sh -c "...; sleep 4"`) does NOT stop the step promptly: the step's
`exec.CommandContext` kills the DIRECT child (the `sh`), but the forked
descendants SURVIVE (different process group; POSIX `CommandContext` kills
only the immediate child) and KEEP HOLDING THE STEP'S STDOUT PIPE, so the
handler's `cmd.Wait()` blocks until the last descendant exits naturally.
Witnessed: cancellation relayed at t+~1 ms; the step unwound only at the
sleeper's NATURAL expiry (~4 s later). Collateral effects: the worker's
terminal report lands after the job is already `cancelled` in the DB and is
rejected (`PermissionDenied: job is no longer running; its token is
invalid` — the token validity gate honors the terminal state), leaving the
job's status to the API's reconciliation (correct outcome, noisy path); and
the agent books the child as `died abnormally` (exit code 3 is the worker's
HONEST classification — the job WAS cancelled — but the report never
arrived).

**Impact.** Functional CORRECTNESS HOLDS (the job ends cancelled; the
report's rejection is by-design F-05 immunity). What is lost: PROMPTNESS
(cancellation latency = the longest-surviving descendant's remaining wall
clock) and cleanliness (duplicate/confusing terminal noise). Long-descendant
steps (compilers spawning workers, `tee`s, daemons) amplify it.

**Mitigation shipped (2026-11-11).** None beyond the e2e disclosure
(`scripts/e2e/26-supervision-channel.sh` uses a SHORT sleeper and flags the
wart). The agent's cancellation REMAINS non-destructive (relayed notice +
poll authority) — the fix below is about the STEP reacting faster, not the
agent forcing it.

**Proposed fix (THIS ITEM).** Kill the step's ENTIRE PROCESS GROUP on
cancellation/timeout: put the shell in its own group (POSIX `SysProcAttr{
Setpgid: true }`; Windows `CREATE_NEW_PROCESS_GROUP`), and on context
expiry walk `/proc` (or `syscall.Kill(-pgid, SIGKILL)`) to terminate the
whole group before `Wait`. Portability care: Windows has no pgid-equivalent
primitive (would need Job Objects — `AssignProcessToJobObject` with
`KILL_ON_JOB_CLOSE`); factor the platform primitive behind a tiny interface
so the unix leg ships now and the Windows leg follows. Blast radius:
`internal/stephandlers/shell.go` only (the handler already distinguishes
`ctx.Err()==DeadlineExceeded` for the timeout path).

Deferred BECAUSE: it is orthogonal to F-30.4's topology goals (the channel
works identically; the wart merely stretches the cancel demo's wall clock),
touches the cross-platform requirement (Windows Job Object leg), and risks
changing behaviour of LONG-DESCENDANT steps that operators may rely on
today (a daemon that SHOULD survive its launcher would now be collected).

### 4. Duplicate agent names clash destructively (twins sabotage each other) — PARKED (2026-11-12)

**Situation (analysed 2026-11-12).** The agent name is the SOLE identity
axis across registration, credential binding (F-30.3), the API's
`watchers`/`live` maps, execution attribution, step-barrier coordinates,
the supervision channel's resume-admission lane (F-30.4), fan-out
derivation, audit, and metric labels. Two agents registering the same
name in the same group (e.g. two k8s pods sharing an `agent_name`
environment variable for "HA") are treated as ONE entity, and the
interaction is destructive:

- **Push-lane flapping.** `WatchJobs` keys `s.watchers` by name; the second
  twin EVICTS the first stream (the "replace the stale one" branch), the
  victim sees the close, reopens, and evicts the winner — endless
  ping-pong, and during each gap neither twin reliably receives nudges.
- **Pull-lane race.** Both twins poll `ListPendingJobs(group)` every second
  and call `StartJobExecution(name=N)`. Deduplication is per-process only
  (the launch ledger); across processes the unique ACTIVE-coordinate index
  (F-30.4 Slice 3, landed 2026-11-12) REFUSES a second ACTIVE execution for
  the same coordinate — and twins share the coordinate (same name!), so the
  DATA-INTEGRITY face is now covered: the first twin's start wins, the
  second's is declined routinely (episode guard, or the index on the raced
  edge). Likewise the register-time BLIND `AbandonAgentExecutions` was
  RETIRED in Slice 3 (the agent is the oracle of its own lineage — it adopts
  orphans, it does not burn them), so the registration-storm face softened:
  a twin's registration no longer destroys the sibling's running
  executions. WHAT REMAINS (and keeps this item open): the push-lane
  eviction FLAPPING (watchers keyed by name), the attribution MERGING
  (one logical seat, inseparable forensics), and the metric-label COLLISION
  (identical `cdrom_agent_*` series).
- **Registration storms.** `RegisterAgent` UNCONDITIONALLY calls
  `AbandonAgentExecutions(N)` (restart semantics: "will not resume what it
  ran before"). Either twin (re)registering burns the OTHER twin's running
  executions — marked failed, job-status re-derived. Under liveness-driven
  pod restarts this cycles indefinitely.
- **Distortions.** Fan-out derivation collapses two physical seats into one
  logical seat (`alive[…]` keyed by name); attribution merges both
  histories into one person (forensics impossible); metric series share
  identical labels (conflicting Prometheus samples); the supervision
  channels scatter telemetry across two supervisors arbitrarily.

Disjoint clusters (separate databases) do not interfere functionally
(isolated universes); only aggregate observability blurs.

**Doctrine (half-stated).** Names must be globally DISTINCT and STABLE.
STABLE is covered by the F-30.4 NAME STABILITY landmark (pin `agent_name`
where hostnames churn); DISTINCT is the unstated clause. The hostname
default REDUCES accidental clashes but does not prevent them (cloned
images with static hostnames). Because credentials are FILES that can be
copied, two pods can legitimately hold IDENTICAL credentials for the same
name — the system cannot CRYPTOGRAPHICALLY distinguish twins, so the cure
is POLICY (enforcement + loud refusal), not stronger signing.

**Proposed fix (THIS ITEM).**
- PRIMARY (safe, cheap): **refuse to STEAL a live name.** `RegisterAgent`
  checks the incumbent's liveness (`s.live[name].lastSeen` younger than ≈3×
  the heartbeat interval): fresh incumbent ⇒ `FailedPrecondition` "name is
  taken by a live agent" (visible at BOOT, not as downstream carnage);
  stale incumbent ⇒ today's eviction + takeover. Tune: the staleness
  margin must exceed crash-loop restart windows (else a genuinely crashed
  agent starves its own replacement).
- SECONDARY (cosmetic, optional): tag the `Agent` row with an incarnation
  `generation` (fresh UUID at registration) surfaced in audit/UI/(perhaps)
  metric labels, distinguishing twins' HISTORIES where they lawfully
  coexist (after the fix, only across disjoint lifespans). Buys no SAFETY
  (does not attribute a live twin's actions to the right seat) — purely
  forensic.
- COMPLEMENT: the Slice 3 unique index on `job_executions` removes the
  DATA-INTEGRITY face (double executions); this item removes the
  CONTROL-PLANE faces (flapping, abandonment storms). Together, duplicate
  names degrade to a boot-time error.

**Acceptance idea.** Twin A live; twin B (same name/group) attempts to
register ⇒ B FAILS LOUDLY (distinct error naming the incumbent), A
undisturbed (stream intact, executions not burned). Kill A; after the
staleness margin B retries ⇒ B succeeds and takes over (eviction +
abandonment behave as today). The single-name e2e fleet stays green.

Deferred BECAUSE: orthogonal to the F-30.4 slices; engages only in a
configuration the doctrine already forbids; the enforcement margin
interacts with crash-loop restart dynamics (needs a dedicated e2e timing
fixture); and the worst data-integrity face is already retired by the
Slice 3 unique index.

## Checklist

- [x] Definitions separated into their own table (§1, LANDED 2026-11-14 — `job_definitions` + promotion migration + special-casing retired)
- [x] F-16 "phantom" diagnosed + start-time toll enforced (§2, 2026-11-11; residual release-path caveat noted therein)
- [ ] Shell steps kill their process group on cancel/timeout (§3, 2026-11-11; unix leg + Windows Job Object)
- [ ] Duplicate agent names refused while live / distinguished by incarnation (§4, 2026-11-12; complemented by the Slice 3 unique index)
