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

Deferred BECAUSE: it is a cross-feature schema change orthogonal to F-30's
goals, and the interim mitigation already removes the practical pain.

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

## Checklist

- [ ] Definitions separated into their own table (§1 future story)
- [x] F-16 "phantom" diagnosed + start-time toll enforced (§2, 2026-11-11; residual release-path caveat noted therein)
