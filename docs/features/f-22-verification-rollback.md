# F-22 · Post-deploy verification & rollback


> Phase 4 — Delivery & observability · [Feature index](../Features.md)


**What.** After a deploy job, run **health checks** (readiness probes / a
verification step) to confirm the release is healthy; if it fails, support
**rollback** to the previous known-good release.

**Why.** Deploying is not done until the system is verified healthy, and a bad
release must be reversible.

**Scope.**
- Job spec — a verification step / health-check definition (pairs with F-01).
- `internal/models` / proto — a "current release" record per environment (what
  is live now, what was previous) to enable rollback.
- `internal/services/scheduler` — run verification after deploy; on failure,
  offer/trigger a rollback run.
- Ties to F-17 (environments).

**Acceptance criteria.**
- [ ] A deploy job can declare a post-deploy verification step that gates the
      run's success.
- [ ] The current release per environment is tracked.
- [ ] A rollback produces a run that restores the previous release.
