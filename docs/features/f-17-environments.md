# F-17 · Environments & deployment targets


> Phase 3 — Safety & governance · [Feature index](../Features.md)


**What.** First-class **environments** (e.g. dev, staging, prod) that group
targets and carry policy (e.g. prod requires approval — ties to F-13). A job
targets an environment, which resolves to the appropriate worker group / agent
pool.

**Why.** "Deploy to staging" vs "deploy to prod" is the core CD distinction,
and prod usually has stricter gates.

**Scope.**
- `internal/models` / proto — an `Environment` entity (name, target group or
  agent pool, policy flags).
- Job spec — target an environment (in addition to / instead of a raw
  `target_group`).
- `internal/services/scheduler` — resolve environment → dispatch target; apply
  environment policy (approval gate).

**Acceptance criteria.**
- [ ] A job targeting an environment is dispatched to that environment's
      targets.
- [ ] An environment marked "requires approval" inserts an approval gate
      (F-13) before dispatch.
- [ ] Environments are manageable (CRUD) and visible in the UI.
