# Cdrom — Feature Roadmap

This document catalogs the common features of a continuous delivery (CD)
platform that Cdrom should implement. It is written as an **implementation
roadmap**: each feature is self-contained, scoped to the concrete components in
this codebase, and given testable acceptance criteria so it can be picked up
and built in isolation.

> **How to use this doc.** This file is the **summary index**: it holds the
> baseline, the per-phase feature list, the suggested build order, and the
> implementation checklist. Each feature's full spec (what/why/scope/acceptance
> criteria/design decisions) lives in its own file under [`features/`](features/).
> When a feature is implemented, tick it in the checklist below and note any
> design decisions that should be folded back into `AGENTS.md` /
> `Architecture.md`.

## Current state (what already exists)

Before adding features, note the baseline that is already built and working:

| Area | Status |
|------|--------|
| Pipeline model | `Pipeline` (name, description, job definitions with `key`/`needs`) — CRUD via DB service; a pipeline's jobs form a DAG (F-08) |
| Job model | `Job` (status, `target_group`, `key`, `depends_on`, start/finish timestamps) |
| Job lifecycle | `pending → running → succeeded / failed / cancelled / timed_out / skipped` |
| Dispatch | `target_group` set → fan out to live workers; empty → ephemeral K8s agent |
| DAG / dependencies | Jobs in a pipeline declare `needs` (other jobs' `key`s), forming a DAG (F-08); the graph is validated at save time (unique keys, known needs, no cycle), and a run's job instances are dispatched in parallel as their dependencies succeed (a failed dependency skips its dependents, F-06) |
| Execution targets | Long-lived **workers** (register, `WatchJobs` stream, heartbeat) and ephemeral **agents** (`GetJob` → run → report) |
| Job execution | **Real** — a job carries a `JobSpec` (ordered steps; each step's `type` selects a handler, defaulting to the built-in `shell` handler that reads its command/args/shell from the step's `params` and runs them with workdir, env, and a per-step timeout); the worker/agent run the steps in order via `internal/executor` and fail on the first step that errors. A job-level `timeout` bounds the whole job and a per-step `timeout` bounds a step; exceeding either terminates the job as `timed_out` (F-03), and a scheduler watchdog reaps jobs whose target goes silent |
| Retry & re-run | A job's `JobSpec` may carry a `retry` policy (`max_attempts` retries after the initial attempt, `backoff` delay); a scheduler retry loop re-dispatches a failed job up to the limit (a new attempt on the same `Job` row, `attempt` counter, F-04). A user can re-run any finished job (`POST /api/jobs/{id}/rerun`) for a fresh attempt; attempts are visible to the UI ("attempt N of M") |
| Cancellation | Cancelling a running job (`POST /api/jobs/{id}/cancel`) signals the execution target to stop the work (F-05): the API delivers a `JobCancellation` down the worker's `WatchJobs` stream (or an agent observes it on its next `GetJob`), the target interrupts the running step and reports `cancelled`; cancelling a finished job is a no-op |
| Job tokens | Minted per job by the API (via IdP), audience exchange, verified on status/artifact RPCs |
| UI auth | OIDC authorization-code + PKCE; the API verifies the OAuth token as a Bearer token (no session cookie) |
| Artifacts | General-purpose, namespaced file store (streamed upload/download); jobs scope files by job id, other callers (e.g. deployed releases) use their own namespaces; proxied through the API |
| Live events | WebSocket `/api/ws` — `job_status`, `worker`, and `job_log` events |
| Job logs | Streamed from the target to the API (`StreamJobLogs`), persisted to the artifacts service (one file per step + `job.log`), fanned out to the UI as `job_log` events, and retrievable via `GET /api/jobs/{id}/logs[/{name}]` |
| Transport | gRPC everywhere, optional mutual TLS |

The biggest remaining gap: a pipeline's definition is not yet versioned
(F-11). Runs (F-07), the job DAG (F-08), and automatic triggers — cron,
webhook, and event (F-09) — are in place; everything below builds toward
governing and automating them.

---

## Feature index

### Phase 1 — Core execution

Make a job actually do work, and make its execution observable and controllable.
These are the foundation every other feature depends on.

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-01 | Job execution spec | done | [F-01](features/f-01-job-execution-spec.md) |
| F-02 | Job log streaming | done | [F-02](features/f-02-job-log-streaming.md) |
| F-03 | Job timeouts | done | [F-03](features/f-03-job-timeouts.md) |
| F-04 | Retry & re-run | done | [F-04](features/f-04-retry-rerun.md) |
| F-05 | Cancellation propagation | done | [F-05](features/f-05-cancellation-propagation.md) |
| F-06 | `skipped` job and step status | done | [F-06](features/f-06-skipped-status.md) |

### Phase 2 — Pipeline orchestration

Turn a flat list of jobs into a real, triggerable, parameterized pipeline.

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-07 | Pipeline runs (first-class execution entity) | done | [F-07](features/f-07-pipeline-runs.md) |
| F-08 | Job dependencies (DAG) | done | [F-08](features/f-08-job-dependencies.md) |
| F-09 | Triggers | done | [F-09](features/f-09-triggers.md) |
| F-10 | Parameters & variables | done | [F-10](features/f-10-parameters-variables.md) |
| F-11 | Pipeline versioning | done | [F-11](features/f-11-pipeline-versioning.md) |

### Phase 3 — Safety & governance

Control who can do what, protect secrets, and keep an accountable record.

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-12 | Secrets management | done | [F-12](features/f-12-secrets-management.md) |
| F-13 | Approval gates | done | [F-13](features/f-13-approval-gates.md) |
| F-14 | Roles & permissions (RBAC) | done | [F-14](features/f-14-rbac.md) |
| F-15 | Audit log | done | [F-15](features/f-15-audit-log.md) |
| F-16 | Concurrency control & queueing | planned | [F-16](features/f-16-concurrency-queueing.md) |
| F-17 | Environments & deployment targets | planned | [F-17](features/f-17-environments.md) |

### Phase 4 — Delivery & observability

The higher-level CD capabilities that make the platform a complete delivery
tool.

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-18 | Notifications | planned | [F-18](features/f-18-notifications.md) |
| F-19 | Artifact promotion | planned | [F-19](features/f-19-artifact-promotion.md) |
| F-20 | Observability & metrics | planned | [F-20](features/f-20-observability.md) |
| F-21 | Config as code | planned | [F-21](features/f-21-config-as-code.md) |
| F-22 | Post-deploy verification & rollback | planned | [F-22](features/f-22-verification-rollback.md) |

### Phase 5 — High availability

Making the control plane survive pod churn and scale beyond a single API
instance. The full design (event log, hybrid push/pull dispatch, leader
election, cross-pod live logs, shared artifacts store) is in
[`HighAvailability.md`](HighAvailability.md).

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-23 | High-availability control plane | done | [F-23](features/f-23-high-availability.md) |

### Phase 6 — Authentication

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-24 | Username/password authentication | done | [F-24](features/f-24-username-password-auth.md) |
| F-25 | API keys | planned | [F-25](features/f-25-api-keys.md) |
| F-26 | Service accounts | planned | [F-26](features/f-26-service-accounts.md) |
| F-27 | Approval groups (named approver sets) | planned | [F-27](features/f-27-approval-groups.md) |
| F-28 | SCIM user provisioning (automatic user management from an external IdP) | planned | [F-28](features/f-28-scim-provisioning.md) |

---

## Suggested build order

The phases are ordered so each builds on the last. A pragmatic first cut:

1. **F-01 → F-02 → F-03** — a job that does real work, with live logs and a
   timeout. This alone turns the placeholder into a usable executor.
2. **F-05 → F-04 → F-06** — control (cancel, retry) and the `skipped` status.
3. **F-07 → F-08** — the pipeline run + DAG, the heart of orchestration.
4. **F-09 → F-10 → F-11** — triggers, parameters, versioning.
5. **F-12 → F-14 → F-13 → F-15** — secrets, RBAC, approvals, audit.
6. **F-16 → F-17** — concurrency and environments.
7. **F-18 → F-20 → F-21 → F-22 → F-19** — notifications, observability,
   config-as-code, verification/rollback, artifact promotion.
8. **F-23** — high-availability control plane (shared event log, hybrid
   push/pull dispatch, scheduler leader election, cross-pod live logs). See
   [`HighAvailability.md`](HighAvailability.md).
9. **F-24** — username/password authentication (a password sign-in path
   alongside OIDC, proxied to the IdP; enabled by default for local dev).
10. **F-25** — API keys (per-user, per-pipeline-scoped, expiring, rotatable
    credentials presented as `Bearer <username>:<apikey>`; the real logic lives
    in the IdP, proxied by the API; a separate API-key lockout).

11. **F-14 → F-25 → F-26** — service accounts (non-human role-bound
    identities, two individually rotatable Database-generated keys,
    granular management permissions, disable/enable, and irreversible
    soft deletion with both key slots cleared). F-15 supplies audit history.
12. **F-13 → F-14 → F-27** — approval groups (named approver sets an
    `approval` step references instead of individual users; membership
    resolved at decision time; optional role references and N-of-M quorum).
13. **F-24 → F-14 → F-28** — SCIM user provisioning (the corporate IdP
    provisions/deprovisions users and groups into the IdP's user directory;
    users are soft-deleted, never hard-deleted, so audit attribution survives
    deprovisioning; groups map to F-14 roles / F-27 approval groups).

---

## Implementation checklist

Tick each feature off as it lands.

- [x] F-01 Job execution spec
- [x] F-02 Job log streaming
- [x] F-03 Job timeouts
- [x] F-04 Retry & re-run
- [x] F-05 Cancellation propagation
- [x] F-06 `skipped` job status
- [x] F-07 Pipeline runs
- [x] F-08 Job dependencies (DAG)
- [x] F-09 Triggers (cron / webhook / event)
- [x] F-10 Parameters & variables
- [x] F-11 Pipeline versioning
- [x] F-12 Secrets management
- [x] F-13 Approval gates
- [x] F-14 Roles & permissions (RBAC)
- [x] F-15 Audit log
- [ ] F-16 Concurrency control & queueing
- [ ] F-17 Environments & deployment targets
- [ ] F-18 Notifications
- [ ] F-19 Artifact promotion
- [ ] F-20 Observability & metrics
- [ ] F-21 Config as code
- [ ] F-22 Post-deploy verification & rollback
- [x] F-23 High-availability control plane
- [x] F-24 Username/password authentication
- [ ] F-25 API keys
- [ ] F-26 Service accounts
- [ ] F-27 Approval groups (named approver sets)
- [ ] F-28 SCIM user provisioning (external IdP; soft-delete only)
