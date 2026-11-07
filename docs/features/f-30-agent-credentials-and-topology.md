# F-30 · Agent credentials & the agent/agent topology

> Phase 8 — Target credentials & execution topology · [Feature index](../Features.md)

**Overview.** F-30 is large, so it is decomposed into independently-buildable
sub-features, each with its own spec file and sequential slices. This file is
the **overview** and holds the shared motivation, terminology, and
cross-cutting design decisions; the sub-features carry the detailed scope
and acceptance criteria for each piece.

| Sub-feature | Spec | Scope | Depends on |
|-------------|------|-------|------------|
| **F-30.1** Role swap & wholesale rename | [f-30.1](f-30.1-role-swap-and-rename.md) | Behavior-preserving nominal inversion: long-lived target → **agent**, ephemeral one-job executable → **worker** — binaries (contents swapped), packages, models, protos/RPCs, config keys, RBAC areas, WS events, metrics, docs. Registry reset on upgrade accepted (no backwards compat) | — |
| **F-30.2** Agent credential model & lifecycle | [f-30.2](f-30.2-agent-credential-model.md) | The issuing/tracked side: `AgentCredential` row (DB-owned, IdP-signs), JWT claims (`sub: agent:<name>`, `jti` = row id, placeholder `exp`), issue/list/renew(self)/rotate/revoke endpoints, TTL + renew-ahead config, lapse semantics, audit | F-30.1 |
| **F-30.3** Target-surface auth & transport | [f-30.3](f-30.3-target-surface-auth.md) | The consuming side: mandatory bearer-credential on every target RPC, shared JWKS + row-liveness verifier (short-TTL cache), `RegisterAgent` name binding, stream revalidation/teardown, agent↔API over TLS+JWT (client certs off the leg; mesh keeps mTLS) | F-30.1, F-30.2 |
| **F-30.4** Agent subprocess & supervision channel | [f-30.4](f-30.4-agent-supervision.md) | The topology change: agent launches per-job worker subprocesses (env-only mandate), worker claims payload from the API with the job token (`StartJobExecution` slims to token-only; `GetJob` mint-on-call retired), loopback `Signals`/`Telemetry` channel, `ReportChildDeath`, parallel children + per-coordinate uniqueness (ledger + unique index) | F-30.1, F-30.3 |

**Suggested build order:** F-30.1 → F-30.2 → F-30.3 → F-30.4.

Rationale: the rename lands FIRST so every later spec speaks the final
vocabulary and its diff contains mechanisms only. F-30.2 + F-30.3 together
deliver the complete credential capability (issuing side, then consuming
side) and operate with the CURRENT process topology intact — i.e., the
original "phase 1" stands alone: agents authenticate with tracked,
revocable JWTs before anyone spawns a subprocess. F-30.4 is the original
"phase 2": the role swap's mechanical payoff. Within each sub-feature the
slices are strictly sequential checkpoint commits (compiled, tested,
demonstrable).

---

**What.** Two coupled goals:

1. **Tracked, revocable identity for execution targets.** An administrator
   (via the upcoming UI, or the API directly) issues an agent credential —
   an IdP-signed JWT wrapping a tracked database row — for a named agent;
   the plaintext is returned once. The agent presents it on every target RPC;
   the API verifies signature (JWKS) + liveness (the row), so revocation
   bites within one heartbeat without disturbing running jobs. Credentials
   renew (self-service, ahead of expiry), rotate, and revoke; the agent↔API
   transport drops mutual TLS in favor of ordinary TLS + bearer JWT while
   the internal mesh keeps mTLS. *(F-30.2, F-30.3.)*
2. **Portable, parallel execution topology.** The long-lived **agent**
   supervises ephemeral per-job **worker** subprocesses: it acquires the
   execution and job token, spawns the worker with the mandate in the
   environment only, and babysits (liveness, opportunistic cancel relay,
   death reporting, telemetry roll-up). The worker claims its payload from
   the API with the job token and executes talking directly to the API.
   Multiple workers run in parallel per agent; duplication safety rests on
   `(job, agent, attempt)` coordinate uniqueness. The env-var handoff
   contract is infrastructure-neutral by design — a future Kubernetes agent
   plays the same role by writing Pod env instead of forking a process.
   *(F-30.4.)*

**Terminology (canonical, post-F-30.1).**

- **Agent** — the long-lived, registered execution target. Resident on a
  deployment host, member of an **agent group**, declares environments
  (F-17) and labels (F-29), holds the `WatchJobs` stream, heartbeats, pulls
  pending jobs, and supervises worker subprocesses (F-30.4). Authenticates
  to the API exclusively with an **agent credential** (F-30.2/F-30.3).
  Pre-swap: this process was called the *worker*.
- **Agent** — the ephemeral per-job subprocess (later: pod) that executes
  exactly one job. Mandate via environment variables (F-30.4); payload
  claimed from the API with its job token; talks to the agent only over the
  loopback supervision channel. Reports its own status; terminates at the
  job's terminal state. Pre-swap: this process was called the *agent*.
- **Agent credential** — the durable, tracked JWT identifying an agent to
  the API (F-30.2). Bound to exactly one agent name.
- **Job token** — unchanged in KIND: the per-job RS256 JWT minted by the IdP,
  scoped to one job, presented by the worker on job-scoped RPCs. Its
  ORIGINATION tightens: minted solely at the agent's credentialed
  `StartJobExecution` (F-30.4 retires `GetJob`'s mint-on-call).

**Why.**

- **Bootstrap & revocation.** Today a target's identity is its transport
  credential (a mesh client cert, or nothing on plaintext): provisioning a
  new machine copies somebody's cert, and there is no way to evict one. A
  tracked JWT gives the UI/admin a first-class "issue a token for this
  machine" flow and instant, auditable revocation.
- **Outside-the-cluster targets.** Deployment hosts sit outside the service
  mesh; handing them mesh client certificates is exactly the friction this
  removes. Ordinary TLS + bearer JWT is the portable norm.
- **Portability of the ephemeral half.** A process fork couples the executor
  to the host OS's process model. Defining the agent→worker handoff as
  *environment variables + loopback gRPC* means a Kubernetes agent can
  fulfill the identical contract by emitting a Pod spec, swapping
  supervisors without touching the worker or the API.
- **Parallelism per host.** One-resident-process-one-job-at-a-time wastes
  capacity on multi-core hosts; per-job subprocesses isolate faults (a hung
  step dies with its process) and admit concurrency safely.

**Charter.** There is **no backwards compatibility** requirement anywhere in
F-30: wire names, config keys, and terminology change outright; upgrades
reset the agent registry (F-30.1 documents the treatment).

**Cross-cutting design decisions (binding on all sub-features).**
- **Two credential regimes, orthogonal by verb.** Agent credentials
  authorize TARGET-SURFACE verbs (register/watch/heartbeat/start/…); job
  tokens authorize JOB-SCOPE verbs (report/barrier/approval/logs/artifacts).
  No verb accepts both — a leaked job token compromises one job; a leaked
  agent credential compromises one agent and is instantly revocable.
- **Signature proves issuance; the row decides life.** Every tracked
  credential (here: agent credentials; cf. F-25/F-26) separates the crypto
  custody proof from the mutable liveness state, so revocation is a row
  mutation propagated by a short cache, not a key ceremony.
- **The agent never hydrates.** Spec and secrets travel directly between API
  and worker over their own connection; the agent's footprint is
  coordination-only (acquisition, spawn, supervision, roll-up).
- **Serial per coordinate, not globally.** Duplication safety (the
  2026-11-07 flake lesson) rests on `(job, agent, attempt)` uniqueness —
  ledger primary, unique index defensive — enabling per-host parallelism.
- **Whole-word rename, no aliases.** Half-renamed vocabularies (two meanings
  of "worker") are worse than churning generated code once.

**Open items.**
1. **Upgrade-data treatment** (raised by F-30.1) — accept the registry reset
   (charter-consistent, recommended) vs. ship the copy-forward SQL as a
   tested migration script.
2. **Max cumulative lifetime** — renewals currently extend without bound
   (revocation is the brake). Optional `max_age` cap on the row? Recommend:
   defer.
3. **Supervision-channel encryption** — loopback plaintext suffices for v1
   (F-30.4); revisit if containers share a netns with untrusted peers.
4. **`ReportChildDeath` validation** (F-30.4) — confirm the API-side check
   (the presenting agent's group actually serviced the job) before treating
   it as a status oracle.
