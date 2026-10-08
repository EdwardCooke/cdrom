# F-13 · Approval gates


> Phase 3 — Safety & governance · [Feature index](../Features.md)


**What.** A job can require a **manual approval** before it proceeds past a
point in its steps. The job pauses in an `awaiting_approval` state at the
approval step until an authorized user approves (or rejects / times out).

**Why.** Production deploys and other high-blast-radius steps are gated behind
a human sign-off.

**Scope.**
- `internal/models` / proto — an `awaiting_approval` job status; an approval
  record on the job (decision, actor, reason, requested/decided timestamps).
- `internal/executor` + `internal/stephandlers` — a built-in `approval` step
  handler that pauses the job at the gate.
- `internal/approval` + `internal/target` — the API-backed approval gate the
  execution target uses to report `awaiting_approval` and poll for the decision.
- `internal/api` — `POST /api/jobs/{id}/approve` / `.../reject` (HTTP) and the
  `CheckApproval` gRPC RPC the target polls.
- `internal/services/scheduler` + `internal/services/database` — persist the
  decision (`ResolveApproval`) and classify `awaiting_approval` in the
  run-status / job-status / watchdog loops.
- UI — an approval prompt (reads `awaiting_approval` + `approval_message` from
  `GET /api/jobs/{id}`, calls approve/reject).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- The gate is a **step** (`type: "approval"`), not a job-level flag: a job
  pauses at the approval step and its later steps run only after approval. The
  step's `message` param is a Go template rendered against the step's condition
  context (secrets, prior steps' outputs, run parameters, upstream jobs, job
  identity) — the same interpolation a step's condition (F-06) and the run's
  spec fields (F-10) use — so a message can reference a secret, a prior step's
  output, or the job's identity.
- The execution target reports the job `awaiting_approval` (with the rendered
  message) via the API's `ReportJobStatus`, then polls the API's `CheckApproval`
  until the gate is resolved. An **approval** succeeds the step (the job
  continues); a **rejection** fails the job; a **timeout** (the step's or the
  job's timeout) reports `timed_out`; a **cancellation** reports `cancelled`.
- `awaiting_approval` is a non-terminal, in-flight status: the run-status loop
  keeps the run `running` while a job is at the gate, and the watchdog reaps a
  gate whose target goes silent. Resolving the gate moves the job out of the
  state (approved → `running`, rejected → `failed`).
- The approval record (decision, actor, reason, timestamps) is persisted on the
  job by the API's `ResolveApproval` (via the scheduler → Database service),
  conditionally (only while the job is still `awaiting_approval`), so a late
  decision is a no-op.

**Acceptance criteria.**
- [x] A gated job does not start (past the approval step) until approved.
- [x] Approval/rejection is recorded (actor + timestamp + optional reason).
- [x] An approval timeout (the step's or job's timeout) fails the job
      (`timed_out`).
- [x] Only authorized actors can approve (pairs with F-14; the actor is the
      authenticated user, recorded on the decision). F-27 extends "authorized"
      to named approval groups, so a pipeline can reference a group instead of
      individual users.
