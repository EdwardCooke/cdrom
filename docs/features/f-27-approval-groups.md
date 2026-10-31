# F-27 · Approval groups (named approver sets)


> Phase 6 — Authentication · [Feature index](../Features.md)


**What.** Named **approval groups** — sets of users that a pipeline's
`approval` step (F-13) can reference — so a pipeline declares *who may
approve* by group instead of hard-coding individual users. An approval step
gains an `approvers` param: a list of references, each a **group name**
(e.g. `group:release-managers`), an **F-14 role** (`role:ops-oncall`), or an
**individual user** (email/subject) — plus an optional `quorum` param (K)
requiring K distinct approvals. Groups may also pull members straight from an
IdP claim. The gate is authorized for the union of every member of the
referenced groups (including claim-derived members), everyone holding a
referenced role, and the named individuals; a decision is accepted only from a
user in that set who also holds the F-14 `jobs.can-approve` /
`jobs.can-reject` permission, and under quorum the gate clears only after K
distinct such approvals (any rejection fails it at once).

**Why.** Today an approval gate is authorized purely by F-14 permission, so a
pipeline can't say "only the release managers may approve this prod deploy"
without making them all admins or enumerating users in the pipeline. Teams
reorganize — people join and leave a team — and a pipeline that names
individuals goes stale. A group is a stable, centrally-managed handle the
pipeline points at; membership changes propagate without touching any pipeline
or version snapshot.

**The model.**
- **ApprovalGroup** — a named set of users: a unique `name`, a `description`,
  and a `members` list (user subjects/emails), plus an optional claim
  derivation. It is a set of *people*, distinct from an F-14 **role** (a set
  of *permissions*); the two bridge via `role:<name>` references in
  `approvers` and via claim-derived membership (requirements below). Stored in
  the Database service (like the IdP's user directory, F-24) so every API
  replica shares the same groups.
- **`approvers` param** on an `approval` step — a list of references. A
  reference is `group:<name>`, `role:<name>`, or a bare user identifier. When
  the param is absent or empty, the gate falls back to F-13 behavior: anyone
  with the relevant F-14 permission may decide (backward compatible).
- **`quorum` param** on an `approval` step — an integer K ≥ 1 (default 1).
  Under K > 1 the gate accumulates distinct approvals before passing; any
  single rejection fails it outright.
- **Pipeline default approvers** — a pipeline-level `approvers` (and optional
  `quorum`) declaration inherited by every `approval` step that does not set
  its own, merged into the per-run spec snapshot at compile/save time.
- **Authorization at decision time** — when a user calls approve/reject, the
  API resolves the gate's authorized set (members of each referenced group,
  including any claim-derived members ∪ everyone holding a referenced role ∪
  named individuals) and accepts the decision only if the actor is in that set
  *and* holds the F-14 permission. Membership is resolved when the decision
  arrives, not at dispatch, so adding or removing a member, changing a claim
  derivation, or re-binding a role takes effect for gates already waiting.

**Requirements** (core + extensions):
- **Core**: named groups of people; an `approvers` param referencing groups
  and/or individuals; decision-time authorization (membership ∩ F-14
  permission); backward-compatible fallback when `approvers` is absent.
- **Reference a role** — an `approvers` entry of `role:<name>` authorizes
  everyone currently holding that F-14 role (whether the role came from the
  token's `roles` claim, the configured JWT claim mapping, or a stored role
  binding), bridging groups and roles without maintaining a separate member
  list. Checked at decision time against the actor's effective roles, so
  assigning/removing the role propagates to waiting gates.
- **N-of-M quorum** — a `quorum` param (integer K ≥ 1, default 1) makes the
  gate pass only after K *distinct* authorized approvers have approved;
  intended for high-blast-radius prod gates ("two of the release managers must
  sign off"). A rejection by any authorized actor still fails the job
  immediately regardless of K.
- **Membership from an IdP claim** — a group may additionally declare a claim
  derivation ({claim, value}); its effective membership is the union of its
  manually maintained `members` and every registered principal whose token
  carries that claim value. Pairs with F-14's claim mapping: because that
  mapping already surfaces external-IdP group claims as roles, a
  `role:<mapped-role>` reference achieves the same tracking for externally
  provisioned teams; the native claim derivation complements it for setups
  that keep the raw claim accessible.
- **Pipeline default approvers** — a pipeline-level default `approvers` set
  (plus optional default `quorum`) that an `approval` step inherits unless it
  overrides, so a team sets its approvers once per pipeline rather than per
  step. Override is wholesale: a step that sets `approvers` ignores the
  pipeline default (same for `quorum`). Merged at compile/save time into the
  per-run spec snapshot (F-11), so a run's gates reflect the defaults in force
  when the run started.
- **UI group management + prompt** — a screen to create/edit groups and their
  members (plus role references and claim derivations); the approval prompt
  (F-13) shows which group(s)/individuals/role(s) the gate is waiting on and,
  under quorum, how many distinct approvals are in.

**Scope.**
- `internal/models` — an `ApprovalGroup` entity (name, description, members
  JSON, plus a claim-derivation field) and an `ApprovalVote` entity (job id,
  step name, actor, decision, timestamp — the quorum ledger) registered in
  `All()`.
- `proto/cdrom/db/v1/db.proto` + `internal/services/database` — approval-group
  CRUD RPCs (create/get/list/update/delete), atomic member add/remove RPCs,
  a `ListApprovalGroupMembers` (or fold members into the group message) the
  API uses to resolve a gate's authorized set, and `RecordApprovalVote` /
  `ListApprovalVotes` (vote-ledger writes are idempotent per actor × step ×
  decision, guarded by the job still being `awaiting_approval`).
- `internal/pipelinefile` — recognize the pipeline-level `approvers` / `quorum`
  defaults and merge them into each `approval` step's params at compile/save
  time (wholesale override when the step sets its own), so the per-run spec
  snapshot (F-11) carries the merged values.
- `internal/stephandlers/approval.go` — read the `approvers` and `quorum`
  params and carry the references onto the gate: `executor.Approval`'s
  `ReportAwaitingApproval` gains the references and the API's `ReportJobStatus`
  proto carries them; the job row stores them beside `approval_message`,
  overwritten each time a step enters a gate (only one gate is active per job
  at a time, so a single field suffices). The handler still reports
  `awaiting_approval` and polls; it does not itself authorize — the API does.
  Members are opaque strings matched against the deciding actor's subject /
  email; they are **not** validated against the user directory at write time,
  so an unknown or departed member degrades gracefully (it simply never
  matches) and the claim-derived membership fits the same shape.
- `internal/approval` + `internal/api` — the gate carries the `approvers`
  references and the quorum depth; `resolveApproval` (F-13) resolves the
  authorized set (group members ∪ claim-derived members ∪ role holders ∪
  individuals), rejects a decision from an actor outside it (403) or lacking
  the F-14 permission, and under quorum records the vote and flips the gate
  resolved only when K distinct approvals accumulate (a rejection resolves it
  immediately).
- `internal/services/scheduler` — `ResolveApproval` validates the actor against
  the gate's authorized set before persisting the decision; under quorum it
  delegates to the vote ledger and computes the resolved transition.
- `internal/api/server.go` — `GET/POST /api/approval-groups`,
  `GET/PUT/DELETE /api/approval-groups/{name}`, plus incremental membership
  ops `POST /api/approval-groups/{name}/members` and
  `DELETE /api/approval-groups/{name}/members/{member}` (gated by an F-14
  permission, e.g. `roles.can-manage` or a new `approvals.can-manage`);
  enforce the membership + permission check in `resolveApproval`. The job's
  REST view exposes the stored `approvers` references and quorum progress so
  the UI prompt can name the waiting group(s)/individuals/role(s).
- `ui/` — approval-group management (create/edit, member list, role
  references, claim derivation) and an approval prompt that names the waiting
  party(ies) and, under quorum, shows approvals collected vs. required.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **A group is people; a role is permissions.** F-14 roles grant *what you may
  do*; an approval group names *who may act on a specific gate*. They are
  separate concepts that compose: the actor must be in the gate's authorized
  set (group member, claim-derived member, role holder, or named individual)
  **and** hold the F-14 permission. This keeps a
  pipeline's approval policy ("release managers") independent of platform-wide
  RBAC, and lets the same group be referenced by many pipelines.
- **Membership is resolved at decision time, not dispatch time.** The gate
  stores the `approvers` references (group names, role names, individuals),
  not a frozen member list, so a member added or removed mid-flight, a claim
  derivation changed, or a role reassigned is reflected the next time a
  decision is checked. This mirrors how F-14 role/binding changes take effect
  on every replica without a restart.
- **Backward compatible with F-13.** No `approvers` param → the existing
  behavior (anyone with the F-14 permission decides). Adding the param is
  opt-in per step, so existing pipelines and version snapshots are unaffected.
- **Incremental membership endpoints.** Managing a group is mostly
  one-person-at-a-time churn (someone joins or leaves a team), so the API
  offers add/remove-single-member endpoints backed by **atomic add/remove
  RPCs in the Database service** (a transactional mutation of the members
  column, not an API-side read-modify-write) — the same pattern as F-26's
  `POST/DELETE /api/service-accounts/{id}/roles[/{role}]`. The full-
  replacement `PUT` stays for bulk edits and import/export. Role references
  and claim derivations attach as separate fields with their own add/remove
  endpoints rather than widening `members` into heterogeneous entry kinds,
  keeping the core shape uniform and the "a group is people; a role is
  permissions" separation intact.
- **Groups live in the Database service; the API resolves and enforces.** Group
  rows are owned by the Database service (shared across replicas, F-24-style);
  the API reads members when a decision arrives and is the only place that
  authorizes, so the check is identical on every replica.
- **Quorum is a gate property, not a step re-run.** When `quorum: K` (> 1) is
  set the gate tracks distinct approving actors in an immutable vote ledger
  (`ApprovalVote` rows: job id, step name, actor, decision, timestamp) and
  reports resolved only at K; a rejection by any authorized actor still fails
  the job immediately. Votes are appended, never rewritten, so a retried or
  re-entered gate recounts deterministically and a duplicate approval by the
  same actor does not advance the count twice. The ledger is consulted at
  decision time, consistent with the decision-time-membership principle.
- **Role references resolve against the actor's effective roles.** A
  `role:<name>` entry authorizes the actor iff the actor's effective role set
  (token `roles` claim ⊕ configured claim mapping ⊕ stored role bindings, the
  same composition F-14 evaluates) contains the referenced role. Resolution is
  therefore per-actor at decision time — no need to enumerate every principal
  holding the role — and picking up a role (or losing one) reflects on waiting
  gates instantly, like membership changes.
- **Claim-derived membership piggybacks on the role machinery.** Because F-14's
  claim mapping already elevates external-IdP claim values into roles, a
  `role:<mapped-role>` reference covers the dominant "track the corporate
  IdP" case natively. Native claim derivation (a group declaring
  {claim, value}) adds lazy computation of members from registered principals'
  tokens for configurations that retain raw claims; both forms contribute to
  the group's effective membership at decision time and neither freezes a
  roster.

**Acceptance criteria.**
- [x] A pipeline's `approval` step can reference a named group; a member of
      that group can approve, and a non-member gets 403.
- [x] `approvers` can mix group names and individual users; the authorized set
      is their union (a member of any referenced group or a named individual
      may decide).
- [x] With no `approvers` param, behavior is unchanged from F-13 (anyone with
      the F-14 `jobs.can-approve` / `jobs.can-reject` permission decides).
- [x] A decision requires both group/individual membership **and** the F-14
      permission (a group member without the permission is still rejected).
- [x] Adding or removing a group member takes effect for a gate already
      `awaiting_approval` (membership resolved at decision time).
- [x] Approval groups are manageable (create/read/update/delete) via the API
      and UI, gated by an F-14 permission; group mutations are audited (F-15).
- [x] The UI approval prompt shows which group(s)/individuals/role(s) the
      gate is waiting on and, under quorum, how many distinct approvals are
      collected vs. required.
- [x] `role:<name>` in `approvers` authorizes everyone currently holding that
      F-14 role (token-stamped, claim-mapped, or bound); gaining or losing the
      role takes effect for a gate already `awaiting_approval`.
- [x] `quorum: K` (> 1) passes the gate only after K *distinct* authorized
      approvers have approved; a duplicate approval by the same actor advances
      the count at most once; a rejection by any authorized actor fails the
      job immediately regardless of K.
- [x] A group with a claim derivation contributes, to its effective
      membership, every registered principal whose token carries the declared
      claim value (on top of its manual members); the union is honored at
      decision time.
- [x] A pipeline-level default `approvers` set (and optional default
      `quorum`) is inherited by an `approval` step that sets neither, and
      wholly replaced by a step that sets its own; the merged values are baked
      into the per-run spec snapshot.
