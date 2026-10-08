# F-27 · Approval groups (named approver sets)


> Phase 6 — Authentication · [Feature index](../Features.md)


**What.** Named **approval groups** — sets of users that a pipeline's
`approval` step (F-13) can reference — so a pipeline declares *who may
approve* by group instead of hard-coding individual users. An approval step
gains an `approvers` param: a list of references, each either a **group name**
(e.g. `group:release-managers`) or an **individual user** (email/subject). The
gate is authorized for the union of every member of the referenced groups plus
the named individuals; a decision is accepted only from a user in that set who
also holds the F-14 `jobs.can-approve` / `jobs.can-reject` permission.

**Why.** Today an approval gate is authorized purely by F-14 permission, so a
pipeline can't say "only the release managers may approve this prod deploy"
without making them all admins or enumerating users in the pipeline. Teams
reorganize — people join and leave a team — and a pipeline that names
individuals goes stale. A group is a stable, centrally-managed handle the
pipeline points at; membership changes propagate without touching any pipeline
or version snapshot.

**The model.**
- **ApprovalGroup** — a named set of users: a unique `name`, a `description`,
  and a `members` list (user subjects/emails). It is a set of *people*,
  distinct from an F-14 **role** (a set of *permissions*); the two overlap only
  in that a group's membership can be derived from a role (a nice-to-have
  below). Stored in the Database service (like the IdP's user directory, F-24)
  so every API replica shares the same groups.
- **`approvers` param** on an `approval` step — a list of references. A
  reference is `group:<name>` or a bare user identifier. When the param is
  absent or empty, the gate falls back to F-13 behavior: anyone with the
  relevant F-14 permission may decide (backward compatible).
- **Authorization at decision time** — when a user calls approve/reject, the
  API resolves the gate's authorized set (members of each referenced group ∪
  named individuals) and accepts the decision only if the actor is in that set
  *and* holds the F-14 permission. Membership is resolved when the decision
  arrives, not at dispatch, so adding or removing a member takes effect for
  gates already waiting.

**Nice-to-haves** (build after the core works):
- **Reference a role** — an `approvers` entry of `role:<name>` authorizes
  everyone currently holding that F-14 role, bridging groups and roles without
  maintaining a separate member list.
- **N-of-M quorum** — a `quorum` param (K) makes the gate pass only after K
  *distinct* authorized approvers have approved, for high-blast-radius prod
  gates ("two of the release managers must sign off").
- **Membership from an IdP claim** — a group whose members are auto-derived
  from a claim value (e.g. everyone whose `groups` claim includes
  `release-managers`), so membership tracks the corporate IdP without manual
  edits (pairs with F-14's claim mapping).
- **Pipeline default approvers** — a pipeline-level default `approvers` set
  that an `approval` step inherits unless it overrides, so a team sets its
  approvers once per pipeline rather than per step.
- **UI group management + prompt** — a screen to create/edit groups and their
  members; the approval prompt (F-13) shows which group(s)/individuals the gate
  is waiting on.

**Scope.**
- `internal/models` — an `ApprovalGroup` entity (name, description, members
  JSON) registered in `All()`.
- `proto/cdrom/db/v1/db.proto` + `internal/services/database` — approval-group
  CRUD RPCs (create/get/list/update/delete) and a `ListApprovalGroupMembers`
  (or fold members into the group message) the API uses to resolve a gate's
  authorized set.
- `internal/stephandlers/approval.go` — read the `approvers` param and carry
  the references onto the gate (the handler still reports `awaiting_approval`
  and polls; it does not itself authorize — the API does).
- `internal/approval` + `internal/api` — the gate carries the `approvers`
  references; `resolveApproval` (F-13) resolves the authorized set and rejects
  a decision from an actor outside it (403) or lacking the F-14 permission.
- `internal/services/scheduler` — `ResolveApproval` validates the actor against
  the gate's authorized set (group members ∪ individuals) before persisting the
  decision.
- `internal/api/server.go` — `GET/POST /api/approval-groups`,
  `GET/PUT/DELETE /api/approval-groups/{name}` (gated by an F-14 permission,
  e.g. `roles.can-manage` or a new `approvals.can-manage`); enforce the
  membership + permission check in `resolveApproval`.
- `ui/` — approval-group management (create/edit, member list) and an approval
  prompt that names the waiting group(s)/individuals.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **A group is people; a role is permissions.** F-14 roles grant *what you may
  do*; an approval group names *who may act on a specific gate*. They are
  separate concepts that compose: the actor must be in the gate's authorized
  set (group/individual) **and** hold the F-14 permission. This keeps a
  pipeline's approval policy ("release managers") independent of platform-wide
  RBAC, and lets the same group be referenced by many pipelines.
- **Membership is resolved at decision time, not dispatch time.** The gate
  stores the `approvers` references (group names / individuals), not a frozen
  member list, so a member added or removed mid-flight is reflected the next
  time a decision is checked. This mirrors how F-14 role/binding changes take
  effect on every replica without a restart.
- **Backward compatible with F-13.** No `approvers` param → the existing
  behavior (anyone with the F-14 permission decides). Adding the param is
  opt-in per step, so existing pipelines and version snapshots are unaffected.
- **Groups live in the Database service; the API resolves and enforces.** Group
  rows are owned by the Database service (shared across replicas, F-24-style);
  the API reads members when a decision arrives and is the only place that
  authorizes, so the check is identical on every replica.
- **Quorum (nice-to-have) is a gate property, not a step re-run.** When
  `quorum: K` is set the gate tracks distinct approving actors and reports
  resolved only at K; a rejection by any authorized actor still fails the job
  immediately.

**Acceptance criteria.**
- [ ] A pipeline's `approval` step can reference a named group; a member of
      that group can approve, and a non-member gets 403.
- [ ] `approvers` can mix group names and individual users; the authorized set
      is their union (a member of any referenced group or a named individual
      may decide).
- [ ] With no `approvers` param, behavior is unchanged from F-13 (anyone with
      the F-14 `jobs.can-approve` / `jobs.can-reject` permission decides).
- [ ] A decision requires both group/individual membership **and** the F-14
      permission (a group member without the permission is still rejected).
- [ ] Adding or removing a group member takes effect for a gate already
      `awaiting_approval` (membership resolved at decision time).
- [ ] Approval groups are manageable (create/read/update/delete) via the API
      and UI, gated by an F-14 permission; group mutations are audited (F-15).
- [ ] The UI approval prompt shows which group(s)/individuals the gate is
      waiting on.
- [ ] (Nice-to-have) `role:<name>` in `approvers` authorizes everyone holding
      that role; `quorum: K` passes the gate only after K distinct authorized
      approvers.
