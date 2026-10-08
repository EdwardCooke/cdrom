# F-15 · Audit log


> Phase 3 — Safety & governance · [Feature index](../Features.md)


**What.** An append-only record of significant actions: who triggered/cancelled
/approved what, when, and the outcome. Queryable from the UI.

**Why.** Compliance and debugging both need "what happened and who did it".

**Scope.**
- `internal/models` — an `AuditEvent` entity (actor, action, target, time,
  metadata).
- `proto/cdrom/db/v1/db.proto` — append + query.
- Instrument the API handlers (and gRPC surface) to emit audit events for the
  actions in F-07/F-09/F-13/F-14.
- `internal/api/server.go` — `GET /api/audit`.

**Acceptance criteria.**
- [ ] Triggering, cancelling, approving, and secret/pipeline mutations each
      write an audit event with the actor.
- [ ] The log is append-only (no update/delete of past events).
- [ ] Events can be filtered by actor, target, and time range.
