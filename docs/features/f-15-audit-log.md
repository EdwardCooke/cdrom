# F-15 · Audit log


> Phase 3 — Safety & governance · [Feature index](../Features.md)


**What.** An append-only record of significant actions: who (actor) did what
(action) to which resource (target), when, from where (source IP), with what
outcome, and what changed (old/new value). Written both to a local audit log
file (separate from the process's default log) and to a shared, queryable
audit log in the database. Queryable from the UI via `GET /api/audit`.

**Why.** Compliance and debugging both need "what happened and who did it". A
local file gives an operator a durable, rotatable record on each host; the
shared database log is the cross-replica store the UI queries.

**Scope.**
- `internal/models` — an `AuditEvent` entity (actor, actor_kind, action,
  target_kind, target_id, target_name, pipeline_id, run_id, outcome,
  source_ip, old_value, new_value, details, created_at). Registered in
  `models.All()` so it is migrated.
- `proto/cdrom/db/v1/db.proto` — `AppendAuditEvent`, `ListAuditEvents`, and
  `PruneAuditEvents` RPCs (the Database service owns the rows).
- `internal/audit` — the audit log: a `Logger` (a rotating file, or stdout
  when the path is `-`, in JSON or text), a `Recorder` (fans each event to the
  local log and a durable `Sink`; best-effort, never fails the caller), a
  `Pruner` (age-based retention), and a `Redactor` (replaces secret plaintext
  with a marker).
- `internal/config` — `AuditConfig` (file, format, rotation, retention,
  prune interval; env `CDROM_AUDIT_*`).
- `internal/api` — `auditSink`/`auditPruner` over the Database client,
  `NewAuditRecorder`, `SetAudit` on both the HTTP and gRPC servers, and
  `GET /api/audit` (requires `audit.can-view`). The HTTP handlers and the
  gRPC surface record an event for each audited action.
- `internal/services/scheduler` — the cron and event trigger loops record a
  `run.trigger` event (actor `system:scheduler`) for each run they start
  directly via the Database service.
- `cmd/api` — builds the recorder from config, attaches it to both servers,
  and runs the background prune loop.

**Configuration** (`audit` section / `CDROM_AUDIT_*`):

| Key | Env | Default | Meaning |
|-----|-----|---------|---------|
| `file` | `CDROM_AUDIT_FILE` | `audit.log` | Path of the current audit log file. Separate from the process's default log (`CDROM_LOG_FILE`). A value of `-` writes to stdout (no rotation). |
| `format` | `CDROM_AUDIT_FORMAT` | `json` | Record format: `json` or `text`. |
| `rotation` | `CDROM_AUDIT_ROTATION` | `daily` | File rotation interval: `hourly` or `daily`. The current file keeps a stable name; on rotation the closed file is renamed with the day/hour appended (`audit.log-20261009` daily, `audit.log-2026100914` hourly) and a fresh file is opened at the stable path. |
| `retention` | `CDROM_AUDIT_RETENTION` | `2160h` (90 days) | Maximum age of audit entries kept in the database before they are pruned. Zero disables age-based pruning. |
| `prune_interval` | `CDROM_AUDIT_PRUNE_INTERVAL` | `24h` | How often the database's audit log is pruned of entries older than `retention`. |

**Audited actions.** Each records the actor (the authenticated user's subject,
a synthetic `system:*` identity, or the worker's name), the action, the
target, the outcome, the source IP, and — where the action changes a resource
— the old and new value documents:

- `pipeline.create` / `pipeline.update` (old/new value = the pipeline's
  non-secret definition; secrets by name only)
- `run.trigger` (a manual run, a standalone job submit, a webhook, or a
  cron/event trigger fired by the scheduler)
- `job.cancel`, `job.rerun`, `job.approve`, `job.reject`
- `job.status` (a status report from a worker/agent), `token.exchange`
- `secret.encrypt` (the value is recorded as `[REDACTED]`)
- `role.create` / `role.update` / `role.delete`, `binding.add` /
  `binding.delete`
- `user.create` / `user.update` / `user.delete`
- `auth.login` (success and failure), `auth.register`
- `worker.register` / `worker.deregister`

**Secrets.** A secret's value is never stored or displayed in the audit log:
a secret is represented by its name only, and any secret plaintext that
appears in a value document is redacted (via `internal/audit`'s `Redactor`)
before it reaches either sink.

**Acceptance criteria.**
- [x] Triggering, cancelling, approving, and secret/pipeline mutations each
      write an audit event with the actor.
- [x] The log is append-only (no update/delete of past events; entries are
      only ever pruned by age, a retention policy, not an edit).
- [x] Events can be filtered by actor, action, target, pipeline, and time
      range (`GET /api/audit`).
- [x] The audit log is written to a file separate from the process's default
      log, in a configurable format (json/text), rotated hourly or daily,
      with the current file keeping a stable name and rotated files carrying
      the day/hour appended.
- [x] A path of `-` writes the audit log to stdout.
- [x] Events are also stored in the database and pruned at a configurable
      interval and maximum age.
- [x] A secret's value is never stored or displayed (name only, redacted).
