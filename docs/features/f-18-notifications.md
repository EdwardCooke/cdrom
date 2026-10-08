# F-18 · Notifications


> Phase 4 — Delivery & observability · [Feature index](../Features.md)


**What.** Emit notifications when a run/job reaches a terminal state (or hits
an approval gate): via **webhook** (generic POST) and/or other channels.
Configurable per pipeline.

**Why.** Teams live in chat/issue trackers, not the CD UI.

**Scope.**
- `internal/models` / proto — notification config on the pipeline (webhook URL
  + secret, event filters).
- `internal/services/scheduler` (or a small notifier) — fire webhooks on the
  relevant events (reuse the EventHub).
- Delivery semantics — retry with backoff on failure; never block the run.

**Acceptance criteria.**
- [ ] A configured webhook receives a payload when a run finishes (with status
      and run link).
- [ ] Notification failures are retried and logged but do not fail the run.
- [ ] Event filters (e.g. only notify on failure) are honored.
