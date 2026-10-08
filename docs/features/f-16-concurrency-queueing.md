# F-16 · Concurrency control & queueing


> Phase 3 — Safety & governance · [Feature index](../Features.md)


**What.** Bound how much runs in parallel: max concurrent jobs per worker
group (and/or globally), a visible pending queue, and optional job priority.
Jobs beyond the limit wait their turn.

**Why.** Without limits a burst of jobs overwhelms targets; operators need to
see and manage the backlog.

**Scope.**
- `internal/config` — concurrency limits (per group / global).
- `internal/api` (worker hub) — track in-flight jobs per worker/group; hold
  excess assignments until a slot frees.
- `internal/services/scheduler` — priority ordering of the pending queue.
- UI — a queue view (pending jobs, who's running what).

**Acceptance criteria.**
- [ ] With a limit of N per group, at most N jobs run concurrently on that
      group; the rest stay `pending`.
- [ ] When a job finishes, a queued job is dispatched.
- [ ] Higher-priority jobs are dispatched before lower-priority ones.
- [ ] The UI shows the pending queue and current load per group.
