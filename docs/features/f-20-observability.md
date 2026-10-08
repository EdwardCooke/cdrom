# F-20 · Observability & metrics


> Phase 4 — Delivery & observability · [Feature index](../Features.md)


**What.** Surface operational metrics: job duration, success/failure rate,
queue depth, worker utilization, artifact volume. Expose a metrics endpoint
and/or a dashboard in the UI.

**Why.** Operators need to see the health and throughput of the platform.

**Scope.**
- Instrument the scheduler, API, and worker hub with counters/histograms
  (decide: a Prometheus `/metrics` endpoint on the API, and/or UI charts fed
  by the DB).
- `internal/api/server.go` — a metrics endpoint and/or dashboard data routes.
- UI — dashboards (run history, durations, failure trends, worker load).

**Acceptance criteria.**
- [ ] A metrics endpoint exposes job duration, success rate, and queue depth.
- [ ] The UI shows per-pipeline run history with durations and outcomes.
- [ ] Worker load (in-flight vs. limit) is visible.
