# F-20 · Observability & metrics


> Phase 4 — Delivery & observability · [Feature index](../Features.md)


**What.** Every Cdrom component exposes **Prometheus metrics** in the
[Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/)
at a `/metrics` endpoint (plus a `/healthz` liveness/readiness probe), built
on the maintained **`github.com/prometheus/client_golang`** client library.
Metrics are served on a **dedicated metrics port** — a small HTTP server
separate from the component's service ports — so the service ports (gRPC,
the UI HTTP, the IdP's OIDC surface) can sit behind a firewall while only
the metrics port is exposed to a scraper. The metric catalog covers what
operators report on for a deployment
platform: job and run throughput and duration, success/failure rates, queue
depth and queue wait, worker utilization, artifact volume, and control-plane
health (event-log tail lag, leader state, background-loop health). Derived
from the run metrics are the standard **deployment-reporting (DORA-style)
indicators** per environment (F-17): deployment frequency, lead time for
changes, change failure rate, and MTTR. The UI additionally shows
dashboards fed from the database (per-pipeline run history with durations
and outcomes, failure trends, worker load, queue depth); Prometheus is the
external interface for Grafana dashboards and alerting.

**Why.** Operators need to see the health and throughput of the platform,
alert on it, and report on delivery performance — how often and how
reliably we deploy, how fast, and how often a deploy breaks. Structured
metrics are the substrate for all three, and a uniform `/metrics` endpoint
on every component means one scraper configuration covers the whole stack.
Serving metrics on a **dedicated port** (separate from each component's
service ports) is the standard arrangement: it lets a deployment keep the
service ports (gRPC, the UI HTTP, the IdP's OIDC surface) behind a firewall
while exposing only the metrics port to a scraper. The UI dashboards give
day-to-day visibility without a separate monitoring stack.

**Design decisions.**
- **`github.com/prometheus/client_golang`.** The maintained, de-facto
  standard Prometheus client for Go. Each binary uses its own
  `prometheus.Registry` (constructed by a small `internal/metrics` package)
  so a component's metric set is self-contained and tests can use a fresh
  registry. The Go runtime collectors (`go_*`, `process_*`: goroutines,
  heap, GC, CPU, file descriptors) are registered into the same registry,
  so process health comes for free.
- **One `internal/metrics` package, per-component metric sets.** The
  package holds the shared plumbing — a `Collector` (registry +
  `promhttp` handler + `/healthz` handler), the label conventions, the
  histogram bucket presets, and a small gRPC server-side `stats.Handler` —
  plus one file per component (`api.go`, `scheduler.go`, `database.go`,
  `artifacts.go`, `idp.go`, `worker.go`) that declares that component's
  metrics. A component constructs its metric set at startup and records
  into it at the hot spots.
- **Where `/metrics` is served — a dedicated port on every component.**
  Every binary (API, IdP, scheduler, database, artifacts, worker, agent)
  starts a small HTTP server on `metrics.addr` serving **only** `/metrics`
  and `/healthz`. The port is separate from the component's service ports,
  so a deployment can firewall the service ports (gRPC, the UI HTTP, the
  IdP's OIDC surface) and expose only the metrics port to a scraper. The
  metrics port is not behind the auth middleware — a scraper needs no
  token — which is safe precisely because it is a separate, low-trust
  surface. Defaults (one per component, `127.0.0.1` for local dev):
  worker/agent `:9100`, database `:9101`, scheduler `:9102`, artifacts
  `:9103`, API `:9104`, IdP `:9105`. In a cluster deployment each pod
  exposes its metrics port to the scraper; an agent is ephemeral, so its
  metrics are best-effort (scraped only while it lives).
- **Configuration.** `metrics.enabled` (default `true`) and
  `metrics.addr` (empty = the component default above) in
  `internal/config`; env vars `CDROM_METRICS_ENABLED` /
  `CDROM_METRICS_ADDR`. When disabled, the metrics listener is not started
  and recording is a no-op.
- **Naming & labels.** All metrics are prefixed `cdrom_`. Domain labels are
  low-cardinality by construction: `status`, `trigger`, `environment`,
  `group`, `step_type`, `method`, `code`, `route`, `kind`, `resource` (a
  concurrency key's or worker group's name), `op`, `outcome`. **Never**
  label by job id, run id, user, worker name, or secret name — those are
  unbounded and would blow up the series count. Per-pipeline breakdowns are
  available through the UI/DB, not through labels.
- **gRPC instrumentation.** A small server-side `stats.Handler` in
  `internal/metrics` (the `grpc_prometheus` interceptor is deprecated)
  records `cdrom_grpc_server_started_total` /
  `cdrom_grpc_server_handled_total` (by `method`, `code`) and
  `cdrom_grpc_server_handling_seconds` on every service server, including
  streaming RPCs (`WatchJobs`, `StreamJobLogs`). The UI-facing HTTP surface
  is instrumented with a middleware that records
  `cdrom_api_http_requests_total{route,method,code}` and
  `cdrom_api_http_request_duration_seconds{route,method}` (route = the mux
  pattern, not the raw path).
- **Histogram buckets.** Presets: request/RPC durations `5ms … 30s`; job
  and run durations `1s, 5s, 30s, 1m, 5m, 15m, 30m, 1h, 3h`; queue wait
  `1s, 5s, 30s, 1m, 5m, 15m, 1h`.
- **HA: record each event exactly once.** In a multi-pod deployment
  (F-23) every *counter* is recorded by exactly one replica per event —
  the pod that handled the event (a status report reaches exactly one API
  pod; a `SubmitJob` is handled by one scheduler replica) or the leader for
  leader-gated work (the admission loop, run-status loop) — so summing the
  per-pod series gives the fleet total. Point-in-time *gauges* of global
  state (queue depth, held slots, event-log size) are exposed by the
  component that owns the state (the database service), while per-pod
  gauges (WS clients, workers on this pod, tail lag, leader flag,
  in-flight jobs) are inherently per-pod and are summed by the scraper.
- **UI dashboards are DB-fed.** The UI's dashboards (run history, failure
  trends, worker load, queue depth) read from the existing API routes
  (`GET /api/pipelines/{id}/runs`, `GET /api/runs/{id}`, `GET /api/queue`,
  `GET /api/workers`) — the UI has no Prometheus dependency. Prometheus is
  the interface for external dashboards (Grafana) and alerting.

**Metric catalog.** (Counters are `_total`; durations are `_seconds`
histograms; gauges are point-in-time.)

*Every component* (via the shared plumbing):
- `cdrom_up` — gauge, 1 while the process is serving.
- `cdrom_build_info{version,commit}` — gauge, 1 (from `runtime/debug`
  build info).
- `go_*` / `process_*` — Go runtime (goroutines, heap, GC, CPU, fds).

*API (control plane):*
- `cdrom_api_http_requests_total{route,method,code}` /
  `cdrom_api_http_request_duration_seconds{route,method}` — the UI HTTP
  surface.
- `cdrom_grpc_server_*` (shared) — the worker/agent/scheduler gRPC surface.
- `cdrom_api_ws_clients` — gauge, live WebSocket UI clients on this pod.
- `cdrom_api_workers{group,environment,state}` — gauge, workers registered
  on this pod (state: `registered` / `watching`).
- `cdrom_api_event_log_lag` — gauge, how far this pod's tail cursor is
  behind the shared event log head (F-23) — the key HA health signal.
- `cdrom_api_job_status_total{status}` — job status transitions as reported
  by targets (running / succeeded / failed / cancelled / timed_out /
  skipped); recorded by the pod that receives the report.
- `cdrom_api_job_duration_seconds{status}` — histogram, from a job's
  start/finish timestamps, observed when a job reaches a terminal state.
- `cdrom_api_queue_wait_seconds` — histogram, a job's pending→running wait
  (recorded by the scheduler, see below).
- `cdrom_api_artifact_bytes_total{op,kind}` — upload/download bytes
  (kind: `artifact` / `log`), through the API's artifact proxy.
- `cdrom_api_log_stream_bytes_total` — job log bytes persisted (F-02).
- `cdrom_api_auth_failures_total{reason}` — 401/403 (reason:
  `unauthenticated` / `forbidden` / `lockout`).
- `cdrom_api_tokens_minted_total{kind}` — job tokens minted / exchanged
  (kind: `job` / `exchanged`).
- `cdrom_api_secrets_decrypted_total` — secret decryptions at dispatch
  (F-12).
- `cdrom_api_notifications_total{kind,outcome}` — email/webhook sends
  (F-18; outcome: `sent` / `failed`).

*Scheduler:*
- `cdrom_scheduler_dispatches_total{outcome}` — outcome: `dispatched` /
  `blocked_limit` / `blocked_key` / `held_manual` / `skipped`.
- `cdrom_scheduler_admission_checks_total` /
  `cdrom_scheduler_admission_duration_seconds` — the leader-gated
  admission loop (F-16).
- `cdrom_scheduler_queue_wait_seconds` — histogram, pending→running per
  job (the queue's wait time), recorded by the replica that dispatches it.
- `cdrom_scheduler_runs_total{status,trigger,environment}` — run status
  transitions from the run-status loop (F-07; trigger: `manual` / `cron` /
  `webhook` / `event`; environment: the highest environment the run
  reached, empty when it targets none) — recorded by the leader only.
- `cdrom_scheduler_run_duration_seconds{status,trigger,environment}` — run
  start → finish.
- `cdrom_scheduler_run_lead_time_seconds{trigger,environment}` — run
  creation (trigger) → completion; the lead-time-for-changes signal.
- `cdrom_scheduler_watchdog_reaps_total` — jobs reaped as timed out (F-03).
- `cdrom_scheduler_retries_total` — retry re-dispatches (F-04).
- `cdrom_scheduler_dependencies_resolved_total{outcome}` — released /
  skipped (F-06/F-08).
- `cdrom_scheduler_triggers_fired_total{kind}` — cron / webhook / event
  (F-09).
- `cdrom_scheduler_leader` — gauge, 1 while this replica holds the leader
  lease (F-23).
- `cdrom_scheduler_loop_duration_seconds{loop}` — per background-loop tick
  (loop: `watchdog` / `retry` / `dependencies` / `run_status` /
  `job_status` / `admission` / `cron` / `event`).

*Database service:*
- `cdrom_grpc_server_*` (shared).
- `cdrom_db_rpc_duration_seconds{method}` — per-RPC latency (the whole
  platform's data path goes through here).
- `cdrom_db_event_log_size` — gauge, row count of the shared event log
  (F-23; feeds the retention open item).
- `cdrom_db_queue_depth{group}` — gauge, pending jobs per group (empty
  label = the global queue), from the durable queue state (F-16).
- `cdrom_db_concurrency_slots{kind,resource}` — gauge, durable slots held
  per concurrency resource (kind: `global` / `group` / `key`; F-16).

*Artifacts service:*
- `cdrom_artifacts_operations_total{op,code}` — op: `upload` /
  `download` / `append_log` / `list`.
- `cdrom_artifacts_bytes_total{op}` — bytes moved.
- `cdrom_artifacts_duration_seconds{op}`.

*IdP:*
- `cdrom_idp_tokens_minted_total{kind}` — kind: `ui` / `job` /
  `exchanged`.
- `cdrom_idp_logins_total{outcome}` — username/password sign-ins (success /
  `invalid_password` / `locked`).
- `cdrom_idp_keys` — gauge, signing keys in the keyring.
- `cdrom_idp_key_rotations_total`.
- `cdrom_idp_users` — gauge, user directory size.

*Worker / agent (execution targets):*
- `cdrom_worker_jobs_total{status}` — jobs this target completed, by
  terminal status.
- `cdrom_worker_job_duration_seconds{status}`.
- `cdrom_worker_steps_total{step_type,status}` — per step handler
  (`shell` / `token_exchange` / `approval` / `email` / `webhook` / …).
- `cdrom_worker_step_duration_seconds{step_type}`.
- `cdrom_worker_inflight` — gauge, jobs currently running on this target
  (utilization: in-flight vs. the target's limit of one).
- `cdrom_worker_watch_state` — gauge, `WatchJobs` stream state
  (`connected` / `reconnecting`) — the target's link to the control plane.
- `cdrom_worker_polls_total{outcome}` — the 1 s pending poll (F-23;
  outcome: `idle` / `claimed`).
- `cdrom_worker_heartbeat_failures_total`.
- `cdrom_worker_log_bytes_total` — log bytes streamed to the API (F-02).

**Deployment reporting (DORA-style indicators).** The run/job metrics above
are enough to compute the standard delivery-performance indicators per
environment (F-17), either as Grafana queries or in the UI dashboard:
- **Deployment frequency** — `sum(rate(cdrom_scheduler_runs_total{status="succeeded",environment="prod"}[1d]))`.
- **Lead time for changes** — `cdrom_scheduler_run_lead_time_seconds`
  (trigger → completion).
- **Change failure rate** — failed runs ÷ total runs per environment over a
  window (e.g. 7 days): `sum(rate(cdrom_scheduler_runs_total{environment="prod",status="failed"}[7d])) / sum(rate(cdrom_scheduler_runs_total{environment="prod"}[7d]))`.
- **MTTR** — time from a failed run to the next successful run in the same
  environment (computed in the UI from run timestamps in the database).

**Scope.**
- `internal/metrics` (new package) — shared plumbing (registry, `promhttp`
  handler, `/healthz`, label conventions, bucket presets, gRPC
  `stats.Handler`) + per-component metric sets (`api.go`, `scheduler.go`,
  `database.go`, `artifacts.go`, `idp.go`, `worker.go`).
- `internal/config` — `MetricsConfig` (`enabled`, `addr`) +
  `CDROM_METRICS_ENABLED` / `CDROM_METRICS_ADDR`.
- `cmd/api`, `cmd/idp`, `cmd/scheduler`, `cmd/db`, `cmd/artifacts`,
  `cmd/worker`, `cmd/agent` — every binary starts the small metrics HTTP
  server on `metrics.addr` (serving only `/metrics` + `/healthz`, on a port
  separate from its service ports); the API and IdP additionally construct
  their metric set.
- `internal/api` — HTTP middleware (requests / durations / auth failures),
  the gRPC `stats.Handler`, WS-client and worker-hub gauges, event-log tail
  lag, job status + duration recording, artifact-proxy bytes, log-stream
  bytes, token mint/exchange, secret decryption, notification sends.
- `internal/services/scheduler` — dispatch outcomes, admission loop, queue
  wait, run status/duration/lead-time, watchdog / retry / dependency /
  trigger counters, leader gauge, loop tick durations.
- `internal/services/database` — per-RPC durations, event-log size, queue
  depth + slot gauges.
- `internal/services/artifacts` — operation counters / bytes / durations.
- `internal/idp` — token mint, login, key, and user metrics.
- `internal/worker` + `internal/agent` (with seams in `internal/executor` /
  `internal/target`) — job/step counters and durations, in-flight, watch
  state, poll/heartbeat, log bytes.
- `config.example.yaml` — a `metrics:` section.
- `local/docker-compose.yaml` — expose the metrics ports.
- `docs/` — a short "monitoring" note: the scrape targets per component and
  the DORA-style Grafana queries above.
- UI — dashboards: per-pipeline run history (durations + outcomes), failure
  trends, worker load (in-flight vs. limit), queue depth (fed from the
  existing API routes, not Prometheus).

**Acceptance criteria.**
- [x] `GET /metrics` on every component's dedicated metrics port
      (`metrics.addr`, separate from the component's service ports) returns
      the Prometheus text format and includes the component's metric set
      plus `go_goroutines`; `GET /healthz` returns 200. The service ports
      (gRPC, the UI HTTP, the IdP's OIDC surface) do not serve `/metrics`.
- [x] The API records `cdrom_api_http_requests_total` /
      `cdrom_api_http_request_duration_seconds` for the UI surface and
      `cdrom_grpc_server_handled_total{method,code}` (including streaming
      RPCs such as `WatchJobs`) for the gRPC surface.
- [x] A job that runs to completion increments `cdrom_api_job_status_total`
      and `cdrom_worker_jobs_total` (matching statuses) and is observed in
      both `cdrom_api_job_duration_seconds` and
      `cdrom_worker_job_duration_seconds`; a step of each built-in type is
      counted in `cdrom_worker_steps_total{step_type}`.
- [x] A run that completes increments `cdrom_scheduler_runs_total{status,trigger,environment}`
      and is observed in `cdrom_scheduler_run_duration_seconds` and
      `cdrom_scheduler_run_lead_time_seconds`; a cron-fired run is labeled
      `trigger="cron"`.
- [x] A job blocked by a concurrency limit is visible in
      `cdrom_db_queue_depth` and its pending→running wait is observed in
      `cdrom_scheduler_queue_wait_seconds`; held slots are visible in
      `cdrom_db_concurrency_slots`.
- [x] `cdrom_api_event_log_lag` is ~0 on a healthy pod and grows when its
      tail is stopped; `cdrom_scheduler_leader` is 1 on exactly one
      scheduler replica at a time.
- [x] Artifact upload/download and log streaming are counted in
      `cdrom_api_artifact_bytes_total` / `cdrom_artifacts_bytes_total`.
- [x] IdP sign-ins and job-token minting are counted in
      `cdrom_idp_logins_total` / `cdrom_idp_tokens_minted_total`.
- [x] No metric label takes an unbounded value (no job id, run id, user,
      worker name, or secret name appears in any label).
- [x] `CDROM_METRICS_ENABLED=false` disables the endpoint (and recording)
      on every binary.
- [x] The UI shows per-pipeline run history with durations and outcomes, a
      failure-trend view, worker load (in-flight vs. limit), and queue
      depth.
- [x] The DORA-style indicators (deployment frequency, lead time, change
      failure rate, MTTR) can be computed per environment from the exposed
      metrics (the Grafana queries are documented in `docs/`).
- [x] `GOOS=windows GOARCH=amd64 go build ./cmd/...` stays green.
