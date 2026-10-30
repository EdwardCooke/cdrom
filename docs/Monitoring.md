# Monitoring

Every Cdrom component exposes **Prometheus metrics** in the Prometheus text
exposition format at a `/metrics` endpoint (plus a `/healthz` liveness/readiness
probe) on a **dedicated metrics port** — a small HTTP server separate from the
component's service ports (gRPC, the UI HTTP, the IdP's OIDC surface). This
lets a deployment keep the service ports behind a firewall while exposing only
the metrics port to a scraper. The metrics port is not behind the auth
middleware (a scraper needs no token).

Metrics are built on `github.com/prometheus/client_golang` and live in the
`internal/metrics` package (shared plumbing + one metric set per component).
The full catalog, label conventions, and bucket presets are specified in
[features/f-20-observability.md](features/f-20-observability.md).

## Enabling / disabling

Metrics are **enabled by default** on every binary. Disable them (the listener
is not started and all recording becomes a no-op) with:

- config file: `metrics.enabled: false`
- environment: `CDROM_METRICS_ENABLED=false`

The listen address is `metrics.addr` / `CDROM_METRICS_ADDR`. When unset, each
component uses its default port (below).

## Scrape targets

Each component serves `/metrics` and `/healthz` on its own metrics port. Point
Prometheus at each one (adjust the host for your deployment):

| Component | Default metrics port | `CDROM_METRICS_ADDR` |
|-----------|----------------------|----------------------|
| worker    | `127.0.0.1:9100`    | `:9100`             |
| agent     | `127.0.0.1:9100`    | `:9100`             |
| database  | `127.0.0.1:9101`    | `:9101`             |
| scheduler | `127.0.0.1:9102`    | `:9102`             |
| artifacts | `127.0.0.1:9103`    | `:9103`             |
| api       | `127.0.0.1:9104`    | `:9104`             |
| idp       | `127.0.0.1:9105`    | `:9105`             |

> **Note on worker/agent ports.** Workers and agents both default to `:9100`.
> When several targets run on one host, give each a distinct
> `CDROM_METRICS_ADDR` (e.g. `:9106`, `:9107`, …) so their ports do not
> collide. In the bundled `local/docker-compose.yaml` each service is given a
> distinct `CDROM_METRICS_ADDR` and the port is published.

A minimal Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: cdrom
    static_configs:
      - targets:
          - api:9104
          - idp:9105
          - scheduler:9102
          - database:9101
          - artifacts:9103
          - worker-1:9100
```

Every metric is prefixed `cdrom_`. The Go runtime collectors (`go_*`,
`process_*`) are registered into the same registry, so process health (goroutines,
heap, GC, CPU, file descriptors) comes for free.

## Metric catalog (summary)

- **API** — `cdrom_api_http_requests_total` / `cdrom_api_http_request_duration_seconds`
  (UI surface), `cdrom_grpc_server_handled_total{method,code}` (gRPC surface,
  including streaming RPCs such as `WatchJobs`), `cdrom_api_job_status_total` /
  `cdrom_api_job_duration_seconds`, `cdrom_api_ws_clients`, `cdrom_api_workers`,
  `cdrom_api_event_log_lag`, `cdrom_api_artifact_bytes_total`,
  `cdrom_api_log_stream_bytes_total`, `cdrom_api_tokens_minted_total`,
  `cdrom_api_secrets_decrypted_total`, `cdrom_api_notifications_total`,
  `cdrom_api_restorations_total{door,result}` (F-22: resumption initiations
  by door — `armed` (job-mediated) / `manual` (operator) — and outcome). Probe
  steps surface through the worker's generic step counters
  (`cdrom_worker_steps_total{step_type="probe"}`).
- **Scheduler** — `cdrom_scheduler_dispatches_total{outcome}`,
  `cdrom_scheduler_admission_checks_total` / `cdrom_scheduler_admission_duration_seconds`,
  `cdrom_scheduler_queue_wait_seconds{group}`, `cdrom_scheduler_runs_total{status,trigger,environment}`,
  `cdrom_scheduler_run_duration_seconds`, `cdrom_scheduler_run_lead_time_seconds`,
  `cdrom_scheduler_watchdog_reaps_total`, `cdrom_scheduler_retries_total`,
  `cdrom_scheduler_dependencies_resolved_total{outcome}`,
  `cdrom_scheduler_triggers_fired_total{kind}`, `cdrom_scheduler_leader`,
  `cdrom_scheduler_loop_duration_seconds{loop}`.
- **Database** — `cdrom_db_rpc_duration_seconds{method}`,
  `cdrom_db_event_log_size`, `cdrom_db_queue_depth{group}`,
  `cdrom_db_concurrency_slots{kind,resource}`.
- **Artifacts** — `cdrom_artifacts_operations_total{op,code}`,
  `cdrom_artifacts_bytes_total{op}`, `cdrom_artifacts_duration_seconds{op}`.
- **IdP** — `cdrom_idp_tokens_minted_total{kind}`, `cdrom_idp_logins_total{outcome}`,
  `cdrom_idp_keys`, `cdrom_idp_key_rotations_total`, `cdrom_idp_users`.
- **Worker / agent** — `cdrom_worker_jobs_total{status}`,
  `cdrom_worker_job_duration_seconds{status}`, `cdrom_worker_steps_total{step_type,status}`,
  `cdrom_worker_step_duration_seconds{step_type}`, `cdrom_worker_inflight`,
  `cdrom_worker_watch_state{state}` (worker), `cdrom_worker_polls_total{outcome}`
  (worker), `cdrom_worker_heartbeat_failures_total` (worker),
  `cdrom_worker_log_bytes_total`.

## Deployment reporting (DORA-style indicators)

The run/job metrics above are enough to compute the standard delivery-performance
indicators per environment (F-17). These are ready-to-use Grafana/PromQL
queries:

- **Deployment frequency** — successful deployments per unit time:
  ```promql
  sum(rate(cdrom_scheduler_runs_total{status="succeeded",environment="prod"}[1d]))
  ```
- **Lead time for changes** — time from a run's trigger to its completion:
  ```promql
  cdrom_scheduler_run_lead_time_seconds{environment="prod"}
  ```
- **Change failure rate** — failed runs ÷ total runs per environment over a
  window (e.g. 7 days):
  ```promql
  sum(rate(cdrom_scheduler_runs_total{environment="prod",status="failed"}[7d]))
    / sum(rate(cdrom_scheduler_runs_total{environment="prod"}[7d]))
  ```
- **MTTR** — time from a failed run to the next successful run in the same
  environment. This is computed in the UI from run timestamps in the database
  (the run history), not from a single Prometheus series.

## Health checks

`GET /healthz` on each component's metrics port returns `200` when the
component is up. Use it as a liveness/readiness probe or a simple scrape
health check.
