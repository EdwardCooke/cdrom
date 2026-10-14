#!/usr/bin/env bash
#
# 12-metrics.sh — verifies the F-20 metrics endpoints actually return data.
#
# Boots the full local stack (db, scheduler, artifacts, idp, api, worker)
# against a throwaway SQLite database. Every component serves Prometheus
# metrics at /metrics (plus a /healthz probe) on a DEDICATED metrics port,
# separate from its service ports (gRPC, the UI HTTP, the IdP's OIDC surface).
# This test proves that arrangement end to end:
#
#   1. Each component's dedicated metrics port serves GET /healthz (200) and
#      GET /metrics (200, non-empty), and the body carries the always-present
#      process metrics (go_goroutines, cdrom_up) plus the component's own
#      metric set (a component-specific metric name).
#   2. After a job runs to completion on the worker, the execution-target and
#      control-plane components report the job in their metrics: the worker's
#      job/step counters, the API's job-status + gRPC-handled counters, the
#      scheduler's dispatch counter, the artifacts service's operation
#      counter, and the IdP's job-token mint counter. The scheduler also
#      records the job's pending→running queue wait (F-20/F-16).
#   2b. A pipeline run that completes exercises the run-status loop's run
#       metrics: the run status counter, the run duration, and the run lead
#       time (the DORA-style deployment indicators, F-20/F-07).
#   2c. A job that holds a concurrency key (a lock) while a second job on the
#       same key waits exercises the Database service's point-in-time gauges:
#       the durable concurrency slots held and the pending queue depth
#       (F-20/F-16).
#   3. The components' SERVICE ports do NOT serve /metrics (the API's UI HTTP
#      port and the IdP's OIDC port return a non-200 for it) — the metrics
#      live only on the dedicated ports.
#
# The default metrics ports (one per component, 127.0.0.1) are used:
#   worker/agent :9100, database :9101, scheduler :9102, artifacts :9103,
#   api :9104, idp :9105.
#
# Usage: scripts/e2e/12-metrics.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# --- dedicated metrics ports (F-20 defaults) --------------------------------
E2E_METRICS_WORKER=9100
E2E_METRICS_DATABASE=9101
E2E_METRICS_SCHEDULER=9102
E2E_METRICS_ARTIFACTS=9103
E2E_METRICS_API=9104
E2E_METRICS_IDP=9105

# --- helpers -----------------------------------------------------------------
# e2e_metrics_check <name> <port> <metric>...
# Verifies a component's dedicated metrics port (F-20): GET /healthz returns
# 200, and GET /metrics returns 200 with a body that contains the
# always-present process metrics (go_goroutines, cdrom_up) and each of the
# given component-specific metric names. Fails if any is missing.
#
# The check polls (like e2e_wait_for) and, on each attempt, re-fetches the
# whole /metrics body and re-checks every required metric in one pass. This
# matters in the shared-stack run, where this test is last and the host is
# loaded: a component's /metrics body can be large (the database's, once the
# queue/slot gauges have series, is ~180 KB), and a single fetch can be
# truncated mid-transfer on a slow host, dropping the component's own metric
# lines (which are emitted after the go_*/process_* collectors). Re-fetching
# until a complete body carries every required metric makes the check robust
# to that.
#
# The body is written to a temp file and grepped from the file (not piped):
# `printf "$body" | grep -q` under `set -o pipefail` is unreliable for a large
# body — when grep -q exits early after a match, printf (still writing the
# rest of the body) is killed by SIGPIPE and pipefail reports the pipeline as
# failed even though grep matched. Grepping a file has no such race.
e2e_metrics_check() {
  local name="$1" port="$2"; shift 2
  local metrics_url="http://127.0.0.1:${port}/metrics"
  local health_url="http://127.0.0.1:${port}/healthz"

  # Poll /healthz until it returns 200 (the metrics server is up).
  local hcode i
  for i in $(seq 1 100); do
    hcode=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$health_url" || true)
    [ "$hcode" = "200" ] && break
    sleep 0.2
  done
  [ "$hcode" = "200" ] \
    || { echo ">> $name: /healthz on :$port returned $hcode, want 200" >&2; exit 1; }

  # Poll /metrics until a single complete fetch carries every required metric
  # (the always-present process metrics plus the component-specific ones).
  local bodyfile mcode ok m
  bodyfile="$(mktemp "${TMPDIR:-/tmp}/cdrom-metrics.XXXXXX")"
  for i in $(seq 1 100); do
    mcode=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$metrics_url" || true)
    if [ "$mcode" = "200" ]; then
      curl -s --max-time 10 -o "$bodyfile" "$metrics_url" || true
      ok=1
      for m in go_goroutines cdrom_up "$@"; do
        grep -qF "$m" "$bodyfile" || { ok=0; break; }
      done
      [ "$ok" = "1" ] && break
    fi
    sleep 0.2
  done
  rm -f "$bodyfile"
  [ "$ok" = "1" ] \
    || { echo ">> $name: /metrics on :$port is missing one of: go_goroutines cdrom_up $* (last http_code $mcode)" >&2; exit 1; }
  log "   $name (:$port) serves /metrics + /healthz with the expected metrics"
}

# e2e_wait_for_metric <name> <port> <metric> <what>
# Polls a component's dedicated metrics port until /metrics contains <metric>
# (a fixed-string match on the metric name). Used for the job-related metrics
# that only appear once the relevant activity has happened (a label-vec series
# is emitted only after it is first recorded).
#
# The body is written to a temp file and grepped from the file (not piped):
# `printf "$body" | grep -q` under `set -o pipefail` is unreliable for a large
# body (SIGPIPE, see e2e_metrics_check).
e2e_wait_for_metric() {
  local name="$1" port="$2" metric="$3" what="$4" i
  local bodyfile
  bodyfile="$(mktemp "${TMPDIR:-/tmp}/cdrom-metrics.XXXXXX")"
  for i in $(seq 1 100); do
    curl -s --max-time 5 -o "$bodyfile" "http://127.0.0.1:${port}/metrics" || true
    if grep -qF "$metric" "$bodyfile"; then
      rm -f "$bodyfile"
      return 0
    fi
    sleep 0.2
  done
  echo ">> timed out waiting for: $what ($name :$port, metric $metric)" >&2
  grep -F "cdrom_" "$bodyfile" | head -n 40 >&2 || true
  rm -f "$bodyfile"
  exit 1
}

e2e_boot_stack with-worker

# --- 1. every component's dedicated metrics port returns data ----------------
# The always-present process metrics (go_goroutines, cdrom_up) plus one
# component-specific metric that is a plain gauge/counter (always emitted,
# even before any job runs) prove the port serves the component's own metric
# set, not just an empty registry.
log ">> checking each component's dedicated metrics port"
e2e_metrics_check worker    "$E2E_METRICS_WORKER"    cdrom_worker_inflight
e2e_metrics_check database  "$E2E_METRICS_DATABASE"  cdrom_db_event_log_size
e2e_metrics_check scheduler "$E2E_METRICS_SCHEDULER" cdrom_scheduler_leader
# The artifacts metric set is all label-vecs (no plain gauge), so its baseline
# is the process metrics alone; its own operations are asserted after a job.
e2e_metrics_check artifacts "$E2E_METRICS_ARTIFACTS"
e2e_metrics_check api       "$E2E_METRICS_API"       cdrom_api_event_log_lag
e2e_metrics_check idp       "$E2E_METRICS_IDP"       cdrom_idp_keys

# --- 2. a job that runs to completion is reflected in the metrics -----------
# Submit a two-step shell job to the worker group. Running it exercises the
# full path, so every component records the job in its metrics: the worker
# (job + step counters), the API (job-status + gRPC-handled), the scheduler
# (dispatch), the artifacts service (log persistence), and the IdP (job-token
# mint).
log ">> submitting a job so the components record it in their metrics"
BODY=$(cat <<EOF
{
  "name": "e2e-metrics-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo metrics step1"]}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo metrics step2"]}}}
    ]
  }
}
EOF
)
JOB_ID=$(e2e_submit_job "$BODY")
log ">> job id: $JOB_ID"

# Wait for the job to run to completion (status=succeeded, the numeric value 3).
log ">> waiting for the job to reach status=succeeded"
FINAL=""
for i in $(seq 1 100); do
  FINAL=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs/$JOB_ID" || true)
  if [ "$(printf '%s' "$FINAL" | jq -r '.status' 2>/dev/null)" = "3" ]; then
    break
  fi
  sleep 0.2
done
[ "$(printf '%s' "$FINAL" | jq -r '.status' 2>/dev/null)" = "3" ] \
  || { echo ">> job did not reach status=succeeded: $FINAL" >&2; exit 1; }

# Now each component's dedicated metrics port must report the job.
log ">> verifying the job is reflected in each component's metrics"
e2e_wait_for_metric worker    "$E2E_METRICS_WORKER"    cdrom_worker_jobs_total      'worker recorded the completed job'
e2e_wait_for_metric worker    "$E2E_METRICS_WORKER"    cdrom_worker_steps_total     'worker recorded the job steps'
e2e_wait_for_metric api       "$E2E_METRICS_API"       cdrom_api_job_status_total   'api recorded the job status'
e2e_wait_for_metric api       "$E2E_METRICS_API"       cdrom_grpc_server_handled_total 'api recorded the gRPC calls'
e2e_wait_for_metric scheduler "$E2E_METRICS_SCHEDULER" cdrom_scheduler_dispatches_total 'scheduler recorded the dispatch'
e2e_wait_for_metric artifacts "$E2E_METRICS_ARTIFACTS" cdrom_artifacts_operations_total 'artifacts recorded the log persistence'
e2e_wait_for_metric idp       "$E2E_METRICS_IDP"       cdrom_idp_tokens_minted_total 'idp recorded the job-token mint'
e2e_wait_for_metric database  "$E2E_METRICS_DATABASE"  cdrom_db_rpc_duration_seconds 'database recorded the RPCs'

# --- 2b. queue wait (F-20, F-16) --------------------------------------------
# The scheduler records a job's pending→running wait (cdrom_scheduler_queue_wait_seconds)
# when it dispatches the job. The job above was dispatched to the "default" group,
# so the histogram now has a series for that group.
log ">> verifying the scheduler recorded the job's queue wait"
e2e_wait_for_metric scheduler "$E2E_METRICS_SCHEDULER" cdrom_scheduler_queue_wait_seconds 'scheduler recorded the job queue wait'

# --- 2c. run metrics (F-20, F-07) -------------------------------------------
# A pipeline run that completes exercises the run-status loop's recording of the
# run's status transition (cdrom_scheduler_runs_total) and, once the run reaches
# a terminal state, its duration (cdrom_scheduler_run_duration_seconds) and lead
# time (cdrom_scheduler_run_lead_time_seconds) — the DORA-style deployment
# indicators. The loop ticks every 5s, so the metrics land shortly after the
# run finishes.
log ">> creating a pipeline and triggering a run for the run metrics"
RUN_PIPELINE_BODY=$(cat <<'EOF'
{
  "name": "e2e-metrics-run-pipeline",
  "jobs": [
    {
      "key": "run",
      "name": "run",
      "target_group": "default",
      "spec": {
        "steps": [
          {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo run: done"]}}}
        ]
      }
    }
  ]
}
EOF
)
RUN_PIPELINE=$(curl -s -X POST "http://127.0.0.1:8080/api/pipelines" \
  -H 'Content-Type: application/json' -d "$RUN_PIPELINE_BODY")
RUN_PIPELINE_ID=$(printf '%s' "$RUN_PIPELINE" | jq -r '.id')
[ -n "$RUN_PIPELINE_ID" ] && [ "$RUN_PIPELINE_ID" != "null" ] \
  || { echo ">> could not parse pipeline id: $RUN_PIPELINE" >&2; exit 1; }
log "   pipeline id: $RUN_PIPELINE_ID"

RUN=$(curl -s -X POST "http://127.0.0.1:8080/api/pipelines/$RUN_PIPELINE_ID/runs" \
  -H 'Content-Type: application/json' -d '{"trigger":"manual"}')
RUN_ID=$(printf '%s' "$RUN" | jq -r '.id')
[ -n "$RUN_ID" ] && [ "$RUN_ID" != "null" ] \
  || { echo ">> could not parse run id: $RUN" >&2; exit 1; }
log "   run id: $RUN_ID"

log ">> waiting for the run to reach succeeded (3)"
RUN=""
for i in $(seq 1 100); do
  RUN=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/runs/$RUN_ID" || true)
  if [ "$(printf '%s' "$RUN" | jq -r '.status' 2>/dev/null)" = "3" ]; then
    break
  fi
  sleep 0.2
done
[ "$(printf '%s' "$RUN" | jq -r '.status' 2>/dev/null)" = "3" ] \
  || { echo ">> run did not reach status=succeeded: $RUN" >&2; exit 1; }

log ">> verifying the run is reflected in the scheduler's run metrics"
e2e_wait_for_metric scheduler "$E2E_METRICS_SCHEDULER" cdrom_scheduler_runs_total 'scheduler recorded the run status'
e2e_wait_for_metric scheduler "$E2E_METRICS_SCHEDULER" cdrom_scheduler_run_duration_seconds 'scheduler recorded the run duration'
e2e_wait_for_metric scheduler "$E2E_METRICS_SCHEDULER" cdrom_scheduler_run_lead_time_seconds 'scheduler recorded the run lead time'

# --- 2d. queue depth + concurrency slots (F-20, F-16) ------------------------
# The Database service exposes the pending queue's depth per group and the
# durable concurrency slots held per resource as point-in-time gauges, refreshed
# on a 5s background loop. Both emit a series only while non-zero. To make both
# non-zero at the same time, submit a job that holds a concurrency key (a key
# with no configured limit defaults to 1 — a lock) and sleeps long enough for a
# refresh to catch it, then a second job on the same key that is held pending
# (queue depth) while the first holds the slot.
log ">> submitting a job that holds a concurrency key (for the slots gauge)"
SLOTS_BODY=$(cat <<'EOF'
{
  "name": "e2e-metrics-slots",
  "target_group": "default",
  "spec": {
    "concurrency_keys": ["e2e-metrics-key"],
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo slots: holding e2e-metrics-key; sleep 15; echo slots: done"]}}}
    ]
  }
}
EOF
)
SLOTS_JOB=$(e2e_submit_job "$SLOTS_BODY")
log ">> slots job id: $SLOTS_JOB"

log ">> waiting for the slots job to be admitted and running (holding the key)"
S=""
for i in $(seq 1 100); do
  S=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs/$SLOTS_JOB" || true)
  if [ "$(printf '%s' "$S" | jq -r '.status' 2>/dev/null)" = "2" ]; then
    break
  fi
  sleep 0.2
done
[ "$(printf '%s' "$S" | jq -r '.status' 2>/dev/null)" = "2" ] \
  || { echo ">> slots job did not reach running: $S" >&2; exit 1; }

# A second job on the same key is held pending (not admitted) while the first
# holds the key, so the queue-depth gauge is non-zero too.
log ">> submitting a second job on the same key (held pending for the queue-depth gauge)"
QUEUE_BODY=$(cat <<'EOF'
{
  "name": "e2e-metrics-queue",
  "target_group": "default",
  "spec": {
    "concurrency_keys": ["e2e-metrics-key"],
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo queue: done"]}}}
    ]
  }
}
EOF
)
QUEUE_JOB=$(e2e_submit_job "$QUEUE_BODY")
log ">> queue job id: $QUEUE_JOB"

# Both gauges are non-zero while the first job holds the key (15s) and the
# second waits; the database's 5s refresh loop is guaranteed to catch both.
log ">> verifying the database recorded the concurrency slots and queue depth"
e2e_wait_for_metric database "$E2E_METRICS_DATABASE" cdrom_db_concurrency_slots 'database recorded the held concurrency slots'
e2e_wait_for_metric database "$E2E_METRICS_DATABASE" cdrom_db_queue_depth 'database recorded the pending queue depth'

# Let both jobs finish so the stack is left clean (soft wait: the metrics were
# already asserted above, so a slow host must not fail the test on cleanup).
log ">> waiting for the slots + queue jobs to finish"
for i in $(seq 1 100); do
  S=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs/$SLOTS_JOB" || true)
  Q=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs/$QUEUE_JOB" || true)
  if [ "$(printf '%s' "$S" | jq -r '.status' 2>/dev/null)" = "3" ] && [ "$(printf '%s' "$Q" | jq -r '.status' 2>/dev/null)" = "3" ]; then
    break
  fi
  sleep 0.2
done

# --- 3. the service ports do NOT serve /metrics ------------------------------
# The metrics live only on the dedicated ports. The API's UI HTTP port (8080)
# and the IdP's OIDC surface (7104) must not serve /metrics.
log ">> verifying the service ports do not serve /metrics"
API_SVC_CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8080/metrics || true)
[ "$API_SVC_CODE" != "200" ] \
  || { echo ">> API UI HTTP port 8080 serves /metrics (want only the dedicated metrics port)" >&2; exit 1; }
IDP_SVC_CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:7104/metrics || true)
[ "$IDP_SVC_CODE" != "200" ] \
  || { echo ">> IdP OIDC port 7104 serves /metrics (want only the dedicated metrics port)" >&2; exit 1; }
log "   API :8080/metrics -> $API_SVC_CODE, IdP :7104/metrics -> $IDP_SVC_CODE (both non-200, as expected)"

log ">> 12-metrics PASSED"
