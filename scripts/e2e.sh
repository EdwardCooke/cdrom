#!/usr/bin/env bash
#
# e2e.sh — end-to-end smoke test for the cdrom pipeline.
#
# Boots a full local stack (db, scheduler, artifacts, api, worker) against a
# throwaway SQLite database, submits a multi-step shell job to the worker
# group, and verifies the job runs to completion, is persisted as succeeded,
# and its step output was streamed to the API, persisted to the artifacts
# service, and is retrievable over HTTP (F-02). Exercises the full path:
#
#   submit (HTTP) -> api -> scheduler -> db
#        -> api dispatch -> worker WatchJobs stream -> worker executes steps
#        -> step output streamed to api (StreamJobLogs) -> persisted to
#           artifacts (step-<n>.log + job.log) -> retrievable via
#           GET /api/jobs/{id}/logs[/{name}]
#        -> status reported back -> persisted as succeeded
#
# The job spec covers the three JobSpec features: a plain command, a per-step
# env var, and a per-step workdir.
#
# Usage: scripts/e2e.sh
#
# Requires: the built binaries in ./bin (run `make build` first) and curl.
#
# The script always tears the stack down and removes its temp directory on
# exit, even on failure.

set -euo pipefail

cd "$(dirname "$0")/.."

BIN=./bin
WORK="$(mktemp -d /tmp/cdrom-e2e.XXXXXX)"
WORKDIR="$WORK/workdir"
mkdir -p "$WORK/artifacts" "$WORKDIR"

# --- cleanup ---------------------------------------------------------------
PIDS=()
cleanup() {
  local code=$?
  if [ "${#PIDS[@]}" -gt 0 ]; then
    for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    sleep 1
    for pid in "${PIDS[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
  fi
  rm -rf "$WORK"
  if [ "$code" -ne 0 ]; then
    echo ">> e2e FAILED (exit $code)" >&2
    exit "$code"
  fi
}
trap cleanup EXIT

# --- helpers ---------------------------------------------------------------
log() { printf '%s\n' "$*"; }

# start <name> <logfile> <ready-pattern> <env...> -- <cmd...>
# Launches a service in the background, records its PID, and waits until its
# log matches <ready-pattern> (or the process dies / times out).
start() {
  local name="$1" logfile="$2" pattern="$3"; shift 3
  local envs=() cmd=() seen=0 a
  for a in "$@"; do
    if [ "$a" = "--" ]; then seen=1; continue; fi
    if [ "$seen" -eq 0 ]; then envs+=("$a"); else cmd+=("$a"); fi
  done
  log ">> starting $name"
  env "${envs[@]}" "${cmd[@]}" >"$logfile" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  local i
  for i in $(seq 1 50); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo ">> $name exited early:" >&2
      cat "$logfile" >&2
      exit 1
    fi
    if grep -qE "$pattern" "$logfile" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  echo ">> $name did not become ready in time (pattern: $pattern):" >&2
  cat "$logfile" >&2
  exit 1
}

# wait_for <logfile> <pattern> <what>
wait_for() {
  local logfile="$1" pattern="$2" what="$3" i
  for i in $(seq 1 100); do
    if grep -qE "$pattern" "$logfile" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  echo ">> timed out waiting for: $what (pattern: $pattern)" >&2
  tail -n 40 "$logfile" >&2
  exit 1
}

# --- boot the stack --------------------------------------------------------
log ">> booting stack in $WORK"
start db        "$WORK/db.log"        'serving'    CDROM_DB_BACKEND=sqlite CDROM_DB_SQLITE_PATH="$WORK/cdrom.db" CDROM_LOG_FORMAT=text -- "$BIN/db"
start scheduler "$WORK/scheduler.log" 'serving'    CDROM_LOG_FORMAT=text -- "$BIN/scheduler"
start artifacts "$WORK/artifacts.log" 'serving'    CDROM_ARTIFACTS_ROOT="$WORK/artifacts" CDROM_LOG_FORMAT=text -- "$BIN/artifacts"
start api       "$WORK/api.log"       'serving'    CDROM_LOG_FORMAT=text -- "$BIN/api"
start worker    "$WORK/worker.log"    'registered' CDROM_LOG_FORMAT=text -- "$BIN/worker"

# Gate on the API having the worker's WatchJobs stream open before dispatching.
wait_for "$WORK/api.log" 'worker watching' 'worker watch stream on the api'

# --- submit a multi-step shell job to the worker group ---------------------
# The heredoc is unquoted so $WORKDIR expands to the real path; the shell
# snippets ($MY_VAR, $(pwd)) are escaped so they reach the worker verbatim.
# Step 4 names its step type explicitly ("shell") and uses the `shell`
# override: the worker runs `<shell> <args> <command>` (sh -c "echo step4:
# ...") instead of executing command directly. This proves the step-type
# discriminator is honored end-to-end (a step's type selects its handler).
log ">> submitting worker job"
BODY=$(cat <<EOF
{
  "name": "e2e-shell-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo step1: plain command"]}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo step2: env=\$MY_VAR"]}}, "env": {"MY_VAR": "from-spec"}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo step3: pwd=\$(pwd)"]}}, "workdir": "$WORKDIR"},
      {"type": "shell", "params": {"shell": {"string": "sh"}, "args": {"strings": ["-c"]}, "command": {"string": "echo step4: shell override"}}}
    ]
  }
}
EOF
)
RESPONSE=$(curl -s -X POST http://127.0.0.1:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d "$BODY")
log "   $RESPONSE"

JOB_ID=$(printf '%s' "$RESPONSE" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
[ -n "$JOB_ID" ] || { echo ">> could not parse job id from response" >&2; exit 1; }
log ">> job id: $JOB_ID"

# --- verify the worker executed every step ---------------------------------
log ">> waiting for the worker to execute the job"
wait_for "$WORK/worker.log" 'step1: plain command' 'step 1 output'
wait_for "$WORK/worker.log" 'step2: env=from-spec' 'step 2 env var'
wait_for "$WORK/worker.log" "step3: pwd=$WORKDIR" 'step 3 workdir'
wait_for "$WORK/worker.log" 'step4: shell override' 'step 4 shell override'
wait_for "$WORK/worker.log" 'job succeeded' 'job success'

# --- verify the API persisted the job as succeeded -------------------------
log ">> waiting for persisted status=succeeded (3)"
FINAL=""
for i in $(seq 1 100); do
  FINAL=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID")
  if printf '%s' "$FINAL" | grep -q '"status":3'; then
    break
  fi
  sleep 0.2
done
log "   $FINAL"
printf '%s' "$FINAL" | grep -q '"status":3' \
  || { echo ">> job did not reach status=succeeded" >&2; exit 1; }

# --- verify job logs were streamed, persisted, and are retrievable (F-02) ---
# The API persists streamed output to the artifacts service (one file per step
# plus a combined job.log) and exposes it over HTTP. The worker reports
# "succeeded" before the API has necessarily finished persisting every chunk,
# so poll until the logs are complete.
log ">> verifying job logs (F-02)"

# Poll the log listing until all four step logs plus job.log are present.
LOG_LISTING=""
for i in $(seq 1 100); do
  LOG_LISTING=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID/logs")
  if printf '%s' "$LOG_LISTING" | grep -q 'step-0.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-1.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-2.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-3.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'job.log'; then
    break
  fi
  sleep 0.2
done
log "   log listing: $LOG_LISTING"
printf '%s' "$LOG_LISTING" | grep -q 'step-0.log' \
  || { echo ">> step-0.log missing from log listing" >&2; exit 1; }
printf '%s' "$LOG_LISTING" | grep -q 'job.log' \
  || { echo ">> job.log missing from log listing" >&2; exit 1; }

# Poll the combined job.log until it contains every step's output line.
JOB_LOG=""
for i in $(seq 1 100); do
  JOB_LOG=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID/logs/job.log")
  if printf '%s' "$JOB_LOG" | grep -q 'step1: plain command' \
    && printf '%s' "$JOB_LOG" | grep -q 'step2: env=from-spec' \
    && printf '%s' "$JOB_LOG" | grep -q "step3: pwd=$WORKDIR" \
    && printf '%s' "$JOB_LOG" | grep -q 'step4: shell override'; then
    break
  fi
  sleep 0.2
done
log "   job.log contents:"
printf '%s\n' "$JOB_LOG" | sed 's/^/     /'
printf '%s' "$JOB_LOG" | grep -q 'step1: plain command' \
  || { echo ">> step1 output missing from job.log" >&2; exit 1; }
printf '%s' "$JOB_LOG" | grep -q 'step2: env=from-spec' \
  || { echo ">> step2 output missing from job.log" >&2; exit 1; }
printf '%s' "$JOB_LOG" | grep -q "step3: pwd=$WORKDIR" \
  || { echo ">> step3 output missing from job.log" >&2; exit 1; }
printf '%s' "$JOB_LOG" | grep -q 'step4: shell override' \
  || { echo ">> step4 output missing from job.log" >&2; exit 1; }

# Verify a per-step log is retrievable and contains only that step's output.
STEP0_LOG=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID/logs/step-0.log")
log "   step-0.log: $STEP0_LOG"
printf '%s' "$STEP0_LOG" | grep -q 'step1: plain command' \
  || { echo ">> step1 output missing from step-0.log" >&2; exit 1; }
printf '%s' "$STEP0_LOG" | grep -q 'step2' \
  && { echo ">> step-0.log contains step2 output (step attribution broken)" >&2; exit 1; }

# --- verify a job-level timeout terminates the job (F-03) ------------------
# A job whose single step sleeps well past the job's declared timeout is
# terminated by the worker and reported as timed_out (status 6), not failed.
# The step declares no per-step timeout, so only the job-level timeout applies.
log ">> submitting a job that exceeds its job-level timeout"
TIMEOUT_BODY=$(cat <<EOF
{
  "name": "e2e-timeout-job",
  "target_group": "default",
  "spec": {
    "timeout": "2s",
    "steps": [
      {"params": {"command": {"string": "sleep"}, "args": {"strings": ["30"]}}}
    ]
  }
}
EOF
)
TIMEOUT_RESPONSE=$(curl -s -X POST http://127.0.0.1:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d "$TIMEOUT_BODY")
log "   $TIMEOUT_RESPONSE"

TIMEOUT_JOB_ID=$(printf '%s' "$TIMEOUT_RESPONSE" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
[ -n "$TIMEOUT_JOB_ID" ] || { echo ">> could not parse timeout job id" >&2; exit 1; }
log ">> timeout job id: $TIMEOUT_JOB_ID"

# The worker terminates the sleeping step at the job's 2s deadline and reports
# timed_out. Wait for the worker to log the timeout.
wait_for "$WORK/worker.log" 'job timed out' 'worker reports the job timed out'

# Verify the API persisted the job as timed_out (status 6).
TIMEOUT_FINAL=""
for i in $(seq 1 100); do
  TIMEOUT_FINAL=$(curl -s "http://127.0.0.1:8080/api/jobs/$TIMEOUT_JOB_ID")
  if printf '%s' "$TIMEOUT_FINAL" | grep -q '"status":6'; then
    break
  fi
  sleep 0.2
done
log "   $TIMEOUT_FINAL"
printf '%s' "$TIMEOUT_FINAL" | grep -q '"status":6' \
  || { echo ">> timeout job did not reach status=timed_out" >&2; exit 1; }

log ">> e2e PASSED: worker job ran all steps, persisted as succeeded, logs are retrievable, and a job-level timeout terminates the job as timed_out"
