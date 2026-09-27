#!/usr/bin/env bash
#
# e2e.sh — end-to-end smoke test for the cdrom pipeline.
#
# Boots a full local stack (db, scheduler, artifacts, api, worker) against a
# throwaway SQLite database, submits a multi-step shell job to the worker
# group, and verifies the job runs to completion and is persisted as
# succeeded. Exercises the full path:
#
#   submit (HTTP) -> api -> scheduler -> db
#        -> api dispatch -> worker WatchJobs stream -> worker executes steps
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
log ">> submitting worker job"
BODY=$(cat <<EOF
{
  "name": "e2e-shell-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"command": "sh", "args": ["-c", "echo step1: plain command"]},
      {"command": "sh", "args": ["-c", "echo step2: env=\$MY_VAR"], "env": {"MY_VAR": "from-spec"}},
      {"command": "sh", "args": ["-c", "echo step3: pwd=\$(pwd)"], "workdir": "$WORKDIR"}
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

log ">> e2e PASSED: worker job ran all steps and persisted as succeeded"
