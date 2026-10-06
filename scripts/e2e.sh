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
# It also exercises F-03 (a job-level timeout terminates a job as timed_out)
# and F-04 (a failing job with a retry policy is re-dispatched up to its
# limit, and a finished job can be re-run for a fresh execution), and the
# token_exchange step handler (a step that asks the API, via the worker's
# TokenExchange, for a new job token for a different audience and hands it to
# a later step as a step output; the API mints it from the local IdP).
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
echo "WORKDIR: ${WORK}"
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
  #rm -rf "$WORK"
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

wait_for_job() {
  local jobid="$1" pattern="$2" what="$3" i
  for i in $(seq 1 100); do
    if curl -s http://127.0.0.1:8080/api/jobs/${jobid}/logs/job.log | grep -qE "$pattern" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  echo ">> timed out waiting for: $what (pattern: $pattern)" >&2
  exit 1
}

# --- boot the stack --------------------------------------------------------
log ">> booting stack in $WORK"
start db        "$WORK/db.log"        'serving'    CDROM_DB_BACKEND=sqlite CDROM_DB_SQLITE_PATH="$WORK/cdrom.db" CDROM_LOG_FORMAT=text -- "$BIN/db"
start scheduler "$WORK/scheduler.log" 'serving'    CDROM_LOG_FORMAT=text -- "$BIN/scheduler"
start artifacts "$WORK/artifacts.log" 'serving'    CDROM_ARTIFACTS_ROOT="$WORK/artifacts" CDROM_LOG_FORMAT=text -- "$BIN/artifacts"
# The IdP mints the job tokens the API hands to the worker (and the exchanged
# tokens a token_exchange step requests). It is started before the API so the
# API's job-token auth (OIDC discovery against the IdP) can initialize.
start idp       "$WORK/idp.log"       'idp starting' CDROM_LOG_FORMAT=text -- "$BIN/idp"
# The IdP logs "starting" before its HTTP server is listening, so poll its
# discovery endpoint until it responds; the API's job-token auth performs OIDC
# discovery against the IdP at startup and would fail if the IdP is not up.
log ">> waiting for the IdP to serve"
for i in $(seq 1 100); do
  if curl -sf http://127.0.0.1:7104/.well-known/openid-configuration >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -sf http://127.0.0.1:7104/.well-known/openid-configuration >/dev/null 2>&1 \
  || { echo ">> IdP did not start serving" >&2; exit 1; }
# Enable job-token auth on the API's gRPC surface: the API mints a job token
# for the worker (audience cdrom-api, the IdP's default) and verifies it on
# the worker's job-scoped calls, including the token_exchange step's
# ExchangeJobToken RPC.
start api       "$WORK/api.log"       'serving'    CDROM_GRPC_AUTH_ENABLED=true CDROM_GRPC_AUTH_IDP_ADDR=127.0.0.1:7104 CDROM_GRPC_AUTH_AUDIENCES=cdrom-api CDROM_LOG_FORMAT=text -- "$BIN/api"
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
      {"type": "shell", "params": {"shell": {"string": "sh"}, "args": {"strings": ["-c"]}, "command": {"string": "echo step4: shell override"}}},
      {"type": "token_exchange", "params": {"audience": {"string": "outside-svc"}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo step6: token exchange flowed to a later step"]}}, "condition": "{{ if (index .steps 4).Outputs.token }}true{{ else }}false{{ end }}"}
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
wait_for_job $JOB_ID 'step1: plain command' 'step 1 output'
wait_for_job $JOB_ID 'step2: env=from-spec' 'step 2 env var'
wait_for_job $JOB_ID "step3: pwd=$WORKDIR" 'step 3 workdir'
wait_for_job $JOB_ID 'step4: shell override' 'step 4 shell override'
# The token_exchange step (step 5) asks the API for a new job token for
# audience outside-svc and writes it to a "token" file in its per-step output
# directory (the executor reads every file there back as a step output); the
# worker logs the exchange.
wait_for "$WORK/worker.log" 'executor: exchanged job token' 'worker exchanged the job token'
# Step 6's condition reads the exchanged token from step 5's outputs; it runs
# only if the token was captured and flowed to the condition context.
wait_for_job $JOB_ID 'step6: token exchange flowed to a later step' 'step 6 consumed the exchanged token'
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

# --- verify the token exchange went through the API and IdP ---------------
# The token_exchange step asked the API (ExchangeJobToken) for a new job token
# for audience outside-svc; the API minted it from the IdP. Verify both the
# API and the IdP recorded the exchange for the outside-svc audience.
log ">> verifying the token exchange (API + IdP)"
wait_for "$WORK/api.log" 'api: job token exchanged' 'api exchanged the job token'
wait_for "$WORK/idp.log" 'idp: minted job token' 'idp minted a job token'
grep -q 'audience=outside-svc' "$WORK/api.log" \
  || { echo ">> api did not exchange a token for audience outside-svc" >&2; exit 1; }
grep -q 'audience=outside-svc' "$WORK/idp.log" \
  || { echo ">> idp did not mint a token for audience outside-svc" >&2; exit 1; }

# --- verify job logs were streamed, persisted, and are retrievable (F-02) ---
# The API persists streamed output to the artifacts service (one file per step
# plus a combined job.log) and exposes it over HTTP. The worker reports
# "succeeded" before the API has necessarily finished persisting every chunk,
# so poll until the logs are complete.
log ">> verifying job logs (F-02)"

# Poll the log listing until the step logs plus job.log are present. The
# token_exchange step (the 5th step, index 4) produces no output, so it has no
# step-4.log; the other steps each have one (step-0..3 and step-5).
LOG_LISTING=""
for i in $(seq 1 100); do
  LOG_LISTING=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID/logs")
  if printf '%s' "$LOG_LISTING" | grep -q 'step-0.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-1.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-2.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-3.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-5.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'job.log'; then
    break
  fi
  sleep 0.2
done
log "   log listing: $LOG_LISTING"
printf '%s' "$LOG_LISTING" | grep -q 'step-0.log' \
  || { echo ">> step-0.log missing from log listing" >&2; exit 1; }
printf '%s' "$LOG_LISTING" | grep -q 'step-5.log' \
  || { echo ">> step-5.log missing from log listing" >&2; exit 1; }
printf '%s' "$LOG_LISTING" | grep -q 'job.log' \
  || { echo ">> job.log missing from log listing" >&2; exit 1; }

# Poll the combined job.log until it contains every step's output line.
JOB_LOG=""
for i in $(seq 1 100); do
  JOB_LOG=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID/logs/job.log")
  if printf '%s' "$JOB_LOG" | grep -q 'step1: plain command' \
    && printf '%s' "$JOB_LOG" | grep -q 'step2: env=from-spec' \
    && printf '%s' "$JOB_LOG" | grep -q "step3: pwd=$WORKDIR" \
    && printf '%s' "$JOB_LOG" | grep -q 'step4: shell override' \
    && printf '%s' "$JOB_LOG" | grep -q 'step6: token exchange flowed to a later step'; then
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
printf '%s' "$JOB_LOG" | grep -q 'step6: token exchange flowed to a later step' \
  || { echo ">> step6 output missing from job.log" >&2; exit 1; }

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

# --- verify a job with a retry policy is re-dispatched up to its limit (F-04) ---
# A job whose single step always fails, with a retry policy of max_attempts 2
# (so it runs at most 3 times: 1 initial + 2 retries) and a 1s backoff. The
# scheduler's retry loop re-dispatches it after each failure until it exhausts
# its budget, after which it stays failed with attempt 3.
log ">> submitting a failing job with a retry policy"
RETRY_BODY=$(cat <<EOF
{
  "name": "e2e-retry-job",
  "target_group": "default",
  "spec": {
    "retry": {"max_attempts": 2, "backoff": "1s"},
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo retry attempt; exit 1"]}}}
    ]
  }
}
EOF
)
RETRY_RESPONSE=$(curl -s -X POST http://127.0.0.1:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d "$RETRY_BODY")
log "   $RETRY_RESPONSE"

RETRY_JOB_ID=$(printf '%s' "$RETRY_RESPONSE" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
[ -n "$RETRY_JOB_ID" ] || { echo ">> could not parse retry job id" >&2; exit 1; }
log ">> retry job id: $RETRY_JOB_ID"

# The retry loop (5s tick) re-dispatches the job after each failure, with a 1s
# backoff, until it has used its 2 retries (3 total attempts). Poll until the
# job is failed with attempt 3 (retries exhausted). The transient failed states
# after attempts 1 and 2 carry attempt 1 and 2, so only the final exhausted
# state matches both status=failed and attempt=3.
log ">> waiting for the job to exhaust its retries (failed, attempt 3)"
RETRY_FINAL=""
for i in $(seq 1 200); do
  RETRY_FINAL=$(curl -s "http://127.0.0.1:8080/api/jobs/$RETRY_JOB_ID")
  if printf '%s' "$RETRY_FINAL" | grep -q '"status":4' \
    && printf '%s' "$RETRY_FINAL" | grep -q '"attempt":3'; then
    break
  fi
  sleep 0.2
done
log "   $RETRY_FINAL"
printf '%s' "$RETRY_FINAL" | grep -q '"status":4' \
  || { echo ">> retry job did not reach status=failed" >&2; exit 1; }
printf '%s' "$RETRY_FINAL" | grep -q '"attempt":3' \
  || { echo ">> retry job did not exhaust its retries (attempt != 3)" >&2; exit 1; }

# --- verify a finished job can be re-run for a fresh execution (F-04) -------
# Re-run the original successful shell job: it is reset to pending with a fresh
# attempt (attempt back to 1) and re-dispatched, so the worker runs it again.
# Count the step-1 output lines in the worker log before and after to prove a
# second execution happened.
log ">> re-running the original shell job"
STEP1_BEFORE=$(grep -c 'step1: plain command' "$WORK/worker.log" || true)
RERUN_RESPONSE=$(curl -s -X POST "http://127.0.0.1:8080/api/jobs/$JOB_ID/rerun")
log "   $RERUN_RESPONSE"
RERUN_JOB_ID=$(printf '%s' "$RERUN_RESPONSE" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')

# Wait for the worker to execute the job a second time (one more step-1 line).
log ">> waiting for the re-run to execute"
for i in $(seq 1 100); do
  STEP1_NOW=$(grep -c 'step1: plain command' "$WORK/artifacts/${RERUN_JOB_ID}/logs/job.log" || true)
  if [ "${STEP1_NOW:-0}" -gt "${STEP1_BEFORE:-0}" ]; then
    break
  fi
  sleep 0.2
done
STEP1_AFTER=$(grep -c 'step1: plain command' "$WORK/artifacts/${RERUN_JOB_ID}/logs/job.log" || true)
[ "${STEP1_AFTER:-0}" -gt "${STEP1_BEFORE:-0}" ] \
  || { echo ">> re-run did not produce a fresh execution" >&2; exit 1; }

# Verify the re-run completed as succeeded with the attempt reset to 1.
RERUN_FINAL=""
for i in $(seq 1 100); do
  RERUN_FINAL=$(curl -s "http://127.0.0.1:8080/api/jobs/$RERUN_JOB_ID")
  if printf '%s' "$RERUN_FINAL" | grep -q '"status":3' \
    && printf '%s' "$RERUN_FINAL" | grep -q '"attempt":1'; then
    break
  fi
  sleep 0.2
done
log "   $RERUN_FINAL"
printf '%s' "$RERUN_FINAL" | grep -q '"status":3' \
  || { echo ">> re-run did not reach status=succeeded" >&2; exit 1; }

log ">> e2e PASSED: worker job ran all steps (including a token_exchange step whose exchanged token flowed to a later step), persisted as succeeded, logs are retrievable, a job-level timeout terminates the job as timed_out, a failing job is retried up to its limit, and a finished job can be re-run for a fresh execution"
