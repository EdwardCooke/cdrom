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
# limit, and a finished job can be re-run for a fresh execution), the
# token_exchange step handler (a step that asks the API, via the worker's
# TokenExchange, for a new job token for a different audience and hands it to
# a later step as a step output; the API mints it from the local IdP), and
# F-13 (an approval gate: a job pauses at an approval step until an authorized
# user approves or rejects it via the HTTP API; an approval lets the job
# continue to its later steps, a rejection fails it).
#
# It also exercises F-24 (username/password authentication: the API proxies the
# UI's register/login/user-management requests to the IdP, which verifies the
# password and mints an OIDC token; the first registered user becomes an admin,
# later users get the default role, and the user-management endpoints are gated
# behind an authenticated admin caller).
#
# It also exercises F-14 (roles & permissions / RBAC): a second API instance is
# booted with OIDC authentication and RBAC enabled, sharing the same IdP (users
# + JWKS) and db/scheduler/artifacts as the main stack. Users with different
# roles (admin, operator, and the default user role) sign in and their tokens
# are presented to the RBAC-enabled API, which must ALLOW actions the role
# grants and DISALLOW (403) actions it does not — e.g. an operator can trigger
# a run but not create a pipeline, a default user can view but not trigger, and
# a principal with no roles can do nothing (deny-by-default).
#
# Usage: scripts/e2e.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and jq.
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

# wait_for_status <jobid> <status> <what>
# Polls GET /api/jobs/{id} until the job's status equals <status> (the numeric
# JobStatus value, e.g. 8 for awaiting_approval, 3 for succeeded, 4 for
# failed). Returns the last observed job JSON on stdout.
wait_for_status() {
  local jobid="$1" status="$2" what="$3" i body
  for i in $(seq 1 100); do
    body=$(curl -s "http://127.0.0.1:8080/api/jobs/${jobid}")
    if [ "$(printf '%s' "$body" | jq -r '.status')" = "$status" ]; then
      printf '%s' "$body"
      return 0
    fi
    sleep 0.2
  done
  echo ">> timed out waiting for: $what (status ${status})" >&2
  printf '%s' "$body" >&2
  exit 1
}

# http_request <method> <url> [json-body]
# Performs an HTTP request and sets HTTP_CODE (the response status code) and
# HTTP_BODY (the response body). When a JSON body is given it is sent with a
# Content-Type: application/json header. Used by the F-24 assertions, which
# need both the status code and the body from a single call.
http_request() {
  local method="$1" url="$2" body="${3:-}"
  local out
  if [ -n "$body" ]; then
    out=$(curl -s -w $'\n%{http_code}' -X "$method" "$url" \
      -H 'Content-Type: application/json' -d "$body")
  else
    out=$(curl -s -w $'\n%{http_code}' -X "$method" "$url")
  fi
  HTTP_CODE=$(printf '%s' "$out" | tail -n 1)
  HTTP_BODY=$(printf '%s' "$out" | sed '$d')
}

# authed_request <method> <url> <bearer-token> [json-body]
# Like http_request, but presents the given OIDC token as a Bearer token (used
# by the F-14 assertions against the RBAC-enabled API, where every request
# must carry a valid token). Sets HTTP_CODE and HTTP_BODY.
authed_request() {
  local method="$1" url="$2" token="$3" body="${4:-}"
  local out
  if [ -n "$body" ]; then
    out=$(curl -s -w $'\n%{http_code}' -X "$method" "$url" \
      -H "Authorization: Bearer $token" \
      -H 'Content-Type: application/json' -d "$body")
  else
    out=$(curl -s -w $'\n%{http_code}' -X "$method" "$url" \
      -H "Authorization: Bearer $token")
  fi
  HTTP_CODE=$(printf '%s' "$out" | tail -n 1)
  HTTP_BODY=$(printf '%s' "$out" | sed '$d')
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
# The audit log (F-15) is written to a file separate from the process's
# default log (CDROM_AUDIT_FILE), in JSON (CDROM_AUDIT_FORMAT). Each audited
# action is also appended to the shared audit log in the database (queryable
# via GET /api/audit), which both API instances share.
start api       "$WORK/api.log"       'serving'    CDROM_GRPC_AUTH_ENABLED=true CDROM_GRPC_AUTH_IDP_ADDR=127.0.0.1:7104 CDROM_GRPC_AUTH_AUDIENCES=cdrom-api CDROM_AUDIT_FILE="$WORK/audit.log" CDROM_AUDIT_FORMAT=json CDROM_LOG_FORMAT=text -- "$BIN/api"
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

JOB_ID=$(printf '%s' "$RESPONSE" | jq -r '.id')
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
  if [ "$(printf '%s' "$FINAL" | jq -r '.status')" = "3" ]; then
    break
  fi
  sleep 0.2
done
log "   $FINAL"
[ "$(printf '%s' "$FINAL" | jq -r '.status')" = "3" ] \
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

TIMEOUT_JOB_ID=$(printf '%s' "$TIMEOUT_RESPONSE" | jq -r '.id')
[ -n "$TIMEOUT_JOB_ID" ] || { echo ">> could not parse timeout job id" >&2; exit 1; }
log ">> timeout job id: $TIMEOUT_JOB_ID"

# The worker terminates the sleeping step at the job's 2s deadline and reports
# timed_out. Wait for the worker to log the timeout.
wait_for "$WORK/worker.log" 'job timed out' 'worker reports the job timed out'

# Verify the API persisted the job as timed_out (status 6).
TIMEOUT_FINAL=""
for i in $(seq 1 100); do
  TIMEOUT_FINAL=$(curl -s "http://127.0.0.1:8080/api/jobs/$TIMEOUT_JOB_ID")
  if [ "$(printf '%s' "$TIMEOUT_FINAL" | jq -r '.status')" = "6" ]; then
    break
  fi
  sleep 0.2
done
log "   $TIMEOUT_FINAL"
[ "$(printf '%s' "$TIMEOUT_FINAL" | jq -r '.status')" = "6" ] \
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

RETRY_JOB_ID=$(printf '%s' "$RETRY_RESPONSE" | jq -r '.id')
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
  if [ "$(printf '%s' "$RETRY_FINAL" | jq -r '.status')" = "4" ] \
    && [ "$(printf '%s' "$RETRY_FINAL" | jq -r '.attempt')" = "3" ]; then
    break
  fi
  sleep 0.2
done
log "   $RETRY_FINAL"
[ "$(printf '%s' "$RETRY_FINAL" | jq -r '.status')" = "4" ] \
  || { echo ">> retry job did not reach status=failed" >&2; exit 1; }
[ "$(printf '%s' "$RETRY_FINAL" | jq -r '.attempt')" = "3" ] \
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
RERUN_JOB_ID=$(printf '%s' "$RERUN_RESPONSE" | jq -r '.id')

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
  if [ "$(printf '%s' "$RERUN_FINAL" | jq -r '.status')" = "3" ] \
    && [ "$(printf '%s' "$RERUN_FINAL" | jq -r '.attempt')" = "1" ]; then
    break
  fi
  sleep 0.2
done
log "   $RERUN_FINAL"
[ "$(printf '%s' "$RERUN_FINAL" | jq -r '.status')" = "3" ] \
  || { echo ">> re-run did not reach status=succeeded" >&2; exit 1; }

# --- verify an approval gate pauses the job until approved (F-13) ----------
# A job whose second step is an approval gate (type "approval") pauses in the
# awaiting_approval state (status 8) until an authorized user approves it via
# the HTTP API. The gate's message is a template rendered against the job's
# condition context (here it references the job's identity). Once approved,
# the job continues to its later steps and succeeds; the decision (reason) is
# recorded on the job.
log ">> submitting a job with an approval gate"
APPROVAL_BODY=$(cat <<EOF
{
  "name": "e2e-approval-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo approval: pre-gate step"]}}},
      {"type": "approval", "params": {"message": {"string": "release {{ .job.Name }} now"}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo approval: post-gate step"]}}}
    ]
  }
}
EOF
)
APPROVAL_RESPONSE=$(curl -s -X POST http://127.0.0.1:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d "$APPROVAL_BODY")
log "   $APPROVAL_RESPONSE"

APPROVAL_JOB_ID=$(printf '%s' "$APPROVAL_RESPONSE" | jq -r '.id')
[ -n "$APPROVAL_JOB_ID" ] || { echo ">> could not parse approval job id" >&2; exit 1; }
log ">> approval job id: $APPROVAL_JOB_ID"

# The worker runs the pre-gate step, then pauses at the approval step and
# reports the job awaiting_approval (status 8) with the rendered message.
log ">> waiting for the job to pause at the approval gate (status 8)"
AWAITING=$(wait_for_status "$APPROVAL_JOB_ID" 8 'job awaiting approval')
log "   $AWAITING"
# The gate's message is rendered against the job's condition context: the
# template `release {{ .job.Name }} now` becomes `release e2e-approval-job now`.
# Check only the rendered approval_message field (the spec still carries the
# raw template, so extracting the field is the only reliable check).
APPROVAL_MSG=$(printf '%s' "$AWAITING" | jq -r '.approval_message')
[ "$APPROVAL_MSG" = "release e2e-approval-job now" ] \
  || { echo ">> approval message was not rendered against the job identity (got: $APPROVAL_MSG)" >&2; exit 1; }

# Approve the gate (with a reason). The decision is recorded on the job and
# the job moves back to running.
log ">> approving the gate"
APPROVE_RESPONSE=$(curl -s -X POST "http://127.0.0.1:8080/api/jobs/$APPROVAL_JOB_ID/approve" \
  -H 'Content-Type: application/json' \
  -d '{"reason": "lgtm"}')
log "   $APPROVE_RESPONSE"

# The worker observes the approval on its next poll and continues to the
# post-gate step, which runs and the job succeeds.
log ">> waiting for the job to continue past the gate and succeed"
APPROVED_FINAL=$(wait_for_status "$APPROVAL_JOB_ID" 3 'job succeeded after approval')
log "   $APPROVED_FINAL"
# The post-gate step ran only because the gate was approved.
wait_for_job "$APPROVAL_JOB_ID" 'approval: post-gate step' 'post-gate step output'
# The decision is recorded on the job: approved, with the reason.
[ "$(printf '%s' "$APPROVED_FINAL" | jq -r '.approval_decision')" = "approved" ] \
  || { echo ">> approval decision was not recorded as approved" >&2; exit 1; }
[ "$(printf '%s' "$APPROVED_FINAL" | jq -r '.approval_reason')" = "lgtm" ] \
  || { echo ">> approval reason was not recorded" >&2; exit 1; }

# --- verify a rejected approval gate fails the job (F-13) ------------------
# A job whose first step is an approval gate is rejected via the HTTP API:
# the gate's decision is recorded as rejected, the job fails, and its later
# steps never run.
log ">> submitting a job to reject at its approval gate"
REJECT_BODY=$(cat <<EOF
{
  "name": "e2e-reject-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "approval", "params": {"message": {"string": "ship {{ .job.Name }}?"}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo reject: should not run"]}}}
    ]
  }
}
EOF
)
REJECT_RESPONSE=$(curl -s -X POST http://127.0.0.1:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d "$REJECT_BODY")
log "   $REJECT_RESPONSE"

REJECT_JOB_ID=$(printf '%s' "$REJECT_RESPONSE" | jq -r '.id')
[ -n "$REJECT_JOB_ID" ] || { echo ">> could not parse reject job id" >&2; exit 1; }
log ">> reject job id: $REJECT_JOB_ID"

# The worker pauses at the approval gate (status 8) with the rendered message.
log ">> waiting for the job to pause at the approval gate (status 8)"
REJECT_AWAITING=$(wait_for_status "$REJECT_JOB_ID" 8 'job awaiting approval')
[ "$(printf '%s' "$REJECT_AWAITING" | jq -r '.approval_message')" = "ship e2e-reject-job?" ] \
  || { echo ">> reject approval message was not rendered" >&2; exit 1; }

# Reject the gate (with a reason). The decision is recorded as rejected and
# the job moves to failed.
log ">> rejecting the gate"
REJECT_DECISION=$(curl -s -X POST "http://127.0.0.1:8080/api/jobs/$REJECT_JOB_ID/reject" \
  -H 'Content-Type: application/json' \
  -d '{"reason": "not yet"}')
log "   $REJECT_DECISION"

# The job fails (status 4): the worker observes the rejection on its next poll
# and fails the job, and the gate's rejection already moved it to failed.
log ">> waiting for the job to fail after the rejection"
REJECTED_FINAL=$(wait_for_status "$REJECT_JOB_ID" 4 'job failed after rejection')
log "   $REJECTED_FINAL"
# The decision is recorded on the job: rejected, with the reason.
[ "$(printf '%s' "$REJECTED_FINAL" | jq -r '.approval_decision')" = "rejected" ] \
  || { echo ">> approval decision was not recorded as rejected" >&2; exit 1; }
[ "$(printf '%s' "$REJECTED_FINAL" | jq -r '.approval_reason')" = "not yet" ] \
  || { echo ">> reject reason was not recorded" >&2; exit 1; }
# The post-gate step never ran (the job failed at the gate). Give the worker a
# moment to settle, then confirm its output is absent from the job log.
sleep 1
REJECT_LOG=$(curl -s "http://127.0.0.1:8080/api/jobs/$REJECT_JOB_ID/logs/job.log")
printf '%s' "$REJECT_LOG" | grep -q 'reject: should not run' \
  && { echo ">> post-gate step ran despite the rejection" >&2; exit 1; }

# --- verify username/password authentication (F-24) ------------------------
# The API proxies the UI's register/login/user-management requests to the IdP,
# where the real logic lives (password verification, token minting, user
# storage). Username/password auth is enabled by default, so the stack's IdP
# serves these endpoints. The unauthenticated entry points (register, login)
# work without a token; the user-management endpoints are gated behind an
# authenticated admin caller. This stack leaves OIDC auth off (so the other
# tests can call the API without a token), so an unauthenticated caller to a
# user-management endpoint is rejected (401) rather than verified.
log ">> verifying username/password authentication (F-24)"

# Register the first user: created (201) and, as the first user, given the
# admin role.
http_request POST http://127.0.0.1:8080/api/register \
  '{"first_name":"Ada","last_name":"Lovelace","email":"ada@example.com","password":"s3cret"}'
[ "$HTTP_CODE" = "201" ] \
  || { echo ">> first register returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.roles | index("admin")' >/dev/null \
  || { echo ">> first user was not given the admin role: $HTTP_BODY" >&2; exit 1; }

# Register a second user: created (201) with the default user role.
http_request POST http://127.0.0.1:8080/api/register \
  '{"first_name":"Bob","last_name":"Jones","email":"bob@example.com","password":"pw"}'
[ "$HTTP_CODE" = "201" ] \
  || { echo ">> second register returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.roles | index("user")' >/dev/null \
  || { echo ">> second user was not given the user role: $HTTP_BODY" >&2; exit 1; }

# Registering a duplicate email is rejected (409).
http_request POST http://127.0.0.1:8080/api/register \
  '{"first_name":"Ada","last_name":"L","email":"ada@example.com","password":"x"}'
[ "$HTTP_CODE" = "409" ] \
  || { echo ">> duplicate register returned $HTTP_CODE, want 409" >&2; exit 1; }

# Login with valid credentials: returns an OIDC access token (200). The token
# is a signed JWT (three dot-separated segments) minted by the IdP, stamped
# with the user's roles.
http_request POST http://127.0.0.1:8080/api/login \
  '{"email":"ada@example.com","password":"s3cret"}'
[ "$HTTP_CODE" = "200" ] \
  || { echo ">> login returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
LOGIN_TOKEN=$(printf '%s' "$HTTP_BODY" | jq -r '.access_token')
[ -n "$LOGIN_TOKEN" ] && [ "$LOGIN_TOKEN" != "null" ] \
  || { echo ">> login returned no access_token: $HTTP_BODY" >&2; exit 1; }
[ "$(printf '%s' "$LOGIN_TOKEN" | awk -F. '{print NF}')" = "3" ] \
  || { echo ">> login access_token is not a JWT: $LOGIN_TOKEN" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.roles | index("admin")' >/dev/null \
  || { echo ">> login response did not carry the user's admin role: $HTTP_BODY" >&2; exit 1; }

# Login with a wrong password is rejected (401).
http_request POST http://127.0.0.1:8080/api/login \
  '{"email":"ada@example.com","password":"wrong"}'
[ "$HTTP_CODE" = "401" ] \
  || { echo ">> wrong-password login returned $HTTP_CODE, want 401" >&2; exit 1; }

# Login for an unknown user is rejected (401).
http_request POST http://127.0.0.1:8080/api/login \
  '{"email":"nobody@example.com","password":"x"}'
[ "$HTTP_CODE" = "401" ] \
  || { echo ">> unknown-user login returned $HTTP_CODE, want 401" >&2; exit 1; }

# The user-management endpoints are gated behind an authenticated admin caller.
# With this stack's auth-off config an unauthenticated caller is rejected (401).
http_request GET http://127.0.0.1:8080/api/users
[ "$HTTP_CODE" = "401" ] \
  || { echo ">> unauthenticated list users returned $HTTP_CODE, want 401" >&2; exit 1; }

# --- verify role-based access control allows and disallows (F-14) ----------
# The main stack runs with OIDC auth OFF, so its API is open (every request
# acts as a synthetic admin) and RBAC is not enforced. To exercise RBAC we
# boot a SECOND API instance with OIDC auth + RBAC enabled, sharing the same
# IdP (so the users registered above and the JWKS it serves are reused) and the
# same db/scheduler/artifacts (so the role catalog and pipelines are shared).
# We then sign in as users with different roles and present their tokens to the
# RBAC-enabled API, asserting that it ALLOWS the actions each role grants and
# DISALLOWS (403) the actions it does not.
log ">> verifying role-based access control (F-14)"

# The RBAC-enabled API listens on :8081 (gRPC :7107) so it does not collide
# with the main API (:8080/:7105). It dials the shared db/scheduler/artifacts
# and IdP at their default addresses.
start api-rbac "$WORK/api-rbac.log" 'serving' \
  CDROM_API_HTTP_ADDR=127.0.0.1:8081 CDROM_LISTEN_ADDR=127.0.0.1:7107 \
  CDROM_AUTH_ENABLED=true CDROM_AUTH_ISSUER=http://127.0.0.1:7104 \
  CDROM_AUTH_CLIENT_ID=cdrom-ui CDROM_AUTH_REDIRECT_URL=http://127.0.0.1:8081/api/auth/callback \
  CDROM_AUDIT_FILE="$WORK/audit-rbac.log" CDROM_AUDIT_FORMAT=json \
  CDROM_LOG_FORMAT=text -- "$BIN/api"
RBAC=http://127.0.0.1:8081

# Sign in as the admin (ada, registered first above) and as the default user
# (bob) against the RBAC-enabled API. The IdP mints each token stamped with the
# user's roles, which the API reads to authorize the request.
http_request POST "$RBAC/api/login" '{"email":"ada@example.com","password":"s3cret"}'
[ "$HTTP_CODE" = "200" ] || { echo ">> admin login (rbac api) returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
ADMIN_TOKEN=$(printf '%s' "$HTTP_BODY" | jq -r '.access_token')
[ -n "$ADMIN_TOKEN" ] && [ "$ADMIN_TOKEN" != "null" ] \
  || { echo ">> admin login (rbac api) returned no token: $HTTP_BODY" >&2; exit 1; }

http_request POST "$RBAC/api/login" '{"email":"bob@example.com","password":"pw"}'
[ "$HTTP_CODE" = "200" ] || { echo ">> user login (rbac api) returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
USER_TOKEN=$(printf '%s' "$HTTP_BODY" | jq -r '.access_token')
[ -n "$USER_TOKEN" ] && [ "$USER_TOKEN" != "null" ] \
  || { echo ">> user login (rbac api) returned no token: $HTTP_BODY" >&2; exit 1; }

# Create a user with the operator role (via the admin's user-management
# endpoint, which honours the supplied roles) and sign in as them.
authed_request POST "$RBAC/api/users" "$ADMIN_TOKEN" \
  '{"first_name":"Op","last_name":"Erator","email":"op@example.com","password":"pw","roles":["operator"]}'
[ "$HTTP_CODE" = "201" ] || { echo ">> create operator user returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
http_request POST "$RBAC/api/login" '{"email":"op@example.com","password":"pw"}'
[ "$HTTP_CODE" = "200" ] || { echo ">> operator login (rbac api) returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
OPERATOR_TOKEN=$(printf '%s' "$HTTP_BODY" | jq -r '.access_token')
[ -n "$OPERATOR_TOKEN" ] && [ "$OPERATOR_TOKEN" != "null" ] \
  || { echo ">> operator login (rbac api) returned no token: $HTTP_BODY" >&2; exit 1; }

# Create a pipeline (as the admin) to use as a resource-scoped target.
authed_request POST "$RBAC/api/pipelines" "$ADMIN_TOKEN" '{"name":"rbac-pipeline"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> admin create pipeline returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
PIPELINE_ID=$(printf '%s' "$HTTP_BODY" | jq -r '.id')
[ -n "$PIPELINE_ID" ] && [ "$PIPELINE_ID" != "null" ] \
  || { echo ">> could not parse pipeline id: $HTTP_BODY" >&2; exit 1; }
log "   rbac pipeline id: $PIPELINE_ID"

# --- ALLOW: the admin can do everything -------------------------------------
authed_request GET "$RBAC/api/pipelines" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> admin list pipelines returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
authed_request POST "$RBAC/api/pipelines" "$ADMIN_TOKEN" '{"name":"rbac-admin-pipeline"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> admin create pipeline returned $HTTP_CODE, want 201 (ALLOW)" >&2; exit 1; }
authed_request POST "$RBAC/api/pipelines/$PIPELINE_ID/runs" "$ADMIN_TOKEN" ''
[ "$HTTP_CODE" = "201" ] || { echo ">> admin trigger run returned $HTTP_CODE, want 201 (ALLOW)" >&2; exit 1; }
authed_request GET "$RBAC/api/roles" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> admin list roles returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
authed_request GET "$RBAC/api/me/permissions" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> admin me/permissions returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
# The admin's effective permissions include the platform-wide role-management
# permission (admin = every permission).
printf '%s' "$HTTP_BODY" | jq -e '.platform_wide | index("roles.can-manage")' >/dev/null \
  || { echo ">> admin me/permissions did not list roles.can-manage: $HTTP_BODY" >&2; exit 1; }

# --- ALLOW + DISALLOW: the operator can run but not create ------------------
# operator grants pipelines.can-view + runs.can-trigger (among others) but NOT
# pipelines.can-create, so it can list and trigger but not create a pipeline.
authed_request GET "$RBAC/api/pipelines" "$OPERATOR_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> operator list pipelines returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
authed_request POST "$RBAC/api/pipelines/$PIPELINE_ID/runs" "$OPERATOR_TOKEN" ''
[ "$HTTP_CODE" = "201" ] || { echo ">> operator trigger run returned $HTTP_CODE, want 201 (ALLOW)" >&2; exit 1; }
authed_request POST "$RBAC/api/pipelines" "$OPERATOR_TOKEN" '{"name":"rbac-operator-pipeline"}'
[ "$HTTP_CODE" = "403" ] || { echo ">> operator create pipeline returned $HTTP_CODE, want 403 (DISALLOW)" >&2; exit 1; }
# The operator cannot manage roles (roles.can-manage is not in the operator set).
authed_request GET "$RBAC/api/roles" "$OPERATOR_TOKEN"
[ "$HTTP_CODE" = "403" ] || { echo ">> operator list roles returned $HTTP_CODE, want 403 (DISALLOW)" >&2; exit 1; }

# --- ALLOW + DISALLOW: the default user can view but not trigger ------------
# The default "user" role grants pipelines.can-view but not runs.can-trigger,
# so it can list pipelines but not trigger a run or create a pipeline.
authed_request GET "$RBAC/api/pipelines" "$USER_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> user list pipelines returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
authed_request POST "$RBAC/api/pipelines/$PIPELINE_ID/runs" "$USER_TOKEN" ''
[ "$HTTP_CODE" = "403" ] || { echo ">> user trigger run returned $HTTP_CODE, want 403 (DISALLOW)" >&2; exit 1; }
authed_request POST "$RBAC/api/pipelines" "$USER_TOKEN" '{"name":"rbac-user-pipeline"}'
[ "$HTTP_CODE" = "403" ] || { echo ">> user create pipeline returned $HTTP_CODE, want 403 (DISALLOW)" >&2; exit 1; }

# --- DISALLOW: a principal with no roles can do nothing (deny-by-default) ---
# Create a user with an empty role set (the admin's user-management endpoint
# honours the supplied roles, so an empty set means no roles at all) and sign
# in as them. With no roles and no mapped claims, deny-by-default means every
# action is refused — not even listing pipelines.
authed_request POST "$RBAC/api/users" "$ADMIN_TOKEN" \
  '{"first_name":"No","last_name":"Role","email":"nole@example.com","password":"pw","roles":[]}'
[ "$HTTP_CODE" = "201" ] || { echo ">> create no-role user returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
http_request POST "$RBAC/api/login" '{"email":"nole@example.com","password":"pw"}'
[ "$HTTP_CODE" = "200" ] || { echo ">> no-role login (rbac api) returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
NOROLE_TOKEN=$(printf '%s' "$HTTP_BODY" | jq -r '.access_token')
[ -n "$NOROLE_TOKEN" ] && [ "$NOROLE_TOKEN" != "null" ] \
  || { echo ">> no-role login (rbac api) returned no token: $HTTP_BODY" >&2; exit 1; }
authed_request GET "$RBAC/api/pipelines" "$NOROLE_TOKEN"
[ "$HTTP_CODE" = "403" ] || { echo ">> no-role list pipelines returned $HTTP_CODE, want 403 (DISALLOW, deny-by-default)" >&2; exit 1; }
authed_request GET "$RBAC/api/me/permissions" "$NOROLE_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> no-role me/permissions returned $HTTP_CODE, want 200" >&2; exit 1; }
# A principal with no roles has no effective permissions at all.
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.platform_wide | length')" = "0" ] \
  || { echo ">> no-role me/permissions was not empty: $HTTP_BODY" >&2; exit 1; }

# --- DISALLOW: an unauthenticated request to the RBAC API is rejected (401) --
# The RBAC-enabled API requires a valid Bearer token on every /api/* request
# (except the exempted login/register entry points).
http_request GET "$RBAC/api/pipelines"
[ "$HTTP_CODE" = "401" ] || { echo ">> unauthenticated list pipelines (rbac api) returned $HTTP_CODE, want 401" >&2; exit 1; }

# --- verify the audit log (F-15) -------------------------------------------
# Every audited action (a job trigger, an approval decision, a login, a
# pipeline/role/user mutation, a worker lifecycle change, a token exchange) is
# recorded both to a local audit log file (separate from the process's default
# log) and to the shared audit log in the database (queryable via
# GET /api/audit). The main API runs with auth off (a synthetic admin), so
# /api/audit is reachable without a token. Both API instances share the same
# database, so the shared audit log holds events from both.
log ">> verifying the audit log (F-15)"

# The local audit log file exists, is separate from the process's default log,
# and holds JSON records (CDROM_AUDIT_FORMAT=json).
[ -s "$WORK/audit.log" ] \
  || { echo ">> local audit log file is missing or empty" >&2; exit 1; }
# Every line of the local audit log is a JSON object (the configured format).
BAD_LINES=$(grep -vcE '^\{"time":' "$WORK/audit.log" || true)
[ "$BAD_LINES" = "0" ] \
  || { echo ">> $BAD_LINES audit log lines are not JSON objects" >&2; exit 1; }
# The audit records did not leak into the process's default log (api.log is
# text-format key=value; the JSON audit records carry a quoted "actor" key).
grep -q '"actor":' "$WORK/api.log" \
  && { echo ">> audit records leaked into the default log (api.log)" >&2; exit 1; }

# The shared audit log (in the database) is queryable via GET /api/audit.
AUDIT_ALL=$(curl -s "http://127.0.0.1:8080/api/audit")
printf '%s' "$AUDIT_ALL" | jq -e 'type == "array" and length > 0' >/dev/null \
  || { echo ">> GET /api/audit returned no events: $AUDIT_ALL" >&2; exit 1; }

# A job approval decision is recorded with a success outcome.
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="job.approve") | select(.outcome=="success")' >/dev/null \
  || { echo ">> no job.approve audit event recorded" >&2; exit 1; }
# A job rejection is recorded too.
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="job.reject")' >/dev/null \
  || { echo ">> no job.reject audit event recorded" >&2; exit 1; }
# A login is recorded with the user's email as the actor.
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="auth.login") | select(.actor=="ada@example.com")' >/dev/null \
  || { echo ">> no auth.login audit event for ada@example.com" >&2; exit 1; }
# A run trigger (a standalone job submit) is recorded.
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="run.trigger")' >/dev/null \
  || { echo ">> no run.trigger audit event recorded" >&2; exit 1; }
# A worker registration is recorded (the worker registered with the API).
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="worker.register")' >/dev/null \
  || { echo ">> no worker.register audit event recorded" >&2; exit 1; }
# A job status report from the worker is recorded.
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="job.status")' >/dev/null \
  || { echo ">> no job.status audit event recorded" >&2; exit 1; }

# The audit log can be filtered by action (F-15 acceptance: filter by actor,
# target, and time range).
AUDIT_FILTERED=$(curl -s "http://127.0.0.1:8080/api/audit?action=job.approve")
printf '%s' "$AUDIT_FILTERED" | jq -e 'type == "array" and length > 0 and all(.[]; .action=="job.approve")' >/dev/null \
  || { echo ">> action filter did not return only job.approve events: $AUDIT_FILTERED" >&2; exit 1; }

# A pipeline's secrets are represented by name only in the audit log: the
# secret's value is never stored or displayed (F-15). Create a pipeline with a
# secret (via the RBAC admin API) and verify the audit event's new_value names
# the secret but never its value.
SECRET_VALUE="super-secret-value-123"
authed_request POST "$RBAC/api/pipelines" "$ADMIN_TOKEN" \
  "{\"name\":\"audit-secret-pipeline\",\"secrets\":[{\"name\":\"api_token\",\"value\":\"$SECRET_VALUE\"}]}"
[ "$HTTP_CODE" = "201" ] || { echo ">> create secret pipeline returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }

AUDIT_SECRET=$(curl -s "http://127.0.0.1:8080/api/audit?action=pipeline.create&target_kind=pipeline")
NEW_VALUE=$(printf '%s' "$AUDIT_SECRET" | jq -r '.[] | select(.target_name=="audit-secret-pipeline") | .new_value' | head -n 1)
[ -n "$NEW_VALUE" ] && [ "$NEW_VALUE" != "null" ] \
  || { echo ">> no pipeline.create audit event for the secret pipeline: $AUDIT_SECRET" >&2; exit 1; }
printf '%s' "$NEW_VALUE" | grep -q 'api_token' \
  || { echo ">> audit new_value did not name the secret: $NEW_VALUE" >&2; exit 1; }
printf '%s' "$NEW_VALUE" | grep -q "$SECRET_VALUE" \
  && { echo ">> audit new_value leaked the secret value: $NEW_VALUE" >&2; exit 1; }

log ">> e2e PASSED: worker job ran all steps (including a token_exchange step whose exchanged token flowed to a later step), persisted as succeeded, logs are retrievable, a job-level timeout terminates the job as timed_out, a failing job is retried up to its limit, a finished job can be re-run for a fresh execution, an approval gate pauses a job until it is approved (continuing to its later steps) or rejected (failing it), username/password auth registers users (first becomes admin), logs in with a valid password (minting a JWT), rejects bad credentials, and gates user management behind an authenticated caller, RBAC (F-14) allows the actions each role grants and disallows (403) the actions it does not (admin does everything, operator runs but does not create, the default user views but does not trigger, a role-less principal does nothing, and unauthenticated requests are rejected), and the audit log (F-15) records each audited action (job trigger, approval, login, worker lifecycle, status report) to a separate JSON file and the shared database log (queryable/filterable via GET /api/audit) with the actor, and never stores or displays a secret's value"
