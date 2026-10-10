# shellcheck shell=bash
#
# lib.sh — shared helpers for the cdrom e2e test scripts (scripts/e2e/).
#
# Each test script is single-purpose and executable on its own: it sources
# this library, boots the stack it needs, runs its assertions, and tears the
# stack down on exit. The main script (scripts/e2e.sh) boots the stack once
# and runs every test script against it, in order.
#
# The library is NOT executable on its own; it is sourced.

set -euo pipefail

# --- paths -----------------------------------------------------------------
# E2E_ROOT is the scripts/e2e directory (this file's directory); E2E_REPO is
# the repository root. Scripts may be run from anywhere.
E2E_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_REPO="$(cd "${E2E_ROOT}/../.." && pwd)"
BIN="${E2E_REPO}/bin"

# --- stack state (set by e2e_boot_stack) ------------------------------------
# WORK is the temp directory holding the stack's logs, SQLite database, and
# artifacts; WORKDIR is the scratch directory job steps may use as a workdir.
# In shared-stack mode (E2E_STACK_READY set by the main script) these are
# inherited from the environment.
WORK="${WORK:-}"
WORKDIR="${WORKDIR:-}"
# PIDS holds the PIDs of every process this script started; e2e_cleanup kills
# them. E2E_OWN_STACK is 1 when this script booted the stack itself (and so
# must remove WORK on exit) and 0 in shared-stack mode (the main script owns
# WORK).
PIDS=()
E2E_OWN_STACK=0

# --- logging -----------------------------------------------------------------
log() { printf '%s\n' "$*"; }

# --- stack lifecycle ---------------------------------------------------------
# e2e_cleanup kills every process this script started and, when this script
# booted the stack itself, removes the temp directory. It is installed as an
# EXIT trap by e2e_boot_stack, so the stack is always torn down, even on
# failure.
e2e_cleanup() {
  local code=$?
  if [ "${#PIDS[@]}" -gt 0 ]; then
    local pid
    for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    sleep 1
    for pid in "${PIDS[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
  fi
  if [ "$E2E_OWN_STACK" = "1" ] && [ -n "$WORK" ]; then
    rm -rf "$WORK"
  fi
  if [ "$code" -ne 0 ]; then
    echo ">> e2e FAILED (exit $code)" >&2
    exit "$code"
  fi
}

# e2e_boot_stack [with-worker]
# Boots the full local stack (db, scheduler, artifacts, idp, api) against a
# throwaway SQLite database in a fresh temp directory, and installs the
# cleanup trap. With "with-worker" it also starts a worker in the "default"
# group and waits for its WatchJobs stream to be open on the API.
#
# The API runs with job-token auth enabled (the IdP mints the job tokens the
# API hands to execution targets) and with the audit log (F-15) written to a
# file separate from the process's default log, in JSON.
#
# Shared-stack mode: when E2E_STACK_READY is set (by the main script,
# scripts/e2e.sh), the stack is already running and WORK/WORKDIR are
# inherited from the environment; this function only installs the cleanup
# trap (which then tears down only the processes this script itself starts,
# e.g. the RBAC API).
e2e_boot_stack() {
  local with_worker="${1:-}"
  if [ -n "${E2E_STACK_READY:-}" ]; then
    [ -n "$WORK" ] || { echo ">> E2E_STACK_READY is set but WORK is not" >&2; exit 1; }
    E2E_OWN_STACK=0
    trap e2e_cleanup EXIT
    return 0
  fi
  E2E_OWN_STACK=1
  WORK="$(mktemp -d /tmp/cdrom-e2e.XXXXXX)"
  echo "WORKDIR: ${WORK}"
  WORKDIR="$WORK/workdir"
  mkdir -p "$WORK/artifacts" "$WORKDIR"
  trap e2e_cleanup EXIT

  e2e_start db        "$WORK/db.log"        'serving'    CDROM_DB_BACKEND=sqlite CDROM_DB_SQLITE_PATH="$WORK/cdrom.db" CDROM_LOG_FORMAT=text -- "$BIN/db"
  e2e_start scheduler "$WORK/scheduler.log" 'serving'    CDROM_LOG_FORMAT=text -- "$BIN/scheduler"
  e2e_start artifacts "$WORK/artifacts.log" 'serving'    CDROM_ARTIFACTS_ROOT="$WORK/artifacts" CDROM_LOG_FORMAT=text -- "$BIN/artifacts"
  # The IdP mints the job tokens the API hands to execution targets (and the
  # exchanged tokens a token_exchange step requests). It is started before the
  # API so the API's job-token auth (OIDC discovery against the IdP) can
  # initialize.
  e2e_start idp       "$WORK/idp.log"       'idp starting' CDROM_LOG_FORMAT=text -- "$BIN/idp"
  # The IdP logs "starting" before its HTTP server is listening, so poll its
  # discovery endpoint until it responds; the API's job-token auth performs
  # OIDC discovery against the IdP at startup and would fail if the IdP is not
  # up.
  log ">> waiting for the IdP to serve"
  local i
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
  e2e_start api       "$WORK/api.log"       'serving'    CDROM_GRPC_AUTH_ENABLED=true CDROM_GRPC_AUTH_IDP_ADDR=127.0.0.1:7104 CDROM_GRPC_AUTH_AUDIENCES=cdrom-api CDROM_AUDIT_FILE="$WORK/audit.log" CDROM_AUDIT_FORMAT=json CDROM_LOG_FORMAT=text -- "$BIN/api"
  if [ "$with_worker" = "with-worker" ]; then
    e2e_start worker  "$WORK/worker.log"    'registered' CDROM_LOG_FORMAT=text -- "$BIN/worker"
    # Gate on the API having the worker's WatchJobs stream open before
    # dispatching.
    e2e_wait_for "$WORK/api.log" 'worker watching' 'worker watch stream on the api'
  fi
}

# e2e_start <name> <logfile> <ready-pattern> <env...> -- <cmd...>
# Launches a service in the background, records its PID, and waits until its
# log matches <ready-pattern> (or the process dies / times out).
e2e_start() {
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

# e2e_wait_for <logfile> <pattern> <what>
e2e_wait_for() {
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

# e2e_wait_for_job <jobid> <pattern> <what>
# Polls the job's combined log (GET /api/jobs/{id}/logs/job.log) until it
# matches <pattern>.
e2e_wait_for_job() {
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

# e2e_wait_for_status <jobid> <status> <what>
# Polls GET /api/jobs/{id} until the job's status equals <status> (the numeric
# JobStatus value, e.g. 8 for awaiting_approval, 3 for succeeded, 4 for
# failed). Returns the last observed job JSON on stdout.
e2e_wait_for_status() {
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

# --- HTTP helpers ------------------------------------------------------------
# e2e_http_request <method> <url> [json-body]
# Performs an HTTP request and sets HTTP_CODE (the response status code) and
# HTTP_BODY (the response body). When a JSON body is given it is sent with a
# Content-Type: application/json header. Used by the F-24/F-14 assertions,
# which need both the status code and the body from a single call.
e2e_http_request() {
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

# e2e_authed_request <method> <url> <bearer-token> [json-body]
# Like e2e_http_request, but presents the given OIDC token as a Bearer token
# (used by the F-14 assertions against the RBAC-enabled API, where every
# request must carry a valid token). Sets HTTP_CODE and HTTP_BODY.
e2e_authed_request() {
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

# --- job submission ----------------------------------------------------------
# e2e_submit_job <json-body>
# POSTs a job to the API and prints the job's id on stdout. The full response
# is echoed to stderr so it stays out of the caller's command substitution
# (callers capture the id with $(e2e_submit_job ...)).
e2e_submit_job() {
  local body="$1" response
  response=$(curl -s -X POST http://127.0.0.1:8080/api/jobs \
    -H 'Content-Type: application/json' \
    -d "$body")
  echo "   $response" >&2
  local id
  id=$(printf '%s' "$response" | jq -r '.id')
  [ -n "$id" ] || { echo ">> could not parse job id from response" >&2; exit 1; }
  printf '%s' "$id"
}

# --- shared users (F-24/F-14) ------------------------------------------------
# The user accounts the F-24 and F-14 tests rely on. They are registered by
# the F-24 test (the first registered user becomes an admin, the second gets
# the default user role); the F-14 test signs in as them against the
# RBAC-enabled API. When the F-14 test runs standalone (without the F-24
# test), it registers them itself if they are not present yet.
E2E_USER_ADA_EMAIL="ada@example.com"
E2E_USER_ADA_PASSWORD="s3cret"
E2E_USER_BOB_EMAIL="bob@example.com"
E2E_USER_BOB_PASSWORD="pw"

# e2e_ensure_users <api-base>
# Registers the shared users against <api-base> if they are not present yet
# (a duplicate registration is a 409, which is fine).
e2e_ensure_users() {
  local base="$1"
  e2e_http_request POST "$base/api/register" \
    "{\"first_name\":\"Ada\",\"last_name\":\"Lovelace\",\"email\":\"${E2E_USER_ADA_EMAIL}\",\"password\":\"${E2E_USER_ADA_PASSWORD}\"}"
  case "$HTTP_CODE" in
    201|409) ;;
    *) echo ">> register ${E2E_USER_ADA_EMAIL} returned $HTTP_CODE, want 201 or 409 ($HTTP_BODY)" >&2; exit 1 ;;
  esac
  e2e_http_request POST "$base/api/register" \
    "{\"first_name\":\"Bob\",\"last_name\":\"Jones\",\"email\":\"${E2E_USER_BOB_EMAIL}\",\"password\":\"${E2E_USER_BOB_PASSWORD}\"}"
  case "$HTTP_CODE" in
    201|409) ;;
    *) echo ">> register ${E2E_USER_BOB_EMAIL} returned $HTTP_CODE, want 201 or 409 ($HTTP_BODY)" >&2; exit 1 ;;
  esac
}

# --- RBAC-enabled API (F-14) -------------------------------------------------
# e2e_start_rbac_api
# Boots a SECOND API instance with OIDC auth + RBAC enabled, sharing the
# stack's IdP (users + JWKS) and db/scheduler/artifacts. It listens on :8081
# (gRPC :7107) so it does not collide with the main API (:8080/:7105).
e2e_start_rbac_api() {
  e2e_start api-rbac "$WORK/api-rbac.log" 'serving' \
    CDROM_API_HTTP_ADDR=127.0.0.1:8081 CDROM_LISTEN_ADDR=127.0.0.1:7107 \
    CDROM_AUTH_ENABLED=true CDROM_AUTH_ISSUER=http://127.0.0.1:7104 \
    CDROM_AUTH_CLIENT_ID=cdrom-ui CDROM_AUTH_REDIRECT_URL=http://127.0.0.1:8081/api/auth/callback \
    CDROM_AUDIT_FILE="$WORK/audit-rbac.log" CDROM_AUDIT_FORMAT=json \
    CDROM_LOG_FORMAT=text -- "$BIN/api"
}

# e2e_login <api-base> <email> <password>
# Signs in against <api-base> and prints the access token.
e2e_login() {
  local base="$1" email="$2" password="$3"
  e2e_http_request POST "$base/api/login" "{\"email\":\"$email\",\"password\":\"$password\"}"
  [ "$HTTP_CODE" = "200" ] || { echo ">> login ($email, ${base}) returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
  local token
  token=$(printf '%s' "$HTTP_BODY" | jq -r '.access_token')
  [ -n "$token" ] && [ "$token" != "null" ] \
    || { echo ">> login ($email, ${base}) returned no token: $HTTP_BODY" >&2; exit 1; }
  printf '%s' "$token"
}
