#!/usr/bin/env bash
#
# e2e.sh — end-to-end smoke test for the cdrom pipeline (main script).
#
# Boots a full local stack (db, scheduler, artifacts, idp, api, worker)
# against a throwaway SQLite database once, then runs the single-purpose
# test scripts in scripts/e2e/ against it, in order:
#
#   01-shell-job.sh   core job execution + log streaming (F-02) + token exchange
#   02-timeout.sh     job-level timeout (F-03)
#   03-retry-rerun.sh retry policy + re-run (F-04)
#   04-approval.sh    approval gates (F-13)
#   05-userpass.sh    username/password authentication (F-24)
#   06-rbac.sh        roles & permissions (F-14)
#   07-audit.sh       audit log (F-15)
#   08-apikeys.sh     API keys (F-25)
#
# Each test script is also executable on its own (it boots and tears down its
# own stack); when run from this main script they share the one stack booted
# here. The shared helpers live in scripts/e2e/lib.sh.
#
# Usage: scripts/e2e.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq.
#
# The script always tears the stack down and removes its temp directory on
# exit, even on failure.

set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/e2e/lib.sh

# --- boot the shared stack ---------------------------------------------------
WORK="$(mktemp -d /tmp/cdrom-e2e.XXXXXX)"
echo "WORKDIR: ${WORK}"
WORKDIR="$WORK/workdir"
mkdir -p "$WORK/artifacts" "$WORKDIR"

cleanup() {
  local code=$?
  if [ "${#PIDS[@]}" -gt 0 ]; then
    local pid
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

log ">> booting shared stack in $WORK"
e2e_start db        "$WORK/db.log"        'serving'    CDROM_DB_BACKEND=sqlite CDROM_DB_SQLITE_PATH="$WORK/cdrom.db" CDROM_LOG_FORMAT=text -- "$BIN/db"
e2e_start scheduler "$WORK/scheduler.log" 'serving'    CDROM_LOG_FORMAT=text -- "$BIN/scheduler"
e2e_start artifacts "$WORK/artifacts.log" 'serving'    CDROM_ARTIFACTS_ROOT="$WORK/artifacts" CDROM_LOG_FORMAT=text -- "$BIN/artifacts"
# The IdP mints the job tokens the API hands to the worker (and the exchanged
# tokens a token_exchange step requests). It is started before the API so the
# API's job-token auth (OIDC discovery against the IdP) can initialize.
e2e_start idp       "$WORK/idp.log"       'idp starting' CDROM_LOG_FORMAT=text -- "$BIN/idp"
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
e2e_start api       "$WORK/api.log"       'serving'    CDROM_GRPC_AUTH_ENABLED=true CDROM_GRPC_AUTH_IDP_ADDR=127.0.0.1:7104 CDROM_GRPC_AUTH_AUDIENCES=cdrom-api CDROM_AUDIT_FILE="$WORK/audit.log" CDROM_AUDIT_FORMAT=json CDROM_LOG_FORMAT=text -- "$BIN/api"
e2e_start worker    "$WORK/worker.log"    'registered' CDROM_LOG_FORMAT=text -- "$BIN/worker"

# Gate on the API having the worker's WatchJobs stream open before dispatching.
e2e_wait_for "$WORK/api.log" 'worker watching' 'worker watch stream on the api'

# --- run the test scripts against the shared stack ---------------------------
# E2E_STACK_READY tells each test script (via lib.sh) that the stack is
# already up and that WORK/WORKDIR are provided by this script; the scripts
# then skip booting their own stack and only tear down what they themselves
# started.
export E2E_STACK_READY=1 WORK WORKDIR

run_test() {
  log ">> running $1"
  "$E2E_ROOT/$1"
}

run_test 01-shell-job.sh
run_test 02-timeout.sh
run_test 03-retry-rerun.sh
run_test 04-approval.sh
run_test 05-userpass.sh
run_test 06-rbac.sh
run_test 07-audit.sh
run_test 08-apikeys.sh

log ">> e2e PASSED: worker job ran all steps (including a token_exchange step whose exchanged token flowed to a later step), persisted as succeeded, logs are retrievable, a job-level timeout terminates the job as timed_out, a failing job is retried up to its limit, a finished job can be re-run for a fresh execution, an approval gate pauses a job until it is approved (continuing to its later steps) or rejected (failing it), username/password auth registers users (first becomes admin), logs in with a valid password (minting a JWT), rejects bad credentials, and gates user management behind an authenticated caller, RBAC (F-14) allows the actions each role grants and disallows (403) the actions it does not (admin does everything, operator runs but does not create, the default user views but does not trigger, a role-less principal does nothing, and unauthenticated requests are rejected), the audit log (F-15) records each audited action (job trigger, approval, login, worker lifecycle, status report) to a separate JSON file and the shared database log (queryable/filterable via GET /api/audit) with the actor, and never stores or displays a secret's value, and API keys (F-25) let an admin create a key for another user (the plaintext returned once), authenticate as that user via `Bearer <username>:<apikey>` (even with OIDC on), list every key (admin) or only their own (a user), limit a key's resource-scoped permissions to its pipeline scope, lock a user out after repeated failures until an admin resets it, and rotate (new plaintext, old stops working) or delete (revokes) a key"
