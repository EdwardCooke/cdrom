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
#   09-concurrency.sh concurrency keys (F-16)
#   10-notifications.sh notifications (F-18)
#   11-labeling.sh      labels on pipelines/jobs/groups combined onto a run and searchable by label (F-29)
#   12-metrics.sh       every component's dedicated metrics port serves /metrics + /healthz with data (F-20)
#   13-source-fetcher.sh the source-fetcher (F-21.2) fetches a file by reference (file/http/git),
#                        rooted file reads + traversal rejection, credential headers, git refetch
#                        absorbs a rewritten branch, and its dedicated metrics port
#   14-source-credentials.sh source credentials (F-21.3): the fetcher's http handler sends the
#                        credential's header (name + value) on the GET, and the API redacts a
#                        sensitive http header on read while returning a non-sensitive one in
#                        plaintext
#   (the shared stack also runs a source-fetcher on the default :7107 — the
#    API dials it at startup — so 15 can exercise the API ↔ fetcher link)
#   15-externally-sourced-pipelines.sh externally-sourced pipelines (F-21.5): attaching a source
#                        bootstraps the import (the file's jobs + periodicRefresh policy adopt
#                        onto the pipeline, the raw YAML + hash are stored), a manual refresh
#                        applies a changed file (version bump + a run executes the new
#                        definition), an unchanged refresh is a no-op, the run row carries the
#                        kickoff provenance (surviving a repoint), the source view explains the
#                        effective cadence, a per-pipeline gate denial falls back to the
#                        2-minute system default, and a broken source records last_error
#                        without wiping the last-good state
#   16-artifact-packages.sh artifact packages (F-21.6): the publish endpoint seeds the registry
#                        (immutability 409), download_artifact/extract consume packages in all
#                        three formats (magic-byte detection) with the stable log lines + step
#                        outputs, the negatives (unknown/corrupt/escaping/symlink) fail with
#                        their diagnostics, spec.artifacts resolutions are per-run snapshots
#                        (a newer registry entry does not disturb an old run), the stage area
#                        is cleaned up, a job's publish_artifact output is promoted into the
#                        registry (byte-identity + audit), and publish_artifact's glob/negation/
#                        sanitization/collision semantics round-trip through extract
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
# The F-18 notification sinks (a fake SMTP server and a fake webhook HTTP
# server) are started before the API so the API's email/webhook sends have a
# target. The API is configured to send email to the SMTP sink (plaintext,
# STARTTLS disabled); a webhook step's url points at the webhook sink.
e2e_start_sinks
# The STOCK source-fetcher (F-21.2) on :7109 (metrics :9112). It must be up
# BEFORE the API, which dials it at startup (CDROM_PIPELINE_SOURCE_ADDR, set
# below) — otherwise the API's source-sync routines have no fetcher to dial
# (15-externally-sourced-pipelines.sh exercises the API ↔ fetcher link). Ports
# are chosen CLEAR of the suite's incumbents: :7107/:9106 (13's fetcher),
# :7108/:9107 (13's no-root fetcher), :7110/:9110 (14's fetcher), :7111/:9111
# (15's standalone fetcher), :8081 (06-rbac's auxiliary API). Its file root is
# $WORK/shared-files (15 writes its definition files there).
mkdir -p "$WORK/shared-files" "$WORK/shared-ps-cache"
e2e_start pipelinesource "$WORK/pipelinesource.log" 'service starting' \
  CDROM_LISTEN_ADDR=127.0.0.1:7109 \
  CDROM_METRICS_ADDR=127.0.0.1:9112 \
  CDROM_PIPELINE_SOURCE_FILE_ROOT="$WORK/shared-files" \
  CDROM_PIPELINE_SOURCE_REPO_CACHE_DIR="$WORK/shared-ps-cache" \
  CDROM_LOG_FORMAT=text -- "$BIN/pipelinesource"
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
# via GET /api/audit), which both API instances share. The email config points
# the API's SMTP sender at the fake SMTP sink (F-18).
e2e_start api       "$WORK/api.log"       'serving'    CDROM_GRPC_AUTH_ENABLED=true CDROM_GRPC_AUTH_IDP_ADDR=127.0.0.1:7104 CDROM_GRPC_AUTH_AUDIENCES=cdrom-api CDROM_EMAIL_HOST=127.0.0.1 CDROM_EMAIL_PORT="$E2E_SMTP_PORT" CDROM_EMAIL_STARTTLS=false CDROM_EMAIL_FROM=e2e@cdrom.local CDROM_AUDIT_FILE="$WORK/audit.log" CDROM_AUDIT_FORMAT=json CDROM_PIPELINE_SOURCE_ADDR=127.0.0.1:7109 CDROM_LOG_FORMAT=text -- "$BIN/api"
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
run_test 09-concurrency.sh
run_test 10-notifications.sh
run_test 11-labeling.sh
run_test 12-metrics.sh
run_test 13-source-fetcher.sh
run_test 14-source-credentials.sh
run_test 15-externally-sourced-pipelines.sh
run_test 16-artifact-packages.sh
run_test 17-restoration.sh
run_test 18-approval-groups.sh

log ">> e2e PASSED"
