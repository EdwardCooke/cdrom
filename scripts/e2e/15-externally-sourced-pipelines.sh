#!/usr/bin/env bash
#
# 15-externally-sourced-pipelines.sh — verifies F-21.5 (externally-sourced
# pipelines) end to end.
#
# The scenario: a pipeline whose definition LIVES IN A FILE (served over HTTP
# by a local sink) is attached as an externally-sourced pipeline, and the
# platform keeps it current and traceable:
#
#   1. ATTACH BOOTSTRAPS — attaching a source kicks off an initial sync: the
#      file is fetched, compiled, and IMPORTED (the pipeline adopts the
#      file's jobs + its spec.periodicRefresh policy), the raw YAML + hash
#      are stored on the pipeline, and the source state records the version.
#
#   2. MANUAL REFRESH APPLIES CHANGES — rewriting the file (marker v2) and
#      forcing a refresh re-imports the pipeline: the version bumps, the
#      stored source_file/hash follow the new content, and a run of the
#      pipeline executes the NEW definition (the v2 marker appears in the
#      job's log).
#
#   3. AN UNCHANGED REFRESH IS A NO-OP — refreshing again (same content)
#      bumps NOTHING: the version and the stored hash are untouched.
#
#   4. KICKOFF PROVENANCE — a run of the sourced pipeline carries the
#      source's location and revision (source_kind / source_reference /
#      source_version / source_file_hash) on the run row, EVEN AFTER the
#      source is repointed (the provenance sticks to the run).
#
#   5. THE CADENCE VIEW — GET /api/pipelines/{id}/source explains WHY the
#      source polls at the rate it does: the file's declared periodicRefresh,
#      the admin gate's verdict, and the effective cadence (the cron drives
#      when both are declared).
#
#   6. THE ADMIN GATE — a per-pipeline DENIAL (overridesAllowed=false) makes
#      the effective cadence fall back to the SYSTEM DEFAULT (the safe
#      restore — it never halts polling), and the view says so.
#
#   7. A BROKEN SOURCE SURFACES — re-pointing the source at a dead endpoint,
#      a refresh fails with 422 and records last_error WHILE PRESERVING the
#      last-good version/hash (the kickoff-provenance fallback leans on the
#      survivors).
#
# The refreshes are driven through the API's sync routine (manual refresh),
# which is the SAME routine the periodic loop and the refresh-on-run hook
# use (F-21.5.2). The periodic loop itself runs on the stack's API (leader
# lease); its cadence resolution is covered by the unit tests — this script
# proves the end-to-end DATA PLUMBING.
#
# Standalone: boots its own stack (WITH A WORKER) + its own fetcher, which the
# API dials (CDROM_PIPELINE_SOURCE_ADDR pointed at it). Shared (run from
# e2e.sh): the stack is already up INCLUDING a stock fetcher (the API dials it
# at startup), so this script reuses it and places its definition files in the
# stock fetcher's file root ($WORK/shared-files).
#
# Registered in scripts/e2e.sh (after 14-source-credentials.sh).
#
# Usage: scripts/e2e/15-externally-sourced-pipelines.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, jq,
# and python3.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

FS_METRICS=9111
FS_HTTP=10093
FS_DEAD=10094

# Standalone: point the API at OUR fetcher (booted on :7111) — the API reads
# CDROM_PIPELINE_SOURCE_ADDR AT STARTUP, so this must precede e2e_boot_stack.
# Shared: the stack's API already dials the default :7107 (the stack's own
# fetcher), so nothing to export.
if [ -z "${E2E_STACK_READY:-}" ]; then
  E2E_FS_ADDR=127.0.0.1:7111
  export CDROM_PIPELINE_SOURCE_ADDR="$E2E_FS_ADDR"
fi

# WITH A WORKER (standalone): the run-half needs a target to execute the job
# on. (The shared stack already runs one.)
e2e_boot_stack with-worker

if [ -z "${E2E_STACK_READY:-}" ]; then
  # Standalone: our own fetcher's file root.
  SRCROOT="$WORK/fs15-files"
  CACHE="$WORK/fs15-cache"
  mkdir -p "$SRCROOT" "$CACHE"
else
  # Shared: the stack's stock fetcher serves $WORK/shared-files.
  SRCROOT="$WORK/shared-files"
fi

# The definition file the sink serves. v1: a single job echoing MARKER_V1,
# declaring a periodicRefresh policy (interval + cron — the cron drives).
FILE_YAML_v1='apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: fs-demo
  description: externally sourced demo
spec:
  # Default worker group (F-17): the job targets the resident "default" group
  # (an EMPTY target group would await an ephemeral agent, which the e2e
  # stack has none of).
  workerGroup: default
  periodicRefresh:
    interval: 5m
    schedule: "0 3 * * *"
  jobs:
    hello:
      steps:
        greet:
          shell: echo MARKER_V1
'
printf '%s\n' "$FILE_YAML_v1" > "$SRCROOT/demo.yaml"

# Boot the fetcher (file-kind source rooted at SRCROOT) — standalone only;
# the shared stack already runs one (on the default :7107, file root
# $WORK/shared-files).
if [ -z "${E2E_STACK_READY:-}" ]; then
  e2e_start pipelinesource15 "$WORK/pipelinesource15.log" 'service starting' \
    CDROM_LISTEN_ADDR="$E2E_FS_ADDR" \
    CDROM_PIPELINE_SOURCE_FILE_ROOT="$SRCROOT" \
    CDROM_PIPELINE_SOURCE_REPO_CACHE_DIR="$CACHE" \
    CDROM_METRICS_ADDR="127.0.0.1:${FS_METRICS}" \
    CDROM_LOG_FORMAT=text -- "$BIN/pipelinesource"
fi

# A local HTTP sink serving SRCROOT (a stable ETag — the CONTENT is the
# change detector, not the etag).
python3 - "$FS_HTTP" "$SRCROOT" <<'PY' &
import http.server, sys
port, root = int(sys.argv[1]), sys.argv[2]
class H(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=root, **k)
    def end_headers(self):
        self.send_header("ETag", '"stable"')
        super().end_headers()
    def log_message(self, *a):
        pass
http.server.ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
PY
PIDS+=($!)
# FS_DEAD is a CLOSED port (nothing listens there) — the broken-source half.
for i in $(seq 1 50); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$FS_HTTP") 2>/dev/null; then
    exec 3>&- 2>/dev/null || true
    break
  fi
  sleep 0.1
done

REF="http:http://127.0.0.1:${FS_HTTP}/demo.yaml"
DEAD_REF="http:http://127.0.0.1:${FS_DEAD}/gone.yaml"
REPINT_REF="http:http://127.0.0.1:${FS_HTTP}/some-other-name.yaml"
PIPELINE_JSON='{"name": "fs-demo", "description": "placeholder before sourcing"}'
POLICY_DENY='{"periodicRefresh":{"interval":"5m","schedule":"0 3 * * *","overridesAllowed":false}}'
POLICY_ALLOW='{"periodicRefresh":{"interval":"5m","schedule":"0 3 * * *","overridesAllowed":true}}'
API=http://127.0.0.1:8080

assert_eq() { # assert_eq <actual> <wanted> <what>
  if [ "$1" != "$2" ]; then
    echo ">> ASSERTION FAILED: $3 = '$1', want '$2'" >&2
    exit 1
  fi
}
# wait_until <what> <tries> <cmd...>: runs cmd until it exits 0. Emits a
# HEARTBEAT on every iteration (long silent stretches can look idle to a
# supervising terminal, which may reap the process tree — the heartbeat keeps
# the output flowing).
wait_until() {
  local desc="$1" tries="$2"; shift 2
  local i
  for i in $(seq 1 "$tries"); do
    if "$@"; then
      return 0
    fi
    printf '  .. waiting [%d/%d]: %s\n' "$i" "$tries" "$desc"
    sleep 0.5
  done
  echo ">> timed out waiting for: $desc" >&2
  exit 1
}
pipeline_json() { curl -s "$API/api/pipelines/$1"; }
pipeline_has_hello() {
  pipeline_json "$1" | jq -e '[.jobs[]?.key] | any(. == "hello")' >/dev/null
}

# --- 1. attach bootstraps the initial sync ----------------------------------
log ">> attach: the source is attached and the initial sync imports the file"
e2e_http_request POST "$API/api/pipelines" "$PIPELINE_JSON"
assert_eq "$HTTP_CODE" "201" "pipeline create"
PID=$(printf '%s' "$HTTP_BODY" | jq -r '.id')

e2e_http_request POST "$API/api/pipelines/${PID}/source" "{\"kind\":\"http\",\"reference\":\"${REF}\"}"
assert_eq "$HTTP_CODE" "201" "attach source"

# The bootstrap sync (async on attach) imports the file: the pipeline adopts
# the file's JOB (hello) and its periodicRefresh POLICY (5m + the cron), and
# stores the raw YAML + hash.
wait_until "the file's job imported onto the pipeline" 30 pipeline_has_hello "$PID"
DOC=$(pipeline_json "$PID")
assert_eq "$(printf '%s' "$DOC" | jq -r '.periodic_refresh_interval')" "5m" "policy interval adopted from the file"
assert_eq "$(printf '%s' "$DOC" | jq -r '.periodic_refresh_schedule')" "0 3 * * *" "policy cron adopted from the file"
assert_eq "$(printf '%s' "$DOC" | jq -r '.source_hash | length')" "64" "stored source_hash is a SHA-256 (hex)"
assert_eq "$(printf '%s' "$DOC" | jq -r '.source_file | contains("MARKER_V1")')" "true" "stored source_file is the raw YAML (v1 marker)"

# The source state records the last sync's version + hash (no error).
e2e_http_request GET "$API/api/pipelines/${PID}/source"
assert_eq "$HTTP_CODE" "200" "get source"
SRCDOC="$HTTP_BODY"
assert_eq "$(printf '%s' "$SRCDOC" | jq -r '.state.file_hash | length')" "64" "source state file_hash recorded"
assert_eq "$(printf '%s' "$SRCDOC" | jq -r '.state.last_error // ""')" "" "no last_error after a successful bootstrap"
FIRST_HASH=$(printf '%s' "$SRCDOC" | jq -r '.state.file_hash')

# --- 2. manual refresh applies a changed file -------------------------------
log ">> refresh: rewriting the file (v2) + a manual refresh re-imports it"
FILE_YAML_v2=${FILE_YAML_v1/MARKER_V1/MARKER_V2}
printf '%s\n' "$FILE_YAML_v2" > "$SRCROOT/demo.yaml"
VERSION_BEFORE=$(printf '%s' "$DOC" | jq -r '.version')

e2e_http_request POST "$API/api/pipelines/${PID}/source/refresh"
assert_eq "$HTTP_CODE" "200" "manual refresh"
SECOND_HASH=$(printf '%s' "$HTTP_BODY" | jq -r '.file_hash')
if [ "$SECOND_HASH" = "$FIRST_HASH" ]; then
  echo ">> the refresh did not detect the change (hash unchanged: $FIRST_HASH)" >&2
  exit 1
fi

# The re-import is reflected on the pipeline (retry: the API may be briefly
# busy settling the update).
pipeline_has_mk2() {
  pipeline_json "$1" | jq -e '.source_file | contains("MARKER_V2")' >/dev/null
}
wait_until "the re-import reflected on the pipeline" 20 pipeline_has_mk2 "$PID"
DOC=$(pipeline_json "$PID")
assert_eq "$(printf '%s' "$DOC" | jq -r '.version')" "$((VERSION_BEFORE + 1))" "the version bumped on the re-import"
assert_eq "$(printf '%s' "$DOC" | jq -r '.source_hash')" "$SECOND_HASH" "the stored hash followed the new content"

# A run of the pipeline executes the NEW definition (the v2 marker in the
# job's log). Posting the run ALSO exercises the refresh-on-run hook (the
# source is synced before the run is created — a no-op here, already current).
RUN_ID=""
run_posted() {
  local doc
  doc=$(curl -s -X POST "$API/api/pipelines/${PID}/runs" -H 'Content-Type: application/json' -d '{}')
  RUN_ID=$(printf '%s' "$doc" | jq -r '.id // empty')
  [ -n "$RUN_ID" ]
}
wait_until "the run was created" 10 run_posted
job_of_run() {
  curl -s "$API/api/jobs?pipeline_id=${PID}" | jq -r --argjson rid "$RUN_ID" '[.[] | select(.run_id==$rid)][0].id // empty'
}
have_job() { [ -n "$(job_of_run)" ]; }
wait_until "the run's job instance appeared" 20 have_job
JID=$(job_of_run)
e2e_wait_for_job "$JID" "MARKER_V2" "the run executed the v2 definition"

# --- 3. an unchanged refresh is a no-op -------------------------------------
log ">> refresh: an unchanged file is a no-op (no version bump)"
VERSION_NOW=$(pipeline_json "$PID" | jq -r '.version')
e2e_http_request POST "$API/api/pipelines/${PID}/source/refresh"
assert_eq "$HTTP_CODE" "200" "manual refresh (unchanged)"
UNCHANGED_HASH=$(printf '%s' "$HTTP_BODY" | jq -r '.file_hash')
assert_eq "$UNCHANGED_HASH" "$SECOND_HASH" "the hash is unchanged"
VERSION_AFTER=$(pipeline_json "$PID" | jq -r '.version')
assert_eq "$VERSION_AFTER" "$VERSION_NOW" "the version did NOT bump on an unchanged refresh"

# --- 4. kickoff provenance ---------------------------------------------------
log ">> provenance: the run row carries the source's location + revision"
PROV=$(curl -s "$API/api/runs/${RUN_ID}")
assert_eq "$(printf '%s' "$PROV" | jq -r '.source_kind')" "http" "run.source_kind"
assert_eq "$(printf '%s' "$PROV" | jq -r '.source_reference')" "$REF" "run.source_reference"
assert_eq "$(printf '%s' "$PROV" | jq -r '.source_file_hash')" "$SECOND_HASH" "run.source_file_hash (the definition it executed)"
if [ -z "$(printf '%s' "$PROV" | jq -r '.source_version')" ]; then
  echo ">> run.source_version is empty (want the last sync's version)" >&2
  exit 1
fi
# Repoint the source: the run's provenance STAYS (historical immutability).
e2e_http_request POST "$API/api/pipelines/${PID}/source" "{\"kind\":\"http\",\"reference\":\"${REPINT_REF}\"}"
assert_eq "$HTTP_CODE" "201" "repoint the source"
PROV2=$(curl -s "$API/api/runs/${RUN_ID}")
assert_eq "$(printf '%s' "$PROV2" | jq -r '.source_reference')" "$REF" "the run's provenance survived the repoint"

# Restore the reference for the remaining halves.
e2e_http_request POST "$API/api/pipelines/${PID}/source" "{\"kind\":\"http\",\"reference\":\"${REF}\"}"
assert_eq "$HTTP_CODE" "201" "restore the source reference"

# --- 5. the cadence view -----------------------------------------------------
log ">> cadence view: the source view explains the effective cadence"
e2e_http_request GET "$API/api/pipelines/${PID}/source"
CV=$(printf '%s' "$HTTP_BODY" | jq -r '.cadence')
assert_eq "$(printf '%s' "$CV" | jq -r '.declared_interval')" "5m" "cadence.declared_interval"
assert_eq "$(printf '%s' "$CV" | jq -r '.declared_schedule')" "0 3 * * *" "cadence.declared_schedule"
assert_eq "$(printf '%s' "$CV" | jq -r '.override_honored')" "true" "cadence.override_honored (gate allows)"
assert_eq "$(printf '%s' "$CV" | jq -r '.effective_mode')" "cron" "cadence.effective_mode (the cron drives)"
assert_eq "$(printf '%s' "$CV" | jq -r '.effective_schedule')" "0 3 * * *" "cadence.effective_schedule"
assert_eq "$(printf '%s' "$CV" | jq -r '.system_default')" "2m0s" "cadence.system_default (the 2-minute default)"

# --- 6. the admin gate: a denial falls back to the system default -----------
log ">> admin gate: a per-pipeline denial falls back to the system default"
e2e_http_request PUT "$API/api/pipelines/${PID}" "$POLICY_DENY"
assert_eq "$HTTP_CODE" "200" "update with the gate denied"
e2e_http_request GET "$API/api/pipelines/${PID}/source"
CV=$(printf '%s' "$HTTP_BODY" | jq -r '.cadence')
assert_eq "$(printf '%s' "$CV" | jq -r '.pipeline_gate_allowed')" "false" "cadence.pipeline_gate_allowed (denied)"
assert_eq "$(printf '%s' "$CV" | jq -r '.override_honored')" "false" "cadence.override_honored (denied)"
assert_eq "$(printf '%s' "$CV" | jq -r '.effective_mode')" "interval" "cadence.effective_mode (fell back)"
assert_eq "$(printf '%s' "$CV" | jq -r '.effective_interval')" "2m0s" "cadence.effective_interval (THE SYSTEM DEFAULT — the safe restore)"

# Lift the denial again (the file's policy is intact underneath).
e2e_http_request PUT "$API/api/pipelines/${PID}" "$POLICY_ALLOW"
assert_eq "$HTTP_CODE" "200" "update lifting the gate"

# --- 7. a broken source surfaces last_error (preserving the survivors) ------
log ">> broken source: a failed refresh records last_error, keeps the last-good"
GOOD_HASH=$(curl -s "$API/api/pipelines/${PID}/source" | jq -r '.state.file_hash')
GOOD_VER=$(curl -s "$API/api/pipelines/${PID}/source" | jq -r '.state.last_version')
e2e_http_request POST "$API/api/pipelines/${PID}/source" "{\"kind\":\"http\",\"reference\":\"${DEAD_REF}\"}"
assert_eq "$HTTP_CODE" "201" "repoint the source at a dead endpoint"
e2e_http_request POST "$API/api/pipelines/${PID}/source/refresh"
assert_eq "$HTTP_CODE" "422" "the refresh of a dead source fails (422)"
BADSTATE=$(curl -s "$API/api/pipelines/${PID}/source" | jq -r '.state')
ERRLEN=$(printf '%s' "$BADSTATE" | jq -r '.last_error | length')
if [ "$ERRLEN" -lt 1 ]; then
  echo ">> last_error is empty after a failed refresh (want a recorded error)" >&2
  exit 1
fi
assert_eq "$(printf '%s' "$BADSTATE" | jq -r '.file_hash')" "$GOOD_HASH" "the last-good file_hash SURVIVED the failure"
assert_eq "$(printf '%s' "$BADSTATE" | jq -r '.last_version')" "$GOOD_VER" "the last-good version SURVIVED the failure"

log ">> 15 PASSED: externally-sourced pipelines (F-21.5) — attach bootstraps the import, a manual refresh applies a changed file (version bump + the run executes it), an unchanged refresh is a no-op, the run row carries the kickoff provenance (surviving a repoint), the source view explains the effective cadence, a per-pipeline gate denial falls back to the 2-minute system default, and a broken source records last_error without wiping the last-good state"
