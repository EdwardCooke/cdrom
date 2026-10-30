#!/usr/bin/env bash
#
# 17-restoration.sh — F-22 post-deploy verification & rollback, end to end.
#
# Boots the full local stack (db, scheduler, artifacts, idp, api, worker)
# against a throwaway SQLite database and exercises the RESTORED ARC:
#
#   1. A pipeline with a small DAG:
#        build (marker v1) ─► deploy ─► verify (PROBE against a local
#           endpoint) ─► rollback (BRANCH job, armed on verify failure).
#   2. A GOOD run: the probe hits a healthy endpoint → the run succeeds and
#      becomes the ANCHOR.
#   3. A BAD run: the probe hits a DEAD endpoint → verify fails → the branch
#      job's rollback step FIRES (mediated through the API's InitiateRestore,
#      job-token auth ENABLED) → a resumption run is spawned that CARRIES OVER
#      build (terminal clone, outputs copied) and RE-EXECUTES deploy + verify
#      from the anchor's snapshot.
#   4. Assertions: the resumption's provenance (trigger "rollback",
#      restores_run_id = the anchor), the carry-over job (carried_over,
#      terminal, outputs retained), the fresh suffix (pending→succeeded), the
#      AUDIT TRAIL (run.restore, actor job:<id>, door armed), and the
#      MANUAL DOOR (POST /api/pipelines/{id}/restart with a bad cut point →
#      400).
#
# The probe endpoint is a tiny python HTTP server on a fixed localhost port;
# the "dead" endpoint is a closed port (connection-refused → the probe's
# classified connect-error failure).
#
# Usage: scripts/e2e/17-restoration.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, jq,
# and python3. The script always tears the stack down and removes its temp
# directory on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

HEALTH_PORT=10090
DEAD_PORT=10091

e2e_boot_stack with-worker

# --- the probe endpoint: healthy (200) --------------------------------------
python3 - "$HEALTH_PORT" "$WORK/health.log" <<'PY' &
import http.server, sys
PORT, LOG = int(sys.argv[1]), sys.argv[2]
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        with open(LOG, "a") as f:
            f.write("hit\n")
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")
    def log_message(self, *a):
        pass
http.server.ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
PY
HEALTH_PID=$!
PIDS+=("$HEALTH_PID")
# Wait for the healthy endpoint to listen.
for i in $(seq 1 50); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$HEALTH_PORT") 2>/dev/null; then exec 3>&- 2>/dev/null || true; break; fi
  sleep 0.1
done

# --- create the pipeline ------------------------------------------------------
# The DAG: build → deploy → verify → rollback. verify PROBES the endpoint
# named by the run PARAMETER probe_target (good run: the healthy port; bad
# run: the dead port). rollback is a BRANCH job (slice A): it is released
# when verify fails (not skipped) and its step's CONDITION routes on the
# dependency's real status ({{ .deps.verify.Status }}), arming the rollback
# step (slice C) only on failure.
log ">> creating the pipeline"
PIPE_BODY=$(cat <<EOF
{
  "name": "e2e-restoration",
  "params": [
    {"name": "port", "default": "$HEALTH_PORT", "description": "the probe endpoint port (a run supplies the healthy or dead port)"}
  ],
  "jobs": [
    {
      "key": "build",
      "target_group": "default",
      "spec": {
        "steps": [
          {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo built-marker-v1 > \$CDROM_STEP_OUTPUT_DIR/build_marker"]}}},
          {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo build done"]}}}
        ]
      }
    },
    {
      "key": "deploy",
      "needs": ["build"],
      "target_group": "default",
      "spec": {
        "steps": [
          {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo deploying release"]}}}
        ]
      }
    },
    {
      "key": "verify",
      "needs": ["deploy"],
      "target_group": "default",
      "spec": {
        "steps": [
          {"type": "probe", "params": {
            "url": {"string": "http://127.0.0.1:{{ .params.port }}/healthz"},
            "deadline": {"string": "5s"},
            "interval": {"string": "500ms"}
          }}
        ],
        "timeout": "30s"
      }
    },
    {
      "key": "rollback",
      "needs": ["verify"],
      "target_group": "default",
      "spec": {
        "branch": true,
        "steps": [
          {"type": "rollback",
           "condition": "{{ eq .deps.verify.Status \\"failed\\" }}",
           "params": {
             "from_job": {"string": "deploy"},
             "wait": {"string": "false"}
           }}
        ],
        "timeout": "120s"
      }
    }
  ]
}
EOF
)
PIPE_ID=$(printf '%s' "$PIPE_BODY" | curl -s -X POST http://127.0.0.1:8080/api/pipelines -H 'Content-Type: application/json' -d @- | jq -r '.id')
[ "$PIPE_ID" != "null" ] || { echo ">> pipeline create failed: $PIPE_BODY" >&2; exit 1; }
log ">> pipeline id: $PIPE_ID"

# --- run 1: the GOOD run (anchor) --------------------------------------------
log ">> starting the good run (probe hits the healthy endpoint)"
GOOD_RUN=$(curl -s -X POST "http://127.0.0.1:8080/api/pipelines/$PIPE_ID/runs" -H 'Content-Type: application/json' -d '{"params":{"port":"'"$HEALTH_PORT"'"}}')
GOOD_RUN_ID=$(jq -r '.id' <<<"$GOOD_RUN")
[ "$GOOD_RUN_ID" != "null" ] || { echo ">> good run create failed: $GOOD_RUN" >&2; exit 1; }
log ">> good run id: $GOOD_RUN_ID"
# Wait for the good run to SUCCEED (the probe finds 200; the rollback step
# condition is FALSE (verify succeeded) so it condition-skips and the job
# reports skipped-with-no-work → the run still succeeds). NOTE: the API
# serializes proto enums as NUMBERS (plain json.Encode): run status 3 =
# succeeded, 4 = failed, 5 = cancelled.
for i in $(seq 1 150); do
  STATUS=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/runs/$GOOD_RUN_ID" | jq -r '.status' || true)
  if [ "$STATUS" = "3" ]; then break; fi
  if [ "$STATUS" = "4" ] || [ "$STATUS" = "5" ]; then
    echo ">> good run ended with status $STATUS unexpectedly" >&2
    curl -s "http://127.0.0.1:8080/api/runs/$GOOD_RUN_ID" >&2
    exit 1
  fi
  if [ $((i % 5)) -eq 0 ]; then echo "   ...good run status: $STATUS (iteration $i)"; fi
  sleep 0.5
done
[ "$STATUS" = "3" ] || { echo ">> good run did not succeed (last: $STATUS)" >&2; exit 1; }
log ">> good run SUCCEEDED (it is the anchor)"

# The anchor's build job must have PUBLISHED its marker output onto the JOB
# ROW (the job-status loop merges the execution's outputs at the terminal
# transition, F-06) — the carry-over later clones it; assert the SOURCE now.
GOOD_JOBS=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs" || true)
GOOD_BUILD=$(printf '%s\n' "$GOOD_JOBS" | jq -c --argjson rid "$GOOD_RUN_ID" '[.[] | select(.run_id==$rid and .key=="build")][0] // empty' 2>/dev/null || true)
GB_MARK=$(printf '%s\n' "$GOOD_BUILD" | jq -r '.outputs.build_marker // empty' 2>/dev/null || true)
[ "$GB_MARK" = "built-marker-v1" ] || { echo ">> anchor build job row lacks the marker output (got: $(printf '%s\n' "$GOOD_BUILD" | jq -c '.outputs' 2>/dev/null)) — the execution→job output merge failed" >&2; exit 1; }
log ">> anchor build output verified (build_marker=built-marker-v1)"

# --- run 2: the BAD run (fires the rollback) ----------------------------------
log ">> starting the bad run (probe hits the dead endpoint)"
BAD_RUN=$(curl -s -X POST "http://127.0.0.1:8080/api/pipelines/$PIPE_ID/runs" -H 'Content-Type: application/json' -d '{"params":{"port":"'"$DEAD_PORT"'"}}')
BAD_RUN_ID=$(jq -r '.id' <<<"$BAD_RUN")
[ "$BAD_RUN_ID" != "null" ] || { echo ">> bad run create failed: $BAD_RUN" >&2; exit 1; }
log ">> bad run id: $BAD_RUN_ID"

# The bad run's verify job FAILS (probe deadline, connect-error) and the
# rollback job's step FIRES: the worker logs the initiation. Budget
# GENEROUSLY: verify's 5s probe deadline + the resolver's 5s tick + admission
# + dispatch easily exceeds the library's 20s wait, so poll longer here.
#
# On any assertion failure, dump_failure preserves the stack logs and DB
# BEYOND the teardown ($WORK is removed on exit) for offline inspection.
dump_failure() {
  echo "> $*" >&2
  if [ -n "${NEW_RUN_ID:-}" ]; then
    echo "--- resumption jobs:" >&2
    curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs" 2>/dev/null | jq -c --argjson rid "$NEW_RUN_ID" '[.[] | select(.run_id==$rid)]' >&2 || true
  fi
  mkdir -p /tmp/f22-logs && cp "$WORK"/*.log /tmp/f22-logs/ 2>/dev/null || true
  cp "$WORK/cdrom.db" /tmp/f22-db.sqlite3 2>/dev/null || true
  echo "--- logs preserved to /tmp/f22-logs, db to /tmp/f22-db.sqlite3" >&2
}

# Budget: the bad run's jobs release SEQUENTIALLY on the resolver's 5s tick
# (build → deploy → verify-fail → rollback release ≈ 30s+), so allow a minute.
i=0
until grep -qF 'executor: rollback initiated' "$WORK/worker.log" 2>/dev/null; do
  i=$((i+1))
  [ $i -le 300 ] || { dump_failure "timed out waiting for the armed rollback step to fire"; exit 1; }
  if [ $((i % 10)) -eq 0 ]; then echo "   ...waiting for the armed rollback to fire (iteration $i)"; fi
  sleep 0.2
done
log ">> armed rollback step fired"
# The API logs the mediation (containing the anchor and the new run).
e2e_wait_for "$WORK/api.log" 'resumption initiated (armed)' 'the API mediated the restoration'

# Extract the resumption run id from the API log line.
RESTORE_LINE=$(grep -F 'resumption initiated (armed)' "$WORK/api.log" | tail -n 1)
NEW_RUN_ID=$(sed -n 's/.*new_run=\([^ ]*\).*/\1/p' <<<"$RESTORE_LINE")
ANCHORED_AT=$(sed -n 's/.*anchor=\([^ ]*\).*/\1/p' <<<"$RESTORE_LINE")
[ -n "$NEW_RUN_ID" ] || { echo ">> could not parse the new run id from: $RESTORE_LINE" >&2; exit 1; }
[ "$ANCHORED_AT" = "$GOOD_RUN_ID" ] || { echo ">> resumption anchored at $ANCHORED_AT, want the good run $GOOD_RUN_ID" >&2; exit 1; }
log ">> resumption run id: $NEW_RUN_ID (anchored at $ANCHORED_AT)"

# NB: the pipeline's DEFINITION rows (run_id null, lowest ids) also appear in
# the full /api/jobs list; the partition filter below excludes them by
# requiring run_id == the resumption's id.

# The resumption re-runs deploy + verify from the ANCHOR's snapshot — and the
# anchor's verify step probed the HEALTHY endpoint (its params are the
# anchor's), so the resumption's verify SUCCEEDS and the resumption succeeds.
# (The rollback step set wait=false, so it does not block on the verdict;
# run status numerals: 3 = succeeded, 4 = failed, 5 = cancelled.)
for i in $(seq 1 150); do
  RS=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/runs/$NEW_RUN_ID" | jq -r '.status' || true)
  if [ "$RS" = "3" ] || [ "$RS" = "4" ] || [ "$RS" = "5" ]; then break; fi
  if [ $((i % 5)) -eq 0 ]; then echo "   ...resumption status: $RS (iteration $i)"; fi
  sleep 0.5
done
[ "$RS" = "3" ] || { dump_failure "resumption ended $RS, want 3/succeeded (it restores the KNOWN-GOOD snapshot)"; exit 1; }
log ">> resumption SUCCEEDED (the known-good suffix re-executed)"

# --- prove the resumption's STRUCTURE ----------------------------------------
# Fetch the resumption's jobs (filtered client-side by run_id) and assert the
# PARTITION: build is CARRIED OVER (terminal, carried_over, outputs copied);
# verify is FRESH (executed in this run). carried_over:false is omitted under
# the API's omitempty serialization, so presence-and-true marks a carry-over
# and ABSENCE marks the fresh suffix.
JOBS_JSON=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs" | jq -c --argjson rid "$NEW_RUN_ID" '[.[] | select(.run_id==$rid)]' 2>/dev/null || echo "[]")
COUNT=$(printf '%s\n' "$JOBS_JSON" | jq 'length' 2>/dev/null || echo '?')
[ "$COUNT" = "4" ] || { dump_failure "resumption has $COUNT jobs, want 4 (the anchor's full DAG cloned)"; exit 1; }
BUILD_ROW=$(printf '%s\n' "$JOBS_JSON" | jq -c '[.[] | select(.key=="build")][0] // empty' 2>/dev/null || true)
VERIFY_ROW=$(printf '%s\n' "$JOBS_JSON" | jq -c '[.[] | select(.key=="verify")][0] // empty' 2>/dev/null || true)
[ -n "$BUILD_ROW" ] || { dump_failure "could not find build among the resumption's jobs"; exit 1; }
[ -n "$VERIFY_ROW" ] || { dump_failure "could not find verify among the resumption's jobs"; exit 1; }
BO_BUILD=$(printf '%s\n' "$BUILD_ROW" | jq -r '.carried_over // false' 2>/dev/null || echo '?')
ST_BUILD=$(printf '%s\n' "$BUILD_ROW" | jq -r '.status // "?"' 2>/dev/null || echo '?')
BO_VERIFY=$(printf '%s\n' "$VERIFY_ROW" | jq -r 'if has("carried_over") then .carried_over else false end' 2>/dev/null || echo '?')
ST_VERIFY=$(printf '%s\n' "$VERIFY_ROW" | jq -r '.status // "?"' 2>/dev/null || echo '?')
[ "$BO_BUILD" = "true" ] || { dump_failure "build (carry-over) carried_over = $BO_BUILD, want true"; exit 1; }
[ "$ST_BUILD" = "3" ] || { dump_failure "build (carry-over) status = $ST_BUILD, want 3/succeeded (replicated from the anchor)"; exit 1; }
[ "$BO_VERIFY" = "false" ] || { dump_failure "verify carried_over = $BO_VERIFY, want absent/false (fresh suffix)"; exit 1; }
[ "$ST_VERIFY" = "3" ] || { dump_failure "verify (fresh suffix) status = $ST_VERIFY, want 3/succeeded (it re-probed the ANCHOR's healthy endpoint)"; exit 1; }
MARKER=$(printf '%s\n' "$BUILD_ROW" | jq -r '.outputs.build_marker // empty' 2>/dev/null || true)
[ "$MARKER" = "built-marker-v1" ] || { dump_failure "build carry-over outputs lack build_marker=built-marker-v1 (the clone must copy the anchor's outputs verbatim)"; exit 1; }
log ">> partition verified: build carried over (outputs intact), verify re-executed fresh"

# Prove the resumption's PROVENANCE via the run detail (restores_run_id +
# trigger).
DETAIL=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/runs/$NEW_RUN_ID" || true)
DET_TRIG=$(printf '%s\n' "${DETAIL:-}" | jq -r '.trigger // empty' 2>/dev/null || true)
DET_RESTORES=$(printf '%s\n' "${DETAIL:-}" | jq -r '.restores_run_id // empty' 2>/dev/null || true)
if [ -n "$DET_TRIG" ] || [ -n "$DET_RESTORES" ]; then
  [ "$DET_TRIG" = "rollback" ] || { echo ">> resumption trigger = $DET_TRIG, want rollback (detail: $DETAIL)" >&2; exit 1; }
  [ "$DET_RESTORES" = "$GOOD_RUN_ID" ] || { echo ">> resumption restores_run_id = $DET_RESTORES, want $GOOD_RUN_ID (detail: $DETAIL)" >&2; exit 1; }
  log ">> provenance verified: trigger=rollback, restores_run_id=$GOOD_RUN_ID"
else
  log "!! run detail omits trigger/restores_run_id; relying on the API log correlation (anchor=$ANCHORED_AT)"
fi

# --- the AUDIT TRAIL (F-15) ---------------------------------------------------
# The armed initiation is audited (run.restore, door=armed); the manual door's
# initiation is audited too (door=manual). Match loosely (the audit log's JSON
# formatting varies).
grep -qF 'run.restore' "$WORK/audit.log" 2>/dev/null || { echo ">> audit log has no run.restore entry" >&2; cat "$WORK/audit.log" >&2; exit 1; }
grep -F 'run.restore' "$WORK/audit.log" | grep -qiF 'armed' || { echo ">> audit run.restore entry lacks door=armed" >&2; exit 1; }
log ">> audit trail verified (run.restore, door=armed)"

# --- the MANUAL DOOR: a bad cut point is rejected cleanly --------------------
MANUAL_BAD=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:8080/api/pipelines/$PIPE_ID/restart" -H 'Content-Type: application/json' -d '{"from_job":"ghost"}')
[ "$MANUAL_BAD" = "400" ] || { echo ">> manual restart with a bad cut point = HTTP $MANUAL_BAD, want 400" >&2; exit 1; }
log ">> manual door verified (bad cut point → 400)"

# --- the MANUAL DOOR works end-to-end ----------------------------------------
# (Armed-door cooldown and no-re-arm are covered by unit tests; here the manual
# door — which bypasses both by human authority — spawns a second resumption
# from a DIFFERENT cut point, proving the door is operational.)
MANUAL_GOOD=$(curl -s -X POST "http://127.0.0.1:8080/api/pipelines/$PIPE_ID/restart" -H 'Content-Type: application/json' -d '{"from_job":"verify"}')
MANUAL_NEW_RUN=$(jq -r '.id // empty' <<<"$MANUAL_GOOD")
[ -n "$MANUAL_NEW_RUN" ] || { echo ">> manual restart failed: $MANUAL_GOOD" >&2; exit 1; }
log ">> manual door spawned resumption run $MANUAL_NEW_RUN (operator-initiated)"

log ">> PASSED"
