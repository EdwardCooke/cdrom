#!/usr/bin/env bash
#
# 17-restoration.sh — F-22 post-deploy verification & rollback, end to end.
#
# Boots the full local stack (db, scheduler, artifacts, idp, api, agent)
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

e2e_boot_stack with-agent

# Per-instance log baselines: on the SHARED stack (make e2e) the component
# logs ACCUMULATE across suite members, so the log-driven waits below must
# consider only what was written AFTER this incarnation began — otherwise a
# stale line from an earlier member satisfies a grep early (or correlates the
# WRONG run). Standalone mode: the logs are fresh, so the baseline is ~0.
WORKER_LOG_BASE=$(stat -c%s "$WORK/agent.log" 2>/dev/null || echo 0)
API_LOG_BASE=$(stat -c%s "$WORK/api.log" 2>/dev/null || echo 0)
AUDIT_BOOT_BASE=$(stat -c%s "$WORK/audit.log" 2>/dev/null || echo 0)
# newpart FILE OFFSET DEST copies the bytes of FILE from OFFSET onwards (the
# per-incarnation slice) into DEST. A file COPY (not a pipe) sidesteps the
# grep -q SIGPIPE-under-pipefail pitfall documented in lib.sh's neighbours.
newpart() { tail -c +"$2" "$1" > "$3" 2>/dev/null || : > "$3"; }

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
  # Probe in a SUBSHELL (its fd 3 dies with it). Do NOT add a bare `exec`
  # cleanup here: with no command, `exec` applies its redirections
  # persistently to THIS shell, and `exec 3>&- 2>/dev/null` rewired the whole
  # script's stderr to /dev/null for the rest of the run (silent-death
  # plague, 2026-11-05; see lib.sh's POISON RULE).
  if (exec 3<>"/dev/tcp/127.0.0.1/$HEALTH_PORT") 2>/dev/null; then break; fi
  sleep 0.1
done

# --- create the pipeline ------------------------------------------------------
# Unique PER PROCESS INSTANCE: the pipeline name is globally unique, and this
# script runs MULTIPLE TIMES against the shared stack (make e2e) — a fixed
# name would collide with our own earlier incarnation (UNIQUE constraint).
# $$ (the script's PID) is unique per run and unset-safe under set -u.
PIPE_NAME="e2e-restoration-$$"
# The DAG: build → deploy → verify → rollback. verify PROBES the endpoint
# named by the run PARAMETER probe_target (good run: the healthy port; bad
# run: the dead port). rollback is a BRANCH job (slice A): it is released
# when verify fails (not skipped) and its step's CONDITION routes on the
# dependency's real status ({{ .deps.verify.Status }}), arming the rollback
# step (slice C) only on failure.
log ">> creating the pipeline"
PIPE_BODY=$(cat <<EOF
{
  "name": "$PIPE_NAME",
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
# succeeded, 4 = failed, 5 = cancelled. The budget is GENEROUS (symmetrical
# with the resumption wait below): the DAG advances strictly on the
# scheduler's 5 s ticks (resolver → admission → job-status → run-status), so
# a nominal ~30 s run routinely doubles when the shared stack is loaded with
# the residue of the preceding suites — a tight budget turns routine load
# into a flake.
for i in $(seq 1 300); do
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
# rollback job's step FIRES: the agent logs the initiation. Budget
# GENEROUSLY: verify's 5s probe deadline + the resolver's 5s tick + admission
# + dispatch easily exceeds the library's 20s wait, so poll longer here.
#
# On any assertion failure, dump_failure preserves the stack logs and DB
# BEYOND the teardown ($WORK is removed on exit) for offline inspection. It
# ALSO dumps the DATABASE'S VIEW OF THE WORLD (every job/run/execution with
# its timestamps) straight from the SQLite file, so a stall that predates the
# resumption (e.g. the good run never finishing) is diagnosable from the dump
# alone: which job sat where, since when, and whether the agent acknowledged
# it at all.
dump_failure() {
  echo "> $*" >&2
  if [ -n "${NEW_RUN_ID:-}" ]; then
    echo "--- resumption jobs:" >&2
    curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs" 2>/dev/null | jq -c --argjson rid "$NEW_RUN_ID" '[.[] | select(.run_id==$rid)]' >&2 || true
  fi
  if [ -n "${GOOD_RUN_ID:-}" ]; then
    echo "--- good run ($GOOD_RUN_ID) jobs (straight from the db):" >&2
    sqlite3 -batch -separator ' | ' "$WORK/cdrom.db" \
      "SELECT 'job '||id||' key='||key||' status='||status||' attempt='||COALESCE(attempt,0)||' created='||created_at||' started='||COALESCE(started_at,'-')||' finished='||COALESCE(finished_at,'-');
       SELECT 'exe '||id||' job='||job_id||' agent='||agent_name||' status='||status||' created='||created_at||' started='||COALESCE(started_at,'-')||' finished='||COALESCE(finished_at,'-');" \
      2>/dev/null | grep -E "^job|^exe" >&2 || true
    echo "--- good run row:" >&2
    sqlite3 -batch -separator ' | ' "$WORK/cdrom.db" \
      "SELECT 'run '||id||' status='||status||' trigger='||COALESCE(trigger,'-')||' created='||created_at||' started='||COALESCE(started_at,'-')||' finished='||COALESCE(finished_at,'-');" \
      2>/dev/null | grep "^run" >&2 || true
  fi
  echo "--- agent.log (full):" >&2
  cat "$WORK/agent.log" >&2 2>/dev/null || true
  echo "--- scheduler.log (tail 40):" >&2
  tail -n 40 "$WORK/scheduler.log" >&2 2>/dev/null || true
  echo "--- api.log (tail 40):" >&2
  tail -n 40 "$WORK/api.log" >&2 2>/dev/null || true
  mkdir -p /tmp/f22-logs && cp "$WORK"/*.log /tmp/f22-logs/ 2>/dev/null || true
  cp "$WORK/cdrom.db" /tmp/f22-db.sqlite3 2>/dev/null || true
  echo "--- logs preserved to /tmp/f22-logs, db to /tmp/f22-db.sqlite3" >&2
}

# Budget: the bad run's jobs advance STRICTLY ON THE SCHEDULER'S 5 s TICKS
# (resolver → admission → job-status, one hop each: build → deploy →
# verify-fail → rollback release ≈ 30 s nominal). Allow TWO minutes: the
# nominal figure assumes an unloaded scheduler, and in shared-stack mode
# (make e2e) the components absorb the residue of the preceding suites, so
# the ticks stretch. A tight budget here is a standing invitation to flake.
SLICE_W="$WORK/.agent.slice" SLICE_A="$WORK/.api.slice"
i=0
until newpart "$WORK/agent.log" "$WORKER_LOG_BASE" "$SLICE_W" && grep -qF 'executor: rollback initiated' "$SLICE_W" 2>/dev/null; do
  i=$((i+1))
  [ $i -le 600 ] || { dump_failure "timed out waiting for the armed rollback step to fire"; exit 1; }
  if [ $((i % 10)) -eq 0 ]; then echo "...waiting for the armed rollback to fire (iteration $i)"; fi
  sleep 0.2
done
log ">> armed rollback step fired"
# The API logs the mediation (containing the anchor and the new run). Give it a
# LONGER budget than the library's e2e_wait_for (20 s): in shared-stack mode
# (make e2e) the components absorb the residue of the earlier suites, so the
# mediation can legitimately take tens of seconds. A tight budget here turned
# routine load into a flake.
i=0
until newpart "$WORK/api.log" "$API_LOG_BASE" "$SLICE_A" && grep -qF 'resumption initiated (armed)' "$SLICE_A" 2>/dev/null; do
  i=$((i+1))
  [ $i -le 300 ] || { dump_failure "timed out waiting for the API to mediate the restoration"; exit 1; }
  if [ $((i % 10)) -eq 0 ]; then echo "   ...waiting for the API mediation (iteration $i)"; fi
  sleep 0.2
done

# Extract the resumption run id from the API log line (THIS incarnation's
# slice: on the shared stack an earlier member's mediation line precedes ours).
RESTORE_LINE=$(grep -F 'resumption initiated (armed)' "$SLICE_A" | tail -n 1)
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
# run status numerals: 3 = succeeded, 4 = failed, 5 = cancelled.) Generous
# budget, symmetric with the good-run wait: the resumption re-drives a full
# DAG on the 5 s scheduler ticks.
for i in $(seq 1 300); do
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
# formatting varies). Judge against THIS incarnation's slice (from the boot
# baseline): on the shared stack an earlier member's run.restore entry would
# otherwise satisfy the check vacuously.
AUDIT_SLICE="$WORK/.audit.slice"
newpart "$WORK/audit.log" "$AUDIT_BOOT_BASE" "$AUDIT_SLICE"
grep -qF 'run.restore' "$AUDIT_SLICE" 2>/dev/null || { echo ">> audit log gained no run.restore entry this incarnation" >&2; tail -n 20 "$WORK/audit.log" >&2; exit 1; }
grep -F 'run.restore' "$AUDIT_SLICE" | grep -qiF 'armed' || { echo ">> audit run.restore entry lacks door=armed" >&2; exit 1; }
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

# CLEANUP: the manual-door assertion deliberately stops at SPAWN (the armed
# door already proved the full restore arc). Leaving the manual resumption
# LIVE abandons work on the shared stack: its jobs tie up the lone agent and
# inflate the scheduler's per-tick scans for every SUITE MEMBER THAT RUNS NEXT
# (classic self-inflicted flake source). Cancel the run's jobs and VERIFY THEY
# SETTLE: the job list can race the row creation (and a partial cancel leaves
# survivors that keep running), so loop until ZERO of the run's jobs is
# non-terminal, judged against the DATABASE (the authoritative source; the
# run-row status alone lies while survivor jobs linger). Cancelling a
# finished job is a documented no-op (F-05), so blanket re-cancelling is safe.
SETTLED=no
for attempt in $(seq 1 40); do
  MANUAL_JIDS=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs" | jq -c --argjson rid "$MANUAL_NEW_RUN" '[.[] | select(.run_id==$rid) | .id] // []' 2>/dev/null || echo '[]')
  NJ=$(printf '%s' "$MANUAL_JIDS" | jq 'length' 2>/dev/null || echo 0)
  if [ "$NJ" -ge 1 ]; then
    for jid in $(printf '%s' "$MANUAL_JIDS" | jq -r '.[]'); do
      curl -s -o /dev/null --max-time 5 -X POST "http://127.0.0.1:8080/api/jobs/$jid/cancel" || true
    done
  fi
  ACTIVE=$(sqlite3 -batch "$WORK/cdrom.db" "SELECT COUNT(*) FROM jobs WHERE run_id=$MANUAL_NEW_RUN AND status IN ('pending','running','awaiting_approval','awaiting_proceed');" 2>/dev/null || echo '?')
  if [ "$ACTIVE" = "0" ]; then SETTLED=yes; break; fi
  sleep 0.5
done
if [ "$SETTLED" = "yes" ]; then
  log ">> manual resumption cancelled and settled (the stack is left quiet)"
else
  log "!! manual resumption did not settle within 20s (active=$ACTIVE); continuing anyway"
fi

log ">> PASSED"
