#!/usr/bin/env bash
#
# 27-failure-concurrency.sh — F-30.4 Slice 3: failure escalation, the
# active-coordinate index, parallel children, and the k8s-restart STORY
# (item B: the worker's mid-life reconnector + the agent's orphaned-
# observation adoption).
#
# Dedicated agent "s3-agent" (group s3grp) runs on a FIXED supervision bind
# (127.0.0.1:7401 — simulating the k8s STABLE-address posture: a successor
# incarnation rebinds the same port, so the worker's reconnector can actually
# reunite with it) and its own metrics port (:9107 — the shared agent owns
# :9100). Short shutdown horizons (2 s / 1 s) keep the teardown brisk.
#
# Proven here (PATIENCE FOR PRESENCE, IMMUTABILITY FOR MEASURE, DURABLE
# MEDIA FOR VERDICTS — the SHARED agent log suffers the harness's reader
# pathology, so HARD verdicts ride REST state + the agent's exported
# metrics; the DEDICATED s3-agent logs are exception-grade reliable: the
# worker's stderr IS the agent's log fd (fd inheritance through
# setsid+detach; logging opens O_APPEND), so checks against them are HARD):
#
#   A. SIGKILL DRILL (AC #6) — a worker killed mid-job (-9, uncatchable)
#      makes the agent ESCALATE (ReportChildDeath): the job ends FAILED
#      promptly (< 30 s — well inside any F-03 watchdog horizon), not after
#      riding out the timeout. F-05 immunity is unit-pinned (a terminal
#      execution is left as-is).
#   B. PARALLELISM — two jobs on one agent overlap (launch(L2) before
#      exit(L1)) and BOTH complete (the artificial cap stayed removed; the
#      index does not hinder distinct coordinates — the twin edge is
#      unit/schema-proven: TestActiveCoordinateIndexRejectsTwin +
#      TestStartJobExecutionPaysTheConcurrencyToll's neighbor).
#   C. RESTART / ADOPTION / RECONNECTION (ITEM B) — the agent is SIGKILLed
#      mid-job; the worker SURVIVES (detached process group) and its channel
#      tears; the SUCCESSOR incarnation (same name/credential/address)
#      ADOPTS the orphaned execution at boot (the register-time blind
#      AbandonAgentExecutions is retired — the agent is the oracle of its
#      own lineage); the worker RECONNECTS mid-life (resume lane) and
#      completes the job; the successor's OWN exported series absorb the
#      adopted account (jobs_total{succeeded} advances on :9107 — the
#      discriminator: the shared agent's series do NOT advance for this
#      job).
#
# Usage: scripts/e2e/27-failure-concurrency.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, jq.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-agent

API=http://127.0.0.1:8080
S3_METRICS=http://127.0.0.1:9107
BIND=127.0.0.1:7401
S3_NAME=s3-agent
S3_GROUP=s3grp
S3_LOG="$WORK/s3-agent.log"
S3_LOG2="$WORK/s3-agent-round2.log" # the SUCCESSOR incarnation's log (.log name: the archiver's glob)

# --- helpers (self-contained; the pattern is 26's) ------------------------

metric_value() { # metric_value PORT METRIC_NAME LABEL_PAIRS... -> value|''
  # Matches the line whose NAME is exactly METRIC_NAME (followed by '{' or
  # whitespace — no prefix-shadowing), then requires EACH label pair to
  # appear somewhere in the line (pairs are matched verbatim, e.g.
  # status="succeeded"): Prometheus exposition sorts label names, so the
  # pairs are never contiguous — individual matching is the only reliable
  # geometry (lesson of 2026-11-11). Prints the LAST matching line's value.
  local port="$1" name="$2"; shift 2
  local body
  body=$(curl -sf "http://127.0.0.1:${port}/metrics" 2>/dev/null || true)
  [ -n "$body" ] || return 0
  local line rest val=""
  while IFS= read -r line; do
    case "$line" in
      '#'*|'') continue ;;
    esac
    case "$line" in
      "$name"{*) ;;
      "$name"[[:space:]]*) ;;
      *) continue ;;
    esac
    rest="${line#"$name"}"
    # The VALUE is the last whitespace-separated token ({labels} aside).
    # (NOTE: ${rest#*}} is a PITFALL — it parses as ${rest#*} + literal "}}".)
    val="$(printf '%s' "$rest" | awk '{print $NF}')"
    local ok=1 lbl
    for lbl in "$@"; do
      case "$line" in *"$lbl"*) ;; *) ok=0; break ;; esac
    done
    if [ "$ok" = "1" ]; then
      case "$val" in
        ''|*[!0-9.eE+-]*) ;;
        *) printf '%s' "$val"; return 0 ;;
      esac
    fi
  done <<<"$body"
  return 0
}

gt_float() { # gt_float A B -> 0 if A>B (floats)
  awk -v a="${1:-0}" -v b="${2:-0}" 'BEGIN{exit !(a>b)}'
}

wait_presence() { # wait_presence FILE PATTERN DESCRIPTION [TIMEOUT_SEC] — HARD tier (fails the scenario)
  local file="$1" pat="$2" desc="$3" timeout="${4:-45}"
  local deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if grep -aqE "$pat" "$file" 2>/dev/null; then return 0; fi
    sleep 0.5
  done
  diag_dump
  echo ">> $desc: pattern '$pat' never appeared in $file" >&2
  exit 1
}

maybe_present() { # maybe_present FILE PATTERN DESCRIPTION [TIMEOUT_SEC] — SOFT tier (patient, tolerated-vanished)
  local file="$1" pat="$2" desc="$3" timeout="${4:-45}"
  local deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if grep -aqE "$pat" "$file" 2>/dev/null; then
      log ">> $desc (soft corroboration present)"
      return 0
    fi
    sleep 0.5
  done
  log ">> $desc: SOFT line tolerated-vanished (pathology) — continuing"
  return 0
}

diag_dump() {
  echo ">>> DIAGNOSTIC DUMP ($(date -u '+%Y-%m-%dT%H:%M:%SZ'))" >&2
  for f in "$S3_LOG" "$WORK/agent.log" "$WORK/api.log"; do
    echo "--- $f (tail) ---" >&2
    tail -n 25 "$f" 2>/dev/null >&2 || true
  done
}

job_status() { # job_status ID -> status int ('' on transport trouble)
  local body
  body=$(curl -sf "$API/api/jobs/$1" 2>/dev/null || true)
  printf '%s' "$body" | jq -r '.status // empty' 2>/dev/null || true
}

submit_sleep_job() { # submit_sleep_job NAME SECONDS_SLEEP -> job id
  local name="$1" secs="$2"
  e2e_submit_job "$(cat <<EOF
{
  "name": "$name",
  "target_group": "$S3_GROUP",
  "spec": {
    "steps": [
      {
        "params": {
          "command": { "string": "sh" },
          "args": { "strings": [ "-c", "sleep $secs; echo $name-done" ] }
        }
      }
    ]
  }
}
EOF
)"
}

# --- dedicate the s3 agent --------------------------------------------------

log ">> issuing credential + booting the dedicated agent (fixed bind $BIND, metrics :9107)"
e2e_issue_agent_credential "$S3_NAME" "$WORK/s3-agent.cred" >/dev/null

env CDROM_AGENT_NAME="$S3_NAME" \
    CDROM_AGENT_GROUP="$S3_GROUP" \
    CDROM_AGENT_CREDENTIAL_FILE="$WORK/s3-agent.cred" \
    CDROM_AGENT_WORKER_BINARY="$BIN/worker" \
    CDROM_SUPERVISION_BIND="$BIND" \
    CDROM_METRICS_ADDR=:9107 \
    CDROM_SHUTDOWN_GRACE=2s \
    CDROM_SHUTDOWN_KILL_GRACE=1s \
    CDROM_LOG_FORMAT=text \
    "$BIN/agent" >"$S3_LOG" 2>&1 &
PIDS+=($!)
S3_PID=$!

ready=0
for i in $(seq 1 60); do
  if grep -qa 'registered' "$S3_LOG" 2>/dev/null; then ready=1; break; fi
  sleep 0.5
done
[ "$ready" = "1" ] || { diag_dump; echo ">> s3-agent never registered" >&2; exit 1; }
log ">> s3-agent up (pid $S3_PID)"

# =============================================================================
# PART A — SIGKILL DRILL: abrupt worker death ⇒ ReportChildDeath ⇒ prompt fail
# =============================================================================
log ">> PART A: submitting a 30 s job, then SIGKILLing its worker mid-run"
A_ID=$(submit_sleep_job "s3-sigkill-drill" 30)
log ">> job id: $A_ID"

A_WPID=''
for i in $(seq 1 60); do
  A_WPID=$(grep -oa 'child_id=[0-9]*' "$S3_LOG" 2>/dev/null | tail -1 | cut -d= -f2 || true)
  [ -n "$A_WPID" ] && break
  sleep 0.5
done
[ -n "$A_WPID" ] || { diag_dump; echo ">> worker pid never parsed from $S3_LOG" >&2; exit 1; }
log ">> worker pid: $A_WPID"
sleep 2 # let the worker settle into its sleep

T0=$(date +%s)
kill -9 "$A_WPID"
log ">> worker SIGKILLed; expecting the job to end FAILED promptly (< 30 s)"

# REST status mirrors the proto enum: 1 pending, 2 running, 3 succeeded,
# 4 failed, 5 cancelled, 6 timed_out, 7 skipped. Break on TERMINAL only.
A_FINAL=''
for i in $(seq 1 60); do
  A_FINAL=$(job_status "$A_ID")
  case "$A_FINAL" in 3|4|5|6|7) break;; esac
  sleep 0.5
done
ELAPSED=$(( $(date +%s) - T0 ))
log ">> job status: $A_FINAL (${ELAPSED}s after the kill)"
[ "$A_FINAL" = "4" ] || { diag_dump; echo ">> PART A FAILED: status $A_FINAL, want 4 (failed)" >&2; exit 1; }
[ "$ELAPSED" -lt 30 ] || { echo ">> PART A FAILED: took ${ELAPSED}s (want < 30 — the watchdog ride-out is the anti-pattern)" >&2; exit 1; }
# SOFT corroboration (presence-patient; tolerated-vanished):
maybe_present "$S3_LOG" 'worker died abnormally' "PART A soft: abnormal-death line" 10
maybe_present "$S3_LOG" 'child death reported' "PART A soft: escalation line" 10
log ">> PART A PASSED (prompt failure via ReportChildDeath, ${ELAPSED}s)"

# =============================================================================
# PART B — PARALLELISM: two jobs overlap on one agent; both complete
# =============================================================================
log ">> PART B: two concurrent jobs on the s3 agent"
B1_ID=$(submit_sleep_job "s3-par-one" 8)
B1_LAUNCH_LINE='msg="agent: worker launched" job='"$B1_ID"
wait_presence "$S3_LOG" "$B1_LAUNCH_LINE" "PART B: first job launched" 30
B2_ID=$(submit_sleep_job "s3-par-two" 8)
wait_presence "$S3_LOG" 'msg="agent: worker launched" job='"$B2_ID" "PART B: second job launched" 30
# Both must reach success (3).
B1_FINAL='' B2_FINAL=''
for i in $(seq 1 90); do
  B1_FINAL=$(job_status "$B1_ID"); B2_FINAL=$(job_status "$B2_ID")
  case "$B1_FINAL$B2_FINAL" in *3*3*) break;; esac
  sleep 0.5
done
[ "$B1_FINAL" = "3" ] && [ "$B2_FINAL" = "3" ] || { diag_dump; echo ">> PART B FAILED: statuses $B1_FINAL/$B2_FINAL, want 3/3" >&2; exit 1; }
log ">> PART B PASSED (overlap submitted; both succeeded)"

# =============================================================================
# PART C — RESTART / ADOPTION / MID-LIFE RECONNECTION (ITEM B)
# =============================================================================
log ">> PART C: 45 s job; SIGKILL the AGENT mid-run; relaunch the successor;"
log ">> the worker must survive, reconnect, finish, and the successor must absorb its account"
C_ID=$(submit_sleep_job "s3-reunion" 45)
C_WPID=''
for i in $(seq 1 60); do
  C_WPID=$(grep -oa 'child_id=[0-9]*' "$S3_LOG" 2>/dev/null | tail -1 | cut -d= -f2 || true)
  [ -n "$C_WPID" ] && break
  sleep 0.5
done
[ -n "$C_WPID" ] || { diag_dump; echo ">> PART C: worker pid never parsed" >&2; exit 1; }
sleep 2

T1=$(date +%s)
kill -9 "$S3_PID"
log ">> s3-agent SIGKILLed at T+0 (worker $C_WPID survives — detached process group)"
# The worker's channel tears; its reconnector ramps (15 s base). Bring up the
# SUCCESSOR (same name/credential/bind) promptly — it must be listening before
# the worker's first re-dial.
sleep 2
env CDROM_AGENT_NAME="$S3_NAME" \
    CDROM_AGENT_GROUP="$S3_GROUP" \
    CDROM_AGENT_CREDENTIAL_FILE="$WORK/s3-agent.cred" \
    CDROM_AGENT_WORKER_BINARY="$BIN/worker" \
    CDROM_SUPERVISION_BIND="$BIND" \
    CDROM_METRICS_ADDR=:9107 \
    CDROM_SHUTDOWN_GRACE=2s \
    CDROM_SHUTDOWN_KILL_GRACE=1s \
    CDROM_LOG_FORMAT=text \
    "$BIN/agent" >"$S3_LOG2" 2>&1 &
PIDS+=( $! )
S3_PID=$!
ready=0
for i in $(seq 1 60); do
  if grep -qa 'registered' "$S3_LOG2" 2>/dev/null; then ready=1; break; fi
  sleep 0.5
done
[ "$ready" = "1" ] || { diag_dump; echo ">> PART C: successor never registered" >&2; exit 1; }
log ">> successor up at T+$(( $(date +%s) - T1 ))"

# Baseline from the SUCCESSOR's FRESH series (the original agent's metrics
# died with it — measuring before the reboot would compare against the
# predecessor's counts and poison the advancement check).
BASE_SUCC_BEFORE=$(metric_value 9107 'cdrom_agent_jobs_total' 'status="succeeded"' || true)
log ">> successor baseline jobs_total{succeeded}='${BASE_SUCC_BEFORE:-0}'"

# ADOPTION (medium tier: the successor's own log — fewer competing readers).
wait_presence "$S3_LOG2" 'ADOPTED orphaned execution' "PART C: adoption" 30
# RECONNECTION (hard: deterministic — the worker notices the torn signals
# stream instantly and retries at the 15 s base delay; its stderr is the
# ORIGINAL agent's log fd, so the line lands in $S3_LOG reliably).
wait_presence "$S3_LOG" 'RECONNECTED \(mid-life\)' "PART C: worker reconnected (mid-life)" 60

# The job completes (the worker's own terminal report is the authority).
C_FINAL=''
for i in $(seq 1 120); do
  C_FINAL=$(job_status "$C_ID")
  case "$C_FINAL" in 3|4|5|6|7) break;; esac
  sleep 0.5
done
[ "$C_FINAL" = "3" ] || { diag_dump; echo ">> PART C FAILED: status $C_FINAL, want 3 (succeeded)" >&2; exit 1; }
log ">> job reunited-and-completed at T+$(( $(date +%s) - T1 ))"

# ABSORPTION (HARD: the SUCCESSOR's own exported series advance — the adopted
# account folded into THIS instance; the shared agent's series are untouched
# by this job).
succ_after=''
for i in $(seq 1 30); do
  succ_after=$(metric_value 9107 'cdrom_agent_jobs_total' 'status="succeeded"' || true)
  [ -n "$succ_after" ] && gt_float "$succ_after" "${BASE_SUCC_BEFORE:-0}" && break
  sleep 0.5
done
if [ -z "$succ_after" ] || ! gt_float "$succ_after" "${BASE_SUCC_BEFORE:-0}"; then
  diag_dump
  echo ">> PART C FAILED: successor's jobs_total{succeeded} '${succ_after:-none}' did not advance past '${BASE_SUCC_BEFORE:-0}'" >&2
  exit 1
fi
maybe_present "$S3_LOG2" 'adopted worker completed' "PART C medium: absorption line" 15
log ">> PART C PASSED (adoption + mid-life reconnection + absorption into the successor's series)"

log ">> 27-failure-concurrency PASSED"
exit 0
