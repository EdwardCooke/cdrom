#!/usr/bin/env bash
#
# 24-agent-credential-rotation.sh — F-30.3 Slices 2+3: the consumer-side
# EFFECTS of the agent-credential lifecycle (ROTATION + REVOCATION) on a
# RUNNING agent, over the relaxed target leg (TLS-with-server-verification +
# bearer credential; no client certificate on the agent↔API leg).
#
# Story:
#   1. BASELINE: the shared stack's agent (agent-1) runs with a provisioned
#      credential (the harness issued it at boot). A job COMPLETES — the
#      credential works end-to-end (register/watch/heartbeat/poll rode it).
#   2. ROTATE: the credential is rotated (management endpoint) — the old row
#      is RETIRED (chained via replaced_by_id) and a successor JWT is issued.
#      Within the spec's bound (cache TTL + one heartbeat) the agent's
#      heartbeat is DENIED (the retired row fails liveness) and its WatchJobs
#      stream TEARS DOWN on the API ("credential lapsed; watch stream torn
#      down"). The agent notices the stream loss and its reconnect attempts
#      keep presenting the RETIRED credential — UNAUTHENTICATED.
#   3. LIVE ADOPTION: the successor JWT OVERWRITES the agent's credential
#      file (the operator's distribution motion — atomic, 0600). The agent's
#      watch-reconnect loop (2 s backoff) RE-READS THE FILE LAZILY on the
#      next open and ADOPTS the successor WITHOUT A RESTART: the stream
#      reopens and a second job COMPLETES.
#   4. REVOKE: the successor is then REVOKED — the teardown repeats and the
#      agent CANNOT re-establish identity (persistent UNAUTHENTICATED; no new
#      watch-open).
#
# Anti-spoofing strategy: the shared agent.log accumulates across scenarios,
# so waits measure DELTAS (counts rising above a pre-rotation baseline) rather
# than absolute presence. Deltas are additionally immune to the log file's
# prefix changing underneath the wait (observed 2026-11-08: the file shrunk
# BELOW a positional marker mid-wait while its content stayed coherent —
# cause undetermined, deltas sidestep it).
#
# Usage: scripts/e2e/24-agent-credential-rotation.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, jq.
# Shares the stack when launched by e2e.sh; boots its own otherwise.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-agent

API=http://127.0.0.1:8080
AGENT_NAME=agent-1
CRED_FILE="$WORK/agent-1.cred"

# wait_delta_gt <logfile> <pattern> <base> <what> <iterations>: wait until the
# TOTAL count of PATTERN in <logfile> rises ABOVE <base> (taken beforehand).
# Count-based (not positional): unaffected by the file's prefix mutating.
wait_delta_gt() {
  local lf="$1" pat="$2" base="$3" what="$4" n="$5" i c
  for ((i = 0; i < n; i++)); do
    c=$(grep -acF -- "$pat" "$lf" 2>/dev/null || true)
    [ "$c" -gt "$base" ] && return 0
    sleep 0.5
  done
  echo ">> timed out (${n}x0.5s) waiting for: $what (need >$base of '$pat')" >&2
  tail -n 40 "$lf" >&2
  exit 1
}

# wait_count_gt <logfile> <pattern> <min_count> <what> <iterations>: wait
# until the TOTAL count of PATTERN in <logfile> exceeds <min_count>.
wait_count_gt() {
  local lf="$1" pat="$2" min="$3" what="$4" n="$5" i c
  for ((i = 0; i < n; i++)); do
    c=$(grep -aFc -- "$pat" "$lf" 2>/dev/null || true)
    [ "$c" -gt "$min" ] && return 0
    sleep 0.5
  done
  echo ">> timed out (${n}x0.5s) waiting for: $what (need >$min of '$pat')" >&2
  tail -n 40 "$lf" >&2
  exit 1
}

# wait_job_done <job_id>: poll the API until the job is SUCCEEDED (status 3).
wait_job_done() {
  local id="$1" st i
  for i in $(seq 1 150); do
    st=$(curl -s "$API/api/jobs/$id" | jq -r '.status')
    [ "$st" = "3" ] && return 0
    sleep 0.2
  done
  echo ">> job $id did not reach succeeded (last status: $st)" >&2
  exit 1
}

# distribute <jwt> <file>: the operator's distribution motion — atomic
# overwrite (temp + rename), 0600.
distribute() {
  local jwt="$1" file="$2" tmp
  umask 077
  tmp="${file}.tmp.$$"
  printf '%s\n' "$jwt" > "$tmp"
  mv "$tmp" "$file"
  chmod 600 "$file"
}

# --- 1. baseline ---------------------------------------------------------------
log ">> [1/4] baseline job on the provisioned credential"
JOB1=$(e2e_submit_job "{
  \"name\": \"e2e-accred-baseline\",
  \"target_group\": \"default\",
  \"spec\": { \"steps\": [ {\"params\": {\"command\": {\"string\": \"sh\"}, \"args\": {\"strings\": [\"-c\", \"echo accredited baseline\"]}} } ] }
}")
e2e_wait_for_job "$JOB1" 'accredited baseline' 'baseline job output'
wait_job_done "$JOB1"

# The agent's ACTIVE credential id (the one the harness issued at boot).
ACTIVE_ID=$(curl -s "$API/api/agents/$AGENT_NAME/credentials" | jq -r \
  '[.[] | select(.replaced_by_id == null and .revoked_at == null)] | .[-1].id')
[ -n "$ACTIVE_ID" ] && [ "$ACTIVE_ID" != "null" ] \
  || { echo ">> could not find the agent's active credential" >&2; exit 1; }
log "   active credential: $ACTIVE_ID"

LAPEASED_BASE=$(grep -aFc 'credential lapsed; watch stream torn down' "$WORK/api.log" || true)
WATCH_BASE=$(grep -aFc 'api: agent watching' "$WORK/api.log" || true)
# Pre-rotation baselines on the agent log (deltas rise above THESE):
UA_BASE=$(grep -ac 'Unauthenticated' "$WORK/agent.log" || true)
REC_BASE=$(grep -acF -- '; reconnecting' "$WORK/agent.log" || true)

# --- 2. ROTATE: old row retired -> heartbeat denied, watch stream torn down ----
log ">> [2/4] rotating the credential (old row retired; expect denial + teardown)"
ROTATE_RESP=$(curl -sf -X POST "$API/api/agents/$AGENT_NAME/credentials/$ACTIVE_ID/rotate" \
  -H 'Content-Type: application/json' -d '{"description":"e2e rotation drill"}') \
  || { echo ">> rotate failed" >&2; exit 1; }
SUCCESSOR_JWT=$(printf '%s' "$ROTATE_RESP" | jq -r '.jwt')
SUCCESSOR_ID=$(printf '%s' "$ROTATE_RESP" | jq -r '.credential.id')
[ -n "$SUCCESSOR_JWT" ] && [ "$SUCCESSOR_JWT" != "null" ] \
  || { echo ">> rotate response carried no successor JWT" >&2; exit 1; }
log "   successor credential: $SUCCESSOR_ID"

# The OLD row is now RETIRED, chained to the successor (replaced_by_id).
OLD_CHAIN=$(curl -s "$API/api/agents/$AGENT_NAME/credentials" | jq -r \
  --arg id "$ACTIVE_ID" '.[] | select(.id == $id) | .replaced_by_id')
[ "$OLD_CHAIN" = "$SUCCESSOR_ID" ] \
  || { echo ">> old row not chained to the successor (replaced_by_id='$OLD_CHAIN')" >&2; exit 1; }

# The API tears the watch stream down within cache TTL + one heartbeat
# (nominal ≈ 25 s: the ≤15 s positive cache shields beats until it drains,
# then two consecutive failed beats exhaust the one-heartbeat grace). Under
# shared-stack load the positive cache can be HELD longer (lookup errors keep
# the last positive), so the generous bound is doubled.
wait_count_gt "$WORK/api.log" 'credential lapsed; watch stream torn down' "$LAPEASED_BASE" 'watch stream teardown (rotation)' 240

# The agent's heartbeat is DENIED (its credential's row is retired) — the
# denial count RISES above the pre-rotation baseline within the same bound.
wait_delta_gt "$WORK/agent.log" 'Unauthenticated' "$UA_BASE" 'heartbeat denied (retired credential)' 240

# The agent NOTICES the stream loss (its watch loop logs the reopen —
# "; reconnecting" covers both the remote-close and the open-error forms);
# its count rises above the baseline too.
wait_delta_gt "$WORK/agent.log" '; reconnecting' "$REC_BASE" 'agent noticed the stream loss' 120

# --- 3. LIVE ADOPTION: successor overwrites the file (no restart) --------------
log ">> [3/4] distributing the successor to the credential file (live adoption)"
distribute "$SUCCESSOR_JWT" "$CRED_FILE"

# The agent's watch-reconnect loop (2 s backoff) re-reads the file on the
# next open and ADOPTS the successor: a NEW watch-open appears on the API.
wait_count_gt "$WORK/api.log" 'api: agent watching' "$WATCH_BASE" 'agent watch stream reopened (successor adopted)' 60
log "   watch stream reopened on the successor (live adoption, no restart)"

# --- 4. a second job completes on the successor --------------------------------
log ">> [4/4] second job on the successor credential"
JOB2=$(e2e_submit_job "{
  \"name\": \"e2e-accred-second\",
  \"target_group\": \"default\",
  \"spec\": { \"steps\": [ {\"params\": {\"command\": {\"string\": \"sh\"}, \"args\": {\"strings\": [\"-c\", \"echo accredited second\"]}} } ] }
}")
e2e_wait_for_job "$JOB2" 'accredited second' 'second job output'
wait_job_done "$JOB2"

# The successor's last_seen_at ADVANCED (verified presentations bump it,
# throttled to ≤1/min): the successor row is fresh (no prior mark), so the
# first successful verify marks it promptly.
LAST_SEEN=null
for i in $(seq 1 60); do
  LAST_SEEN=$(curl -s "$API/api/agents/$AGENT_NAME/credentials" | jq -r \
    --arg id "$SUCCESSOR_ID" '.[] | select(.id == $id) | .last_seen_at // "null"')
  [ "$LAST_SEEN" != "null" ] && break
  sleep 0.5
done
[ "$LAST_SEEN" != "null" ] \
  || { echo ">> successor last_seen_at never populated (throttled bump missing)" >&2; exit 1; }
log "   successor last_seen_at: $LAST_SEEN"

# --- bonus: REVOKE the successor — the agent cannot re-establish identity ------
log ">> [bonus] revoking the successor (expect teardown + persistent denial)"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$API/api/agents/$AGENT_NAME/credentials/$SUCCESSOR_ID" \
  -H 'Content-Type: application/json' -d '{"reason":"e2e revocation drill"}')
[ "$CODE" = "200" ] || { echo ">> revoke returned $CODE, want 200" >&2; exit 1; }

# The teardown REPEATS (a second "credential lapsed" line)...
wait_count_gt "$WORK/api.log" 'credential lapsed; watch stream torn down' "$((LAPEASED_BASE + 1))" 'watch stream teardown (revocation)' 120

# ...and the agent keeps being DENIED — the denial count KEEPS RISING above
# the post-adoption baseline (it cannot re-establish identity without a valid
# credential).
UA_PRE_REVOKE=$(grep -ac 'Unauthenticated' "$WORK/agent.log" || true)
wait_delta_gt "$WORK/agent.log" 'Unauthenticated' "$UA_PRE_REVOKE" 'persistent denials after revocation' 60
DENIALS_NEW=$(($(grep -ac 'Unauthenticated' "$WORK/agent.log" || true) - UA_PRE_REVOKE))
log "   persistent denials after revocation: $DENIALS_NEW new"

# --- epilogue: RESTORE the agent (leave the shared stack usable) -------------
# The drills above end with EVERY credential revoked — on a STANDALONE stack
# that is harmless (it dies anyway), but on the SHARED stack the agent would
# sit permanently unauthenticated and poison every later scenario that needs
# it to fulfil a job (25's worker-spawn discovery would find no worker: the
# assignment nobody can accept never dispatches). Operator recovery motion:
# issue a fresh credential, distribute it (lazy re-read adopts it), prove the
# watch stream reopens and a job completes.
RESTORED_ID=$(e2e_issue_agent_credential "$AGENT_NAME" "$CRED_FILE.tmp")
[ -n "$RESTORED_ID" ] && [ "$RESTORED_ID" != "null" ] \
  || { echo ">> could not issue the restoration credential" >&2; exit 1; }
mv "$CRED_FILE.tmp" "$CRED_FILE"
chmod 600 "$CRED_FILE"
# Fresh baseline: phase 3's adoption already raised the watch-open count above
# WATCH_BASE, so comparing against the pre-rotation figure would fire
# instantly on the stale increment.
WATCH_PRE_RESTORE=$(grep -aFc 'api: agent watching' "$WORK/api.log" || true)
wait_count_gt "$WORK/api.log" 'api: agent watching' "$WATCH_PRE_RESTORE" 'restored agent watch stream' 60
JOB3=$(e2e_submit_job "{
  \"name\": \"e2e-accred-restored\",
  \"target_group\": \"default\",
  \"spec\": { \"steps\": [ {\"params\": {\"command\": {\"string\": \"sh\"}, \"args\": {\"strings\": [\"-c\", \"echo restored\"]}} } ] }
}")
e2e_wait_for_job "$JOB3" 'restored' 'restoration job output'
wait_job_done "$JOB3"
log "   agent restored (credential $RESTORED_ID; watch reopened; job completed)"

log ">> PASSED: agent credential rotation (live adoption) + revocation (F-30.3 S2/S3)"
