#!/usr/bin/env bash
#
# 09-concurrency.sh — F-16: concurrency keys serialize jobs that share a key.
#
# Boots the stack (with a worker) and submits two jobs that both carry the
# concurrency key "e2e-key". A key with no configured limit defaults to a
# limit of 1 — a lock — so the two jobs serialize on it:
#
#   1. the first job is admitted (it acquires the key) and runs to succeeded;
#   2. the second job, submitted while the first still holds the key, is held
#      pending and reports the key as the resource blocking it (the keys are
#      OR'd — a job is blocked while any one of its keys is held);
#   3. once the first job finishes and releases the key, the scheduler's
#      admission loop admits the second job and it runs to succeeded.
#
# This exercises the full path: the API parses the job's concurrency keys from
# the spec, the Database service owns the durable slot accounting, the
# scheduler's admission gate holds the contending job, and the leader-gated
# admission loop dispatches it once the slot frees.
#
# Usage: scripts/e2e/09-concurrency.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# Boot the stack (with a worker). The key "e2e-key" is a lock by default (a
# key with no configured limit defaults to a limit of 1), so the two jobs
# that carry it serialize on it.
e2e_boot_stack with-worker

# --- exercise the concurrency-limit CRUD endpoints (F-16) -------------------
# The limits are stored as regex-based rules in the database (not
# configuration) and managed at runtime via the API. Create a key limit rule,
# verify it is listed, then delete it. It uses a key ("e2e-key-crud") that the
# jobs below do not use, so it does not affect the key-lock test.
log ">> creating a concurrency limit rule via the API"
RULE=$(curl -s -X POST http://127.0.0.1:8080/api/concurrency-limits \
  -H 'Content-Type: application/json' \
  -d '{"kind":"key","pattern":"^e2e-key-crud$","limit":2}')
RULE_ID=$(printf '%s' "$RULE" | jq -r '.id')
[ -n "$RULE_ID" ] && [ "$RULE_ID" != "null" ] \
  || { echo ">> could not create the concurrency limit rule: $RULE" >&2; exit 1; }
log "   created rule id: $RULE_ID"

log ">> verifying the rule is listed"
LISTED=$(curl -s "http://127.0.0.1:8080/api/concurrency-limits")
printf '%s' "$LISTED" | jq -e --arg id "$RULE_ID" \
  '.[] | select(.id == ($id | tonumber)) | .limit == 2' >/dev/null 2>&1 \
  || { echo ">> the created rule is not listed: $LISTED" >&2; exit 1; }

log ">> deleting the concurrency limit rule"
curl -s -X DELETE "http://127.0.0.1:8080/api/concurrency-limits/$RULE_ID" >/dev/null \
  || { echo ">> could not delete the concurrency limit rule" >&2; exit 1; }
LISTED_AFTER=$(curl -s "http://127.0.0.1:8080/api/concurrency-limits")
printf '%s' "$LISTED_AFTER" | jq -e --arg id "$RULE_ID" \
  '[.[] | select(.id == ($id | tonumber))] | length == 0' >/dev/null 2>&1 \
  || { echo ">> the rule is still listed after delete: $LISTED_AFTER" >&2; exit 1; }
log "   concurrency-limit CRUD OK"

# --- submit the first job (it acquires the key and runs) --------------------
log ">> submitting the first job (holds the concurrency key)"
JOB_A_BODY=$(cat <<EOF
{
  "name": "e2e-conc-a",
  "target_group": "default",
  "spec": {
    "concurrency_keys": ["e2e-key"],
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo a: holding e2e-key; sleep 2; echo a: done"]}}}
    ]
  }
}
EOF
)
JOB_A=$(e2e_submit_job "$JOB_A_BODY")
log ">> job A id: $JOB_A"

# Wait until job A is running: it has been admitted (acquiring the key) and
# dispatched to the worker. While it runs it holds the key.
log ">> waiting for job A to be admitted and running"
A=""
for i in $(seq 1 100); do
  A=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_A")
  if [ "$(printf '%s' "$A" | jq -r '.status')" = "2" ]; then
    break
  fi
  sleep 0.2
done
[ "$(printf '%s' "$A" | jq -r '.status')" = "2" ] \
  || { echo ">> job A did not reach running (it was not admitted): $A" >&2; exit 1; }

# --- submit the second job while A holds the key ----------------------------
log ">> submitting the second job (shares the key, must be held pending)"
JOB_B_BODY=$(cat <<EOF
{
  "name": "e2e-conc-b",
  "target_group": "default",
  "spec": {
    "concurrency_keys": ["e2e-key"],
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo b: acquired e2e-key; echo b: done"]}}}
    ]
  }
}
EOF
)
JOB_B=$(e2e_submit_job "$JOB_B_BODY")
log ">> job B id: $JOB_B"

# While A runs (holding the key), B must be held pending and report the key as
# the resource blocking it. Poll the queue until B is present and blocked by
# the key (the queue is the UI's view of which key is holding a job up).
log ">> waiting for the queue to show job B blocked by the key"
QUEUE=""
for i in $(seq 1 100); do
  QUEUE=$(curl -s "http://127.0.0.1:8080/api/queue")
  B_ENTRY=$(printf '%s' "$QUEUE" | jq -c --argjson id "$JOB_B" \
    '.pending[]? | select(.id == $id)')
  if [ -n "$B_ENTRY" ] && [ "$B_ENTRY" != "null" ] \
    && printf '%s' "$B_ENTRY" | jq -e '.blocked_by | index("e2e-key")' >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
log "   queue: $QUEUE"
[ -n "$B_ENTRY" ] && [ "$B_ENTRY" != "null" ] \
  || { echo ">> job B not found in the pending queue" >&2; exit 1; }
printf '%s' "$B_ENTRY" | jq -e '.blocked_by | index("e2e-key")' >/dev/null 2>&1 \
  || { echo ">> job B is not blocked by e2e-key: $B_ENTRY" >&2; exit 1; }

# --- A finishes and releases the key; the admission loop admits B -----------
log ">> waiting for job A to finish (releasing the key)"
A_FINAL=""
for i in $(seq 1 100); do
  A_FINAL=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_A")
  if [ "$(printf '%s' "$A_FINAL" | jq -r '.status')" = "3" ]; then
    break
  fi
  sleep 0.2
done
[ "$(printf '%s' "$A_FINAL" | jq -r '.status')" = "3" ] \
  || { echo ">> job A did not reach succeeded: $A_FINAL" >&2; exit 1; }

# Once A releases the key, the scheduler's admission loop (5s tick) admits B
# and dispatches it; it then runs to succeeded. Poll until B is succeeded.
log ">> waiting for the admission loop to admit and run job B"
B_FINAL=""
for i in $(seq 1 200); do
  B_FINAL=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_B")
  if [ "$(printf '%s' "$B_FINAL" | jq -r '.status')" = "3" ]; then
    break
  fi
  sleep 0.2
done
log "   $B_FINAL"
[ "$(printf '%s' "$B_FINAL" | jq -r '.status')" = "3" ] \
  || { echo ">> job B did not reach succeeded after the key freed" >&2; exit 1; }

log ">> 09-concurrency PASSED"
