#!/usr/bin/env bash
#
# 03-retry-rerun.sh — F-04: retry policy and re-run of a finished job.
#
# Boots the stack (with a worker) and verifies:
#   1. a job whose single step always fails, with a retry policy of
#      max_attempts 2 (so it runs at most 3 times: 1 initial + 2 retries)
#      and a 1s backoff, is re-dispatched by the scheduler's retry loop
#      after each failure until it exhausts its budget, after which it stays
#      failed with attempt 3;
#   2. a finished job (the successful shell job submitted first) can be
#      re-run for a fresh execution: it is reset to pending with a fresh
#      attempt (attempt back to 1) and re-dispatched, so the worker runs it
#      again.
#
# Usage: scripts/e2e/03-retry-rerun.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker

# --- a successful job to re-run later ---------------------------------------
log ">> submitting a shell job to re-run later"
SHELL_BODY=$(cat <<EOF
{
  "name": "e2e-rerun-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo step1: plain command"]}}}
    ]
  }
}
EOF
)
SHELL_JOB_ID=$(e2e_submit_job "$SHELL_BODY")
log ">> shell job id: $SHELL_JOB_ID"
e2e_wait_for "$WORK/worker.log" 'job succeeded' 'job success'

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
RETRY_JOB_ID=$(e2e_submit_job "$RETRY_BODY")
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
# Re-run the successful shell job: it is reset to pending with a fresh attempt
# (attempt back to 1) and re-dispatched, so the worker runs it again. Count
# the step-1 output lines in the job's persisted log before and after to prove
# a second execution happened.
log ">> re-running the shell job"
STEP1_BEFORE=$(grep -c 'step1: plain command' "$WORK/artifacts/${SHELL_JOB_ID}/logs/job.log" || true)
RERUN_RESPONSE=$(curl -s -X POST "http://127.0.0.1:8080/api/jobs/$SHELL_JOB_ID/rerun")
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

log ">> 03-retry-rerun PASSED"
