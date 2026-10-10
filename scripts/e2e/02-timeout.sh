#!/usr/bin/env bash
#
# 02-timeout.sh — F-03: a job-level timeout terminates the job as timed_out.
#
# Boots the stack (with a worker) and submits a job whose single step sleeps
# well past the job's declared timeout. The worker terminates the sleeping
# step at the job's deadline and reports the job as timed_out (status 6), not
# failed. The step declares no per-step timeout, so only the job-level
# timeout applies.
#
# Usage: scripts/e2e/02-timeout.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker

# --- submit a job that exceeds its job-level timeout ------------------------
log ">> submitting a job that exceeds its job-level timeout"
BODY=$(cat <<EOF
{
  "name": "e2e-timeout-job",
  "target_group": "default",
  "spec": {
    "timeout": "2s",
    "steps": [
      {"params": {"command": {"string": "sleep"}, "args": {"strings": ["30"]}}}
    ]
  }
}
EOF
)
JOB_ID=$(e2e_submit_job "$BODY")
log ">> timeout job id: $JOB_ID"

# The worker terminates the sleeping step at the job's 2s deadline and reports
# timed_out. Wait for the worker to log the timeout.
e2e_wait_for "$WORK/worker.log" 'job timed out' 'worker reports the job timed out'

# Verify the API persisted the job as timed_out (status 6).
FINAL=""
for i in $(seq 1 100); do
  FINAL=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID")
  if [ "$(printf '%s' "$FINAL" | jq -r '.status')" = "6" ]; then
    break
  fi
  sleep 0.2
done
log "   $FINAL"
[ "$(printf '%s' "$FINAL" | jq -r '.status')" = "6" ] \
  || { echo ">> timeout job did not reach status=timed_out" >&2; exit 1; }

log ">> 02-timeout PASSED"
