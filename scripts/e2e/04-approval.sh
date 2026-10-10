#!/usr/bin/env bash
#
# 04-approval.sh — F-13: approval gates.
#
# Boots the stack (with a worker) and verifies:
#   1. a job whose second step is an approval gate (type "approval") pauses in
#      the awaiting_approval state (status 8) until an authorized user
#      approves it via the HTTP API. The gate's message is a template rendered
#      against the job's condition context (here it references the job's
#      identity). Once approved, the job continues to its later steps and
#      succeeds; the decision (reason) is recorded on the job.
#   2. a job whose first step is an approval gate is rejected via the HTTP
#      API: the gate's decision is recorded as rejected, the job fails, and
#      its later steps never run.
#
# Usage: scripts/e2e/04-approval.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker

# --- verify an approval gate pauses the job until approved (F-13) ----------
log ">> submitting a job with an approval gate"
APPROVAL_BODY=$(cat <<EOF
{
  "name": "e2e-approval-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo approval: pre-gate step"]}}},
      {"type": "approval", "params": {"message": {"string": "release {{ .job.Name }} now"}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo approval: post-gate step"]}}}
    ]
  }
}
EOF
)
APPROVAL_JOB_ID=$(e2e_submit_job "$APPROVAL_BODY")
log ">> approval job id: $APPROVAL_JOB_ID"

# The worker runs the pre-gate step, then pauses at the approval step and
# reports the job awaiting_approval (status 8) with the rendered message.
log ">> waiting for the job to pause at the approval gate (status 8)"
AWAITING=$(e2e_wait_for_status "$APPROVAL_JOB_ID" 8 'job awaiting approval')
log "   $AWAITING"
# The gate's message is rendered against the job's condition context: the
# template `release {{ .job.Name }} now` becomes `release e2e-approval-job now`.
# Check only the rendered approval_message field (the spec still carries the
# raw template, so extracting the field is the only reliable check).
APPROVAL_MSG=$(printf '%s' "$AWAITING" | jq -r '.approval_message')
[ "$APPROVAL_MSG" = "release e2e-approval-job now" ] \
  || { echo ">> approval message was not rendered against the job identity (got: $APPROVAL_MSG)" >&2; exit 1; }

# Approve the gate (with a reason). The decision is recorded on the job and
# the job moves back to running.
log ">> approving the gate"
APPROVE_RESPONSE=$(curl -s -X POST "http://127.0.0.1:8080/api/jobs/$APPROVAL_JOB_ID/approve" \
  -H 'Content-Type: application/json' \
  -d '{"reason": "lgtm"}')
log "   $APPROVE_RESPONSE"

# The worker observes the approval on its next poll and continues to the
# post-gate step, which runs and the job succeeds.
log ">> waiting for the job to continue past the gate and succeed"
APPROVED_FINAL=$(e2e_wait_for_status "$APPROVAL_JOB_ID" 3 'job succeeded after approval')
log "   $APPROVED_FINAL"
# The post-gate step ran only because the gate was approved.
e2e_wait_for_job "$APPROVAL_JOB_ID" 'approval: post-gate step' 'post-gate step output'
# The decision is recorded on the job: approved, with the reason.
[ "$(printf '%s' "$APPROVED_FINAL" | jq -r '.approval_decision')" = "approved" ] \
  || { echo ">> approval decision was not recorded as approved" >&2; exit 1; }
[ "$(printf '%s' "$APPROVED_FINAL" | jq -r '.approval_reason')" = "lgtm" ] \
  || { echo ">> approval reason was not recorded" >&2; exit 1; }

# --- verify a rejected approval gate fails the job (F-13) ------------------
log ">> submitting a job to reject at its approval gate"
REJECT_BODY=$(cat <<EOF
{
  "name": "e2e-reject-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "approval", "params": {"message": {"string": "ship {{ .job.Name }}?"}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo reject: should not run"]}}}
    ]
  }
}
EOF
)
REJECT_JOB_ID=$(e2e_submit_job "$REJECT_BODY")
log ">> reject job id: $REJECT_JOB_ID"

# The worker pauses at the approval gate (status 8) with the rendered message.
log ">> waiting for the job to pause at the approval gate (status 8)"
REJECT_AWAITING=$(e2e_wait_for_status "$REJECT_JOB_ID" 8 'job awaiting approval')
[ "$(printf '%s' "$REJECT_AWAITING" | jq -r '.approval_message')" = "ship e2e-reject-job?" ] \
  || { echo ">> reject approval message was not rendered" >&2; exit 1; }

# Reject the gate (with a reason). The decision is recorded as rejected and
# the job moves to failed.
log ">> rejecting the gate"
REJECT_DECISION=$(curl -s -X POST "http://127.0.0.1:8080/api/jobs/$REJECT_JOB_ID/reject" \
  -H 'Content-Type: application/json' \
  -d '{"reason": "not yet"}')
log "   $REJECT_DECISION"

# The job fails (status 4): the worker observes the rejection on its next poll
# and fails the job, and the gate's rejection already moved it to failed.
log ">> waiting for the job to fail after the rejection"
REJECTED_FINAL=$(e2e_wait_for_status "$REJECT_JOB_ID" 4 'job failed after rejection')
log "   $REJECTED_FINAL"
# The decision is recorded on the job: rejected, with the reason.
[ "$(printf '%s' "$REJECTED_FINAL" | jq -r '.approval_decision')" = "rejected" ] \
  || { echo ">> approval decision was not recorded as rejected" >&2; exit 1; }
[ "$(printf '%s' "$REJECTED_FINAL" | jq -r '.approval_reason')" = "not yet" ] \
  || { echo ">> reject reason was not recorded" >&2; exit 1; }
# The post-gate step never ran (the job failed at the gate). Give the worker a
# moment to settle, then confirm its output is absent from the job log.
sleep 1
REJECT_LOG=$(curl -s "http://127.0.0.1:8080/api/jobs/$REJECT_JOB_ID/logs/job.log")
printf '%s' "$REJECT_LOG" | grep -q 'reject: should not run' \
  && { echo ">> post-gate step ran despite the rejection" >&2; exit 1; }

log ">> 04-approval PASSED"
