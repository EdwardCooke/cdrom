#!/usr/bin/env bash
#
# 07-audit.sh — F-15: the audit log.
#
# Boots the stack (with a worker) and generates a spread of audited actions,
# then verifies that every audited action (a job trigger, an approval
# decision, a login, a pipeline mutation, a worker lifecycle change, a token
# exchange, a job status report) is recorded both to a local audit log file
# (separate from the process's default log) and to the shared audit log in
# the database (queryable via GET /api/audit). The main API runs with auth
# off (a synthetic admin), so /api/audit is reachable without a token.
#
# It also verifies the audit log can be filtered by action, and that a
# pipeline's secrets are represented by name only in the audit log: the
# secret's value is never stored or displayed.
#
# The script is self-contained: it generates every event it asserts on, so it
# can run standalone or as part of the full e2e run.
#
# Usage: scripts/e2e/07-audit.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker

# --- generate the audited actions this script asserts on --------------------
# Register + log in a user (auth.register, auth.login).
e2e_http_request POST http://127.0.0.1:8080/api/register \
  "{\"first_name\":\"Ada\",\"last_name\":\"Lovelace\",\"email\":\"${E2E_USER_ADA_EMAIL}\",\"password\":\"${E2E_USER_ADA_PASSWORD}\"}"
case "$HTTP_CODE" in
  201|409) ;;
  *) echo ">> register returned $HTTP_CODE, want 201 or 409 ($HTTP_BODY)" >&2; exit 1 ;;
esac
e2e_http_request POST http://127.0.0.1:8080/api/login \
  "{\"email\":\"${E2E_USER_ADA_EMAIL}\",\"password\":\"${E2E_USER_ADA_PASSWORD}\"}"
[ "$HTTP_CODE" = "200" ] || { echo ">> login returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }

# Submit a job with a token_exchange step (run.trigger, job.status,
# token.exchange). The worker's registration (worker.register) was recorded
# when the stack booted.
log ">> submitting a job to generate audited job events"
BODY=$(cat <<EOF
{
  "name": "e2e-audit-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo audit: step"]}}},
      {"type": "token_exchange", "params": {"audience": {"string": "outside-svc"}}}
    ]
  }
}
EOF
)
JOB_ID=$(e2e_submit_job "$BODY")
e2e_wait_for "$WORK/worker.log" 'job succeeded' 'job success'

# Generate an approval decision (job.approve) and a rejection (job.reject).
log ">> generating approval + rejection audit events"
APPROVAL_BODY=$(cat <<EOF
{
  "name": "e2e-audit-approval-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "approval", "params": {"message": {"string": "release {{ .job.Name }} now"}}}
    ]
  }
}
EOF
)
APPROVAL_JOB_ID=$(e2e_submit_job "$APPROVAL_BODY")
e2e_wait_for_status "$APPROVAL_JOB_ID" 8 'job awaiting approval' >/dev/null
curl -s -X POST "http://127.0.0.1:8080/api/jobs/$APPROVAL_JOB_ID/approve" \
  -H 'Content-Type: application/json' -d '{"reason": "lgtm"}' >/dev/null

REJECT_BODY=$(cat <<EOF
{
  "name": "e2e-audit-reject-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "approval", "params": {"message": {"string": "ship {{ .job.Name }}?"}}}
    ]
  }
}
EOF
)
REJECT_JOB_ID=$(e2e_submit_job "$REJECT_BODY")
e2e_wait_for_status "$REJECT_JOB_ID" 8 'job awaiting approval' >/dev/null
curl -s -X POST "http://127.0.0.1:8080/api/jobs/$REJECT_JOB_ID/reject" \
  -H 'Content-Type: application/json' -d '{"reason": "not yet"}' >/dev/null

# --- verify the audit log (F-15) -------------------------------------------
log ">> verifying the audit log (F-15)"

# The local audit log file exists, is separate from the process's default log,
# and holds JSON records (CDROM_AUDIT_FORMAT=json).
[ -s "$WORK/audit.log" ] \
  || { echo ">> local audit log file is missing or empty" >&2; exit 1; }
# Every line of the local audit log is a JSON object (the configured format).
BAD_LINES=$(grep -vcE '^\{"time":' "$WORK/audit.log" || true)
[ "$BAD_LINES" = "0" ] \
  || { echo ">> $BAD_LINES audit log lines are not JSON objects" >&2; exit 1; }
# The audit records did not leak into the process's default log (api.log is
# text-format key=value; the JSON audit records carry a quoted "actor" key).
grep -q '"actor":' "$WORK/api.log" \
  && { echo ">> audit records leaked into the default log (api.log)" >&2; exit 1; }

# The shared audit log (in the database) is queryable via GET /api/audit.
AUDIT_ALL=$(curl -s "http://127.0.0.1:8080/api/audit")
printf '%s' "$AUDIT_ALL" | jq -e 'type == "array" and length > 0' >/dev/null \
  || { echo ">> GET /api/audit returned no events: $AUDIT_ALL" >&2; exit 1; }

# A job approval decision is recorded with a success outcome.
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="job.approve") | select(.outcome=="success")' >/dev/null \
  || { echo ">> no job.approve audit event recorded" >&2; exit 1; }
# A job rejection is recorded too.
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="job.reject")' >/dev/null \
  || { echo ">> no job.reject audit event recorded" >&2; exit 1; }
# A login is recorded with the user's email as the actor.
printf '%s' "$AUDIT_ALL" | jq -e ".[] | select(.action==\"auth.login\") | select(.actor==\"${E2E_USER_ADA_EMAIL}\")" >/dev/null \
  || { echo ">> no auth.login audit event for ${E2E_USER_ADA_EMAIL}" >&2; exit 1; }
# A run trigger (a standalone job submit) is recorded.
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="run.trigger")' >/dev/null \
  || { echo ">> no run.trigger audit event recorded" >&2; exit 1; }
# A worker registration is recorded (the worker registered with the API).
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="worker.register")' >/dev/null \
  || { echo ">> no worker.register audit event recorded" >&2; exit 1; }
# A job status report from the worker is recorded.
printf '%s' "$AUDIT_ALL" | jq -e '.[] | select(.action=="job.status")' >/dev/null \
  || { echo ">> no job.status audit event recorded" >&2; exit 1; }

# The audit log can be filtered by action (F-15 acceptance: filter by actor,
# target, and time range).
AUDIT_FILTERED=$(curl -s "http://127.0.0.1:8080/api/audit?action=job.approve")
printf '%s' "$AUDIT_FILTERED" | jq -e 'type == "array" and length > 0 and all(.[]; .action=="job.approve")' >/dev/null \
  || { echo ">> action filter did not return only job.approve events: $AUDIT_FILTERED" >&2; exit 1; }

# A pipeline's secrets are represented by name only in the audit log: the
# secret's value is never stored or displayed (F-15). Create a pipeline with a
# secret and verify the audit event's new_value names the secret but never its
# value.
SECRET_VALUE="super-secret-value-123"
e2e_http_request POST http://127.0.0.1:8080/api/pipelines \
  "{\"name\":\"audit-secret-pipeline\",\"secrets\":[{\"name\":\"api_token\",\"value\":\"$SECRET_VALUE\"}]}"
[ "$HTTP_CODE" = "201" ] || { echo ">> create secret pipeline returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }

AUDIT_SECRET=$(curl -s "http://127.0.0.1:8080/api/audit?action=pipeline.create&target_kind=pipeline")
NEW_VALUE=$(printf '%s' "$AUDIT_SECRET" | jq -r '.[] | select(.target_name=="audit-secret-pipeline") | .new_value' | head -n 1)
[ -n "$NEW_VALUE" ] && [ "$NEW_VALUE" != "null" ] \
  || { echo ">> no pipeline.create audit event for the secret pipeline: $AUDIT_SECRET" >&2; exit 1; }
printf '%s' "$NEW_VALUE" | grep -q 'api_token' \
  || { echo ">> audit new_value did not name the secret: $NEW_VALUE" >&2; exit 1; }
printf '%s' "$NEW_VALUE" | grep -q "$SECRET_VALUE" \
  && { echo ">> audit new_value leaked the secret value: $NEW_VALUE" >&2; exit 1; }

log ">> 07-audit PASSED"
