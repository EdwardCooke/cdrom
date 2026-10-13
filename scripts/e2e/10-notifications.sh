#!/usr/bin/env bash
#
# 10-notifications.sh — notifications end-to-end test (F-18).
#
# Boots the full local stack (db, scheduler, artifacts, idp, api, worker) plus
# the F-18 notification sinks (a fake SMTP server and a fake webhook HTTP
# server, started by lib.sh's e2e_boot_stack) and a second worker in the
# "default" group (so a fan-out job runs on two workers, which is what the
# `once` assertion needs). The API is configured to send email to the SMTP
# sink (plaintext, STARTTLS disabled); a webhook step's url points at the
# webhook sink.
#
# It verifies, end to end, that a notification step's transmission happens on
# the API (the target only renders templates and calls the API):
#
#   1. an `email` step sends an SMTP message from the API's configured SMTP
#      server; the body's content_type selects plain / html / markdown
#      (markdown, the default, is rendered to HTML by the API).
#   2. a `webhook` step performs an HTTP request from the API to the templated
#      url, with the step's headers and body (markdown rendered to HTML, or
#      plain sent as-is).
#   3. the webhook's url/headers/body are templated against the step's
#      condition context (the job's identity).
#   4. a notification step with the standard `once` field (F-01) sends at most
#      once per job attempt: a fan-out job (one execution per worker) sends
#      exactly one email, not one per worker.
#   5. a failed send (a webhook non-2xx) fails the job, unless the step sets
#      ignore_failed (F-06).
#
# Usage: scripts/e2e/10-notifications.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, jq,
# and python3 (for the notification sinks). The script always tears down the
# processes it started on exit.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker

# A second worker in the "default" group, so a fan-out job runs on two workers
# (the `once` assertion needs more than one target to prove it dedupes). It is
# started by this script (tracked in PIDS) and torn down on exit.
e2e_start worker-b "$WORK/worker-b.log" 'registered' CDROM_WORKER_NAME=worker-b CDROM_WORKER_GROUP=default CDROM_LOG_FORMAT=text -- "$BIN/worker"
e2e_wait_for "$WORK/api.log" 'worker=worker-b' 'second worker watching on the api'

# --- 1. email step: markdown (default) rendered to HTML by the API ----------
log ">> [1/5] email step (markdown -> HTML)"
BODY=$(cat <<EOF
{
  "name": "e2e-email-md",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "email", "params": {
        "to": {"strings": ["e2e-recipient@example.com"]},
        "subject": {"string": "E2E EMAIL MD"},
        "body": {"string": "Hello **world**"}
      }}
    ]
  }
}
EOF
)
JOB_ID=$(e2e_submit_job "$BODY")
log ">> job id: $JOB_ID"
e2e_wait_for_status "$JOB_ID" 3 'email job succeeded' >/dev/null
e2e_wait_for "$WORK/api.log" 'api: sent email' 'api sent the email'
# The SMTP sink recorded the message: the subject, an HTML content type
# (markdown was rendered to HTML by the API), and the rendered <strong> tag
# (the markdown **world** became HTML, not left as markdown).
e2e_wait_for "$WORK/smtp.log" 'Subject: E2E EMAIL MD' 'smtp sink received the email'
grep -q 'Subject: E2E EMAIL MD' "$WORK/smtp.log" \
  || { echo ">> email subject missing from smtp sink" >&2; exit 1; }
grep -q 'Content-Type: text/html' "$WORK/smtp.log" \
  || { echo ">> email was not sent as HTML (markdown not rendered)" >&2; exit 1; }
grep -q '<strong>' "$WORK/smtp.log" \
  || { echo ">> email body was not rendered from markdown to HTML" >&2; exit 1; }

# --- 2. webhook step: markdown rendered to HTML, custom header --------------
log ">> [2/5] webhook step (markdown -> HTML, custom header)"
BODY=$(cat <<EOF
{
  "name": "e2e-webhook-md",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "webhook", "params": {
        "url": {"string": "http://127.0.0.1:${E2E_WEBHOOK_PORT}/md"},
        "content_type": {"string": "markdown"},
        "headers": {"strings": ["X-E2E: md-test"]},
        "body": {"string": "Web **hook**"}
      }}
    ]
  }
}
EOF
)
JOB_ID=$(e2e_submit_job "$BODY")
log ">> job id: $JOB_ID"
e2e_wait_for_status "$JOB_ID" 3 'webhook job succeeded' >/dev/null
e2e_wait_for "$WORK/api.log" 'api: sent webhook' 'api sent the webhook'
# The webhook sink recorded the request: the path, the custom header, and the
# body rendered from markdown to HTML.
e2e_wait_for "$WORK/webhook.log" 'PATH: /md' 'webhook sink received the request'
grep -q 'PATH: /md' "$WORK/webhook.log" \
  || { echo ">> webhook path missing from sink" >&2; exit 1; }
# The custom header is matched case-insensitively: Go's HTTP client canonicalizes
# the header name (X-E2E -> X-E2e) before sending, and the sink records the
# canonical form, so the assertion must not depend on the exact casing.
grep -qi 'x-e2e: md-test' "$WORK/webhook.log" \
  || { echo ">> webhook custom header missing from sink" >&2; exit 1; }
grep -q '<strong>' "$WORK/webhook.log" \
  || { echo ">> webhook body was not rendered from markdown to HTML" >&2; exit 1; }

# --- 3. webhook step: plain body, fields templated against the job ----------
log ">> [3/5] webhook step (plain, templated url/headers/body)"
BODY=$(cat <<EOF
{
  "name": "e2e-webhook-plain",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "webhook", "params": {
        "url": {"string": "http://127.0.0.1:${E2E_WEBHOOK_PORT}/plain?job={{ .job.ID }}"},
        "content_type": {"string": "plain"},
        "headers": {"strings": ["X-Name: {{ .job.Name }}"]},
        "body": {"string": "plain body {{ .job.Name }}"}
      }}
    ]
  }
}
EOF
)
JOB_ID=$(e2e_submit_job "$BODY")
log ">> job id: $JOB_ID"
e2e_wait_for_status "$JOB_ID" 3 'templated webhook job succeeded' >/dev/null
# The url's {{ .job.ID }} rendered to the job's numeric id; the header and
# body's {{ .job.Name }} rendered to the job's name. The plain body is sent
# as-is (no markdown rendering).
e2e_wait_for "$WORK/webhook.log" "PATH: /plain?job=${JOB_ID}" 'templated webhook url'
grep -q "PATH: /plain?job=${JOB_ID}" "$WORK/webhook.log" \
  || { echo ">> templated webhook url (job id) missing from sink" >&2; exit 1; }
grep -q 'X-Name: e2e-webhook-plain' "$WORK/webhook.log" \
  || { echo ">> templated webhook header (job name) missing from sink" >&2; exit 1; }
grep -q 'plain body e2e-webhook-plain' "$WORK/webhook.log" \
  || { echo ">> templated webhook body (job name) missing from sink" >&2; exit 1; }

# --- 4. `once` (F-01): a fan-out job sends at most one email ----------------
log ">> [4/5] once: fan-out job sends exactly one email"
# Both workers run this job; the email step has `once` set, so only the first
# worker to reach it claims the slot and sends, the other skips it. Exactly one
# email with this subject must reach the SMTP sink.
BODY=$(cat <<EOF
{
  "name": "e2e-email-once",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "email", "once": true, "params": {
        "to": {"strings": ["e2e-recipient@example.com"]},
        "subject": {"string": "E2E ONCE"},
        "body": {"string": "sent once"}
      }}
    ]
  }
}
EOF
)
JOB_ID=$(e2e_submit_job "$BODY")
log ">> job id: $JOB_ID"
e2e_wait_for_status "$JOB_ID" 3 'once job succeeded' >/dev/null
e2e_wait_for "$WORK/smtp.log" 'Subject: E2E ONCE' 'smtp sink received the once email'
# Give both workers a moment to finish (one sends, the other skips), then count
# the messages with this subject: it must be exactly one.
sleep 1
ONCE_COUNT=$(grep -c 'Subject: E2E ONCE' "$WORK/smtp.log" || true)
log "   once emails sent: $ONCE_COUNT"
[ "$ONCE_COUNT" = "1" ] \
  || { echo ">> expected exactly 1 'E2E ONCE' email (once dedupe), got $ONCE_COUNT" >&2; exit 1; }

# --- 5. failure semantics: a failed send fails the job unless ignore_failed --
log ">> [5/5] failure semantics (webhook non-2xx)"
# A webhook to the failing sink (returns 500) fails the step, and thus the job.
BODY=$(cat <<EOF
{
  "name": "e2e-webhook-fail",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "webhook", "params": {
        "url": {"string": "http://127.0.0.1:${E2E_WEBHOOK_FAIL_PORT}/fail"},
        "content_type": {"string": "plain"},
        "body": {"string": "will get a 500"}
      }}
    ]
  }
}
EOF
)
JOB_ID=$(e2e_submit_job "$BODY")
log ">> job id: $JOB_ID (expect failed)"
e2e_wait_for_status "$JOB_ID" 4 'failing webhook job failed' >/dev/null

# The same webhook with ignore_failed set: the step fails but the job continues
# to succeeded.
BODY=$(cat <<EOF
{
  "name": "e2e-webhook-fail-ignored",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "webhook", "ignore_failed": true, "params": {
        "url": {"string": "http://127.0.0.1:${E2E_WEBHOOK_FAIL_PORT}/fail"},
        "content_type": {"string": "plain"},
        "body": {"string": "will get a 500 but is ignored"}
      }}
    ]
  }
}
EOF
)
JOB_ID=$(e2e_submit_job "$BODY")
log ">> job id: $JOB_ID (expect succeeded via ignore_failed)"
e2e_wait_for_status "$JOB_ID" 3 'ignore_failed webhook job succeeded' >/dev/null

log ">> 10-notifications PASSED: email and webhook notifications are sent from the API (markdown rendered to HTML, plain sent as-is, fields templated against the condition context), a once notification step sends at most once per job attempt in a fan-out job, and a failed send fails the job unless ignore_failed is set"
