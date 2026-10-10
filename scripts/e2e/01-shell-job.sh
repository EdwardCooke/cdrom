#!/usr/bin/env bash
#
# 01-shell-job.sh — the core end-to-end smoke test.
#
# Boots the full local stack (db, scheduler, artifacts, idp, api, worker)
# against a throwaway SQLite database, submits a multi-step shell job to the
# worker group, and verifies the job runs to completion, is persisted as
# succeeded, and its step output was streamed to the API, persisted to the
# artifacts service, and is retrievable over HTTP (F-02). Exercises the full
# path:
#
#   submit (HTTP) -> api -> scheduler -> db
#        -> api dispatch -> worker WatchJobs stream -> worker executes steps
#        -> step output streamed to api (StreamJobLogs) -> persisted to
#           artifacts (step-<n>.log + job.log) -> retrievable via
#           GET /api/jobs/{id}/logs[/{name}]
#        -> status reported back -> persisted as succeeded
#
# The job spec covers the JobSpec features: a plain command, a per-step env
# var, a per-step workdir, and an explicit step-type + `shell` override. It
# also exercises the token_exchange step handler (a step that asks the API,
# via the worker's TokenExchange, for a new job token for a different
# audience and hands it to a later step as a step output; the API mints it
# from the local IdP).
#
# Usage: scripts/e2e/01-shell-job.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker

# --- submit a multi-step shell job to the worker group ---------------------
# The heredoc is unquoted so $WORKDIR expands to the real path; the shell
# snippets ($MY_VAR, $(pwd)) are escaped so they reach the worker verbatim.
# Step 4 names its step type explicitly ("shell") and uses the `shell`
# override: the worker runs `<shell> <args> <command>` (sh -c "echo step4:
# ...") instead of executing command directly. This proves the step-type
# discriminator is honored end-to-end (a step's type selects its handler).
log ">> submitting worker job"
BODY=$(cat <<EOF
{
  "name": "e2e-shell-job",
  "target_group": "default",
  "spec": {
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo step1: plain command"]}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo step2: env=\$MY_VAR"]}}, "env": {"MY_VAR": "from-spec"}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo step3: pwd=\$(pwd)"]}}, "workdir": "$WORKDIR"},
      {"type": "shell", "params": {"shell": {"string": "sh"}, "args": {"strings": ["-c"]}, "command": {"string": "echo step4: shell override"}}},
      {"type": "token_exchange", "params": {"audience": {"string": "outside-svc"}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo step6: token exchange flowed to a later step"]}}, "condition": "{{ if (index .steps 4).Outputs.token }}true{{ else }}false{{ end }}"}
    ]
  }
}
EOF
)
JOB_ID=$(e2e_submit_job "$BODY")
log ">> job id: $JOB_ID"

# --- verify the worker executed every step ---------------------------------
log ">> waiting for the worker to execute the job"
e2e_wait_for_job "$JOB_ID" 'step1: plain command' 'step 1 output'
e2e_wait_for_job "$JOB_ID" 'step2: env=from-spec' 'step 2 env var'
e2e_wait_for_job "$JOB_ID" "step3: pwd=$WORKDIR" 'step 3 workdir'
e2e_wait_for_job "$JOB_ID" 'step4: shell override' 'step 4 shell override'
# The token_exchange step (step 5) asks the API for a new job token for
# audience outside-svc and writes it to a "token" file in its per-step output
# directory (the executor reads every file there back as a step output); the
# worker logs the exchange.
e2e_wait_for "$WORK/worker.log" 'executor: exchanged job token' 'worker exchanged the job token'
# Step 6's condition reads the exchanged token from step 5's outputs; it runs
# only if the token was captured and flowed to the condition context.
e2e_wait_for_job "$JOB_ID" 'step6: token exchange flowed to a later step' 'step 6 consumed the exchanged token'
e2e_wait_for "$WORK/worker.log" 'job succeeded' 'job success'

# --- verify the API persisted the job as succeeded -------------------------
log ">> waiting for persisted status=succeeded (3)"
FINAL=""
for i in $(seq 1 100); do
  FINAL=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID")
  if [ "$(printf '%s' "$FINAL" | jq -r '.status')" = "3" ]; then
    break
  fi
  sleep 0.2
done
log "   $FINAL"
[ "$(printf '%s' "$FINAL" | jq -r '.status')" = "3" ] \
  || { echo ">> job did not reach status=succeeded" >&2; exit 1; }

# --- verify the token exchange went through the API and IdP ---------------
# The token_exchange step asked the API (ExchangeJobToken) for a new job token
# for audience outside-svc; the API minted it from the IdP. Verify both the
# API and the IdP recorded the exchange for the outside-svc audience.
log ">> verifying the token exchange (API + IdP)"
e2e_wait_for "$WORK/api.log" 'api: job token exchanged' 'api exchanged the job token'
e2e_wait_for "$WORK/idp.log" 'idp: minted job token' 'idp minted a job token'
grep -q 'audience=outside-svc' "$WORK/api.log" \
  || { echo ">> api did not exchange a token for audience outside-svc" >&2; exit 1; }
grep -q 'audience=outside-svc' "$WORK/idp.log" \
  || { echo ">> idp did not mint a token for audience outside-svc" >&2; exit 1; }

# --- verify job logs were streamed, persisted, and are retrievable (F-02) ---
# The API persists streamed output to the artifacts service (one file per step
# plus a combined job.log) and exposes it over HTTP. The worker reports
# "succeeded" before the API has necessarily finished persisting every chunk,
# so poll until the logs are complete.
log ">> verifying job logs (F-02)"

# Poll the log listing until the step logs plus job.log are present. The
# token_exchange step (the 5th step, index 4) produces no output, so it has no
# step-4.log; the other steps each have one (step-0..3 and step-5).
LOG_LISTING=""
for i in $(seq 1 100); do
  LOG_LISTING=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID/logs")
  if printf '%s' "$LOG_LISTING" | grep -q 'step-0.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-1.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-2.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-3.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'step-5.log' \
    && printf '%s' "$LOG_LISTING" | grep -q 'job.log'; then
    break
  fi
  sleep 0.2
done
log "   log listing: $LOG_LISTING"
printf '%s' "$LOG_LISTING" | grep -q 'step-0.log' \
  || { echo ">> step-0.log missing from log listing" >&2; exit 1; }
printf '%s' "$LOG_LISTING" | grep -q 'step-5.log' \
  || { echo ">> step-5.log missing from log listing" >&2; exit 1; }
printf '%s' "$LOG_LISTING" | grep -q 'job.log' \
  || { echo ">> job.log missing from log listing" >&2; exit 1; }

# Poll the combined job.log until it contains every step's output line.
JOB_LOG=""
for i in $(seq 1 100); do
  JOB_LOG=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID/logs/job.log")
  if printf '%s' "$JOB_LOG" | grep -q 'step1: plain command' \
    && printf '%s' "$JOB_LOG" | grep -q 'step2: env=from-spec' \
    && printf '%s' "$JOB_LOG" | grep -q "step3: pwd=$WORKDIR" \
    && printf '%s' "$JOB_LOG" | grep -q 'step4: shell override' \
    && printf '%s' "$JOB_LOG" | grep -q 'step6: token exchange flowed to a later step'; then
    break
  fi
  sleep 0.2
done
log "   job.log contents:"
printf '%s\n' "$JOB_LOG" | sed 's/^/     /'
printf '%s' "$JOB_LOG" | grep -q 'step1: plain command' \
  || { echo ">> step1 output missing from job.log" >&2; exit 1; }
printf '%s' "$JOB_LOG" | grep -q 'step2: env=from-spec' \
  || { echo ">> step2 output missing from job.log" >&2; exit 1; }
printf '%s' "$JOB_LOG" | grep -q "step3: pwd=$WORKDIR" \
  || { echo ">> step3 output missing from job.log" >&2; exit 1; }
printf '%s' "$JOB_LOG" | grep -q 'step4: shell override' \
  || { echo ">> step4 output missing from job.log" >&2; exit 1; }
printf '%s' "$JOB_LOG" | grep -q 'step6: token exchange flowed to a later step' \
  || { echo ">> step6 output missing from job.log" >&2; exit 1; }

# Verify a per-step log is retrievable and contains only that step's output.
STEP0_LOG=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID/logs/step-0.log")
log "   step-0.log: $STEP0_LOG"
printf '%s' "$STEP0_LOG" | grep -q 'step1: plain command' \
  || { echo ">> step1 output missing from step-0.log" >&2; exit 1; }
printf '%s' "$STEP0_LOG" | grep -q 'step2' \
  && { echo ">> step-0.log contains step2 output (step attribution broken)" >&2; exit 1; }

log ">> 01-shell-job PASSED"
