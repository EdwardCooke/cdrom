#!/usr/bin/env bash
#
# 25-worker-env-mandate.sh — F-30.4 Slice 1: the agent SUPERVISES a worker
# subprocess (env-only mandate) and the payload crossing is secret-clean.
#
# Proves the Slice-1 demonstrable:
#   assignment ⇒ spawn ⇒ claim ⇒ execute ⇒ report ⇒ exit, with:
#   1. the worker's ARGV CLEAN (world-readable): the mandate (job id + job
#      token) rides the ENVIRONMENT ONLY;
#   2. the worker's ENVIRONMENT carries the job token but NEITHER the secret
#      NOR the agent's credential (deliberately-absent contract);
#   3. the secret INTERPOLATED INTO THE WORKER (positive control: the step
#      hashes it and logs the digest — the digests agree iff the exact value
#      arrived; the VALUE itself never reaches a log, the pipeline
#      definition, argv, or env);
#   4. the persisted job log (API-redacted, F-12) contains the control but
#      NOT the secret value;
#   5. the secret value is ABSENT from the agent's log (which folds the
#      worker's log in) and from disk outside the (cleaned) staging area.
#
# Usage: scripts/e2e/25-worker-env-mandate.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, jq.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-agent

# --- a pipeline with a DISTINCTIVE secret ----------------------------------
SECRET_VAL="scan-${RANDOM}${RANDOM}-${RANDOM}zzz"

# --- a job whose step RECEIVES the secret (interpolated) and DIGESTS it -----
# The step hashes the secret (positive control: decryption + interpolation
# reached the worker — the digests agree iff the exact value arrived) and
# logs ONLY the digest — the value never reaches a log, the pipeline
# definition, argv, or env. It then sleeps so the live-process scans below
# can observe the worker mid-job.
#
# The job RIDES A PIPELINE RUN: secrets are declared on pipelines (F-12), so
# a standalone job would interpolate nothing. Trigger the run and resolve its
# job instance (the pipeline's jobs, narrowed to this run's run_id).
EXPECTED_HASH=$(printf '%s' "$SECRET_VAL" | sha256sum | cut -d' ' -f1)
PIPE_FULL_BODY=$(cat <<EOF
{
  "name": "worker-mandate-pipeline",
  "secrets": [{ "name": "scan_secret", "value": "$SECRET_VAL" }],
  "jobs": [
    {
      "key": "secjob",
      "name": "secjob",
      "target_group": "default",
      "spec": {
        "steps": [
          {
            "env": { "MY_SEC": "{{ .secrets.scan_secret }}" },
            "params": {
              "command": { "string": "sh" },
              "args": { "strings": [ "-c", "printf '%s' \"\$MY_SEC\" | sha256sum; sleep 4" ] }
            }
          }
        ]
      }
    }
  ]
}
EOF
)
e2e_http_request POST http://127.0.0.1:8080/api/pipelines "$PIPE_FULL_BODY"
[ "$HTTP_CODE" = "201" ] || { echo ">> create secret pipeline: $HTTP_CODE $HTTP_BODY" >&2; exit 1; }
PIPE_ID=$(printf '%s' "$HTTP_BODY" | jq -r '.id')

RUN_RESP=$(curl -s -X POST "http://127.0.0.1:8080/api/pipelines/$PIPE_ID/runs" \
  -H 'Content-Type: application/json' -d '{}')
RUN_ID=$(printf '%s' "$RUN_RESP" | jq -r '.id')
[ -n "$RUN_ID" ] && [ "$RUN_ID" != "null" ] \
  || { echo ">> could not trigger the run: $RUN_RESP" >&2; exit 1; }
JOB_ID=""
CANDS=""
for i in $(seq 1 50); do
  CANDS=$(curl -s "http://127.0.0.1:8080/api/jobs?pipeline_id=$PIPE_ID")
  JOB_ID=$(printf '%s' "$CANDS" | jq -r --argjson rid "$RUN_ID" '[.[] | select(.run_id == $rid)] | .[0]?.id // empty')
  [ -n "$JOB_ID" ] && break
  sleep 0.2
done
[ -n "$JOB_ID" ] || { echo ">> the run's job instance never appeared (candidates: $CANDS)" >&2; exit 1; }
log ">> run id: $RUN_ID, job id: $JOB_ID"

# --- live-process scan (mid-job, while the worker holds the secret) --------
# Find the worker process (argv = the worker binary path; the mandate is NOT
# in argv). Wait for the worker to be up (dispatch + spawn + claim takes a
# couple of seconds under load).
WPID=""
for i in $(seq 1 150); do
  for cand in $(pgrep -x worker 2>/dev/null || true); do WPID=$cand; done
  [ -n "$WPID" ] && break
  sleep 0.2
done
[ -n "$WPID" ] || { echo ">> no live worker process found mid-job" >&2; exit 1; }
log ">> worker pid: $WPID"

ARGV=$(tr '\0' ' ' < "/proc/$WPID/cmdline")
ENVIRON=$(tr '\0' '\n' < "/proc/$WPID/environ")
log ">> worker argv: $ARGV"

# 1. Argv cleanness: the mandate (job id / token) is NOT in argv.
case "$ARGV" in
  *"$JOB_ID"*) echo ">> job id leaked into the worker's argv: $ARGV" >&2; exit 1;;
esac
if printf '%s\n' "$ENVIRON" | grep -qF "CDROM_WORKER_JOB_ID=$JOB_ID"; then
  log "   mandate present in env (CDROM_WORKER_JOB_ID=$JOB_ID)"
else
  echo ">> CDROM_WORKER_JOB_ID missing from the worker's env" >&2; exit 1
fi
if printf '%s\n' "$ENVIRON" | grep -qF 'CDROM_WORKER_JOB_TOKEN='; then
  log "   job token present in env (CDROM_WORKER_JOB_TOKEN=*)"
else
  echo ">> CDROM_WORKER_JOB_TOKEN missing from the worker's env" >&2; exit 1
fi

# 2. The secret and the agent credential are ABSENT from the worker's SPAWN
#    environment (the transfer media). NB: the secret DOES legitimately live
#    in the STEP's command process (the worker's child shell, via the
#    interpolated env) — that is essential to execution (the old in-process
#    agent held it equally); the contract forbids it in the MANDATE (spawn
#    env/argv), the local channel, logs, and disk.
if printf '%s\n' "$ENVIRON" | grep -qF "$SECRET_VAL"; then
  echo ">> the secret value leaked into the worker's spawn environment (mandate)" >&2; exit 1
fi
AGENT_CRED_CONTENT=$(cat "$WORK/agent-1.cred")
if printf '%s\n' "$ENVIRON" | grep -qF "$AGENT_CRED_CONTENT"; then
  echo ">> the agent credential leaked into the worker's environment" >&2; exit 1
fi
log "   secret + agent credential absent from the worker's spawn env"

# Other cdrom processes (agent, api, ...) must not carry the secret in their
# env either (the API decrypts in-memory; no process holds it as an env var).
OTHER_LEAK=""
for proc in $(pgrep -x agent 2>/dev/null) $(pgrep -fx 'bin/(api|db|scheduler|idp|artifacts|pipelinesource)' 2>/dev/null); do
  if tr '\0' '\n' < "/proc/$proc/environ" 2>/dev/null | grep -qF "$SECRET_VAL"; then
    OTHER_LEAK="$OTHER_LEAK $proc"
  fi
done
[ -z "$OTHER_LEAK" ] || { echo ">> the secret value is in a cdrom process's env:$OTHER_LEAK" >&2; exit 1; }
log "   no other cdrom process carries the secret in its env"

# --- concurrency: TWO distinct jobs ⇒ TWO live workers at once -------------
# F-30.4 lifted the one-job-at-a-time semaphore: the ledger permits unlimited
# DISTINCT coordinates. Proof: job A's step holds for ~4 s; wait for its
# worker to be UP, submit job B while A's worker runs, and observe TWO
# concurrent workers. Then await both completions.
SECOND_BODY=$(cat <<EOF
{
  "name": "worker-mandate-job-two",
  "target_group": "default",
  "spec": {
    "steps": [
      {
        "params": {
          "command": { "string": "sh" },
          "args": { "strings": [ "-c", "sleep 4; echo second-done" ] }
        }
      }
    ]
  }
}
EOF
)
UP=0
for i in $(seq 1 50); do
  CUR=$(pgrep -cx worker 2>/dev/null || echo 0)
  if [ "$CUR" -ge 1 ]; then UP=1; break; fi
  sleep 0.2
done
[ "$UP" = "1" ] || { echo ">> first worker never observed running" >&2; exit 1; }
SECOND_ID=$(e2e_submit_job "$SECOND_BODY")
log ">> second job id: $SECOND_ID (submitted while the first worker runs)"
TWO_LIVE=0
for i in $(seq 1 50); do
  CUR=$(pgrep -cx worker 2>/dev/null || echo 0)
  if [ "$CUR" -ge 2 ]; then TWO_LIVE=1; break; fi
  sleep 0.2
done
[ "$TWO_LIVE" = "1" ] || { echo ">> never observed two concurrent workers (peak $(pgrep -cx worker 2>/dev/null || echo 0))" >&2; exit 1; }
log ">> two concurrent workers observed (parallel children)"

# --- wait for BOTH jobs to succeed -----------------------------------------
e2e_wait_for_job "$JOB_ID" "$EXPECTED_HASH" 'digest control (secret reached the worker)'
e2e_wait_for_job "$SECOND_ID" 'second-done' 'second job output'
e2e_wait_for "$WORK/agent.log" 'job succeeded' 'job success'
log ">> both jobs succeeded (the workers executed them end-to-end)"

# 3. The persisted job log (API side, redacted per F-12): the DIGEST control
# is present, the secret value is NOT.
LOG_NAMES=$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID/logs" | jq -r '.[]?.name // empty')
ALL_LOGS=""
for nm in $LOG_NAMES; do
  ALL_LOGS+="$(curl -s "http://127.0.0.1:8080/api/jobs/$JOB_ID/logs/$nm")"$'\n'
done
printf '%s' "$ALL_LOGS" | grep -qF "$EXPECTED_HASH" \
  || { echo ">> persisted job log lacks the digest control" >&2; exit 1; }
if printf '%s' "$ALL_LOGS" | grep -qF "$SECRET_VAL"; then
  echo ">> the secret value leaked into the persisted job log" >&2; exit 1
fi
log "   persisted job log: control present, secret value absent"

# 4. The agent's log (which FOLDS the worker's log in): secret value absent.
if grep -aqF "$SECRET_VAL" "$WORK/agent.log"; then
  echo ">> the secret value leaked into the agent's log (incl. folded worker log)" >&2; exit 1
fi
log "   agent log (incl. folded worker log): secret value absent"

# 5. Disk outside the staging area: the secret value is nowhere under WORK.
DISK_HITS=$(grep -raF "$SECRET_VAL" "$WORK" 2>/dev/null | grep -v ':agent-1.cred' || true)
if [ -n "$DISK_HITS" ]; then
  echo ">> the secret value persists on disk under \$WORK: $DISK_HITS" >&2; exit 1
fi
log "   disk scan under \$WORK: clean"

log ">> 25-worker-env-mandate PASSED: agent spawned the worker with the env-only mandate; the secret reached the worker (control) but stayed out of argv, env, logs, and disk (F-30.4 S1)"
