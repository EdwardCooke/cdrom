#!/usr/bin/env bash
#
# 11-labeling.sh — F-29: labels on pipelines, jobs, environments, worker
# groups, and workers are combined onto a run's job instances and are
# searchable by label key/value.
#
# Boots the stack (with a worker in the "default" group) and:
#
#   1. creates a worker group ("default") carrying a label (rack=1);
#   2. creates a pipeline carrying a label (team=backend) with a single job
#      that targets the "default" group;
#   3. triggers a run of the pipeline and waits for it to run to succeeded;
#   4. verifies the run's job instance carries the combined labels (the
#      pipeline's team label + the worker group's rack label);
#   5. searches for the pipeline by its label (GET /api/pipelines?label=team=backend)
#      and verifies it is the only match;
#   6. searches for the run by its combined labels (GET
#      /api/pipelines/{id}/runs?label=team=backend&label=rack=1) and verifies
#      the run is returned;
#   7. searches for the run by a label it does not carry and verifies it is
#      not returned (the filter is real, not a no-op).
#
# This exercises the full path: the API parses the labels from the pipeline
# and worker-group bodies, the Database service stores them normalized in the
# Label table (one row per key/value pair), createRunAndInstances combines the
# pipeline + job + environment + worker-group labels onto the run's job
# instances, and the list endpoints filter by label with an indexed query on
# the Label table (a JSON column could not be searched this way).
#
# Usage: scripts/e2e/11-labeling.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker

API=http://127.0.0.1:8080

# --- create a worker group carrying a label (F-29) --------------------------
# The e2e worker registers in the "default" group; giving that group a row
# with a label exercises the worker-group label source of the run's combined
# set.
log ">> creating the 'default' worker group with a label"
curl -s -X POST "$API/api/worker-groups" \
  -H 'Content-Type: application/json' \
  -d '{"name":"default","description":"e2e worker group","labels":{"rack":"1"}}' >/dev/null \
  || { echo ">> could not create the worker group" >&2; exit 1; }

# --- create a labeled pipeline with a job targeting the group --------------
log ">> creating a labeled pipeline with a job targeting the 'default' group"
PIPELINE_BODY=$(cat <<'EOF'
{
  "name": "e2e-label-pipeline",
  "labels": {"team": "backend"},
  "jobs": [
    {
      "key": "hello",
      "name": "hello",
      "target_group": "default",
      "spec": {
        "steps": [
          {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo labeling: ran"]}}}
        ]
      }
    }
  ]
}
EOF
)
PIPELINE=$(curl -s -X POST "$API/api/pipelines" \
  -H 'Content-Type: application/json' \
  -d "$PIPELINE_BODY")
PIPELINE_ID=$(printf '%s' "$PIPELINE" | jq -r '.id')
[ -n "$PIPELINE_ID" ] && [ "$PIPELINE_ID" != "null" ] \
  || { echo ">> could not parse pipeline id: $PIPELINE" >&2; exit 1; }
log "   pipeline id: $PIPELINE_ID"

# The pipeline's labels are returned on the create response.
printf '%s' "$PIPELINE" | jq -e '.labels.team == "backend"' >/dev/null 2>&1 \
  || { echo ">> pipeline labels not returned on create: $PIPELINE" >&2; exit 1; }

# --- trigger a run of the pipeline and wait for it to succeed --------------
log ">> triggering a run of the pipeline"
RUN=$(curl -s -X POST "$API/api/pipelines/$PIPELINE_ID/runs" \
  -H 'Content-Type: application/json' \
  -d '{"trigger":"manual"}')
RUN_ID=$(printf '%s' "$RUN" | jq -r '.id')
[ -n "$RUN_ID" ] && [ "$RUN_ID" != "null" ] \
  || { echo ">> could not parse run id: $RUN" >&2; exit 1; }
log "   run id: $RUN_ID"

log ">> waiting for the run to reach succeeded (3)"
RUN=""
for i in $(seq 1 100); do
  RUN=$(curl -s "$API/api/runs/$RUN_ID")
  if [ "$(printf '%s' "$RUN" | jq -r '.status')" = "3" ]; then
    break
  fi
  sleep 0.2
done
[ "$(printf '%s' "$RUN" | jq -r '.status')" = "3" ] \
  || { echo ">> run did not reach succeeded: $RUN" >&2; exit 1; }

# --- verify the run's job instance carries the combined labels -------------
# The job instance's labels are the union of the pipeline's labels
# (team=backend) and the worker group's labels (rack=1). Poll the run's job
# until its combined labels are present (they are stored when the instance is
# created).
log ">> verifying the run's job instance carries the combined labels"
JOB=""
for i in $(seq 1 100); do
  JOBS=$(curl -s "$API/api/jobs?pipeline_id=$PIPELINE_ID")
  JOB=$(printf '%s' "$JOBS" | jq -c --argjson rid "$RUN_ID" \
    '[.[] | select(.run_id == $rid)] | .[0] // empty')
  if [ -n "$JOB" ] && [ "$JOB" != "null" ] \
    && [ "$(printf '%s' "$JOB" | jq -r '.labels.team // empty')" = "backend" ] \
    && [ "$(printf '%s' "$JOB" | jq -r '.labels.rack // empty')" = "1" ]; then
    break
  fi
  sleep 0.2
done
[ -n "$JOB" ] && [ "$JOB" != "null" ] \
  || { echo ">> no job instance found for the run" >&2; exit 1; }
[ "$(printf '%s' "$JOB" | jq -r '.labels.team // empty')" = "backend" ] \
  || { echo ">> job instance missing the pipeline's team label: $JOB" >&2; exit 1; }
[ "$(printf '%s' "$JOB" | jq -r '.labels.rack // empty')" = "1" ] \
  || { echo ">> job instance missing the worker group's rack label: $JOB" >&2; exit 1; }
log "   job instance combined labels: $(printf '%s' "$JOB" | jq -c '.labels')"

# --- search for the pipeline by its label ----------------------------------
log ">> searching for the pipeline by label team=backend"
MATCHES=$(curl -s "$API/api/pipelines?label=team=backend")
COUNT=$(printf '%s' "$MATCHES" | jq 'length')
[ "$COUNT" = "1" ] \
  || { echo ">> expected exactly 1 pipeline labeled team=backend, got $COUNT: $MATCHES" >&2; exit 1; }
GOT_ID=$(printf '%s' "$MATCHES" | jq -r '.[0].id')
[ "$GOT_ID" = "$PIPELINE_ID" ] \
  || { echo ">> label search returned the wrong pipeline (got $GOT_ID, want $PIPELINE_ID): $MATCHES" >&2; exit 1; }
log "   pipeline label search OK (matched $GOT_ID)"

# A label the pipeline does not carry must not match it.
NOMATCH=$(curl -s "$API/api/pipelines?label=team=frontend")
[ "$(printf '%s' "$NOMATCH" | jq 'length')" = "0" ] \
  || { echo ">> pipeline label search matched a pipeline that lacks the label: $NOMATCH" >&2; exit 1; }
log "   pipeline label search negative case OK (no match for team=frontend)"

# --- search for the run by its combined labels ------------------------------
log ">> searching for the run by labels team=backend & rack=1"
RUNS=$(curl -s "$API/api/pipelines/$PIPELINE_ID/runs?label=team=backend&label=rack=1")
RUN_COUNT=$(printf '%s' "$RUNS" | jq 'length')
[ "$RUN_COUNT" = "1" ] \
  || { echo ">> expected exactly 1 run with the combined labels, got $RUN_COUNT: $RUNS" >&2; exit 1; }
GOT_RUN_ID=$(printf '%s' "$RUNS" | jq -r '.[0].id')
[ "$GOT_RUN_ID" = "$RUN_ID" ] \
  || { echo ">> run label search returned the wrong run (got $GOT_RUN_ID, want $RUN_ID): $RUNS" >&2; exit 1; }
log "   run label search OK (matched $GOT_RUN_ID)"

# A label the run's job instances do not carry must not match the run.
RUN_NOMATCH=$(curl -s "$API/api/pipelines/$PIPELINE_ID/runs?label=team=frontend")
[ "$(printf '%s' "$RUN_NOMATCH" | jq 'length')" = "0" ] \
  || { echo ">> run label search matched a run that lacks the label: $RUN_NOMATCH" >&2; exit 1; }
log "   run label search negative case OK (no match for team=frontend)"

log ">> labeling e2e PASSED: a labeled pipeline + worker group combine their labels onto the run's job instance, and both the pipeline and the run are searchable by label (with a real negative case)"
