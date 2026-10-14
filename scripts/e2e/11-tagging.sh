#!/usr/bin/env bash
#
# 11-tagging.sh — F-29: tags on pipelines, jobs, environments, worker groups,
# and workers are combined onto a run's job instances and are searchable by
# tag key/value.
#
# Boots the stack (with a worker in the "default" group) and:
#
#   1. creates a worker group ("default") carrying a tag (rack=1);
#   2. creates a pipeline carrying a tag (team=backend) with a single job that
#      targets the "default" group;
#   3. triggers a run of the pipeline and waits for it to run to succeeded;
#   4. verifies the run's job instance carries the combined tags (the
#      pipeline's team tag + the worker group's rack tag);
#   5. searches for the pipeline by its tag (GET /api/pipelines?tag=team=backend)
#      and verifies it is the only match;
#   6. searches for the run by its combined tags (GET
#      /api/pipelines/{id}/runs?tag=team=backend&tag=rack=1) and verifies the
#      run is returned;
#   7. searches for the run by a tag it does not carry and verifies it is not
#      returned (the filter is real, not a no-op).
#
# This exercises the full path: the API parses the tags from the pipeline and
# worker-group bodies, the Database service stores them normalized in the Tag
# table (one row per key/value pair), createRunAndInstances combines the
# pipeline + job + environment + worker-group tags onto the run's job
# instances, and the list endpoints filter by tag with an indexed query on the
# Tag table (a JSON column could not be searched this way).
#
# Usage: scripts/e2e/11-tagging.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker

API=http://127.0.0.1:8080

# --- create a worker group carrying a tag (F-29) ---------------------------
# The e2e worker registers in the "default" group; giving that group a row
# with a tag exercises the worker-group tag source of the run's combined set.
log ">> creating the 'default' worker group with a tag"
curl -s -X POST "$API/api/worker-groups" \
  -H 'Content-Type: application/json' \
  -d '{"name":"default","description":"e2e worker group","tags":{"rack":"1"}}' >/dev/null \
  || { echo ">> could not create the worker group" >&2; exit 1; }

# --- create a tagged pipeline with a job targeting the group ---------------
log ">> creating a tagged pipeline with a job targeting the 'default' group"
PIPELINE_BODY=$(cat <<'EOF'
{
  "name": "e2e-tag-pipeline",
  "tags": {"team": "backend"},
  "jobs": [
    {
      "key": "hello",
      "name": "hello",
      "target_group": "default",
      "spec": {
        "steps": [
          {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "echo tagging: ran"]}}}
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

# The pipeline's tags are returned on the create response.
printf '%s' "$PIPELINE" | jq -e '.tags.team == "backend"' >/dev/null 2>&1 \
  || { echo ">> pipeline tags not returned on create: $PIPELINE" >&2; exit 1; }

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

# --- verify the run's job instance carries the combined tags ---------------
# The job instance's tags are the union of the pipeline's tags (team=backend)
# and the worker group's tags (rack=1). Poll the run's job until its combined
# tags are present (they are stored when the instance is created).
log ">> verifying the run's job instance carries the combined tags"
JOB=""
for i in $(seq 1 100); do
  JOBS=$(curl -s "$API/api/jobs?pipeline_id=$PIPELINE_ID")
  JOB=$(printf '%s' "$JOBS" | jq -c --argjson rid "$RUN_ID" \
    '[.[] | select(.run_id == $rid)] | .[0] // empty')
  if [ -n "$JOB" ] && [ "$JOB" != "null" ] \
    && [ "$(printf '%s' "$JOB" | jq -r '.tags.team // empty')" = "backend" ] \
    && [ "$(printf '%s' "$JOB" | jq -r '.tags.rack // empty')" = "1" ]; then
    break
  fi
  sleep 0.2
done
[ -n "$JOB" ] && [ "$JOB" != "null" ] \
  || { echo ">> no job instance found for the run" >&2; exit 1; }
[ "$(printf '%s' "$JOB" | jq -r '.tags.team // empty')" = "backend" ] \
  || { echo ">> job instance missing the pipeline's team tag: $JOB" >&2; exit 1; }
[ "$(printf '%s' "$JOB" | jq -r '.tags.rack // empty')" = "1" ] \
  || { echo ">> job instance missing the worker group's rack tag: $JOB" >&2; exit 1; }
log "   job instance combined tags: $(printf '%s' "$JOB" | jq -c '.tags')"

# --- search for the pipeline by its tag ------------------------------------
log ">> searching for the pipeline by tag team=backend"
MATCHES=$(curl -s "$API/api/pipelines?tag=team=backend")
COUNT=$(printf '%s' "$MATCHES" | jq 'length')
[ "$COUNT" = "1" ] \
  || { echo ">> expected exactly 1 pipeline tagged team=backend, got $COUNT: $MATCHES" >&2; exit 1; }
GOT_ID=$(printf '%s' "$MATCHES" | jq -r '.[0].id')
[ "$GOT_ID" = "$PIPELINE_ID" ] \
  || { echo ">> tag search returned the wrong pipeline (got $GOT_ID, want $PIPELINE_ID): $MATCHES" >&2; exit 1; }
log "   pipeline tag search OK (matched $GOT_ID)"

# A tag the pipeline does not carry must not match it.
NOMATCH=$(curl -s "$API/api/pipelines?tag=team=frontend")
[ "$(printf '%s' "$NOMATCH" | jq 'length')" = "0" ] \
  || { echo ">> pipeline tag search matched a pipeline that lacks the tag: $NOMATCH" >&2; exit 1; }
log "   pipeline tag search negative case OK (no match for team=frontend)"

# --- search for the run by its combined tags --------------------------------
log ">> searching for the run by tags team=backend & rack=1"
RUNS=$(curl -s "$API/api/pipelines/$PIPELINE_ID/runs?tag=team=backend&tag=rack=1")
RUN_COUNT=$(printf '%s' "$RUNS" | jq 'length')
[ "$RUN_COUNT" = "1" ] \
  || { echo ">> expected exactly 1 run with the combined tags, got $RUN_COUNT: $RUNS" >&2; exit 1; }
GOT_RUN_ID=$(printf '%s' "$RUNS" | jq -r '.[0].id')
[ "$GOT_RUN_ID" = "$RUN_ID" ] \
  || { echo ">> run tag search returned the wrong run (got $GOT_RUN_ID, want $RUN_ID): $RUNS" >&2; exit 1; }
log "   run tag search OK (matched $GOT_RUN_ID)"

# A tag the run's job instances do not carry must not match the run.
RUN_NOMATCH=$(curl -s "$API/api/pipelines/$PIPELINE_ID/runs?tag=team=frontend")
[ "$(printf '%s' "$RUN_NOMATCH" | jq 'length')" = "0" ] \
  || { echo ">> run tag search matched a run that lacks the tag: $RUN_NOMATCH" >&2; exit 1; }
log "   run tag search negative case OK (no match for team=frontend)"

log ">> tagging e2e PASSED: a tagged pipeline + worker group combine their tags onto the run's job instance, and both the pipeline and the run are searchable by tag (with a real negative case)"
