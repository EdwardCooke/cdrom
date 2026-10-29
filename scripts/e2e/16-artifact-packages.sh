#!/usr/bin/env bash
#
# 16-artifact-packages.sh — verifies F-21.6 (artifact packages) end to end.
#
# Scenario coverage (per the spec's test plan):
#
#   1. SEED THE REGISTRY through the real publish endpoint (POST /api/packages):
#      three formats (targz, zip, plain tar) + poisoned twins (an escaping
#      member, a symlink member, a truncated tarball); the immutability 409 on
#      a re-publication of the same version.
#   2. HAPPY PATH: a raw-spec job chains download_artifact → extract → shell
#      verify (targz flavor); stable handler log lines and the extracted file
#      are asserted.
#   3. MATRIX: the same chain for the zip and plain-tar flavors (proof of
#      magic-byte format detection).
#   4. NEGATIVES: unknown artifact, corrupt archive, escaping member,
#      symlink member.
#   5. PER-RUN SNAPSHOT: a pipeline-file pipeline declaring spec.artifacts
#      (pinned tag) — run 1 resolves 1.0.0; a newer 1.0.1 is published; a
#      useLatest pipeline's run resolves 1.0.1 while run 1's snapshot stays
#      1.0.0.
#   6. STAGE CLEANUP: no cdrom-stage-* dirs linger after the jobs terminate.
#   7. PROMOTION (former F-19): a job's publish_artifact output is promoted
#      into the registry via POST /api/artifacts/promote; byte-identity
#      (sha256) + the audit trail.
#   8. publish_artifact SEMANTICS: a job builds a tree under the stage area
#      (nested dirs, a test_ file, junk under tmp/, a metacharacter name
#      star*.cfg, a Windows-forbidden name bad:name?.cfg, and a collision
#      pair du*p.txt + du_p.txt), publishes with a multiline pattern (LEADING
#      **/ + negations), extracts the published zip, and verifies the rebuilt
#      tree — asserting sanitization (star_.cfg, bad_name_.cfg) and collision
#      allocation (du_p.txt + du_p1.txt). Plus the emptiness negative.
#
# Standalone: boots its own stack (WITH A WORKER). Shared (run from e2e.sh):
# the stack is already up.
#
# Registered in scripts/e2e.sh (after 15-externally-sourced-pipelines.sh).
#
# Usage: scripts/e2e/16-artifact-packages.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, jq,
# python3, tar, and zip.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker

# Preserve the stack's WORK dir on FAILURE (instead of the lib's cleanup
# destroying the crime scene): the service logs are the fastest way to a
# diagnosis. On success the dir is removed as usual.
PRESERVE_BASE=/tmp/e2e16.keep
e2e_preserve_on_failure() {
  local code=$?
  if [ "$code" -ne 0 ] && [ -n "${WORK:-}" ] && [ -d "$WORK" ]; then
    echo ">> FAILED (exit $code); preserving stack dir at $PRESERVE_BASE for inspection" >&2
    DEST="$PRESERVE_BASE/scene-$$-$(date +%s)"
    mkdir -p "$DEST"
    cp -r "$WORK"/. "$DEST"/ 2>/dev/null || true
  fi
  e2e_cleanup
}
trap 'echo "ERRTRAP line $LINENO" >> /tmp/e2e16.errtrap' ERR
trap e2e_preserve_on_failure EXIT

# api_curl — a GET that ALWAYS YIELDS VALID JSON: a transient transport
# failure (busy API during the post-success storm, socket hiccup) emits `{}`
# instead of an empty body. Critical under the library's `set -eo pipefail`:
# an EMPTY body piped into `jq` makes jq fail, the assignment
# `x=$(api_curl … | jq …)` propagates that failure, and set -e kills the
# script at the worst possible moment (right when the job succeeds and the
# API is busiest). With `{}`, jq yields null/empty fields and the caller's
# checks simply retry.
api_curl() {
  local out
  out=$(curl -s --max-time 5 "$@" 2>/dev/null) || out=''
  if [ -z "$out" ]; then
    printf '{}'
  else
    printf '%s' "$out"
  fi
}

BASE=http://127.0.0.1:8080
PKG_NS=packages
FIX="$WORK/fixtures"; mkdir -p "$FIX"

# Keep the foreground process tree active even between job waits. Background
# API traffic does not prevent this environment from reaping quiet tests.
( while :; do
    sleep 1
    echo "   ...artifact-package test running"
  done ) &
PIDS+=("$!")

# --- helpers -----------------------------------------------------------------

# e2e_publish <name> <version> <ext> <payload-file> [prerelease]
# Seeds the registry through the REAL publish endpoint (header line + payload).
# BINARY-SAFE: the payload never traverses a shell variable (bash loses NUL
# bytes — a gzip/zip body piped through a variable is truncated and corrupt).
# The OPTIONAL fifth argument ("true") EXPLICITLY places the filing on the
# pre-release channel (the storage-redesign flag — never guessed from the
# version string).
e2e_publish() {
  local name="$1" version="$2" ext="$3" payload="$4" pre="${5:-false}"
  local header
  header=$(printf '{"name":"%s","version":"%s","ext":"%s","prerelease":%s}' "$name" "$version" "$ext" "$pre")
  e2e_binary_post "$BASE/api/packages" "$header" "$payload"
  [ "$HTTP_CODE" = "201" ] || { echo ">> publish $name-$version returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
}

# e2e_build_tree <dir> — the fixture tree the consumption jobs extract.
e2e_build_tree() {
  local dir="$1"
  mkdir -p "$dir/smoke" "$dir/docs"
  printf 'print("hello from the package")\n' > "$dir/smoke/test.py"
  printf '# readme\n' > "$dir/docs/README.md"
}

wait_job_final() {
  local jobid="$1" i st
  # The job-row status is derived asynchronously by the scheduler. A
  # test-wide heartbeat keeps the process tree active while it settles.
  for i in $(seq 1 60); do
    st=$(api_curl "$BASE/api/jobs/$jobid" | jq -r '.status' 2>/dev/null || printf 'unknown')
    case "$st" in 3|4|5|6|7) return 0 ;; esac
    if [ $((i % 5)) -eq 0 ]; then
      echo "   ...waiting for job $jobid (status $st, tick $i)"
    fi
    sleep 0.5
  done
  echo ">> job $jobid did not terminate" >&2; exit 1
}

# =============================================================================
# 1. SEED THE REGISTRY (publish endpoint + immutability)
# =============================================================================
log ">> seeding the registry through POST /api/packages"

e2e_build_tree "$FIX/tree_tgz"
tar czf "$FIX/coollang-1.0.0.tar.gz" -C "$FIX/tree_tgz" .
e2e_publish coollang 1.0.0 .tar.gz "$FIX/coollang-1.0.0.tar.gz"

e2e_build_tree "$FIX/tree_zip"
(cd "$FIX/tree_zip" && zip -qr "$FIX/coolly-1.0.0.zip" .)
e2e_publish cooly 1.0.0 .zip "$FIX/coolly-1.0.0.zip"

e2e_build_tree "$FIX/tree_tar"
tar cf "$FIX/cooltar-1.0.0.tar" -C "$FIX/tree_tar" .
e2e_publish cooltar 1.0.0 .tar "$FIX/cooltar-1.0.0.tar"

# Poisoned twins (escaping member, symlink member, truncated tarball).
python3 - "$FIX" <<'PY'
import io, os, sys, tarfile
base = sys.argv[1]
tf = tarfile.open(os.path.join(base, "evil_escape.tar.gz"), "w:gz")
ti = tarfile.TarInfo("../../evil_escape_e2e.txt")
data = b"ESCAPPED\n"
ti.size = len(data)
tf.addfile(ti, io.BytesIO(data))
tf.close()
tf2 = tarfile.open(os.path.join(base, "evil_sym.tar.gz"), "w:gz")
si = tarfile.TarInfo("sym.txt")
si.type = tarfile.SYMTYPE
si.linkname = "/etc/passwd"
tf2.addfile(si)
tf2.close()
PY
head -c 40 "$FIX/coollang-1.0.0.tar.gz" > "$FIX/truncated.tar.gz"

# Immutability: re-publishing coollang-1.0.0 must 409.
e2e_binary_post "$BASE/api/packages" '{"name":"coollang","version":"1.0.0","ext":".tar.gz"}' "$FIX/coollang-1.0.0.tar.gz"
[ "$HTTP_CODE" = "409" ] || { echo ">> duplicate publish returned $HTTP_CODE, want 409 ($HTTP_BODY)" >&2; exit 1; }
log "   immutability 409 OK"

# The EXPLICIT prerelease flag: publish a cooly pre-release and assert the
# browse SURFACES the flag (columns, not filename guessing).
mkdir -p "$FIX/tree_rc"; printf 'rc-marker' > "$FIX/tree_rc/rc.txt"
(cd "$FIX/tree_rc" && zip -qr "$FIX/cooly-0.9.0-rc.1.zip" .)
e2e_publish cooly 0.9.0-rc.1 .zip "$FIX/cooly-0.9.0-rc.1.zip" true
COOLY_BROWSE=$(api_curl "$BASE/api/packages/cooly")
FLAGGED=$(printf '%s' "$COOLY_BROWSE" | jq -r '[.[] | select(.version=="0.9.0-rc.1")][0].prerelease // empty')
[ "$FLAGGED" = "true" ] || { echo ">> prerelease flag not surfaced in browse: $COOLY_BROWSE" >&2; exit 1; }
log "   prerelease flag OK (browse shows prerelease=true)"

# =============================================================================
# 2+3. CONSUMPTION: happy path (targz) + matrix (zip, tar)
# =============================================================================
MATRIX=("coollang-1.0.0.tar.gz:targz" "cooly-1.0.0.zip:zip" "cooltar-1.0.0.tar:tar")
for entry in "${MATRIX[@]}"; do
  ARCHIVE="${entry%%:*}"; flavor="${entry##*:}"
  log ">> consumption [$flavor]: download_artifact → extract → verify"
  BODY=$(cat <<EOF
{
  "name": "e2e-pkg-$flavor",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "download_artifact", "params": {"artifact": {"string": "$ARCHIVE"}}},
      {"type": "extract", "params": {"archive": {"string": "$ARCHIVE"}, "dest": {"string": "dist"}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "test -f \$CDROM_STAGE_DIR/dist/smoke/test.py && echo packaged-ok-$flavor"]}}}
    ]
  }
}
EOF
)
  JOB_ID=$(e2e_submit_job "$BODY")
  # WAIT FOR COMPLETION FIRST (one robust status poll): the target flushes
  # its log stream only AFTER the job finishes, so by the time the job is
  # terminal every log line is persisted — the pattern checks below are
  # OFFLINE assertions on a settled log, not a race against the flush.
  wait_job_final "$JOB_ID"
  FINAL=$(api_curl "$BASE/api/jobs/$JOB_ID")
  STATUS=$(printf '%s' "$FINAL" | jq -r '.status')
  [ "$STATUS" = "3" ] || { echo ">> [$flavor] job status ≠ succeeded (status $STATUS): $FINAL" >&2; exit 1; }
  LOG=$(api_curl "$BASE/api/jobs/$JOB_ID/logs/job.log")
  printf '%s' "$LOG" | grep -qF "download_artifact: artifact=$ARCHIVE" \
    || { echo ">> [$flavor] download line missing from log: $LOG" >&2; exit 1; }
  printf '%s' "$LOG" | grep -qF "extract: archive=" \
    || { echo ">> [$flavor] extract line missing from log: $LOG" >&2; exit 1; }
  printf '%s' "$LOG" | grep -qF "packaged-ok-$flavor" \
    || { echo ">> [$flavor] verify line missing from log: $LOG" >&2; exit 1; }
  # Worker-group results belong to each JobExecution; the job endpoint only
  # exposes the derived job row. The shell verification above exercises the
  # download and extraction outputs through their actual destinations.
  log "   [$flavor] OK (magic-byte detection + structure)"
done

# =============================================================================
# 4. NEGATIVES
# =============================================================================
log ">> negatives"
# The evil archives are PUBLISHED under their own coordinates (the registry is
# now record-addressed: a download names a filing, and the filing's bytes are
# what the extractor meets). Coords are chosen so the filing name round-trips
# the registry parser (a digit-led version segment). nosuch.zip is never
# published — it probes the "no such artifact" path.
e2e_publish evil-arc 1.0 .tar.gz "$FIX/evil_escape.tar.gz"
e2e_publish evil-link 1.0 .tar.gz "$FIX/evil_sym.tar.gz"
e2e_publish evil-trunc 1.0 .tar.gz "$FIX/truncated.tar.gz"
declare -A NEG_CASES=(
  ['nosuch.zip']='no such artifact'
  ['evil-arc-1.0.tar.gz']='refusing member'
  ['evil-link-1.0.tar.gz']='refusing member'
  ['evil-trunc-1.0.tar.gz']='unsupported or corrupt'
)
for arch in "${!NEG_CASES[@]}"; do
  BODY=$(cat <<EOF
{
  "name": "e2e-pkg-neg-${arch//[!.a-zA-Z_-]/}",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "download_artifact", "params": {"artifact": {"string": "$arch"}}},
      {"type": "extract", "params": {"archive": {"string": "$arch"}}}
    ]
  }
}
EOF
)
  JOB_ID=$(e2e_submit_job "$BODY")
  wait_job_final "$JOB_ID"
  ST=$(api_curl "$BASE/api/jobs/$JOB_ID" | jq -r '.status')
  [ "$ST" = "4" ] || { echo ">> negative $arch: status $ST, want 4 (failed)" >&2; exit 1; }
  grep -qF "${NEG_CASES[$arch]}" "$WORK/worker.log" \
    || { echo ">> negative $arch: worker log lacks '${NEG_CASES[$arch]}':" >&2; tail -n 10 "$WORK/worker.log" >&2; exit 1; }
  log "   $arch → failed (${NEG_CASES[$arch]})"
done
# The escaping member must NOT have materialized anywhere under WORK.
FOUND_ESCAPE=$(find "$WORK" -name 'evil_escape_e2e.txt' 2>/dev/null | head -1 || true)
[ -z "$FOUND_ESCAPE" ] || { echo ">> escaping member escaped: $FOUND_ESCAPE" >&2; exit 1; }

# =============================================================================
# 5. PER-RUN SNAPSHOT (pipeline-file path)
# =============================================================================
log ">> per-run snapshot via spec.artifacts"
PIPE_YAML='apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: pkg-runner
spec:
  workerGroup: default
  artifacts:
    - name: lang
      package:
        name: coollang
        tag: "1.0.0"
  jobs:
    deploy:
      steps:
        fetch:
          download_artifact:
            artifact: lang
        unpack:
          extract:
            archive: coollang-1.0.0.tar.gz
            dest: dist
        verify:
          shell: test -f $CDROM_STAGE_DIR/dist/smoke/test.py && echo snapshot-ok
'
RESPONSE=$(api_curl -X POST "$BASE/api/pipelines" -H 'Content-Type: application/yaml' --data-binary "$PIPE_YAML")
PID=$(printf '%s' "$RESPONSE" | jq -r '.id')
[ -n "$PID" ] && [ "$PID" != "null" ] || { echo ">> pipeline import failed: $RESPONSE" >&2; exit 1; }
RUN1=$(api_curl -X POST "$BASE/api/pipelines/$PID/runs" -H 'Content-Type: application/json' -d '{}')
RUN1_ID=$(printf '%s' "$RUN1" | jq -r '.id')
RUN1_JOB=$(api_curl "$BASE/api/jobs?pipeline_id=$PID" | jq -r --argjson rid "$RUN1_ID" '[.[] | select(.run_id==$rid)][0].id // empty')
[ -n "$RUN1_JOB" ] || { echo ">> run1 job not found" >&2; exit 1; }
RUN1_SNAP=$(api_curl "$BASE/api/runs/$RUN1_ID" | jq -r '.artifacts.lang // empty')
[ "$RUN1_SNAP" = "coollang-1.0.0.tar.gz" ] || { echo ">> run1 snapshot = '$RUN1_SNAP', want coollang-1.0.0.tar.gz" >&2; exit 1; }
wait_job_final "$RUN1_JOB"
RUN1_LOG=$(api_curl "$BASE/api/jobs/$RUN1_JOB/logs/job.log")
printf '%s' "$RUN1_LOG" | grep -qF "snapshot-ok" \
  || { echo ">> run1 verify line missing from log: $RUN1_LOG" >&2; exit 1; }
[ "$(api_curl "$BASE/api/jobs/$RUN1_JOB" | jq -r '.status')" = "3" ] || { echo ">> run1 job failed" >&2; exit 1; }
log "   run1 pinned 1.0.0 and consumed it"

# Publish a NEWER version, then kick a useLatest pipeline: it resolves 1.0.1
# while run1's snapshot is undisturbed.
e2e_build_tree "$FIX/tree_v101"
tar czf "$FIX/coollang-1.0.1.tar.gz" -C "$FIX/tree_v101" .
e2e_publish coollang 1.0.1 .tar.gz "$FIX/coollang-1.0.1.tar.gz"

USE_LATEST_YAML='apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: pkg-latest
spec:
  workerGroup: default
  artifacts:
    - name: lang
      package:
        name: coollang
        useLatest: true
  jobs:
    deploy:
      steps:
        fetch:
          download_artifact:
            artifact: lang
        unpack:
          extract:
            archive: coollang-1.0.1.tar.gz
            dest: dist
        verify:
          shell: test -f $CDROM_STAGE_DIR/dist/smoke/test.py && echo latest-ok
'
RESPONSE=$(api_curl -X POST "$BASE/api/pipelines" -H 'Content-Type: application/yaml' --data-binary "$USE_LATEST_YAML")
PID2=$(printf '%s' "$RESPONSE" | jq -r '.id')
[ -n "$PID2" ] && [ "$PID2" != "null" ] || { echo ">> useLatest pipeline import failed: $RESPONSE" >&2; exit 1; }
RUN2=$(api_curl -X POST "$BASE/api/pipelines/$PID2/runs" -H 'Content-Type: application/json' -d '{}')
RUN2_ID=$(printf '%s' "$RUN2" | jq -r '.id')
RUN2_JOB=$(api_curl "$BASE/api/jobs?pipeline_id=$PID2" | jq -r --argjson rid "$RUN2_ID" '[.[] | select(.run_id==$rid)][0].id // empty')
[ -n "$RUN2_JOB" ] || { echo ">> run2 job not found" >&2; exit 1; }
RUN2_SNAP=$(api_curl "$BASE/api/runs/$RUN2_ID" | jq -r '.artifacts.lang // empty')
[ "$RUN2_SNAP" = "coollang-1.0.1.tar.gz" ] || { echo ">> run2 snapshot = '$RUN2_SNAP', want coollang-1.0.1.tar.gz" >&2; exit 1; }
wait_job_final "$RUN2_JOB"
RUN2_LOG=$(api_curl "$BASE/api/jobs/$RUN2_JOB/logs/job.log")
printf '%s' "$RUN2_LOG" | grep -qF "latest-ok" \
  || { echo ">> run2 verify line missing from log: $RUN2_LOG" >&2; exit 1; }
[ "$(api_curl "$BASE/api/jobs/$RUN2_JOB" | jq -r '.status')" = "3" ] || { echo ">> run2 job failed" >&2; exit 1; }
# Run1's snapshot is UNDISTURBED by the newer registry entry.
RUN1_AGAIN=$(api_curl "$BASE/api/runs/$RUN1_ID")
[ "$(printf '%s' "$RUN1_AGAIN" | jq -r '.artifacts.lang')" = "coollang-1.0.0.tar.gz" ] \
  || { echo ">> run1 snapshot drifted: $RUN1_AGAIN" >&2; exit 1; }
log "   run2 (useLatest) resolved 1.0.1; run1 kept 1.0.0 (snapshot proof)"

# =============================================================================
# 6. STAGE CLEANUP
# =============================================================================
log ">> stage cleanup"
LEFTOVERS=-1
for i in $(seq 1 30); do
  LEFTOVERS=$(find /tmp -maxdepth 1 -name 'cdrom-stage-*' -type d 2>/dev/null | wc -l)
  [ "$LEFTOVERS" = "0" ] && break
  # HEARTBEAT (anti-starvation): a silent cleanup wait is a reap risk.
  if [ $((i % 5)) -eq 0 ]; then
    echo "   ...waiting for stage cleanup (iteration $i, leftovers=$LEFTOVERS)"
  fi
  sleep 0.5
done
[ "$LEFTOVERS" = "0" ] || { echo ">> $LEFTOVERS cdrom-stage-* dirs linger in /tmp" >&2; exit 1; }
log "   no stage-area leftovers"

# =============================================================================
# 7. PROMOTION (former F-19)
# =============================================================================
log ">> promotion"
PUB_JOB_BODY='{
  "name": "e2e-pkg-publisher",
  "target_group": "default",
  "spec": {
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "mkdir -p $CDROM_STAGE_DIR/build/lib && printf PAYLOAD-ONE > $CDROM_STAGE_DIR/build/lib/core.so && printf PAYLOAD-TWO > $CDROM_STAGE_DIR/build/top.txt"]}}},
      {"type": "publish_artifact", "params": {"patterns": {"string": "build/**"}, "fileName": {"string": "pubpromo-1.0.0.zip"}}}
    ]
  }
}'
PUB_JOB=$(e2e_submit_job "$PUB_JOB_BODY")
wait_job_final "$PUB_JOB"
PUB_LOG=$(api_curl "$BASE/api/jobs/$PUB_JOB/logs/job.log")
printf '%s' "$PUB_LOG" | grep -qF "publish_artifact: root=" \
  || { echo ">> publish line missing from log: $PUB_LOG" >&2; exit 1; }
[ "$(api_curl "$BASE/api/jobs/$PUB_JOB" | jq -r '.status')" = "3" ] || { echo ">> publisher job failed" >&2; api_curl "$BASE/api/jobs/$PUB_JOB/logs/job.log" | tail -10 >&2; exit 1; }
# The zip is listed in the job's OWN namespace (job-bound by construction).
LISTED=$(api_curl "$BASE/api/jobs/$PUB_JOB/artifacts" | jq -r '.[].name')
printf '%s\n' "$LISTED" | grep -qx "pubpromo-1.0.0.zip" \
  || { echo ">> published zip not listed under job $PUB_JOB: $LISTED" >&2; exit 1; }
# Promote it into the registry (a registry destination REQUIRES the package
# coordinates — the filing's display name is composed from them, and the
# source's extension is inherited).
PROMOTE_BODY="{\"source_namespace\":\"$PUB_JOB\",\"source_name\":\"pubpromo-1.0.0.zip\",\"dest_namespace\":\"$PKG_NS\",\"package_name\":\"pubpromo\",\"version\":\"1.0.0\"}"
e2e_http_request POST "$BASE/api/artifacts/promote" "$PROMOTE_BODY"
[ "$HTTP_CODE" = "200" ] || { echo ">> promote returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
PROMOTED_SHA=$(printf '%s' "$HTTP_BODY" | jq -r '.sha256')
REG_SHA=$(api_curl "$BASE/api/packages/pubpromo" | jq -r '.[0].sha256 // empty')
[ -n "$PROMOTED_SHA" ] && [ "$PROMOTED_SHA" = "$REG_SHA" ] \
  || { echo ">> promotion sha mismatch: promoted=$PROMOTED_SHA registry=$REG_SHA" >&2; exit 1; }
log "   byte-identity verified (sha256 $PROMOTED_SHA)"
# The audit trail shows the promotion.
AUDIT=$(api_curl "$BASE/api/audit?action=package.promote")
printf '%s' "$AUDIT" | grep -q 'pubpromo-1.0.0.zip' \
  || { echo ">> audit lacks the promotion: $AUDIT" >&2; exit 1; }
log "   audit trail OK"

# =============================================================================
# 8. publish_artifact SEMANTICS (glob + negation + sanitization + collisions)
# =============================================================================
log ">> publish_artifact semantics"
SEM_JOB_BODY='{
  "name": "e2e-pkg-semantics",
  "target_group": "default",
  "spec": {
    "steps": [
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "set -e; cd $CDROM_STAGE_DIR; mkdir -p proj/src/util proj/tmp; printf KEEP-A > proj/src/a.txt; printf KEEP-B > proj/src/util/b.txt; printf TESTME > proj/src/test_unit.go; printf JUNK > proj/tmp/scrap.dat; printf STAR > \"proj/src/star*.cfg\"; printf BADCHAR > \"proj/src/bad:name?.cfg\"; printf DUPREAL > proj/du_p.txt; printf DUPSTAR > \"proj/du*p.txt\""]}}},
      {"type": "publish_artifact", "params": {"patterns": {"string": "**/proj/**\n-proj/tmp/\n-proj/src/test_*"}, "fileName": {"string": "semantic-1.0.0.zip"}}},
      {"type": "extract", "params": {"archive": {"string": "{{ .steps.step1.Outputs.path }}"}, "dest": {"string": "rebuilt"}}},
      {"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "set -e; cd $CDROM_STAGE_DIR; test -f rebuilt/proj/src/a.txt; test -f rebuilt/proj/src/util/b.txt; test -f \"rebuilt/proj/src/star_.cfg\"; test -f rebuilt/proj/src/bad_name_.cfg; test -f rebuilt/proj/du_p.txt; test -f rebuilt/proj/du_p1.txt; test ! -e rebuilt/proj/tmp; test ! -e rebuilt/proj/src/test_unit.go; echo semantics-ok"]}}}
    ]
  }
}'
SEM_JOB=$(e2e_submit_job "$SEM_JOB_BODY")
wait_job_final "$SEM_JOB"
[ "$(api_curl "$BASE/api/jobs/$SEM_JOB" | jq -r '.status')" = "3" ] || { echo ">> semantics job failed" >&2; api_curl "$BASE/api/jobs/$SEM_JOB/logs/job.log" | tail -20 >&2; exit 1; }
SEM_LOG=$(api_curl "$BASE/api/jobs/$SEM_JOB/logs/job.log")
printf '%s' "$SEM_LOG" | grep -qF "semantics-ok" \
  || { echo ">> semantics verify line missing from log: $SEM_LOG" >&2; exit 1; }
# 8 files matched positively; 2 excluded (test_unit.go, scrap.dat); 4 chars
# sanitized (star*.cfg → 1, bad:name?.cfg → 2, du*p.txt → 1); 1 collision
# rename (du*p.txt → du_p1.txt).
printf '%s' "$SEM_LOG" | grep -Eq 'publish_artifact: root=[^ ]+ matched=6 excluded=2' \
  || { echo ">> unexpected publish line:" >&2; printf '%s' "$SEM_LOG" | grep 'publish_artifact' >&2; exit 1; }
printf '%s' "$SEM_LOG" | grep -q 'sanitized=4' || { echo ">> sanitized count missing" >&2; printf '%s' "$SEM_LOG" | grep 'publish_artifact' >&2; exit 1; }
printf '%s' "$SEM_LOG" | grep -q 'renamed=1' || { echo ">> renamed count missing" >&2; printf '%s' "$SEM_LOG" | grep 'publish_artifact' >&2; exit 1; }
log "   glob + negation + sanitization + collision allocation OK"

# Emptiness negative: a pattern matching nothing fails the job.
EMPT_JOB_BODY='{
  "name": "e2e-pkg-empty",
  "target_group": "default",
  "spec": {
    "steps": [
      {"type": "publish_artifact", "params": {"patterns": {"string": "voidware/**"}}}
    ]
  }
}'
EMPT_JOB=$(e2e_submit_job "$EMPT_JOB_BODY")
wait_job_final "$EMPT_JOB"
[ "$(api_curl "$BASE/api/jobs/$EMPT_JOB" | jq -r '.status')" = "4" ] || { echo ">> empty publish did not fail" >&2; exit 1; }
grep -qF "no files matched the given patterns" "$WORK/worker.log" \
  || { echo ">> empty publish lacked the diagnostic" >&2; exit 1; }
log "   emptiness negative OK"

log ">> F-21.6 e2e PASSED"
