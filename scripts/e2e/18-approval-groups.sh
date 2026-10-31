#!/usr/bin/env bash
#
# 18-approval-groups.sh — F-27: approval groups (named approver sets).
#
# Boots the stack (with a worker) plus a SECOND, RBAC-enabled API (:8081) that
# shares the stack's IdP + db + scheduler. Approval-gated JOBS are submitted to
# the OPEN main API (:8080) — the worker runs them and pauses them at the gate
# — while the approve/reject decisions and group MANAGEMENT are driven on the
# ENFORCING API (:8081) as distinct logged-in users. This verifies the F-27
# authorization model:
#
#   • a decision requires BOTH the F-14 permission (jobs.can-approve/-reject)
#     AND membership in the gate's authorized set (group members ∪ role
#     holders ∪ named individuals), resolved at DECISION time;
#   • adding/removing a group member takes effect on a gate already waiting;
#   • `role:<name>` authorizes everyone holding that F-14 role;
#   • `quorum: K` (>1) passes only after K DISTINCT authorized approvals, and
#     any single rejection fails the job immediately regardless of K;
#   • with no `approvers`, behavior is the F-13 baseline (anyone with the
#     permission decides);
#   • group management is gated by approvals.can-manage (a plain user gets 403)
#     and is audited (F-15); the job view exposes the gate's quorum + votes.
#
# Usage: scripts/e2e/18-approval-groups.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and jq.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker
e2e_ensure_users http://127.0.0.1:8080
e2e_start_rbac_api

OPEN=http://127.0.0.1:8080   # open API (synthetic admin): job submission + reads
AUTH=http://127.0.0.1:8081   # RBAC-enabled API: decisions + gated group mgmt

# want <code> <what> — assert the last e2e_*_request HTTP_CODE.
want() {
  local code="$1" what="$2"
  [ "$HTTP_CODE" = "$code" ] || { echo ">> $what: got $HTTP_CODE, want $code ($HTTP_BODY)" >&2; exit 1; }
}

# --- fixtures: users with distinct roles -------------------------------------
# cara/mallory/solo: operator (HOLD jobs.can-approve/-reject). erin: user
# (VIEWER-grade; does NOT hold jobs.can-approve) — the "member without
# permission" case.
log ">> creating test users (3 operators + 1 permission-less member)"
ADA_TOKEN=$(e2e_login "$AUTH" "$E2E_USER_ADA_EMAIL" "$E2E_USER_ADA_PASSWORD")
create_user() { # <prefix> <role>
  e2e_authed_request POST "$AUTH/api/users" "$ADA_TOKEN" \
    "{\"first_name\":\"$(echo "$1" | tr a-z A-Z)\",\"last_name\":\"T\",\"email\":\"$1@example.com\",\"password\":\"pw\",\"roles\":[\"$2\"]}"
  want 201 "create user $1 ($2)"
}
create_user cara operator;   CARA_TOK=$(e2e_login "$AUTH" "cara@example.com" pw)
create_user mallory operator; MALLORY_TOK=$(e2e_login "$AUTH" "mallory@example.com" pw)
create_user solo operator;    SOLO_TOK=$(e2e_login "$AUTH" "solo@example.com" pw)
create_user erin user;        ERIN_TOK=$(e2e_login "$AUTH" "erin@example.com" pw)

# --- fixture: the approval group "eng" (initially just erin) ------------------
log ">> creating approval group 'eng' (member: erin)"
e2e_authed_request POST "$AUTH/api/approval-groups" "$ADA_TOKEN" \
  '{"name":"eng","description":"engineering approvers","members":["erin@example.com"]}'
want 201 "create group eng"

# --- AC6: group management is gated by approvals.can-manage + audited ---------
log ">> a plain user cannot manage approval groups (403); the admin can (audited)"
BOB_TOKEN=$(e2e_login "$AUTH" "$E2E_USER_BOB_EMAIL" "$E2E_USER_BOB_PASSWORD")
e2e_authed_request POST "$AUTH/api/approval-groups" "$BOB_TOKEN" '{"name":"nope"}'
want 403 "bob create group (denied)"
e2e_authed_request POST "$AUTH/api/approval-groups" "$ADA_TOKEN" '{"name":"scratch"}'
want 201 "admin create group scratch"
AUDIT=$(curl -s "$OPEN/api/audit?action=approval-group.create" -H "Authorization: Bearer $ADA_TOKEN")
printf '%s' "$AUDIT" | jq -e '[.[] | select(.action=="approval-group.create")] | length >= 1' >/dev/null \
  || { echo ">> approval-group.create not found in audit log: $AUDIT" >&2; exit 1; }
e2e_authed_request DELETE "$AUTH/api/approval-groups/scratch" "$ADA_TOKEN"
want 200 "admin delete group scratch"

# =============================================================================
# submit_gate_job <suffix> <approvers-json-array> <quorum-int>
# Submits an approval-gated job to the OPEN API (the worker runs it and pauses
# at the gate) and waits for it to reach awaiting_approval (status 8). Prints
# JUST the job id on stdout (clean for command substitution). Diagnostics go
# to stderr. The approval step carries the given approvers + quorum params.
# =============================================================================
submit_gate_job() {
  local suffix="$1" approvers="$2" quorum="$3"
  local name="ag-$suffix"
  # Build the body with jq (-c: compact, guaranteed-valid JSON BY CONSTRUCTION
  # — no string splicing of quotes/braces, which proved treacherous). An empty
  # approvers argument means "none"; quorum 0 means unset.
  if [ -z "$approvers" ]; then approvers="[]"; fi
  local appr_arg=null quorum_arg=null
  if [ "$approvers" != "[]" ]; then appr_arg="$approvers"; fi
  if [ "$quorum" != "0" ]; then quorum_arg="$quorum"; fi
  local body
  body=$(jq -cn \
    --arg name "$name" \
    --arg msg "gate $name" \
    --arg shcmd "echo $name-done" \
    --argjson appr "$appr_arg" \
    --argjson quorum "$quorum_arg" \
    '((if $appr == null then {} else {approvers: {strings: $appr}} end)
      + (if $quorum == null then {} else {quorum: {string: ($quorum | tostring)}} end)
      + {message: {string: $msg}}) as $p
     | {name: $name, target_group: "default", spec: {steps: [
        {type: "approval", params: $p},
        {params: {command: {string: "sh"}, args: {strings: ["-c", $shcmd]}}}
      ]}}')
  # Validate the generated body BEFORE submitting: a malformed body would
  # otherwise surface deep in the API's error path.
  printf '%s' "$body" | jq -e '.' >/dev/null 2>&1 \
    || { echo ">> invalid job body for $name: $body" >&2; return 1; }
  local id
  id=$(e2e_submit_job "$body")
  # log() writes to STDOUT — and this function runs inside $( ), so ANY
  # stdout leak corrupts the job id the caller captures. Divert to stderr.
  log "   submitted ag-$suffix (id=$id)" >&2
  # Wait for the gate; the helper echoes the final job JSON — divert it to
  # STDERR so only the id reaches the caller. On timeout, dump the job for
  # diagnosis before the helper exits non-zero.
  local cur
  if ! cur=$(e2e_wait_for_status "$id" 8 "ag-$suffix awaiting approval" 2>/dev/null); then
    echo "   DIAG ag-$suffix: job never reached awaiting_approval; current state:" >&2
    curl -s "$OPEN/api/jobs/$id" >&2 || true
    echo >&2
    return 1
  fi
  printf '%s' "$cur" >&2
  printf '%s' "$id"
}

# --- AC1 + AC4 + AC5(add): job A (group:eng) ----------------------------------
log ">> A (group:eng): non-member 403, member-without-permission 403, add a member live, approve"
JA=$(submit_gate_job A '["group:eng"]' 0)
e2e_authed_request POST "$AUTH/api/jobs/$JA/approve" "$MALLORY_TOK" '{}'; want 403 "A: mallory (operator, non-member)"
e2e_authed_request POST "$AUTH/api/jobs/$JA/approve" "$ERIN_TOK" '{}';    want 403 "A: erin (member, no permission)"
e2e_authed_request POST "$AUTH/api/approval-groups/eng/members" "$ADA_TOKEN" '{"member":"cara@example.com"}'; want 200 "A: add cara to eng"
e2e_authed_request POST "$AUTH/api/jobs/$JA/approve" "$CARA_TOK" '{"reason":"signed off"}'; want 200 "A: cara (member+permission)"
FA=$(e2e_wait_for_status "$JA" 3 "A succeeded after approval")
printf '%s' "$FA" | jq -e '.approval_decision=="approved"' >/dev/null || { echo ">> A decision not approved" >&2; exit 1; }
e2e_wait_for_job "$JA" 'ag-A-done' 'A post-gate step ran'

# --- AC5(remove): job R (group:eng) — remove a member; bite on the same gate ---
log ">> R (group:eng): remove a member; their in-flight approval is refused"
JR=$(submit_gate_job R '["group:eng"]' 0)
e2e_authed_request DELETE "$AUTH/api/approval-groups/eng/members/cara@example.com" "$ADA_TOKEN"; want 200 "R: remove cara from eng"
e2e_authed_request POST "$AUTH/api/jobs/$JR/approve" "$CARA_TOK" '{}'; want 403 "R: cara (former member) after removal"
e2e_authed_request POST "$AUTH/api/approval-groups/eng/members" "$ADA_TOKEN" '{"member":"mallory@example.com"}'; want 200 "R: add mallory to eng"
e2e_authed_request POST "$AUTH/api/jobs/$JR/approve" "$MALLORY_TOK" '{}'; want 200 "R: mallory approve"
e2e_wait_for_status "$JR" 3 "R succeeded after approval" >/dev/null

# --- AC2: job B (group:eng + a named individual) — union of the set -----------
log ">> B (group:eng + solo@example.com): a named individual (not in the group) may approve"
JB=$(submit_gate_job B '["group:eng","solo@example.com"]' 0)
e2e_authed_request POST "$AUTH/api/jobs/$JB/approve" "$SOLO_TOK" '{}'; want 200 "B: solo (named individual)"
e2e_wait_for_status "$JB" 3 "B succeeded after approval" >/dev/null

# --- AC3: job C (no approvers) — F-13 baseline --------------------------------
log ">> C (no approvers): F-13 baseline (anyone with the permission decides)"
JC=$(submit_gate_job C '[]' 0)
e2e_authed_request POST "$AUTH/api/jobs/$JC/approve" "$MALLORY_TOK" '{}'; want 200 "C: mallory (permission, no refs)"
e2e_wait_for_status "$JC" 3 "C succeeded after approval" >/dev/null

# --- AC8(quorum): job Q (role:operator, quorum 2) — two DISTINCT approvals -----
log ">> Q (role:operator, quorum 2): first approval holds the gate, second resolves it"
JQ=$(submit_gate_job Q '["role:operator"]' 2)
e2e_authed_request POST "$AUTH/api/jobs/$JQ/approve" "$CARA_TOK" '{}'; want 200 "Q: cara approve (1st of 2)"
STILL_Q=$(curl -s "$OPEN/api/jobs/$JQ" -H "Authorization: Bearer $ADA_TOKEN")
[ "$(printf '%s' "$STILL_Q" | jq -r '.status')" = "8" ] \
  || { echo ">> Q advanced past the gate after 1 of 2 approvals ($(printf '%s' "$STILL_Q" | jq -r '.status'))" >&2; exit 1; }
printf '%s' "$STILL_Q" | jq -e '.approval_quorum==2 and ((.approval_votes|length)>=1)' >/dev/null \
  || { echo ">> Q job view missing quorum/votes: $(printf '%s' "$STILL_Q" | jq -c '{approval_quorum,approval_votes}')" >&2; exit 1; }
e2e_authed_request POST "$AUTH/api/jobs/$JQ/approve" "$MALLORY_TOK" '{}'; want 200 "Q: mallory approve (2nd of 2)"
e2e_wait_for_status "$JQ" 3 "Q succeeded after quorum met" >/dev/null

# --- AC8(rejection): job W (role:operator, quorum 3) — rejection fails at once -
log ">> W (role:operator, quorum 3): one approval then a rejection fails the job immediately"
JW=$(submit_gate_job W '["role:operator"]' 3)
e2e_authed_request POST "$AUTH/api/jobs/$JW/approve" "$CARA_TOK" '{}'; want 200 "W: cara approve (held at quorum 3)"
e2e_authed_request POST "$AUTH/api/jobs/$JW/reject" "$MALLORY_TOK" '{"reason":"stop"}'; want 200 "W: mallory reject (fails immediately)"
FW=$(e2e_wait_for_status "$JW" 4 "W failed after rejection")
printf '%s' "$FW" | jq -e '.approval_decision=="rejected"' >/dev/null || { echo ">> W decision not rejected" >&2; exit 1; }

# --- AC9: pipeline default approvers/quorum merge into the saved definition ---
log ">> PD: a pipeline's default approvers/quorum merge into an approval step that sets neither (saved definition)"
PD_PIPE=$(jq -nc '{name:"pd-pipe",
  approvers:["group:eng"], quorum:2,
  jobs:[{key:"dep", spec:{steps:[
    {type:"approval", params:{}},
    {params:{command:{string:"sh"},args:{strings:["-c","echo pd-done"]}}}]}}]}' )
e2e_http_request POST "$OPEN/api/pipelines" "$PD_PIPE"
want 201 "create pipeline with default approvers/quorum"
PD_PID=$(printf '%s' "$HTTP_BODY" | jq -r '.id')
PD_VIEW=$(curl -s "$OPEN/api/pipelines/$PD_PID" -H "Authorization: Bearer $ADA_TOKEN")
# The saved job definition's approval step must carry the MERGED params.
MERGED_APPROVERS=$(printf '%s' "$PD_VIEW" | jq -rc '[.. | objects | select(has("approvers")) | .approvers.strings // empty] | first // empty')
MERGED_QUORUM=$(printf '%s' "$PD_VIEW" | jq -rc '[.. | objects | select(has("quorum")) | .quorum.string // empty] | first // empty')
[ "$MERGED_APPROVERS" = '["group:eng"]' ] \
  || { echo ">> pipeline default approvers not merged into the step (got: $MERGED_APPROVERS; view: $PD_VIEW)" >&2; exit 1; }
[ "$MERGED_QUORUM" = '2' ] \
  || { echo ">> pipeline default quorum not merged into the step (got: $MERGED_QUORUM)" >&2; exit 1; }

log ">> 18-approval-groups PASSED"
