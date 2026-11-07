#!/usr/bin/env bash
#
# 23-scim-groups.sh — F-28.5: the SCIM /Groups resource + the group→role
# mapping applicator + the read-only /api/groups admin surface.
#
# Like 22, the shared stack's IdP runs with the SCIM surface DISABLED, so
# this script boots an EXTRA IdP replica (sharing the database — same user,
# group, binding, and event tables) with the surface enabled, a key pepper,
# AND the group→role mapping table (CDROM_SCIM_GROUP_ROLE_MAPPINGS), plus a
# fifth API whose IdP steering points at the replica. Legs:
#
#   - key bootstrap (401 before a peppered key exists; mint unlocks);
#   - POST /Groups creates a group (duplicate displayName legal);
#   - MAPPING GRANT: patching a user into the mapped group makes HER OWN
#     token's /api/me/permissions gain the mapped role's permission on the
#     STEERING API WITHOUT A RESTART (role_change ⇒ cache invalidation,
#     F-23 — the live-enforcement observation the spec demands);
#   - OPERATOR-GRANT SURVIVAL: a manual role binding (via the RBAC API's
#     /api/role-bindings) survives the group's removal — only the
#     scim_group-derived binding is stripped;
#   - DELETE /Groups revokes the derivation (permission gone again);
#   - STRETCH (F-27): an "ag:" mapping target mirrors membership into the
#     named approval group (additive, idempotent);
#   - /api/groups: admin 200 (enriched docs: resolved members,
#     derived_roles), non-manager 403, mutating verbs 405 (read-only);
#   - the durable audit log carries the group mutations + the role-diff
#     companions, connector-attributed.
#
# Usage: scripts/e2e/23-scim-groups.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# tick <seconds>: sleep with a progress DOT every 0.5 s.
tick() {
  local n i
  n=$(awk -v s="$1" 'BEGIN{printf "%d", s*2}')
  for (( i=0; i<n; i++ )); do sleep 0.5; printf '.'; done
  printf '\n'
}

# Preflight: the extras need FREE ports (7118/7119/7120 for the IdP replica
# + fifth API gRPC, 8084 for the fifth API HTTP, 9113/9114 for their
# metrics — 7109 is the shared pipeline source's home, so it is UNAVAILABLE
# in shared-stack mode).
if [ -n "${E2E_STACK_READY:-}" ]; then
  PREFLIGHT_PATTERN='7108|7118|7119|7120|8084|9113|9114'
else
  PREFLIGHT_PATTERN='710[1-9]|711[0-9]|712[0-9]|808[0-9]|91[01][0-9]|1025|1008[01]'
fi
BUSY_PORTS=$(ss -lnt 2>/dev/null | awk '{print $4}' | grep -oE '[0-9]+$' | sort -nu | grep -xE "$PREFLIGHT_PATTERN" | tr '\n' ' ') || BUSY_PORTS=""
if [ -n "$BUSY_PORTS" ]; then
  echo ">> ERROR: required ports already occupied: $BUSY_PORTS"
  echo ">> a previous e2e run likely left orphans behind; find and kill them:"
  echo ">>   ss -lntp | grep -E ':(710[1-9]|711[0-9]|712[0-9]|808[0-9]|91[01][0-9]|1025|1008[01])\\b'"
  exit 1
fi

e2e_boot_stack

log ">> verifying the SCIM /Groups resource + mapping applicator (F-28.5)"

# Shared users (ada admin) — a duplicate registration is a 409, fine.
e2e_ensure_users http://127.0.0.1:8080

# The RBAC-enabled API (:8081) hosts the key-minting surface + logins + the
# role-binding/admin probes.
e2e_start_rbac_api
RBAC=http://127.0.0.1:8081
ADMIN_TOKEN=$(e2e_login "$RBAC" "$E2E_USER_ADA_USERNAME" "$E2E_USER_ADA_PASSWORD")

# --- the peppered IdP replica (WITH the mapping table) + the fifth API -----
PEPPER="s5-e2e-pepper"
MAPS="engineering=operator,reviews=ag:release-reviewers"
e2e_start idp-g "$WORK/idp-g.log" 'idp starting' \
  CDROM_LISTEN_ADDR=127.0.0.1:7119 CDROM_IDP_GRPC_ADDR=127.0.0.1:7118 \
  CDROM_IDP_ISSUER=https://127.0.0.1:7104 CDROM_DB_ADDR=127.0.0.1:7101 \
  CDROM_IDP_SCIM_KEYS_PEPPER="$PEPPER" CDROM_SCIM_ENABLED=true \
  CDROM_SCIM_GROUP_ROLE_MAPPINGS="$MAPS" \
  CDROM_METRICS_ADDR=127.0.0.1:9114 \
  CDROM_AUDIT_FILE="$WORK/idp-g-audit.log" CDROM_AUDIT_FORMAT=json \
  CDROM_LOG_FORMAT=text -- "$BIN/idp"
e2e_start api-g "$WORK/api-g.log" 'serving' \
  CDROM_API_HTTP_ADDR=127.0.0.1:8084 CDROM_LISTEN_ADDR=127.0.0.1:7120 \
  CDROM_AUTH_ENABLED=true CDROM_AUTH_ISSUER=https://127.0.0.1:7104 \
  CDROM_AUTH_CLIENT_ID=cdrom-ui CDROM_AUTH_REDIRECT_URL=http://127.0.0.1:8084/api/auth/callback \
  CDROM_IDP_GRPC_ADDR=127.0.0.1:7118 \
  CDROM_METRICS_ADDR=127.0.0.1:9113 \
  CDROM_AUDIT_FILE="$WORK/audit-g.log" CDROM_AUDIT_FORMAT=json \
  CDROM_LOG_FORMAT=text -- "$BIN/api"
API_G=http://127.0.0.1:8084
SCIM=https://127.0.0.1:7119/scim/v2
MAIN=http://127.0.0.1:8080

# Admin token for the fifth API (same IdP-issued realm).
ADA_G_TOKEN=$(e2e_login "$API_G" "$E2E_USER_ADA_USERNAME" "$E2E_USER_ADA_PASSWORD")

# --- bootstrap ritual: 401 before a peppered key exists --------------------
BOOT=$(curl -s --max-time 10 -o /dev/null -w '%{http_code}' "$SCIM/Users" \
  -H "Authorization: Bearer not-a-key" -H 'Accept: application/scim+json' || true)
[ "$BOOT" = "401" ] || { echo ">> pre-bootstrap probe = $BOOT, want 401"; exit 1; }

# Bootstrap the key. The POOL IS SHARED ACROSS SCENARIOS (same database):
# 19 + 22 may have filled all five slots, so a capacity 409 is EXPECTED in
# the shared run — reclaim the OLDEST slot through the main API (pool
# maintenance is admin-operable) and retry once.
mint_key() {
  e2e_authed_request POST "$API_G/api/scim/keys" "$ADA_G_TOKEN" '{"description":"s5 e2e connector"}'
  [ "$HTTP_CODE" = "201" ] || return 1
  printf '%s' "$HTTP_BODY" | jq -r '.key.plaintext // .plaintext // empty'
}
SCIM_KEY=$(mint_key) || {
  echo ">> key pool at capacity — reclaiming the oldest slot (shared-pool residue)"
  e2e_authed_request GET "$MAIN/api/scim/keys" "$ADMIN_TOKEN"
  [ "$HTTP_CODE" = "200" ] || { echo ">> key list = $HTTP_CODE, want 200 ($HTTP_BODY)"; exit 1; }
  OLDEST=$(printf '%s' "$HTTP_BODY" | jq -r '[.[] | select(.revoked==false)] | min_by((.identifier|tonumber)) | .identifier // empty')
  [ -n "$OLDEST" ] || { echo ">> no reclaimable key slot: $HTTP_BODY"; exit 1; }
  e2e_authed_request POST "$MAIN/api/scim/keys/$OLDEST/revoke" "$ADMIN_TOKEN" ''
  [ "$HTTP_CODE" = "200" ] || { echo ">> reclaim revoke = $HTTP_CODE, want 200 ($HTTP_BODY)"; exit 1; }
  SCIM_KEY=$(mint_key) || { echo ">> key mint (retry) failed"; exit 1; }
}
[ -n "$SCIM_KEY" ] || { echo ">> key mint returned no plaintext"; exit 1; }
tick 1.2 # settle the guard's ~1 s bundle memo

scim_req() { # <method> <path> [json-body] → SCIM_CODE / SCIM_BODY
  local method="$1" path="$2" body="${3:-}"
  local out
  if [ -n "$body" ]; then
    out=$(curl -s --max-time 10 -w $'\n%{http_code}' -X "$method" "$SCIM$path" \
      -H "Authorization: Bearer $SCIM_KEY" -H 'Accept: application/scim+json' \
      -H 'Content-Type: application/json' -d "$body" || true)
  else
    out=$(curl -s --max-time 10 -w $'\n%{http_code}' -X "$method" "$SCIM$path" \
      -H "Authorization: Bearer $SCIM_KEY" -H 'Accept: application/scim+json' || true)
  fi
  SCIM_CODE=$(printf '%s' "$out" | tail -n 1)
  SCIM_BODY=$(printf '%s' "$out" | sed '$d')
}

# api_req <method> <url> <token> [json] → HTTP_CODE / HTTP_BODY
api_req() {
  local method="$1" url="$2" token="$3" body="${4:-}"
  local out
  if [ -n "$body" ]; then
    out=$(curl -s --max-time 10 -w $'\n%{http_code}' -X "$method" "$url" \
      -H "Authorization: Bearer $token" -H 'Content-Type: application/json' -d "$body" || true)
  else
    out=$(curl -s --max-time 10 -w $'\n%{http_code}' -X "$method" "$url" \
      -H "Authorization: Bearer $token" || true)
  fi
  HTTP_CODE=$(printf '%s' "$out" | tail -n 1)
  HTTP_BODY=$(printf '%s' "$out" | sed '$d')
}

# shim_token <user> <roles> — the dev shim stands in for a CORPORATE ISSUER:
# it signs a token for <user> bearing the given roles (the built-in IdP
# never signs for unknown subjects on its own). Presented to api-g (whose
# issuer is the main IdP, 7104 — the shim's token matches).
shim_token() {
  local user="$1" roles="${2:-}" qs location code_val tok_raw tok
  qs="user=$user&redirect_uri=http://127.0.0.1:8081/api/auth/callback&client_id=cdrom-ui"
  [ -n "$roles" ] && qs="$qs&roles=$roles"
  location=$(curl -s --max-time 10 -o /dev/null -w '%{redirect_url}' "https://127.0.0.1:7104/auth?$qs")
  code_val=$(printf '%s' "$location" | sed -n 's/.*code=\([^&]*\).*/\1/p')
  [ -n "$code_val" ] || { echo ">> shim redirect lacked a code: $location"; exit 1; }
  tok_raw=$(curl -s --max-time 10 -X POST "https://127.0.0.1:7104/token" \
    -d "grant_type=authorization_code&code=$code_val&client_id=cdrom-ui&redirect_uri=http://127.0.0.1:8081/api/auth/callback")
  tok=$(printf '%s' "$tok_raw" | jq -r '.access_token // empty')
  [ -n "$tok" ] || { echo ">> shim token exchange failed: $tok_raw"; exit 1; }
  printf '%s' "$tok"
}

# wait_perm <api> <token> <permission> <want: yes|no> — polls the presenter's
# /api/me/permissions until the permission's presence matches (≤ 10 s). The
# role_change event ⇒ api-g's cache INVALIDATES (F-23), so the flip lands
# promptly; the poll absorbs the event-log latency.
wait_perm() {
  local api="$1" token="$2" perm="$3" want="$4" i has
  for i in $(seq 1 20); do
    api_req GET "$api/api/me/permissions" "$token"
    if [ "$HTTP_CODE" = "200" ]; then
      has=no
      printf '%s' "$HTTP_BODY" | jq -e --arg p "$perm" '.platform_wide | index($p) != null' >/dev/null 2>&1 && has=yes
      [ "$has" = "$want" ] && return 0
    fi
    sleep 0.5
  done
  return 1
}

# --- provision the test user via SCIM --------------------------------------
log ">> POST /Users provisions dana (the mapping subject)"
scim_req POST "/Users" '{"schemas":["urn:ietf:params:scim:schemas:core:2.0:User"],"userName":"dana.eng","name":{"givenName":"Dana","familyName":"Eng"},"externalId":"az-dana","active":true}'
[ "$SCIM_CODE" = "201" ] || { echo ">> provision dana = $SCIM_CODE, want 201 ($SCIM_BODY)"; exit 1; }
DANA_ID=$(printf '%s' "$SCIM_BODY" | jq -r '.id')
[ -n "$DANA_ID" ] && [ "$DANA_ID" != "null" ] || { echo ">> provision returned no id: $SCIM_BODY"; exit 1; }

DANA_TOK=$(shim_token "dana.eng" "user")

# Baseline: dana's stock "user" role does NOT carry runs.can-trigger.
if ! wait_perm "$API_G" "$DANA_TOK" "runs.can-trigger" no; then
  echo ">> baseline: dana unexpectedly HOLDS runs.can-trigger before any group"
  exit 1
fi

# --- the approval group the stretch mirrors into ----------------------------
api_req POST "$RBAC/api/approval-groups" "$ADMIN_TOKEN" '{"name":"release-reviewers","description":"s5 e2e"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> approval-group create = $HTTP_CODE, want 201 ($HTTP_BODY)"; exit 1; }

# --- MAPPING GRANT: join ⇒ the permission LANDS without a restart -----------
log ">> POST /Groups engineering (dana a member) ⇒ runs.can-trigger appears on api-g"
scim_req POST "/Groups" '{"schemas":["urn:ietf:params:scim:schemas:core:2.0:Group"],"displayName":"engineering","description":"deploys things","members":[{"value":"az-dana"},{"value":"ghost.person"}]}'
[ "$SCIM_CODE" = "201" ] || { echo ">> create engineering = $SCIM_CODE, want 201 ($SCIM_BODY)"; exit 1; }
ENG_ID=$(printf '%s' "$SCIM_BODY" | jq -r '.id')
[ -n "$ENG_ID" ] || { echo ">> no group id: $SCIM_BODY"; exit 1; }
# The unresolvable member is KEPT (opacity) and the writable response came
# back — the derivation proceeded for the RESOLVED member.
if ! wait_perm "$API_G" "$DANA_TOK" "runs.can-trigger" yes; then
  echo ">> mapping grant: runs.can-trigger never appeared on api-g (no-restart enforcement failed)"
  exit 1
fi

# --- OPERATOR-GRANT SURVIVAL ------------------------------------------------
# The operator grants dana a SECOND role via PUT /api/users/{id} (the
# spec's stated channel — a roles-only payload passes the SCIM restricted-
# write lock) WHILE SHE IS A MEMBER: the classic temporary-adjustment
# overlay. A custom role ('s5-temp' ⇒ roles.can-assign, a permission dana's
# stock 'user' role lacks) makes the grant OBSERVABLE. Leaving the group
# must strip ONLY the scim_group-derived role; the manual grant survives.
# (Row roles flow into tokens at sign-in, so the probe uses a FRESH token
# carrying the row's role set — dana's next SSO.)
log ">> operator grants dana 's5-temp' via PUT /api/users/{id} (overlap)"
api_req POST "$RBAC/api/roles" "$ADMIN_TOKEN" '{"name":"s5-temp","description":"s5 e2e temporary access","permissions":["roles.can-assign"]}'
[ "$HTTP_CODE" = "201" ] || { echo ">> custom role create = $HTTP_CODE, want 201 ($HTTP_BODY)"; exit 1; }
api_req PUT "$RBAC/api/users/$DANA_ID" "$ADMIN_TOKEN" '{"roles":["user","s5-temp"]}'
[ "$HTTP_CODE" = "200" ] || { echo ">> PUT user roles = $HTTP_CODE, want 200 ($HTTP_BODY)"; exit 1; }
DANA_TOK2=$(shim_token "dana.eng" "user,s5-temp")
if ! wait_perm "$API_G" "$DANA_TOK2" "roles.can-assign" yes; then
  echo ">> manual grant not effective: roles.can-assign missing on the fresh token"
  exit 1
fi

# Definite replace: dana LEAVES engineering (wholesale swap to empty).
scim_req PUT "/Groups/$ENG_ID" '{"displayName":"engineering","members":[]}'
[ "$SCIM_CODE" = "200" ] || { echo ">> PUT empty members = $SCIM_CODE, want 200 ($SCIM_BODY)"; exit 1; }
if ! wait_perm "$API_G" "$DANA_TOK2" "runs.can-trigger" no; then
  echo ">> derived role SURVIVED the group's loss (runs.can-trigger still present)"
  exit 1
fi
# The manual grant persists on the SAME token (survival proven), the derived
# one is gone. Bookkeeping: the row still carries s5-temp.
if ! wait_perm "$API_G" "$DANA_TOK2" "roles.can-assign" yes; then
  echo ">> manual grant STRIPPED by the group's loss (survival violated)"
  exit 1
fi
api_req GET "$RBAC/api/users?include_deleted=false" "$ADMIN_TOKEN"
ROW_ROLES=$(printf '%s' "$HTTP_BODY" | jq -r --arg id "$DANA_ID" '[.[] | select(.id==$id)][0].roles // [] | @csv')
printf '%s' "$ROW_ROLES" | grep -q "s5-temp" \
  || { echo ">> row lost the manual role after the group churn: $ROW_ROLES"; exit 1; }

# --- DELETE revokes ------------------------------------------------------------
log ">> DELETE /Groups cleans the lane (recomputed; no orphaned scim_group bindings)"
scim_req DELETE "/Groups/$ENG_ID"
[ "$SCIM_CODE" = "204" ] || { echo ">> delete engineering = $SCIM_CODE, want 204 ($SCIM_BODY)"; exit 1; }
api_req GET "$RBAC/api/role-bindings?principal_id=dana.eng" "$ADMIN_TOKEN"
TOTAL_BINDINGS=$(printf '%s' "$HTTP_BODY" | jq 'length')
[ "$TOTAL_BINDINGS" = "0" ] || { echo ">> bindings linger after delete (lane should be clean): $HTTP_BODY"; exit 1; }
# The manual grant (row/channel) is UNAFFECTED by the group delete.
if ! wait_perm "$API_G" "$DANA_TOK2" "roles.can-assign" yes; then
  echo ">> group delete TOUCHED the manual grant (survival violated)"
  exit 1
fi

# --- STRETCH: the "ag:" target mirrors membership into the approval group ----
log ">> POST /Groups reviews (ag: mapping) mirrors dana into release-reviewers"
scim_req POST "/Groups" '{"displayName":"reviews","members":[{"value":"az-dana"}]}'
[ "$SCIM_CODE" = "201" ] || { echo ">> create reviews = $SCIM_CODE, want 201 ($SCIM_BODY)"; exit 1; }
REV_ID=$(printf '%s' "$SCIM_BODY" | jq -r '.id')
MIRROR_OK=no
for i in $(seq 1 10); do
  api_req GET "$RBAC/api/approval-groups/release-reviewers" "$ADMIN_TOKEN"
  if [ "$HTTP_CODE" = "200" ] && printf '%s' "$HTTP_BODY" | jq -e '.members | index("dana.eng") != null' >/dev/null 2>&1; then
    MIRROR_OK=yes; break
  fi
  sleep 0.5
done
[ "$MIRROR_OK" = "yes" ] || { echo ">> approval-group mirror never landed: $HTTP_BODY"; exit 1; }

# --- /api/groups admin surface -------------------------------------------------
log ">> GET /api/groups (admin 200, enriched; non-manager 403; verbs 405)"
api_req GET "$API_G/api/groups" "$ADA_G_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> admin GET /api/groups = $HTTP_CODE, want 200 ($HTTP_BODY)"; exit 1; }
REV_DOC=$(printf '%s' "$HTTP_BODY" | jq -c --arg id "$REV_ID" '.[] | select(.id==$id)')
[ -n "$REV_DOC" ] && [ "$REV_DOC" != "null" ] || { echo ">> reviews group missing from /api/groups: $HTTP_BODY"; exit 1; }
printf '%s' "$REV_DOC" | jq -e '.display_name=="reviews" and (.members | length)==1 and .members[0].value=="az-dana" and .members[0].username=="dana.eng" and (.derived_roles | length)==0' >/dev/null \
  || { echo ">> reviews doc shape wrong: $REV_DOC"; exit 1; }
# Single-item fetch.
api_req GET "$API_G/api/groups/$REV_ID" "$ADA_G_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> admin GET /api/groups/{id} = $HTTP_CODE, want 200"; exit 1; }
GOT_REV_ID=$(printf '%s' "$HTTP_BODY" | jq -r '.id')
[ "$GOT_REV_ID" = "$REV_ID" ] || { echo ">> single-item id = $GOT_REV_ID, want $REV_ID"; exit 1; }
# Dana (holds 'viewer' — NOT users.can-manage) is denied.
api_req GET "$API_G/api/groups" "$DANA_TOK"
[ "$HTTP_CODE" = "403" ] || { echo ">> non-manager GET /api/groups = $HTTP_CODE, want 403"; exit 1; }
# Mutating verbs are NOT wired (read-only by design).
api_req POST "$API_G/api/groups" "$ADA_G_TOKEN" '{}'
[ "$HTTP_CODE" = "405" ] || { echo ">> POST /api/groups = $HTTP_CODE, want 405 (read-only)"; exit 1; }
api_req PUT "$API_G/api/groups/$REV_ID" "$ADA_G_TOKEN" '{}'
[ "$HTTP_CODE" = "405" ] || { echo ">> PUT /api/groups/{id} = $HTTP_CODE, want 405 (read-only)"; exit 1; }

# --- audit: group mutations + role-diff companions, connector-attributed ------
log ">> the durable audit log carries the SCIM group ops + derived companions"
AUDIT_OK=no
for i in $(seq 1 10); do
  GC=$(curl -s --max-time 10 "$MAIN/api/audit?action=scim.group.create" || true)
  GD=$(curl -s --max-time 10 "$MAIN/api/audit?action=scim.group.derived" || true)
  GX=$(curl -s --max-time 10 "$MAIN/api/audit?action=scim.group.delete" || true)
  if printf '%s' "$GC" | jq -e 'type=="array" and length>=1 and (.[0].actor|startswith("SCIM connector"))' >/dev/null 2>&1 \
     && printf '%s' "$GD" | jq -e 'type=="array" and length>=1' >/dev/null 2>&1 \
     && printf '%s' "$GX" | jq -e 'type=="array" and length>=1' >/dev/null 2>&1; then
    AUDIT_OK=yes; break
  fi
  sleep 0.3
done
[ "$AUDIT_OK" = "yes" ] \
  || { echo ">> group audit events missing/misinattributed (create: ${GC:0:200}; derived: ${GD:0:200})"; exit 1; }

log ">> PASSED: SCIM /Groups + mapping applicator + /api/groups (F-28.5)"
