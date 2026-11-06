#!/usr/bin/env bash
#
# 22-scim-users.sh — F-28.4 Slice 2: the SCIM /Users resource.
#
# Exercises the user lifecycle over SCIM against a real stack:
#   - the shared stack's IdP runs with the SCIM surface DISABLED, so this
#     script boots an EXTRA IdP replica (sharing the database, hence the same
#     user table) with the surface enabled + a key pepper, and a FOURTH API
#     whose key derivation is steered at the peppered replica (cribbing the
#     19-scim-keys bootstrap ritual);
#   - key bootstrap: the guarded surface 401s before a peppered key exists;
#     the first key minted through the peppered replica unlocks it;
#   - POST /Users provisions (201, Location, meta from the row); a duplicate
#     userName POST is a strict 409 uniqueness;
#   - GET /Users honors the filter subset (userName eq) and pagination
#     envelopes (totalResults/itemsPerPage);
#   - PATCH active:false deactivates — and the user's login is REFUSED on the
#     enforcing API (provisioned users are SSO-only; deactivation compounds
#     the suspension);
#   - DELETE deprovisions (204); item ops on the tombstone 404;
#   - a re-POST (rehire) REVIVES THE SAME NUMERIC ID (history intact);
#   - every mutation lands in the DURABLE audit log attributed to the
#     presenting SCIM connector.
#
# Usage: scripts/e2e/22-scim-users.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# tick <seconds>: sleep with a progress DOT every 0.5 s (long silent stretches
# invite trouble in constrained runners; the chatter keeps the run noticed).
tick() {
  local n i
  n=$(awk -v s="$1" 'BEGIN{printf "%d", s*2}')
  for (( i=0; i<n; i++ )); do sleep 0.5; printf '.'; done
  printf '\n'
}

# Preflight: the extras need FREE ports (7114/7116 for the IdP replica,
# 8083/7118 for the fourth API — 7109 is the shared pipeline source's home,
# so it is UNAVAILABLE in shared-stack mode). Standalone mode also needs the shared-stack
# ports free (orphans from an abandoned run squat them).
if [ -n "${E2E_STACK_READY:-}" ]; then
  PREFLIGHT_PATTERN='7114|7116|7118|8083'
else
  PREFLIGHT_PATTERN='710[1-9]|711[0-9]|808[0-9]|910[0-9]|1025|1008[01]'
fi
BUSY_PORTS=$(ss -lnt 2>/dev/null | awk '{print $4}' | grep -oE '[0-9]+$' | sort -nu | grep -xE "$PREFLIGHT_PATTERN" | tr '\n' ' ') || BUSY_PORTS=""
if [ -n "$BUSY_PORTS" ]; then
  echo ">> ERROR: required ports already occupied: $BUSY_PORTS"
  echo ">> a previous e2e run likely left orphans behind; find and kill them:"
  echo ">>   ss -lntp | grep -E ':(710[1-9]|711[0-9]|808[0-9]|910[0-9]|1025|1008[01])\\b'"
  exit 1
fi

e2e_boot_stack

log ">> verifying the SCIM /Users resource (F-28.4 slice 2)"

# Shared users (ida admin / bob) — a duplicate registration is a 409, fine.
e2e_ensure_users http://127.0.0.1:8080

# The RBAC-enabled API (:8081) hosts the key-minting surface + logins.
e2e_start_rbac_api
RBAC=http://127.0.0.1:8081
ADMIN_TOKEN=$(e2e_login "$RBAC" "$E2E_USER_ADA_USERNAME" "$E2E_USER_ADA_PASSWORD")

# --- the peppered IdP replica + the fourth API steering at it ---------------
PEPPER="s2-e2e-pepper"
e2e_start idp-u "$WORK/idp-u.log" 'idp starting' \
  CDROM_LISTEN_ADDR=127.0.0.1:7114 CDROM_IDP_GRPC_ADDR=127.0.0.1:7116 \
  CDROM_IDP_ISSUER=http://127.0.0.1:7104 CDROM_DB_ADDR=127.0.0.1:7101 \
  CDROM_IDP_SCIM_KEYS_PEPPER="$PEPPER" CDROM_SCIM_ENABLED=true \
  CDROM_AUDIT_FILE="$WORK/idp-u-audit.log" CDROM_AUDIT_FORMAT=json \
  CDROM_LOG_FORMAT=text -- "$BIN/idp"
# Deterministic handle for the fail-closed leg: e2e_start recorded THIS
# process as the newest member of the harness's PIDS array.
idpu_pid="${PIDS[${#PIDS[@]}-1]}"
e2e_start api-u "$WORK/api-u.log" 'serving' \
  CDROM_API_HTTP_ADDR=127.0.0.1:8083 CDROM_LISTEN_ADDR=127.0.0.1:7118 \
  CDROM_AUTH_ENABLED=true CDROM_AUTH_ISSUER=http://127.0.0.1:7104 \
  CDROM_AUTH_CLIENT_ID=cdrom-ui CDROM_AUTH_REDIRECT_URL=http://127.0.0.1:8083/api/auth/callback \
  CDROM_IDP_GRPC_ADDR=127.0.0.1:7116 \
  CDROM_AUDIT_FILE="$WORK/audit-u.log" CDROM_AUDIT_FORMAT=json \
  CDROM_LOG_FORMAT=text -- "$BIN/api"
API_U=http://127.0.0.1:8083
SCIM=http://127.0.0.1:7114/scim/v2

# Admin token for the fourth API (same IdP-issued realm).
ADMIN_U_TOKEN=$(e2e_login "$API_U" "$E2E_USER_ADA_USERNAME" "$E2E_USER_ADA_PASSWORD")

# --- bootstrap ritual: 401 before a peppered key exists ---------------------
# The pool already holds unpeppered keys from the shared IdP's perspective,
# but the peppered replica's encodings refuse to verify them.
BOOT=$(curl -s --max-time 10 -o /dev/null -w '%{http_code}' "$SCIM/Users" \
  -H "Authorization: Bearer not-a-key" -H 'Accept: application/scim+json' || true)
[ "$BOOT" = "401" ] || { echo ">> pre-bootstrap probe = $BOOT, want 401" ; exit 1; }

# Mint the FIRST peppered key through the fourth API (derivation steered at
# the peppered replica) — it unlocks the surface.
e2e_authed_request POST "$API_U/api/scim/keys" "$ADMIN_U_TOKEN" '{"description":"s2 e2e connector"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> key mint returned $HTTP_CODE, want 201 ($HTTP_BODY)"; exit 1; }
SCIM_KEY=$(printf '%s' "$HTTP_BODY" | jq -r '.key.plaintext // .plaintext // empty')
[ -n "$SCIM_KEY" ] || { echo ">> key mint returned no plaintext: $HTTP_BODY"; exit 1; }
tick 1.2 # settle the guard's ~1 s bundle memo (the new key becomes visible)

scim_req() { # <method> <path> [json-body]
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

# The peppered key now validates.
scim_req GET "/Users"
[ "$SCIM_CODE" = "200" ] || { echo ">> post-bootstrap list = $SCIM_CODE, want 200 ($SCIM_BODY)"; exit 1; }

# --- provision ---------------------------------------------------------------
log ">> POST /Users provisions (201 + Location + row-timestamped meta)"
scim_req POST "/Users" '{"schemas":["urn:ietf:params:scim:schemas:core:2.0:User"],"userName":"alice.corp","name":{"givenName":"Alice","familyName":"Corp"},"emails":[{"value":"alice@corp.example","type":"primary"}],"externalId":"az-alice","active":true,"groups":[{"display":"Engineering"}]}'
[ "$SCIM_CODE" = "201" ] || { echo ">> provision = $SCIM_CODE, want 201 ($SCIM_BODY)"; exit 1; }
ALICE_ID=$(printf '%s' "$SCIM_BODY" | jq -r '.id')
META_LOC=$(printf '%s' "$SCIM_BODY" | jq -r '.meta.location')
[ -n "$ALICE_ID" ] && [ "$ALICE_ID" != "null" ] || { echo ">> provision returned no id: $SCIM_BODY"; exit 1; }
[[ "$ALICE_ID" =~ ^[0-9]+$ ]] || { echo ">> id is not a decimal string: $ALICE_ID"; exit 1; }
printf '%s' "$SCIM_BODY" | jq -e '.userName == "alice.corp" and .name.givenName == "Alice" and .externalId == "az-alice" and .active == true' >/dev/null \
  || { echo ">> provisioned doc mismatch: $SCIM_BODY"; exit 1; }
printf '%s' "$SCIM_BODY" | jq -e '.meta.resourceType == "User" and (.meta.created | test("^\\d{4}-")) and (.meta.modified | test("^\\d{4}-"))' >/dev/null \
  || { echo ">> meta missing row timestamps: $SCIM_BODY"; exit 1; }
[[ "$META_LOC" == *"/Users/$ALICE_ID" ]] || { echo ">> meta.location = $META_LOC, want …/Users/$ALICE_ID"; exit 1; }

# Strict duplicate: same userName, no externalId → 409 uniqueness.
scim_req POST "/Users" '{"userName":"alice.corp"}'
[ "$SCIM_CODE" = "409" ] || { echo ">> duplicate POST = $SCIM_CODE, want 409 ($SCIM_BODY)"; exit 1; }
printf '%s' "$SCIM_BODY" | jq -e '.scimType == "uniqueness"' >/dev/null \
  || { echo ">> duplicate scimType = $(printf '%s' "$SCIM_BODY" | jq -r '.scimType'), want uniqueness"; exit 1; }

# --- filtered list -----------------------------------------------------------
log ">> GET /Users honors the filter subset (userName eq)"
scim_req GET "/Users?filter=userName%20eq%20%22alice.corp%22"
[ "$SCIM_CODE" = "200" ] || { echo ">> filtered list = $SCIM_CODE, want 200 ($SCIM_BODY)"; exit 1; }
TOT=$(printf '%s' "$SCIM_BODY" | jq -r '.totalResults')
[ "$TOT" = "1" ] || { echo ">> filtered totalResults = $TOT, want 1"; exit 1; }
GOT_UNAME=$(printf '%s' "$SCIM_BODY" | jq -r '.Resources[0].userName')
[ "$GOT_UNAME" = "alice.corp" ] || { echo ">> filtered Resources[0].userName = $GOT_UNAME, want alice.corp"; exit 1; }
# Unsupported filter form → 400 invalidFilter.
scim_req GET "/Users?filter=userName%20co%20ali"
[ "$SCIM_CODE" = "400" ] || { echo ">> unsupported filter = $SCIM_CODE, want 400 ($SCIM_BODY)"; exit 1; }
printf '%s' "$SCIM_BODY" | jq -e '.scimType == "invalidFilter"' >/dev/null \
  || { echo ">> filter scimType = $(printf '%s' "$SCIM_BODY" | jq -r '.scimType'), want invalidFilter"; exit 1; }

# --- deactivation suspends login ---------------------------------------------
# (Leg honesty: provisioned users are SSO-only, so the password channel
# refuses them REGARDLESS of active state — deactivation compounds the
# suspension; the admission gate (F-28.4 S3) is what operationalizes the
# active-bit for the SSO channel. The control leg proves the API itself is
# healthy.)
log ">> PATCH active:false deactivates; login is refused on the enforcing API"
scim_req PATCH "/Users/$ALICE_ID" '{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"operations":[{"op":"replace","path":"/active","value":false}]}'
[ "$SCIM_CODE" = "200" ] || { echo ">> PATCH deactivate = $SCIM_CODE, want 200 ($SCIM_BODY)"; exit 1; }
printf '%s' "$SCIM_BODY" | jq -e '.active == false' >/dev/null \
  || { echo ">> active after PATCH = $(printf '%s' "$SCIM_BODY" | jq -r '.active'), want false"; exit 1; }
LOGIN_DENIED=$(curl -s --max-time 10 -o /dev/null -w '%{http_code}' -X POST "$RBAC/api/login" \
  -H 'Content-Type: application/json' -d '{"username":"alice.corp","password":"anything"}' || true)
[ "$LOGIN_DENIED" != "200" ] || { echo ">> deactivated user LOGGED IN (200), want refusal"; exit 1; }
# Control: the admin still logs in.
CTRL_LOGIN=$(curl -s --max-time 10 -o /dev/null -w '%{http_code}' -X POST "$RBAC/api/login" \
  -H 'Content-Type: application/json' -d "{\"username\":\"$E2E_USER_ADA_USERNAME\",\"password\":\"$E2E_USER_ADA_PASSWORD\"}" || true)
[ "$CTRL_LOGIN" = "200" ] || { echo ">> admin control login = $CTRL_LOGIN, want 200"; exit 1; }

# --- deprovision + rehire -----------------------------------------------------
log ">> DELETE deprovisions (204); item ops 404; re-POST revives the SAME id"
scim_req DELETE "/Users/$ALICE_ID"
[ "$SCIM_CODE" = "204" ] || { echo ">> DELETE = $SCIM_CODE, want 204 ($SCIM_BODY)"; exit 1; }
scim_req GET "/Users/$ALICE_ID"
[ "$SCIM_CODE" = "404" ] || { echo ">> GET tombstone = $SCIM_CODE, want 404 ($SCIM_BODY)"; exit 1; }
printf '%s' "$SCIM_BODY" | jq -e '.scimType == "notFound"' >/dev/null \
  || { echo ">> tombstone scimType = $(printf '%s' "$SCIM_BODY" | jq -r '.scimType'), want notFound"; exit 1; }
scim_req POST "/Users" '{"userName":"alice.corp","externalId":"az-alice","name":{"givenName":"Alice","familyName":"Returneth"}}'
[ "$SCIM_CODE" = "200" ] || { echo ">> rehire POST = $SCIM_CODE, want 200 ($SCIM_BODY)"; exit 1; }
NEW_ID=$(printf '%s' "$SCIM_BODY" | jq -r '.id')
[ "$NEW_ID" = "$ALICE_ID" ] || { echo ">> rehire id = $NEW_ID, want the SAME $ALICE_ID"; exit 1; }
printf '%s' "$SCIM_BODY" | jq -e '.active == true' >/dev/null \
  || { echo ">> rehire active = $(printf '%s' "$SCIM_BODY" | jq -r '.active'), want true"; exit 1; }

# --- audit: every mutation attributed to the presenting connector -------------
# The MAIN API runs with auth OFF (synthetic admin), so its /api/audit reads
# the SHARED database audit log (the same one the IdP replica wrote to).
log ">> the durable audit log carries the SCIM mutations (connector-attributed)"
AUDIT_OK=no
for i in $(seq 1 10); do
  PROV=$(curl -s --max-time 10 "http://127.0.0.1:8080/api/audit?action=scim.provision" || true)
  REP=$(curl -s --max-time 10 "http://127.0.0.1:8080/api/audit?action=scim.reprovision" || true)
  DEP=$(curl -s --max-time 10 "http://127.0.0.1:8080/api/audit?action=scim.deprovision" || true)
  DEA=$(curl -s --max-time 10 "http://127.0.0.1:8080/api/audit?action=scim.deactivate" || true)
  if printf '%s' "$PROV" | jq -e 'type=="array" and length>=1 and (.[0].actor|startswith("SCIM connector"))' >/dev/null 2>&1 \
     && printf '%s' "$REP" | jq -e 'type=="array" and length>=1 and (.[0].actor|startswith("SCIM connector"))' >/dev/null 2>&1 \
     && printf '%s' "$DEP" | jq -e 'type=="array" and length>=1' >/dev/null 2>&1 \
     && printf '%s' "$DEA" | jq -e 'type=="array" and length>=1' >/dev/null 2>&1; then
    AUDIT_OK=yes
    break
  fi
  sleep 0.3
done
[ "$AUDIT_OK" = "yes" ] \
  || { echo ">> SCIM audit events missing/misinattributed (provision: ${PROV:0:200}; reprovision: ${REP:0:200})"; exit 1; }

# ============================================================================
# Slice 3 — the admission gate (JIT vs SCIM modes, verify-per-request)
# ============================================================================
# Two MODE HOSTS share the stack's database (hence the same user table):
#   - api-rbac  (:8081) ⇢ the STACK's IdP (:7104, SCIM DISABLED) ⇒ JIT mode
#   - api-u     (:8083) ⇢ the peppered replica (:7116, SCIM ENABLED) ⇒ SCIM mode
# The dev shim (GET /auth?user=X&roles=A,B) STANDS IN FOR A CORPORATE ISSUER:
# it signs a token for X bearing simulated group claims — the built-in IdP
# never signs for unknown subjects on its own, so this is the e2e's only
# foreign-token vector. api-rbac's issuer (127.0.0.1:7104) matches the shim
# token's iss (the shim echoes its request host).
log ">> admission gate: JIT mode (SCIM disabled) auto-provisions on first sight"
# shim_token <user> [<comma-roles>] — run the dev shim's authorization-code
# flow against the STACK's IdP (SCIM disabled ⇒ JIT mode) and print the
# ACCESS TOKEN. The ?roles= param simulates a CORPORATE issuer's group claims
# (the built-in IdP never signs for unknown subjects on its own, so this is
# the e2e's only foreign-token vector). The shim token's iss echoes its
# request host (127.0.0.1:7104) — api-rbac's configured issuer. The probe
# route is GET /api/pipelines (the stock "user" role holds
# pipelines.can-view — the gate's 403s are distinguished from RBAC's by
# BODY TEXT, so the gate and RBAC layers are told apart).
shim_token() {
  local user="$1" roles="${2:-}" qs location code_val tok_raw tok
  qs="user=$user&redirect_uri=http://127.0.0.1:8081/api/auth/callback&client_id=cdrom-ui"
  [ -n "$roles" ] && qs="$qs&roles=$roles"
  location=$(curl -s --max-time 10 -o /dev/null -w '%{redirect_url}' "http://127.0.0.1:7104/auth?$qs")
  code_val=$(printf '%s' "$location" | sed -n 's/.*code=\([^&]*\).*/\1/p')
  [ -n "$code_val" ] || { echo ">> shim redirect lacked a code: $location"; exit 1; }
  tok_raw=$(curl -s --max-time 10 -X POST "http://127.0.0.1:7104/token" \
    -d "grant_type=authorization_code&code=$code_val&client_id=cdrom-ui&redirect_uri=http://127.0.0.1:8081/api/auth/callback")
  tok=$(printf '%s' "$tok_raw" | jq -r '.access_token // empty')
  [ -n "$tok" ] || { echo ">> shim token exchange failed: $tok_raw"; exit 1; }
  printf '%s' "$tok"
}
gate_probe() { # <api-base> <token> → sets GP_CODE / GP_BODY
  local out
  out=$(curl -s --max-time 10 -w $'\n%{http_code}' "$1/api/pipelines" -H "Authorization: Bearer $2" || true)
  GP_CODE=$(printf '%s' "$out" | tail -n 1)
  GP_BODY=$(printf '%s' "$out" | sed '$d')
}
JIT_TOK=$(shim_token "jit.alice" "user")
gate_probe "$RBAC" "$JIT_TOK"
[ "$GP_CODE" = "200" ] || { echo ">> JIT first sight = $GP_CODE, want 200 (auto-provisioned + admitted) [$GP_BODY]"; exit 1; }
# The row EXISTS now (visible through the SCIM surface's list — same table).
scim_req GET "/Users?filter=userName%20eq%20%22jit.alice%22"
[ "$SCIM_CODE" = "200" ] || { echo ">> JIT row list = $SCIM_CODE, want 200 ($SCIM_BODY)"; exit 1; }
printf '%s' "$SCIM_BODY" | jq -e '.totalResults == 1 and .Resources[0].userName == "jit.alice"' >/dev/null \
  || { echo ">> JIT row missing/altered: $SCIM_BODY"; exit 1; }
# Return visit: admitted again (monotonic — the claim set is a subset, so the
# birth grants RETAIN; nothing is stripped).
gate_probe "$RBAC" "$JIT_TOK"
[ "$GP_CODE" = "200" ] || { echo ">> JIT return visit = $GP_CODE, want 200 [$GP_BODY]"; exit 1; }

log ">> admission gate: JIT with ZERO mapped roles is denied — and creates NO row"
ZERO_TOK=$(shim_token "zero.claim")
gate_probe "$RBAC" "$ZERO_TOK"
[ "$GP_CODE" = "403" ] || { echo ">> zero-roles = $GP_CODE, want 403 (no_roles_rejected) [$GP_BODY]"; exit 1; }
printf '%s' "$GP_BODY" | grep -q "no roles are mapped" \
  || { echo ">> zero-roles 403 lacks the gate's distinct text: $GP_BODY"; exit 1; }
scim_req GET "/Users?filter=userName%20eq%20%22zero.claim%22"
printf '%s' "$SCIM_BODY" | jq -e '.totalResults == 0' >/dev/null \
  || { echo ">> zero-roles denial left a residual row: $SCIM_BODY"; exit 1; }

log ">> admission gate: SCIM mode denies an unknown subject (no JIT)"
# phantom.unknown has NO row (unlike jit.alice, which JIT-created) — the
# peppered replica (SCIM ENABLED) must refuse it outright.
PHANTOM_TOK=$(shim_token "phantom.unknown" "user")
gate_probe "$API_U" "$PHANTOM_TOK"
[ "$GP_CODE" = "403" ] || { echo ">> SCIM-mode unknown = $GP_CODE, want 403 (unknown_rejected) [$GP_BODY]"; exit 1; }
printf '%s' "$GP_BODY" | grep -q "not recognized" \
  || { echo ">> SCIM-mode 403 lacks the gate's distinct text: $GP_BODY"; exit 1; }
scim_req GET "/Users?filter=userName%20eq%20%22phantom.unknown%22"
printf '%s' "$SCIM_BODY" | jq -e '.totalResults == 0' >/dev/null \
  || { echo ">> SCIM-mode denial left a residual row: $SCIM_BODY"; exit 1; }

log ">> admission gate: a SCIM deactivation BITES THE NEXT LOGIN (verify-per-request)"
# alice.corp is alive + active (rehired in the lifecycle leg). Her shim token
# is admitted, then SCIM deactivates her, then the SAME token is refused
# IMMEDIATELY (no cache grace period).
HIRER_TOK=$(shim_token "alice.corp" "user")
gate_probe "$API_U" "$HIRER_TOK"
[ "$GP_CODE" = "200" ] || { echo ">> pre-deactivation admit = $GP_CODE, want 200 [$GP_BODY]"; exit 1; }
scim_req GET "/Users?filter=userName%20eq%20%22alice.corp%22"
HIRER_ID=$(printf '%s' "$SCIM_BODY" | jq -r '.Resources[0].id // empty')
[ -n "$HIRER_ID" ] || { echo ">> could not locate alice.corp's id: $SCIM_BODY"; exit 1; }
scim_req PATCH "/Users/$HIRER_ID" '{"operations":[{"op":"replace","path":"/active","value":false}]}'
[ "$SCIM_CODE" = "200" ] || { echo ">> SCIM deactivate = $SCIM_CODE, want 200 ($SCIM_BODY)"; exit 1; }
gate_probe "$API_U" "$HIRER_TOK"
[ "$GP_CODE" = "403" ] || { echo ">> post-deactivation admit = $GP_CODE, want 403 (bite NOW — no cache) [$GP_BODY]"; exit 1; }
printf '%s' "$GP_BODY" | grep -q "deactivated" \
  || { echo ">> deactivation 403 lacks the gate's distinct text: $GP_BODY"; exit 1; }
# Restore her (tidiness).
scim_req PATCH "/Users/$HIRER_ID" '{"operations":[{"op":"replace","path":"/active","value":true}]}'
[ "$SCIM_CODE" = "200" ] || { echo ">> restore-active = $SCIM_CODE, want 200"; exit 1; }

log ">> admission gate: RBAC denial is DISTINGUISHABLE from a gate denial"
# NEGATIVE CONTROL: a known user (ada) with a role that grants nothing — the
# gate ADMITS her (she exists + is active), THEN RBAC refuses. The 403 body
# must lack every gate phrase (layer separation proven).
CONTROL_TOK=$(shim_token "$E2E_USER_ADA_USERNAME" "voidrole")
gate_probe "$RBAC" "$CONTROL_TOK"
[ "$GP_CODE" = "403" ] || { echo ">> RBAC control = $GP_CODE, want 403 [$GP_BODY]"; exit 1; }
if printf '%s' "$GP_BODY" | grep -qiE "no roles are mapped|not recognized|deactivated|deprovisioned"; then
  echo ">> RBAC 403 mimicked a gate denial: $GP_BODY"; exit 1
fi

log ">> admission gate: IdP down ⇒ FAIL CLOSED (503, not a disguised denial)"
# Stop the peppered replica (api-u's IdP); its lazy gRPC dial then fails and
# the gate must refuse with 503 — never a 403 (a down gate is not a denial).
kill "$idpu_pid" 2>/dev/null || true
wait_dead=0
while kill -0 "$idpu_pid" 2>/dev/null && [ "$wait_dead" -lt 25 ]; do
  sleep 0.2; wait_dead=$((wait_dead + 1))
done
gate_probe "$API_U" "$HIRER_TOK"
[ "$GP_CODE" = "503" ] || { echo ">> IdP-down admit = $GP_CODE, want 503 (fail closed) [$GP_BODY]"; exit 1; }

log ">> 22-scim-users PASSED"
