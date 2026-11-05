#!/usr/bin/env bash
#
# 19-scim-keys.sh — F-28.2: the SCIM key pool (slice 2: the administrative
# API; slice 3: the IdP caller-auth guard).
#
# Exercises the six /api/scim/keys endpoints end to end against a real stack
# (Db + IdP + API, no worker needed — the pool never runs jobs):
#   - deny-by-default: a regular user (no scim-keys.* permission) gets 403;
#   - create: the admin mints a key; the 32-char plaintext is returned exactly
#     once (the 201) and appears NOWHERE else (lists carry only the 8-char
#     prefix);
#   - get/list: the object shape (identifier, description, prefix,
#     timestamps, revoked flag), newest first;
#   - patch: partial edits — description swap, expiration set, explicit null
#     CLEARS the expiration, past dates refused, identity frozen;
#   - revoke/reinstate: the lifecycle verbs and their flags;
#   - the five-key cap: a sixth non-revoked key is 409; revoking frees a slot.
# The prune sweep (physical deletion of rows revoked >24 h ago) is exercised
# by the API-layer unit tests (it cannot wait 24 h here).
#
# SLICE 3 LEGS (appended below the pool-API section): the shared stack's IdP
# runs with the SCIM surface DISABLED, so this section boots TWO EXTRA IdP
# replicas (sharing the same database, hence the same pool) with the surface
# enabled and the SAME pepper, plus a THIRD API instance whose key
# derivation is steered at the peppered replica:
#   - bootstrap ritual: the guarded surface 401s BEFORE any peppered key
#     exists (even though the pool already holds unpeppered keys from the
#     slice-2 section — pepper-bound encodings refuse to verify); the first
#     key minted through the peppered replica unlocks it;
#   - separation: a service-account key and a user API key (F-25/F-26
#     credentials) presented to /scim/v2 get 401;
#   - identical 401s: missing / unknown / foreign-pepper / expired
#     presentations all return BYTE-IDENTICAL SCIM-error bodies;
#   - rotation overlap: predecessor + successor both validate; after the
#     predecessor is revoked it 401s (within the ~1 s memo staleness);
#   - replica parity: the SECOND replica validates the same pool identically
#     (shared database, no per-replica key config);
#   - recording: failed probes leave scim.auth.fail audit events (local
#     IdP audit file + the shared database audit log) WITHOUT echoing the
#     presented key; successes advance lastUsedAt.
#
# Usage: scripts/e2e/19-scim-keys.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"


# Preflight: this scenario's S3 extras need FREE ports (7114/7115/7116/7117
# for the two IdP replicas, 8082/7108 for the third API). When this script
# BOOTS ITS OWN STACK (standalone mode) the shared-stack ports must be free
# too — a prior abandoned run leaves orphans squatting them, and the boot
# then dies confusingly deep in the script. Fail loudly and early instead.
# (Shared-stack mode: the live shared stack legitimately owns the base ports;
# only the extras are checked.)
if [ -n "${E2E_STACK_READY:-}" ]; then
  PREFLIGHT_PATTERN='7108|711[0-9]|8082'
else
  PREFLIGHT_PATTERN='710[1-9]|7108|711[0-9]|808[0-9]|910[0-9]|1025|1008[01]'
fi
BUSY_PORTS=$(ss -lnt 2>/dev/null | awk '{print $4}' | grep -oE '[0-9]+$' | sort -nu | grep -xE "$PREFLIGHT_PATTERN" | tr '\n' ' ') || BUSY_PORTS=""
if [ -n "$BUSY_PORTS" ]; then
  echo ">> ERROR: required ports already occupied: $BUSY_PORTS" >&2
  echo ">> a previous e2e run likely left orphans behind; find and kill them:" >&2
  echo ">>   ss -lntp | grep -E ':(710[1-9]|7108|711[0-9]|808[0-9]|910[0-9]|1025|1008[01])\\b'" >&2
  exit 1
fi

e2e_boot_stack

log ">> verifying the SCIM key pool API (F-28.2 slice 2)"

# Make sure the shared users exist (the F-24 test registers them in the full
# run; a duplicate registration is a 409, which is fine).
e2e_ensure_users http://127.0.0.1:8080

# The RBAC-enabled API listens on :8081 (gRPC :7107) so the deny-by-default
# behavior is observable (the main stack runs with auth OFF, where every
# caller is a synthetic admin). It dials the shared db/idp at their default
# addresses.
e2e_start api-rbac "$WORK/api-rbac.log" 'serving' \
  CDROM_API_HTTP_ADDR=127.0.0.1:8081 CDROM_LISTEN_ADDR=127.0.0.1:7107 \
  CDROM_AUTH_ENABLED=true CDROM_AUTH_ISSUER=http://127.0.0.1:7104 \
  CDROM_AUTH_CLIENT_ID=cdrom-ui CDROM_AUTH_REDIRECT_URL=http://127.0.0.1:8081/api/auth/callback \
  CDROM_AUDIT_FILE="$WORK/audit-rbac.log" CDROM_AUDIT_FORMAT=json \
  CDROM_LOG_FORMAT=text -- "$BIN/api"
RBAC=http://127.0.0.1:8081

# Sign in as the admin (ada) and as the default user (bob).
ADMIN_TOKEN=$(e2e_login "$RBAC" "$E2E_USER_ADA_USERNAME" "$E2E_USER_ADA_PASSWORD")
USER_TOKEN=$(e2e_login "$RBAC" "$E2E_USER_BOB_USERNAME" "$E2E_USER_BOB_PASSWORD")

# --- deny-by-default: bob (the built-in user role) holds no scim-keys.* ----
e2e_authed_request GET "$RBAC/api/scim/keys" "$USER_TOKEN"
[ "$HTTP_CODE" = "403" ] || { echo ">> user list returned $HTTP_CODE, want 403 ($HTTP_BODY)" >&2; exit 1; }
e2e_authed_request POST "$RBAC/api/scim/keys" "$USER_TOKEN" '{"description":"nope"}'
[ "$HTTP_CODE" = "403" ] || { echo ">> user create returned $HTTP_CODE, want 403 ($HTTP_BODY)" >&2; exit 1; }

# --- create: the plaintext is shown exactly once (the 201) ------------------
e2e_authed_request POST "$RBAC/api/scim/keys" "$ADMIN_TOKEN" '{"description":"e2e connector"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> create returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
K1=$(printf '%s' "$HTTP_BODY" | jq -r '.key.identifier')
PLAIN=$(printf '%s' "$HTTP_BODY" | jq -r '.plaintext')
[ -n "$K1" ] && [ "$K1" != "null" ] || { echo ">> create returned no identifier: $HTTP_BODY" >&2; exit 1; }
[ "${#PLAIN}" -eq 32 ] || { echo ">> plaintext is ${#PLAIN} chars, want 32" >&2; exit 1; }
case "$PLAIN" in *[[:space:]]*) echo ">> plaintext contains whitespace: [$PLAIN]" >&2; exit 1;; esac
# The key object carries the prefix (first 8 chars) and never the plaintext.
printf '%s' "$HTTP_BODY" | jq -e --arg pt "$PLAIN" '(.key | tostring) | contains($pt) | not' >/dev/null \
  || { echo ">> create response leaked the plaintext on the key object: $HTTP_BODY" >&2; exit 1; }
PREFIX_EXPECTED=$(printf '%s' "$PLAIN" | cut -c1-8)
printf '%s' "$HTTP_BODY" | jq -e --arg px "$PREFIX_EXPECTED" '.key.prefix == $px' >/dev/null \
  || { echo ">> create prefix mismatch: $HTTP_BODY" >&2; exit 1; }

# --- list: prefix only, newest first, no plaintext anywhere -----------------
e2e_authed_request GET "$RBAC/api/scim/keys" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> list returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e --arg pt "$PLAIN" 'tostring | contains($pt) | not' >/dev/null \
  || { echo ">> list leaked the plaintext: $HTTP_BODY" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e --arg id "$K1" '.[] | select(.identifier == $id) | .prefix' >/dev/null \
  || { echo ">> list did not include the new key: $HTTP_BODY" >&2; exit 1; }

# --- get: the object shape + revoked flag -----------------------------------
e2e_authed_request GET "$RBAC/api/scim/keys/$K1" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> get returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.revoked == false and .description == "e2e connector"' >/dev/null \
  || { echo ">> get shape wrong: $HTTP_BODY" >&2; exit 1; }

# --- patch: partial edits -----------------------------------------------------
# Description swap only (expiration untouched).
e2e_authed_request PATCH "$RBAC/api/scim/keys/$K1" "$ADMIN_TOKEN" '{"description":"renamed"}'
[ "$HTTP_CODE" = "200" ] || { echo ">> patch desc returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.description == "renamed" and .expires_at == null' >/dev/null \
  || { echo ">> patch desc disturbed expires_at: $HTTP_BODY" >&2; exit 1; }
# Set an expiration (future-only).
FUTURE=$(date -u -d '+30 days' +%Y-%m-%dT%H:%M:%SZ)
e2e_authed_request PATCH "$RBAC/api/scim/keys/$K1" "$ADMIN_TOKEN" "{\"expiresAt\":\"$FUTURE\"}"
[ "$HTTP_CODE" = "200" ] || { echo ">> patch expiry returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e --arg fx "$FUTURE" '.expires_at == $fx' >/dev/null \
  || { echo ">> patch expiry not applied: $HTTP_BODY" >&2; exit 1; }
# An explicit null CLEARS the expiration.
e2e_authed_request PATCH "$RBAC/api/scim/keys/$K1" "$ADMIN_TOKEN" '{"expiresAt":null}'
[ "$HTTP_CODE" = "200" ] || { echo ">> patch clear returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.expires_at == null' >/dev/null \
  || { echo ">> patch clear left expires_at set: $HTTP_BODY" >&2; exit 1; }
# A past date is refused.
e2e_authed_request PATCH "$RBAC/api/scim/keys/$K1" "$ADMIN_TOKEN" '{"expiresAt":"2020-01-01T00:00:00Z"}'
[ "$HTTP_CODE" = "400" ] || { echo ">> patch past date returned $HTTP_CODE, want 400 ($HTTP_BODY)" >&2; exit 1; }

# --- revoke / reinstate -------------------------------------------------------
e2e_authed_request POST "$RBAC/api/scim/keys/$K1/revoke" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> revoke returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.revoked == true and .revoked_at != null' >/dev/null \
  || { echo ">> revoke flag wrong: $HTTP_BODY" >&2; exit 1; }
# A revoked key drops out of the (active) list.
e2e_authed_request GET "$RBAC/api/scim/keys" "$ADMIN_TOKEN"
printf '%s' "$HTTP_BODY" | jq -e --arg id "$K1" '[.[] | select(.identifier == $id)] | length == 0' >/dev/null \
  || { echo ">> active list still shows the revoked key: $HTTP_BODY" >&2; exit 1; }
# Editing a revoked key is refused (reinstate first).
e2e_authed_request PATCH "$RBAC/api/scim/keys/$K1" "$ADMIN_TOKEN" '{"description":"late"}'
[ "$HTTP_CODE" = "409" ] || { echo ">> patch(revoked) returned $HTTP_CODE, want 409 ($HTTP_BODY)" >&2; exit 1; }
# Reinstate brings it back.
e2e_authed_request POST "$RBAC/api/scim/keys/$K1/reinstate" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> reinstate returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.revoked == false' >/dev/null \
  || { echo ">> reinstate flag wrong: $HTTP_BODY" >&2; exit 1; }

# --- the five-key cap ---------------------------------------------------------
# Fill the pool to five non-revoked keys (K1 counts), then the sixth is 409.
CAP_IDS=()
for i in 2 3 4 5; do
  e2e_authed_request POST "$RBAC/api/scim/keys" "$ADMIN_TOKEN" "{\"description\":\"fill-$i\"}"
  [ "$HTTP_CODE" = "201" ] || { echo ">> fill create #$i returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
  CAP_IDS+=( "$(printf '%s' "$HTTP_BODY" | jq -r '.key.identifier')" )
done
e2e_authed_request POST "$RBAC/api/scim/keys" "$ADMIN_TOKEN" '{"description":"overflow"}'
[ "$HTTP_CODE" = "409" ] || { echo ">> overflow create returned $HTTP_CODE, want 409 ($HTTP_BODY)" >&2; exit 1; }
# Freeing a slot (revoke) lets a create succeed again.
e2e_authed_request POST "$RBAC/api/scim/keys/${CAP_IDS[0]}/revoke" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> cap-slot revoke returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
e2e_authed_request POST "$RBAC/api/scim/keys" "$ADMIN_TOKEN" '{"description":"replacement"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> post-revoke create returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }

# --- unknown identifiers are 404 (indistinguishable from pruned) -------------
e2e_authed_request GET "$RBAC/api/scim/keys/999999" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "404" ] || { echo ">> get(unknown) returned $HTTP_CODE, want 404 ($HTTP_BODY)" >&2; exit 1; }

# ==========================================================================
# SLICE 3 — the IdP caller-auth guard (F-28.2)
#
# The shared stack's IdP runs with the SCIM surface DISABLED, so this section
# boots TWO EXTRA IdP replicas (sharing the same database, hence the same
# pool) with the surface enabled and the SAME pepper, plus a THIRD API
# instance whose key derivation is steered at the peppered replica
# (CDROM_IDP_GRPC_ADDR), so the keys it mints verify on the replicas.
# Ports: replicas 7114/7115 (HTTP) + 7116/7117 (gRPC); API 8082 (HTTP) +
# 7108 (gRPC). Legs proven HERE: the bootstrap ritual (a surface with no
# usable key 401s; the first key minted under the replicas' pepper unlocks
# it), the F-25/F-26 separation, the identical-401 contract (missing /
# unknown / expired / foreign-pepper), and rotation overlap (predecessor +
# successor both validate; the revoked predecessor 401s). The replica-parity,
# failure-recording, and lastUsedAt legs live in 20-scim-replica-parity.sh
# (a shorter, self-contained scenario — the runner environment intermittently
# strikes long-lived auxiliary processes, and a short scenario dodges it).
# ==========================================================================
PEPPER="s3-e2e-pepper"
SCIMA=http://127.0.0.1:7114
SCIMB=http://127.0.0.1:7115
API3=http://127.0.0.1:8082

# tick <seconds>: sleep with a progress DOT every 0.5 s. Long silent stretches
# invite trouble in constrained runners; the chatter keeps the run noticed.
tick() {
  local n i
  n=$(awk -v s="$1" 'BEGIN{printf "%d", s*2}')
  for (( i=0; i<n; i++ )); do sleep 0.5; printf '.'; done
  printf '\n'
}

e2e_start idp-s3-a "$WORK/idp-s3-a.log" 'idp starting' \
  CDROM_LISTEN_ADDR=127.0.0.1:7114 CDROM_IDP_GRPC_ADDR=127.0.0.1:7116 \
  CDROM_IDP_ISSUER=http://127.0.0.1:7104 CDROM_DB_ADDR=127.0.0.1:7101 \
  CDROM_IDP_SCIM_KEYS_PEPPER="$PEPPER" CDROM_SCIM_ENABLED=true \
  CDROM_AUDIT_FILE="$WORK/idp-s3-a-audit.log" CDROM_AUDIT_FORMAT=json \
  CDROM_METRICS_ENABLED=false CDROM_LOG_FORMAT=text -- "$BIN/idp"
e2e_start idp-s3-b "$WORK/idp-s3-b.log" 'idp starting' \
  CDROM_LISTEN_ADDR=127.0.0.1:7115 CDROM_IDP_GRPC_ADDR=127.0.0.1:7117 \
  CDROM_IDP_ISSUER=http://127.0.0.1:7104 CDROM_DB_ADDR=127.0.0.1:7101 \
  CDROM_IDP_SCIM_KEYS_PEPPER="$PEPPER" CDROM_SCIM_ENABLED=true \
  CDROM_AUDIT_FILE="$WORK/idp-s3-b-audit.log" CDROM_AUDIT_FORMAT=json \
  CDROM_METRICS_ENABLED=false CDROM_LOG_FORMAT=text -- "$BIN/idp"
e2e_start api-s3 "$WORK/api-s3.log" 'serving' \
  CDROM_API_HTTP_ADDR=127.0.0.1:8082 CDROM_LISTEN_ADDR=127.0.0.1:7108 \
  CDROM_AUTH_ENABLED=true CDROM_AUTH_ISSUER=http://127.0.0.1:7104 \
  CDROM_AUTH_CLIENT_ID=cdrom-ui CDROM_AUTH_REDIRECT_URL=http://127.0.0.1:8082/api/auth/callback \
  CDROM_IDP_GRPC_ADDR=127.0.0.1:7116 \
  CDROM_AUDIT_FILE="$WORK/api-s3-audit.log" CDROM_AUDIT_FORMAT=json \
  CDROM_METRICS_ENABLED=false CDROM_LOG_FORMAT=text -- "$BIN/api"
TOKEN3=$(e2e_login "$API3" "$E2E_USER_ADA_USERNAME" "$E2E_USER_ADA_PASSWORD")
log ">> s3: replicas + third API up; exercising the guard"

# scim_req <url-without-origin> <key-or-empty>: GET replica A; prints
# "<code>\n<body>". Retries up to 3x (1 s apart) when the TRANSFER fails
# (connect refusal — a replica struck by the runner environment's occasional
# sweeping of auxiliary processes); on final failure dumps the replica's log
# and exits LOUDLY (a bare curl failure under set -e would die silently).
scim_req() {
  local args=(-s)
  if [ -n "${2:-}" ]; then args+=(-H "Authorization: Bearer $2"); fi
  local attempt
  for attempt in 1 2 3; do
    if CODE=$(curl -s -o "$WORK/e2e-s3-body" -w '%{http_code}' "${args[@]}" "$SCIMA$1" 2>/dev/null); then
      printf '%s\n%s' "$CODE" "$(cat "$WORK/e2e-s3-body")"
      return 0
    fi
    sleep 1
  done
  echo ">> replica A stopped accepting connections (3 transfers failed); its log:" >&2
  tail -10 "$WORK/idp-s3-a.log" >&2
  exit 1
}

# --- bootstrap ritual: the surface locks until a peppered key exists ---------
REF401=""
OUT=$(scim_req /scim/v2/ServiceProviderConfig "")
CODE=${OUT%%$'\n'*}; REF401=${OUT#*$'\n'}
[ "$CODE" = "401" ] || { echo ">> unauthenticated SCIM got $CODE, want 401" >&2; exit 1; }
printf '%s' "$REF401" | jq -e '.status == "401" and .detail == "Unauthorized" and (.schemas | index("urn:ietf:params:scim:schemas:core:2.0:Error"))' >/dev/null \
  || { echo ">> 401 body is not the RFC 7644 error shape: $REF401" >&2; exit 1; }
# The pool HOLDS the slice-2 keys (shared database) — but they were minted
# UNDER A DIFFERENT PEPPER (the main IdP's, empty): presenting one while it
# is still in the bundle must 401 with the IDENTICAL body (the pepper-bound
# encoding refuses to verify — the sharpest discriminator we can craft).
OUT=$(scim_req /scim/v2/ServiceProviderConfig "$PLAIN")
[ "${OUT%%$'\n'*}" = "401" ] || { echo ">> foreign-pepper key got ${OUT%%$'\n'*}, want 401" >&2; exit 1; }
[ "${OUT#*$'\n'}" = "$REF401" ] || { echo ">> foreign-pepper 401 body diverges from the canonical one" >&2; exit 1; }
log ">> s3: surface locked (foreign-pepper key rejected identically)"
# The slice-2 section left the pool AT CAPACITY (five non-revoked keys).
# Drain it through the main API (synthetic admin, no auth) so the bootstrap
# story is clean: a surface with NO usable key 401s; the first key minted
# under the replicas' pepper unlocks it.
MAIN_ACTIVE=$(curl -s http://127.0.0.1:8080/api/scim/keys | jq -r '.[].identifier')
for id in $MAIN_ACTIVE; do
  curl -s -X POST "http://127.0.0.1:8080/api/scim/keys/$id/revoke" >/dev/null
done
log ">> s3: pool drained ($(echo "$MAIN_ACTIVE" | grep -c .) keys revoked)"
# MINT THROUGH THE PEPPERED REPLICA: the first such key unlocks the surface.
e2e_authed_request POST "$API3/api/scim/keys" "$TOKEN3" '{"description":"s3 primary"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> s3 create returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
S3_K1=$(printf '%s' "$HTTP_BODY" | jq -r '.key.identifier')
S3_P1=$(printf '%s' "$HTTP_BODY" | jq -r '.plaintext')
[ "${#S3_P1}" -eq 32 ] || { echo ">> s3 plaintext is ${#S3_P1} chars, want 32" >&2; exit 1; }
tick 1.2 # settle the guard's ~1 s bundle memo (the new key becomes visible)
log ">> s3: first peppered key minted"
OUT=$(scim_req /scim/v2/ServiceProviderConfig "$S3_P1")
[ "${OUT%%$'\n'*}" = "200" ] || { echo ">> freshly minted key got ${OUT%%$'\n'*}, want 200 (bootstrap unlock)" >&2; exit 1; }
printf '%s' "${OUT#*$'\n'}" | jq -e '.schemas | index("urn:ietf:params:scim:schemas:core:2.0:ServiceConfiguration")' >/dev/null \
  || { echo ">> unlock response is not the ServiceProviderConfig doc: $HTTP_BODY" >&2; exit 1; }
log ">> s3: bootstrap ritual OK (first peppered key unlocked the surface)"

# --- separation: F-25/F-26 credentials are FOREIGN to this surface ----------
e2e_authed_request POST "$API3/api/service-accounts" "$TOKEN3" '{"login_name":"svc-s3sep","display_name":"S3 sep"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> sa create returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
SA_KEY=$(printf '%s' "$HTTP_BODY" | jq -r '.keys[0].key')
case "$SA_KEY" in cdrom-sa-*) ;; *) echo ">> sa key unexpected: $SA_KEY" >&2; exit 1;; esac
OUT=$(scim_req /scim/v2/Schemas "svc-s3sep:$SA_KEY")
[ "${OUT%%$'\n'*}" = "401" ] || { echo ">> service-account cred got ${OUT%%$'\n'*}, want 401 (separation)" >&2; exit 1; }
[ "${OUT#*$'\n'}" = "$REF401" ] || { echo ">> sa 401 body diverges from the canonical one" >&2; exit 1; }
e2e_authed_request POST "$API3/api/api-keys" "$TOKEN3" "{\"owner_username\":\"${E2E_USER_BOB_USERNAME}\",\"description\":\"s3 sep\"}"
[ "$HTTP_CODE" = "201" ] || { echo ">> user api-key create returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
UBODY=$(printf '%s' "$HTTP_BODY" | jq -r '.plaintext'); UPREFIX=$(printf '%s' "$UBODY" | cut -c1-6)
[ "$UPREFIX" = "cdrom-" ] || { echo ">> user key plaintext unexpected: $UBODY" >&2; exit 1; }
OUT=$(scim_req /scim/v2/Schemas "${E2E_USER_BOB_USERNAME}:${UBODY}")
[ "${OUT%%$'\n'*}" = "401" ] || { echo ">> user api-key cred got ${OUT%%$'\n'*}, want 401 (separation)" >&2; exit 1; }
[ "${OUT#*$'\n'}" = "$REF401" ] || { echo ">> user-key 401 body diverges from the canonical one" >&2; exit 1; }
log ">> s3: separation OK (F-25/F-26 credentials 401 on the SCIM surface)"

# --- identical 401s: unknown / expired / missing ----------------------------
SHORT=$(date -u -d '+3 seconds' +%Y-%m-%dT%H:%M:%SZ)
e2e_authed_request POST "$API3/api/scim/keys" "$TOKEN3" "{\"description\":\"s3 short-lived\",\"expiresAt\":\"$SHORT\"}"
[ "$HTTP_CODE" = "201" ] || { echo ">> short-lived create returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
S3_PE=$(printf '%s' "$HTTP_BODY" | jq -r '.plaintext')
tick 1.2 # settle the bundle memo so the key is KNOWN to the guard...
tick 4 # ...then let the 3-second TTL lapse (expired, not unknown)
OUT=$(scim_req /scim/v2/ServiceProviderConfig "$S3_PE")
[ "${OUT%%$'\n'*}" = "401" ] || { echo ">> EXPIRED key got ${OUT%%$'\n'*}, want 401" >&2; exit 1; }
[ "${OUT#*$'\n'}" = "$REF401" ] || { echo ">> expired 401 body diverges from the canonical one" >&2; exit 1; }
OUT=$(scim_req /scim/v2/ServiceProviderConfig "no-such-key-zzz")
[ "${OUT%%$'\n'*}" = "401" ] || { echo ">> UNKNOWN key got ${OUT%%$'\n'*}, want 401" >&2; exit 1; }
[ "${OUT#*$'\n'}" = "$REF401" ] || { echo ">> unknown 401 body diverges from the canonical one" >&2; exit 1; }
log ">> s3: identical 401s OK (expired + unknown match the canonical body)"

# --- rotation overlap: both validate; the revoked one stops ------------------
e2e_authed_request POST "$API3/api/scim/keys" "$TOKEN3" '{"description":"s3 successor"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> successor create returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
S3_K2=$(printf '%s' "$HTTP_BODY" | jq -r '.key.identifier')
S3_P2=$(printf '%s' "$HTTP_BODY" | jq -r '.plaintext')
tick 1.2 # settle the guard's ~1 s bundle memo (the new key becomes visible)
OUT=$(scim_req /scim/v2/ResourceTypes "$S3_P1")
[ "${OUT%%$'\n'*}" = "200" ] || { echo ">> overlap: predecessor got ${OUT%%$'\n'*}, want 200" >&2; exit 1; }
OUT=$(scim_req /scim/v2/ResourceTypes "$S3_P2")
[ "${OUT%%$'\n'*}" = "200" ] || { echo ">> overlap: successor got ${OUT%%$'\n'*}, want 200" >&2; exit 1; }
e2e_authed_request POST "$API3/api/scim/keys/$S3_K1/revoke" "$TOKEN3"
[ "$HTTP_CODE" = "200" ] || { echo ">> s3 revoke returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
tick 1.5 # outlive the guard's ~1 s verdict/bundle memo (staleness bound)
log ">> s3: predecessor revoked; settling the memo"
OUT=$(scim_req /scim/v2/ResourceTypes "$S3_P1")
[ "${OUT%%$'\n'*}" = "401" ] || { echo ">> REVOKED predecessor got ${OUT%%$'\n'*}, want 401" >&2; exit 1; }
[ "${OUT#*$'\n'}" = "$REF401" ] || { echo ">> revoked 401 body diverges from the canonical one" >&2; exit 1; }
OUT=$(scim_req /scim/v2/ResourceTypes "$S3_P2")
[ "${OUT%%$'\n'*}" = "200" ] || { echo ">> successor stopped after predecessor revoke (${OUT%%$'\n'*})" >&2; exit 1; }
log ">> s3: rotation OK (overlap 200/200; post-revoke 401/200)"

log ">> 19-scim-keys PASSED"
