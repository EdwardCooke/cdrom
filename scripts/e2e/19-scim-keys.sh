#!/usr/bin/env bash
#
# 19-scim-keys.sh — F-28.2 slice 2: the SCIM key pool's administrative API.
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
# Usage: scripts/e2e/19-scim-keys.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

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
ADMIN_TOKEN=$(e2e_login "$RBAC" "$E2E_USER_ADA_EMAIL" "$E2E_USER_ADA_PASSWORD")
USER_TOKEN=$(e2e_login "$RBAC" "$E2E_USER_BOB_EMAIL" "$E2E_USER_BOB_PASSWORD")

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

log ">> 19-scim-keys PASSED"
