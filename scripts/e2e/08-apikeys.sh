#!/usr/bin/env bash
#
# 08-apikeys.sh — F-25: API keys.
#
# Boots the stack (no worker needed — API keys do not run jobs) and a SECOND
# API instance with OIDC authentication + RBAC + API-key lockout enabled,
# sharing the same IdP (users + JWKS) and db/scheduler/artifacts as the main
# stack. The main stack runs with OIDC auth OFF, so its API is open; the
# RBAC-enabled API is where the API-key management and authentication
# behavior is exercised (it is the only surface where RBAC distinguishes an
# admin — who may list every key in the system — from a regular user, who
# sees only their own keys).
#
# API keys are `cdrom-…` credentials that authenticate a user to the API in
# place of a JWT, presented as `Authorization: Bearer <username>:<apikey>`.
# The real logic (key generation, hashing, verification, and lockout) lives
# in the IdP; the API is a thin proxy. This script verifies, end to end:
#   - an admin can create a key for another user (the plaintext is returned
#     exactly once);
#   - an admin can list every key in the system, while a regular user sees
#     only their own;
#   - a user can authenticate with their API key (the middleware's API-key
#     branch, active even though OIDC is also on);
#   - a key's pipeline scope limits the caller's resource-scoped permissions
#     (in-scope allowed, out-of-scope 403);
#   - a wrong key is rejected (401) and repeated failures lock the user out,
#     until an admin resets the lockout;
#   - rotating a key issues a new plaintext (the old one stops working), and
#     deleting a key revokes it.
#
# The shared test users (ada@example.com, admin; bob@example.com, the default
# user role) are registered by the F-24 test in the full run; when this
# script runs standalone it registers them itself if they are not present.
#
# Usage: scripts/e2e/08-apikeys.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack

log ">> verifying API keys (F-25)"

# Make sure the shared users exist (the F-24 test registers them in the full
# run; a duplicate registration is a 409, which is fine).
e2e_ensure_users http://127.0.0.1:8080

# The RBAC-enabled API listens on :8081 (gRPC :7107) so it does not collide
# with the main API (:8080/:7105). It dials the shared db/scheduler/artifacts
# and IdP at their default addresses. API-key lockout is enabled (3 failures
# -> a 1h lockout) so the lockout + reset behavior can be exercised.
e2e_start api-rbac "$WORK/api-rbac.log" 'serving' \
  CDROM_API_HTTP_ADDR=127.0.0.1:8081 CDROM_LISTEN_ADDR=127.0.0.1:7107 \
  CDROM_AUTH_ENABLED=true CDROM_AUTH_ISSUER=http://127.0.0.1:7104 \
  CDROM_AUTH_CLIENT_ID=cdrom-ui CDROM_AUTH_REDIRECT_URL=http://127.0.0.1:8081/api/auth/callback \
  CDROM_AUTH_APIKEY_MAX_FAILURES=3 CDROM_AUTH_APIKEY_LOCKOUT_DURATION=1h \
  CDROM_AUDIT_FILE="$WORK/audit-rbac.log" CDROM_AUDIT_FORMAT=json \
  CDROM_LOG_FORMAT=text -- "$BIN/api"
RBAC=http://127.0.0.1:8081

# Sign in as the admin (ada, registered first) and as the default user (bob)
# against the RBAC-enabled API. The IdP mints each token stamped with the
# user's roles, which the API reads to authorize the request.
ADMIN_TOKEN=$(e2e_login "$RBAC" "$E2E_USER_ADA_EMAIL" "$E2E_USER_ADA_PASSWORD")
USER_TOKEN=$(e2e_login "$RBAC" "$E2E_USER_BOB_EMAIL" "$E2E_USER_BOB_PASSWORD")

# Create two pipelines to use as pipeline-scope targets.
e2e_authed_request POST "$RBAC/api/pipelines" "$ADMIN_TOKEN" '{"name":"apikey-pipeline-a"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> create pipeline A returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
PIPELINE_A=$(printf '%s' "$HTTP_BODY" | jq -r '.id')
e2e_authed_request POST "$RBAC/api/pipelines" "$ADMIN_TOKEN" '{"name":"apikey-pipeline-b"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> create pipeline B returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
PIPELINE_B=$(printf '%s' "$HTTP_BODY" | jq -r '.id')

# --- an admin creates a key for another user --------------------------------
# The plaintext `cdrom-…` key is returned exactly once (at creation and at
# each rotation); the stored record holds only its hash.
e2e_authed_request POST "$RBAC/api/api-keys" "$ADMIN_TOKEN" \
  "{\"owner_email\":\"${E2E_USER_BOB_EMAIL}\",\"description\":\"bob's key\",\"pipeline_scope\":[${PIPELINE_A}]}"
[ "$HTTP_CODE" = "201" ] || { echo ">> create bob key returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
BOB_KEY_ID=$(printf '%s' "$HTTP_BODY" | jq -r '.key.id')
BOB_KEY=$(printf '%s' "$HTTP_BODY" | jq -r '.plaintext')
BOB_OWNER_ID=$(printf '%s' "$HTTP_BODY" | jq -r '.key.owner_id')
[ -n "$BOB_KEY_ID" ] && [ "$BOB_KEY_ID" != "null" ] \
  || { echo ">> create bob key returned no key id: $HTTP_BODY" >&2; exit 1; }
[ "$(printf '%s' "$BOB_KEY" | cut -c1-6)" = "cdrom-" ] \
  || { echo ">> bob key plaintext does not start with cdrom-: $BOB_KEY" >&2; exit 1; }
# The stored record never carries the plaintext or the hash.
printf '%s' "$HTTP_BODY" | jq -e '.key | has("plaintext") | not' >/dev/null \
  || { echo ">> create response leaked the plaintext on the key record: $HTTP_BODY" >&2; exit 1; }

# A second key, owned by the admin, so the "list all" vs "list own"
# distinction is observable (two different owners).
e2e_authed_request POST "$RBAC/api/api-keys" "$ADMIN_TOKEN" \
  "{\"owner_email\":\"${E2E_USER_ADA_EMAIL}\",\"description\":\"ada's key\"}"
[ "$HTTP_CODE" = "201" ] || { echo ">> create ada key returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
ADA_KEY_ID=$(printf '%s' "$HTTP_BODY" | jq -r '.key.id')

# --- a user can manage their own keys, but not another user's --------------
# Creating a key for another user (or acting on another user's key) requires
# the api-keys.can-manage-all permission, which the built-in user role does
# not grant (only the admin role does).
# A user can create a key for themselves.
e2e_authed_request POST "$RBAC/api/api-keys" "$USER_TOKEN" \
  '{"description":"bob self key"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> user create own key returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
# A user cannot create a key for another user (403).
e2e_authed_request POST "$RBAC/api/api-keys" "$USER_TOKEN" \
  "{\"owner_email\":\"${E2E_USER_ADA_EMAIL}\",\"description\":\"x\"}"
[ "$HTTP_CODE" = "403" ] || { echo ">> user create-for-other returned $HTTP_CODE, want 403" >&2; exit 1; }
# A user cannot manage another user's key (403).
e2e_authed_request PUT "$RBAC/api/api-keys/${ADA_KEY_ID}" "$USER_TOKEN" '{"description":"x"}'
[ "$HTTP_CODE" = "403" ] || { echo ">> user update other key returned $HTTP_CODE, want 403" >&2; exit 1; }
e2e_authed_request DELETE "$RBAC/api/api-keys/${ADA_KEY_ID}" "$USER_TOKEN"
[ "$HTTP_CODE" = "403" ] || { echo ">> user delete other key returned $HTTP_CODE, want 403" >&2; exit 1; }

# --- a user lists only their own keys; an admin lists every key ------------
# The plain list endpoint always returns only the caller's own keys.
e2e_authed_request GET "$RBAC/api/api-keys" "$USER_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> user list keys returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e --arg id "$BOB_KEY_ID" '.[] | select(.id == $id)' >/dev/null \
  || { echo ">> user list did not include bob's own key: $HTTP_BODY" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e --arg id "$ADA_KEY_ID" '[.[] | select(.id == $id)] | length == 0' >/dev/null \
  || { echo ">> user list leaked another user's key: $HTTP_BODY" >&2; exit 1; }

# The privileged list-all endpoint (api-keys.can-list-all) returns every key
# in the system; it is granted to the admin role.
e2e_authed_request GET "$RBAC/api/api-keys/all" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> admin list-all keys returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e --arg id "$BOB_KEY_ID" '.[] | select(.id == $id)' >/dev/null \
  || { echo ">> admin list-all did not include bob's key: $HTTP_BODY" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e --arg id "$ADA_KEY_ID" '.[] | select(.id == $id)' >/dev/null \
  || { echo ">> admin list-all did not include ada's key: $HTTP_BODY" >&2; exit 1; }

# A regular user (no api-keys.can-list-all) cannot list every key (403).
e2e_authed_request GET "$RBAC/api/api-keys/all" "$USER_TOKEN"
[ "$HTTP_CODE" = "403" ] || { echo ">> user list-all keys returned $HTTP_CODE, want 403" >&2; exit 1; }

# --- a user authenticates with their API key (the middleware's API-key branch)
# The credential is `Bearer <username>:<apikey>`; the owner is placed in the
# context and the request is authorized as that user.
e2e_authed_request GET "$RBAC/api/me/permissions" "${E2E_USER_BOB_EMAIL}:${BOB_KEY}"
[ "$HTTP_CODE" = "200" ] || { echo ">> api-key auth returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }

# --- a key's pipeline scope limits the caller's resource-scoped permissions -
# bob's key is scoped to pipeline A: an in-scope resource is allowed, an
# out-of-scope one is denied (403) even though bob's role would otherwise
# grant pipelines.can-view.
e2e_authed_request GET "$RBAC/api/pipelines/${PIPELINE_A}/versions" "${E2E_USER_BOB_EMAIL}:${BOB_KEY}"
[ "$HTTP_CODE" = "200" ] || { echo ">> in-scope pipeline returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
e2e_authed_request GET "$RBAC/api/pipelines/${PIPELINE_B}/versions" "${E2E_USER_BOB_EMAIL}:${BOB_KEY}"
[ "$HTTP_CODE" = "403" ] || { echo ">> out-of-scope pipeline returned $HTTP_CODE, want 403 (DISALLOW)" >&2; exit 1; }

# --- a wrong key is rejected, and repeated failures lock the user out -------
# max_failures=3: three consecutive misses lock bob out for the configured
# duration, after which even the correct key is rejected until an admin
# resets the lockout.
e2e_authed_request GET "$RBAC/api/me/permissions" "${E2E_USER_BOB_EMAIL}:cdrom-wrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrong"
[ "$HTTP_CODE" = "401" ] || { echo ">> wrong key (1) returned $HTTP_CODE, want 401" >&2; exit 1; }
e2e_authed_request GET "$RBAC/api/me/permissions" "${E2E_USER_BOB_EMAIL}:cdrom-wrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrong"
[ "$HTTP_CODE" = "401" ] || { echo ">> wrong key (2) returned $HTTP_CODE, want 401" >&2; exit 1; }
e2e_authed_request GET "$RBAC/api/me/permissions" "${E2E_USER_BOB_EMAIL}:cdrom-wrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrong"
[ "$HTTP_CODE" = "401" ] || { echo ">> wrong key (3) returned $HTTP_CODE, want 401" >&2; exit 1; }
# bob is now locked out: even the correct key is rejected.
e2e_authed_request GET "$RBAC/api/me/permissions" "${E2E_USER_BOB_EMAIL}:${BOB_KEY}"
[ "$HTTP_CODE" = "401" ] || { echo ">> locked-out correct key returned $HTTP_CODE, want 401" >&2; exit 1; }

# An admin resets bob's lockout; the correct key works again.
e2e_authed_request POST "$RBAC/api/api-keys/lockout/reset" "$ADMIN_TOKEN" "{\"user_id\":\"${BOB_OWNER_ID}\"}"
[ "$HTTP_CODE" = "200" ] || { echo ">> reset lockout returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
e2e_authed_request GET "$RBAC/api/me/permissions" "${E2E_USER_BOB_EMAIL}:${BOB_KEY}"
[ "$HTTP_CODE" = "200" ] || { echo ">> post-reset api-key auth returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }

# --- rotating a key issues a new plaintext; the old one stops working -------
e2e_authed_request POST "$RBAC/api/api-keys/${BOB_KEY_ID}/rotate" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> rotate returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
BOB_KEY_NEW=$(printf '%s' "$HTTP_BODY" | jq -r '.plaintext')
[ "$(printf '%s' "$BOB_KEY_NEW" | cut -c1-6)" = "cdrom-" ] \
  || { echo ">> rotated key plaintext does not start with cdrom-: $BOB_KEY_NEW" >&2; exit 1; }
# The old plaintext no longer authenticates.
e2e_authed_request GET "$RBAC/api/me/permissions" "${E2E_USER_BOB_EMAIL}:${BOB_KEY}"
[ "$HTTP_CODE" = "401" ] || { echo ">> old key after rotate returned $HTTP_CODE, want 401" >&2; exit 1; }
# The new plaintext does.
e2e_authed_request GET "$RBAC/api/me/permissions" "${E2E_USER_BOB_EMAIL}:${BOB_KEY_NEW}"
[ "$HTTP_CODE" = "200" ] || { echo ">> new key after rotate returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }

# --- deleting a key revokes it ----------------------------------------------
e2e_authed_request DELETE "$RBAC/api/api-keys/${BOB_KEY_ID}" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> delete returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
e2e_authed_request GET "$RBAC/api/me/permissions" "${E2E_USER_BOB_EMAIL}:${BOB_KEY_NEW}"
[ "$HTTP_CODE" = "401" ] || { echo ">> deleted key returned $HTTP_CODE, want 401" >&2; exit 1; }

log ">> 08-apikeys PASSED"
