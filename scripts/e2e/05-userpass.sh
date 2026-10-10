#!/usr/bin/env bash
#
# 05-userpass.sh — F-24: username/password authentication.
#
# Boots the stack (no worker needed) and verifies that the API proxies the
# UI's register/login/user-management requests to the IdP, where the real
# logic lives (password verification, token minting, user storage).
# Username/password auth is enabled by default, so the stack's IdP serves
# these endpoints. The unauthenticated entry points (register, login) work
# without a token; the user-management endpoints are gated behind an
# authenticated admin caller. This stack leaves OIDC auth off (so the other
# tests can call the API without a token), so an unauthenticated caller to a
# user-management endpoint is rejected (401) rather than verified.
#
# This script registers the shared test users (ada@example.com, the first
# user, becomes an admin; bob@example.com gets the default user role) that
# the F-14 RBAC test signs in as.
#
# Usage: scripts/e2e/05-userpass.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack

log ">> verifying username/password authentication (F-24)"

# Register the first user: created (201) and, as the first user, given the
# admin role.
e2e_http_request POST http://127.0.0.1:8080/api/register \
  "{\"first_name\":\"Ada\",\"last_name\":\"Lovelace\",\"email\":\"${E2E_USER_ADA_EMAIL}\",\"password\":\"${E2E_USER_ADA_PASSWORD}\"}"
[ "$HTTP_CODE" = "201" ] \
  || { echo ">> first register returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.roles | index("admin")' >/dev/null \
  || { echo ">> first user was not given the admin role: $HTTP_BODY" >&2; exit 1; }

# Register a second user: created (201) with the default user role.
e2e_http_request POST http://127.0.0.1:8080/api/register \
  "{\"first_name\":\"Bob\",\"last_name\":\"Jones\",\"email\":\"${E2E_USER_BOB_EMAIL}\",\"password\":\"${E2E_USER_BOB_PASSWORD}\"}"
[ "$HTTP_CODE" = "201" ] \
  || { echo ">> second register returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.roles | index("user")' >/dev/null \
  || { echo ">> second user was not given the user role: $HTTP_BODY" >&2; exit 1; }

# Registering a duplicate email is rejected (409).
e2e_http_request POST http://127.0.0.1:8080/api/register \
  "{\"first_name\":\"Ada\",\"last_name\":\"L\",\"email\":\"${E2E_USER_ADA_EMAIL}\",\"password\":\"x\"}"
[ "$HTTP_CODE" = "409" ] \
  || { echo ">> duplicate register returned $HTTP_CODE, want 409" >&2; exit 1; }

# Login with valid credentials: returns an OIDC access token (200). The token
# is a signed JWT (three dot-separated segments) minted by the IdP, stamped
# with the user's roles.
e2e_http_request POST http://127.0.0.1:8080/api/login \
  "{\"email\":\"${E2E_USER_ADA_EMAIL}\",\"password\":\"${E2E_USER_ADA_PASSWORD}\"}"
[ "$HTTP_CODE" = "200" ] \
  || { echo ">> login returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
LOGIN_TOKEN=$(printf '%s' "$HTTP_BODY" | jq -r '.access_token')
[ -n "$LOGIN_TOKEN" ] && [ "$LOGIN_TOKEN" != "null" ] \
  || { echo ">> login returned no access_token: $HTTP_BODY" >&2; exit 1; }
[ "$(printf '%s' "$LOGIN_TOKEN" | awk -F. '{print NF}')" = "3" ] \
  || { echo ">> login access_token is not a JWT: $LOGIN_TOKEN" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e '.roles | index("admin")' >/dev/null \
  || { echo ">> login response did not carry the user's admin role: $HTTP_BODY" >&2; exit 1; }

# Login with a wrong password is rejected (401).
e2e_http_request POST http://127.0.0.1:8080/api/login \
  "{\"email\":\"${E2E_USER_ADA_EMAIL}\",\"password\":\"wrong\"}"
[ "$HTTP_CODE" = "401" ] \
  || { echo ">> wrong-password login returned $HTTP_CODE, want 401" >&2; exit 1; }

# Login for an unknown user is rejected (401).
e2e_http_request POST http://127.0.0.1:8080/api/login \
  '{"email":"nobody@example.com","password":"x"}'
[ "$HTTP_CODE" = "401" ] \
  || { echo ">> unknown-user login returned $HTTP_CODE, want 401" >&2; exit 1; }

# The user-management endpoints are gated behind an authenticated admin caller.
# With this stack's auth-off config an unauthenticated caller is rejected (401).
e2e_http_request GET http://127.0.0.1:8080/api/users
[ "$HTTP_CODE" = "401" ] \
  || { echo ">> unauthenticated list users returned $HTTP_CODE, want 401" >&2; exit 1; }

log ">> 05-userpass PASSED"
