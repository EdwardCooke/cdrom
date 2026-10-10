#!/usr/bin/env bash
#
# 06-rbac.sh — F-14: roles & permissions (RBAC).
#
# Boots the stack (with a worker, so the operator's triggered run has a
# target to dispatch to) and a SECOND API instance with OIDC authentication
# and RBAC enabled, sharing the same IdP (users + JWKS) and
# db/scheduler/artifacts as the main stack. The main stack runs with OIDC
# auth OFF, so its API is open (every request acts as a synthetic admin) and
# RBAC is not enforced.
#
# Users with different roles (admin, operator, and the default user role)
# sign in and their tokens are presented to the RBAC-enabled API, which must
# ALLOW the actions the role grants and DISALLOW (403) the actions it does
# not — e.g. an operator can trigger a run but not create a pipeline, a
# default user can view but not trigger, and a principal with no roles can do
# nothing (deny-by-default).
#
# The shared test users (ada@example.com, admin; bob@example.com, the default
# user role) are registered by the F-24 test in the full run; when this
# script runs standalone it registers them itself if they are not present.
#
# Usage: scripts/e2e/06-rbac.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack with-worker

log ">> verifying role-based access control (F-14)"

# Make sure the shared users exist (the F-24 test registers them in the full
# run; a duplicate registration is a 409, which is fine).
e2e_ensure_users http://127.0.0.1:8080

# The RBAC-enabled API listens on :8081 (gRPC :7107) so it does not collide
# with the main API (:8080/:7105). It dials the shared db/scheduler/artifacts
# and IdP at their default addresses.
e2e_start_rbac_api
RBAC=http://127.0.0.1:8081

# Sign in as the admin (ada, registered first) and as the default user (bob)
# against the RBAC-enabled API. The IdP mints each token stamped with the
# user's roles, which the API reads to authorize the request.
ADMIN_TOKEN=$(e2e_login "$RBAC" "$E2E_USER_ADA_EMAIL" "$E2E_USER_ADA_PASSWORD")
USER_TOKEN=$(e2e_login "$RBAC" "$E2E_USER_BOB_EMAIL" "$E2E_USER_BOB_PASSWORD")

# Create a user with the operator role (via the admin's user-management
# endpoint, which honours the supplied roles) and sign in as them.
e2e_authed_request POST "$RBAC/api/users" "$ADMIN_TOKEN" \
  '{"first_name":"Op","last_name":"Erator","email":"op@example.com","password":"pw","roles":["operator"]}'
[ "$HTTP_CODE" = "201" ] || { echo ">> create operator user returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
OPERATOR_TOKEN=$(e2e_login "$RBAC" "op@example.com" "pw")

# Create a pipeline (as the admin) to use as a resource-scoped target.
e2e_authed_request POST "$RBAC/api/pipelines" "$ADMIN_TOKEN" '{"name":"rbac-pipeline"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> admin create pipeline returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
PIPELINE_ID=$(printf '%s' "$HTTP_BODY" | jq -r '.id')
[ -n "$PIPELINE_ID" ] && [ "$PIPELINE_ID" != "null" ] \
  || { echo ">> could not parse pipeline id: $HTTP_BODY" >&2; exit 1; }
log "   rbac pipeline id: $PIPELINE_ID"

# --- ALLOW: the admin can do everything -------------------------------------
e2e_authed_request GET "$RBAC/api/pipelines" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> admin list pipelines returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
e2e_authed_request POST "$RBAC/api/pipelines" "$ADMIN_TOKEN" '{"name":"rbac-admin-pipeline"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> admin create pipeline returned $HTTP_CODE, want 201 (ALLOW)" >&2; exit 1; }
e2e_authed_request POST "$RBAC/api/pipelines/$PIPELINE_ID/runs" "$ADMIN_TOKEN" ''
[ "$HTTP_CODE" = "201" ] || { echo ">> admin trigger run returned $HTTP_CODE, want 201 (ALLOW)" >&2; exit 1; }
e2e_authed_request GET "$RBAC/api/roles" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> admin list roles returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
e2e_authed_request GET "$RBAC/api/me/permissions" "$ADMIN_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> admin me/permissions returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
# The admin's effective permissions include the platform-wide role-management
# permission (admin = every permission).
printf '%s' "$HTTP_BODY" | jq -e '.platform_wide | index("roles.can-manage")' >/dev/null \
  || { echo ">> admin me/permissions did not list roles.can-manage: $HTTP_BODY" >&2; exit 1; }

# --- ALLOW + DISALLOW: the operator can run but not create ------------------
# operator grants pipelines.can-view + runs.can-trigger (among others) but NOT
# pipelines.can-create, so it can list and trigger but not create a pipeline.
e2e_authed_request GET "$RBAC/api/pipelines" "$OPERATOR_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> operator list pipelines returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
e2e_authed_request POST "$RBAC/api/pipelines/$PIPELINE_ID/runs" "$OPERATOR_TOKEN" ''
[ "$HTTP_CODE" = "201" ] || { echo ">> operator trigger run returned $HTTP_CODE, want 201 (ALLOW)" >&2; exit 1; }
e2e_authed_request POST "$RBAC/api/pipelines" "$OPERATOR_TOKEN" '{"name":"rbac-operator-pipeline"}'
[ "$HTTP_CODE" = "403" ] || { echo ">> operator create pipeline returned $HTTP_CODE, want 403 (DISALLOW)" >&2; exit 1; }
# The operator cannot manage roles (roles.can-manage is not in the operator set).
e2e_authed_request GET "$RBAC/api/roles" "$OPERATOR_TOKEN"
[ "$HTTP_CODE" = "403" ] || { echo ">> operator list roles returned $HTTP_CODE, want 403 (DISALLOW)" >&2; exit 1; }

# --- ALLOW + DISALLOW: the default user can view but not trigger ------------
# The default "user" role grants pipelines.can-view but not runs.can-trigger,
# so it can list pipelines but not trigger a run or create a pipeline.
e2e_authed_request GET "$RBAC/api/pipelines" "$USER_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> user list pipelines returned $HTTP_CODE, want 200 (ALLOW)" >&2; exit 1; }
e2e_authed_request POST "$RBAC/api/pipelines/$PIPELINE_ID/runs" "$USER_TOKEN" ''
[ "$HTTP_CODE" = "403" ] || { echo ">> user trigger run returned $HTTP_CODE, want 403 (DISALLOW)" >&2; exit 1; }
e2e_authed_request POST "$RBAC/api/pipelines" "$USER_TOKEN" '{"name":"rbac-user-pipeline"}'
[ "$HTTP_CODE" = "403" ] || { echo ">> user create pipeline returned $HTTP_CODE, want 403 (DISALLOW)" >&2; exit 1; }

# --- DISALLOW: a principal with no roles can do nothing (deny-by-default) ---
# Create a user with an empty role set (the admin's user-management endpoint
# honours the supplied roles, so an empty set means no roles at all) and sign
# in as them. With no roles and no mapped claims, deny-by-default means every
# action is refused — not even listing pipelines.
e2e_authed_request POST "$RBAC/api/users" "$ADMIN_TOKEN" \
  '{"first_name":"No","last_name":"Role","email":"nole@example.com","password":"pw","roles":[]}'
[ "$HTTP_CODE" = "201" ] || { echo ">> create no-role user returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
NOROLE_TOKEN=$(e2e_login "$RBAC" "nole@example.com" "pw")
e2e_authed_request GET "$RBAC/api/pipelines" "$NOROLE_TOKEN"
[ "$HTTP_CODE" = "403" ] || { echo ">> no-role list pipelines returned $HTTP_CODE, want 403 (DISALLOW, deny-by-default)" >&2; exit 1; }
e2e_authed_request GET "$RBAC/api/me/permissions" "$NOROLE_TOKEN"
[ "$HTTP_CODE" = "200" ] || { echo ">> no-role me/permissions returned $HTTP_CODE, want 200" >&2; exit 1; }
# A principal with no roles has no effective permissions at all.
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.platform_wide | length')" = "0" ] \
  || { echo ">> no-role me/permissions was not empty: $HTTP_BODY" >&2; exit 1; }

# --- DISALLOW: an unauthenticated request to the RBAC API is rejected (401) --
# The RBAC-enabled API requires a valid Bearer token on every /api/* request
# (except the exempted login/register entry points).
e2e_http_request GET "$RBAC/api/pipelines"
[ "$HTTP_CODE" = "401" ] || { echo ">> unauthenticated list pipelines (rbac api) returned $HTTP_CODE, want 401" >&2; exit 1; }

log ">> 06-rbac PASSED"
