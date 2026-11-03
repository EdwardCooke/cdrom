#!/usr/bin/env bash
#
# 21-break-glass.sh — F-28.3 Slice 3: the break-glass escape hatch.
#
# The break-glass endpoint (POST /emergency/break-glass on the IdP's HTTP
# listener) is the guaranteed local foothold when SSO/SCIM is the broken link:
# a holder of a CA-signed client certificate coins an EPHEMERAL,
# RANDOM-IDENTITY admin (breakglass-…@invalid) with a self-expiring TEMPORAL
# password. Coinage is strictly additive (never modifies an existing user),
# capped on live credentials, and every invocation is audited with the
# caller's certificate attributed.
#
# This e2e asserts the OBSERVABLE SECURITY PROPERTY that holds in ANY
# deployment (TLS or not): the route is MOUNTED (reachable, not a 404) and
# DENIES UNIFORMLY (403, a fixed body advertising no state) to any caller who
# does not present a valid CA-signed client certificate. On the plaintext
# shared stack no client certificate can be presented, so every probe — POST,
# GET, with or without a body — must get the identical 403.
#
# The POSITIVE mTLS path (holding the API's client certificate → coin an
# ephemeral admin → sign in with the temporal password → it self-expires) is
# exercised by the UNIT tests (internal/idp/breakglass_test.go), which spin up
# REAL ECDSA certificates and verify the presenter chain end to end. Driving
# that here would require a TLS IdP speaking mTLS to a TLS DB (a two-node
# cluster), which is disproportionate and environmentally brittle for what is
# ultimately the same code path.
#
# Usage: scripts/e2e/21-break-glass.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. Runnable standalone (boots its own stack) or against the shared stack.

set -uo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

e2e_boot_stack

# The break-glass route is a FLAT LITERAL on the IdP (not configurable), so
# the drill URL is derivable from the IdP's base address alone.
readonly BREAKGLASS_ROUTE=/emergency/break-glass
BG_URL="http://127.0.0.1:7104${BREAKGLASS_ROUTE}"
DENIAL='{"error":"forbidden"}'

# Probe: expect a UNIFORM 403 with the fixed denial body (no state advertised).
uniform_denial() {
  local method="$1" body="$2"
  local args=(-s -o /dev/null -w "%{http_code}")
  [ "$method" = "GET" ] && args+=(--head)
  [ -n "$body" ] && args+=(-X "$method" -d "$body")
  local code
  code=$(curl "${args[@]}" "$BG_URL")
  if [ "$code" != "403" ]; then
    echo ">> [$method] status = $code, want 403 (uniform denial)" >&2
    return 1
  fi
  # Body equality (POST carries a body; HEAD/GET do not).
  if [ -n "$body" ]; then
    local got
    got=$(curl -s -X POST -d "$body" "$BG_URL")
    if [ "$got" != "$DENIAL" ]; then
      echo ">> [POST] body = $got, want the fixed denial ($DENIAL)" >&2
      return 1
    fi
  fi
  return 0
}

# Leg 1: the route is MOUNTED (a 403, not a 404) and denies uniformly.
uniform_denial POST '{}' || exit 1
uniform_denial POST '{"ttl_seconds":3600}' || exit 1
uniform_denial GET '' || exit 1
log "✓ break-glass route is mounted and denies uniformly (403, no state) without a valid client certificate"

# Leg 2: the denial is IDENTICAL regardless of the probe shape (no oracle).
declare -a CODES=(
  "$(curl -s -o /dev/null -w '%{http_code}' -X POST -d '{}' "$BG_URL")"
  "$(curl -s -o /dev/null -w '%{http_code}' -X POST -d 'garbage' "$BG_URL")"
  "$(curl -s -o /dev/null -w '%{http_code}' --head "$BG_URL")"
)
UNIQ=$(printf '%s\n' "${CODES[@]}" | sort -u | tr '\n' ',')
if [ "$UNIQ" != "403," ]; then
  echo ">> probe statuses differ (${CODES[*]}), want all 403 (no oracle)" >&2
  exit 1
fi
log "✓ every probe shape yields the same 403 (no differential oracle)"

# Leg 3: no INFRASTRUCTURE FAULT hid behind the denial — a 5xx would mean the
# endpoint crashed (misconfigured root pool, panic) rather than calmly
# denying. The uniform 403 above already implies no coinage occurred (the
# handler denies BEFORE it ever reaches the coiner; the unit tests assert the
# directory diff).
FAULT_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST -d '{}' "$BG_URL")
if [ "${FAULT_CODE:0:1}" = "5" ]; then
  echo ">> break-glass responded $FAULT_CODE (infrastructure fault), want a calm 4xx denial" >&2
  exit 1
fi
log "✓ denials are calm 4xx (no 5xx infrastructure fault)"

echo ">> 21-break-glass PASSED"
