#!/usr/bin/env bash
#
# 20-scim-replica-parity.sh — F-28.2 slice 3: the guard's MULTIPLICATION legs.
#
# Companion to 19-scim-keys.sh (which proves the bootstrap ritual, the
# F-25/F-26 separation, the identical-401 contract, and rotation overlap).
# Here: two IdP replicas serving the SAME pool (shared database) judge
# identically — no per-replica key config — failed attempts are AUDITED
# (locally + in the shared database audit log) without EVER echoing the
# presented key, and successful validations advance lastUsedAt.
#
# Self-contained: boots its own stack + two peppered replicas + a third API
# (steered at the peppered replica for derivation), mints its own keys, and
# tears everything down on exit. Kept SHORT on purpose: the runner
# environment intermittently sweeps long-lived auxiliary processes, and a
# short scenario sidesteps it.
#
# Usage: scripts/e2e/20-scim-replica-parity.sh
#
# Requires: the built binaries in ./bin (run `make build` first), curl, and
# jq. The script always tears the stack down and removes its temp directory
# on exit, even on failure.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# Preflight: this scenario's extras need FREE ports (7114/7115/7116/7117 for
# the replicas, 8082/7108 for the third API). Standalone mode also needs the
# shared-stack ports free (orphans from an abandoned run would otherwise
# poison the boot deep in the script).
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

log ">> verifying the SCIM guard's multiplication legs (F-28.2 slice 3)"

e2e_ensure_users http://127.0.0.1:8080

PEPPER="s3-parity-pepper"
SCIMA=http://127.0.0.1:7114
SCIMB=http://127.0.0.1:7115
API3=http://127.0.0.1:8082

e2e_start idp-p-a "$WORK/idp-p-a.log" 'idp starting' \
  CDROM_LISTEN_ADDR=127.0.0.1:7114 CDROM_IDP_GRPC_ADDR=127.0.0.1:7116 \
  CDROM_IDP_ISSUER=http://127.0.0.1:7104 CDROM_DB_ADDR=127.0.0.1:7101 \
  CDROM_IDP_SCIM_KEYS_PEPPER="$PEPPER" CDROM_SCIM_ENABLED=true \
  CDROM_AUDIT_FILE="$WORK/idp-p-a-audit.log" CDROM_AUDIT_FORMAT=json \
  CDROM_METRICS_ENABLED=false CDROM_LOG_FORMAT=text -- "$BIN/idp"
e2e_start idp-p-b "$WORK/idp-p-b.log" 'idp starting' \
  CDROM_LISTEN_ADDR=127.0.0.1:7115 CDROM_IDP_GRPC_ADDR=127.0.0.1:7117 \
  CDROM_IDP_ISSUER=http://127.0.0.1:7104 CDROM_DB_ADDR=127.0.0.1:7101 \
  CDROM_IDP_SCIM_KEYS_PEPPER="$PEPPER" CDROM_SCIM_ENABLED=true \
  CDROM_AUDIT_FILE="$WORK/idp-p-b-audit.log" CDROM_AUDIT_FORMAT=json \
  CDROM_METRICS_ENABLED=false CDROM_LOG_FORMAT=text -- "$BIN/idp"
e2e_start api-p "$WORK/api-p.log" 'serving' \
  CDROM_API_HTTP_ADDR=127.0.0.1:8082 CDROM_LISTEN_ADDR=127.0.0.1:7108 \
  CDROM_AUTH_ENABLED=true CDROM_AUTH_ISSUER=http://127.0.0.1:7104 \
  CDROM_AUTH_CLIENT_ID=cdrom-ui CDROM_AUTH_REDIRECT_URL=http://127.0.0.1:8082/api/auth/callback \
  CDROM_IDP_GRPC_ADDR=127.0.0.1:7116 \
  CDROM_AUDIT_FILE="$WORK/api-p-audit.log" CDROM_AUDIT_FORMAT=json \
  CDROM_METRICS_ENABLED=false CDROM_LOG_FORMAT=text -- "$BIN/api"
TOKEN3=$(e2e_login "$API3" "$E2E_USER_ADA_USERNAME" "$E2E_USER_ADA_PASSWORD")
log ">> replicas + third API up"

# scim_req <origin-url-base> <url-rel> <key-or-empty>: prints "<code>\n<body>".
# Retries up to 3x (1 s apart) when the TRANSFER fails (a replica struck by
# the runner environment's occasional sweeping of auxiliary processes); on
# final failure dumps the replica's log and exits LOUDLY.
scim_req() {
  local base="$1" rel="$2" kf="$3"
  local args=(-s)
  if [ -n "$kf" ]; then args+=(-H "Authorization: Bearer $kf"); fi
  local attempt
  for attempt in 1 2 3; do
    if RESP=$(curl -s -o "$WORK/parity-body" -w '%{http_code}' "${args[@]}" "$base$rel" 2>/dev/null); then
      printf '%s\n%s' "$RESP" "$(cat "$WORK/parity-body")"
      return 0
    fi
    sleep 1
  done
  echo ">> a replica stopped accepting connections (3 transfers failed); its log:" >&2
  tail -10 "$WORK/idp-p-a.log" "$WORK/idp-p-b.log" >&2
  exit 1
}

# Mint two keys (predecessor + successor) through the peppered replica.
e2e_authed_request POST "$API3/api/scim/keys" "$TOKEN3" '{"description":"parity pred"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> pred mint returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
M1="$HTTP_BODY"
e2e_authed_request POST "$API3/api/scim/keys" "$TOKEN3" '{"description":"parity succ"}'
[ "$HTTP_CODE" = "201" ] || { echo ">> succ mint returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
M2="$HTTP_BODY"
K1=$(printf '%s' "$M1" | jq -r '.key.identifier'); P1=$(printf '%s' "$M1" | jq -r '.plaintext')
K2=$(printf '%s' "$M2" | jq -r '.key.identifier'); P2=$(printf '%s' "$M2" | jq -r '.plaintext')
[ "${#P1}" -eq 32 ] && [ "${#P2}" -eq 32 ] || { echo ">> bad minted keys" >&2; exit 1; }
sleep 1.2 # settle the guard's ~1 s bundle memo (both keys become visible)
log ">> two keys minted"

# --- replica parity: the SAME pool, judged identically by both replicas -----
OA=$(scim_req "$SCIMA" /scim/v2/ServiceProviderConfig "$P2")
OB=$(scim_req "$SCIMB" /scim/v2/ServiceProviderConfig "$P2")
[ "${OA%%$'\n'*}" = "200" ] || { echo ">> replica A rejected the shared key (${OA%%$'\n'*})" >&2; exit 1; }
[ "${OB%%$'\n'*}" = "200" ] || { echo ">> replica B rejected the shared key (${OB%%$'\n'*})" >&2; exit 1; }
# And the SAME rejection for a bogus key (identical 401 on both replicas).
BA=$(scim_req "$SCIMA" /scim/v2/ServiceProviderConfig "bogus-parity-key")
BB=$(scim_req "$SCIMB" /scim/v2/ServiceProviderConfig "bogus-parity-key")
[ "${BA%%$'\n'*}" = "401" ] && [ "${BB%%$'\n'*}" = "401" ] \
  || { echo ">> bogus key: A=${BA%%$'\n'*} B=${BB%%$'\n'*}, want 401/401" >&2; exit 1; }
[ "${BA#*$'\n'}" = "${BB#*$'\n'}" ] || { echo ">> replicas disagree on the 401 body" >&2; exit 1; }
log ">> replica parity OK (same pool, identical verdicts)"

# --- recording: failures audited, the key NEVER echoed ------------------------
PROBE="PROBE-KEY-THAT-MUST-STAY-SECRET"
OP=$(scim_req "$SCIMA" /scim/v2/ServiceProviderConfig "$PROBE")
[ "${OP%%$'\n'*}" = "401" ] || { echo ">> probe got ${OP%%$'\n'*}, want 401" >&2; exit 1; }
grep -q 'scim.auth.fail' "$WORK/idp-p-a-audit.log" \
  || { echo ">> no scim.auth.fail event in the IdP's local audit file" >&2; exit 1; }
if grep -qF "$PROBE" "$WORK/idp-p-a-audit.log"; then
  echo ">> the IdP audit FILE echoed the presented key" >&2; exit 1
fi
if grep -qiF "$PROBE" "$WORK/idp-p-a.log"; then
  echo ">> the IdP LOG echoed the presented key" >&2; exit 1
fi
e2e_authed_request GET "$API3/api/audit?action=scim.auth.fail" "$TOKEN3"
[ "$HTTP_CODE" = "200" ] || { echo ">> shared audit query returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
printf '%s' "$HTTP_BODY" | jq -e 'length >= 1' >/dev/null \
  || { echo ">> shared audit log holds no scim.auth.fail events: $HTTP_BODY" >&2; exit 1; }
if printf '%s' "$HTTP_BODY" | grep -qF "$PROBE"; then
  echo ">> the SHARED audit log echoed the presented key" >&2; exit 1
fi
log ">> recording OK (failures audited, key never echoed)"

# --- successes advance lastUsedAt --------------------------------------------
# K2 was just VALIDATED by the parity leg above (a successful presentation
# bumps last_used_at, throttled at the Database service): its column must now
# be set. One retried read keeps this leg impervious to transient blips.
LU=""
for attempt in 1 2 3; do
  if e2e_authed_request GET "$API3/api/scim/keys/$K2" "$TOKEN3"; then
    LU=$(printf '%s' "$HTTP_BODY" | jq -r '.last_used_at' 2>/dev/null) || LU=""
    if [ -n "$LU" ] && [ "$LU" != "null" ]; then break; fi
  fi
  sleep 1
done
[ -n "$LU" ] || { echo ">> lastUsedAt did not advance after a successful validation" >&2; exit 1; }
log ">> lastUsedAt advanced ($LU)"

log ">> 20-scim-replica-parity PASSED"
