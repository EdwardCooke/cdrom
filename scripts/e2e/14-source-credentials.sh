#!/usr/bin/env bash
#
# 14-source-credentials.sh — verifies the F-21.3 source credentials end to end,
# focused on the `http` kind's headers.
#
# Two halves:
#
#   1. The fetcher's http handler (F-21.2.4.2): the headers the API hands to the
#      fetcher (a list of name + value) are actually sent on the GET. A local
#      HTTP sink records every request header, so the test asserts the exact
#      header names + values arrived (custom headers, not just Authorization).
#
#   2. The F-21.3 source-credential API: an `http` credential carries a list of
#      headers, each with its own `sensitive` flag — a sensitive header is
#      encrypted at rest and redacted on read (`[REDACTED]`), while a
#      non-sensitive header is stored in plaintext and returned as-is. The test
#      creates single- and multi-header credentials, reads them back, and
#      asserts the per-header redaction difference, then updates and deletes
#      them.
#
# The fetcher half is standalone (it needs no db/api); the API half uses the
# stack's API (which always has a secret store — an all-zero key in the e2e
# run — so it can encrypt/decrypt). In shared mode (run from e2e.sh) the stack
# is already up; in standalone mode e2e_boot_stack boots it.
#
# Usage: scripts/e2e/14-source-credentials.sh
#
# Requires: the built binaries in ./bin (run `make build` first), the
# fetchclient helper (built by `make build`), curl, jq, and python3.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# The main stack runs no fetcher, so this script boots its own (gRPC :7110,
# metrics :9110, http sink :10091, 401 sink :10092) and points the stack's API
# at it (CDROM_PIPELINE_SOURCE_ADDR) so a source refresh dials this fetcher.
# In shared mode the API is already up (dialing the default :7107), so the
# refresh half is skipped; the fetcher + API-redaction halves still run.
E2E_PS_ADDR=127.0.0.1:7110
E2E_PS_METRICS=9110
E2E_PS_HTTP=10091
E2E_PS_401=10092
if [ -z "${E2E_STACK_READY:-}" ]; then
  export CDROM_PIPELINE_SOURCE_ADDR="$E2E_PS_ADDR"
fi

# Ensure the full stack (db/idp/api) is available for the API half. In shared
# mode it is already up; in standalone mode this boots it (and installs the
# cleanup trap).
e2e_boot_stack

PS_CACHE="$WORK/ps14-cache"
PS_HTTPDIR="$WORK/ps14-http"
mkdir -p "$PS_CACHE" "$PS_HTTPDIR"
printf 'http: body\n' > "$PS_HTTPDIR/file.yaml"

# Boot the fetcher (no file root needed; the git cache lives in PS_CACHE).
e2e_start pipelinesource14 "$WORK/pipelinesource14.log" 'service starting' \
  CDROM_LISTEN_ADDR="$E2E_PS_ADDR" \
  CDROM_PIPELINE_SOURCE_REPO_CACHE_DIR="$PS_CACHE" \
  CDROM_METRICS_ADDR="127.0.0.1:$E2E_PS_METRICS" \
  CDROM_LOG_FORMAT=text -- "$BIN/pipelinesource"

# A local HTTP sink that serves PS_HTTPDIR and records EVERY request header
# (name: value, one per line) so the test can assert the exact header the
# fetcher sent.
python3 - "$E2E_PS_HTTP" "$PS_HTTPDIR" "$WORK/ps14-headers.log" <<'PY' &
import http.server, sys
port, root, hlog = int(sys.argv[1]), sys.argv[2], sys.argv[3]
class H(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=root, **k)
    def end_headers(self):
        self.send_header("ETag", '"v1"')
        super().end_headers()
    def do_GET(self):
        with open(hlog, "a") as f:
            for k, v in self.headers.items():
                f.write(f"{k}: {v}\n")
            f.write("---\n")
        super().do_GET()
http.server.ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
PY
PIDS+=($!)
# A 401 sink: simulates an endpoint that rejects the (stale) credential, so a
# sync that uses it fails with an auth error (the F-21.3.2 criterion).
python3 - "$E2E_PS_401" <<'PY' &
import http.server, sys
port = int(sys.argv[1])
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(401)
        self.send_header("WWW-Authenticate", "Bearer")
        self.end_headers()
    def log_message(self, *a):
        pass
http.server.ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
PY
PIDS+=($!)
for i in $(seq 1 50); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$E2E_PS_HTTP") 2>/dev/null; then
    exec 3>&- 2>/dev/null || true
    break
  fi
  sleep 0.1
done

# fetch <ref> [cred-args...] -> sets F_CONTENT (stdout), F_VERSION, F_HASH.
fetch() {
  local ref="$1"; shift
  local meta="$WORK/ps14-meta.txt"
  F_CONTENT=$("$BIN/fetchclient" -addr "$E2E_PS_ADDR" -ref "$ref" "$@" 2>"$meta") || return 1
  F_VERSION=$(grep '^version=' "$meta" | cut -d= -f2- || true)
  F_HASH=$(grep '^hash=' "$meta" | cut -d= -f2- || true)
}

# --- 1. the fetcher's http handler sends the credential's headers -------------
log ">> http handler: multiple custom headers are sent on the GET"
fetch "http:http://127.0.0.1:$E2E_PS_HTTP/file.yaml" \
  -cred-kind http -cred-header "X-Api-Key=secret-value-123" -cred-header "X-Request-Id=req-789"
[ "$F_CONTENT" = "http: body" ] || { echo "http content mismatch: $F_CONTENT" >&2; exit 1; }
# The sink records the last request's headers; assert both custom headers
# arrived with the exact values (and that neither is sent as a default
# Authorization).
grep -q "^X-Api-Key: secret-value-123$" "$WORK/ps14-headers.log" \
  || { echo "custom header X-Api-Key was not sent as-is:" >&2; cat "$WORK/ps14-headers.log" >&2; exit 1; }
grep -q "^X-Request-Id: req-789$" "$WORK/ps14-headers.log" \
  || { echo "custom header X-Request-Id was not sent as-is:" >&2; cat "$WORK/ps14-headers.log" >&2; exit 1; }
# A non-Authorization header must not be duplicated into Authorization.
tail -n 5 "$WORK/ps14-headers.log" | grep -q "^Authorization:" \
  && { echo "an unexpected Authorization header was sent for a custom header" >&2; exit 1; } || true

log ">> http handler: the Authorization header is sent when named"
fetch "http:http://127.0.0.1:$E2E_PS_HTTP/file.yaml" \
  -cred-kind http -cred-header "Authorization=Bearer tok456"
grep -q "^Authorization: Bearer tok456$" "$WORK/ps14-headers.log" \
  || { echo "Authorization header was not sent:" >&2; cat "$WORK/ps14-headers.log" >&2; exit 1; }

# --- 2. the F-21.3 source-credential API (http header redaction) --------------
# The main stack's API is open in the e2e run (auth disabled -> synthetic
# admin), so the CRUD + redaction behavior can be exercised without a token.
API=http://127.0.0.1:8080

log ">> api: a sensitive http header is redacted on read"
e2e_http_request POST "$API/api/source-credentials" \
  '{"name":"sc-sensitive","kind":"http","prefix":"https://api.example.com/","headers":[{"name":"Authorization","value":"Bearer sensitive-secret","sensitive":true}]}'
[ "$HTTP_CODE" = "201" ] || { echo "create sensitive returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request GET "$API/api/source-credentials/sc-sensitive"
[ "$HTTP_CODE" = "200" ] || { echo "get sensitive returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.headers[0].value')" = "[REDACTED]" ] \
  || { echo "sensitive header value was not redacted: $HTTP_BODY" >&2; exit 1; }
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.headers[0].name')" = "Authorization" ] \
  || { echo "sensitive header name was not shown: $HTTP_BODY" >&2; exit 1; }
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.headers[0].sensitive')" = "true" ] \
  || { echo "sensitive header flag wrong: $HTTP_BODY" >&2; exit 1; }

log ">> api: a non-sensitive http header is returned in plaintext"
e2e_http_request POST "$API/api/source-credentials" \
  '{"name":"sc-plain","kind":"http","prefix":"https://api.example.com/","headers":[{"name":"X-Request-Id","value":"visible-value","sensitive":false}]}'
[ "$HTTP_CODE" = "201" ] || { echo "create plain returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request GET "$API/api/source-credentials/sc-plain"
[ "$HTTP_CODE" = "200" ] || { echo "get plain returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.headers[0].value')" = "visible-value" ] \
  || { echo "non-sensitive header value was redacted (want plaintext): $HTTP_BODY" >&2; exit 1; }
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.headers[0].sensitive')" = "false" ] \
  || { echo "plain header flag wrong: $HTTP_BODY" >&2; exit 1; }

log ">> api: multiple headers are stored, each redacted per its own sensitivity"
e2e_http_request POST "$API/api/source-credentials" \
  '{"name":"sc-multi","kind":"http","prefix":"https://api.example.com/","headers":[{"name":"Authorization","value":"Bearer multi-secret","sensitive":true},{"name":"X-Request-Id","value":"multi-visible","sensitive":false}]}'
[ "$HTTP_CODE" = "201" ] || { echo "create multi returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request GET "$API/api/source-credentials/sc-multi"
[ "$HTTP_CODE" = "200" ] || { echo "get multi returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.headers | length')" = "2" ] \
  || { echo "multi header count wrong: $HTTP_BODY" >&2; exit 1; }
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.headers[0].value')" = "[REDACTED]" ] \
  || { echo "multi sensitive header was not redacted: $HTTP_BODY" >&2; exit 1; }
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.headers[1].value')" = "multi-visible" ] \
  || { echo "multi plain header was redacted (want plaintext): $HTTP_BODY" >&2; exit 1; }

log ">> api: list shows each header redacted per its sensitivity"
e2e_http_request GET "$API/api/source-credentials"
[ "$HTTP_CODE" = "200" ] || { echo "list returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
sensitive=$(printf '%s' "$HTTP_BODY" | jq -r '.[] | select(.name=="sc-sensitive") | .headers[0].value')
plain=$(printf '%s' "$HTTP_BODY" | jq -r '.[] | select(.name=="sc-plain") | .headers[0].value')
[ "$sensitive" = "[REDACTED]" ] || { echo "list: sensitive header value = $sensitive, want [REDACTED]" >&2; exit 1; }
[ "$plain" = "visible-value" ] || { echo "list: plain header value = $plain, want visible-value" >&2; exit 1; }

log ">> api: update re-encrypts the secret material"
e2e_http_request PUT "$API/api/source-credentials/sc-sensitive" \
  '{"prefix":"https://api.example.com/","headers":[{"name":"Authorization","value":"Bearer rotated-secret","sensitive":true}]}'
[ "$HTTP_CODE" = "200" ] || { echo "update returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request GET "$API/api/source-credentials/sc-sensitive"
[ "$(printf '%s' "$HTTP_BODY" | jq -r '.headers[0].value')" = "[REDACTED]" ] \
  || { echo "after update, sensitive header value was not redacted: $HTTP_BODY" >&2; exit 1; }

log ">> api: delete removes the credentials"
e2e_http_request DELETE "$API/api/source-credentials/sc-sensitive"
[ "$HTTP_CODE" = "200" ] || { echo "delete sensitive returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request DELETE "$API/api/source-credentials/sc-plain"
[ "$HTTP_CODE" = "200" ] || { echo "delete plain returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request DELETE "$API/api/source-credentials/sc-multi"
[ "$HTTP_CODE" = "200" ] || { echo "delete multi returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request GET "$API/api/source-credentials/sc-sensitive"
[ "$HTTP_CODE" = "404" ] || { echo "get deleted returned $HTTP_CODE, want 404 ($HTTP_BODY)" >&2; exit 1; }

# --- 3 + 4. the sync-failure + delete-guard halves ---------------------------
# These need the API to dial THIS script's fetcher (CDROM_PIPELINE_SOURCE_ADDR),
# which is only the case in standalone mode (in shared mode the API is already
# up, dialing the default :7107). They are covered by the unit tests
# (TestSyncPipelineSourceFailureSurfaces, TestSourceCredentialDeleteGuard)
# regardless.
if [ -n "${E2E_STACK_READY:-}" ]; then
  log ">> (shared mode) skipping the refresh + delete-guard halves (API dials the default fetcher)"
else
# --- 3. a credential that fails to authenticate surfaces as a sync failure ---
# (F-21.3.2 / F-21.5.2) Attach a source to a pipeline that points at the 401
# sink and names a credential; a refresh must fail and record the auth error on
# the source state's last_error (visible via GET …/source).
log ">> sync: a credential that fails to authenticate surfaces as a sync failure"
e2e_http_request POST "$API/api/pipelines" '{"name":"sc-sync-pipeline"}'
[ "$HTTP_CODE" = "201" ] || { echo "create pipeline returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
PIPELINE_ID=$(printf '%s' "$HTTP_BODY" | jq -r '.id')
e2e_http_request POST "$API/api/source-credentials" \
  '{"name":"sc-stale","kind":"http","prefix":"http://127.0.0.1:10092/","headers":[{"name":"Authorization","value":"Bearer stale","sensitive":true}]}'
[ "$HTTP_CODE" = "201" ] || { echo "create stale credential returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request POST "$API/api/pipelines/$PIPELINE_ID/source" \
  "{\"kind\":\"http\",\"reference\":\"http:http://127.0.0.1:$E2E_PS_401/file.yaml\",\"credential\":\"sc-stale\"}"
[ "$HTTP_CODE" = "201" ] || { echo "attach source returned $HTTP_CODE, want 201 ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request POST "$API/api/pipelines/$PIPELINE_ID/source/refresh" ""
[ "$HTTP_CODE" = "422" ] || { echo "refresh returned $HTTP_CODE, want 422 (the sync should fail) ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request GET "$API/api/pipelines/$PIPELINE_ID/source"
[ "$HTTP_CODE" = "200" ] || { echo "get source returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
LAST_ERROR=$(printf '%s' "$HTTP_BODY" | jq -r '.state.last_error // empty')
[ -n "$LAST_ERROR" ] || { echo "last_error was not recorded on the source state: $HTTP_BODY" >&2; exit 1; }
echo "   last_error: $LAST_ERROR"

# --- 4. a credential a source references cannot be deleted (409) ------------
# (F-21.3.3) The source above names sc-stale, so deleting it is rejected; after
# the source is gone it can be deleted.
log ">> api: a credential a source references cannot be deleted (409)"
e2e_http_request DELETE "$API/api/source-credentials/sc-stale"
[ "$HTTP_CODE" = "409" ] || { echo "delete referenced credential returned $HTTP_CODE, want 409 ($HTTP_BODY)" >&2; exit 1; }
# Detach the source (clears the reference), then the credential can be deleted.
e2e_http_request DELETE "$API/api/pipelines/$PIPELINE_ID/source"
[ "$HTTP_CODE" = "200" ] || { echo "detach source returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
e2e_http_request DELETE "$API/api/source-credentials/sc-stale"
[ "$HTTP_CODE" = "200" ] || { echo "delete unreferenced credential returned $HTTP_CODE, want 200 ($HTTP_BODY)" >&2; exit 1; }
fi

log ">> 14-source-credentials PASSED"
