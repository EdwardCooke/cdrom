#!/usr/bin/env bash
#
# 13-source-fetcher.sh — verifies the F-21.2 source-fetcher service end to end.
#
# Boots the standalone source-fetcher (cmd/pipelinesource) and drives its
# FetchSource RPC (via the fetchclient helper) for each reference kind:
#
#   - file:  a rooted local read (content + mtime + sha256), path-traversal
#            rejection, and the disabled-without-root error.
#   - http:  a GET with no credential, and a GET whose Authorization header
#            comes from the http credential.
#   - git:   a clone from a local repository, a refetch that absorbs a
#            rewritten (force-pushed) branch, and the commit-hash version.
#   - plus:  an unknown reference type is a clear error, and the dedicated
#            metrics port serves the fetcher's metric set.
#
# The fetcher is standalone (it needs no db/scheduler/api), so this script
# boots only the fetcher (plus a local HTTP sink and a local git repo), whether
# run on its own or from the shared e2e stack.
#
# Usage: scripts/e2e/13-source-fetcher.sh
#
# Requires: the built binaries in ./bin (run `make build` first), the
# fetchclient helper (built by `make build`), curl, jq, git, and python3.

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

E2E_PS_ADDR=127.0.0.1:7107
E2E_PS_NOROOT_ADDR=127.0.0.1:7108
E2E_PS_METRICS=9106
E2E_PS_HTTP=10090

# --- workspace ----------------------------------------------------------------
# In shared-stack mode WORK is inherited from the main script; otherwise create
# a fresh one. Either way this script owns the processes it starts.
if [ -z "${WORK:-}" ]; then
  WORK="$(mktemp -d /tmp/cdrom-e2e.XXXXXX)"
  E2E_OWN_STACK=1
  echo "WORKDIR: ${WORK}"
fi
trap e2e_cleanup EXIT

PS_FILES="$WORK/ps-files"
PS_CACHE="$WORK/ps-cache"
PS_CACHE_NOROOT="$WORK/ps-cache-noroot"
PS_HTTPDIR="$WORK/ps-http"
mkdir -p "$PS_FILES" "$PS_CACHE" "$PS_CACHE_NOROOT" "$PS_HTTPDIR"

# --- fixtures ------------------------------------------------------------------
# A file: fixture.
printf 'shared: variables\n' > "$PS_FILES/shared-variables.yaml"
# An http: fixture (served by the python sink below).
printf 'http: body\n' > "$PS_HTTPDIR/file.yaml"
# A git: fixture — a local repository with a file on branch main.
PS_REPO="$WORK/ps-repo"
git init -q -b main "$PS_REPO"
git -C "$PS_REPO" config user.name e2e
git -C "$PS_REPO" config user.email e2e@cdrom.local
printf 'git: v1\n' > "$PS_REPO/file.yaml"
git -C "$PS_REPO" add -A
git -C "$PS_REPO" commit -qm v1

# --- boot the fetcher ----------------------------------------------------------
# The file: kind is rooted at PS_FILES; the git cache lives in PS_CACHE.
e2e_start pipelinesource "$WORK/pipelinesource.log" 'service starting' \
  CDROM_LISTEN_ADDR="$E2E_PS_ADDR" \
  CDROM_PIPELINE_SOURCE_FILE_ROOT="$PS_FILES" \
  CDROM_PIPELINE_SOURCE_REPO_CACHE_DIR="$PS_CACHE" \
  CDROM_METRICS_ADDR="127.0.0.1:$E2E_PS_METRICS" \
  CDROM_LOG_FORMAT=text -- "$BIN/pipelinesource"

# A second fetcher with NO file root (to assert the file: kind is disabled).
e2e_start pipelinesource-noroot "$WORK/pipelinesource-noroot.log" 'service starting' \
  CDROM_LISTEN_ADDR="$E2E_PS_NOROOT_ADDR" \
  CDROM_PIPELINE_SOURCE_REPO_CACHE_DIR="$PS_CACHE_NOROOT" \
  CDROM_METRICS_ADDR="127.0.0.1:9107" \
  CDROM_LOG_FORMAT=text -- "$BIN/pipelinesource"

# A local HTTP sink that serves PS_HTTPDIR, sets an ETag, and records the
# Authorization header of each request (so the credential assertion can check
# it).
python3 - "$E2E_PS_HTTP" "$PS_HTTPDIR" "$WORK/ps-http-auth.log" <<'PY' &
import http.server, sys
port, root, authlog = int(sys.argv[1]), sys.argv[2], sys.argv[3]
class H(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=root, **k)
    def end_headers(self):
        self.send_header("ETag", '"v1"')
        super().end_headers()
    def do_GET(self):
        with open(authlog, "a") as f:
            f.write(self.headers.get("Authorization", "") + "\n")
        super().do_GET()
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

# --- helpers -------------------------------------------------------------------
# fetch <addr> <ref> [cred-args...] -> sets F_CONTENT (stdout), F_VERSION, F_HASH
# (from the stderr metadata lines).
fetch() {
  local addr="$1" ref="$2"; shift 2
  local meta
  meta="$WORK/ps-meta.txt"
  F_CONTENT=$("$BIN/fetchclient" -addr "$addr" -ref "$ref" "$@" 2>"$meta") || return 1
  F_VERSION=$(grep '^version=' "$meta" | cut -d= -f2- || true)
  F_HASH=$(grep '^hash=' "$meta" | cut -d= -f2- || true)
}

# --- file: fetch (rooted) ------------------------------------------------------
log ">> file: rooted read"
fetch "$E2E_PS_ADDR" "file:/shared-variables.yaml"
[ "$F_CONTENT" = "shared: variables" ] || { echo "file content mismatch: $F_CONTENT" >&2; exit 1; }
[ -n "$F_VERSION" ] || { echo "file version (mtime) is empty" >&2; exit 1; }
expected=$(sha256sum "$PS_FILES/shared-variables.yaml" | cut -d' ' -f1)
[ "$F_HASH" = "$expected" ] || { echo "file hash mismatch: $F_HASH vs $expected" >&2; exit 1; }

# --- file: path traversal is rejected -----------------------------------------
log ">> file: path traversal rejected"
if fetch "$E2E_PS_ADDR" "file:../escape.yaml"; then
  echo "path traversal was not rejected" >&2; exit 1
fi

# --- file: disabled without a root --------------------------------------------
log ">> file: disabled without a root"
if fetch "$E2E_PS_NOROOT_ADDR" "file:/x.yaml"; then
  echo "file fetch should fail when no root is configured" >&2; exit 1
fi

# --- http: fetch (no credential) ----------------------------------------------
# The reference is "<type>:<spec>", so an http reference is "http:" + the URL.
log ">> http: plain GET"
fetch "$E2E_PS_ADDR" "http:http://127.0.0.1:$E2E_PS_HTTP/file.yaml"
[ "$F_CONTENT" = "http: body" ] || { echo "http content mismatch: $F_CONTENT" >&2; exit 1; }
[ "$F_VERSION" = '"v1"' ] || { echo "http version (ETag) mismatch: $F_VERSION" >&2; exit 1; }

# --- http: fetch (credential header) ------------------------------------------
log ">> http: credential-supplied Authorization header"
fetch "$E2E_PS_ADDR" "http:http://127.0.0.1:$E2E_PS_HTTP/file.yaml" -cred-kind http -cred-token tok123
[ "$F_CONTENT" = "http: body" ] || { echo "http (cred) content mismatch: $F_CONTENT" >&2; exit 1; }
grep -q "Bearer tok123" "$WORK/ps-http-auth.log" || { echo "http credential header was not sent" >&2; exit 1; }

# --- git: fetch (clone) --------------------------------------------------------
log ">> git: clone from a local repository"
gitref="git:$PS_REPO@main[/file.yaml]"
fetch "$E2E_PS_ADDR" "$gitref"
[ "$F_CONTENT" = "git: v1" ] || { echo "git content mismatch: $F_CONTENT" >&2; exit 1; }
head=$(git -C "$PS_REPO" rev-parse HEAD)
[ "$F_VERSION" = "$head" ] || { echo "git version mismatch: $F_VERSION vs $head" >&2; exit 1; }

# --- git: refetch absorbs a rewritten (force-pushed) branch --------------------
log ">> git: refetch absorbs a rewritten branch"
printf 'git: v2\n' > "$PS_REPO/file.yaml"
git -C "$PS_REPO" add -A
git -C "$PS_REPO" commit -q --amend -m v1   # rewrites the single commit (force-push equivalent)
fetch "$E2E_PS_ADDR" "$gitref"
[ "$F_CONTENT" = "git: v2" ] || { echo "git refetch did not absorb the rewritten branch: $F_CONTENT" >&2; exit 1; }
head2=$(git -C "$PS_REPO" rev-parse HEAD)
[ "$F_VERSION" = "$head2" ] || { echo "git refetch version mismatch: $F_VERSION vs $head2" >&2; exit 1; }
[ "$head2" != "$head" ] || { echo "the amend did not rewrite the commit (test is vacuous)" >&2; exit 1; }

# --- unknown reference type is a clear error ----------------------------------
log ">> unknown reference type rejected"
if fetch "$E2E_PS_ADDR" "svn:https://example.com/repo"; then
  echo "unknown reference type was not rejected" >&2; exit 1
fi

# --- metrics port ----------------------------------------------------------------
log ">> dedicated metrics port serves the fetcher's metric set"
curl -sf "http://127.0.0.1:$E2E_PS_METRICS/healthz" >/dev/null || { echo "metrics /healthz failed" >&2; exit 1; }
m=$(curl -sf "http://127.0.0.1:$E2E_PS_METRICS/metrics")
echo "$m" | grep -q "cdrom_pipelinesource_fetches_total" || { echo "fetch metric missing" >&2; exit 1; }
echo "$m" | grep -q "cdrom_up" || { echo "cdrom_up missing" >&2; exit 1; }

log ">> 13-source-fetcher PASSED"
