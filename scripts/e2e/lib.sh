# shellcheck shell=bash
#
# lib.sh — shared helpers for the cdrom e2e test scripts (scripts/e2e/).
#
# Each test script is single-purpose and executable on its own: it sources
# this library, boots the stack it needs, runs its assertions, and tears the
# stack down on exit. The main script (scripts/e2e.sh) boots the stack once
# and runs every test script against it, in order.
#
# The library is NOT executable on its own; it is sourced.

set -euo pipefail

# --- death forensics (flake defense, 2026-11-05) ----------------------------
# Export E2E_DEATHLOG=<file> and EVERY command that fatally trips `set -e`
# (including inside $( ) subshells — errexit propagates upward) appends one
# line naming the culprit: pid, line, function context, rc, and the command
# itself. The ERR trap fires only on UNRESCUED failures (guarded forms like
# `cmd || true` never trip it), so the happy path costs nothing and produces
# no noise. Writes to its OWN file, so it survives whatever the harness does
# to fds 1/2 (a lesson from the 18-approval-groups silent-death hunt: bash -x
# traces evaporate when forked children inherit the trace's descriptors).
_e2e_death_hook() {
  local ec=$?
  [ -n "${E2E_DEATHLOG:-}" ] || return 0
  { printf 'death pid=%s line=%s fn=[%s] rc=%s cmd=[%.400s]\n' \
      "$$" "${LINENO}" "${FUNCNAME[*]:-top}" "$ec" "${BASH_COMMAND:-unknown}" ; } \
      >>"${E2E_DEATHLOG}" 2>/dev/null || true
}
set -E
trap '_e2e_death_hook' ERR

# --- THE POISON RULE (learned the hard way, 2026-11-05) --------------------
# NEVER put a bare `exec <redir>` (especially anything touching fd 2) in the
# CONTROLLING SHELL of a test script. Bash applies `exec`'s redirections
# persistently to the current shell when no command follows, so
# `exec 3>&- 2>/dev/null` — the former socket-probe cleanup idiom in several
# readiness loops — rewired the ENTIRE SCRIPT'S stderr to /dev/null for the
# rest of the run. Every failure diagnostic afterwards (want mismatches,
# timeouts, login errors) was computed and thrown away: the "silent death"
# plague behind weeks of undiagnosable 18-approval-groups flakes. Socket
# probes belong in a SUBSHELL — `(exec 3<>"/dev/tcp/host/port") 2>/dev/null`
# — whose fds die with it; there is nothing to clean up outside.
# Related consequence: TERMINAL diagnostics (messages paired with `exit 1` /
# `return 1`) are written to STDOUT below, not stderr — a dead script prints
# nothing after them anyway, so stdout is pollution-free and survives any
# future stderr mishaps. Progress chatter (heartbeats) may stay on stderr.
# ----------------------------------------------------------------------------

# --- paths -----------------------------------------------------------------
# E2E_ROOT is the scripts/e2e directory (this file's directory); E2E_REPO is
# the repository root. Scripts may be run from anywhere.
E2E_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_REPO="$(cd "${E2E_ROOT}/../.." && pwd)"
BIN="${E2E_REPO}/bin"

# --- stack state (set by e2e_boot_stack) ------------------------------------
# WORK is the temp directory holding the stack's logs, SQLite database, and
# artifacts; WORKDIR is the scratch directory job steps may use as a workdir.
# In shared-stack mode (E2E_STACK_READY set by the main script) these are
# inherited from the environment.
WORK="${WORK:-}"
WORKDIR="${WORKDIR:-}"
# PIDS holds the PIDs of every process this script started; e2e_cleanup kills
# them. E2E_OWN_STACK is 1 when this script booted the stack itself (and so
# must remove WORK on exit) and 0 in shared-stack mode (the main script owns
# WORK).
PIDS=()
E2E_OWN_STACK=0

# --- mTLS foundation (the whole gRPC mesh + the IdP's HTTPS surface) --------
# Generates a THROWAWAY CA + leaf certificate set into $WORK/certs (hermetic —
# never depends on committed/stale certs; ruled 2026-11-06) and EXPORTS the
# CDROM_TLS_* triple that every service launched afterwards inherits: e2e_start
# execs `env … cmd` in the CURRENT environment, so one export turns the ENTIRE
# gRPC mesh (db, scheduler, artifacts, idp, api, pipelinesource, worker, agent,
# and every per-scenario replica) into mutual TLS with zero per-launch edits.
# CURL_CA_BUNDLE makes every curl in the suite trust OUR CA, so https:// probes
# to the IdP's OIDC/SCIM surfaces verify instead of failing.
#
# WHY THE IdP'S HTTP SURFACE NECESSARILY BECOMES HTTPS: the IdP multiplexes its
# single cfg.TLS onto BOTH its gRPC server and its OIDC/SCIM/break-glass HTTP
# listener, and break-glass's mTLS client-cert check lives on THAT listener —
# so a TLS IdP serves HTTPS unavoidably. The e2e embraces it (ruled 2026-11-06):
# the main IdP is launched with CDROM_IDP_ISSUER=https://127.0.0.1:7104
# (otherwise it would advertise the http:// default), and the API's
# CDROM_AUTH_ISSUER follows suit (the discovery URL it fetches AND the iss it
# verifies — both must match the IdP's announced issuer).
e2e_setup_tls() {
  [ -n "$WORK" ] || { echo ">> e2e_setup_tls: WORK is not set" >&2; exit 1; }
  local certs="$WORK/certs"
  "${E2E_REPO}/scripts/gencerts.sh" "$certs" >/dev/null \
    || { echo ">> gencerts failed (openssl required)" >&2; exit 1; }
  export CDROM_TLS_CA_FILE="$certs/ca.crt"
  export CDROM_TLS_CERT_FILE="$certs/cert.crt"
  export CDROM_TLS_KEY_FILE="$certs/cert.key"
  export CURL_CA_BUNDLE="$certs/ca.crt"
  log ">> mTLS: certs in $certs (CDROM_TLS_* + CURL_CA_BUNDLE exported — the gRPC mesh negotiates mutual TLS)"
}

# F-18 notification sinks: the fake SMTP server and fake webhook HTTP server
# the API's email/webhook sends reach. They run on fixed localhost ports and
# record what they receive under WORK (the test scripts read those files).
E2E_SMTP_PORT=1025
E2E_WEBHOOK_PORT=10080
E2E_WEBHOOK_FAIL_PORT=10081

# --- logging -----------------------------------------------------------------
log() { printf '%s\n' "$*"; }

# --- log archiving -------------------------------------------------------------
# E2E_LOG_HOME is where the suite PERSISTS the stack logs + database for
# post-mortems instead of losing them with $WORK. It lives under the repo's
# gitignored .local/ directory (and is excluded from Copilot's workspace
# search via .vscode/settings.json); the ten newest archives are retained.
E2E_LOG_HOME="${E2E_LOG_HOME:-$E2E_REPO/.local/e2e}"

# e2e_archive_logs <tag>
# Snapshots $WORK's logs + the SQLite database into
# E2E_LOG_HOME/<utc-timestamp>-<tag>/ and prunes older archives beyond the
# retention window. Invoked from the cleanup paths (standalone: this script's
# e2e_cleanup; shared-stack: scripts/e2e.sh's cleanup) so the logs survive the
# WORK-directory removal — on success AND on failure alike.
e2e_archive_logs() {
  local tag="${1:-adhoc}" dest
  [ -n "${WORK:-}" ] && [ -d "${WORK:-}" ] || return 0
  dest="$E2E_LOG_HOME/$(date -u +%Y%m%d-%H%M%S)-$tag"
  mkdir -p "$dest"
  cp -a "$WORK"/*.log "$dest/" 2>/dev/null || true
  if [ -f "$WORK/cdrom.db" ]; then cp -a "$WORK/cdrom.db" "$dest/" 2>/dev/null || true; fi
  # Retention: keep the ten newest archives.
  ls -1dt "$E2E_LOG_HOME"/*/ 2>/dev/null | tail -n +11 | xargs -r rm -rf 2>/dev/null || true
  echo ">> e2e logs archived to $dest"
}

# --- stack lifecycle ---------------------------------------------------------
# e2e_cleanup kills every process this script started and, when this script
# booted the stack itself, removes the temp directory. It is installed as an
# EXIT trap by e2e_boot_stack, so the stack is always torn down, even on
# failure.
e2e_cleanup() {
  local code=$?
  if [ "${#PIDS[@]}" -gt 0 ]; then
    local pid
    for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    sleep 1
    for pid in "${PIDS[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
  fi
  if [ "$E2E_OWN_STACK" = "1" ] && [ -n "$WORK" ]; then
    e2e_archive_logs "${0##*/}"
    rm -rf "$WORK"
  fi
  if [ "$code" -ne 0 ]; then
    echo ">> e2e FAILED (exit $code)"
    exit "$code"
  fi
}

# e2e_boot_stack [with-worker]
# Boots the full local stack (db, scheduler, artifacts, idp, api) against a
# throwaway SQLite database in a fresh temp directory, and installs the
# cleanup trap. With "with-worker" it also starts a worker in the "default"
# group and waits for its WatchJobs stream to be open on the API.
#
# The API runs with job-token auth enabled (the IdP mints the job tokens the
# API hands to execution targets) and with the audit log (F-15) written to a
# file separate from the process's default log, in JSON.
#
# Shared-stack mode: when E2E_STACK_READY is set (by the main script,
# scripts/e2e.sh), the stack is already running and WORK/WORKDIR are
# inherited from the environment; this function only installs the cleanup
# trap (which then tears down only the processes this script itself starts,
# e.g. the RBAC API).
e2e_boot_stack() {
  local with_worker="${1:-}"
  if [ -n "${E2E_STACK_READY:-}" ]; then
    [ -n "$WORK" ] || { echo ">> E2E_STACK_READY is set but WORK is not"; exit 1; }
    E2E_OWN_STACK=0
    trap e2e_cleanup EXIT
    return 0
  fi
  E2E_OWN_STACK=1
  WORK="$(mktemp -d /tmp/cdrom-e2e.XXXXXX)"
  echo "WORKDIR: ${WORK}"
  WORKDIR="$WORK/workdir"
  mkdir -p "$WORK/artifacts" "$WORKDIR"
  trap e2e_cleanup EXIT
  # Mutual TLS for the WHOLE gRPC mesh (ruled 2026-11-06): generate a
  # throwaway CA + leaf set and export the CDROM_TLS_* triple every service
  # inherits (see e2e_setup_tls for the full rationale).
  e2e_setup_tls

  # The SQLite DSN carries two pragmas (the pure-Go driver, modernc.org/sqlite
  # via glebarez/sqlite, applies them on EVERY connection it opens):
  #   journal_mode=wal — switch the file to write-ahead logging: READERS then
  #     proceed CONCURRENTLY with writers (the legacy rollback journal blocks
  #     all readers for the whole write transaction). The local stack funnels
  #     many concurrent connections onto one file (the db service's own pool,
  #     the API's event-log tail, worker heartbeats, telemetry), so WAL is the
  #     difference between smooth concurrency and lock storms.
  #   busy_timeout=10000 — when a statement DOES meet a lock, WAIT up to 10 s
  #     for the holder to release it instead of failing immediately with
  #     SQLITE_BUSY ("database is locked"). It is per-CONNECTION (not
  #     persistent), which is why it travels in the DSN rather than being
  #     issued once. Raised from 5 s (2026-11-05): under the 18-approval-groups
  #     burst of rapid job submits + approvals + audit writes, 5 s still
  #     exhausted and the API surfaced a 500.
  # Needed together: at peak e2e load the unlocked default surfaced SQLITE_BUSY
  # on the API's event tail and worker heartbeats, stretching the scheduler's
  # 5 s ticks and starving the test waits (the 17-restoration flake). Belt-and-
  # braces with the request-level locked-retry in __e2e_retry_locked below.
  e2e_start db        "$WORK/db.log"        'serving'    CDROM_DB_BACKEND=sqlite CDROM_DB_SQLITE_PATH="$WORK/cdrom.db?_pragma=busy_timeout=10000&_pragma=journal_mode=wal" CDROM_LOG_FORMAT=text -- "$BIN/db"
  e2e_start scheduler "$WORK/scheduler.log" 'serving'    CDROM_LOG_FORMAT=text -- "$BIN/scheduler"
  e2e_start artifacts "$WORK/artifacts.log" 'serving'    CDROM_ARTIFACTS_ROOT="$WORK/artifacts" CDROM_LOG_FORMAT=text -- "$BIN/artifacts"
  # The IdP mints the job tokens the API hands to execution targets (and the
  # exchanged tokens a token_exchange step requests). It is started before the
  # API so the API's job-token auth (OIDC discovery against the IdP) can
  # initialize.
  # CDROM_IDP_ISSUER=https://… is REQUIRED under mTLS: the IdP's OIDC surface
  # necessarily serves HTTPS (it shares cfg.TLS), so its discovery document
  # announces an https issuer — and the API's CDROM_AUTH_ISSUER must match it.
  e2e_start idp       "$WORK/idp.log"       'idp starting' CDROM_IDP_ISSUER=https://127.0.0.1:7104 CDROM_LOG_FORMAT=text -- "$BIN/idp"
  # The IdP logs "starting" before its HTTP server is listening, so poll its
  # discovery endpoint until it responds; the API's job-token auth performs
  # OIDC discovery against the IdP at startup and would fail if the IdP is not
  # up. (https — the IdP serves TLS; CURL_CA_BUNDLE trusts our CA.)
  log ">> waiting for the IdP to serve"
  local i
  for i in $(seq 1 100); do
    if curl -sf https://127.0.0.1:7104/.well-known/openid-configuration >/dev/null 2>&1; then
      break
    fi
    # HEARTBEAT (anti-starvation): a silent readiness wait is a reap risk.
    if [ $((i % 5)) -eq 0 ]; then
      echo "   ...waiting for the IdP (iteration $i)"
    fi
    sleep 0.2
  done
  curl -sf https://127.0.0.1:7104/.well-known/openid-configuration >/dev/null 2>&1 \
    || { echo ">> IdP did not start serving"; exit 1; }
  # The F-18 notification sinks (a fake SMTP server and a fake webhook HTTP
  # server) are started before the API so the API's email/webhook sends have a
  # target. The API is configured to send email to the SMTP sink (plaintext,
  # STARTTLS disabled); a webhook step's url points at the webhook sink.
  e2e_start_sinks
  # Enable job-token auth on the API's gRPC surface: the API mints a job token
  # for the worker (audience cdrom-api, the IdP's default) and verifies it on
  # the worker's job-scoped calls, including the token_exchange step's
  # ExchangeJobToken RPC. The email config points the API's SMTP sender at the
  # fake SMTP sink (F-18).
  # CDROM_GRPC_AUTH_IDP_ADDR is a full https:// URL (not a bare host:port):
  # the job-token auth performs OIDC discovery + mint against the IdP's
  # HTTPS surface (a bare address would be coerced to http:// by
  # normalizeIDPURL and fail handshake). The gRPC DIAL address is separate
  # (CDROM_IDP_GRPC_ADDR, inherited default 127.0.0.1:7106).
  e2e_start api       "$WORK/api.log"       'serving'    CDROM_GRPC_AUTH_ENABLED=true CDROM_GRPC_AUTH_IDP_ADDR=https://127.0.0.1:7104 CDROM_GRPC_AUTH_AUDIENCES=cdrom-api CDROM_EMAIL_HOST=127.0.0.1 CDROM_EMAIL_PORT="$E2E_SMTP_PORT" CDROM_EMAIL_STARTTLS=false CDROM_EMAIL_FROM=e2e@cdrom.local CDROM_AUDIT_FILE="$WORK/audit.log" CDROM_AUDIT_FORMAT=json CDROM_LOG_FORMAT=text -- "$BIN/api"
  if [ "$with_worker" = "with-worker" ]; then
    e2e_start worker  "$WORK/worker.log"    'registered' CDROM_LOG_FORMAT=text -- "$BIN/worker"
    # Gate on the API having the worker's WatchJobs stream open before
    # dispatching.
    e2e_wait_for "$WORK/api.log" 'worker watching' 'worker watch stream on the api'
  fi
}

# e2e_start <name> <logfile> <ready-pattern> <env...> -- <cmd...>
# Launches a service in the background, records its PID, and waits until its
# log matches <ready-pattern> (or the process dies / times out).
e2e_start() {
  local name="$1" logfile="$2" pattern="$3"; shift 3
  local envs=() cmd=() seen=0 a
  for a in "$@"; do
    if [ "$a" = "--" ]; then seen=1; continue; fi
    if [ "$seen" -eq 0 ]; then envs+=("$a"); else cmd+=("$a"); fi
  done
  log ">> starting $name"
  env "${envs[@]}" "${cmd[@]}" >"$logfile" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  local i
  for i in $(seq 1 50); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo ">> $name exited early:"
      cat "$logfile"
      exit 1
    fi
    if grep -qE "$pattern" "$logfile" 2>/dev/null; then
      return 0
    fi
    # HEARTBEAT (anti-starvation): a silent readiness wait is a reap risk.
    if [ $((i % 5)) -eq 0 ]; then
      echo "   ...waiting for $name (iteration $i)"
    fi
    sleep 0.2
  done
  echo ">> $name did not become ready in time (pattern: $pattern):"
  cat "$logfile"
  exit 1
}

# e2e_start_sinks
# Starts the F-18 notification sinks (a fake SMTP server on E2E_SMTP_PORT and
# a fake webhook HTTP server on E2E_WEBHOOK_PORT, plus a second webhook server
# on E2E_WEBHOOK_FAIL_PORT that returns 500 for the failure-semantics case).
# Each records what it receives under WORK (smtp.log / webhook.log /
# webhook-fail.log). The sinks are started before the API so the API's
# email/webhook sends have a target; they are killed by e2e_cleanup.
e2e_start_sinks() {
  python3 "${E2E_ROOT}/sinks/smtp_sink.py" "$E2E_SMTP_PORT" "$WORK/smtp.log" &
  PIDS+=($!)
  python3 "${E2E_ROOT}/sinks/webhook_sink.py" "$E2E_WEBHOOK_PORT" "$WORK/webhook.log" 200 &
  PIDS+=($!)
  python3 "${E2E_ROOT}/sinks/webhook_sink.py" "$E2E_WEBHOOK_FAIL_PORT" "$WORK/webhook-fail.log" 500 &
  PIDS+=($!)
  # Wait for the sinks to be listening before the API (and any test) uses them.
  local i
  for i in $(seq 1 50); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$E2E_SMTP_PORT") 2>/dev/null; then
      # The probe's fd 3 lives and dies with ITS SUBSHELL — nothing to close
      # here. WARNING: do NOT reintroduce a bare `exec 3>&- ...` in this
      # (controlling) shell: with no command, `exec` applies its redirections
      # PERSISTENTLY to the current shell, and `exec 3>&- 2>/dev/null` hereby
      # rewired the WHOLE SCRIPT'S stderr to /dev/null for the rest of every
      # run — vaporising all failure diagnostics ("silent death" plague,
      # 2026-11-05).
      break
    fi
    # HEARTBEAT (anti-starvation): a silent readiness wait is a reap risk.
    if [ $((i % 10)) -eq 0 ]; then
      echo "   ...waiting for the notification sinks (iteration $i)"
    fi
    sleep 0.1
  done
}

# e2e_wait_for <logfile> <pattern> <what>
# Polls <logfile> until it contains <pattern>. The pattern is matched as a
# fixed string (grep -F), not a regex: callers pass literal log lines (which
# may contain regex metacharacters such as the '?' in a templated webhook url),
# so a regex interpretation would silently fail to match.
e2e_wait_for() {
  local logfile="$1" pattern="$2" what="$3" i
  for i in $(seq 1 100); do
    if grep -qF -- "$pattern" "$logfile" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  echo ">> timed out waiting for: $what (pattern: $pattern)"
  tail -n 40 "$logfile"
  exit 1
}

# e2e_wait_for_job <jobid> <pattern> <what>
# Polls the job's combined log (GET /api/jobs/{id}/logs/job.log) until it
# contains <pattern>. The pattern is matched as a FIXED STRING (substring
# test, not a regex). Each request is bounded (--max-time) so a transiently
# slow API response is retried rather than blocking the test forever.
#
# IMPORTANT: the response is CAPTURED THEN MATCHED — never piped into
# `grep -q`: under the library's `set -o pipefail`, a `grep -q` that finds
# its match early exits and SIGPIPEs a still-writing curl, flipping the
# pipeline's status and killing the script via `set -e` at the very moment
# the awaited output appears.
e2e_wait_for_job() {
  local jobid="$1" pattern="$2" what="$3" i body
  for i in $(seq 1 100); do
    body=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs/${jobid}/logs/job.log" || true)
    if [[ "$body" == *"$pattern"* ]]; then
      return 0
    fi
    # HEARTBEAT (anti-starvation): the opening stretch of a wait must never
    # be silent (tick 1 + every fifth poll).
    if [ "$i" -eq 1 ] || [ $((i % 5)) -eq 0 ]; then
      echo "   ...waiting for job $jobid: $what (iteration $i)"
    fi
    sleep 0.2
  done
  echo ">> timed out waiting for: $what (pattern: $pattern)"
  exit 1
}

# e2e_wait_for_status <jobid> <status> <what>
# Polls GET /api/jobs/{id} until the job's status equals <status> (the numeric
# JobStatus value, e.g. 8 for awaiting_approval, 3 for succeeded, 4 for
# failed). Returns the last observed job JSON on stdout. Each request is
# bounded (--max-time) so a transiently slow API response is retried rather
# than blocking the test forever.
e2e_wait_for_status() {
  local jobid="$1" status="$2" what="$3" i body
  for i in $(seq 1 100); do
    # A transiently slow or unreachable API response is treated as "not ready
    # yet" (retry) rather than aborting the test: `|| true` keeps the
    # assignment's exit status 0 so set -e does not stop on a failed curl.
    body=$(curl -s --max-time 5 "http://127.0.0.1:8080/api/jobs/${jobid}" || true)
    if [ "$(printf '%s' "$body" | jq -r '.status' 2>/dev/null)" = "$status" ]; then
      printf '%s' "$body"
      return 0
    fi
    # HEARTBEAT (anti-starvation): the opening stretch of a wait must never
    # be silent (tick 1 + every fifth poll).
    if [ "$i" -eq 1 ] || [ $((i % 5)) -eq 0 ]; then
      echo "   ...waiting for job $jobid: $what (iteration $i)" >&2
    fi
    sleep 0.2
  done
  echo ">> timed out waiting for: $what (status ${status})"
  printf '%s' "$body"
  exit 1
}

# --- HTTP helpers ------------------------------------------------------------
# __e2e_perform <curl-flags...>
# INTERNAL: performs one HTTP request (via curl) and REPEATS it, bounded, when
# the API answers 500 citing a transient SQLite lock (SQLITE_BUSY /
# "database is locked"). Such a write ABORTED its transaction and took no
# effect, so replaying the IDENTICAL request is safe and correct — unlike a
# retry of an unknown outcome. Transport errors are swallowed (|| true) so a
# network blip surfaces as an empty HTTP_CODE the caller judges, never a
# set -e death. Sets HTTP_CODE and HTTP_BODY. Flags exclude -s/-w (added here).
__e2e_perform() {
  local out attempt
  for attempt in 1 2 3 4 5; do
    # --max-time bounds EVERY request (hung-response insurance); callers need
    # not supply it. Kept ABOVE the DB busy_timeout (10 s) so a lock-contended
    # handler still RETURNS its 500/SQLITE_BUSY (reactable) instead of curl
    # cutting the connection first (empty HTTP_CODE, unrecoverable).
    out=$(curl -s --max-time 15 -w $'\n%{http_code}' "$@" || true)
    HTTP_CODE=$(printf '%s' "$out" | tail -n 1)
    HTTP_BODY=$(printf '%s' "$out" | sed '$d')
    if [ "$HTTP_CODE" = "500" ] && printf '%s' "$HTTP_BODY" | grep -qi 'SQLITE_BUSY\|database is locked'; then
      echo "   ...met a transient SQLite lock (SQLITE_BUSY); retry $attempt/5" >&2
      sleep 0.5
      continue
    fi
    break
  done
}

# e2e_http_request <method> <url> [json-body]
# Performs an HTTP request and sets HTTP_CODE (the response status code) and
# HTTP_BODY (the response body). When a JSON body is given it is sent with a
# Content-Type: application/json header. Retries transient SQLite locks (see
# __e2e_perform). Used by the F-24/F-14 assertions, which need both the
# status code and the body from a single call.
e2e_http_request() {
  local method="$1" url="$2" body="${3:-}"
  if [ -n "$body" ]; then
    __e2e_perform -X "$method" "$url" -H 'Content-Type: application/json' -d "$body"
  else
    __e2e_perform -X "$method" "$url"
  fi
}

# e2e_binary_post <url> <header-json> <payload-file>
# Posts a BINARY-SAFE body: the header JSON line + newline + the file's raw
# bytes, assembled in a temp file and sent with `curl --data-binary @file`.
# Bash VARIABLES LOSE NUL bytes (command substitution truncates them — a
# gzip/zip payload piped through a variable is corrupted), so binary payloads
# must never traverse a shell variable. Sets HTTP_CODE and HTTP_BODY (the
# response TEXT; binary responses are not expected here).
e2e_binary_post() {
  local url="$1" header_json="$2" payload_file="$3"
  local body_file out
  body_file=$(mktemp)
  { printf '%s\n' "$header_json"; cat "$payload_file"; } > "$body_file"
  out=$(curl -s -w $'\n%{http_code}' -X POST "$url" \
    -H 'Content-Type: application/json' --data-binary "@$body_file" || true)
  rm -f "$body_file"
  HTTP_CODE=$(printf '%s' "$out" | tail -n 1)
  HTTP_BODY=$(printf '%s' "$out" | sed '$d')
}

# e2e_authed_request <method> <url> <bearer-token> [json-body]
# Like e2e_http_request, but presents the given OIDC token as a Bearer token
# (used by the F-14 assertions against the RBAC-enabled API, where every
# request must carry a valid token). Sets HTTP_CODE and HTTP_BODY.
#
# PARITY WITH e2e_http_request (bugfix, 2026-11-05): transport errors are
# ALSO swallowed here (|| true) and the request is bounded (--max-time). A
# transient curl failure (connection reset/refused spike) must surface as an
# EMPTY HTTP_CODE that the caller's `want` check fails LOUDLY — never as a
# silent set -e death mid-script (which produced undiagnosible e2e flakes).
e2e_authed_request() {
  local method="$1" url="$2" token="$3" body="${4:-}"
  # (__e2e_perform supplies the --max-time bound internally.)
  if [ -n "$body" ]; then
    __e2e_perform -X "$method" "$url" \
      -H "Authorization: Bearer $token" \
      -H 'Content-Type: application/json' -d "$body"
  else
    __e2e_perform -X "$method" "$url" \
      -H "Authorization: Bearer $token"
  fi
}

# --- job submission ----------------------------------------------------------
# e2e_submit_job <json-body>
# POSTs a job to the API and prints the job's id on stdout. The full response
# is echoed to stderr so it stays out of the caller's command substitution
# (callers capture the id with $(e2e_submit_job ...)).
e2e_submit_job() {
  local body="$1" response
  # Locked-retries happen inside __e2e_perform (transparent to the caller);
  # transport errors leave HTTP_BODY empty and the id-parse check below fails
  # cleanly instead of killing the script under set -e.
  __e2e_perform -X POST http://127.0.0.1:8080/api/jobs \
    -H 'Content-Type: application/json' \
    -d "$body"
  response="$HTTP_BODY"
  echo "   $response" >&2
  local id
  # Rescue the jq (|| true): an empty/transient response must fail the CHECK
  # below, not kill the script via set -e.
  id=$(printf '%s' "$response" | jq -r '.id' 2>/dev/null || true)
  [ -n "$id" ] && [ "$id" != "null" ] || { echo ">> could not parse job id from response"; exit 1; }
  printf '%s' "$id"
}

# --- shared users (F-24/F-14) ------------------------------------------------
# The user accounts the F-24 and F-14 tests rely on. They are registered by
# the F-24 test (the first registered user becomes an admin, the second gets
# the default user role); the F-14 test signs in as them against the
# RBAC-enabled API. When the F-14 test runs standalone (without the F-24
# test), it registers them itself if they are not present yet.
E2E_USER_ADA_USERNAME="ada"
E2E_USER_ADA_PASSWORD="s3cret"
E2E_USER_BOB_USERNAME="bob"
E2E_USER_BOB_PASSWORD="pw"

# e2e_ensure_users <api-base>
# Registers the shared users against <api-base> if they are not present yet
# (a duplicate registration is a 409, which is fine).
e2e_ensure_users() {
  local base="$1"
  e2e_http_request POST "$base/api/register" \
    "{\"first_name\":\"Ada\",\"last_name\":\"Lovelace\",\"username\":\"${E2E_USER_ADA_USERNAME}\",\"password\":\"${E2E_USER_ADA_PASSWORD}\"}"
  case "$HTTP_CODE" in
    201|409) ;;
    *) echo ">> register ${E2E_USER_ADA_USERNAME} returned $HTTP_CODE, want 201 or 409 ($HTTP_BODY)"; exit 1 ;;
  esac
  e2e_http_request POST "$base/api/register" \
    "{\"first_name\":\"Bob\",\"last_name\":\"Jones\",\"username\":\"${E2E_USER_BOB_USERNAME}\",\"password\":\"${E2E_USER_BOB_PASSWORD}\"}"
  case "$HTTP_CODE" in
    201|409) ;;
    *) echo ">> register ${E2E_USER_BOB_USERNAME} returned $HTTP_CODE, want 201 or 409 ($HTTP_BODY)"; exit 1 ;;
  esac
}

# --- RBAC-enabled API (F-14) -------------------------------------------------
# e2e_start_rbac_api
# Boots a SECOND API instance with OIDC auth + RBAC enabled, sharing the
# stack's IdP (users + JWKS) and db/scheduler/artifacts. It listens on :8081
# (gRPC :7107) so it does not collide with the main API (:8080/:7105).
e2e_start_rbac_api() {
  e2e_start api-rbac "$WORK/api-rbac.log" 'serving' \
    CDROM_API_HTTP_ADDR=127.0.0.1:8081 CDROM_LISTEN_ADDR=127.0.0.1:7107 \
    CDROM_AUTH_ENABLED=true CDROM_AUTH_ISSUER=https://127.0.0.1:7104 \
    CDROM_AUTH_CLIENT_ID=cdrom-ui CDROM_AUTH_REDIRECT_URL=http://127.0.0.1:8081/api/auth/callback \
    CDROM_AUDIT_FILE="$WORK/audit-rbac.log" CDROM_AUDIT_FORMAT=json \
    CDROM_LOG_FORMAT=text -- "$BIN/api"
}

# e2e_login <api-base> <username> <password>
# Signs in against <api-base> and prints the access token.
e2e_login() {
  local base="$1" username="$2" password="$3"
  e2e_http_request POST "$base/api/login" "{\"username\":\"$username\",\"password\":\"$password\"}"
  [ "$HTTP_CODE" = "200" ] || { echo ">> login ($username, ${base}) returned $HTTP_CODE, want 200 ($HTTP_BODY)"; exit 1; }
  local token
  token=$(printf '%s' "$HTTP_BODY" | jq -r '.access_token')
  [ -n "$token" ] && [ "$token" != "null" ] \
    || { echo ">> login ($username, ${base}) returned no token: $HTTP_BODY"; exit 1; }
  printf '%s' "$token"
}
