#!/usr/bin/env bash
#
# update-known-hosts.sh — refresh the built-in SSH host keys for common public
# git providers (F-21.2.4.1).
#
# Scans the configured hosts with ssh-keyscan and rewrites
# internal/services/pipelinesource/common_known_hosts — the file embedded in
# the source-fetcher binary for strict git-ssh host-key verification — with
# the providers' current keys.
#
# Usage:
#   scripts/update-known-hosts.sh [target-file]
#
# Environment:
#   CDROM_KNOWN_HOSTS_HOSTS    host list, space- or comma-separated
#                              (default: github.com gitlab.com bitbucket.org
#                               ssh.dev.azure.com)
#   CDROM_KNOWN_HOSTS_TIMEOUT  per-host scan timeout in seconds (default: 25)
#
# Requires: ssh-keyscan, ssh-keygen, timeout, bash.
#
# The script refuses to write the file if any host yields no keys (e.g. the
# network is blocked), so a partial scan can never silently drop a provider's
# keys.

set -euo pipefail

HOSTS="${CDROM_KNOWN_HOSTS_HOSTS:-github.com gitlab.com bitbucket.org ssh.dev.azure.com}"
HOSTS="${HOSTS//,/ }"
TIMEOUT_SECS="${CDROM_KNOWN_HOSTS_TIMEOUT:-25}"

# Resolve the default target relative to the repo root so the script works
# from anywhere.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${1:-$ROOT/internal/services/pipelinesource/common_known_hosts}"

for tool in ssh-keyscan ssh-keygen timeout; do
  command -v "$tool" >/dev/null || { echo "error: $tool not found" >&2; exit 1; }
done

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

# Scan each host and collect its keys (banner/comment lines dropped, deduped,
# sorted for stable output).
for host in $HOSTS; do
  echo "scanning $host ..." >&2
  keys="$(timeout "$TIMEOUT_SECS" ssh-keyscan -t ssh-rsa,ecdsa,ed25519 -T "$TIMEOUT_SECS" "$host" 2>/dev/null | grep -v '^#' | sort -u || true)"
  if [[ -z "$keys" ]]; then
    echo "error: no keys returned for $host (network blocked or host unreachable); not updating $TARGET" >&2
    exit 1
  fi
  printf '%s\n' "$keys" >> "$TMP"
done

# Build the new file: header + keys.
{
  echo "# Built-in SSH host keys for common public git providers (F-21.2.4.1)."
  echo "# Embedded in the source-fetcher binary so these hosts get strict host-key"
  echo "# verification with no configuration. Refreshed via"
  echo "# scripts/update-known-hosts.sh on $(date -u +%Y-%m-%d). Providers rotate"
  echo "# keys occasionally; if a fetch of one of these hosts fails host-key"
  echo "# verification, re-run that script."
  echo ""
  cat "$TMP"
} > "${TARGET}.new"

# Verify the new file parses and every host is present before replacing.
for host in $HOSTS; do
  ssh-keygen -F "$host" -f "${TARGET}.new" >/dev/null
done

# Show what changed, if anything.
if [[ -f "$TARGET" ]] && ! diff -q "$TARGET" "${TARGET}.new" >/dev/null 2>&1; then
  echo "--- changes to $TARGET ---" >&2
  diff -u "$TARGET" "${TARGET}.new" >&2 || true
fi

mv "${TARGET}.new" "$TARGET"
echo "updated $TARGET:" >&2
for host in $HOSTS; do
  n="$(ssh-keygen -F "$host" -f "$TARGET" | grep -cv '^#')"
  echo "  $host: $n key(s)" >&2
done
