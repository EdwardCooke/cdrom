#!/usr/bin/env bash
#
# gencerts.sh — generate a local development mTLS certificate set for cdrom.
#
# Produces, under the output directory (default ./certs):
#
#   ca.crt / ca.key   the certificate authority
#   cert.crt / cert.key  a single certificate (CN=cdrom, SANs for
#                        localhost and 127.0.0.1) carrying BOTH serverAuth
#                        and clientAuth extended key usages
#
# Because the one certificate is valid for both roles, every cdrom process
# — service servers and clients alike — presents the same cert and verifies
# peers against the same CA. That means a single config file works for every
# binary:
#
#   tls:
#     ca_file:   certs/ca.crt
#     cert_file: certs/cert.crt
#     key_file:  certs/cert.key
#
# Usage: scripts/gencerts.sh [output-dir]
#
# Requires: openssl, bash.

set -euo pipefail

OUT="${1:-certs}"
DAYS="${CDROM_CERT_DAYS:-365}"
SUBJ="/C=US/O=cdrom/CN=cdrom"

# Resolve to an absolute path so the final listing works after we cd in.
case "$OUT" in
  /*) ;;
  *) OUT="$(pwd)/$OUT" ;;
esac

mkdir -p "$OUT"
cd "$OUT"

echo ">> generating CA"
openssl genrsa -out ca.key 2048 >/dev/null 2>&1
openssl req -x509 -new -nodes -key ca.key -sha256 -days "$DAYS" \
  -subj "$SUBJ" -out ca.crt

echo ">> generating certificate (server + client)"
openssl genrsa -out cert.key 2048 >/dev/null 2>&1
openssl req -new -key cert.key -subj "$SUBJ" -out cert.csr
cat > cert.ext <<'EOF'
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = DNS:localhost, IP:127.0.0.1, IP:::1
EOF
openssl x509 -req -in cert.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days "$DAYS" -sha256 -extfile cert.ext -out cert.crt >/dev/null 2>&1

# Clean up intermediates; keep only the .crt/.key files the config references.
rm -f cert.csr cert.ext ca.srl

echo ">> done. Certificates written to $OUT:"
ls -1
echo
echo "Point your config file (or CDROM_TLS_* env vars) at:"
echo "  tls:"
echo "    ca_file:   $OUT/ca.crt"
echo "    cert_file: $OUT/cert.crt"
echo "    key_file:  $OUT/cert.key"
