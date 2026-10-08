#!/usr/bin/env bash
# Regenerate gRPC/protobuf Go code from the .proto files in proto/.
#
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc (see Makefile `proto`
# target for install hints). Generated code is committed to internal/gen/.
set -euo pipefail

cd "$(dirname "$0")/.."

OUT=internal/gen
mkdir -p "$OUT"

protoc \
  --proto_path=proto \
  --go_out="$OUT" --go_opt=paths=source_relative \
  --go-grpc_out="$OUT" --go-grpc_opt=paths=source_relative \
  proto/cdrom/db/v1/db.proto \
  proto/cdrom/scheduler/v1/scheduler.proto \
  proto/cdrom/artifacts/v1/artifacts.proto \
  proto/cdrom/api/v1/api.proto \
  proto/cdrom/idp/v1/idp.proto

echo "generated Go code in $OUT"
