# Cdrom build automation.
#
# Cross-compiles worker and agent for Windows + Linux (cross-platform
# requirement). The API server and the gRPC service servers (db, scheduler,
# artifacts, logs) are control-plane binaries and are built for the host
# platform only.

# Control-plane binaries (host platform only).
CONTROL_BINS := api db scheduler artifacts idp
# Execution-layer binaries (shipped for Windows + Linux).
EXEC_BINS    := worker agent

GOOS_LINUX := linux
GOOS_WIN   := windows

.PHONY: all build certs proto test vet fmt clean

all: build

## build: build all binaries for the host platform into ./bin
build: $(addprefix build-,$(CONTROL_BINS) $(EXEC_BINS))

## certs: generate a local development mTLS certificate set into ./certs
##        (CA, server, and client certs). Point your config file's tls
##        section (or CDROM_TLS_* env vars) at the generated files.
certs:
	./scripts/gencerts.sh certs

## e2e: build and run the end-to-end smoke test against a local stack
e2e: build
	./scripts/e2e.sh

## proto: regenerate gRPC/protobuf Go code from proto/ (requires protoc,
##        protoc-gen-go, protoc-gen-go-grpc on PATH)
proto:
	./scripts/genproto.sh

## test: run all Go tests
test:
	go test ./...

## vet: run go vet
vet:
	go vet ./...

## fmt: format all Go code
fmt:
	gofmt -w -l .

## clean: remove build output
clean:
	rm -rf bin

# Control-plane binaries.
build-api:
	go build -o bin/api ./cmd/api

build-db:
	go build -o bin/db ./cmd/db

build-scheduler:
	go build -o bin/scheduler ./cmd/scheduler

build-artifacts:
	go build -o bin/artifacts ./cmd/artifacts

build-idp:
	go build -o bin/idp ./cmd/idp

# Execution-layer binaries.
build-worker:
	go build -o bin/worker ./cmd/worker

build-agent:
	go build -o bin/agent ./cmd/agent
