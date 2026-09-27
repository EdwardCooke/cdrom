# Cdrom

Cdrom — continuous delivery runtime, orchestrator and manager. A pipeline
tool: defines, schedules, and executes pipeline jobs across distributed
execution targets.

See [AGENTS.md](AGENTS.md) for the full architectural context.

## Repository Layout

```
cmd/
  api/        API/controller service entrypoint (HTTP for UI + gRPC for workers/agents/scheduler)
  db/         database service entrypoint
  scheduler/  scheduler service entrypoint
  artifacts/  artifacts service entrypoint
  worker/     long-lived worker entrypoint
  agent/      ephemeral Kubernetes agent entrypoint
internal/
  api/        API/controller layer: HTTP routing + gRPC control-plane server
  config/     shared configuration loading
  logging/    centralized logging (stdout or CDROM_LOG_FILE)
  services/
    data/     pipeline/job data service
    artifacts/ job artifact storage & retrieval
    scheduler/ job lifecycle + dispatch
    database/ storage backend abstraction (SQLite / PostgreSQL)
  worker/     long-lived worker implementation
  agent/      ephemeral agent implementation
ui/           React frontend
  tests/      UI tests
docs/         documentation
scripts/      build/release scripts
```

## Quick Start

```sh
make build   # build all binaries for the host platform into ./bin
make test    # run Go tests
make vet     # run go vet
```

## Binaries

| Binary | Purpose | Platforms |
|--------|---------|-----------|
| `api`    | API/controller serving the UI | host |
| `worker` | long-lived, group-targetable worker | Windows, Linux |
| `agent`  | ephemeral, one-off job execution in Kubernetes | Windows, Linux |
