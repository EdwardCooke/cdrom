# F-19 · Artifact promotion


> Phase 4 — Delivery & observability · [Feature index](../Features.md)


**What.** Promote an artifact produced in one run/environment to another
environment (e.g. the build artifact from staging is what prod deploys), so
environments deploy the *same* artifact rather than rebuilding.

**Why.** "Build once, deploy everywhere" is a core CD guarantee.

**Scope.**
- `proto/cdrom/artifacts/v1/artifacts.proto` — a promote/copy operation
  (source namespace → target namespace/environment).
- `internal/services/artifacts` — implement the copy (streamed).
- `internal/api/server.go` — `POST /api/artifacts/promote`.
- Ties to F-17 (environments) and F-07 (runs).

**Acceptance criteria.**
- [ ] An artifact from run A can be promoted to run B / another environment.
- [ ] The promoted artifact is byte-identical (checksum verified).
- [ ] Promotion is authorized (F-14) and audited (F-15).
