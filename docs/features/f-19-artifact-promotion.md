# F-19 · Artifact promotion


> Phase 4 — Delivery & observability · [Feature index](../Features.md)
>
> ⚠️ **SUPERSEDED — ABSORBED INTO [F-21.6](f-21.6-artifact-package-references.md).**
> Packaging (getting a build product to the jobs that consume it) and
> promotion (moving it between environments/runs) are two halves of one
> capability, so F-19 is retired as a standalone feature. Its scope — the
> checksummed, streamed copy between namespaces, `POST /api/artifacts/promote`,
> byte-identity verified, authorized (F-14) and audited (F-15) — is carried by
> F-21.6's "Promotion (former F-19)" subsection, and its acceptance criteria
> live in F-21.6's checklist. Kept here for traceability only; do not schedule
> or implement it separately.

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
