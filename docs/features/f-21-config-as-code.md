# F-21 · Config as code


> Phase 4 — Delivery & observability · [Feature index](../Features.md)


**What.** Define pipelines (jobs, steps, deps, triggers, params) in a
declarative file format (YAML) that can be imported/exported and checked into
source control. The UI editor and the file format are two views of the same
model.

**Why.** Teams want pipeline definitions in version control, reviewable via PR.

**Scope.**
- A schema for the pipeline definition file (documented in `docs/`).
- `internal/api/server.go` — import (validate + create/update pipeline) and
  export (render current definition).
- Validation shared with F-08 (DAG) and F-10 (params).

**Acceptance criteria.**
- [ ] A pipeline defined in YAML can be imported and becomes runnable.
- [ ] Exporting a pipeline reproduces a file that re-imports equivalently
      (round-trip).
- [ ] Invalid definitions are rejected with actionable errors.
