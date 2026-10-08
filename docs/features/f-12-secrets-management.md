# F-12 · Secrets management


> Phase 3 — Safety & governance · [Feature index](../Features.md)


**What.** Pipelines declare **named secrets** (a name and a value), like their
parameters. The API encrypts each value with a configured key when the pipeline
is created or updated, and decrypts it when it hands a job to an execution
target. A job's spec references a secret with a Go template, `{{
.secrets.name }}` (the same mechanism a step's `condition` and F-10 parameter
interpolation use). The plaintext is never persisted, never returned to the UI,
and is redacted from job logs before they are stored or fanned out.

**Why.** Every real pipeline needs credentials; they must not live in the
pipeline definition (in plaintext) or in logs.

**Scope.**
- `internal/config` — a `secrets` section (store `kind` + a base64 AES-256
  `key`); when no key is set the built-in AES store falls back to an all-zero
  key (test / local-dev convenience).
- `internal/secrets` — a `Store` interface (encrypt/decrypt) with a built-in
  AES-256-GCM store; `vault` / `openbao` are reserved first-class kinds (not
  yet implemented). A `NonceSource` supplies a unique nonce per encryption.
- `internal/models` — a `Secret` (name, encrypted value) on `Pipeline` and
  `PipelineVersion`, denormalized onto each run's `Job` instances; a
  `SecretNonce` counter row.
- `proto/cdrom/db/v1/db.proto` — a `Secret` message; `secrets` on `Pipeline`,
  `CreatePipelineRequest`, `UpdatePipelineRequest`, `PipelineVersion`, and
  `Job`; a `NextSecretNonce` RPC. The database service stores the ciphertext
  opaquely and never sees the key.
- `proto/cdrom/api/v1/api.proto` — `secrets` (a name → **plaintext** map) on
  the API's `Job`, populated by the API at dispatch.
- `internal/api` — encrypt on pipeline create/update; decrypt at dispatch
  (`GetJob`, `StartJobExecution`, the dispatch push); redact secret plaintext
  from streamed job logs before persistence/fan-out. An authenticated user can
  also encrypt a value directly via `POST /api/secrets/encrypt` (body
  `{"value": "…"}` → `{"ciphertext": "…"}`), which returns the ciphertext a
  pipeline's secret declaration stores; the plaintext is never persisted or
  returned.
- `internal/executor` / `internal/target` — carry the decrypted secrets into
  the run context so the spec can interpolate `{{ .secrets.name }}`.

**Acceptance criteria.**
- [x] A secret can be created on a pipeline and referenced by a job's spec;
      the value is available to the step (via `{{ .secrets.name }}`).
- [x] No API/UI path returns the plaintext secret value (the UI sees only
      names; the API decrypts only at dispatch, for the target).
- [x] Secret values never appear in job logs (the API redacts the plaintext
      from every streamed chunk before it is persisted or fanned out).
- [x] Secrets are encrypted at rest (AES-256-GCM with a configured key; the
      nonce is a shared, DB-backed counter so no (key, nonce) pair is ever
      reused across API replicas).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **A single key in configuration; the API is the only encryption authority.**
  The built-in store is AES-256-GCM with a 32-byte key from the `secrets`
  config section (`CDROM_SECRETS_KEY`, base64). When no key is configured the
  built-in store falls back to an all-zero key, so secrets can be exercised in
  test / local-dev scenarios without a real key (the zero key provides no real
  security — set a real key in production). The database service stores the
  ciphertext **opaquely** — it never imports the secrets package or holds the
  key, so no single service or config can decrypt a secret on its own.
- **The nonce is a shared counter in the database.** AES-GCM requires a unique
  (key, nonce) pair per encryption. Because every API replica shares one key,
  they must share one nonce sequence: the built-in store draws each nonce from
  a `SecretNonce` counter row via the database service's `NextSecretNonce` RPC
  (an atomic `UPDATE … RETURNING value + 1`), so no two encryptions — even
  across replicas — ever reuse a nonce. The nonce is embedded in the stored
  ciphertext (`base64(nonce ‖ gcm-ciphertext)`), so decryption needs only the
  key.
- **Secrets are declared on the pipeline, like parameters.** A `Secret` (name,
  encrypted value) is a first-class message on `Pipeline` (and on
  `CreatePipelineRequest` / `UpdatePipelineRequest` / `PipelineVersion`),
  stored as a JSON column. Names must be non-empty and unique within the
  pipeline; the database service validates this at create and update time. The
  API encrypts the plaintext the UI sends before it reaches the database.
- **The ciphertext is denormalized onto each run's job instances.** At run
  creation the database service copies the pipeline's (or the version
  snapshot's) secrets onto each `Job` instance, mirroring the F-10
  `run_params` denormalization. A run against a specific version carries that
  version's secrets, so a re-run of an old run uses the old secrets.
- **The API decrypts at dispatch and hands plaintext to the target.** When the
  API hands a job to an execution target (`GetJob` for agents,
  `StartJobExecution` for workers, and the dispatch push), it decrypts the
  job's stored ciphertexts into a name → plaintext map on the API's `Job`
  (`secrets`), which the target puts on the executor's run context. The
  executor exposes it as `{{ .secrets.name }}` (never top-level, so a secret
  can never collide with a same-named parameter). A ciphertext that fails to
  decrypt (the key changed since it was written) is logged and dropped rather
  than failing the dispatch.
- **The API redacts secrets from job logs.** The API is the only component
  that holds the key, so it is the only place that can both know the plaintext
  (to look for it) and remove it. Before a streamed log chunk is persisted to
  the artifacts store or fanned out to the UI, the API replaces every
  occurrence of a secret's plaintext with a redaction marker, so a secret that
  a step echoes to stdout is never stored or shown.
- **An authenticated user can encrypt a value directly.** `POST
  /api/secrets/encrypt` (body `{"value": "…"}`) runs the value through the
  API's secret store and returns `{"ciphertext": "…"}` — the exact ciphertext a
  pipeline's secret declaration stores. It is a POST (it performs an action and
  draws a nonce), sits behind the same OIDC Bearer-token middleware as every
  other `/api/*` route. It never persists the value and never returns the
  plaintext, so it is safe to call from the UI or a script.
- **New fields / RPCs.** `Secret` message + `NextSecretNonce` RPC (db);
  `secrets` on `Pipeline`, `CreatePipelineRequest`, `UpdatePipelineRequest`,
  `PipelineVersion`, and `Job` (db); `secrets` (name → plaintext map) on the
  API's `Job` (api); `POST /api/secrets/encrypt` (HTTP). The `secrets` config
  section and the `internal/secrets` package are new.
