# F-18 · Notifications


> Phase 4 — Delivery & observability · [Feature index](../Features.md)


**What.** Two built-in **step handlers** that emit a notification from the
**API server** while a job runs: **`email`** (an SMTP message) and
**`webhook`** (an HTTP request to an arbitrary URL). A step of either type is
declared in a job's spec like any other step — its `type` selects the handler
and its settings live in the step's `params`. The step's user-facing fields
(the email's `to`/`cc`/`bcc`/`subject`/`body`, the webhook's `url`/`headers`/
`body`) are Go templates rendered against the step's **condition context** —
the same data a step's `condition` (F-06) and the run's spec fields (F-10)
use: the step's env, prior steps' status/outputs, upstream jobs, the job's
identity, the run's parameters, and the pipeline's named secrets. When a
field's content is **markdown** (an email or webhook body whose `content_type`
is `markdown`), it is rendered to **HTML** before transmission.
The execution target only renders the templates and hands the result to the
API; the **API performs the actual send** (SMTP for email, HTTP for webhook)
using its own configuration — the email server (SMTP) settings and the webhook
response timeout live on the API, not on the target. A notification step may
set the standard `once` step field (F-01) so a job that fans out to a worker
group (one execution per worker) sends the notification at most once per job
attempt instead of once per worker.

**Why.** Teams live in email and chat/issue trackers, not the CD UI. A
pipeline should be able to notify a person (email) or an external system
(webhook) at any point in a job — "the deploy failed, here's the run link" —
without the target needing SMTP credentials or outbound egress.

**Scope.**
- `internal/executor` — a `Notifier` interface (a step handler's seam to send
  a notification through the API) + context plumbing
  (`ContextWithNotifier` / `NotifierFromContext`), mirroring `TokenExchange`
  and `Approval`.
- `internal/notify` (new package) — the API-backed `Notifier` implementation:
  it calls the API's `SendEmail` / `SendWebhook` gRPC RPCs, presenting the
  job's own token (like `internal/approval` and `internal/tokenexchange`).
  Shared by the worker and the agent.
- `internal/stephandlers` — two new built-in handlers registered at package
  init: `email` (`email.go`) and `webhook` (`webhook.go`). Each renders its
  params against the step's condition context and calls the `Notifier`.
- `internal/target` — wire the `Notifier` into the executor context in
  `RunJob` (worker + agent).
- `proto/cdrom/api/v1/api.proto` — `SendEmail` / `SendWebhook` RPCs (called by
  the target, job-token-authenticated like `ExchangeJobToken` /
  `CheckApproval`).
- A notification step uses the standard `once` step field (F-01) to send at
  most once per job attempt: the executor runs the step on a single worker of
  a fan-out group, so the job does not notify once per worker.
- `internal/api` — implement the RPCs: build and send the SMTP message (email)
  and perform the HTTP request (webhook); render markdown→HTML for a `markdown`
  email or webhook body; enforce the webhook response timeout.
- `internal/config` — an `EmailConfig` (SMTP host/port/auth/from/STARTTLS) and
  the webhook timeout settings (`webhook_timeout`, default 10s;
  `webhook_timeout_max`, the cap a step's `timeout` can reach).
- `cmd/api` — construct the SMTP sender, the markdown renderer, and the
  webhook HTTP client from config and hand them to the API server.
- A markdown→HTML renderer is added as a dependency (e.g.
  `github.com/gomarkdown/markdown` or `github.com/russross/blackfriday/v2`);
  the API is the only component that uses it.

**Acceptance criteria.**
- [x] An `email` step sends an SMTP message from the API: `to`/`cc`/`bcc`/
      `subject`/`body` are templated against the condition context (a body can
      reference a secret, a prior step's output, a run parameter, or the job's
      identity); the body's `content_type` selects `plain` (sent as text),
      `html` (sent as-is), or `markdown` (rendered to HTML, the default).
- [x] A `webhook` step performs an HTTP request from the API to the templated
      `url`, with any number of custom headers (names and values templated,
      not through markdown); the body's `content_type` selects `plain` (sent
      as-is, e.g. JSON/YAML, the default), `html` (sent as-is), or `markdown`
      (rendered to HTML).
- [x] The email is sent using the API's configured SMTP server (`from`
      defaults to the API's configured sender); the webhook uses the API's
      configured response timeout.
- [x] A webhook response timeout defaults to 10 seconds and is configurable at
      the API, with a configurable maximum that caps a step's per-request
      `timeout`.
- [x] A notification that fails (SMTP error, webhook non-2xx, or timeout)
      fails the step (and thus the job), unless the step sets `ignore_failed`
      (F-06).
- [x] A notification step with the standard `once` field (F-01) set sends at
      most once per job attempt: in a fan-out job (one execution per worker)
      only the first worker to reach the step runs it (and sends), the others
      skip it, and a retry (a new attempt) may send again.
- [x] The target never opens an SMTP or outbound HTTP connection itself: it
      only renders templates and calls the API.
- [x] `GOOS=windows GOARCH=amd64 go build ./cmd/...` stays green.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **Notifications are step handlers, sent by the API.** `email` and `webhook`
  are built-in step types (registered in `internal/stephandlers` under `email`
  and `webhook`), so a notification is just another step in a job's spec — it
  can be conditional (F-06), ordered among other steps, and targeted like any
  job. But unlike the `shell` handler (which runs on the target), the
  *transmission* happens on the **API server**: the target renders the step's
  templates (it owns the condition context) and calls the API's `SendEmail` /
  `SendWebhook` gRPC RPC, presenting the job's token; the API does the SMTP
  send or the HTTP request. This keeps SMTP credentials and outbound egress on
  the control plane, and a notification behaves identically whether the job
  ran on a worker or an ephemeral agent.
- **Templates render against the condition context.** Every user-facing field
  of both handlers — the email's `to`/`cc`/`bcc`/`subject`/`body` and the
  webhook's `url`/`headers`/`body` — is a Go template rendered against the
  step's condition context (the same `executor.ConditionContext` a step's
  `condition` (F-06) and the approval message (F-13) use: the step's env,
  prior steps' status/outputs, upstream jobs' status/outputs, the job's
  identity, the run's parameters, and the pipeline's named secrets as
  plaintext). The handler renders each field with `executor.RenderTemplate`
  (the same `missingkey=error` semantics as spec interpolation, F-10), so a
  reference to an undefined key fails the step. Rendering happens on the
  target (where the condition context is built); the rendered values are what
  the API receives.
- **Rendered lazily, like the approval message.** These content fields are not
  rendered during the run-level spec interpolation (F-10) — which only has the
  run's parameters and identity — but are rendered lazily by the step handler
  against the step's full condition context (F-06), which additionally has
  prior steps' status/outputs and upstream jobs. This mirrors how the approval
  handler's `message` (F-13) is handled: `interpolateStep` leaves these fields
  unrendered, and the handler renders them against
  `executor.ConditionContextFromContext`.
- **Content types: `plain` / `html` / `markdown`.** Both handlers take a
  `content_type` (default `markdown` for email, `plain` for webhook) that selects how the `body` is sent:
  `plain` sends the body as-is (a `text/plain` email, or a raw webhook body —
  JSON, YAML, or anything the end user wants); `html` sends the body as-is
  (a `text/html` email, or an HTML webhook body — the body is already HTML);
  `markdown` renders the body from markdown to HTML and sends it as HTML. The
  markdown→HTML conversion is done by the API (the only component that holds
  the renderer), so the target sends the rendered body and the API produces
  the final content.
- **Email options.** The `email` handler reads from the step's `params`: `to`
  (a list of addresses, or a single address; required), `cc` and `bcc`
  (optional lists), `subject` (string, optional), `body` (string, the
  message body), `content_type` (`plain` / `html` / `markdown`, default
  `markdown`), and optional `from` / `reply_to` overrides. The step may also
  set the standard `once` field (F-01) to send at most once per job attempt.
  The SMTP server itself (host, port, auth, the
  default `from`, STARTTLS) is configured on the **API** (`EmailConfig`), not
  per step — a step names its recipients and content, the API supplies the
  sender and the server. A step's `from` / `reply_to`, when set, override the
  API's configured sender for that message.
- **Webhook options.** The `webhook` handler reads from the step's `params`:
  `url` (string, required), `method` (string, default `POST`), `headers` (a
  list of `Name: Value` entries — any number of headers, each a template so
  both the name and the value can reference the condition context; each entry
  is rendered and then split on the first `:` into name and value), `body`
  (string), `content_type` (`plain` / `html` / `markdown`, default
  `plain`), and an optional `timeout` (a Go duration the step may request for
  the response). The step may also set the standard `once` field (F-01) to
  send at most once per job attempt. The API performs the
  request with the timeout: a step's `timeout` is honored but clamped to the
  API's `webhook_timeout_max`; when the step sets none, the API's
  `webhook_timeout` (default 10s) applies.
- **API configuration.** The API is configured (via `internal/config`, env
  vars, or the config file) with: an `EmailConfig` (SMTP `host`, `port`
  default 587, optional `username` / `password`, the default `from` address,
  `starttls` default on, `insecure` for self-signed servers) and the webhook
  timeout pair (`webhook_timeout`, default 10s, and `webhook_timeout_max`, the
  cap a step's `timeout` can reach). When no email server is configured, an
  `email` step fails with a clear error; the webhook timeout is always
  available (it defaults to 10s).
- **Failure semantics.** The step succeeds when the API confirms the
  notification went out (the SMTP server accepted the message, or the webhook
  returned a 2xx within the timeout). It fails otherwise — an SMTP error, a
  webhook connection error, a non-2xx response, or a response timeout — and a
  failed step stops the job unless it sets `ignore_failed` (F-06). The API
  logs the outcome (and, for a webhook, the response status) to its local log;
  the step's streamed output (F-02) records a short "sent email to …" /
  "webhook … → 200" line so it is visible in the job log.
- **`once` — at most one notification per job attempt.** A notification step
  uses the standard `once` step field (F-01) to send at most once per job
  attempt. A job that targets a worker group runs on every worker, so without
  it a person would be emailed once per worker; with `once` set, the executor
  runs the step on a single worker of the group (the first to claim the
  durable slot, job id + step name + attempt) and the other workers skip it,
  so the notification goes out exactly once. A retry (F-04) is a new attempt,
  so it may send again. When `once` is not set (the default), every worker
  that runs the step sends its own notification.
- **Job-token auth.** Like `ExchangeJobToken` and `CheckApproval`,
  `SendEmail` / `SendWebhook` are on the API's gRPC surface and, when
  job-token auth is enabled, require the target to present a job token scoped
  to the job; the API rejects a token for a job that is no longer in a live
  state.
