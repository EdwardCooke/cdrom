# F-09 · Triggers


> Phase 2 — Pipeline orchestration · [Feature index](../Features.md)


**What.** Ways to start a pipeline run other than a manual click:
- **Schedule (cron)** — run on a cron expression.
- **Webhook** — an external POST (e.g. from a git host) starts a run, passing
  payload (commit, branch) as run parameters. A webhook trigger can also
  authenticate the caller by **OIDC claims**: the caller presents a Bearer
  token issued by a configured issuer, and the trigger matches only when the
  token's claims satisfy the trigger's required claims (with `*` wildcards and
  dot-addressed nested claims). The matched token's claims are recorded on the
  run and stamped onto the run's job tokens as `upstream_*` claims.
- **Event** — start when another pipeline/run reaches a state (chaining).

**Why.** CD is driven by events (a push, a timer, an upstream deploy), not
just by humans.

**Scope.**
- `internal/models` / proto — trigger definitions on the pipeline (cron spec,
  optional webhook secret, optional webhook OIDC issuer + required claims,
  event rule); trigger source + name + source run on the run; the run's
  `upstream_claims` (the full claims of a webhook caller's token, as a JSON
  object — a claim that is itself an object or a list keeps its structure);
  `trigger_name` / `trigger_type` / `upstream_claims` on the job;
  `pipeline_name` on the run (so the event loop matches a finished run
  against a watched pipeline without fetching every pipeline).
- `internal/services/database` — trigger validation/persistence; the atomic
  `TriggerRun` claim (dedup); `ListPipelines` filtering by trigger type.
- `internal/services/scheduler` — a cron trigger loop; an event trigger loop;
  the `TriggerRun` RPC that drives a claimed run's instances.
- `internal/api/server.go` — `POST /api/pipelines/{id}/webhook` (auth via the
  trigger's shared secret and/or its OIDC claims; open when neither is set).
- `internal/api/webhookoidc.go` — the webhook token verifier (per-issuer OIDC
  discovery + verification), claim flattening (nested claims become
  dot-addressed keys, used only for matching a trigger's `oidc_claims`), and
  the wildcard-aware claim matcher.
- `internal/api/jobsauth.go` — mints/exchanges job tokens stamped with the
  job's `trigger_name` / `trigger_type` and `upstream_*` claims.
- `internal/idp` — the `MintJobToken` gRPC RPC stamps `trigger_name`,
  `trigger_type`, and `upstream_*` claims onto the token it mints.

**Acceptance criteria.**
- [x] A cron-triggered pipeline runs at the scheduled times.
- [x] A webhook POST with a valid secret starts a run and records the payload
      as run parameters.
- [x] A webhook trigger with an empty secret is open: any POST starts a run.
- [x] A webhook trigger with an `oidc_issuer` requires a Bearer token from
      that issuer whose claims satisfy the trigger's `oidc_claims` (a `*`
      value is a wildcard; nested claims are matched dot-addressed); a token
      that fails verification or mismatches a claim is rejected (401).
- [x] The matched webhook token's claims are recorded on the run
      (`upstream_claims`, as a JSON object that preserves a claim's structure
      — an object or a list stays an object/list) and stamped onto the run's
      job tokens as `upstream_*` claims.
- [x] Job tokens carry the run's `trigger_name` and `trigger_type` claims.
- [x] An event trigger starts a run when the watched condition is met.
- [x] Trigger source is recorded on the run (manual / cron / webhook / event).

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **Triggers live on the pipeline.** A `Pipeline` carries a `Triggers` list
  (a JSON column via GORM's `serializer:json`), each a `Trigger` with a
  `name` (unique within the pipeline), a `type` (`cron` / `webhook` /
  `event`), and the fields its type needs: a cron trigger a standard
  five-field `cron` expression; a webhook trigger an optional `secret` and an
  optional `oidc_issuer` + `oidc_claims` (see Webhook OIDC below); an event
  trigger an `event_pipeline` (the name of the pipeline to watch) and an
  `event_status` (the run status that fires it). Any trigger may carry
  static `params` merged into the run it starts. Triggers are validated at
  save time (unique non-empty names; a cron expression that parses; an event
  pipeline + status; a webhook's `oidc_claims` entries have non-empty claim
  names), so a pipeline is never persisted with an unrunnable trigger. A
  webhook trigger's secret is optional: when set it is enforced on the
  webhook endpoint, when empty the trigger is open (any POST starts a run).
- **Webhook OIDC claims.** A webhook trigger may set an `oidc_issuer` (the
  issuer URL of the token a caller must present) and `oidc_claims` (a map of
  claim name → required value). On the webhook endpoint the API verifies the
  caller's Bearer token against the issuer (OIDC discovery + JWKS, cached per
  issuer) and matches the token's claims against the trigger's required
  claims: a required value of `*` is a wildcard (it matches any value, or the
  claim being absent), any other value must be present and equal. Nested
  claims (some providers, e.g. GitLab, nest their claim values) are flattened
  to dot-addressed keys (`a.b.c`) **for matching only**, so a trigger can
  require a nested claim by its dot path. When both a `secret` and an
  `oidc_issuer` are set, **both** must be satisfied (AND); a trigger with
  neither is open. The matched token's claims are recorded on the run as
  `upstream_claims` — as a JSON object that preserves each claim's structure
  (a claim that is itself an object or a list, e.g. GitLab's `user_identities`
  or `job_config`, is kept as that object/list, not flattened to a string) —
  and denormalized onto each of the run's job instances, so downstream
  applications can make allow/deny decisions from the identity that triggered
  the run.
- **`TriggerRun` is the single, atomic claim path.** All three trigger kinds
  funnel through `Database.TriggerRun` (and the scheduler's `TriggerRun` RPC
  that wraps it): it creates a `PipelineRun` (started by the named trigger)
  plus one job instance per job definition — reusing `CreateRun`'s
  `createRunAndInstances` — but only if the trigger has not already started a
  run for the same fire window. The check and the create run in one
  transaction that locks the pipeline row, so a racing replica is serialized:
  the first claim creates the run, the rest see it and no-op. The run records
  its `trigger` source (`cron` / `webhook` / `event`), its `trigger_name`,
  and, for an event trigger, its `source_run_id`. For a webhook trigger it
  also records the caller's token claims as the run's `upstream_claims` (a
  JSON object that preserves each claim's structure), which are denormalized
  onto each of the run's job instances (so a job token can carry them, see
  Job-token trigger claims below).
- **Dedup keeps a trigger from firing twice.** A cron trigger passes a dedup
  window (shorter than the minimum cron period): a run the same trigger
  started within the window suppresses a duplicate, so a racing replica cannot
  fire the same scheduled time twice while the next legitimate fire (a full
  period later) is not suppressed. An event trigger records the source run's
  id: the same source run can never start the same downstream run twice.
- **Cron loop.** A leader-gated background loop
  (`internal/services/scheduler/cron.go`, started from `cmd/scheduler`) ticks
  once a second, keeps the parsed schedule and next fire time per
  (pipeline, trigger), and — when a trigger is due — calls `TriggerRun` to
  claim the run and advances to the next fire time. On a restart it recomputes
  the next fire as `schedule.Next(now)`, so a time that already passed is not
  re-fired; the database's dedup is the authoritative guard against the
  racing-replica case. It lists only the pipelines that carry a cron trigger
  (`ListPipelines` with `trigger_type` set to `cron`), so a pipeline with no
  cron trigger is never fetched or considered.
- **Event loop.** A leader-gated background loop
  (`internal/services/scheduler/eventtrigger.go`, started from
  `cmd/scheduler`) ticks every few seconds, lists only the pipelines that
  carry an event trigger (`ListPipelines` with `trigger_type` set to `event`)
  to build its rules, and — for each recently-finished run (a lookback
  window) whose pipeline name and status match an event trigger — calls
  `TriggerRun` to start a run of the triggered pipeline, recording the source
  run's id and the source pipeline's name in the run's params. The run's
  `pipeline_name` (preloaded with the run) is what the loop matches against,
  so a watched pipeline that carries no trigger of its own still matches. An
  in-memory set of already-fired source run ids is an optimization; the
  database's dedup (by source run id) makes a re-fire after a restart a
  no-op.
- **Webhook endpoint.** `POST /api/pipelines/{id}/webhook` (API) looks up the
  pipeline's webhook triggers and, for each, checks every credential the
  trigger sets: the presented secret (constant-time, via the
  `X-Cdrom-Webhook-Secret` header) and, when the trigger has an
  `oidc_issuer`, the caller's Bearer token (verified against the issuer and
  matched against the trigger's `oidc_claims`, see Webhook OIDC claims above).
  A trigger matches when **all** of its set credentials are satisfied; a
  trigger with neither a secret nor an `oidc_issuer` is open and matches any
  request. On a match the handler decodes the request's JSON body (a flat
  object of string values; an empty body is allowed) as run parameters, merged
  over the trigger's static params (the body wins on a collision), and calls
  the scheduler's `TriggerRun` (which atomically dedups it), passing the
  matched token's claims (as a JSON object preserving each claim's structure)
  as the run's `upstream_claims`. A request that matches no trigger is 401; a
  pipeline with no webhook trigger is 404.
- **Job-token trigger claims.** The job tokens the API mints for a run's jobs
  carry the run's trigger context so a downstream application (a step handler,
  an external service the job calls) can make allow/deny decisions: a
  `trigger_name` claim (the trigger's name) and a `trigger_type` claim
  (`cron` / `webhook` / `event`), plus — for a webhook-triggered run — the
  run's `upstream_claims` stamped as `upstream_<claim>` (e.g.
  `upstream_org`, `upstream_user`); a claim that is itself an object or a
  list (e.g. GitLab's `user_identities` or `job_config`) is stamped as that
  object/list, not flattened to a string. The API reads these off the job
  (they are denormalized onto the job instance at run creation) and passes
  them to the IdP's `MintJobToken` gRPC RPC, which stamps them onto the RS256
  token it signs. `ExchangeJobToken` re-stamps the same trigger context onto the
  exchanged token.
- **New RPCs / fields.** `Database.TriggerRun` / `Scheduler.TriggerRun`;
  `Trigger` message + `TriggerType` enum (db); `triggers` on `Pipeline`,
  `CreatePipelineRequest`, and `UpdatePipelineRequest` (db); `trigger_type`
  on `ListPipelinesRequest` (db, so the cron and event loops fetch only the
  pipelines that carry a trigger of that type); `trigger_name`,
  `source_run_id`, `pipeline_name`, and `upstream_claims` (a
  `google.protobuf.Struct` — a JSON object, so a claim that is an object or a
  list keeps its structure) on `PipelineRun` (db, scheduler);
  `upstream_claims` (a `google.protobuf.Struct`) on `TriggerRunRequest` (db,
  scheduler); `oidc_issuer` and `oidc_claims` (a `map<string, string>` of
  dot-addressed claim name → expected value, used for matching) on `Trigger`
  (db); `trigger_name`, `trigger_type`, and `upstream_claims` (a
  `google.protobuf.Struct`) on `Job` (db, api); and `finished_after` on
  `ListRunsRequest` (db, so the event loop can scan recently-finished runs).
  `POST /api/pipelines/{id}/webhook` (API).
