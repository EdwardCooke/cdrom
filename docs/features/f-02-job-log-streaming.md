# F-02 · Job log streaming


> Phase 1 — Core execution · [Feature index](../Features.md)


**What.** Execution targets stream job output (stdout/stderr per step) back to
the API in near-real-time; the API persists it and fans it out to the UI over
the existing WebSocket event hub. The UI can tail a running job's logs and
replay finished logs.

**Why.** "Where is it stuck?" is the #1 question in CD. Today all logging is
local to the worker/agent and there is no central view.

**Scope.**
- `proto/cdrom/api/v1/api.proto` — a new streaming RPC (e.g.
  `StreamJobLogs` client-stream, or piggyback on `ReportJobStatus` with a log
  payload). Decide the chunking/flow-control model.
- `internal/api` — accept the stream, buffer/persist (via DB service), publish
  a new `job_log` event type on the `EventHub`.
- `internal/models` — a `JobLog`/`JobLogLine` entity (or a blob per job) so
  logs survive past the run.
- `internal/worker`, `internal/agent` — capture step output and stream it.
- `internal/api/ws.go` — new event type; UI consumes it.
- `proto/cdrom/artifacts/v1/artifacts.proto` - new store, retrieve and append to log
  endpoints.
- `internal/artifacts/filesystem/` - filesystem based implementation for storing and
  updating artifacts on the local file system. Currently focused on logs, will focus
  on package storage later.
- `cmd/artifacts/main.go` - wire up the filesystem based artifact implementation.
  Implementation type should be configurable to allow for other builtin implementations
  example: S3 or Azure Blob.
**Acceptance criteria.**
- [x] While a job runs, its stdout/stderr appears in the UI within ~1s.
- [x] Logs are attributed to the correct step.
- [x] After a job finishes, its full log can still be fetched (replay).
- [x] A slow UI client does not block the worker (backpressure/drop policy
      defined, consistent with the existing EventHub drop-and-resync behavior).
- [x] The step logs should be stored via the artifact service and updated as
      the step progresses.
- [x] Ability to retrieve the logs for a particular step and job from the api server.

**Design decisions (folded into AGENTS.md / Architecture.md).**
- **Transport:** a new `StreamJobLogs` client-stream RPC on the API
  (`proto/cdrom/api/v1/api.proto`). The target opens one stream per job and
  sends a `JobLogChunk` per output chunk; the first chunk of a step's output
  carries `JobLogMetadata` (job_id, step_index, stream) and the rest carry
  data. The API verifies the caller's job token once, up front.
- **Storage:** logs are stored in the **artifacts service** (not the DB), as
  files under `<root>/<namespace>/logs/` where the namespace is the job's id —
  one file per step (`step-<n>.log`) plus a combined `job.log`. New artifacts
  RPCs `AppendLog` (client stream, append-or-create), `DownloadLog` (server
  stream), `GetLog`, and `ListLogs` back them. The artifacts service is a
  general-purpose, namespaced file store: its store is an interface
  (`internal/services/artifacts/store.go`) with a filesystem implementation
  (`filesystem.go`); the store kind is configurable (`artifacts_store`,
  `CDROM_ARTIFACTS_STORE`, default `filesystem`) so S3 / Azure Blob can be
  added later without touching the server.
- **Capture:** the executor takes an optional `executor.LogSink`
  (`WriteStepOutput(stepIndex, stream, data)`) via the context. The built-in
  shell handler tees each step's stdout/stderr to the target's local streams
  *and* to the sink when one is present (via `StdoutPipe`/`StderrPipe`); with
  no sink it inherits `os.Stdout`/`os.Stderr` as before, so local logging is
  unchanged.
- **Sink & backpressure:** `internal/logstream.Sink` buffers chunks in a
  bounded channel (1024) drained by a background goroutine to the gRPC
  stream. When the queue is full (the API is slower than the command produces
  output) chunks are **dropped** — the sink never blocks or fails the job.
  This is consistent with the EventHub drop-and-resync behavior: the
  persisted log (written by the API from the chunks it did receive) is the
  source of truth for replay, and a slow UI client resynchronizes from it on
  reconnect.
- **Resilience (outages & redeploys):** both sides ride out a peer going away
  (a pod restart or scale event in Kubernetes). *Target side:* if the API's
  `StreamJobLogs` stream breaks, the sink reopens a fresh stream (the gRPC
  channel reconnects on its own) and resumes; chunks queued while the API was
  down are resent, and if the API is still down when the queue fills they are
  dropped. *API side:* if the artifacts service is unreachable, the API
  retries persisting each chunk (`appendLogWithRetry`) and, if the outage
  outlasts the retry budget, drops the chunk and continues — an artifacts
  outage never tears down a target's log stream. The gRPC connections
  themselves (API↔artifacts, target↔API) are long-lived `*grpc.ClientConn`s
  that reconnect transparently.
- **Fan-out:** the API publishes a `job_log` event (job id, step index,
  stream, data) on the `EventHub` for each chunk, so the UI can tail a
  running job's logs over the existing `/api/ws` WebSocket.
- **Retrieval:** `GET /api/jobs/{id}/logs` lists a job's log files and
  `GET /api/jobs/{id}/logs/{name}` streams one (a step's output or the
  combined `job.log`) — both proxied through the artifacts service.
