// Package api implements the API/controller layer: an HTTP surface consumed
// by the UI that routes requests to the gRPC services. No business logic
// belongs here — it is a thin bridge between the UI and the service layer.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// Clients are the gRPC clients the API bridges to.
type Clients struct {
	Database  dbpb.DatabaseClient
	Scheduler schedpb.SchedulerClient
	Artifacts artifactspb.ArtifactsClient
}

// Server is the HTTP API server.
type Server struct {
	clients Clients
	hub     *EventHub
	// webhookOIDC verifies the Bearer token a webhook caller presents against
	// a trigger's oidc_issuer and returns its flattened claims. It is the
	// production verifier by default; tests substitute a fake so the
	// claim-matching path can be exercised without a live identity provider.
	webhookOIDC webhookTokenVerifier
}

// New creates an API server over the given gRPC clients. hub is the event
// hub that backs the /api/ws WebSocket endpoint; it may be nil to disable
// the WebSocket endpoint.
func New(clients Clients, hub *EventHub) *Server {
	return &Server{clients: clients, hub: hub, webhookOIDC: newWebhookOIDCVerifier()}
}

// Handler builds the HTTP handler for the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Pipelines (via the database service).
	mux.HandleFunc("POST /api/pipelines", s.createPipeline)
	mux.HandleFunc("GET /api/pipelines", s.listPipelines)
	mux.HandleFunc("PUT /api/pipelines/{id}", s.updatePipeline)

	// Pipeline runs (F-07): triggering a pipeline creates a run and its job
	// instances (via the scheduler); runs are listed and fetched via the
	// database service.
	mux.HandleFunc("POST /api/pipelines/{id}/runs", s.createRun)
	mux.HandleFunc("GET /api/pipelines/{id}/runs", s.listRuns)
	mux.HandleFunc("GET /api/runs/{id}", s.getRun)

	// Webhook trigger (F-09): an external POST (authenticated with the
	// pipeline's webhook trigger secret) starts a run, passing the request's
	// JSON body as run parameters.
	mux.HandleFunc("POST /api/pipelines/{id}/webhook", s.webhook)

	// Jobs (via the scheduler service).
	mux.HandleFunc("POST /api/jobs", s.submitJob)
	mux.HandleFunc("GET /api/jobs", s.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /api/jobs/{id}/cancel", s.cancelJob)
	mux.HandleFunc("POST /api/jobs/{id}/rerun", s.rerunJob)

	// Workers (via the scheduler service).
	mux.HandleFunc("GET /api/workers", s.listWorkers)

	// Artifacts (via the artifacts service).
	mux.HandleFunc("GET /api/jobs/{id}/artifacts", s.listJobArtifacts)

	// Job logs (via the artifacts service). A job's logs are stored as one
	// file per step (step-<n>.log) plus a combined job.log.
	mux.HandleFunc("GET /api/jobs/{id}/logs", s.listJobLogs)
	mux.HandleFunc("GET /api/jobs/{id}/logs/{name}", s.getJobLog)

	// Live event stream (WebSocket).
	if s.hub != nil {
		mux.HandleFunc("GET /api/ws", s.handleWebSocket)
	}

	return mux
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

type pipelineRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	// Jobs are the pipeline's job definitions (F-08): each carries a key (the
	// job's stable name), needs (dependencies, expressed as the keys of other
	// jobs in the pipeline), a target group, and an execution spec. When
	// present the pipeline's DAG is validated (unique keys, known needs, no
	// cycle) before the pipeline is saved.
	Jobs []pipelineJobRequest `json:"jobs,omitempty"`
	// Triggers are the ways a run of the pipeline can be started other than a
	// manual click (F-09): a cron schedule, a webhook, or an event (another
	// pipeline's run reaching a state). When present they are validated
	// (unique names, kind-specific fields present) before the pipeline is
	// saved.
	Triggers []triggerRequest `json:"triggers,omitempty"`
	// Params are the pipeline's parameter declarations (F-10): the named,
	// typed inputs a run can supply. When present they are validated (unique,
	// non-empty names) before the pipeline is saved.
	Params []parameterRequest `json:"params,omitempty"`
}

// parameterRequest is the JSON form of a single pipeline parameter (F-10): a
// name, an optional default value, and an optional description. A run
// supplies concrete values for the pipeline's parameters; a parameter not
// supplied falls back to its default.
type parameterRequest struct {
	Name        string `json:"name"`
	Default     string `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// triggerRequest is the JSON form of a single pipeline trigger (F-09). Type
// selects the trigger kind ("cron", "webhook", or "event") and the remaining
// fields carry the kind-specific settings: Cron for a cron trigger, Secret and
// OIDCIssuer/OIDCClaims for a webhook trigger, and EventPipeline/EventStatus
// for an event trigger.
type triggerRequest struct {
	Name          string            `json:"name"`
	Type          string            `json:"type"`
	Cron          string            `json:"cron,omitempty"`
	Secret        string            `json:"secret,omitempty"`
	EventPipeline string            `json:"event_pipeline,omitempty"`
	EventStatus   string            `json:"event_status,omitempty"`
	Params        map[string]string `json:"params,omitempty"`
	// OIDCIssuer is the OIDC issuer of the token a webhook caller must
	// present (a webhook trigger); when set the API verifies the caller's
	// Bearer token against it and matches its claims against OIDCClaims.
	OIDCIssuer string `json:"oidc_issuer,omitempty"`
	// OIDCClaims are the claims a webhook caller's OIDC token must carry for
	// the trigger to match (a webhook trigger); a value of "*" is a wildcard.
	OIDCClaims map[string]string `json:"oidc_claims,omitempty"`
}

// pipelineJobRequest is the JSON form of a single job definition in a
// pipeline (F-08). Key is the job's stable, pipeline-scoped identifier; Needs
// lists the keys of the jobs this job depends on; Spec is the execution spec
// to snapshot onto each of the job's run instances.
type pipelineJobRequest struct {
	Key         string          `json:"key"`
	Name        string          `json:"name,omitempty"`
	TargetGroup string          `json:"target_group,omitempty"`
	Needs       []string        `json:"needs,omitempty"`
	Spec        *jobSpecRequest `json:"spec,omitempty"`
}

func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request) {
	var req pipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	jobs, err := pipelineJobsToProto(req.Jobs)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid jobs: %v", err)
		return
	}
	pipeline, err := s.clients.Database.CreatePipeline(r.Context(), &dbpb.CreatePipelineRequest{
		Name:        req.Name,
		Description: req.Description,
		Jobs:        jobs,
		Triggers:    triggersToProto(req.Triggers),
		Params:      pipelineParamsToProto(req.Params),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, pipeline)
}

func (s *Server) updatePipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	var req pipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	jobs, err := pipelineJobsToProto(req.Jobs)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid jobs: %v", err)
		return
	}
	pipeline, err := s.clients.Database.UpdatePipeline(r.Context(), &dbpb.UpdatePipelineRequest{
		Id:          pipelineID,
		Name:        req.Name,
		Description: req.Description,
		Jobs:        jobs,
		Triggers:    triggersToProto(req.Triggers),
		Params:      pipelineParamsToProto(req.Params),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pipeline)
}

// triggersToProto converts the JSON trigger definitions into the proto Trigger
// list carried to the database service (F-09). It returns nil when there are
// no triggers, so an update with no triggers leaves the pipeline's triggers
// unchanged.
func triggersToProto(triggers []triggerRequest) []*dbpb.Trigger {
	if len(triggers) == 0 {
		return nil
	}
	out := make([]*dbpb.Trigger, 0, len(triggers))
	for _, trigger := range triggers {
		out = append(out, &dbpb.Trigger{
			Name:          trigger.Name,
			Type:          triggerTypeFromName(trigger.Type),
			Cron:          trigger.Cron,
			Secret:        trigger.Secret,
			EventPipeline: trigger.EventPipeline,
			EventStatus:   runStatusFromName(trigger.EventStatus),
			Params:        trigger.Params,
			OidcIssuer:    trigger.OIDCIssuer,
			OidcClaims:    trigger.OIDCClaims,
		})
	}
	return out
}

// pipelineParamsToProto converts the JSON parameter declarations into the
// proto Parameter list carried to the database service (F-10). It returns nil
// when there are no params, so an update with no params leaves the pipeline's
// parameters unchanged.
func pipelineParamsToProto(params []parameterRequest) []*dbpb.Parameter {
	if len(params) == 0 {
		return nil
	}
	out := make([]*dbpb.Parameter, 0, len(params))
	for _, param := range params {
		out = append(out, &dbpb.Parameter{
			Name:        param.Name,
			Default:     param.Default,
			Description: param.Description,
		})
	}
	return out
}

// triggerTypeFromName maps a trigger's JSON type to the proto TriggerType
// (F-09).
func triggerTypeFromName(name string) dbpb.TriggerType {
	switch name {
	case "cron":
		return dbpb.TriggerType_TRIGGER_TYPE_CRON
	case "webhook":
		return dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK
	case "event":
		return dbpb.TriggerType_TRIGGER_TYPE_EVENT
	default:
		return dbpb.TriggerType_TRIGGER_TYPE_UNSPECIFIED
	}
}

// runStatusFromName maps a run status name to the proto RunStatus (F-09).
func runStatusFromName(name string) dbpb.RunStatus {
	switch name {
	case "pending":
		return dbpb.RunStatus_RUN_STATUS_PENDING
	case "running":
		return dbpb.RunStatus_RUN_STATUS_RUNNING
	case "succeeded":
		return dbpb.RunStatus_RUN_STATUS_SUCCEEDED
	case "failed":
		return dbpb.RunStatus_RUN_STATUS_FAILED
	case "cancelled":
		return dbpb.RunStatus_RUN_STATUS_CANCELLED
	default:
		return dbpb.RunStatus_RUN_STATUS_UNSPECIFIED
	}
}

// pipelineJobsToProto converts the JSON job definitions into the proto
// JobDefinition list carried to the database service (F-08). It returns nil
// when there are no jobs, so an update with no jobs leaves the pipeline's
// definitions unchanged.
func pipelineJobsToProto(jobs []pipelineJobRequest) ([]*dbpb.JobDefinition, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	defs := make([]*dbpb.JobDefinition, 0, len(jobs))
	for _, job := range jobs {
		spec, err := job.Spec.toProtoSpec()
		if err != nil {
			return nil, fmt.Errorf("job %q: %w", job.Key, err)
		}
		defs = append(defs, &dbpb.JobDefinition{
			Key:         job.Key,
			Name:        job.Name,
			TargetGroup: job.TargetGroup,
			Needs:       job.Needs,
			Spec:        spec,
		})
	}
	return defs, nil
}

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {
	response, err := s.clients.Database.ListPipelines(r.Context(), &dbpb.ListPipelinesRequest{})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetPipelines()))
}

// ---------------------------------------------------------------------------
// Pipeline runs (F-07)
// ---------------------------------------------------------------------------

// runRequest is the body of POST /api/pipelines/{id}/runs. Trigger is how the
// run was started (e.g. "manual"); Params are the run's parameters (F-10).
type runRequest struct {
	Trigger string            `json:"trigger,omitempty"`
	Params  map[string]string `json:"params,omitempty"`
}

// createRun triggers a new execution of a pipeline (F-07): it asks the
// scheduler to create a PipelineRun and one job instance per job definition in
// the pipeline, then drive them. The run's overall status is derived from its
// job instances by the scheduler's run-status loop.
func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	var req runRequest
	// An empty body is allowed (a run with the default trigger and no params);
	// json.Decoder reports io.EOF for one, which we treat as "no body".
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	run, err := s.clients.Scheduler.CreateRun(r.Context(), &schedpb.CreateRunRequest{
		PipelineId: pipelineID,
		Trigger:    req.Trigger,
		Params:     req.Params,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.publish(Event{Type: EventRunStatus, RunID: run.GetId(), Status: runStatusName(run.GetStatus())})
	writeJSON(w, http.StatusCreated, run)
}

// listRuns lists a pipeline's runs (F-07), most recent first.
func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	response, err := s.clients.Database.ListRuns(r.Context(), &dbpb.ListRunsRequest{PipelineId: pipelineID})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetRuns()))
}

// getRun fetches a single run (F-07).
func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	runID, ok := pathID(w, r)
	if !ok {
		return
	}
	run, err := s.clients.Database.GetRun(r.Context(), &dbpb.GetRunRequest{Id: runID})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

// webhookSecretHeader is the header a webhook caller presents the pipeline's
// webhook trigger secret in (F-09).
const webhookSecretHeader = "X-Cdrom-Webhook-Secret"

// webhook starts a run of a pipeline from a webhook trigger (F-09): an
// external POST (e.g. from a git host). The request's JSON body (a flat object
// of string values) is recorded as the run's parameters, so the caller can
// pass a commit, branch, or any other context to the run.
//
// The handler looks up the pipeline's webhook triggers and matches the request
// against them. A trigger may authenticate the caller in two independent
// ways, both of which must hold when both are set:
//
//   - a non-empty secret: the presented secret (in the X-Cdrom-Webhook-Secret
//     header) must match it (constant-time, to avoid a timing oracle);
//   - a non-empty oidc_issuer: the caller must present an OIDC Bearer token
//     that verifies against that issuer and whose claims satisfy the
//     trigger's oidc_claims (a value of "*" is a wildcard matching any value,
//     including the claim being absent).
//
// A trigger with neither a secret nor an oidc_issuer is open: any POST starts
// a run. A matched trigger starts a run via the scheduler's TriggerRun RPC
// (which atomically deduplicates it); when the caller authenticated with an
// OIDC token the token's flattened claims are recorded on the run (and its
// job instances) so the API can stamp them (prefixed with upstream_) onto the
// job tokens it hands to execution targets. A pipeline with no webhook
// trigger is rejected with 404.
func (s *Server) webhook(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	pipeline, err := s.clients.Database.GetPipeline(r.Context(), &dbpb.GetPipelineRequest{Id: pipelineID})
	if err != nil {
		grpcError(w, err)
		return
	}
	// Find the pipeline's webhook triggers.
	var webhookTriggers []*dbpb.Trigger
	for _, trigger := range pipeline.GetTriggers() {
		if trigger.GetType() == dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK {
			webhookTriggers = append(webhookTriggers, trigger)
		}
	}
	if len(webhookTriggers) == 0 {
		httpError(w, http.StatusNotFound, "pipeline has no webhook trigger")
		return
	}
	// Match the request against the webhook triggers. A trigger is matched
	// when every credential it sets is satisfied: a set secret must equal the
	// presented secret (constant-time), and a set oidc_issuer must have a
	// verifying Bearer token whose claims satisfy the trigger's oidc_claims.
	// A trigger with neither is open (any request matches it).
	secret := r.Header.Get(webhookSecretHeader)
	token := httpBearerToken(r)
	var matched *dbpb.Trigger
	var upstreamClaims map[string]any
	for _, trigger := range webhookTriggers {
		if !webhookTriggerMatches(trigger, secret, token, r.Context(), s.webhookOIDC, &upstreamClaims) {
			continue
		}
		matched = trigger
		break
	}
	if matched == nil {
		httpError(w, http.StatusUnauthorized, "webhook request matched no trigger")
		return
	}
	// The request body is a flat object of string values, recorded as the
	// run's parameters. An empty body is allowed (a run with the trigger's
	// static params only).
	var payload map[string]string
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	// Merge the trigger's static params (the payload overrides them on a key
	// collision).
	params := make(map[string]string, len(matched.GetParams())+len(payload))
	for k, v := range matched.GetParams() {
		params[k] = v
	}
	for k, v := range payload {
		params[k] = v
	}
	run, err := s.clients.Scheduler.TriggerRun(r.Context(), &schedpb.TriggerRunRequest{
		PipelineId:     pipelineID,
		TriggerName:    matched.GetName(),
		TriggerType:    dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK,
		Params:         params,
		UpstreamClaims: upstreamClaimsToStruct(upstreamClaims),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.publish(Event{Type: EventRunStatus, RunID: run.GetId(), Status: runStatusName(run.GetStatus())})
	writeJSON(w, http.StatusCreated, run)
}

// webhookTriggerMatches reports whether a webhook trigger is satisfied by the
// presented secret and OIDC token. A trigger with neither a secret nor an
// oidc_issuer is open and always matches. When the trigger sets an
// oidc_issuer the caller's Bearer token is verified against it and its claims
// (flattened to dot-addressed string values) are matched against the
// trigger's oidc_claims (a value of "*" is a wildcard); on a match the token's
// full structured claims are written to upstreamClaims (so the caller can
// record them on the run). A set secret must additionally equal the presented
// secret (constant-time).
func webhookTriggerMatches(trigger *dbpb.Trigger, secret, token string, ctx context.Context, verifier webhookTokenVerifier, upstreamClaims *map[string]any) bool {
	issuer := trigger.GetOidcIssuer()
	if issuer != "" {
		if token == "" {
			return false
		}
		claims, err := verifier.verifyToken(ctx, issuer, token)
		if err != nil {
			return false
		}
		// Match against the flattened claims (a trigger's oidc_claims are
		// dot-addressed string values); record the raw structured claims so a
		// complex claim (an object or a list) keeps its shape on the run.
		if !oidcClaimsMatch(flattenClaims(claims), trigger.GetOidcClaims()) {
			return false
		}
		if upstreamClaims != nil {
			*upstreamClaims = claims
		}
	}
	if trigger.GetSecret() != "" {
		if subtle.ConstantTimeCompare([]byte(secret), []byte(trigger.GetSecret())) != 1 {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

// paramValueRequest is the JSON form of a single step param value: either a
// scalar String or a list of Strings (by convention, only one is set).
type paramValueRequest struct {
	String  string   `json:"string,omitempty"`
	Strings []string `json:"strings,omitempty"`
}

// jobStepRequest is the JSON form of a single execution step. Type selects the
// step handler that runs the step; empty means the built-in "shell" handler.
// Timeout is a duration string (e.g. "30s", "5m"); empty means no per-step
// timeout. Handler-specific settings live in Params, not in the step's own
// fields, so a new step type can be added without changing the request schema.
// The built-in shell handler reads its command, args, and shell from Params:
//   - "command" (string) — the executable to run (required for the shell
//     handler).
//   - "args" (list of strings) — the command's arguments, in order.
//   - "shell" (string) — when set, the command is run through this shell
//     instead of directly: the target executes `<shell> <args> <command>`
//     (e.g. shell "pwsh", args ["-NoProfile", "-Command"], command
//     "Get-ChildItem"). Empty means run command directly (no shell).
type jobStepRequest struct {
	Type         string                        `json:"type,omitempty"`
	Workdir      string                        `json:"workdir,omitempty"`
	Env          map[string]string             `json:"env,omitempty"`
	Timeout      string                        `json:"timeout,omitempty"`
	Condition    string                        `json:"condition,omitempty"`
	IgnoreFailed bool                          `json:"ignore_failed,omitempty"`
	Params       map[string]*paramValueRequest `json:"params,omitempty"`
}

// jobRequest is the body of POST /api/jobs. Spec is the execution spec to
// snapshot onto the job; when omitted the job has no steps and succeeds
// without doing any work. DependsOn (F-06) is a minimal, single-level
// dependency mechanism ahead of F-08's full DAG/`needs`: when non-empty the
// job is held pending until every dependency succeeds — see
// docs/Architecture.md.
type jobRequest struct {
	PipelineID  int64   `json:"pipeline_id"`
	Name        string  `json:"name"`
	TargetGroup string  `json:"target_group"`
	DependsOn   []int64 `json:"depends_on,omitempty"`
	// Key is the job's stable, pipeline-scoped identifier (F-08); required
	// when the job belongs to a pipeline and is referenced by other jobs'
	// needs.
	Key   string          `json:"key,omitempty"`
	Needs []string        `json:"needs,omitempty"`
	Spec  *jobSpecRequest `json:"spec,omitempty"`
}

// jobSpecRequest is the JSON form of a job's execution spec. Timeout is a
// duration string (e.g. "30s", "5m") bounding the whole job (all steps
// combined); empty means no job-level timeout. Retry is the job's retry
// policy (F-04); when omitted the job is never retried.
type jobSpecRequest struct {
	Steps        []jobStepRequest    `json:"steps,omitempty"`
	Timeout      string              `json:"timeout,omitempty"`
	Retry        *retryPolicyRequest `json:"retry,omitempty"`
	IgnoreFailed bool                `json:"ignore_failed,omitempty"`
}

// retryPolicyRequest is the JSON form of a job's retry policy (F-04).
// MaxAttempts is the number of retries after the initial attempt (0 means the
// job is never retried); Backoff is a duration string (e.g. "10s") to wait
// before each retry (empty means retries are dispatched immediately).
type retryPolicyRequest struct {
	MaxAttempts int    `json:"max_attempts,omitempty"`
	Backoff     string `json:"backoff,omitempty"`
}

// toProtoSpec converts the JSON spec into the proto JobSpec carried to the
// scheduler. It returns nil when the spec is absent and has no steps, no job
// level timeout, and no retry policy.
func (r *jobSpecRequest) toProtoSpec() (*dbpb.JobSpec, error) {
	if r == nil || (len(r.Steps) == 0 && r.Timeout == "" && r.Retry == nil) {
		return nil, nil
	}
	spec := &dbpb.JobSpec{}
	if r.Timeout != "" {
		duration, err := time.ParseDuration(r.Timeout)
		if err != nil {
			return nil, fmt.Errorf("spec: invalid timeout %q: %w", r.Timeout, err)
		}
		spec.Timeout = durationpb.New(duration)
	}
	if r.Retry != nil {
		retry := &dbpb.RetryPolicy{MaxAttempts: int32(r.Retry.MaxAttempts)}
		if r.Retry.Backoff != "" {
			duration, err := time.ParseDuration(r.Retry.Backoff)
			if err != nil {
				return nil, fmt.Errorf("spec: invalid retry backoff %q: %w", r.Retry.Backoff, err)
			}
			retry.Backoff = durationpb.New(duration)
		}
		spec.Retry = retry
	}
	spec.IgnoreFailed = r.IgnoreFailed
	for i, step := range r.Steps {
		// A command is required for the built-in shell handler (the default
		// when type is empty); other step types may carry their work in
		// params instead.
		if step.Type == "" && paramString(step.Params, "command") == "" {
			return nil, fmt.Errorf("spec: step %d: command is required", i)
		}
		protoStep := &dbpb.JobStep{
			Type:         step.Type,
			Workdir:      step.Workdir,
			Env:          step.Env,
			Condition:    step.Condition,
			IgnoreFailed: step.IgnoreFailed,
			Params:       paramsToProto(step.Params),
		}
		if step.Timeout != "" {
			duration, err := time.ParseDuration(step.Timeout)
			if err != nil {
				return nil, fmt.Errorf("spec: step %d: invalid timeout %q: %w", i, step.Timeout, err)
			}
			protoStep.Timeout = durationpb.New(duration)
		}
		spec.Steps = append(spec.Steps, protoStep)
	}
	return spec, nil
}

// paramsToProto converts the JSON params map into the proto params map.
func paramsToProto(in map[string]*paramValueRequest) map[string]*dbpb.ParamValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*dbpb.ParamValue, len(in))
	for k, v := range in {
		out[k] = &dbpb.ParamValue{
			String_: v.String,
			Strings: v.Strings,
		}
	}
	return out
}

// paramString returns the string value of a param, or "" if the param is
// absent or not a string.
func paramString(in map[string]*paramValueRequest, key string) string {
	if v := in[key]; v != nil {
		return v.String
	}
	return ""
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	var req jobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	spec, err := req.Spec.toProtoSpec()
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid spec: %v", err)
		return
	}
	// A job that belongs to a pipeline is a job *definition* (F-08): it is
	// persisted on the pipeline (via the database service) and becomes part of
	// the pipeline's DAG, from which runs create job instances. It is not
	// dispatched on its own. A job with no pipeline is a standalone job and is
	// created and dispatched by the scheduler.
	if req.PipelineID > 0 {
		created, err := s.clients.Database.CreateJob(r.Context(), &dbpb.CreateJobRequest{
			PipelineId:  req.PipelineID,
			Name:        req.Name,
			TargetGroup: req.TargetGroup,
			DependsOn:   req.DependsOn,
			Key:         req.Key,
			Needs:       req.Needs,
			Spec:        spec,
		})
		if err != nil {
			grpcError(w, err)
			return
		}
		s.publish(Event{Type: EventJobStatus, JobID: created.GetId(), Status: jobStatusName(created.GetStatus())})
		writeJSON(w, http.StatusCreated, created)
		return
	}
	job, err := s.clients.Scheduler.SubmitJob(r.Context(), &schedpb.SubmitJobRequest{
		PipelineId:  req.PipelineID,
		Name:        req.Name,
		TargetGroup: req.TargetGroup,
		DependsOn:   req.DependsOn,
		Key:         req.Key,
		Needs:       req.Needs,
		Spec:        spec,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.publish(Event{Type: EventJobStatus, JobID: job.GetId(), Status: jobStatusName(job.GetStatus())})
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	req := &schedpb.ListJobsRequest{}
	if value := query.Get("pipeline_id"); value != "" {
		if id, err := strconv.ParseInt(value, 10, 64); err == nil {
			req.PipelineId = id
		}
	}
	if value := query.Get("status"); value != "" {
		req.Status = jobStatusFromName(value)
	}
	response, err := s.clients.Scheduler.ListJobs(r.Context(), req)
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetJobs()))
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := s.clients.Scheduler.GetJob(r.Context(), &schedpb.GetJobRequest{Id: jobID})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := s.clients.Scheduler.CancelJob(r.Context(), &schedpb.CancelJobRequest{Id: jobID})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.publish(Event{Type: EventJobStatus, JobID: job.GetId(), Status: jobStatusName(job.GetStatus())})
	writeJSON(w, http.StatusOK, job)
}

// rerunJob re-runs a finished job (F-04): it resets the job to pending with a
// fresh attempt and re-dispatches it, so the job runs again with the same
// spec. Only jobs in a terminal state can be re-run.
func (s *Server) rerunJob(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := s.clients.Scheduler.RerunJob(r.Context(), &schedpb.RerunJobRequest{Id: jobID})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.publish(Event{Type: EventJobStatus, JobID: job.GetId(), Status: jobStatusName(job.GetStatus())})
	writeJSON(w, http.StatusOK, job)
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	response, err := s.clients.Database.ListWorkers(r.Context(), &dbpb.ListWorkersRequest{
		Group: r.URL.Query().Get("group"),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetWorkers()))
}

// ---------------------------------------------------------------------------
// Artifacts
// ---------------------------------------------------------------------------

func (s *Server) listJobArtifacts(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	response, err := s.clients.Artifacts.ListArtifacts(r.Context(), &artifactspb.ListArtifactsRequest{
		Namespace: strconv.FormatInt(jobID, 10),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetArtifacts()))
}

// ---------------------------------------------------------------------------
// Job logs
// ---------------------------------------------------------------------------

// listJobLogs lists a job's log files (one per step plus the combined
// job.log) via the artifacts service.
func (s *Server) listJobLogs(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	response, err := s.clients.Artifacts.ListLogs(r.Context(), &artifactspb.ListLogsRequest{
		Namespace: strconv.FormatInt(jobID, 10),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetLogs()))
}

// getJobLog streams a job's log file (a step's output or the combined
// job.log) to the caller. The name is the log file name, e.g. "step-0.log"
// or "job.log".
func (s *Server) getJobLog(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "log name is required")
		return
	}
	stream, err := s.clients.Artifacts.DownloadLog(r.Context(), &artifactspb.DownloadLogRequest{
		Namespace: strconv.FormatInt(jobID, 10),
		Name:      name,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	// The first chunk carries metadata; set the content type from it, then
	// stream the data chunks to the response body.
	first, err := stream.Recv()
	if err != nil {
		grpcError(w, err)
		return
	}
	if contentType := first.GetMetadata().GetContentType(); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(first.GetData()); err != nil {
		return
	}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			return
		}
		if _, err := w.Write(chunk.GetData()); err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// publish sends an event to the hub, if one is configured.
func (s *Server) publish(ev Event) {
	if s.hub != nil {
		s.hub.Publish(ev)
	}
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	value := r.PathValue("id")
	jobID, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid job id %q", value)
		return 0, false
	}
	return jobID, true
}

func jobStatusFromName(name string) dbpb.JobStatus {
	switch name {
	case "pending":
		return dbpb.JobStatus_JOB_STATUS_PENDING
	case "running":
		return dbpb.JobStatus_JOB_STATUS_RUNNING
	case "succeeded":
		return dbpb.JobStatus_JOB_STATUS_SUCCEEDED
	case "failed":
		return dbpb.JobStatus_JOB_STATUS_FAILED
	case "cancelled":
		return dbpb.JobStatus_JOB_STATUS_CANCELLED
	case "timed_out":
		return dbpb.JobStatus_JOB_STATUS_TIMED_OUT
	case "skipped":
		return dbpb.JobStatus_JOB_STATUS_SKIPPED
	default:
		return dbpb.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

// runStatusName maps a run status enum to the short name the UI uses.
func runStatusName(status dbpb.RunStatus) string {
	switch status {
	case dbpb.RunStatus_RUN_STATUS_PENDING:
		return "pending"
	case dbpb.RunStatus_RUN_STATUS_RUNNING:
		return "running"
	case dbpb.RunStatus_RUN_STATUS_SUCCEEDED:
		return "succeeded"
	case dbpb.RunStatus_RUN_STATUS_FAILED:
		return "failed"
	case dbpb.RunStatus_RUN_STATUS_CANCELLED:
		return "cancelled"
	default:
		return "unknown"
	}
}

// nonNil returns a non-nil slice so that empty results marshal to [] rather
// than null in JSON responses.
func nonNil[T any](slice []T) []T {
	if slice == nil {
		return []T{}
	}
	return slice
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func httpError(w http.ResponseWriter, code int, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf(format, args...)})
}
