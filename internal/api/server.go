// Package api implements the API/controller layer: an HTTP surface consumed
// by the UI that routes requests to the gRPC services. No business logic
// belongs here — it is a thin bridge between the UI and the service layer.
package api

import (
	"encoding/json"
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
}

// New creates an API server over the given gRPC clients. hub is the event
// hub that backs the /api/ws WebSocket endpoint; it may be nil to disable
// the WebSocket endpoint.
func New(clients Clients, hub *EventHub) *Server {
	return &Server{clients: clients, hub: hub}
}

// Handler builds the HTTP handler for the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Pipelines (via the database service).
	mux.HandleFunc("POST /api/pipelines", s.createPipeline)
	mux.HandleFunc("GET /api/pipelines", s.listPipelines)

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
}

func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request) {
	var req pipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	pipeline, err := s.clients.Database.CreatePipeline(r.Context(), &dbpb.CreatePipelineRequest{
		Name:        req.Name,
		Description: req.Description,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, pipeline)
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
	Outputs      []string                      `json:"outputs,omitempty"`
	Params       map[string]*paramValueRequest `json:"params,omitempty"`
}

// jobRequest is the body of POST /api/jobs. Spec is the execution spec to
// snapshot onto the job; when omitted the job has no steps and succeeds
// without doing any work. DependsOn (F-06) is a minimal, single-level
// dependency mechanism ahead of F-08's full DAG/`needs`: when non-empty the
// job is held pending until every dependency succeeds — see
// docs/Architecture.md.
type jobRequest struct {
	PipelineID  int64           `json:"pipeline_id"`
	Name        string          `json:"name"`
	TargetGroup string          `json:"target_group"`
	DependsOn   []int64         `json:"depends_on,omitempty"`
	Spec        *jobSpecRequest `json:"spec,omitempty"`
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
			Outputs:      step.Outputs,
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
	job, err := s.clients.Scheduler.SubmitJob(r.Context(), &schedpb.SubmitJobRequest{
		PipelineId:  req.PipelineID,
		Name:        req.Name,
		TargetGroup: req.TargetGroup,
		DependsOn:   req.DependsOn,
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
