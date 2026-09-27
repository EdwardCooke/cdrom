// Package api implements the API/controller layer: an HTTP surface consumed
// by the UI that routes requests to the gRPC services. No business logic
// belongs here — it is a thin bridge between the UI and the service layer.
package api

import (
	"encoding/json"
	"fmt"
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

	// Workers (via the scheduler service).
	mux.HandleFunc("GET /api/workers", s.listWorkers)

	// Artifacts (via the artifacts service).
	mux.HandleFunc("GET /api/jobs/{id}/artifacts", s.listJobArtifacts)

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

// jobStepRequest is the JSON form of a single execution step. Type selects the
// step handler that runs the step; empty means the built-in "shell" handler.
// Timeout is a duration string (e.g. "30s", "5m"); empty means no per-step
// timeout. For the shell handler, Shell, when set, is the interpreter the step
// is run through: the target executes `<shell> <args> <command>` instead of
// command directly (e.g. shell "pwsh", args ["-NoProfile", "-Command"],
// command "Get-ChildItem"). Empty means run command directly (no shell).
// Params carries handler-specific settings (ignored by the shell handler).
type jobStepRequest struct {
	Type    string            `json:"type,omitempty"`
	Command string            `json:"command"`
	Args    []string          `json:"args,omitempty"`
	Workdir string            `json:"workdir,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	Timeout string            `json:"timeout,omitempty"`
	Shell   string            `json:"shell,omitempty"`
	Params  map[string]string `json:"params,omitempty"`
}

// jobRequest is the body of POST /api/jobs. Spec is the execution spec to
// snapshot onto the job; when omitted the job has no steps and succeeds
// without doing any work.
type jobRequest struct {
	PipelineID  int64           `json:"pipeline_id"`
	Name        string          `json:"name"`
	TargetGroup string          `json:"target_group"`
	Spec        *jobSpecRequest `json:"spec,omitempty"`
}

// jobSpecRequest is the JSON form of a job's execution spec.
type jobSpecRequest struct {
	Steps []jobStepRequest `json:"steps,omitempty"`
}

// toProtoSpec converts the JSON spec into the proto JobSpec carried to the
// scheduler. It returns nil when the spec is absent or has no steps.
func (r *jobSpecRequest) toProtoSpec() (*dbpb.JobSpec, error) {
	if r == nil || len(r.Steps) == 0 {
		return nil, nil
	}
	spec := &dbpb.JobSpec{}
	for i, step := range r.Steps {
		// A command is required for the built-in shell handler (the default
		// when type is empty); other step types may carry their work in
		// params instead.
		if step.Type == "" && step.Command == "" {
			return nil, fmt.Errorf("spec: step %d: command is required", i)
		}
		protoStep := &dbpb.JobStep{
			Type:    step.Type,
			Command: step.Command,
			Args:    step.Args,
			Workdir: step.Workdir,
			Env:     step.Env,
			Shell:   step.Shell,
			Params:  step.Params,
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
		JobId: strconv.FormatInt(jobID, 10),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetArtifacts()))
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
