// Package scheduler implements the cdrom.scheduler.v1.Scheduler gRPC
// service: job lifecycle and dispatch.
//
// The API/controller layer calls SubmitJob to create a job. When the job
// targets a group of long-lived workers, the scheduler pushes it to the API
// (DispatchJob), which fans it out to the live workers in that group. Jobs
// with an empty target group are queued for ephemeral Kubernetes agents,
// which the control plane spawns.
//
// The scheduler holds no durable state of its own: all pipelines and jobs are
// persisted through the Database service. Worker registration, WatchJobs
// streams, and job status reporting live on the API service.
package scheduler

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// Server implements the Scheduler gRPC service.
type Server struct {
	schedpb.UnimplementedSchedulerServer
	db     dbpb.DatabaseClient
	api    apipb.APIClient
	logger *slog.Logger
}

// NewServer creates a Scheduler service. db is the client for the Database
// service, which owns all durable state; api is the client for the API
// service, which holds the live worker streams and receives dispatches.
func NewServer(db dbpb.DatabaseClient, api apipb.APIClient, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{db: db, api: api, logger: logger}
}

// ---------------------------------------------------------------------------
// Job control
// ---------------------------------------------------------------------------

// SubmitJob creates the job (persisted via the Database service) and, when a
// target group is set, dispatches it to the API, which fans it out to the
// live workers in that group. Jobs with an empty target group are left
// pending for an ephemeral agent.
//
// A job with dependencies (DependsOn, F-06) is never dispatched here: it is
// left pending and the background dependency resolver (dependencies.go)
// dispatches it once every dependency has succeeded, or marks it skipped if
// any of them does not.
func (s *Server) SubmitJob(ctx context.Context, req *schedpb.SubmitJobRequest) (*schedpb.Job, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "job name is required")
	}
	created, err := s.db.CreateJob(ctx, &dbpb.CreateJobRequest{
		PipelineId:  req.GetPipelineId(),
		Name:        req.GetName(),
		TargetGroup: req.GetTargetGroup(),
		Spec:        req.GetSpec(),
		DependsOn:   req.GetDependsOn(),
	})
	if err != nil {
		return nil, err
	}
	job := toProtoJob(created)

	if len(req.GetDependsOn()) > 0 {
		s.logger.Info("scheduler: job held pending dependencies", "job", created.GetId(), "depends_on", req.GetDependsOn())
		return job, nil
	}
	if req.GetTargetGroup() != "" {
		if _, err := s.api.DispatchJob(ctx, &apipb.DispatchJobRequest{Job: toAPIJob(job)}); err != nil {
			s.logger.Warn("scheduler: dispatch to api failed; job left pending",
				"job", created.GetId(), "group", req.GetTargetGroup(), "err", err)
		}
	} else {
		s.logger.Info("scheduler: job queued for ephemeral agent", "job", created.GetId())
	}
	return job, nil
}

// GetJob fetches a job from the Database service.
func (s *Server) GetJob(ctx context.Context, req *schedpb.GetJobRequest) (*schedpb.Job, error) {
	job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetId()})
	if err != nil {
		return nil, err
	}
	return toProtoJob(job), nil
}

// ListJobs lists jobs from the Database service.
func (s *Server) ListJobs(ctx context.Context, req *schedpb.ListJobsRequest) (*schedpb.ListJobsResponse, error) {
	response, err := s.db.ListJobs(ctx, &dbpb.ListJobsRequest{
		PipelineId: req.GetPipelineId(),
		Status:     req.GetStatus(),
	})
	if err != nil {
		return nil, err
	}
	result := &schedpb.ListJobsResponse{}
	for _, job := range response.GetJobs() {
		result.Jobs = append(result.Jobs, toProtoJob(job))
	}
	return result, nil
}

// CancelJob cancels a job (F-05): it marks the job cancelled (persisted via
// the Database service) and signals the execution target running it to stop
// the work. Only pending or running jobs can be cancelled; cancelling an
// already-finished job is a no-op (idempotent).
//
// The cancellation is persisted with a conditional update (Database.CancelJob)
// so a job that already reported a terminal status is left untouched. The
// target is then signalled through the API (API.CancelJob), which delivers a
// JobCancellation down the worker's WatchJobs stream (for a long-lived worker)
// or, for an ephemeral agent, is observed on the agent's next GetJob. Signalling
// the target is best-effort: if it cannot be reached (no live worker, or the
// API is down) the job is still marked cancelled in the database, and the
// target's next status report (or the scheduler's watchdog) reconciles it.
func (s *Server) CancelJob(ctx context.Context, req *schedpb.CancelJobRequest) (*schedpb.Job, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job id is required")
	}
	now := timestamppb.Now()
	resp, err := s.db.CancelJob(ctx, &dbpb.CancelJobRequest{Id: req.GetId(), FinishedAt: now})
	if err != nil {
		return nil, err
	}
	if !resp.GetCancelled() {
		// The job already reached a terminal state; cancelling it is a no-op.
		job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetId()})
		if err != nil {
			return nil, err
		}
		return toProtoJob(job), nil
	}
	// Signal the execution target to stop the work. Best-effort: a failure to
	// reach the API (or a job with no live target) does not undo the
	// cancellation already persisted above.
	if s.api != nil {
		if _, err := s.api.CancelJob(ctx, &apipb.CancelJobRequest{JobId: req.GetId()}); err != nil {
			s.logger.Warn("scheduler: signal target to cancel failed; job already marked cancelled",
				"job", req.GetId(), "err", err)
		}
	}
	updated, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetId()})
	if err != nil {
		return nil, err
	}
	return toProtoJob(updated), nil
}

// jobRerunner is the subset of the Database client the RerunJob RPC uses to
// reset a finished job to pending with a fresh attempt. The concrete
// dbpb.DatabaseClient satisfies it; tests inject a fake.
type jobRerunner interface {
	RerunJob(ctx context.Context, in *dbpb.RerunJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error)
}

// jobDispatcher is the subset of the API client the RerunJob RPC uses to
// dispatch the re-run job to the live workers. The concrete apipb.APIClient
// satisfies it; tests inject a fake.
type jobDispatcher interface {
	DispatchJob(ctx context.Context, in *apipb.DispatchJobRequest, opts ...grpc.CallOption) (*apipb.DispatchJobResponse, error)
}

// RerunJob re-runs a finished job (F-04): it resets the job to pending with a
// fresh attempt (attempt 1) and re-dispatches it, so the job runs again with
// the same spec. Only jobs in a terminal state (succeeded, failed, cancelled,
// or timed_out) can be re-run; a job that is still pending or running is
// rejected. db and api are the RPC's dependencies (the concrete clients in
// production, fakes in tests); api may be nil to skip the dispatch.
func (s *Server) rerunJob(ctx context.Context, db jobRerunner, api jobDispatcher, jobID int64) (*schedpb.Job, error) {
	updated, err := db.RerunJob(ctx, &dbpb.RerunJobRequest{Id: jobID})
	if err != nil {
		return nil, err
	}
	job := toProtoJob(updated)
	s.logger.Info("scheduler: job re-run", "job", updated.GetId())
	if api == nil {
		return job, nil
	}
	if updated.GetTargetGroup() != "" {
		if _, err := api.DispatchJob(ctx, &apipb.DispatchJobRequest{Job: toAPIJob(job)}); err != nil {
			s.logger.Warn("scheduler: dispatch re-run job failed; job left pending",
				"job", updated.GetId(), "group", updated.GetTargetGroup(), "err", err)
		}
	} else {
		s.logger.Info("scheduler: re-run job queued for ephemeral agent", "job", updated.GetId())
	}
	return job, nil
}

// RerunJob is the gRPC entrypoint for re-running a finished job (F-04).
func (s *Server) RerunJob(ctx context.Context, req *schedpb.RerunJobRequest) (*schedpb.Job, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job id is required")
	}
	return s.rerunJob(ctx, s.db, s.api, req.GetId())
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

func toProtoJob(job *dbpb.Job) *schedpb.Job {
	return &schedpb.Job{
		Id:           job.GetId(),
		PipelineId:   job.GetPipelineId(),
		Name:         job.GetName(),
		Status:       job.GetStatus(),
		TargetGroup:  job.GetTargetGroup(),
		StartedAt:    job.GetStartedAt(),
		FinishedAt:   job.GetFinishedAt(),
		Spec:         job.GetSpec(),
		Attempt:      job.GetAttempt(),
		MaxAttempts:  job.GetMaxAttempts(),
		DependsOn:    job.GetDependsOn(),
		StepResults:  job.GetStepResults(),
		Outputs:      job.GetOutputs(),
		UpstreamJobs: job.GetUpstreamJobs(),
		IgnoreFailed: job.GetIgnoreFailed(),
	}
}

func toAPIJob(job *schedpb.Job) *apipb.Job {
	return &apipb.Job{
		Id:           job.GetId(),
		PipelineId:   job.GetPipelineId(),
		Name:         job.GetName(),
		Status:       job.GetStatus(),
		TargetGroup:  job.GetTargetGroup(),
		StartedAt:    job.GetStartedAt(),
		FinishedAt:   job.GetFinishedAt(),
		Spec:         job.GetSpec(),
		Attempt:      job.GetAttempt(),
		MaxAttempts:  job.GetMaxAttempts(),
		DependsOn:    job.GetDependsOn(),
		StepResults:  job.GetStepResults(),
		Outputs:      job.GetOutputs(),
		UpstreamJobs: job.GetUpstreamJobs(),
		IgnoreFailed: job.GetIgnoreFailed(),
	}
}
