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
func (s *Server) SubmitJob(ctx context.Context, req *schedpb.SubmitJobRequest) (*schedpb.Job, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "job name is required")
	}
	created, err := s.db.CreateJob(ctx, &dbpb.CreateJobRequest{
		PipelineId:  req.GetPipelineId(),
		Name:        req.GetName(),
		TargetGroup: req.GetTargetGroup(),
		Spec:        req.GetSpec(),
	})
	if err != nil {
		return nil, err
	}
	job := toProtoJob(created)

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

// CancelJob marks a job cancelled. Only pending or running jobs can be
// cancelled.
func (s *Server) CancelJob(ctx context.Context, req *schedpb.CancelJobRequest) (*schedpb.Job, error) {
	job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetId()})
	if err != nil {
		return nil, err
	}
	switch job.GetStatus() {
	case dbpb.JobStatus_JOB_STATUS_SUCCEEDED, dbpb.JobStatus_JOB_STATUS_FAILED, dbpb.JobStatus_JOB_STATUS_CANCELLED:
		return nil, status.Errorf(codes.FailedPrecondition, "job %d is already %v", job.GetId(), job.GetStatus())
	}
	now := timestamppb.Now()
	updated, err := s.db.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         req.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_CANCELLED,
		FinishedAt: now,
	})
	if err != nil {
		return nil, err
	}
	return toProtoJob(updated), nil
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

func toProtoJob(job *dbpb.Job) *schedpb.Job {
	return &schedpb.Job{
		Id:          job.GetId(),
		PipelineId:  job.GetPipelineId(),
		Name:        job.GetName(),
		Status:      job.GetStatus(),
		TargetGroup: job.GetTargetGroup(),
		StartedAt:   job.GetStartedAt(),
		FinishedAt:  job.GetFinishedAt(),
		Spec:        job.GetSpec(),
	}
}

func toAPIJob(job *schedpb.Job) *apipb.Job {
	return &apipb.Job{
		Id:          job.GetId(),
		PipelineId:  job.GetPipelineId(),
		Name:        job.GetName(),
		Status:      job.GetStatus(),
		TargetGroup: job.GetTargetGroup(),
		StartedAt:   job.GetStartedAt(),
		FinishedAt:  job.GetFinishedAt(),
		Spec:        job.GetSpec(),
	}
}
