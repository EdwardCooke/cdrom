// Package scheduler implements the cdrom.scheduler.v1.Scheduler gRPC
// service: job lifecycle and dispatch.
//
// The API/controller layer calls SubmitJob to create a job. For high
// availability (F-23) the scheduler writes to the shared Database service
// only: it persists the job and appends an "assignment" event to the shared
// event log, which every API pod tails and fans out to its local workers. The
// scheduler no longer dials a specific API pod, so a job is delivered to
// whichever pod a worker happens to be connected to. Jobs with an empty
// target group are left pending for ephemeral Kubernetes agents.
//
// The scheduler holds no durable state of its own: all pipelines, jobs, and
// the event log are owned by the Database service. Worker registration,
// WatchJobs streams, and job status reporting live on the API service.
package scheduler

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// Server implements the Scheduler gRPC service.
type Server struct {
	schedpb.UnimplementedSchedulerServer
	db     dbpb.DatabaseClient
	logger *slog.Logger
}

// NewServer creates a Scheduler service. db is the client for the Database
// service, which owns all durable state and the shared event log the scheduler
// publishes to (F-23).
func NewServer(db dbpb.DatabaseClient, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{db: db, logger: logger}
}

// ---------------------------------------------------------------------------
// Job control
// ---------------------------------------------------------------------------

// SubmitJob creates the job (persisted via the Database service) and, when a
// target group is set, appends an "assignment" event to the shared event log
// (F-23). Every API pod tails the log and nudges its local workers in the
// group, which fetch the job and claim it atomically. Jobs with an empty
// target group are left pending for an ephemeral agent.
//
// A job with dependencies (DependsOn, F-06) is never dispatched here: it is
// left pending and the background dependency resolver (dependencies.go)
// publishes it once every dependency has succeeded, or marks it skipped if
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
		Key:         req.GetKey(),
		Needs:       req.GetNeeds(),
	})
	if err != nil {
		return nil, err
	}
	job := toProtoJob(created)

	// A job with dependencies (needs, F-08, or depends_on, F-06) is never
	// dispatched here: it is left pending and the background dependency
	// resolver (dependencies.go) dispatches it once every dependency has
	// succeeded, or marks it skipped if any of them does not. The database
	// service resolves needs to depends_on, so a job created with needs comes
	// back with a non-empty depends_on.
	if len(created.GetDependsOn()) > 0 {
		s.logger.Info("scheduler: job held pending dependencies", "job", created.GetId(), "depends_on", created.GetDependsOn())
		return job, nil
	}
	if req.GetTargetGroup() != "" {
		s.publishAssignment(ctx, created.GetId(), req.GetTargetGroup())
	} else {
		s.logger.Info("scheduler: job queued for ephemeral agent", "job", created.GetId())
	}
	return job, nil
}

// publishAssignment appends an "assignment" event to the shared event log
// (F-23) so every API pod can nudge its local workers in the group. It is
// best-effort: a failure to append does not fail the submit (the worker's
// periodic poll is the authoritative path).
func (s *Server) publishAssignment(ctx context.Context, jobID int64, group string) {
	if s.db == nil {
		return
	}
	if _, err := s.db.PublishAssignment(ctx, &dbpb.PublishAssignmentRequest{JobId: jobID, TargetGroup: group}); err != nil {
		s.logger.Warn("scheduler: publish assignment event", "job", jobID, "group", group, "err", err)
	}
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
// the Database service) and appends a "cancel" event to the shared event log
// (F-23). Every API pod tails the log and delivers a JobCancellation down its
// local workers' WatchJobs streams (for a long-lived worker) so the worker
// running the job interrupts it; an ephemeral agent observes the cancellation
// on its next GetJob. Only pending or running jobs can be cancelled;
// cancelling an already-finished job is a no-op (idempotent).
//
// The cancellation is persisted with a conditional update (Database.CancelJob)
// so a job that already reported a terminal status is left untouched. The
// cancel event is best-effort: if no pod can deliver it (no live worker, or a
// pod is down) the job is still marked cancelled in the database, and the
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
	// Append a cancel event to the shared event log (F-23) so every API pod
	// can deliver the cancellation to its local workers. Best-effort: a
	// failure to append does not undo the cancellation already persisted above.
	s.publishCancel(ctx, req.GetId())
	updated, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetId()})
	if err != nil {
		return nil, err
	}
	return toProtoJob(updated), nil
}

// publishCancel appends a "cancel" event to the shared event log (F-23) so
// every API pod can deliver the cancellation to its local workers.
func (s *Server) publishCancel(ctx context.Context, jobID int64) {
	if s.db == nil {
		return
	}
	if _, err := s.db.PublishCancel(ctx, &dbpb.PublishCancelRequest{JobId: jobID}); err != nil {
		s.logger.Warn("scheduler: publish cancel event", "job", jobID, "err", err)
	}
}

// jobRerunner is the subset of the Database client the RerunJob RPC uses to
// reset a finished job to pending with a fresh attempt. The concrete
// dbpb.DatabaseClient satisfies it; tests inject a fake.
type jobRerunner interface {
	RerunJob(ctx context.Context, in *dbpb.RerunJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error)
}

// jobPublisher is the subset of the Database client the RerunJob RPC uses to
// append an "assignment" event for the re-run job to the shared event log
// (F-23). The concrete dbpb.DatabaseClient satisfies it; tests inject a fake.
type jobPublisher interface {
	PublishAssignment(ctx context.Context, in *dbpb.PublishAssignmentRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error)
}

// RerunJob re-runs a finished job (F-04): it resets the job to pending with a
// fresh attempt (attempt 1) and appends an "assignment" event to the shared
// event log (F-23), so the job runs again with the same spec. Only jobs in a
// terminal state (succeeded, failed, cancelled, or timed_out) can be re-run;
// a job that is still pending or running is rejected. db and publisher are
// the RPC's dependencies (the concrete clients in production, fakes in
// tests); publisher may be nil to skip the event append.
func (s *Server) rerunJob(ctx context.Context, db jobRerunner, publisher jobPublisher, jobID int64) (*schedpb.Job, error) {
	updated, err := db.RerunJob(ctx, &dbpb.RerunJobRequest{Id: jobID})
	if err != nil {
		return nil, err
	}
	job := toProtoJob(updated)
	s.logger.Info("scheduler: job re-run", "job", updated.GetId())
	if publisher == nil {
		return job, nil
	}
	if updated.GetTargetGroup() != "" {
		if _, err := publisher.PublishAssignment(ctx, &dbpb.PublishAssignmentRequest{JobId: updated.GetId(), TargetGroup: updated.GetTargetGroup()}); err != nil {
			s.logger.Warn("scheduler: publish re-run assignment failed; job left pending",
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
	return s.rerunJob(ctx, s.db, s.db, req.GetId())
}

// CreateRun starts a new execution of a pipeline (F-07): it asks the Database
// service to create the PipelineRun and one job instance per job definition in
// the pipeline (the database remaps each instance's depends_on from the
// definition ids to the new instance ids, atomically, in a single
// transaction), and then drives the instances:
//
//   - an instance with dependencies is left pending; the background dependency
//     resolver (dependencies.go) dispatches it once its dependencies succeed,
//     or marks it skipped if any of them does not;
//   - an instance with a target group is dispatched to the API, which fans it
//     out to the live workers in that group;
//   - an instance with an empty target group is left pending for an ephemeral
//     Kubernetes agent.
//
// The run's overall status is derived from its job instances by the
// scheduler's run-status loop (runstatus.go), not here.
func (s *Server) CreateRun(ctx context.Context, req *schedpb.CreateRunRequest) (*schedpb.Run, error) {
	if req.GetPipelineId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "pipeline_id is required")
	}
	created, err := s.db.CreateRun(ctx, &dbpb.CreateRunRequest{
		PipelineId: req.GetPipelineId(),
		Trigger:    req.GetTrigger(),
		Params:     req.GetParams(),
	})
	if err != nil {
		return nil, err
	}
	run := toProtoRun(created.GetRun())
	for _, job := range created.GetJobs() {
		s.dispatchRunInstance(ctx, job)
	}
	s.logger.Info("scheduler: run created", "run", run.GetId(), "pipeline", req.GetPipelineId(), "jobs", len(created.GetJobs()))
	return run, nil
}

// dispatchRunInstance drives a single job instance of a run (F-07), mirroring
// SubmitJob's dispatch logic: an instance with dependencies (needs, F-08, or
// depends_on, F-06) is left pending for the dependency resolver; an instance
// with a target group has an "assignment" event appended to the shared event
// log (F-23), which every API pod tails and fans out to its local workers; an
// instance with an empty target group is left pending for an ephemeral agent.
func (s *Server) dispatchRunInstance(ctx context.Context, job *dbpb.Job) {
	if len(job.GetDependsOn()) > 0 {
		s.logger.Info("scheduler: run instance held pending dependencies", "job", job.GetId(), "run", job.GetRunId(), "depends_on", job.GetDependsOn())
		return
	}
	if job.GetTargetGroup() != "" {
		s.publishAssignment(ctx, job.GetId(), job.GetTargetGroup())
	} else {
		s.logger.Info("scheduler: run instance queued for ephemeral agent", "job", job.GetId(), "run", job.GetRunId())
	}
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

func toProtoJob(job *dbpb.Job) *schedpb.Job {
	return &schedpb.Job{
		Id:           job.GetId(),
		PipelineId:   job.GetPipelineId(),
		RunId:        job.GetRunId(),
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
		Key:          job.GetKey(),
	}
}

// toProtoRun converts a db proto PipelineRun into the scheduler's Run message.
func toProtoRun(run *dbpb.PipelineRun) *schedpb.Run {
	return &schedpb.Run{
		Id:         run.GetId(),
		PipelineId: run.GetPipelineId(),
		Status:     run.GetStatus(),
		Trigger:    run.GetTrigger(),
		Params:     run.GetParams(),
		StartedAt:  run.GetStartedAt(),
		FinishedAt: run.GetFinishedAt(),
		CreatedAt:  run.GetCreatedAt(),
		UpdatedAt:  run.GetUpdatedAt(),
	}
}
