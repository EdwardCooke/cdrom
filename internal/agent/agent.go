// Package agent implements the ephemeral agent: a short-lived process
// spawned in Kubernetes to execute a single one-off job, then terminate.
// Must run on both Windows and Linux.
//
// The control plane spawns an agent with a job ID. The agent talks ONLY to
// the API service: it fetches the job from the API, executes it, and reports
// the final status back to the API before exiting. All logging is local
// (stdout or a file) — there is no central logs service.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logstream"

	// Register the built-in step handlers (e.g. the shell handler) with the
	// executor. A target or plugin adds more step types the same way.
	_ "cdrom/internal/stephandlers"
)

// Dependencies are the gRPC clients an agent needs. The agent talks only to
// the API service.
type Dependencies struct {
	API apipb.APIClient
}

// Agent is an ephemeral job executor.
type Agent struct {
	jobID  int64
	deps   Dependencies
	logger *slog.Logger
	token  string // job token handed over by the API via GetJob
}

// New creates an agent that will execute jobID.
func New(jobID int64, deps Dependencies, logger *slog.Logger) *Agent {
	if logger == nil {
		logger = slog.Default()
	}
	return &Agent{jobID: jobID, deps: deps, logger: logger}
}

// Run executes the job and returns the final status. The process should exit
// once Run returns.
func (a *Agent) Run(ctx context.Context) (dbpb.JobStatus, error) {
	if a.jobID == 0 {
		return dbpb.JobStatus_JOB_STATUS_UNSPECIFIED, fmt.Errorf("agent: job id is required")
	}
	job, err := a.deps.API.GetJob(ctx, &apipb.GetJobRequest{Id: a.jobID})
	if err != nil {
		return dbpb.JobStatus_JOB_STATUS_UNSPECIFIED, fmt.Errorf("agent: get job: %w", err)
	}
	a.token = job.GetToken()
	a.logger.Info("agent: executing job", "job", job.GetId(), "name", job.GetName())

	if _, err := a.deps.API.ReportJobStatus(grpcutil.WithBearerToken(ctx, a.token), &apipb.ReportJobStatusRequest{
		JobId:     a.jobID,
		Status:    dbpb.JobStatus_JOB_STATUS_RUNNING,
		StartedAt: timestamppb.Now(),
	}); err != nil {
		return dbpb.JobStatus_JOB_STATUS_UNSPECIFIED, fmt.Errorf("agent: report running: %w", err)
	}

	a.logger.Info("job started", "job", a.jobID)
	if err := a.runJob(ctx, job); err != nil {
		// A timeout (the job-level or a step's per-step timeout, F-03) is
		// reported as timed_out rather than failed, so the UI can tell a hung
		// job apart from one that ran and errored.
		if errors.Is(err, executor.ErrTimeout) {
			a.logger.Error("job timed out", "job", a.jobID, "err", err.Error())
			a.report(ctx, dbpb.JobStatus_JOB_STATUS_TIMED_OUT)
			return dbpb.JobStatus_JOB_STATUS_TIMED_OUT, nil
		}
		a.logger.Error("job failed", "job", a.jobID, "err", err.Error())
		a.report(ctx, dbpb.JobStatus_JOB_STATUS_FAILED)
		return dbpb.JobStatus_JOB_STATUS_FAILED, nil
	}
	a.logger.Info("job succeeded", "job", a.jobID)
	a.report(ctx, dbpb.JobStatus_JOB_STATUS_SUCCEEDED)
	return dbpb.JobStatus_JOB_STATUS_SUCCEEDED, nil
}

// runJob executes the job's execution spec: the steps run in order and the
// job fails on the first step that errors. A job with no spec (or an empty
// spec) succeeds without doing any work.
//
// A log sink is opened for the job so each step's stdout/stderr is streamed
// to the API in near-real-time (F-02); the API persists it and fans it out to
// the UI. Streaming is best-effort and resilient: if the API goes away mid-job
// the sink reopens its stream and resumes, and if it cannot be opened at all
// the job still runs (its output is only logged locally).
func (a *Agent) runJob(ctx context.Context, job *apipb.Job) error {
	sink, err := logstream.NewSink(a.deps.API, ctx, a.jobID, a.token, a.logger)
	if err != nil {
		a.logger.Warn("agent: open log stream; continuing without streaming", "job", a.jobID, "err", err)
	}
	defer sink.Close()
	return executor.Execute(executor.ContextWithLogSink(ctx, sink), job.GetSpec(), a.logger)
}

func (a *Agent) report(ctx context.Context, status dbpb.JobStatus) {
	if _, err := a.deps.API.ReportJobStatus(grpcutil.WithBearerToken(ctx, a.token), &apipb.ReportJobStatusRequest{
		JobId:      a.jobID,
		Status:     status,
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		a.logger.Error("agent: report status", "job", a.jobID, "status", status, "err", err)
	}
}
