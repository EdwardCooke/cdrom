// Package worker implements the long-lived worker: a resident process on a
// deployment target, targetable by group. Must run on both Windows and Linux.
//
// A worker talks ONLY to the API service. It registers with the API, holds a
// WatchJobs stream open, and executes the jobs the API dispatches to its
// group. Job status is reported back to the API, and all logging is local
// (stdout or a file) — there is no central logs service.
package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

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

// Dependencies are the gRPC clients a worker needs. The worker talks only to
// the API service.
type Dependencies struct {
	API apipb.APIClient
}

// Worker is a long-lived worker process.
type Worker struct {
	name   string
	group  string
	deps   Dependencies
	logger *slog.Logger
}

// New creates a worker with the given identity and service dependencies.
func New(name, group string, deps Dependencies, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{name: name, group: group, deps: deps, logger: logger}
}

// Run registers the worker, then serves jobs until ctx is cancelled. It
// returns nil on a clean shutdown.
func (w *Worker) Run(ctx context.Context) error {
	if _, err := w.deps.API.RegisterWorker(ctx, &apipb.RegisterWorkerRequest{
		Name:    w.name,
		Group:   w.group,
		Address: w.name,
	}); err != nil {
		return fmt.Errorf("worker: register: %w", err)
	}
	w.logger.Info("worker: registered", "name", w.name, "group", w.group)

	defer func() {
		if _, err := w.deps.API.DeregisterWorker(context.WithoutCancel(ctx), &apipb.DeregisterWorkerRequest{Name: w.name}); err != nil {
			w.logger.Warn("worker: deregister", "err", err)
		}
	}()

	// Heartbeat in the background.
	go w.heartbeat(ctx)

	// Watch for job assignments, reconnecting on transient failures.
	for {
		if err := w.watch(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Warn("worker: watch stream ended; reconnecting", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
		}
	}
}

// watch opens a WatchJobs stream and processes assignments until the stream
// closes or ctx is cancelled.
func (w *Worker) watch(ctx context.Context) error {
	stream, err := w.deps.API.WatchJobs(ctx, &apipb.WatchJobsRequest{
		WorkerName: w.name,
		Group:      w.group,
	})
	if err != nil {
		return fmt.Errorf("worker: watch: %w", err)
	}
	for {
		assignment, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("worker: recv: %w", err)
		}
		w.execute(ctx, assignment.GetJob(), assignment.GetToken())
	}
}

// heartbeat refreshes the worker's liveness on an interval.
func (w *Worker) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := w.deps.API.Heartbeat(ctx, &apipb.HeartbeatRequest{Name: w.name}); err != nil {
				w.logger.Warn("worker: heartbeat", "err", err)
			}
		}
	}
}

// execute runs a single job assignment: mark running, execute, and report the
// final status. All job logging is local. token is the job token the API
// handed over with the assignment; it is presented on the status reports so
// the API (and any outside resources) can authenticate the worker for this
// job.
func (w *Worker) execute(ctx context.Context, job *apipb.Job, token string) {
	jobID := fmt.Sprintf("%d", job.GetId())
	w.logger.Info("worker: executing job", "job", job.GetId(), "name", job.GetName())

	if _, err := w.deps.API.ReportJobStatus(grpcutil.WithBearerToken(ctx, token), &apipb.ReportJobStatusRequest{
		JobId:     job.GetId(),
		Status:    dbpb.JobStatus_JOB_STATUS_RUNNING,
		StartedAt: timestamppb.Now(),
	}); err != nil {
		w.logger.Error("worker: report running", "job", job.GetId(), "err", err)
	}

	w.logger.Info("job started", "job", jobID)
	err := w.runJob(ctx, job, token)
	if err != nil {
		w.logger.Error("job failed", "job", jobID, "err", err.Error())
		w.report(ctx, job.GetId(), token, dbpb.JobStatus_JOB_STATUS_FAILED)
		return
	}
	w.logger.Info("job succeeded", "job", jobID)
	w.report(ctx, job.GetId(), token, dbpb.JobStatus_JOB_STATUS_SUCCEEDED)
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
func (w *Worker) runJob(ctx context.Context, job *apipb.Job, token string) error {
	sink, err := logstream.NewSink(w.deps.API, ctx, job.GetId(), token, w.logger)
	if err != nil {
		w.logger.Warn("worker: open log stream; continuing without streaming", "job", job.GetId(), "err", err)
	}
	defer sink.Close()
	return executor.Execute(executor.ContextWithLogSink(ctx, sink), job.GetSpec(), w.logger)
}

// report sets the finished timestamp and final status, presenting the job
// token.
func (w *Worker) report(ctx context.Context, jobID int64, token string, status dbpb.JobStatus) {
	if _, err := w.deps.API.ReportJobStatus(grpcutil.WithBearerToken(ctx, token), &apipb.ReportJobStatusRequest{
		JobId:      jobID,
		Status:     status,
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		w.logger.Error("worker: report status", "job", jobID, "status", status, "err", err)
	}
}
