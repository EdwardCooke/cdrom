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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
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

	// sem bounds the worker to one job at a time (preserving the previous
	// sequential behavior) while the watch loop stays free to receive
	// cancellations for the running job (F-05).
	sem chan struct{}

	mu       sync.Mutex
	inflight map[int64]*jobRun // jobID -> run state for a running job
}

// New creates a worker with the given identity and service dependencies.
func New(name, group string, deps Dependencies, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		name:     name,
		group:    group,
		deps:     deps,
		logger:   logger,
		sem:      make(chan struct{}, 1),
		inflight: make(map[int64]*jobRun),
	}
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

// watch opens a WatchJobs stream and processes messages until the stream
// closes or ctx is cancelled. Each message is either an assignment (a new job
// to run) or a cancellation (stop a job the worker is running, F-05).
//
// Assignments are run in a goroutine (bounded by the worker's semaphore to
// one job at a time) so the watch loop stays free to receive a cancellation
// for the running job. A cancellation delivered while a job runs interrupts
// that job's step (see cancelJob); one delivered for a job that is not
// running on this worker is ignored (the job's status is already reconciled
// by the database).
func (w *Worker) watch(ctx context.Context) error {
	stream, err := w.deps.API.WatchJobs(ctx, &apipb.WatchJobsRequest{
		WorkerName: w.name,
		Group:      w.group,
	})
	if err != nil {
		return fmt.Errorf("worker: watch: %w", err)
	}
	for {
		message, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("worker: recv: %w", err)
		}
		switch m := message.GetMessage().(type) {
		case *apipb.WatchMessage_Assignment:
			assignment := m.Assignment
			// Run the job in a goroutine so the watch loop can keep receiving
			// cancellations for it (F-05). The semaphore bounds the worker to
			// one job at a time, preserving the previous sequential behavior.
			w.sem <- struct{}{}
			go func() {
				defer func() { <-w.sem }()
				w.execute(ctx, assignment.GetJob(), assignment.GetToken())
			}()
		case *apipb.WatchMessage_Cancellation:
			w.cancelJob(m.Cancellation.GetJobId())
		}
	}
}

// cancelJob interrupts the job the worker is running (F-05): it marks the
// job's run as user-cancelled and cancels its context, which terminates the
// running step. A cancellation for a job that is not running on this worker
// (already finished, or running on another target) is a no-op — the job's
// status is reconciled by the database.
func (w *Worker) cancelJob(jobID int64) {
	w.mu.Lock()
	run, running := w.inflight[jobID]
	if running {
		run.userCancel = true
	}
	w.mu.Unlock()
	if !running {
		w.logger.Info("worker: cancel for job not running on this worker", "job", jobID)
		return
	}
	w.logger.Info("worker: cancelling job", "job", jobID)
	run.cancel()
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

// jobRun is the per-job execution state the worker tracks so a cancellation
// (F-05) can interrupt a running job: cancel terminates the job's context
// (killing the running step), and userCancel records that the interruption
// was a user cancellation (as opposed to a timeout or a worker shutdown).
type jobRun struct {
	cancel     context.CancelFunc
	userCancel bool
}

// execute runs a single job assignment: mark running, execute, and report the
// final status. All job logging is local. token is the job token the API
// handed over with the assignment; it is presented on the status reports so
// the API (and any outside resources) can authenticate the worker for this
// job.
//
// The job runs under a cancellable context registered in the worker's
// in-flight set (F-05): a cancellation delivered on the WatchJobs stream
// cancels this context, which terminates the running step, and the job is
// reported as cancelled.
func (w *Worker) execute(ctx context.Context, job *apipb.Job, token string) {
	jobID := fmt.Sprintf("%d", job.GetId())
	w.logger.Info("worker: executing job", "job", job.GetId(), "name", job.GetName())

	// A cancellable context bounds the job's execution; a cancellation (F-05)
	// cancels it to interrupt the running step. It is also cancelled on
	// worker shutdown (via the parent ctx).
	jobCtx, cancel := context.WithCancel(ctx)
	run := &jobRun{cancel: cancel}
	w.mu.Lock()
	w.inflight[job.GetId()] = run
	w.mu.Unlock()
	defer func() {
		cancel()
		w.mu.Lock()
		delete(w.inflight, job.GetId())
		w.mu.Unlock()
	}()

	if _, err := w.deps.API.ReportJobStatus(grpcutil.WithBearerToken(ctx, token), &apipb.ReportJobStatusRequest{
		JobId:     job.GetId(),
		Status:    dbpb.JobStatus_JOB_STATUS_RUNNING,
		StartedAt: timestamppb.Now(),
	}); err != nil {
		w.logger.Error("worker: report running", "job", job.GetId(), "err", err)
	}

	w.logger.Info("job started", "job", jobID)
	collector := &executor.StepResultCollector{}
	err := w.runJob(jobCtx, job, token, collector)
	w.mu.Lock()
	userCancelled := run.userCancel
	w.mu.Unlock()
	if err != nil {
		// A user cancellation (F-05) interrupts the running step and is
		// reported as cancelled, distinct from a failure.
		if userCancelled {
			w.logger.Info("job cancelled", "job", jobID)
			w.report(ctx, job.GetId(), token, dbpb.JobStatus_JOB_STATUS_CANCELLED, collector)
			return
		}
		// A timeout (the job-level or a step's per-step timeout, F-03) is
		// reported as timed_out rather than failed, so the UI can tell a hung
		// job apart from one that ran and errored.
		if errors.Is(err, executor.ErrTimeout) {
			w.logger.Error("job timed out", "job", jobID, "err", err.Error())
			w.report(ctx, job.GetId(), token, dbpb.JobStatus_JOB_STATUS_TIMED_OUT, collector)
			return
		}
		w.logger.Error("job failed", "job", jobID, "err", err.Error())
		w.report(ctx, job.GetId(), token, dbpb.JobStatus_JOB_STATUS_FAILED, collector)
		return
	}
	w.logger.Info("job succeeded", "job", jobID)
	w.report(ctx, job.GetId(), token, dbpb.JobStatus_JOB_STATUS_SUCCEEDED, collector)
}

// runJob executes the job's execution spec: the steps run in order and the
// job fails on the first step that errors. A job with no spec (or an empty
// spec) succeeds without doing any work.
//
// A log sink is opened for the job so each step's stdout/stderr is streamed
// to the API in near-real-time (F-02); the API persists it and fans it out to
// the UI. Streaming is best-effort and resilient: if the API goes away mid-job
// the sink reopens its stream and resumes, and if it cannot be opened at all
// the job still runs (its output is only logged locally). collector gathers
// each step's terminal status (F-06) for report to attach to the final
// status report.
func (w *Worker) runJob(ctx context.Context, job *apipb.Job, token string, collector *executor.StepResultCollector) error {
	sink, err := logstream.NewSink(w.deps.API, ctx, job.GetId(), token, w.logger)
	if err != nil {
		w.logger.Warn("worker: open log stream; continuing without streaming", "job", job.GetId(), "err", err)
	}
	defer sink.Close()
	ctx = executor.ContextWithLogSink(ctx, sink)
	ctx = executor.ContextWithStepStatusReporter(ctx, collector)
	// A step's condition (F-06) can reference the status/outputs of the jobs
	// this job depends on (carried by the API) and the job's own identity.
	ctx = executor.ContextWithUpstreamJobs(ctx, job.GetUpstreamJobs())
	ctx = executor.ContextWithJobIdentity(ctx, executor.JobIdentity{
		ID:     job.GetId(),
		Name:   job.GetName(),
		Status: "running",
	})
	return executor.Execute(ctx, job.GetSpec(), w.logger)
}

// report sets the finished timestamp and final status, presenting the job
// token, and attaches the job's collected per-step outcomes (F-06).
func (w *Worker) report(ctx context.Context, jobID int64, token string, status dbpb.JobStatus, collector *executor.StepResultCollector) {
	if _, err := w.deps.API.ReportJobStatus(grpcutil.WithBearerToken(ctx, token), &apipb.ReportJobStatusRequest{
		JobId:       jobID,
		Status:      status,
		FinishedAt:  timestamppb.Now(),
		StepResults: collector.ToProto(),
		Outputs:     collector.JobOutputs(),
	}); err != nil {
		w.logger.Error("worker: report status", "job", jobID, "status", status, "err", err)
	}
}
