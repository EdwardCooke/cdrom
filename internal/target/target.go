package target

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logstream"
	"cdrom/internal/tokenexchange"
)

// Context carries what a shared execution needs from a target: the API client
// (the target talks only to the API), the job's token (presented on the API
// calls the shared execution makes), the target's name (the worker's name, so
// a fan-out job's outcome is recorded against the worker's execution; empty
// for an ephemeral agent, which reports straight to the job row), and the
// logger. StepBarrier is the target's cross-worker step barrier (the worker's
// implementation, when the job has the step-barrier flag set and targets a
// worker group); it is nil for an agent and for a job that runs unbarriered.
type Context struct {
	API         apipb.APIClient
	WorkerName  string
	Token       string
	StepBarrier executor.StepBarrier
	Logger      *slog.Logger
}

// RunJob executes job's spec through the shared executor, building the
// executor's run context from the target's context: a log sink that streams
// each step's output to the API in near-real-time (F-02), a step-status
// reporter that gathers each step's terminal outcome (F-06), the upstream
// jobs and the job's identity (for a step's condition, F-06), the run's
// parameters and identity (for spec interpolation, F-10), and a token
// exchanger (so a step handler can request a job token for a different
// audience). When the job has the step-barrier flag set and targets a worker
// group, the target's StepBarrier is set in the context so the executor
// synchronizes the job's workers at each step boundary.
//
// The provided ctx bounds the job (the target's cancellable job context, so a
// cancellation or a target shutdown interrupts the running step). Streaming
// is best-effort: if the log sink cannot be opened the job still runs (its
// output is only logged locally). collector gathers each step's terminal
// status (F-06) for the target to attach to its final status report.
func RunJob(ctx context.Context, tc *Context, job *apipb.Job, collector *executor.StepResultCollector) error {
	logger := tc.Logger
	if logger == nil {
		logger = slog.Default()
	}
	sink, err := logstream.NewSink(tc.API, ctx, job.GetId(), tc.Token, logger)
	if err != nil {
		logger.Warn("target: open log stream; continuing without streaming", "job", job.GetId(), "err", err)
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
	// The run's parameters (F-10) and the pipeline's secrets as plaintext
	// (F-12) are interpolated into the job's spec (env, command, workdir) and
	// are available to a step's condition. They are denormalized from the run
	// onto the job by the API (the API decrypts the job's stored secret
	// ciphertexts into the plaintext map carried on the job).
	ctx = executor.ContextWithRunInfo(ctx, executor.RunInfo{
		Params:      job.GetRunParams(),
		Secrets:     job.GetSecrets(),
		ID:          job.GetRunId(),
		PipelineID:  job.GetPipelineId(),
		Trigger:     job.GetTriggerType(),
		TriggerName: job.GetTriggerName(),
	})
	// A step handler (the built-in "token_exchange" handler) can request a new
	// job token for a different audience (e.g. an outside resource the job
	// needs to call) while the job runs; the exchanger calls the API's
	// ExchangeJobToken RPC, presenting the job's own token.
	ctx = executor.ContextWithTokenExchange(ctx, tokenexchange.New(tc.API, job.GetId(), tc.Token))
	// The cross-worker step barrier: when the job has the step-barrier flag set
	// and targets a worker group, the workers synchronize at each step boundary
	// (after a worker completes a step it waits until every worker alive at the
	// step's start has completed it). A job on a single target (an ephemeral
	// agent) runs its steps unbarriered.
	if job.GetStepBarrier() && job.GetTargetGroup() != "" && tc.StepBarrier != nil {
		ctx = executor.ContextWithStepBarrier(ctx, tc.StepBarrier)
	}
	return executor.Execute(ctx, job.GetSpec(), logger)
}

// ReportRunning reports that the target has started the job, presenting the
// job's token and stamping the start time. The target's name is included so
// the API records the start against the target's execution (fan-out).
func ReportRunning(ctx context.Context, tc *Context, jobID int64) {
	if _, err := tc.API.ReportJobStatus(grpcutil.WithBearerToken(ctx, tc.Token), &apipb.ReportJobStatusRequest{
		JobId:      jobID,
		Status:     dbpb.JobStatus_JOB_STATUS_RUNNING,
		StartedAt:  timestamppb.Now(),
		WorkerName: tc.WorkerName,
	}); err != nil {
		tc.Logger.Error("target: report running", "job", jobID, "err", err)
	}
}

// ReportStatus reports a job's final status to the API, presenting the job's
// token, and attaches the job's collected per-step outcomes (F-06) and
// outputs. The target's name is included so the API records the outcome
// against the target's execution (fan-out); the job's overall status is
// derived from the executions by the scheduler.
func ReportStatus(ctx context.Context, tc *Context, jobID int64, status dbpb.JobStatus, collector *executor.StepResultCollector) {
	if _, err := tc.API.ReportJobStatus(grpcutil.WithBearerToken(ctx, tc.Token), &apipb.ReportJobStatusRequest{
		JobId:       jobID,
		Status:      status,
		FinishedAt:  timestamppb.Now(),
		StepResults: collector.ToProto(),
		Outputs:     collector.JobOutputs(),
		WorkerName:  tc.WorkerName,
	}); err != nil {
		tc.Logger.Error("target: report status", "job", jobID, "status", status, "err", err)
	}
}

// FinishJob reports the job's final status to the API based on the outcome of
// RunJob and returns it. cancelled reports whether the target observed a user
// cancellation of the job while it ran (the worker via a cancellation on its
// WatchJobs stream, the agent via its status poller, F-05); it is checked
// before the error's kind, so a cancellation that interrupted the running
// step is reported as cancelled rather than failed or timed out. A
// cancellation observed at a step barrier (executor.ErrCancelled) is likewise
// reported as cancelled. A timeout (the job-level or a step's per-step
// timeout, F-03) is reported as timed_out rather than failed, so the UI can
// tell a hung job apart from one that ran and errored. A nil error is
// reported as succeeded.
func FinishJob(ctx context.Context, tc *Context, jobID int64, collector *executor.StepResultCollector, err error, cancelled bool) dbpb.JobStatus {
	if err != nil {
		if cancelled {
			tc.Logger.Info("job cancelled", "job", jobID)
			ReportStatus(ctx, tc, jobID, dbpb.JobStatus_JOB_STATUS_CANCELLED, collector)
			return dbpb.JobStatus_JOB_STATUS_CANCELLED
		}
		// A cancellation observed at a step barrier (the job was cancelled
		// while a worker waited for its peers to finish a step) is also
		// reported as cancelled, distinct from a failure.
		if errors.Is(err, executor.ErrCancelled) {
			tc.Logger.Info("job cancelled (step barrier)", "job", jobID)
			ReportStatus(ctx, tc, jobID, dbpb.JobStatus_JOB_STATUS_CANCELLED, collector)
			return dbpb.JobStatus_JOB_STATUS_CANCELLED
		}
		// A timeout (the job-level or a step's per-step timeout, F-03) is
		// reported as timed_out rather than failed, so the UI can tell a hung
		// job apart from one that ran and errored.
		if errors.Is(err, executor.ErrTimeout) {
			tc.Logger.Error("job timed out", "job", jobID, "err", err.Error())
			ReportStatus(ctx, tc, jobID, dbpb.JobStatus_JOB_STATUS_TIMED_OUT, collector)
			return dbpb.JobStatus_JOB_STATUS_TIMED_OUT
		}
		tc.Logger.Error("job failed", "job", jobID, "err", err.Error())
		ReportStatus(ctx, tc, jobID, dbpb.JobStatus_JOB_STATUS_FAILED, collector)
		return dbpb.JobStatus_JOB_STATUS_FAILED
	}
	tc.Logger.Info("job succeeded", "job", jobID)
	ReportStatus(ctx, tc, jobID, dbpb.JobStatus_JOB_STATUS_SUCCEEDED, collector)
	return dbpb.JobStatus_JOB_STATUS_SUCCEEDED
}
