package target

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/approval"
	"cdrom/internal/artifacttransfer"
	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logstream"
	"cdrom/internal/metrics"
	"cdrom/internal/notify"
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
	// Metrics, when set, is the target's step/job metric set (F-20). RunJob
	// derives from it the executor's step observer (each step's type and
	// duration) and wraps the job's log sink so the target counts the log
	// bytes it streams to the API. Nil when the target records no metrics.
	Metrics *metrics.WorkerMetrics
	Logger  *slog.Logger
	// WorkerRoot is the base directory the job's staged working area (F-21.6)
	// is created under; empty means the OS temp dir. Supplied by the worker /
	// agent from their config (worker_root / CDROM_WORKER_ROOT).
	WorkerRoot string
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
	// Wrap the sink so the target can count the log bytes it streams to the
	// API (F-20); a nil sink (streaming disabled) is a no-op.
	counting := &countingSink{sink: sink, m: tc.Metrics}
	defer counting.Close()
	ctx = executor.ContextWithLogSink(ctx, counting)
	ctx = executor.ContextWithStepStatusReporter(ctx, collector)
	// The target's step observer (F-20) records each step's type and duration
	// in the target's metrics; nil when the target records no step metrics.
	ctx = executor.ContextWithStepObserver(ctx, stepObserverAdapter(tc.Metrics))
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
		// The run's resolved-artifact snapshot (F-21.6): reference name →
		// stored file name, denormalized from the run onto the job by the
		// API. Lets the download_artifact handler resolve a spec.artifacts
		// reference to the version the run picked.
		Artifacts: job.GetArtifacts(),
	})
	// A step handler (the built-in "download_artifact" and "publish_artifact"
	// handlers, F-21.6) moves files to/from the artifacts store THROUGH THE
	// API (the topology rule), presenting the job's own token.
	ctx = executor.ContextWithArtifactTransfer(ctx, artifacttransfer.New(tc.API, job.GetId(), tc.Token))
	// A step handler (the built-in "token_exchange" handler) can request a new
	// job token for a different audience (e.g. an outside resource the job
	// needs to call) while the job runs; the exchanger calls the API's
	// ExchangeJobToken RPC, presenting the job's own token.
	ctx = executor.ContextWithTokenExchange(ctx, tokenexchange.New(tc.API, job.GetId(), tc.Token))
	// A step handler (the built-in "approval" handler, F-13) can pause the job
	// at an approval gate: it reports the job as awaiting approval (with a
	// message shown to the user) and polls the API for the gate's decision
	// (approved or rejected), presenting the job's own token.
	ctx = executor.ContextWithApproval(ctx, approval.New(tc.API, job.GetId(), tc.Token))
	// A step handler (the built-in "email" and "webhook" handlers, F-18) sends
	// its notification from the API server: the target renders the step's
	// fields against the step's condition context and asks the API to send,
	// presenting the job's own token.
	ctx = executor.ContextWithNotifier(ctx, notify.NewNotifier(tc.API, job.GetId(), tc.Token))
	// A step's "once" field (F-01) runs the step on at most one target of a
	// fan-out job: the target claims a durable slot (job id + step name +
	// attempt) through the API before running it, and a target that does not
	// claim it skips the step. A retry (a new attempt) may run it again.
	ctx = executor.ContextWithOnceClaimer(ctx, notify.NewClaimer(tc.API, job.GetId(), tc.Token, job.GetAttempt()))
	// The cross-worker step barrier: when the job has the step-barrier flag set
	// and targets a worker group, the workers synchronize at each step boundary
	// (after a worker completes a step it waits until every worker alive at the
	// step's start has completed it). A job on a single target (an ephemeral
	// agent) runs its steps unbarriered.
	if job.GetStepBarrier() && job.GetTargetGroup() != "" && tc.StepBarrier != nil {
		ctx = executor.ContextWithStepBarrier(ctx, tc.StepBarrier)
	}
	// The job's staged working area (F-21.6): lazily created on first use,
	// exported to every step as CDROM_STAGE_DIR, removed best-effort once the
	// job finishes. Rooted at the target's WORKER ROOT (config worker_root /
	// CDROM_WORKER_ROOT; empty = the OS temp dir) so operators can steer staged
	// files AWAY from RAM-backed mounts.
	sa := newStageArea(tc.WorkerRoot, logger)
	ctx = executor.ContextWithStageArea(ctx, &executor.StageArea{Func: sa.Dir, Clean: sa.Clean})
	defer sa.Clean()
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

// stepObserverAdapter adapts a target's step metrics (F-20) to the executor's
// StepObserver seam: it records each step's type and status in the target's
// step counters and its duration in the target's step-duration histogram. A
// nil metrics set (metrics disabled) yields a nil observer, so the executor
// records nothing.
func stepObserverAdapter(m *metrics.WorkerMetrics) executor.StepObserver {
	if m == nil {
		return nil
	}
	return &stepObserver{m: m}
}

type stepObserver struct {
	m *metrics.WorkerMetrics
}

// StepStarted implements executor.StepObserver.
func (a *stepObserver) StepStarted(stepType string, stepName string) {}

// StepFinished implements executor.StepObserver.
func (a *stepObserver) StepFinished(stepType string, status executor.StepStatus, d time.Duration) {
	a.m.RecordStep(stepType, stepStatusName(status), d)
}

// stepStatusName maps a step's terminal status to its metric label value.
func stepStatusName(status executor.StepStatus) string {
	switch status {
	case executor.StepStatusSucceeded:
		return "succeeded"
	case executor.StepStatusFailed:
		return "failed"
	case executor.StepStatusSkipped:
		return "skipped"
	case executor.StepStatusTimedOut:
		return "timed_out"
	default:
		return "unknown"
	}
}

// JobStatusName maps a job's terminal status to its metric label value (the
// same convention the API uses for cdrom_api_job_status_total, so a job's
// status is labeled identically on both sides). The worker and agent use it
// to label their cdrom_worker_jobs_total / job-duration metrics.
func JobStatusName(status dbpb.JobStatus) string {
	switch status {
	case dbpb.JobStatus_JOB_STATUS_SUCCEEDED:
		return "succeeded"
	case dbpb.JobStatus_JOB_STATUS_FAILED:
		return "failed"
	case dbpb.JobStatus_JOB_STATUS_CANCELLED:
		return "cancelled"
	case dbpb.JobStatus_JOB_STATUS_TIMED_OUT:
		return "timed_out"
	case dbpb.JobStatus_JOB_STATUS_SKIPPED:
		return "skipped"
	default:
		return "unknown"
	}
}

// countingSink wraps a logstream.Sink and counts the bytes it streams to the
// API (F-20, cdrom_worker_log_bytes_total). It is the value a target puts in
// the executor's context as the job's log sink. When the underlying sink is
// nil (streaming disabled) it is a no-op.
type countingSink struct {
	sink *logstream.Sink
	m    *metrics.WorkerMetrics
}

// WriteStepOutput implements executor.LogSink.
func (s *countingSink) WriteStepOutput(stepName string, stream string, data []byte) {
	if s.sink != nil {
		s.sink.WriteStepOutput(stepName, stream, data)
	}
	if s.m != nil {
		s.m.AddLogBytes(int64(len(data)))
	}
}

// Close ends the underlying sink.
func (s *countingSink) Close() {
	if s.sink != nil {
		s.sink.Close()
	}
}
