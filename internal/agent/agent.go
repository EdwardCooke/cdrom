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
	"fmt"
	"log/slog"
	"time"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/metrics"
	"cdrom/internal/target"

	// Register the built-in step handlers (e.g. the shell handler) with the
	// executor. A target or plugin adds more step types the same way.
	_ "cdrom/internal/stephandlers"
)

// agentCancelPollInterval is how often an ephemeral agent polls the API for
// its job's status to observe a cancellation (F-05). An agent holds no
// WatchJobs stream, so it cannot be signalled directly; it observes the
// cancellation on the API instead. It is a variable (not a constant) so tests
// can shorten the interval.
var agentCancelPollInterval = 2 * time.Second

// Dependencies are the gRPC clients an agent needs. The agent talks only to
// the API service.
type Dependencies struct {
	API apipb.APIClient
}

// Agent is an ephemeral job executor.
type Agent struct {
	name   string // the agent's identity (its host name); reported with the job's status
	jobID  int64
	deps   Dependencies
	logger *slog.Logger
	token  string // job token handed over by the API via GetJob
	// metrics is the agent's metric set (F-20); nil when metrics are
	// disabled, in which case all recording is a no-op. An agent records only
	// the job/step/in-flight/log metrics (it holds no WatchJobs stream and
	// polls no queue).
	metrics *metrics.WorkerMetrics
}

// New creates an agent that will execute jobID. name is the agent's identity
// (its host name); it is reported with the job's status so the API can record
// which target ran the job.
func New(name string, jobID int64, deps Dependencies, logger *slog.Logger) *Agent {
	if logger == nil {
		logger = slog.Default()
	}
	return &Agent{name: name, jobID: jobID, deps: deps, logger: logger}
}

// SetMetrics sets the agent's metric set (F-20). It must be called before
// Run. A nil set disables all metric recording.
func (a *Agent) SetMetrics(m *metrics.WorkerMetrics) {
	a.metrics = m
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

	// The shared execution context: the target's API client, the agent's name
	// (its host name, so the API can record which target ran the job), the
	// job's token, and the agent's step metrics (F-20). An ephemeral agent has
	// no step barrier (it runs its steps unbarriered).
	tc := &target.Context{
		API:        a.deps.API,
		WorkerName: a.name,
		Token:      a.token,
		Metrics:    a.metrics,
		Logger:     a.logger,
	}

	// The agent runs exactly one job, so it is in flight for the whole run.
	a.metrics.SetInflight(1)
	defer a.metrics.SetInflight(0)

	target.ReportRunning(ctx, tc, a.jobID)
	a.logger.Info("job started", "job", a.jobID)
	// The job runs under a cancellable context (F-05): a background poller
	// watches the job's status and cancels the context if the job is
	// cancelled, which interrupts the running step.
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go a.watchCancellation(jobCtx, cancel)

	collector := &executor.StepResultCollector{}
	start := time.Now()
	err = target.RunJob(jobCtx, tc, job, collector)
	// A cancellation observed by the agent's status poller (F-05) — or the
	// agent's own shutdown — cancelled the job's context; FinishJob reports
	// the job as cancelled, distinct from a failure.
	finalStatus := target.FinishJob(ctx, tc, a.jobID, collector, err, jobCtx.Err() == context.Canceled)
	// Record the job's terminal status and duration (F-20), matching the
	// status the API records for the same job.
	a.metrics.RecordJob(target.JobStatusName(finalStatus), time.Since(start))
	return finalStatus, nil
}

// watchCancellation polls the job's status (F-05) and cancels the job's
// context if the job is cancelled. An ephemeral agent holds no WatchJobs
// stream, so it observes the cancellation on the API: the scheduler's
// CancelJob marks the job cancelled in the database, and the agent's next
// poll sees it and interrupts the running step (by cancelling the job's
// context). Polling stops when the job's context is done (the job finished or
// was cancelled).
func (a *Agent) watchCancellation(ctx context.Context, cancel context.CancelFunc) {
	ticker := time.NewTicker(agentCancelPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			job, err := a.deps.API.GetJob(ctx, &apipb.GetJobRequest{Id: a.jobID})
			if err != nil {
				// A transient failure to fetch the job is not a cancellation;
				// keep polling.
				continue
			}
			if job.GetStatus() == dbpb.JobStatus_JOB_STATUS_CANCELLED {
				a.logger.Info("agent: job cancelled", "job", a.jobID)
				cancel()
				return
			}
		}
	}
}
