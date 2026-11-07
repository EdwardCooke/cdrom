// Package agent implements the long-lived agent: a resident process on a
// deployment target, targetable by group. Must run on both Windows and Linux.
//
// An agent talks ONLY to the API service. It registers with the API, holds a
// WatchJobs stream open, and executes the jobs the API dispatches to its
// group. Job status is reported back to the API, and all logging is local
// (stdout or a file) — there is no central logs service.
package agent

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/metrics"
	"cdrom/internal/target"

	// Register the built-in step handlers (e.g. the shell handler) with the
	// executor. A target or plugin adds more step types the same way.
	_ "cdrom/internal/stephandlers"
)

// Dependencies are the gRPC clients an agent needs. The agent talks only to
// the API service.
type Dependencies struct {
	API apipb.APIClient
	// AgentRoot is the base directory the agent's staged working area
	// (F-21.6) is created under; empty means the OS temp dir. Carried from
	// the agent's config (agent_root / CDROM_AGENT_ROOT).
	AgentRoot string
}

// Agent is a long-lived agent process.
type Agent struct {
	name  string
	group string
	// environments are the environments the agent belongs to (F-17); a job
	// that targets one of them is dispatched to this agent.
	environments []string
	// labels are the agent's labels (F-29); a job dispatched to the agent merges
	// them into the job's execution's label set.
	labels   map[string]string
	deps   Dependencies
	logger *slog.Logger
	// metrics is the agent's metric set (F-20); nil when metrics are
	// disabled, in which case all recording is a no-op.
	metrics *metrics.AgentMetrics

	// sem bounds the agent to one job at a time (preserving the previous
	// sequential behavior) while the watch loop stays free to receive
	// cancellations for the running job (F-05).
	//
	// LOAD BEARING (do not widen casually): the database service's
	// StartJobExecution duplicate-delivery guard (F-23/flake 2026-11-07) is a
	// check-then-act that is race-free ONLY because a single agent's
	// acquisitions are SERIAL — no second StartJobExecution for this agent
	// can be in flight while the first's execution row is uncommitted.
	// Growing this semaphore to N (internally parallel jobs) re-opens the
	// double-execution race; enforce the invariant at the storage layer
	// first (e.g. a partial unique index on (job, agent, attempt) WHERE
	// status='running') before scaling. Horizontal fan-out (MORE agents,
	// each serial) is unaffected and encouraged.
	sem chan struct{}

	mu       sync.Mutex
	inflight map[int64]*jobRun // jobID -> run state for a running job
}

// New creates an agent with the given identity and service dependencies.
// environments are the environments the agent belongs to (F-17); they are
// declared at registration and used to route environment jobs to this agent.
// labels are the agent's labels (F-29); they are declared at registration and
// merged into the label set of the jobs the agent runs.
func New(name, group string, environments []string, labels map[string]string, deps Dependencies, logger *slog.Logger) *Agent {
	if logger == nil {
		logger = slog.Default()
	}
	// Advise (warn) once at startup when the staged-working-area root is
	// RAM-backed (F-21.6): staged files would consume memory.
	target.WarnTmpfsAgentRoot(deps.AgentRoot, logger)
	return &Agent{
		name:         name,
		group:        group,
		environments: environments,
		labels:         labels,
		deps:         deps,
		logger:       logger,
		sem:          make(chan struct{}, 1),
		inflight:     make(map[int64]*jobRun),
	}
}

// SetMetrics sets the agent's metric set (F-20). It must be called before
// Run. A nil set disables all metric recording.
func (w *Agent) SetMetrics(m *metrics.AgentMetrics) {
	w.metrics = m
}

// Run registers the agent, then serves jobs until ctx is cancelled. It
// returns nil on a clean shutdown.
func (w *Agent) Run(ctx context.Context) error {
	if _, err := w.deps.API.RegisterAgent(ctx, &apipb.RegisterAgentRequest{
		Name:         w.name,
		Group:        w.group,
		Address:      w.name,
		Environments: w.environments,
		Labels:         w.labels,
	}); err != nil {
		return fmt.Errorf("agent: register: %w", err)
	}
	w.logger.Info("agent: registered", "name", w.name, "group", w.group, "environments", w.environments, "labels", w.labels)

	defer func() {
		if _, err := w.deps.API.DeregisterAgent(context.WithoutCancel(ctx), &apipb.DeregisterAgentRequest{Name: w.name}); err != nil {
			w.logger.Warn("agent: deregister", "err", err)
		}
	}()

	// Heartbeat in the background.
	go w.heartbeat(ctx)
	// Poll for pending jobs in the background (F-23): the authoritative
	// catch-up path that picks up jobs published while the agent was
	// unreachable or whose push nudge was dropped.
	go w.poll(ctx)

	// Watch for job assignments, reconnecting on transient failures.
	for {
		if err := w.watch(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.Warn("agent: watch stream ended; reconnecting", "err", err)
			w.metrics.SetWatchState("reconnecting")
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
// to run) or a cancellation (stop a job the agent is running, F-05).
//
// Assignments are run in a goroutine (bounded by the agent's semaphore to
// one job at a time) so the watch loop stays free to receive a cancellation
// for the running job. A cancellation delivered while a job runs interrupts
// that job's step (see cancelJob); one delivered for a job that is not
// running on this agent is ignored (the job's status is already reconciled
// by the database).
func (w *Agent) watch(ctx context.Context) error {
	stream, err := w.deps.API.WatchJobs(ctx, &apipb.WatchJobsRequest{
		AgentName:   w.name,
		Group:        w.group,
		Environments: w.environments,
	})
	if err != nil {
		return fmt.Errorf("agent: watch: %w", err)
	}
	w.metrics.SetWatchState("connected")
	for {
		message, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("agent: recv: %w", err)
		}
		switch m := message.GetMessage().(type) {
		case *apipb.WatchMessage_Assignment:
			// A full assignment (legacy push). The agent starts its own
			// execution of the job (fan-out): a job that targets the agent's
			// group runs on every agent in the group, so the start is not
			// exclusive. The start returns the fresh spec + token.
			w.startAndExecute(ctx, m.Assignment.GetJob().GetId())
		case *apipb.WatchMessage_Nudge:
			// A nudge (F-23): the push carries only the job id. The agent
			// starts its own execution, so a job delivered via both push and
			// poll is run once by this agent (its execution is created once).
			w.startAndExecute(ctx, m.Nudge.GetJobId())
		case *apipb.WatchMessage_Cancellation:
			w.cancelJob(m.Cancellation.GetJobId())
		}
	}
}

// pollInterval is how often the agent polls the API for pending jobs in its
// group (F-23). It is the authoritative catch-up path: if a push nudge is
// dropped (queue full, pod restart, stream flap), the next poll picks the job
// up. It is a variable (not a constant) so tests can shorten it.
var pollInterval = 1 * time.Second

// poll runs the agent's pull loop (F-23): on each tick it asks the API for
// the pending jobs targeting its group and claims each one. It is the
// authoritative delivery path — a job published while the agent was
// unreachable (or whose push was dropped) is picked up here, so no job is
// stranded pending.
func (w *Agent) poll(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.pollOnce(ctx)
		}
	}
}

// pollOnce lists the pending jobs in the agent's group and environments and
// starts each one.
func (w *Agent) pollOnce(ctx context.Context) {
	resp, err := w.deps.API.ListPendingJobs(ctx, &apipb.ListPendingJobsRequest{
		Group:        w.group,
		Environments: w.environments,
	})
	if err != nil {
		w.logger.Warn("agent: list pending jobs", "err", err)
		return
	}
	if len(resp.GetJobs()) == 0 {
		w.metrics.RecordPoll("idle")
		return
	}
	w.metrics.RecordPoll("claimed")
	for _, job := range resp.GetJobs() {
		w.startAndExecute(ctx, job.GetId())
	}
}

// startAndExecute starts this agent's execution of a job (fan-out) and, if
// the job is still pending, executes it. It is the single entry point for job
// acquisition, used by both the push path (a nudge or full assignment on the
// WatchJobs stream) and the pull path (the periodic poll). A job that targets
// the agent's group runs on every agent in the group, so the start is not
// exclusive: this agent creates its own execution and runs the job. A job
// that is no longer pending (it reached a terminal state, or it is not
// targeted at the agent's group) is not started (started=false), so a job
// delivered to several agents — or via both push and poll — is run once by
// each of them, and a job that finished before a slow agent started it is not
// run.
//
// The job runs in a goroutine (bounded by the agent's semaphore to one job
// at a time) so the watch loop stays free to receive cancellations (F-05).
func (w *Agent) startAndExecute(ctx context.Context, jobID int64) {
	w.sem <- struct{}{}
	go func() {
		defer func() { <-w.sem }()
		w.doStartAndExecute(ctx, jobID)
	}()
}

// doStartAndExecute starts the agent's execution of the job and, on success,
// executes it (which carries its spec, upstream jobs, and a fresh job token).
func (w *Agent) doStartAndExecute(ctx context.Context, jobID int64) {
	resp, err := w.deps.API.StartJobExecution(ctx, &apipb.StartJobExecutionRequest{
		JobId:      jobID,
		AgentName: w.name,
	})
	if err != nil {
		w.logger.Warn("agent: start job execution", "job", jobID, "err", err)
		return
	}
	if !resp.GetStarted() {
		// The job is no longer pending (it reached a terminal state, or it is
		// not targeted at the agent's group).
		return
	}
	job := resp.GetJob()
	w.execute(ctx, job, job.GetToken())
}

// cancelJob interrupts the job the agent is running (F-05): it marks the
// job's run as user-cancelled and cancels its context, which terminates the
// running step. A cancellation for a job that is not running on this agent
// (already finished, or running on another target) is a no-op — the job's
// status is reconciled by the database.
func (w *Agent) cancelJob(jobID int64) {
	w.mu.Lock()
	run, running := w.inflight[jobID]
	if running {
		run.userCancel = true
	}
	w.mu.Unlock()
	if !running {
		w.logger.Info("agent: cancel for job not running on this agent", "job", jobID)
		return
	}
	w.logger.Info("agent: cancelling job", "job", jobID)
	run.cancel()
}

// heartbeat refreshes the agent's liveness on an interval.
func (w *Agent) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := w.deps.API.Heartbeat(ctx, &apipb.HeartbeatRequest{Name: w.name}); err != nil {
				w.logger.Warn("agent: heartbeat", "err", err)
				w.metrics.RecordHeartbeatFailure()
			}
		}
	}
}

// jobRun is the per-job execution state the agent tracks so a cancellation
// (F-05) can interrupt a running job: cancel terminates the job's context
// (killing the running step), and userCancel records that the interruption
// was a user cancellation (as opposed to a timeout or an agent shutdown).
type jobRun struct {
	cancel     context.CancelFunc
	userCancel bool
}

// execute runs a single job assignment: mark running, execute, and report the
// final status. All job logging is local. token is the job token the API
// handed over with the assignment; it is presented on the status reports so
// the API (and any outside resources) can authenticate the agent for this
// job.
//
// The job runs under a cancellable context registered in the agent's
// in-flight set (F-05): a cancellation delivered on the WatchJobs stream
// cancels this context, which terminates the running step, and the job is
// reported as cancelled.
func (w *Agent) execute(ctx context.Context, job *apipb.Job, token string) {
	w.logger.Info("agent: executing job", "job", job.GetId(), "name", job.GetName())

	// A cancellable context bounds the job's execution; a cancellation (F-05)
	// cancels it to interrupt the running step. It is also cancelled on
	// agent shutdown (via the parent ctx).
	jobCtx, cancel := context.WithCancel(ctx)
	run := &jobRun{cancel: cancel}
	w.mu.Lock()
	w.inflight[job.GetId()] = run
	w.metrics.SetInflight(len(w.inflight))
	w.mu.Unlock()
	defer func() {
		cancel()
		w.mu.Lock()
		delete(w.inflight, job.GetId())
		w.metrics.SetInflight(len(w.inflight))
		w.mu.Unlock()
	}()

	// The shared execution context: the target's API client, the agent's
	// name (so a fan-out job's outcome is recorded against this agent's
	// execution), the job's token, the agent's step metrics (F-20), and the
	// agent's step barrier (when the job has the step-barrier flag set and
	// targets an agent group).
	tc := &target.Context{
		API:        w.deps.API,
		AgentName: w.name,
		Token:      token,
		Metrics:    w.metrics,
		Logger:     w.logger,
		AgentRoot:   w.deps.AgentRoot,
	}
	if job.GetStepBarrier() && job.GetTargetGroup() != "" {
		tc.StepBarrier = newStepBarrier(w.deps.API, w.name, job.GetId(), token, w.logger, 0)
	}

	target.ReportRunning(ctx, tc, job.GetId())
	w.logger.Info("job started", "job", job.GetId())
	collector := &executor.StepResultCollector{}
	start := time.Now()
	err := target.RunJob(jobCtx, tc, job, collector)
	// A cancellation delivered on the WatchJobs stream (F-05) marks the run
	// as user-cancelled; FinishJob reports the job as cancelled, distinct
	// from a failure.
	w.mu.Lock()
	userCancelled := run.userCancel
	w.mu.Unlock()
	finalStatus := target.FinishJob(ctx, tc, job.GetId(), collector, err, userCancelled)
	// Record the job's terminal status and duration (F-20), matching the
	// status the API records for the same job.
	w.metrics.RecordJob(target.JobStatusName(finalStatus), time.Since(start))
}
