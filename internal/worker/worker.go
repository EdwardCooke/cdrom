// Package worker implements the ephemeral worker: a short-lived PROCESS SPAWNED
// BY THE SUPERVISORY AGENT (F-30.4) to execute a single job, then terminate.
// Must run on both Windows and Linux.
//
// The agent spawns the worker with the mandate IN THE ENVIRONMENT ONLY (argv
// is world-readable; env is not): CDROM_WORKER_JOB_ID (the mandated job) and
// CDROM_WORKER_JOB_TOKEN (the job token the agent's StartJobExecution minted).
// The worker talks ONLY to the API service: its first call is GetJob,
// presenting the job token — the API verifies the token and returns the
// hydrated payload (spec, upstream outputs, decrypted secrets). The worker
// then executes the job, reports its own status, and exits. All logging is
// local (stdout or a file) — there is no central logs service.
//
// Attribution (F-30.4): the worker is anonymous to the API apart from its job
// token, so it ATTRIBUTES ITS REPORTS TO THE AGENT'S EXECUTION under the
// agent's registered name — decoded LOCALLY from its own token's agent_name
// claim (unsigned read; the token is the agent's StartJobExecution response,
// so the claim is trustworthy for attribution purposes). Its own hostname is
// retained for LOGGING only.
package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/metrics"
	"cdrom/internal/target"

	// Register the built-in step handlers (e.g. the shell handler) with the
	// executor. A target or plugin adds more step types the same way.
	_ "cdrom/internal/stephandlers"
)

// workerCancelPollInterval is how often a worker polls the API for its job's
// status to observe a cancellation (F-05). A worker holds no WatchJobs
// stream, so it cannot be signalled directly; it observes the cancellation on
// the API instead. It is a variable (not a constant) so tests can shorten the
// interval.
var workerCancelPollInterval = 2 * time.Second

// Dependencies are the inputs a worker needs. The worker talks only to the
// API service.
type Dependencies struct {
	API apipb.APIClient
	// JobToken is the job token the agent's StartJobExecution minted (F-30.4):
	// the worker presents it to GetJob (payload claim) and every job-scoped
	// RPC. Delivered in the ENVIRONMENT ONLY (CDROM_WORKER_JOB_TOKEN).
	JobToken string
	// AgentRoot is the base directory the worker's staged working area
	// (F-21.6) is created under; empty means the OS temp dir. Carried from
	// the worker's config (agent_root / CDROM_WORKER_ROOT).
	AgentRoot string
	// ParentGrpcAddr is the parent agent's SUPERVISION CHANNEL address
	// (F-30.4 slice 2; CDROM_PARENT_GRPC_ADDR, loopback). Empty = the agent
	// has no channel (degraded) — the worker runs channel-less and the agent
	// infers its outcome from the exit code.
	ParentGrpcAddr string
}

// Work is an ephemeral job executor.
type Work struct {
	name   string // the worker's HOSTNAME (logging only; F-30.4: reports attribute under the agent name)
	agent  string // the AGENT NAME the worker acts as (decoded from the job token's agent_name claim)
	jobID  int64
	deps   Dependencies
	logger *slog.Logger
	// metrics is the worker's metric set (F-20); nil when metrics are
	// disabled, in which case all recording is a no-op. A worker records only
	// the job/step/in-flight/log metrics (it holds no WatchJobs stream and
	// polls no queue).
	metrics *metrics.AgentMetrics
}

// New creates a worker that will execute jobID with the job token the agent
// minted (F-30.4). name is the worker's hostname (logged, not reported); the
// agent name is recovered from the token's agent_name claim.
func New(name string, jobID int64, deps Dependencies, logger *slog.Logger) *Work {
	if logger == nil {
		logger = slog.Default()
	}
	// Advise (warn) once at startup when the staged-working-area root is
	// RAM-backed (F-21.6): staged files would consume memory.
	target.WarnTmpfsAgentRoot(deps.AgentRoot, logger)
	return &Work{name: name, jobID: jobID, deps: deps, logger: logger, agent: tokenAgentName(deps.JobToken)}
}

// tokenAgentName decodes the agent_name claim from a compact JWT UNSIGNED
// (F-30.4): the worker trusts the claim for ATTRIBUTION because the token is
// the very artifact the agent's credentialed StartJobExecution returned — the
// API minted it and stamped the agent's registered name. Returns "" when the
// token is absent/garbled/pre-F-30.4-shaped (the worker then reports under its
// own hostname, the pre-subprocess behavior).
func tokenAgentName(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		AgentName string `json:"agent_name"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	return claims.AgentName
}

// SetMetrics sets the worker's metric set (F-20). It must be called before
// Run. A nil set disables all metric recording.
func (a *Work) SetMetrics(m *metrics.AgentMetrics) {
	a.metrics = m
}

// Run executes the job and returns the final status. The process should exit
// once Run returns.
func (a *Work) Run(ctx context.Context) (dbpb.JobStatus, error) {
	if a.jobID == 0 {
		return dbpb.JobStatus_JOB_STATUS_UNSPECIFIED, fmt.Errorf("worker: job id is required")
	}
	if a.deps.JobToken == "" {
		// F-30.4: the job token is the worker's ONLY credential — without it
		// the payload claim (GetJob) is impossible. Refuse loudly (the agent
		// spawned this worker; a missing mandate is a bootstrap fault).
		return dbpb.JobStatus_JOB_STATUS_UNSPECIFIED, fmt.Errorf("worker: job token is required (CDROM_WORKER_JOB_TOKEN)")
	}
	// CLAIM THE PAYLOAD (F-30.4): GetJob with the job token. The API verifies
	// the token (scoped to this job) and returns the hydrated job — spec,
	// upstream outputs, decrypted secrets, variables, the environment
	// approval gate. The payload crosses the wire exactly once, worker←API.
	job, err := a.claimPayload(ctx)
	if err != nil {
		return dbpb.JobStatus_JOB_STATUS_UNSPECIFIED, fmt.Errorf("worker: claim payload: %w", err)
	}
	reporter := a.agent
	if reporter == "" {
		reporter = a.name // degenerate (no claim): report under the hostname
	}
	a.logger.Info("worker: executing job", "job", job.GetId(), "name", job.GetName(), "under", reporter)

	// The job runs under a cancellable context (F-05): a background poller
	// watches the job's status and cancels the context if the job is
	// cancelled, which interrupts the running step. Created BEFORE the
	// supervision channel so the channel's Signals notice can accelerate the
	// SAME cancel() (the poll remains the authority).
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go a.watchCancellation(jobCtx, cancel)

	// SUPERVISION CHANNEL (F-30.4 slice 2): dial the parent agent's loopback
	// channel (the handshake needs the claimed job's attempt). Total
	// degradation: a nil client means channel-less (the agent infers the
	// outcome from the exit code); the channel NEVER gates the job.
	if a.deps.ParentGrpcAddr == "" {
		a.logger.Info("worker: no supervision channel addressed (running channel-less)", "job", a.jobID)
	}
	var sc *SupervisorClient
	if a.deps.ParentGrpcAddr != "" {
		var cerr error
		// The ticket carries the worker's ATTRIBUTION name (decoded unsigned
		// from its own job token — the very name the API stamped when THIS
		// agent started the execution): on a RESUME (the agent restarted
		// mid-job) the fresh instance admits the worker only when the ticket
		// is consistent with its own name.
		sc, cerr = NewSupervisorClient(a.deps.ParentGrpcAddr, a.jobID, job.GetAttempt(), a.deps.JobToken, reporter, a.logger)
		if cerr != nil {
			a.logger.Warn("worker: supervision channel init", "err", cerr)
			sc = nil
		}
		if sc != nil {
			defer sc.Flush() // the FINAL FLUSH precedes exit (bounded; best-effort)
			sc.SetCancelNotify(func() {
				a.logger.Info("worker: cancellation signalled over the supervision channel (acting)", "job", a.jobID)
				cancel()
			})
		}
	}

	// Metric recording (F-20): when the channel is up the worker FORWARDS
	// its samples to the agent (the agent folds them into its own exported
	// series — nothing scrapes the worker); otherwise the worker records
	// into its own set (kept for the channel-less posture; the worker
	// exports no /metrics of its own).
	var rec metrics.Recorder
	if sc != nil {
		rec = sc.Recorder()
	} else if a.metrics != nil {
		rec = a.metrics
	}

	// The shared execution context: the target's API client, the AGENT NAME
	// (F-30.4: the worker's reports attribute to the agent's JobExecution),
	// the job's token, and the worker's step metrics (F-20). A worker
	// participates in the STEP BARRIER under the same agent name (the
	// barrier's participant set is keyed by agent name).
	tc := &target.Context{
		API:       a.deps.API,
		AgentName: reporter,
		Token:     a.deps.JobToken,
		Metrics:   rec,
		Logger:    a.logger,
		AgentRoot: a.deps.AgentRoot,
	}
	if job.GetStepBarrier() && job.GetTargetGroup() != "" {
		tc.StepBarrier = target.NewStepBarrier(a.deps.API, reporter, a.jobID, a.deps.JobToken, a.logger, 0)
	}

	// The worker runs exactly one job, so it is in flight for the whole run
	// (its own gauge; the channel recorder defers inflight to the agent).
	if a.metrics != nil {
		a.metrics.SetInflight(1)
		defer a.metrics.SetInflight(0)
	}

	target.ReportRunning(ctx, tc, a.jobID)
	a.logger.Info("job started", "job", a.jobID)

	collector := &executor.StepResultCollector{}
	start := time.Now().UTC() // house style: measured/stored times in UTC
	err = target.RunJob(jobCtx, tc, job, collector)
	// A cancellation observed by the worker's status poller (F-05) — or the
	// worker's own shutdown — cancelled the job's context; FinishJob reports
	// the job as cancelled, distinct from a failure.
	finalStatus := target.FinishJob(ctx, tc, a.jobID, collector, err, jobCtx.Err() == context.Canceled)
	// Record the job's terminal status and duration (F-20), matching the
	// status the API records for the same job. Via the channel recorder this
	// BUFFERS the final account for the pre-exit flush (consumed by the
	// agent); channel-less it records into the worker's own set.
	if rec != nil {
		rec.RecordJob(target.JobStatusName(finalStatus), time.Since(start))
	}
	return finalStatus, nil
}

// claimPayload fetches the job's hydrated payload from the API, presenting
// the job token (F-30.4). The token decorates ONLY this call family (plus the
// job-scoped RPCs the shared execution makes); the worker holds no other
// credential.
func (a *Work) claimPayload(ctx context.Context) (*apipb.Job, error) {
	return a.deps.API.GetJob(grpcutil.WithBearerToken(ctx, a.deps.JobToken), &apipb.GetJobRequest{Id: a.jobID})
}

// watchCancellation polls the job's status (F-05) and cancels the job's
// context if the job is cancelled. A worker holds no WatchJobs stream, so it
// observes the cancellation on the API: the scheduler's CancelJob marks the
// job cancelled in the database, and the worker's next poll sees it and
// interrupts the running step (by cancelling the job's context). Polling
// stops when the job's context is done (the job finished or was cancelled).
//
// The poll presents the job token (F-30.4: GetJob is the payload claim, hence
// token-gated) and TOLERATES A TERMINAL STATUS (the API's GetJob gate accepts
// the token for a finished job — the owner is the job itself) so the
// observer converges on a cancellation it caused or witnessed.
func (a *Work) watchCancellation(ctx context.Context, cancel context.CancelFunc) {
	ticker := time.NewTicker(workerCancelPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			job, err := a.claimPayload(ctx)
			if err != nil {
				// A transient failure to fetch the job is not a cancellation;
				// keep polling.
				continue
			}
			if job.GetStatus() == dbpb.JobStatus_JOB_STATUS_CANCELLED {
				a.logger.Info("worker: job cancelled", "job", a.jobID)
				cancel()
				return
			}
		}
	}
}
