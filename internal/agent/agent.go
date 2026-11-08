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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/grpcutil"
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
	// CredentialSupplier provides the agent's CREDENTIAL (F-30.3): the
	// agent-credential JWT the agent presents on its identity RPCs (register /
	// heartbeat / watch / poll). It is consulted LAZILY — at registration and
	// again on each retry — so a credential that ARRIVES AFTER BOOT (the
	// operator distributing it, or a rotation being adopted) is picked up
	// without a restart. Nil = no credential (disabled posture: the API's
	// agent-credential enforcement must be off, or registration is denied).
	CredentialSupplier func() (string, error)
	// APIHTTPBase is the API's HTTP surface (e.g. http://127.0.0.1:8080) —
	// the auto-renew loop calls the SELF-SERVICE RENEW ENDPOINT (F-30.2 S3)
	// over it. Empty = auto-renew disabled (the loop sleeps).
	APIHTTPBase string
	// AutoRenewWindow is the renew-ahead horizon (F-30.2/F-30.3): the agent
	// renews when the row's deadline falls within this window. Zero/negative
	// falls back to the default (24 h).
	AutoRenewWindow time.Duration
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
	labels map[string]string
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

	// deadlineMu protects the auto-renew loop's learned row deadline (F-30.3
	// S3): the renew response EXTENDS the row, and the loop schedules its next
	// wake off that deadline (minus the renew-ahead window).
	deadlineMu   sync.Mutex
	nextDeadline time.Time
	hasDeadline  bool
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
		labels:       labels,
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

// Run registers the agent (PRESENTING ITS CREDENTIAL, F-30.3), then serves
// jobs until ctx is cancelled. Registration RETRIES on a capped exponential
// backoff: the credential may arrive after boot (the operator distributes it
// to the credential file; a rotation is adopted live), and the agent's
// SUPERVISOR (systemd / Windows service / k8s) restarts it on a non-zero
// exit — settling by retrying beats crashing. It returns nil on a clean
// shutdown.
func (w *Agent) Run(ctx context.Context) error {
	w.registerResilient(ctx)
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
	// Auto-renew the credential in the background (F-30.3 S3): scheduled off
	// the ROW's deadline (extended by each renew), INSIDE the configured
	// window; failures back off and retry WITHOUT disturbing the stream.
	go w.autoRenew(ctx)

	// Watch for job assignments, REOPENING the stream whenever it ends —
	// whether by error OR by a remote close (io.EOF). A remote close is the
	// API tearing the stream down (e.g. the agent's credential LAPSED, F-30.3
	// S3): the agent must NOT exit (its supervisor would restart it, losing
	// in-flight coordination) — it reconnects, and each open re-reads the
	// credential file lazily, so a ROTATED credential is adopted on the next
	// open without a restart.
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := w.watch(ctx)
		if ctx.Err() != nil {
			return nil
		}
		w.metrics.SetWatchState("reconnecting")
		if err != nil {
			w.logger.Warn("agent: watch stream ended; reconnecting", "err", err)
		} else {
			w.logger.Warn("agent: watch stream closed; reconnecting")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

// registerResilient registers the agent, RETRYING on a capped exponential
// backoff until it succeeds or the context is cancelled. The retry exists
// because the credential may ARRIVE AFTER BOOT: the operator provisions the
// credential file (or a rotation is adopted) asynchronously, and the agent's
// supervisor restarts it on a non-zero exit — settling by retrying beats
// crashing. Each attempt presents the CURRENT credential (lazily re-fetched,
// so an arrival/rotation is picked up without a restart).
func (w *Agent) registerResilient(ctx context.Context) {
	backoff := 500 * time.Millisecond
	for {
		ctxCred, err := w.credentialCtx(ctx)
		if err != nil {
			w.logger.Warn("agent: credential unavailable; retrying registration", "err", err)
		} else if _, regErr := w.deps.API.RegisterAgent(ctxCred, &apipb.RegisterAgentRequest{
			Name:         w.name,
			Group:        w.group,
			Address:      w.name,
			Environments: w.environments,
			Labels:       w.labels,
		}); regErr != nil {
			err = regErr
			w.logger.Warn("agent: register; retrying", "err", err)
		} else {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			if backoff *= 2; backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
		}
	}
}

// credentialCtx returns ctx augmented with the agent's credential as the
// outgoing Authorization bearer (F-30.3). With no supplier configured it
// returns ctx unchanged (DISABLED POSTURE: the API's agent-credential
// enforcement must be off, or the call is denied).
func (w *Agent) credentialCtx(ctx context.Context) (context.Context, error) {
	if w.deps.CredentialSupplier == nil {
		return ctx, nil
	}
	tok, err := w.deps.CredentialSupplier()
	if err != nil {
		return ctx, err
	}
	return grpcutil.WithBearerToken(ctx, tok), nil
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
func (w *Agent) watch(base context.Context) error {
	// Opening the stream is an identity verb (the agent credential decorates
	// the OPEN); the RECEIVE LOOP and the jobs it starts run on the BASE
	// context (job-scoped verbs present the JOB TOKEN — F-30.3 orthogonality
	// by verb).
	ctx, err := w.credentialCtx(base)
	if err != nil {
		return fmt.Errorf("agent: credential unavailable: %w", err)
	}
	stream, err := w.deps.API.WatchJobs(ctx, &apipb.WatchJobsRequest{
		AgentName:    w.name,
		Group:        w.group,
		Environments: w.environments,
	})
	if err != nil {
		return fmt.Errorf("agent: watch: %w", err)
	}
	w.metrics.SetWatchState("connected")
	// STREAM-RECYCLE WATCHDOG (F-30.3 S3): the API tears the stream down when
	// the agent's credential lapses and sends the terminator — but IF the
	// terminator is ever LOST (a stalled server-side send, a wedged
	// transport), the agent would sit on a dead stream forever. A maximum
	// stream AGE forces a periodic reopen: each open re-presents the
	// credential AND re-reads the credential file lazily, so a ROTATED
	// credential is adopted on the reopen without a restart. Generous (5
	// min): an idle stream is NORMAL (the poll loop is the authoritative
	// catch-up path), so the recycle is insurance, not a heartbeat.
	watchdog := time.NewTimer(maxWatchAge)
	defer watchdog.Stop()
	for {
		if base.Err() != nil {
			return nil
		}
		// Receive in a SIDE GOROUTINE: a blocked Recv cannot observe timers,
		// so the watchdog (below) must compete with it in a select.
		outcome := make(chan watchRecv, 1)
		go func() {
			m, e := stream.Recv()
			outcome <- watchRecv{msg: m, err: e}
		}()
		select {
		case <-base.Done():
			return nil
		case <-watchdog.C:
			w.logger.Warn("agent: watch stream recycled (maximum age); reopening")
			return nil // the blocked Recv goroutine is abandoned (collected when the stream dies)
		case r := <-outcome:
			if r.err != nil {
				if r.err == io.EOF || base.Err() != nil {
					return nil // remote close (teardown) or shutdown: reopen
				}
				return fmt.Errorf("agent: recv: %w", r.err)
			}
			watchdog.Reset(maxWatchAge)
			w.handleWatchMessage(base, r.msg)
		}
	}
}

// maxWatchAge is the oldest the agent lets a watch stream get before forcing
// a reopen (the stream-recycle watchdog; see watch).
const maxWatchAge = 5 * time.Minute

// watchRecv is the side-goroutine's receipt of one watch message.
type watchRecv struct {
	msg *apipb.WatchMessage
	err error
}

// handleWatchMessage dispatches one watch message (factored from the receive
// loop so the watchdog's select stays tidy).
func (w *Agent) handleWatchMessage(base context.Context, message *apipb.WatchMessage) {
	switch m := message.GetMessage().(type) {
	case *apipb.WatchMessage_Assignment:
		// A full assignment (legacy push). The agent starts its own
		// execution of the job (fan-out): a job that targets the agent's
		// group runs on every agent in the group, so the start is not
		// exclusive. The start returns the fresh spec + token. Jobs run on
		// the BASE context (job-scoped verbs present the JOB TOKEN).
		w.startAndExecute(base, m.Assignment.GetJob().GetId())
	case *apipb.WatchMessage_Nudge:
		// A nudge (F-23): the push carries only the job id. The agent starts
		// its own execution, so a job delivered via both push and poll is run
		// once by this agent (its execution is created once).
		w.startAndExecute(base, m.Nudge.GetJobId())
	case *apipb.WatchMessage_Cancellation:
		w.cancelJob(m.Cancellation.GetJobId())
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
// starts each one. The LIST is an identity verb (decorated with the agent
// credential); the JOBS THEMSELVES run on the BASE context — job-scoped verbs
// (start/report/logs/exchange) present the JOB TOKEN, never the agent
// credential (F-30.3 orthogonality by verb).
func (w *Agent) pollOnce(base context.Context) {
	ctx, err := w.credentialCtx(base)
	if err != nil {
		w.logger.Warn("agent: credential unavailable; skipping poll", "err", err)
		return
	}
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
		w.startAndExecute(base, job.GetId())
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
		JobId:     jobID,
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
			ctxBeat, err := w.credentialCtx(ctx)
			if err != nil {
				w.logger.Warn("agent: credential unavailable; skipping heartbeat", "err", err)
				w.metrics.RecordHeartbeatFailure()
				continue
			}
			if _, hbErr := w.deps.API.Heartbeat(ctxBeat, &apipb.HeartbeatRequest{Name: w.name}); hbErr != nil {
				w.logger.Warn("agent: heartbeat", "err", hbErr)
				w.metrics.RecordHeartbeatFailure()
			}
		}
	}
}

// leaves it unset (matches the config default).
const defaultAutoRenewWindow = 24 * time.Hour

// minAutoRenewDelay floors the sleep-before-renew so a tight window cannot
// turn the loop into a hot spinner.
const minAutoRenewDelay = 5 * time.Second

// autoRenewBackoff is the retry spacing after a FAILED renew (network blip,
// API maintenance): the loop keeps sleeping, the stream is UNDISTURBED.
const autoRenewBackoff = 30 * time.Second

// autoRenewIdleCap caps the idle sleep between scheduled renews so a HUGE
// deadline (a fresh 30-day issue) does not blind the loop to a rotation
// shrinking it: the loop revisits the computation regularly.
const autoRenewIdleCap = 10 * time.Minute

// autoRenew keeps the agent's credential ALIVE (F-30.3 S3 / F-30.2 renew-
// ahead): it learns the row's deadline from each renew's RESPONSE (the JWT's
// own exp is a placeholder — the ROW is the deadline's keeper), wakes when
// the deadline falls inside the configured window, and calls the SELF-SERVICE
// RENEW ENDPOINT presenting the CURRENT credential (F-30.2 S3). Failures back
// off and retry WITHOUT tearing down the stream; a rotated credential is
// picked up lazily (the supplier re-reads the file each wake).
func (w *Agent) autoRenew(ctx context.Context) {
	window := w.deps.AutoRenewWindow
	if window <= 0 {
		window = defaultAutoRenewWindow
	}
	for {
		delay := w.renewDelay(window)
		sleepOrDie(ctx, delay)
		if ctx.Err() != nil {
			return
		}
		if w.deps.CredentialSupplier == nil || w.deps.APIHTTPBase == "" {
			// Auto-renew disabled (no credential source / no HTTP surface):
			// park quietly; the credential's own deadline governs.
			continue
		}
		tok, err := w.deps.CredentialSupplier()
		if err != nil {
			w.logger.Warn("agent: auto-renew: credential unreadable; retrying", "err", err)
			sleepOrDie(ctx, autoRenewBackoff)
			continue
		}
		if err := w.renewCredential(ctx, tok); err != nil {
			// Back off and retry — the STREAM IS UNDISTURBED (a failed renew
			// does not imply a lapsed credential; the heartbeat keeps
			// revalidating the current one).
			w.logger.Warn("agent: auto-renew failed; retrying", "err", err)
			sleepOrDie(ctx, autoRenewBackoff)
			continue
		}
	}
}

// renewDelay computes the sleep until the next renew: when the deadline is
// known, fire at (deadline - window), floored and capped; when unknown, fire
// soon (the floor) to learn one.
func (w *Agent) renewDelay(window time.Duration) time.Duration {
	deadline := w.learnedDeadline()
	if deadline.IsZero() {
		return minAutoRenewDelay
	}
	delay := time.Until(deadline.Add(-window))
	if delay < minAutoRenewDelay {
		return minAutoRenewDelay
	}
	if delay > autoRenewIdleCap {
		return autoRenewIdleCap
	}
	return delay
}

// sleepOrDie sleeps d unless ctx is cancelled first.
func sleepOrDie(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// learnedDeadline returns the latest row deadline learned from a renew
// response (zero when unknown).
func (w *Agent) learnedDeadline() time.Time {
	w.deadlineMu.Lock()
	defer w.deadlineMu.Unlock()
	if !w.hasDeadline {
		return time.Time{}
	}
	return w.nextDeadline
}

// rememberDeadline records the row deadline learned from a renew response.
func (w *Agent) rememberDeadline(d time.Time) {
	w.deadlineMu.Lock()
	w.nextDeadline = d
	w.hasDeadline = true
	w.deadlineMu.Unlock()
}

// renewCredential calls the SELF-SERVICE RENEW ENDPOINT (F-30.2 S3) presenting
// the agent's own credential. The endpoint is addressed by (name, id): the
// agent KNOWS its name and LEARNS its credential id from the token's OWN jti
// claim (decoded locally — no signature check is needed to read a claim the
// agent already possesses), so no directory call is required.
func (w *Agent) renewCredential(ctx context.Context, token string) error {
	jti, err := jwtJTI(token)
	if err != nil {
		return fmt.Errorf("agent: auto-renew: read jti: %w", err)
	}
	url := w.deps.APIHTTPBase + "/api/agents/" + w.name + "/credentials/" + jti + "/renew"
	ren, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("agent: auto-renew: build request: %w", err)
	}
	ren.Header.Set("Authorization", "Bearer "+token)
	rs, err := http.DefaultClient.Do(ren)
	if err != nil {
		return fmt.Errorf("agent: auto-renew: %w", err)
	}
	defer rs.Body.Close()
	if rs.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(rs.Body)
		return fmt.Errorf("agent: auto-renew: status %d: %s", rs.StatusCode, strings.TrimSpace(string(buf)))
	}
	// Learn the EXTENDED deadline from the response (the scheduling basis for
	// the next wake). The response is the credential row serialized by
	// encoding/json: the well-known Timestamp renders as its STRUCT FORM
	// ({"seconds":N,"nanos":M}), not an RFC3339 string (that is protojson).
	var resp struct {
		ExpiresAt *struct {
			Seconds int64 `json:"seconds"`
			Nanos   int32 `json:"nanos"`
		} `json:"expires_at"`
	}
	if err := json.NewDecoder(rs.Body).Decode(&resp); err == nil && resp.ExpiresAt != nil && resp.ExpiresAt.Seconds > 0 {
		d := time.Unix(resp.ExpiresAt.Seconds, int64(resp.ExpiresAt.Nanos)).UTC()
		w.rememberDeadline(d)
		w.logger.Info("agent: credential renewed", "jti", jti, "deadline", d.Format(time.RFC3339))
		return nil
	}
	w.logger.Info("agent: credential renewed (deadline unreported)", "jti", jti)
	return nil
}

// jwtJTI decodes the jti claim from a COMPACT JWT's payload segment WITHOUT
// verifying the signature — the agent reads a claim from ITS OWN token (the
// file is the custody proof; the API re-verifies on presentation).
func jwtJTI(compact string) (string, error) {
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("malformed compact JWT")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("decode payload: %w", err)
	}
	var claims struct {
		JTI string `json:"jti"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		return "", fmt.Errorf("parse claims: %w", err)
	}
	if claims.JTI == "" {
		return "", fmt.Errorf("payload carries no jti")
	}
	return claims.JTI, nil
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
		API:       w.deps.API,
		AgentName: w.name,
		Token:     token,
		Metrics:   w.metrics,
		Logger:    w.logger,
		AgentRoot: w.deps.AgentRoot,
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
