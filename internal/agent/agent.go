// Package agent implements the long-lived AGENT: a resident SUPERVISOR
// process on a deployment target, targetable by group. Must run on both
// Windows and Linux.
//
// An agent talks ONLY to the API service. It registers with the API, holds a
// WatchJobs stream open, and OBSERVES the jobs the API dispatches to its
// group. Since F-30.4 the agent does NOT execute jobs in-process: on each
// assignment it (1) checks its LAUNCH LEDGER ((job, attempt) already
// claimed ⇒ drop — duplicate delivery, F-23 nudges); (2) calls
// StartJobExecution ITSELF, presenting its agent credential — acquiring the
// execution and receiving the freshly minted JOB TOKEN (the response carries
// no payload; the agent never hydrates spec or secrets); (3) spawns a
// per-job WORKER SUBPROCESS with the mandate in the ENVIRONMENT ONLY (argv
// is world-readable; env is not). The worker claims its payload from the API
// (GetJob with the job token), executes the job, reports its own status, and
// exits. The agent is a dumb BABYSITTER: it observes child liveness,
// relays cancellation OPPORTUNISTICALLY (the worker's own API status poll
// remains the authoritative cancellation path, F-05), and (slice 3) reports
// abnormal child deaths.
//
// Duplication safety rests on SERIALIZITY PER (job, agent, attempt)
// COORDINATE — upheld PRIMARILY by the agent's launch ledger and DEFENSIVELY
// by the database service's duplicate-start guard (a constraint-violating
// start declines as Started:false).
package agent

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/metrics"
	"cdrom/internal/target"
)

// Dependencies are the inputs an agent needs. The agent talks only to the
// API service.
type Dependencies struct {
	API apipb.APIClient
	// CredentialSupplier provides the agent's CREDENTIAL (F-30.3): the
	// agent-credential JWT the agent presents on its identity RPCs (register /
	// heartbeat / watch / poll / StartJobExecution). It is consulted LAZILY —
	// at registration and again on each retry — so a credential that ARRIVES
	// AFTER BOOT (the operator distributing it, or a rotation being adopted)
	// is picked up without a restart. Nil = no credential (disabled posture:
	// the API's agent-credential enforcement must be off, or registration is
	// denied).
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
	// ---- F-30.4: the worker-spawning inputs (the CANONICAL ENV CONTRACT) ----
	// WorkerBinary is the path to the worker executable the agent spawns per
	// job (typically the sibling bin/worker). Empty = spawning DISABLED
	// (assignments are acknowledged but not executed — a misconfiguration
	// surfaced by the WARN on every assignment).
	WorkerBinary string
	// WorkerArgs are appended to the worker invocation (production: none —
	// the mandate rides the environment; tests: e.g. ["-c","sleep 5"] on a
	// shell to obtain a controllable long-running child).
	WorkerArgs []string
	// APIAddress is the API gRPC address handed to the worker (CDROM_API_ADDR).
	APIAddress string
	// TLSCAFile is the CA bundle path handed to the worker (CDROM_TLS_CA_FILE)
	// so it verifies the API's server certificate. Empty = plaintext leg.
	TLSCAFile string
	// WorkerRoot is the worker's staged-work-area base (CDROM_WORKER_ROOT);
	// empty lets the worker default to the OS temp dir.
	WorkerRoot string
}

// Agent is a long-lived agent process (the supervisor; F-30.4).
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

	// LAUNCH LEDGER (F-30.4): the (job, attempt) coordinates this agent has
	// CLAIMED (successfully started + spawned). A duplicate delivery of an
	// already-claimed coordinate is DROPPED here (before hitting the API);
	// the database service's duplicate-start guard defends the same invariant
	// server-side (defense in depth). Coordinates are FREEd when the child
	// exits so a RETRY (a fresh attempt) or a RERUN (reset to pending) can
	// legitimately re-enter.
	ledgerMu sync.Mutex
	ledger   map[spawnCoordinate]struct{}

	// LIVE CHILDREN (F-30.4): the worker subprocesses currently running,
	// keyed by coordinate. The agent observes their liveness (Cmd.Wait) and
	// relays cancellation opportunistically (kill on a WatchJobs
	// JobCancellation; the worker's own API poll is authoritative, F-05).
	childrenMu   sync.Mutex
	liveChildren map[spawnCoordinate]*childProcess
	wg           sync.WaitGroup // outstanding child-wait goroutines

	// deadlineMu protects the auto-renew loop's learned row deadline (F-30.3
	// S3): the renew response EXTENDS the row, and the loop schedules its next
	// wake off that deadline (minus the renew-ahead window).
	deadlineMu   sync.Mutex
	nextDeadline time.Time
	hasDeadline  bool
}

// spawnCoordinate is the ledger/livespace key: one worker per (job, attempt).
type spawnCoordinate struct {
	jobID   int64
	attempt int32
}

// childProcess is one supervised worker subprocess.
type childProcess struct {
	cmd   *exec.Cmd
	pid   int
	jobID int64
	// workerLog is the child's OWN log file (F-30.4): the agent folds it
	// into its log after the child exits.
	workerLog string
	// spawnedAt is when the child was started (F-30.4): the agent measures
	// the supervised job's wall-clock duration for its metrics roll-up.
	spawnedAt time.Time
	// exited is closed when the child's Wait returns (observation-complete).
	exited chan struct{}
}

// New creates an agent with the given identity and service dependencies.
// environments are the environments the agent belongs to (F-17); they are
// declared at registration and used to route environment jobs to this agent.
// labels are the agent's labels (F-29); they are declared at registration
// and merged into the label set of the jobs the agent runs.
func New(name, group string, environments []string, labels map[string]string, deps Dependencies, logger *slog.Logger) *Agent {
	if logger == nil {
		logger = slog.Default()
	}
	// Advise (warn) once at startup when the staged-working-area root is
	// RAM-backed (F-21.6): staged files would consume memory.
	target.WarnTmpfsAgentRoot(deps.AgentRoot, logger)
	if deps.WorkerBinary == "" {
		logger.Warn("agent: no worker binary configured; assignments will NOT be executed",
			"hint", "set worker_binary (CDROM_AGENT_WORKER_BINARY) to the worker executable")
	}
	return &Agent{
		name:         name,
		group:        group,
		environments: environments,
		labels:       labels,
		deps:         deps,
		logger:       logger,
		ledger:       make(map[spawnCoordinate]struct{}),
		liveChildren: make(map[spawnCoordinate]*childProcess),
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
// shutdown. Workers spawned before shutdown DETACH from the agent's lifetime
// (their context is independent): they finish their jobs and report to the
// API even if the agent exits (controlled drain/kill ordering is slice 3).
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
		w.setWatchMetric("reconnecting")
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
// Assignments are processed SYNCHRONOUSLY in the watch loop (the spawn is
// fast — a StartJobExecution RPC + an exec — and the heavy lifting happens in
// the detached child), so the watch loop stays free to receive a
// cancellation for a running job. A cancellation delivered while a job runs
// INTERRUPTS THE CHILD (see cancelJob); one delivered for a job that is not
// running on this agent is ignored (the job's status is already reconciled
// by the database).
func (w *Agent) watch(base context.Context) error {
	// Opening the stream is an identity verb (the agent credential decorates
	// the OPEN); the RECEIVE LOOP and the jobs it starts run on the BASE
	// context (job-scoped verbs present the JOB TOKEN — F-30.3 orthogonality
	// by verb; StartJobExecution itself re-decorates with the credential).
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
	w.setWatchMetric("connected")
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
		// A full assignment (legacy push). The agent CLAIMS its own execution
		// of the job (fan-out) and spawns the worker (F-30.4).
		w.supervise(base, m.Assignment.GetJob().GetId())
	case *apipb.WatchMessage_Nudge:
		// A nudge (F-23): the push carries only the job id. The agent claims
		// its own execution, so a job delivered via both push and poll is run
		// once by this agent (its execution is created once).
		w.supervise(base, m.Nudge.GetJobId())
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
// credential); the JOBS THEMSELVES are claimed on the BASE context —
// StartJobExecution re-decorates with the credential (it is an identity verb
// that RETURNS the job token; F-30.3/F-30.4).
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
		w.recordPoll("idle")
		return
	}
	w.recordPoll("claimed")
	for _, job := range resp.GetJobs() {
		w.supervise(base, job.GetId())
	}
}

// supervise claims the agent's execution of a job (fan-out) and, on success,
// SPAWNS the worker subprocess (F-30.4). It is the single entry point for job
// acquisition, used by both the push path (a nudge or full assignment on the
// WatchJobs stream) and the pull path (the periodic poll). A job that targets
// the agent's group runs on every agent in the group, so the start is not
// exclusive: this agent claims its own execution and spawns its own worker.
// A job that is no longer pending (it reached a terminal state, or it is not
// targeted at the agent's group) is not started (started=false), so a job
// delivered to several agents — or via both push and poll — is run once by
// each of them, and a job that finished before a slow agent started it is not
// run.
//
// Deduplication (F-30.4): the LAUNCH LEDGER drops a coordinate this agent
// already claimed (checked AFTER the start succeeds — the attempt number is
// only known from the start's response); the DATABASE SERVICE's
// duplicate-start guard declines the same coordinate server-side (defense in
// depth: a racing twin that slips past the ledger still loses the start).
func (w *Agent) supervise(base context.Context, jobID int64) {
	// StartJobExecution is an IDENTITY VERB (F-30.3/F-30.4): it claims the
	// agent's name, so it is decorated with the agent credential — unlike
	// the job-scoped verbs the worker runs (those present the job token).
	ctx, err := w.credentialCtx(base)
	if err != nil {
		w.logger.Warn("agent: credential unavailable; dropping assignment", "job", jobID, "err", err)
		return
	}
	resp, err := w.deps.API.StartJobExecution(ctx, &apipb.StartJobExecutionRequest{
		JobId:     jobID,
		AgentName: w.name,
	})
	if err != nil {
		w.logger.Warn("agent: start job execution", "job", jobID, "err", err)
		return
	}
	if !resp.GetStarted() {
		// The job was not started (no longer pending, not served by this
		// agent, or the (job, agent, attempt) coordinate was already
		// claimed — a duplicate delivery). ROUTINE: permanently-pending
		// definition rows are re-polled every second and ALWAYS decline, so
		// this is DEBUG (a WARN/INFO here floods the log); the ledger drop
		// below — a twin slipping past the start — stays loud.
		w.logger.Debug("agent: job not started (dropped)", "job", jobID)
		return
	}
	coord := spawnCoordinate{jobID: jobID, attempt: resp.GetAttempt()}
	if !w.reserveLedger(coord) {
		// A twin delivery slipped past the start (the DB guard should have
		// declined it; belt-and-braces): do NOT spawn twice.
		w.logger.Warn("agent: duplicate coordinate dropped by launch ledger", "job", jobID, "attempt", coord.attempt)
		return
	}
	cp := w.spawnWorker(coord, resp.GetJobToken())
	if cp == nil {
		// Spawn failed (binary missing, exec error): FREE the reservation so
		// a redelivery (nudge/poll) can retry — the execution exists in the
		// DB (the start succeeded) and the worker WILL claim it.
		w.freeLedger(coord)
		return
	}
	w.refreshInflight()
}

// reserveLedger claims a coordinate in the launch ledger. False when the
// coordinate is already claimed (duplicate delivery).
func (w *Agent) reserveLedger(coord spawnCoordinate) bool {
	w.ledgerMu.Lock()
	defer w.ledgerMu.Unlock()
	if _, ok := w.ledger[coord]; ok {
		return false
	}
	w.ledger[coord] = struct{}{}
	return true
}

// freeLedger releases a coordinate (the child exited, or the spawn failed):
// a retry/rerun re-opening the coordinate may re-enter.
func (w *Agent) freeLedger(coord spawnCoordinate) {
	w.ledgerMu.Lock()
	defer w.ledgerMu.Unlock()
	delete(w.ledger, coord)
}

// spawnWorker forks the worker subprocess with the CANONICAL ENV CONTRACT
// (F-30.4): the mandate rides the ENVIRONMENT ONLY — argv is world-readable
// (process listings, /proc/*/cmdline); env is not. The child DETACHES from
// the agent's lifetime (its context is independent): it finishes its job and
// reports to the API even if the agent exits (controlled drain/kill ordering
// is slice 3). Detached I/O (Discard): the worker's own logging convention
// (CDROM_LOG_*) governs where its logs go.
func (w *Agent) spawnWorker(coord spawnCoordinate, jobToken string) *childProcess {
	if w.deps.WorkerBinary == "" {
		w.logger.Warn("agent: worker binary not configured; job NOT executed", "job", coord.jobID, "attempt", coord.attempt)
		return nil
	}
	cmd := exec.CommandContext(context.Background(), w.deps.WorkerBinary, w.deps.WorkerArgs...)
	// Env: the INHERITED environment MINUS the agent's private secrets (the
	// agent credential file must NOT propagate into the worker — the worker
	// never needs it), PLUS the canonical mandate. Our variables OVERRIDE
	// any inherited ones (we strip them from the inherited set first).
	privileged := map[string]struct{}{
		"CDROM_AGENT_CREDENTIAL_FILE": {}, // the agent's credential (private)
		"CDROM_WORKER_JOB_ID":         {}, // overridden below
		"CDROM_WORKER_JOB_TOKEN":      {}, // overridden below
	}
	env := make([]string, 0, 16)
	for _, kv := range os.Environ() {
		k := kv
		if i := strings.IndexByte(kv, '='); i > 0 {
			k = kv[:i]
		}
		if _, priv := privileged[k]; priv {
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"CDROM_WORKER_JOB_ID="+strconv.FormatInt(coord.jobID, 10),
		"CDROM_WORKER_JOB_TOKEN="+jobToken,
		"CDROM_API_ADDR="+w.deps.APIAddress,
	)
	if w.deps.WorkerRoot != "" {
		env = append(env, "CDROM_WORKER_ROOT="+w.deps.WorkerRoot)
	}
	if w.deps.TLSCAFile != "" {
		env = append(env, "CDROM_TLS_CA_FILE="+w.deps.TLSCAFile)
	}
	// The worker's log DESTINATION (F-30.4): when the agent's own stdout is a
	// REGULAR FILE (the norm: harness/service-manager redirection), the
	// worker APPENDS THERE LIVE (concurrent O_APPEND writers) — the worker's
	// lines are visible the instant they are written, so a reader that waits
	// for the job's terminal status never races a post-exit fold. When the
	// agent's stdout is a pipe/socket/dev-null (CI, journald), the worker
	// writes to its OWN file and the agent folds it in after the child exits
	// (appendWorkerLog — the only case with a small settle window).
	dest := agentLogDest()
	var wl string
	if dest != "" {
		wl = dest
	} else {
		wl = workerLogPath(coord.jobID, randHex())
	}
	env = append(env, "CDROM_LOG_FILE="+wl)
	cmd.Env = env
	// Detach the child into its own process group (POSIX): a later
	// kill-on-shutdown (slice 3) can take the WHOLE TREE (the worker's own
	// grandchild commands) with one signal. No-op on Windows.
	w.attachDetachment(cmd)
	// STDIO IS INHERITED (nil): the worker's logs flow to the agent's log
	// destination (CDROM_LOG_FILE / the service journal) — one coherent log
	// per target host, distinguishable by the "worker:"/"agent:" prefixes.
	// (The worker's own logging convention, CDROM_LOG_*, is inherited too.)
	if err := cmd.Start(); err != nil {
		w.logger.Error("agent: spawn worker", "job", coord.jobID, "attempt", coord.attempt, "err", err)
		return nil
	}
	// House style: all STORED/measured times are UTC (duration arithmetic
	// is location-immune either way; UTC keeps exhibits comparable).
	cp := &childProcess{cmd: cmd, pid: cmd.Process.Pid, jobID: coord.jobID, workerLog: wl, spawnedAt: time.Now().UTC(), exited: make(chan struct{})}
	w.childrenMu.Lock()
	w.liveChildren[coord] = cp
	w.childrenMu.Unlock()
	if w.metrics != nil {
		w.metrics.SetInflight(len(w.liveChildren))
	}
	w.wg.Add(1)
	go w.observeChild(coord, cp)
	w.logger.Info("agent: worker spawned", "job", coord.jobID, "attempt", coord.attempt, "pid", cp.pid)
	return cp
}

// observeChild waits on the child (portable Cmd.Wait) and books its exit:
// frees the ledger coordinate, removes the live entry, rolls the job's
// outcome into the agent's target metrics, and (slice 3) reports an ABNORMAL
// exit (non-zero WITHOUT a terminal status having been reported) via
// ReportChildDeath. A NORMAL exit is bookkeeping only — the worker already
// reported its terminal status to the API.
func (w *Agent) observeChild(coord spawnCoordinate, cp *childProcess) {
	defer w.wg.Done()
	err := cp.cmd.Wait()
	close(cp.exited)
	w.childrenMu.Lock()
	delete(w.liveChildren, coord)
	w.childrenMu.Unlock()
	w.freeLedger(coord)
	w.refreshInflight()
	// Roll the supervised job's outcome into the AGENT's target metrics
	// (F-30.4): the agent is the durable target representative; the worker
	// is ephemeral and exports no /metrics of its own.
	exitCode := 0
	if err != nil {
		exitCode = -1
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exitCode = ee.ExitCode()
		}
	}
	w.rollUpCompletedJob(coord, cp, exitCode)
	// FALLBACK FOLD (F-30.4): only when the worker wrote to its OWN file
	// (the agent's stdout was not a regular file): append it to the agent's
	// log for continuity. Skipped when the worker appended live to the
	// agent's log (wl == dest: nothing to fold).
	if wl := cp.workerLog; wl != "" && wl != agentLogDest() {
		w.foldWorkerLog(wl)
	}
	if err == nil {
		w.logger.Info("agent: worker exited", "job", coord.jobID, "attempt", coord.attempt, "pid", cp.pid)
		return
	}
	// Abnormal exit (slice 3 escalates this to ReportChildDeath; for now the
	// worker's absence of a terminal report is reaped by the F-03 watchdog —
	// the loud log keeps it diagnosable meanwhile).
	w.logger.Warn("agent: worker died abnormally", "job", coord.jobID, "attempt", coord.attempt, "pid", cp.pid, "exit_code", exitCode, "err", err)
}

// classifyExit maps a worker's exit code to the job's terminal status name
// (F-30.4): the worker encodes its outcome in the exit code (0 succeeded,
// 1 failed, 3 cancelled, 4 timed_out). Anything else (a signaled/killed
// child, a Go panic — code 2, a crash) is classified failed — slice 3
// sharpens the abnormal-death case via ReportChildDeath.
func classifyExit(exitCode int) string {
	switch exitCode {
	case 0:
		return "succeeded"
	case 1:
		return "failed"
	case 3:
		return "cancelled"
	case 4:
		return "timed_out"
	default:
		return "failed"
	}
}

// rollUpCompletedJob records a supervised job's outcome into the agent's own
// target metrics (F-30.4): the job counter (by terminal status) and its
// wall-clock duration (spawn → exit). Nil-safe (recording is a no-op when
// metrics are disabled).
func (w *Agent) rollUpCompletedJob(coord spawnCoordinate, cp *childProcess, exitCode int) {
	if w.metrics == nil {
		return
	}
	w.metrics.RecordJob(classifyExit(exitCode), time.Since(cp.spawnedAt))
}

// refreshInflight recomputes the in-flight gauge from the live-child count
// (nil-safe).
func (w *Agent) refreshInflight() {
	w.childrenMu.Lock()
	n := len(w.liveChildren)
	w.childrenMu.Unlock()
	if w.metrics != nil {
		w.metrics.SetInflight(n)
	}
}

// agentLogDest resolves the REGULAR FILE backing the agent's stdout (fd 1)
// — the harness / service manager redirects it (e.g. to agent.log). Services
// log to stdout by default (CDROM_LOG_FILE is OPTIONAL), so fd1 is the
// authoritative destination. Returns "" when fd1 is not a regular file
// (pipes, sockets, journals — there is no file to fold into).
func agentLogDest() string {
	link, err := os.Readlink("/proc/self/fd/1")
	if err != nil {
		return ""
	}
	switch {
	case strings.HasPrefix(link, "pipe:"), strings.HasPrefix(link, "socket:"), link == "/dev/null":
		return ""
	}
	fi, err := os.Stat(link)
	if err != nil || !fi.Mode().IsRegular() {
		return ""
	}
	return link
}

// workerLogPath is the worker's OWN log file (F-30.4): BESIDE THE AGENT'S
// LOG when the agent's stdout is a file (the norm: harness redirects, log
// shippers), else in the OS temp dir. Giving the worker a dedicated file is
// ROBUST to however the agent's stdio is wired and keeps the worker's volume
// out of the agent's own stream; the agent APPENDS the file to its log after
// the child exits (appendWorkerLog).
func workerLogPath(jobID int64, nonce string) string {
	dir := os.TempDir()
	if dest := agentLogDest(); dest != "" {
		if d := filepath.Dir(dest); d != "" {
			dir = d
		}
	}
	return filepath.Join(dir, fmt.Sprintf("cdrom.worker-%d-%s.log", jobID, nonce))
}

// randHex returns 8 hex chars of randomness (filenames for co-temporal
// worker logs).
func randHex() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return strconv.Itoa(int(time.Now().UnixNano()))
	}
	return hex.EncodeToString(b)
}

// foldWorkerLog appends the worker's OWN log file to the agent's log file
// (the fallback continuity path, F-30.4: used only when the worker could not
// append live to the agent's log — the agent's stdout was a pipe/socket).
// Best-effort: no-op when the agent's stdout is not a regular file.
func (w *Agent) foldWorkerLog(file string) {
	if file == "" {
		return
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return
	}
	dest := agentLogDest()
	if dest == "" {
		return
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		w.logger.Warn("agent: append worker log", "dst", dest, "err", err)
		return
	}
	defer f.Close()
	if len(data) > 0 {
		if _, err := f.Write(append(data, '\n')); err != nil {
			w.logger.Warn("agent: append worker log", "dst", dest, "err", err)
		}
	}
	// Remove the worker's file (folded in; bounded litter).
	_ = os.Remove(file)
}

// childCount returns the number of live worker children (test observability).
func (w *Agent) childCount() int {
	w.childrenMu.Lock()
	defer w.childrenMu.Unlock()
	return len(w.liveChildren)
}

// holdings returns the launch-ledger coordinates currently held (test
// observability).
func (w *Agent) holdings() []spawnCoordinate {
	w.ledgerMu.Lock()
	defer w.ledgerMu.Unlock()
	coords := make([]spawnCoordinate, 0, len(w.ledger))
	for c := range w.ledger {
		coords = append(coords, c)
	}
	return coords
}

// setWatchMetric records the watch state (nil-safe).
func (w *Agent) setWatchMetric(state string) {
	if w.metrics != nil {
		w.metrics.SetWatchState(state)
	}
}

// recordPoll records a poll outcome (nil-safe).
func (w *Agent) recordPoll(kind string) {
	if w.metrics != nil {
		w.metrics.RecordPoll(kind)
	}
}

// cancelJob NOTICEs the cancellation of a job running on this agent (F-05).
// SLICE 1 SEMANTICS: the relay is ADVISORY AND NON-DESTRUCTIVE — the agent
// does NOT kill the worker: the worker's OWN 2-second API status poll is the
// AUTHORITY (the scheduler marks the job cancelled in the database; the
// worker's next poll sees it, interrupts the running step, and reports the
// job cancelled). Killing the worker here would leave NO ONE to report the
// cancellation (the dead worker can't; the agent must not forge the worker's
// terminal status) — the job would ride out the F-03 watchdog with the WRONG
// terminal status. The destructive kill-relay (shrinking reaction latency
// further) arrives with the supervision channel (slice 2) and the
// death-reporting semantics (slice 3). A cancellation for a job that is not
// running on this agent (finished, or running elsewhere) is a no-op — the
// job's status is reconciled by the database.
func (w *Agent) cancelJob(jobID int64) {
	w.childrenMu.Lock()
	held := false
	for _, cp := range w.liveChildren {
		if cp.jobID == jobID {
			held = true
			break
		}
	}
	w.childrenMu.Unlock()
	if !held {
		w.logger.Info("agent: cancel for job not running on this agent", "job", jobID)
		return
	}
	w.logger.Info("agent: cancellation noticed; the worker's own status poll is authoritative (F-05)", "job", jobID)
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
				if w.metrics != nil {
					w.metrics.RecordHeartbeatFailure()
				}
				continue
			}
			if _, hbErr := w.deps.API.Heartbeat(ctxBeat, &apipb.HeartbeatRequest{Name: w.name}); hbErr != nil {
				w.logger.Warn("agent: heartbeat", "err", hbErr)
				if w.metrics != nil {
					w.metrics.RecordHeartbeatFailure()
				}
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
