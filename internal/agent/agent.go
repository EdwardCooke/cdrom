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
// relays cancellation OPPORTUNISTICALLY over the supervision channel (the
// worker's own API status poll remains the authoritative cancellation path,
// F-05), absorbs the child's telemetry (slice 2), and (slice 3) reports
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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/metrics"
	"cdrom/internal/target"

	"google.golang.org/protobuf/types/known/timestamppb"
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
	// ---- F-30.4: the LAUNCHER SEAM (substrate injection) ----
	// Launcher brings workers UP (the default is the local os/exec launcher
	// — see launcher_local.go; a Kubernetes launcher would emit a Pod spec
	// instead). Nil = launching DISABLED (assignments are acknowledged but
	// not executed — a misconfiguration surfaced by the WARN on every
	// assignment). Injected by cmd/agent (or a test) so the agent's
	// supervision core stays substrate-blind.
	Launcher Launcher
	// ChannelBind is the address the SUPERVISION CHANNEL listens on
	// (F-30.4 slice 2). Empty = the loopback default (127.0.0.1:0 — the
	// local-substrate posture: the channel is intra-host by construction).
	// A node-network agent (whose workers are PODS that cannot reach the
	// agent's localhost) sets this to a routable bind, e.g. ":7401" or
	// "<pod-ip>:7401".
	ChannelBind string
	// APIAddress is the API gRPC address handed to the worker (CDROM_API_ADDR).
	APIAddress string
	// TLSCAFile is the CA bundle path handed to the worker (CDROM_TLS_CA_FILE)
	// so it verifies the API's server certificate. Empty = plaintext leg.
	TLSCAFile string
	// WorkerRoot is the worker's staged-work-area base (CDROM_WORKER_ROOT);
	// empty lets the worker default to the OS temp dir.
	WorkerRoot string
	// ---- F-30.4 slice 3: graceful-stop ordering + orphan adoption ----
	// AdoptionHorizon bounds the boot-time ADOPTION sweep (item B2): an
	// orphan older than this is deemed DEAD (its worker is gone) and is
	// failed via ReportChildDeath instead of awaited. Zero/negative falls
	// back to the default (1 hour).
	AdoptionHorizon time.Duration
	// ShutdownGrace is the STOP drain horizon (stop ordering: drain →
	// interrupt → kill). Zero/negative falls back to the default (30 s).
	ShutdownGrace time.Duration
	// ShutdownKillGrace is the stop INTERRUPT horizon. Zero/negative falls
	// back to the default (5 s).
	ShutdownKillGrace time.Duration
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
	// CLAIMED (successfully started + spawned, or ADOPTED from a
	// predecessor). Duplication safety rests on SERIALITY PER COORDINATE —
	// not on "the process is serial" (parallel children are lawful; F-16's
	// durable admission bounds the group's concurrency): a duplicate
	// delivery of an already-held coordinate is DROPPED here (before hitting
	// the API); the database service defends the same invariant TWICE
	// server-side (defense in depth) — the duplicate-EPIISODE guard (counts
	// existing executions) and the unique ACTIVE-coordinate index on
	// job_executions (rejects two ACTIVE rows for one coordinate — the
	// racers' edge). Coordinates are FREEd when the child exits (or the
	// terminal report arrives for an adopted orphan) so a RETRY (a fresh
	// attempt) or a RERUN (reset to pending) can legitimately re-enter.
	ledgerMu sync.Mutex
	ledger   map[spawnCoordinate]struct{}

	// LIVE CHILDREN (F-30.4): the worker subprocesses currently running,
	// keyed by coordinate. The agent observes their liveness (Cmd.Wait) and
	// relays cancellation OPPORTUNISTICALLY over the supervision channel
	// (Signals) when a WatchJobs JobCancellation arrives; the worker's own
	// API poll remains authoritative (F-05).
	childrenMu   sync.Mutex
	liveChildren map[spawnCoordinate]Child
	wg           sync.WaitGroup // outstanding child-wait goroutines

	// deadlineMu protects the auto-renew loop's learned row deadline (F-30.3
	// S3): the renew response EXTENDS the row, and the loop schedules its next
	// wake off that deadline (minus the renew-ahead window).
	deadlineMu   sync.Mutex
	nextDeadline time.Time
	hasDeadline  bool

	// channel is the LOCAL SUPERVISION CHANNEL (F-30.4 slice 2): the loopback
	// gRPC server the worker subprocesses dial (Signals + Telemetry). Nil when
	// the listen failed (degraded: workers run channel-less; the agent falls
	// back to exit-code inference for their metrics).
	channel *channelServer

	// ADOPTED ORPHANS (F-30.4 slice 3, item B2): the coordinates this
	// INSTANCE adopted from a predecessor (discovered at boot; the worker
	// reconnects via the resume lane and its telemetry lands here). Freed
	// when the terminal report arrives (observeChild) or the sweep gives up.
	adoptMu   sync.Mutex
	adopted   map[spawnCoordinate]bool
	sweepDone chan struct{} // closed when the adoption sweep has run (once)
}

// spawnCoordinate is the ledger/livespace key: one worker per (job, attempt).
type spawnCoordinate struct {
	jobID   int64
	attempt int32
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
	if deps.Launcher == nil || !deps.Launcher.Enabled() {
		logger.Warn("agent: no worker launcher configured; assignments will NOT be executed",
			"hint", "set worker_binary (CDROM_AGENT_WORKER_BINARY) to the worker executable")
	}
	a := &Agent{
		name:         name,
		group:        group,
		environments: environments,
		labels:       labels,
		deps:         deps,
		logger:       logger,
		ledger:       make(map[spawnCoordinate]struct{}),
		liveChildren: make(map[spawnCoordinate]Child),
		adopted:      make(map[spawnCoordinate]bool),
		sweepDone:    make(chan struct{}),
	}
	// Boot the supervision channel (F-30.4 slice 2): a listener on the
	// CONFIGURED BIND (default loopback — the local-substrate posture),
	// advertised to each worker via CDROM_PARENT_GRPC_ADDR. Admittance is the
	// LIVE-CHILD check (the launch ledger is the custody proof). A bind
	// failure DEGRADES the agent (no channel; exit-code inference remains).
	a.channel = newChannelServer(deps.ChannelBind, a.admitChannelWorker, logger)
	if err := a.channel.start(); err != nil {
		logger.Warn("agent: supervision channel disabled (listen failed); workers will run channel-less", "err", err)
		a.channel = nil
	} else {
		// ADOPTED SETTLEMENT (item B2): an adopted worker has no local child
		// to Observe — its final Telemetry flush is the completion signal.
		a.channel.setAdoptedSettler(a.settleAdopted)
		logger.Info("agent: supervision channel listening", "addr", a.channel.Addr())
	}
	return a
}

// settleAdopted discharges an ADOPTED coordinate whose worker completed
// (item B2): the final JobSample rode the Telemetry stream to its end —
// consume the account (the rolled-up metrics land in THIS instance's series
// — the fresh instance represents the lineage), free the ledger, and drop
// the adoption mark. Guards: only coordinates this instance ADOPTED (a
// live child settles via observeChild; a foreign final is ignored).
func (w *Agent) settleAdopted(coord spawnCoordinate) {
	if !w.isAdopted(coord) {
		return
	}
	account := w.takeChildAccount(coord)
	if account != nil && account.finalSeen && w.metrics != nil {
		w.metrics.RecordJob(account.finalStatus, account.finalDur)
	}
	if account != nil && w.metrics != nil {
		for _, st := range account.steps {
			w.metrics.RecordStep(st.stepType, st.status, st.dur)
		}
		if account.logBytes > 0 {
			w.metrics.AddLogBytes(account.logBytes)
		}
		for i := uint32(0); i < account.cancelAcks; i++ {
			w.metrics.RecordCancelSignalAck()
		}
	}
	w.freeLedger(coord)
	w.releaseAdopted(coord)
	w.refreshInflight()
	w.logger.Info("agent: adopted worker completed; account absorbed", "job", coord.jobID, "attempt", coord.attempt)
}

// isAdopted reports whether the coordinate is one the agent ADOPTED from a
// predecessor (item B2): the resume lane admits the worker's reconnect for
// it (custody transferred to this instance).
func (w *Agent) isAdopted(coord spawnCoordinate) bool {
	w.adoptMu.Lock()
	defer w.adoptMu.Unlock()
	return w.adopted[coord]
}

// markAdopted records the coordinate as adopted (idempotent).
func (w *Agent) markAdopted(coord spawnCoordinate) {
	w.adoptMu.Lock()
	defer w.adoptMu.Unlock()
	w.adopted[coord] = true
}

// releaseAdopted drops the coordinate from the adoption set (the terminal
// report arrived — observation duty discharged).
func (w *Agent) releaseAdopted(coord spawnCoordinate) {
	w.adoptMu.Lock()
	defer w.adoptMu.Unlock()
	delete(w.adopted, coord)
}

// isLiveChild reports whether the coordinate is one of the agent's live
// children.
func (w *Agent) isLiveChild(coord spawnCoordinate) bool {
	w.childrenMu.Lock()
	defer w.childrenMu.Unlock()
	_, ok := w.liveChildren[coord]
	return ok
}

// admitChannelWorker is the supervision channel's CONNECT-JOB ADMISSANCE
// predicate (two lanes, F-30.4 slice 2 + the k8s-resume story):
//
//   - COLD: the coordinate is one of the agent's LIVE CHILDREN (the launch
//     ledger is the custody proof — a worker associates only with a job the
//     agent actually spawned).
//   - RESUME: the agent RESTARTED mid-job and no longer holds the live
//     child (its in-memory state is gone). The worker reconnects to the
//     fresh instance; admission then rests on the TICKET'S CONSISTENCY with
//     this agent — the coordinator names a real job (jobID != 0) and the
//     token's agent_name claim EQUALS THE AGENT'S OWN REGISTERED NAME. The
//     token is IdP-signed and was SPENT against the API (GetJob / reports),
//     so a forged one is worthless: it is the custody proof tying the
//     worker to this agent lineage. (Observational only — the job's fate
//     never depended on the channel; the resumed worker re-establishes the
//     relay + telemetry. Orphaned-OBSERVATION adoption — rolling the
//     rejoined child's metrics into THIS instance's series — ships with
//     Slice 3.)
//
// Returns (ok, resumed). ok=false is PERMANENT (the worker runs channel-less
// for its life; retrying cannot help).
func (w *Agent) admitChannelWorker(coord spawnCoordinate, ticketAgentName, jobToken string) (bool, bool) {
	if w.isLiveChild(coord) {
		return true, false
	}
	// Adopted lane (item B2): the coordinate is one this instance ADOPTED
	// from a predecessor — the strongest custody proof (we took the duty).
	if w.isAdopted(coord) {
		return true, true
	}
	// Resume lane (item A): the ticket NAMES A REAL JOB and its token's
	// agent_name claim EQUALS THIS AGENT'S OWN NAME.
	if !(coord.jobID != 0 && ticketAgentName != "") {
		return false, false
	}
	// ROUTABLE POSTURE (item B3): the unsigned decode is WEAK across the
	// network (any valid token minted for this agent would pass). When the
	// channel escapes loopback, VERIFY the token's signature via the API's
	// attestation (the agent holds no JWKS — the API does). Loopback keeps
	// the cheap decode (intra-host: holding a valid token for this agent
	// implies owning one of its workers).
	if w.routableChannel() {
		valid, name, enabled := w.verifyTicketViaAPI(coord, jobToken)
		if !enabled {
			// Attestation unavailable (job-token auth disabled, or a
			// transient API failure): fall back to the unsigned decode —
			// the weak-but-functional posture (a refusal here would sever
			// a rightful worker's telemetry for no security gain).
			return ticketAgentName == w.name, true
		}
		return valid && name == w.name, true
	}
	return ticketAgentName == w.name, true
}

// routableChannel reports whether the channel's bind ESCAPEs loopback (the
// posture where the unsigned decode is unsafe — item B3).
func (w *Agent) routableChannel() bool {
	if w.channel == nil {
		return false
	}
	addr := w.channel.Addr()
	return !strings.HasPrefix(addr, "127.") && !strings.HasPrefix(addr, "::1") && !strings.HasPrefix(addr, "[::1]")
}

// verifyTicketViaAPI asks the API to attest the ticket's token (item B3):
// signature + audience + job binding + non-terminal job, echoing the
// VERIFIED agent_name claim. Returns (valid, name, enabled) — enabled=false
// means attestation is unavailable (fall back to the unsigned decode).
func (w *Agent) verifyTicketViaAPI(coord spawnCoordinate, jobToken string) (valid bool, name string, enabled bool) {
	ctx, err := w.credentialCtx(context.Background())
	if err != nil {
		return false, "", false
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := w.deps.API.VerifyJobToken(callCtx, &apipb.VerifyJobTokenRequest{
		JobId: coord.jobID,
		Token: jobToken,
	})
	if err != nil {
		w.logger.Warn("agent: verify ticket (resume lane)", "job", coord.jobID, "err", err)
		return false, "", false
	}
	return resp.GetValid(), resp.GetAgentName(), resp.GetEnabled()
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

	// ADOPTION SWEEP (F-30.4 slice 3, item B2): discover this lineage's
	// still-running executions (orphans of a predecessor incarnation) and
	// adopt observation duty for each. Runs in the BACKGROUND (the boot
	// must not stall on the API); Stop WAITS for it (a stopped agent must
	// not leave adopted duties dangling).
	go w.adoptionSweep(ctx)

	defer func() {
		// Graceful-stop ordering (F-30.4 slice 3): drain → interrupt →
		// kill → deregister → tear down the channel LAST (a draining
		// child's final flush must still reach the agent). The horizon
		// sums the phases (drain + interrupt + the final reap margin).
		drain, kill := w.stopHorizons()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), drain+kill+10*time.Second)
		defer stopCancel()
		w.Stop(stopCtx)
		if _, err := w.deps.API.DeregisterAgent(context.WithoutCancel(ctx), &apipb.DeregisterAgentRequest{Name: w.name}); err != nil {
			w.logger.Warn("agent: deregister", "err", err)
		}
		w.channel.stop()
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

// defaultStopHorizons are the graceful-stop fallbacks (F-30.4 slice 3): the
// drain horizon (let live children FINISH) and the interrupt horizon (give
// the polite signal room to bite before the kill).
const (
	defaultShutdownGrace     = 30 * time.Second
	defaultShutdownKillGrace = 5 * time.Second
)

// stopHorizons resolves the configured stop horizons (fallbacks on
// zero/negative).
func (w *Agent) stopHorizons() (drain, kill time.Duration) {
	drain = w.deps.ShutdownGrace
	if drain <= 0 {
		drain = defaultShutdownGrace
	}
	kill = w.deps.ShutdownKillGrace
	if kill <= 0 {
		kill = defaultShutdownKillGrace
	}
	return drain, kill
}

// Stop implements the graceful-stop ORDERING (F-30.4 slice 3):
//
//  1. DRAIN — wait for the live children to finish on their own, up to the
//     drain horizon (their final flushes + terminal reports still land);
//  2. INTERRUPT — politely signal the survivors (os.Interrupt: SIGINT on
//     unix; the child's own group on Windows), giving them the kill horizon
//     to react (a cooperative worker traps it and exits CLEANLY — code 1);
//  3. KILL — force-stop whatever survives (the worker cannot trap SIGKILL);
//  4. WAIT — reap the observation goroutines (bounded by the horizons above,
//     so a wedged child cannot wedge the shutdown).
//
// The channel is intentionally LEFT UP until the agent's Run defers its
// teardown — a draining child's final flush must still reach this agent.
// Safe to call with no children (idle).
func (w *Agent) Stop(ctx context.Context) {
	drain, kill := w.stopHorizons()
	if w.childCount() == 0 {
		return
	}
	w.logger.Info("agent: shutting down; draining live children", "count", w.childCount(), "horizon", drain.String())
	select {
	case <-w.allExited():
		w.logger.Info("agent: drained; all children exited")
		return
	case <-ctx.Done():
	case <-time.After(drain):
	}
	if w.childCount() == 0 {
		return
	}
	w.logger.Info("agent: drain expired; interrupting survivors", "count", w.childCount())
	for _, c := range w.liveSnapshot() {
		c.Interrupt()
	}
	interrupted := make(chan struct{})
	go func() {
		w.waitForChildren(kill)
		close(interrupted)
	}()
	select {
	case <-interrupted:
		return
	case <-time.After(kill):
	}
	if w.childCount() == 0 {
		return
	}
	w.logger.Warn("agent: interrupt ignored; killing leftovers", "count", w.childCount())
	for _, c := range w.liveSnapshot() {
		c.Terminate()
	}
	w.waitForChildren(10 * time.Second)
}

// liveSnapshot returns the current live children (a copy — iteration must
// not hold the map lock).
func (w *Agent) liveSnapshot() []Child {
	w.childrenMu.Lock()
	defer w.childrenMu.Unlock()
	out := make([]Child, 0, len(w.liveChildren))
	for _, c := range w.liveChildren {
		out = append(out, c)
	}
	return out
}

// waitForChildren awaits the observation goroutines (bounded).
func (w *Agent) waitForChildren(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

// allExited is a fresh channel closed when wg empties (waits for the CURRENT
// cohort; a new child spawned afterwards is not waited on — shutdown does
// not race new work).
func (w *Agent) allExited() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	return done
}

// adoptionSweep (F-30.4 slice 3, item B2): at boot, discover this LINEAGE's
// still-running executions (the orphans of a predecessor incarnation — the
// API's blindness to restarts is cured by retiring the register-time
// AbandonAgentExecutions) and ADOPT observation duty for each young one:
// reserve the ledger coordinate (a redelivery of the same job is dropped —
// the worker is still running) and await the worker's CONNECT (resume lane)
// or its terminal report. Orphans OLDER than the horizon are deemed DEAD
// (their worker is gone): fail them via ReportChildDeath (the fast-failure
// escape hatch) instead of awaiting forever. Runs once; Stop waits for it.
func (w *Agent) adoptionSweep(ctx context.Context) {
	defer close(w.sweepDone)
	// Brief settle: the registration (and its credential exchange) happens
	// moments before Run returns to its loops; give the API a beat to have
	// processed it so the sweep's identity call is not racing the
	// registration's own writes.
	select {
	case <-ctx.Done():
		return
	case <-time.After(2 * time.Second):
	}
	ctx, err := w.credentialCtx(ctx)
	if err != nil {
		w.logger.Warn("agent: adoption sweep: credential unavailable; skipping", "err", err)
		return
	}
	resp, err := w.deps.API.DiscoverAgentExecutions(ctx, &dbpb.DiscoverAgentExecutionsRequest{AgentName: w.name})
	if err != nil {
		w.logger.Warn("agent: adoption sweep: discover", "err", err)
		return
	}
	if len(resp.GetExecutions()) == 0 {
		w.logger.Info("agent: adoption sweep: no orphans (fresh lineage)")
		return
	}
	horizon := w.deps.AdoptionHorizon
	if horizon <= 0 {
		horizon = defaultAdoptionHorizon
	}
	now := time.Now().UTC()
	for _, ex := range resp.GetExecutions() {
		coord := spawnCoordinate{jobID: ex.GetJobId(), attempt: ex.GetAttempt()}
		age := now.Sub(startedAtOf(ex.GetStartedAt()))
		if age > horizon {
			w.logger.Warn("agent: adoption sweep: orphan too old; failing it",
				"job", coord.jobID, "attempt", coord.attempt, "age", age.Round(time.Second))
			w.failDeadOrphan(ctx, coord, "predecessor's worker presumed dead (adoption horizon exceeded)")
			continue
		}
		if !w.reserveLedger(coord) {
			// A live child already holds it (should not happen at boot —
			// the ledger is empty then; belt).
			continue
		}
		w.markAdopted(coord)
		w.logger.Info("agent: ADOPTED orphaned execution (observation duty)",
			"job", coord.jobID, "attempt", coord.attempt, "age", age.Round(time.Second))
	}
}

// defaultAdoptionHorizon bounds how old an orphan may be for the sweep to
// AWAIT it (versus deem its worker dead and fail it). Generous: a long
// build/probe step is LEGITIMATE; the horizon trips only for orphans older
// than any sane foreground job (and the F-03 watchdog would have reaped
// them eventually anyway).
const defaultAdoptionHorizon = time.Hour

// failDeadOrphan reports a dead orphan's execution via ReportChildDeath
// (the fast-failure escape hatch) — best-effort (the F-03 watchdog is the
// ultimate backstop).
func (w *Agent) failDeadOrphan(ctx context.Context, coord spawnCoordinate, reason string) {
	failCtx, err := w.credentialCtx(ctx)
	if err != nil {
		w.logger.Warn("agent: fail dead orphan: credential unavailable", "err", err)
		return
	}
	if _, err := w.deps.API.ReportChildDeath(failCtx, &apipb.ReportChildDeathRequest{
		JobId:     coord.jobID,
		AgentName: w.name,
		Reason:    reason,
	}); err != nil {
		w.logger.Warn("agent: fail dead orphan", "job", coord.jobID, "err", err)
		return
	}
	w.freeLedger(coord)
	w.releaseAdopted(coord)
}

// startedAtOf extracts a proto Timestamp's time (zero when absent).
func startedAtOf(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
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
// is RELAYED to the worker over the supervision channel (opportunistically)
// and noticed in the agent's log (see cancelJob) — the worker's own status
// poll remains the authority (F-05); one delivered for a job that is not
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
	child := w.launchWorker(coord, resp.GetJobToken())
	if child == nil {
		// Launch failed (launcher disabled, exec error): FREE the reservation
		// so a redelivery (nudge/poll) can retry — the execution exists in
		// the DB (the start succeeded) and the worker WILL claim it.
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

// launchWorker brings a worker UP via the injected LAUNCHER (F-30.4 seam):
// the mandate (coordinate + job token + the ambient inputs) rides the
// MISSION; the launcher decides HOW the worker is realized (a local
// subprocess with the canonical env contract, or — for a node-network agent
// — a Pod). The agent core only RESERVES the live slot and OBSERVES the
// resulting Child; it never inspects the substrate.
func (w *Agent) launchWorker(coord spawnCoordinate, jobToken string) Child {
	launcher := w.deps.Launcher
	if launcher == nil || !launcher.Enabled() {
		w.logger.Warn("agent: worker launcher not configured; job NOT executed", "job", coord.jobID, "attempt", coord.attempt)
		return nil
	}
	child, err := launcher.Launch(Mission{
		Coord:       coord,
		JobToken:    jobToken,
		APIAddress:  w.deps.APIAddress,
		TLSCAFile:   w.deps.TLSCAFile,
		WorkerRoot:  w.deps.WorkerRoot,
		ChannelAddr: w.channelAddr(),
	})
	if err != nil {
		w.logger.Error("agent: launch worker", "job", coord.jobID, "attempt", coord.attempt, "err", err)
		return nil
	}
	w.childrenMu.Lock()
	w.liveChildren[coord] = child
	w.childrenMu.Unlock()
	if w.metrics != nil {
		w.metrics.SetInflight(len(w.liveChildren))
	}
	w.wg.Add(1)
	go w.observeChild(coord, child)
	w.logger.Info("agent: worker launched", "job", coord.jobID, "attempt", coord.attempt, "child_id", child.ID())
	return child
}

// channelAddr is the supervision channel's advertised address ("" when the
// channel is degraded — the worker then runs channel-less and the agent
// infers its metrics from the exit code).
func (w *Agent) channelAddr() string {
	if w.channel == nil {
		return ""
	}
	return w.channel.Addr()
}

// observeChild waits on the child (via Child.Observe — substrate-agnostic)
// and books its exit: frees the ledger coordinate, removes the live entry,
// rolls the job's outcome into the agent's target metrics, and (slice 3)
// reports an ABNORMAL exit (non-zero WITHOUT a terminal status having been
// reported) via ReportChildDeath. A NORMAL exit is bookkeeping only — the
// worker already reported its terminal status to the API.
func (w *Agent) observeChild(coord spawnCoordinate, child Child) {
	defer w.wg.Done()
	err := child.Observe()
	w.childrenMu.Lock()
	delete(w.liveChildren, coord)
	w.childrenMu.Unlock()
	w.freeLedger(coord)
	// Observation duty discharged (item B2): whether the child was born of
	// this instance or ADOPTED from a predecessor, its terminal outcome has
	// now been booked — drop the adoption mark.
	w.releaseAdopted(coord)
	w.refreshInflight()
	// Roll the supervised job's outcome into the AGENT's target metrics
	// (F-30.4): the agent is the durable target representative; the worker
	// is ephemeral and exports no /metrics of its own.
	exitCode := child.ExitCode()
	// Book the child's outcome into the agent's metrics (F-30.4). The
	// worker's pre-exit FINAL FLUSH (supervision channel, slice 2) deposits
	// its EXACT status + duration; when present it SUPERSEDES the exit-code
	// inference (one record, not two). Nil account (channel down, worker
	// died before flushing) ⇒ the exit-code roll-up stands. Either way the
	// child's streamed log bytes fold into the agent's own
	// cdrom_agent_log_bytes_total.
	account := w.takeChildAccount(coord)
	if account != nil && account.finalSeen && w.metrics != nil {
		w.metrics.RecordJob(account.finalStatus, account.finalDur)
	} else {
		w.rollUpCompletedJob(child, exitCode)
	}
	// Absorb the rest of the child's account (F-30.4 slice 2): its STEP
	// outcomes land in the agent's own cdrom_agent_steps_total / step
	// durations (they cannot be inferred from the exit code — a partially
	// flushed account still contributes whatever occurred), and its streamed
	// log bytes join the agent's cdrom_agent_log_bytes_total.
	if account != nil && w.metrics != nil {
		for _, st := range account.steps {
			w.metrics.RecordStep(st.stepType, st.status, st.dur)
		}
		if account.logBytes > 0 {
			w.metrics.AddLogBytes(account.logBytes)
		}
		for i := uint32(0); i < account.cancelAcks; i++ {
			w.metrics.RecordCancelSignalAck()
		}
	}
	// FALLBACK FOLD (F-30.4): only when the worker wrote to its OWN file
	// (the agent's stdout was not a regular file): append it to the agent's
	// log for continuity. Skipped when the worker appended live to the
	// agent's log (wl == dest: nothing to fold).
	if wl := child.WorkerLog(); wl != "" && wl != agentLogDest() {
		w.foldWorkerLog(wl)
	}
	if err == nil {
		w.logger.Info("agent: worker exited", "job", coord.jobID, "attempt", coord.attempt, "child_id", child.ID())
		return
	}
	// Abnormal exit (F-30.4 slice 3): ESCALATE to ReportChildDeath — the
	// worker will never report its terminal status, so the agent (the
	// execution's oracle) fails the EXECUTION now: the job fails fast
	// instead of riding out the F-03 watchdog. F-05 immunity is honored
	// server-side (an execution already terminal — the worker beat the
	// death — is left as-is). Best-effort: a failed report degrades to the
	// watchdog's eventual reap.
	w.logger.Warn("agent: worker died abnormally", "job", coord.jobID, "attempt", coord.attempt, "child_id", child.ID(), "exit_code", exitCode, "err", err)
	w.reportChildDeath(coord, fmt.Sprintf("worker exited abnormally (exit_code=%d: %v)", exitCode, err))
}

// reportChildDeath tells the API the child died abnormally (F-30.4 slice 3):
// the API validates the agent serviced the job and fails the execution
// (honoring F-05's terminal immunity). Identity verb: the agent credential.
// Best-effort — a failure here degrades to the F-03 watchdog's reap.
func (w *Agent) reportChildDeath(coord spawnCoordinate, reason string) {
	ctx, err := w.credentialCtx(context.Background())
	if err != nil {
		w.logger.Warn("agent: report child death: credential unavailable", "job", coord.jobID, "err", err)
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := w.deps.API.ReportChildDeath(callCtx, &apipb.ReportChildDeathRequest{
		JobId:     coord.jobID,
		AgentName: w.name,
		Reason:    reason,
	})
	if err != nil {
		w.logger.Warn("agent: report child death", "job", coord.jobID, "err", err)
		return
	}
	if resp.GetFailed() {
		w.logger.Info("agent: child death reported; execution failed", "job", coord.jobID, "attempt", coord.attempt)
	} else {
		w.logger.Info("agent: child death reported; execution already terminal (immune)", "job", coord.jobID, "attempt", coord.attempt)
	}
}

// takeChildAccount retrieves (and deletes) the child's accumulated telemetry
// account from the supervision channel (F-30.4 slice 2). Nil when the agent
// has no channel or the child deposited nothing.
func (w *Agent) takeChildAccount(coord spawnCoordinate) *childAccount {
	return w.channel.takeAccount(coord)
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
func (w *Agent) rollUpCompletedJob(child Child, exitCode int) {
	if w.metrics == nil {
		return
	}
	w.metrics.RecordJob(classifyExit(exitCode), time.Since(child.SpawnedAt()))
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
// RELAY SEMANTICS (SLICE 2): the cancellation is conveyed OPPORTUNISTICALLY
// over the supervision channel (Signals) — the worker reacts quickly when
// connected — but the worker's OWN 2-second API status poll remains the
// AUTHORITY (F-05): the scheduler marks the job cancelled in the database,
// and the worker's next poll sees it even when the channel is down. The
// agent does NOT kill the worker: killing it would leave NO ONE to report
// the cancellation (the dead worker can't; the agent must not forge the
// worker's terminal status) — the job would ride out the F-03 watchdog with
// the WRONG terminal status. The destructive interrupt (reacting to the
// worker's acknowledgment, escalating to a kill) ships with slice 3's
// graceful-stop ordering. A cancellation for a job that is not running on
// this agent (finished, or running elsewhere) is a no-op — the job's status
// is reconciled by the database.
func (w *Agent) cancelJob(jobID int64) {
	// Opportunistic early notice over the channel (non-blocking; a worker
	// that isn't subscribed — channel degraded, or already finishing —
	// relies on its own status poll).
	w.broadcastCancellationForJob(jobID)
	w.childrenMu.Lock()
	held := false
	for coord := range w.liveChildren {
		if coord.jobID == jobID {
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

// broadcastCancellationForJob pushes the opportunistic cancellation notice to
// every subscribed worker of the job's live coordinates (F-30.4 slice 2).
func (w *Agent) broadcastCancellationForJob(jobID int64) {
	if w.channel == nil {
		return
	}
	w.childrenMu.Lock()
	coords := make([]spawnCoordinate, 0, 1)
	for coord := range w.liveChildren {
		if coord.jobID == jobID {
			coords = append(coords, coord)
		}
	}
	w.childrenMu.Unlock()
	for _, coord := range coords {
		w.channel.broadcastCancellation(coord)
	}
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
