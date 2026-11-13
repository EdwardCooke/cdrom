package agent

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// testLogger returns a silent logger so test output stays clean.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fakeWatchStream is a grpc.ServerStreamingClient[WatchMessage] backed by a
// channel: the test pushes WatchMessages into the channel and the agent's
// watch loop receives them. It embeds a nil grpc.ClientStream to satisfy the
// interface's other methods (Context, Header, Trailer, SendMsg, RecvMsg,
// CloseSend), which the agent never calls.
type fakeWatchStream struct {
	grpc.ClientStream
	ch chan *apipb.WatchMessage
}

func (f *fakeWatchStream) Recv() (*apipb.WatchMessage, error) {
	msg, ok := <-f.ch
	if !ok {
		return nil, context.Canceled // the test closed the feed
	}
	return msg, nil
}

// coordKey identifies a (job, attempt) coordinate the fake's EMULATED
// DUPLICATE-START GUARD (the database service's episode guard, F-23 flake
// 2026-11-07) remembers.
type coordKey struct {
	job int64
	att int32
}

// fakeSupervisorAPI is a stub APIClient for SUPERVISION tests (F-30.4): the
// agent no longer executes jobs in-process, so the fake models the verbs the
// SUPERVISOR uses — StartJobExecution (with the DB guard's duplicate
// declination emulated), WatchJobs (fed by the test), and the lifecycle
// no-ops. It records the agent name each start claimed.
type fakeSupervisorAPI struct {
	apipb.APIClient // nil; only the methods below are used by the agent
	mu              sync.Mutex
	watchCh         chan *apipb.WatchMessage
	starts          int
	accepted        int
	declined        int
	seenCoords      map[coordKey]bool
	lastAgent       string
	// F-30.4 slice 3: the adoption sweep's discoveries + the death-report
	// calls (stubbable per test).
	discoveredOrphans []*dbpb.JobExecution
	reportDeaths      []int64
	verifyTokens      []string
	// VerifyJobToken seeds (item B3): job → the token string the fake will
	// attest as valid; verifyNameFor is the attested agent_name (default
	// "w1" — the supervised agent's name).
	validTickets  map[int64]string
	verifyNameFor string
}

// DiscoverAgentExecutions feeds the adoption sweep (nil = no orphans).
func (f *fakeSupervisorAPI) DiscoverAgentExecutions(ctx context.Context, in *dbpb.DiscoverAgentExecutionsRequest, opts ...grpc.CallOption) (*dbpb.DiscoverAgentExecutionsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &dbpb.DiscoverAgentExecutionsResponse{Executions: f.discoveredOrphans}, nil
}

// ReportChildDeath records the escalated death (the fake plays the API's
// oracle: it trusts the presenter).
func (f *fakeSupervisorAPI) ReportChildDeath(ctx context.Context, in *apipb.ReportChildDeathRequest, opts ...grpc.CallOption) (*apipb.ReportChildDeathResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reportDeaths = append(f.reportDeaths, in.GetJobId())
	return &apipb.ReportChildDeathResponse{Failed: true}, nil
}

// VerifyJobToken plays the API's verifier: a token is VALID iff it is
// seeded in validTickets (mapping job → the token string); the attested
// name is the SEEDING agent (tests inject the expected name via
// verifyNameFor). Records every request.
func (f *fakeSupervisorAPI) VerifyJobToken(ctx context.Context, in *apipb.VerifyJobTokenRequest, opts ...grpc.CallOption) (*apipb.VerifyJobTokenResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verifyTokens = append(f.verifyTokens, in.GetToken())
	want, ok := f.validTickets[in.GetJobId()]
	if !ok || want != in.GetToken() {
		return &apipb.VerifyJobTokenResponse{Valid: false, Enabled: true}, nil
	}
	name := f.verifyNameFor
	if name == "" {
		name = "w1"
	}
	return &apipb.VerifyJobTokenResponse{Valid: true, AgentName: name, Enabled: true}, nil
}

func (f *fakeSupervisorAPI) RegisterAgent(ctx context.Context, in *apipb.RegisterAgentRequest, opts ...grpc.CallOption) (*apipb.Agent, error) {
	return &apipb.Agent{Name: in.GetName(), Group: in.GetGroup()}, nil
}

func (f *fakeSupervisorAPI) DeregisterAgent(ctx context.Context, in *apipb.DeregisterAgentRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeSupervisorAPI) Heartbeat(ctx context.Context, in *apipb.HeartbeatRequest, opts ...grpc.CallOption) (*apipb.HeartbeatResponse, error) {
	return &apipb.HeartbeatResponse{}, nil
}

func (f *fakeSupervisorAPI) WatchJobs(ctx context.Context, in *apipb.WatchJobsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[apipb.WatchMessage], error) {
	return &fakeWatchStream{ch: f.watchCh}, nil
}

// StartJobExecution emulates the database service's duplicate-start guard:
// the FIRST start of a (job, attempt) coordinate is accepted (a job token is
// minted); repeats of the same coordinate are DECLINED (started=false) —
// exactly the behavior the agent's launch ledger complements (belt and
// braces: the ledger drops twins before/at spawn; the guard declines them
// server-side).
func (f *fakeSupervisorAPI) StartJobExecution(ctx context.Context, in *apipb.StartJobExecutionRequest, opts ...grpc.CallOption) (*apipb.StartJobExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++
	f.lastAgent = in.GetAgentName()
	const att = int32(1)
	k := coordKey{job: in.GetJobId(), att: att}
	if f.seenCoords[k] {
		f.declined++
		return &apipb.StartJobExecutionResponse{Started: false}, nil
	}
	f.seenCoords[k] = true
	f.accepted++
	return &apipb.StartJobExecutionResponse{
		Started:  true,
		JobToken: "jt-" + strconv.FormatInt(in.GetJobId(), 10),
		Attempt:  att,
	}, nil
}

func (f *fakeSupervisorAPI) ListPendingJobs(ctx context.Context, in *apipb.ListPendingJobsRequest, opts ...grpc.CallOption) (*apipb.ListPendingJobsResponse, error) {
	return &apipb.ListPendingJobsResponse{}, nil
}

// waitFor polls cond until true or the deadline elapses (helper for the
// asynchronous spawn/observe choreography).
func waitFor(t *testing.T, cond func() bool, desc string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

// newSupervisedAgent builds an agent whose worker is a CONTROLLED SHELL CHILD
// (a LocalLauncher steered by /bin/sh + args): the tests steer the child's
// lifespan and observable effects without needing the real worker binary.
func newSupervisedAgent(args []string, api *fakeSupervisorAPI) *Agent {
	watchCh := make(chan *apipb.WatchMessage, 8)
	api.watchCh = watchCh
	w := New("w1", "pool-a", nil, nil, Dependencies{
		API:        api,
		Launcher:   NewLocalLauncher("/bin/sh", args, testLogger()),
		APIAddress: "127.0.0.1:7105",
	}, testLogger())
	return w
}

// TestAgentSpawnWorkerOnAssignment verifies the F-30.4 core arc: an
// assignment on the WatchJobs stream leads the agent to claim its execution
// (StartJobExecution, presenting its name) and SPAWN the worker subprocess;
// the child's exit is observed and the ledger coordinate released.
func TestAgentSpawnWorkerOnAssignment(t *testing.T) {
	api := &fakeSupervisorAPI{seenCoords: map[coordKey]bool{}}
	w := newSupervisedAgent([]string{"-c", "sleep 0.3"}, api)
	t.Cleanup(func() { close(api.watchCh) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	api.watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: &apipb.Job{Id: 42}, AgentName: "w1"},
	}}

	// Positive wait: the worker is UP...
	waitFor(t, func() bool { return w.childCount() == 1 }, "worker spawned", 5*time.Second)
	// ...and then settles (the child exits; observation frees the ledger).
	waitFor(t, func() bool { return w.childCount() == 0 }, "child settled", 5*time.Second)
	api.mu.Lock()
	starts, accepted, agent := api.starts, api.accepted, api.lastAgent
	api.mu.Unlock()
	if starts != 1 || accepted != 1 {
		t.Errorf("starts = %d (accepted %d), want 1/1", starts, accepted)
	}
	if agent != "w1" {
		t.Errorf("start claimed agent %q, want w1", agent)
	}
	if len(w.holdings()) != 0 {
		t.Errorf("ledger still holds %v after the child exited", w.holdings())
	}
	cancel()
	<-done
}

// TestAgentEnvOnlyMandate verifies the CANONICAL ENV CONTRACT (F-30.4): the
// child's environment carries the mandate (job id + job token + API address)
// and PRIVILEGED VARIABLES ARE STRIPPED (the agent's credential file never
// propagates into the worker). The child dumps its own environ to a file for
// inspection.
func TestAgentEnvOnlyMandate(t *testing.T) {
	dir := t.TempDir()
	envDump := filepath.Join(dir, "child_environ")
	api := &fakeSupervisorAPI{seenCoords: map[coordKey]bool{}}
	w := newSupervisedAgent([]string{"-c", "env > " + envDump + "; exit 0"}, api)
	t.Cleanup(func() { close(api.watchCh) })

	// Seed a privileged-looking variable in the AGENT's environment: it must
	// NOT appear in the child's.
	t.Setenv("CDROM_AGENT_CREDENTIAL_FILE", filepath.Join(dir, "cred.jwt"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	api.watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: &apipb.Job{Id: 42}, AgentName: "w1"},
	}}

	waitFor(t, func() bool {
		_, err := os.Stat(envDump)
		return err == nil
	}, "environ dumped", 5*time.Second)
	data, err := os.ReadFile(envDump)
	if err != nil {
		t.Fatalf("read environ dump: %v", err)
	}
	env := string(data)
	for _, want := range []string{
		"CDROM_WORKER_JOB_ID=42",
		"CDROM_WORKER_JOB_TOKEN=jt-42",
		"CDROM_API_ADDR=127.0.0.1:7105",
	} {
		if !strings.Contains(env, want+"\n") && !strings.HasSuffix(strings.TrimRight(env, "\n"), want) {
			t.Errorf("child environ lacks %q", want)
		}
	}
	if strings.Contains(env, "CDROM_AGENT_CREDENTIAL_FILE") {
		t.Errorf("privileged variable leaked into the worker's environment (F-30.4 violation)")
	}
	cancel()
	<-done
}

// TestAgentDuplicateDeliverySpawnsOnce verifies the deduplication arc (F-30.4
// / F-23): THREE deliveries of the same (job, attempt) lead to at most ONE
// worker — the first start is accepted and spawns; the repeats are declined
// by the (emulated) database guard (started=false) and/or dropped by the
// launch ledger.
func TestAgentDuplicateDeliverySpawnsOnce(t *testing.T) {
	api := &fakeSupervisorAPI{seenCoords: map[coordKey]bool{}}
	// A slow child so the coordinate stays HELD across the duplicate
	// deliveries.
	w := newSupervisedAgent([]string{"-c", "sleep 0.5"}, api)
	t.Cleanup(func() { close(api.watchCh) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	deliver := func() {
		api.watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
			Assignment: &apipb.JobAssignment{Job: &apipb.Job{Id: 42}, AgentName: "w1"},
		}}
	}
	deliver()
	waitFor(t, func() bool { return w.childCount() == 1 }, "first worker spawned", 5*time.Second)
	deliver()
	deliver()
	// Allow the duplicates to be processed; the child count must stay at ONE.
	time.Sleep(300 * time.Millisecond)
	if n := w.childCount(); n != 1 {
		t.Errorf("live children = %d, want 1 (duplicate deliveries must not respawn)", n)
	}
	api.mu.Lock()
	starts, accepted, declined := api.starts, api.accepted, api.declined
	api.mu.Unlock()
	if accepted != 1 {
		t.Errorf("accepted starts = %d, want 1", accepted)
	}
	if starts < 2 || declined == 0 {
		t.Errorf("starts = %d (declined %d): duplicates were not guarded", starts, declined)
	}
	cancel()
	<-done
}

// TestAgentCancelNoticeIsNonDestructive verifies the SLICE-1 cancellation
// relay (F-05 / F-30.4): a JobCancellation on the watch stream is NOTICEd by
// the agent but does NOT kill the worker — the worker's own API status poll
// is the authority (it converges on the cancel and reports it). The child
// SURVIVES the notice.
func TestAgentCancelNoticeIsNonDestructive(t *testing.T) {
	api := &fakeSupervisorAPI{seenCoords: map[coordKey]bool{}}
	w := newSupervisedAgent([]string{"-c", "sleep 30"}, api)
	t.Cleanup(func() { close(api.watchCh) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	api.watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: &apipb.Job{Id: 42}, AgentName: "w1"},
	}}
	waitFor(t, func() bool { return w.childCount() == 1 }, "worker spawned", 5*time.Second)

	api.watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Cancellation{
		Cancellation: &apipb.JobCancellation{JobId: 42},
	}}
	// Give the notice time to be processed; the worker must STILL be alive.
	time.Sleep(300 * time.Millisecond)
	if n := w.childCount(); n != 1 {
		t.Errorf("live children = %d, want 1 (the slice-1 relay must not kill the worker)", n)
	}
	// Tear the child down ourselves (cleanup hygiene).
	w.childrenMu.Lock()
	victim := w.liveChildren[spawnCoordinate{jobID: 42, attempt: 1}]
	w.childrenMu.Unlock()
	if victim != nil {
		victim.Terminate()
		waitFor(t, func() bool { return w.childCount() == 0 }, "worker reaped", 5*time.Second)
	}
	cancel()
	<-done
}

// TestAgentStartNotStartedDrops verifies that a DECLINED start (the job is no
// longer pending / not served / coordinate already claimed) is DROPPED
// without spawning (F-30.4).
func TestAgentStartNotStartedDrops(t *testing.T) {
	api := &fakeSupervisorAPI{seenCoords: map[coordKey]bool{}}
	// Pre-poison the coordinate: the (emulated) DB guard declines the start.
	api.seenCoords[coordKey{job: 42, att: 1}] = true
	w := newSupervisedAgent([]string{"-c", "exit 0"}, api)
	t.Cleanup(func() { close(api.watchCh) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	api.watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: &apipb.Job{Id: 42}, AgentName: "w1"},
	}}
	// Positive wait: the (DECLINED) start was attempted ...
	waitFor(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return api.starts >= 1
	}, "start attempted", 5*time.Second)
	// ...and led NOWHERE: no child, no ledger hold.
	if n := w.childCount(); n != 0 {
		t.Errorf("live children = %d, want 0 (a declined start must not spawn)", n)
	}
	if len(w.holdings()) != 0 {
		t.Errorf("ledger holds %v, want empty", w.holdings())
	}
	cancel()
	<-done
}

// ---------------------------------------------------------------------------
// F-30.4 slice 3: failure escalation, orphan adoption, stop ordering.
// ---------------------------------------------------------------------------

// TestAgentReportsChildDeathOnAbruptExit verifies the ESCALATION: a worker
// that dies abnormally (signaled — exit code -1, no terminal report) causes
// the agent to call ReportChildDeath (the fast-failure escape hatch; the
// job fails instead of riding out the F-03 watchdog).
func TestAgentReportsChildDeathOnAbruptExit(t *testing.T) {
	api := &fakeSupervisorAPI{seenCoords: map[coordKey]bool{}}
	w := newSupervisedAgent([]string{"-c", "trap 'exit 0' TERM; sleep 30"}, api)
	t.Cleanup(func() { close(api.watchCh) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	api.watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: &apipb.Job{Id: 77}, AgentName: "w1"},
	}}
	waitFor(t, func() bool { return w.childCount() == 1 }, "worker spawned", 5*time.Second)

	// Signaled death (not a clean exit): the child ignores nothing — SIGKILL
	// is uncatchable, so the worker vanishes mid-job.
	victim := w.liveSnapshot()[0]
	victim.Terminate()
	waitFor(t, func() bool { return w.childCount() == 0 }, "child reaped", 5*time.Second)

	api.mu.Lock()
	deaths := append([]int64(nil), api.reportDeaths...)
	api.mu.Unlock()
	found := false
	for _, jd := range deaths {
		if jd == 77 {
			found = true
		}
	}
	if !found {
		t.Errorf("ReportChildDeath not called for job 77 (got %+v)", deaths)
	}
	cancel()
	<-done
}

// TestAgentAdoptsOrphanedExecutions verifies the ADOPTION SWEEP (item B2):
// at boot, the agent discovers its lineage's still-running executions and
// reserves their coordinates — a redelivery of the SAME job is then DROPPED
// (the worker is still running; the agent must not spawn a twin), and the
// resume lane admits the worker's reconnect for the adopted coordinate.
func TestAgentAdoptsOrphanedExecutions(t *testing.T) {
	young := timestamppb.New(time.Now().UTC().Add(-time.Minute))
	api := &fakeSupervisorAPI{
		seenCoords: map[coordKey]bool{},
		discoveredOrphans: []*dbpb.JobExecution{
			{JobId: 55, AgentName: "w1", Attempt: 1, Status: dbpb.JobStatus_JOB_STATUS_RUNNING, StartedAt: young},
		},
	}
	w := newSupervisedAgent([]string{"-c", "sleep 0.3"}, api)
	t.Cleanup(func() { close(api.watchCh) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	// The sweep runs ~2 s after registration; wait for the reservation.
	coord := spawnCoordinate{jobID: 55, attempt: 1}
	waitFor(t, func() bool { return w.isAdopted(coord) }, "orphan adopted", 10*time.Second)

	// A redelivery of the SAME job is DROPPED (no twin spawn): the ledger
	// holds the adopted coordinate.
	api.watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: &apipb.Job{Id: 55}, AgentName: "w1"},
	}}
	time.Sleep(500 * time.Millisecond)
	if n := w.childCount(); n != 0 {
		t.Errorf("live children = %d, want 0 (adopted coordinate must not re-spawn)", n)
	}

	// The RESUME lane admits the worker's reconnect for the adopted
	// coordinate (foreign job id + consistent name).
	if ok, resumed := w.admitChannelWorker(coord, "w1", "whatever-the-token"); !ok || !resumed {
		t.Errorf("admitChannelWorker(adopted) = (%v,%v), want (true,true)", ok, resumed)
	}
	cancel()
	<-done
}

// TestAgentStopOrdersDrainThenKill verifies the graceful-stop ORDERING
// (drain → interrupt → kill) with SHORT horizons:
//   - Stage 1 (untrapped child): the drain expires, the INTERRUPT (SIGINT —
//     the default disposition kills the shell) reaps it; the abnormal exit
//     ESCALATES to ReportChildDeath (the child never reported).
//   - Stage 2 (COOPERATIVE child: traps SIGINT and exits 0): the interrupt
//     achieves a CLEAN exit — no escalation (the child's own terminal
//     outcome stood), and no force-kill was needed.
func TestAgentStopOrdersDrainThenKill(t *testing.T) {
	shortStop := Dependencies{
		ShutdownGrace:     200 * time.Millisecond,
		ShutdownKillGrace: 1 * time.Second,
	}
	// Stage 1: untrapped child (sleep 30) — interrupt reaps it abnormally.
	api := &fakeSupervisorAPI{seenCoords: map[coordKey]bool{}}
	w := newSupervisedAgent([]string{"-c", "sleep 30"}, api)
	w.deps.ShutdownGrace = shortStop.ShutdownGrace // tighten the horizons (test speed)
	w.deps.ShutdownKillGrace = shortStop.ShutdownKillGrace
	t.Cleanup(func() { close(api.watchCh) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	api.watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: &apipb.Job{Id: 88}, AgentName: "w1"},
	}}
	waitFor(t, func() bool { return w.childCount() == 1 }, "worker spawned", 5*time.Second)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	w.Stop(stopCtx)
	stopCancel()
	waitFor(t, func() bool { return w.childCount() == 0 }, "child reaped", 5*time.Second)
	api.mu.Lock()
	stage1Deaths := append([]int64(nil), api.reportDeaths...)
	api.mu.Unlock()
	if len(stage1Deaths) == 0 {
		t.Error("stage 1: no ReportChildDeath after Stop (the aborted child must escalate)")
	}
	cancel()
	<-done

	// Stage 2: cooperative child (traps SIGINT, exits 0 promptly).
	api2 := &fakeSupervisorAPI{seenCoords: map[coordKey]bool{}}
	w2 := newSupervisedAgent([]string{"-c", `trap 'exit 0' INT; while :; do sleep 0.2; done`}, api2)
	w2.deps.ShutdownGrace = shortStop.ShutdownGrace
	w2.deps.ShutdownKillGrace = shortStop.ShutdownKillGrace
	t.Cleanup(func() { close(api2.watchCh) })
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := make(chan struct{})
	go func() { _ = w2.Run(ctx2); close(done2) }()
	api2.watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: &apipb.Job{Id: 99}, AgentName: "w1"},
	}}
	waitFor(t, func() bool { return w2.childCount() == 1 }, "worker spawned", 5*time.Second)
	stopCtx2, stopCancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	w2.Stop(stopCtx2)
	stopCancel2()
	waitFor(t, func() bool { return w2.childCount() == 0 }, "cooperative child settled", 5*time.Second)
	api2.mu.Lock()
	stage2Deaths := len(api2.reportDeaths)
	api2.mu.Unlock()
	if stage2Deaths != 0 {
		t.Errorf("stage 2: %d ReportChildDeath(es) after a CLEAN interrupt exit (want 0)", stage2Deaths)
	}
	cancel2()
	<-done2
}
