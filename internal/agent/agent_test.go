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

	apipb "cdrom/internal/gen/cdrom/api/v1"
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
// (WorkerBinary=/bin/sh + WorkerArgs): the tests steer the child's lifespan
// and observable effects without needing the real worker binary.
func newSupervisedAgent(args []string, api *fakeSupervisorAPI) *Agent {
	watchCh := make(chan *apipb.WatchMessage, 8)
	api.watchCh = watchCh
	w := New("w1", "pool-a", nil, nil, Dependencies{
		API:          api,
		WorkerBinary: "/bin/sh",
		WorkerArgs:   args,
		APIAddress:   "127.0.0.1:7105",
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
		_ = victim.cmd.Process.Kill()
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
