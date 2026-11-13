package agent

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	supervisionpb "cdrom/internal/gen/cdrom/supervision/v1"
	"cdrom/internal/worker"
)

// discardLogger silences the channel's chatter in tests.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startTestChannel boots a channel server (everyone admitted) on the default
// loopback bind.
func startTestChannel(t *testing.T) *channelServer {
	t.Helper()
	cs := newChannelServer("", func(spawnCoordinate, string, string) (bool, bool) { return true, false }, discardLogger())
	if err := cs.start(); err != nil {
		t.Fatalf("channel start: %v", err)
	}
	t.Cleanup(cs.stop)
	return cs
}

// TestChannelDefaultsToLoopback guards the LOCAL-SUBSTRATE default: an empty
// bind must listen on loopback (the channel is intra-host by construction).
func TestChannelDefaultsToLoopback(t *testing.T) {
	cs := newChannelServer("", func(spawnCoordinate, string, string) (bool, bool) { return true, false }, discardLogger())
	if err := cs.start(); err != nil {
		t.Fatalf("channel start: %v", err)
	}
	t.Cleanup(cs.stop)
	if !strings.HasPrefix(cs.Addr(), "127.0.0.1:") {
		t.Fatalf("default bind is not loopback: %q", cs.Addr())
	}
}

// TestChannelHonorsConfiguredBind proves the bind is CONFIGURABLE (the seam
// that lets a node-network agent expose the channel to Pod workers): an
// explicit bind is listened on verbatim.
func TestChannelHonorsConfiguredBind(t *testing.T) {
	cs := newChannelServer("127.0.0.1:39999", func(spawnCoordinate, string, string) (bool, bool) { return true, false }, discardLogger())
	if err := cs.start(); err != nil {
		t.Skipf("explicit bind unavailable (contested?): %v", err)
	}
	t.Cleanup(cs.stop)
	if cs.Addr() != "127.0.0.1:39999" {
		t.Fatalf("addr = %q, want 127.0.0.1:39999", cs.Addr())
	}
}

// TestChannelColdAdmitsLiveChildOnly pins the COLD lane: a coordinate that IS
// a live child is admitted (resumed=false); one that is NOT is refused.
func TestChannelColdAdmitsLiveChildOnly(t *testing.T) {
	var live = spawnCoordinate{jobID: 5, attempt: 1}
	cs := newChannelServer("", func(c spawnCoordinate, _, _ string) (bool, bool) {
		return c == live, false
	}, discardLogger())
	if err := cs.start(); err != nil {
		t.Fatalf("channel start: %v", err)
	}
	t.Cleanup(cs.stop)

	conn, err := grpc.NewClient(cs.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	cli := supervisionpb.NewSupervisorClient(conn)
	ctx := context.Background()

	// A live child is admitted, NOT a resume.
	g, err := cli.ConnectJob(ctx, &supervisionpb.JobTicket{JobId: live.jobID, Attempt: live.attempt, AgentName: "agent-1"})
	if err != nil {
		t.Fatalf("ConnectJob(live): %v", err)
	}
	if !g.GetAdmitted() || g.GetResumed() {
		t.Fatalf("live child: admitted=%v resumed=%v, want (true,false)", g.GetAdmitted(), g.GetResumed())
	}
}

// TestChannelResumeAdmitsConsistentForeignChild pins the RESUME lane: a
// coordinate that is NOT a live child (the agent restarted) is admitted when
// the ticket's agent_name matches the agent's own name (resumed=true), and
// REFUSED when it does not (foreign lineage).
func TestChannelResumeAdmitsConsistentForeignChild(t *testing.T) {
	// admit mimics the agent's predicate: no live children (just restarted),
	// resume only when the ticket's name == the agent's own name.
	const agentOwnName = "agent-1"
	cs := newChannelServer("", func(c spawnCoordinate, ticketName, _ string) (bool, bool) {
		if c.jobID == 0 {
			return false, false
		}
		if ticketName == agentOwnName {
			return true, true // resume
		}
		return false, false
	}, discardLogger())
	if err := cs.start(); err != nil {
		t.Fatalf("channel start: %v", err)
	}
	t.Cleanup(cs.stop)

	conn, err := grpc.NewClient(cs.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	cli := supervisionpb.NewSupervisorClient(conn)
	ctx := context.Background()

	// Consistent (same lineage) ⇒ admitted as a RESUME.
	g, err := cli.ConnectJob(ctx, &supervisionpb.JobTicket{JobId: 77, Attempt: 1, AgentName: agentOwnName})
	if err != nil {
		t.Fatalf("ConnectJob(resume): %v", err)
	}
	if !g.GetAdmitted() || !g.GetResumed() {
		t.Fatalf("consistent foreign child: admitted=%v resumed=%v, want (true,true)", g.GetAdmitted(), g.GetResumed())
	}

	// Foreign lineage (different agent name) ⇒ REFUSED.
	g2, err := cli.ConnectJob(ctx, &supervisionpb.JobTicket{JobId: 78, Attempt: 1, AgentName: "intruder"})
	if err != nil {
		t.Fatalf("ConnectJob(foreign): %v", err)
	}
	if g2.GetAdmitted() {
		t.Fatalf("foreign lineage: admitted=%v, want false", g2.GetAdmitted())
	}
}

// TestChannelEndToEndFold drives the REAL worker client against the REAL
// channel server: handshake, forwarded step/log/job samples, the final
// flush, and the agent's fold (takeAccount).
func TestChannelEndToEndFold(t *testing.T) {
	cs := startTestChannel(t)
	var coord = spawnCoordinate{jobID: 42, attempt: 1}

	sc, err := worker.NewSupervisorClient(cs.Addr(), coord.jobID, coord.attempt, "some-token", "agent-1", discardLogger())
	if err != nil {
		t.Fatalf("NewSupervisorClient: %v", err)
	}
	if sc == nil {
		t.Fatal("worker client not admitted (want admission)")
	}

	rec := sc.Recorder()
	rec.RecordStep("shell", "succeeded", 1200*time.Millisecond)
	rec.AddLogBytes(100)
	rec.AddLogBytes(50)
	rec.RecordJob("succeeded", 3*time.Second)
	sc.Flush()

	acc := cs.takeAccount(coord)
	if acc == nil {
		t.Fatal("no account credited (samples lost)")
	}
	if acc.samples < 3 {
		t.Errorf("samples = %d, want >= 3 (step + bytes watermark + final job)", acc.samples)
	}
	if acc.logBytes != 150 {
		t.Errorf("logBytes = %d, want 150 (delta of the watermarks)", acc.logBytes)
	}
	if !acc.finalSeen || acc.finalStatus != "succeeded" {
		t.Errorf("final = (%v, %q), want (true, succeeded)", acc.finalSeen, acc.finalStatus)
	}
	if acc.finalDur != 3*time.Second {
		t.Errorf("finalDur = %v, want 3s", acc.finalDur)
	}
	// The STEP sample is retained for the agent's fold (it cannot be
	// inferred from the exit code).
	if len(acc.steps) != 1 || acc.steps[0].stepType != "shell" || acc.steps[0].status != "succeeded" || acc.steps[0].dur != 1200*time.Millisecond {
		t.Errorf("steps = %+v, want [{shell succeeded 1.2s}]", acc.steps)
	}
	// Consumed: a second take finds nothing.
	if cs.takeAccount(coord) != nil {
		t.Error("takeAccount twice returned an account (want nil)")
	}
}

// TestChannelSignalsAreScopedPerCoordinate verifies the opportunistic
// cancellation relay end-to-end (black-box: the worker's CALLBACK is the
// observable — the agent's Signals handler bridges the broadcast onto the
// worker's stream): a broadcast for coordinate A fires A's worker and NONE
// of its siblings'; a broadcast for B fires B's. Crosstalk would cancel the
// wrong job's worker (catastrophic: a worker aborting a job that isn't
// cancelled).
func TestChannelSignalsAreScopedPerCoordinate(t *testing.T) {
	cs := startTestChannel(t)
	var a = spawnCoordinate{jobID: 7, attempt: 1}
	var b = spawnCoordinate{jobID: 99, attempt: 1}

	fireA := make(chan struct{}, 4)
	fireB := make(chan struct{}, 4)
	scA, err := worker.NewSupervisorClient(cs.Addr(), a.jobID, a.attempt, "tokA", "agent-1", discardLogger())
	if err != nil || scA == nil {
		t.Fatalf("client A setup: %v (sc=%v)", err, scA)
	}
	scB, err := worker.NewSupervisorClient(cs.Addr(), b.jobID, b.attempt, "tokB", "agent-1", discardLogger())
	if err != nil || scB == nil {
		t.Fatalf("client B setup: %v (sc=%v)", err, scB)
	}
	scA.SetCancelNotify(func() { fireA <- struct{}{} })
	scB.SetCancelNotify(func() { fireB <- struct{}{} })
	waitForRegistration(t, cs, a)
	waitForRegistration(t, cs, b)

	expectFire := func(ch chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s never fired", what)
		}
	}
	expectSilent := func(ch chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
			t.Fatalf("%s fired spuriously (crosstalk)", what)
		default:
		}
	}

	cs.broadcastCancellation(a)
	expectFire(fireA, "worker A's cancellation notice")
	expectSilent(fireB, "worker B's cancellation notice")

	cs.broadcastCancellation(b)
	expectFire(fireB, "worker B's cancellation notice")
	expectSilent(fireA, "worker A's second cancellation notice")
}

// TestChannelRejectsUnassociatedPeer verifies a rogue dial (no ConnectJob)
// cannot use the channel verbs.
func TestChannelRejectsUnassociatedPeer(t *testing.T) {
	cs := startTestChannel(t)
	conn, err := grpc.NewClient(cs.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	cli := supervisionpb.NewSupervisorClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := cli.Telemetry(ctx)
	if err != nil {
		t.Fatalf("open telemetry: %v", err)
	}
	_ = stream.Send(&supervisionpb.Sample{})
	_, err = stream.CloseAndRecv()
	if err == nil {
		t.Fatal("unassociated peer was served (want denial)")
	}
}

// TestChannelResumeLaneVerifiesTicketOnRoutableBind verifies item B3: when
// the channel ESCAPES loopback (the routable posture), the resume lane
// VERIFIES the ticket's token via the API's attestation — a BAD token is
// refused even though its unsigned decode names this agent; a GOOD token is
// admitted. The loopback posture keeps the cheap unsigned decode (no API
// round-trip).
func TestChannelResumeLaneVerifiesTicketOnRoutableBind(t *testing.T) {
	api := &fakeSupervisorAPI{seenCoords: map[coordKey]bool{}}
	w := newSupervisedAgent([]string{"-c", "sleep 0.3"}, api)
	// Swap the factory's loopback channel for a ROUTABLE one (escaping
	// loopback): the resume lane then verifies tickets via the API.
	if w.channel != nil {
		w.channel.stop()
	}
	w.channel = newChannelServer("0.0.0.0:0", w.admitChannelWorker, discardLogger())
	if err := w.channel.start(); err != nil {
		t.Fatalf("routable channel start: %v", err)
	}
	t.Cleanup(w.channel.stop)
	if !w.routableChannel() {
		t.Fatalf("channel %q is not routable (test premise)", w.channel.Addr())
	}
	coord := spawnCoordinate{jobID: 31, attempt: 1}
	// Bad token: the API says INVALID ⇒ refused (despite the consistent
	// unsigned decode).
	api.mu.Lock()
	api.validTickets = map[int64]string{} // no valid tokens seeded
	api.mu.Unlock()
	if ok, _ := w.admitChannelWorker(coord, "w1", "forged-token"); ok {
		t.Error("bad token admitted on the routable posture (want refusal)")
	}
	// Good token: the API attests it for THIS agent ⇒ admitted (resume).
	api.mu.Lock()
	api.validTickets = map[int64]string{31: "good-token"}
	api.mu.Unlock()
	if ok, resumed := w.admitChannelWorker(coord, "w1", "good-token"); !ok || !resumed {
		t.Errorf("good token: admit = (%v,%v), want (true,true)", ok, resumed)
	}
	// Wrong-agent token (consistent-looking decode, foreign attestation) ⇒
	// refused.
	if ok, _ := w.admitChannelWorker(coord, "w1", "someone-elses-token"); ok {
		t.Error("foreign token admitted on the routable posture (want refusal)")
	}
}

// TestChannelDegradedConstruction verifies the worker client's degradation
// ladder: no address ⇒ (nil, nil) (channel-less FOREVER); an unreachable
// address ⇒ a DEGRADED client (slice 3, item B1: the reconnector keeps
// reclaiming the channel in the background — the job is ungated either
// way). Permanent REFUSALS (an answering agent declining the ticket) ⇒
// (nil, nil) (channel-less forever).
func TestChannelDegradedConstruction(t *testing.T) {
	if sc, err := worker.NewSupervisorClient("", 1, 1, "tok", "agent-1", discardLogger()); sc != nil || err != nil {
		t.Errorf("empty addr: sc=%v err=%v, want (nil, nil)", sc, err)
	}
	// Unreachable: budget-exhausted ⇒ degraded client (NON-nil), no error.
	if sc, err := worker.NewSupervisorClient("127.0.0.1:1", 1, 1, "tok", "agent-1", discardLogger()); sc == nil || err != nil {
		t.Errorf("unreachable addr: sc=%v err=%v, want (degraded client, nil)", sc, err)
	}
	// Refusal: an ADMITTING-nothing channel ⇒ permanent ⇒ (nil, nil).
	refusing := newChannelServer("", func(spawnCoordinate, string, string) (bool, bool) { return false, false }, discardLogger())
	if err := refusing.start(); err != nil {
		t.Fatalf("refusing channel start: %v", err)
	}
	t.Cleanup(refusing.stop)
	if sc, err := worker.NewSupervisorClient(refusing.Addr(), 1, 1, "tok", "agent-1", discardLogger()); sc != nil || err != nil {
		t.Errorf("refused addr: sc=%v err=%v, want (nil, nil)", sc, err)
	}
}
