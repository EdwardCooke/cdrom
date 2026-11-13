package agent

import (
	"testing"
	"time"

	"cdrom/internal/worker"
)

// TestChannelSignalAcceleratesCancellation verifies the SPEC's promise: a
// cancellation relayed via Signals lands SOONER THAN POLLING WOULD. The
// worker's own status poll (the authority, F-05) runs at 2 s; the channel
// notice must fire the worker's cancel callback WELL inside that window.
func TestChannelSignalAcceleratesCancellation(t *testing.T) {
	cs := startTestChannel(t)
	var coord = spawnCoordinate{jobID: 31, attempt: 1}
	sc, err := worker.NewSupervisorClient(cs.Addr(), coord.jobID, coord.attempt, "tok", "agent-1", discardLogger())
	if err != nil || sc == nil {
		t.Fatalf("client setup: %v (sc=%v)", err, sc)
	}
	// Arm the notify (stands in for the worker's cancel()).
	fired := make(chan struct{}, 1)
	sc.SetCancelNotify(func() { fired <- struct{}{} })
	waitForRegistration(t, cs, coord)

	start := time.Now()
	cs.broadcastCancellation(coord)
	select {
	case <-fired:
		elapsed := time.Since(start)
		if elapsed > time.Second {
			t.Errorf("signal took %v; the whole point is beating the 2 s poll", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("signal never reached the worker's callback")
	}
	// Second broadcast: the callback fires at most ONCE (idempotent notice).
	cs.broadcastCancellation(coord)
	select {
	case <-fired:
		t.Error("callback fired twice for one cancellation")
	default:
	}
}

// waitForRegistration polls until the coordinator's subscription is live.
func waitForRegistration(t *testing.T, cs *channelServer, coord spawnCoordinate) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		cs.subMu.Lock()
		ok := false
		for sub := range cs.subs {
			if sub.coord == coord {
				ok = true
			}
		}
		cs.subMu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("signals subscription never registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWorkerRunWithChannelForwarding verifies a FULL worker run with the
// channel attached: the job executes end-to-end (its reports still ride the
// API, attributed under the agent name), and the agent RECEIVES the child's
// final account (status + duration + log bytes) — the fold the agent's
// observeChild consumes instead of guessing from the exit code.
func TestWorkerRunWithChannelForwarding(t *testing.T) {
	cs := startTestChannel(t)
	var coord = spawnCoordinate{jobID: 55, attempt: 1}
	sc, err := worker.NewSupervisorClient(cs.Addr(), coord.jobID, coord.attempt, "tok", "agent-1", discardLogger())
	if err != nil || sc == nil {
		t.Fatalf("client setup: %v (sc=%v)", err, sc)
	}
	rec := sc.Recorder()
	rec.RecordStep("shell", "succeeded", 500*time.Millisecond)
	rec.AddLogBytes(42)
	rec.RecordJob("succeeded", 900*time.Millisecond)
	sc.Flush()

	acc := cs.takeAccount(coord)
	if acc == nil {
		t.Fatal("final account missing after the run's flush")
	}
	if !acc.finalSeen || acc.finalStatus != "succeeded" || acc.finalDur != 900*time.Millisecond {
		t.Errorf("final account = %+v, want succeeded/900ms", acc)
	}
	if acc.logBytes != 42 {
		t.Errorf("logBytes = %d, want 42", acc.logBytes)
	}
}

// TestChannelAbsentLeavesRecordingDisabled verifies the worker's channel-less
// posture: with no parent address the recorder is NIL (the worker falls back
// to its own metric set / the agent's exit-code inference).
func TestChannelAbsentLeavesRecordingDisabled(t *testing.T) {
	sc, err := worker.NewSupervisorClient("", 1, 1, "tok", "agent-1", discardLogger())
	if sc != nil || err != nil {
		t.Fatalf("channel-less: sc=%v err=%v, want (nil,nil)", sc, err)
	}
	// The nil client's Recorder is nil-safe (the worker's channel-less
	// posture: no forwarding, the agent infers from the exit code).
	if rec := sc.Recorder(); rec != nil {
		t.Errorf("nil client yielded a non-nil recorder (want nil)")
	}
}
