package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
	"cdrom/internal/metrics"
)

// ---------------------------------------------------------------------------
// F-21.5.2 / F-21.5.3 — the sync routine, the cadence resolution, and the
// refresh triggers (API-layer tests; the database-side behaviors are tested
// in internal/services/database).
// ---------------------------------------------------------------------------

// testCfg returns a minimal config for cadence-resolution tests: the
// system-default interval and the global override gate.
func testCfg(defaultInterval time.Duration, overridesAllowed bool) *config.Config {
	return &config.Config{
		PipelineSourceRefreshInterval:  defaultInterval,
		PipelineSourceOverridesAllowed: overridesAllowed,
	}
}

// TestEffectiveCadenceResolution verifies the effective-cadence ladder
// (F-21.5.1): cron-preferred when honored, interval when honored, the system
// default when unhonored or undeclared, and the honored-opt-out.
func TestEffectiveCadenceResolution(t *testing.T) {
	sys := 2 * time.Minute
	cases := []struct {
		name       string
		honored    bool
		interval   string
		schedule   string
		wantMode   string
		wantIntvl  string
		wantSchdl  string
		wantOptOut bool
	}{
		{name: "undeclared → system default", honored: true, wantMode: "interval", wantIntvl: sys.String()},
		{name: "gate denies → system default (restore, not halt)", honored: false, interval: "1h", schedule: "@weekly-nope", wantMode: "interval", wantIntvl: sys.String()},
		{name: "honored interval", honored: true, interval: "5m", wantMode: "interval", wantIntvl: "5m"},
		{name: "honored cron drives (interval ignored)", honored: true, interval: "5m", schedule: "0 3 * * *", wantMode: "cron", wantSchdl: "0 3 * * *"},
		{name: "honored opt-out (interval 0, no cron)", honored: true, interval: "0", wantMode: "interval", wantIntvl: "0", wantOptOut: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mode, interval, schedule, optedOut := effectiveCadence(tc.honored, tc.interval, tc.schedule, sys)
			if mode != tc.wantMode {
				t.Errorf("mode = %q, want %q", mode, tc.wantMode)
			}
			if interval != tc.wantIntvl {
				t.Errorf("interval = %q, want %q", interval, tc.wantIntvl)
			}
			if schedule != tc.wantSchdl {
				t.Errorf("schedule = %q, want %q", schedule, tc.wantSchdl)
			}
			if optedOut != tc.wantOptOut {
				t.Errorf("optedOut = %v, want %v", optedOut, tc.wantOptOut)
			}
		})
	}
}

// TestSystemDefaultFloor verifies the system-default flooring (F-21.5.1): a
// nil cfg and a non-positive configured value both fall back to the built-in
// 2-minute default.
func TestSystemDefaultFloor(t *testing.T) {
	if got := (&Server{}).systemDefaultInterval(); got != config.DefaultPipelineSourceRefreshInterval {
		t.Errorf("nil cfg: systemDefaultInterval = %v, want %v", got, config.DefaultPipelineSourceRefreshInterval)
	}
	s := &Server{cfg: testCfg(-1, true)}
	if got := s.systemDefaultInterval(); got != config.DefaultPipelineSourceRefreshInterval {
		t.Errorf("negative cfg: systemDefaultInterval = %v, want %v", got, config.DefaultPipelineSourceRefreshInterval)
	}
	s = &Server{cfg: testCfg(7*time.Minute, true)}
	if got := s.systemDefaultInterval(); got != 7*time.Minute {
		t.Errorf("positive cfg: systemDefaultInterval = %v, want 7m", got)
	}
}

// TestGlobalGateNilDefaultsAllowed verifies the global gate defaults to
// ALLOWED when no config is attached (tests/local runs stay functional).
func TestGlobalGateNilDefaultsAllowed(t *testing.T) {
	if !(&Server{}).globalOverridesAllowed() {
		t.Error("nil cfg: globalOverridesAllowed = false, want true (default)")
	}
}

// TestSyncRoutineReimportOnChange verifies the sync routine's CHANGED path
// (F-21.5.2): a fetch whose hash differs from the stored source_hash
// recompiles + re-imports the file (recording the new source_file /
// source_hash on the update) and settles the state (version + hash, no
// error).
func TestSyncRoutineReimportOnChange(t *testing.T) {
	const file = "# pipelines/demo.yaml\napiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: demo\nspec:\n  jobs:\n    hello:\n      steps:\n        greet:\n          shell: echo hi\n"
	hash := sha256Hex([]byte(file))
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "file",
			Reference: "file:/demo.yaml",
		},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: "OLDHASH"},
	}
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte(file), Version: "rev-1", Hash: hash,
	}}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	state, err := srv.syncPipelineSource(context.Background(), 7)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if state.GetFileHash() != hash || state.GetLastVersion() != "rev-1" {
		t.Errorf("state = {%q %q}, want {%q rev-1}", state.GetFileHash(), state.GetLastVersion(), hash)
	}
	upd := fake.updatedPipeline
	if upd == nil {
		t.Fatal("no re-import occurred (want UpdatePipeline on a changed hash)")
	}
	if upd.GetSourceFile() != file || upd.GetSourceHash() != hash {
		t.Errorf("re-import source_file/hash = {%q %q}, want the fetched file + hash", upd.GetSourceFile(), upd.GetSourceHash())
	}
	if !upd.GetHasPeriodicRefresh() {
		t.Error("re-import did not carry the file's periodic-refresh policy (has_periodic_refresh)")
	}
	if fake.pipelineSourceState.GetLastError() != "" {
		t.Errorf("last_error = %q, want empty on success", fake.pipelineSourceState.GetLastError())
	}
}

// TestSyncRoutineNoOpOnUnchanged verifies the sync routine's UNCHANGED path
// (F-21.5.2): a fetch whose hash equals the stored source_hash is a no-op —
// no recompile, no version bump (NO UpdatePipeline), only the state's
// last_synced_at advances.
func TestSyncRoutineNoOpOnUnchanged(t *testing.T) {
	const file = "# pipelines/demo.yaml\napiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: demo\nspec:\n  jobs:\n    hello:\n      steps:\n        greet:\n          shell: echo hi\n"
	hash := sha256Hex([]byte(file))
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "file",
			Reference: "file:/demo.yaml",
		},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: hash},
	}
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte(file), Version: "rev-1", Hash: hash,
	}}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	if _, err := srv.syncPipelineSource(context.Background(), 7); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if fake.updatedPipeline != nil {
		t.Errorf("unchanged hash triggered a re-import (want no-op)")
	}
	if fake.pipelineSourceState == nil || fake.pipelineSourceState.GetLastSyncedAt() == nil {
		t.Errorf("unchanged sync did not advance last_synced_at: %+v", fake.pipelineSourceState)
	}
}

// TestSyncRoutineRecordsFailurePreservesLastGood verifies the sync routine's
// FAILURE path (F-21.5.2): a broken FETCH records last_error (preserving the
// surviving version/hash — the kickoff-provenance fallback) and returns the
// error; the run proceeds on the last good definition.
func TestSyncRoutineRecordsFailurePreservesLastGood(t *testing.T) {
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "http",
			Reference: "http://down.local/file.yaml",
		},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: "GOOD-HASH"},
		// Seed the last-good state (a prior successful sync).
		pipelineSourceState: &dbpb.PipelineSourceState{
			PipelineId: 7, LastVersion: "good-ver", FileHash: "GOOD-HASH",
		},
	}
	ps := &fakePipelineSource{err: status.Error(codes.Unavailable, "connection refused")}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	_, err := srv.syncPipelineSource(context.Background(), 7)
	if err == nil {
		t.Fatal("sync: want an error for a failed fetch")
	}
	st := fake.pipelineSourceState
	if st == nil || st.GetLastError() == "" {
		t.Fatalf("last_error not recorded: %+v", st)
	}
	// The LAST-GOOD survivors are intact (the database service — and the
	// faithful fake — preserves version/hash when a failure record carries
	// empties): the kickoff-provenance fallback leans on them.
	if st.GetLastVersion() != "good-ver" || st.GetFileHash() != "GOOD-HASH" {
		t.Errorf("survivors wiped by the failure record: {%q %q}, want {good-ver GOOD-HASH}", st.GetLastVersion(), st.GetFileHash())
	}
}

// TestSyncRoutineRejectsBadFile verifies the sync routine's COMPILE-FAILURE
// path (F-21.2 / F-21.5.2): a fetched file that FAILS compilation is a sync
// failure — recorded on last_error, the pipeline keeps its last-good
// definition (no re-import applied).
func TestSyncRoutineRejectsBadFile(t *testing.T) {
	const bad = "apiVersion: nope\nkind: Pipeline\nmetadata:\n  name: demo\nspec: {}\n"
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "file",
			Reference: "file:/bad.yaml",
		},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: "OLD"},
	}
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte(bad), Version: "rev-2", Hash: sha256Hex([]byte(bad)),
	}}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	_, err := srv.syncPipelineSource(context.Background(), 7)
	if err == nil {
		t.Fatal("sync: want an error for a file that fails compilation")
	}
	if fake.updatedPipeline != nil {
		t.Errorf("a BAD file was re-imported (want the last-good definition kept)")
	}
	if fake.pipelineSourceState == nil || fake.pipelineSourceState.GetLastError() == "" {
		t.Errorf("compile failure not recorded on last_error: %+v", fake.pipelineSourceState)
	}
}

// TestSyncSerializedAcrossReplicas verifies the per-pipeline CROSS-REPLICA
// sync lease (F-21.5.2): when another replica HOLDS the sync lease, this
// replica's sync DEFERS (ErrSyncDeferred) rather than contending — so two
// pods can never double-apply a change.
func TestSyncSerializedAcrossReplicas(t *testing.T) {
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "file", Reference: "file:/demo.yaml",
		},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: "OLD"},
	}
	// Simulate PEER B holding the sync lease.
	fake.mu.Lock()
	fake.leased = true
	fake.holder = "peer-B"
	fake.mu.Unlock()
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte("# x"), Version: "v", Hash: "h",
	}}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	if _, err := srv.syncPipelineSource(context.Background(), 7); !errors.Is(err, ErrSyncDeferred) {
		t.Fatalf("sync error = %v, want ErrSyncDeferred (a peer holds the lease)", err)
	}
}

// TestManualRefreshEndpointHappyPath verifies MANUAL REFRESH (F-21.5.3):
// POST /api/pipelines/{id}/source/refresh runs the sync and returns the
// settled state (200) on success.
func TestManualRefreshEndpointHappyPath(t *testing.T) {
	const file = "# pipelines/demo.yaml\napiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: demo\nspec:\n  jobs:\n    hello:\n      steps:\n        greet:\n          shell: echo hi\n"
	hash := sha256Hex([]byte(file))
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "file", Reference: "file:/demo.yaml",
		},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: "OLD"},
	}
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte(file), Version: "rev-9", Hash: hash,
	}}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/pipelines/7/source/refresh", "application/json", nil)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("refresh status = %d, want 200 (body %s)", resp.StatusCode, body)
	}
	var state dbpb.PipelineSourceState
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if state.GetLastVersion() != "rev-9" || state.GetFileHash() != hash {
		t.Errorf("returned state = {%q %q}, want {rev-9 %q}", state.GetLastVersion(), state.GetFileHash(), hash)
	}
}

// TestManualRefreshDeclinesWhilePeerSyncs verifies MANUAL REFRESH's polite
// refusal (F-21.5.2 / F-21.5.3): while a peer replica holds the sync lease,
// the endpoint returns 409 CONFLICT (not a failure) — the in-flight sync
// settles the state.
func TestManualRefreshDeclinesWhilePeerSyncs(t *testing.T) {
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "file", Reference: "file:/demo.yaml",
		},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo"},
	}
	fake.mu.Lock()
	fake.leased = true
	fake.holder = "peer-B"
	fake.mu.Unlock()
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte("# x"), Version: "v", Hash: "h",
	}}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/pipelines/7/source/refresh", "application/json", nil)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("refresh status = %d, want 409 (conflict with a peer sync)", resp.StatusCode)
	}
}

// TestSourceViewCarriesCadence verifies GET /api/pipelines/{id}/source's
// CADENCE VIEW (F-21.5.1): the declared policy, the gate verdict, and the
// effective cadence.
func TestSourceViewCarriesCadence(t *testing.T) {
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "file", Reference: "file:/demo.yaml",
		},
		pipeline: &dbpb.Pipeline{
			Id:                              7,
			Name:                            "demo",
			PeriodicRefreshInterval:         "5m",
			PeriodicRefreshSchedule:         "0 3 * * *",
			PeriodicRefreshOverridesAllowed: true,
		},
	}
	ps := &fakePipelineSource{}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	srv.SetConfig(testCfg(2*time.Minute, true))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/pipelines/7/source")
	if err != nil {
		t.Fatalf("get source: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get source status = %d, want 200 (body %s)", resp.StatusCode, body)
	}
	var view struct {
		Cadence *cadenceView `json:"cadence"`
	}
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatalf("decode view: %v", err)
	}
	cv := view.Cadence
	if cv == nil {
		t.Fatalf("no cadence view in the response: %s", body)
	}
	if cv.DeclaredInterval != "5m" || cv.DeclaredSchedule != "0 3 * * *" {
		t.Errorf("declared = {%q %q}, want {5m \"0 3 * * *\"}", cv.DeclaredInterval, cv.DeclaredSchedule)
	}
	if !cv.GlobalGateAllowed || !cv.PipelineGateAllowed || !cv.OverrideHonored {
		t.Errorf("gate verdict = {global:%v pipeline:%v honored:%v}, want all true", cv.GlobalGateAllowed, cv.PipelineGateAllowed, cv.OverrideHonored)
	}
	// The cron drives (both declared, honored).
	if cv.EffectiveMode != "cron" || cv.EffectiveSchedule != "0 3 * * *" {
		t.Errorf("effective = {%q %q}, want {cron \"0 3 * * *\"}", cv.EffectiveMode, cv.EffectiveSchedule)
	}
	if cv.SystemDefault != "2m0s" {
		t.Errorf("system default = %q, want 2m0s", cv.SystemDefault)
	}
}

// TestSourceViewShowsDeniedFallback verifies the cadence view EXPLAINS a
// denial (F-21.5.1): when the global gate denies, the effective cadence is
// the system default (the safe restore), and the view says so.
func TestSourceViewShowsDeniedFallback(t *testing.T) {
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "file", Reference: "file:/demo.yaml",
		},
		pipeline: &dbpb.Pipeline{
			Id:                              7,
			Name:                            "demo",
			PeriodicRefreshInterval:         "1h",
			PeriodicRefreshOverridesAllowed: true,
		},
	}
	ps := &fakePipelineSource{}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	srv.SetConfig(testCfg(2*time.Minute /*global=*/, false))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/pipelines/7/source")
	if err != nil {
		t.Fatalf("get source: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var view struct {
		Cadence *cadenceView `json:"cadence"`
	}
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatalf("decode view: %v", err)
	}
	cv := view.Cadence
	if cv == nil {
		t.Fatalf("no cadence view: %s", body)
	}
	if cv.OverrideHonored {
		t.Error("override honored = true, want false (global gate denies)")
	}
	if cv.EffectiveMode != "interval" || cv.EffectiveInterval != "2m0s" {
		t.Errorf("effective = {%q %q}, want {interval 2m0s} (fallback to system default)", cv.EffectiveMode, cv.EffectiveInterval)
	}
}

// TestSourceRefreshLoopSyncsDueSource smokes the LEADER LOOP's poll (F-21.5.3):
// a source whose effective cadence is DUE gets synced (the first poll PRIMEs
// the tracker — cold start — the second, moments later, finds it due and
// syncs).
func TestSourceRefreshLoopSyncsDueSource(t *testing.T) {
	const file = "# pipelines/demo.yaml\napiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: demo\nspec:\n  jobs:\n    hello:\n      steps:\n        greet:\n          shell: echo hi\n"
	hash := sha256Hex([]byte(file))
	fake := &fakeDatabase{
		sources: []*dbpb.PipelineSource{{
			Id: 1, PipelineId: 7, Kind: "file", Reference: "file:/demo.yaml",
		}},
		allPipelines: []*dbpb.Pipeline{{
			Id:                              7,
			Name:                            "demo",
			SourceHash:                      "STALE",
			PeriodicRefreshInterval:         "1ms", // effectively always due
			PeriodicRefreshOverridesAllowed: true,
		}},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: "STALE", PeriodicRefreshOverridesAllowed: true},
	}
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte(file), Version: "loop-1", Hash: hash,
	}}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	srv.SetConfig(testCfg(2*time.Minute, true))
	ctx := context.Background()
	tracker := &srcAttemptTracker{}
	inFlight := srv.srcInFlight
	// Poll 1: cold start — prime the tracker (no sync yet).
	srv.pollDueSources(ctx, tracker, inFlight, 0)
	if fake.pipelineSourceState != nil {
		t.Fatalf("cold-start poll synced immediately (want a primed no-wave)")
	}
	time.Sleep(5 * time.Millisecond)
	// Poll 2: the 1ms cadence is due — sync (re-import) happens.
	srv.pollDueSources(ctx, tracker, inFlight, 0)
	waitFor(t, 2*time.Second, func() bool { return fake.updatedPipeline != nil })
	if fake.updatedPipeline == nil {
		t.Fatalf("due source was not synced by the loop")
	}
	if fake.pipelineSourceState == nil || fake.pipelineSourceState.GetLastVersion() != "loop-1" {
		t.Errorf("loop sync did not settle the state: %+v", fake.pipelineSourceState)
	}
	if got := inFlight.Get(); got != 0 {
		t.Errorf("in-flight gauge = %d after settlement, want 0", got)
	}
}

// TestSourceRefreshLoopSkipsOptOut verifies the loop RESPECTS an honored
// opt-out (interval 0, no cron): the source is never polled.
func TestSourceRefreshLoopSkipsOptOut(t *testing.T) {
	fake := &fakeDatabase{
		sources: []*dbpb.PipelineSource{{
			Id: 1, PipelineId: 7, Kind: "file", Reference: "file:/demo.yaml",
		}},
		allPipelines: []*dbpb.Pipeline{{
			Id:                              7,
			Name:                            "demo",
			SourceHash:                      "STALE",
			PeriodicRefreshInterval:         "0", // honored opt-out
			PeriodicRefreshOverridesAllowed: true,
		}},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: "STALE", PeriodicRefreshOverridesAllowed: true},
	}
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte("# x"), Version: "v", Hash: "h",
	}}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	srv.SetConfig(testCfg(2*time.Minute, true))
	ctx := context.Background()
	tracker := &srcAttemptTracker{}
	srv.pollDueSources(ctx, tracker, srv.srcInFlight, 0) // prime
	time.Sleep(5 * time.Millisecond)
	srv.pollDueSources(ctx, tracker, srv.srcInFlight, 0) // would be due if not opted out
	if fake.updatedPipeline != nil || fake.pipelineSourceState != nil {
		t.Fatalf("opted-out source was polled (want skipped)")
	}
}

// TestSourceRefreshLoopSkipsInFlight verifies SKIP-IF-IN-FLIGHT (F-21.5.3): a
// due source whose previous sync is still presumed running is SKIPPED (no
// queued second sync, no parked goroutine) — the next due instant is tried
// again. Seeded by planting an in-flight reservation far ahead.
func TestSourceRefreshLoopSkipsInFlight(t *testing.T) {
	fake := &fakeDatabase{
		sources: []*dbpb.PipelineSource{{
			Id: 1, PipelineId: 7, Kind: "file", Reference: "file:/demo.yaml",
		}},
		allPipelines: []*dbpb.Pipeline{{
			Id:                              7,
			Name:                            "demo",
			SourceHash:                      "STALE",
			PeriodicRefreshInterval:         "1ms",
			PeriodicRefreshOverridesAllowed: true,
		}},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: "STALE", PeriodicRefreshOverridesAllowed: true},
	}
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte("# x"), Version: "v", Hash: "h",
	}}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	srv.SetConfig(testCfg(2*time.Minute, true))
	reg := prometheus.NewRegistry()
	srv.SetMetrics(metrics.NewAPIMetrics(reg))
	ctx := context.Background()
	tracker := &srcAttemptTracker{}
	tracker.prime(7) // cold-start seeding
	// Presume in flight: the last issuance was 1s AGO (so the 1ms cadence is
	// due NOW) and its expected finish is an HOUR hence (so the in-flight
	// presumption holds).
	tracker.reserve(7, time.Now().Add(-time.Second), time.Now().Add(time.Hour))
	srv.pollDueSources(ctx, tracker, srv.srcInFlight, 0)
	if got := metricSample(t, reg, "cdrom_api_source_syncs_total", map[string]string{"outcome": "skipped_busy"}); got != 1 {
		t.Fatalf("skipped_busy = %v, want 1 (a due fire while in flight is skipped)", got)
	}
	if fake.updatedPipeline != nil || fake.pipelineSourceState != nil {
		t.Fatalf("an in-flight source was re-synced (want skipped)")
	}
	// Once the presumption lapses, the next poll syncs (the canned file is
	// uncompilable — the sync ERRORS — but the ISSUE is what's under test).
	tracker.forget(7)
	time.Sleep(5 * time.Millisecond)
	srv.pollDueSources(ctx, tracker, srv.srcInFlight, 0)
	issued := func() float64 {
		return metricSample(t, reg, "cdrom_api_source_syncs_total", map[string]string{"outcome": "success"}) +
			metricSample(t, reg, "cdrom_api_source_syncs_total", map[string]string{"outcome": "error"})
	}
	waitFor(t, 2*time.Second, func() bool { return issued() > 0 })
	if issued() == 0 {
		t.Fatalf("source not synced after the in-flight presumption lapsed")
	}
}

// TestSourceRefreshLoopThrottleDefers verifies the MAX-PARALLEL THROTTLE
// (F-21.5.3): with a finite cap already SATURATED, a due source is DEFERRED
// to the next tick (delayed, never blocked/starved).
func TestSourceRefreshLoopThrottleDefers(t *testing.T) {
	fake := &fakeDatabase{
		sources: []*dbpb.PipelineSource{{
			Id: 1, PipelineId: 7, Kind: "file", Reference: "file:/demo.yaml",
		}},
		allPipelines: []*dbpb.Pipeline{{
			Id:                              7,
			Name:                            "demo",
			SourceHash:                      "STALE",
			PeriodicRefreshInterval:         "1ms",
			PeriodicRefreshOverridesAllowed: true,
		}},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: "STALE", PeriodicRefreshOverridesAllowed: true},
	}
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte("# x"), Version: "v", Hash: "h",
	}}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	srv.SetConfig(testCfg(2*time.Minute, true))
	reg := prometheus.NewRegistry()
	srv.SetMetrics(metrics.NewAPIMetrics(reg))
	ctx := context.Background()
	tracker := &srcAttemptTracker{}
	tracker.prime(7)
	issued := func() float64 {
		return metricSample(t, reg, "cdrom_api_source_syncs_total", map[string]string{"outcome": "success"}) +
			metricSample(t, reg, "cdrom_api_source_syncs_total", map[string]string{"outcome": "error"})
	}
	// Saturate the single-slot cap (simulating another in-flight sync).
	srv.srcInFlight.Inc()
	srv.pollDueSources(ctx, tracker, srv.srcInFlight, 1)
	if got := issued(); got != 0 {
		t.Fatalf("a due source was issued while the throttle was saturated (want deferred; issued=%v)", got)
	}
	// Drain the slot: the next poll issues it (the sync itself may ERROR —
	// the canned file is not compilable — but the ISSUE is what's under test).
	srv.srcInFlight.Dec()
	time.Sleep(5 * time.Millisecond)
	srv.pollDueSources(ctx, tracker, srv.srcInFlight, 1)
	waitFor(t, 2*time.Second, func() bool { return issued() > 0 })
	if got := issued(); got == 0 {
		t.Fatalf("source not issued after the throttle drained")
	}
}

// TestJitterPhaseStableAndBounded verifies the ANTI-PHASE JITTER (F-21.5.3):
// the per-source offset is DETERMINISTIC (same source, same phase) and
// CENTERED within ±3/8 of the interval.
func TestJitterPhaseStableAndBounded(t *testing.T) {
	interval := 2 * time.Minute
	bound := time.Duration(float64(interval) * 3.0 / 8.0)
	for _, id := range []int64{1, 7, 42, 1000, 999999999} {
		first := jitterOffset(id, interval)
		if first < -bound || first > bound {
			t.Errorf("offset(%d) = %v outside ±%v", id, first, bound)
		}
		for trial := 0; trial < 5; trial++ {
			if again := jitterOffset(id, interval); again != first {
				t.Errorf("offset(%d) unstable: %v then %v", id, first, again)
			}
		}
	}
	// Different sources spread across the band (sanity: not degenerate).
	distinct := map[time.Duration]int{}
	for id := int64(0); id < 64; id++ {
		distinct[jitterOffset(id, interval)]++
	}
	if len(distinct) < 4 {
		t.Errorf("jitter produced only %d distinct phases over 64 sources (want spread)", len(distinct))
	}
}

// TestSlowSyncWarnerRateLimited verifies the RATE-LIMITED slow-sync warning
// (F-21.5.3): the first overrun warns, rapid repeats are suppressed within
// the cooldown, and a later overrun warns again.
func TestSlowSyncWarnerRateLimited(t *testing.T) {
	w := &slowSyncWarner{cooldown: 50 * time.Millisecond}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if !w.note(log, 7, 10*time.Millisecond, 30*time.Millisecond) {
		t.Fatal("first overrun did not warn")
	}
	if w.note(log, 7, 10*time.Millisecond, 30*time.Millisecond) {
		t.Fatal("rapid repeat warned (want suppressed within the cooldown)")
	}
	// Under-span syncs never warn.
	if w.note(log, 7, 30*time.Second, 10*time.Second) {
		t.Fatal("an under-span sync warned (want silent)")
	}
	time.Sleep(60 * time.Millisecond)
	if !w.note(log, 7, 10*time.Millisecond, 30*time.Millisecond) {
		t.Fatal("overrun after the cooldown did not warn")
	}
}

// TestSyncObservedByMetrics verifies the F-20 OBSERVABILITY (F-21.5.3): a
// sync increments the outcome counter + duration histogram, and the in-flight
// gauge returns to 0 after settlement.
func TestSyncObservedByMetrics(t *testing.T) {
	const file = "# pipelines/demo.yaml\napiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: demo\nspec:\n  jobs:\n    hello:\n      steps:\n        greet:\n          shell: echo hi\n"
	hash := sha256Hex([]byte(file))
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "file", Reference: "file:/demo.yaml",
		},
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", SourceHash: "OLD", PeriodicRefreshOverridesAllowed: true},
	}
	ps := &fakePipelineSource{response: &pipelinesourcepb.FetchSourceResponse{
		Content: []byte(file), Version: "met-1", Hash: hash,
	}}
	reg := prometheus.NewRegistry()
	m := metrics.NewAPIMetrics(reg)
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	srv.SetConfig(testCfg(2*time.Minute, true))
	srv.SetMetrics(m)
	if _, err := srv.syncPipelineSource(context.Background(), 7); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if got := metricSample(t, reg, "cdrom_api_source_syncs_total", map[string]string{"outcome": "success"}); got != 1 {
		t.Errorf("source_syncs_total{success} = %v, want 1", got)
	}
	if got := metricSample(t, reg, "cdrom_api_source_sync_duration_seconds", map[string]string{"outcome": "success"}); got != 1 {
		t.Errorf("source_sync_duration_seconds{success} observations = %v, want 1", got)
	}
	if got := metricSample(t, reg, "cdrom_api_source_sync_in_flight", nil); got != 0 {
		t.Errorf("source_sync_in_flight = %v after settlement, want 0", got)
	}
}

// metricSample gathers a metric family from a registry and returns the VALUE
// of the sample matching the wanted labels: counters/gauges return their
// value; histograms return their SAMPLE COUNT (observations). 0 when absent.
func metricSample(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			match := true
			for k, v := range labels {
				var lv string
				for _, lp := range m.GetLabel() {
					if lp.GetName() == k {
						lv = lp.GetValue()
						break
					}
				}
				if lv != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				return float64(m.GetHistogram().GetSampleCount())
			default:
				return 0
			}
		}
	}
	return 0
}

// waitFor polls cond until true or the deadline expires (fails the test).
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestGetPipelineEndpoint verifies the singular pipeline getter (F-21.5.1):
// GET /api/pipelines/{id} returns the pipeline's compiled definition (the
// same shape the UI editor shows) — the "two views of the same model" leg of
// the file/storage story.
func TestGetPipelineEndpoint(t *testing.T) {
	fake := &fakeDatabase{
		pipeline: &dbpb.Pipeline{Id: 7, Name: "demo", Version: 3,
			PeriodicRefreshInterval: "5m", PeriodicRefreshSchedule: "0 3 * * *"},
	}
	ps := &fakePipelineSource{}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/pipelines/7")
	if err != nil {
		t.Fatalf("get pipeline: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get pipeline status = %d, want 200 (body %s)", resp.StatusCode, body)
	}
	var got dbpb.Pipeline
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode pipeline: %v", err)
	}
	if got.GetName() != "demo" || got.GetVersion() != 3 {
		t.Errorf("pipeline = {%q v%d}, want {demo v3}", got.GetName(), got.GetVersion())
	}
	if got.GetPeriodicRefreshInterval() != "5m" || got.GetPeriodicRefreshSchedule() != "0 3 * * *" {
		t.Errorf("policy = {%q %q}, want {5m \"0 3 * * *\"}", got.GetPeriodicRefreshInterval(), got.GetPeriodicRefreshSchedule())
	}
}

// sha256Hex is a local content-hash helper (matching the fetcher's
// SHA-256-hex hash).
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
