package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fakeCronDB is a stub Database client for the cron-loop tests: it returns the
// pipelines (with their cron triggers) from ListPipelines and records the
// TriggerRun calls the loop makes.
type fakeCronDB struct {
	dbpb.DatabaseClient // nil
	pipelines           []*dbpb.Pipeline
	triggerRuns         []*dbpb.TriggerRunRequest
}

func (f *fakeCronDB) ListPipelines(ctx context.Context, in *dbpb.ListPipelinesRequest, opts ...grpc.CallOption) (*dbpb.ListPipelinesResponse, error) {
	return &dbpb.ListPipelinesResponse{Pipelines: f.pipelines}, nil
}

func (f *fakeCronDB) TriggerRun(ctx context.Context, in *dbpb.TriggerRunRequest, opts ...grpc.CallOption) (*dbpb.TriggerRunResponse, error) {
	f.triggerRuns = append(f.triggerRuns, in)
	return &dbpb.TriggerRunResponse{Created: true}, nil
}

// TestCronFiresDueTrigger verifies the cron loop (F-09): on its first tick it
// records the next fire time for each cron trigger without firing (the next
// scheduled time is in the future), and on a later tick — once that time is
// due — it claims a run via TriggerRun (with the trigger's params and the
// dedup window) and advances to the next fire time.
func TestCronFiresDueTrigger(t *testing.T) {
	db := &fakeCronDB{pipelines: []*dbpb.Pipeline{
		{
			Id:   7,
			Name: "nightly",
			Triggers: []*dbpb.Trigger{
				{Name: "every-minute", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "* * * * *",
					Params: map[string]string{"branch": "main"}},
			},
		},
	}}
	s := &Server{logger: testLogger()}
	state := make(map[string]*cronTriggerState)

	// First tick: the trigger is seen for the first time; its next fire is the
	// next minute boundary, which is in the future, so nothing fires.
	s.processCronTriggers(context.Background(), db, &cronTestMu, state)
	if len(db.triggerRuns) != 0 {
		t.Fatalf("first tick fired %d triggers, want 0 (next fire is in the future)", len(db.triggerRuns))
	}
	entry, ok := state["7|every-minute"]
	if !ok {
		t.Fatalf("state has no entry for 7|every-minute")
	}
	if !entry.next.After(time.Now()) {
		t.Fatalf("next fire %v is not in the future", entry.next)
	}

	// Force the trigger due (rewind its next fire time) and tick again: it
	// fires and advances to the next scheduled time.
	entry.next = time.Now().Add(-time.Second)
	due := entry.next
	s.processCronTriggers(context.Background(), db, &cronTestMu, state)
	if len(db.triggerRuns) != 1 {
		t.Fatalf("second tick fired %d triggers, want 1", len(db.triggerRuns))
	}
	req := db.triggerRuns[0]
	if req.GetPipelineId() != 7 {
		t.Errorf("pipeline = %d, want 7", req.GetPipelineId())
	}
	if req.GetTriggerName() != "every-minute" {
		t.Errorf("trigger = %q, want every-minute", req.GetTriggerName())
	}
	if req.GetTriggerType() != dbpb.TriggerType_TRIGGER_TYPE_CRON {
		t.Errorf("type = %v, want CRON", req.GetTriggerType())
	}
	if req.GetParams()["branch"] != "main" {
		t.Errorf("params = %v, want branch=main", req.GetParams())
	}
	if got := req.GetDedupWindow().AsDuration(); got != cronDedupWindow {
		t.Errorf("dedup window = %v, want %v", got, cronDedupWindow)
	}
	// The next fire advanced past the due time.
	if !state["7|every-minute"].next.After(due) {
		t.Errorf("next fire %v did not advance past the due time %v", state["7|every-minute"].next, due)
	}
}

// TestCronSkipsNonCronTriggers verifies the cron loop ignores triggers that
// are not cron triggers (webhook and event triggers are handled by their own
// paths).
func TestCronSkipsNonCronTriggers(t *testing.T) {
	db := &fakeCronDB{pipelines: []*dbpb.Pipeline{
		{
			Id:   1,
			Name: "mixed",
			Triggers: []*dbpb.Trigger{
				{Name: "hook", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK, Secret: "s3cret"},
				{Name: "chain", Type: dbpb.TriggerType_TRIGGER_TYPE_EVENT, EventPipeline: "other",
					EventStatus: dbpb.RunStatus_RUN_STATUS_SUCCEEDED},
			},
		},
	}}
	s := &Server{logger: testLogger()}
	state := make(map[string]*cronTriggerState)

	s.processCronTriggers(context.Background(), db, &cronTestMu, state)

	if len(db.triggerRuns) != 0 {
		t.Errorf("fired %d triggers, want 0 (no cron triggers)", len(db.triggerRuns))
	}
	if len(state) != 0 {
		t.Errorf("state has %d entries, want 0 (no cron triggers)", len(state))
	}
}

// TestCronPrunesStaleState verifies the cron loop drops in-memory state for
// triggers that no longer exist (a pipeline was deleted or its triggers
// changed), so the map does not grow without bound.
func TestCronPrunesStaleState(t *testing.T) {
	db := &fakeCronDB{pipelines: []*dbpb.Pipeline{
		{
			Id:       1,
			Name:     "p",
			Triggers: []*dbpb.Trigger{{Name: "a", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "* * * * *"}},
		},
	}}
	s := &Server{logger: testLogger()}
	state := make(map[string]*cronTriggerState)

	// Seed state for a trigger that no longer exists.
	state["9|gone"] = &cronTriggerState{next: time.Now().Add(time.Hour)}
	// A tick that sees only trigger "a" prunes the stale "9|gone" entry.
	s.processCronTriggers(context.Background(), db, &cronTestMu, state)

	if _, ok := state["9|gone"]; ok {
		t.Errorf("stale state 9|gone was not pruned")
	}
	if _, ok := state["1|a"]; !ok {
		t.Errorf("state 1|a is missing")
	}
}

// TestCronDedupWindowIsSubMinute guards the cron dedup window's invariant: it
// must be shorter than the minimum cron period (one minute) so a racing
// replica's duplicate fire is suppressed while the next legitimate fire (a
// full period later) is not.
func TestCronDedupWindowIsSubMinute(t *testing.T) {
	if cronDedupWindow >= time.Minute {
		t.Errorf("cronDedupWindow = %v, want < 1m (the minimum cron period)", cronDedupWindow)
	}
	if cronDedupWindow <= 0 {
		t.Errorf("cronDedupWindow = %v, want > 0", cronDedupWindow)
	}
}

// cronTestMu is a shared mutex for the cron-loop tests (processCronTriggers
// takes a *sync.Mutex; the tests are single-goroutine so a package-level
// mutex is fine).
var cronTestMu sync.Mutex
