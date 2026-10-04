package scheduler

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// fakeEventDB is a stub Database client for the event-loop tests: it returns
// the pipelines (with their event triggers) and the recently-finished runs,
// and records the TriggerRun calls the loop makes.
type fakeEventDB struct {
	dbpb.DatabaseClient // nil
	pipelines           []*dbpb.Pipeline
	runs                []*dbpb.PipelineRun
	triggerRuns         []*dbpb.TriggerRunRequest
}

func (f *fakeEventDB) ListPipelines(ctx context.Context, in *dbpb.ListPipelinesRequest, opts ...grpc.CallOption) (*dbpb.ListPipelinesResponse, error) {
	// Mirror the database's trigger-type filter (F-09): when a type is
	// requested, return only the pipelines that carry a trigger of that type.
	if triggerType := in.GetTriggerType(); triggerType != dbpb.TriggerType_TRIGGER_TYPE_UNSPECIFIED {
		filtered := make([]*dbpb.Pipeline, 0, len(f.pipelines))
		for _, pipeline := range f.pipelines {
			for _, trigger := range pipeline.GetTriggers() {
				if trigger.GetType() == triggerType {
					filtered = append(filtered, pipeline)
					break
				}
			}
		}
		return &dbpb.ListPipelinesResponse{Pipelines: filtered}, nil
	}
	return &dbpb.ListPipelinesResponse{Pipelines: f.pipelines}, nil
}

func (f *fakeEventDB) ListRuns(ctx context.Context, in *dbpb.ListRunsRequest, opts ...grpc.CallOption) (*dbpb.ListRunsResponse, error) {
	return &dbpb.ListRunsResponse{Runs: f.runs}, nil
}

func (f *fakeEventDB) TriggerRun(ctx context.Context, in *dbpb.TriggerRunRequest, opts ...grpc.CallOption) (*dbpb.TriggerRunResponse, error) {
	f.triggerRuns = append(f.triggerRuns, in)
	return &dbpb.TriggerRunResponse{Created: true}, nil
}

// TestEventFiresMatchingRun verifies the event loop (F-09): when a run of the
// watched pipeline reaches the watched status, the loop starts a run of the
// triggered pipeline (via TriggerRun), recording the source run's id and the
// source pipeline's name in the run's params.
func TestEventFiresMatchingRun(t *testing.T) {
	db := &fakeEventDB{
		pipelines: []*dbpb.Pipeline{
			{Id: 1, Name: "build"}, // the watched pipeline (no triggers of its own)
			{
				Id:   2,
				Name: "deploy",
				Triggers: []*dbpb.Trigger{
					{
						Name:          "on-build-success",
						Type:          dbpb.TriggerType_TRIGGER_TYPE_EVENT,
						EventPipeline: "build",
						EventStatus:   dbpb.RunStatus_RUN_STATUS_SUCCEEDED,
						Params:        map[string]string{"env": "prod"},
					},
				},
			},
		},
		runs: []*dbpb.PipelineRun{
			{Id: 100, PipelineId: 1, PipelineName: "build", Status: dbpb.RunStatus_RUN_STATUS_SUCCEEDED,
				FinishedAt: timestamppb.New(time.Now().Add(-time.Minute))},
		},
	}
	s := &Server{logger: testLogger()}
	processed := make(map[int64]struct{})

	s.processEventTriggers(context.Background(), db, processed)

	if len(db.triggerRuns) != 1 {
		t.Fatalf("TriggerRun called %d times, want 1", len(db.triggerRuns))
	}
	req := db.triggerRuns[0]
	if req.GetPipelineId() != 2 {
		t.Errorf("pipeline = %d, want 2 (the triggered pipeline)", req.GetPipelineId())
	}
	if req.GetTriggerName() != "on-build-success" {
		t.Errorf("trigger = %q, want on-build-success", req.GetTriggerName())
	}
	if req.GetTriggerType() != dbpb.TriggerType_TRIGGER_TYPE_EVENT {
		t.Errorf("type = %v, want EVENT", req.GetTriggerType())
	}
	if req.GetParams()[models.ParamKeySourceRun] != "100" {
		t.Errorf("source_run param = %q, want 100", req.GetParams()[models.ParamKeySourceRun])
	}
	if req.GetParams()[models.ParamKeySourcePipeline] != "build" {
		t.Errorf("source_pipeline param = %q, want build", req.GetParams()[models.ParamKeySourcePipeline])
	}
	if req.GetParams()["env"] != "prod" {
		t.Errorf("static param env = %q, want prod", req.GetParams()["env"])
	}
	// The source run is marked processed so it is not re-fired on the next
	// tick.
	if _, done := processed[100]; !done {
		t.Errorf("source run 100 not marked processed")
	}
}

// TestEventIgnoresNonMatchingStatus verifies the event loop does not fire a
// trigger when the run's status does not match the trigger's watched status.
func TestEventIgnoresNonMatchingStatus(t *testing.T) {
	db := &fakeEventDB{
		pipelines: []*dbpb.Pipeline{
			{Id: 1, Name: "build"},
			{
				Id:   2,
				Name: "deploy",
				Triggers: []*dbpb.Trigger{
					{Name: "on-build-success", Type: dbpb.TriggerType_TRIGGER_TYPE_EVENT,
						EventPipeline: "build", EventStatus: dbpb.RunStatus_RUN_STATUS_SUCCEEDED},
				},
			},
		},
		runs: []*dbpb.PipelineRun{
			// The build run failed, not succeeded: the trigger does not fire.
			{Id: 100, PipelineId: 1, PipelineName: "build", Status: dbpb.RunStatus_RUN_STATUS_FAILED,
				FinishedAt: timestamppb.New(time.Now().Add(-time.Minute))},
		},
	}
	s := &Server{logger: testLogger()}
	processed := make(map[int64]struct{})

	s.processEventTriggers(context.Background(), db, processed)

	if len(db.triggerRuns) != 0 {
		t.Errorf("TriggerRun called %d times, want 0 (status does not match)", len(db.triggerRuns))
	}
}

// TestEventIgnoresNonMatchingPipeline verifies the event loop does not fire a
// trigger when the finished run belongs to a different pipeline than the one
// the trigger watches.
func TestEventIgnoresNonMatchingPipeline(t *testing.T) {
	db := &fakeEventDB{
		pipelines: []*dbpb.Pipeline{
			{Id: 1, Name: "build"},
			{Id: 2, Name: "other"},
			{
				Id:   3,
				Name: "deploy",
				Triggers: []*dbpb.Trigger{
					{Name: "on-build-success", Type: dbpb.TriggerType_TRIGGER_TYPE_EVENT,
						EventPipeline: "build", EventStatus: dbpb.RunStatus_RUN_STATUS_SUCCEEDED},
				},
			},
		},
		runs: []*dbpb.PipelineRun{
			// A run of "other" (not "build"): the trigger does not fire.
			{Id: 100, PipelineId: 2, PipelineName: "other", Status: dbpb.RunStatus_RUN_STATUS_SUCCEEDED,
				FinishedAt: timestamppb.New(time.Now().Add(-time.Minute))},
		},
	}
	s := &Server{logger: testLogger()}
	processed := make(map[int64]struct{})

	s.processEventTriggers(context.Background(), db, processed)

	if len(db.triggerRuns) != 0 {
		t.Errorf("TriggerRun called %d times, want 0 (pipeline does not match)", len(db.triggerRuns))
	}
}

// TestEventDoesNotRefireProcessedRun verifies the event loop does not re-fire
// a source run it has already fired (the in-memory processed set), so a run
// that keeps appearing in the lookback window is fired once.
func TestEventDoesNotRefireProcessedRun(t *testing.T) {
	db := &fakeEventDB{
		pipelines: []*dbpb.Pipeline{
			{Id: 1, Name: "build"},
			{
				Id:   2,
				Name: "deploy",
				Triggers: []*dbpb.Trigger{
					{Name: "on-build-success", Type: dbpb.TriggerType_TRIGGER_TYPE_EVENT,
						EventPipeline: "build", EventStatus: dbpb.RunStatus_RUN_STATUS_SUCCEEDED},
				},
			},
		},
		runs: []*dbpb.PipelineRun{
			{Id: 100, PipelineId: 1, PipelineName: "build", Status: dbpb.RunStatus_RUN_STATUS_SUCCEEDED,
				FinishedAt: timestamppb.New(time.Now().Add(-time.Minute))},
		},
	}
	s := &Server{logger: testLogger()}
	processed := make(map[int64]struct{})

	// First tick fires the run.
	s.processEventTriggers(context.Background(), db, processed)
	if len(db.triggerRuns) != 1 {
		t.Fatalf("first tick fired %d times, want 1", len(db.triggerRuns))
	}
	// Second tick: the run is already processed, so it is not re-fired.
	s.processEventTriggers(context.Background(), db, processed)
	if len(db.triggerRuns) != 1 {
		t.Errorf("after second tick TriggerRun called %d times, want 1 (no re-fire)", len(db.triggerRuns))
	}
}

// TestEventNoRulesIsNoop verifies the event loop is a no-op when no pipeline
// has an event trigger (it does not even scan for runs).
func TestEventNoRulesIsNoop(t *testing.T) {
	db := &fakeEventDB{
		pipelines: []*dbpb.Pipeline{
			{Id: 1, Name: "build"},
			{Id: 2, Name: "deploy", Triggers: []*dbpb.Trigger{
				{Name: "hook", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK, Secret: "s3cret"},
			}},
		},
		runs: []*dbpb.PipelineRun{
			{Id: 100, PipelineId: 1, PipelineName: "build", Status: dbpb.RunStatus_RUN_STATUS_SUCCEEDED,
				FinishedAt: timestamppb.New(time.Now().Add(-time.Minute))},
		},
	}
	s := &Server{logger: testLogger()}
	processed := make(map[int64]struct{})

	s.processEventTriggers(context.Background(), db, processed)

	if len(db.triggerRuns) != 0 {
		t.Errorf("TriggerRun called %d times, want 0 (no event triggers)", len(db.triggerRuns))
	}
}
