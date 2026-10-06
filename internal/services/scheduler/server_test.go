package scheduler

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// fakeCancelDB is a stub Database client for the scheduler's CancelJob RPC.
// It records the conditional CancelJob call and the "cancel" event the
// scheduler appends to the shared event log (F-23). Only the methods the RPC
// uses are implemented; the rest are never called.
type fakeCancelDB struct {
	dbpb.DatabaseClient // nil
	cancelResult        bool
	job                 *dbpb.Job
	cancelCalls         int
	cancelEvents        []int64 // job ids a "cancel" event was published for
}

func (f *fakeCancelDB) CancelJob(ctx context.Context, in *dbpb.CancelJobRequest, opts ...grpc.CallOption) (*dbpb.CancelJobResponse, error) {
	f.cancelCalls++
	return &dbpb.CancelJobResponse{Cancelled: f.cancelResult}, nil
}

func (f *fakeCancelDB) GetJob(ctx context.Context, in *dbpb.GetJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	return f.job, nil
}

func (f *fakeCancelDB) PublishCancel(ctx context.Context, in *dbpb.PublishCancelRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	f.cancelEvents = append(f.cancelEvents, in.GetJobId())
	return &dbpb.PublishEventResponse{}, nil
}

// TestCancelJobSignalsTarget verifies that the scheduler's CancelJob RPC
// (F-05) persists the cancellation (Database.CancelJob) and appends a "cancel"
// event to the shared event log (F-23), which every API pod tails and delivers
// to its local workers.
func TestCancelJobSignalsTarget(t *testing.T) {
	db := &fakeCancelDB{cancelResult: true, job: &dbpb.Job{Id: 42, Status: dbpb.JobStatus_JOB_STATUS_CANCELLED}}
	s := &Server{db: db, logger: testLogger()}

	_, err := s.CancelJob(context.Background(), &schedpb.CancelJobRequest{Id: 42})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if db.cancelCalls != 1 {
		t.Errorf("db.CancelJob called %d times, want 1", db.cancelCalls)
	}
	if len(db.cancelEvents) != 1 || db.cancelEvents[0] != 42 {
		t.Errorf("cancel events = %v, want [42]", db.cancelEvents)
	}
}

// TestCancelJobNoopWhenTerminal verifies that cancelling a job that already
// reached a terminal state is a no-op (F-05): the database reports it was not
// cancelled, and the scheduler does not append a cancel event.
func TestCancelJobNoopWhenTerminal(t *testing.T) {
	db := &fakeCancelDB{cancelResult: false, job: &dbpb.Job{Id: 42, Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED}}
	s := &Server{db: db, logger: testLogger()}

	job, err := s.CancelJob(context.Background(), &schedpb.CancelJobRequest{Id: 42})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if job.GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("status = %v, want SUCCEEDED (unchanged)", job.GetStatus())
	}
	if len(db.cancelEvents) != 0 {
		t.Errorf("cancel events = %v, want none (terminal job is a no-op)", db.cancelEvents)
	}
}

// fakeDispatchDB is a stub Database client for the scheduler's dispatch logic:
// it records the "assignment" events the scheduler appends to the shared event
// log (F-23). Only the methods dispatchRunInstance uses are implemented.
type fakeDispatchDB struct {
	dbpb.DatabaseClient         // nil
	assignments         []int64 // job ids an "assignment" event was published for
}

func (f *fakeDispatchDB) PublishAssignment(ctx context.Context, in *dbpb.PublishAssignmentRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	f.assignments = append(f.assignments, in.GetJobId())
	return &dbpb.PublishEventResponse{}, nil
}

// TestDispatchRunInstanceHoldsNeeds verifies that a run's job instance with
// needs (F-08) is held pending for the dependency resolver (not dispatched),
// while a dependency-free instance with a target group is dispatched.
func TestDispatchRunInstanceHoldsNeeds(t *testing.T) {
	db := &fakeDispatchDB{}
	s := &Server{db: db, logger: testLogger()}
	ctx := context.Background()

	// An instance that depends on another job is held pending (not dispatched).
	s.dispatchRunInstance(ctx, &dbpb.Job{Id: 1, RunId: 10, DependsOn: []int64{100}, TargetGroup: "linux"})
	// An instance with no dependencies and a target group is dispatched.
	s.dispatchRunInstance(ctx, &dbpb.Job{Id: 2, RunId: 10, TargetGroup: "linux"})
	// An instance with no dependencies and no target group is queued for an
	// ephemeral agent (not dispatched).
	s.dispatchRunInstance(ctx, &dbpb.Job{Id: 3, RunId: 10})

	if got := db.assignments; len(got) != 1 || got[0] != 2 {
		t.Errorf("assignments = %v, want [2] (only the dependency-free group job)", got)
	}
}

// fakeTriggerRunDB is a stub Database client for the scheduler's TriggerRun
// RPC (F-09): it records the TriggerRun call and the "assignment" events the
// scheduler appends to the shared event log (F-23) for the run's job
// instances.
type fakeTriggerRunDB struct {
	dbpb.DatabaseClient // nil
	run                 *dbpb.PipelineRun
	jobs                []*dbpb.Job
	created             bool
	triggerRuns         []*dbpb.TriggerRunRequest
	assignments         []int64
}

func (f *fakeTriggerRunDB) TriggerRun(ctx context.Context, in *dbpb.TriggerRunRequest, opts ...grpc.CallOption) (*dbpb.TriggerRunResponse, error) {
	f.triggerRuns = append(f.triggerRuns, in)
	resp := &dbpb.TriggerRunResponse{Created: f.created, Run: f.run}
	for _, job := range f.jobs {
		resp.Jobs = append(resp.Jobs, job)
	}
	return resp, nil
}

func (f *fakeTriggerRunDB) PublishAssignment(ctx context.Context, in *dbpb.PublishAssignmentRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	f.assignments = append(f.assignments, in.GetJobId())
	return &dbpb.PublishEventResponse{}, nil
}

// TestTriggerRunDispatchesWhenCreated verifies the scheduler's TriggerRun RPC
// (F-09): when the database creates the run, the scheduler drives its job
// instances (dispatching a dependency-free group job to the event log).
func TestTriggerRunDispatchesWhenCreated(t *testing.T) {
	db := &fakeTriggerRunDB{
		created: true,
		run:     &dbpb.PipelineRun{Id: 50, PipelineId: 7, Status: dbpb.RunStatus_RUN_STATUS_PENDING, Trigger: "cron", TriggerName: "nightly"},
		jobs:    []*dbpb.Job{{Id: 51, RunId: 50, TargetGroup: "linux-pool"}},
	}
	s := &Server{db: db, logger: testLogger()}

	run, err := s.TriggerRun(context.Background(), &schedpb.TriggerRunRequest{
		PipelineId:  7,
		TriggerName: "nightly",
		TriggerType: dbpb.TriggerType_TRIGGER_TYPE_CRON,
	})
	if err != nil {
		t.Fatalf("TriggerRun: %v", err)
	}
	if run.GetId() != 50 {
		t.Errorf("run id = %d, want 50", run.GetId())
	}
	if len(db.triggerRuns) != 1 {
		t.Fatalf("db.TriggerRun called %d times, want 1", len(db.triggerRuns))
	}
	if got := db.assignments; len(got) != 1 || got[0] != 51 {
		t.Errorf("assignments = %v, want [51] (the created run's group job)", got)
	}
}

// TestTriggerRunNoopWhenAlreadyClaimed verifies the scheduler's TriggerRun RPC
// (F-09): when the database reports the run was already claimed by an earlier
// call (a duplicate for the same fire window), the scheduler drives nothing.
func TestTriggerRunNoopWhenAlreadyClaimed(t *testing.T) {
	db := &fakeTriggerRunDB{
		created: false,
		run:     &dbpb.PipelineRun{Id: 50, PipelineId: 7, Status: dbpb.RunStatus_RUN_STATUS_PENDING, Trigger: "cron", TriggerName: "nightly"},
		jobs:    []*dbpb.Job{{Id: 51, RunId: 50, TargetGroup: "linux-pool"}},
	}
	s := &Server{db: db, logger: testLogger()}

	run, err := s.TriggerRun(context.Background(), &schedpb.TriggerRunRequest{
		PipelineId:  7,
		TriggerName: "nightly",
		TriggerType: dbpb.TriggerType_TRIGGER_TYPE_CRON,
	})
	if err != nil {
		t.Fatalf("TriggerRun: %v", err)
	}
	if run.GetId() != 50 {
		t.Errorf("run id = %d, want 50 (the existing run)", run.GetId())
	}
	if len(db.triggerRuns) != 1 {
		t.Fatalf("db.TriggerRun called %d times, want 1", len(db.triggerRuns))
	}
	if len(db.assignments) != 0 {
		t.Errorf("assignments = %v, want none (the run was already claimed)", db.assignments)
	}
}

// TestTriggerRunValidates verifies the scheduler's TriggerRun RPC (F-09)
// rejects a request with no pipeline id or trigger name.
func TestTriggerRunValidates(t *testing.T) {
	s := &Server{db: &fakeTriggerRunDB{}, logger: testLogger()}

	if _, err := s.TriggerRun(context.Background(), &schedpb.TriggerRunRequest{TriggerName: "x"}); err == nil {
		t.Errorf("TriggerRun with no pipeline_id: want error, got nil")
	}
	if _, err := s.TriggerRun(context.Background(), &schedpb.TriggerRunRequest{PipelineId: 7}); err == nil {
		t.Errorf("TriggerRun with no trigger_name: want error, got nil")
	}
}

// TestCreateRunForwardsPipelineVersion verifies the scheduler's CreateRun RPC
// forwards the requested pipeline version to the database (F-11), so a run can
// be created against a specific (older) version of the pipeline.
func TestCreateRunForwardsPipelineVersion(t *testing.T) {
	db := &fakeCreateRunDB{
		response: &dbpb.CreateRunResponse{
			Run: &dbpb.PipelineRun{Id: 42, PipelineId: 3, PipelineVersion: 2, Status: dbpb.RunStatus_RUN_STATUS_PENDING, Trigger: "manual"},
		},
	}
	s := &Server{db: db, logger: testLogger()}

	run, err := s.CreateRun(context.Background(), &schedpb.CreateRunRequest{
		PipelineId:      3,
		Trigger:         "manual",
		PipelineVersion: 2,
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if got := run.GetPipelineVersion(); got != 2 {
		t.Errorf("run pipeline_version = %d, want 2", got)
	}
	if db.request == nil {
		t.Fatal("db.CreateRun was not called")
	}
	if got := db.request.GetPipelineVersion(); got != 2 {
		t.Errorf("db.CreateRun pipeline_version = %d, want 2", got)
	}
}
