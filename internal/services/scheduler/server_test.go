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
