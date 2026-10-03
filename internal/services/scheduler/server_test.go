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
