package scheduler

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// fakeCancelDB is a stub Database client for the scheduler's CancelJob RPC.
// Only the methods the RPC uses are implemented; the rest are never called.
type fakeCancelDB struct {
	dbpb.DatabaseClient // nil
	cancelResult        bool
	job                 *dbpb.Job
	cancelCalls         int
}

func (f *fakeCancelDB) CancelJob(ctx context.Context, in *dbpb.CancelJobRequest, opts ...grpc.CallOption) (*dbpb.CancelJobResponse, error) {
	f.cancelCalls++
	return &dbpb.CancelJobResponse{Cancelled: f.cancelResult}, nil
}

func (f *fakeCancelDB) GetJob(ctx context.Context, in *dbpb.GetJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	return f.job, nil
}

// fakeCancelAPI is a stub API client for the scheduler's CancelJob RPC. It
// records the jobs the scheduler signalled to cancel.
type fakeCancelAPI struct {
	apipb.APIClient // nil
	cancelled       []int64
}

func (f *fakeCancelAPI) CancelJob(ctx context.Context, in *apipb.CancelJobRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	f.cancelled = append(f.cancelled, in.GetJobId())
	return &emptypb.Empty{}, nil
}

// TestCancelJobSignalsTarget verifies that the scheduler's CancelJob RPC
// (F-05) persists the cancellation (Database.CancelJob) and signals the
// execution target to stop the work (API.CancelJob).
func TestCancelJobSignalsTarget(t *testing.T) {
	db := &fakeCancelDB{cancelResult: true, job: &dbpb.Job{Id: 42, Status: dbpb.JobStatus_JOB_STATUS_CANCELLED}}
	api := &fakeCancelAPI{}
	s := &Server{db: db, api: api, logger: testLogger()}

	_, err := s.CancelJob(context.Background(), &schedpb.CancelJobRequest{Id: 42})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if db.cancelCalls != 1 {
		t.Errorf("db.CancelJob called %d times, want 1", db.cancelCalls)
	}
	if len(api.cancelled) != 1 || api.cancelled[0] != 42 {
		t.Errorf("api.CancelJob = %v, want [42]", api.cancelled)
	}
}

// TestCancelJobNoopWhenTerminal verifies that cancelling a job that already
// reached a terminal state is a no-op (F-05): the database reports it was not
// cancelled, and the scheduler does not signal the target.
func TestCancelJobNoopWhenTerminal(t *testing.T) {
	db := &fakeCancelDB{cancelResult: false, job: &dbpb.Job{Id: 42, Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED}}
	api := &fakeCancelAPI{}
	s := &Server{db: db, api: api, logger: testLogger()}

	job, err := s.CancelJob(context.Background(), &schedpb.CancelJobRequest{Id: 42})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if job.GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("status = %v, want SUCCEEDED (unchanged)", job.GetStatus())
	}
	if len(api.cancelled) != 0 {
		t.Errorf("api.CancelJob = %v, want none (terminal job is a no-op)", api.cancelled)
	}
}
