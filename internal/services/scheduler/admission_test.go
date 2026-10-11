package scheduler

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fakeAdmissionDB is a stub Database client for the concurrency admission
// tests (F-16). It serves a fixed set of jobs from ListJobs, scripts the
// AcquireJobSlots result per job (acquired, or blocked by the given
// resources), and records the "assignment" events the admission loop appends
// to the shared event log. It satisfies the concurrencyAdmitter interface, so
// it can be used both as the Server's db (for tryAdmit) and as the loop's db
// argument (for reconcile / list / publish).
type fakeAdmissionDB struct {
	dbpb.DatabaseClient // nil
	jobs                []*dbpb.Job
	acquired            map[int64]bool     // job id -> whether AcquireJobSlots admits it
	blockedBy           map[int64][]string // job id -> resources blocking it (when not acquired)
	assignments         []int64            // job ids an "assignment" event was published for
	reconciled          int                // how many times ReconcileConcurrency was called
}

func (f *fakeAdmissionDB) ListJobs(ctx context.Context, in *dbpb.ListJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error) {
	resp := &dbpb.ListJobsResponse{}
	for _, job := range f.jobs {
		if in.GetStatus() == dbpb.JobStatus_JOB_STATUS_UNSPECIFIED || job.GetStatus() == in.GetStatus() {
			resp.Jobs = append(resp.Jobs, job)
		}
	}
	return resp, nil
}

func (f *fakeAdmissionDB) AcquireJobSlots(ctx context.Context, in *dbpb.AcquireJobSlotsRequest, opts ...grpc.CallOption) (*dbpb.AcquireJobSlotsResponse, error) {
	resp := &dbpb.AcquireJobSlotsResponse{}
	if f.acquired[in.GetJobId()] {
		resp.Acquired = true
	} else {
		resp.Acquired = false
		resp.BlockedBy = f.blockedBy[in.GetJobId()]
	}
	return resp, nil
}

func (f *fakeAdmissionDB) ReconcileConcurrency(ctx context.Context, in *dbpb.ReconcileConcurrencyRequest, opts ...grpc.CallOption) (*dbpb.ReconcileConcurrencyResponse, error) {
	f.reconciled++
	return &dbpb.ReconcileConcurrencyResponse{}, nil
}

func (f *fakeAdmissionDB) PublishAssignment(ctx context.Context, in *dbpb.PublishAssignmentRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	f.assignments = append(f.assignments, in.GetJobId())
	return &dbpb.PublishEventResponse{}, nil
}

// TestRunAdmissionDispatchesAdmittedJobs verifies the admission loop (F-16):
// it reconciles first, then admits every pending, dependency-free job that is
// not yet admitted — dispatching the ones the database admits (an "assignment"
// event for a group job) and leaving the blocked ones pending. A job with
// dependencies is held for the dependency resolver, and a job that is already
// admitted is not dispatched again.
func TestRunAdmissionDispatchesAdmittedJobs(t *testing.T) {
	db := &fakeAdmissionDB{
		jobs: []*dbpb.Job{
			// Pending, dependency-free, group job: admitted -> dispatched.
			{Id: 1, Status: dbpb.JobStatus_JOB_STATUS_PENDING, TargetGroup: "linux"},
			// Pending, dependency-free, group job: blocked on a key -> held.
			{Id: 2, Status: dbpb.JobStatus_JOB_STATUS_PENDING, TargetGroup: "linux"},
			// Pending with a dependency: held for the dependency resolver.
			{Id: 3, Status: dbpb.JobStatus_JOB_STATUS_PENDING, TargetGroup: "linux", DependsOn: []int64{100}},
			// Pending, dependency-free, already admitted: not dispatched again.
			{Id: 4, Status: dbpb.JobStatus_JOB_STATUS_PENDING, TargetGroup: "linux", ConcurrencyAdmitted: true},
		},
		acquired:  map[int64]bool{1: true, 2: false},
		blockedBy: map[int64][]string{2: {"prod-db"}},
	}
	s := &Server{db: db, logger: testLogger()}

	s.runAdmission(context.Background(), db)

	// Reconcile ran once, before the admission pass.
	if db.reconciled != 1 {
		t.Errorf("reconciled = %d, want 1", db.reconciled)
	}
	// Only the admitted, dependency-free job (1) is dispatched.
	if got := db.assignments; len(got) != 1 || got[0] != 1 {
		t.Errorf("assignments = %v, want [1] (only the admitted job)", got)
	}
}

// TestRunAdmissionDispatchesAgentJobs verifies that the admission loop also
// admits jobs that target no worker group (ephemeral agents, F-16): they are
// admitted (acquiring their slots) but not nudged to a live worker group.
func TestRunAdmissionDispatchesAgentJobs(t *testing.T) {
	db := &fakeAdmissionDB{
		jobs: []*dbpb.Job{
			// Pending, dependency-free, no target group: admitted for an agent.
			{Id: 7, Status: dbpb.JobStatus_JOB_STATUS_PENDING, RunId: 50},
		},
		acquired: map[int64]bool{7: true},
	}
	s := &Server{db: db, logger: testLogger()}

	s.runAdmission(context.Background(), db)

	// The agent job is admitted but not dispatched to a worker group.
	if got := db.assignments; len(got) != 0 {
		t.Errorf("assignments = %v, want none (an agent job is not nudged to a group)", got)
	}
}
