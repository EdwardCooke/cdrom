package scheduler

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fakeDependencyDB is a stub DatabaseClient for the dependency-resolver
// tests: it holds a set of jobs keyed by id, serves ListJobs (filtered by
// status) and GetJob from that set, and records SkipJob/UpdateJob calls.
type fakeDependencyDB struct {
	jobs map[int64]*dbpb.Job

	skipped        map[int64]bool // job id -> whether SkipJob reports skipped
	skipCalls      []int64
	clearedDeps    []int64 // job ids UpdateJob cleared depends_on for
	updateJobError error
}

func (f *fakeDependencyDB) ListJobs(ctx context.Context, in *dbpb.ListJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error) {
	resp := &dbpb.ListJobsResponse{}
	for _, job := range f.jobs {
		if in.GetStatus() == dbpb.JobStatus_JOB_STATUS_UNSPECIFIED || job.GetStatus() == in.GetStatus() {
			resp.Jobs = append(resp.Jobs, job)
		}
	}
	return resp, nil
}

func (f *fakeDependencyDB) GetJob(ctx context.Context, in *dbpb.GetJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	job, ok := f.jobs[in.GetId()]
	if !ok {
		return nil, errNotFound
	}
	return job, nil
}

func (f *fakeDependencyDB) SkipJob(ctx context.Context, in *dbpb.SkipJobRequest, opts ...grpc.CallOption) (*dbpb.SkipJobResponse, error) {
	f.skipCalls = append(f.skipCalls, in.GetId())
	skipped := true
	if f.skipped != nil {
		skipped = f.skipped[in.GetId()]
	}
	if skipped {
		if job, ok := f.jobs[in.GetId()]; ok {
			job.Status = dbpb.JobStatus_JOB_STATUS_SKIPPED
		}
	}
	return &dbpb.SkipJobResponse{Skipped: skipped}, nil
}

func (f *fakeDependencyDB) UpdateJob(ctx context.Context, in *dbpb.UpdateJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	if f.updateJobError != nil {
		return nil, f.updateJobError
	}
	if in.GetClearDependsOn() {
		f.clearedDeps = append(f.clearedDeps, in.GetId())
		if job, ok := f.jobs[in.GetId()]; ok {
			job.DependsOn = nil
		}
	}
	return f.jobs[in.GetId()], nil
}

var errNotFound = errFakeNotFound("job not found")

type errFakeNotFound string

func (e errFakeNotFound) Error() string { return string(e) }

// TestResolveJobDependenciesSkipsOnFailedDependency verifies that a pending
// job with a dependency that reached a terminal state other than succeeded
// is skipped (F-06), and the UI is notified.
func TestResolveJobDependenciesSkipsOnFailedDependency(t *testing.T) {
	db := &fakeDependencyDB{jobs: map[int64]*dbpb.Job{
		1: {Id: 1, Status: dbpb.JobStatus_JOB_STATUS_FAILED},
		2: {Id: 2, Status: dbpb.JobStatus_JOB_STATUS_PENDING, DependsOn: []int64{1}},
	}}
	api := &fakeRelayer{}
	s := NewServer(nil, nil, testLogger())
	s.resolveJobDependencies(context.Background(), db, api)

	if len(db.skipCalls) != 1 || db.skipCalls[0] != 2 {
		t.Fatalf("skipCalls = %v, want [2]", db.skipCalls)
	}
	if status, ok := api.notified[2]; !ok || status != dbpb.JobStatus_JOB_STATUS_SKIPPED {
		t.Errorf("notified[2] = %v, %v, want SKIPPED, true", status, ok)
	}
}

// TestResolveJobDependenciesDispatchesWhenAllSucceeded verifies that a
// pending job whose dependencies all succeeded has its depends_on cleared
// and is dispatched (F-06).
func TestResolveJobDependenciesDispatchesWhenAllSucceeded(t *testing.T) {
	db := &fakeDependencyDB{jobs: map[int64]*dbpb.Job{
		1: {Id: 1, Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED},
		2: {Id: 2, Status: dbpb.JobStatus_JOB_STATUS_PENDING, DependsOn: []int64{1}, TargetGroup: "linux"},
	}}
	api := &fakeRelayer{}
	s := NewServer(nil, nil, testLogger())
	s.resolveJobDependencies(context.Background(), db, api)

	if len(db.clearedDeps) != 1 || db.clearedDeps[0] != 2 {
		t.Fatalf("clearedDeps = %v, want [2]", db.clearedDeps)
	}
	if got := api.dispatchedIDs(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("dispatched = %v, want [2]", got)
	}
	if len(db.skipCalls) != 0 {
		t.Errorf("skipCalls = %v, want none", db.skipCalls)
	}
}

// TestResolveJobDependenciesWaitsWhilePending verifies that a job whose
// dependency is still pending or running is left alone (neither skipped nor
// dispatched), to be reconsidered on the resolver's next tick.
func TestResolveJobDependenciesWaitsWhilePending(t *testing.T) {
	db := &fakeDependencyDB{jobs: map[int64]*dbpb.Job{
		1: {Id: 1, Status: dbpb.JobStatus_JOB_STATUS_RUNNING},
		2: {Id: 2, Status: dbpb.JobStatus_JOB_STATUS_PENDING, DependsOn: []int64{1}, TargetGroup: "linux"},
	}}
	api := &fakeRelayer{}
	s := NewServer(nil, nil, testLogger())
	s.resolveJobDependencies(context.Background(), db, api)

	if len(db.skipCalls) != 0 {
		t.Errorf("skipCalls = %v, want none", db.skipCalls)
	}
	if len(db.clearedDeps) != 0 {
		t.Errorf("clearedDeps = %v, want none", db.clearedDeps)
	}
	if got := api.dispatchedIDs(); len(got) != 0 {
		t.Errorf("dispatched = %v, want none", got)
	}
}

// TestResolveJobDependenciesIgnoresJobsWithoutDependencies verifies that a
// pending job with no DependsOn is left untouched by the resolver (it is not
// its concern — SubmitJob already dispatched it, or it awaits an ephemeral
// agent).
func TestResolveJobDependenciesIgnoresJobsWithoutDependencies(t *testing.T) {
	db := &fakeDependencyDB{jobs: map[int64]*dbpb.Job{
		1: {Id: 1, Status: dbpb.JobStatus_JOB_STATUS_PENDING},
	}}
	api := &fakeRelayer{}
	s := NewServer(nil, nil, testLogger())
	s.resolveJobDependencies(context.Background(), db, api)

	if len(db.skipCalls) != 0 || len(db.clearedDeps) != 0 || len(api.dispatchedIDs()) != 0 {
		t.Error("resolver acted on a job with no dependencies")
	}
}

// TestResolveJobDependenciesEmptyTargetGroupNoDispatch verifies that a
// released job with no target group (queued for an ephemeral agent) has its
// depends_on cleared but is not dispatched (there is no live worker group to
// fan the job out to).
func TestResolveJobDependenciesEmptyTargetGroupNoDispatch(t *testing.T) {
	db := &fakeDependencyDB{jobs: map[int64]*dbpb.Job{
		1: {Id: 1, Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED},
		2: {Id: 2, Status: dbpb.JobStatus_JOB_STATUS_PENDING, DependsOn: []int64{1}},
	}}
	api := &fakeRelayer{}
	s := NewServer(nil, nil, testLogger())
	s.resolveJobDependencies(context.Background(), db, api)

	if len(db.clearedDeps) != 1 || db.clearedDeps[0] != 2 {
		t.Fatalf("clearedDeps = %v, want [2]", db.clearedDeps)
	}
	if got := api.dispatchedIDs(); len(got) != 0 {
		t.Errorf("dispatched = %v, want none (no target group)", got)
	}
}

// TestResolveJobDependenciesSkipPropagatesThroughSkippedDependency verifies
// that a job depending on an already-skipped job is itself skipped (skip
// propagates transitively, one resolver tick at a time).
func TestResolveJobDependenciesSkipPropagatesThroughSkippedDependency(t *testing.T) {
	db := &fakeDependencyDB{jobs: map[int64]*dbpb.Job{
		1: {Id: 1, Status: dbpb.JobStatus_JOB_STATUS_SKIPPED},
		2: {Id: 2, Status: dbpb.JobStatus_JOB_STATUS_PENDING, DependsOn: []int64{1}},
	}}
	api := &fakeRelayer{}
	s := NewServer(nil, nil, testLogger())
	s.resolveJobDependencies(context.Background(), db, api)

	if len(db.skipCalls) != 1 || db.skipCalls[0] != 2 {
		t.Fatalf("skipCalls = %v, want [2]", db.skipCalls)
	}
}

// TestResolveJobDependenciesSkipAlreadyStartedIsNoop verifies that when
// SkipJob reports the job was not actually skipped (it already started or
// finished between the list and the skip), the resolver does not notify the
// UI.
func TestResolveJobDependenciesSkipAlreadyStartedIsNoop(t *testing.T) {
	db := &fakeDependencyDB{
		jobs: map[int64]*dbpb.Job{
			1: {Id: 1, Status: dbpb.JobStatus_JOB_STATUS_FAILED},
			2: {Id: 2, Status: dbpb.JobStatus_JOB_STATUS_PENDING, DependsOn: []int64{1}},
		},
		skipped: map[int64]bool{2: false},
	}
	api := &fakeRelayer{}
	s := NewServer(nil, nil, testLogger())
	s.resolveJobDependencies(context.Background(), db, api)

	if len(db.skipCalls) != 1 {
		t.Fatalf("skipCalls = %v, want one call", db.skipCalls)
	}
	if _, notified := api.notified[2]; notified {
		t.Error("UI was notified for a job that was not actually skipped")
	}
}

// TestResolveJobDependenciesFailedDependencyWithIgnoreFailedDispatches
// verifies that a dependency that failed (or timed out) but has ignore_failed
// set counts as satisfied (F-06): the dependent job is released and
// dispatched rather than skipped.
func TestResolveJobDependenciesFailedDependencyWithIgnoreFailedDispatches(t *testing.T) {
	db := &fakeDependencyDB{jobs: map[int64]*dbpb.Job{
		1: {Id: 1, Status: dbpb.JobStatus_JOB_STATUS_FAILED, IgnoreFailed: true},
		2: {Id: 2, Status: dbpb.JobStatus_JOB_STATUS_PENDING, DependsOn: []int64{1}, TargetGroup: "linux"},
	}}
	api := &fakeRelayer{}
	s := NewServer(nil, nil, testLogger())
	s.resolveJobDependencies(context.Background(), db, api)

	if len(db.skipCalls) != 0 {
		t.Errorf("skipCalls = %v, want none (ignore_failed dependency is satisfied)", db.skipCalls)
	}
	if len(db.clearedDeps) != 1 || db.clearedDeps[0] != 2 {
		t.Errorf("clearedDeps = %v, want [2]", db.clearedDeps)
	}
	if got := api.dispatchedIDs(); len(got) != 1 || got[0] != 2 {
		t.Errorf("dispatched = %v, want [2]", got)
	}
}

// TestResolveJobDependenciesTimedOutDependencyWithIgnoreFailedDispatches
// verifies that a timed_out dependency with ignore_failed set is likewise
// treated as satisfied (F-06).
func TestResolveJobDependenciesTimedOutDependencyWithIgnoreFailedDispatches(t *testing.T) {
	db := &fakeDependencyDB{jobs: map[int64]*dbpb.Job{
		1: {Id: 1, Status: dbpb.JobStatus_JOB_STATUS_TIMED_OUT, IgnoreFailed: true},
		2: {Id: 2, Status: dbpb.JobStatus_JOB_STATUS_PENDING, DependsOn: []int64{1}, TargetGroup: "linux"},
	}}
	api := &fakeRelayer{}
	s := NewServer(nil, nil, testLogger())
	s.resolveJobDependencies(context.Background(), db, api)

	if len(db.skipCalls) != 0 {
		t.Errorf("skipCalls = %v, want none", db.skipCalls)
	}
	if got := api.dispatchedIDs(); len(got) != 1 || got[0] != 2 {
		t.Errorf("dispatched = %v, want [2]", got)
	}
}

// TestResolveJobDependenciesCancelledDependencyWithIgnoreFailedSkips verifies
// that a cancelled dependency is NOT overridden by ignore_failed (F-06): a
// cancellation always blocks the dependent job, so it is skipped.
func TestResolveJobDependenciesCancelledDependencyWithIgnoreFailedSkips(t *testing.T) {
	db := &fakeDependencyDB{jobs: map[int64]*dbpb.Job{
		1: {Id: 1, Status: dbpb.JobStatus_JOB_STATUS_CANCELLED, IgnoreFailed: true},
		2: {Id: 2, Status: dbpb.JobStatus_JOB_STATUS_PENDING, DependsOn: []int64{1}, TargetGroup: "linux"},
	}}
	api := &fakeRelayer{}
	s := NewServer(nil, nil, testLogger())
	s.resolveJobDependencies(context.Background(), db, api)

	if len(db.skipCalls) != 1 || db.skipCalls[0] != 2 {
		t.Fatalf("skipCalls = %v, want [2] (a cancelled dependency always blocks)", db.skipCalls)
	}
	if got := api.dispatchedIDs(); len(got) != 0 {
		t.Errorf("dispatched = %v, want none", got)
	}
}
