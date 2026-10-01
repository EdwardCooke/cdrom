package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// statusNotFound and statusFailedPrecondition are canned gRPC errors the
// fakeRerunDB returns, mirroring the Database service's behavior.
var (
	statusNotFound           = status.Error(codes.NotFound, "record not found")
	statusFailedPrecondition = status.Error(codes.FailedPrecondition, "job is not in a terminal state")
)

// fakeRetryDB is a stub DatabaseClient for the retry-loop tests: it returns
// the retriable jobs (failed and still within budget, mirroring the database
// filter) from ListRetriableJobs and records ClaimJobRetry calls.
type fakeRetryDB struct {
	jobs    []*dbpb.Job
	claimed map[int64]bool // job id -> whether ClaimJobRetry reports claimed
	claims  []int64        // job ids ClaimJobRetry was called for
}

// retriable mirrors the database's ListRetriableJobs filter: a job is
// retriable when it is failed and still has retries remaining (a retry policy
// with max_attempts > 0 and an attempt counter below the budget).
func retriable(job *dbpb.Job) bool {
	if job.GetStatus() != dbpb.JobStatus_JOB_STATUS_FAILED {
		return false
	}
	if job.GetMaxAttempts() == 0 {
		return false
	}
	return job.GetAttempt() < job.GetMaxAttempts()+1
}

func (f *fakeRetryDB) ListRetriableJobs(ctx context.Context, in *dbpb.ListRetriableJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error) {
	resp := &dbpb.ListJobsResponse{}
	for _, job := range f.jobs {
		if retriable(job) {
			resp.Jobs = append(resp.Jobs, job)
		}
	}
	return resp, nil
}

func (f *fakeRetryDB) ClaimJobRetry(ctx context.Context, in *dbpb.ClaimJobRetryRequest, opts ...grpc.CallOption) (*dbpb.ClaimJobRetryResponse, error) {
	f.claims = append(f.claims, in.GetId())
	claimed := f.claimed[in.GetId()]
	return &dbpb.ClaimJobRetryResponse{Claimed: claimed, Attempt: 2}, nil
}

// fakeRelayer is a stub APIClient that records NotifyJobStatus and
// DispatchJob calls.
type fakeRelayer struct {
	// mu guards dispatched: a retry's backoff dispatch runs in a detached
	// goroutine that appends to it while the test's goroutine reads it.
	mu         sync.Mutex
	notified   map[int64]dbpb.JobStatus
	dispatched []int64
}

func (f *fakeRelayer) NotifyJobStatus(ctx context.Context, in *apipb.NotifyJobStatusRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	if f.notified == nil {
		f.notified = make(map[int64]dbpb.JobStatus)
	}
	f.notified[in.GetJobId()] = in.GetStatus()
	return &emptypb.Empty{}, nil
}

func (f *fakeRelayer) DispatchJob(ctx context.Context, in *apipb.DispatchJobRequest, opts ...grpc.CallOption) (*apipb.DispatchJobResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatched = append(f.dispatched, in.GetJob().GetId())
	return &apipb.DispatchJobResponse{Dispatched: 1}, nil
}

// dispatchedIDs returns a copy of the recorded dispatches, safe to call from
// the test's goroutine while a retry's backoff goroutine may still be
// dispatching (F-04).
func (f *fakeRelayer) dispatchedIDs() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int64, len(f.dispatched))
	copy(out, f.dispatched)
	return out
}

// TestRetryFailedJobs verifies the retry loop (F-04): the loop asks the
// database for the jobs that still have retries remaining (the database
// filters out jobs with no retry policy and jobs that have exhausted their
// budget) and re-dispatches each (with the reset to pending fanned out to the
// UI). Jobs the database does not report are never claimed or dispatched.
func TestRetryFailedJobs(t *testing.T) {
	db := &fakeRetryDB{
		jobs: []*dbpb.Job{
			// Has retries remaining (attempt 1 of 2) and a target group.
			{Id: 1, Status: dbpb.JobStatus_JOB_STATUS_FAILED, TargetGroup: "pool-a", Attempt: 1, MaxAttempts: 2,
				Spec: &dbpb.JobSpec{Retry: &dbpb.RetryPolicy{MaxAttempts: 2}}},
			// No retry policy: never retried.
			{Id: 2, Status: dbpb.JobStatus_JOB_STATUS_FAILED, TargetGroup: "pool-a", Attempt: 1, MaxAttempts: 0},
			// Exhausted its retries (attempt 3 of 1 + max 2): not retried.
			{Id: 3, Status: dbpb.JobStatus_JOB_STATUS_FAILED, TargetGroup: "pool-a", Attempt: 3, MaxAttempts: 2,
				Spec: &dbpb.JobSpec{Retry: &dbpb.RetryPolicy{MaxAttempts: 2}}},
		},
		claimed: map[int64]bool{1: true},
	}
	api := &fakeRelayer{}
	s := &Server{logger: testLogger()}

	s.retryFailedJobs(context.Background(), db, api)

	// Only job 1 was claimed.
	if len(db.claims) != 1 || db.claims[0] != 1 {
		t.Errorf("claims = %v, want [1]", db.claims)
	}
	// Job 1 was fanned out to the UI as pending and dispatched.
	if status, ok := api.notified[1]; !ok || status != dbpb.JobStatus_JOB_STATUS_PENDING {
		t.Errorf("job 1 notified = %v, want PENDING", api.notified[1])
	}
	if ids := api.dispatchedIDs(); len(ids) != 1 || ids[0] != 1 {
		t.Errorf("dispatched = %v, want [1]", ids)
	}
	// Jobs 2 and 3 were not claimed, notified, or dispatched.
	if _, ok := api.notified[2]; ok {
		t.Error("job 2 (no retry policy) was notified, want it left alone")
	}
	if _, ok := api.notified[3]; ok {
		t.Error("job 3 (exhausted retries) was notified, want it left alone")
	}
	// Job 3 was not claimed either.
	for _, id := range db.claims {
		if id == 3 {
			t.Error("job 3 (exhausted retries) was claimed, want it left alone")
		}
	}
}

// TestRetryFailedJobsBackoff verifies that a retry with a backoff is
// dispatched after the delay (not immediately).
func TestRetryFailedJobsBackoff(t *testing.T) {
	db := &fakeRetryDB{
		jobs: []*dbpb.Job{
			{Id: 1, Status: dbpb.JobStatus_JOB_STATUS_FAILED, TargetGroup: "pool-a", Attempt: 1, MaxAttempts: 2,
				Spec: &dbpb.JobSpec{Retry: &dbpb.RetryPolicy{MaxAttempts: 2, Backoff: durationOf(50 * time.Millisecond)}}},
		},
		claimed: map[int64]bool{1: true},
	}
	api := &fakeRelayer{}
	s := &Server{logger: testLogger()}

	s.retryFailedJobs(context.Background(), db, api)

	// The reset to pending is fanned out immediately.
	if _, ok := api.notified[1]; !ok {
		t.Error("job 1 was not notified as pending")
	}
	// The dispatch is delayed by the backoff: it has not happened yet.
	if len(api.dispatchedIDs()) != 0 {
		t.Errorf("dispatched = %v immediately, want none (backoff pending)", api.dispatchedIDs())
	}
	// Wait past the backoff; the dispatch happens.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ids := api.dispatchedIDs(); len(ids) == 1 && ids[0] == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ids := api.dispatchedIDs(); len(ids) != 1 || ids[0] != 1 {
		t.Errorf("dispatched = %v after backoff, want [1]", ids)
	}
}

// TestRetryFailedJobsNoAPI verifies the retry loop still claims (via the db)
// when the API client is nil (e.g. a test or a degraded control plane): the
// notification and dispatch are skipped but the job is still claimed.
func TestRetryFailedJobsNoAPI(t *testing.T) {
	db := &fakeRetryDB{
		jobs: []*dbpb.Job{
			{Id: 1, Status: dbpb.JobStatus_JOB_STATUS_FAILED, TargetGroup: "pool-a", Attempt: 1, MaxAttempts: 2,
				Spec: &dbpb.JobSpec{Retry: &dbpb.RetryPolicy{MaxAttempts: 2}}},
		},
		claimed: map[int64]bool{1: true},
	}
	s := &Server{logger: testLogger()}

	s.retryFailedJobs(context.Background(), db, nil)

	if len(db.claims) != 1 || db.claims[0] != 1 {
		t.Errorf("claims = %v, want [1]", db.claims)
	}
}

// TestRerunJob verifies the scheduler's RerunJob RPC (F-04): it resets a
// finished job to pending with a fresh attempt (via the Database service) and
// re-dispatches it to the API.
func TestRerunJob(t *testing.T) {
	db := &fakeRerunDB{
		jobs: map[int64]*dbpb.Job{
			1: {Id: 1, Name: "build", Status: dbpb.JobStatus_JOB_STATUS_FAILED, TargetGroup: "pool-a", Attempt: 1, MaxAttempts: 0},
		},
	}
	api := &fakeRelayer{}
	s := &Server{logger: testLogger()}

	job, err := s.rerunJob(context.Background(), db, api, 1)
	if err != nil {
		t.Fatalf("rerunJob: %v", err)
	}
	if job.GetStatus() != dbpb.JobStatus_JOB_STATUS_PENDING {
		t.Errorf("status = %v, want PENDING", job.GetStatus())
	}
	if job.GetAttempt() != 1 {
		t.Errorf("attempt = %d, want 1 (fresh attempt)", job.GetAttempt())
	}
	if ids := api.dispatchedIDs(); len(ids) != 1 || ids[0] != 1 {
		t.Errorf("dispatched = %v, want [1]", ids)
	}
}

// TestRerunJobRunning verifies that re-running a job that is still running is
// rejected (the Database service returns FailedPrecondition).
func TestRerunJobRunning(t *testing.T) {
	db := &fakeRerunDB{
		jobs: map[int64]*dbpb.Job{
			1: {Id: 1, Name: "build", Status: dbpb.JobStatus_JOB_STATUS_RUNNING, TargetGroup: "pool-a"},
		},
	}
	api := &fakeRelayer{}
	s := &Server{logger: testLogger()}

	if _, err := s.rerunJob(context.Background(), db, api, 1); err == nil {
		t.Error("rerunJob on a running job succeeded, want an error")
	}
	if len(api.dispatchedIDs()) != 0 {
		t.Errorf("dispatched = %v, want none", api.dispatchedIDs())
	}
}

// fakeRerunDB is a stub DatabaseClient for the RerunJob tests: it returns the
// stored job from GetJob and, on RerunJob, resets the job to pending with a
// fresh attempt (or fails when the job is not in a terminal state).
type fakeRerunDB struct {
	jobs map[int64]*dbpb.Job
}

func (f *fakeRerunDB) GetJob(ctx context.Context, in *dbpb.GetJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	job, ok := f.jobs[in.GetId()]
	if !ok {
		return nil, statusNotFound
	}
	return job, nil
}

func (f *fakeRerunDB) RerunJob(ctx context.Context, in *dbpb.RerunJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	job, ok := f.jobs[in.GetId()]
	if !ok {
		return nil, statusNotFound
	}
	switch job.GetStatus() {
	case dbpb.JobStatus_JOB_STATUS_PENDING, dbpb.JobStatus_JOB_STATUS_RUNNING:
		return nil, statusFailedPrecondition
	}
	job.Status = dbpb.JobStatus_JOB_STATUS_PENDING
	job.Attempt = 1
	job.FinishedAt = nil
	return job, nil
}
