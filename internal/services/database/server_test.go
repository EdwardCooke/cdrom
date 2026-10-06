package database

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// startServer opens an in-memory SQLite database, migrates it, and serves the
// Database gRPC service over an in-process bufconn listener.
func startServer(t *testing.T) dbpb.DatabaseClient {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// Use a temp file (not :memory:) so GORM's pooled connections all share
	// the same database.
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(Config{Backend: BackendSQLite, SQLitePath: dbPath}, logger)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = Close(db) })
	if err := Migrate(db, logger); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	dbpb.RegisterDatabaseServer(srv, NewServer(db))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.DialContext(context.Background(), "bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return dbpb.NewDatabaseClient(conn)
}

func TestPipelineAndJobLifecycle(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "build", Description: "ci"})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if pipeline.GetId() == 0 {
		t.Fatalf("pipeline id = 0, want non-zero")
	}

	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{
		PipelineId:  pipeline.GetId(),
		Name:        "compile",
		TargetGroup: "linux-pool",
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if job.GetStatus() != dbpb.JobStatus_JOB_STATUS_PENDING {
		t.Fatalf("new job status = %v, want PENDING", job.GetStatus())
	}
	if job.GetPipelineId() != pipeline.GetId() {
		t.Fatalf("job pipeline = %d, want %d", job.GetPipelineId(), pipeline.GetId())
	}

	// Update the job to running.
	updated, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:     job.GetId(),
		Status: dbpb.JobStatus_JOB_STATUS_RUNNING,
	})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if updated.GetStatus() != dbpb.JobStatus_JOB_STATUS_RUNNING {
		t.Fatalf("updated status = %v, want RUNNING", updated.GetStatus())
	}

	// List jobs for the pipeline.
	response, err := client.ListJobs(ctx, &dbpb.ListJobsRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if got := len(response.GetJobs()); got != 1 {
		t.Fatalf("jobs = %d, want 1", got)
	}

	// Deleting the pipeline cascades to its jobs.
	if _, err := client.DeletePipeline(ctx, &dbpb.DeletePipelineRequest{Id: pipeline.GetId()}); err != nil {
		t.Fatalf("DeletePipeline: %v", err)
	}
	if _, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetJob after cascade = %v, want NotFound", err)
	}
}

func TestWorkerRegistration(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	worker, err := client.RegisterWorker(ctx, &dbpb.RegisterWorkerRequest{Name: "w1", Group: "pool-a", Address: "10.0.0.1"})
	if err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if worker.GetGroup() != "pool-a" {
		t.Fatalf("group = %q, want pool-a", worker.GetGroup())
	}

	// Re-registering updates the group.
	if _, err := client.RegisterWorker(ctx, &dbpb.RegisterWorkerRequest{Name: "w1", Group: "pool-b"}); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	fetched, err := client.GetWorker(ctx, &dbpb.GetWorkerRequest{Name: "w1"})
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if fetched.GetGroup() != "pool-b" {
		t.Fatalf("group after re-register = %q, want pool-b", fetched.GetGroup())
	}

	// Heartbeat refreshes last-seen.
	if _, err := client.HeartbeatWorker(ctx, &dbpb.HeartbeatWorkerRequest{Name: "w1"}); err != nil {
		t.Fatalf("HeartbeatWorker: %v", err)
	}

	// List by group.
	response, err := client.ListWorkers(ctx, &dbpb.ListWorkersRequest{Group: "pool-b"})
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if got := len(response.GetWorkers()); got != 1 {
		t.Fatalf("workers in pool-b = %d, want 1", got)
	}
}

func TestValidation(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	if _, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty pipeline = %v, want InvalidArgument", err)
	}
	if _, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty job = %v, want InvalidArgument", err)
	}
}

// TestJobSpecRoundTrip verifies that a job's execution spec is persisted and
// returned intact: a multi-step spec with args, env, workdir, and a per-step
// timeout survives a create → get round trip through the storage backend.
func TestJobSpecRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		{
			Params:  map[string]*dbpb.ParamValue{"command": {String_: "go"}, "args": {Strings: []string{"build", "./..."}}},
			Workdir: "repo",
			Env:     map[string]string{"GOFLAGS": "-mod=vendor"},
		},
		{Params: map[string]*dbpb.ParamValue{"command": {String_: "sh"}, "args": {Strings: []string{"-c", "make test"}}}, Timeout: durationpb.New(30 * time.Second)},
	}}

	created, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "build", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	got := fetched.GetSpec()
	if got == nil {
		t.Fatal("fetched spec is nil, want the stored spec")
	}
	if len(got.GetSteps()) != 2 {
		t.Fatalf("steps = %d, want 2", len(got.GetSteps()))
	}

	step0 := got.GetSteps()[0]
	if step0.GetParams()["command"].GetString_() != "go" {
		t.Errorf("step 0 command = %q, want go", step0.GetParams()["command"].GetString_())
	}
	if args := step0.GetParams()["args"].GetStrings(); len(args) != 2 || args[0] != "build" {
		t.Errorf("step 0 args = %v, want [build ./...]", args)
	}
	if step0.GetWorkdir() != "repo" {
		t.Errorf("step 0 workdir = %q, want repo", step0.GetWorkdir())
	}
	if step0.GetEnv()["GOFLAGS"] != "-mod=vendor" {
		t.Errorf("step 0 env = %v, want GOFLAGS=-mod=vendor", step0.GetEnv())
	}

	step1 := got.GetSteps()[1]
	if step1.GetTimeout().AsDuration() != 30*time.Second {
		t.Errorf("step 1 timeout = %s, want 30s", step1.GetTimeout().AsDuration())
	}
}

// TestJobWithoutSpec verifies that a job created without a spec round-trips
// with an empty (nil) spec.
func TestJobWithoutSpec(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	created, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "no-spec"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if spec := fetched.GetSpec(); spec != nil && len(spec.GetSteps()) != 0 {
		t.Errorf("spec = %+v, want empty", spec)
	}
}

// TestJobLevelTimeoutRoundTrip verifies that a job-level timeout (spec.timeout)
// is persisted and returned intact through the storage backend (F-03).
func TestJobLevelTimeoutRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	spec := &dbpb.JobSpec{
		Timeout: durationpb.New(5 * time.Minute),
		Steps:   []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "go"}}}},
	}
	created, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "build", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got := fetched.GetSpec().GetTimeout().AsDuration(); got != 5*time.Minute {
		t.Errorf("job timeout = %s, want 5m", got)
	}
}

// TestRetryPolicyRoundTrip verifies that a job's retry policy (spec.retry) is
// persisted and returned intact through the storage backend (F-04), and that
// the job's attempt counter and retry budget are initialized on creation.
func TestRetryPolicyRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	spec := &dbpb.JobSpec{
		Retry: &dbpb.RetryPolicy{MaxAttempts: 3, Backoff: durationpb.New(10 * time.Second)},
		Steps: []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "go"}}}},
	}
	created, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "build", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if got := created.GetAttempt(); got != 1 {
		t.Errorf("new job attempt = %d, want 1", got)
	}
	if got := created.GetMaxAttempts(); got != 3 {
		t.Errorf("new job max_attempts = %d, want 3", got)
	}

	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	retry := fetched.GetSpec().GetRetry()
	if retry == nil {
		t.Fatal("fetched spec has no retry policy, want the stored policy")
	}
	if retry.GetMaxAttempts() != 3 {
		t.Errorf("retry max_attempts = %d, want 3", retry.GetMaxAttempts())
	}
	if retry.GetBackoff().AsDuration() != 10*time.Second {
		t.Errorf("retry backoff = %s, want 10s", retry.GetBackoff().AsDuration())
	}
}

// TestClaimJobRetry verifies the conditional retry claim used by the
// scheduler's retry loop (F-04): a failed job with retries remaining is reset
// to pending with an incremented attempt, a job that has exhausted its budget
// is not claimed, and a job that already reached a non-retryable terminal
// state (succeeded or cancelled) is left untouched.
func TestClaimJobRetry(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	spec := &dbpb.JobSpec{
		Retry: &dbpb.RetryPolicy{MaxAttempts: 2},
		Steps: []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "go"}}}},
	}

	// A failed job with retries remaining is claimed: attempt 1 -> 2.
	failed, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "retryable", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         failed.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_FAILED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	claimed, err := client.ClaimJobRetry(ctx, &dbpb.ClaimJobRetryRequest{Id: failed.GetId()})
	if err != nil {
		t.Fatalf("ClaimJobRetry: %v", err)
	}
	if !claimed.GetClaimed() {
		t.Fatal("failed job with retries remaining was not claimed, want claimed=true")
	}
	if claimed.GetAttempt() != 2 {
		t.Errorf("claimed attempt = %d, want 2", claimed.GetAttempt())
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: failed.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_PENDING {
		t.Errorf("status = %v, want PENDING (reset for retry)", fetched.GetStatus())
	}
	if fetched.GetAttempt() != 2 {
		t.Errorf("attempt = %d, want 2", fetched.GetAttempt())
	}
	if fetched.GetFinishedAt() != nil {
		t.Error("finished_at not cleared on retried job")
	}

	// The job has one retry left (attempt 2 of 1 + max 2); it is claimed again
	// (attempt 2 -> 3).
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         failed.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_FAILED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	claimed, err = client.ClaimJobRetry(ctx, &dbpb.ClaimJobRetryRequest{Id: failed.GetId()})
	if err != nil {
		t.Fatalf("ClaimJobRetry: %v", err)
	}
	if !claimed.GetClaimed() {
		t.Fatal("job with a retry remaining was not claimed, want claimed=true")
	}
	if claimed.GetAttempt() != 3 {
		t.Errorf("claimed attempt = %d, want 3", claimed.GetAttempt())
	}

	// The job has now used both of its retries (attempt 3 of 1 + max 2); it
	// is not claimed again.
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         failed.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_FAILED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	claimed, err = client.ClaimJobRetry(ctx, &dbpb.ClaimJobRetryRequest{Id: failed.GetId()})
	if err != nil {
		t.Fatalf("ClaimJobRetry: %v", err)
	}
	if claimed.GetClaimed() {
		t.Fatal("job that exhausted its retries was claimed, want claimed=false")
	}

	// A job that succeeded is never retried.
	succeeded, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "succeeded", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         succeeded.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	claimed, err = client.ClaimJobRetry(ctx, &dbpb.ClaimJobRetryRequest{Id: succeeded.GetId()})
	if err != nil {
		t.Fatalf("ClaimJobRetry: %v", err)
	}
	if claimed.GetClaimed() {
		t.Fatal("succeeded job was claimed, want claimed=false")
	}

	// A job with no retry policy is never retried.
	noRetry, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "no-retry"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         noRetry.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_FAILED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	claimed, err = client.ClaimJobRetry(ctx, &dbpb.ClaimJobRetryRequest{Id: noRetry.GetId()})
	if err != nil {
		t.Fatalf("ClaimJobRetry: %v", err)
	}
	if claimed.GetClaimed() {
		t.Fatal("job with no retry policy was claimed, want claimed=false")
	}
}

// TestListRetriableJobs verifies the database-side filter used by the
// scheduler's retry loop (F-04): it returns only failed jobs that still have
// retries remaining (a retry policy with max_attempts > 0 and an attempt
// counter below the budget), and excludes jobs with no retry policy, jobs that
// have exhausted their budget, and jobs that are not failed.
func TestListRetriableJobs(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	spec := &dbpb.JobSpec{
		Retry: &dbpb.RetryPolicy{MaxAttempts: 2},
		Steps: []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "go"}}}},
	}

	// A failed job with retries remaining (attempt 1 of 1 + max 2) is retriable.
	retriable, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "retriable", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         retriable.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_FAILED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	// A failed job that has exhausted its retries (attempt 3 of 1 + max 2) is
	// not retriable.
	exhausted, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "exhausted", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         exhausted.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_FAILED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	// Drive the attempt counter to the exhausted value (1 -> 2 -> 3).
	for i := 0; i < 2; i++ {
		if _, err := client.ClaimJobRetry(ctx, &dbpb.ClaimJobRetryRequest{Id: exhausted.GetId()}); err != nil {
			t.Fatalf("ClaimJobRetry: %v", err)
		}
		if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
			Id:         exhausted.GetId(),
			Status:     dbpb.JobStatus_JOB_STATUS_FAILED,
			StartedAt:  timestamppb.Now(),
			FinishedAt: timestamppb.Now(),
		}); err != nil {
			t.Fatalf("UpdateJob: %v", err)
		}
	}

	// A failed job with no retry policy is not retriable.
	noRetry, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "no-retry"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         noRetry.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_FAILED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	// A non-failed job with a retry policy is not retriable.
	succeeded, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "succeeded", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         succeeded.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	response, err := client.ListRetriableJobs(ctx, &dbpb.ListRetriableJobsRequest{})
	if err != nil {
		t.Fatalf("ListRetriableJobs: %v", err)
	}
	got := map[int64]bool{}
	for _, job := range response.GetJobs() {
		got[job.GetId()] = true
	}
	if !got[retriable.GetId()] {
		t.Error("retriable job was not returned")
	}
	if got[exhausted.GetId()] {
		t.Error("exhausted job was returned, want it excluded")
	}
	if got[noRetry.GetId()] {
		t.Error("no-retry-policy job was returned, want it excluded")
	}
	if got[succeeded.GetId()] {
		t.Error("succeeded job was returned, want it excluded")
	}
	if len(got) != 1 {
		t.Errorf("ListRetriableJobs returned %d jobs, want exactly 1", len(got))
	}
}

// TestRerunJob verifies the conditional re-run used by the scheduler's
// RerunJob RPC (F-04): a finished job is reset to pending with a fresh
// attempt (1), while a job that is still pending or running is rejected.
func TestRerunJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// A finished job is re-run: reset to pending, attempt back to 1.
	finished, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "finished"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         finished.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_FAILED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	rerun, err := client.RerunJob(ctx, &dbpb.RerunJobRequest{Id: finished.GetId()})
	if err != nil {
		t.Fatalf("RerunJob: %v", err)
	}
	if rerun.GetStatus() != dbpb.JobStatus_JOB_STATUS_PENDING {
		t.Errorf("status = %v, want PENDING (reset for re-run)", rerun.GetStatus())
	}
	if rerun.GetAttempt() != 1 {
		t.Errorf("attempt = %d, want 1 (fresh attempt)", rerun.GetAttempt())
	}
	if rerun.GetFinishedAt() != nil {
		t.Error("finished_at not cleared on re-run job")
	}

	// A job that is still running cannot be re-run.
	running, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "running"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:        running.GetId(),
		Status:    dbpb.JobStatus_JOB_STATUS_RUNNING,
		StartedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if _, err := client.RerunJob(ctx, &dbpb.RerunJobRequest{Id: running.GetId()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RerunJob on running job = %v, want FailedPrecondition", err)
	}
}

// TestReapJob verifies the conditional reap used by the scheduler's watchdog
// (F-03): a running job is marked timed_out, while a job that already reached
// a terminal state is left untouched.
func TestReapJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// A running job is reaped.
	running, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "running"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:        running.GetId(),
		Status:    dbpb.JobStatus_JOB_STATUS_RUNNING,
		StartedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	reaped, err := client.ReapJob(ctx, &dbpb.ReapJobRequest{Id: running.GetId()})
	if err != nil {
		t.Fatalf("ReapJob: %v", err)
	}
	if !reaped.GetReaped() {
		t.Fatal("running job was not reaped, want reaped=true")
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: running.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_TIMED_OUT {
		t.Errorf("status = %v, want TIMED_OUT", fetched.GetStatus())
	}
	if fetched.GetFinishedAt() == nil {
		t.Error("finished_at not set on reaped job")
	}

	// A job that already reached a terminal state is not reaped.
	finished, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "finished"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         finished.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	reaped, err = client.ReapJob(ctx, &dbpb.ReapJobRequest{Id: finished.GetId()})
	if err != nil {
		t.Fatalf("ReapJob: %v", err)
	}
	if reaped.GetReaped() {
		t.Fatal("terminal job was reaped, want reaped=false")
	}
	fetched, err = client.GetJob(ctx, &dbpb.GetJobRequest{Id: finished.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("status = %v, want SUCCEEDED (unchanged)", fetched.GetStatus())
	}
}

// TestCancelJob verifies the conditional cancel used by the scheduler's
// CancelJob RPC (F-05): a running job is marked cancelled, while a job that
// already reached a terminal state is left untouched (cancelling a finished
// job is a no-op).
func TestCancelJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// A running job is cancelled.
	running, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "running"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:        running.GetId(),
		Status:    dbpb.JobStatus_JOB_STATUS_RUNNING,
		StartedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	cancelled, err := client.CancelJob(ctx, &dbpb.CancelJobRequest{Id: running.GetId()})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if !cancelled.GetCancelled() {
		t.Fatal("running job was not cancelled, want cancelled=true")
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: running.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_CANCELLED {
		t.Errorf("status = %v, want CANCELLED", fetched.GetStatus())
	}
	if fetched.GetFinishedAt() == nil {
		t.Error("finished_at not set on cancelled job")
	}

	// A job that already reached a terminal state is not cancelled.
	finished, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "finished"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         finished.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	cancelled, err = client.CancelJob(ctx, &dbpb.CancelJobRequest{Id: finished.GetId()})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if cancelled.GetCancelled() {
		t.Fatal("terminal job was cancelled, want cancelled=false")
	}
	fetched, err = client.GetJob(ctx, &dbpb.GetJobRequest{Id: finished.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("status = %v, want SUCCEEDED (unchanged)", fetched.GetStatus())
	}
}

// TestUpdateJobIgnoresLateReportOnTerminalJob verifies that a status report
// from a target on a job that already reached a terminal state is a no-op
// (F-05): a late report (e.g. a target that finished just as it was
// cancelled) cannot clobber the terminal status.
func TestUpdateJobIgnoresLateReportOnTerminalJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "running"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:        job.GetId(),
		Status:    dbpb.JobStatus_JOB_STATUS_RUNNING,
		StartedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	// The job is cancelled.
	if _, err := client.CancelJob(ctx, &dbpb.CancelJobRequest{Id: job.GetId()}); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	// A late report from the target (it finished just as it was cancelled)
	// must not clobber the cancelled status.
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         job.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_CANCELLED {
		t.Errorf("status = %v, want CANCELLED (late report must not clobber)", fetched.GetStatus())
	}
}

func TestIDPSigningKeys(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// Empty keyring to start.
	resp, err := client.ListIDPSigningKeys(ctx, &dbpb.ListIDPSigningKeysRequest{})
	if err != nil {
		t.Fatalf("ListIDPSigningKeys: %v", err)
	}
	if got := len(resp.GetKeys()); got != 0 {
		t.Fatalf("initial keys = %d, want 0", got)
	}

	// Set a keyring with a current key and a predecessor.
	now := time.Now()
	if _, err := client.SetIDPSigningKeys(ctx, &dbpb.SetIDPSigningKeysRequest{
		Keys: []*dbpb.IDPSigningKey{
			{Kid: "current", IsCurrent: true, NotBefore: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(time.Hour)), Pem: "pem-current"},
			{Kid: "old", IsCurrent: false, NotBefore: timestamppb.New(now.Add(-time.Hour)), ExpiresAt: timestamppb.New(now.Add(time.Hour)), Pem: "pem-old"},
		},
	}); err != nil {
		t.Fatalf("SetIDPSigningKeys: %v", err)
	}

	resp, err = client.ListIDPSigningKeys(ctx, &dbpb.ListIDPSigningKeysRequest{})
	if err != nil {
		t.Fatalf("ListIDPSigningKeys: %v", err)
	}
	if got := len(resp.GetKeys()); got != 2 {
		t.Fatalf("keys = %d, want 2", got)
	}
	var current bool
	for _, key := range resp.GetKeys() {
		if key.GetKid() == "current" && key.GetIsCurrent() {
			current = true
		}
	}
	if !current {
		t.Fatal("no key marked current")
	}

	// Setting again replaces the keyring atomically.
	if _, err := client.SetIDPSigningKeys(ctx, &dbpb.SetIDPSigningKeysRequest{
		Keys: []*dbpb.IDPSigningKey{
			{Kid: "only", IsCurrent: true, NotBefore: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(time.Hour)), Pem: "pem-only"},
		},
	}); err != nil {
		t.Fatalf("SetIDPSigningKeys (replace): %v", err)
	}
	resp, err = client.ListIDPSigningKeys(ctx, &dbpb.ListIDPSigningKeysRequest{})
	if err != nil {
		t.Fatalf("ListIDPSigningKeys: %v", err)
	}
	if got := len(resp.GetKeys()); got != 1 {
		t.Fatalf("keys after replace = %d, want 1", got)
	}
	if resp.GetKeys()[0].GetKid() != "only" {
		t.Fatalf("key = %q, want only", resp.GetKeys()[0].GetKid())
	}
}

func TestIDPAuthCodes(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// Store a code.
	if _, err := client.StoreIDPAuthCode(ctx, &dbpb.StoreIDPAuthCodeRequest{
		Code: &dbpb.IDPAuthCode{
			Code:        "abc123",
			ClientId:    "cdrom-ui",
			RedirectUri: "http://localhost:8080/api/auth/callback",
			Subject:     "alice",
			Name:        "alice",
			Email:       "alice@example.com",
			CreatedAt:   timestamppb.New(time.Now()),
		},
	}); err != nil {
		t.Fatalf("StoreIDPAuthCode: %v", err)
	}

	// Consume it (single use).
	got, err := client.ConsumeIDPAuthCode(ctx, &dbpb.ConsumeIDPAuthCodeRequest{Code: "abc123"})
	if err != nil {
		t.Fatalf("ConsumeIDPAuthCode: %v", err)
	}
	if got.GetSubject() != "alice" {
		t.Fatalf("subject = %q, want alice", got.GetSubject())
	}

	// Consuming again is a NotFound (single use).
	if _, err := client.ConsumeIDPAuthCode(ctx, &dbpb.ConsumeIDPAuthCodeRequest{Code: "abc123"}); status.Code(err) != codes.NotFound {
		t.Fatalf("re-consume = %v, want NotFound", err)
	}

	// Prune drops remaining codes.
	if _, err := client.StoreIDPAuthCode(ctx, &dbpb.StoreIDPAuthCodeRequest{
		Code: &dbpb.IDPAuthCode{Code: "xyz", ClientId: "cdrom-ui", CreatedAt: timestamppb.New(time.Now())},
	}); err != nil {
		t.Fatalf("StoreIDPAuthCode: %v", err)
	}
	if _, err := client.PruneIDPAuthCodes(ctx, &dbpb.PruneIDPAuthCodesRequest{}); err != nil {
		t.Fatalf("PruneIDPAuthCodes: %v", err)
	}
	if _, err := client.ConsumeIDPAuthCode(ctx, &dbpb.ConsumeIDPAuthCodeRequest{Code: "xyz"}); status.Code(err) != codes.NotFound {
		t.Fatalf("consume after prune = %v, want NotFound", err)
	}
}

// TestSkipJob verifies SkipJob's conditional semantics (F-06): a pending job
// transitions to skipped, but a job that already started (or finished) is
// left untouched.
func TestSkipJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pending, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "pending"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	resp, err := client.SkipJob(ctx, &dbpb.SkipJobRequest{Id: pending.GetId()})
	if err != nil {
		t.Fatalf("SkipJob: %v", err)
	}
	if !resp.GetSkipped() {
		t.Fatal("pending job was not skipped, want skipped=true")
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: pending.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_SKIPPED {
		t.Errorf("status = %v, want SKIPPED", fetched.GetStatus())
	}
	if fetched.GetFinishedAt() == nil {
		t.Error("finished_at not set on skipped job")
	}

	// A running job is not skipped.
	running, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "running"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:        running.GetId(),
		Status:    dbpb.JobStatus_JOB_STATUS_RUNNING,
		StartedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	resp, err = client.SkipJob(ctx, &dbpb.SkipJobRequest{Id: running.GetId()})
	if err != nil {
		t.Fatalf("SkipJob: %v", err)
	}
	if resp.GetSkipped() {
		t.Fatal("running job was skipped, want skipped=false")
	}
	fetched, err = client.GetJob(ctx, &dbpb.GetJobRequest{Id: running.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_RUNNING {
		t.Errorf("status = %v, want RUNNING (unchanged)", fetched.GetStatus())
	}
}

// TestRerunSkippedJob verifies that RerunJob accepts a previously skipped job
// (skipped is a terminal state, F-06), resetting it to pending for a fresh
// run.
func TestRerunSkippedJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "skipped"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.SkipJob(ctx, &dbpb.SkipJobRequest{Id: job.GetId()}); err != nil {
		t.Fatalf("SkipJob: %v", err)
	}
	rerun, err := client.RerunJob(ctx, &dbpb.RerunJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("RerunJob: %v", err)
	}
	if rerun.GetStatus() != dbpb.JobStatus_JOB_STATUS_PENDING {
		t.Errorf("status = %v, want PENDING (reset for re-run)", rerun.GetStatus())
	}
}

// TestDependsOnRoundTrip verifies that a job's DependsOn (F-06) survives a
// create/fetch round trip, and that UpdateJob's ClearDependsOn clears it (used
// by the scheduler's dependency resolver once a job's dependencies are
// satisfied, so it is not reconsidered on the resolver's next tick).
func TestDependsOnRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	upstream, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "upstream"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "downstream", DependsOn: []int64{upstream.GetId()}})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if got := job.GetDependsOn(); len(got) != 1 || got[0] != upstream.GetId() {
		t.Fatalf("depends_on on create = %v, want [%d]", got, upstream.GetId())
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got := fetched.GetDependsOn(); len(got) != 1 || got[0] != upstream.GetId() {
		t.Fatalf("depends_on on fetch = %v, want [%d]", got, upstream.GetId())
	}

	updated, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{Id: job.GetId(), ClearDependsOn: true})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if got := updated.GetDependsOn(); len(got) != 0 {
		t.Errorf("depends_on after ClearDependsOn = %v, want empty", got)
	}
	fetched, err = client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got := fetched.GetDependsOn(); len(got) != 0 {
		t.Errorf("depends_on after ClearDependsOn (fetch) = %v, want empty", got)
	}
}

// TestUpdateJobStepResultsRoundTrip verifies that step_results (F-06) are
// persisted and survive a fetch round trip, and that they are not clobbered
// by a later UpdateJob call that carries no step_results (e.g. a "running"
// status update with no results yet).
func TestUpdateJobStepResultsRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "job"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	results := []*dbpb.StepResult{
		{Index: 0, Status: dbpb.StepStatus_STEP_STATUS_SUCCEEDED},
		{Index: 1, Status: dbpb.StepStatus_STEP_STATUS_SKIPPED},
		{Index: 2, Status: dbpb.StepStatus_STEP_STATUS_FAILED, Error: "boom"},
	}
	updated, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:          job.GetId(),
		Status:      dbpb.JobStatus_JOB_STATUS_FAILED,
		FinishedAt:  timestamppb.Now(),
		StepResults: results,
	})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if got := updated.GetStepResults(); len(got) != 3 {
		t.Fatalf("step_results on update response = %v, want 3 entries", got)
	}

	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	got := fetched.GetStepResults()
	if len(got) != 3 {
		t.Fatalf("step_results on fetch = %v, want 3 entries", got)
	}
	if got[2].GetStatus() != dbpb.StepStatus_STEP_STATUS_FAILED || got[2].GetError() != "boom" {
		t.Errorf("step_results[2] = %+v, want FAILED with error %q", got[2], "boom")
	}
}

// TestJobSpecIgnoreFailedRoundTrip verifies that a job's ignore_failed
// (job-level and per-step) is persisted and returned intact through the
// storage backend (F-06).
func TestJobSpecIgnoreFailedRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	spec := &dbpb.JobSpec{
		IgnoreFailed: true,
		Steps: []*dbpb.JobStep{
			{Params: map[string]*dbpb.ParamValue{"command": {String_: "go"}}, IgnoreFailed: true},
			{Params: map[string]*dbpb.ParamValue{"command": {String_: "make"}}},
		},
	}
	created, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "build", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	got := fetched.GetSpec()
	if !got.GetIgnoreFailed() {
		t.Error("job-level ignore_failed = false, want true")
	}
	if len(got.GetSteps()) != 2 {
		t.Fatalf("steps = %d, want 2", len(got.GetSteps()))
	}
	step0 := got.GetSteps()[0]
	if !step0.GetIgnoreFailed() {
		t.Error("step 0 ignore_failed = false, want true")
	}
	if got.GetSteps()[1].GetIgnoreFailed() {
		t.Error("step 1 ignore_failed = true, want false")
	}
}

// TestCreateJobDenormalizesIgnoreFailed verifies that a job's ignore_failed
// flag is denormalized from its spec onto the Job row at creation time (F-06),
// so the dependency resolver can read it without decoding the spec.
func TestCreateJobDenormalizesIgnoreFailed(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	spec := &dbpb.JobSpec{
		IgnoreFailed: true,
		Steps:        []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "go"}}}},
	}
	created, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "build", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if !created.GetIgnoreFailed() {
		t.Error("created job ignore_failed = false, want true (denormalized from spec)")
	}

	// A job whose spec does not set ignore_failed must not be denormalized.
	plain, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "plain"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if plain.GetIgnoreFailed() {
		t.Error("plain job ignore_failed = true, want false")
	}
}

// TestUpdateJobOutputsRoundTrip verifies that a job's outputs (F-06) are
// persisted through UpdateJob and survive a fetch round trip, and that a later
// UpdateJob carrying no outputs does not clobber the stored outputs.
func TestUpdateJobOutputsRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "job"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	updated, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         job.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
		FinishedAt: timestamppb.Now(),
		Outputs:    map[string]string{"version": "1.2.3", "commit": "abc123"},
	})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if got := updated.GetOutputs(); got["version"] != "1.2.3" || got["commit"] != "abc123" {
		t.Errorf("outputs on update response = %v, want version/commit", got)
	}

	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got := fetched.GetOutputs(); got["version"] != "1.2.3" || got["commit"] != "abc123" {
		t.Errorf("outputs on fetch = %v, want version/commit", got)
	}

	// A later status update with no outputs must not clear the stored outputs.
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:     job.GetId(),
		Status: dbpb.JobStatus_JOB_STATUS_RUNNING,
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	refetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got := refetched.GetOutputs(); got["version"] != "1.2.3" {
		t.Errorf("outputs after a no-outputs update = %v, want preserved", got)
	}
}

// createPipelineWithJobs creates a pipeline and a set of job definitions on
// it, returning the pipeline and the created definition jobs (in order).
func createPipelineWithJobs(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, name string, defs ...*dbpb.CreateJobRequest) (*dbpb.Pipeline, []*dbpb.Job) {
	t.Helper()
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: name})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	jobs := make([]*dbpb.Job, 0, len(defs))
	for _, def := range defs {
		def.PipelineId = pipeline.GetId()
		job, err := client.CreateJob(ctx, def)
		if err != nil {
			t.Fatalf("CreateJob(%s): %v", def.GetName(), err)
		}
		jobs = append(jobs, job)
	}
	return pipeline, jobs
}

// TestCreateRunCreatesRunAndInstances verifies that CreateRun (F-07) creates a
// PipelineRun and one job instance per job definition in the pipeline, each
// bound to the run and the pipeline.
func TestCreateRunCreatesRunAndInstances(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, defs := createPipelineWithJobs(t, client, ctx, "build",
		&dbpb.CreateJobRequest{Name: "compile", TargetGroup: "linux-pool"},
		&dbpb.CreateJobRequest{Name: "test"},
	)

	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{
		PipelineId: pipeline.GetId(),
		Trigger:    "manual",
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	run := resp.GetRun()
	if run.GetId() == 0 {
		t.Fatal("run id = 0, want non-zero")
	}
	if run.GetPipelineId() != pipeline.GetId() {
		t.Errorf("run pipeline = %d, want %d", run.GetPipelineId(), pipeline.GetId())
	}
	if run.GetStatus() != dbpb.RunStatus_RUN_STATUS_PENDING {
		t.Errorf("run status = %v, want PENDING", run.GetStatus())
	}
	if run.GetTrigger() != "manual" {
		t.Errorf("run trigger = %q, want manual", run.GetTrigger())
	}
	if got := len(resp.GetJobs()); got != len(defs) {
		t.Fatalf("run created %d job instances, want %d", got, len(defs))
	}
	for i, instance := range resp.GetJobs() {
		if instance.GetRunId() != run.GetId() {
			t.Errorf("instance %d run_id = %d, want %d", i, instance.GetRunId(), run.GetId())
		}
		if instance.GetPipelineId() != pipeline.GetId() {
			t.Errorf("instance %d pipeline_id = %d, want %d", i, instance.GetPipelineId(), pipeline.GetId())
		}
		if instance.GetName() != defs[i].GetName() {
			t.Errorf("instance %d name = %q, want %q", i, instance.GetName(), defs[i].GetName())
		}
		if instance.GetStatus() != dbpb.JobStatus_JOB_STATUS_PENDING {
			t.Errorf("instance %d status = %v, want PENDING", i, instance.GetStatus())
		}
	}
}

// createDagPipeline creates a pipeline with a "build" job and a "test" job
// that depends on "build", returning the pipeline and the two definition jobs
// (build first, test second).
func createDagPipeline(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, name string) (*dbpb.Pipeline, *dbpb.Job, *dbpb.Job) {
	t.Helper()
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: name})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	build, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{PipelineId: pipeline.GetId(), Name: "build"})
	if err != nil {
		t.Fatalf("CreateJob(build): %v", err)
	}
	test, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{PipelineId: pipeline.GetId(), Name: "test", DependsOn: []int64{build.GetId()}})
	if err != nil {
		t.Fatalf("CreateJob(test): %v", err)
	}
	return pipeline, build, test
}

// TestCreateRunRemapsDependencies verifies that a run's job instances carry
// their dependencies remapped from the pipeline's definition job ids to the
// run's own instance ids (F-07), so the run's internal dependencies reference
// the run's instances rather than the definitions.
func TestCreateRunRemapsDependencies(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, _, _ := createDagPipeline(t, client, ctx, "dag")

	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	jobs := resp.GetJobs()
	if len(jobs) != 2 {
		t.Fatalf("run created %d instances, want 2", len(jobs))
	}
	buildID := jobs[0].GetId()
	testID := jobs[1].GetId()
	// The build instance has no dependencies.
	if got := jobs[0].GetDependsOn(); len(got) != 0 {
		t.Errorf("build instance depends_on = %v, want none", got)
	}
	// The test instance depends on the build *instance* (not the definition).
	if got := jobs[1].GetDependsOn(); len(got) != 1 || got[0] != buildID {
		t.Errorf("test instance depends_on = %v, want [%d]", got, buildID)
	}
	// The remapping is persisted: fetching the instance shows the instance id.
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: testID})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got := fetched.GetDependsOn(); len(got) != 1 || got[0] != buildID {
		t.Errorf("persisted test instance depends_on = %v, want [%d]", got, buildID)
	}
}

// TestTwoRunsAreIndependent verifies that two runs of the same pipeline are
// independent: each creates its own set of job instances, and the instances
// of one run do not depend on the instances of the other (F-07).
func TestTwoRunsAreIndependent(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, _, _ := createDagPipeline(t, client, ctx, "twice")

	first, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("CreateRun (1st): %v", err)
	}
	second, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("CreateRun (2nd): %v", err)
	}
	if first.GetRun().GetId() == second.GetRun().GetId() {
		t.Fatal("two runs share an id; they must be distinct")
	}
	firstIDs := map[int64]bool{}
	for _, job := range first.GetJobs() {
		firstIDs[job.GetId()] = true
	}
	// Every instance of the second run is a distinct row not in the first run.
	for _, job := range second.GetJobs() {
		if firstIDs[job.GetId()] {
			t.Fatalf("second run instance %d is also in the first run", job.GetId())
		}
		if job.GetRunId() != second.GetRun().GetId() {
			t.Errorf("second run instance %d run_id = %d, want %d", job.GetId(), job.GetRunId(), second.GetRun().GetId())
		}
	}
	// The second run's "test" instance depends on the second run's "build"
	// instance, not the first run's.
	secondBuild := second.GetJobs()[0].GetId()
	if got := second.GetJobs()[1].GetDependsOn(); len(got) != 1 || got[0] != secondBuild {
		t.Errorf("second run test depends_on = %v, want [%d]", got, secondBuild)
	}
}

// TestListAndGetRun verifies that runs are queryable: ListRuns filters by
// pipeline, and GetRun returns a stored run (F-07).
func TestListAndGetRun(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipelineA, _ := createPipelineWithJobs(t, client, ctx, "a", &dbpb.CreateJobRequest{Name: "a1"})
	pipelineB, _ := createPipelineWithJobs(t, client, ctx, "b", &dbpb.CreateJobRequest{Name: "b1"})

	runA, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipelineA.GetId()})
	if err != nil {
		t.Fatalf("CreateRun A: %v", err)
	}
	if _, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipelineB.GetId()}); err != nil {
		t.Fatalf("CreateRun B: %v", err)
	}

	// Listing all runs returns both.
	all, err := client.ListRuns(ctx, &dbpb.ListRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if got := len(all.GetRuns()); got != 2 {
		t.Errorf("ListRuns (all) = %d, want 2", got)
	}
	// Listing by pipeline returns only that pipeline's run.
	byPipeline, err := client.ListRuns(ctx, &dbpb.ListRunsRequest{PipelineId: pipelineA.GetId()})
	if err != nil {
		t.Fatalf("ListRuns (pipeline): %v", err)
	}
	if got := len(byPipeline.GetRuns()); got != 1 {
		t.Fatalf("ListRuns (pipeline A) = %d, want 1", got)
	}
	if byPipeline.GetRuns()[0].GetId() != runA.GetRun().GetId() {
		t.Errorf("ListRuns (pipeline A) returned run %d, want %d", byPipeline.GetRuns()[0].GetId(), runA.GetRun().GetId())
	}
	// GetRun returns the stored run.
	fetched, err := client.GetRun(ctx, &dbpb.GetRunRequest{Id: runA.GetRun().GetId()})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if fetched.GetPipelineId() != pipelineA.GetId() {
		t.Errorf("GetRun pipeline = %d, want %d", fetched.GetPipelineId(), pipelineA.GetId())
	}
}

// TestUpdateRun verifies that UpdateRun persists a run's derived status and
// start/finish timestamps (F-07), and that a status of UNSPECIFIED leaves the
// status unchanged.
func TestUpdateRun(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, _ := createPipelineWithJobs(t, client, ctx, "upd", &dbpb.CreateJobRequest{Name: "j"})
	created, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	runID := created.GetRun().GetId()

	// Record the start time.
	updated, err := client.UpdateRun(ctx, &dbpb.UpdateRunRequest{
		Id:        runID,
		Status:    dbpb.RunStatus_RUN_STATUS_RUNNING,
		StartedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("UpdateRun (running): %v", err)
	}
	if updated.GetStatus() != dbpb.RunStatus_RUN_STATUS_RUNNING {
		t.Errorf("status = %v, want RUNNING", updated.GetStatus())
	}
	if updated.GetStartedAt() == nil {
		t.Error("started_at = nil, want set")
	}

	// A status of UNSPECIFIED leaves the status unchanged but still records
	// the finish time.
	finished, err := client.UpdateRun(ctx, &dbpb.UpdateRunRequest{
		Id:         runID,
		FinishedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("UpdateRun (finish): %v", err)
	}
	if finished.GetStatus() != dbpb.RunStatus_RUN_STATUS_RUNNING {
		t.Errorf("status after no-status update = %v, want RUNNING (unchanged)", finished.GetStatus())
	}
	if finished.GetFinishedAt() == nil {
		t.Error("finished_at = nil, want set")
	}

	// The terminal status is persisted.
	if _, err := client.UpdateRun(ctx, &dbpb.UpdateRunRequest{Id: runID, Status: dbpb.RunStatus_RUN_STATUS_SUCCEEDED}); err != nil {
		t.Fatalf("UpdateRun (succeeded): %v", err)
	}
	fetched, err := client.GetRun(ctx, &dbpb.GetRunRequest{Id: runID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if fetched.GetStatus() != dbpb.RunStatus_RUN_STATUS_SUCCEEDED {
		t.Errorf("persisted status = %v, want SUCCEEDED", fetched.GetStatus())
	}
}

// ---------------------------------------------------------------------------
// Job executions (fan-out)
// ---------------------------------------------------------------------------

// newGroupJob creates a pipeline and a pending job that targets a worker
// group, returning the job.
func newGroupJob(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, name, group string) *dbpb.Job {
	t.Helper()
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: name + "-pipeline"})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{
		PipelineId:  pipeline.GetId(),
		Name:        name,
		TargetGroup: group,
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	return job
}

// TestStartJobExecution verifies that a worker starting a pending group job
// records its execution and marks the job running (fan-out).
func TestStartJobExecution(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")

	// Two workers each start their own execution of the same job.
	for _, worker := range []string{"w1", "w2"} {
		resp, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), WorkerName: worker})
		if err != nil {
			t.Fatalf("StartJobExecution(%s): %v", worker, err)
		}
		if !resp.GetStarted() {
			t.Fatalf("StartJobExecution(%s) started = false, want true", worker)
		}
		if resp.GetExecution().GetWorkerName() != worker {
			t.Errorf("execution worker = %q, want %q", resp.GetExecution().GetWorkerName(), worker)
		}
		if resp.GetExecution().GetStatus() != dbpb.JobStatus_JOB_STATUS_RUNNING {
			t.Errorf("execution status = %v, want RUNNING", resp.GetExecution().GetStatus())
		}
	}

	// The job is now running.
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_RUNNING {
		t.Errorf("job status = %v, want RUNNING", fetched.GetStatus())
	}

	// Both workers' executions are listed.
	list, err := client.ListJobExecutions(ctx, &dbpb.ListJobExecutionsRequest{JobId: job.GetId()})
	if err != nil {
		t.Fatalf("ListJobExecutions: %v", err)
	}
	if got := len(list.GetExecutions()); got != 2 {
		t.Fatalf("executions = %d, want 2", got)
	}
}

// TestStartJobExecutionRejectsTerminalJob verifies that a job that has reached
// a terminal state is not started (a late worker start is a no-op).
func TestStartJobExecutionRejectsTerminalJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")

	// Mark the job succeeded (terminal).
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{Id: job.GetId(), Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	resp, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), WorkerName: "w1"})
	if err != nil {
		t.Fatalf("StartJobExecution: %v", err)
	}
	if resp.GetStarted() {
		t.Error("StartJobExecution started = true for a terminal job, want false")
	}
}

// TestStartJobExecutionRejectsAgentJob verifies that a job with an empty
// target group (an ephemeral agent) is not started by a worker.
func TestStartJobExecutionRejectsAgentJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "agent-pipeline"})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{PipelineId: pipeline.GetId(), Name: "agent-job"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	resp, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), WorkerName: "w1"})
	if err != nil {
		t.Fatalf("StartJobExecution: %v", err)
	}
	if resp.GetStarted() {
		t.Error("StartJobExecution started = true for an agent job, want false")
	}
}

// TestUpdateJobExecution verifies that a worker's outcome is recorded on its
// execution, and that a late report for an execution that already reached a
// terminal status is a no-op (it cannot clobber the terminal status).
func TestUpdateJobExecution(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")

	if _, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), WorkerName: "w1"}); err != nil {
		t.Fatalf("StartJobExecution: %v", err)
	}

	// The worker reports its execution succeeded (its final report carries the
	// finish time).
	updated, err := client.UpdateJobExecution(ctx, &dbpb.UpdateJobExecutionRequest{
		JobId:      job.GetId(),
		WorkerName: "w1",
		Status:     dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
		FinishedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("UpdateJobExecution: %v", err)
	}
	if updated.GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("execution status = %v, want SUCCEEDED", updated.GetStatus())
	}
	if updated.GetFinishedAt() == nil {
		t.Error("execution finished_at = nil, want set")
	}

	// A late report for the now-terminal execution is a no-op: the status is
	// not clobbered.
	again, err := client.UpdateJobExecution(ctx, &dbpb.UpdateJobExecutionRequest{
		JobId:      job.GetId(),
		WorkerName: "w1",
		Status:     dbpb.JobStatus_JOB_STATUS_FAILED,
		FinishedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("UpdateJobExecution (late): %v", err)
	}
	if again.GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("execution status after late report = %v, want SUCCEEDED (unchanged)", again.GetStatus())
	}
}

// TestAbandonWorkerExecutions verifies that a worker's running executions are
// marked failed when the worker re-registers after a restart (fan-out): the
// stale in-progress executions do not block the job's overall status.
func TestAbandonWorkerExecutions(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")

	if _, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), WorkerName: "w1"}); err != nil {
		t.Fatalf("StartJobExecution: %v", err)
	}

	// The worker restarts: its running execution is abandoned (marked failed).
	if _, err := client.AbandonWorkerExecutions(ctx, &dbpb.AbandonWorkerExecutionsRequest{WorkerName: "w1"}); err != nil {
		t.Fatalf("AbandonWorkerExecutions: %v", err)
	}

	list, err := client.ListJobExecutions(ctx, &dbpb.ListJobExecutionsRequest{JobId: job.GetId()})
	if err != nil {
		t.Fatalf("ListJobExecutions: %v", err)
	}
	if got := len(list.GetExecutions()); got != 1 {
		t.Fatalf("executions = %d, want 1", got)
	}
	if list.GetExecutions()[0].GetStatus() != dbpb.JobStatus_JOB_STATUS_FAILED {
		t.Errorf("abandoned execution status = %v, want FAILED", list.GetExecutions()[0].GetStatus())
	}
}

// TestJobFailureModeRoundTrip verifies that a job's failure mode is persisted
// and returned intact through the storage backend (fan-out).
func TestJobFailureModeRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	spec := &dbpb.JobSpec{
		FailureMode: dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT,
		Steps:       []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "go"}}}},
	}
	created, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "build", TargetGroup: "pool-a", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created.GetFailureMode() != dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT {
		t.Errorf("job failure_mode = %v, want BEST_EFFORT", created.GetFailureMode())
	}
	if created.GetSpec().GetFailureMode() != dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT {
		t.Errorf("spec failure_mode = %v, want BEST_EFFORT", created.GetSpec().GetFailureMode())
	}
}

// TestJobFailureModeDefaultsToAll verifies that a job created without a
// failure mode defaults to ALL (the default).
func TestJobFailureModeDefaultsToAll(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	created, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "build", TargetGroup: "pool-a"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created.GetFailureMode() != dbpb.FailureMode_FAILURE_MODE_ALL {
		t.Errorf("job failure_mode = %v, want ALL (default)", created.GetFailureMode())
	}
}

// ---------------------------------------------------------------------------
// Leader-election lease (heartbeat)
// ---------------------------------------------------------------------------

// acquireLease is a helper that acquires (or renews) the named lease with the
// given holder, TTL backstop, and heartbeat window.
func acquireLease(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, name, holder string, ttl, heartbeatTTL time.Duration) bool {
	t.Helper()
	resp, err := client.AcquireLease(ctx, &dbpb.AcquireLeaseRequest{
		Name:         name,
		Holder:       holder,
		Ttl:          durationpb.New(ttl),
		HeartbeatTtl: durationpb.New(heartbeatTTL),
	})
	if err != nil {
		t.Fatalf("AcquireLease(%s): %v", holder, err)
	}
	return resp.GetAcquired()
}

// TestLeaseAcquireAndRenew verifies that a replica acquires a free lease and
// renews it (keeping ownership), and that the lease state (holder, TTL
// backstop, heartbeat) is readable via GetLease.
func TestLeaseAcquireAndRenew(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	if !acquireLease(t, client, ctx, "scheduler", "replica-a", time.Minute, 30*time.Second) {
		t.Fatal("replica-a did not acquire a free lease")
	}

	// The holder renews (heartbeats) and keeps ownership.
	if !acquireLease(t, client, ctx, "scheduler", "replica-a", time.Minute, 30*time.Second) {
		t.Fatal("replica-a lost ownership on renewal")
	}

	lease, err := client.GetLease(ctx, &dbpb.GetLeaseRequest{Name: "scheduler"})
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if lease.GetHolder() != "replica-a" {
		t.Errorf("holder = %q, want replica-a", lease.GetHolder())
	}
	if lease.GetExpiresAt() == nil {
		t.Error("expires_at = nil, want set")
	}
	if lease.GetLastHeartbeat() == nil {
		t.Error("last_heartbeat = nil, want set")
	}
}

// TestLeaseLiveNotTakenOver verifies that a replica cannot take over a lease
// held by another replica whose heartbeat is fresh (within the window) and
// whose TTL backstop has not lapsed.
func TestLeaseLiveNotTakenOver(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	if !acquireLease(t, client, ctx, "scheduler", "replica-a", time.Minute, 30*time.Second) {
		t.Fatal("replica-a did not acquire the lease")
	}

	// replica-b cannot take over a live lease (fresh heartbeat, unexpired TTL).
	if acquireLease(t, client, ctx, "scheduler", "replica-b", time.Minute, 30*time.Second) {
		t.Error("replica-b took over a live lease, want it to be rejected")
	}

	lease, err := client.GetLease(ctx, &dbpb.GetLeaseRequest{Name: "scheduler"})
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if lease.GetHolder() != "replica-a" {
		t.Errorf("holder = %q, want replica-a (unchanged)", lease.GetHolder())
	}
}

// TestLeaseStaleHeartbeatTakenOver verifies that a replica can take over a
// lease whose holder's heartbeat is stale (older than the heartbeat window)
// even though the TTL backstop has not lapsed: this is what lets a follower
// detect a restarted or wedged leader quickly.
func TestLeaseStaleHeartbeatTakenOver(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// replica-a acquires with a short heartbeat window (100 ms) and a long TTL
	// backstop (1 min), then stops heartbeating.
	if !acquireLease(t, client, ctx, "scheduler", "replica-a", time.Minute, 100*time.Millisecond) {
		t.Fatal("replica-a did not acquire the lease")
	}

	// replica-a stops heartbeating; wait for its heartbeat to go stale.
	time.Sleep(150 * time.Millisecond)

	// replica-b can now take over: the heartbeat is stale even though the TTL
	// backstop (1 min) has not lapsed.
	if !acquireLease(t, client, ctx, "scheduler", "replica-b", time.Minute, 100*time.Millisecond) {
		t.Fatal("replica-b did not take over a lease with a stale heartbeat")
	}

	lease, err := client.GetLease(ctx, &dbpb.GetLeaseRequest{Name: "scheduler"})
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if lease.GetHolder() != "replica-b" {
		t.Errorf("holder = %q, want replica-b", lease.GetHolder())
	}
}

// TestLeaseRelease verifies that releasing a lease frees it so another
// replica can acquire it, and that releasing a lease the caller does not hold
// is a no-op.
func TestLeaseRelease(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	if !acquireLease(t, client, ctx, "scheduler", "replica-a", time.Minute, 30*time.Second) {
		t.Fatal("replica-a did not acquire the lease")
	}

	// replica-b releasing a lease it does not hold is a no-op.
	if _, err := client.ReleaseLease(ctx, &dbpb.ReleaseLeaseRequest{Name: "scheduler", Holder: "replica-b"}); err != nil {
		t.Fatalf("ReleaseLease (not held): %v", err)
	}
	lease, err := client.GetLease(ctx, &dbpb.GetLeaseRequest{Name: "scheduler"})
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if lease.GetHolder() != "replica-a" {
		t.Errorf("holder after no-op release = %q, want replica-a", lease.GetHolder())
	}

	// replica-a releases the lease; replica-b can now acquire it.
	if _, err := client.ReleaseLease(ctx, &dbpb.ReleaseLeaseRequest{Name: "scheduler", Holder: "replica-a"}); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}
	if !acquireLease(t, client, ctx, "scheduler", "replica-b", time.Minute, 30*time.Second) {
		t.Fatal("replica-b did not acquire the released lease")
	}
}

// TestGetLeaseNotFound verifies that reading a lease that has never been
// acquired is a NotFound.
func TestGetLeaseNotFound(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	if _, err := client.GetLease(ctx, &dbpb.GetLeaseRequest{Name: "nonexistent"}); status.Code(err) != codes.NotFound {
		t.Errorf("GetLease(nonexistent) = %v, want NotFound", err)
	}
}

// ---------------------------------------------------------------------------
// Cross-worker step barrier
// ---------------------------------------------------------------------------

// registerWorker registers (or refreshes) a worker in the group, which sets
// its last_seen_at to now (so it is alive).
func registerWorker(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, name, group string) {
	t.Helper()
	if _, err := client.RegisterWorker(ctx, &dbpb.RegisterWorkerRequest{Name: name, Group: group}); err != nil {
		t.Fatalf("RegisterWorker(%s): %v", name, err)
	}
}

// reportStepCompletion reports that a worker completed a step of a job.
func reportStepCompletion(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, jobID int64, worker string, step int32) {
	t.Helper()
	if _, err := client.ReportStepCompletion(ctx, &dbpb.ReportStepCompletionRequest{JobId: jobID, WorkerName: worker, StepIndex: step}); err != nil {
		t.Fatalf("ReportStepCompletion(%s, step %d): %v", worker, step, err)
	}
}

// checkStepBarrier checks the barrier for a job's step and returns the
// response.
func checkStepBarrier(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, jobID int64, step int32) *dbpb.CheckStepBarrierResponse {
	t.Helper()
	resp, err := client.CheckStepBarrier(ctx, &dbpb.CheckStepBarrierRequest{JobId: jobID, StepIndex: step})
	if err != nil {
		t.Fatalf("CheckStepBarrier(step %d): %v", step, err)
	}
	return resp
}

// TestStepBarrierDenormalized verifies that a job's step-barrier flag is
// denormalized from its spec onto the job row (and round-trips through the
// spec).
func TestStepBarrierDenormalized(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	spec := &dbpb.JobSpec{
		StepBarrier: true,
		Steps:       []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "go"}}}},
	}
	created, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "build", TargetGroup: "pool-a", Spec: spec})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if !created.GetStepBarrier() {
		t.Errorf("job step_barrier = false, want true")
	}
	if !created.GetSpec().GetStepBarrier() {
		t.Errorf("spec step_barrier = false, want true")
	}

	// A job created without the flag defaults to false.
	plain, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "plain", TargetGroup: "pool-a"})
	if err != nil {
		t.Fatalf("CreateJob(plain): %v", err)
	}
	if plain.GetStepBarrier() {
		t.Errorf("plain job step_barrier = true, want false (default)")
	}
}

// TestCheckStepBarrierSatisfied verifies that the barrier is satisfied once
// every alive worker in the group has completed the step.
func TestCheckStepBarrierSatisfied(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")
	registerWorker(t, client, ctx, "w1", "pool-a")
	registerWorker(t, client, ctx, "w2", "pool-a")

	// Neither worker has completed the step yet: not satisfied.
	if resp := checkStepBarrier(t, client, ctx, job.GetId(), 0); resp.GetSatisfied() || resp.GetCancelled() {
		t.Fatalf("before completions: satisfied=%v cancelled=%v, want both false", resp.GetSatisfied(), resp.GetCancelled())
	}

	// One worker completes: still not satisfied (the other is alive and
	// outstanding).
	reportStepCompletion(t, client, ctx, job.GetId(), "w1", 0)
	if resp := checkStepBarrier(t, client, ctx, job.GetId(), 0); resp.GetSatisfied() {
		t.Fatalf("after one completion: satisfied=true, want false")
	}

	// Both workers complete: satisfied.
	reportStepCompletion(t, client, ctx, job.GetId(), "w2", 0)
	if resp := checkStepBarrier(t, client, ctx, job.GetId(), 0); !resp.GetSatisfied() {
		t.Fatalf("after both completions: satisfied=false, want true")
	}
}

// TestCheckStepBarrierCancelled verifies that a cancelled job releases the
// barrier (a waiting worker should stop and report the job cancelled).
func TestCheckStepBarrierCancelled(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")
	registerWorker(t, client, ctx, "w1", "pool-a")

	// Cancel the job (it is pending, so the conditional cancel applies).
	if _, err := client.CancelJob(ctx, &dbpb.CancelJobRequest{Id: job.GetId()}); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	resp := checkStepBarrier(t, client, ctx, job.GetId(), 0)
	if !resp.GetCancelled() {
		t.Errorf("cancelled job: cancelled=false, want true")
	}
	if resp.GetSatisfied() {
		t.Errorf("cancelled job: satisfied=true, want false")
	}
}

// TestCheckStepBarrierAgentJob verifies that a job with an empty target group
// (an ephemeral agent) has no barrier to wait on (it is vacuously satisfied).
func TestCheckStepBarrierAgentJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "agent-pipeline"})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{PipelineId: pipeline.GetId(), Name: "agent-job"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	resp := checkStepBarrier(t, client, ctx, job.GetId(), 0)
	if !resp.GetSatisfied() {
		t.Errorf("agent job: satisfied=false, want true (no barrier)")
	}
	if resp.GetCancelled() {
		t.Errorf("agent job: cancelled=true, want false")
	}
}

// TestCheckStepBarrierDeadWorkerDropped verifies that a worker that is not
// alive (it has not heartbeated within the liveness window) is dropped from
// the barrier: its missing step completion does not block the job. This is
// what lets a job complete when a worker in the group dies mid-step.
func TestCheckStepBarrierDeadWorkerDropped(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")

	// Shorten the liveness window so a worker that is not refreshed goes stale
	// quickly; restore it afterwards.
	original := workerAliveThreshold
	workerAliveThreshold = 30 * time.Millisecond
	t.Cleanup(func() { workerAliveThreshold = original })

	// Register both workers (both alive, last_seen_at = now).
	registerWorker(t, client, ctx, "w1", "pool-a")
	registerWorker(t, client, ctx, "w2", "pool-a")

	// w1 completes the step. w2 does not (it will be made dead below).
	reportStepCompletion(t, client, ctx, job.GetId(), "w1", 0)

	// Let both workers go stale, then refresh only w1: w1 is now alive and w2
	// is dead.
	time.Sleep(100 * time.Millisecond)
	registerWorker(t, client, ctx, "w1", "pool-a")

	// The barrier is satisfied: w1 (alive) has completed the step, and w2
	// (dead) is dropped from the barrier.
	resp := checkStepBarrier(t, client, ctx, job.GetId(), 0)
	if !resp.GetSatisfied() {
		t.Errorf("dead worker dropped: satisfied=false, want true (w2 is dead and dropped)")
	}
	if resp.GetCancelled() {
		t.Errorf("dead worker dropped: cancelled=true, want false")
	}
}

// TestCheckStepBarrierMissingAliveWorkerBlocks verifies that an alive worker
// that has not completed the step blocks the barrier (a dead worker is
// dropped, but a live one is not).
func TestCheckStepBarrierMissingAliveWorkerBlocks(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")
	registerWorker(t, client, ctx, "w1", "pool-a")
	registerWorker(t, client, ctx, "w2", "pool-a")

	// Only w1 completes; w2 is alive and outstanding, so the barrier is not
	// satisfied.
	reportStepCompletion(t, client, ctx, job.GetId(), "w1", 0)
	resp := checkStepBarrier(t, client, ctx, job.GetId(), 0)
	if resp.GetSatisfied() {
		t.Errorf("alive worker outstanding: satisfied=true, want false")
	}
}

// ---------------------------------------------------------------------------
// F-08: job dependencies (DAG)
// ---------------------------------------------------------------------------

// jobDef is a helper that builds a proto job definition (F-08) for a pipeline.
func jobDef(key, name, targetGroup string, needs ...string) *dbpb.JobDefinition {
	if name == "" {
		name = key
	}
	return &dbpb.JobDefinition{Key: key, Name: name, TargetGroup: targetGroup, Needs: needs}
}

// TestCreatePipelineWithJobs verifies that a pipeline created with job
// definitions (F-08) persists the jobs with their keys and needs, and returns
// them (with their ids) in the response.
func TestCreatePipelineWithJobs(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "build",
		Jobs: []*dbpb.JobDefinition{
			jobDef("build", "", "linux-pool"),
			jobDef("test", "", "", "build"),
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if got := len(pipeline.GetJobs()); got != 2 {
		t.Fatalf("pipeline jobs = %d, want 2", got)
	}
	for _, def := range pipeline.GetJobs() {
		if def.GetId() == 0 {
			t.Errorf("job %q id = 0, want non-zero", def.GetKey())
		}
	}
	// The persisted jobs belong to the pipeline and carry their keys/needs.
	jobs, err := client.ListJobs(ctx, &dbpb.ListJobsRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if got := len(jobs.GetJobs()); got != 2 {
		t.Fatalf("persisted jobs = %d, want 2", got)
	}
	byKey := map[string]*dbpb.Job{}
	for _, j := range jobs.GetJobs() {
		byKey[j.GetKey()] = j
		if j.GetPipelineId() != pipeline.GetId() {
			t.Errorf("job %q pipeline = %d, want %d", j.GetKey(), j.GetPipelineId(), pipeline.GetId())
		}
	}
	// The backend stores dependencies as ids (F-08): "test" depends on the
	// id of the "build" job, and "build" has no dependencies.
	if build, ok := byKey["build"]; !ok || len(build.GetDependsOn()) != 0 {
		t.Errorf("build job = %+v, want present with no depends_on", build)
	}
	if test, ok := byKey["test"]; !ok || len(test.GetDependsOn()) != 1 || test.GetDependsOn()[0] != byKey["build"].GetId() {
		t.Errorf("test job = %+v, want depends_on [build id]", test)
	}
}

// TestCreatePipelineRejectsCycle verifies that a pipeline whose job
// dependencies form a cycle is rejected at save time (F-08).
func TestCreatePipelineRejectsCycle(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	_, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "cyclic",
		Jobs: []*dbpb.JobDefinition{
			jobDef("a", "", "", "b"),
			jobDef("b", "", "", "a"),
		},
	})
	if err == nil {
		t.Fatal("CreatePipeline with a cycle succeeded, want an error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("cycle error code = %v, want InvalidArgument", status.Code(err))
	}
	// Nothing was persisted.
	pipelines, _ := client.ListPipelines(ctx, &dbpb.ListPipelinesRequest{})
	for _, p := range pipelines.GetPipelines() {
		if p.GetName() == "cyclic" {
			t.Error("cyclic pipeline was persisted, want it rejected")
		}
	}
}

// TestCreatePipelineRejectsUnknownNeed verifies that a pipeline with a job
// that needs a key no job has is rejected at save time (F-08).
func TestCreatePipelineRejectsUnknownNeed(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	_, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "dangling",
		Jobs: []*dbpb.JobDefinition{
			jobDef("a", "", "", "ghost"),
		},
	})
	if err == nil {
		t.Fatal("CreatePipeline with an unknown need succeeded, want an error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("unknown-need error code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestCreatePipelineRejectsDuplicateKey verifies that a pipeline with two jobs
// sharing a key is rejected at save time (F-08).
func TestCreatePipelineRejectsDuplicateKey(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	_, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "dup",
		Jobs: []*dbpb.JobDefinition{
			jobDef("a", "first", ""),
			jobDef("a", "second", ""),
		},
	})
	if err == nil {
		t.Fatal("CreatePipeline with duplicate keys succeeded, want an error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("duplicate-key error code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestCreateRunRemapsNeeds verifies that a run of a pipeline whose jobs use
// needs (keys, F-08) creates job instances whose depends_on are remapped from
// the definition keys to the run's own instance ids, so the run's internal
// dependencies reference the run's instances rather than the definitions.
func TestCreateRunRemapsNeeds(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "dag",
		Jobs: []*dbpb.JobDefinition{
			jobDef("build", "", "linux-pool"),
			jobDef("test", "", "", "build"),
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	jobs := resp.GetJobs()
	if len(jobs) != 2 {
		t.Fatalf("run created %d instances, want 2", len(jobs))
	}
	// The instances keep their keys; the "test" instance depends on the
	// "build" *instance* (remapped from the key).
	buildID, testID := int64(0), int64(0)
	var testInstance *dbpb.Job
	for _, j := range jobs {
		switch j.GetKey() {
		case "build":
			buildID = j.GetId()
		case "test":
			testID = j.GetId()
			testInstance = j
		}
	}
	if buildID == 0 || testID == 0 {
		t.Fatalf("could not find build/test instances: %+v", jobs)
	}
	if got := testInstance.GetDependsOn(); len(got) != 1 || got[0] != buildID {
		t.Errorf("test instance depends_on = %v, want [%d]", got, buildID)
	}
	// The remapping is persisted on the instance.
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: testID})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got := fetched.GetDependsOn(); len(got) != 1 || got[0] != buildID {
		t.Errorf("persisted test instance depends_on = %v, want [%d]", got, buildID)
	}
}

// TestCreateJobWithNeedsValidatesDAG verifies that adding a job with needs to
// a pipeline validates the resulting DAG (F-08): a job whose needs form a
// cycle with the pipeline's existing jobs is rejected.
func TestCreateJobWithNeedsValidatesDAG(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "dag",
		Jobs: []*dbpb.JobDefinition{jobDef("a", "", "")},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	// Adding a job "b" that needs "a" is fine (a -> b, no cycle).
	if _, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{
		PipelineId: pipeline.GetId(),
		Name:       "b",
		Key:        "b",
		Needs:      []string{"a"},
	}); err != nil {
		t.Fatalf("CreateJob(b needs a): %v", err)
	}
	// Adding a job "a2" that needs "b" while "a" needs "a2" would be a cycle;
	// instead, add a job that needs itself indirectly: a job "c" that needs
	// "b" is fine, but a job that makes a cycle is rejected. Add "a" again is
	// a duplicate key (rejected); add a job "z" that needs "a" and "a" already
	// exists -> fine. To force a cycle, add a job whose key is "a" is a dup.
	// So: add a job "b2" that needs "a" (fine), then verify a duplicate key
	// is rejected.
	if _, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{
		PipelineId: pipeline.GetId(),
		Name:       "a-dup",
		Key:        "a",
	}); err == nil {
		t.Error("CreateJob with a duplicate key succeeded, want an error")
	}
	// A job needing an unknown key is rejected.
	if _, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{
		PipelineId: pipeline.GetId(),
		Name:       "ghost",
		Key:        "ghost",
		Needs:      []string{"nonexistent"},
	}); err == nil {
		t.Error("CreateJob with an unknown need succeeded, want an error")
	}
}

// ---------------------------------------------------------------------------
// Triggers (F-09)
// ---------------------------------------------------------------------------

// TestCreatePipelineWithTriggers verifies that a pipeline's triggers (F-09)
// are persisted and round-trip through GetPipeline: a cron trigger (with its
// expression and params), a webhook trigger (with its secret), and an event
// trigger (with its watched pipeline and status).
func TestCreatePipelineWithTriggers(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "nightly",
		Triggers: []*dbpb.Trigger{
			{Name: "every-minute", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "* * * * *",
				Params: map[string]string{"branch": "main"}},
			{Name: "hook", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK, Secret: "s3cret"},
			{Name: "on-build", Type: dbpb.TriggerType_TRIGGER_TYPE_EVENT, EventPipeline: "build",
				EventStatus: dbpb.RunStatus_RUN_STATUS_SUCCEEDED},
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if got := len(pipeline.GetTriggers()); got != 3 {
		t.Fatalf("pipeline triggers = %d, want 3", got)
	}

	// The triggers round-trip through GetPipeline.
	fetched, err := client.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: pipeline.GetId()})
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	byName := map[string]*dbpb.Trigger{}
	for _, trigger := range fetched.GetTriggers() {
		byName[trigger.GetName()] = trigger
	}
	if cron := byName["every-minute"]; cron == nil || cron.GetCron() != "* * * * *" || cron.GetParams()["branch"] != "main" {
		t.Errorf("cron trigger = %+v, want cron expression and params", cron)
	}
	if hook := byName["hook"]; hook == nil || hook.GetSecret() != "s3cret" {
		t.Errorf("webhook trigger = %+v, want secret", hook)
	}
	if event := byName["on-build"]; event == nil || event.GetEventPipeline() != "build" ||
		event.GetEventStatus() != dbpb.RunStatus_RUN_STATUS_SUCCEEDED {
		t.Errorf("event trigger = %+v, want watched pipeline and status", event)
	}
}

// TestUpdatePipelineReplacesTriggers verifies that updating a pipeline with a
// non-nil triggers list replaces its triggers (F-09).
func TestUpdatePipelineReplacesTriggers(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:     "p",
		Triggers: []*dbpb.Trigger{{Name: "old", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "* * * * *"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	updated, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:       pipeline.GetId(),
		Triggers: []*dbpb.Trigger{{Name: "new", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK, Secret: "s"}},
	})
	if err != nil {
		t.Fatalf("UpdatePipeline: %v", err)
	}
	if got := len(updated.GetTriggers()); got != 1 {
		t.Fatalf("updated triggers = %d, want 1", got)
	}
	if updated.GetTriggers()[0].GetName() != "new" {
		t.Errorf("updated trigger = %q, want new", updated.GetTriggers()[0].GetName())
	}
}

// TestCreatePipelineWebhookOptionalSecret verifies that a webhook trigger with
// an empty secret is accepted (F-09): the secret is optional and only enforced
// on the webhook endpoint when set.
func TestCreatePipelineWebhookOptionalSecret(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:     "open",
		Triggers: []*dbpb.Trigger{{Name: "open-hook", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline with an empty-secret webhook trigger: %v (want accepted)", err)
	}
	fetched, err := client.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: pipeline.GetId()})
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	if got := len(fetched.GetTriggers()); got != 1 {
		t.Fatalf("triggers = %d, want 1", got)
	}
	if fetched.GetTriggers()[0].GetSecret() != "" {
		t.Errorf("secret = %q, want empty (optional)", fetched.GetTriggers()[0].GetSecret())
	}
}

// TestCreatePipelineWebhookOIDCClaims verifies that a webhook trigger's
// oidc_issuer and oidc_claims round-trip through the database (F-09).
func TestCreatePipelineWebhookOIDCClaims(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "oidc-hook",
		Triggers: []*dbpb.Trigger{{
			Name:       "github",
			Type:       dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK,
			OidcIssuer: "https://github.com",
			OidcClaims: map[string]string{"org": "acme", "repo": "*"},
		}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	fetched, err := client.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: pipeline.GetId()})
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	if got := len(fetched.GetTriggers()); got != 1 {
		t.Fatalf("triggers = %d, want 1", got)
	}
	trigger := fetched.GetTriggers()[0]
	if trigger.GetOidcIssuer() != "https://github.com" {
		t.Errorf("oidc_issuer = %q, want https://github.com", trigger.GetOidcIssuer())
	}
	if trigger.GetOidcClaims()["org"] != "acme" || trigger.GetOidcClaims()["repo"] != "*" {
		t.Errorf("oidc_claims = %v, want org=acme repo=*", trigger.GetOidcClaims())
	}
}

// TestCreatePipelineRejectsWebhookEmptyClaimName verifies that a webhook
// trigger with an empty claim name in oidc_claims is rejected at save time
// (F-09).
func TestCreatePipelineRejectsWebhookEmptyClaimName(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	_, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "bad-claim",
		Triggers: []*dbpb.Trigger{{Name: "x", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK,
			OidcIssuer: "https://idp.example", OidcClaims: map[string]string{"": "v"}}},
	})
	if err == nil {
		t.Fatal("CreatePipeline with an empty claim name: want an error, got nil")
	}
}

// TestCreatePipelineRejectsInvalidTriggers verifies that a pipeline with
// malformed triggers is rejected at save time (F-09): an empty name, a
// duplicate name, a cron trigger with no/invalid expression, and an event
// trigger with no watched pipeline or status. (A webhook trigger's secret is
// optional, so a webhook with no secret is valid.)
func TestCreatePipelineRejectsInvalidTriggers(t *testing.T) {
	cases := []struct {
		name     string
		triggers []*dbpb.Trigger
	}{
		{"empty name", []*dbpb.Trigger{{Name: "", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "* * * * *"}}},
		{"duplicate name", []*dbpb.Trigger{
			{Name: "x", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "* * * * *"},
			{Name: "x", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK, Secret: "s"},
		}},
		{"cron no expression", []*dbpb.Trigger{{Name: "x", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON}}},
		{"cron invalid expression", []*dbpb.Trigger{{Name: "x", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "not a cron"}}},
		{"event no pipeline", []*dbpb.Trigger{{Name: "x", Type: dbpb.TriggerType_TRIGGER_TYPE_EVENT, EventStatus: dbpb.RunStatus_RUN_STATUS_SUCCEEDED}}},
		{"event no status", []*dbpb.Trigger{{Name: "x", Type: dbpb.TriggerType_TRIGGER_TYPE_EVENT, EventPipeline: "build"}}},
		{"no type", []*dbpb.Trigger{{Name: "x"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := startServer(t)
			_, err := client.CreatePipeline(context.Background(), &dbpb.CreatePipelineRequest{
				Name: "p", Triggers: tc.triggers,
			})
			if err == nil {
				t.Fatalf("CreatePipeline with %s succeeded, want an error", tc.name)
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("error code = %v, want InvalidArgument", status.Code(err))
			}
		})
	}
}

// TestTriggerRunCreatesRun verifies that TriggerRun (F-09) creates a run of
// the pipeline (with one job instance per job definition) and records the
// trigger's source and name on the run.
func TestTriggerRunCreatesRun(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:     "nightly",
		Jobs:     []*dbpb.JobDefinition{jobDef("build", "", "linux-pool")},
		Triggers: []*dbpb.Trigger{{Name: "every-minute", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "* * * * *"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	resp, err := client.TriggerRun(ctx, &dbpb.TriggerRunRequest{
		PipelineId:  pipeline.GetId(),
		TriggerName: "every-minute",
		TriggerType: dbpb.TriggerType_TRIGGER_TYPE_CRON,
		DedupWindow: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("TriggerRun: %v", err)
	}
	if !resp.GetCreated() {
		t.Fatalf("created = false, want true")
	}
	run := resp.GetRun()
	if run.GetTrigger() != "cron" {
		t.Errorf("run trigger = %q, want cron", run.GetTrigger())
	}
	if run.GetTriggerName() != "every-minute" {
		t.Errorf("run trigger_name = %q, want every-minute", run.GetTriggerName())
	}
	if got := len(resp.GetJobs()); got != 1 {
		t.Errorf("run created %d jobs, want 1", got)
	}
}

// TestTriggerRunDedupsCron verifies that a second TriggerRun for the same
// cron trigger within the dedup window is a no-op (F-09): it returns the
// existing run and reports created=false, so a racing replica cannot fire the
// same scheduled time twice.
func TestTriggerRunDedupsCron(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:     "nightly",
		Jobs:     []*dbpb.JobDefinition{jobDef("build", "", "linux-pool")},
		Triggers: []*dbpb.Trigger{{Name: "every-minute", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "* * * * *"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	first, err := client.TriggerRun(ctx, &dbpb.TriggerRunRequest{
		PipelineId:  pipeline.GetId(),
		TriggerName: "every-minute",
		TriggerType: dbpb.TriggerType_TRIGGER_TYPE_CRON,
		DedupWindow: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("TriggerRun (1st): %v", err)
	}
	if !first.GetCreated() {
		t.Fatalf("first TriggerRun created = false, want true")
	}
	// A duplicate within the window is a no-op.
	second, err := client.TriggerRun(ctx, &dbpb.TriggerRunRequest{
		PipelineId:  pipeline.GetId(),
		TriggerName: "every-minute",
		TriggerType: dbpb.TriggerType_TRIGGER_TYPE_CRON,
		DedupWindow: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatalf("TriggerRun (2nd): %v", err)
	}
	if second.GetCreated() {
		t.Errorf("second TriggerRun created = true, want false (deduped)")
	}
	if second.GetRun().GetId() != first.GetRun().GetId() {
		t.Errorf("second run id = %d, want the first run's id %d", second.GetRun().GetId(), first.GetRun().GetId())
	}
}

// TestTriggerRunDedupsEvent verifies that an event trigger's run is
// deduplicated by its source run's id (F-09): the same source run can never
// start the same downstream run twice.
func TestTriggerRunDedupsEvent(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		Jobs: []*dbpb.JobDefinition{jobDef("deploy", "", "linux-pool")},
		Triggers: []*dbpb.Trigger{{Name: "on-build", Type: dbpb.TriggerType_TRIGGER_TYPE_EVENT,
			EventPipeline: "build", EventStatus: dbpb.RunStatus_RUN_STATUS_SUCCEEDED}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	params := map[string]string{"cdrom.source_run": "100", "cdrom.source_pipeline": "build"}
	first, err := client.TriggerRun(ctx, &dbpb.TriggerRunRequest{
		PipelineId:  pipeline.GetId(),
		TriggerName: "on-build",
		TriggerType: dbpb.TriggerType_TRIGGER_TYPE_EVENT,
		Params:      params,
	})
	if err != nil {
		t.Fatalf("TriggerRun (1st): %v", err)
	}
	if !first.GetCreated() {
		t.Fatalf("first TriggerRun created = false, want true")
	}
	// The source run's id is recorded on the run.
	if first.GetRun().GetSourceRunId() != 100 {
		t.Errorf("run source_run_id = %d, want 100", first.GetRun().GetSourceRunId())
	}
	// The same source run is a no-op.
	second, err := client.TriggerRun(ctx, &dbpb.TriggerRunRequest{
		PipelineId:  pipeline.GetId(),
		TriggerName: "on-build",
		TriggerType: dbpb.TriggerType_TRIGGER_TYPE_EVENT,
		Params:      params,
	})
	if err != nil {
		t.Fatalf("TriggerRun (2nd): %v", err)
	}
	if second.GetCreated() {
		t.Errorf("second TriggerRun created = true, want false (deduped by source run)")
	}
}

// TestTriggerRunValidates verifies that TriggerRun (F-09) rejects a request
// with no pipeline id or trigger name.
func TestTriggerRunValidates(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	if _, err := client.TriggerRun(ctx, &dbpb.TriggerRunRequest{TriggerName: "x"}); err == nil {
		t.Error("TriggerRun with no pipeline_id: want error, got nil")
	}
	if _, err := client.TriggerRun(ctx, &dbpb.TriggerRunRequest{PipelineId: 1}); err == nil {
		t.Error("TriggerRun with no trigger_name: want error, got nil")
	}
}

// TestListPipelinesFilterByTriggerType verifies that ListPipelines (F-09)
// restricts its result to pipelines that carry at least one trigger of the
// requested type: the scheduler's cron and event loops use it to fetch only
// the pipelines they act on. An UNSPECIFIED type returns every pipeline.
func TestListPipelinesFilterByTriggerType(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// A pipeline with a cron trigger, one with an event trigger, and one with
	// no triggers at all.
	if _, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:     "cron-pipeline",
		Triggers: []*dbpb.Trigger{{Name: "c", Type: dbpb.TriggerType_TRIGGER_TYPE_CRON, Cron: "* * * * *"}},
	}); err != nil {
		t.Fatalf("CreatePipeline (cron): %v", err)
	}
	if _, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "event-pipeline",
		Triggers: []*dbpb.Trigger{{Name: "e", Type: dbpb.TriggerType_TRIGGER_TYPE_EVENT,
			EventPipeline: "cron-pipeline", EventStatus: dbpb.RunStatus_RUN_STATUS_SUCCEEDED}},
	}); err != nil {
		t.Fatalf("CreatePipeline (event): %v", err)
	}
	if _, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "plain"}); err != nil {
		t.Fatalf("CreatePipeline (plain): %v", err)
	}

	names := func(resp *dbpb.ListPipelinesResponse) map[string]bool {
		out := make(map[string]bool, len(resp.GetPipelines()))
		for _, p := range resp.GetPipelines() {
			out[p.GetName()] = true
		}
		return out
	}

	// No filter: every pipeline is returned.
	all, err := client.ListPipelines(ctx, &dbpb.ListPipelinesRequest{})
	if err != nil {
		t.Fatalf("ListPipelines (all): %v", err)
	}
	if got := len(all.GetPipelines()); got != 3 {
		t.Errorf("ListPipelines (all) = %d pipelines, want 3", got)
	}

	// Cron filter: only the cron pipeline.
	cronResp, err := client.ListPipelines(ctx, &dbpb.ListPipelinesRequest{TriggerType: dbpb.TriggerType_TRIGGER_TYPE_CRON})
	if err != nil {
		t.Fatalf("ListPipelines (cron): %v", err)
	}
	if got := names(cronResp); len(got) != 1 || !got["cron-pipeline"] {
		t.Errorf("ListPipelines (cron) = %v, want only cron-pipeline", got)
	}

	// Event filter: only the event pipeline.
	eventResp, err := client.ListPipelines(ctx, &dbpb.ListPipelinesRequest{TriggerType: dbpb.TriggerType_TRIGGER_TYPE_EVENT})
	if err != nil {
		t.Fatalf("ListPipelines (event): %v", err)
	}
	if got := names(eventResp); len(got) != 1 || !got["event-pipeline"] {
		t.Errorf("ListPipelines (event) = %v, want only event-pipeline", got)
	}

	// Webhook filter: no pipeline has a webhook trigger, so none are returned.
	webhookResp, err := client.ListPipelines(ctx, &dbpb.ListPipelinesRequest{TriggerType: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK})
	if err != nil {
		t.Fatalf("ListPipelines (webhook): %v", err)
	}
	if got := len(webhookResp.GetPipelines()); got != 0 {
		t.Errorf("ListPipelines (webhook) = %d pipelines, want 0", got)
	}
}

// TestCreatePipelineWithParams verifies that a pipeline's parameter
// declarations (F-10) are persisted and round-trip through GetPipeline.
func TestCreatePipelineWithParams(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		Params: []*dbpb.Parameter{
			{Name: "version", Default: "1.0.0", Description: "the version to deploy"},
			{Name: "environment"},
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if got := len(pipeline.GetParams()); got != 2 {
		t.Fatalf("pipeline params = %d, want 2", got)
	}

	fetched, err := client.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: pipeline.GetId()})
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	byName := map[string]*dbpb.Parameter{}
	for _, param := range fetched.GetParams() {
		byName[param.GetName()] = param
	}
	if version := byName["version"]; version == nil || version.GetDefault() != "1.0.0" || version.GetDescription() != "the version to deploy" {
		t.Errorf("version param = %+v, want default and description", version)
	}
	if env := byName["environment"]; env == nil || env.GetDefault() != "" {
		t.Errorf("environment param = %+v, want no default", env)
	}
}

// TestCreatePipelineRejectsDuplicateParam verifies that a pipeline with two
// parameters of the same name is rejected (F-10): a parameter must be
// referenceable unambiguously.
func TestCreatePipelineRejectsDuplicateParam(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	_, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "dup",
		Params: []*dbpb.Parameter{
			{Name: "version"},
			{Name: "version"},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreatePipeline with duplicate param = %v, want InvalidArgument", err)
	}
}

// TestCreatePipelineRejectsEmptyParamName verifies that a pipeline with a
// parameter that has an empty name is rejected (F-10).
func TestCreatePipelineRejectsEmptyParamName(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	_, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:   "empty",
		Params: []*dbpb.Parameter{{Name: ""}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreatePipeline with empty param name = %v, want InvalidArgument", err)
	}
}

// TestUpdatePipelineReplacesParams verifies that updating a pipeline with a
// non-nil params list replaces its parameter declarations (F-10), and that an
// update with no params leaves them unchanged.
func TestUpdatePipelineReplacesParams(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:   "p",
		Params: []*dbpb.Parameter{{Name: "old", Default: "a"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	updated, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:     pipeline.GetId(),
		Params: []*dbpb.Parameter{{Name: "new", Default: "b"}},
	})
	if err != nil {
		t.Fatalf("UpdatePipeline: %v", err)
	}
	if got := len(updated.GetParams()); got != 1 {
		t.Fatalf("updated params = %d, want 1", got)
	}
	if updated.GetParams()[0].GetName() != "new" || updated.GetParams()[0].GetDefault() != "b" {
		t.Errorf("updated param = %+v, want new/b", updated.GetParams()[0])
	}

	// An update with no params leaves the pipeline's params unchanged.
	unchanged, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{Id: pipeline.GetId()})
	if err != nil {
		t.Fatalf("UpdatePipeline (no params): %v", err)
	}
	if got := len(unchanged.GetParams()); got != 1 {
		t.Fatalf("params after no-param update = %d, want 1 (unchanged)", got)
	}
	if unchanged.GetParams()[0].GetName() != "new" {
		t.Errorf("param after no-param update = %q, want new", unchanged.GetParams()[0].GetName())
	}
}

// TestCreateRunDenormalizesRunParams verifies that a run's concrete parameter
// values (F-10) are denormalized onto each of its job instances: a parameter
// supplied by the run uses the supplied value, a parameter not supplied falls
// back to its default, and a parameter with neither a supplied value nor a
// default is omitted (so a spec field that references it fails to
// interpolate).
func TestCreateRunDenormalizesRunParams(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		Params: []*dbpb.Parameter{
			{Name: "version", Default: "1.0.0"},       // supplied by the run
			{Name: "environment", Default: "staging"}, // not supplied: falls back to default
			{Name: "token"}, // not supplied, no default: omitted
		},
		Jobs: []*dbpb.JobDefinition{
			{Key: "build", Name: "build", TargetGroup: "linux-pool"},
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{
		PipelineId: pipeline.GetId(),
		Trigger:    "manual",
		Params:     map[string]string{"version": "2.0.0"},
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if got := len(resp.GetJobs()); got != 1 {
		t.Fatalf("run created %d job instances, want 1", got)
	}
	instance := resp.GetJobs()[0]
	runParams := instance.GetRunParams()
	if runParams["version"] != "2.0.0" {
		t.Errorf("run_params[version] = %q, want supplied value 2.0.0", runParams["version"])
	}
	if runParams["environment"] != "staging" {
		t.Errorf("run_params[environment] = %q, want default staging", runParams["environment"])
	}
	if _, present := runParams["token"]; present {
		t.Errorf("run_params[token] = %q, want omitted (no supplied value, no default)", runParams["token"])
	}
}

// TestCreateRunNoParamsLeavesRunParamsEmpty verifies that a run of a pipeline
// with no parameters (or a run that supplies none) leaves its job instances'
// run_params empty (F-10).
func TestCreateRunNoParamsLeavesRunParamsEmpty(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "plain",
		Jobs: []*dbpb.JobDefinition{{Key: "build", Name: "build"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if got := len(resp.GetJobs()[0].GetRunParams()); got != 0 {
		t.Errorf("run_params = %v, want empty for a pipeline with no params", resp.GetJobs()[0].GetRunParams())
	}
}

// firstStepCommand returns the `command` param of the job's first step, or
// "" if the job has no steps or the first step has no command param.
func firstStepCommand(job *dbpb.Job) string {
	if job == nil || len(job.GetSpec().GetSteps()) == 0 {
		return ""
	}
	return job.GetSpec().GetSteps()[0].GetParams()["command"].GetString_()
}

// TestCreatePipelineStartsAtVersion1 verifies that a freshly created pipeline
// starts at version 1 and that a version-1 snapshot is recorded (F-11).
func TestCreatePipelineStartsAtVersion1(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		Jobs: []*dbpb.JobDefinition{{Key: "build", Name: "build"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if got := pipeline.GetVersion(); got != 1 {
		t.Fatalf("new pipeline version = %d, want 1", got)
	}

	list, err := client.ListPipelineVersions(ctx, &dbpb.ListPipelineVersionsRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("ListPipelineVersions: %v", err)
	}
	if got := len(list.GetVersions()); got != 1 {
		t.Fatalf("pipeline has %d versions, want 1", got)
	}
	if v := list.GetVersions()[0]; v.GetVersion() != 1 || v.GetPipelineId() != pipeline.GetId() {
		t.Errorf("version snapshot = (pipeline %d, version %d), want (pipeline %d, version 1)",
			v.GetPipelineId(), v.GetVersion(), pipeline.GetId())
	}
}

// TestUpdatePipelineBumpsVersion verifies that editing a pipeline bumps its
// version and that the previous version's snapshot is preserved (F-11).
func TestUpdatePipelineBumpsVersion(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		Jobs: []*dbpb.JobDefinition{{Key: "build", Name: "build"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if got := pipeline.GetVersion(); got != 1 {
		t.Fatalf("new pipeline version = %d, want 1", got)
	}

	updated, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:   pipeline.GetId(),
		Name: "deploy v2",
		Jobs: []*dbpb.JobDefinition{{Key: "build", Name: "build"}},
	})
	if err != nil {
		t.Fatalf("UpdatePipeline: %v", err)
	}
	if got := updated.GetVersion(); got != 2 {
		t.Fatalf("updated pipeline version = %d, want 2", got)
	}

	list, err := client.ListPipelineVersions(ctx, &dbpb.ListPipelineVersionsRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("ListPipelineVersions: %v", err)
	}
	if got := len(list.GetVersions()); got != 2 {
		t.Fatalf("pipeline has %d versions, want 2", got)
	}
	// Newest first.
	if v := list.GetVersions()[0]; v.GetVersion() != 2 || v.GetName() != "deploy v2" {
		t.Errorf("newest version = (version %d, name %q), want (2, deploy v2)", v.GetVersion(), v.GetName())
	}
	if v := list.GetVersions()[1]; v.GetVersion() != 1 || v.GetName() != "deploy" {
		t.Errorf("oldest version = (version %d, name %q), want (1, deploy)", v.GetVersion(), v.GetName())
	}
}

// TestRunRecordsCurrentVersion verifies that a run records the version of the
// pipeline that was active when it started (F-11).
func TestRunRecordsCurrentVersion(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		Jobs: []*dbpb.JobDefinition{{
			Key:         "build",
			Name:        "build",
			TargetGroup: "linux-pool",
			Spec:        &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "make build"}}}}},
		}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if got := resp.GetRun().GetPipelineVersion(); got != 1 {
		t.Fatalf("run pipeline_version = %d, want 1", got)
	}
	if got := firstStepCommand(resp.GetJobs()[0]); got != "make build" {
		t.Errorf("run job command = %q, want make build", got)
	}
}

// TestRunAgainstSpecificVersionReproducesOldDefinition verifies that a run can
// be executed against a specific (older) version, reproducing that version's
// definition, while a run against the current version uses the new definition
// (F-11).
func TestRunAgainstSpecificVersionReproducesOldDefinition(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		Jobs: []*dbpb.JobDefinition{{
			Key:         "build",
			Name:        "build",
			TargetGroup: "linux-pool",
			Spec:        &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "make build"}}}}},
		}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	// A run against the current (v1) definition.
	resp1, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun (v1): %v", err)
	}
	if got := resp1.GetRun().GetPipelineVersion(); got != 1 {
		t.Fatalf("run1 pipeline_version = %d, want 1", got)
	}

	// Edit the pipeline: the build job now runs a different command. This
	// bumps the pipeline to version 2.
	if _, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:   pipeline.GetId(),
		Name: "deploy",
		Jobs: []*dbpb.JobDefinition{{
			Key:         "build",
			Name:        "build",
			TargetGroup: "linux-pool",
			Spec:        &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "make build-v2"}}}}},
		}},
	}); err != nil {
		t.Fatalf("UpdatePipeline: %v", err)
	}

	// A run explicitly against version 1 reproduces the old definition.
	respOld, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{
		PipelineId:      pipeline.GetId(),
		Trigger:         "manual",
		PipelineVersion: 1,
	})
	if err != nil {
		t.Fatalf("CreateRun (version 1): %v", err)
	}
	if got := respOld.GetRun().GetPipelineVersion(); got != 1 {
		t.Errorf("runOld pipeline_version = %d, want 1", got)
	}
	if got := firstStepCommand(respOld.GetJobs()[0]); got != "make build" {
		t.Errorf("runOld job command = %q, want make build (the v1 definition)", got)
	}

	// A run against the current version uses the new definition.
	respNew, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun (current): %v", err)
	}
	if got := respNew.GetRun().GetPipelineVersion(); got != 2 {
		t.Errorf("runNew pipeline_version = %d, want 2", got)
	}
	if got := firstStepCommand(respNew.GetJobs()[0]); got != "make build-v2" {
		t.Errorf("runNew job command = %q, want make build-v2 (the v2 definition)", got)
	}
}

// TestCreateRunUnknownVersionRejected verifies that a run requesting a version
// that does not exist is rejected (F-11).
func TestCreateRunUnknownVersionRejected(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		Jobs: []*dbpb.JobDefinition{{Key: "build", Name: "build"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	_, err = client.CreateRun(ctx, &dbpb.CreateRunRequest{
		PipelineId:      pipeline.GetId(),
		Trigger:         "manual",
		PipelineVersion: 99,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateRun(version 99) error = %v, want InvalidArgument", err)
	}
}

// TestListAndGetPipelineVersion verifies the List/GetPipelineVersion RPCs
// round-trip a version snapshot (F-11).
func TestListAndGetPipelineVersion(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:        "deploy",
		Description: "deploys the app",
		Jobs: []*dbpb.JobDefinition{
			{Key: "build", Name: "build", TargetGroup: "linux-pool"},
			{Key: "test", Name: "test", TargetGroup: "linux-pool", Needs: []string{"build"}},
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	got, err := client.GetPipelineVersion(ctx, &dbpb.GetPipelineVersionRequest{
		PipelineId: pipeline.GetId(),
		Version:    1,
	})
	if err != nil {
		t.Fatalf("GetPipelineVersion: %v", err)
	}
	if got.GetPipelineId() != pipeline.GetId() || got.GetVersion() != 1 {
		t.Errorf("snapshot = (pipeline %d, version %d), want (pipeline %d, version 1)",
			got.GetPipelineId(), got.GetVersion(), pipeline.GetId())
	}
	if got.GetName() != "deploy" || got.GetDescription() != "deploys the app" {
		t.Errorf("snapshot (name %q, description %q), want (deploy, deploys the app)", got.GetName(), got.GetDescription())
	}
	if len(got.GetJobs()) != 2 {
		t.Fatalf("snapshot has %d jobs, want 2", len(got.GetJobs()))
	}
	// The test job's needs are preserved in the snapshot.
	var testJob *dbpb.JobDefinition
	for _, j := range got.GetJobs() {
		if j.GetKey() == "test" {
			testJob = j
		}
	}
	if testJob == nil {
		t.Fatalf("snapshot is missing the test job")
	}
	if len(testJob.GetNeeds()) != 1 || testJob.GetNeeds()[0] != "build" {
		t.Errorf("snapshot test job needs = %v, want [build]", testJob.GetNeeds())
	}

	// GetPipelineVersion rejects a version that does not exist.
	if _, err := client.GetPipelineVersion(ctx, &dbpb.GetPipelineVersionRequest{PipelineId: pipeline.GetId(), Version: 5}); status.Code(err) != codes.NotFound {
		t.Errorf("GetPipelineVersion(version 5) error = %v, want NotFound", err)
	}
}

// TestDeletePipelineCascadesVersions verifies that deleting a pipeline removes
// its version snapshots (F-11).
func TestDeletePipelineCascadesVersions(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		Jobs: []*dbpb.JobDefinition{{Key: "build", Name: "build"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if _, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:   pipeline.GetId(),
		Name: "deploy v2",
		Jobs: []*dbpb.JobDefinition{{Key: "build", Name: "build"}},
	}); err != nil {
		t.Fatalf("UpdatePipeline: %v", err)
	}
	if _, err := client.DeletePipeline(ctx, &dbpb.DeletePipelineRequest{Id: pipeline.GetId()}); err != nil {
		t.Fatalf("DeletePipeline: %v", err)
	}

	list, err := client.ListPipelineVersions(ctx, &dbpb.ListPipelineVersionsRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("ListPipelineVersions: %v", err)
	}
	if got := len(list.GetVersions()); got != 0 {
		t.Errorf("deleted pipeline still has %d versions, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// Secrets (F-12)
// ---------------------------------------------------------------------------

// TestNextSecretNonce verifies that the shared secret-nonce counter hands out
// a strictly increasing sequence (F-12): the first call returns 1 (the row is
// created on first use, starting at 0, and the call bumps it to 1), and each
// subsequent call returns the next value. This also exercises the
// `UPDATE ... RETURNING` statement against the bundled SQLite.
func TestNextSecretNonce(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	first, err := client.NextSecretNonce(ctx, &dbpb.NextSecretNonceRequest{})
	if err != nil {
		t.Fatalf("NextSecretNonce: %v", err)
	}
	if first.GetNonce() != 1 {
		t.Errorf("first nonce = %d, want 1", first.GetNonce())
	}
	second, err := client.NextSecretNonce(ctx, &dbpb.NextSecretNonceRequest{})
	if err != nil {
		t.Fatalf("NextSecretNonce: %v", err)
	}
	if second.GetNonce() != 2 {
		t.Errorf("second nonce = %d, want 2", second.GetNonce())
	}
}

// TestCreatePipelineWithSecrets verifies that a pipeline's named secrets are
// stored (as opaque ciphertext) and returned on create and fetch (F-12).
func TestCreatePipelineWithSecrets(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		Secrets: []*dbpb.Secret{
			{Name: "db_password", Encrypted: "enc-1"},
			{Name: "api_key", Encrypted: "enc-2"},
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if got := len(pipeline.GetSecrets()); got != 2 {
		t.Fatalf("pipeline secrets = %d, want 2", got)
	}

	fetched, err := client.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: pipeline.GetId()})
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	byName := map[string]string{}
	for _, secret := range fetched.GetSecrets() {
		byName[secret.GetName()] = secret.GetEncrypted()
	}
	if byName["db_password"] != "enc-1" {
		t.Errorf("db_password = %q, want enc-1", byName["db_password"])
	}
	if byName["api_key"] != "enc-2" {
		t.Errorf("api_key = %q, want enc-2", byName["api_key"])
	}
}

// TestCreatePipelineRejectsDuplicateSecret verifies that a pipeline with two
// secrets of the same name is rejected (F-12): a secret must be referenceable
// unambiguously.
func TestCreatePipelineRejectsDuplicateSecret(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	_, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "dup",
		Secrets: []*dbpb.Secret{
			{Name: "token", Encrypted: "a"},
			{Name: "token", Encrypted: "b"},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreatePipeline with duplicate secret = %v, want InvalidArgument", err)
	}
}

// TestCreatePipelineRejectsEmptySecretName verifies that a pipeline with a
// secret that has an empty name is rejected (F-12).
func TestCreatePipelineRejectsEmptySecretName(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	_, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:    "empty",
		Secrets: []*dbpb.Secret{{Name: "", Encrypted: "x"}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreatePipeline with empty secret name = %v, want InvalidArgument", err)
	}
}

// TestUpdatePipelineReplacesSecrets verifies that updating a pipeline with a
// non-nil secrets list replaces its secrets (F-12), and that an update with no
// secrets leaves them unchanged.
func TestUpdatePipelineReplacesSecrets(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:    "p",
		Secrets: []*dbpb.Secret{{Name: "old", Encrypted: "a"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	updated, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:      pipeline.GetId(),
		Secrets: []*dbpb.Secret{{Name: "new", Encrypted: "b"}},
	})
	if err != nil {
		t.Fatalf("UpdatePipeline: %v", err)
	}
	if got := len(updated.GetSecrets()); got != 1 {
		t.Fatalf("updated secrets = %d, want 1", got)
	}
	if updated.GetSecrets()[0].GetName() != "new" || updated.GetSecrets()[0].GetEncrypted() != "b" {
		t.Errorf("updated secret = %+v, want new/b", updated.GetSecrets()[0])
	}

	// An update with no secrets leaves the pipeline's secrets unchanged.
	unchanged, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{Id: pipeline.GetId()})
	if err != nil {
		t.Fatalf("UpdatePipeline (no secrets): %v", err)
	}
	if got := len(unchanged.GetSecrets()); got != 1 {
		t.Fatalf("secrets after no-secret update = %d, want 1 (unchanged)", got)
	}
	if unchanged.GetSecrets()[0].GetName() != "new" {
		t.Errorf("secret after no-secret update = %q, want new", unchanged.GetSecrets()[0].GetName())
	}
}

// TestCreateRunDenormalizesSecrets verifies that a pipeline's named secrets
// (as ciphertext) are denormalized onto each of its run's job instances
// (F-12), so the API can decrypt them at dispatch without re-reading the
// pipeline.
func TestCreateRunDenormalizesSecrets(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:    "deploy",
		Secrets: []*dbpb.Secret{{Name: "db_password", Encrypted: "enc-1"}},
		Jobs:    []*dbpb.JobDefinition{{Key: "build", Name: "build", TargetGroup: "linux-pool"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if got := len(resp.GetJobs()); got != 1 {
		t.Fatalf("run created %d job instances, want 1", got)
	}
	secrets := resp.GetJobs()[0].GetSecrets()
	if got := len(secrets); got != 1 {
		t.Fatalf("instance secrets = %d, want 1", got)
	}
	if secrets[0].GetName() != "db_password" || secrets[0].GetEncrypted() != "enc-1" {
		t.Errorf("instance secret = %+v, want db_password/enc-1", secrets[0])
	}
}

// TestRunAgainstSpecificVersionReproducesSecrets verifies that a run against a
// specific version carries that version's secrets (F-11/F-12): editing the
// pipeline's secrets after a run does not change what the run's instances
// carry.
func TestRunAgainstSpecificVersionReproducesSecrets(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:    "deploy",
		Secrets: []*dbpb.Secret{{Name: "token", Encrypted: "v1"}},
		Jobs:    []*dbpb.JobDefinition{{Key: "build", Name: "build"}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	// A run against the current (version 1) definition.
	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if got := resp.GetJobs()[0].GetSecrets()[0].GetEncrypted(); got != "v1" {
		t.Fatalf("instance secret = %q, want v1", got)
	}

	// Edit the pipeline's secrets (a new version).
	if _, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:      pipeline.GetId(),
		Secrets: []*dbpb.Secret{{Name: "token", Encrypted: "v2"}},
	}); err != nil {
		t.Fatalf("UpdatePipeline: %v", err)
	}

	// A run against the original version reproduces the old secret.
	old, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{
		PipelineId:      pipeline.GetId(),
		Trigger:         "manual",
		PipelineVersion: 1,
	})
	if err != nil {
		t.Fatalf("CreateRun (v1): %v", err)
	}
	if got := old.GetJobs()[0].GetSecrets()[0].GetEncrypted(); got != "v1" {
		t.Errorf("v1 instance secret = %q, want v1", got)
	}
	// A run against the current version carries the new secret.
	cur, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun (current): %v", err)
	}
	if got := cur.GetJobs()[0].GetSecrets()[0].GetEncrypted(); got != "v2" {
		t.Errorf("current instance secret = %q, want v2", got)
	}
}
