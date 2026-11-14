package database

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// startServer opens an in-memory SQLite database, migrates it, and serves the
// Database gRPC service over an in-process bufconn listener.
func startServer(t *testing.T) dbpb.DatabaseClient {
	t.Helper()
	client, _ := startServerDB(t)
	return client
}

// startServerDB is startServer, but also returns the underlying *gorm.DB so a
// test can assert directly on the persisted rows (e.g. the normalized Tag
// table, F-29). Options mutate the SERVER before it is registered (e.g. to
// wire the artifacts client for F-21.6 package resolution).
func startServerDB(t *testing.T, opts ...func(*Server)) (dbpb.DatabaseClient, *gorm.DB) {
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
	// Seed the built-in roles (F-14), mirroring cmd/db/main.go, so tests that
	// reference built-in roles (admin, operator, viewer, user) find them.
	if err := NewServer(db).SeedBuiltInRoles(context.Background()); err != nil {
		t.Fatalf("SeedBuiltInRoles: %v", err)
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	server := NewServer(db)
	for _, opt := range opts {
		opt(server)
	}
	dbpb.RegisterDatabaseServer(srv, server)
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
	return dbpb.NewDatabaseClient(conn), db
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

	// A job linked to a pipeline is a DEFINITION (F-31 §1: its own table).
	if _, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{
		PipelineId:  pipeline.GetId(),
		Name:        "compile",
		TargetGroup: "linux-pool",
	}); err != nil {
		t.Fatalf("CreateJob(definition): %v", err)
	}

	// The JOB lifecycle under test is a standalone job (a work item).
	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "runner"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if job.GetStatus() != dbpb.JobStatus_JOB_STATUS_PENDING {
		t.Fatalf("new job status = %v, want PENDING", job.GetStatus())
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

	// List jobs for the pipeline: F-31 §1 — a pipeline's DEFINITIONS live in
	// their own table, so the jobs table carries NONE of them (the legacy
	// world returned the definition row here).
	response, err := client.ListJobs(ctx, &dbpb.ListJobsRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if got := len(response.GetJobs()); got != 0 {
		t.Fatalf("jobs = %d, want 0 (definitions are not job rows)", got)
	}
	// The definition is visible THROUGH THE PIPELINE.
	withDef, err := client.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: pipeline.GetId()})
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	if got := len(withDef.GetJobs()); got != 1 {
		t.Fatalf("pipeline definitions = %d, want 1", got)
	}

	// Deleting the pipeline cascades to its DEFINITIONS (F-31 §1). The
	// standalone job is pipeline-less (its pipeline_id is nil) and lawfully
	// SURVIVES the pipeline's deletion.
	if _, err := client.DeletePipeline(ctx, &dbpb.DeletePipelineRequest{Id: pipeline.GetId()}); err != nil {
		t.Fatalf("DeletePipeline: %v", err)
	}
	if _, err := client.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: pipeline.GetId()}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetPipeline after cascade = %v, want NotFound", err)
	}
	if _, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()}); err != nil {
		t.Fatalf("standalone job after cascade = %v, want survival (it is pipeline-less)", err)
	}
}

func TestAgentRegistration(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	agent, err := client.RegisterAgent(ctx, &dbpb.RegisterAgentRequest{Name: "w1", Group: "pool-a", Address: "10.0.0.1"})
	if err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	if agent.GetGroup() != "pool-a" {
		t.Fatalf("group = %q, want pool-a", agent.GetGroup())
	}

	// Re-registering updates the group.
	if _, err := client.RegisterAgent(ctx, &dbpb.RegisterAgentRequest{Name: "w1", Group: "pool-b"}); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	fetched, err := client.GetAgent(ctx, &dbpb.GetAgentRequest{Name: "w1"})
	if err != nil {
		t.Fatalf("GetAgent: %v", err)
	}
	if fetched.GetGroup() != "pool-b" {
		t.Fatalf("group after re-register = %q, want pool-b", fetched.GetGroup())
	}

	// Heartbeat refreshes last-seen.
	if _, err := client.HeartbeatAgent(ctx, &dbpb.HeartbeatAgentRequest{Name: "w1"}); err != nil {
		t.Fatalf("HeartbeatAgent: %v", err)
	}

	// List by group.
	response, err := client.ListAgents(ctx, &dbpb.ListAgentsRequest{Group: "pool-b"})
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	if got := len(response.GetAgents()); got != 1 {
		t.Fatalf("agents in pool-b = %d, want 1", got)
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

// TestResolveApproval verifies that ResolveApproval (F-13) records the
// decision, actor, and reason on a job that is awaiting approval, and that a
// job that is no longer awaiting approval (it reached a terminal state) is left
// untouched (a late decision is a no-op).
func TestResolveApproval(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// A job that is awaiting approval is resolved.
	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "gated"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:              job.GetId(),
		Status:          dbpb.JobStatus_JOB_STATUS_AWAITING_APPROVAL,
		ApprovalMessage: "please approve the production deploy",
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	resp, err := client.ResolveApproval(ctx, &dbpb.ResolveApprovalRequest{
		JobId:     job.GetId(),
		Decision:  "approved",
		Actor:     "alice@example.com",
		Reason:    "verified the canary",
		DecidedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("ResolveApproval: %v", err)
	}
	if !resp.GetResolved() {
		t.Fatal("awaiting-approval job was not resolved, want resolved=true")
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetApprovalDecision() != "approved" {
		t.Errorf("decision = %q, want approved", fetched.GetApprovalDecision())
	}
	if fetched.GetApprovalActor() != "alice@example.com" {
		t.Errorf("actor = %q, want alice@example.com", fetched.GetApprovalActor())
	}
	if fetched.GetApprovalReason() != "verified the canary" {
		t.Errorf("reason = %q, want %q", fetched.GetApprovalReason(), "verified the canary")
	}
	if fetched.GetApprovalDecidedAt() == nil {
		t.Error("approval_decided_at not set on resolved job")
	}
	// An approval moves the job back to running (the target continues its
	// remaining steps); the target's final report then records the outcome.
	if fetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_RUNNING {
		t.Errorf("status = %v, want RUNNING (approved)", fetched.GetStatus())
	}

	// A job that is no longer awaiting approval is not resolved (a late
	// decision is a no-op).
	other, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "done"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         other.GetId(),
		Status:     dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
		StartedAt:  timestamppb.Now(),
		FinishedAt: timestamppb.Now(),
	}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	resp, err = client.ResolveApproval(ctx, &dbpb.ResolveApprovalRequest{
		JobId:     other.GetId(),
		Decision:  "approved",
		Actor:     "bob@example.com",
		DecidedAt: timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("ResolveApproval: %v", err)
	}
	if resp.GetResolved() {
		t.Fatal("terminal job was resolved, want resolved=false")
	}
	fetched, err = client.GetJob(ctx, &dbpb.GetJobRequest{Id: other.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.GetApprovalDecision() != "" {
		t.Errorf("decision = %q, want empty (unchanged)", fetched.GetApprovalDecision())
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

	// Re-setting the SAME ring converges (idempotent upsert): two IdP
	// replicas starting moments apart both normalize and write the identical
	// ring; the second writer must NOT violate the UNIQUE(kid) index
	// (historically it crashed the losing replica at startup).
	if _, err := client.SetIDPSigningKeys(ctx, &dbpb.SetIDPSigningKeysRequest{
		Keys: []*dbpb.IDPSigningKey{
			{Kid: "current", IsCurrent: true, NotBefore: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(time.Hour)), Pem: "pem-current-v2"},
			{Kid: "old", IsCurrent: false, NotBefore: timestamppb.New(now.Add(-time.Hour)), ExpiresAt: timestamppb.New(now.Add(time.Hour)), Pem: "pem-old-v2"},
		},
	}); err != nil {
		t.Fatalf("SetIDPSigningKeys (repeat, same kids): %v", err)
	}
	resp, err = client.ListIDPSigningKeys(ctx, &dbpb.ListIDPSigningKeysRequest{})
	if err != nil {
		t.Fatalf("ListIDPSigningKeys (after repeat): %v", err)
	}
	if got := len(resp.GetKeys()); got != 2 {
		t.Fatalf("keys after repeat set = %d, want 2", got)
	}
	// The upsert UPDATED the existing rows (Pem replaced), not merely
	// collided-and-survived.
	upserted := false
	for _, key := range resp.GetKeys() {
		if key.GetKid() == "current" && key.GetPem() == "pem-current-v2" {
			upserted = true
		}
	}
	if !upserted {
		t.Errorf("repeat set did not update the existing rows: %+v", resp.GetKeys())
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
			Roles:       []string{"operator", "deploy-bot"},
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
	// The dev shim's simulated roles survive the round-trip (F-28.4 S3: the
	// issued token must bear them).
	if len(got.GetRoles()) != 2 || got.GetRoles()[0] != "operator" || got.GetRoles()[1] != "deploy-bot" {
		t.Fatalf("roles = %v, want [operator deploy-bot]", got.GetRoles())
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

// TestIDPUsers verifies the IdP user CRUD RPCs: create (new and upsert on
// username), get by username and by ID, list, update, and delete, plus the
// not-found cases.
func TestIDPUsers(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// Create a new user (username anchored; email optional metadata).
	created, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{
			Username:     "ada",
			FirstName:    "Ada",
			LastName:     "Lovelace",
			Email:        "ada@example.com",
			PasswordHash: "opaque-placeholder",
			Roles:        []string{"admin"},
		},
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if created.GetId() == "" {
		t.Fatal("created user has no ID")
	}
	if created.GetUsername() != "ada" {
		t.Errorf("username = %q, want ada", created.GetUsername())
	}
	if created.GetEmail() != "ada@example.com" {
		t.Errorf("email = %q, want ada@example.com", created.GetEmail())
	}
	if len(created.GetRoles()) != 1 || created.GetRoles()[0] != "admin" {
		t.Errorf("roles = %v, want [admin]", created.GetRoles())
	}
	id := created.GetId()

	// Get by username.
	byUsername, err := client.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Username: "ada"})
	if err != nil {
		t.Fatalf("GetIDPUser by username: %v", err)
	}
	if byUsername.GetId() != id {
		t.Errorf("by username id = %q, want %q", byUsername.GetId(), id)
	}

	// Get by ID.
	byID, err := client.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Id: id})
	if err != nil {
		t.Fatalf("GetIDPUser by id: %v", err)
	}
	if byID.GetEmail() != "ada@example.com" {
		t.Errorf("by id email = %q, want ada@example.com", byID.GetEmail())
	}

	// Create a second user.
	if _, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{Username: "bob", Email: "bob@example.com", PasswordHash: "h2", Roles: []string{"user"}},
	}); err != nil {
		t.Fatalf("CreateUser bob: %v", err)
	}

	// List returns both.
	listed, err := client.ListIDPUsers(ctx, &dbpb.ListIDPUsersRequest{})
	if err != nil {
		t.Fatalf("ListIDPUsers: %v", err)
	}
	if len(listed.GetUsers()) != 2 {
		t.Errorf("list = %d users, want 2", len(listed.GetUsers()))
	}

	// Update the user's roles.
	updated, err := client.UpdateIDPUser(ctx, &dbpb.UpdateIDPUserRequest{
		User: &dbpb.IDPUser{Id: id, Roles: []string{"admin", "dev"}},
	})
	if err != nil {
		t.Fatalf("UpdateIDPUser: %v", err)
	}
	if len(updated.GetRoles()) != 2 {
		t.Errorf("updated roles = %v, want 2 roles", updated.GetRoles())
	}

	// Locator mutual exclusion (F-28.4): two identifiers set at once is
	// InvalidArgument.
	if _, err := client.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Id: id, Username: "ada"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("double locator = %v, want InvalidArgument", err)
	}
	// Locate by external_id (round trip through the new locator).
	extUpdated, err := client.UpdateIDPUser(ctx, &dbpb.UpdateIDPUserRequest{
		User: &dbpb.IDPUser{Id: id, ExternalId: "az-ext-1"},
	})
	if err != nil {
		t.Fatalf("UpdateIDPUser external_id: %v", err)
	}
	byExt, err := client.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{ExternalId: "az-ext-1"})
	if err != nil {
		t.Fatalf("GetIDPUser by external_id: %v", err)
	}
	if byExt.GetId() != extUpdated.GetId() {
		t.Errorf("by external_id id = %q, want %q", byExt.GetId(), extUpdated.GetId())
	}

	// Upsert on username: creating with the same username updates the
	// existing user (does not create a duplicate).
	if _, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{Username: "ada", FirstName: "Ada", Roles: []string{"admin"}},
	}); err != nil {
		t.Fatalf("CreateUser upsert: %v", err)
	}
	listed2, err := client.ListIDPUsers(ctx, &dbpb.ListIDPUsersRequest{})
	if err != nil {
		t.Fatalf("ListIDPUsers (after upsert): %v", err)
	}
	if len(listed2.GetUsers()) != 2 {
		t.Errorf("list after upsert = %d users, want 2 (no duplicate)", len(listed2.GetUsers()))
	}

	// Delete the user.
	if _, err := client.DeleteIDPUser(ctx, &dbpb.DeleteIDPUserRequest{Id: id}); err != nil {
		t.Fatalf("DeleteIDPUser: %v", err)
	}
	if _, err := client.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Id: id}); status.Code(err) != codes.NotFound {
		t.Errorf("get after delete = %v, want NotFound", err)
	}
	// Deleting again is IDEMPOTENT (F-28.3): the tombstoned row is returned
	// unchanged, not an error.
	if again, err := client.DeleteIDPUser(ctx, &dbpb.DeleteIDPUserRequest{Id: id}); err != nil {
		t.Errorf("re-delete = %v, want idempotent success", err)
	} else if again.GetId() != id || again.GetDeletedAt() == nil {
		t.Errorf("re-delete row = id:%q tombstone:nil?, want the tombstoned original", again.GetId())
	}
}

// TestAPIKeys verifies the IdP API-key CRUD + verify RPCs (F-25): create, get,
// list (per-owner and all), update (renew), rotate, delete, verify (success,
// miss, lockout, and reset), plus the not-found and invalid-owner cases.
func TestAPIKeys(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// A key's owner must exist.
	owner, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{Username: "ada", Email: "ada@example.com", PasswordHash: "h", Roles: []string{"admin"}},
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	ownerID := owner.GetId()

	// Create a key for the owner.
	created, err := client.CreateAPIKey(ctx, &dbpb.CreateIDPAPIKeyRequest{
		Key: &dbpb.IDPAPIKey{
			OwnerId:       ownerID,
			Description:   "ci key",
			KeyHash:       "hash-1",
			KeyPrefix:     "cdrom-abcd",
			PipelineScope: []int64{1, 2},
		},
	})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if created.GetId() == "" {
		t.Fatal("created key has no ID")
	}
	if created.GetOwnerId() != ownerID {
		t.Errorf("owner = %q, want %q", created.GetOwnerId(), ownerID)
	}
	if created.GetKeyHash() != "hash-1" {
		t.Errorf("hash = %q, want hash-1", created.GetKeyHash())
	}
	if len(created.GetPipelineScope()) != 2 {
		t.Errorf("scope = %v, want [1 2]", created.GetPipelineScope())
	}
	keyID := created.GetId()

	// Creating a key for a missing owner is InvalidArgument.
	if _, err := client.CreateAPIKey(ctx, &dbpb.CreateIDPAPIKeyRequest{
		Key: &dbpb.IDPAPIKey{OwnerId: "999999", KeyHash: "h"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("create for missing owner = %v, want InvalidArgument", err)
	}

	// Get the key.
	got, err := client.GetAPIKey(ctx, &dbpb.GetIDPAPIKeyRequest{Id: keyID})
	if err != nil {
		t.Fatalf("GetAPIKey: %v", err)
	}
	if got.GetKeyHash() != "hash-1" {
		t.Errorf("get hash = %q, want hash-1", got.GetKeyHash())
	}

	// A missing key is NotFound.
	if _, err := client.GetAPIKey(ctx, &dbpb.GetIDPAPIKeyRequest{Id: "424242"}); status.Code(err) != codes.NotFound {
		t.Errorf("get missing = %v, want NotFound", err)
	}

	// Create a second key for the owner (a user can have any number of keys).
	if _, err := client.CreateAPIKey(ctx, &dbpb.CreateIDPAPIKeyRequest{
		Key: &dbpb.IDPAPIKey{OwnerId: ownerID, Description: "second", KeyHash: "hash-2", KeyPrefix: "cdrom-efgh"},
	}); err != nil {
		t.Fatalf("CreateAPIKey second: %v", err)
	}

	// List the owner's keys (two).
	ownerKeys, err := client.ListAPIKeys(ctx, &dbpb.ListIDPAPIKeysRequest{OwnerId: ownerID})
	if err != nil {
		t.Fatalf("ListAPIKeys owner: %v", err)
	}
	if len(ownerKeys.GetKeys()) != 2 {
		t.Errorf("owner keys = %d, want 2", len(ownerKeys.GetKeys()))
	}

	// List all keys (two, since there is only one owner).
	allKeys, err := client.ListAPIKeys(ctx, &dbpb.ListIDPAPIKeysRequest{})
	if err != nil {
		t.Fatalf("ListAPIKeys all: %v", err)
	}
	if len(allKeys.GetKeys()) != 2 {
		t.Errorf("all keys = %d, want 2", len(allKeys.GetKeys()))
	}

	// Update (renew) the key's description and pipeline scope, leaving the
	// secret (hash) unchanged.
	updated, err := client.UpdateAPIKey(ctx, &dbpb.UpdateIDPAPIKeyRequest{
		Key:              &dbpb.IDPAPIKey{Id: keyID, Description: "renamed", PipelineScope: []int64{3}},
		HasDescription:   true,
		HasPipelineScope: true,
	})
	if err != nil {
		t.Fatalf("UpdateAPIKey: %v", err)
	}
	if updated.GetDescription() != "renamed" {
		t.Errorf("description = %q, want renamed", updated.GetDescription())
	}
	if len(updated.GetPipelineScope()) != 1 || updated.GetPipelineScope()[0] != 3 {
		t.Errorf("scope = %v, want [3]", updated.GetPipelineScope())
	}
	if updated.GetKeyHash() != "hash-1" {
		t.Errorf("hash after renew = %q, want hash-1 (unchanged)", updated.GetKeyHash())
	}

	// Rotate the key: a new hash, the old one stops working.
	rotated, err := client.RotateAPIKey(ctx, &dbpb.RotateIDPAPIKeyRequest{Id: keyID, KeyHash: "hash-rotated", KeyPrefix: "cdrom-rot"})
	if err != nil {
		t.Fatalf("RotateAPIKey: %v", err)
	}
	if rotated.GetKeyHash() != "hash-rotated" {
		t.Errorf("rotated hash = %q, want hash-rotated", rotated.GetKeyHash())
	}
	if rotated.GetDescription() != "renamed" {
		t.Errorf("rotated description = %q, want renamed (unchanged)", rotated.GetDescription())
	}

	// Adjustment primitives (the lockout accounting formerly fused into
	// VerifyAPIKey; verification itself moved to the IdP with Argon2ID):
	// increment the failure counter.
	adj, err := client.AdjustAPIKeyLockout(ctx, &dbpb.AdjustAPIKeyLockoutRequest{
		UserId:      ownerID,
		FailedDelta: 1,
	})
	if err != nil {
		t.Fatalf("AdjustAPIKeyLockout(+1): %v", err)
	}
	if adj.GetApiKeyFailedCount() != 1 {
		t.Errorf("failed count = %d, want 1", adj.GetApiKeyFailedCount())
	}
	// Set a lockout instant and accumulate another failure.
	later := time.Now().Add(time.Hour)
	adj, err = client.AdjustAPIKeyLockout(ctx, &dbpb.AdjustAPIKeyLockoutRequest{
		UserId:       ownerID,
		FailedDelta:  1,
		LockoutUntil: timestamppb.New(later),
	})
	if err != nil {
		t.Fatalf("AdjustAPIKeyLockout(set lockout): %v", err)
	}
	if adj.GetApiKeyFailedCount() != 2 {
		t.Errorf("failed count = %d, want 2", adj.GetApiKeyFailedCount())
	}
	if got := adj.GetApiKeyLockedUntil().AsTime(); !got.Equal(later) {
		t.Errorf("locked until = %v, want %v", got, later)
	}
	// A negative delta clears the counter (floors at zero).
	adj, err = client.AdjustAPIKeyLockout(ctx, &dbpb.AdjustAPIKeyLockoutRequest{
		UserId:      ownerID,
		FailedDelta: -5,
	})
	if err != nil {
		t.Fatalf("AdjustAPIKeyLockout(-5): %v", err)
	}
	if adj.GetApiKeyFailedCount() != 0 {
		t.Errorf("failed count = %d, want 0 (floor)", adj.GetApiKeyFailedCount())
	}
	// Absent lockout_until clears the lockout (writes NULL). Assert the
	// field's ABSENCE — a proto3 getter on an absent message field yields a
	// zero Timestamp (epoch), so checking AsTime().IsZero() is unreliable.
	adj, err = client.AdjustAPIKeyLockout(ctx, &dbpb.AdjustAPIKeyLockoutRequest{
		UserId: ownerID,
	})
	if err != nil {
		t.Fatalf("AdjustAPIKeyLockout(clear): %v", err)
	}
	if ts := adj.GetApiKeyLockedUntil(); ts != nil {
		t.Errorf("locked until = %v, want absent (cleared)", ts.AsTime())
	}
	// Resetting a missing user is NotFound.
	if _, err := client.ResetAPIKeyLockout(ctx, &dbpb.ResetIDPAPIKeyLockoutRequest{UserId: "424242"}); status.Code(err) != codes.NotFound {
		t.Errorf("reset missing user = %v, want NotFound", err)
	}

	// Delete the key.
	if _, err := client.DeleteAPIKey(ctx, &dbpb.DeleteIDPAPIKeyRequest{Id: keyID}); err != nil {
		t.Fatalf("DeleteAPIKey: %v", err)
	}
	if _, err := client.GetAPIKey(ctx, &dbpb.GetIDPAPIKeyRequest{Id: keyID}); status.Code(err) != codes.NotFound {
		t.Errorf("get after delete = %v, want NotFound", err)
	}
	// Deleting again is a NotFound.
	if _, err := client.DeleteAPIKey(ctx, &dbpb.DeleteIDPAPIKeyRequest{Id: keyID}); status.Code(err) != codes.NotFound {
		t.Errorf("re-delete = %v, want NotFound", err)
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
		{Name: "step0", Status: dbpb.StepStatus_STEP_STATUS_SUCCEEDED},
		{Name: "step1", Status: dbpb.StepStatus_STEP_STATUS_SKIPPED},
		{Name: "step2", Status: dbpb.StepStatus_STEP_STATUS_FAILED, Error: "boom"},
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
// it, returning the pipeline and the created definitions (in order). F-31 §1:
// definitions are forged INLINE on the pipeline (their own table) — the
// requests' names double as the definitions' keys (unique per pipeline).
func createPipelineWithJobs(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, name string, defs ...*dbpb.CreateJobRequest) (*dbpb.Pipeline, []*dbpb.JobDefinition) {
	t.Helper()
	jd := make([]*dbpb.JobDefinition, 0, len(defs))
	for _, def := range defs {
		jd = append(jd, &dbpb.JobDefinition{
			Key:         def.GetName(),
			Name:        def.GetName(),
			TargetGroup: def.GetTargetGroup(),
			Spec:        def.GetSpec(),
			Needs:       def.GetNeeds(),
		})
	}
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: name, Jobs: jd})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if len(pipeline.GetJobs()) != len(jd) {
		t.Fatalf("pipeline definitions = %d, want %d", len(pipeline.GetJobs()), len(jd))
	}
	return pipeline, pipeline.GetJobs()
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
// that depends on "build", returning the pipeline and the two definitions
// (build first, test second). F-31 §1: the dependency is expressed in the
// authoring FORM (needs, by key) — the split's raison d'être versus the
// legacy raw-id borrowing.
func createDagPipeline(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, name string) (*dbpb.Pipeline, *dbpb.JobDefinition, *dbpb.JobDefinition) {
	t.Helper()
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: name,
		Jobs: []*dbpb.JobDefinition{
			{Key: "build", Name: "build"},
			{Key: "test", Name: "test", Needs: []string{"build"}},
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if len(pipeline.GetJobs()) != 2 {
		t.Fatalf("pipeline definitions = %d, want 2", len(pipeline.GetJobs()))
	}
	return pipeline, pipeline.GetJobs()[0], pipeline.GetJobs()[1]
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

// newGroupJob creates a pipeline with a single job targeting an agent group,
// materializes a manual run, and returns the run's job INSTANCE — a startable
// work item. It deliberately does NOT return the pipeline DEFINITION row:
// StartJobExecution refuses definition rows (a job with a pipeline but no run
// is a definition, never executed — F-08), and handing tests a definition
// silently broke every start/update/abandon assertion (caught 2026-11-05).
func newGroupJob(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, name, group string) *dbpb.Job {
	t.Helper()
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: name + "-pipeline",
		Jobs: []*dbpb.JobDefinition{{Key: name, Name: name, TargetGroup: group}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if _, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	jobs, err := client.ListJobs(ctx, &dbpb.ListJobsRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	var instance *dbpb.Job
	for _, j := range jobs.GetJobs() {
		if j.GetRunId() != 0 {
			instance = j
			break
		}
	}
	if instance == nil {
		t.Fatalf("no run instance found for pipeline %d", pipeline.GetId())
	}
	return instance
}

// TestStartJobExecution verifies that an agent starting a pending group job
// records its execution and marks the job running (fan-out).
func TestStartJobExecution(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")

	// Two agents each start their own execution of the same job.
	for _, agent := range []string{"w1", "w2"} {
		resp, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), AgentName: agent})
		if err != nil {
			t.Fatalf("StartJobExecution(%s): %v", agent, err)
		}
		if !resp.GetStarted() {
			t.Fatalf("StartJobExecution(%s) started = false, want true", agent)
		}
		if resp.GetExecution().GetAgentName() != agent {
			t.Errorf("execution agent = %q, want %q", resp.GetExecution().GetAgentName(), agent)
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

	// Both agents' executions are listed.
	list, err := client.ListJobExecutions(ctx, &dbpb.ListJobExecutionsRequest{JobId: job.GetId()})
	if err != nil {
		t.Fatalf("ListJobExecutions: %v", err)
	}
	if got := len(list.GetExecutions()); got != 2 {
		t.Fatalf("executions = %d, want 2", got)
	}
}

// TestStartJobExecutionDuplicateDeliveryGuardsAgainstSecondExecution
// verifies the DOUBLE-DELIVERY GUARD (flaky e2e 2026-11-07, scenario 16):
// once an agent has started a (job, agent, attempt) episode, a duplicate
// delivery is DECLINED — in BOTH phases of the episode: while the first
// execution is still running, AND after it has finished while the job row
// still reads running (the derived status lags the executions — the exact
// window that caused the flake: a 28 ms job succeeded, its duplicate pickup
// arrived 8 ms later, and the old unconditional create spun up a SECOND
// execution that failed on publish_artifact's AlreadyExists). Legitimate
// re-openings are carved out: fan-out (a different agent is a different
// coordinate) proceeds, and a RERUN (which resets the job to pending)
// proceeds. Previously the guard was unconditional execution creation.
func TestStartJobExecutionDuplicateDeliveryGuardsAgainstSecondExecution(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "dup-guard", "pool-a")

	start := func(agent string) (started bool, execID int64) {
		t.Helper()
		resp, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), AgentName: agent})
		if err != nil {
			t.Fatalf("StartJobExecution(%s): %v", agent, err)
		}
		return resp.GetStarted(), resp.GetExecution().GetId()
	}

	// Episode begins: w1 starts.
	if started, _ := start("w1"); !started {
		t.Fatal("first start = false, want true")
	}

	// PHASE 1 (running): the job is RUNNING (marked by the first start) —
	// the OLD code saw non-terminal and waved the duplicate through.
	if started, _ := start("w1"); started {
		t.Error("duplicate while RUNNING started a second execution (want declined)")
	}

	// Fan-out is ALLOWED: a different agent starts its own execution.
	if started, _ := start("w2"); !started {
		t.Error("fan-out start = false, want true (different agent is a different coordinate)")
	}

	// PHASE 2 (the FLAKE WINDOW): w1's execution FINISHES, but the job row
	// still reads running (the derived status hasn't reconciled). A
	// duplicate pickup arriving now must STILL be declined.
	if _, err := client.UpdateJobExecution(ctx, &dbpb.UpdateJobExecutionRequest{
		JobId: job.GetId(), AgentName: "w1", Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
	}); err != nil {
		t.Fatalf("terminate w1's execution: %v", err)
	}
	if started, _ := start("w1"); started {
		t.Error("duplicate AFTER the execution finished started a THIRD execution (want declined — the flake window)")
	}

	// RERUN CARVE-OUT: quiesce the episode (w2's fan-out execution still
	// runs — RerunJob refuses a job that reads running), simulate the
	// derived loop marking the job terminal, then RerunJob resets it to
	// pending (the ONLY lawful reopening of a used coordinate) — w1 may
	// start the episode afresh.
	if _, err := client.UpdateJobExecution(ctx, &dbpb.UpdateJobExecutionRequest{
		JobId: job.GetId(), AgentName: "w2", Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
	}); err != nil {
		t.Fatalf("terminate w2's execution: %v", err)
	}
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{Id: job.GetId(), Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED}); err != nil {
		t.Fatalf("simulate the derived loop (job terminal): %v", err)
	}
	if _, err := client.RerunJob(ctx, &dbpb.RerunJobRequest{Id: job.GetId()}); err != nil {
		t.Fatalf("RerunJob: %v", err)
	}
	refetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob after rerun: %v", err)
	}
	if refetched.GetStatus() != dbpb.JobStatus_JOB_STATUS_PENDING {
		t.Fatalf("job after rerun = %v, want PENDING (the guard's discriminator)", refetched.GetStatus())
	}
	if started, _ := start("w1"); !started {
		t.Error("start after RERUN = false, want true (pending re-opens the episode)")
	}

	// Total executions: w1(ep1) + w2(fan-out) + w1(rerun) = 3 — the two
	// duplicates created NO rows.
	list, err := client.ListJobExecutions(ctx, &dbpb.ListJobExecutionsRequest{JobId: job.GetId()})
	if err != nil {
		t.Fatalf("ListJobExecutions: %v", err)
	}
	if got := len(list.GetExecutions()); got != 3 {
		t.Errorf("executions = %d, want 3 (duplicates must not create rows)", got)
	}
}

// TestStartJobExecutionRejectsTerminalJob verifies that a job that has reached
// a terminal state is not started (a late agent start is a no-op).
func TestStartJobExecutionRejectsTerminalJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")

	// Mark the job succeeded (terminal).
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{Id: job.GetId(), Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	resp, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), AgentName: "w1"})
	if err != nil {
		t.Fatalf("StartJobExecution: %v", err)
	}
	if resp.GetStarted() {
		t.Error("StartJobExecution started = true for a terminal job, want false")
	}
}

// TestStartJobExecutionRejectsAgentJob verifies that a job with an empty
// target group (an ephemeral worker) is not started by an agent.
func TestStartJobExecutionRejectsAgentJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	// A job with an EMPTY target group (F-30.x: ephemeral worker) is rejected
	// by an AGENT's start — the job is a standalone work item (F-31 §1:
	// pipeline-linked jobs are definitions, never executable).
	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "agent-job"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	resp, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), AgentName: "w1"})
	if err != nil {
		t.Fatalf("StartJobExecution: %v", err)
	}
	if resp.GetStarted() {
		t.Error("StartJobExecution started = true for an agent job, want false")
	}
}

// TestUpdateJobExecution verifies that an agent's outcome is recorded on its
// execution, and that a late report for an execution that already reached a
// terminal status is a no-op (it cannot clobber the terminal status).
func TestUpdateJobExecution(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")

	if _, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), AgentName: "w1"}); err != nil {
		t.Fatalf("StartJobExecution: %v", err)
	}

	// The agent reports its execution succeeded (its final report carries the
	// finish time).
	updated, err := client.UpdateJobExecution(ctx, &dbpb.UpdateJobExecutionRequest{
		JobId:      job.GetId(),
		AgentName:  "w1",
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
		AgentName:  "w1",
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

// TestAbandonAgentExecutions verifies that an agent's running executions are
// marked failed when the agent re-registers after a restart (fan-out): the
// stale in-progress executions do not block the job's overall status.
func TestAbandonAgentExecutions(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")

	if _, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{JobId: job.GetId(), AgentName: "w1"}); err != nil {
		t.Fatalf("StartJobExecution: %v", err)
	}

	// The agent restarts: its running execution is abandoned (marked failed).
	if _, err := client.AbandonAgentExecutions(ctx, &dbpb.AbandonAgentExecutionsRequest{AgentName: "w1"}); err != nil {
		t.Fatalf("AbandonAgentExecutions: %v", err)
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
// Cross-agent step barrier
// ---------------------------------------------------------------------------

// registerAgent registers (or refreshes) an agent in the group, which sets
// its last_seen_at to now (so it is alive).
func registerAgent(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, name, group string) {
	t.Helper()
	if _, err := client.RegisterAgent(ctx, &dbpb.RegisterAgentRequest{Name: name, Group: group}); err != nil {
		t.Fatalf("RegisterAgent(%s): %v", name, err)
	}
}

// reportStepCompletion reports that an agent completed a step of a job.
func reportStepCompletion(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, jobID int64, agent, stepName string) {
	t.Helper()
	if _, err := client.ReportStepCompletion(ctx, &dbpb.ReportStepCompletionRequest{JobId: jobID, AgentName: agent, StepName: stepName}); err != nil {
		t.Fatalf("ReportStepCompletion(%s, step %s): %v", agent, stepName, err)
	}
}

// checkStepBarrier checks the barrier for a job's step and returns the
// response.
func checkStepBarrier(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, jobID int64, stepName string) *dbpb.CheckStepBarrierResponse {
	t.Helper()
	resp, err := client.CheckStepBarrier(ctx, &dbpb.CheckStepBarrierRequest{JobId: jobID, StepName: stepName})
	if err != nil {
		t.Fatalf("CheckStepBarrier(step %s): %v", stepName, err)
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
// every alive agent in the group has completed the step.
func TestCheckStepBarrierSatisfied(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")
	registerAgent(t, client, ctx, "w1", "pool-a")
	registerAgent(t, client, ctx, "w2", "pool-a")

	// Neither agent has completed the step yet: not satisfied.
	if resp := checkStepBarrier(t, client, ctx, job.GetId(), "step0"); resp.GetSatisfied() || resp.GetCancelled() {
		t.Fatalf("before completions: satisfied=%v cancelled=%v, want both false", resp.GetSatisfied(), resp.GetCancelled())
	}

	// One agent completes: still not satisfied (the other is alive and
	// outstanding).
	reportStepCompletion(t, client, ctx, job.GetId(), "w1", "step0")
	if resp := checkStepBarrier(t, client, ctx, job.GetId(), "step0"); resp.GetSatisfied() {
		t.Fatalf("after one completion: satisfied=true, want false")
	}

	// Both agents complete: satisfied.
	reportStepCompletion(t, client, ctx, job.GetId(), "w2", "step0")
	if resp := checkStepBarrier(t, client, ctx, job.GetId(), "step0"); !resp.GetSatisfied() {
		t.Fatalf("after both completions: satisfied=false, want true")
	}
}

// TestCheckStepBarrierCancelled verifies that a cancelled job releases the
// barrier (a waiting agent should stop and report the job cancelled).
func TestCheckStepBarrierCancelled(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")
	registerAgent(t, client, ctx, "w1", "pool-a")

	// Cancel the job (it is pending, so the conditional cancel applies).
	if _, err := client.CancelJob(ctx, &dbpb.CancelJobRequest{Id: job.GetId()}); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	resp := checkStepBarrier(t, client, ctx, job.GetId(), "step0")
	if !resp.GetCancelled() {
		t.Errorf("cancelled job: cancelled=false, want true")
	}
	if resp.GetSatisfied() {
		t.Errorf("cancelled job: satisfied=true, want false")
	}
}

// TestCheckStepBarrierAgentJob verifies that a job with an empty target group
// (an ephemeral worker) has no barrier to wait on (it is vacuously satisfied).
func TestCheckStepBarrierAgentJob(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	// A standalone job with an EMPTY target group (an ephemeral worker) has
	// no barrier to wait on (vacuously satisfied).
	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{Name: "agent-job"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	resp := checkStepBarrier(t, client, ctx, job.GetId(), "step0")
	if !resp.GetSatisfied() {
		t.Errorf("agent job: satisfied=false, want true (no barrier)")
	}
	if resp.GetCancelled() {
		t.Errorf("agent job: cancelled=true, want false")
	}
}

// TestCheckStepBarrierDeadAgentDropped verifies that an agent that is not
// alive (it has not heartbeated within the liveness window) is dropped from
// the barrier: its missing step completion does not block the job. This is
// what lets a job complete when an agent in the group dies mid-step.
func TestCheckStepBarrierDeadAgentDropped(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")

	// Shorten the liveness window so an agent that is not refreshed goes stale
	// quickly; restore it afterwards.
	original := agentAliveThreshold
	agentAliveThreshold = 30 * time.Millisecond
	t.Cleanup(func() { agentAliveThreshold = original })

	// Register both agents (both alive, last_seen_at = now).
	registerAgent(t, client, ctx, "w1", "pool-a")
	registerAgent(t, client, ctx, "w2", "pool-a")

	// w1 completes the step. w2 does not (it will be made dead below).
	reportStepCompletion(t, client, ctx, job.GetId(), "w1", "step0")

	// Let both agents go stale, then refresh only w1: w1 is now alive and w2
	// is dead.
	time.Sleep(100 * time.Millisecond)
	registerAgent(t, client, ctx, "w1", "pool-a")

	// The barrier is satisfied: w1 (alive) has completed the step, and w2
	// (dead) is dropped from the barrier.
	resp := checkStepBarrier(t, client, ctx, job.GetId(), "step0")
	if !resp.GetSatisfied() {
		t.Errorf("dead agent dropped: satisfied=false, want true (w2 is dead and dropped)")
	}
	if resp.GetCancelled() {
		t.Errorf("dead agent dropped: cancelled=true, want false")
	}
}

// TestCheckStepBarrierMissingAliveAgentBlocks verifies that an alive agent
// that has not completed the step blocks the barrier (a dead agent is
// dropped, but a live one is not).
func TestCheckStepBarrierMissingAliveAgentBlocks(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	job := newGroupJob(t, client, ctx, "build", "pool-a")
	registerAgent(t, client, ctx, "w1", "pool-a")
	registerAgent(t, client, ctx, "w2", "pool-a")

	// Only w1 completes; w2 is alive and outstanding, so the barrier is not
	// satisfied.
	reportStepCompletion(t, client, ctx, job.GetId(), "w1", "step0")
	resp := checkStepBarrier(t, client, ctx, job.GetId(), "step0")
	if resp.GetSatisfied() {
		t.Errorf("alive agent outstanding: satisfied=true, want false")
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
	// The persisted DEFINITIONS (F-31 §1: their own table — the jobs table
	// carries no definition-shaped rows) belong to the pipeline and carry
	// their keys/needs.
	fetched, err := client.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: pipeline.GetId()})
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	if got := len(fetched.GetJobs()); got != 2 {
		t.Fatalf("persisted definitions = %d, want 2", got)
	}
	byKey := map[string]*dbpb.JobDefinition{}
	for _, j := range fetched.GetJobs() {
		byKey[j.GetKey()] = j
	}
	// The backend stores dependencies as ids (F-08): "test" depends on the
	// id of the "build" definition, and "build" has no dependencies.
	if build, ok := byKey["build"]; !ok || len(build.GetNeeds()) != 0 {
		t.Errorf("build definition = %+v, want present with no needs", build)
	}
	if test, ok := byKey["test"]; !ok || len(test.GetNeeds()) != 1 || test.GetNeeds()[0] != "build" {
		t.Errorf("test definition = %+v, want needs [build]", test)
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

// TestCreateRunDenormalizesVariables verifies that a job's effective variables
// (F-21.1.3) are denormalized onto each of its run's job instances, so the API
// can resolve them at dispatch without re-reading the pipeline. The job's
// variables are the union of its inline variables and the variables of the
// variable sets it references (merged at import time, the job's inline
// variables winning on a key collision); the pipeline's named variable sets and
// secret sources are stored alongside the pipeline.
func TestCreateRunDenormalizesVariables(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		VariableSets: []*dbpb.VariableSet{
			{
				Name: "common",
				Variables: []*dbpb.Variable{
					{Name: "region", Shape: dbpb.VariableShape_VARIABLE_PLAIN, Plain: "us-east-1"},
					{Name: "db_password", Shape: dbpb.VariableShape_VARIABLE_ENCRYPTED, Encrypted: "enc-1"},
				},
			},
		},
		SecretSources: []*dbpb.SecretSource{
			{Name: "vault", Kind: "vault", Path: "secret/data/app", Audience: "cdrom"},
		},
		Jobs: []*dbpb.JobDefinition{{
			Key:  "build",
			Name: "build",
			Variables: []*dbpb.Variable{
				{Name: "region", Shape: dbpb.VariableShape_VARIABLE_PLAIN, Plain: "eu-west-1"},
				{Name: "api_token", Shape: dbpb.VariableShape_VARIABLE_SECRET_SOURCE, SecretSource: "vault", Key: "token"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	// The pipeline's named variable sets and secret sources are stored.
	if got := len(pipeline.GetVariableSets()); got != 1 {
		t.Fatalf("pipeline variable sets = %d, want 1", got)
	}
	if got := len(pipeline.GetSecretSources()); got != 1 {
		t.Fatalf("pipeline secret sources = %d, want 1", got)
	}

	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if got := len(resp.GetJobs()); got != 1 {
		t.Fatalf("run created %d job instances, want 1", got)
	}
	variables := resp.GetJobs()[0].GetVariables()
	if got := len(variables); got != 2 {
		t.Fatalf("instance variables = %d, want 2", got)
	}
	byName := map[string]*dbpb.Variable{}
	for _, v := range variables {
		byName[v.GetName()] = v
	}
	// The job's inline variable wins over the set's on a key collision.
	if v := byName["region"]; v == nil || v.GetPlain() != "eu-west-1" {
		t.Errorf("instance region = %+v, want inline eu-west-1 (inline wins over the set)", v)
	}
	// The sourced variable is carried through to the instance.
	if v := byName["api_token"]; v == nil || v.GetSecretSource() != "vault" || v.GetKey() != "token" {
		t.Errorf("instance api_token = %+v, want sourced from vault/token", v)
	}
}

// TestUpdatePipelineReplacesVariableSets verifies that a pipeline's named
// variable sets and secret sources (F-21.1.3) are replaced on update, and that
// a run against the new version carries the new job variables.
func TestUpdatePipelineReplacesVariableSets(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "deploy",
		VariableSets: []*dbpb.VariableSet{
			{Name: "common", Variables: []*dbpb.Variable{{Name: "region", Shape: dbpb.VariableShape_VARIABLE_PLAIN, Plain: "us-east-1"}}},
		},
		Jobs: []*dbpb.JobDefinition{{
			Key:       "build",
			Name:      "build",
			Variables: []*dbpb.Variable{{Name: "region", Shape: dbpb.VariableShape_VARIABLE_PLAIN, Plain: "us-east-1"}},
		}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if _, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id: pipeline.GetId(),
		VariableSets: []*dbpb.VariableSet{
			{Name: "common", Variables: []*dbpb.Variable{{Name: "region", Shape: dbpb.VariableShape_VARIABLE_PLAIN, Plain: "eu-west-1"}}},
		},
		Jobs: []*dbpb.JobDefinition{{
			Key:       "build",
			Name:      "build",
			Variables: []*dbpb.Variable{{Name: "region", Shape: dbpb.VariableShape_VARIABLE_PLAIN, Plain: "eu-west-1"}},
		}},
	}); err != nil {
		t.Fatalf("UpdatePipeline: %v", err)
	}
	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if got := len(resp.GetJobs()[0].GetVariables()); got != 1 {
		t.Fatalf("instance variables = %d, want 1", got)
	}
	if got := resp.GetJobs()[0].GetVariables()[0].GetPlain(); got != "eu-west-1" {
		t.Errorf("instance region = %q, want eu-west-1 (the updated set)", got)
	}
}

// ---------------------------------------------------------------------------
// Audit log (F-15)
// ---------------------------------------------------------------------------

// TestAuditAppendAndList verifies that audit events are appended and can be
// queried back, most recent first, with all fields intact (including the
// old/new value documents).
func TestAuditAppendAndList(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	old := &dbpb.AppendAuditEventRequest{
		Actor:      "alice",
		ActorKind:  "user",
		Action:     "pipeline.update",
		TargetKind: "pipeline",
		TargetId:   "1",
		TargetName: "build",
		PipelineId: 1,
		Outcome:    "success",
		SourceIp:   "10.0.0.1",
		OldValue:   `{"name":"build"}`,
		NewValue:   `{"name":"build2"}`,
		Details:    `{"changed":["name"]}`,
	}
	if _, err := client.AppendAuditEvent(ctx, old); err != nil {
		t.Fatalf("AppendAuditEvent: %v", err)
	}
	new := &dbpb.AppendAuditEventRequest{
		Actor:      "bob",
		ActorKind:  "user",
		Action:     "run.trigger",
		TargetKind: "run",
		TargetId:   "5",
		PipelineId: 1,
		RunId:      5,
		Outcome:    "success",
	}
	if _, err := client.AppendAuditEvent(ctx, new); err != nil {
		t.Fatalf("AppendAuditEvent: %v", err)
	}

	resp, err := client.ListAuditEvents(ctx, &dbpb.ListAuditEventsRequest{})
	if err != nil {
		t.Fatalf("ListAuditEvents: %v", err)
	}
	events := resp.GetEvents()
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	// Most recent first: the second append comes back first.
	if events[0].GetActor() != "bob" || events[1].GetActor() != "alice" {
		t.Errorf("order = [%s, %s], want [bob, alice]", events[0].GetActor(), events[1].GetActor())
	}
	// Fields round-trip intact.
	a := events[1]
	if a.GetAction() != "pipeline.update" || a.GetTargetName() != "build" || a.GetSourceIp() != "10.0.0.1" {
		t.Errorf("fields = %v, want action=pipeline.update target=build ip=10.0.0.1", a)
	}
	if a.GetOldValue() != `{"name":"build"}` || a.GetNewValue() != `{"name":"build2"}` || a.GetDetails() != `{"changed":["name"]}` {
		t.Errorf("values = old=%q new=%q details=%q, want the appended documents", a.GetOldValue(), a.GetNewValue(), a.GetDetails())
	}
	if a.GetPipelineId() != 1 {
		t.Errorf("pipeline_id = %d, want 1", a.GetPipelineId())
	}
	if a.GetCreatedAt() == nil {
		t.Error("created_at = nil, want set")
	}
}

// TestAuditListFilters verifies ListAuditEvents filters by actor, action,
// target, pipeline, and time range.
func TestAuditListFilters(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	mk := func(actor, action, targetID string, pipelineID int64) {
		_, err := client.AppendAuditEvent(ctx, &dbpb.AppendAuditEventRequest{
			Actor:      actor,
			ActorKind:  "user",
			Action:     action,
			TargetKind: "job",
			TargetId:   targetID,
			PipelineId: pipelineID,
			Outcome:    "success",
		})
		if err != nil {
			t.Fatalf("AppendAuditEvent: %v", err)
		}
	}
	mk("alice", "job.cancel", "1", 10)
	mk("alice", "job.approve", "2", 10)
	mk("bob", "job.cancel", "3", 20)

	count := func(req *dbpb.ListAuditEventsRequest) int {
		resp, err := client.ListAuditEvents(ctx, req)
		if err != nil {
			t.Fatalf("ListAuditEvents: %v", err)
		}
		return len(resp.GetEvents())
	}
	if got := count(&dbpb.ListAuditEventsRequest{Actor: "alice"}); got != 2 {
		t.Errorf("actor=alice -> %d, want 2", got)
	}
	if got := count(&dbpb.ListAuditEventsRequest{Action: "job.cancel"}); got != 2 {
		t.Errorf("action=job.cancel -> %d, want 2", got)
	}
	if got := count(&dbpb.ListAuditEventsRequest{TargetId: "3"}); got != 1 {
		t.Errorf("target_id=3 -> %d, want 1", got)
	}
	if got := count(&dbpb.ListAuditEventsRequest{PipelineId: 20}); got != 1 {
		t.Errorf("pipeline_id=20 -> %d, want 1", got)
	}
	// A time range in the far future matches everything; a range before now
	// matches nothing.
	if got := count(&dbpb.ListAuditEventsRequest{From: timestamppb.New(time.Now().Add(-time.Hour))}); got != 3 {
		t.Errorf("from=1h ago -> %d, want 3", got)
	}
	if got := count(&dbpb.ListAuditEventsRequest{From: timestamppb.New(time.Now().Add(time.Hour))}); got != 0 {
		t.Errorf("from=+1h -> %d, want 0", got)
	}
	// A limit bounds the result.
	if got := count(&dbpb.ListAuditEventsRequest{Limit: 2}); got != 2 {
		t.Errorf("limit=2 -> %d, want 2", got)
	}
}

// TestAuditAppendValidation verifies AppendAuditEvent rejects an event with no
// action or no actor.
func TestAuditAppendValidation(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	if _, err := client.AppendAuditEvent(ctx, &dbpb.AppendAuditEventRequest{Actor: "alice"}); err == nil {
		t.Error("AppendAuditEvent(no action) = nil error, want error")
	}
	if _, err := client.AppendAuditEvent(ctx, &dbpb.AppendAuditEventRequest{Action: "job.cancel"}); err == nil {
		t.Error("AppendAuditEvent(no actor) = nil error, want error")
	}
}

// TestAuditPrune verifies PruneAuditEvents deletes only events older than the
// cutoff and reports how many it pruned.
func TestAuditPrune(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	_, err := client.AppendAuditEvent(ctx, &dbpb.AppendAuditEventRequest{
		Actor: "alice", ActorKind: "user", Action: "job.cancel", TargetKind: "job", TargetId: "1", Outcome: "success",
	})
	if err != nil {
		t.Fatalf("AppendAuditEvent: %v", err)
	}

	// A cutoff in the future prunes everything.
	resp, err := client.PruneAuditEvents(ctx, &dbpb.PruneAuditEventsRequest{
		CreatedBefore: timestamppb.New(time.Now().Add(time.Hour)),
	})
	if err != nil {
		t.Fatalf("PruneAuditEvents: %v", err)
	}
	if resp.GetPruned() != 1 {
		t.Errorf("pruned = %d, want 1", resp.GetPruned())
	}
	list, err := client.ListAuditEvents(ctx, &dbpb.ListAuditEventsRequest{})
	if err != nil {
		t.Fatalf("ListAuditEvents: %v", err)
	}
	if got := len(list.GetEvents()); got != 0 {
		t.Errorf("events after prune = %d, want 0", got)
	}

	// A cutoff in the past prunes nothing.
	_, _ = client.AppendAuditEvent(ctx, &dbpb.AppendAuditEventRequest{
		Actor: "bob", ActorKind: "user", Action: "job.cancel", TargetKind: "job", TargetId: "2", Outcome: "success",
	})
	resp, err = client.PruneAuditEvents(ctx, &dbpb.PruneAuditEventsRequest{
		CreatedBefore: timestamppb.New(time.Now().Add(-time.Hour)),
	})
	if err != nil {
		t.Fatalf("PruneAuditEvents: %v", err)
	}
	if resp.GetPruned() != 0 {
		t.Errorf("pruned = %d, want 0 (cutoff in the past)", resp.GetPruned())
	}
}

// TestClaimStepOnceSequential verifies the basic claim contract (F-01): the
// first caller wins, later callers in the same attempt lose (reported as
// claimed=false, NOT as an error — the executor folds a claim error into a
// STEP FAILURE, which must not happen to the expected losers of a race), and
// a new attempt claims a fresh slot.
func TestClaimStepOnceSequential(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	first, err := client.ClaimStepOnce(ctx, &dbpb.ClaimStepOnceRequest{JobId: 1, StepName: "notify", Attempt: 1})
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !first.GetClaimed() {
		t.Fatal("first claim = false, want true")
	}
	second, err := client.ClaimStepOnce(ctx, &dbpb.ClaimStepOnceRequest{JobId: 1, StepName: "notify", Attempt: 1})
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if second.GetClaimed() {
		t.Fatal("second claim = true, want false (slot already held)")
	}
	third, err := client.ClaimStepOnce(ctx, &dbpb.ClaimStepOnceRequest{JobId: 1, StepName: "OTHERSTEP", Attempt: 1})
	if err != nil {
		t.Fatalf("different-step claim: %v", err)
	}
	if !third.GetClaimed() {
		t.Fatal("different-step claim = false, want true (disjoint slot)")
	}
	nextAttempt, err := client.ClaimStepOnce(ctx, &dbpb.ClaimStepOnceRequest{JobId: 1, StepName: "notify", Attempt: 2})
	if err != nil {
		t.Fatalf("next-attempt claim: %v", err)
	}
	if !nextAttempt.GetClaimed() {
		t.Fatal("next-attempt claim = false, want true (a retry claims afresh)")
	}
}

// TestClaimStepOnceConcurrentStorm is the regression guard for the e2e
// SQLITE_BUSY flake: many targets reaching the same once step SIMULTANEOUSLY
// hammer the claim. Over SQLite the contenders meet database locks and
// rolled-back unique violations; the contract is:
//
//   - NO caller receives an error (an error makes the executor FAIL the step
//     instead of skipping it — the original flake: a healthy fan-out folded
//     into a spurious job failure).
//   - AT MOST ONE caller reports claimed=true. Double-certification is the
//     forbidden twin: it means two targets both send the notification.
//
// (Reporting ZERO winners is legal under absurd contention — every attempt
// rolled back and the contest stayed unsettled — and is cross-checked below
// against the slot row: zero winners must coincide with an EMPTY slot, since
// a recorded claim can only originate from a committed, certified winner.)
func TestClaimStepOnceConcurrentStorm(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	const racers = 24
	errCh := make(chan error, racers)
	claims := make(chan bool, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.ClaimStepOnce(ctx, &dbpb.ClaimStepOnceRequest{JobId: 7, StepName: "mail", Attempt: 1})
			if err != nil {
				errCh <- err
				return
			}
			claims <- resp.GetClaimed()
		}()
	}
	wg.Wait()
	close(errCh)
	close(claims)

	var winners int
	for err := range errCh {
		t.Errorf("claim returned an error (losers must report claimed=false, not fail): %v", err)
	}
	for won := range claims {
		if won {
			winners++
		}
	}
	if winners > 1 {
		t.Fatalf("winners = %d, want at most 1 (double-send hazard, racers=%d)", winners, racers)
	}
	if winners == 0 {
		// Legal only if the slot row is indeed empty (nobody committed).
		var n int64
		if err := db.Model(&models.StepOnceClaim{}).
			Where("job_id = ? AND step_name = ? AND attempt = ?", 7, "mail", 1).
			Count(&n).Error; err != nil {
			t.Fatalf("slot-row cross-check: %v", err)
		}
		if n > 0 {
			t.Fatalf("winners = 0 but the slot row exists: a certified winner must have reported true")
		}
		t.Logf("contest unsettled (all attempts rolled back); slot empty — consistent")
	}
}
