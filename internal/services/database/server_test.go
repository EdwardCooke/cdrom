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
