package scheduler

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// ---------------------------------------------------------------------------
// deriveRunStatus
// ---------------------------------------------------------------------------

func jobWithStatus(id int64, status dbpb.JobStatus) *dbpb.Job {
	return &dbpb.Job{Id: id, Status: status}
}

func TestDeriveRunStatus(t *testing.T) {
	cases := []struct {
		name string
		jobs []*dbpb.Job
		want string // the model status name
	}{
		{"no jobs", nil, "succeeded"},
		{"all succeeded", []*dbpb.Job{jobWithStatus(1, dbpb.JobStatus_JOB_STATUS_SUCCEEDED), jobWithStatus(2, dbpb.JobStatus_JOB_STATUS_SUCCEEDED)}, "succeeded"},
		{"succeeded and skipped", []*dbpb.Job{jobWithStatus(1, dbpb.JobStatus_JOB_STATUS_SUCCEEDED), jobWithStatus(2, dbpb.JobStatus_JOB_STATUS_SKIPPED)}, "succeeded"},
		{"one failed", []*dbpb.Job{jobWithStatus(1, dbpb.JobStatus_JOB_STATUS_SUCCEEDED), jobWithStatus(2, dbpb.JobStatus_JOB_STATUS_FAILED)}, "failed"},
		{"one timed out", []*dbpb.Job{jobWithStatus(1, dbpb.JobStatus_JOB_STATUS_SUCCEEDED), jobWithStatus(2, dbpb.JobStatus_JOB_STATUS_TIMED_OUT)}, "failed"},
		{"failed beats cancelled", []*dbpb.Job{jobWithStatus(1, dbpb.JobStatus_JOB_STATUS_CANCELLED), jobWithStatus(2, dbpb.JobStatus_JOB_STATUS_FAILED)}, "failed"},
		{"one cancelled", []*dbpb.Job{jobWithStatus(1, dbpb.JobStatus_JOB_STATUS_SUCCEEDED), jobWithStatus(2, dbpb.JobStatus_JOB_STATUS_CANCELLED)}, "cancelled"},
		{"one pending", []*dbpb.Job{jobWithStatus(1, dbpb.JobStatus_JOB_STATUS_SUCCEEDED), jobWithStatus(2, dbpb.JobStatus_JOB_STATUS_PENDING)}, "running"},
		{"one running", []*dbpb.Job{jobWithStatus(1, dbpb.JobStatus_JOB_STATUS_SUCCEEDED), jobWithStatus(2, dbpb.JobStatus_JOB_STATUS_RUNNING)}, "running"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveRunStatus(tc.jobs)
			if string(got) != tc.want {
				t.Errorf("deriveRunStatus(%v) = %q, want %q", tc.jobs, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// run-status loop (processRunStatuses)
// ---------------------------------------------------------------------------

// fakeRunStatusDB is a stub Database client for the run-status loop. It serves
// a fixed set of runs (filtered by status) and a fixed set of jobs (filtered
// by run_id), and records the UpdateRun calls it receives.
type fakeRunStatusDB struct {
	dbpb.DatabaseClient // nil
	runs                []*dbpb.PipelineRun
	jobs                []*dbpb.Job
	updated             []*dbpb.UpdateRunRequest
}

func (f *fakeRunStatusDB) ListRuns(ctx context.Context, in *dbpb.ListRunsRequest, opts ...grpc.CallOption) (*dbpb.ListRunsResponse, error) {
	response := &dbpb.ListRunsResponse{}
	for _, run := range f.runs {
		if in.GetStatus() != dbpb.RunStatus_RUN_STATUS_UNSPECIFIED && run.GetStatus() != in.GetStatus() {
			continue
		}
		response.Runs = append(response.Runs, run)
	}
	return response, nil
}

func (f *fakeRunStatusDB) ListJobs(ctx context.Context, in *dbpb.ListJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error) {
	response := &dbpb.ListJobsResponse{}
	for _, job := range f.jobs {
		if in.GetRunId() != 0 && job.GetRunId() != in.GetRunId() {
			continue
		}
		response.Jobs = append(response.Jobs, job)
	}
	return response, nil
}

func (f *fakeRunStatusDB) UpdateRun(ctx context.Context, in *dbpb.UpdateRunRequest, opts ...grpc.CallOption) (*dbpb.PipelineRun, error) {
	f.updated = append(f.updated, in)
	return &dbpb.PipelineRun{Id: in.GetId(), Status: in.GetStatus()}, nil
}

// fakeRunStatusPublisher is a stub Database publisher for the run-status
// loop. It records the "run_status" events the scheduler appends to the
// shared event log (F-23).
type fakeRunStatusPublisher struct {
	notified []*dbpb.PublishRunStatusRequest
}

func (f *fakeRunStatusPublisher) PublishRunStatus(ctx context.Context, in *dbpb.PublishRunStatusRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	f.notified = append(f.notified, in)
	return &dbpb.PublishEventResponse{}, nil
}

// TestRunStatusLoopSucceedsRun verifies that a pending run whose job instances
// all succeeded is re-derived as succeeded, persisted (with start and finish
// times), and fanned out to the UI (F-07).
func TestRunStatusLoopSucceedsRun(t *testing.T) {
	db := &fakeRunStatusDB{
		runs: []*dbpb.PipelineRun{{Id: 100, PipelineId: 1, Status: dbpb.RunStatus_RUN_STATUS_PENDING}},
		jobs: []*dbpb.Job{
			{Id: 1, RunId: 100, Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED},
			{Id: 2, RunId: 100, Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED},
		},
	}
	publisher := &fakeRunStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	s.processRunStatuses(context.Background(), db, publisher)

	if len(db.updated) != 1 {
		t.Fatalf("UpdateRun called %d times, want 1", len(db.updated))
	}
	update := db.updated[0]
	if update.GetId() != 100 {
		t.Errorf("UpdateRun id = %d, want 100", update.GetId())
	}
	if update.GetStatus() != dbpb.RunStatus_RUN_STATUS_SUCCEEDED {
		t.Errorf("UpdateRun status = %v, want SUCCEEDED", update.GetStatus())
	}
	if update.GetStartedAt() == nil {
		t.Error("UpdateRun started_at = nil, want set (run left pending)")
	}
	if update.GetFinishedAt() == nil {
		t.Error("UpdateRun finished_at = nil, want set (run reached a terminal state)")
	}
	if len(publisher.notified) != 1 || publisher.notified[0].GetRunId() != 100 || publisher.notified[0].GetStatus() != dbpb.RunStatus_RUN_STATUS_SUCCEEDED {
		t.Errorf("PublishRunStatus = %+v, want run 100 SUCCEEDED", publisher.notified)
	}
}

// TestRunStatusLoopRunningRun verifies that a pending run with an in-flight
// job is re-derived as running: the start time is recorded but not the finish
// time (the run is not yet terminal) (F-07).
func TestRunStatusLoopRunningRun(t *testing.T) {
	db := &fakeRunStatusDB{
		runs: []*dbpb.PipelineRun{{Id: 200, PipelineId: 1, Status: dbpb.RunStatus_RUN_STATUS_PENDING}},
		jobs: []*dbpb.Job{
			{Id: 1, RunId: 200, Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED},
			{Id: 2, RunId: 200, Status: dbpb.JobStatus_JOB_STATUS_RUNNING},
		},
	}
	publisher := &fakeRunStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	s.processRunStatuses(context.Background(), db, publisher)

	if len(db.updated) != 1 {
		t.Fatalf("UpdateRun called %d times, want 1", len(db.updated))
	}
	update := db.updated[0]
	if update.GetStatus() != dbpb.RunStatus_RUN_STATUS_RUNNING {
		t.Errorf("UpdateRun status = %v, want RUNNING", update.GetStatus())
	}
	if update.GetStartedAt() == nil {
		t.Error("UpdateRun started_at = nil, want set")
	}
	if update.GetFinishedAt() != nil {
		t.Error("UpdateRun finished_at = set, want nil (run is not terminal)")
	}
}

// TestRunStatusLoopNoopWhenUnchanged verifies that a run whose stored status
// already matches its jobs' derived status is left untouched (no update, no
// notification) (F-07).
func TestRunStatusLoopNoopWhenUnchanged(t *testing.T) {
	db := &fakeRunStatusDB{
		runs: []*dbpb.PipelineRun{{Id: 300, PipelineId: 1, Status: dbpb.RunStatus_RUN_STATUS_SUCCEEDED}},
		jobs: []*dbpb.Job{{Id: 1, RunId: 300, Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED}},
	}
	publisher := &fakeRunStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	// The run is already succeeded and its only job succeeded: the derived
	// status matches the stored one, so nothing is persisted or fanned out.
	s.processRunStatuses(context.Background(), db, publisher)

	if len(db.updated) != 0 {
		t.Errorf("UpdateRun called %d times, want 0 (status unchanged)", len(db.updated))
	}
	if len(publisher.notified) != 0 {
		t.Errorf("PublishRunStatus called %d times, want 0 (status unchanged)", len(publisher.notified))
	}
}

// TestRunStatusLoopSkippedJobsDoNotFailRun verifies that a run whose jobs are
// all succeeded or skipped is succeeded (skipped jobs do not fail the run)
// (F-07).
func TestRunStatusLoopSkippedJobsDoNotFailRun(t *testing.T) {
	db := &fakeRunStatusDB{
		runs: []*dbpb.PipelineRun{{Id: 400, PipelineId: 1, Status: dbpb.RunStatus_RUN_STATUS_PENDING}},
		jobs: []*dbpb.Job{
			{Id: 1, RunId: 400, Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED},
			{Id: 2, RunId: 400, Status: dbpb.JobStatus_JOB_STATUS_SKIPPED},
		},
	}
	publisher := &fakeRunStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	s.processRunStatuses(context.Background(), db, publisher)

	if len(db.updated) != 1 || db.updated[0].GetStatus() != dbpb.RunStatus_RUN_STATUS_SUCCEEDED {
		t.Errorf("UpdateRun = %+v, want run 400 SUCCEEDED", db.updated)
	}
}

// ---------------------------------------------------------------------------
// CreateRun
// ---------------------------------------------------------------------------

// fakeCreateRunDB is a stub Database client for the scheduler's CreateRun RPC.
// It records the CreateRun request, returns a canned run + job instances, and
// records the "assignment" events the scheduler appends to the shared event
// log (F-23) for the run's job instances.
type fakeCreateRunDB struct {
	dbpb.DatabaseClient // nil
	request             *dbpb.CreateRunRequest
	response            *dbpb.CreateRunResponse
	dispatched          []int64 // job ids an "assignment" event was published for
}

func (f *fakeCreateRunDB) CreateRun(ctx context.Context, in *dbpb.CreateRunRequest, opts ...grpc.CallOption) (*dbpb.CreateRunResponse, error) {
	f.request = in
	return f.response, nil
}

func (f *fakeCreateRunDB) PublishAssignment(ctx context.Context, in *dbpb.PublishAssignmentRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	f.dispatched = append(f.dispatched, in.GetJobId())
	return &dbpb.PublishEventResponse{}, nil
}

// TestCreateRunDrivesInstances verifies that the scheduler's CreateRun RPC
// (F-07) creates the run via the Database service and then drives the
// instances: dependency-free instances with a target group are dispatched to
// the API, instances with dependencies are held pending (not dispatched), and
// instances with an empty target group are left for an ephemeral agent (not
// dispatched).
func TestCreateRunDrivesInstances(t *testing.T) {
	db := &fakeCreateRunDB{
		response: &dbpb.CreateRunResponse{
			Run: &dbpb.PipelineRun{Id: 10, PipelineId: 1, Status: dbpb.RunStatus_RUN_STATUS_PENDING},
			Jobs: []*dbpb.Job{
				{Id: 1, RunId: 10, Name: "build", TargetGroup: "linux-pool", Status: dbpb.JobStatus_JOB_STATUS_PENDING},
				{Id: 2, RunId: 10, Name: "test", TargetGroup: "linux-pool", DependsOn: []int64{1}, Status: dbpb.JobStatus_JOB_STATUS_PENDING},
				{Id: 3, RunId: 10, Name: "agent-job", TargetGroup: "", Status: dbpb.JobStatus_JOB_STATUS_PENDING},
			},
		},
	}
	s := &Server{db: db, logger: testLogger()}

	run, err := s.CreateRun(context.Background(), &schedpb.CreateRunRequest{
		PipelineId: 1,
		Trigger:    "manual",
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if run.GetId() != 10 {
		t.Errorf("run id = %d, want 10", run.GetId())
	}
	if run.GetStatus() != dbpb.RunStatus_RUN_STATUS_PENDING {
		t.Errorf("run status = %v, want PENDING", run.GetStatus())
	}
	if db.request == nil || db.request.GetPipelineId() != 1 || db.request.GetTrigger() != "manual" {
		t.Errorf("db.CreateRun request = %+v, want pipeline 1 trigger manual", db.request)
	}
	// Only the dependency-free, group-targeted instance (build) is dispatched.
	// The dependent instance (test) is held pending and the agent instance
	// (agent-job) is not dispatched to live workers.
	if len(db.dispatched) != 1 {
		t.Fatalf("assignment events = %v, want one (build)", db.dispatched)
	}
	if db.dispatched[0] != 1 {
		t.Errorf("dispatched job = %d, want 1 (build)", db.dispatched[0])
	}
}

// TestCreateRunRequiresPipeline verifies that CreateRun rejects a request with
// no pipeline id (F-07).
func TestCreateRunRequiresPipeline(t *testing.T) {
	db := &fakeCreateRunDB{}
	s := &Server{db: db, logger: testLogger()}

	if _, err := s.CreateRun(context.Background(), &schedpb.CreateRunRequest{Trigger: "manual"}); err == nil {
		t.Fatal("CreateRun with no pipeline_id = nil error, want an error")
	}
	if db.request != nil {
		t.Error("db.CreateRun was called despite a missing pipeline_id")
	}
}
