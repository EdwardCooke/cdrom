package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// fakeScheduler is a stub SchedulerClient that records the last SubmitJob,
// RerunJob, and CreateRun requests and returns a canned job / run.
type fakeScheduler struct {
	submitted *schedpb.SubmitJobRequest
	rerun     *schedpb.RerunJobRequest
	created   *schedpb.CreateRunRequest
	job       *schedpb.Job
	run       *schedpb.Run
}

func (f *fakeScheduler) SubmitJob(ctx context.Context, in *schedpb.SubmitJobRequest, opts ...grpc.CallOption) (*schedpb.Job, error) {
	f.submitted = in
	return f.job, nil
}

func (f *fakeScheduler) CreateRun(ctx context.Context, in *schedpb.CreateRunRequest, opts ...grpc.CallOption) (*schedpb.Run, error) {
	f.created = in
	if f.run != nil {
		return f.run, nil
	}
	return &schedpb.Run{Id: 1, PipelineId: in.GetPipelineId(), Status: dbpb.RunStatus_RUN_STATUS_PENDING, Trigger: in.GetTrigger()}, nil
}

func (f *fakeScheduler) GetJob(ctx context.Context, in *schedpb.GetJobRequest, opts ...grpc.CallOption) (*schedpb.Job, error) {
	return f.job, nil
}

func (f *fakeScheduler) ListJobs(ctx context.Context, in *schedpb.ListJobsRequest, opts ...grpc.CallOption) (*schedpb.ListJobsResponse, error) {
	return &schedpb.ListJobsResponse{}, nil
}

func (f *fakeScheduler) CancelJob(ctx context.Context, in *schedpb.CancelJobRequest, opts ...grpc.CallOption) (*schedpb.Job, error) {
	return f.job, nil
}

func (f *fakeScheduler) RerunJob(ctx context.Context, in *schedpb.RerunJobRequest, opts ...grpc.CallOption) (*schedpb.Job, error) {
	f.rerun = in
	return f.job, nil
}

// TestSubmitJobWithSpec posts a job with a multi-step execution spec and
// verifies the spec is converted and forwarded to the scheduler intact.
func TestSubmitJobWithSpec(t *testing.T) {
	fake := &fakeScheduler{job: &schedpb.Job{Id: 7, Name: "build", Status: dbpb.JobStatus_JOB_STATUS_PENDING}}
	srv := New(Clients{Scheduler: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := `{
		"name": "build",
		"target_group": "linux-pool",
		"spec": {
			"steps": [
				{"params": {"command": {"string": "go"}, "args": {"strings": ["build", "./..."]}}, "workdir": "repo", "env": {"GOFLAGS": "-mod=vendor"}},
				{"params": {"command": {"string": "sh"}, "args": {"strings": ["-c", "make test"]}}, "timeout": "30s"}
			]
		}
	}`
	resp, err := http.Post(ts.URL+"/api/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body %s)", resp.StatusCode, data)
	}

	if fake.submitted == nil {
		t.Fatal("scheduler.SubmitJob was not called")
	}
	spec := fake.submitted.GetSpec()
	if spec == nil {
		t.Fatal("spec was not forwarded to the scheduler")
	}
	if got := len(spec.GetSteps()); got != 2 {
		t.Fatalf("steps = %d, want 2", got)
	}
	step0 := spec.GetSteps()[0]
	if step0.GetParams()["command"].GetString_() != "go" || step0.GetWorkdir() != "repo" || step0.GetEnv()["GOFLAGS"] != "-mod=vendor" {
		t.Errorf("step 0 = %+v, want go build in repo with GOFLAGS", step0)
	}
	if got := step0.GetParams()["args"].GetStrings(); len(got) != 2 || got[0] != "build" || got[1] != "./..." {
		t.Errorf("step 0 args = %v, want [build ./...]", got)
	}
	step1 := spec.GetSteps()[1]
	if step1.GetTimeout().AsDuration().Seconds() != 30 {
		t.Errorf("step 1 timeout = %s, want 30s", step1.GetTimeout().AsDuration())
	}
}

// TestSubmitJobWithoutSpec verifies that a job submitted without a spec is
// forwarded with a nil spec (a no-op job).
func TestSubmitJobWithoutSpec(t *testing.T) {
	fake := &fakeScheduler{job: &schedpb.Job{Id: 8, Name: "noop"}}
	srv := New(Clients{Scheduler: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/jobs", "application/json", bytes.NewBufferString(`{"name": "noop"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if spec := fake.submitted.GetSpec(); spec != nil {
		t.Errorf("spec = %+v, want nil", spec)
	}
}

// TestSubmitJobInvalidSpec verifies that an invalid spec (missing command, or
// a malformed timeout) is rejected with a 400 before reaching the scheduler.
func TestSubmitJobInvalidSpec(t *testing.T) {
	cases := []string{
		`{"name": "x", "spec": {"steps": [{"params": {"args": {"strings": ["build"]}}}]}}`,
		`{"name": "x", "spec": {"steps": [{"params": {"command": {"string": "go"}}, "timeout": "not-a-duration"}]}}`,
	}
	for _, body := range cases {
		fake := &fakeScheduler{}
		srv := New(Clients{Scheduler: fake}, nil)
		ts := httptest.NewServer(srv.Handler())

		resp, err := http.Post(ts.URL+"/api/jobs", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		if resp.StatusCode != http.StatusBadRequest {
			data, _ := io.ReadAll(resp.Body)
			t.Errorf("body %s: status = %d, want 400 (body %s)", body, resp.StatusCode, data)
		}
		if fake.submitted != nil {
			t.Errorf("body %s: scheduler was called despite an invalid spec", body)
		}
		resp.Body.Close()
		ts.Close()
	}
}

// TestToProtoSpec verifies the JSON-to-proto spec conversion directly,
// including the no-steps → nil case.
func TestToProtoSpec(t *testing.T) {
	spec := &jobSpecRequest{Steps: []jobStepRequest{
		{Params: map[string]*paramValueRequest{"command": {String: "go"}, "args": {Strings: []string{"build"}}}, Timeout: "1m"},
	}}
	proto, err := spec.toProtoSpec()
	if err != nil {
		t.Fatalf("toProtoSpec: %v", err)
	}
	if proto.GetSteps()[0].GetTimeout().AsDuration().Minutes() != 1 {
		t.Errorf("timeout = %s, want 1m", proto.GetSteps()[0].GetTimeout().AsDuration())
	}

	if got, _ := (&jobSpecRequest{}).toProtoSpec(); got != nil {
		t.Errorf("empty spec = %+v, want nil", got)
	}
	if got, _ := (*jobSpecRequest)(nil).toProtoSpec(); got != nil {
		t.Errorf("nil spec = %+v, want nil", got)
	}
}

// TestToProtoSpecTimeout is a guard that durationpb is exercised by the
// conversion (kept separate so a regression in timeout parsing is obvious).
func TestToProtoSpecTimeout(t *testing.T) {
	spec := &jobSpecRequest{Steps: []jobStepRequest{{Params: map[string]*paramValueRequest{"command": {String: "sleep"}}, Timeout: "45s"}}}
	proto, err := spec.toProtoSpec()
	if err != nil {
		t.Fatalf("toProtoSpec: %v", err)
	}
	if got := proto.GetSteps()[0].GetTimeout().AsDuration(); got != 45*time.Second {
		t.Errorf("timeout = %s, want 45s", got)
	}
}

// TestToProtoSpecRetry verifies that a job's retry policy (F-04) is carried
// into the proto, and that a spec with only a retry policy (no steps) is still
// a real spec.
func TestToProtoSpecRetry(t *testing.T) {
	spec := &jobSpecRequest{
		Retry: &retryPolicyRequest{MaxAttempts: 3, Backoff: "10s"},
		Steps: []jobStepRequest{{Params: map[string]*paramValueRequest{"command": {String: "go"}}}},
	}
	proto, err := spec.toProtoSpec()
	if err != nil {
		t.Fatalf("toProtoSpec: %v", err)
	}
	retry := proto.GetRetry()
	if retry == nil {
		t.Fatal("retry policy was not forwarded to the proto")
	}
	if retry.GetMaxAttempts() != 3 {
		t.Errorf("retry max_attempts = %d, want 3", retry.GetMaxAttempts())
	}
	if retry.GetBackoff().AsDuration() != 10*time.Second {
		t.Errorf("retry backoff = %s, want 10s", retry.GetBackoff().AsDuration())
	}

	// A spec with only a retry policy (no steps) is still a real spec.
	onlyRetry, err := (&jobSpecRequest{Retry: &retryPolicyRequest{MaxAttempts: 1}}).toProtoSpec()
	if err != nil {
		t.Fatalf("toProtoSpec: %v", err)
	}
	if onlyRetry == nil || onlyRetry.GetRetry() == nil {
		t.Error("spec with only a retry policy was dropped, want it kept")
	}

	// An invalid backoff is rejected.
	if _, err := (&jobSpecRequest{Retry: &retryPolicyRequest{Backoff: "not-a-duration"}}).toProtoSpec(); err == nil {
		t.Error("expected an error for an invalid retry backoff, got nil")
	}
}

// TestToProtoSpecStepType verifies that a step's type and params are carried
// into the proto, and that a non-shell step type does not require a command
// (only the built-in shell handler does).
func TestToProtoSpecStepType(t *testing.T) {
	spec := &jobSpecRequest{Steps: []jobStepRequest{
		{Type: "ansible", Params: map[string]*paramValueRequest{"inventory": {String: "prod"}}},
	}}
	proto, err := spec.toProtoSpec()
	if err != nil {
		t.Fatalf("toProtoSpec: %v", err)
	}
	step := proto.GetSteps()[0]
	if step.GetType() != "ansible" {
		t.Errorf("type = %q, want %q", step.GetType(), "ansible")
	}
	if step.GetParams()["inventory"].GetString_() != "prod" {
		t.Errorf("params = %v, want inventory=prod", step.GetParams())
	}

	// A shell step (empty type) with no command is still rejected.
	if _, err := (&jobSpecRequest{Steps: []jobStepRequest{{}}}).toProtoSpec(); err == nil {
		t.Error("expected an error for a shell step with no command, got nil")
	}
}

// TestRerunJob verifies that POST /api/jobs/{id}/rerun (F-04) forwards the job
// id to the scheduler's RerunJob RPC and returns the re-run job.
func TestRerunJob(t *testing.T) {
	fake := &fakeScheduler{job: &schedpb.Job{Id: 9, Name: "build", Status: dbpb.JobStatus_JOB_STATUS_PENDING, Attempt: 1}}
	srv := New(Clients{Scheduler: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/jobs/9/rerun", "application/json", nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	if fake.rerun == nil {
		t.Fatal("scheduler.RerunJob was not called")
	}
	if fake.rerun.GetId() != 9 {
		t.Errorf("rerun job id = %d, want 9", fake.rerun.GetId())
	}
}

// TestCreateRun verifies that POST /api/pipelines/{id}/runs (F-07) forwards the
// pipeline id, trigger, and params to the scheduler's CreateRun RPC and
// returns the created run.
func TestCreateRun(t *testing.T) {
	fake := &fakeScheduler{run: &schedpb.Run{Id: 42, PipelineId: 3, Status: dbpb.RunStatus_RUN_STATUS_PENDING, Trigger: "manual"}}
	srv := New(Clients{Scheduler: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := `{"trigger": "manual", "params": {"env": "prod"}}`
	resp, err := http.Post(ts.URL+"/api/pipelines/3/runs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if fake.created == nil {
		t.Fatal("scheduler.CreateRun was not called")
	}
	if fake.created.GetPipelineId() != 3 {
		t.Errorf("pipeline id = %d, want 3", fake.created.GetPipelineId())
	}
	if fake.created.GetTrigger() != "manual" {
		t.Errorf("trigger = %q, want manual", fake.created.GetTrigger())
	}
	if got := fake.created.GetParams()["env"]; got != "prod" {
		t.Errorf("params[env] = %q, want prod", got)
	}
}

// TestCreateRunEmptyBody verifies that a run can be triggered with no body
// (the default trigger and no params), exercising the io.EOF tolerance in the
// handler.
func TestCreateRunEmptyBody(t *testing.T) {
	fake := &fakeScheduler{}
	srv := New(Clients{Scheduler: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/pipelines/3/runs", "application/json", nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if fake.created == nil {
		t.Fatal("scheduler.CreateRun was not called")
	}
	if fake.created.GetPipelineId() != 3 {
		t.Errorf("pipeline id = %d, want 3", fake.created.GetPipelineId())
	}
	// The fake returns a default run when none is canned.
	if fake.created.GetTrigger() != "" {
		t.Errorf("trigger = %q, want empty", fake.created.GetTrigger())
	}
}

// fakeDatabase is a stub DatabaseClient for the API's pipeline/job handlers
// (F-08): it records CreatePipeline and CreateJob calls and returns canned
// results. Only the methods the handlers use are implemented.
type fakeDatabase struct {
	dbpb.DatabaseClient // nil
	createdPipeline     *dbpb.CreatePipelineRequest
	createdJob          *dbpb.CreateJobRequest
	pipeline            *dbpb.Pipeline
	job                 *dbpb.Job
}

func (f *fakeDatabase) CreatePipeline(ctx context.Context, in *dbpb.CreatePipelineRequest, opts ...grpc.CallOption) (*dbpb.Pipeline, error) {
	f.createdPipeline = in
	if f.pipeline != nil {
		return f.pipeline, nil
	}
	return &dbpb.Pipeline{Id: 1, Name: in.GetName()}, nil
}

func (f *fakeDatabase) CreateJob(ctx context.Context, in *dbpb.CreateJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	f.createdJob = in
	if f.job != nil {
		return f.job, nil
	}
	return &dbpb.Job{Id: 1, Name: in.GetName(), Status: dbpb.JobStatus_JOB_STATUS_PENDING}, nil
}

// TestCreatePipelineWithJobs verifies that POST /api/pipelines with job
// definitions (F-08) forwards the jobs (keys, needs, specs) to the database
// service.
func TestCreatePipelineWithJobs(t *testing.T) {
	fake := &fakeDatabase{}
	srv := New(Clients{Database: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := `{
		"name": "build",
		"jobs": [
			{"key": "build", "target_group": "linux-pool", "spec": {"steps": [{"params": {"command": {"string": "go"}, "args": {"strings": ["build"]}}}]}}
			,
			{"key": "test", "needs": ["build"], "spec": {"steps": [{"params": {"command": {"string": "go"}, "args": {"strings": ["test"]}}}]}}
		]
	}`
	resp, err := http.Post(ts.URL+"/api/pipelines", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if fake.createdPipeline == nil {
		t.Fatal("database.CreatePipeline was not called")
	}
	if got := len(fake.createdPipeline.GetJobs()); got != 2 {
		t.Fatalf("jobs = %d, want 2", got)
	}
	build := fake.createdPipeline.GetJobs()[0]
	if build.GetKey() != "build" || build.GetTargetGroup() != "linux-pool" {
		t.Errorf("build job = %+v, want key build in linux-pool", build)
	}
	if len(build.GetNeeds()) != 0 {
		t.Errorf("build needs = %v, want none", build.GetNeeds())
	}
	test := fake.createdPipeline.GetJobs()[1]
	if test.GetKey() != "test" || len(test.GetNeeds()) != 1 || test.GetNeeds()[0] != "build" {
		t.Errorf("test job = %+v, want key test needing build", test)
	}
	if test.GetSpec() == nil || len(test.GetSpec().GetSteps()) != 1 {
		t.Errorf("test spec = %+v, want one step", test.GetSpec())
	}
}

// TestSubmitPipelineJobGoesToDatabase verifies that a job submitted with a
// pipeline_id (F-08) is persisted as a job definition via the database service
// (not dispatched by the scheduler), carrying its key and needs.
func TestSubmitPipelineJobGoesToDatabase(t *testing.T) {
	fakeDB := &fakeDatabase{}
	fakeSched := &fakeScheduler{}
	srv := New(Clients{Database: fakeDB, Scheduler: fakeSched}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := `{"pipeline_id": 5, "name": "test", "key": "test", "needs": ["build"], "target_group": "linux-pool"}`
	resp, err := http.Post(ts.URL+"/api/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if fakeDB.createdJob == nil {
		t.Fatal("database.CreateJob was not called for a pipeline job")
	}
	if fakeDB.createdJob.GetPipelineId() != 5 {
		t.Errorf("pipeline id = %d, want 5", fakeDB.createdJob.GetPipelineId())
	}
	if fakeDB.createdJob.GetKey() != "test" {
		t.Errorf("key = %q, want test", fakeDB.createdJob.GetKey())
	}
	if got := fakeDB.createdJob.GetNeeds(); len(got) != 1 || got[0] != "build" {
		t.Errorf("needs = %v, want [build]", got)
	}
	if fakeSched.submitted != nil {
		t.Error("scheduler.SubmitJob was called for a pipeline job, want it routed to the database")
	}
}

// TestSubmitStandaloneJobGoesToScheduler verifies that a job submitted without
// a pipeline_id is still created and dispatched by the scheduler (F-08 routing
// leaves standalone jobs on the existing path).
func TestSubmitStandaloneJobGoesToScheduler(t *testing.T) {
	fakeDB := &fakeDatabase{}
	fakeSched := &fakeScheduler{job: &schedpb.Job{Id: 9, Name: "standalone", Status: dbpb.JobStatus_JOB_STATUS_PENDING}}
	srv := New(Clients{Database: fakeDB, Scheduler: fakeSched}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/jobs", "application/json", bytes.NewBufferString(`{"name": "standalone", "target_group": "linux-pool"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if fakeSched.submitted == nil {
		t.Fatal("scheduler.SubmitJob was not called for a standalone job")
	}
	if fakeDB.createdJob != nil {
		t.Error("database.CreateJob was called for a standalone job, want it routed to the scheduler")
	}
}
