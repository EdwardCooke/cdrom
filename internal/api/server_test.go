package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
	"cdrom/internal/secrets"
)

// fakePipelineSource is a stub source-fetcher client (F-21.2) for the
// /api/pipelines/{id}/source/refresh handler tests. It returns a canned
// response or a canned error.
type fakePipelineSource struct {
	response *pipelinesourcepb.FetchSourceResponse
	err      error
}

func (f *fakePipelineSource) FetchSource(ctx context.Context, in *pipelinesourcepb.FetchSourceRequest, opts ...grpc.CallOption) (*pipelinesourcepb.FetchSourceResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.response != nil {
		return f.response, nil
	}
	return &pipelinesourcepb.FetchSourceResponse{Content: []byte("x"), Version: "v1", Hash: "abc"}, nil
}

// fakeScheduler is a stub SchedulerClient that records the last SubmitJob,
// RerunJob, CreateRun, and TriggerRun requests and returns a canned job / run.
type fakeScheduler struct {
	submitted *schedpb.SubmitJobRequest
	rerun     *schedpb.RerunJobRequest
	resolved  *schedpb.ResolveApprovalRequest
	created   *schedpb.CreateRunRequest
	triggered *schedpb.TriggerRunRequest
	job       *schedpb.Job
	run       *schedpb.Run
	// Restoration (F-22): the last RestartRun request and an error to return
	// (e.g. a bad cut point surfacing as NotFound).
	lastRestart *schedpb.RestartRunRequest
	restartErr  error
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

func (f *fakeScheduler) TriggerRun(ctx context.Context, in *schedpb.TriggerRunRequest, opts ...grpc.CallOption) (*schedpb.Run, error) {
	f.triggered = in
	if f.run != nil {
		return f.run, nil
	}
	return &schedpb.Run{Id: 1, PipelineId: in.GetPipelineId(), Status: dbpb.RunStatus_RUN_STATUS_PENDING, Trigger: "webhook", TriggerName: in.GetTriggerName()}, nil
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

func (f *fakeScheduler) ResolveApproval(ctx context.Context, in *schedpb.ResolveApprovalRequest, opts ...grpc.CallOption) (*schedpb.Job, error) {
	f.resolved = in
	return f.job, nil
}

func (f *fakeScheduler) ProceedRun(ctx context.Context, in *schedpb.ProceedRunRequest, opts ...grpc.CallOption) (*schedpb.ProceedRunResponse, error) {
	return &schedpb.ProceedRunResponse{Run: f.run, Proceeded: 0}, nil
}

func (f *fakeScheduler) ProceedJob(ctx context.Context, in *schedpb.ProceedJobRequest, opts ...grpc.CallOption) (*schedpb.Job, error) {
	return f.job, nil
}

func (f *fakeScheduler) RestartRun(ctx context.Context, in *schedpb.RestartRunRequest, opts ...grpc.CallOption) (*schedpb.Run, error) {
	f.lastRestart = in
	if f.restartErr != nil {
		return nil, f.restartErr
	}
	if f.run != nil {
		return f.run, nil
	}
	return &schedpb.Run{Id: 1, PipelineId: 0, Status: dbpb.RunStatus_RUN_STATUS_PENDING, Trigger: "rollback"}, nil
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

// TestApproveJob verifies that POST /api/jobs/{id}/approve (F-13) forwards the
// job id and the "approved" decision to the scheduler's ResolveApproval RPC and
// returns the job.
func TestApproveJob(t *testing.T) {
	fake := &fakeScheduler{job: &schedpb.Job{Id: 9, Name: "build", Status: dbpb.JobStatus_JOB_STATUS_PENDING}}
	srv := New(Clients{Scheduler: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/jobs/9/approve", "application/json", nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	if fake.resolved == nil {
		t.Fatal("scheduler.ResolveApproval was not called")
	}
	if fake.resolved.GetId() != 9 {
		t.Errorf("resolved job id = %d, want 9", fake.resolved.GetId())
	}
	if fake.resolved.GetDecision() != "approved" {
		t.Errorf("decision = %q, want approved", fake.resolved.GetDecision())
	}
}

// TestRejectJob verifies that POST /api/jobs/{id}/reject (F-13) forwards the
// job id, the "rejected" decision, and an optional reason to the scheduler's
// ResolveApproval RPC and returns the job.
func TestRejectJob(t *testing.T) {
	fake := &fakeScheduler{job: &schedpb.Job{Id: 9, Name: "build", Status: dbpb.JobStatus_JOB_STATUS_PENDING}}
	srv := New(Clients{Scheduler: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := `{"reason": "not ready for production"}`
	resp, err := http.Post(ts.URL+"/api/jobs/9/reject", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	if fake.resolved == nil {
		t.Fatal("scheduler.ResolveApproval was not called")
	}
	if fake.resolved.GetId() != 9 {
		t.Errorf("resolved job id = %d, want 9", fake.resolved.GetId())
	}
	if fake.resolved.GetDecision() != "rejected" {
		t.Errorf("decision = %q, want rejected", fake.resolved.GetDecision())
	}
	if fake.resolved.GetReason() != "not ready for production" {
		t.Errorf("reason = %q, want %q", fake.resolved.GetReason(), "not ready for production")
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

// TestCreateRunForwardsPipelineVersion verifies that a run request carrying a
// pipeline_version is forwarded to the scheduler (F-11).
func TestCreateRunForwardsPipelineVersion(t *testing.T) {
	fake := &fakeScheduler{}
	srv := New(Clients{Scheduler: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := `{"trigger": "manual", "pipeline_version": 2}`
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
	if got := fake.created.GetPipelineVersion(); got != 2 {
		t.Errorf("pipeline_version = %d, want 2", got)
	}
}

// TestListPipelineVersions verifies that GET /api/pipelines/{id}/versions
// returns the pipeline's version history (F-11).
func TestListPipelineVersions(t *testing.T) {
	fake := &fakeDatabase{versions: &dbpb.ListPipelineVersionsResponse{
		Versions: []*dbpb.PipelineVersion{
			{PipelineId: 3, Version: 2, Name: "deploy v2"},
			{PipelineId: 3, Version: 1, Name: "deploy"},
		},
	}}
	srv := New(Clients{Database: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/pipelines/3/versions")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	var versions []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&versions); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("got %d versions, want 2", len(versions))
	}
	if versions[0]["version"] != float64(2) {
		t.Errorf("newest version = %v, want 2", versions[0]["version"])
	}
	if versions[1]["version"] != float64(1) {
		t.Errorf("oldest version = %v, want 1", versions[1]["version"])
	}
}

// TestGetPipelineVersion verifies that GET /api/pipelines/{id}/versions/{v}
// returns the snapshot of one version (F-11).
func TestGetPipelineVersion(t *testing.T) {
	fake := &fakeDatabase{version: &dbpb.PipelineVersion{
		PipelineId:  3,
		Version:     1,
		Name:        "deploy",
		Description: "deploys the app",
		Jobs:        []*dbpb.JobDefinition{{Key: "build", Name: "build"}},
	}}
	srv := New(Clients{Database: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/pipelines/3/versions/1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	var version map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&version); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if version["version"] != float64(1) {
		t.Errorf("version = %v, want 1", version["version"])
	}
	if version["name"] != "deploy" {
		t.Errorf("name = %v, want deploy", version["name"])
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
	versions            *dbpb.ListPipelineVersionsResponse
	version             *dbpb.PipelineVersion
	queue               *dbpb.QueueStatus
	// SSH known hosts (F-21.2.4.1) canned responses for the /api/ssh-known-hosts
	// handler tests.
	sshKnownHosts       *dbpb.ListSSHKnownHostsResponse
	createdSSHKnownHost *dbpb.SSHKnownHost
	deletedSSHKnownHost int64
	// Source credentials (F-21.3) canned responses for the
	// /api/source-credentials handler tests.
	sourceCredentials *dbpb.ListSourceCredentialsResponse
	createdSourceCred *dbpb.CreateSourceCredentialRequest
	updatedSourceCred *dbpb.UpdateSourceCredentialRequest
	deletedSourceCred string
	getSourceCred     *dbpb.SourceCredential
	getSourceCredErr  error
	// Pipeline sources (F-21.5.1, minimal slice for F-21.3) canned responses
	// for the /api/pipelines/{id}/source handler tests.
	pipelineSource      *dbpb.PipelineSource
	pipelineSourceState *dbpb.PipelineSourceState
	sourceRefCount      int64
	// Leases (F-23 / F-21.5.2 cross-replica sync serialization): simulate a
	// held lease so AcquireLease declines (leased=true, holder≠requester).
	mu     sync.Mutex
	leased bool
	holder string
	// Source-refresh loop (F-21.5.3) canned populations.
	sources      []*dbpb.PipelineSource
	allPipelines []*dbpb.Pipeline
	// Recorded re-import (F-21.5.2).
	updatedPipeline *dbpb.UpdatePipelineRequest
}

// GetQueueStatus returns the canned queue status (F-16) for the /api/queue
// handler tests.
func (f *fakeDatabase) GetQueueStatus(ctx context.Context, in *dbpb.GetQueueStatusRequest, opts ...grpc.CallOption) (*dbpb.QueueStatus, error) {
	if f.queue != nil {
		return f.queue, nil
	}
	return &dbpb.QueueStatus{}, nil
}

// ListSSHKnownHosts returns the canned SSH known hosts (F-21.2.4.1) for the
// /api/ssh-known-hosts handler tests.
func (f *fakeDatabase) ListSSHKnownHosts(ctx context.Context, in *dbpb.ListSSHKnownHostsRequest, opts ...grpc.CallOption) (*dbpb.ListSSHKnownHostsResponse, error) {
	if f.sshKnownHosts != nil {
		return f.sshKnownHosts, nil
	}
	return &dbpb.ListSSHKnownHostsResponse{}, nil
}

// CreateSSHKnownHost records the create request and returns a canned host
// (F-21.2.4.1) for the /api/ssh-known-hosts handler tests.
func (f *fakeDatabase) CreateSSHKnownHost(ctx context.Context, in *dbpb.CreateSSHKnownHostRequest, opts ...grpc.CallOption) (*dbpb.SSHKnownHost, error) {
	f.createdSSHKnownHost = &dbpb.SSHKnownHost{
		Id:          1,
		HostPattern: in.GetHostPattern(),
		KeyType:     in.GetKeyType(),
		Key:         in.GetKey(),
		Description: in.GetDescription(),
	}
	return f.createdSSHKnownHost, nil
}

// DeleteSSHKnownHost records the deleted id (F-21.2.4.1) for the
// /api/ssh-known-hosts handler tests.
func (f *fakeDatabase) DeleteSSHKnownHost(ctx context.Context, in *dbpb.DeleteSSHKnownHostRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	f.deletedSSHKnownHost = in.GetId()
	return &emptypb.Empty{}, nil
}

// ListSourceCredentials returns the canned source credentials (F-21.3) for the
// /api/source-credentials handler tests.
func (f *fakeDatabase) ListSourceCredentials(ctx context.Context, in *dbpb.ListSourceCredentialsRequest, opts ...grpc.CallOption) (*dbpb.ListSourceCredentialsResponse, error) {
	if f.sourceCredentials != nil {
		return f.sourceCredentials, nil
	}
	return &dbpb.ListSourceCredentialsResponse{}, nil
}

// GetSourceCredential returns the canned credential (F-21.3) for the
// /api/source-credentials handler tests.
func (f *fakeDatabase) GetSourceCredential(ctx context.Context, in *dbpb.GetSourceCredentialRequest, opts ...grpc.CallOption) (*dbpb.SourceCredential, error) {
	if f.getSourceCredErr != nil {
		return nil, f.getSourceCredErr
	}
	if f.getSourceCred != nil {
		return f.getSourceCred, nil
	}
	return &dbpb.SourceCredential{Name: in.GetName()}, nil
}

// CreateSourceCredential records the create request and returns a canned
// credential (F-21.3) for the /api/source-credentials handler tests.
func (f *fakeDatabase) CreateSourceCredential(ctx context.Context, in *dbpb.CreateSourceCredentialRequest, opts ...grpc.CallOption) (*dbpb.SourceCredential, error) {
	f.createdSourceCred = in
	return &dbpb.SourceCredential{
		Id:          1,
		Name:        in.GetName(),
		Kind:        in.GetKind(),
		Prefix:      in.GetPrefix(),
		Description: in.GetDescription(),
		Username:    in.GetUsername(),
		Token:       in.GetToken(),
		PrivateKey:  in.GetPrivateKey(),
		HostKey:     in.GetHostKey(),
		Headers:     in.GetHeaders(),
	}, nil
}

// UpdateSourceCredential records the update request and returns a canned
// credential (F-21.3) for the /api/source-credentials handler tests.
func (f *fakeDatabase) UpdateSourceCredential(ctx context.Context, in *dbpb.UpdateSourceCredentialRequest, opts ...grpc.CallOption) (*dbpb.SourceCredential, error) {
	f.updatedSourceCred = in
	return &dbpb.SourceCredential{
		Id:          1,
		Name:        in.GetName(),
		Kind:        "git-https",
		Prefix:      in.GetPrefix(),
		Description: in.GetDescription(),
		Username:    in.GetUsername(),
		Token:       in.GetToken(),
		Headers:     in.GetHeaders(),
	}, nil
}

// DeleteSourceCredential records the deleted name (F-21.3) for the
// /api/source-credentials handler tests.
func (f *fakeDatabase) DeleteSourceCredential(ctx context.Context, in *dbpb.DeleteSourceCredentialRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	f.deletedSourceCred = in.GetName()
	return &emptypb.Empty{}, nil
}

// AttachPipelineSource returns the canned source (F-21.5.1) for the
// /api/pipelines/{id}/source handler tests.
func (f *fakeDatabase) AttachPipelineSource(ctx context.Context, in *dbpb.AttachPipelineSourceRequest, opts ...grpc.CallOption) (*dbpb.PipelineSource, error) {
	return &dbpb.PipelineSource{
		Id:         1,
		PipelineId: in.GetPipelineId(),
		Kind:       in.GetKind(),
		Reference:  in.GetReference(),
		Credential: in.GetCredential(),
	}, nil
}

// AcquireLease simulates the Database service's lease semantics (F-23) for
// the source-refresh loop + per-pipeline sync-serialization tests: a lease is
// acquired iff nobody ELSE holds it (or the holder is the same identity — a
// renewal). Lease-held assertions flip leased=false.
func (f *fakeDatabase) AcquireLease(ctx context.Context, in *dbpb.AcquireLeaseRequest, opts ...grpc.CallOption) (*dbpb.AcquireLeaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leased && f.holder != in.GetHolder() {
		return &dbpb.AcquireLeaseResponse{Acquired: false}, nil
	}
	f.leased = true
	f.holder = in.GetHolder()
	return &dbpb.AcquireLeaseResponse{Acquired: true}, nil
}

// ReleaseLease flips the simulated lease state (F-23).
func (f *fakeDatabase) ReleaseLease(ctx context.Context, in *dbpb.ReleaseLeaseRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.holder == in.GetHolder() {
		f.leased = false
		f.holder = ""
	}
	return &emptypb.Empty{}, nil
}

// ListPipelineSources returns the canned source list (F-21.5.3) for the
// source-refresh loop tests.
func (f *fakeDatabase) ListPipelineSources(ctx context.Context, in *dbpb.ListPipelineSourcesRequest, opts ...grpc.CallOption) (*dbpb.ListPipelineSourcesResponse, error) {
	return &dbpb.ListPipelineSourcesResponse{Sources: f.sources}, nil
}

// ListPipelines returns the canned pipeline list (F-21.5.3) for the
// source-refresh loop tests.
func (f *fakeDatabase) ListPipelines(ctx context.Context, in *dbpb.ListPipelinesRequest, opts ...grpc.CallOption) (*dbpb.ListPipelinesResponse, error) {
	return &dbpb.ListPipelinesResponse{Pipelines: f.allPipelines}, nil
}

// UpdatePipeline records the update (F-21.5.2 re-import) and returns the
// canned pipeline.
func (f *fakeDatabase) UpdatePipeline(ctx context.Context, in *dbpb.UpdatePipelineRequest, opts ...grpc.CallOption) (*dbpb.Pipeline, error) {
	f.updatedPipeline = in
	if f.pipeline != nil {
		return f.pipeline, nil
	}
	return &dbpb.Pipeline{Id: in.GetId(), Name: in.GetName()}, nil
}

// GetPipelineSource returns the canned source (F-21.5.1) for the
// /api/pipelines/{id}/source handler tests.
func (f *fakeDatabase) GetPipelineSource(ctx context.Context, in *dbpb.GetPipelineSourceRequest, opts ...grpc.CallOption) (*dbpb.PipelineSource, error) {
	if f.pipelineSource != nil {
		return f.pipelineSource, nil
	}
	return &dbpb.PipelineSource{PipelineId: in.GetPipelineId(), Kind: "http", Reference: "http://example.com/file.yaml"}, nil
}

// CountSourcesByCredential returns the canned reference count (F-21.3.3's
// delete guard) for the /api/source-credentials handler tests.
func (f *fakeDatabase) CountSourcesByCredential(ctx context.Context, in *dbpb.CountSourcesByCredentialRequest, opts ...grpc.CallOption) (*dbpb.CountSourcesByCredentialResponse, error) {
	return &dbpb.CountSourcesByCredentialResponse{Count: f.sourceRefCount}, nil
}

// SetPipelineSourceState records the state (F-21.5.1) for the
// /api/pipelines/{id}/source/refresh handler tests. It MIRRORS the real
// database service's semantics: a failure record (EMPTY version/hash) must
// not WIPE the last-good survivors — they are preserved (the
// kickoff-provenance fallback leans on them).
func (f *fakeDatabase) SetPipelineSourceState(ctx context.Context, in *dbpb.SetPipelineSourceStateRequest, opts ...grpc.CallOption) (*dbpb.PipelineSourceState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prev := f.pipelineSourceState
	if prev == nil {
		prev = &dbpb.PipelineSourceState{}
	}
	next := &dbpb.PipelineSourceState{
		PipelineId:   in.GetPipelineId(),
		LastSyncedAt: in.GetLastSyncedAt(),
		LastError:    in.GetLastError(),
		LastVersion:  prev.GetLastVersion(),
		FileHash:     prev.GetFileHash(),
	}
	if in.GetLastVersion() != "" {
		next.LastVersion = in.GetLastVersion()
	}
	if in.GetFileHash() != "" {
		next.FileHash = in.GetFileHash()
	}
	f.pipelineSourceState = next
	return next, nil
}

// GetPipelineSourceState returns the canned state (F-21.5.1) for the
// /api/pipelines/{id}/source handler tests.
func (f *fakeDatabase) GetPipelineSourceState(ctx context.Context, in *dbpb.GetPipelineSourceStateRequest, opts ...grpc.CallOption) (*dbpb.PipelineSourceState, error) {
	if f.pipelineSourceState != nil {
		return f.pipelineSourceState, nil
	}
	return nil, status.Error(codes.NotFound, "no source state")
}

// DetachPipelineSource is a no-op (F-21.5.1) for the /api/pipelines/{id}/source
// handler tests.
func (f *fakeDatabase) DetachPipelineSource(ctx context.Context, in *dbpb.DetachPipelineSourceRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeDatabase) CreatePipeline(ctx context.Context, in *dbpb.CreatePipelineRequest, opts ...grpc.CallOption) (*dbpb.Pipeline, error) {
	f.createdPipeline = in
	if f.pipeline != nil {
		return f.pipeline, nil
	}
	return &dbpb.Pipeline{Id: 1, Name: in.GetName()}, nil
}

func (f *fakeDatabase) GetPipeline(ctx context.Context, in *dbpb.GetPipelineRequest, opts ...grpc.CallOption) (*dbpb.Pipeline, error) {
	if f.pipeline != nil {
		return f.pipeline, nil
	}
	return &dbpb.Pipeline{Id: in.GetId(), Name: "pipeline"}, nil
}

func (f *fakeDatabase) CreateJob(ctx context.Context, in *dbpb.CreateJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	f.createdJob = in
	if f.job != nil {
		return f.job, nil
	}
	return &dbpb.Job{Id: 1, Name: in.GetName(), Status: dbpb.JobStatus_JOB_STATUS_PENDING}, nil
}

func (f *fakeDatabase) ListPipelineVersions(ctx context.Context, in *dbpb.ListPipelineVersionsRequest, opts ...grpc.CallOption) (*dbpb.ListPipelineVersionsResponse, error) {
	if f.versions != nil {
		return f.versions, nil
	}
	return &dbpb.ListPipelineVersionsResponse{}, nil
}

func (f *fakeDatabase) GetPipelineVersion(ctx context.Context, in *dbpb.GetPipelineVersionRequest, opts ...grpc.CallOption) (*dbpb.PipelineVersion, error) {
	if f.version != nil {
		return f.version, nil
	}
	return &dbpb.PipelineVersion{PipelineId: in.GetPipelineId(), Version: in.GetVersion()}, nil
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

// TestCreatePipelineFromFileFormat verifies that POST /api/pipelines accepts a
// definition in the file format (F-21.1.4 import): a YAML document with an
// apiVersion, kind, metadata, and spec is run through the compiler and mapped
// onto the schema the API hands to the database service. The pipeline's named
// variable sets and secret sources are carried to the database service, and
// each job's effective variables (its inline variables merged with the
// variables of the variable sets it references, the inline variables winning
// on a key collision) are computed at import time and carried on the job
// definition.
func TestCreatePipelineFromFileFormat(t *testing.T) {
	fake := &fakeDatabase{}
	srv := New(Clients{Database: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: deploy
  description: a file-defined pipeline
spec:
  workerGroup: linux-pool
  failureMode: best_effort
  variableSets:
    common:
      variables:
        region: us-east-1
        db_password:
          encrypted: enc-1
  secretSources:
    vault:
      kind: vault
      path: secret/data/app
      audience: cdrom
  jobs:
    build:
      steps:
        compile:
          shell: go build ./...
      variables:
        region: eu-west-1
        api_token:
          secretSource: vault
          key: token
      variableSets:
        - common
`
	resp, err := http.Post(ts.URL+"/api/pipelines", "application/yaml", bytes.NewBufferString(body))
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
	req := fake.createdPipeline
	// The file's metadata and pipeline-level deployment options are mapped.
	if req.GetName() != "deploy" {
		t.Errorf("name = %q, want deploy", req.GetName())
	}
	if req.GetFailureMode() != dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT {
		t.Errorf("failure mode = %v, want BEST_EFFORT", req.GetFailureMode())
	}
	// The pipeline's named variable sets and secret sources are carried to the
	// database service.
	if got := len(req.GetVariableSets()); got != 1 {
		t.Fatalf("variable sets = %d, want 1", got)
	}
	if got := len(req.GetSecretSources()); got != 1 {
		t.Fatalf("secret sources = %d, want 1", got)
	}
	if src := req.GetSecretSources()[0]; src.GetName() != "vault" || src.GetKind() != "vault" || src.GetPath() != "secret/data/app" {
		t.Errorf("secret source = %+v, want vault/vault/secret/data/app", src)
	}
	// The single job: its worker group defaults to the pipeline's
	// spec.workerGroup, and its effective variables are the inline variables
	// merged with the referenced set's variables (inline wins on a collision).
	if got := len(req.GetJobs()); got != 1 {
		t.Fatalf("jobs = %d, want 1", got)
	}
	job := req.GetJobs()[0]
	if job.GetKey() != "build" {
		t.Errorf("job key = %q, want build", job.GetKey())
	}
	if job.GetTargetGroup() != "linux-pool" {
		t.Errorf("job target group = %q, want linux-pool (the pipeline's workerGroup)", job.GetTargetGroup())
	}
	if job.GetSpec() == nil || len(job.GetSpec().GetSteps()) != 1 {
		t.Errorf("job spec = %+v, want one step", job.GetSpec())
	}
	vars := map[string]*dbpb.Variable{}
	for _, v := range job.GetVariables() {
		vars[v.GetName()] = v
	}
	if got := len(vars); got != 3 {
		t.Fatalf("job variables = %d, want 3 (region, db_password, api_token)", got)
	}
	// region: the job's inline value wins over the set's.
	if v := vars["region"]; v == nil || v.GetShape() != dbpb.VariableShape_VARIABLE_PLAIN || v.GetPlain() != "eu-west-1" {
		t.Errorf("region = %+v, want plain eu-west-1 (inline wins)", v)
	}
	// db_password: from the set (encrypted).
	if v := vars["db_password"]; v == nil || v.GetShape() != dbpb.VariableShape_VARIABLE_ENCRYPTED || v.GetEncrypted() != "enc-1" {
		t.Errorf("db_password = %+v, want encrypted enc-1 (from the set)", v)
	}
	// api_token: sourced from the named secret source.
	if v := vars["api_token"]; v == nil || v.GetShape() != dbpb.VariableShape_VARIABLE_SECRET_SOURCE || v.GetSecretSource() != "vault" || v.GetKey() != "token" {
		t.Errorf("api_token = %+v, want sourced from vault/token", v)
	}
}

// TestExportPipelineEndpoint verifies that GET /api/pipelines/{id}/export
// (F-21.1.4) renders the pipeline's compiled definition back into the file
// format (YAML): the response is a valid file-format document (apiVersion,
// kind, metadata, spec) that re-imports to the same compiled definition
// (round-trip).
func TestExportPipelineEndpoint(t *testing.T) {
	fake := &fakeDatabase{
		pipeline: &dbpb.Pipeline{
			Id:          7,
			Name:        "deploy",
			Description: "an exported pipeline",
			FailureMode: dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT,
			VariableSets: []*dbpb.VariableSet{
				{Name: "common", Variables: []*dbpb.Variable{{Name: "region", Shape: dbpb.VariableShape_VARIABLE_PLAIN, Plain: "us-east-1"}}},
			},
			SecretSources: []*dbpb.SecretSource{{Name: "vault", Kind: "vault", Path: "secret/data/app"}},
			Jobs: []*dbpb.JobDefinition{{
				Key:         "build",
				Name:        "build",
				TargetGroup: "linux-pool",
				Variables:   []*dbpb.Variable{{Name: "region", Shape: dbpb.VariableShape_VARIABLE_PLAIN, Plain: "eu-west-1"}},
				Spec:        &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Name: "compile", Type: "shell", Params: map[string]*dbpb.ParamValue{"command": {String_: "go build ./..."}}}}},
			}},
		},
	}
	srv := New(Clients{Database: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/pipelines/7/export")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/yaml" {
		t.Errorf("content-type = %q, want application/yaml", ct)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	// The export is a valid file-format document that re-imports.
	req, variableSets, secretSources, err := pipelineFileToRequest("", data)
	if err != nil {
		t.Fatalf("re-import export: %v (export was: %s)", err, data)
	}
	if req.Name != "deploy" {
		t.Errorf("re-imported name = %q, want deploy", req.Name)
	}
	if got := len(variableSets); got != 1 {
		t.Errorf("re-imported variable sets = %d, want 1", got)
	}
	if got := len(secretSources); got != 1 {
		t.Errorf("re-imported secret sources = %d, want 1", got)
	}
	if got := len(req.Jobs); got != 1 {
		t.Fatalf("re-imported jobs = %d, want 1", got)
	}
	job := req.Jobs[0]
	if job.Key != "build" || job.WorkerGroup != "linux-pool" {
		t.Errorf("re-imported job = key %q group %q, want build/linux-pool", job.Key, job.WorkerGroup)
	}
	if job.Spec == nil || len(job.Spec.Steps) != 1 || job.Spec.Steps[0].Name != "compile" {
		t.Errorf("re-imported steps = %+v, want one step named compile", job.Spec)
	}
	if got := len(job.Variables); got != 1 || job.Variables[0].GetName() != "region" || job.Variables[0].GetPlain() != "eu-west-1" {
		t.Errorf("re-imported variables = %+v, want region=eu-west-1", job.Variables)
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

// webhookPipeline returns a canned pipeline with a single webhook trigger (the
// given secret) for the API's webhook handler tests (F-09).
func webhookPipeline(id int64, secret string) *dbpb.Pipeline {
	return &dbpb.Pipeline{
		Id:   id,
		Name: "deploy",
		Triggers: []*dbpb.Trigger{
			{Name: "hook", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK, Secret: secret,
				Params: map[string]string{"env": "staging"}},
		},
	}
}

// doWebhook posts to the pipeline's webhook endpoint with the given secret
// header and JSON body.
func doWebhook(t *testing.T, ts *httptest.Server, pipelineID int64, secret, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+fmt.Sprintf("/api/pipelines/%d/webhook", pipelineID), bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		req.Header.Set(webhookSecretHeader, secret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

// TestWebhookStartsRun verifies that a webhook POST with a valid secret
// (F-09) starts a run of the pipeline via the scheduler's TriggerRun RPC,
// merging the trigger's static params with the request's JSON body (the body
// wins on a key collision).
func TestWebhookStartsRun(t *testing.T) {
	fakeDB := &fakeDatabase{pipeline: webhookPipeline(3, "s3cret")}
	fakeSched := &fakeScheduler{}
	srv := New(Clients{Database: fakeDB, Scheduler: fakeSched}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := doWebhook(t, ts, 3, "s3cret", `{"commit": "abc123", "env": "prod"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if fakeSched.triggered == nil {
		t.Fatal("scheduler.TriggerRun was not called")
	}
	if fakeSched.triggered.GetPipelineId() != 3 {
		t.Errorf("pipeline id = %d, want 3", fakeSched.triggered.GetPipelineId())
	}
	if fakeSched.triggered.GetTriggerName() != "hook" {
		t.Errorf("trigger = %q, want hook", fakeSched.triggered.GetTriggerName())
	}
	if fakeSched.triggered.GetTriggerType() != dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK {
		t.Errorf("type = %v, want WEBHOOK", fakeSched.triggered.GetTriggerType())
	}
	// The static param (env=staging) is overridden by the body (env=prod);
	// the body's commit param is added.
	if got := fakeSched.triggered.GetParams()["env"]; got != "prod" {
		t.Errorf("params[env] = %q, want prod (body overrides static)", got)
	}
	if got := fakeSched.triggered.GetParams()["commit"]; got != "abc123" {
		t.Errorf("params[commit] = %q, want abc123", got)
	}
}

// TestWebhookEmptyBody verifies that a webhook POST with a valid secret and an
// empty body starts a run with the trigger's static params only (F-09).
func TestWebhookEmptyBody(t *testing.T) {
	fakeDB := &fakeDatabase{pipeline: webhookPipeline(3, "s3cret")}
	fakeSched := &fakeScheduler{}
	srv := New(Clients{Database: fakeDB, Scheduler: fakeSched}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := doWebhook(t, ts, 3, "s3cret", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if fakeSched.triggered == nil {
		t.Fatal("scheduler.TriggerRun was not called")
	}
	if got := fakeSched.triggered.GetParams()["env"]; got != "staging" {
		t.Errorf("params[env] = %q, want staging (static param)", got)
	}
}

// TestWebhookRejectsBadSecret verifies that a webhook POST with a secret that
// matches none of the pipeline's webhook triggers is rejected with 401 (F-09).
func TestWebhookRejectsBadSecret(t *testing.T) {
	fakeDB := &fakeDatabase{pipeline: webhookPipeline(3, "s3cret")}
	fakeSched := &fakeScheduler{}
	srv := New(Clients{Database: fakeDB, Scheduler: fakeSched}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := doWebhook(t, ts, 3, "wrong", `{"commit": "abc"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if fakeSched.triggered != nil {
		t.Error("scheduler.TriggerRun was called for a bad secret, want it rejected")
	}
}

// TestWebhookRejectsMissingSecret verifies that a webhook POST with no secret
// header is rejected with 401 (F-09).
func TestWebhookRejectsMissingSecret(t *testing.T) {
	fakeDB := &fakeDatabase{pipeline: webhookPipeline(3, "s3cret")}
	fakeSched := &fakeScheduler{}
	srv := New(Clients{Database: fakeDB, Scheduler: fakeSched}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := doWebhook(t, ts, 3, "", `{"commit": "abc"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if fakeSched.triggered != nil {
		t.Error("scheduler.TriggerRun was called for a missing secret, want it rejected")
	}
}

// TestWebhookOpenTrigger verifies that a webhook trigger with an empty secret
// is open: any POST (with or without a secret header) starts a run (F-09).
func TestWebhookOpenTrigger(t *testing.T) {
	fakeDB := &fakeDatabase{pipeline: webhookPipeline(3, "")}
	fakeSched := &fakeScheduler{}
	srv := New(Clients{Database: fakeDB, Scheduler: fakeSched}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// No secret header at all.
	resp := doWebhook(t, ts, 3, "", `{"commit": "abc"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if fakeSched.triggered == nil {
		t.Fatal("scheduler.TriggerRun was not called for an open trigger")
	}
	if fakeSched.triggered.GetTriggerName() != "hook" {
		t.Errorf("trigger = %q, want hook", fakeSched.triggered.GetTriggerName())
	}
	if got := fakeSched.triggered.GetParams()["env"]; got != "staging" {
		t.Errorf("params[env] = %q, want staging (static param)", got)
	}
	if got := fakeSched.triggered.GetParams()["commit"]; got != "abc" {
		t.Errorf("params[commit] = %q, want abc", got)
	}
}

// TestWebhookNoTrigger verifies that a webhook POST to a pipeline with no
// webhook trigger is rejected with 404 (F-09).
func TestWebhookNoTrigger(t *testing.T) {
	fakeDB := &fakeDatabase{pipeline: &dbpb.Pipeline{Id: 3, Name: "deploy"}}
	fakeSched := &fakeScheduler{}
	srv := New(Clients{Database: fakeDB, Scheduler: fakeSched}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := doWebhook(t, ts, 3, "s3cret", `{"commit": "abc"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if fakeSched.triggered != nil {
		t.Error("scheduler.TriggerRun was called for a pipeline with no webhook trigger")
	}
}

// TestWebhookRejectsInvalidBody verifies that a webhook POST with a valid
// secret but a malformed JSON body is rejected with 400 (F-09).
func TestWebhookRejectsInvalidBody(t *testing.T) {
	fakeDB := &fakeDatabase{pipeline: webhookPipeline(3, "s3cret")}
	fakeSched := &fakeScheduler{}
	srv := New(Clients{Database: fakeDB, Scheduler: fakeSched}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := doWebhook(t, ts, 3, "s3cret", `{not valid json`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if fakeSched.triggered != nil {
		t.Error("scheduler.TriggerRun was called for an invalid body, want it rejected")
	}
}

// testSecretStore builds a real AES-256-GCM store backed by a throwaway nonce
// counter, so the API's encrypt endpoint can be exercised end to end.
func testSecretStore(t *testing.T) secrets.Store {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	store, err := secrets.NewStore(
		config.SecretsConfig{Kind: "aes", Key: base64.StdEncoding.EncodeToString(key)},
		&testNonceSource{},
	)
	if err != nil {
		t.Fatalf("secrets.NewStore: %v", err)
	}
	return store
}

// testNonceSource hands out an increasing nonce sequence.
type testNonceSource struct{ next uint64 }

func (n *testNonceSource) NextNonce(context.Context) (uint64, error) {
	n.next++
	return n.next, nil
}

// TestEncryptSecret verifies that POST /api/secrets/encrypt returns a
// ciphertext that decrypts back to the supplied value, and that two
// encryptions of the same value differ (a fresh nonce is drawn each time).
func TestEncryptSecret(t *testing.T) {
	store := testSecretStore(t)
	srv := New(Clients{}, nil, store)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	encrypt := func(value string) string {
		resp, err := http.Post(ts.URL+"/api/secrets/encrypt", "application/json",
			bytes.NewBufferString(`{"value":`+jsonString(value)+`}`))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			data, _ := io.ReadAll(resp.Body)
			t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, data)
		}
		var out struct {
			Ciphertext string `json:"ciphertext"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return out.Ciphertext
	}

	plaintext := "hunter2"
	c1 := encrypt(plaintext)
	c2 := encrypt(plaintext)
	if c1 == "" {
		t.Fatal("ciphertext is empty")
	}
	if c1 == c2 {
		t.Error("two encryptions of the same value produced identical ciphertexts, want distinct nonces")
	}
	decrypted, err := store.Decrypt(context.Background(), c1)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if decrypted != plaintext {
		t.Errorf("decrypt = %q, want %q", decrypted, plaintext)
	}
}

// TestEncryptSecretNoStore verifies that the endpoint rejects the request when
// no secret store is configured (a test-only situation; in production the
// built-in AES store is always available, falling back to an all-zero key).
func TestEncryptSecretNoStore(t *testing.T) {
	srv := New(Clients{}, nil) // no secret store
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/secrets/encrypt", "application/json",
		bytes.NewBufferString(`{"value":"x"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 400 (body %s)", resp.StatusCode, data)
	}
}

// TestEncryptSecretInvalidBody verifies that a malformed body is rejected with
// a 400 before any encryption is attempted.
func TestEncryptSecretInvalidBody(t *testing.T) {
	srv := New(Clients{}, nil, testSecretStore(t))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/secrets/encrypt", "application/json",
		bytes.NewBufferString(`{not valid json`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// jsonString renders a Go string as a JSON string literal.
func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
