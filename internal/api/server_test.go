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

// fakeScheduler is a stub SchedulerClient that records the last SubmitJob and
// RerunJob requests and returns a canned job.
type fakeScheduler struct {
	submitted *schedpb.SubmitJobRequest
	rerun     *schedpb.RerunJobRequest
	job       *schedpb.Job
}

func (f *fakeScheduler) SubmitJob(ctx context.Context, in *schedpb.SubmitJobRequest, opts ...grpc.CallOption) (*schedpb.Job, error) {
	f.submitted = in
	return f.job, nil
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
