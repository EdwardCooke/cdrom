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

// fakeScheduler is a stub SchedulerClient that records the last SubmitJob
// request and returns a canned job.
type fakeScheduler struct {
	submitted *schedpb.SubmitJobRequest
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
				{"command": "go", "args": ["build", "./..."], "workdir": "repo", "env": {"GOFLAGS": "-mod=vendor"}},
				{"command": "sh", "args": ["-c", "make test"], "timeout": "30s"}
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
	if step0.GetCommand() != "go" || step0.GetWorkdir() != "repo" || step0.GetEnv()["GOFLAGS"] != "-mod=vendor" {
		t.Errorf("step 0 = %+v, want go build in repo with GOFLAGS", step0)
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
		`{"name": "x", "spec": {"steps": [{"args": ["build"]}]}}`,
		`{"name": "x", "spec": {"steps": [{"command": "go", "timeout": "not-a-duration"}]}}`,
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
		{Command: "go", Args: []string{"build"}, Timeout: "1m"},
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
	spec := &jobSpecRequest{Steps: []jobStepRequest{{Command: "sleep", Timeout: "45s"}}}
	proto, err := spec.toProtoSpec()
	if err != nil {
		t.Fatalf("toProtoSpec: %v", err)
	}
	if got := proto.GetSteps()[0].GetTimeout().AsDuration(); got != 45*time.Second {
		t.Errorf("timeout = %s, want 45s", got)
	}
}

// TestToProtoSpecStepType verifies that a step's type and params are carried
// into the proto, and that a non-shell step type does not require a command
// (only the built-in shell handler does).
func TestToProtoSpecStepType(t *testing.T) {
	spec := &jobSpecRequest{Steps: []jobStepRequest{
		{Type: "ansible", Params: map[string]string{"inventory": "prod"}},
	}}
	proto, err := spec.toProtoSpec()
	if err != nil {
		t.Fatalf("toProtoSpec: %v", err)
	}
	step := proto.GetSteps()[0]
	if step.GetType() != "ansible" {
		t.Errorf("type = %q, want %q", step.GetType(), "ansible")
	}
	if step.GetParams()["inventory"] != "prod" {
		t.Errorf("params = %v, want inventory=prod", step.GetParams())
	}

	// A shell step (empty type) with no command is still rejected.
	if _, err := (&jobSpecRequest{Steps: []jobStepRequest{{}}}).toProtoSpec(); err == nil {
		t.Error("expected an error for a shell step with no command, got nil")
	}
}
