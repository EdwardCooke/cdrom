package target

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// testLogger returns a silent logger so test output stays clean.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// testStepType is a step type a test registers to capture the executor's
// context, so a test can assert the run context RunJob built.
const testStepType = "target-test-step"

// captureContext registers a step handler (under testStepType) that records
// the context it runs with, so a test can inspect the executor's run context
// RunJob built. It returns a function that yields the captured context (or
// nil if the step never ran).
func captureContext() func() context.Context {
	var captured context.Context
	var ran bool
	executor.RegisterStepType(testStepType, func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		captured = ctx
		ran = true
		return nil
	})
	return func() context.Context {
		if !ran {
			return nil
		}
		return captured
	}
}

// fakeTargetAPI is a stub APIClient for target tests. Only the methods the
// shared execution uses are implemented; the rest are never called.
// StreamJobLogs fails so the log sink (best-effort) does not block the job.
type fakeTargetAPI struct {
	apipb.APIClient // nil; only the methods below are used by the shared execution
	mu              sync.Mutex
	lastReport      *apipb.ReportJobStatusRequest
}

func (f *fakeTargetAPI) ReportJobStatus(ctx context.Context, in *apipb.ReportJobStatusRequest, opts ...grpc.CallOption) (*apipb.Job, error) {
	f.mu.Lock()
	f.lastReport = in
	f.mu.Unlock()
	return &apipb.Job{Id: in.GetJobId(), Status: in.GetStatus()}, nil
}

func (f *fakeTargetAPI) StreamJobLogs(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[apipb.JobLogChunk, apipb.StreamJobLogsResponse], error) {
	return nil, status.Error(codes.Unavailable, "streaming disabled in test")
}

// TestRunJobBuildsRunContext verifies that RunJob builds the executor's run
// context the way both targets used to: the log sink, the step-status
// reporter, the upstream jobs and the job's identity (for a step's
// condition), the run's parameters and identity (for spec interpolation), and
// the token exchanger are all set, and the step barrier is set only when the
// job has the step-barrier flag set and targets a worker group.
func TestRunJobBuildsRunContext(t *testing.T) {
	captured := captureContext()
	api := &fakeTargetAPI{}
	tc := &Context{API: api, WorkerName: "w1", Token: "tok", Logger: testLogger()}
	job := &apipb.Job{
		Id:          7,
		Name:        "j",
		TargetGroup: "g",
		StepBarrier: true,
		RunId:       9,
		PipelineId:  3,
		TriggerType: "manual",
		TriggerName: "t",
		RunParams:   map[string]string{"p": "v"},
		Spec:        &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Type: testStepType}}},
	}
	collector := &executor.StepResultCollector{}
	if err := RunJob(context.Background(), tc, job, collector); err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	ctx := captured()
	if ctx == nil {
		t.Fatal("step handler was not run (the run context was not built)")
	}
	if executor.LogSinkFromContext(ctx) == nil {
		t.Error("log sink not set in the run context")
	}
	if executor.StepStatusReporterFromContext(ctx) != collector {
		t.Error("step-status reporter not set in the run context")
	}
	if got := executor.UpstreamJobsFromContext(ctx); len(got) != 0 {
		t.Errorf("upstream jobs = %v, want none", got)
	}
	if id := executor.JobIdentityFromContext(ctx); id.ID != 7 || id.Name != "j" || id.Status != "running" {
		t.Errorf("job identity = %+v, want {7 j running}", id)
	}
	run := executor.RunInfoFromContext(ctx)
	if run.ID != 9 || run.PipelineID != 3 || run.Trigger != "manual" || run.TriggerName != "t" || run.Params["p"] != "v" {
		t.Errorf("run info = %+v, want {9 3 manual t p=v}", run)
	}
	if executor.TokenExchangeFromContext(ctx) == nil {
		t.Error("token exchanger not set in the run context")
	}
	if executor.StepBarrierFromContext(ctx) != tc.StepBarrier {
		t.Error("step barrier not set in the run context for a barriered group job")
	}
}

// TestRunJobNoBarrierForAgent verifies that a job that does not target a
// worker group (an ephemeral agent) runs its steps unbarriered: the step
// barrier is not set in the run context even when the job has the flag set.
func TestRunJobNoBarrierForAgent(t *testing.T) {
	captured := captureContext()
	api := &fakeTargetAPI{}
	tc := &Context{API: api, Token: "tok", Logger: testLogger()}
	job := &apipb.Job{
		Id:          8,
		Name:        "agent-job",
		StepBarrier: true, // set, but the job targets no group
		Spec:        &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Type: testStepType}}},
	}
	if err := RunJob(context.Background(), tc, job, &executor.StepResultCollector{}); err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	ctx := captured()
	if ctx == nil {
		t.Fatal("step handler was not run")
	}
	if executor.StepBarrierFromContext(ctx) != nil {
		t.Error("step barrier set in the run context for a job that targets no group")
	}
}

// TestReportStatus verifies that ReportStatus reports the job's final status
// to the API (presenting the job's token) and attaches the collected step
// results and the target's name.
func TestReportStatus(t *testing.T) {
	api := &fakeTargetAPI{}
	tc := &Context{API: api, WorkerName: "w1", Token: "tok", Logger: testLogger()}
	collector := &executor.StepResultCollector{}
	collector.ReportStepStatus(0, executor.StepStatusSucceeded, "", nil)
	ReportStatus(context.Background(), tc, 7, dbpb.JobStatus_JOB_STATUS_SUCCEEDED, collector)
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.lastReport == nil {
		t.Fatal("ReportJobStatus was not called")
	}
	if api.lastReport.GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("status = %v, want SUCCEEDED", api.lastReport.GetStatus())
	}
	if api.lastReport.GetWorkerName() != "w1" {
		t.Errorf("worker name = %q, want w1", api.lastReport.GetWorkerName())
	}
	if got := api.lastReport.GetStepResults(); len(got) != 1 || got[0].GetIndex() != 0 {
		t.Errorf("step results = %v, want one step at index 0", got)
	}
}

// TestRunJobTimeout verifies that a job whose step exceeds its timeout is
// surfaced as executor.ErrTimeout (the target reports it as timed_out, F-03).
func TestRunJobTimeout(t *testing.T) {
	executor.RegisterStepType("target-test-timeout", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	})
	api := &fakeTargetAPI{}
	tc := &Context{API: api, Token: "tok", Logger: testLogger()}
	job := &apipb.Job{
		Id:   9,
		Name: "slow",
		Spec: &dbpb.JobSpec{
			Timeout: &durationpb.Duration{Seconds: 1},
			Steps:   []*dbpb.JobStep{{Type: "target-test-timeout"}},
		},
	}
	err := RunJob(context.Background(), tc, job, &executor.StepResultCollector{})
	if err == nil {
		t.Fatal("RunJob: want a timeout error, got nil")
	}
	if !errors.Is(err, executor.ErrTimeout) {
		t.Errorf("RunJob error = %v, want it to wrap executor.ErrTimeout", err)
	}
}

// TestFinishJob verifies the shared post-RunJob status decision (F-03/F-05):
// a user cancellation (the cancelled flag) or a step-barrier cancellation
// (executor.ErrCancelled) is reported as cancelled, a timeout as timed_out,
// any other error as failed, and a nil error as succeeded. The status
// reported to the API matches the status returned.
func TestFinishJob(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		cancelled bool
		want      dbpb.JobStatus
	}{
		{"succeeded", nil, false, dbpb.JobStatus_JOB_STATUS_SUCCEEDED},
		{"cancelled (user)", errors.New("interrupted"), true, dbpb.JobStatus_JOB_STATUS_CANCELLED},
		{"cancelled (step barrier)", executor.ErrCancelled, false, dbpb.JobStatus_JOB_STATUS_CANCELLED},
		{"timed out", executor.ErrTimeout, false, dbpb.JobStatus_JOB_STATUS_TIMED_OUT},
		{"failed", errors.New("boom"), false, dbpb.JobStatus_JOB_STATUS_FAILED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := &fakeTargetAPI{}
			ctx := &Context{API: api, WorkerName: "w1", Token: "tok", Logger: testLogger()}
			got := FinishJob(context.Background(), ctx, 7, &executor.StepResultCollector{}, tc.err, tc.cancelled)
			if got != tc.want {
				t.Errorf("FinishJob = %v, want %v", got, tc.want)
			}
			api.mu.Lock()
			defer api.mu.Unlock()
			if api.lastReport == nil {
				t.Fatal("ReportJobStatus was not called")
			}
			if api.lastReport.GetStatus() != tc.want {
				t.Errorf("reported status = %v, want %v", api.lastReport.GetStatus(), tc.want)
			}
		})
	}
}
