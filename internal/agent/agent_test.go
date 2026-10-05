package agent

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// testLogger returns a silent logger so test output stays clean.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fakeAgentAPI is a stub APIClient for agent tests. Only the methods an agent
// uses are implemented; the rest are never called. GetJob returns the job's
// stored status until a short delay elapses, after which it reports the job
// as cancelled (simulating the scheduler's CancelJob marking it cancelled in
// the database, which the agent observes on its next poll, F-05).
// StreamJobLogs fails so the log sink (best-effort) does not block the job.
type fakeAgentAPI struct {
	apipb.APIClient // nil; only the methods below are used by the agent
	mu              sync.Mutex
	statuses        []dbpb.JobStatus
	job             *apipb.Job
	start           time.Time
	// exchanges records the audiences the token_exchange step handler
	// requested a new job token for.
	exchanges []string
}

func (f *fakeAgentAPI) GetJob(ctx context.Context, in *apipb.GetJobRequest, opts ...grpc.CallOption) (*apipb.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.start.IsZero() {
		f.start = time.Now()
	}
	// Construct a fresh Job (copying a proto message would copy its internal
	// mutex). After a short delay the job is cancelled (the scheduler marked
	// it so).
	status := f.job.GetStatus()
	if time.Since(f.start) > 100*time.Millisecond {
		status = dbpb.JobStatus_JOB_STATUS_CANCELLED
	}
	return &apipb.Job{
		Id:          f.job.GetId(),
		Name:        f.job.GetName(),
		Status:      status,
		TargetGroup: f.job.GetTargetGroup(),
		Spec:        f.job.GetSpec(),
		Token:       f.job.GetToken(),
	}, nil
}

func (f *fakeAgentAPI) ReportJobStatus(ctx context.Context, in *apipb.ReportJobStatusRequest, opts ...grpc.CallOption) (*apipb.Job, error) {
	f.mu.Lock()
	f.statuses = append(f.statuses, in.GetStatus())
	f.mu.Unlock()
	return &apipb.Job{Id: in.GetJobId(), Status: in.GetStatus()}, nil
}

func (f *fakeAgentAPI) StreamJobLogs(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[apipb.JobLogChunk, apipb.StreamJobLogsResponse], error) {
	return nil, status.Error(codes.Unavailable, "streaming disabled in test")
}

// ExchangeJobToken mints a new job token for a different audience (the
// token_exchange step handler). The fake records the (job, audience) and
// returns a token derived from the audience so a test can assert the exchange
// happened.
func (f *fakeAgentAPI) ExchangeJobToken(ctx context.Context, in *apipb.ExchangeJobTokenRequest, opts ...grpc.CallOption) (*apipb.ExchangeJobTokenResponse, error) {
	f.mu.Lock()
	f.exchanges = append(f.exchanges, in.GetAudience())
	f.mu.Unlock()
	return &apipb.ExchangeJobTokenResponse{Token: "token-for-" + in.GetAudience()}, nil
}

// TestAgentCancelInterruptsRunningJob verifies cancellation propagation for an
// ephemeral agent (F-05): the agent polls the API for its job's status and,
// when it observes the job cancelled, interrupts the running step (terminating
// the command) and reports the job as cancelled.
func TestAgentCancelInterruptsRunningJob(t *testing.T) {
	// A step that blocks until its context is done: a stand-in for a long
	// command that must be interrupted on cancel.
	executor.RegisterStepType("agent-cancel-blocker", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		<-ctx.Done()
		return ctx.Err()
	})

	// Shorten the poll interval so the test is fast.
	orig := agentCancelPollInterval
	agentCancelPollInterval = 20 * time.Millisecond
	t.Cleanup(func() { agentCancelPollInterval = orig })

	api := &fakeAgentAPI{job: &apipb.Job{
		Id:     7,
		Name:   "long",
		Status: dbpb.JobStatus_JOB_STATUS_PENDING,
		Spec:   &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Type: "agent-cancel-blocker"}}},
	}}
	a := New(7, Dependencies{API: api}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status, err := a.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != dbpb.JobStatus_JOB_STATUS_CANCELLED {
		t.Errorf("status = %v, want CANCELLED", status)
	}

	// The agent reported running and then cancelled.
	api.mu.Lock()
	got := append([]dbpb.JobStatus(nil), api.statuses...)
	api.mu.Unlock()
	var sawRunning, sawCancelled bool
	for _, s := range got {
		if s == dbpb.JobStatus_JOB_STATUS_RUNNING {
			sawRunning = true
		}
		if s == dbpb.JobStatus_JOB_STATUS_CANCELLED {
			sawCancelled = true
		}
	}
	if !sawRunning {
		t.Errorf("agent did not report RUNNING (reported %v)", got)
	}
	if !sawCancelled {
		t.Errorf("agent did not report CANCELLED (reported %v)", got)
	}
}

// TestAgentTokenExchangeStep verifies that an agent running a job with a
// token_exchange step calls the API's ExchangeJobToken RPC (presenting the
// job's own token) for the step's audience and reports the job succeeded.
func TestAgentTokenExchangeStep(t *testing.T) {
	// The job completes (the token_exchange step is fast) well before the fake
	// API's 100ms cancellation delay, so it reports succeeded.
	api := &fakeAgentAPI{job: &apipb.Job{
		Id:    9,
		Name:  "exchange",
		Token: "job-token-9",
		Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{
			{
				Type:    "token_exchange",
				Params:  map[string]*dbpb.ParamValue{"audience": {String_: "outside-svc"}},
				Outputs: []string{"token"},
			},
		}},
	}}
	a := New(9, Dependencies{API: api}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status, err := a.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("status = %v, want SUCCEEDED", status)
	}

	// The agent exchanged a token for the step's audience.
	api.mu.Lock()
	got := append([]string(nil), api.exchanges...)
	api.mu.Unlock()
	if len(got) != 1 || got[0] != "outside-svc" {
		t.Errorf("exchanges = %v, want [outside-svc]", got)
	}
}
