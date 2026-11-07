package agent

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// testLogger returns a silent logger so test output stays clean.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fakeWatchStream is a grpc.ServerStreamingClient[WatchMessage] backed by a
// channel: the test pushes WatchMessages into the channel and the agent's
// watch loop receives them. It embeds a nil grpc.ClientStream to satisfy the
// interface's other methods (Context, Header, Trailer, SendMsg, RecvMsg,
// CloseSend), which the agent never calls.
type fakeWatchStream struct {
	grpc.ClientStream
	ch chan *apipb.WatchMessage
}

func (f *fakeWatchStream) Recv() (*apipb.WatchMessage, error) {
	msg, ok := <-f.ch
	if !ok {
		return nil, io.EOF
	}
	return msg, nil
}

// fakeAPIClient is a stub APIClient for agent tests. Only the methods a
// agent uses are implemented; the rest are never called. StreamJobLogs fails
// so the log sink (best-effort) does not block the job. StartJobExecution
// returns the job in jobs for a startable job id (the agent fetches the full
// job from the start, not from the nudge); ListPendingJobs returns no jobs so
// the agent's periodic poll does not interfere with the tests.
// stepCompletion is a (job, step) pair the fake was told a agent completed
// (the cross-agent step barrier).
type stepCompletion struct {
	jobID int64
	step  string
}

// barrierKey identifies a (job, step) pair for a configured barrier outcome.
type barrierKey struct {
	jobID int64
	step  string
}

// barrierOutcome is the configured CheckStepBarrier response for a (job, step).
type barrierOutcome struct {
	satisfied bool
	cancelled bool
}

type fakeAPIClient struct {
	apipb.APIClient // nil; only the methods below are used by the agent
	watchCh         chan *apipb.WatchMessage
	mu              sync.Mutex
	statuses        []dbpb.JobStatus
	jobs            map[int64]*apipb.Job // job id -> job returned by StartJobExecution
	startable       map[int64]bool       // job id -> whether the start succeeds
	// stepCompletions records the (job, step) pairs the agent reported as
	// completed (the cross-agent step barrier).
	stepCompletions []stepCompletion
	// barrierOutcomes configures the CheckStepBarrier response per (job, step);
	// a (job, step) with no entry is treated as satisfied.
	barrierOutcomes map[barrierKey]barrierOutcome
	// exchanges records the ExchangeJobToken calls the token_exchange step
	// handler made (the job's token exchange).
	exchanges []exchangeRequest
}

func (f *fakeAPIClient) RegisterAgent(ctx context.Context, in *apipb.RegisterAgentRequest, opts ...grpc.CallOption) (*apipb.Agent, error) {
	return &apipb.Agent{Name: in.GetName(), Group: in.GetGroup()}, nil
}

func (f *fakeAPIClient) DeregisterAgent(ctx context.Context, in *apipb.DeregisterAgentRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeAPIClient) Heartbeat(ctx context.Context, in *apipb.HeartbeatRequest, opts ...grpc.CallOption) (*apipb.HeartbeatResponse, error) {
	return &apipb.HeartbeatResponse{}, nil
}

func (f *fakeAPIClient) WatchJobs(ctx context.Context, in *apipb.WatchJobsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[apipb.WatchMessage], error) {
	return &fakeWatchStream{ch: f.watchCh}, nil
}

func (f *fakeAPIClient) ReportJobStatus(ctx context.Context, in *apipb.ReportJobStatusRequest, opts ...grpc.CallOption) (*apipb.Job, error) {
	f.mu.Lock()
	f.statuses = append(f.statuses, in.GetStatus())
	f.mu.Unlock()
	return &apipb.Job{Id: in.GetJobId(), Status: in.GetStatus()}, nil
}

func (f *fakeAPIClient) StreamJobLogs(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[apipb.JobLogChunk, apipb.StreamJobLogsResponse], error) {
	return nil, status.Error(codes.Unavailable, "streaming disabled in test")
}

func (f *fakeAPIClient) StartJobExecution(ctx context.Context, in *apipb.StartJobExecutionRequest, opts ...grpc.CallOption) (*apipb.StartJobExecutionResponse, error) {
	started := f.startable[in.GetJobId()]
	if !started {
		return &apipb.StartJobExecutionResponse{Started: false}, nil
	}
	return &apipb.StartJobExecutionResponse{Started: true, Job: f.jobs[in.GetJobId()]}, nil
}

func (f *fakeAPIClient) ListPendingJobs(ctx context.Context, in *apipb.ListPendingJobsRequest, opts ...grpc.CallOption) (*apipb.ListPendingJobsResponse, error) {
	return &apipb.ListPendingJobsResponse{}, nil
}

// ReportStepCompletion records a step completion (the cross-agent step
// barrier). The fake records the (job, step) pairs it is told about so a test
// can assert the agent reported its steps.
func (f *fakeAPIClient) ReportStepCompletion(ctx context.Context, in *apipb.ReportStepCompletionRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	f.stepCompletions = append(f.stepCompletions, stepCompletion{jobID: in.GetJobId(), step: in.GetStepName()})
	f.mu.Unlock()
	return &emptypb.Empty{}, nil
}

// CheckStepBarrier reports whether the barrier for a step is satisfied or the
// job is cancelled. The fake returns the configured per-(job, step) outcome
// (barrierOutcomes); a (job, step) with no configured outcome is treated as
// satisfied (the barrier is a no-op in tests that do not exercise it).
func (f *fakeAPIClient) CheckStepBarrier(ctx context.Context, in *apipb.CheckStepBarrierRequest, opts ...grpc.CallOption) (*apipb.CheckStepBarrierResponse, error) {
	f.mu.Lock()
	outcome, ok := f.barrierOutcomes[barrierKey{jobID: in.GetJobId(), step: in.GetStepName()}]
	f.mu.Unlock()
	if !ok {
		return &apipb.CheckStepBarrierResponse{Satisfied: true}, nil
	}
	return &apipb.CheckStepBarrierResponse{Satisfied: outcome.satisfied, Cancelled: outcome.cancelled}, nil
}

// exchangeRequest is a recorded ExchangeJobToken call (the token_exchange step
// handler).
type exchangeRequest struct {
	jobID    int64
	audience string
}

// ExchangeJobToken mints a new job token for a different audience (the
// token_exchange step handler). The fake records the (job, audience) and
// returns a token derived from the audience so a test can assert the exchange
// happened.
func (f *fakeAPIClient) ExchangeJobToken(ctx context.Context, in *apipb.ExchangeJobTokenRequest, opts ...grpc.CallOption) (*apipb.ExchangeJobTokenResponse, error) {
	f.mu.Lock()
	f.exchanges = append(f.exchanges, exchangeRequest{jobID: in.GetJobId(), audience: in.GetAudience()})
	f.mu.Unlock()
	return &apipb.ExchangeJobTokenResponse{Token: "token-for-" + in.GetAudience()}, nil
}

// exchangesSnapshot returns a copy of the ExchangeJobToken calls the fake
// recorded.
func (f *fakeAPIClient) exchangesSnapshot() []exchangeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]exchangeRequest, len(f.exchanges))
	copy(out, f.exchanges)
	return out
}

// stepCompletionsSnapshot returns a copy of the (job, step) pairs the agent
// reported as completed.
func (f *fakeAPIClient) stepCompletionsSnapshot() []stepCompletion {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]stepCompletion, len(f.stepCompletions))
	copy(out, f.stepCompletions)
	return out
}

// waitForStatus polls the fake API client until the job reports the wanted
// status (or the deadline elapses).
func waitForStatus(t *testing.T, api *fakeAPIClient, want dbpb.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		api.mu.Lock()
		for _, s := range api.statuses {
			if s == want {
				api.mu.Unlock()
				return
			}
		}
		api.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	api.mu.Lock()
	got := append([]dbpb.JobStatus(nil), api.statuses...)
	api.mu.Unlock()
	t.Fatalf("timed out waiting for status %v (reported %v)", want, got)
}

// TestAgentCancelInterruptsRunningJob verifies cancellation propagation
// (F-05): a JobCancellation delivered on the agent's WatchJobs stream
// interrupts the running step (terminating the command) and the agent
// reports the job as cancelled.
func TestAgentCancelInterruptsRunningJob(t *testing.T) {
	// A step that blocks until its context is done: a stand-in for a long
	// command that must be interrupted on cancel.
	executor.RegisterStepType("cancel-blocker", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		<-ctx.Done()
		return ctx.Err()
	})

	watchCh := make(chan *apipb.WatchMessage, 8)
	t.Cleanup(func() { close(watchCh) }) // unblock the watch loop on test end

	// A job that blocks until it is cancelled. The agent starts it and
	// fetches the full job from the start, so the fake returns it for the
	// startable job id.
	job := &apipb.Job{
		Id:   42,
		Name: "long",
		Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Type: "cancel-blocker"}}},
	}
	api := &fakeAPIClient{
		watchCh:   watchCh,
		jobs:      map[int64]*apipb.Job{42: job},
		startable: map[int64]bool{42: true},
	}
	w := New("w1", "pool-a", nil, nil, Dependencies{API: api}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	// Dispatch the job: the agent claims it and runs it until it is cancelled.
	watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: job, AgentName: "w1"},
	}}

	// Wait for the agent to report the job running.
	waitForStatus(t, api, dbpb.JobStatus_JOB_STATUS_RUNNING)

	// Cancel the job: the API delivers a JobCancellation down the stream.
	watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Cancellation{
		Cancellation: &apipb.JobCancellation{JobId: 42},
	}}

	// The agent interrupts the running step and reports the job cancelled.
	waitForStatus(t, api, dbpb.JobStatus_JOB_STATUS_CANCELLED)
}

// TestAgentCancelForUnknownJobIsNoop verifies that a cancellation for a job
// that is not running on the agent is ignored (the job's status is
// reconciled by the database), and does not disturb the agent.
func TestAgentCancelForUnknownJobIsNoop(t *testing.T) {
	watchCh := make(chan *apipb.WatchMessage, 8)
	t.Cleanup(func() { close(watchCh) })
	api := &fakeAPIClient{watchCh: watchCh}
	w := New("w1", "pool-a", nil, nil, Dependencies{API: api}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	// A cancellation for a job the agent is not running is a no-op.
	watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Cancellation{
		Cancellation: &apipb.JobCancellation{JobId: 99},
	}}

	// The agent keeps running (no status is reported for the unknown job).
	time.Sleep(100 * time.Millisecond)
	api.mu.Lock()
	got := append([]dbpb.JobStatus(nil), api.statuses...)
	api.mu.Unlock()
	if len(got) != 0 {
		t.Errorf("agent reported statuses %v for an unknown job, want none", got)
	}
}

// TestAgentStepBarrierSyncs verifies that a agent running a job with the
// step-barrier flag set (and targeting a agent group) reports each completed
// step to the API and waits at the barrier between steps, then reports the
// job succeeded once the barrier is satisfied.
func TestAgentStepBarrierSyncs(t *testing.T) {
	// A fast no-op step.
	executor.RegisterStepType("barrier-fast", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return nil
	})

	// Shorten the barrier's polling interval so the test is fast; restore it
	// afterwards.
	original := stepBarrierPollInterval
	stepBarrierPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { stepBarrierPollInterval = original })

	watchCh := make(chan *apipb.WatchMessage, 8)
	t.Cleanup(func() { close(watchCh) })

	// A two-step job with the step-barrier flag set and a target group (so the
	// barrier is active). The barrier is satisfied by default in the fake, so
	// the agent proceeds through both steps and reports the job succeeded.
	job := &apipb.Job{
		Id:          77,
		Name:        "barriered",
		TargetGroup: "pool-a",
		StepBarrier: true,
		Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{
			{Type: "barrier-fast"},
			{Type: "barrier-fast"},
		}},
	}
	api := &fakeAPIClient{
		watchCh:   watchCh,
		jobs:      map[int64]*apipb.Job{77: job},
		startable: map[int64]bool{77: true},
	}
	w := New("w1", "pool-a", nil, nil, Dependencies{API: api}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: job, AgentName: "w1"},
	}}

	// The agent runs both steps (synchronizing at the barrier after step 0)
	// and reports the job succeeded.
	waitForStatus(t, api, dbpb.JobStatus_JOB_STATUS_SUCCEEDED)

	// The agent reported its completion of step 0 (the barrier is not
	// consulted after the last step, so step 1 has no completion report).
	completions := api.stepCompletionsSnapshot()
	foundStep0 := false
	for _, c := range completions {
		if c.jobID == 77 && c.step == "step0" {
			foundStep0 = true
		}
	}
	if !foundStep0 {
		t.Errorf("agent did not report completion of step 0 (completions = %+v)", completions)
	}
}

// TestAgentStepBarrierCancelled verifies that a agent waiting at a step
// barrier reports the job cancelled when the API reports the job was
// cancelled (distinct from a failure).
func TestAgentStepBarrierCancelled(t *testing.T) {
	executor.RegisterStepType("barrier-fast2", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return nil
	})

	original := stepBarrierPollInterval
	stepBarrierPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { stepBarrierPollInterval = original })

	watchCh := make(chan *apipb.WatchMessage, 8)
	t.Cleanup(func() { close(watchCh) })

	job := &apipb.Job{
		Id:          78,
		Name:        "barriered-cancel",
		TargetGroup: "pool-a",
		StepBarrier: true,
		Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{
			{Type: "barrier-fast2"},
			{Type: "barrier-fast2"},
		}},
	}
	api := &fakeAPIClient{
		watchCh:   watchCh,
		jobs:      map[int64]*apipb.Job{78: job},
		startable: map[int64]bool{78: true},
		// The barrier for step 0 reports the job was cancelled: the agent
		// stops and reports the job cancelled.
		barrierOutcomes: map[barrierKey]barrierOutcome{
			{jobID: 78, step: "step0"}: {cancelled: true},
		},
	}
	w := New("w1", "pool-a", nil, nil, Dependencies{API: api}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: job, AgentName: "w1"},
	}}

	// The agent runs step 0, then at the barrier the API reports the job was
	// cancelled: the agent reports the job cancelled.
	waitForStatus(t, api, dbpb.JobStatus_JOB_STATUS_CANCELLED)
}

// TestAgentTokenExchangeStep verifies that a agent running a job with a
// token_exchange step calls the API's ExchangeJobToken RPC (presenting the
// job's own token) for the step's audience and reports the job succeeded.
func TestAgentTokenExchangeStep(t *testing.T) {
	watchCh := make(chan *apipb.WatchMessage, 8)
	t.Cleanup(func() { close(watchCh) })

	// A job with a token_exchange step that requests a token for an outside
	// resource. The agent's token exchanger calls the API's ExchangeJobToken.
	job := &apipb.Job{
		Id:   55,
		Name: "exchange",
		Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{
			{
				Type:   "token_exchange",
				Params: map[string]*dbpb.ParamValue{"audience": {String_: "outside-svc"}},
			},
		}},
	}
	api := &fakeAPIClient{
		watchCh:   watchCh,
		jobs:      map[int64]*apipb.Job{55: job},
		startable: map[int64]bool{55: true},
	}
	w := New("w1", "pool-a", nil, nil, Dependencies{API: api}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: job, AgentName: "w1"},
	}}

	// The agent runs the token_exchange step and reports the job succeeded.
	waitForStatus(t, api, dbpb.JobStatus_JOB_STATUS_SUCCEEDED)

	// The agent exchanged a token for the step's audience, scoped to the job.
	exchanges := api.exchangesSnapshot()
	if len(exchanges) != 1 {
		t.Fatalf("exchanges = %+v, want one exchange", exchanges)
	}
	if exchanges[0].jobID != 55 {
		t.Errorf("exchange job_id = %d, want 55", exchanges[0].jobID)
	}
	if exchanges[0].audience != "outside-svc" {
		t.Errorf("exchange audience = %q, want %q", exchanges[0].audience, "outside-svc")
	}
}
