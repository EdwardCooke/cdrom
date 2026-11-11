package worker

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
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

// fixtureJobToken builds a SYNTACTIC compact JWT (three base64url segments)
// whose payload carries the given claims — enough for the worker's LOCAL
// (unsigned) agent_name decode (F-30.4 attribution); the worker never
// signature-verifies its own token.
func fixtureJobToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	sig := make([]byte, 16)
	rand.Read(sig)
	return base64.RawURLEncoding.EncodeToString(header) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(sig)
}

// fakeWorkerAPI is a stub APIClient for worker tests. Only the methods a
// worker uses are implemented; the rest are never called. GetJob plays the
// API's F-30.4 payload claim: it returns the job's stored status until a
// short delay elapses, after which it reports the job as cancelled
// (simulating the scheduler's CancelJob marking it cancelled in the database,
// which the worker observes on its next poll, F-05). StreamJobLogs fails so
// the log sink (best-effort) does not block the job.
type fakeWorkerAPI struct {
	apipb.APIClient // nil; only the methods below are used by the worker
	mu              sync.Mutex
	statuses        []dbpb.JobStatus
	agentNames      []string // the AgentName each ReportJobStatus carried (F-30.4 attribution)
	gets            int
	job             *apipb.Job
	start           time.Time
	// exchanges records the audiences the token_exchange step handler
	// requested a new job token for.
	exchanges []string
	// stepCompletions records the (job, agent, step) triples the barrier
	// reported (F-30.4: participation is accounted under the AGENT name).
	stepCompletions []namedStep
	// barrierOutcomes configures the CheckStepBarrier response per (job, step);
	// a (job, step) with no entry is treated as satisfied.
	barrierOutcomes map[barrierKey]barrierOutcome
}

type namedStep struct {
	jobID int64
	agent string
	step  string
}

type barrierKey struct {
	jobID int64
	step  string
}

type barrierOutcome struct {
	satisfied bool
	cancelled bool
}

func (f *fakeWorkerAPI) GetJob(ctx context.Context, in *apipb.GetJobRequest, opts ...grpc.CallOption) (*apipb.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.start.IsZero() {
		f.start = time.Now()
	}
	// Construct a fresh Job (copying a proto message would copy its internal
	// mutex). After a short delay the job is cancelled (the scheduler marked
	// it so). NOTE (F-30.4): the Job carries NO token anymore — the worker
	// presents its own (from the mandate) and the API returns the payload.
	status := f.job.GetStatus()
	if time.Since(f.start) > 100*time.Millisecond {
		status = dbpb.JobStatus_JOB_STATUS_CANCELLED
	}
	return &apipb.Job{
		Id:             f.job.GetId(),
		Name:           f.job.GetName(),
		Status:         status,
		TargetGroup:    f.job.GetTargetGroup(),
		StepBarrier:    f.job.GetStepBarrier(),
		Spec:           f.job.GetSpec(),
		UpstreamJobs:   f.job.GetUpstreamJobs(),
		RunParams:      f.job.GetRunParams(),
		Secrets:        f.job.GetSecrets(),
		Attempt:        f.job.GetAttempt(),
		MaxAttempts:    f.job.GetMaxAttempts(),
		IgnoreFailed:   f.job.GetIgnoreFailed(),
		FailureMode:    f.job.GetFailureMode(),
		TriggerName:    f.job.GetTriggerName(),
		TriggerType:    f.job.GetTriggerType(),
		UpstreamClaims: f.job.GetUpstreamClaims(),
		RunId:          f.job.GetRunId(),
		Artifacts:      f.job.GetArtifacts(),
		Environment:    f.job.GetEnvironment(),
	}, nil
}

func (f *fakeWorkerAPI) ReportJobStatus(ctx context.Context, in *apipb.ReportJobStatusRequest, opts ...grpc.CallOption) (*apipb.Job, error) {
	f.mu.Lock()
	f.statuses = append(f.statuses, in.GetStatus())
	f.agentNames = append(f.agentNames, in.GetAgentName())
	f.mu.Unlock()
	return &apipb.Job{Id: in.GetJobId(), Status: in.GetStatus()}, nil
}

func (f *fakeWorkerAPI) StreamJobLogs(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[apipb.JobLogChunk, apipb.StreamJobLogsResponse], error) {
	return nil, status.Error(codes.Unavailable, "streaming disabled in test")
}

// ExchangeJobToken mints a new job token for a different audience (the
// token_exchange step handler). The fake records the (job, audience) and
// returns a token derived from the audience so a test can assert the exchange
// happened.
func (f *fakeWorkerAPI) ExchangeJobToken(ctx context.Context, in *apipb.ExchangeJobTokenRequest, opts ...grpc.CallOption) (*apipb.ExchangeJobTokenResponse, error) {
	f.mu.Lock()
	f.exchanges = append(f.exchanges, in.GetAudience())
	f.mu.Unlock()
	return &apipb.ExchangeJobTokenResponse{Token: "token-for-" + in.GetAudience()}, nil
}

// ReportStepCompletion records a step completion (the cross-participant step
// barrier). The fake records the (job, AGENT, step) triple so a test can
// assert the worker participated UNDER THE AGENT'S NAME (F-30.4).
func (f *fakeWorkerAPI) ReportStepCompletion(ctx context.Context, in *apipb.ReportStepCompletionRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	f.stepCompletions = append(f.stepCompletions, namedStep{jobID: in.GetJobId(), agent: in.GetAgentName(), step: in.GetStepName()})
	f.mu.Unlock()
	return &emptypb.Empty{}, nil
}

// CheckStepBarrier reports whether the barrier for a step is satisfied or the
// job is cancelled. The fake returns the configured per-(job, step) outcome
// (barrierOutcomes); a (job, step) with no configured outcome is treated as
// satisfied (the barrier is a no-op in tests that do not exercise it).
func (f *fakeWorkerAPI) CheckStepBarrier(ctx context.Context, in *apipb.CheckStepBarrierRequest, opts ...grpc.CallOption) (*apipb.CheckStepBarrierResponse, error) {
	f.mu.Lock()
	outcome, ok := f.barrierOutcomes[barrierKey{jobID: in.GetJobId(), step: in.GetStepName()}]
	f.mu.Unlock()
	if !ok {
		return &apipb.CheckStepBarrierResponse{Satisfied: true}, nil
	}
	return &apipb.CheckStepBarrierResponse{Satisfied: outcome.satisfied, Cancelled: outcome.cancelled}, nil
}

// TestWorkerRunsEndToEnd verifies the F-30.4 claim-then-execute path: the
// worker presents its mandate token to GetJob (payload claim), executes the
// steps, and reports the terminal status — attributing EVERY report to the
// agent name decoded from its token's agent_name claim.
func TestWorkerRunsEndToEnd(t *testing.T) {
	executor.RegisterStepType("worker-ok-step", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return nil
	})
	api := &fakeWorkerAPI{job: &apipb.Job{
		Id:     7,
		Name:   "simple",
		Status: dbpb.JobStatus_JOB_STATUS_PENDING,
		Spec:   &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Type: "worker-ok-step"}}},
	}}
	tok := fixtureJobToken(t, map[string]any{"job_id": "7", "agent_name": "agent-1"})
	a := New("worker-host", 7, Dependencies{API: api, JobToken: tok}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status, err := a.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("status = %v, want SUCCEEDED", status)
	}
	if api.gets == 0 {
		t.Errorf("worker never claimed the payload (GetJob calls = 0)")
	}
	// F-30.4 attribution: every status report rode the AGENT's name (decoded
	// from the token), not the worker's hostname.
	api.mu.Lock()
	names := append([]string(nil), api.agentNames...)
	api.mu.Unlock()
	if len(names) == 0 {
		t.Fatalf("no status reports recorded")
	}
	for _, n := range names {
		if n != "agent-1" {
			t.Errorf("report attributed to %q, want agent-1 (F-30.4)", n)
		}
	}
}

// TestWorkerMissingTokenRefuses verifies the worker REFUSES TO RUN without a
// mandate token (F-30.4: the token is the worker's only credential).
func TestWorkerMissingTokenRefuses(t *testing.T) {
	api := &fakeWorkerAPI{job: &apipb.Job{Id: 8, Name: "nomandate", Status: dbpb.JobStatus_JOB_STATUS_PENDING}}
	a := New("worker-host", 8, Dependencies{API: api}, testLogger())
	_, err := a.Run(context.Background())
	if err == nil {
		t.Fatalf("Run without a job token succeeded, want refusal")
	}
	if api.gets != 0 {
		t.Errorf("worker attempted a payload claim without a token (gets = %d)", api.gets)
	}
}

// TestWorkerCancelInterruptsRunningJob verifies cancellation propagation for a
// worker (F-05): the worker polls the API for its job's status and, when it
// observes the job cancelled, interrupts the running step (terminating the
// command) and reports the job as cancelled.
func TestWorkerCancelInterruptsRunningJob(t *testing.T) {
	// A step that blocks until its context is done: a stand-in for a long
	// command that must be interrupted on cancel.
	executor.RegisterStepType("worker-cancel-blocker", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		<-ctx.Done()
		return ctx.Err()
	})

	// Shorten the poll interval so the test is fast.
	orig := workerCancelPollInterval
	workerCancelPollInterval = 20 * time.Millisecond
	t.Cleanup(func() { workerCancelPollInterval = orig })

	api := &fakeWorkerAPI{job: &apipb.Job{
		Id:     7,
		Name:   "long",
		Status: dbpb.JobStatus_JOB_STATUS_PENDING,
		Spec:   &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Type: "worker-cancel-blocker"}}},
	}}
	tok := fixtureJobToken(t, map[string]any{"job_id": "7", "agent_name": "agent-1"})
	a := New("worker-host", 7, Dependencies{API: api, JobToken: tok}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status, err := a.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != dbpb.JobStatus_JOB_STATUS_CANCELLED {
		t.Errorf("status = %v, want CANCELLED", status)
	}

	// The worker reported running and then cancelled.
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
		t.Errorf("worker did not report RUNNING (reported %v)", got)
	}
	if !sawCancelled {
		t.Errorf("worker did not report CANCELLED (reported %v)", got)
	}
}

// TestWorkerParticipatesInBarrierUnderAgentName verifies that a worker running
// a step-barrier job PARTICIPATES UNDER THE AGENT'S NAME (F-30.4: the barrier
// participant set is keyed by agent name; the worker decodes it from its
// token's agent_name claim) — and reports the job succeeded once the barrier
// is satisfied.
func TestWorkerParticipatesInBarrierUnderAgentName(t *testing.T) {
	executor.RegisterStepType("barrier-fast", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return nil
	})
	// NB: the barrier's poll interval (target.stepBarrierPollInterval, 1 s
	// default) is package-private to target; the test simply tolerates the
	// ~1 s barrier hop (still fast compared to the suite).
	api := &fakeWorkerAPI{job: &apipb.Job{
		Id:          77,
		Name:        "barriered",
		TargetGroup: "pool-a",
		StepBarrier: true,
		Status:      dbpb.JobStatus_JOB_STATUS_PENDING,
		Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{
			{Type: "barrier-fast"},
			{Type: "barrier-fast"},
		}},
	}}
	tok := fixtureJobToken(t, map[string]any{"job_id": "77", "agent_name": "agent-1"})
	a := New("worker-host", 77, Dependencies{API: api, JobToken: tok}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status, err := a.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("status = %v, want SUCCEEDED", status)
	}
	// The worker reported its step completion(s) UNDER THE AGENT NAME.
	api.mu.Lock()
	completions := append([]namedStep(nil), api.stepCompletions...)
	api.mu.Unlock()
	if len(completions) == 0 {
		t.Fatalf("worker reported no step completions (barrier inactive?)")
	}
	for _, c := range completions {
		if c.agent != "agent-1" {
			t.Errorf("completion (%d,%s) attributed to %q, want agent-1 (F-30.4)", c.jobID, c.step, c.agent)
		}
	}
}

// TestWorkerBarriersCanceled verifies that a worker waiting at a step barrier
// reports the job cancelled when the API reports the job was cancelled
// (distinct from a failure).
func TestWorkerBarriersCanceled(t *testing.T) {
	executor.RegisterStepType("barrier-fast2", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return nil
	})
	api := &fakeWorkerAPI{
		job: &apipb.Job{
			Id:          78,
			Name:        "barriered-cancel",
			TargetGroup: "pool-a",
			StepBarrier: true,
			Status:      dbpb.JobStatus_JOB_STATUS_PENDING,
			Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{
				{Type: "barrier-fast2"},
				{Type: "barrier-fast2"},
			}},
		},
		// The barrier for step 0 reports the job was cancelled: the worker
		// stops and reports the job cancelled.
		barrierOutcomes: map[barrierKey]barrierOutcome{
			{jobID: 78, step: "step0"}: {cancelled: true},
		},
	}
	tok := fixtureJobToken(t, map[string]any{"job_id": "78", "agent_name": "agent-1"})
	a := New("worker-host", 78, Dependencies{API: api, JobToken: tok}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status, err := a.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != dbpb.JobStatus_JOB_STATUS_CANCELLED {
		t.Errorf("status = %v, want CANCELLED", status)
	}
}

// TestWorkerTokenExchangeStep verifies that a worker running a job with a
// token_exchange step calls the API's ExchangeJobToken RPC (presenting the
// job's own token) for the step's audience and reports the job succeeded.
func TestWorkerTokenExchangeStep(t *testing.T) {
	// The job completes (the token_exchange step is fast) well before the fake
	// API's 100ms cancellation delay, so it reports succeeded.
	api := &fakeWorkerAPI{job: &apipb.Job{
		Id:     9,
		Name:   "exchange",
		Status: dbpb.JobStatus_JOB_STATUS_PENDING,
		Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{
			{
				Type:   "token_exchange",
				Params: map[string]*dbpb.ParamValue{"audience": {String_: "outside-svc"}},
			},
		}},
	}}
	tok := fixtureJobToken(t, map[string]any{"job_id": "9", "agent_name": "agent-1"})
	a := New("worker-host", 9, Dependencies{API: api, JobToken: tok}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	status, err := a.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("status = %v, want SUCCEEDED", status)
	}

	// The worker exchanged a token for the step's audience.
	api.mu.Lock()
	got := append([]string(nil), api.exchanges...)
	api.mu.Unlock()
	if len(got) != 1 || got[0] != "outside-svc" {
		t.Errorf("exchanges = %v, want [outside-svc]", got)
	}
}
