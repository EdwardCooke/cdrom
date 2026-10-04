package api

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// heartbeatInterval is the recommended worker heartbeat cadence.
const heartbeatInterval = 10 * time.Second

// assignmentQueueSize bounds how many assignments can be queued for a worker
// between deliveries.
const assignmentQueueSize = 64

// maxLogChunkSize bounds a single streamed log chunk to keep memory bounded.
const maxLogChunkSize = 4 << 20 // 4 MiB

// jobLogName is the name of the combined per-job log file (the concatenation
// of every step's output, in order). Per-step logs are named step-<n>.log.
const jobLogName = "job.log"

// appendLogAttempts and appendLogRetryDelay bound how long the API keeps
// retrying a single log chunk against the artifacts service before giving up
// (and dropping the chunk). The gRPC channel to the artifacts service
// reconnects on its own, so a brief outage (a pod restart or scale event) is
// ridden out: the chunk is retried until the channel recovers, then persisted.
// If the outage outlasts the budget the chunk is dropped and the stream
// continues, so a slow or down artifacts service never blocks or tears down a
// target's log stream. They are variables (not constants) so tests can
// shorten the budget.
var (
	appendLogAttempts   = 20
	appendLogRetryDelay = 500 * time.Millisecond
)

// GRPCServer implements the cdrom.api.v1.API gRPC service. It is the
// control-plane hub that long-lived workers and ephemeral agents talk to: it
// holds the workers' WatchJobs streams, relays job assignments pushed by the
// scheduler (DispatchJob), and proxies artifact traffic to the artifacts
// service. Execution targets never talk to the scheduler, database, or
// artifacts services directly.
//
// For high availability (F-23) the server also runs an event-log tail loop
// (StartEventLogTail): it tails the shared event log and fans events out to
// its local workers and UI clients, so a job published by any pod reaches the
// workers this pod holds streams for. The fields below back that loop.
type GRPCServer struct {
	apipb.UnimplementedAPIServer
	db        dbpb.DatabaseClient
	artifacts artifactspb.ArtifactsClient
	logger    *slog.Logger
	hub       *EventHub
	jobAuth   *JobTokenAuth

	mu       sync.Mutex
	live     map[string]*liveWorker // worker name -> live worker
	watchers map[string]chan *apipb.WatchMessage

	// tailCursor is this pod's position in the shared event log (the last
	// event id it has fanned out). It is per-pod: each pod tails independently
	// and fans out to its own local workers/UI.
	tailCursor atomic.Int64
	// logCursors tracks, per (jobID|log), the byte offset this pod has already
	// range-read for cross-pod near-live logs (F-23).
	logCursors map[string]int64
	// coalescer coalesces job_log_updated events so the event log stays lean
	// when a job produces many log chunks in a short time (F-23).
	coalescer *logCoalescer
	// published tracks event ids this pod published itself, so the tail loop
	// does not re-publish them to the local EventHub (the pod already published
	// them directly when it observed the state change).
	publishedMu sync.Mutex
	published   map[int64]struct{}
}

// liveWorker is a worker with an open WatchJobs stream.
type liveWorker struct {
	name     string
	group    string
	address  string
	lastSeen time.Time
}

// NewGRPCServer creates the API gRPC service. db is the client for the
// Database Service (which owns all durable state); artifacts is the client
// for the Artifacts service (proxied to execution targets); hub is the event
// hub that backs the /api/ws WebSocket endpoint (may be nil); jobAuth verifies
// and mints the job tokens handed to execution targets (may be nil to disable
// job-token auth).
func NewGRPCServer(db dbpb.DatabaseClient, artifacts artifactspb.ArtifactsClient, hub *EventHub, jobAuth *JobTokenAuth, logger *slog.Logger) *GRPCServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &GRPCServer{
		db:         db,
		artifacts:  artifacts,
		hub:        hub,
		jobAuth:    jobAuth,
		logger:     logger,
		live:       make(map[string]*liveWorker),
		watchers:   make(map[string]chan *apipb.WatchMessage),
		logCursors: make(map[string]int64),
		coalescer:  newLogCoalescer(logCoalesceWindow),
		published:  make(map[int64]struct{}),
	}
}

// ---------------------------------------------------------------------------
// Worker lifecycle
// ---------------------------------------------------------------------------

// RegisterWorker persists the worker via the Database service and marks it
// live in memory.
func (s *GRPCServer) RegisterWorker(ctx context.Context, req *apipb.RegisterWorkerRequest) (*apipb.Worker, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker name is required")
	}
	registered, err := s.db.RegisterWorker(ctx, &dbpb.RegisterWorkerRequest{
		Name:    req.GetName(),
		Group:   req.GetGroup(),
		Address: req.GetAddress(),
	})
	if err != nil {
		return nil, err
	}
	// A worker that re-registers after a restart will not resume the jobs it
	// was running before the restart. Abandon its stale running executions
	// (fan-out) so they do not block the jobs' overall status. Best-effort: a
	// failure here does not fail the registration (the scheduler's job-status
	// loop re-derives the jobs' status from the executions).
	if _, err := s.db.AbandonWorkerExecutions(ctx, &dbpb.AbandonWorkerExecutionsRequest{WorkerName: registered.GetName()}); err != nil {
		s.logger.Warn("api: abandon worker executions", "worker", registered.GetName(), "err", err)
	}
	s.mu.Lock()
	s.live[registered.GetName()] = &liveWorker{
		name:     registered.GetName(),
		group:    registered.GetGroup(),
		address:  registered.GetAddress(),
		lastSeen: time.Now(),
	}
	s.mu.Unlock()
	s.logger.Info("api: worker registered", "worker", registered.GetName(), "group", registered.GetGroup())
	s.publish(Event{Type: EventWorker, Worker: registered.GetName(), Group: registered.GetGroup(), Action: WorkerRegistered})
	s.publishWorkerEvent(ctx, registered.GetName(), registered.GetGroup(), WorkerRegistered)
	return &apipb.Worker{
		Name:       registered.GetName(),
		Group:      registered.GetGroup(),
		Address:    registered.GetAddress(),
		LastSeenAt: registered.GetLastSeenAt(),
	}, nil
}

// DeregisterWorker removes the worker from the live registry and deletes its
// persistent record.
func (s *GRPCServer) DeregisterWorker(ctx context.Context, req *apipb.DeregisterWorkerRequest) (*emptypb.Empty, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker name is required")
	}
	if _, err := s.db.DeleteWorker(ctx, &dbpb.DeleteWorkerRequest{Name: req.GetName()}); err != nil {
		return nil, err
	}
	s.removeLive(req.GetName())
	s.logger.Info("api: worker deregistered", "worker", req.GetName())
	s.publish(Event{Type: EventWorker, Worker: req.GetName(), Action: WorkerDeregistered})
	s.publishWorkerEvent(ctx, req.GetName(), "", WorkerDeregistered)
	return &emptypb.Empty{}, nil
}

// Heartbeat refreshes the worker's last-seen timestamp.
func (s *GRPCServer) Heartbeat(ctx context.Context, req *apipb.HeartbeatRequest) (*apipb.HeartbeatResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker name is required")
	}
	if _, err := s.db.HeartbeatWorker(ctx, &dbpb.HeartbeatWorkerRequest{Name: req.GetName()}); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if worker, ok := s.live[req.GetName()]; ok {
		worker.lastSeen = time.Now()
	}
	s.mu.Unlock()
	return &apipb.HeartbeatResponse{PollIntervalSeconds: int32(heartbeatInterval.Seconds())}, nil
}

// ---------------------------------------------------------------------------
// Execution side
// ---------------------------------------------------------------------------

// WatchJobs is a server stream: the worker identifies itself and receives a
// WatchMessage for every job dispatched to its group (a JobAssignment) and
// for every cancellation of a job it is running (a JobCancellation, F-05)
// until the stream closes.
func (s *GRPCServer) WatchJobs(req *apipb.WatchJobsRequest, stream grpc.ServerStreamingServer[apipb.WatchMessage]) error {
	if req.GetWorkerName() == "" {
		return status.Error(codes.InvalidArgument, "worker_name is required")
	}
	name := req.GetWorkerName()
	assignmentChannel := make(chan *apipb.WatchMessage, assignmentQueueSize)

	s.mu.Lock()
	if existing, ok := s.watchers[name]; ok {
		// A worker should have at most one stream; replace the stale one.
		close(existing)
	}
	s.watchers[name] = assignmentChannel
	s.live[name] = &liveWorker{name: name, group: req.GetGroup(), lastSeen: time.Now()}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if current := s.watchers[name]; current == assignmentChannel {
			delete(s.watchers, name)
		}
		delete(s.live, name)
		s.mu.Unlock()
	}()

	s.logger.Info("api: worker watching", "worker", name, "group", req.GetGroup())
	s.publish(Event{Type: EventWorker, Worker: name, Group: req.GetGroup(), Action: WorkerWatching})
	s.publishWorkerEvent(stream.Context(), name, req.GetGroup(), WorkerWatching)
	for {
		select {
		case <-stream.Context().Done():
			s.logger.Info("api: worker stream closed", "worker", name)
			return nil
		case message := <-assignmentChannel:
			if err := stream.Send(message); err != nil {
				s.logger.Warn("api: failed to send watch message", "worker", name, "err", err)
				return nil
			}
		}
	}
}

// GetJob fetches a job from the Database service.
func (s *GRPCServer) GetJob(ctx context.Context, req *apipb.GetJobRequest) (*apipb.Job, error) {
	job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetId()})
	if err != nil {
		return nil, err
	}
	apiJob := toAPIJob(job)
	// Hand the agent the status/outputs of the jobs this job depends on (F-06)
	// so a step's condition can reference them.
	apiJob.UpstreamJobs = s.upstreamJobsFor(ctx, apiJob)
	// Hand the agent a job token so it can authenticate its status reports and
	// any outside-resource calls for this job.
	if token := s.mintJobToken(ctx, apiJob); token != "" {
		apiJob.Token = token
	}
	return apiJob, nil
}

// StartJobExecution records that the calling worker has started running a job
// (fan-out). It creates the worker's JobExecution for the job's current
// attempt via the Database service and, on success, returns the job with its
// spec, upstream jobs, and a fresh job token. A job that targets a worker
// group runs on every worker in the group, so the start is not exclusive:
// every worker in the group starts its own execution. Starting an execution
// for a job that is no longer pending (it reached a terminal state, or it is
// not targeted at a worker group) is rejected, so a job that finished before a
// slow worker started it is not run.
func (s *GRPCServer) StartJobExecution(ctx context.Context, req *apipb.StartJobExecutionRequest) (*apipb.StartJobExecutionResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if req.GetWorkerName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_name is required")
	}
	if s.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	resp, err := s.db.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{
		JobId:      req.GetJobId(),
		WorkerName: req.GetWorkerName(),
	})
	if err != nil {
		return nil, err
	}
	if !resp.GetStarted() {
		// The job was no longer pending (it reached a terminal state, or it is
		// not targeted at a worker group); the caller skips it.
		return &apipb.StartJobExecutionResponse{Started: false}, nil
	}
	// The job is now running on this worker. Fetch it fresh (it now carries the
	// running status and started_at) and hand the worker its spec, upstream
	// jobs, and a job token.
	job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetJobId()})
	if err != nil {
		return nil, err
	}
	apiJob := toAPIJob(job)
	apiJob.UpstreamJobs = s.upstreamJobsFor(ctx, apiJob)
	if token := s.mintJobToken(ctx, apiJob); token != "" {
		apiJob.Token = token
	}
	s.logger.Info("api: job execution started", "job", req.GetJobId(), "worker", req.GetWorkerName())
	return &apipb.StartJobExecutionResponse{Started: true, Job: apiJob}, nil
}

// ListPendingJobs returns the pending jobs that target the caller's worker
// group (F-23). This is the worker's pull path: it polls it on a short
// interval to pick up jobs that were published while it was unreachable, so
// no job is stranded pending. It wraps the Database service's
// ListPendingByGroup.
func (s *GRPCServer) ListPendingJobs(ctx context.Context, req *apipb.ListPendingJobsRequest) (*apipb.ListPendingJobsResponse, error) {
	if s.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	resp, err := s.db.ListPendingByGroup(ctx, &dbpb.ListPendingByGroupRequest{Group: req.GetGroup()})
	if err != nil {
		return nil, err
	}
	result := &apipb.ListPendingJobsResponse{}
	for _, job := range resp.GetJobs() {
		result.Jobs = append(result.Jobs, toAPIJob(job))
	}
	return result, nil
}

// ReportJobStatus updates a job's status (and timestamps) via the Database
// service. When job-token auth is enabled, the caller must present a valid job
// token scoped to this job.
func (s *GRPCServer) ReportJobStatus(ctx context.Context, req *apipb.ReportJobStatusRequest) (*apipb.Job, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if req.GetStatus() == dbpb.JobStatus_JOB_STATUS_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}
	if err := s.checkJobToken(ctx, req.GetJobId()); err != nil {
		return nil, err
	}
	// A report from a named worker for a job that targets a worker group is a
	// fan-out report: it is recorded against the worker's execution of the job
	// (one per worker), not the job row. The job's overall status is derived
	// from the executions by the scheduler's job-status loop, so the job row
	// and the job_status event are left for that loop.
	if req.GetWorkerName() != "" && s.db != nil {
		job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetJobId()})
		if err != nil {
			return nil, err
		}
		if job.GetTargetGroup() != "" {
			if _, err := s.db.UpdateJobExecution(ctx, &dbpb.UpdateJobExecutionRequest{
				JobId:       req.GetJobId(),
				WorkerName:  req.GetWorkerName(),
				Status:      req.GetStatus(),
				FinishedAt:  req.GetFinishedAt(),
				StepResults: req.GetStepResults(),
				Outputs:     req.GetOutputs(),
			}); err != nil {
				return nil, err
			}
			s.logger.Info("api: job execution status reported", "job", req.GetJobId(), "worker", req.GetWorkerName(), "status", req.GetStatus())
			return toAPIJob(job), nil
		}
	}
	// A report from an ephemeral agent (no worker name) — or a job that does
	// not target a worker group — updates the job row directly.
	updated, err := s.db.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:          req.GetJobId(),
		Status:      req.GetStatus(),
		StartedAt:   req.GetStartedAt(),
		FinishedAt:  req.GetFinishedAt(),
		StepResults: req.GetStepResults(),
		Outputs:     req.GetOutputs(),
	})
	if err != nil {
		return nil, err
	}
	// Publish the job's actual (post-update) status, not the requested one: a
	// late report from a target on a job that already reached a terminal state
	// (e.g. cancelled, F-05) is a no-op, and the event should reflect the
	// status the job is actually in.
	s.logger.Info("api: job status reported", "job", req.GetJobId(), "status", updated.GetStatus())
	s.publish(Event{
		Type:        EventJobStatus,
		JobID:       req.GetJobId(),
		Status:      jobStatusName(updated.GetStatus()),
		Attempt:     updated.GetAttempt(),
		MaxAttempts: updated.GetMaxAttempts(),
	})
	// Append the status change to the shared event log so other API pods fan
	// it out to their UI clients (F-23).
	s.publishJobStatus(ctx, req.GetJobId(), updated.GetStatus(), updated.GetAttempt(), updated.GetMaxAttempts())
	return toAPIJob(updated), nil
}

// ReportStepCompletion records that the calling worker completed a step of a
// job (the cross-worker step barrier). It forwards the report to the Database
// service, which upserts the worker's StepCompletion row for the job's current
// attempt. When job-token auth is enabled the caller must present a valid job
// token scoped to this job.
func (s *GRPCServer) ReportStepCompletion(ctx context.Context, req *apipb.ReportStepCompletionRequest) (*emptypb.Empty, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if req.GetWorkerName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_name is required")
	}
	if err := s.checkJobToken(ctx, req.GetJobId()); err != nil {
		return nil, err
	}
	if s.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	if _, err := s.db.ReportStepCompletion(ctx, &dbpb.ReportStepCompletionRequest{
		JobId:      req.GetJobId(),
		WorkerName: req.GetWorkerName(),
		StepIndex:  req.GetStepIndex(),
	}); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// CheckStepBarrier reports whether the barrier for a job's step is satisfied
// (every worker alive at the step's start has completed it) or whether the job
// has been cancelled. It forwards the check to the Database service. A worker
// that is not alive is dropped from the barrier, so a worker that dies
// mid-step cannot wedge the job. When job-token auth is enabled the caller
// must present a valid job token scoped to this job.
func (s *GRPCServer) CheckStepBarrier(ctx context.Context, req *apipb.CheckStepBarrierRequest) (*apipb.CheckStepBarrierResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if err := s.checkJobToken(ctx, req.GetJobId()); err != nil {
		return nil, err
	}
	if s.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	resp, err := s.db.CheckStepBarrier(ctx, &dbpb.CheckStepBarrierRequest{
		JobId:     req.GetJobId(),
		StepIndex: req.GetStepIndex(),
	})
	if err != nil {
		return nil, err
	}
	return &apipb.CheckStepBarrierResponse{
		Satisfied: resp.GetSatisfied(),
		Cancelled: resp.GetCancelled(),
	}, nil
}

// StreamJobLogs is a client stream: an execution target (worker or agent)
// opens it at the start of a job and sends a JobLogChunk for each chunk of
// step output (stdout/stderr). The first chunk of each step's output carries
// metadata (job_id, step_index, stream); subsequent chunks carry data. The
// API persists the output to the artifacts service — one file per step
// (step-<n>.log) plus a combined job.log — and fans it out to the UI over the
// event hub as a job_log event. When job-token auth is enabled the caller
// must present a job token scoped to the job.
//
// Backpressure: the gRPC stream itself provides flow control between the
// target and the API (a slow API blocks the target's sends). A slow UI
// subscriber does not block the target: job_log events are dropped for
// subscribers whose buffer is full, and the UI resynchronizes from the
// persisted log (which is always complete) on reconnect.
//
// Resilience: a transient artifacts-service outage (a pod restart or scale
// event) does not tear down the target's stream. The API retries persisting
// each chunk (appendLogWithRetry) and, if the outage outlasts the retry
// budget, drops the chunk and continues — the stream stays alive and resumes
// persisting once the artifacts service recovers.
func (s *GRPCServer) StreamJobLogs(stream grpc.ClientStreamingServer[apipb.JobLogChunk, apipb.StreamJobLogsResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	metadata := first.GetMetadata()
	if metadata == nil || metadata.GetJobId() == 0 {
		return status.Error(codes.InvalidArgument, "first chunk must carry metadata with job_id")
	}
	jobID := metadata.GetJobId()
	// Verify the caller's job token is scoped to this job, once, up front.
	if err := s.checkJobToken(stream.Context(), jobID); err != nil {
		return err
	}
	var received int64
	if err := s.handleLogChunk(stream.Context(), jobID, metadata, first.GetData(), &received); err != nil {
		return err
	}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if size := len(chunk.GetData()); size > maxLogChunkSize {
			return status.Errorf(codes.InvalidArgument, "log chunk of %d bytes exceeds limit of %d", size, maxLogChunkSize)
		}
		if err := s.handleLogChunk(stream.Context(), jobID, chunk.GetMetadata(), chunk.GetData(), &received); err != nil {
			return err
		}
	}
	return stream.SendAndClose(&apipb.StreamJobLogsResponse{Received: received})
}

// handleLogChunk persists a chunk of job output to the artifacts service and
// publishes a job_log event. A chunk that carries metadata (the first chunk
// of a step's output) is attributed to that step; a chunk without metadata
// continues the current step.
//
// Persistence is best-effort: if the artifacts service is unreachable the
// chunk is retried (appendLogWithRetry) and, if it still cannot be persisted
// within the retry budget, dropped — the stream is never torn down by an
// artifacts outage. The job_log event is always published so the UI sees the
// output in real time.
func (s *GRPCServer) handleLogChunk(ctx context.Context, jobID int64, metadata *apipb.JobLogMetadata, data []byte, received *int64) error {
	if len(data) == 0 {
		return nil
	}
	stepIndex := int32(0)
	streamName := JobLogStreamStdout
	if metadata != nil {
		stepIndex = metadata.GetStepIndex()
		streamName = jobLogStreamName(metadata.GetStream())
	}
	jobIDStr := strconv.FormatInt(jobID, 10)
	// Persist to the artifacts service: one file per step plus the combined
	// job log. A nil artifacts client (e.g. in tests) skips persistence but
	// still fans the output out to the UI. A failure to persist (the artifacts
	// service is down and the retry budget is exhausted) drops the chunk
	// rather than failing the stream, so an artifacts outage never tears down
	// a target's log stream.
	if s.artifacts != nil {
		stepName := fmt.Sprintf("step-%d.log", stepIndex)
		// Persist the per-step log first. If it cannot be persisted (the
		// artifacts service is down and the retry budget is exhausted) the
		// chunk is dropped and the combined job log is skipped too — both
		// files live in the same (unreachable) store, so retrying the second
		// would only double the time spent blocked on a down service.
		if _, err := s.appendLogWithRetry(ctx, jobIDStr, stepName, data); err != nil {
			s.logger.Warn("api: dropping log chunk (artifacts unreachable)",
				"job", jobID, "step", stepIndex, "err", err)
		} else if size, err := s.appendLogWithRetry(ctx, jobIDStr, jobLogName, data); err != nil {
			s.logger.Warn("api: dropping job log chunk (artifacts unreachable)",
				"job", jobID, "log", jobLogName, "err", err)
		} else {
			// The combined log grew to size bytes. Feed the coalescer (F-23):
			// at most one job_log_updated event per (job, job.log) per window
			// is published by the tail loop, carrying the latest size, so other
			// API pods can range-read the delta from the shared artifacts store.
			s.coalescer.record(jobID, jobLogName, stepIndex, streamName, size)
		}
	}
	*received += int64(len(data))
	s.publish(Event{
		Type:      EventJobLog,
		JobID:     jobID,
		StepIndex: stepIndex,
		Stream:    streamName,
		Data:      string(data),
	})
	return nil
}

// appendLogWithRetry appends data to the named log for the job via the
// artifacts service's AppendLog client stream, retrying on failure up to
// appendLogAttempts times (appendLogRetryDelay apart). It returns the log's
// total size after the append and the last error if the chunk could not be
// persisted within the budget, so the caller can drop it. The gRPC channel to
// the artifacts service reconnects on its own, so a brief outage (a pod
// restart or scale event) is ridden out by these retries: the chunk is
// persisted once the channel recovers.
func (s *GRPCServer) appendLogWithRetry(ctx context.Context, jobID, name string, data []byte) (int64, error) {
	var (
		err  error
		size int64
	)
	for attempt := 0; attempt < appendLogAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(appendLogRetryDelay):
			}
		}
		if size, err = s.appendLog(ctx, jobID, name, data); err == nil {
			return size, nil
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}
	return 0, err
}

// appendLog appends data to the named log for the job via the artifacts
// service's AppendLog client stream. A single chunk carries both the log's
// metadata and the data. It returns the log's total size (in bytes) after the
// append, which the caller uses to publish a job_log_updated pointer event
// (F-23): the publisher already knows the size from the AppendLog response, so
// no extra GetLog metadata call is needed.
func (s *GRPCServer) appendLog(ctx context.Context, jobID, name string, data []byte) (int64, error) {
	stream, err := s.artifacts.AppendLog(ctx)
	if err != nil {
		return 0, err
	}
	if err := stream.Send(&artifactspb.ArtifactChunk{
		Metadata: &artifactspb.Artifact{Namespace: jobID, Name: name},
		Data:     data,
	}); err != nil {
		return 0, err
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return 0, err
	}
	return resp.GetArtifact().GetSize(), nil
}

// jobLogStreamName maps a JobLogStream enum to the short name the UI uses.
func jobLogStreamName(stream apipb.JobLogStream) string {
	switch stream {
	case apipb.JobLogStream_JOB_LOG_STREAM_STDERR:
		return JobLogStreamStderr
	default:
		return JobLogStreamStdout
	}
}

// ExchangeJobToken lets a running job request a new job token with a
// different audience (e.g. for an outside resource the job needs to call).
// The caller presents its current job token (scoped to the job); the API
// mints a replacement scoped to the same job but the requested audience. This
// mirrors how a GitHub Actions runner requests a new token for a different
// audience mid-job. When job-token auth is disabled the RPC is a no-op that
// returns an empty token.
func (s *GRPCServer) ExchangeJobToken(ctx context.Context, req *apipb.ExchangeJobTokenRequest) (*apipb.ExchangeJobTokenResponse, error) {
	if !s.jobAuth.Enabled() {
		return &apipb.ExchangeJobTokenResponse{}, nil
	}
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if req.GetAudience() == "" {
		return nil, status.Error(codes.InvalidArgument, "audience is required")
	}
	// The caller must present a valid job token scoped to this job.
	if err := s.checkJobToken(ctx, req.GetJobId()); err != nil {
		return nil, err
	}
	// Look up the job so the new token carries the same pipeline/group scope.
	// A nil db client (e.g. in tests) simply means the scope is left empty.
	var pipelineID int64
	var targetGroup string
	if s.db != nil {
		job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetJobId()})
		if err != nil {
			return nil, err
		}
		pipelineID = job.GetPipelineId()
		targetGroup = job.GetTargetGroup()
	}
	token, err := s.jobAuth.Exchange(ctx, req.GetJobId(), pipelineID, targetGroup, req.GetAudience())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "api: exchange job token: %v", err)
	}
	s.logger.Info("api: job token exchanged", "job", req.GetJobId(), "audience", req.GetAudience())
	return &apipb.ExchangeJobTokenResponse{Token: token}, nil
}

// ---------------------------------------------------------------------------
// Dispatch (called by the scheduler)
// ---------------------------------------------------------------------------

// DispatchJob fans a job out to the live workers in the job's target group.
// Jobs with an empty target group are for ephemeral agents and are not
// dispatched to live workers.
func (s *GRPCServer) DispatchJob(ctx context.Context, req *apipb.DispatchJobRequest) (*apipb.DispatchJobResponse, error) {
	job := req.GetJob()
	if job == nil {
		return nil, status.Error(codes.InvalidArgument, "job is required")
	}
	group := job.GetTargetGroup()
	if group == "" {
		return &apipb.DispatchJobResponse{Dispatched: 0}, nil
	}
	// Hand the workers the status/outputs of the jobs this job depends on
	// (F-06) so a step's condition can reference them.
	job.UpstreamJobs = s.upstreamJobsFor(ctx, job)
	// Mint a job token so the workers can authenticate their status reports
	// and any outside-resource calls for this job.
	token := s.mintJobToken(ctx, job)
	dispatched := s.dispatch(group, job, token)
	if dispatched == 0 {
		s.logger.Warn("api: no live workers for group; job left pending",
			"job", job.GetId(), "group", group)
	} else {
		s.logger.Info("api: job dispatched", "job", job.GetId(), "group", group, "workers", dispatched)
	}
	// Append an assignment event to the shared event log (F-23) so other API
	// pods can nudge their local workers in the group. This pod already pushed
	// directly to its local workers above; the event log is the cross-pod path.
	s.publishAssignment(ctx, job.GetId(), group)
	return &apipb.DispatchJobResponse{Dispatched: int32(dispatched)}, nil
}

// publishAssignment appends an "assignment" event to the shared event log
// (F-23). It is best-effort: a failure to append does not fail the dispatch
// (the worker's periodic poll is the authoritative path).
func (s *GRPCServer) publishAssignment(ctx context.Context, jobID int64, group string) {
	if s.db == nil {
		return
	}
	resp, err := s.db.PublishAssignment(ctx, &dbpb.PublishAssignmentRequest{JobId: jobID, TargetGroup: group})
	if err != nil {
		s.logger.Warn("api: publish assignment event", "job", jobID, "err", err)
		return
	}
	s.markPublished(resp.GetId())
}

// NotifyJobStatus is called by the scheduler's watchdog to fan a job-status
// change (e.g. a job reaped as timed_out) out to the UI over the WebSocket
// event hub. It mirrors the job_status events the API publishes when a target
// reports a status via ReportJobStatus. It does not touch the database — the
// scheduler already persisted the change through the Database service — it
// only publishes the event.
func (s *GRPCServer) NotifyJobStatus(ctx context.Context, req *apipb.NotifyJobStatusRequest) (*emptypb.Empty, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if req.GetStatus() == dbpb.JobStatus_JOB_STATUS_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}
	s.logger.Info("api: job status notified", "job", req.GetJobId(), "status", req.GetStatus())
	s.publish(Event{
		Type:        EventJobStatus,
		JobID:       req.GetJobId(),
		Status:      jobStatusName(req.GetStatus()),
		Attempt:     req.GetAttempt(),
		MaxAttempts: req.GetMaxAttempts(),
	})
	return &emptypb.Empty{}, nil
}

// NotifyRunStatus is called by the scheduler's run-status loop to fan a
// pipeline run status change out to the UI over the WebSocket event hub
// (F-07). It mirrors the run_status events the API publishes when a run is
// created. It does not touch the database — the scheduler already persisted
// the change through the Database service — it only publishes the event.
func (s *GRPCServer) NotifyRunStatus(ctx context.Context, req *apipb.NotifyRunStatusRequest) (*emptypb.Empty, error) {
	if req.GetRunId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	if req.GetStatus() == dbpb.RunStatus_RUN_STATUS_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}
	s.logger.Info("api: run status notified", "run", req.GetRunId(), "status", req.GetStatus())
	s.publish(Event{
		Type:   EventRunStatus,
		RunID:  req.GetRunId(),
		Status: runStatusName(req.GetStatus()),
	})
	return &emptypb.Empty{}, nil
}

// CancelJob is called by the scheduler to signal the execution target running
// a job to stop the work (F-05). For a long-lived worker it delivers a
// JobCancellation down the worker's WatchJobs stream; the worker interrupts
// the running step and reports the job as cancelled. For an ephemeral agent
// (no live worker stream) there is nothing to signal — the agent observes the
// cancellation on its next GetJob and stops on its own. It is a no-op when the
// job is not running on a live target.
func (s *GRPCServer) CancelJob(ctx context.Context, req *apipb.CancelJobRequest) (*emptypb.Empty, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	s.mu.Lock()
	var delivered int
	for name, channel := range s.watchers {
		select {
		case channel <- &apipb.WatchMessage{
			Message: &apipb.WatchMessage_Cancellation{Cancellation: &apipb.JobCancellation{JobId: req.GetJobId()}},
		}:
			delivered++
		default:
			s.logger.Warn("api: cancel queue full; worker will observe the status on its next report",
				"worker", name, "job", req.GetJobId())
		}
	}
	s.mu.Unlock()
	if delivered > 0 {
		s.logger.Info("api: job cancellation delivered", "job", req.GetJobId(), "workers", delivered)
	} else {
		s.logger.Info("api: no live worker to signal for job cancellation", "job", req.GetJobId())
	}
	// Append a cancel event to the shared event log (F-23) so other API pods
	// can deliver the cancellation to their local workers. This pod already
	// pushed directly to its local workers above.
	s.publishCancel(ctx, req.GetJobId())
	return &emptypb.Empty{}, nil
}

// publishCancel appends a "cancel" event to the shared event log (F-23).
// Best-effort: a failure does not fail the cancellation (the job is already
// marked cancelled in the database; the target's next report reconciles it).
func (s *GRPCServer) publishCancel(ctx context.Context, jobID int64) {
	if s.db == nil {
		return
	}
	resp, err := s.db.PublishCancel(ctx, &dbpb.PublishCancelRequest{JobId: jobID})
	if err != nil {
		s.logger.Warn("failed to publish cancel event", "job_id", jobID, "error", err)
		return
	}
	s.markPublished(resp.GetId())
}

// publishJobStatus appends a "job_status" event to the shared event log so
// every API pod can fan the status change out to its UI clients (F-23).
func (s *GRPCServer) publishJobStatus(ctx context.Context, jobID int64, status dbpb.JobStatus, attempt, maxAttempts int32) {
	if s.db == nil {
		return
	}
	resp, err := s.db.PublishJobStatus(ctx, &dbpb.PublishJobStatusRequest{
		JobId:       jobID,
		Status:      status,
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		s.logger.Warn("failed to publish job_status event", "job_id", jobID, "error", err)
		return
	}
	s.markPublished(resp.GetId())
}

// publishRunStatus appends a "run_status" event to the shared event log so
// every API pod can fan the run status change out to its UI clients (F-23).
func (s *GRPCServer) publishRunStatus(ctx context.Context, runID int64, status dbpb.RunStatus) {
	if s.db == nil {
		return
	}
	resp, err := s.db.PublishRunStatus(ctx, &dbpb.PublishRunStatusRequest{RunId: runID, Status: status})
	if err != nil {
		s.logger.Warn("failed to publish run_status event", "run_id", runID, "error", err)
		return
	}
	s.markPublished(resp.GetId())
}

// publishWorkerEvent appends a "worker" event to the shared event log so every
// API pod can fan the worker lifecycle change out to its UI clients (F-23).
func (s *GRPCServer) publishWorkerEvent(ctx context.Context, name, group, action string) {
	if s.db == nil {
		return
	}
	resp, err := s.db.PublishWorkerEvent(ctx, &dbpb.PublishWorkerEventRequest{
		Name:   name,
		Group:  group,
		Action: action,
	})
	if err != nil {
		s.logger.Warn("failed to publish worker event", "worker", name, "error", err)
		return
	}
	s.markPublished(resp.GetId())
}

// publishLogUpdated appends a "job_log_updated" pointer event to the shared
// event log (F-23): it carries only the job id, log name, step index, stream,
// and the current byte size — never the log bytes. Every API pod tails the
// log and, when it has UI clients watching the job, range-reads the new bytes
// from the shared artifacts store and fans them out.
func (s *GRPCServer) publishLogUpdated(ctx context.Context, jobID int64, log string, stepIndex int32, stream string, size int64) {
	if s.db == nil {
		return
	}
	resp, err := s.db.PublishLogUpdated(ctx, &dbpb.PublishLogUpdatedRequest{
		JobId:     jobID,
		Log:       log,
		StepIndex: stepIndex,
		Stream:    stream,
		Size:      size,
	})
	if err != nil {
		s.logger.Warn("failed to publish job_log_updated event", "job_id", jobID, "error", err)
		return
	}
	s.markPublished(resp.GetId())
}

// dispatch pushes an assignment (with the job token) to every live worker in
// the group and returns how many workers received it.
func (s *GRPCServer) dispatch(group string, job *apipb.Job, token string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var dispatchedCount int
	for name, worker := range s.live {
		if worker.group != group {
			continue
		}
		assignmentChannel, ok := s.watchers[name]
		if !ok {
			continue
		}
		assignment := &apipb.WatchMessage{
			Message: &apipb.WatchMessage_Assignment{
				Assignment: &apipb.JobAssignment{Job: job, WorkerName: name, Token: token},
			},
		}
		select {
		case assignmentChannel <- assignment:
			dispatchedCount++
		default:
			s.logger.Warn("api: assignment queue full; job left for retry", "worker", name, "job", job.GetId())
		}
	}
	return dispatchedCount
}

// mintJobToken requests a job token for job from the IdP when job-token auth
// is enabled. It returns "" when disabled or when minting fails (the job is
// still dispatched; the target's status reports will be rejected until it has
// a valid token).
func (s *GRPCServer) mintJobToken(ctx context.Context, job *apipb.Job) string {
	if !s.jobAuth.Enabled() {
		return ""
	}
	token, err := s.jobAuth.Mint(ctx, job.GetId(), job.GetPipelineId(), job.GetTargetGroup())
	if err != nil {
		s.logger.Warn("api: mint job token failed; dispatching without token", "job", job.GetId(), "err", err)
		return ""
	}
	return token
}

// ---------------------------------------------------------------------------
// Artifact proxy (forwarded to the artifacts service)
// ---------------------------------------------------------------------------

// UploadArtifact proxies a client-streaming artifact upload to the artifacts
// service.
func (s *GRPCServer) UploadArtifact(stream grpc.ClientStreamingServer[artifactspb.ArtifactChunk, artifactspb.UploadArtifactResponse]) error {
	// The first chunk carries the namespace; verify the caller's job token is
	// scoped to it before proxying the upload.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if err := s.checkArtifactToken(stream.Context(), first.GetMetadata().GetNamespace()); err != nil {
		return err
	}
	remote, err := s.artifacts.UploadArtifact(stream.Context())
	if err != nil {
		return err
	}
	if err := remote.Send(first); err != nil {
		return err
	}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := remote.Send(chunk); err != nil {
			return err
		}
	}
	resp, err := remote.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

// DownloadArtifact proxies a server-streaming artifact download from the
// artifacts service.
func (s *GRPCServer) DownloadArtifact(req *artifactspb.DownloadArtifactRequest, stream grpc.ServerStreamingServer[artifactspb.ArtifactChunk]) error {
	if err := s.checkArtifactToken(stream.Context(), req.GetNamespace()); err != nil {
		return err
	}
	remote, err := s.artifacts.DownloadArtifact(stream.Context(), req)
	if err != nil {
		return err
	}
	for {
		chunk, err := remote.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}
}

// GetArtifact proxies an artifact metadata lookup to the artifacts service.
func (s *GRPCServer) GetArtifact(ctx context.Context, req *artifactspb.GetArtifactRequest) (*artifactspb.Artifact, error) {
	if err := s.checkArtifactToken(ctx, req.GetNamespace()); err != nil {
		return nil, err
	}
	return s.artifacts.GetArtifact(ctx, req)
}

// ListArtifacts proxies an artifact listing to the artifacts service. A
// non-empty namespace is scoped to that namespace's token; an empty namespace
// lists all artifacts and is not job-scoped.
func (s *GRPCServer) ListArtifacts(ctx context.Context, req *artifactspb.ListArtifactsRequest) (*artifactspb.ListArtifactsResponse, error) {
	if req.GetNamespace() != "" {
		if err := s.checkArtifactToken(ctx, req.GetNamespace()); err != nil {
			return nil, err
		}
	}
	return s.artifacts.ListArtifacts(ctx, req)
}

// DeleteArtifact proxies an artifact deletion to the artifacts service.
func (s *GRPCServer) DeleteArtifact(ctx context.Context, req *artifactspb.DeleteArtifactRequest) (*emptypb.Empty, error) {
	if err := s.checkArtifactToken(ctx, req.GetNamespace()); err != nil {
		return nil, err
	}
	return s.artifacts.DeleteArtifact(ctx, req)
}

// checkArtifactToken, when job-token auth is enabled, verifies the caller's
// job token is scoped to the job that owns the artifact. A job's artifacts
// and logs use the job's id as the namespace, so the namespace is parsed back
// to a job id and checked against the token. It is a no-op when job-token
// auth is disabled or the namespace is empty (not job-scoped). Namespaces
// that are not numeric job ids cannot be job-token-scoped.
func (s *GRPCServer) checkArtifactToken(ctx context.Context, namespace string) error {
	if !s.jobAuth.Enabled() || namespace == "" {
		return nil
	}
	id, err := strconv.ParseInt(namespace, 10, 64)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid artifact namespace")
	}
	return s.checkJobToken(ctx, id)
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

func (s *GRPCServer) removeLive(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.live, name)
	if assignmentChannel, ok := s.watchers[name]; ok {
		close(assignmentChannel)
		delete(s.watchers, name)
	}
}

// checkJobToken, when job-token auth is enabled, verifies that the caller
// presented a valid job token scoped to jobID. It is a no-op when job-token
// auth is disabled.
func (s *GRPCServer) checkJobToken(ctx context.Context, jobID int64) error {
	if !s.jobAuth.Enabled() {
		return nil
	}
	token := bearerToken(ctx)
	if token == "" {
		return status.Error(codes.Unauthenticated, "job token is required")
	}
	tokenJobID, err := s.jobAuth.Verify(ctx, token)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	if tokenJobID != jobID {
		return status.Error(codes.PermissionDenied, "job token is not scoped to this job")
	}
	return nil
}

// bearerToken returns the bearer token from the request's authorization
// metadata, or "" when absent.
func bearerToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return ""
	}
	const prefix = "bearer "
	if len(vals[0]) < len(prefix) || !strings.EqualFold(vals[0][:len(prefix)], prefix) {
		return ""
	}
	return vals[0][len(prefix):]
}

func toAPIJob(job *dbpb.Job) *apipb.Job {
	return &apipb.Job{
		Id:           job.GetId(),
		PipelineId:   job.GetPipelineId(),
		Name:         job.GetName(),
		Status:       job.GetStatus(),
		TargetGroup:  job.GetTargetGroup(),
		StartedAt:    job.GetStartedAt(),
		FinishedAt:   job.GetFinishedAt(),
		Spec:         job.GetSpec(),
		Attempt:      job.GetAttempt(),
		MaxAttempts:  job.GetMaxAttempts(),
		DependsOn:    job.GetDependsOn(),
		StepResults:  job.GetStepResults(),
		Outputs:      job.GetOutputs(),
		IgnoreFailed: job.GetIgnoreFailed(),
		FailureMode:  job.GetFailureMode(),
		StepBarrier:  job.GetStepBarrier(),
	}
}

// upstreamJobsFor fetches the status and outputs of the jobs job depends on
// (F-06) so a step's condition can reference them. It is best-effort: a
// dependency that cannot be read (e.g. it does not exist) is omitted rather
// than failing the dispatch, and a job with no dependencies yields nil.
func (s *GRPCServer) upstreamJobsFor(ctx context.Context, job *apipb.Job) []*dbpb.UpstreamJob {
	ids := job.GetDependsOn()
	if len(ids) == 0 {
		return nil
	}
	upstream := make([]*dbpb.UpstreamJob, 0, len(ids))
	for _, id := range ids {
		dep, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: id})
		if err != nil {
			s.logger.Warn("api: read upstream job for condition context", "job", job.GetId(), "depends_on", id, "err", err)
			continue
		}
		upstream = append(upstream, &dbpb.UpstreamJob{
			Id:      dep.GetId(),
			Name:    dep.GetName(),
			Status:  dep.GetStatus(),
			Outputs: dep.GetOutputs(),
		})
	}
	return upstream
}

// publish sends an event to the hub, if one is configured.
func (s *GRPCServer) publish(ev Event) {
	if s.hub != nil {
		s.hub.Publish(ev)
	}
}
