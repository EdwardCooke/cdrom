package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/smtp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomarkdown/markdown"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"cdrom/internal/audit"
	"cdrom/internal/authz"
	"cdrom/internal/config"
	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/metrics"
	"cdrom/internal/secrets"
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
	// resolver translates (namespace, name) to storage_object records and
	// drives the blob service by uid (F-21.6 storage redesign). Installed via
	// SetResolver; nil in legacy tests that do not exercise the translated
	// paths (those fail with Unavailable, mirroring the db-absent posture).
	resolver *fileResolver
	// secretStore decrypts a job's stored secret ciphertexts into plaintext
	// when the API hands the job to an execution target (F-12), and redacts
	// the plaintext from streamed job logs before they are persisted or fanned
	// out. It is nil only in tests that do not configure a store; in
	// production the built-in AES store is always available. A job whose
	// pipeline has no secrets needs no store.
	secretStore secrets.Store
	// authz is the shared authorization engine (F-14, RBAC). The HTTP server
	// uses it to enforce role-based access; the gRPC server's event-log tail
	// loop uses it to invalidate its cache when a role_change event arrives
	// (F-23), so a role or binding change takes effect on this replica without
	// a restart. It is the same engine instance the HTTP server holds.
	authz *authz.Engine

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

	// audit records an audit event (F-15) for each audited action the gRPC
	// surface handles (worker lifecycle, job status reports, token exchanges).
	// It is nil when the audit log is not configured (or in tests), in which
	// case no audit events are recorded on the gRPC surface.
	audit *audit.Recorder
	// logUids caches, per (jobID|logName), the uid of the log's blob (F-21.6
	// storage redesign): a job's log stream appends MANY chunks per log file,
	// so establishing the uid once (ensureLog) spares the hot path a database
	// round trip per chunk. Entries are evicted when the job reaches a
	// terminal state (finalizeJobLogs) — a job's logs are sealed at that
	// point, so caching them forever would leak memory across a fleet's
	// worth of finished jobs.
	logUidsMu sync.Mutex
	logUids   map[string]string
	// metrics records the API's Prometheus metrics (F-20) at the gRPC surface's
	// hot spots (job status, log bytes, token minting, secret decryption,
	// notifications, artifact bytes, worker hub, event-log lag). It is nil
	// when metrics are disabled, in which case all recording is a no-op.
	metrics *metrics.APIMetrics

	// email and webhook back the notification step handlers (F-18): the API is
	// the only component that sends an email (SMTP) or performs a webhook's
	// HTTP request. email is the API's SMTP configuration (a zero value means
	// no email server is configured, so an email step fails with a clear
	// error); webhook holds the response-timeout bounds, and webhookClient is
	// the HTTP client the API uses to send webhooks (it carries no global
	// timeout — each request is bounded by its own clamped timeout).
	email         config.EmailConfig
	webhook       config.WebhookConfig
	webhookClient *http.Client
}

// liveWorker is a worker with an open WatchJobs stream.
type liveWorker struct {
	name  string
	group string
	// environments are the environments the worker belongs to (F-17); a job
	// that targets an environment is dispatched to the live workers that
	// registered it.
	environments []string
	// tags are the worker's tags (F-29); a job dispatched to the worker merges
	// them into the job's execution's tag set.
	tags     map[string]string
	address  string
	lastSeen time.Time
}

// NewGRPCServer creates the API gRPC service. db is the client for the
// Database Service (which owns all durable state); artifacts is the client
// for the Artifacts service (proxied to execution targets); hub is the event
// hub that backs the /api/ws WebSocket endpoint (may be nil); jobAuth verifies
// and mints the job tokens handed to execution targets (may be nil to disable
// job-token auth); secretStore decrypts a job's stored secret ciphertexts into
// plaintext at dispatch and redacts them from streamed logs (F-12, may be nil
// in tests that do not configure a store); logger is the logger (defaults to
// slog.Default()).
func NewGRPCServer(db dbpb.DatabaseClient, artifacts artifactspb.ArtifactsClient, hub *EventHub, jobAuth *JobTokenAuth, secretStore secrets.Store, logger *slog.Logger) *GRPCServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &GRPCServer{
		db:          db,
		artifacts:   artifacts,
		hub:         hub,
		jobAuth:     jobAuth,
		secretStore: secretStore,
		logger:      logger,
		live:        make(map[string]*liveWorker),
		watchers:    make(map[string]chan *apipb.WatchMessage),
		logCursors:  make(map[string]int64),
		coalescer:   newLogCoalescer(logCoalesceWindow),
		published:   make(map[int64]struct{}),
		logUids:     make(map[string]string),
		// The (namespace, name) <-> storage_object bridge (F-21.6 storage
		// redesign), wired from the clients at construction — the SAME
		// instance family the HTTP face uses (each face builds its own over
		// the shared clients; both translate identically).
		resolver: NewFileResolver(db, artifacts),
	}
}

// SetAuthz attaches the shared authorization engine (F-14, RBAC). The
// event-log tail loop uses it to invalidate the engine's cache when a
// role_change event arrives (F-23), so a role or binding change takes effect
// on this replica without a restart. It is the same engine instance the HTTP
// server uses.
func (s *GRPCServer) SetAuthz(e *authz.Engine) {
	s.authz = e
}

// SetResolver installs the (namespace, name) <-> storage_object bridge used
// by every translated artifact/log path (F-21.6 storage redesign). The HTTP
// face shares the SAME resolver instance (installed via Server.SetResolver).
func (s *GRPCServer) SetResolver(fr *fileResolver) {
	s.resolver = fr
}

// SetMetrics attaches the API's Prometheus metric set (F-20). When left nil
// (metrics disabled) all recording on the gRPC surface is a no-op.
func (s *GRPCServer) SetMetrics(m *metrics.APIMetrics) {
	s.metrics = m
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
		Name:         req.GetName(),
		Group:        req.GetGroup(),
		Address:      req.GetAddress(),
		Environments: req.GetEnvironments(),
		Tags:         req.GetTags(),
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
		name:         registered.GetName(),
		group:        registered.GetGroup(),
		environments: registered.GetEnvironments(),
		tags:         registered.GetTags(),
		address:      registered.GetAddress(),
		lastSeen:     time.Now(),
	}
	s.mu.Unlock()
	s.logger.Info("api: worker registered", "worker", registered.GetName(), "group", registered.GetGroup())
	s.recordAuditGRPC(audit.Event{
		Action:     audit.ActionWorkerRegister,
		TargetKind: audit.TargetWorker,
		TargetID:   registered.GetName(),
		TargetName: registered.GetName(),
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditJSON(map[string]any{"group": registered.GetGroup(), "address": registered.GetAddress()}),
	}, registered.GetName(), audit.ActorKindWorker, auditPeerAddr(ctx))
	s.publish(Event{Type: EventWorker, Worker: registered.GetName(), Group: registered.GetGroup(), Action: WorkerRegistered})
	s.publishWorkerEvent(ctx, registered.GetName(), registered.GetGroup(), WorkerRegistered)
	s.updateWorkerMetrics()
	return &apipb.Worker{
		Name:         registered.GetName(),
		Group:        registered.GetGroup(),
		Address:      registered.GetAddress(),
		Environments: registered.GetEnvironments(),
		Tags:         registered.GetTags(),
		LastSeenAt:   registered.GetLastSeenAt(),
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
	s.updateWorkerMetrics()
	s.logger.Info("api: worker deregistered", "worker", req.GetName())
	s.recordAuditGRPC(audit.Event{
		Action:     audit.ActionWorkerDeregister,
		TargetKind: audit.TargetWorker,
		TargetID:   req.GetName(),
		TargetName: req.GetName(),
		Outcome:    audit.OutcomeSuccess,
	}, req.GetName(), audit.ActorKindWorker, auditPeerAddr(ctx))
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
	s.live[name] = &liveWorker{name: name, group: req.GetGroup(), environments: req.GetEnvironments(), lastSeen: time.Now()}
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
	s.updateWorkerMetrics()
	for {
		select {
		case <-stream.Context().Done():
			s.logger.Info("api: worker stream closed", "worker", name)
			s.updateWorkerMetrics()
			return nil
		case message := <-assignmentChannel:
			if err := stream.Send(message); err != nil {
				s.logger.Warn("api: failed to send watch message", "worker", name, "err", err)
				s.updateWorkerMetrics()
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
	// Hand the agent the pipeline's secrets as plaintext (F-12), decrypted from
	// the job's stored ciphertexts, so a step's spec can reference them.
	s.resolveSecrets(ctx, apiJob, job)
	// Resolve the job's variables (F-21.1.3) into the run's parameters, so a
	// step's spec can reference them as `{{ .params.name }}`.
	s.resolveVariables(ctx, apiJob, job)
	// Insert an F-13 approval step when the job's environment requires approval
	// (F-17), so the target pauses at the gate before it runs.
	s.applyEnvironmentApprovalGate(ctx, apiJob)
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
	// Hand the worker the pipeline's secrets as plaintext (F-12), decrypted
	// from the job's stored ciphertexts, so a step's spec can reference them.
	s.resolveSecrets(ctx, apiJob, job)
	// Resolve the job's variables (F-21.1.3) into the run's parameters, so a
	// step's spec can reference them as `{{ .params.name }}`.
	s.resolveVariables(ctx, apiJob, job)
	// Insert an F-13 approval step when the job's environment requires approval
	// (F-17), so the target pauses at the gate before it runs.
	s.applyEnvironmentApprovalGate(ctx, apiJob)
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
	resp, err := s.db.ListPendingByGroup(ctx, &dbpb.ListPendingByGroupRequest{
		Group:        req.GetGroup(),
		Environments: req.GetEnvironments(),
	})
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
	if err := s.checkJobToken(ctx, req.GetJobId(), false); err != nil {
		return nil, err
	}
	// A report of awaiting_approval (F-13) is a job-level pause: the approval
	// gate is job-level (one approval unblocks every worker of a fan-out job),
	// so it is recorded on the job row — not a worker's execution — with the
	// message shown to the user. This applies to both group and single-target
	// jobs. The job row is updated directly (not via the execution), and the
	// status is published so the UI sees the gate and its message.
	if req.GetStatus() == dbpb.JobStatus_JOB_STATUS_AWAITING_APPROVAL {
		if s.db == nil {
			return nil, status.Error(codes.Unavailable, "database service is not configured")
		}
		updated, err := s.db.UpdateJob(ctx, &dbpb.UpdateJobRequest{
			Id:              req.GetJobId(),
			Status:          req.GetStatus(),
			ApprovalMessage: req.GetApprovalMessage(),
		})
		if err != nil {
			return nil, err
		}
		s.logger.Info("api: job awaiting approval", "job", req.GetJobId())
		s.recordAuditGRPC(audit.Event{
			Action:     audit.ActionJobStatus,
			TargetKind: audit.TargetJob,
			TargetID:   strconv.FormatInt(req.GetJobId(), 10),
			Outcome:    audit.OutcomeSuccess,
			Details:    auditJSON(map[string]any{"status": jobStatusName(updated.GetStatus()), "approval_message": req.GetApprovalMessage()}),
		}, s.reportActor(req.GetWorkerName(), req.GetJobId()), audit.ActorKindWorker, auditPeerAddr(ctx))
		s.publish(Event{
			Type:        EventJobStatus,
			JobID:       req.GetJobId(),
			Status:      jobStatusName(updated.GetStatus()),
			Attempt:     updated.GetAttempt(),
			MaxAttempts: updated.GetMaxAttempts(),
		})
		s.publishJobStatus(ctx, req.GetJobId(), updated.GetStatus(), updated.GetAttempt(), updated.GetMaxAttempts())
		s.recordJobStatusMetrics(ctx, updated)
		return toAPIJob(updated), nil
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
			s.recordAuditGRPC(audit.Event{
				Action:     audit.ActionJobStatus,
				TargetKind: audit.TargetJob,
				TargetID:   strconv.FormatInt(req.GetJobId(), 10),
				Outcome:    audit.OutcomeSuccess,
				Details:    auditJSON(map[string]any{"status": jobStatusName(req.GetStatus()), "worker": req.GetWorkerName()}),
			}, s.reportActor(req.GetWorkerName(), req.GetJobId()), audit.ActorKindWorker, auditPeerAddr(ctx))
			if s.metrics != nil {
				s.metrics.RecordJobStatus(jobStatusName(req.GetStatus()))
			}
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
	s.recordAuditGRPC(audit.Event{
		Action:     audit.ActionJobStatus,
		TargetKind: audit.TargetJob,
		TargetID:   strconv.FormatInt(req.GetJobId(), 10),
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"status": jobStatusName(updated.GetStatus())}),
	}, s.reportActor(req.GetWorkerName(), req.GetJobId()), audit.ActorKindWorker, auditPeerAddr(ctx))
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
	s.recordJobStatusMetrics(ctx, updated)
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
	if err := s.checkJobToken(ctx, req.GetJobId(), false); err != nil {
		return nil, err
	}
	if s.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	if _, err := s.db.ReportStepCompletion(ctx, &dbpb.ReportStepCompletionRequest{
		JobId:      req.GetJobId(),
		WorkerName: req.GetWorkerName(),
		StepName:   req.GetStepName(),
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
	if err := s.checkJobToken(ctx, req.GetJobId(), false); err != nil {
		return nil, err
	}
	if s.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	resp, err := s.db.CheckStepBarrier(ctx, &dbpb.CheckStepBarrierRequest{
		JobId:    req.GetJobId(),
		StepName: req.GetStepName(),
	})
	if err != nil {
		return nil, err
	}
	return &apipb.CheckStepBarrierResponse{
		Satisfied: resp.GetSatisfied(),
		Cancelled: resp.GetCancelled(),
	}, nil
}

// CheckApproval reports the current state of a job's approval gate (F-13):
// whether it has been resolved (approved or rejected) or whether the job has
// been cancelled. An execution target paused at an approval step polls it until
// the gate is resolved. When job-token auth is enabled the caller must present
// a job token scoped to the job.
func (s *GRPCServer) CheckApproval(ctx context.Context, req *apipb.CheckApprovalRequest) (*apipb.CheckApprovalResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	// allowTerminal: a rejection moves the job to failed (terminal), which
	// would otherwise invalidate the worker's token and make this poll fail
	// before it can observe the rejection. The poll is read-only, so it is
	// safe to keep serving it for a finished job.
	if err := s.checkJobToken(ctx, req.GetJobId(), true); err != nil {
		return nil, err
	}
	if s.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetJobId()})
	if err != nil {
		return nil, err
	}
	return &apipb.CheckApprovalResponse{
		Resolved:  job.GetApprovalDecision() != "",
		Decision:  job.GetApprovalDecision(),
		Cancelled: job.GetStatus() == dbpb.JobStatus_JOB_STATUS_CANCELLED,
	}, nil
}

// StreamJobLogs is a client stream: an execution target (worker or agent)
// opens it at the start of a job and sends a JobLogChunk for each chunk of
// step output (stdout/stderr). The first chunk of each step's output carries
// metadata (job_id, step_name, stream); subsequent chunks carry data. The
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
	if err := s.checkJobToken(stream.Context(), jobID, false); err != nil {
		return err
	}
	// Build the redactor for this job's secrets once (F-12): every chunk is
	// run through it before it is persisted or fanned out, so a secret's
	// plaintext never reaches the artifacts store or the UI.
	redactor := s.secretRedactorFor(stream.Context(), jobID)
	var received int64
	if err := s.handleLogChunk(stream.Context(), jobID, metadata, first.GetData(), &received, redactor); err != nil {
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
		if err := s.handleLogChunk(stream.Context(), jobID, chunk.GetMetadata(), chunk.GetData(), &received, redactor); err != nil {
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
// Before anything is persisted or fanned out, the chunk is run through the
// job's secret redactor (F-12), so a secret's plaintext is never written to
// the artifacts store or shown to the UI.
//
// Persistence is best-effort: if the artifacts service is unreachable the
// chunk is retried (appendLogWithRetry) and, if it still cannot be persisted
// within the retry budget, dropped — the stream is never torn down by an
// artifacts outage. The job_log event is always published so the UI sees the
// output in real time.
func (s *GRPCServer) handleLogChunk(ctx context.Context, jobID int64, metadata *apipb.JobLogMetadata, data []byte, received *int64, redactor *secretRedactor) error {
	// Redact any secret plaintext from the output before it is persisted or
	// fanned out (F-12). A nil redactor (no secrets) leaves the data as-is.
	data = redactor.redact(data)
	if len(data) == 0 {
		return nil
	}
	stepName := "step-0"
	streamName := JobLogStreamStdout
	if metadata != nil {
		if name := metadata.GetStepName(); name != "" {
			stepName = name
		}
		streamName = jobLogStreamName(metadata.GetStream())
	}
	jobIDStr := strconv.FormatInt(jobID, 10)
	// Persist to the artifacts service: one file per step plus the combined
	// job log. A nil artifacts client (e.g. in tests) skips persistence but
	// still fans the output out to the UI. A failure to persist (the artifacts
	// service is down and the retry budget is exhausted) drops the chunk
	// rather than failing the stream, so an artifacts outage never tears down
	// a target's log stream.
	//
	// DURABILITY DECOUPLEMENT (critical): persistence runs on a DETACHED
	// context (context.Background), NOT the stream's. A job that finishes in
	// milliseconds makes the worker CLOSE the stream almost immediately; the
	// stream's context then cancels, and any persistence still in flight
	// (establishing the log's record, appending the blob) abandons the
	// BUFFERED chunks — losing exactly the lines a consumer (the UI, an e2e
	// assertion) waits for. The persisted log must be complete even when the
	// producing stream is already gone.
	if s.artifacts != nil {
		durCtx := context.Background()
		stepLogName := stepName + ".log"
		// Persist the per-step log first. If it cannot be persisted (the
		// artifacts service is down and the retry budget is exhausted) the
		// chunk is dropped and the combined job log is skipped too — both
		// files live in the same (unreachable) store, so retrying the second
		// would only double the time spent blocked on a down service.
		if _, err := s.appendLogWithRetry(durCtx, jobIDStr, stepLogName, data); err != nil {
			s.logger.Warn("api: dropping log chunk (artifacts unreachable)",
				"job", jobID, "step", stepName, "err", err)
		} else if size, err := s.appendLogWithRetry(durCtx, jobIDStr, jobLogName, data); err != nil {
			s.logger.Warn("api: dropping job log chunk (artifacts unreachable)",
				"job", jobID, "log", jobLogName, "err", err)
		} else {
			// The combined log grew to size bytes. Feed the coalescer (F-23):
			// at most one job_log_updated event per (job, job.log) per window
			// is published by the tail loop, carrying the latest size, so other
			// API pods can range-read the delta from the shared artifacts store.
			s.coalescer.record(jobID, jobLogName, stepName, streamName, size)
		}
	}
	*received += int64(len(data))
	if s.metrics != nil {
		s.metrics.AddLogStreamBytes(int64(len(data)))
	}
	s.publish(Event{
		Type:     EventJobLog,
		JobID:    jobID,
		StepName: stepName,
		Stream:   streamName,
		Data:     string(data),
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

// logUIDFor returns the uid of the blob holding the job's named log,
// ESTABLISHING it (allocate-kind=log) when the record does not exist yet.
// Established uids are cached per (jobID|name) so a log stream's BURSTS of
// chunks cause no per-chunk database round trips (F-21.6 storage redesign).
// Allocation adopts a sibling pod's provisional record for the same
// coordinate (stable uid), so racing creators converge on one blob.
func (s *GRPCServer) logUIDFor(ctx context.Context, jobID, name string) (string, error) {
	key := jobID + "|" + name
	s.logUidsMu.Lock()
	if uid, ok := s.logUids[key]; ok {
		s.logUidsMu.Unlock()
		return uid, nil
	}
	s.logUidsMu.Unlock()
	if s.resolver == nil {
		return "", status.Error(codes.Unavailable, "file resolver is not configured")
	}
	rec, err := s.resolver.ensureLog(ctx, jobID, name)
	if err != nil {
		return "", err
	}
	s.logUidsMu.Lock()
	s.logUids[key] = rec.GetUid()
	s.logUidsMu.Unlock()
	return rec.GetUid(), nil
}

// appendLog appends data to the named log for the job via the blob service's
// AppendBlob stream (addressed by the log's uid, F-21.6 storage redesign). It
// returns the log's total size (in bytes) after the append, which the caller
// uses to publish a job_log_updated pointer event (F-23): the publisher
// already knows the size from the AppendBlob response, so no extra stat call
// is needed. The record's size is reconciled BEST-EFFORT (syncLogSize swallows
// its own errors — the next append retries, and the size only feeds UI
// polling, never correctness).
func (s *GRPCServer) appendLog(ctx context.Context, jobID, name string, data []byte) (int64, error) {
	uid, err := s.logUIDFor(ctx, jobID, name)
	if err != nil {
		return 0, err
	}
	stream, err := s.artifacts.AppendBlob(ctx)
	if err != nil {
		return 0, err
	}
	if err := stream.Send(&artifactspb.BlobChunk{Id: uid, Data: data}); err != nil {
		return 0, err
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return 0, err
	}
	size := resp.GetSize()
	if s.resolver != nil {
		s.resolver.syncLogSize(ctx, uid, size)
	}
	return size, nil
}

// finalizeJobLogs seals a job's logs when the job reaches a TERMINAL state
// (F-21.6 storage redesign): each cached (job, log) uid's record size is
// reconciled with the blob's FINAL size (the last append's response is the
// authority, but a belt-and-braces stat here covers the corner where the
// final chunk was dropped by the retry budget) and the uid CACHE ENTRIES are
// EVICTED (a terminated job's logs are sealed; caching them forever would
// leak memory across a fleet's worth of finished jobs). Best-effort: a
// failure here never affects the status report that triggered it.
func (s *GRPCServer) finalizeJobLogs(ctx context.Context, jobID int64) {
	jobIDStr := strconv.FormatInt(jobID, 10)
	s.logUidsMu.Lock()
	keys := make([]string, 0)
	for key := range s.logUids {
		if strings.HasPrefix(key, jobIDStr+"|") {
			keys = append(keys, key)
		}
	}
	s.logUidsMu.Unlock()
	if len(keys) == 0 || s.resolver == nil {
		return
	}
	for _, key := range keys {
		uid := s.takeLogUID(key)
		if uid == "" {
			continue
		}
		ref := &artifactspb.BlobRef{Id: uid}
		if stat, err := s.artifacts.StatBlob(ctx, ref); err == nil {
			s.resolver.syncLogSize(ctx, uid, stat.GetSize())
		}
	}
}

// takeLogUID pops a cached uid (under the lock) for eviction.
func (s *GRPCServer) takeLogUID(key string) string {
	s.logUidsMu.Lock()
	defer s.logUidsMu.Unlock()
	uid := s.logUids[key]
	delete(s.logUids, key)
	return uid
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
// audience mid-job. The exchanged token has a short lifetime (the request's
// expires_in, or the configured default — 15 minutes when unset — when unset)
// — unlike the job's own long-lived token, an exchanged token is a scoped
// credential for an outside resource. When job-token auth is disabled the RPC
// is a no-op that returns an empty token.
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
	if err := s.checkJobToken(ctx, req.GetJobId(), false); err != nil {
		return nil, err
	}
	// Look up the job so the new token carries the same pipeline/group scope
	// and the same trigger and upstream claims as the job's original token.
	// A nil db client (e.g. in tests) simply means the scope is left empty.
	var pipelineID int64
	var targetGroup, triggerName, triggerType string
	var upstreamClaims map[string]any
	if s.db != nil {
		job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetJobId()})
		if err != nil {
			return nil, err
		}
		pipelineID = job.GetPipelineId()
		targetGroup = job.GetTargetGroup()
		triggerName = job.GetTriggerName()
		triggerType = job.GetTriggerType()
		upstreamClaims = upstreamClaimsFromStruct(job.GetUpstreamClaims())
	}
	// The exchanged token's lifetime: the request's expires_in when set, else
	// the configured default (Exchange falls back to it when zero).
	var expiresIn time.Duration
	if d := req.GetExpiresIn(); d != nil {
		expiresIn = d.AsDuration()
	}
	token, err := s.jobAuth.Exchange(ctx, req.GetJobId(), pipelineID, targetGroup, triggerName, triggerType, req.GetAudience(), upstreamClaims, expiresIn)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "api: exchange job token: %v", err)
	}
	s.logger.Info("api: job token exchanged", "job", req.GetJobId(), "audience", req.GetAudience(), "expires_in", expiresIn)
	if s.metrics != nil {
		s.metrics.RecordTokenMinted("exchanged")
	}
	s.recordAuditGRPC(audit.Event{
		Action:     audit.ActionTokenExchange,
		TargetKind: audit.TargetJob,
		TargetID:   strconv.FormatInt(req.GetJobId(), 10),
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"audience": req.GetAudience(), "expires_in": expiresIn.String()}),
	}, s.reportActor("", req.GetJobId()), audit.ActorKindWorker, auditPeerAddr(ctx))
	return &apipb.ExchangeJobTokenResponse{Token: token}, nil
}

// reportActor returns the actor identity for a job status report or token
// exchange from an execution target: the worker's name when the report names
// a worker (a long-lived worker), or the job's identity ("job:<id>") when it
// does not (an ephemeral agent reports against the job row directly).
func (s *GRPCServer) reportActor(workerName string, jobID int64) string {
	if workerName != "" {
		return workerName
	}
	return "job:" + strconv.FormatInt(jobID, 10)
}

// ---------------------------------------------------------------------------
// Notifications (F-18)
// ---------------------------------------------------------------------------

// SendEmail sends an SMTP message from the API's configured email server
// (F-18). The execution target renders the email step's fields against the
// step's condition context and hands the rendered values to the API; the API is
// the only component that opens an SMTP connection. When job-token auth is
// enabled the caller must present a job token scoped to the job.
//
// The body's content type selects how it is sent: "plain" (a text/plain
// message), "html" (a text/html message, sent as-is), or "markdown" (rendered
// to HTML by the API). A send that fails (an SMTP error) is reported as an
// error, which fails the step (and the job) unless the step sets
// ignore_failed (F-06).
func (s *GRPCServer) SendEmail(ctx context.Context, req *apipb.SendEmailRequest) (resp *apipb.SendEmailResponse, err error) {
	if s.metrics != nil {
		defer func() {
			if err == nil {
				s.metrics.RecordNotification("email", "sent")
			} else {
				s.metrics.RecordNotification("email", "failed")
			}
		}()
	}
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if err := s.checkJobToken(ctx, req.GetJobId(), false); err != nil {
		return nil, err
	}
	if !s.email.Configured() {
		return nil, status.Error(codes.FailedPrecondition, "no email server is configured on the API")
	}
	if len(req.GetTo()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one recipient (to) is required")
	}
	// The sender: the step's from override, else the API's configured sender.
	from := req.GetFrom()
	if from == "" {
		from = s.email.From
	}
	if from == "" {
		return nil, status.Error(codes.FailedPrecondition, "no sender (from) is configured; set the API's email.from or the step's from")
	}
	// The content type selects how the body is sent (plain / html / markdown).
	contentType := strings.ToLower(req.GetContentType())
	if contentType == "" {
		contentType = "markdown"
	}
	var body, mimeSubtype string
	switch contentType {
	case "plain":
		body, mimeSubtype = req.GetBody(), "plain"
	case "html":
		body, mimeSubtype = req.GetBody(), "html"
	case "markdown":
		body, mimeSubtype = string(renderMarkdown(req.GetBody())), "html"
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported content_type %q (want plain, html, or markdown)", req.GetContentType())
	}
	// Build the MIME message. BCC recipients are in the message but not in the
	// SMTP envelope (so they are not revealed to the other recipients); the
	// envelope carries the To and Cc recipients.
	var msg bytes.Buffer
	fmt.Fprintf(&msg, "From: %s\r\n", from)
	fmt.Fprintf(&msg, "To: %s\r\n", strings.Join(req.GetTo(), ", "))
	if len(req.GetCc()) > 0 {
		fmt.Fprintf(&msg, "Cc: %s\r\n", strings.Join(req.GetCc(), ", "))
	}
	if len(req.GetBcc()) > 0 {
		fmt.Fprintf(&msg, "Bcc: %s\r\n", strings.Join(req.GetBcc(), ", "))
	}
	if req.GetReplyTo() != "" {
		fmt.Fprintf(&msg, "Reply-To: %s\r\n", req.GetReplyTo())
	}
	if req.GetSubject() != "" {
		fmt.Fprintf(&msg, "Subject: %s\r\n", req.GetSubject())
	}
	msg.WriteString("MIME-Version: 1.0\r\n")
	fmt.Fprintf(&msg, "Content-Type: text/%s; charset=UTF-8\r\n", mimeSubtype)
	msg.WriteString("\r\n")
	msg.WriteString(body)

	// The SMTP envelope recipients: To and Cc (BCC is hidden from the envelope).
	recipients := make([]string, 0, len(req.GetTo())+len(req.GetCc()))
	recipients = append(recipients, req.GetTo()...)
	recipients = append(recipients, req.GetCc()...)

	addr := net.JoinHostPort(s.email.Host, strconv.Itoa(s.email.EffectivePort()))
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "email: dial %s: %v", addr, err)
	}
	client, err := smtp.NewClient(conn, s.email.Host)
	if err != nil {
		_ = conn.Close()
		return nil, status.Errorf(codes.Internal, "email: %v", err)
	}
	defer client.Close()
	// Upgrade to TLS via STARTTLS when the API is configured to (the default).
	if s.email.EffectiveStartTLS() {
		if ok, _ := client.Extension("STARTTLS"); ok {
			if err := client.StartTLS(&tls.Config{ServerName: s.email.Host, InsecureSkipVerify: s.email.Insecure}); err != nil {
				return nil, status.Errorf(codes.Internal, "email: starttls: %v", err)
			}
		}
	}
	// Authenticate to the SMTP server when a username is configured.
	if s.email.Username != "" {
		if ok, _ := client.Extension("AUTH"); ok {
			if err := client.Auth(smtp.PlainAuth("", s.email.Username, s.email.Password, s.email.Host)); err != nil {
				return nil, status.Errorf(codes.Internal, "email: auth: %v", err)
			}
		}
	}
	if err := client.Mail(from); err != nil {
		return nil, status.Errorf(codes.Internal, "email: mail: %v", err)
	}
	for _, rcpt := range recipients {
		if err := client.Rcpt(rcpt); err != nil {
			return nil, status.Errorf(codes.Internal, "email: rcpt %s: %v", rcpt, err)
		}
	}
	wr, err := client.Data()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "email: data: %v", err)
	}
	if _, err := wr.Write(msg.Bytes()); err != nil {
		return nil, status.Errorf(codes.Internal, "email: write: %v", err)
	}
	if err := wr.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "email: close: %v", err)
	}
	if err := client.Quit(); err != nil {
		return nil, status.Errorf(codes.Internal, "email: quit: %v", err)
	}
	s.logger.Info("api: sent email", "job", req.GetJobId(), "to", req.GetTo(), "content_type", contentType)
	return &apipb.SendEmailResponse{}, nil
}

// SendWebhook performs an HTTP request from the API to the step's URL (F-18).
// The execution target renders the step's fields against the step's condition
// context and hands the rendered values to the API; the API is the only
// component that opens the outbound HTTP connection. When job-token auth is
// enabled the caller must present a job token scoped to the job.
//
// The body's content type selects how it is sent: "plain" (sent as-is, e.g.
// JSON/YAML — the step's own headers, e.g. Content-Type, apply), "html" (sent
// as-is), or "markdown" (rendered to HTML by the API). The response timeout is
// the step's requested timeout clamped to the API's configured maximum, or the
// API's configured default when the step sets none. A request that fails (a
// connection error, a non-2xx response, or a timeout) is reported as an error,
// which fails the step (and the job) unless the step sets ignore_failed (F-06).
func (s *GRPCServer) SendWebhook(ctx context.Context, req *apipb.SendWebhookRequest) (resp *apipb.SendWebhookResponse, err error) {
	if s.metrics != nil {
		defer func() {
			if err == nil {
				s.metrics.RecordNotification("webhook", "sent")
			} else {
				s.metrics.RecordNotification("webhook", "failed")
			}
		}()
	}
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if err := s.checkJobToken(ctx, req.GetJobId(), false); err != nil {
		return nil, err
	}
	if req.GetUrl() == "" {
		return nil, status.Error(codes.InvalidArgument, "url is required")
	}
	// The response timeout: the step's requested timeout (clamped to the
	// configured maximum), or the configured default when the step sets none.
	timeout := s.webhook.EffectiveTimeout()
	if d := req.GetTimeout(); d != nil {
		if t := d.AsDuration(); t > 0 {
			if max := s.webhook.EffectiveTimeoutMax(); t > max {
				t = max
			}
			timeout = t
		}
	}
	// The content type selects how the body is sent (plain / html / markdown).
	contentType := strings.ToLower(req.GetContentType())
	if contentType == "" {
		contentType = "plain"
	}
	body := req.GetBody()
	var bodyCT string
	switch contentType {
	case "plain":
		// Sent as-is; the step's own headers (e.g. Content-Type) apply.
	case "html":
		bodyCT = "text/html; charset=UTF-8"
	case "markdown":
		body = string(renderMarkdown(req.GetBody()))
		bodyCT = "text/html; charset=UTF-8"
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported content_type %q (want plain, html, or markdown)", req.GetContentType())
	}
	method := req.GetMethod()
	if method == "" {
		method = http.MethodPost
	}
	// Bound the request by the clamped timeout (on top of the step's own
	// context, which is already bounded by the step's timeout when set).
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(reqCtx, method, req.GetUrl(), strings.NewReader(body))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "webhook: %v", err)
	}
	// The step's headers (each a "Name: Value" entry). A step-supplied
	// Content-Type takes precedence over the one derived from the content type.
	for _, h := range req.GetHeaders() {
		name, value, ok := strings.Cut(h, ":")
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "webhook: header %q is not a \"Name: Value\" entry", h)
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, status.Errorf(codes.InvalidArgument, "webhook: header %q has an empty name", h)
		}
		httpReq.Header.Set(name, strings.TrimSpace(value))
	}
	if bodyCT != "" && httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", bodyCT)
	}
	client := s.webhookClient
	if client == nil {
		client = http.DefaultClient
	}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "webhook: %v", err)
	}
	defer httpResp.Body.Close()
	_, _ = io.Copy(io.Discard, httpResp.Body)
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, status.Errorf(codes.Internal, "webhook: %s %s returned %d", method, req.GetUrl(), httpResp.StatusCode)
	}
	s.logger.Info("api: sent webhook", "job", req.GetJobId(), "url", req.GetUrl(), "status", httpResp.StatusCode)
	return &apipb.SendWebhookResponse{StatusCode: int32(httpResp.StatusCode)}, nil
}

// ClaimStepOnce (F-01) atomically claims the "run this step" slot for a step
// of a job. The execution target calls it before running a step with the
// `once` field set: when it returns true this target won the claim (it should
// run the step) and when it returns false another target already claimed it in
// the same attempt (this target should skip the step). The claim is durable in
// the Database service (keyed by job id + step name + attempt), so a retry
// (a new attempt) may run the step again. When job-token auth is enabled the
// caller must present a job token scoped to the job.
func (s *GRPCServer) ClaimStepOnce(ctx context.Context, req *apipb.ClaimStepOnceRequest) (*apipb.ClaimStepOnceResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if err := s.checkJobToken(ctx, req.GetJobId(), false); err != nil {
		return nil, err
	}
	if s.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	resp, err := s.db.ClaimStepOnce(ctx, &dbpb.ClaimStepOnceRequest{
		JobId:    req.GetJobId(),
		StepName: req.GetStepName(),
		Attempt:  req.GetAttempt(),
	})
	if err != nil {
		return nil, err
	}
	return &apipb.ClaimStepOnceResponse{Claimed: resp.GetClaimed()}, nil
}

// renderMarkdown renders markdown to HTML using gomarkdown. It is used to
// render a notification body whose content type is "markdown" (F-18) before it
// is sent (an email's HTML body, or a webhook's HTML body).
func renderMarkdown(md string) []byte {
	return markdown.ToHTML([]byte(md), nil, nil)
}

// ---------------------------------------------------------------------------
// Dispatch (called by the scheduler)
// ---------------------------------------------------------------------------

// DispatchJob fans a job out to the live workers that serve it. A job that
// targets an environment (F-17) is dispatched to the live workers that belong
// to it; a job that targets a raw group (and no environment) is dispatched to
// the live workers in that group. A job with neither an environment nor a
// target group is for an ephemeral agent and is not dispatched to live
// workers.
func (s *GRPCServer) DispatchJob(ctx context.Context, req *apipb.DispatchJobRequest) (*apipb.DispatchJobResponse, error) {
	job := req.GetJob()
	if job == nil {
		return nil, status.Error(codes.InvalidArgument, "job is required")
	}
	group := job.GetTargetGroup()
	environment := job.GetEnvironment()
	if group == "" && environment == "" {
		return &apipb.DispatchJobResponse{Dispatched: 0}, nil
	}
	// Hand the workers the status/outputs of the jobs this job depends on
	// (F-06) so a step's condition can reference them.
	job.UpstreamJobs = s.upstreamJobsFor(ctx, job)
	// Mint a job token so the workers can authenticate their status reports
	// and any outside-resource calls for this job.
	token := s.mintJobToken(ctx, job)
	dispatched := s.dispatch(group, environment, job, token)
	if dispatched == 0 {
		s.logger.Warn("api: no live workers for job; job left pending",
			"job", job.GetId(), "group", group, "environment", environment)
	} else {
		s.logger.Info("api: job dispatched", "job", job.GetId(), "group", group, "environment", environment, "workers", dispatched)
	}
	// Append an assignment event to the shared event log (F-23) so other API
	// pods can nudge their local workers in the group (or, for an environment
	// job, that belong to the environment). This pod already pushed directly
	// to its local workers above; the event log is the cross-pod path.
	s.publishAssignment(ctx, job.GetId(), group, environment)
	return &apipb.DispatchJobResponse{Dispatched: int32(dispatched)}, nil
}

// publishAssignment appends an "assignment" event to the shared event log
// (F-23). It is best-effort: a failure to append does not fail the dispatch
// (the worker's periodic poll is the authoritative path).
func (s *GRPCServer) publishAssignment(ctx context.Context, jobID int64, group, environment string) {
	if s.db == nil {
		return
	}
	resp, err := s.db.PublishAssignment(ctx, &dbpb.PublishAssignmentRequest{JobId: jobID, TargetGroup: group, Environment: environment})
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
// event log (F-23): it carries only the job id, log name, step name, stream,
// and the current byte size — never the log bytes. Every API pod tails the
// log and, when it has UI clients watching the job, range-reads the new bytes
// from the shared artifacts store and fans them out.
func (s *GRPCServer) publishLogUpdated(ctx context.Context, jobID int64, log string, stepName string, stream string, size int64) {
	if s.db == nil {
		return
	}
	resp, err := s.db.PublishLogUpdated(ctx, &dbpb.PublishLogUpdatedRequest{
		JobId:    jobID,
		Log:      log,
		StepName: stepName,
		Stream:   stream,
		Size:     size,
	})
	if err != nil {
		s.logger.Warn("failed to publish job_log_updated event", "job_id", jobID, "error", err)
		return
	}
	s.markPublished(resp.GetId())
}

// dispatch pushes an assignment (with the job token) to every live worker that
// serves the job and returns how many workers received it. A job that targets
// an environment (F-17) is pushed to the live workers that belong to it; a job
// that targets a raw group (and no environment) is pushed to the live workers
// in that group. The push is best-effort: a worker that receives it but does
// not actually serve the job (e.g. it is not in the environment's target
// group) is rejected when it starts the job (StartJobExecution is the
// authoritative routing gate).
func (s *GRPCServer) dispatch(group, environment string, job *apipb.Job, token string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var dispatchedCount int
	for name, worker := range s.live {
		if environment != "" {
			if !slices.Contains(worker.environments, environment) {
				continue
			}
		} else if worker.group != group {
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
// a valid token). The token carries the trigger that started the run
// (trigger_name/trigger_type) and the upstream OIDC claims a webhook caller
// presented (prefixed with upstream_), so a job's steps — and the outside
// resources they call — can see how the run was started.
func (s *GRPCServer) mintJobToken(ctx context.Context, job *apipb.Job) string {
	if !s.jobAuth.Enabled() {
		return ""
	}
	token, err := s.jobAuth.Mint(ctx, job.GetId(), job.GetPipelineId(), job.GetTargetGroup(),
		job.GetTriggerName(), job.GetTriggerType(), upstreamClaimsFromStruct(job.GetUpstreamClaims()))
	if err != nil {
		s.logger.Warn("api: mint job token failed; dispatching without token", "job", job.GetId(), "err", err)
		return ""
	}
	if s.metrics != nil {
		s.metrics.RecordTokenMinted("job")
	}
	return token
}

// ---------------------------------------------------------------------------
// Artifact proxy (forwarded to the artifacts service)
// ---------------------------------------------------------------------------

// toLogFile renders a storage_object record as the target-facing LogFile
// view (the logical file contract: namespace, name, size, born-at).
func toLogFile(rec *dbpb.StorageObject) *apipb.LogFile {
	return &apipb.LogFile{
		Namespace: rec.GetNamespace(),
		Name:      rec.GetName(),
		Size:      rec.GetSize(),
		CreatedAt: rec.GetCreatedAt(),
	}
}

// pumpUpload relays a target-facing LogFileChunk stream into a blob PutBlob
// CLIENT stream seeded with the record's uid, measuring the SHA-256 of the
// relayed bytes AS THEY FLOW (the API is the meter of record — writer
// attestations are never trusted). It returns the measured hash and the
// blob-reported stored size.
func pumpUpload(in grpc.ClientStreamingServer[apipb.LogFileChunk, apipb.LogFile], out grpc.ClientStreamingClient[artifactspb.BlobChunk, artifactspb.BlobStat], first *apipb.LogFileChunk, uid string) (string, int64, error) {
	h := sha256.New()
	send := func(f *apipb.LogFileChunk, withID bool) error {
		ch := &artifactspb.BlobChunk{Data: f.GetData()}
		if withID {
			ch.Id = uid
		}
		h.Write(f.GetData())
		return out.Send(ch)
	}
	if err := send(first, true); err != nil {
		return "", 0, err
	}
	for {
		chunk, err := in.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", 0, err
		}
		if err := send(chunk, false); err != nil {
			return "", 0, err
		}
	}
	stat, err := out.CloseAndRecv()
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), stat.GetSize(), nil
}

// UploadArtifact accepts a client-streaming artifact upload from an execution
// target and stores it THROUGH THE STORAGE-OBJECT LEDGER (F-21.6 storage
// redesign): the target's (namespace, name) contract is translated to a
// record (allocating one) and a blob (PutBlob by uid); the record is then
// confirmed with the blob-reported size and the API-measured SHA-256. A job
// may WRITE only into its own (job-id) namespace — the publish_artifact step
// relies on exactly this; run namespaces ("run:<id>") and the package
// registry are read-only for jobs (movement between namespaces is the
// API-side promotion operation).
func (s *GRPCServer) UploadArtifact(stream grpc.ClientStreamingServer[apipb.LogFileChunk, apipb.LogFile]) (err error) {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	meta := first.GetMetadata()
	if meta == nil || meta.GetNamespace() == "" || meta.GetName() == "" {
		return status.Error(codes.InvalidArgument, "first chunk must carry metadata with namespace and name")
	}
	ns, name := meta.GetNamespace(), meta.GetName()
	if strings.HasPrefix(ns, "run:") {
		return status.Error(codes.PermissionDenied, "a job cannot upload into a run's namespace")
	}
	if ns == "packages" {
		// The registry is human/CI territory (POST /api/packages); a job
		// deposits packages into its OWN namespace via publish_artifact.
		return status.Error(codes.PermissionDenied, "a job cannot upload into the package registry")
	}
	if err := s.checkArtifactToken(stream.Context(), ns); err != nil {
		return err
	}
	if s.resolver == nil {
		return status.Error(codes.Unavailable, "file resolver is not configured")
	}
	// Provision the record. Allocation ADOPTS a sibling's provisional record
	// for the same coordinate (stable uid across interruptions), so a
	// restarted uploader converges on the same blob.
	rec, err := s.resolver.allocate(stream.Context(), &dbpb.AllocateStorageObjectRequest{
		Kind:       "artifact",
		Namespace:  ns,
		Name:       name,
		UploadedBy: "job:" + ns, // a writable namespace is always the job's own id
	})
	if err != nil {
		return err
	}
	// Clear any PARTIAL blob a previous interrupted upload left at this uid
	// (the allocation adopts the sibling's record; the blob may linger
	// non-empty, and a put to a non-empty id fails) — a no-op when the blob
	// is absent.
	_, _ = s.artifacts.DeleteBlob(stream.Context(), &artifactspb.BlobRef{Id: rec.GetUid()})
	put, err := s.artifacts.PutBlob(stream.Context())
	if err != nil {
		return err
	}
	sum, size, err := pumpUpload(stream, put, first, rec.GetUid())
	if err != nil {
		// Interrupted upload: abandon the partial blob (the record stays
		// provisional and is reusable via adoption; pruning ages it out).
		_, _ = s.artifacts.DeleteBlob(stream.Context(), &artifactspb.BlobRef{Id: rec.GetUid()})
		return err
	}
	confirmed, err := s.resolver.confirm(stream.Context(), rec.GetUid(), size, sum)
	if err != nil {
		return err
	}
	if s.metrics != nil {
		s.metrics.AddArtifactBytes("upload", "artifact", size)
	}
	return stream.SendAndClose(toLogFile(confirmed))
}

// DownloadArtifact streams a file back to an execution target. The
// (namespace, name) contract is translated to the record's blob uid; the
// blob's HEAD FRAME (total size) is folded into the LogFile metadata so the
// target sees the TRUE size (exact for still-growing logs). A "run:<id>"
// pseudo-namespace is substituted with the run's actual store namespace
// after verifying the caller's job belongs to that run (F-21.6); a numeric
// namespace is the caller's own job-id namespace. Retrieval is recorded on
// the record (best-effort analytics).
func (s *GRPCServer) DownloadArtifact(req *apipb.LogFileRef, stream grpc.ServerStreamingServer[apipb.LogFileChunk]) error {
	effective, err := s.effectiveArtifactNamespace(stream.Context(), req.GetNamespace())
	if err != nil {
		return err
	}
	if s.resolver == nil {
		return status.Error(codes.Unavailable, "file resolver is not configured")
	}
	rec, err := s.resolver.getFile(stream.Context(), effective, req.GetName())
	if err != nil {
		return err
	}
	remote, err := s.artifacts.GetBlob(stream.Context(), &artifactspb.BlobRef{Id: rec.GetUid()})
	if err != nil {
		return err
	}
	head, err := remote.Recv()
	if err != nil {
		return err
	}
	view := toLogFile(rec)
	view.Size = head.GetSize() // the blob's truth (authoritative for logs)
	if err := stream.Send(&apipb.LogFileChunk{Metadata: view}); err != nil {
		return err
	}
	var downloaded int64
	for {
		chunk, err := remote.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&apipb.LogFileChunk{Data: chunk.GetData()}); err != nil {
			return err
		}
		downloaded += int64(len(chunk.GetData()))
	}
	if s.metrics != nil {
		s.metrics.AddArtifactBytes("download", "artifact", downloaded)
	}
	s.resolver.touch(stream.Context(), rec.GetUid())
	return nil
}

// GetArtifact returns a file's metadata (translated from its record).
func (s *GRPCServer) GetArtifact(ctx context.Context, req *apipb.LogFileRef) (*apipb.LogFile, error) {
	effective, err := s.effectiveArtifactNamespace(ctx, req.GetNamespace())
	if err != nil {
		return nil, err
	}
	if s.resolver == nil {
		return nil, status.Error(codes.Unavailable, "file resolver is not configured")
	}
	rec, err := s.resolver.getFile(ctx, effective, req.GetName())
	if err != nil {
		return nil, err
	}
	return toLogFile(rec), nil
}

// ListArtifacts lists a namespace's files (translated from its records). A
// non-empty namespace is scoped to that namespace's token; an empty
// namespace lists all artifacts and is not job-scoped.
func (s *GRPCServer) ListArtifacts(ctx context.Context, req *apipb.ListLogFilesRequest) (*apipb.ListLogFilesResponse, error) {
	namespace := req.GetNamespace()
	effective := namespace
	if namespace != "" {
		var err error
		effective, err = s.effectiveArtifactNamespace(ctx, namespace)
		if err != nil {
			return nil, err
		}
	}
	if s.resolver == nil {
		return nil, status.Error(codes.Unavailable, "file resolver is not configured")
	}
	objects, err := s.resolver.listFiles(ctx, effective, "artifact")
	if err != nil {
		return nil, err
	}
	out := &apipb.ListLogFilesResponse{}
	for _, o := range objects {
		out.Files = append(out.Files, toLogFile(o))
	}
	return out, nil
}

// DeleteArtifact removes a file (record THEN blob). A job may delete only
// from its own (job-id) namespace; run namespaces and the package registry
// are read-only for jobs (F-21.6).
func (s *GRPCServer) DeleteArtifact(ctx context.Context, req *apipb.LogFileRef) (*emptypb.Empty, error) {
	ns := req.GetNamespace()
	if strings.HasPrefix(ns, "run:") {
		return nil, status.Error(codes.PermissionDenied, "a job cannot delete from a run's namespace")
	}
	if ns == "packages" {
		return nil, status.Error(codes.PermissionDenied, "a job cannot delete from the package registry")
	}
	if err := s.checkArtifactToken(ctx, ns); err != nil {
		return nil, err
	}
	if s.resolver == nil {
		return nil, status.Error(codes.Unavailable, "file resolver is not configured")
	}
	if err := s.resolver.delete(ctx, ns, req.GetName()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// checkArtifactToken, when job-token auth is enabled, verifies the caller's
// job token is scoped to the job that owns the artifact. A job's artifacts
// and logs use the job's id as the namespace, so the namespace is parsed back
// to a job id and checked against the token. It is a no-op when job-token
// auth is disabled or the namespace is empty (not job-scoped).
//
// F-21.6 widens READ access to the run's store namespace: a namespace of the
// form "run:<id>" is substituted with the run's ACTUAL store namespace (the
// run's id — the database service fetches the run's packages there) and the
// caller's token is checked against the run's OWNING job (the job that
// presents the token must belong to that run). Writes (uploads) are never
// accepted into a run namespace — a job deposits packages into ITS OWN
// (job-id) namespace via the publish_artifact step, and movement between
// namespaces is the promotion operation (API-side, user-authenticated).
func (s *GRPCServer) checkArtifactToken(ctx context.Context, namespace string) error {
	if !s.jobAuth.Enabled() || namespace == "" {
		return nil
	}
	id, err := strconv.ParseInt(namespace, 10, 64)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid artifact namespace")
	}
	return s.checkJobToken(ctx, id, false)
}

// effectiveArtifactNamespace resolves the namespace a proxied artifact call
// should ADDRESS, enforcing the caller's job-token scope (when job-token
// auth is enabled):
//
//   - an EMPTY namespace (list-all) passes through unchanged;
//   - the package REGISTRY namespace ("packages") is PUBLIC for reads: any
//     job with a valid (ACTIVE) token may download published packages. It is
//     not confusable with a job-id namespace (it is not numeric), so only the
//     token/activity check applies — no ownership check;
//   - a NUMERIC namespace is the caller's own job-id namespace (verified
//     against the token when auth is enabled);
//   - anything ELSE (including the retired "run:<id>" pseudo-namespace — run
//     namespaces no longer hold files since the storage redesign; a run's
//     packages are pinned by the per-run snapshot and read straight from the
//     registry) is rejected.
func (s *GRPCServer) effectiveArtifactNamespace(ctx context.Context, namespace string) (string, error) {
	if namespace == "" {
		return namespace, nil
	}
	// The package REGISTRY namespace (F-21.6) is PUBLIC for reads: any job
	// with a valid (active) token may download published packages.
	if namespace == "packages" {
		if s.jobAuth.Enabled() {
			token := bearerToken(ctx)
			if token == "" {
				return "", status.Error(codes.Unauthenticated, "job token is required")
			}
			jobID, verr := s.jobAuth.Verify(ctx, token)
			if verr != nil {
				return "", status.Error(codes.Unauthenticated, verr.Error())
			}
			if err := s.checkJobActive(ctx, jobID); err != nil {
				return "", err
			}
		}
		return namespace, nil
	}
	id, err := strconv.ParseInt(namespace, 10, 64)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "invalid artifact namespace %q (allowed: a job id, or \"packages\")", namespace)
	}
	if err := s.checkJobToken(ctx, id, false); err != nil {
		return "", err
	}
	return namespace, nil
}

// checkJobActive enforces checkJobToken's LIVE-STATUS gate for a job id whose
// token has ALREADY been verified (avoiding a double verify): the job must be
// pending or running. A nil db client skips the check (tests parity).
func (s *GRPCServer) checkJobActive(ctx context.Context, jobID int64) error {
	if s.db == nil {
		return nil
	}
	job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: jobID})
	if err != nil {
		return status.Error(codes.PermissionDenied, "caller's job is unknown")
	}
	if jobStatusIsTerminal(job.GetStatus()) {
		return status.Error(codes.PermissionDenied, "job is no longer running; its token is invalid")
	}
	return nil
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

// workerStates snapshots the live workers' identity and state for the workers
// gauge (F-20): a worker that holds an open WatchJobs stream is "watching",
// otherwise it is "registered".
func (s *GRPCServer) workerStates() []metrics.WorkerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	states := make([]metrics.WorkerState, 0, len(s.live))
	for name, w := range s.live {
		state := "registered"
		if _, ok := s.watchers[name]; ok {
			state = "watching"
		}
		states = append(states, metrics.WorkerState{
			Group:        w.group,
			Environments: w.environments,
			State:        state,
		})
	}
	return states
}

// updateWorkerMetrics refreshes the workers gauge from the live registry. It
// is a no-op when metrics are disabled.
func (s *GRPCServer) updateWorkerMetrics() {
	if s.metrics == nil {
		return
	}
	s.metrics.UpdateWorkers(s.workerStates())
}

// recordJobStatusMetrics records a job status transition (F-20) and, when the
// job reached a terminal state with both a start and finish timestamp, its
// duration. It is a no-op when metrics are disabled.
func (s *GRPCServer) recordJobStatusMetrics(ctx context.Context, job *dbpb.Job) {
	if job == nil {
		return
	}
	// Seal the job's logs when it reaches a terminal state (F-21.6 storage
	// redesign): reconcile the records' sizes with the blobs' final sizes and
	// evict the per-(job,log) uid cache (a terminated job's logs are sealed;
	// caching them forever leaks memory across a fleet's worth of finished
	// jobs). Independent of metrics (finalization runs even when metrics are
	// disabled).
	if jobStatusIsTerminal(job.GetStatus()) {
		s.finalizeJobLogs(ctx, job.GetId())
	}
	if s.metrics == nil {
		return
	}
	status := jobStatusName(job.GetStatus())
	s.metrics.RecordJobStatus(status)
	if started := job.GetStartedAt(); started != nil {
		if finished := job.GetFinishedAt(); finished != nil {
			d := finished.AsTime().Sub(started.AsTime())
			if d >= 0 {
				s.metrics.ObserveJobDuration(status, d)
			}
		}
	}
}

// checkJobToken, when job-token auth is enabled, verifies that the caller
// presented a valid job token scoped to jobID and that the job is still
// active (pending or running). A job token's exp claim is a long placeholder
// (the token is meant to live as long as the job runs), so the API gates the
// token's validity on the job's live status in the database: once the job
// reaches a terminal state (succeeded, failed, cancelled, timed_out, or
// skipped) its token is invalid, so a token for a finished job cannot be
// reused. A job that is still pending or running is active, so its token is
// valid. A nil db client (e.g. in tests) cannot check the status, so the
// status check is skipped. It is a no-op when job-token auth is disabled.
//
// allowTerminal relaxes the status gate: a read-only poll that must keep
// working after the job reaches a terminal state (the approval gate's
// CheckApproval, which a target paused at a gate polls to observe a rejection
// that has already moved the job to failed) passes true so the token is
// accepted even for a finished job. Every other call site passes false.
func (s *GRPCServer) checkJobToken(ctx context.Context, jobID int64, allowTerminal bool) error {
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
	// The token's exp claim is a long placeholder, so the API gates the token's
	// validity on the job's live status: a token for a job that has reached a
	// terminal state is invalid (the job is no longer running). A nil db client
	// (e.g. in tests) cannot check the status, so the check is skipped.
	if s.db != nil {
		job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: jobID})
		if err != nil {
			return status.Error(codes.Unauthenticated, "job token could not be validated")
		}
		if !allowTerminal && jobStatusIsTerminal(job.GetStatus()) {
			return status.Error(codes.PermissionDenied, "job is no longer running; its token is invalid")
		}
	}
	return nil
}

// jobStatusIsTerminal reports whether a job's status is a terminal state (the
// job has finished and will not run again): a job in a terminal state has no
// active work, so its job token is invalid. A job that is pending or running
// is not terminal.
func jobStatusIsTerminal(status dbpb.JobStatus) bool {
	switch status {
	case dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
		dbpb.JobStatus_JOB_STATUS_FAILED,
		dbpb.JobStatus_JOB_STATUS_CANCELLED,
		dbpb.JobStatus_JOB_STATUS_TIMED_OUT,
		dbpb.JobStatus_JOB_STATUS_SKIPPED:
		return true
	default:
		return false
	}
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
		Id:             job.GetId(),
		PipelineId:     job.GetPipelineId(),
		Name:           job.GetName(),
		Status:         job.GetStatus(),
		TargetGroup:    job.GetTargetGroup(),
		StartedAt:      job.GetStartedAt(),
		FinishedAt:     job.GetFinishedAt(),
		Spec:           job.GetSpec(),
		Attempt:        job.GetAttempt(),
		MaxAttempts:    job.GetMaxAttempts(),
		DependsOn:      job.GetDependsOn(),
		StepResults:    job.GetStepResults(),
		Outputs:        job.GetOutputs(),
		IgnoreFailed:   job.GetIgnoreFailed(),
		FailureMode:    job.GetFailureMode(),
		StepBarrier:    job.GetStepBarrier(),
		Key:            job.GetKey(),
		TriggerName:    job.GetTriggerName(),
		TriggerType:    job.GetTriggerType(),
		UpstreamClaims: job.GetUpstreamClaims(),
		RunId:          job.GetRunId(),
		RunParams:      job.GetRunParams(),
		// The run's resolved package snapshot (F-21.6): reference name →
		// stored file name. Handed to the execution target so the
		// download_artifact step handler can resolve a spec.artifacts
		// reference to the version the run picked.
		Artifacts:           job.GetArtifacts(),
		ApprovalMessage:     job.GetApprovalMessage(),
		ApprovalDecision:    job.GetApprovalDecision(),
		ApprovalActor:       job.GetApprovalActor(),
		ApprovalReason:      job.GetApprovalReason(),
		ApprovalRequestedAt: job.GetApprovalRequestedAt(),
		ApprovalDecidedAt:   job.GetApprovalDecidedAt(),
		Environment:         job.GetEnvironment(),
		Approval:            job.GetApproval(),
	}
}

// applyEnvironmentApprovalGate inserts an F-13 approval step at the front of
// the job's spec when the job's environment requires approval (F-17). The
// environment's requires_approval flag is the default; the job's own approval
// override (none / required) takes precedence. A job that targets no
// environment (or an environment that is missing) is left unchanged. The
// inserted step pauses the job at the gate (awaiting_approval) until an
// authorized user approves or rejects it, exactly like a pipeline-authored
// approval step. It is a no-op when the job's spec is nil or the gate is not
// required.
func (s *GRPCServer) applyEnvironmentApprovalGate(ctx context.Context, apiJob *apipb.Job) {
	if apiJob.GetEnvironment() == "" {
		return
	}
	env, err := s.db.GetEnvironment(ctx, &dbpb.GetEnvironmentRequest{Name: apiJob.GetEnvironment()})
	if err != nil {
		// The environment is missing (it was deleted, or the Database client is
		// unavailable): the gate is not applied (the job runs without it).
		return
	}
	// The job's approval override takes precedence over the environment's
	// requires_approval flag: required inserts a gate even if the environment
	// does not require one; none skips the gate even if it does. When the job
	// sets no override (unspecified) the environment's flag decides.
	requires := env.GetRequiresApproval()
	switch apiJob.GetApproval() {
	case dbpb.ApprovalPolicy_APPROVAL_POLICY_NONE:
		requires = false
	case dbpb.ApprovalPolicy_APPROVAL_POLICY_REQUIRED:
		requires = true
	}
	if !requires {
		return
	}
	spec := apiJob.GetSpec()
	if spec == nil {
		spec = &dbpb.JobSpec{}
	}
	gate := &dbpb.JobStep{
		Type: executor.TypeApproval,
		Params: map[string]*dbpb.ParamValue{
			"message": {String_: fmt.Sprintf("Approve deployment of job %q to environment %q", apiJob.GetName(), apiJob.GetEnvironment())},
		},
	}
	steps := append([]*dbpb.JobStep{gate}, spec.GetSteps()...)
	apiJob.Spec = &dbpb.JobSpec{
		Steps:           steps,
		Timeout:         spec.GetTimeout(),
		Retry:           spec.GetRetry(),
		IgnoreFailed:    spec.GetIgnoreFailed(),
		FailureMode:     spec.GetFailureMode(),
		StepBarrier:     spec.GetStepBarrier(),
		ConcurrencyKeys: spec.GetConcurrencyKeys(),
	}
}

// resolveSecrets decrypts the job's stored secret ciphertexts (F-12) into a
// name -> plaintext map and sets it on the API job handed to an execution
// target. The target interpolates the values into the job's spec (`{{
// .secrets.name }}`) before it runs. A job whose pipeline has no secrets (or a
// nil secret store) is left unchanged. A ciphertext that fails to decrypt (the
// key changed since it was written) is logged and dropped rather than failing
// the dispatch: the job's spec that references it will then fail to
// interpolate, which is a clearer signal than a half-dispatched job.
func (s *GRPCServer) resolveSecrets(ctx context.Context, apiJob *apipb.Job, dbJob *dbpb.Job) {
	if s.secretStore == nil || len(dbJob.GetSecrets()) == 0 {
		return
	}
	plaintext := make(map[string]string, len(dbJob.GetSecrets()))
	var decrypted int
	for _, secret := range dbJob.GetSecrets() {
		value, err := s.secretStore.Decrypt(ctx, secret.GetEncrypted())
		if err != nil {
			s.logger.Warn("api: decrypt secret for job", "job", dbJob.GetId(), "secret", secret.GetName(), "err", err)
			continue
		}
		plaintext[secret.GetName()] = value
		decrypted++
	}
	if decrypted > 0 && s.metrics != nil {
		s.metrics.AddSecretsDecrypted(decrypted)
	}
	if len(plaintext) > 0 {
		apiJob.Secrets = plaintext
	}
}

// pipelineSecretSources fetches the pipeline's named secret sources (F-21.1.3)
// as a name -> source map, for resolving a sourced variable. It is best-effort:
// a pipeline that cannot be read (it was deleted, or the Database client is
// unavailable) yields an empty map, so a sourced variable simply resolves to
// nothing rather than failing the dispatch.
func (s *GRPCServer) pipelineSecretSources(ctx context.Context, pipelineID int64) map[string]*dbpb.SecretSource {
	if s.db == nil {
		return nil
	}
	pipeline, err := s.db.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: pipelineID})
	if err != nil {
		s.logger.Warn("api: fetch pipeline for variable sources", "pipeline", pipelineID, "err", err)
		return nil
	}
	sources := make(map[string]*dbpb.SecretSource, len(pipeline.GetSecretSources()))
	for _, src := range pipeline.GetSecretSources() {
		sources[src.GetName()] = src
	}
	return sources
}

// resolveVariableValue resolves a single variable (F-21.1.3) to its concrete
// value. A plain variable is its value as-is; an encrypted variable is
// decrypted (with the configured key); a sourced variable is resolved through
// the pipeline's named secret source (the built-in AES store decrypts the
// source's path; an external store such as Vault / OpenBao is not yet
// implemented and yields an empty value). The second return reports whether the
// variable is masked (its value is not a plain string), so the caller can
// redact the resolved plaintext from the job's logs. A variable that cannot be
// resolved (no secret store, a missing source, a failed decrypt) yields an
// empty value and is not masked.
func (s *GRPCServer) resolveVariableValue(ctx context.Context, dbJob *dbpb.Job, v *dbpb.Variable, sources map[string]*dbpb.SecretSource) (string, bool) {
	switch v.GetShape() {
	case dbpb.VariableShape_VARIABLE_PLAIN:
		return v.GetPlain(), false
	case dbpb.VariableShape_VARIABLE_ENCRYPTED:
		if s.secretStore == nil {
			return "", false
		}
		decrypted, err := s.secretStore.Decrypt(ctx, v.GetEncrypted())
		if err != nil {
			s.logger.Warn("api: decrypt variable for job", "job", dbJob.GetId(), "variable", v.GetName(), "err", err)
			return "", false
		}
		return decrypted, true
	case dbpb.VariableShape_VARIABLE_SECRET_SOURCE:
		return s.resolveSecretSourceValue(ctx, dbJob, v, sources)
	default:
		return "", false
	}
}

// resolveSecretSourceValue resolves a sourced variable (F-21.1.3) to a
// concrete value through the pipeline's named secret source. The source's kind
// selects the backend: the built-in AES store (the default) decrypts the
// source's path as a ciphertext; an external store (Vault / OpenBao) is not
// yet implemented and yields an empty value (logged). A missing or
// unresolvable source yields an empty value (logged), so the variable is
// simply absent from the run params rather than failing the dispatch.
func (s *GRPCServer) resolveSecretSourceValue(ctx context.Context, dbJob *dbpb.Job, v *dbpb.Variable, sources map[string]*dbpb.SecretSource) (string, bool) {
	source, ok := sources[v.GetSecretSource()]
	if !ok {
		s.logger.Warn("api: unknown secret source for variable", "job", dbJob.GetId(), "variable", v.GetName(), "source", v.GetSecretSource())
		return "", false
	}
	kind := source.GetKind()
	if kind == "" {
		kind = string(secrets.StoreKindAES)
	}
	switch secrets.StoreKind(kind) {
	case secrets.StoreKindAES, "":
		if s.secretStore == nil {
			return "", false
		}
		decrypted, err := s.secretStore.Decrypt(ctx, source.GetPath())
		if err != nil {
			s.logger.Warn("api: resolve secret source for variable", "job", dbJob.GetId(), "variable", v.GetName(), "source", source.GetName(), "err", err)
			return "", false
		}
		// Vault (F-21.4): the path decrypts to a JSON object of key -> value;
		// the variable's key selects the value within it.
		obj, objErr := aesVaultObject(decrypted)
		if objErr != nil {
			s.logger.Warn("api: vault source path is not a JSON object", "job", dbJob.GetId(), "source", source.GetName(), "err", objErr)
			return "", false
		}
		val, ok := obj[v.GetKey()]
		if !ok {
			s.logger.Warn("api: vault key not found", "job", dbJob.GetId(), "source", source.GetName(), "key", v.GetKey())
			return "", false
		}
		return val, true
	case secrets.StoreKindVault, secrets.StoreKindOpenBao:
		s.logger.Warn("api: secret source kind not implemented", "job", dbJob.GetId(), "variable", v.GetName(), "source", source.GetName(), "kind", kind)
		return "", false
	default:
		s.logger.Warn("api: unknown secret source kind", "job", dbJob.GetId(), "variable", v.GetName(), "source", source.GetName(), "kind", kind)
		return "", false
	}
}

// needSources reports whether the job's effective variables (its stored
// variables plus its vault imports, F-21.4) include anything that needs the
// pipeline's named secret sources to resolve: a sourced variable, or a vault
// import (an every-key vault import is expanded into sourced variables at
// dispatch). A job with none of these does not need the sources fetched.
func needSources(dbJob *dbpb.Job) bool {
	for _, v := range dbJob.GetVariables() {
		if v.GetShape() == dbpb.VariableShape_VARIABLE_SECRET_SOURCE {
			return true
		}
	}
	return len(dbJob.GetVaultImports()) > 0
}

// collectEffectiveVariables gathers the job's effective variables (F-21.1.3 /
// F-21.4): its stored variables plus the variables its every-key vault imports
// expand into (one VARIABLE_SECRET_SOURCE variable per vault key). It returns
// the gathered variables and the pipeline's named secret sources (needed to
// resolve the sourced ones; nil when the job needs none).
func (s *GRPCServer) collectEffectiveVariables(ctx context.Context, dbJob *dbpb.Job) ([]*dbpb.Variable, map[string]*dbpb.SecretSource) {
	variables := dbJob.GetVariables()
	var sources map[string]*dbpb.SecretSource
	if needSources(dbJob) {
		sources = s.pipelineSecretSources(ctx, dbJob.GetPipelineId())
	}
	for _, imp := range dbJob.GetVaultImports() {
		variables = append(variables, s.expandVaultImport(ctx, dbJob, imp, sources)...)
	}
	return variables, sources
}

// expandVaultImport expands an every-key vault import (F-21.4) into one
// VARIABLE_SECRET_SOURCE variable per key in the named secret source (the
// vault's values, as masked variables). The source's kind selects the backend:
// the built-in AES store (the default) decrypts the source's path as a JSON
// object of key -> value and emits one variable per key; an external store
// (Vault / OpenBao) is not yet implemented and yields no variables (logged).
// A missing or unreadable source yields no variables (logged), so the import
// is simply absent from the run params rather than failing the dispatch.
func (s *GRPCServer) expandVaultImport(ctx context.Context, dbJob *dbpb.Job, imp *dbpb.VaultImport, sources map[string]*dbpb.SecretSource) []*dbpb.Variable {
	source, ok := sources[imp.GetSource()]
	if !ok {
		s.logger.Warn("api: unknown vault for import", "job", dbJob.GetId(), "vault", imp.GetSource())
		return nil
	}
	kind := source.GetKind()
	if kind == "" {
		kind = string(secrets.StoreKindAES)
	}
	switch secrets.StoreKind(kind) {
	case secrets.StoreKindAES, "":
		if s.secretStore == nil {
			return nil
		}
		// The AES-kind vault stores its values as a JSON object (key -> value)
		// in the source's path (ciphertext). Enumerate the keys and emit one
		// variable per key; each resolves through resolveSecretSourceValue.
		decrypted, err := s.secretStore.Decrypt(ctx, source.GetPath())
		if err != nil {
			s.logger.Warn("api: decrypt vault for import", "job", dbJob.GetId(), "vault", imp.GetSource(), "err", err)
			return nil
		}
		obj, objErr := aesVaultObject(decrypted)
		if objErr != nil {
			s.logger.Warn("api: vault source path is not a JSON object", "job", dbJob.GetId(), "vault", imp.GetSource(), "err", objErr)
			return nil
		}
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]*dbpb.Variable, 0, len(keys))
		for _, key := range keys {
			out = append(out, &dbpb.Variable{
				Name:         key,
				Shape:        dbpb.VariableShape_VARIABLE_SECRET_SOURCE,
				SecretSource: imp.GetSource(),
				Key:          key,
			})
		}
		return out
	case secrets.StoreKindVault, secrets.StoreKindOpenBao:
		s.logger.Warn("api: vault import kind not implemented", "job", dbJob.GetId(), "vault", imp.GetSource(), "kind", kind)
		return nil
	default:
		s.logger.Warn("api: unknown vault import kind", "job", dbJob.GetId(), "vault", imp.GetSource(), "kind", kind)
		return nil
	}
}

// aesVaultObject parses a decrypted AES-vault source path as a JSON object of
// key -> value (F-21.4). It is how an every-key vault import enumerates the
// vault's keys and how a per-key sourced variable selects its value. A legacy
// path that decrypts to a plain (non-object) value is an error here (it is
// handled separately by resolveSecretSourceValue's no-key branch).
func aesVaultObject(raw string) (map[string]string, error) {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil, fmt.Errorf("expected a JSON object of key -> value: %w", err)
	}
	out := make(map[string]string, len(obj))
	for k, v := range obj {
		switch sv := v.(type) {
		case string:
			out[k] = sv
		case float64:
			out[k] = strconv.FormatFloat(sv, 'f', -1, 64)
		case bool:
			out[k] = strconv.FormatBool(sv)
		default:
			b, _ := json.Marshal(v)
			out[k] = string(b)
		}
	}
	return out, nil
}

// resolveVariables resolves the job's effective variables (F-21.1.3 / F-21.4)
// into the run's parameters so a step's spec can reference them as
// `{{ .params.name }}`. The effective variables are the job's stored variables
// plus the variables its every-key vault imports expand into (one per vault
// key). Each variable is resolved to its concrete value (plain as-is, encrypted
// decrypted, sourced through the pipeline's named secret source) and merged
// into the job's run params (a variable's value wins over a run parameter of
// the same name, since a variable is a more specific value for this job). The
// function returns the plaintexts of the masked variables (encrypted, sourced,
// or vault-derived) so the caller can redact them from the job's logs. A job
// with no variables is left unchanged.
func (s *GRPCServer) resolveVariables(ctx context.Context, apiJob *apipb.Job, dbJob *dbpb.Job) []string {
	variables, sources := s.collectEffectiveVariables(ctx, dbJob)
	if len(variables) == 0 {
		return nil
	}
	params := apiJob.GetRunParams()
	merged := make(map[string]string, len(params)+len(variables))
	for k, v := range params {
		merged[k] = v
	}
	var masked []string
	for _, v := range variables {
		name := v.GetName()
		if name == "" {
			continue
		}
		value, isMasked := s.resolveVariableValue(ctx, dbJob, v, sources)
		if value == "" && !isMasked {
			// A variable that could not be resolved (no store, a missing
			// source, a failed decrypt) is left out of the run params.
			continue
		}
		merged[name] = value
		if isMasked && value != "" {
			masked = append(masked, value)
		}
	}
	if len(merged) > 0 {
		apiJob.RunParams = merged
	}
	return masked
}

// maskedVariableValues resolves the job's masked variables (F-21.1.3 / F-21.4)
// to their plaintexts, for log redaction. It considers the job's effective
// variables (its stored variables plus the variables its vault imports expand
// into) and returns only the masked plaintexts (the values to redact from the
// job's logs). A job with no variables, or no masked variables that resolve,
// yields nil.
func (s *GRPCServer) maskedVariableValues(ctx context.Context, dbJob *dbpb.Job) []string {
	variables, sources := s.collectEffectiveVariables(ctx, dbJob)
	if len(variables) == 0 {
		return nil
	}
	var masked []string
	for _, v := range variables {
		value, isMasked := s.resolveVariableValue(ctx, dbJob, v, sources)
		if isMasked && value != "" {
			masked = append(masked, value)
		}
	}
	return masked
}

// secretRedactor replaces a job's secret plaintext values in log output so the
// plaintext is never persisted to the artifacts store or fanned out to the UI
// (F-12). The API is the only component that holds the key, so it is the only
// place that can both decrypt the ciphertexts (to know the plaintext to look
// for) and redact them. A nil redactor (a job with no secrets, or no secrets
// key configured) leaves the output unchanged.
type secretRedactor struct {
	// values are the secret plaintexts to redact, sorted longest-first so a
	// value that is a substring of a longer one is not partially redacted
	// before the longer one is.
	values []string
}

// redact replaces every occurrence of a secret plaintext in data with a
// redaction marker. A nil receiver is a no-op.
func (r *secretRedactor) redact(data []byte) []byte {
	if r == nil || len(r.values) == 0 {
		return data
	}
	out := string(data)
	for _, v := range r.values {
		if v == "" {
			continue
		}
		out = strings.ReplaceAll(out, v, "[REDACTED]")
	}
	return []byte(out)
}

// secretRedactorFor fetches the job and returns a redactor for its secret
// plaintexts (F-12). A job with no secrets (or a nil secret store) yields a
// nil redactor (no redaction needed). A ciphertext that fails to decrypt is
// logged and skipped: without the plaintext there is nothing to redact.
func (s *GRPCServer) secretRedactorFor(ctx context.Context, jobID int64) *secretRedactor {
	if s.secretStore == nil {
		return nil
	}
	job, err := s.db.GetJob(ctx, &dbpb.GetJobRequest{Id: jobID})
	if err != nil {
		s.logger.Warn("api: fetch job for log redaction", "job", jobID, "err", err)
		return nil
	}
	values := make([]string, 0, len(job.GetSecrets()))
	for _, secret := range job.GetSecrets() {
		value, err := s.secretStore.Decrypt(ctx, secret.GetEncrypted())
		if err != nil {
			s.logger.Warn("api: decrypt secret for log redaction", "job", jobID, "secret", secret.GetName(), "err", err)
			continue
		}
		if value != "" {
			values = append(values, value)
		}
	}
	// The job's masked variables (F-21.1.3) are redacted too: their plaintexts
	// are resolved the same way as secrets (decrypted, or resolved through the
	// pipeline's named secret source) and must not leak into the job's logs.
	values = append(values, s.maskedVariableValues(ctx, job)...)
	if len(values) == 0 {
		return nil
	}
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	return &secretRedactor{values: values}
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

// publish sends an event to the hub, if one is configured. A job status
// change frees or takes a concurrency slot (F-16), so it also refreshes the
// pending queue for the UI (best-effort and async, so a slow database never
// blocks the status event).
func (s *GRPCServer) publish(ev Event) {
	if s.hub == nil {
		return
	}
	s.hub.Publish(ev)
	if ev.Type == EventJobStatus && s.db != nil {
		go s.publishQueueRefresh()
	}
}

// publishQueueRefresh reads the current pending queue (F-16) and publishes it
// as a queue event. It is best-effort: a failure to read the queue is
// silently dropped (the UI resynchronizes from the snapshot on reconnect).
func (s *GRPCServer) publishQueueRefresh() {
	if s.hub == nil || s.db == nil {
		return
	}
	queue, err := s.db.GetQueueStatus(context.Background(), &dbpb.GetQueueStatusRequest{})
	if err != nil {
		return
	}
	s.hub.Publish(Event{Type: EventQueue, Queue: queue})
}
