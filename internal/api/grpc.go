package api

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
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

// GRPCServer implements the cdrom.api.v1.API gRPC service. It is the
// control-plane hub that long-lived workers and ephemeral agents talk to: it
// holds the workers' WatchJobs streams, relays job assignments pushed by the
// scheduler (DispatchJob), and proxies artifact traffic to the artifacts
// service. Execution targets never talk to the scheduler, database, or
// artifacts services directly.
type GRPCServer struct {
	apipb.UnimplementedAPIServer
	db        dbpb.DatabaseClient
	artifacts artifactspb.ArtifactsClient
	logger    *slog.Logger
	hub       *EventHub
	jobAuth   *JobTokenAuth

	mu       sync.Mutex
	live     map[string]*liveWorker // worker name -> live worker
	watchers map[string]chan *apipb.JobAssignment
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
		db:        db,
		artifacts: artifacts,
		hub:       hub,
		jobAuth:   jobAuth,
		logger:    logger,
		live:      make(map[string]*liveWorker),
		watchers:  make(map[string]chan *apipb.JobAssignment),
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
// JobAssignment for every job dispatched to its group until the stream
// closes.
func (s *GRPCServer) WatchJobs(req *apipb.WatchJobsRequest, stream grpc.ServerStreamingServer[apipb.JobAssignment]) error {
	if req.GetWorkerName() == "" {
		return status.Error(codes.InvalidArgument, "worker_name is required")
	}
	name := req.GetWorkerName()
	assignmentChannel := make(chan *apipb.JobAssignment, assignmentQueueSize)

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
	for {
		select {
		case <-stream.Context().Done():
			s.logger.Info("api: worker stream closed", "worker", name)
			return nil
		case assignment := <-assignmentChannel:
			if err := stream.Send(assignment); err != nil {
				s.logger.Warn("api: failed to send assignment", "worker", name, "err", err)
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
	// Hand the agent a job token so it can authenticate its status reports and
	// any outside-resource calls for this job.
	if token := s.mintJobToken(ctx, apiJob); token != "" {
		apiJob.Token = token
	}
	return apiJob, nil
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
	updated, err := s.db.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:         req.GetJobId(),
		Status:     req.GetStatus(),
		StartedAt:  req.GetStartedAt(),
		FinishedAt: req.GetFinishedAt(),
	})
	if err != nil {
		return nil, err
	}
	s.logger.Info("api: job status reported", "job", req.GetJobId(), "status", req.GetStatus())
	s.publish(Event{Type: EventJobStatus, JobID: req.GetJobId(), Status: jobStatusName(req.GetStatus())})
	return toAPIJob(updated), nil
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
	return &apipb.DispatchJobResponse{Dispatched: int32(dispatched)}, nil
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
		assignment := &apipb.JobAssignment{Job: job, WorkerName: name, Token: token}
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
	// The first chunk carries the job_id; verify the caller's job token is
	// scoped to it before proxying the upload.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if err := s.checkArtifactToken(stream.Context(), first.GetMetadata().GetJobId()); err != nil {
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
	if err := s.checkArtifactToken(stream.Context(), req.GetJobId()); err != nil {
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
	if err := s.checkArtifactToken(ctx, req.GetJobId()); err != nil {
		return nil, err
	}
	return s.artifacts.GetArtifact(ctx, req)
}

// ListArtifacts proxies an artifact listing to the artifacts service. A
// non-empty job_id is scoped to that job's token; an empty job_id lists all
// artifacts and is not job-scoped.
func (s *GRPCServer) ListArtifacts(ctx context.Context, req *artifactspb.ListArtifactsRequest) (*artifactspb.ListArtifactsResponse, error) {
	if req.GetJobId() != "" {
		if err := s.checkArtifactToken(ctx, req.GetJobId()); err != nil {
			return nil, err
		}
	}
	return s.artifacts.ListArtifacts(ctx, req)
}

// DeleteArtifact proxies an artifact deletion to the artifacts service.
func (s *GRPCServer) DeleteArtifact(ctx context.Context, req *artifactspb.DeleteArtifactRequest) (*emptypb.Empty, error) {
	if err := s.checkArtifactToken(ctx, req.GetJobId()); err != nil {
		return nil, err
	}
	return s.artifacts.DeleteArtifact(ctx, req)
}

// checkArtifactToken, when job-token auth is enabled, verifies the caller's
// job token is scoped to the job that owns the artifact. It is a no-op when
// job-token auth is disabled or jobID is empty (not job-scoped).
func (s *GRPCServer) checkArtifactToken(ctx context.Context, jobID string) error {
	if !s.jobAuth.Enabled() || jobID == "" {
		return nil
	}
	id, err := strconv.ParseInt(jobID, 10, 64)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid artifact job_id")
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
		Id:          job.GetId(),
		PipelineId:  job.GetPipelineId(),
		Name:        job.GetName(),
		Status:      job.GetStatus(),
		TargetGroup: job.GetTargetGroup(),
		StartedAt:   job.GetStartedAt(),
		FinishedAt:  job.GetFinishedAt(),
		Spec:        job.GetSpec(),
	}
}

// publish sends an event to the hub, if one is configured.
func (s *GRPCServer) publish(ev Event) {
	if s.hub != nil {
		s.hub.Publish(ev)
	}
}
