package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// Server implements the cdrom.db.v1.Database gRPC service on top of a
// GORM database handle. It is the only component allowed to touch the
// storage backend; every other component reads and writes data through it.
type Server struct {
	dbpb.UnimplementedDatabaseServer
	db *gorm.DB
}

// NewServer creates a Database gRPC server over db.
func NewServer(db *gorm.DB) *Server {
	return &Server{db: db}
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

func (s *Server) CreatePipeline(ctx context.Context, req *dbpb.CreatePipelineRequest) (*dbpb.Pipeline, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "pipeline name is required")
	}
	pipeline := &models.Pipeline{Name: req.GetName(), Description: req.GetDescription()}
	if err := s.db.WithContext(ctx).Create(pipeline).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoPipeline(pipeline), nil
}

func (s *Server) GetPipeline(ctx context.Context, req *dbpb.GetPipelineRequest) (*dbpb.Pipeline, error) {
	var pipeline models.Pipeline
	if err := s.db.WithContext(ctx).First(&pipeline, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoPipeline(&pipeline), nil
}

func (s *Server) ListPipelines(ctx context.Context, _ *dbpb.ListPipelinesRequest) (*dbpb.ListPipelinesResponse, error) {
	var pipelines []models.Pipeline
	if err := s.db.WithContext(ctx).Order("id").Find(&pipelines).Error; err != nil {
		return nil, grpcErr(err)
	}
	response := &dbpb.ListPipelinesResponse{}
	for i := range pipelines {
		response.Pipelines = append(response.Pipelines, toProtoPipeline(&pipelines[i]))
	}
	return response, nil
}

func (s *Server) UpdatePipeline(ctx context.Context, req *dbpb.UpdatePipelineRequest) (*dbpb.Pipeline, error) {
	var pipeline models.Pipeline
	if err := s.db.WithContext(ctx).First(&pipeline, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	if req.GetName() != "" {
		pipeline.Name = req.GetName()
	}
	pipeline.Description = req.GetDescription()
	if err := s.db.WithContext(ctx).Save(&pipeline).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoPipeline(&pipeline), nil
}

func (s *Server) DeletePipeline(ctx context.Context, req *dbpb.DeletePipelineRequest) (*emptypb.Empty, error) {
	// Remove child jobs first, then the pipeline itself.
	if err := s.db.WithContext(ctx).Where("pipeline_id = ?", req.GetId()).Delete(&models.Job{}).Error; err != nil {
		return nil, grpcErr(err)
	}
	result := s.db.WithContext(ctx).Delete(&models.Pipeline{}, req.GetId())
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, status.Error(codes.NotFound, "pipeline not found")
	}
	return &emptypb.Empty{}, nil
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

func (s *Server) CreateJob(ctx context.Context, req *dbpb.CreateJobRequest) (*dbpb.Job, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "job name is required")
	}
	spec := specFromProto(req.GetSpec())
	job := &models.Job{
		Name:        req.GetName(),
		TargetGroup: req.GetTargetGroup(),
		Status:      models.JobStatusPending,
		Spec:        spec,
		// A new job starts on its first attempt (F-04). The retry budget is
		// denormalized from the spec's retry policy so the UI can render
		// "attempt N of M" without the spec.
		Attempt:   1,
		DependsOn: dependsOnFromProto(req.GetDependsOn()),
	}
	if spec.Retry != nil {
		job.MaxAttempts = spec.Retry.MaxAttempts
	}
	if req.GetPipelineId() > 0 {
		pipelineID := uint(req.GetPipelineId())
		job.PipelineID = &pipelineID
		var pipeline models.Pipeline
		if err := s.db.WithContext(ctx).First(&pipeline, pipelineID).Error; err != nil {
			return nil, grpcErr(err)
		}
	}
	if err := s.db.WithContext(ctx).Create(job).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoJob(job), nil
}

func (s *Server) GetJob(ctx context.Context, req *dbpb.GetJobRequest) (*dbpb.Job, error) {
	var job models.Job
	if err := s.db.WithContext(ctx).First(&job, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoJob(&job), nil
}

func (s *Server) ListJobs(ctx context.Context, req *dbpb.ListJobsRequest) (*dbpb.ListJobsResponse, error) {
	query := s.db.WithContext(ctx).Model(&models.Job{})
	if req.GetPipelineId() > 0 {
		query = query.Where("pipeline_id = ?", req.GetPipelineId())
	}
	if jobStatus := req.GetStatus(); jobStatus != dbpb.JobStatus_JOB_STATUS_UNSPECIFIED {
		modelStatus, err := jobStatusFromProto(jobStatus)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		query = query.Where("status = ?", modelStatus)
	}
	var jobs []models.Job
	if err := query.Order("id").Find(&jobs).Error; err != nil {
		return nil, grpcErr(err)
	}
	response := &dbpb.ListJobsResponse{}
	for i := range jobs {
		response.Jobs = append(response.Jobs, toProtoJob(&jobs[i]))
	}
	return response, nil
}

// ListRetriableJobs returns the jobs the scheduler's retry loop should
// re-dispatch (F-04): jobs that are failed and still have retries remaining
// (a retry policy with max_attempts > 0 and an attempt counter below the
// budget, i.e. attempt < max_attempts + 1). The filter is applied in the
// database so the retry loop never pulls every failed job over the wire as
// the system grows. A job that has exhausted its retries (attempt >=
// max_attempts + 1) or has no retry policy (max_attempts = 0) is not returned.
func (s *Server) ListRetriableJobs(ctx context.Context, req *dbpb.ListRetriableJobsRequest) (*dbpb.ListJobsResponse, error) {
	var jobs []models.Job
	err := s.db.WithContext(ctx).
		Model(&models.Job{}).
		Where("status = ? AND max_attempts > 0 AND attempt < max_attempts + 1", models.JobStatusFailed).
		Order("id").
		Find(&jobs).Error
	if err != nil {
		return nil, grpcErr(err)
	}
	response := &dbpb.ListJobsResponse{}
	for i := range jobs {
		response.Jobs = append(response.Jobs, toProtoJob(&jobs[i]))
	}
	return response, nil
}

// UpdateJob applies a partial update: a status of JOB_STATUS_UNSPECIFIED
// leaves the status unchanged, and nil timestamps leave the corresponding
// field unchanged.
func (s *Server) UpdateJob(ctx context.Context, req *dbpb.UpdateJobRequest) (*dbpb.Job, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job id is required")
	}
	// The job must exist.
	var job models.Job
	if err := s.db.WithContext(ctx).First(&job, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	// Build the set of fields to update.
	updates := map[string]any{}
	if jobStatus := req.GetStatus(); jobStatus != dbpb.JobStatus_JOB_STATUS_UNSPECIFIED {
		modelStatus, err := jobStatusFromProto(jobStatus)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		updates["status"] = modelStatus
	}
	if timestamp := req.GetStartedAt(); timestamp != nil {
		instant := timestamp.AsTime()
		updates["started_at"] = &instant
	}
	if timestamp := req.GetFinishedAt(); timestamp != nil {
		instant := timestamp.AsTime()
		updates["finished_at"] = &instant
	}
	// step_results (F-06) are informational per-step outcomes, not part of
	// the terminal-status guard below: they are persisted whenever the
	// execution target reports them, even on the job's final report.
	//
	// This uses Updates with an explicit Select (not the map-based Update)
	// because GORM only runs a field's serializer (here, JSON) when the
	// update value flows through the model's reflected field — a raw
	// map-based Update hands the driver the unserialized Go value directly,
	// which SQLite's driver cannot bind.
	if results := req.GetStepResults(); len(results) > 0 {
		if err := s.db.WithContext(ctx).
			Model(&models.Job{}).
			Where("id = ?", req.GetId()).
			Select("StepResults").
			Updates(&models.Job{StepResults: stepResultsFromProto(results)}).Error; err != nil {
			return nil, grpcErr(err)
		}
	}
	// clear_depends_on (F-06) is set by the scheduler's dependency resolver
	// once a job's dependencies are satisfied and it has been dispatched, so
	// the resolver does not reconsider it on its next tick. It is independent
	// of the terminal-status guard below (clearing it does not change
	// status). Like step_results above, this uses Updates with an explicit
	// Select rather than the map-based Update: the value is nil either way,
	// but a map-based Update would not run the field's serializer for a
	// non-nil DependsOn if this were ever reused for that.
	if req.GetClearDependsOn() {
		if err := s.db.WithContext(ctx).
			Model(&models.Job{}).
			Where("id = ?", req.GetId()).
			Select("DependsOn").
			Updates(&models.Job{DependsOn: nil}).Error; err != nil {
			return nil, grpcErr(err)
		}
	}
	if len(updates) > 0 {
		updates["updated_at"] = time.Now()
		// A status report from a target must not clobber a job that has
		// already reached a terminal state (F-05): once a job is succeeded,
		// failed, cancelled, or timed_out, a late report (e.g. a target that
		// finished just as it was cancelled) is ignored. The conditional
		// update is atomic, so a concurrent cancellation (Database.CancelJob)
		// or reap (Database.ReapJob) wins over a late target report.
		result := s.db.WithContext(ctx).
			Model(&models.Job{}).
			Where("id = ? AND status IN ?", req.GetId(), []models.JobStatus{models.JobStatusPending, models.JobStatusRunning}).
			Updates(updates)
		if result.Error != nil {
			return nil, grpcErr(result.Error)
		}
		if result.RowsAffected == 0 {
			// The job already reached a terminal state; the update was a
			// no-op. Return its current state.
			if err := s.db.WithContext(ctx).First(&job, req.GetId()).Error; err != nil {
				return nil, grpcErr(err)
			}
			return toProtoJob(&job), nil
		}
	}
	if err := s.db.WithContext(ctx).First(&job, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoJob(&job), nil
}

// ReapJob conditionally marks a job as timed_out: it sets the status to
// timed_out (and the finished timestamp) only if the job is still in a
// non-terminal state (pending or running). It returns whether the job was
// reaped. This is how the scheduler's watchdog reaps a job whose target went
// silent (F-03): a job that already reported a terminal status (succeeded,
// failed, cancelled, or timed_out) is left untouched. The conditional update
// is done atomically with a WHERE clause on the status so a concurrent status
// report from the target cannot be clobbered.
func (s *Server) ReapJob(ctx context.Context, req *dbpb.ReapJobRequest) (*dbpb.ReapJobResponse, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job id is required")
	}
	finishedAt := time.Now()
	if timestamp := req.GetFinishedAt(); timestamp != nil {
		finishedAt = timestamp.AsTime()
	}
	result := s.db.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status IN ?", req.GetId(), []models.JobStatus{models.JobStatusPending, models.JobStatusRunning}).
		Updates(map[string]any{
			"status":      models.JobStatusTimedOut,
			"finished_at": &finishedAt,
			"updated_at":  time.Now(),
		})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	return &dbpb.ReapJobResponse{Reaped: result.RowsAffected > 0}, nil
}

// CancelJob conditionally marks a job as cancelled: it sets the status to
// cancelled (and the finished timestamp) only if the job is still in a
// non-terminal state (pending or running). It returns whether the job was
// cancelled. This is how the scheduler's CancelJob RPC persists a cancellation
// (F-05): a job that already reported a terminal status (succeeded, failed,
// cancelled, or timed_out) is left untouched, so cancelling a finished job is
// a no-op. The conditional update is done atomically with a WHERE clause on
// the status so a concurrent status report from the target cannot be
// clobbered.
func (s *Server) CancelJob(ctx context.Context, req *dbpb.CancelJobRequest) (*dbpb.CancelJobResponse, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job id is required")
	}
	finishedAt := time.Now()
	if timestamp := req.GetFinishedAt(); timestamp != nil {
		finishedAt = timestamp.AsTime()
	}
	result := s.db.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status IN ?", req.GetId(), []models.JobStatus{models.JobStatusPending, models.JobStatusRunning}).
		Updates(map[string]any{
			"status":      models.JobStatusCancelled,
			"finished_at": &finishedAt,
			"updated_at":  time.Now(),
		})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	return &dbpb.CancelJobResponse{Cancelled: result.RowsAffected > 0}, nil
}

// SkipJob conditionally marks a job as skipped: it sets the status to
// skipped (and the finished timestamp) only if the job is still pending (has
// not started). It returns whether the job was skipped. This is how the
// scheduler's dependency resolver marks a job skipped when one of its
// dependencies did not succeed (F-06): a job that already started (or
// finished) is left untouched. The conditional update is done atomically
// with a WHERE clause on the status so a concurrent dispatch/status report
// cannot be clobbered.
func (s *Server) SkipJob(ctx context.Context, req *dbpb.SkipJobRequest) (*dbpb.SkipJobResponse, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job id is required")
	}
	finishedAt := time.Now()
	if timestamp := req.GetFinishedAt(); timestamp != nil {
		finishedAt = timestamp.AsTime()
	}
	result := s.db.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", req.GetId(), models.JobStatusPending).
		Updates(map[string]any{
			"status":      models.JobStatusSkipped,
			"finished_at": &finishedAt,
			"updated_at":  time.Now(),
		})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	return &dbpb.SkipJobResponse{Skipped: result.RowsAffected > 0}, nil
}

// ClaimJobRetry atomically claims the next retry attempt for a job (F-04):
// it increments the job's attempt counter and resets it to pending (clearing
// the finished timestamp) only if the job is in a retryable state (pending,
// running, failed, or timed_out) and has not exhausted its retry budget. The
// conditional update is done atomically with a WHERE clause on the status and
// the attempt counter, so a job that reported a terminal status (succeeded or
// cancelled) between the check and the claim is left untouched, and a job
// that has used up its retries is never re-dispatched. It returns the claimed
// attempt number (1-based) and whether the claim succeeded.
func (s *Server) ClaimJobRetry(ctx context.Context, req *dbpb.ClaimJobRetryRequest) (*dbpb.ClaimJobRetryResponse, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job id is required")
	}
	var job models.Job
	if err := s.db.WithContext(ctx).First(&job, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	// A job that succeeded or was cancelled is not retried.
	switch job.Status {
	case models.JobStatusSucceeded, models.JobStatusCancelled:
		return &dbpb.ClaimJobRetryResponse{Claimed: false, Attempt: int32(job.Attempt)}, nil
	}
	// A job with no retry policy (max_attempts 0) is never retried.
	if job.MaxAttempts == 0 {
		return &dbpb.ClaimJobRetryResponse{Claimed: false, Attempt: int32(job.Attempt)}, nil
	}
	// A job that has exhausted its retry budget is not retried. The budget is
	// the number of retries after the initial attempt, so the attempt counter
	// may reach at most 1 + max_attempts; once it has, no further attempt is
	// claimed.
	if job.Attempt >= job.MaxAttempts+1 {
		return &dbpb.ClaimJobRetryResponse{Claimed: false, Attempt: int32(job.Attempt)}, nil
	}
	nextAttempt := job.Attempt + 1
	result := s.db.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status IN ? AND attempt = ?", req.GetId(),
			[]models.JobStatus{models.JobStatusPending, models.JobStatusRunning, models.JobStatusFailed, models.JobStatusTimedOut}, job.Attempt).
		Updates(map[string]any{
			"status":      models.JobStatusPending,
			"attempt":     nextAttempt,
			"finished_at": nil, // clear the previous attempt's finished timestamp
			"updated_at":  time.Now(),
		})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	if result.RowsAffected == 0 {
		// The job changed state (or attempt) between the read and the update;
		// it was not claimed.
		return &dbpb.ClaimJobRetryResponse{Claimed: false, Attempt: int32(job.Attempt)}, nil
	}
	return &dbpb.ClaimJobRetryResponse{Claimed: true, Attempt: int32(nextAttempt)}, nil
}

// RerunJob resets a finished job to pending with a fresh attempt (F-04): it
// sets the status to pending, the attempt to 1, and clears the finished
// timestamp. Only jobs in a terminal state (succeeded, failed, cancelled,
// timed_out, or skipped) can be re-run; a job that is still pending or
// running is rejected. The conditional update is atomic, so a job that is
// re-run concurrently is not clobbered.
func (s *Server) RerunJob(ctx context.Context, req *dbpb.RerunJobRequest) (*dbpb.Job, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job id is required")
	}
	var job models.Job
	if err := s.db.WithContext(ctx).First(&job, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	// Only a job in a terminal state can be re-run.
	switch job.Status {
	case models.JobStatusPending, models.JobStatusRunning:
		return nil, status.Errorf(codes.FailedPrecondition, "job %d is %s and cannot be re-run", req.GetId(), job.Status)
	}
	result := s.db.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status IN ?", req.GetId(),
			[]models.JobStatus{models.JobStatusSucceeded, models.JobStatusFailed, models.JobStatusCancelled, models.JobStatusTimedOut, models.JobStatusSkipped}).
		Updates(map[string]any{
			"status":      models.JobStatusPending,
			"attempt":     1,
			"finished_at": nil, // clear the previous attempt's finished timestamp
			"updated_at":  time.Now(),
		})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "job %d is no longer in a terminal state", req.GetId())
	}
	updated, err := s.GetJob(ctx, &dbpb.GetJobRequest{Id: req.GetId()})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *Server) DeleteJob(ctx context.Context, req *dbpb.DeleteJobRequest) (*emptypb.Empty, error) {
	result := s.db.WithContext(ctx).Delete(&models.Job{}, req.GetId())
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	return &emptypb.Empty{}, nil
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

// RegisterWorker upserts a worker by name: an unknown name creates the
// worker, a known name updates its group and address.
func (s *Server) RegisterWorker(ctx context.Context, req *dbpb.RegisterWorkerRequest) (*dbpb.Worker, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker name is required")
	}
	worker := &models.Worker{}
	if err := s.db.WithContext(ctx).Where("name = ?", req.GetName()).FirstOrCreate(worker, models.Worker{Name: req.GetName()}).Error; err != nil {
		return nil, grpcErr(err)
	}
	worker.Group = req.GetGroup()
	worker.Address = req.GetAddress()
	now := time.Now()
	worker.LastSeenAt = &now
	if err := s.db.WithContext(ctx).Save(worker).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoWorker(worker), nil
}

func (s *Server) GetWorker(ctx context.Context, req *dbpb.GetWorkerRequest) (*dbpb.Worker, error) {
	var worker models.Worker
	if err := s.db.WithContext(ctx).Where("name = ?", req.GetName()).First(&worker).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoWorker(&worker), nil
}

func (s *Server) ListWorkers(ctx context.Context, req *dbpb.ListWorkersRequest) (*dbpb.ListWorkersResponse, error) {
	query := s.db.WithContext(ctx).Model(&models.Worker{})
	if req.GetGroup() != "" {
		// worker_group: the column name (group is a reserved SQL keyword).
		query = query.Where("worker_group = ?", req.GetGroup())
	}
	var workers []models.Worker
	if err := query.Order("id").Find(&workers).Error; err != nil {
		return nil, grpcErr(err)
	}
	response := &dbpb.ListWorkersResponse{}
	for i := range workers {
		response.Workers = append(response.Workers, toProtoWorker(&workers[i]))
	}
	return response, nil
}

func (s *Server) HeartbeatWorker(ctx context.Context, req *dbpb.HeartbeatWorkerRequest) (*dbpb.Worker, error) {
	now := time.Now()
	result := s.db.WithContext(ctx).Model(&models.Worker{}).Where("name = ?", req.GetName()).Update("last_seen_at", now)
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, status.Error(codes.NotFound, "worker not found")
	}
	var worker models.Worker
	if err := s.db.WithContext(ctx).Where("name = ?", req.GetName()).First(&worker).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoWorker(&worker), nil
}

func (s *Server) DeleteWorker(ctx context.Context, req *dbpb.DeleteWorkerRequest) (*emptypb.Empty, error) {
	result := s.db.WithContext(ctx).Where("name = ?", req.GetName()).Delete(&models.Worker{})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, status.Error(codes.NotFound, "worker not found")
	}
	return &emptypb.Empty{}, nil
}

// ---------------------------------------------------------------------------
// IdP signing keys
// ---------------------------------------------------------------------------

// ListIDPSigningKeys returns the IdP's signing keyring: the current key plus
// any not-yet-expired predecessor keys.
func (s *Server) ListIDPSigningKeys(ctx context.Context, _ *dbpb.ListIDPSigningKeysRequest) (*dbpb.ListIDPSigningKeysResponse, error) {
	var keys []models.IDPSigningKey
	if err := s.db.WithContext(ctx).Order("id").Find(&keys).Error; err != nil {
		return nil, grpcErr(err)
	}
	response := &dbpb.ListIDPSigningKeysResponse{}
	for i := range keys {
		response.Keys = append(response.Keys, toProtoIDPSigningKey(&keys[i]))
	}
	return response, nil
}

// SetIDPSigningKeys atomically replaces the IdP's signing keyring with the
// given keys. It runs in a single transaction (delete all, insert the new
// set) so concurrent IdP replicas never observe a torn keyring.
func (s *Server) SetIDPSigningKeys(ctx context.Context, req *dbpb.SetIDPSigningKeysRequest) (*emptypb.Empty, error) {
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("1 = 1").Delete(&models.IDPSigningKey{}).Error; err != nil {
			return err
		}
		for _, key := range req.GetKeys() {
			mk, err := signingKeyFromProto(key)
			if err != nil {
				return err
			}
			if err := tx.Create(mk).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

// ---------------------------------------------------------------------------
// IdP authorization codes
// ---------------------------------------------------------------------------

// StoreIDPAuthCode stores a single-use OIDC authorization code.
func (s *Server) StoreIDPAuthCode(ctx context.Context, req *dbpb.StoreIDPAuthCodeRequest) (*emptypb.Empty, error) {
	code := req.GetCode()
	if code == nil || code.GetCode() == "" {
		return nil, status.Error(codes.InvalidArgument, "code is required")
	}
	ac := &models.IDPAuthCode{
		Code:          code.GetCode(),
		ClientID:      code.GetClientId(),
		RedirectURI:   code.GetRedirectUri(),
		CodeChallenge: code.GetCodeChallenge(),
		Subject:       code.GetSubject(),
		Name:          code.GetName(),
		Email:         code.GetEmail(),
	}
	if ts := code.GetCreatedAt(); ts != nil {
		ac.CreatedAt = ts.AsTime()
	}
	if err := s.db.WithContext(ctx).Create(ac).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

// ConsumeIDPAuthCode atomically fetches and deletes an authorization code so
// a code can be redeemed by at most one replica. A missing code returns
// NotFound.
func (s *Server) ConsumeIDPAuthCode(ctx context.Context, req *dbpb.ConsumeIDPAuthCodeRequest) (*dbpb.IDPAuthCode, error) {
	if req.GetCode() == "" {
		return nil, status.Error(codes.InvalidArgument, "code is required")
	}
	var ac models.IDPAuthCode
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("code = ?", req.GetCode()).First(&ac).Error; err != nil {
			return err
		}
		return tx.Delete(&ac).Error
	})
	if err != nil {
		return nil, grpcErr(err)
	}
	return toProtoIDPAuthCode(&ac), nil
}

// PruneIDPAuthCodes deletes authorization codes created before the given
// instant, or all of them when the instant is unset.
func (s *Server) PruneIDPAuthCodes(ctx context.Context, req *dbpb.PruneIDPAuthCodesRequest) (*emptypb.Empty, error) {
	query := s.db.WithContext(ctx).Model(&models.IDPAuthCode{})
	if ts := req.GetCreatedBefore(); ts != nil {
		query = query.Where("created_at < ?", ts.AsTime())
	} else {
		query = query.Where("1 = 1")
	}
	if err := query.Delete(&models.IDPAuthCode{}).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

// ---------------------------------------------------------------------------
// Conversions and helpers
// ---------------------------------------------------------------------------

func toProtoPipeline(pipeline *models.Pipeline) *dbpb.Pipeline {
	return &dbpb.Pipeline{
		Id:          int64(pipeline.ID),
		Name:        pipeline.Name,
		Description: pipeline.Description,
		CreatedAt:   timestamppb.New(pipeline.CreatedAt),
		UpdatedAt:   timestamppb.New(pipeline.UpdatedAt),
	}
}

func toProtoJob(job *models.Job) *dbpb.Job {
	proto := &dbpb.Job{
		Id:          int64(job.ID),
		Name:        job.Name,
		Status:      jobStatusToProto(job.Status),
		TargetGroup: job.TargetGroup,
		CreatedAt:   timestamppb.New(job.CreatedAt),
		UpdatedAt:   timestamppb.New(job.UpdatedAt),
		Spec:        specToProto(job.Spec),
		Attempt:     int32(job.Attempt),
		MaxAttempts: int32(job.MaxAttempts),
		DependsOn:   dependsOnToProto(job.DependsOn),
		StepResults: stepResultsToProto(job.StepResults),
	}
	if job.PipelineID != nil {
		proto.PipelineId = int64(*job.PipelineID)
	}
	if job.StartedAt != nil {
		proto.StartedAt = timestamppb.New(*job.StartedAt)
	}
	if job.FinishedAt != nil {
		proto.FinishedAt = timestamppb.New(*job.FinishedAt)
	}
	return proto
}

// dependsOnFromProto converts a list of proto job ids into the model's
// []uint. An empty list yields nil.
func dependsOnFromProto(ids []int64) []uint {
	if len(ids) == 0 {
		return nil
	}
	out := make([]uint, len(ids))
	for i, id := range ids {
		out[i] = uint(id)
	}
	return out
}

// dependsOnToProto converts the model's []uint into a list of proto job ids.
// An empty list yields nil.
func dependsOnToProto(ids []uint) []int64 {
	if len(ids) == 0 {
		return nil
	}
	out := make([]int64, len(ids))
	for i, id := range ids {
		out[i] = int64(id)
	}
	return out
}

// stepResultsFromProto converts a list of proto StepResults into the model's
// []StepResult. An empty list yields nil.
func stepResultsFromProto(results []*dbpb.StepResult) []models.StepResult {
	if len(results) == 0 {
		return nil
	}
	out := make([]models.StepResult, len(results))
	for i, r := range results {
		out[i] = models.StepResult{
			Index:  int(r.GetIndex()),
			Status: stepStatusFromProto(r.GetStatus()),
			Error:  r.GetError(),
		}
	}
	return out
}

// stepResultsToProto converts the model's []StepResult into a list of proto
// StepResults. An empty list yields nil.
func stepResultsToProto(results []models.StepResult) []*dbpb.StepResult {
	if len(results) == 0 {
		return nil
	}
	out := make([]*dbpb.StepResult, len(results))
	for i, r := range results {
		out[i] = &dbpb.StepResult{
			Index:  int32(r.Index),
			Status: stepStatusToProto(r.Status),
			Error:  r.Error,
		}
	}
	return out
}

func stepStatusFromProto(status dbpb.StepStatus) models.StepStatus {
	switch status {
	case dbpb.StepStatus_STEP_STATUS_SUCCEEDED:
		return models.StepStatusSucceeded
	case dbpb.StepStatus_STEP_STATUS_SKIPPED:
		return models.StepStatusSkipped
	case dbpb.StepStatus_STEP_STATUS_TIMED_OUT:
		return models.StepStatusTimedOut
	default:
		return models.StepStatusFailed
	}
}

func stepStatusToProto(status models.StepStatus) dbpb.StepStatus {
	switch status {
	case models.StepStatusSucceeded:
		return dbpb.StepStatus_STEP_STATUS_SUCCEEDED
	case models.StepStatusSkipped:
		return dbpb.StepStatus_STEP_STATUS_SKIPPED
	case models.StepStatusTimedOut:
		return dbpb.StepStatus_STEP_STATUS_TIMED_OUT
	default:
		return dbpb.StepStatus_STEP_STATUS_FAILED
	}
}

// specFromProto converts a proto JobSpec into the model's JobSpec. A nil
// proto yields a zero-value spec (no steps).
func specFromProto(spec *dbpb.JobSpec) models.JobSpec {
	if spec == nil {
		return models.JobSpec{}
	}
	steps := make([]models.JobStep, 0, len(spec.GetSteps()))
	for _, step := range spec.GetSteps() {
		steps = append(steps, models.JobStep{
			Type:      step.GetType(),
			Workdir:   step.GetWorkdir(),
			Env:       step.GetEnv(),
			Timeout:   step.GetTimeout().AsDuration(),
			Params:    paramsFromProto(step.GetParams()),
			Condition: step.GetCondition(),
		})
	}
	return models.JobSpec{
		Steps:   steps,
		Timeout: spec.GetTimeout().AsDuration(),
		Retry:   retryPolicyFromProto(spec.GetRetry()),
	}
}

// specToProto converts the model's JobSpec into a proto JobSpec. A spec with
// no steps, no timeout, and no retry policy yields a nil proto (so it
// round-trips to an empty spec).
func specToProto(spec models.JobSpec) *dbpb.JobSpec {
	if len(spec.Steps) == 0 && spec.Timeout == 0 && spec.Retry == nil {
		return nil
	}
	steps := make([]*dbpb.JobStep, 0, len(spec.Steps))
	for _, step := range spec.Steps {
		steps = append(steps, &dbpb.JobStep{
			Type:      step.Type,
			Workdir:   step.Workdir,
			Env:       step.Env,
			Timeout:   durationpb.New(step.Timeout),
			Params:    paramsToProto(step.Params),
			Condition: step.Condition,
		})
	}
	return &dbpb.JobSpec{
		Steps:   steps,
		Timeout: durationpb.New(spec.Timeout),
		Retry:   retryPolicyToProto(spec.Retry),
	}
}

// retryPolicyFromProto converts a proto RetryPolicy into the model's
// RetryPolicy. A nil proto yields a nil policy (the job is never retried).
func retryPolicyFromProto(policy *dbpb.RetryPolicy) *models.RetryPolicy {
	if policy == nil {
		return nil
	}
	return &models.RetryPolicy{
		MaxAttempts: int(policy.GetMaxAttempts()),
		Backoff:     policy.GetBackoff().AsDuration(),
	}
}

// retryPolicyToProto converts the model's RetryPolicy into a proto
// RetryPolicy. A nil policy yields a nil proto.
func retryPolicyToProto(policy *models.RetryPolicy) *dbpb.RetryPolicy {
	if policy == nil {
		return nil
	}
	return &dbpb.RetryPolicy{
		MaxAttempts: int32(policy.MaxAttempts),
		Backoff:     durationpb.New(policy.Backoff),
	}
}

// paramsFromProto converts a proto params map into the model's params map.
func paramsFromProto(in map[string]*dbpb.ParamValue) map[string]*models.ParamValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*models.ParamValue, len(in))
	for k, v := range in {
		out[k] = &models.ParamValue{
			String:  v.GetString_(),
			Strings: v.GetStrings(),
		}
	}
	return out
}

// paramsToProto converts the model's params map into a proto params map.
func paramsToProto(in map[string]*models.ParamValue) map[string]*dbpb.ParamValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*dbpb.ParamValue, len(in))
	for k, v := range in {
		out[k] = &dbpb.ParamValue{
			String_: v.String,
			Strings: v.Strings,
		}
	}
	return out
}

func toProtoWorker(worker *models.Worker) *dbpb.Worker {
	proto := &dbpb.Worker{Id: int64(worker.ID), Name: worker.Name, Group: worker.Group, Address: worker.Address}
	if worker.LastSeenAt != nil {
		proto.LastSeenAt = timestamppb.New(*worker.LastSeenAt)
	}
	return proto
}

func toProtoIDPSigningKey(key *models.IDPSigningKey) *dbpb.IDPSigningKey {
	return &dbpb.IDPSigningKey{
		Kid:       key.Kid,
		IsCurrent: key.IsCurrent,
		NotBefore: timestamppb.New(key.NotBefore),
		ExpiresAt: timestamppb.New(key.ExpiresAt),
		Pem:       key.Pem,
	}
}

func signingKeyFromProto(key *dbpb.IDPSigningKey) (*models.IDPSigningKey, error) {
	if key.GetKid() == "" {
		return nil, status.Error(codes.InvalidArgument, "kid is required")
	}
	if key.GetPem() == "" {
		return nil, status.Error(codes.InvalidArgument, "pem is required")
	}
	mk := &models.IDPSigningKey{
		Kid:       key.GetKid(),
		IsCurrent: key.GetIsCurrent(),
		NotBefore: time.Now(),
		ExpiresAt: time.Now(),
		Pem:       key.GetPem(),
	}
	if ts := key.GetNotBefore(); ts != nil {
		mk.NotBefore = ts.AsTime()
	}
	if ts := key.GetExpiresAt(); ts != nil {
		mk.ExpiresAt = ts.AsTime()
	}
	return mk, nil
}

func toProtoIDPAuthCode(code *models.IDPAuthCode) *dbpb.IDPAuthCode {
	return &dbpb.IDPAuthCode{
		Code:          code.Code,
		ClientId:      code.ClientID,
		RedirectUri:   code.RedirectURI,
		CodeChallenge: code.CodeChallenge,
		Subject:       code.Subject,
		Name:          code.Name,
		Email:         code.Email,
		CreatedAt:     timestamppb.New(code.CreatedAt),
	}
}

func jobStatusToProto(status models.JobStatus) dbpb.JobStatus {
	switch status {
	case models.JobStatusPending:
		return dbpb.JobStatus_JOB_STATUS_PENDING
	case models.JobStatusRunning:
		return dbpb.JobStatus_JOB_STATUS_RUNNING
	case models.JobStatusSucceeded:
		return dbpb.JobStatus_JOB_STATUS_SUCCEEDED
	case models.JobStatusFailed:
		return dbpb.JobStatus_JOB_STATUS_FAILED
	case models.JobStatusCancelled:
		return dbpb.JobStatus_JOB_STATUS_CANCELLED
	case models.JobStatusTimedOut:
		return dbpb.JobStatus_JOB_STATUS_TIMED_OUT
	case models.JobStatusSkipped:
		return dbpb.JobStatus_JOB_STATUS_SKIPPED
	default:
		return dbpb.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

func jobStatusFromProto(status dbpb.JobStatus) (models.JobStatus, error) {
	switch status {
	case dbpb.JobStatus_JOB_STATUS_PENDING:
		return models.JobStatusPending, nil
	case dbpb.JobStatus_JOB_STATUS_RUNNING:
		return models.JobStatusRunning, nil
	case dbpb.JobStatus_JOB_STATUS_SUCCEEDED:
		return models.JobStatusSucceeded, nil
	case dbpb.JobStatus_JOB_STATUS_FAILED:
		return models.JobStatusFailed, nil
	case dbpb.JobStatus_JOB_STATUS_CANCELLED:
		return models.JobStatusCancelled, nil
	case dbpb.JobStatus_JOB_STATUS_TIMED_OUT:
		return models.JobStatusTimedOut, nil
	case dbpb.JobStatus_JOB_STATUS_SKIPPED:
		return models.JobStatusSkipped, nil
	default:
		return "", fmt.Errorf("unknown job status %v", status)
	}
}

// grpcErr maps storage errors onto gRPC status codes.
func grpcErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return status.Error(codes.NotFound, "record not found")
	}
	return status.Error(codes.Internal, err.Error())
}
