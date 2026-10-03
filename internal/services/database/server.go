package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	// Remove child jobs and runs first, then the pipeline itself.
	if err := s.db.WithContext(ctx).Where("pipeline_id = ?", req.GetId()).Delete(&models.Job{}).Error; err != nil {
		return nil, grpcErr(err)
	}
	if err := s.db.WithContext(ctx).Where("pipeline_id = ?", req.GetId()).Delete(&models.PipelineRun{}).Error; err != nil {
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
// Pipeline runs (F-07)
// ---------------------------------------------------------------------------

// CreateRun atomically creates a pipeline run (F-07) and one job instance per
// job definition in the pipeline. Each instance is a fresh Job row bound to
// the run (RunID) and the pipeline (PipelineID), carrying a snapshot of its
// definition's spec, target group, retry policy, and ignore-failed flag.
//
// Each instance's depends_on is remapped from the definition job ids to the
// new instance ids, so the run's internal dependencies (F-06) reference the
// run's own job instances rather than the pipeline's definitions. This is what
// makes two runs of the same pipeline independent: each run's instances
// depend on each other, not on the other run's instances.
//
// The whole operation runs in a single transaction, so a run is never created
// with a partial set of job instances. The run starts in the pending state;
// the scheduler's CreateRun RPC (which calls this) then drives the instances,
// and the scheduler's run-status loop derives the run's overall status from
// them.
func (s *Server) CreateRun(ctx context.Context, req *dbpb.CreateRunRequest) (*dbpb.CreateRunResponse, error) {
	if req.GetPipelineId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "pipeline_id is required")
	}
	pipelineID := uint(req.GetPipelineId())
	var pipeline models.Pipeline
	if err := s.db.WithContext(ctx).First(&pipeline, pipelineID).Error; err != nil {
		return nil, grpcErr(err)
	}
	response := &dbpb.CreateRunResponse{}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// The run row is created first so the job instances can reference it.
		run := &models.PipelineRun{
			PipelineID: pipelineID,
			Status:     models.RunStatusPending,
			Trigger:    req.GetTrigger(),
			Params:     req.GetParams(),
		}
		if err := tx.Create(run).Error; err != nil {
			return err
		}
		response.Run = toProtoRun(run)

		// Load the pipeline's job definitions (the run's job instances are
		// created from these).
		var defs []models.Job
		if err := tx.Where("pipeline_id = ?", pipelineID).Order("id").Find(&defs).Error; err != nil {
			return err
		}
		// Pass 1: create every job instance (without depends_on yet) so all
		// instance ids are known, and record the definition-id -> instance-id
		// mapping used to remap each instance's dependencies.
		instances := make([]models.Job, 0, len(defs))
		defToInstance := make(map[uint]uint, len(defs))
		for i := range defs {
			def := &defs[i]
			job := &models.Job{
				PipelineID:  &pipelineID,
				RunID:       &run.ID,
				Name:        def.Name,
				TargetGroup: def.TargetGroup,
				Status:      models.JobStatusPending,
				Spec:        def.Spec,
				// A new instance starts on its first attempt (F-04); the retry
				// budget and ignore-failed flag are denormalized from the
				// definition's spec, mirroring CreateJob.
				Attempt: 1,
			}
			if def.Spec.Retry != nil {
				job.MaxAttempts = def.Spec.Retry.MaxAttempts
			}
			job.IgnoreFailed = def.Spec.IgnoreFailed
			if err := tx.Create(job).Error; err != nil {
				return err
			}
			defToInstance[def.ID] = job.ID
			instances = append(instances, *job)
		}
		// Pass 2: remap each instance's depends_on from the definition ids to
		// the new instance ids. A dependency on a job outside this pipeline is
		// left as-is (best-effort); in practice a pipeline's job dependencies
		// reference other jobs in the same pipeline.
		for i := range instances {
			def := &defs[i]
			if len(def.DependsOn) == 0 {
				continue
			}
			remapped := make([]uint, len(def.DependsOn))
			for j, defDepID := range def.DependsOn {
				if instID, ok := defToInstance[defDepID]; ok {
					remapped[j] = instID
				} else {
					remapped[j] = defDepID
				}
			}
			if err := tx.Model(&models.Job{}).
				Where("id = ?", instances[i].ID).
				Select("DependsOn").
				Updates(&models.Job{DependsOn: remapped}).Error; err != nil {
				return err
			}
			instances[i].DependsOn = remapped
		}
		for i := range instances {
			response.Jobs = append(response.Jobs, toProtoJob(&instances[i]))
		}
		return nil
	})
	if err != nil {
		return nil, grpcErr(err)
	}
	return response, nil
}

// GetRun fetches a pipeline run from the Database service.
func (s *Server) GetRun(ctx context.Context, req *dbpb.GetRunRequest) (*dbpb.PipelineRun, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "run id is required")
	}
	var run models.PipelineRun
	if err := s.db.WithContext(ctx).First(&run, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoRun(&run), nil
}

// ListRuns lists pipeline runs, optionally filtered by pipeline and status.
func (s *Server) ListRuns(ctx context.Context, req *dbpb.ListRunsRequest) (*dbpb.ListRunsResponse, error) {
	query := s.db.WithContext(ctx).Model(&models.PipelineRun{})
	if req.GetPipelineId() > 0 {
		query = query.Where("pipeline_id = ?", req.GetPipelineId())
	}
	if runStatus := req.GetStatus(); runStatus != dbpb.RunStatus_RUN_STATUS_UNSPECIFIED {
		modelStatus, err := runStatusFromProto(runStatus)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		query = query.Where("status = ?", modelStatus)
	}
	var runs []models.PipelineRun
	if err := query.Order("id").Find(&runs).Error; err != nil {
		return nil, grpcErr(err)
	}
	response := &dbpb.ListRunsResponse{}
	for i := range runs {
		response.Runs = append(response.Runs, toProtoRun(&runs[i]))
	}
	return response, nil
}

// UpdateRun applies a partial update to a run: a status of
// RUN_STATUS_UNSPECIFIED leaves the status unchanged, and nil timestamps leave
// the corresponding field unchanged. The scheduler's run-status loop uses it
// to persist a run's derived status and start/finish timestamps.
func (s *Server) UpdateRun(ctx context.Context, req *dbpb.UpdateRunRequest) (*dbpb.PipelineRun, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "run id is required")
	}
	var run models.PipelineRun
	if err := s.db.WithContext(ctx).First(&run, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	updates := map[string]any{}
	if runStatus := req.GetStatus(); runStatus != dbpb.RunStatus_RUN_STATUS_UNSPECIFIED {
		modelStatus, err := runStatusFromProto(runStatus)
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
	if len(updates) > 0 {
		updates["updated_at"] = time.Now()
		if err := s.db.WithContext(ctx).Model(&models.PipelineRun{}).
			Where("id = ?", req.GetId()).
			Updates(updates).Error; err != nil {
			return nil, grpcErr(err)
		}
	}
	if err := s.db.WithContext(ctx).First(&run, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoRun(&run), nil
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
	// The job's ignore-failed flag is denormalized from the spec so the
	// scheduler's dependency resolver can treat a failed job with the flag set
	// as satisfied without re-reading the spec (F-06).
	job.IgnoreFailed = spec.IgnoreFailed
	if req.GetPipelineId() > 0 {
		pipelineID := uint(req.GetPipelineId())
		job.PipelineID = &pipelineID
		var pipeline models.Pipeline
		if err := s.db.WithContext(ctx).First(&pipeline, pipelineID).Error; err != nil {
			return nil, grpcErr(err)
		}
	}
	if req.GetRunId() > 0 {
		runID := uint(req.GetRunId())
		job.RunID = &runID
		var run models.PipelineRun
		if err := s.db.WithContext(ctx).First(&run, runID).Error; err != nil {
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
	if req.GetRunId() > 0 {
		query = query.Where("run_id = ?", req.GetRunId())
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
	// outputs (F-06) are the named values the job produced, reported by the
	// execution target alongside its final status. Like step_results above,
	// they are persisted whenever the target reports them (even on the job's
	// final report) and are not part of the terminal-status guard below.
	if outputs := req.GetOutputs(); len(outputs) > 0 {
		if err := s.db.WithContext(ctx).
			Model(&models.Job{}).
			Where("id = ?", req.GetId()).
			Select("Outputs").
			Updates(&models.Job{Outputs: outputs}).Error; err != nil {
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

// ClaimJob atomically claims a pending job for execution (F-23): it sets the
// status to running (and the started timestamp) only if the job is still
// pending. It returns whether the job was claimed. This is how a worker (or
// the API on a worker's behalf) takes ownership of a job it picked up via the
// pull path or a push nudge: the conditional update is atomic, so a job
// delivered by both push and pull is claimed exactly once and the duplicate is
// a no-op.
func (s *Server) ClaimJob(ctx context.Context, req *dbpb.ClaimJobRequest) (*dbpb.ClaimJobResponse, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job id is required")
	}
	startedAt := time.Now()
	if timestamp := req.GetStartedAt(); timestamp != nil {
		startedAt = timestamp.AsTime()
	}
	result := s.db.WithContext(ctx).
		Model(&models.Job{}).
		Where("id = ? AND status = ?", req.GetId(), models.JobStatusPending).
		Updates(map[string]any{
			"status":     models.JobStatusRunning,
			"started_at": &startedAt,
			"updated_at": time.Now(),
		})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	return &dbpb.ClaimJobResponse{Claimed: result.RowsAffected > 0}, nil
}

// ListPendingByGroup returns the pending jobs that target the given worker
// group (F-23). This is the pull path: a worker (or the API on its behalf)
// polls it to pick up jobs that were published while it was unreachable, so
// no job is stranded pending. Filtering in the database keeps the poll from
// pulling every pending job over the wire as the system grows.
func (s *Server) ListPendingByGroup(ctx context.Context, req *dbpb.ListPendingByGroupRequest) (*dbpb.ListJobsResponse, error) {
	var jobs []models.Job
	err := s.db.WithContext(ctx).
		Model(&models.Job{}).
		Where("status = ? AND target_group = ?", models.JobStatusPending, req.GetGroup()).
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
// Event log (F-23, high availability)
// ---------------------------------------------------------------------------
//
// The event log is the shared, append-only coordination bus. The scheduler's
// background loops and the API publish state changes to it; every API pod
// tails it (TailEvents) and fans the events out to its local workers and UI
// clients. An event is a pointer, not the data: the payload is a small JSON
// object with just enough to handle the event, never the job spec, the log
// bytes, or a token.
//
// Each publish appends one event row. (The design calls for the state change
// and the event append to be a single transaction; the state changes here are
// performed by the caller through the existing conditional RPCs, and the
// event is appended immediately after. Because the log is a recent buffer and
// the database state is the source of truth, a consumer that misses an event
// resyncs from a state snapshot rather than depending on the log being
// perfectly transactional with the state.)

// defaultTailLimit bounds how many events TailEvents returns in one call when
// the caller does not specify a limit.
const defaultTailLimit = 1000

// defaultLeaseTTL is the lease validity when the caller does not specify one.
const defaultLeaseTTL = 10 * time.Second

// appendEvent inserts a single event row and returns its id.
func (s *Server) appendEvent(ctx context.Context, name, workerGroup string, payload any) (int64, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("database: marshal event payload: %w", err)
	}
	event := &models.Event{
		Name:        name,
		WorkerGroup: workerGroup,
		Payload:     string(data),
	}
	if err := s.db.WithContext(ctx).Create(event).Error; err != nil {
		return 0, grpcErr(err)
	}
	return int64(event.ID), nil
}

// PublishAssignment appends an "assignment" event for a job (a nudge: the
// payload carries only the job id and target group, not the spec or token).
func (s *Server) PublishAssignment(ctx context.Context, req *dbpb.PublishAssignmentRequest) (*dbpb.PublishEventResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	id, err := s.appendEvent(ctx, "assignment", req.GetTargetGroup(), map[string]any{
		"job_id": req.GetJobId(),
	})
	if err != nil {
		return nil, err
	}
	return &dbpb.PublishEventResponse{Id: id}, nil
}

// PublishCancel appends a "cancel" event for a job.
func (s *Server) PublishCancel(ctx context.Context, req *dbpb.PublishCancelRequest) (*dbpb.PublishEventResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	id, err := s.appendEvent(ctx, "cancel", "", map[string]any{"job_id": req.GetJobId()})
	if err != nil {
		return nil, err
	}
	return &dbpb.PublishEventResponse{Id: id}, nil
}

// PublishJobStatus appends a "job_status" event for a job's status change.
func (s *Server) PublishJobStatus(ctx context.Context, req *dbpb.PublishJobStatusRequest) (*dbpb.PublishEventResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if req.GetStatus() == dbpb.JobStatus_JOB_STATUS_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}
	id, err := s.appendEvent(ctx, "job_status", "", map[string]any{
		"job_id":       req.GetJobId(),
		"status":       jobStatusName(req.GetStatus()),
		"attempt":      req.GetAttempt(),
		"max_attempts": req.GetMaxAttempts(),
	})
	if err != nil {
		return nil, err
	}
	return &dbpb.PublishEventResponse{Id: id}, nil
}

// PublishRunStatus appends a "run_status" event for a run's status change.
func (s *Server) PublishRunStatus(ctx context.Context, req *dbpb.PublishRunStatusRequest) (*dbpb.PublishEventResponse, error) {
	if req.GetRunId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	if req.GetStatus() == dbpb.RunStatus_RUN_STATUS_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}
	id, err := s.appendEvent(ctx, "run_status", "", map[string]any{
		"run_id": req.GetRunId(),
		"status": runStatusName(req.GetStatus()),
	})
	if err != nil {
		return nil, err
	}
	return &dbpb.PublishEventResponse{Id: id}, nil
}

// PublishWorkerEvent appends a "worker" event for a worker lifecycle change.
func (s *Server) PublishWorkerEvent(ctx context.Context, req *dbpb.PublishWorkerEventRequest) (*dbpb.PublishEventResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	id, err := s.appendEvent(ctx, "worker", req.GetGroup(), map[string]any{
		"name":   req.GetName(),
		"action": req.GetAction(),
	})
	if err != nil {
		return nil, err
	}
	return &dbpb.PublishEventResponse{Id: id}, nil
}

// PublishLogUpdated appends a "job_log_updated" event: a pointer that says a
// job's log has grown to a given size. The log bytes live in the artifacts
// store, never in the event log; a consumer range-reads the delta.
func (s *Server) PublishLogUpdated(ctx context.Context, req *dbpb.PublishLogUpdatedRequest) (*dbpb.PublishEventResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	id, err := s.appendEvent(ctx, "job_log_updated", "", map[string]any{
		"job_id":     req.GetJobId(),
		"log":        req.GetLog(),
		"step_index": req.GetStepIndex(),
		"stream":     req.GetStream(),
		"size":       req.GetSize(),
	})
	if err != nil {
		return nil, err
	}
	return &dbpb.PublishEventResponse{Id: id}, nil
}

// TailEvents returns the events with id greater than cursor, in id order, up
// to limit. This is how an API pod tails the log: it keeps a per-pod cursor,
// fetches the new events, fans them out, and advances the cursor.
func (s *Server) TailEvents(ctx context.Context, req *dbpb.TailEventsRequest) (*dbpb.TailEventsResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultTailLimit
	}
	var events []models.Event
	err := s.db.WithContext(ctx).
		Model(&models.Event{}).
		Where("id > ?", req.GetCursor()).
		Order("id").
		Limit(limit).
		Find(&events).Error
	if err != nil {
		return nil, grpcErr(err)
	}
	response := &dbpb.TailEventsResponse{Cursor: req.GetCursor()}
	for i := range events {
		response.Events = append(response.Events, toProtoEvent(&events[i]))
		response.Cursor = int64(events[i].ID)
	}
	return response, nil
}

// AcquireLease acquires the named lease for the caller, or re-acquires it if
// the caller already holds it (a renewal). It succeeds if the lease is free,
// expired, or already held by the caller; it fails if another replica holds
// an unexpired lease.
//
// The acquire-or-renew is a portable compare-and-swap that is safe on both
// SQLite and PostgreSQL:
//
//  1. A conditional UPDATE claims the row if it is held by the caller or is
//     expired. It is a single atomic statement, so two replicas racing for an
//     expired lease cannot both match it.
//  2. If the update matched nothing, the row is either absent or held by
//     someone else with an unexpired lease. The caller tries to INSERT; the
//     unique constraint on name means at most one racing inserter wins, and a
//     loser reports that it did not acquire.
func (s *Server) AcquireLease(ctx context.Context, req *dbpb.AcquireLeaseRequest) (*dbpb.AcquireLeaseResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if req.GetHolder() == "" {
		return nil, status.Error(codes.InvalidArgument, "holder is required")
	}
	ttl := req.GetTtl().AsDuration()
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	now := time.Now()
	expires := now.Add(ttl)
	db := s.db.WithContext(ctx)

	// Step 1: claim the row if it is ours or expired.
	result := db.Model(&models.Lease{}).
		Where("name = ? AND (holder = ? OR expires_at < ?)", req.GetName(), req.GetHolder(), now).
		Updates(map[string]interface{}{"holder": req.GetHolder(), "expires_at": expires})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	if result.RowsAffected > 0 {
		return &dbpb.AcquireLeaseResponse{Acquired: true}, nil
	}

	// Step 2: the row is absent or held by someone else unexpired. Try to
	// insert; a racing inserter loses the unique constraint and reports that
	// it did not acquire.
	if err := db.Create(&models.Lease{Name: req.GetName(), Holder: req.GetHolder(), ExpiresAt: expires}).Error; err != nil {
		if isUniqueViolation(err) {
			return &dbpb.AcquireLeaseResponse{Acquired: false}, nil
		}
		return nil, grpcErr(err)
	}
	return &dbpb.AcquireLeaseResponse{Acquired: true}, nil
}

// ReleaseLease releases the named lease if the caller holds it. Releasing a
// lease the caller does not hold is a no-op, so a replica that has already
// lost the lease can still call this on shutdown without error.
func (s *Server) ReleaseLease(ctx context.Context, req *dbpb.ReleaseLeaseRequest) (*emptypb.Empty, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if req.GetHolder() == "" {
		return nil, status.Error(codes.InvalidArgument, "holder is required")
	}
	result := s.db.WithContext(ctx).
		Where("name = ? AND holder = ?", req.GetName(), req.GetHolder()).
		Delete(&models.Lease{})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	return &emptypb.Empty{}, nil
}

// isUniqueViolation reports whether err is a unique-constraint violation from
// either supported backend.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "duplicate key value")
}

// toProtoEvent converts a model Event into a proto Event.
func toProtoEvent(event *models.Event) *dbpb.Event {
	return &dbpb.Event{
		Id:          int64(event.ID),
		Name:        event.Name,
		WorkerGroup: event.WorkerGroup,
		Payload:     event.Payload,
		CreatedAt:   timestamppb.New(event.CreatedAt),
	}
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

func toProtoRun(run *models.PipelineRun) *dbpb.PipelineRun {
	proto := &dbpb.PipelineRun{
		Id:         int64(run.ID),
		PipelineId: int64(run.PipelineID),
		Status:     runStatusToProto(run.Status),
		Trigger:    run.Trigger,
		Params:     run.Params,
		CreatedAt:  timestamppb.New(run.CreatedAt),
		UpdatedAt:  timestamppb.New(run.UpdatedAt),
	}
	if run.StartedAt != nil {
		proto.StartedAt = timestamppb.New(*run.StartedAt)
	}
	if run.FinishedAt != nil {
		proto.FinishedAt = timestamppb.New(*run.FinishedAt)
	}
	return proto
}

func toProtoJob(job *models.Job) *dbpb.Job {
	proto := &dbpb.Job{
		Id:           int64(job.ID),
		Name:         job.Name,
		Status:       jobStatusToProto(job.Status),
		TargetGroup:  job.TargetGroup,
		CreatedAt:    timestamppb.New(job.CreatedAt),
		UpdatedAt:    timestamppb.New(job.UpdatedAt),
		Spec:         specToProto(job.Spec),
		Attempt:      int32(job.Attempt),
		MaxAttempts:  int32(job.MaxAttempts),
		DependsOn:    dependsOnToProto(job.DependsOn),
		StepResults:  stepResultsToProto(job.StepResults),
		Outputs:      job.Outputs,
		IgnoreFailed: job.IgnoreFailed,
	}
	if job.PipelineID != nil {
		proto.PipelineId = int64(*job.PipelineID)
	}
	if job.RunID != nil {
		proto.RunId = int64(*job.RunID)
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
			Index:   int(r.GetIndex()),
			Status:  stepStatusFromProto(r.GetStatus()),
			Error:   r.GetError(),
			Outputs: r.GetOutputs(),
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
			Index:   int32(r.Index),
			Status:  stepStatusToProto(r.Status),
			Error:   r.Error,
			Outputs: r.Outputs,
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
			Type:         step.GetType(),
			Workdir:      step.GetWorkdir(),
			Env:          step.GetEnv(),
			Timeout:      step.GetTimeout().AsDuration(),
			Params:       paramsFromProto(step.GetParams()),
			Condition:    step.GetCondition(),
			IgnoreFailed: step.GetIgnoreFailed(),
			Outputs:      step.GetOutputs(),
		})
	}
	return models.JobSpec{
		Steps:        steps,
		Timeout:      spec.GetTimeout().AsDuration(),
		Retry:        retryPolicyFromProto(spec.GetRetry()),
		IgnoreFailed: spec.GetIgnoreFailed(),
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
			Type:         step.Type,
			Workdir:      step.Workdir,
			Env:          step.Env,
			Timeout:      durationpb.New(step.Timeout),
			Params:       paramsToProto(step.Params),
			Condition:    step.Condition,
			IgnoreFailed: step.IgnoreFailed,
			Outputs:      step.Outputs,
		})
	}
	return &dbpb.JobSpec{
		Steps:        steps,
		Timeout:      durationpb.New(spec.Timeout),
		Retry:        retryPolicyToProto(spec.Retry),
		IgnoreFailed: spec.IgnoreFailed,
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

func runStatusToProto(status models.RunStatus) dbpb.RunStatus {
	switch status {
	case models.RunStatusPending:
		return dbpb.RunStatus_RUN_STATUS_PENDING
	case models.RunStatusRunning:
		return dbpb.RunStatus_RUN_STATUS_RUNNING
	case models.RunStatusSucceeded:
		return dbpb.RunStatus_RUN_STATUS_SUCCEEDED
	case models.RunStatusFailed:
		return dbpb.RunStatus_RUN_STATUS_FAILED
	case models.RunStatusCancelled:
		return dbpb.RunStatus_RUN_STATUS_CANCELLED
	default:
		return dbpb.RunStatus_RUN_STATUS_UNSPECIFIED
	}
}

func runStatusFromProto(status dbpb.RunStatus) (models.RunStatus, error) {
	switch status {
	case dbpb.RunStatus_RUN_STATUS_PENDING:
		return models.RunStatusPending, nil
	case dbpb.RunStatus_RUN_STATUS_RUNNING:
		return models.RunStatusRunning, nil
	case dbpb.RunStatus_RUN_STATUS_SUCCEEDED:
		return models.RunStatusSucceeded, nil
	case dbpb.RunStatus_RUN_STATUS_FAILED:
		return models.RunStatusFailed, nil
	case dbpb.RunStatus_RUN_STATUS_CANCELLED:
		return models.RunStatusCancelled, nil
	default:
		return "", fmt.Errorf("unknown run status %v", status)
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

// jobStatusName maps a job status enum to the short name the UI and the
// event-log payload use (F-23). It mirrors the API's jobStatusName so a
// job_status event published by the database service carries the same status
// string the UI expects.
func jobStatusName(status dbpb.JobStatus) string {
	switch status {
	case dbpb.JobStatus_JOB_STATUS_PENDING:
		return "pending"
	case dbpb.JobStatus_JOB_STATUS_RUNNING:
		return "running"
	case dbpb.JobStatus_JOB_STATUS_SUCCEEDED:
		return "succeeded"
	case dbpb.JobStatus_JOB_STATUS_FAILED:
		return "failed"
	case dbpb.JobStatus_JOB_STATUS_CANCELLED:
		return "cancelled"
	case dbpb.JobStatus_JOB_STATUS_TIMED_OUT:
		return "timed_out"
	case dbpb.JobStatus_JOB_STATUS_SKIPPED:
		return "skipped"
	default:
		return "unknown"
	}
}

// runStatusName maps a run status enum to the short name the UI and the
// event-log payload use (F-23).
func runStatusName(status dbpb.RunStatus) string {
	switch status {
	case dbpb.RunStatus_RUN_STATUS_PENDING:
		return "pending"
	case dbpb.RunStatus_RUN_STATUS_RUNNING:
		return "running"
	case dbpb.RunStatus_RUN_STATUS_SUCCEEDED:
		return "succeeded"
	case dbpb.RunStatus_RUN_STATUS_FAILED:
		return "failed"
	case dbpb.RunStatus_RUN_STATUS_CANCELLED:
		return "cancelled"
	default:
		return "unknown"
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
