package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
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
	pipeline := &models.Pipeline{
		Name:        req.GetName(),
		Description: req.GetDescription(),
		FailureMode: failureModeFromProto(req.GetFailureMode()),
		// A new pipeline starts at version 1 (F-11); each subsequent edit is
		// the next version.
		Version: 1,
		// The pipeline's triggers (F-09) are validated before anything is
		// persisted: a malformed trigger (a bad cron expression, a webhook
		// with no secret, a duplicate name) rejects the whole create, so a
		// pipeline is never saved with a trigger that could never fire.
		Triggers: triggersFromProto(req.GetTriggers()),
		// The pipeline's parameter declarations (F-10) are validated before
		// anything is persisted: a parameter with an empty or duplicate name
		// rejects the whole create, so a pipeline is never saved with a
		// parameter that could never be referenced unambiguously.
		Params: pipelineParamsFromProto(req.GetParams()),
	}
	if err := validateTriggers(req.GetTriggers()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateParams(req.GetParams()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// The pipeline's job definitions (F-08) are validated as a DAG before
	// anything is persisted: a cycle or an unknown key rejects the whole
	// create, so a pipeline is never saved in an unrunnable state.
	defs := jobDefinitionsFromProto(req.GetJobs())
	if err := validateJobDAG(req.GetJobs()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// needsByIndex[i] carries the needs (keys) of defs[i] from the request, so
	// they can be resolved to depends_on ids (F-08) once the jobs are created
	// and their ids are known. The backend stores dependencies as ids; needs is
	// only the authoring form.
	needsByIndex := make([][]string, len(defs))
	for i, def := range req.GetJobs() {
		needsByIndex[i] = def.GetNeeds()
	}
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(pipeline).Error; err != nil {
			return err
		}
		for i := range defs {
			def := &defs[i]
			pipelineID := pipeline.ID
			def.PipelineID = &pipelineID
			def.Status = models.JobStatusPending
			def.Attempt = 1
			if def.Spec.Retry != nil {
				def.MaxAttempts = def.Spec.Retry.MaxAttempts
			}
			def.IgnoreFailed = def.Spec.IgnoreFailed
			def.StepBarrier = def.Spec.StepBarrier
			def.FailureMode = def.Spec.FailureMode
			if def.FailureMode == "" {
				def.FailureMode = pipeline.FailureMode
			}
			if def.FailureMode == "" {
				def.FailureMode = models.FailureModeAll
			}
			if err := tx.Create(def).Error; err != nil {
				return err
			}
		}
		// Pass 2: now that all job ids are known, resolve each definition's
		// needs (keys) to the ids of the jobs they reference and store the
		// result in depends_on (F-08).
		keyToID := keyToIDMap(defs)
		for i := range defs {
			if len(needsByIndex[i]) == 0 {
				continue
			}
			defs[i].DependsOn = resolveNeedsToDependsOn(needsByIndex[i], keyToID)
			if err := tx.Model(&models.Job{}).
				Where("id = ?", defs[i].ID).
				Select("DependsOn").
				Updates(&models.Job{DependsOn: defs[i].DependsOn}).Error; err != nil {
				return err
			}
		}
		// Snapshot the pipeline's definition as version 1 (F-11): the create
		// is the pipeline's first version, so a run of the pipeline is bound
		// to a version snapshot that can be re-executed or inspected.
		if err := tx.Create(snapshotPipelineVersion(pipeline, defs)).Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, grpcErr(err)
	}
	// The definitions were created in the transaction; attach them so the
	// response carries the pipeline's jobs (with their ids and denormalized
	// fields) back to the caller. needs is reconstructed from the resolved
	// depends_on so the response mirrors what the caller sent.
	pipeline.Jobs = defs
	return toProtoPipeline(pipeline), nil
}

func (s *Server) GetPipeline(ctx context.Context, req *dbpb.GetPipelineRequest) (*dbpb.Pipeline, error) {
	var pipeline models.Pipeline
	if err := s.db.WithContext(ctx).First(&pipeline, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	// Load the pipeline's job definitions so the response carries them (with
	// needs reconstructed from depends_on for display, F-08).
	defs, err := s.loadPipelineJobDefinitions(ctx, pipeline.ID)
	if err != nil {
		return nil, grpcErr(err)
	}
	pipeline.Jobs = defs
	return toProtoPipeline(&pipeline), nil
}

// ListPipelines lists pipelines, optionally restricted to those that carry at
// least one trigger of a given type (F-09). The scheduler's cron and event
// loops use the filter to fetch only the pipelines they act on. Triggers are
// stored as a JSON column, so the type filter is applied in Go (portable
// across SQLite and PostgreSQL) rather than in a driver-specific JSON query.
func (s *Server) ListPipelines(ctx context.Context, req *dbpb.ListPipelinesRequest) (*dbpb.ListPipelinesResponse, error) {
	var pipelines []models.Pipeline
	if err := s.db.WithContext(ctx).Order("id").Find(&pipelines).Error; err != nil {
		return nil, grpcErr(err)
	}
	// When a trigger type is requested, keep only the pipelines that carry at
	// least one trigger of that type.
	if triggerType := req.GetTriggerType(); triggerType != dbpb.TriggerType_TRIGGER_TYPE_UNSPECIFIED {
		filtered := make([]models.Pipeline, 0, len(pipelines))
		for i := range pipelines {
			if pipelineHasTriggerType(&pipelines[i], triggerType) {
				filtered = append(filtered, pipelines[i])
			}
		}
		pipelines = filtered
	}
	response := &dbpb.ListPipelinesResponse{}
	for i := range pipelines {
		// Load each pipeline's job definitions so the response carries them
		// (with needs reconstructed from depends_on for display, F-08).
		defs, err := s.loadPipelineJobDefinitions(ctx, pipelines[i].ID)
		if err != nil {
			return nil, grpcErr(err)
		}
		pipelines[i].Jobs = defs
		response.Pipelines = append(response.Pipelines, toProtoPipeline(&pipelines[i]))
	}
	return response, nil
}

// pipelineHasTriggerType reports whether the pipeline carries at least one
// trigger of the given type (F-09).
func pipelineHasTriggerType(pipeline *models.Pipeline, triggerType dbpb.TriggerType) bool {
	for i := range pipeline.Triggers {
		if triggerTypeToProto(pipeline.Triggers[i].Type) == triggerType {
			return true
		}
	}
	return false
}

func (s *Server) UpdatePipeline(ctx context.Context, req *dbpb.UpdatePipelineRequest) (*dbpb.Pipeline, error) {
	var pipeline models.Pipeline
	if err := s.db.WithContext(ctx).First(&pipeline, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	// Every update is a new version (F-11): the pipeline's version is bumped
	// and a snapshot of the new definition is stored, so a run bound to the
	// previous version still reproduces the old definition.
	pipeline.Version++
	if req.GetName() != "" {
		pipeline.Name = req.GetName()
	}
	pipeline.Description = req.GetDescription()
	if req.GetFailureMode() != dbpb.FailureMode_FAILURE_MODE_UNSPECIFIED {
		pipeline.FailureMode = failureModeFromProto(req.GetFailureMode())
	}
	// When triggers are supplied (F-09) they replace the pipeline's existing
	// triggers: they are validated (unique names, kind-specific fields
	// present) before anything is persisted. When none are supplied the
	// pipeline's triggers are left unchanged.
	if req.GetTriggers() != nil {
		if err := validateTriggers(req.GetTriggers()); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		pipeline.Triggers = triggersFromProto(req.GetTriggers())
	}
	// When parameters are supplied (F-10) they replace the pipeline's existing
	// parameter declarations: they are validated (unique, non-empty names)
	// before anything is persisted. When none are supplied the pipeline's
	// parameters are left unchanged.
	if req.GetParams() != nil {
		if err := validateParams(req.GetParams()); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		pipeline.Params = pipelineParamsFromProto(req.GetParams())
	}
	// When job definitions are supplied (F-08) they replace the pipeline's
	// existing job definitions: the old definitions are removed and the new
	// ones created, and the resulting DAG is validated for cycles and unknown
	// keys before anything is persisted. When none are supplied the pipeline's
	// job definitions are left unchanged.
	if req.GetJobs() != nil {
		defs := jobDefinitionsFromProto(req.GetJobs())
		if err := validateJobDAG(req.GetJobs()); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		// needsByIndex[i] carries the needs (keys) of defs[i] from the request,
		// so they can be resolved to depends_on ids (F-08) once the jobs are
		// created and their ids are known.
		needsByIndex := make([][]string, len(defs))
		for i, def := range req.GetJobs() {
			needsByIndex[i] = def.GetNeeds()
		}
		if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Save(&pipeline).Error; err != nil {
				return err
			}
			if err := tx.Where("pipeline_id = ?", pipeline.ID).Delete(&models.Job{}).Error; err != nil {
				return err
			}
			for i := range defs {
				def := &defs[i]
				pipelineID := pipeline.ID
				def.PipelineID = &pipelineID
				def.Status = models.JobStatusPending
				def.Attempt = 1
				if def.Spec.Retry != nil {
					def.MaxAttempts = def.Spec.Retry.MaxAttempts
				}
				def.IgnoreFailed = def.Spec.IgnoreFailed
				def.StepBarrier = def.Spec.StepBarrier
				def.FailureMode = def.Spec.FailureMode
				if def.FailureMode == "" {
					def.FailureMode = pipeline.FailureMode
				}
				if def.FailureMode == "" {
					def.FailureMode = models.FailureModeAll
				}
				if err := tx.Create(def).Error; err != nil {
					return err
				}
			}
			// Pass 2: resolve each definition's needs (keys) to the ids of the
			// jobs they reference and store the result in depends_on (F-08).
			keyToID := keyToIDMap(defs)
			for i := range defs {
				if len(needsByIndex[i]) == 0 {
					continue
				}
				defs[i].DependsOn = resolveNeedsToDependsOn(needsByIndex[i], keyToID)
				if err := tx.Model(&models.Job{}).
					Where("id = ?", defs[i].ID).
					Select("DependsOn").
					Updates(&models.Job{DependsOn: defs[i].DependsOn}).Error; err != nil {
					return err
				}
			}
			// Snapshot the new definition as the pipeline's new version
			// (F-11), atomically with the update.
			if err := tx.Create(snapshotPipelineVersion(&pipeline, defs)).Error; err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, grpcErr(err)
		}
		pipeline.Jobs = defs
	} else {
		// No job definitions supplied: the pipeline's job definitions are
		// unchanged, so the new version's snapshot carries the pipeline's
		// current definitions.
		defs, err := s.loadPipelineJobDefinitions(ctx, pipeline.ID)
		if err != nil {
			return nil, grpcErr(err)
		}
		if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Save(&pipeline).Error; err != nil {
				return err
			}
			if err := tx.Create(snapshotPipelineVersion(&pipeline, defs)).Error; err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, grpcErr(err)
		}
		pipeline.Jobs = defs
	}
	return toProtoPipeline(&pipeline), nil
}

func (s *Server) DeletePipeline(ctx context.Context, req *dbpb.DeletePipelineRequest) (*emptypb.Empty, error) {
	// Remove child jobs, runs, and version snapshots first, then the pipeline
	// itself.
	if err := s.db.WithContext(ctx).Where("pipeline_id = ?", req.GetId()).Delete(&models.Job{}).Error; err != nil {
		return nil, grpcErr(err)
	}
	if err := s.db.WithContext(ctx).Where("pipeline_id = ?", req.GetId()).Delete(&models.PipelineRun{}).Error; err != nil {
		return nil, grpcErr(err)
	}
	if err := s.db.WithContext(ctx).Where("pipeline_id = ?", req.GetId()).Delete(&models.PipelineVersion{}).Error; err != nil {
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
	// Pre-validate a requested version (F-11) before the transaction so a
	// missing version surfaces as InvalidArgument rather than being wrapped by
	// the transaction into an Internal error.
	if requested := int(req.GetPipelineVersion()); requested > 0 {
		var count int64
		if err := s.db.WithContext(ctx).Model(&models.PipelineVersion{}).
			Where("pipeline_id = ? AND version = ?", pipelineID, requested).
			Count(&count).Error; err != nil {
			return nil, grpcErr(err)
		}
		if count == 0 {
			return nil, status.Error(codes.InvalidArgument,
				fmt.Sprintf("pipeline %d has no version %d", pipelineID, requested))
		}
	}
	response := &dbpb.CreateRunResponse{}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// The version the run executes (F-11): the specific version requested,
		// or the pipeline's current version when none is requested. It is
		// recorded on the run so the run is bound to the definition it
		// executed.
		pipelineVersion := pipeline.Version
		if requested := int(req.GetPipelineVersion()); requested > 0 {
			pipelineVersion = requested
		}
		run := &models.PipelineRun{
			PipelineID:      pipelineID,
			PipelineVersion: pipelineVersion,
			Status:          models.RunStatusPending,
			Trigger:         req.GetTrigger(),
			TriggerName:     req.GetTrigger(),
			Params:          req.GetParams(),
		}
		// The version the run's job instances are created from (F-11): the
		// specific version requested (a re-run of an old run reproduces that
		// version's definition), or 0 for the pipeline's current definitions.
		createdRun, instances, err := s.createRunAndInstances(tx, &pipeline, run, int(req.GetPipelineVersion()))
		if err != nil {
			return err
		}
		response.Run = toProtoRun(createdRun)
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

// createRunAndInstances creates a pipeline run row and one job instance per
// job definition in the pipeline, inside the caller's transaction (F-07).
// Each instance is a fresh Job row bound to the run (RunID) and the pipeline
// (PipelineID), carrying a snapshot of its definition's spec, target group,
// retry policy, and ignore-failed flag. Each instance's depends_on is remapped
// from the definition job ids to the new instance ids, so the run's internal
// dependencies (F-06) reference the run's own job instances rather than the
// pipeline's definitions.
//
// The run row is created first so the job instances can reference it. The
// caller is responsible for committing (or rolling back) the transaction.
// runDefinition is the source a run's job instances are created from (F-11):
// either the pipeline's current job definitions (a run against the current
// version) or a version snapshot's job definitions (a run against a specific
// version, e.g. a re-run of an old run). It carries the definition's failure
// mode (used to default a job that sets none) and, for a version snapshot, the
// needs (keys) of each definition so the run's internal dependencies can be
// remapped from keys to the run's own instance ids.
type runDefinition struct {
	// jobs are the job definitions the run's instances are created from.
	jobs []models.Job
	// failureMode is the definition's default failure mode (the pipeline's
	// FailureMode for the current version, or the snapshot's for a specific
	// version).
	failureMode models.FailureMode
	// params are the definition's parameter declarations (F-10): the
	// pipeline's Params for the current version, or the snapshot's Params for a
	// specific version. A run's concrete parameter values are computed from
	// these.
	params []models.Parameter
	// needsByIndex[i] carries the needs (keys) of jobs[i] (F-08); non-nil only
	// for a version snapshot, where the needs are keys that must be resolved to
	// the run's instance ids. For the current version the needs are already
	// resolved to depends_on ids on the definitions, so this is nil.
	needsByIndex [][]string
}

// createRunAndInstances creates a pipeline run row and one job instance per
// job definition, inside the caller's transaction (F-07). The definitions the
// instances are created from are selected by requestedVersion (F-11): 0 uses
// the pipeline's current job definitions (a run against the current version),
// while a specific version uses that version's snapshot (a re-run of an old
// run reproduces the old definition). Each instance is a fresh Job row bound
// to the run (RunID) and the pipeline (PipelineID), carrying a snapshot of its
// definition's spec, target group, retry policy, and ignore-failed flag.
//
// Each instance's dependencies are remapped to the run's own instance ids, so
// the run's internal dependencies (F-06) reference the run's own job
// instances rather than the pipeline's definitions. This is what makes two
// runs of the same pipeline independent: each run's instances depend on each
// other, not on the other run's instances.
//
// The run row is created first so the job instances can reference it. The
// caller is responsible for committing (or rolling back) the transaction.
func (s *Server) createRunAndInstances(tx *gorm.DB, pipeline *models.Pipeline, run *models.PipelineRun, requestedVersion int) (*models.PipelineRun, []models.Job, error) {
	if err := tx.Create(run).Error; err != nil {
		return nil, nil, err
	}
	pipelineID := run.PipelineID
	// Select the definition the run's instances are created from (F-11): the
	// pipeline's current job definitions for a run against the current version,
	// or the requested version's snapshot for a run against a specific version
	// (a re-run of an old run reproduces the old definition).
	def, err := s.runDefinitionFor(tx, pipeline, requestedVersion)
	if err != nil {
		return nil, nil, err
	}
	// The run's concrete parameter values (F-10): the values supplied for the
	// definition's parameters, a parameter not supplied falling back to its
	// default. They are denormalized onto every job instance so the execution
	// target can interpolate them into the job's spec (env, command, workdir)
	// before the job runs.
	runParams := runParamsFor(def.params, run.Params)
	// Pass 1: create every job instance (without depends_on yet) so all
	// instance ids are known, and record the key -> instance-id mapping used to
	// remap each instance's dependencies to the run's own instance ids (a
	// version snapshot's needs are keys, so the mapping is keyed by the job's
	// stable key, F-08).
	instances := make([]models.Job, 0, len(def.jobs))
	keyToInstanceID := make(map[string]uint, len(def.jobs))
	defToInstance := make(map[uint]uint, len(def.jobs))
	for i := range def.jobs {
		jobDef := &def.jobs[i]
		job := &models.Job{
			PipelineID:  &pipelineID,
			RunID:       &run.ID,
			Key:         jobDef.Key,
			Name:        jobDef.Name,
			TargetGroup: jobDef.TargetGroup,
			Status:      models.JobStatusPending,
			Spec:        jobDef.Spec,
			// A new instance starts on its first attempt (F-04); the retry
			// budget and ignore-failed flag are denormalized from the
			// definition's spec, mirroring CreateJob.
			Attempt:   1,
			RunParams: runParams,
		}
		if jobDef.Spec.Retry != nil {
			job.MaxAttempts = jobDef.Spec.Retry.MaxAttempts
		}
		job.IgnoreFailed = jobDef.Spec.IgnoreFailed
		// The instance's step-barrier flag is denormalized from its
		// definition's spec, mirroring CreateJob.
		job.StepBarrier = jobDef.Spec.StepBarrier
		// The instance's failure mode is denormalized from its definition's
		// spec; when the spec sets none it inherits the definition's default
		// (defaulting to FailureModeAll), mirroring CreateJob.
		job.FailureMode = jobDef.Spec.FailureMode
		if job.FailureMode == "" {
			job.FailureMode = def.failureMode
		}
		if job.FailureMode == "" {
			job.FailureMode = models.FailureModeAll
		}
		// The instance's trigger origin and upstream claims are denormalized
		// from the run (F-09): the API stamps them (the trigger name/type and
		// the upstream OIDC claims, prefixed with upstream_) onto the job
		// token it hands to the execution target.
		job.TriggerName = run.TriggerName
		job.TriggerType = run.Trigger
		job.UpstreamClaims = run.UpstreamClaims
		if err := tx.Create(job).Error; err != nil {
			return nil, nil, err
		}
		if jobDef.ID != 0 {
			defToInstance[jobDef.ID] = job.ID
		}
		if job.Key != "" {
			keyToInstanceID[job.Key] = job.ID
		}
		instances = append(instances, *job)
	}
	// Pass 2: remap each instance's dependencies to the run's own instance
	// ids. For the current version a definition's depends_on (already resolved
	// from its needs at save time, F-08) are remapped from the definition ids
	// to the run's instance ids. For a version snapshot the needs (keys) are
	// resolved to the run's instance ids via the snapshot's key->id map. A
	// dependency that cannot be resolved is left as-is (best-effort); in
	// practice a pipeline's job dependencies reference other jobs in the same
	// pipeline.
	for i := range instances {
		remapped := remapRunDependencies(def, i, defToInstance, keyToInstanceID)
		if len(remapped) == 0 {
			continue
		}
		if err := tx.Model(&models.Job{}).
			Where("id = ?", instances[i].ID).
			Select("DependsOn").
			Updates(&models.Job{DependsOn: remapped}).Error; err != nil {
			return nil, nil, err
		}
		instances[i].DependsOn = remapped
	}
	return run, instances, nil
}

// runDefinitionFor selects the definition a run's job instances are created
// from (F-11). A run against the current version (PipelineVersion 0) uses the
// pipeline's current job definitions (run_id IS NULL) and the pipeline's
// failure mode. A run against a specific version uses that version's snapshot:
// its job definitions (converted to the Job model, with needs carried in
// needsByIndex so they can be remapped to the run's instance ids) and the
// snapshot's failure mode. A version that does not exist is an error (a run
// can only be created against a version that was recorded).
func (s *Server) runDefinitionFor(tx *gorm.DB, pipeline *models.Pipeline, requestedVersion int) (*runDefinition, error) {
	if requestedVersion <= 0 {
		// A run against the current version: use the pipeline's current job
		// definitions (the definitions, run_id IS NULL; a pipeline's job
		// instances from prior runs are excluded so they are not mistaken for
		// definitions).
		var defs []models.Job
		if err := tx.Where("pipeline_id = ? AND run_id IS NULL", pipeline.ID).Order("id").Find(&defs).Error; err != nil {
			return nil, err
		}
		return &runDefinition{jobs: defs, failureMode: pipeline.FailureMode, params: pipeline.Params}, nil
	}
	// A run against a specific version: use that version's snapshot.
	var version models.PipelineVersion
	if err := tx.Where("pipeline_id = ? AND version = ?", pipeline.ID, requestedVersion).
		First(&version).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Errorf(codes.InvalidArgument, "pipeline %d has no version %d", pipeline.ID, requestedVersion)
		}
		return nil, err
	}
	jobs := make([]models.Job, 0, len(version.Jobs))
	needsByIndex := make([][]string, len(version.Jobs))
	for i := range version.Jobs {
		snapshot := &version.Jobs[i]
		jobs = append(jobs, models.Job{
			Key:         snapshot.Key,
			Name:        snapshot.Name,
			TargetGroup: snapshot.TargetGroup,
			Spec:        snapshot.Spec,
		})
		needsByIndex[i] = snapshot.Needs
	}
	return &runDefinition{jobs: jobs, failureMode: version.FailureMode, params: version.Params, needsByIndex: needsByIndex}, nil
}

// remapRunDependencies computes a run instance's depends_on (the ids of the
// run's own job instances it depends on) from the run's definition (F-11). For
// the current version the instance's DependsOn (already resolved from its
// needs at save time, F-08) are remapped from the definition ids to the run's
// instance ids. For a version snapshot the needs (keys) are resolved to the
// run's instance ids via the snapshot's key->id map. A dependency that cannot
// be resolved is dropped (best-effort); in practice a pipeline's job
// dependencies reference other jobs in the same pipeline.
func remapRunDependencies(def *runDefinition, index int, defToInstance map[uint]uint, keyToInstanceID map[string]uint) []uint {
	if def.needsByIndex != nil {
		// A version snapshot: resolve the needs (keys) to the run's instance
		// ids via the key->instance-id map built in pass 1.
		return resolveNeedsToDependsOn(def.needsByIndex[index], keyToInstanceID)
	}
	// The current version: remap the definition's depends_on (definition ids,
	// already resolved from its needs at save time, F-08) to the run's
	// instance ids.
	remapped := make([]uint, 0, len(def.jobs[index].DependsOn))
	for _, defDepID := range def.jobs[index].DependsOn {
		if instID, ok := defToInstance[defDepID]; ok {
			remapped = append(remapped, instID)
		} else {
			remapped = append(remapped, defDepID)
		}
	}
	return remapped
}

// TriggerRun atomically claims a trigger-fired run (F-09): it creates a
// PipelineRun (started by the named trigger of the pipeline) and one job
// instance per job definition, exactly like CreateRun, but only if the
// trigger has not already started a run for the same fire window. The dedup
// is what keeps a trigger from firing twice for the same window when the
// scheduler is restarted or two replicas race:
//
//   - a cron trigger passes its cron period as the dedup window: a run
//     started by the same trigger within that window suppresses a duplicate;
//   - an event trigger records the source run's id: the same source run can
//     never start the same downstream run twice.
//
// The check and the create run in a single transaction that locks the
// pipeline row, so two concurrent claims are serialized: the first creates the
// run, the second sees it and is a no-op. It returns the run that claimed the
// window (created by this call, or by an earlier call) and whether this call
// created it.
func (s *Server) TriggerRun(ctx context.Context, req *dbpb.TriggerRunRequest) (*dbpb.TriggerRunResponse, error) {
	if req.GetPipelineId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "pipeline_id is required")
	}
	if req.GetTriggerName() == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_name is required")
	}
	pipelineID := uint(req.GetPipelineId())
	response := &dbpb.TriggerRunResponse{}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Lock the pipeline row so concurrent claims of the same trigger are
		// serialized: the first creates the run, the rest see it and no-op.
		var pipeline models.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&pipeline, pipelineID).Error; err != nil {
			return err
		}
		// Deduplicate against a run the same trigger already started for this
		// fire window.
		if existing, created, err := s.findDuplicateTriggerRun(tx, pipelineID, req); err != nil {
			return err
		} else if created {
			// An earlier claim already started this run; this call is a no-op.
			response.Run = toProtoRun(existing)
			response.Created = false
			return nil
		}
		// No duplicate: create the run and its job instances. A trigger-fired
		// run always executes the pipeline's current version (F-11), which is
		// recorded on the run.
		sourceRunID := sourceRunIDFromParams(req.GetParams())
		run := &models.PipelineRun{
			PipelineID:      pipelineID,
			PipelineVersion: pipeline.Version,
			Status:          models.RunStatusPending,
			Trigger:         triggerSourceFromType(req.GetTriggerType()),
			TriggerName:     req.GetTriggerName(),
			SourceRunID:     sourceRunID,
			Params:          req.GetParams(),
			UpstreamClaims:  upstreamClaimsFromStruct(req.GetUpstreamClaims()),
		}
		createdRun, instances, err := s.createRunAndInstances(tx, &pipeline, run, 0)
		if err != nil {
			return err
		}
		response.Run = toProtoRun(createdRun)
		response.Created = true
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

// findDuplicateTriggerRun reports whether a run the same trigger already
// started for this fire window exists (F-09). It returns the existing run and
// true when a duplicate is found (the caller should no-op), or nil and false
// when the trigger may create a run. A cron trigger (a dedup window is set)
// matches a run started by the same trigger within the window; an event
// trigger (no window) matches a run started by the same source run.
func (s *Server) findDuplicateTriggerRun(tx *gorm.DB, pipelineID uint, req *dbpb.TriggerRunRequest) (*models.PipelineRun, bool, error) {
	query := tx.Model(&models.PipelineRun{}).
		Where("pipeline_id = ? AND trigger_name = ?", pipelineID, req.GetTriggerName())
	if window := req.GetDedupWindow().AsDuration(); window > 0 {
		// A cron trigger: a run started by the same trigger within the window
		// (its cron period) suppresses a duplicate.
		since := time.Now().Add(-window)
		query = query.Where("created_at >= ?", since)
	} else if sourceRunID := sourceRunIDFromParams(req.GetParams()); sourceRunID != nil {
		// An event trigger: the same source run can never start the same
		// downstream run twice.
		query = query.Where("source_run_id = ?", *sourceRunID)
	}
	var existing []models.PipelineRun
	if err := query.Order("id DESC").Limit(1).Find(&existing).Error; err != nil {
		return nil, false, err
	}
	if len(existing) == 0 {
		return nil, false, nil
	}
	return &existing[0], true, nil
}

// sourceRunIDFromParams extracts the source run's id (an event trigger, F-09)
// from a run's params, or nil when absent.
func sourceRunIDFromParams(params map[string]string) *uint {
	raw, ok := params[models.ParamKeySourceRun]
	if !ok || raw == "" {
		return nil
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nil
	}
	value := uint(id)
	return &value
}

// triggerSourceFromType maps a trigger's kind to the run's trigger source
// (F-09): the value recorded on the run's Trigger field.
func triggerSourceFromType(t dbpb.TriggerType) string {
	switch t {
	case dbpb.TriggerType_TRIGGER_TYPE_CRON:
		return models.TriggerSourceCron
	case dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK:
		return models.TriggerSourceWebhook
	case dbpb.TriggerType_TRIGGER_TYPE_EVENT:
		return models.TriggerSourceEvent
	default:
		return models.TriggerSourceManual
	}
}

// GetRun fetches a pipeline run from the Database service.
func (s *Server) GetRun(ctx context.Context, req *dbpb.GetRunRequest) (*dbpb.PipelineRun, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "run id is required")
	}
	var run models.PipelineRun
	if err := s.db.WithContext(ctx).Preload("Pipeline").First(&run, req.GetId()).Error; err != nil {
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
	if finishedAfter := req.GetFinishedAfter(); finishedAfter != nil {
		// A run that has not finished (finished_at is null) is excluded.
		query = query.Where("finished_at IS NOT NULL AND finished_at >= ?", finishedAfter.AsTime())
	}
	// Preload each run's pipeline so the response carries the pipeline's name
	// (F-09): the scheduler's event loop matches a finished run against the
	// pipelines its event triggers watch by name, without fetching every
	// pipeline.
	var runs []models.PipelineRun
	if err := query.Preload("Pipeline").Order("id").Find(&runs).Error; err != nil {
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
// Pipeline versions (F-11)
// ---------------------------------------------------------------------------

// ListPipelineVersions returns a pipeline's version history (F-11), most recent
// first. Each version is an immutable snapshot of the pipeline's definition at
// that version, so a run bound to a version can be inspected (or re-executed)
// against the exact definition it ran.
func (s *Server) ListPipelineVersions(ctx context.Context, req *dbpb.ListPipelineVersionsRequest) (*dbpb.ListPipelineVersionsResponse, error) {
	if req.GetPipelineId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "pipeline_id is required")
	}
	var versions []models.PipelineVersion
	if err := s.db.WithContext(ctx).
		Where("pipeline_id = ?", req.GetPipelineId()).
		Order("version DESC").
		Find(&versions).Error; err != nil {
		return nil, grpcErr(err)
	}
	response := &dbpb.ListPipelineVersionsResponse{}
	for i := range versions {
		response.Versions = append(response.Versions, toProtoPipelineVersion(&versions[i]))
	}
	return response, nil
}

// GetPipelineVersion returns the immutable snapshot of one version of a
// pipeline's definition (F-11). A version that does not exist is NotFound.
func (s *Server) GetPipelineVersion(ctx context.Context, req *dbpb.GetPipelineVersionRequest) (*dbpb.PipelineVersion, error) {
	if req.GetPipelineId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "pipeline_id is required")
	}
	if req.GetVersion() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "version is required")
	}
	var version models.PipelineVersion
	if err := s.db.WithContext(ctx).
		Where("pipeline_id = ? AND version = ?", req.GetPipelineId(), req.GetVersion()).
		First(&version).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoPipelineVersion(&version), nil
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
		Key:         req.GetKey(),
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
	// The job's step-barrier flag is denormalized from the spec so the
	// execution target can tell from the job alone whether to synchronize its
	// workers at step boundaries.
	job.StepBarrier = spec.StepBarrier
	// The job's failure mode is denormalized from the spec; when the spec sets
	// none it inherits the pipeline's default (defaulting to FailureModeAll).
	job.FailureMode = spec.FailureMode
	if req.GetPipelineId() > 0 {
		pipelineID := uint(req.GetPipelineId())
		job.PipelineID = &pipelineID
		var pipeline models.Pipeline
		if err := s.db.WithContext(ctx).First(&pipeline, pipelineID).Error; err != nil {
			return nil, grpcErr(err)
		}
		if job.FailureMode == "" {
			job.FailureMode = pipeline.FailureMode
		}
		// Adding a job definition to a pipeline changes the pipeline's DAG
		// (F-08): the new job's key must be unique and its needs must reference
		// existing keys without forming a cycle. Validate the pipeline's
		// existing job definitions together with the new job before persisting
		// anything, so a pipeline is never left in an unrunnable state. A job
		// created directly under a run (run_id set) is not a definition and is
		// not part of the pipeline's DAG, so it is not validated here.
		if req.GetRunId() == 0 {
			var existing []models.Job
			if err := s.db.WithContext(ctx).
				Where("pipeline_id = ? AND run_id IS NULL", pipelineID).
				Order("id").Find(&existing).Error; err != nil {
				return nil, grpcErr(err)
			}
			// Reconstruct each existing definition's needs (keys) from its
			// depends_on (ids) so the DAG can be validated (F-08): the backend
			// stores dependencies as ids, and needs is only the authoring form.
			idToKey := pipelineKeyMap(existing)
			existingDefs := make([]*dbpb.JobDefinition, 0, len(existing)+1)
			for i := range existing {
				existingDefs = append(existingDefs, &dbpb.JobDefinition{
					Key:   existing[i].Key,
					Name:  existing[i].Name,
					Needs: needsFromDependsOn(existing[i].DependsOn, idToKey),
				})
			}
			newDef := &dbpb.JobDefinition{Key: job.Key, Name: job.Name, Needs: req.GetNeeds()}
			if err := validateJobDAG(append(existingDefs, newDef)); err != nil {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
			// Resolve the new job's needs (keys) to the ids of the jobs they
			// reference and append the result to depends_on (F-08). The job's
			// raw depends_on (F-06, set from the request) is preserved: a job
			// may carry both raw-id dependencies and key-based needs.
			if resolved := resolveNeedsToDependsOn(req.GetNeeds(), keyToIDMap(existing)); len(resolved) > 0 {
				job.DependsOn = append(job.DependsOn, resolved...)
			}
		}
	}
	if job.FailureMode == "" {
		job.FailureMode = models.FailureModeAll
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

// StartJobExecution records that the calling worker has started running a job
// (fan-out): it creates the worker's JobExecution for the job's current
// attempt and marks the job running. A job that targets a worker group runs on
// every worker in the group; each worker's run is a separate execution, so the
// start is not exclusive (unlike the old single-claim work queue).
//
// The start is rejected (started=false) when the job is no longer pending — it
// reached a terminal state, or it is not targeted at the worker's group — so a
// job that finished before a slow worker started it is not run. The execution
// row is created only when the job is still pending and the job is marked
// running, so a job that is already running (started by another worker) still
// gets a fresh execution row for this worker (fan-out), but a job that is
// terminal does not.
func (s *Server) StartJobExecution(ctx context.Context, req *dbpb.StartJobExecutionRequest) (*dbpb.StartJobExecutionResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if req.GetWorkerName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_name is required")
	}
	db := s.db.WithContext(ctx)
	var job models.Job
	if err := db.First(&job, req.GetJobId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	// A job that has reached a terminal state is not started. A job that is
	// still pending or running is started: for fan-out, several workers in the
	// group each start their own execution, so the start is allowed even after
	// the first worker has marked the job running.
	if isTerminalJobStatus(job.Status) {
		return &dbpb.StartJobExecutionResponse{Started: false}, nil
	}
	if job.TargetGroup == "" {
		// A job with an empty target group runs on an ephemeral agent, not on
		// a long-lived worker; a worker starting it is a no-op.
		return &dbpb.StartJobExecutionResponse{Started: false}, nil
	}
	startedAt := time.Now()
	if timestamp := req.GetStartedAt(); timestamp != nil {
		startedAt = timestamp.AsTime()
	}
	execution := &models.JobExecution{
		JobID:      uint(req.GetJobId()),
		WorkerName: req.GetWorkerName(),
		Attempt:    job.Attempt,
		Status:     models.JobStatusRunning,
		StartedAt:  &startedAt,
	}
	if err := db.Create(execution).Error; err != nil {
		return nil, grpcErr(err)
	}
	// Mark the job running (best-effort: it is already running if another
	// worker started it first; the conditional update is a no-op then).
	now := time.Now()
	db.Model(&models.Job{}).
		Where("id = ? AND status = ?", req.GetJobId(), models.JobStatusPending).
		Updates(map[string]any{
			"status":     models.JobStatusRunning,
			"started_at": &startedAt,
			"updated_at": &now,
		})
	return &dbpb.StartJobExecutionResponse{Started: true, Execution: toProtoJobExecution(execution)}, nil
}

// ListJobExecutions returns a job's executions (one per worker that started
// it, for its current attempt). The scheduler's job-status loop uses them to
// derive the job's overall status from the per-worker outcomes (fan-out).
func (s *Server) ListJobExecutions(ctx context.Context, req *dbpb.ListJobExecutionsRequest) (*dbpb.ListJobExecutionsResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	var job models.Job
	if err := s.db.WithContext(ctx).First(&job, req.GetJobId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	var executions []models.JobExecution
	err := s.db.WithContext(ctx).
		Where("job_id = ? AND attempt = ?", req.GetJobId(), job.Attempt).
		Order("id").
		Find(&executions).Error
	if err != nil {
		return nil, grpcErr(err)
	}
	response := &dbpb.ListJobExecutionsResponse{}
	for i := range executions {
		response.Executions = append(response.Executions, toProtoJobExecution(&executions[i]))
	}
	return response, nil
}

// UpdateJobExecution records a worker's outcome for its execution of a job
// (fan-out): it sets the execution's status (and finished timestamp, step
// results, and outputs) for the worker's execution of the job's current
// attempt. The job row's overall status is not touched here — it is derived
// from the executions by the scheduler's job-status loop. A late report for an
// execution that already reached a terminal status is a no-op (the conditional
// update only applies while the execution is running), so a worker that
// finished just as its job was cancelled cannot clobber the terminal status.
func (s *Server) UpdateJobExecution(ctx context.Context, req *dbpb.UpdateJobExecutionRequest) (*dbpb.JobExecution, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if req.GetWorkerName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_name is required")
	}
	if req.GetStatus() == dbpb.JobStatus_JOB_STATUS_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}
	db := s.db.WithContext(ctx)
	var job models.Job
	if err := db.First(&job, req.GetJobId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	modelStatus, err := jobStatusFromProto(req.GetStatus())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	updates := map[string]any{
		"status":     modelStatus,
		"updated_at": time.Now(),
	}
	if timestamp := req.GetFinishedAt(); timestamp != nil {
		instant := timestamp.AsTime()
		updates["finished_at"] = &instant
	}
	// The conditional update applies only while the execution is running, so a
	// late report for an execution that already reached a terminal status is a
	// no-op (mirroring the job-level terminal-status guard, F-05).
	result := db.Model(&models.JobExecution{}).
		Where("job_id = ? AND worker_name = ? AND attempt = ? AND status = ?",
			req.GetJobId(), req.GetWorkerName(), job.Attempt, models.JobStatusRunning).
		Updates(updates)
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	// step_results and outputs (F-06) are informational per-step outcomes, not
	// part of the terminal-status guard: they are persisted whenever the worker
	// reports them (even on the execution's final report). Like the job-level
	// equivalents, they use an explicit Select so the JSON serializer runs.
	if results := req.GetStepResults(); len(results) > 0 {
		if err := db.Model(&models.JobExecution{}).
			Where("job_id = ? AND worker_name = ? AND attempt = ?", req.GetJobId(), req.GetWorkerName(), job.Attempt).
			Select("StepResults").
			Updates(&models.JobExecution{StepResults: stepResultsFromProto(results)}).Error; err != nil {
			return nil, grpcErr(err)
		}
	}
	if outputs := req.GetOutputs(); len(outputs) > 0 {
		if err := db.Model(&models.JobExecution{}).
			Where("job_id = ? AND worker_name = ? AND attempt = ?", req.GetJobId(), req.GetWorkerName(), job.Attempt).
			Select("Outputs").
			Updates(&models.JobExecution{Outputs: outputs}).Error; err != nil {
			return nil, grpcErr(err)
		}
	}
	var execution models.JobExecution
	if err := db.Where("job_id = ? AND worker_name = ? AND attempt = ?", req.GetJobId(), req.GetWorkerName(), job.Attempt).
		First(&execution).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoJobExecution(&execution), nil
}

// AbandonWorkerExecutions marks all of a worker's running executions as failed
// (fan-out). It is called when a worker re-registers after a restart: the
// worker's in-progress executions from before the restart are abandoned (the
// worker will not resume them) so they do not block the job's overall status.
// The job row's overall status is re-derived by the scheduler's job-status
// loop.
func (s *Server) AbandonWorkerExecutions(ctx context.Context, req *dbpb.AbandonWorkerExecutionsRequest) (*emptypb.Empty, error) {
	if req.GetWorkerName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_name is required")
	}
	now := time.Now()
	result := s.db.WithContext(ctx).
		Model(&models.JobExecution{}).
		Where("worker_name = ? AND status = ?", req.GetWorkerName(), models.JobStatusRunning).
		Updates(map[string]any{
			"status":      models.JobStatusFailed,
			"finished_at": &now,
			"updated_at":  now,
		})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	return &emptypb.Empty{}, nil
}

// ReportStepCompletion records that a worker completed a step of a job (the
// cross-worker step barrier). It upserts a StepCompletion row for the job's
// current attempt: a worker that reports the same step twice (e.g. after a
// retry of the same attempt) updates the existing row rather than inserting a
// duplicate. The row is keyed by (job, worker, step, attempt).
func (s *Server) ReportStepCompletion(ctx context.Context, req *dbpb.ReportStepCompletionRequest) (*emptypb.Empty, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if req.GetWorkerName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_name is required")
	}
	db := s.db.WithContext(ctx)
	var job models.Job
	if err := db.First(&job, req.GetJobId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	completion := &models.StepCompletion{
		JobID:      uint(req.GetJobId()),
		WorkerName: req.GetWorkerName(),
		StepIndex:  int(req.GetStepIndex()),
		Attempt:    job.Attempt,
	}
	// Upsert on the composite key: insert the row if it is absent, otherwise
	// refresh its timestamp (the completion is idempotent).
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "job_id"}, {Name: "worker_name"}, {Name: "step_index"}, {Name: "attempt"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"updated_at"}),
	}).Create(completion).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

// CheckStepBarrier reports whether the barrier for a job's step is satisfied
// (every worker alive at the step's start has completed it) or whether the job
// has been cancelled. A worker that is not alive (it has not heartbeated
// within the liveness window) is dropped from the barrier, so a worker that
// dies mid-step cannot wedge the job. The barrier is satisfied when every
// alive worker in the job's group has a StepCompletion row for the job's
// current attempt and the step in question.
func (s *Server) CheckStepBarrier(ctx context.Context, req *dbpb.CheckStepBarrierRequest) (*dbpb.CheckStepBarrierResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	db := s.db.WithContext(ctx)
	var job models.Job
	if err := db.First(&job, req.GetJobId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	// A cancelled job releases the barrier: a waiting worker should stop and
	// report the job cancelled.
	if job.Status == models.JobStatusCancelled {
		return &dbpb.CheckStepBarrierResponse{Cancelled: true}, nil
	}
	// The barrier only applies to a job that fans out to a worker group; a job
	// on a single target (an ephemeral agent) has no barrier to wait on.
	if job.TargetGroup == "" {
		return &dbpb.CheckStepBarrierResponse{Satisfied: true}, nil
	}
	var workers []models.Worker
	if err := db.Where("worker_group = ?", job.TargetGroup).Find(&workers).Error; err != nil {
		return nil, grpcErr(err)
	}
	// A worker is alive when its last_seen_at is within the liveness window.
	now := time.Now()
	alive := make([]string, 0, len(workers))
	for i := range workers {
		lastSeen := workers[i].LastSeenAt
		if lastSeen == nil {
			continue
		}
		if now.Sub(*lastSeen) <= workerAliveThreshold {
			alive = append(alive, workers[i].Name)
		}
	}
	if len(alive) == 0 {
		// No alive worker in the group (e.g. every worker is down). The
		// barrier is vacuously satisfied; the job-status loop and watchdog
		// handle the job's overall status.
		return &dbpb.CheckStepBarrierResponse{Satisfied: true}, nil
	}
	// Collect the set of workers that have completed this step for the job's
	// current attempt.
	var completions []models.StepCompletion
	if err := db.Where("job_id = ? AND step_index = ? AND attempt = ?",
		job.ID, int(req.GetStepIndex()), job.Attempt).Find(&completions).Error; err != nil {
		return nil, grpcErr(err)
	}
	completed := make(map[string]bool, len(completions))
	for i := range completions {
		completed[completions[i].WorkerName] = true
	}
	// The barrier is satisfied when every alive worker has completed the step.
	for _, name := range alive {
		if !completed[name] {
			return &dbpb.CheckStepBarrierResponse{Satisfied: false}, nil
		}
	}
	return &dbpb.CheckStepBarrierResponse{Satisfied: true}, nil
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

// defaultLeaseTTL is the lease's TTL backstop when the caller does not
// specify one: ownership lapses this long after the last renewal even if the
// holder is still heartbeating.
const defaultLeaseTTL = 10 * time.Second

// defaultLeaseHeartbeatTTL is the liveness window when the caller does not
// specify one: a replica may take over the lease once the holder's last
// heartbeat is older than this, even if the TTL backstop has not lapsed.
const defaultLeaseHeartbeatTTL = 30 * time.Second

// workerAliveThreshold is how recent a worker's last_seen_at must be for it to
// be considered alive. A worker that has not heartbeated within this window is
// considered dead and is dropped from a job's step barrier (its missing step
// completion does not block the job). The worker heartbeats every ~10s, so the
// threshold is a few missed heartbeats. It mirrors the scheduler's
// workerAliveThreshold so the step barrier and the job-status loop agree on
// which workers are alive. It is a variable (not a constant) so tests can
// shorten it.
var workerAliveThreshold = 30 * time.Second

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
// the caller already holds it (a renewal, which also refreshes the
// heartbeat). It succeeds if the lease is free, its TTL backstop has lapsed,
// its heartbeat is stale, or it is already held by the caller; it fails if
// another replica holds a live lease.
//
// Ownership and liveness are decoupled: the long TTL backstop keeps the
// holder's ownership stable across transient blips, while the short heartbeat
// window lets a follower take over quickly once the holder stops heartbeating
// (a restarted or wedged leader). The acquire-or-renew is a portable
// compare-and-swap that is safe on both SQLite and PostgreSQL:
//
//  1. A conditional UPDATE claims the row if it is held by the caller, or is
//     held by someone else whose TTL backstop has lapsed or whose heartbeat
//     is stale. It is a single atomic statement, so two replicas racing for a
//     stale lease cannot both match it.
//  2. If the update matched nothing, the row is either absent or held by
//     someone else with a live lease. The caller tries to INSERT; the unique
//     constraint on name means at most one racing inserter wins, and a loser
//     reports that it did not acquire.
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
	heartbeatTTL := req.GetHeartbeatTtl().AsDuration()
	if heartbeatTTL <= 0 {
		heartbeatTTL = defaultLeaseHeartbeatTTL
	}
	now := time.Now()
	expires := now.Add(ttl)
	// A lease held by another replica is stale (and thus take-over-able) when
	// its TTL backstop has lapsed or its last heartbeat is older than the
	// heartbeat window.
	heartbeatCutoff := now.Add(-heartbeatTTL)
	db := s.db.WithContext(ctx)

	// Step 1: claim the row if it is ours, or held by someone else whose TTL
	// backstop has lapsed or whose heartbeat is stale.
	result := db.Model(&models.Lease{}).
		Where("name = ? AND (holder = ? OR expires_at < ? OR last_heartbeat < ?)",
			req.GetName(), req.GetHolder(), now, heartbeatCutoff).
		Updates(map[string]interface{}{
			"holder":         req.GetHolder(),
			"expires_at":     expires,
			"last_heartbeat": now,
		})
	if result.Error != nil {
		return nil, grpcErr(result.Error)
	}
	if result.RowsAffected > 0 {
		return &dbpb.AcquireLeaseResponse{Acquired: true}, nil
	}

	// Step 2: the row is absent or held by someone else with a live lease.
	// Try to insert; a racing inserter loses the unique constraint and reports
	// that it did not acquire.
	if err := db.Create(&models.Lease{Name: req.GetName(), Holder: req.GetHolder(), ExpiresAt: expires, LastHeartbeat: now}).Error; err != nil {
		if isUniqueViolation(err) {
			return &dbpb.AcquireLeaseResponse{Acquired: false}, nil
		}
		return nil, grpcErr(err)
	}
	return &dbpb.AcquireLeaseResponse{Acquired: true}, nil
}

// GetLease returns the current state of the named lease (holder, TTL expiry,
// and last heartbeat) so followers and observers can watch the leader's
// liveness. A lease that has never been acquired is NotFound.
func (s *Server) GetLease(ctx context.Context, req *dbpb.GetLeaseRequest) (*dbpb.Lease, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	var lease models.Lease
	if err := s.db.WithContext(ctx).Where("name = ?", req.GetName()).First(&lease).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &dbpb.Lease{
		Name:          lease.Name,
		Holder:        lease.Holder,
		ExpiresAt:     timestamppb.New(lease.ExpiresAt),
		LastHeartbeat: timestamppb.New(lease.LastHeartbeat),
	}, nil
}

// ReleaseLease releases the named lease if the caller holds it. Releasing a
// lease the caller does not hold is a no-op, so a replica that has already
// lost the lease can still call this on shutdown without error. The row is
// hard-deleted (Unscoped) so the lease's name is freed and another replica
// can re-acquire it: a soft delete would leave the row occupying the unique
// name index and block the next acquire's insert.
func (s *Server) ReleaseLease(ctx context.Context, req *dbpb.ReleaseLeaseRequest) (*emptypb.Empty, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if req.GetHolder() == "" {
		return nil, status.Error(codes.InvalidArgument, "holder is required")
	}
	result := s.db.WithContext(ctx).
		Unscoped().
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

// validateJobDAG checks a set of job definitions (F-08) for a well-formed DAG
// over the jobs that participate in it (those with a non-empty key). A job
// with a key must have a unique key, and its needs must reference keys that
// are present; the keyed subgraph must have no cycle. A job with no key is
// not part of the needs-based DAG and is ignored — except that a job that
// declares needs must have a key (its needs reference other jobs by key, so a
// keyless job could never be referenced). It returns a descriptive error
// naming the offending jobs, or nil when the graph is valid.
func validateJobDAG(defs []*dbpb.JobDefinition) error {
	keyed := make(map[string]*dbpb.JobDefinition, len(defs))
	for _, def := range defs {
		if def.GetKey() == "" {
			if len(def.GetNeeds()) > 0 {
				return fmt.Errorf("job %q declares needs but has no key", def.GetName())
			}
			continue
		}
		if _, dup := keyed[def.GetKey()]; dup {
			return fmt.Errorf("duplicate job key %q", def.GetKey())
		}
		keyed[def.GetKey()] = def
	}
	indeg := make(map[string]int, len(keyed))
	adj := make(map[string][]string, len(keyed))
	for key := range keyed {
		indeg[key] = 0
	}
	for key, def := range keyed {
		for _, need := range def.GetNeeds() {
			if _, ok := keyed[need]; !ok {
				return fmt.Errorf("job %q depends on unknown key %q", key, need)
			}
			indeg[key]++
			adj[need] = append(adj[need], key)
		}
	}
	// Kahn's algorithm: a cycle leaves at least one node with a non-zero
	// in-degree, so the number of nodes drained is less than the total.
	queue := make([]string, 0, len(keyed))
	for key, d := range indeg {
		if d == 0 {
			queue = append(queue, key)
		}
	}
	processed := 0
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		processed++
		for _, next := range adj[key] {
			indeg[next]--
			if indeg[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if processed != len(keyed) {
		cycle := make([]string, 0, len(keyed)-processed)
		for key, d := range indeg {
			if d > 0 {
				cycle = append(cycle, key)
			}
		}
		return fmt.Errorf("job dependencies contain a cycle involving: %v", cycle)
	}
	return nil
}

// validateTriggers checks a set of trigger definitions (F-09) for well-formed
// triggers: names are non-empty and unique within the pipeline, and each
// trigger carries the fields its kind requires (a cron trigger a cron
// expression that parses, an event trigger a watched pipeline and status). A
// webhook trigger's secret is optional: when set it is enforced on the
// webhook endpoint, when empty the trigger is open. It returns a descriptive
// error naming the offending trigger, or nil when the set is valid.
func validateTriggers(triggers []*dbpb.Trigger) error {
	seen := make(map[string]struct{}, len(triggers))
	for _, trigger := range triggers {
		name := trigger.GetName()
		if name == "" {
			return fmt.Errorf("trigger has an empty name")
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("duplicate trigger name %q", name)
		}
		seen[name] = struct{}{}
		switch trigger.GetType() {
		case dbpb.TriggerType_TRIGGER_TYPE_UNSPECIFIED:
			return fmt.Errorf("trigger %q has no type", name)
		case dbpb.TriggerType_TRIGGER_TYPE_CRON:
			if trigger.GetCron() == "" {
				return fmt.Errorf("cron trigger %q has no cron expression", name)
			}
			if _, err := cron.ParseStandard(trigger.GetCron()); err != nil {
				return fmt.Errorf("cron trigger %q has an invalid cron expression %q: %v", name, trigger.GetCron(), err)
			}
		case dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK:
			// A webhook trigger's secret is optional: when set it is enforced
			// on the webhook endpoint, when empty the trigger is open (any
			// POST starts a run). Its OIDC issuer and claims are also
			// optional: when the issuer is set the API verifies the caller's
			// Bearer token against it and matches its claims against
			// oidc_claims (a value of "*" is a wildcard).
			for claim := range trigger.GetOidcClaims() {
				if claim == "" {
					return fmt.Errorf("webhook trigger %q has an oidc_claims entry with an empty claim name", name)
				}
			}
		case dbpb.TriggerType_TRIGGER_TYPE_EVENT:
			if trigger.GetEventPipeline() == "" {
				return fmt.Errorf("event trigger %q has no event_pipeline", name)
			}
			if trigger.GetEventStatus() == dbpb.RunStatus_RUN_STATUS_UNSPECIFIED {
				return fmt.Errorf("event trigger %q has no event_status", name)
			}
		default:
			return fmt.Errorf("trigger %q has unknown type %v", name, trigger.GetType())
		}
	}
	return nil
}

// triggersFromProto converts a list of proto triggers into the model's
// triggers (F-09). An empty list yields nil, so an update with no triggers
// leaves the pipeline's triggers unchanged.
func triggersFromProto(triggers []*dbpb.Trigger) []models.Trigger {
	if len(triggers) == 0 {
		return nil
	}
	out := make([]models.Trigger, 0, len(triggers))
	for _, trigger := range triggers {
		var eventStatus models.RunStatus
		if trigger.GetEventStatus() != dbpb.RunStatus_RUN_STATUS_UNSPECIFIED {
			eventStatus, _ = runStatusFromProto(trigger.GetEventStatus())
		}
		out = append(out, models.Trigger{
			Name:          trigger.GetName(),
			Type:          triggerTypeFromProto(trigger.GetType()),
			Cron:          trigger.GetCron(),
			Secret:        trigger.GetSecret(),
			EventPipeline: trigger.GetEventPipeline(),
			EventStatus:   eventStatus,
			Params:        trigger.GetParams(),
			OIDCIssuer:    trigger.GetOidcIssuer(),
			OIDCClaims:    trigger.GetOidcClaims(),
		})
	}
	return out
}

// triggersToProto converts the model's triggers into a list of proto triggers
// (F-09). An empty slice yields nil.
func triggersToProto(triggers []models.Trigger) []*dbpb.Trigger {
	if len(triggers) == 0 {
		return nil
	}
	out := make([]*dbpb.Trigger, 0, len(triggers))
	for i := range triggers {
		out = append(out, &dbpb.Trigger{
			Name:          triggers[i].Name,
			Type:          triggerTypeToProto(triggers[i].Type),
			Cron:          triggers[i].Cron,
			Secret:        triggers[i].Secret,
			EventPipeline: triggers[i].EventPipeline,
			EventStatus:   runStatusToProto(triggers[i].EventStatus),
			Params:        triggers[i].Params,
			OidcIssuer:    triggers[i].OIDCIssuer,
			OidcClaims:    triggers[i].OIDCClaims,
		})
	}
	return out
}

// triggerTypeFromProto converts a proto TriggerType to the model's
// TriggerType (F-09).
func triggerTypeFromProto(t dbpb.TriggerType) models.TriggerType {
	switch t {
	case dbpb.TriggerType_TRIGGER_TYPE_CRON:
		return models.TriggerTypeCron
	case dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK:
		return models.TriggerTypeWebhook
	case dbpb.TriggerType_TRIGGER_TYPE_EVENT:
		return models.TriggerTypeEvent
	default:
		return ""
	}
}

// triggerTypeToProto converts the model's TriggerType to a proto TriggerType
// (F-09).
func triggerTypeToProto(t models.TriggerType) dbpb.TriggerType {
	switch t {
	case models.TriggerTypeCron:
		return dbpb.TriggerType_TRIGGER_TYPE_CRON
	case models.TriggerTypeWebhook:
		return dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK
	case models.TriggerTypeEvent:
		return dbpb.TriggerType_TRIGGER_TYPE_EVENT
	default:
		return dbpb.TriggerType_TRIGGER_TYPE_UNSPECIFIED
	}
}

// validateParams checks a set of parameter declarations (F-10) for
// well-formed parameters: names are non-empty and unique within the pipeline.
// A parameter's default is optional (an empty default means the parameter has
// no default). It returns a descriptive error naming the offending parameter,
// or nil when the set is valid.
func validateParams(params []*dbpb.Parameter) error {
	seen := make(map[string]struct{}, len(params))
	for _, param := range params {
		name := param.GetName()
		if name == "" {
			return fmt.Errorf("parameter has an empty name")
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("duplicate parameter name %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// pipelineParamsFromProto converts a list of proto parameter declarations into
// the model's parameters (F-10). An empty list yields nil, so an update with
// no params leaves the pipeline's parameters unchanged.
func pipelineParamsFromProto(params []*dbpb.Parameter) []models.Parameter {
	if len(params) == 0 {
		return nil
	}
	out := make([]models.Parameter, 0, len(params))
	for _, param := range params {
		out = append(out, models.Parameter{
			Name:        param.GetName(),
			Default:     param.GetDefault(),
			Description: param.GetDescription(),
		})
	}
	return out
}

// pipelineParamsToProto converts the model's parameter declarations into a
// list of proto parameters (F-10). An empty slice yields nil.
func pipelineParamsToProto(params []models.Parameter) []*dbpb.Parameter {
	if len(params) == 0 {
		return nil
	}
	out := make([]*dbpb.Parameter, 0, len(params))
	for i := range params {
		out = append(out, &dbpb.Parameter{
			Name:        params[i].Name,
			Default:     params[i].Default,
			Description: params[i].Description,
		})
	}
	return out
}

// runParamsFor merges a run's supplied parameter values over a pipeline's
// parameter declarations (F-10): a parameter not supplied falls back to its
// default, and a parameter with no supplied value and no default is omitted
// (so a spec field that references it fails to interpolate, rather than
// silently rendering to an empty string). It returns nil when the result is
// empty.
func runParamsFor(params []models.Parameter, supplied map[string]string) map[string]string {
	if len(params) == 0 {
		return nil
	}
	out := make(map[string]string, len(params))
	for _, param := range params {
		if value, ok := supplied[param.Name]; ok {
			out[param.Name] = value
			continue
		}
		if param.Default != "" {
			out[param.Name] = param.Default
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// jobDefinitionsFromProto converts a list of proto job definitions into the
// model's job definitions (F-08). A definition's name defaults to its key when
// empty, and its failure mode defaults to FailureModeAll. The definition's
// needs (keys) are not carried on the model: the caller resolves them to
// depends_on ids (see resolveNeedsToDependsOn) after the jobs are created.
func jobDefinitionsFromProto(defs []*dbpb.JobDefinition) []models.Job {
	jobs := make([]models.Job, 0, len(defs))
	for _, def := range defs {
		name := def.GetName()
		if name == "" {
			name = def.GetKey()
		}
		jobs = append(jobs, models.Job{
			Key:         def.GetKey(),
			Name:        name,
			TargetGroup: def.GetTargetGroup(),
			Spec:        specFromProto(def.GetSpec()),
		})
	}
	return jobs
}

// loadPipelineJobDefinitions loads a pipeline's job definitions (the Job rows
// with run_id IS NULL, F-08) in id order. A pipeline's run instances are
// excluded so they are not mistaken for definitions.
func (s *Server) loadPipelineJobDefinitions(ctx context.Context, pipelineID uint) ([]models.Job, error) {
	var defs []models.Job
	if err := s.db.WithContext(ctx).
		Where("pipeline_id = ? AND run_id IS NULL", pipelineID).
		Order("id").Find(&defs).Error; err != nil {
		return nil, err
	}
	return defs, nil
}

// pipelineKeyMap builds a map from a job's id to its key for a set of job
// definitions (F-08). It is used to resolve a job's needs (keys) to the ids
// of the jobs they reference, and to reconstruct needs from depends_on for
// display.
func pipelineKeyMap(jobs []models.Job) map[uint]string {
	m := make(map[uint]string, len(jobs))
	for i := range jobs {
		if jobs[i].Key != "" {
			m[jobs[i].ID] = jobs[i].Key
		}
	}
	return m
}

// keyToIDMap builds a map from a job's key to its id for a set of jobs (F-08).
// It is used to resolve a job's needs (keys) to the ids of the jobs they
// reference.
func keyToIDMap(jobs []models.Job) map[string]uint {
	m := make(map[string]uint, len(jobs))
	for i := range jobs {
		if jobs[i].Key != "" {
			m[jobs[i].Key] = jobs[i].ID
		}
	}
	return m
}

// resolveNeedsToDependsOn resolves a job's needs (the keys of the jobs it
// depends on, F-08) to the ids of those jobs, using the key->id map. A need
// that cannot be resolved is dropped (best-effort); in practice a pipeline's
// needs reference other jobs in the same pipeline, so every need resolves.
func resolveNeedsToDependsOn(needs []string, keyToID map[string]uint) []uint {
	if len(needs) == 0 {
		return nil
	}
	out := make([]uint, 0, len(needs))
	for _, key := range needs {
		if id, ok := keyToID[key]; ok {
			out = append(out, id)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// needsFromDependsOn reconstructs a job's needs (the keys of the jobs it
// depends on) from its depends_on (ids), using the id->key map. It is used to
// present a job's dependencies as keys (the authoring form, F-08) when the
// backend stores them as ids. A dependency whose key is unknown is dropped.
func needsFromDependsOn(dependsOn []uint, idToKey map[uint]string) []string {
	if len(dependsOn) == 0 {
		return nil
	}
	out := make([]string, 0, len(dependsOn))
	for _, id := range dependsOn {
		if key, ok := idToKey[id]; ok {
			out = append(out, key)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// toProtoPipelineJobDefinitions converts a pipeline's job definitions into the
// proto JobDefinition list carried on the Pipeline message (F-08). Each
// definition's id is populated so a create/update response carries the ids of
// the job rows that were created.
func toProtoPipelineJobDefinitions(jobs []models.Job, keyMap map[uint]string) []*dbpb.JobDefinition {
	defs := make([]*dbpb.JobDefinition, 0, len(jobs))
	for i := range jobs {
		defs = append(defs, &dbpb.JobDefinition{
			Id:          int64(jobs[i].ID),
			Key:         jobs[i].Key,
			Name:        jobs[i].Name,
			TargetGroup: jobs[i].TargetGroup,
			Spec:        specToProto(jobs[i].Spec),
			Needs:       needsFromDependsOn(jobs[i].DependsOn, keyMap),
		})
	}
	return defs
}

func toProtoPipeline(pipeline *models.Pipeline) *dbpb.Pipeline {
	return &dbpb.Pipeline{
		Id:          int64(pipeline.ID),
		Name:        pipeline.Name,
		Description: pipeline.Description,
		CreatedAt:   timestamppb.New(pipeline.CreatedAt),
		UpdatedAt:   timestamppb.New(pipeline.UpdatedAt),
		FailureMode: failureModeToProto(pipeline.FailureMode),
		Jobs:        toProtoPipelineJobDefinitions(pipeline.Jobs, pipelineKeyMap(pipeline.Jobs)),
		Triggers:    triggersToProto(pipeline.Triggers),
		Params:      pipelineParamsToProto(pipeline.Params),
		Version:     int32(pipeline.Version),
	}
}

// toProtoPipelineVersion converts a pipeline version snapshot into its proto
// form (F-11). The snapshot's job definitions are presented in the declarative
// JobDefinition form (key, name, target group, needs, spec), so a version can
// be re-executed or re-saved without loss.
func toProtoPipelineVersion(v *models.PipelineVersion) *dbpb.PipelineVersion {
	return &dbpb.PipelineVersion{
		Id:          int64(v.ID),
		PipelineId:  int64(v.PipelineID),
		Version:     int32(v.Version),
		Name:        v.Name,
		Description: v.Description,
		FailureMode: failureModeToProto(v.FailureMode),
		Jobs:        jobDefinitionsToProto(v.Jobs),
		Triggers:    triggersToProto(v.Triggers),
		Params:      pipelineParamsToProto(v.Params),
		CreatedAt:   timestamppb.New(v.CreatedAt),
	}
}

// jobDefinitionsToProto converts a version snapshot's job definitions into the
// proto JobDefinition list (F-11).
func jobDefinitionsToProto(snapshots []models.JobDefinitionSnapshot) []*dbpb.JobDefinition {
	out := make([]*dbpb.JobDefinition, 0, len(snapshots))
	for i := range snapshots {
		out = append(out, &dbpb.JobDefinition{
			Key:         snapshots[i].Key,
			Name:        snapshots[i].Name,
			TargetGroup: snapshots[i].TargetGroup,
			Needs:       snapshots[i].Needs,
			Spec:        specToProto(snapshots[i].Spec),
		})
	}
	return out
}

// snapshotPipelineVersion builds the immutable snapshot of a pipeline's
// definition at its current version (F-11). The job definitions' needs are
// reconstructed from their depends_on (ids) via the key map, so the snapshot
// carries the authoring form (needs, by key) rather than the stored form
// (depends_on, by id). The snapshot is stored as a PipelineVersion row so a
// run bound to this version can be re-executed (or inspected) without loss.
func snapshotPipelineVersion(pipeline *models.Pipeline, defs []models.Job) *models.PipelineVersion {
	keyMap := pipelineKeyMap(defs)
	snapshots := make([]models.JobDefinitionSnapshot, 0, len(defs))
	for i := range defs {
		snapshots = append(snapshots, models.JobDefinitionSnapshot{
			Key:         defs[i].Key,
			Name:        defs[i].Name,
			TargetGroup: defs[i].TargetGroup,
			Needs:       needsFromDependsOn(defs[i].DependsOn, keyMap),
			Spec:        defs[i].Spec,
		})
	}
	return &models.PipelineVersion{
		PipelineID:  pipeline.ID,
		Version:     pipeline.Version,
		Name:        pipeline.Name,
		Description: pipeline.Description,
		FailureMode: pipeline.FailureMode,
		Jobs:        snapshots,
		Triggers:    pipeline.Triggers,
		Params:      pipeline.Params,
	}
}

// upstreamClaimsToStruct converts a run's or job's upstream claims (a JSON
// object of arbitrary values, e.g. GitLab's nested "user_identities" list or
// "job_config" object) into a protobuf Struct so it can cross the gRPC
// boundary without flattening complex values to strings. A nil or empty map
// yields a nil Struct (the proto's "absent" representation).
func upstreamClaimsToStruct(claims map[string]any) *structpb.Struct {
	if len(claims) == 0 {
		return nil
	}
	s, err := structpb.NewStruct(claims)
	if err != nil {
		// A claim value that is not JSON-encodable (e.g. a time.Time) would
		// fail here; that cannot happen for OIDC claims, which are JSON
		// scalars, objects, or lists. Fall back to an empty struct rather
		// than dropping the run's other fields.
		return &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}
	return s
}

// upstreamClaimsFromStruct converts a protobuf Struct of upstream claims back
// to a JSON-object map (nil for an absent/empty struct). Each field's value is
// decoded to its Go representation (a nested object becomes a map, a list a
// slice, a number a float64), so a complex claim keeps its structure.
func upstreamClaimsFromStruct(s *structpb.Struct) map[string]any {
	if s == nil || len(s.GetFields()) == 0 {
		return nil
	}
	out := make(map[string]any, len(s.GetFields()))
	for k, v := range s.GetFields() {
		out[k] = v.AsInterface()
	}
	return out
}

func toProtoRun(run *models.PipelineRun) *dbpb.PipelineRun {
	proto := &dbpb.PipelineRun{
		Id:              int64(run.ID),
		PipelineId:      int64(run.PipelineID),
		Status:          runStatusToProto(run.Status),
		Trigger:         run.Trigger,
		Params:          run.Params,
		CreatedAt:       timestamppb.New(run.CreatedAt),
		UpdatedAt:       timestamppb.New(run.UpdatedAt),
		TriggerName:     run.TriggerName,
		UpstreamClaims:  upstreamClaimsToStruct(run.UpstreamClaims),
		PipelineVersion: int32(run.PipelineVersion),
	}
	if run.Pipeline != nil {
		// The pipeline's name (F-09): the scheduler's event loop matches a
		// finished run against the pipelines its event triggers watch by name.
		proto.PipelineName = run.Pipeline.Name
	}
	if run.StartedAt != nil {
		proto.StartedAt = timestamppb.New(*run.StartedAt)
	}
	if run.FinishedAt != nil {
		proto.FinishedAt = timestamppb.New(*run.FinishedAt)
	}
	if run.SourceRunID != nil {
		proto.SourceRunId = int64(*run.SourceRunID)
	}
	return proto
}

func toProtoJob(job *models.Job) *dbpb.Job {
	proto := &dbpb.Job{
		Id:             int64(job.ID),
		Name:           job.Name,
		Status:         jobStatusToProto(job.Status),
		TargetGroup:    job.TargetGroup,
		CreatedAt:      timestamppb.New(job.CreatedAt),
		UpdatedAt:      timestamppb.New(job.UpdatedAt),
		Spec:           specToProto(job.Spec),
		Attempt:        int32(job.Attempt),
		MaxAttempts:    int32(job.MaxAttempts),
		DependsOn:      dependsOnToProto(job.DependsOn),
		StepResults:    stepResultsToProto(job.StepResults),
		Outputs:        job.Outputs,
		IgnoreFailed:   job.IgnoreFailed,
		FailureMode:    failureModeToProto(job.FailureMode),
		StepBarrier:    job.StepBarrier,
		Key:            job.Key,
		TriggerName:    job.TriggerName,
		TriggerType:    job.TriggerType,
		UpstreamClaims: upstreamClaimsToStruct(job.UpstreamClaims),
		RunParams:      job.RunParams,
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
		})
	}
	return models.JobSpec{
		Steps:        steps,
		Timeout:      spec.GetTimeout().AsDuration(),
		Retry:        retryPolicyFromProto(spec.GetRetry()),
		IgnoreFailed: spec.GetIgnoreFailed(),
		FailureMode:  failureModeFromProto(spec.GetFailureMode()),
		StepBarrier:  spec.GetStepBarrier(),
	}
}

// specToProto converts the model's JobSpec into a proto JobSpec. A spec with
// no steps, no timeout, and no retry policy yields a nil proto (so it
// round-trips to an empty spec).
func specToProto(spec models.JobSpec) *dbpb.JobSpec {
	if len(spec.Steps) == 0 && spec.Timeout == 0 && spec.Retry == nil && spec.FailureMode == "" && !spec.StepBarrier {
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
		})
	}
	return &dbpb.JobSpec{
		Steps:        steps,
		Timeout:      durationpb.New(spec.Timeout),
		Retry:        retryPolicyToProto(spec.Retry),
		IgnoreFailed: spec.IgnoreFailed,
		FailureMode:  failureModeToProto(spec.FailureMode),
		StepBarrier:  spec.StepBarrier,
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

func toProtoJobExecution(execution *models.JobExecution) *dbpb.JobExecution {
	proto := &dbpb.JobExecution{
		Id:          int64(execution.ID),
		JobId:       int64(execution.JobID),
		WorkerName:  execution.WorkerName,
		Attempt:     int32(execution.Attempt),
		Status:      jobStatusToProto(execution.Status),
		StepResults: stepResultsToProto(execution.StepResults),
		Outputs:     execution.Outputs,
	}
	if execution.StartedAt != nil {
		proto.StartedAt = timestamppb.New(*execution.StartedAt)
	}
	if execution.FinishedAt != nil {
		proto.FinishedAt = timestamppb.New(*execution.FinishedAt)
	}
	return proto
}

// failureModeToProto converts a model FailureMode into its proto form. An
// empty value yields UNSPECIFIED (the caller then applies the default).
func failureModeToProto(mode models.FailureMode) dbpb.FailureMode {
	switch mode {
	case models.FailureModeBestEffort:
		return dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT
	case models.FailureModeAny:
		return dbpb.FailureMode_FAILURE_MODE_ANY
	case models.FailureModeAll:
		return dbpb.FailureMode_FAILURE_MODE_ALL
	default:
		return dbpb.FailureMode_FAILURE_MODE_UNSPECIFIED
	}
}

// failureModeFromProto converts a proto FailureMode into its model form.
// UNSPECIFIED yields "" (the caller then applies the default).
func failureModeFromProto(mode dbpb.FailureMode) models.FailureMode {
	switch mode {
	case dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT:
		return models.FailureModeBestEffort
	case dbpb.FailureMode_FAILURE_MODE_ANY:
		return models.FailureModeAny
	case dbpb.FailureMode_FAILURE_MODE_ALL:
		return models.FailureModeAll
	default:
		return ""
	}
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

// isTerminalJobStatus reports whether a job status is terminal (the job will
// not change again): succeeded, failed, cancelled, timed_out, or skipped.
func isTerminalJobStatus(status models.JobStatus) bool {
	switch status {
	case models.JobStatusSucceeded, models.JobStatusFailed, models.JobStatusCancelled, models.JobStatusTimedOut, models.JobStatusSkipped:
		return true
	default:
		return false
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
