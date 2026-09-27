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
	job := &models.Job{
		Name:        req.GetName(),
		TargetGroup: req.GetTargetGroup(),
		Status:      models.JobStatusPending,
		Spec:        specFromProto(req.GetSpec()),
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

// UpdateJob applies a partial update: a status of JOB_STATUS_UNSPECIFIED
// leaves the status unchanged, and nil timestamps leave the corresponding
// field unchanged.
func (s *Server) UpdateJob(ctx context.Context, req *dbpb.UpdateJobRequest) (*dbpb.Job, error) {
	var job models.Job
	if err := s.db.WithContext(ctx).First(&job, req.GetId()).Error; err != nil {
		return nil, grpcErr(err)
	}
	if jobStatus := req.GetStatus(); jobStatus != dbpb.JobStatus_JOB_STATUS_UNSPECIFIED {
		modelStatus, err := jobStatusFromProto(jobStatus)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		job.Status = modelStatus
	}
	if timestamp := req.GetStartedAt(); timestamp != nil {
		instant := timestamp.AsTime()
		job.StartedAt = &instant
	}
	if timestamp := req.GetFinishedAt(); timestamp != nil {
		instant := timestamp.AsTime()
		job.FinishedAt = &instant
	}
	if err := s.db.WithContext(ctx).Save(&job).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoJob(&job), nil
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

// specFromProto converts a proto JobSpec into the model's JobSpec. A nil
// proto yields a zero-value spec (no steps).
func specFromProto(spec *dbpb.JobSpec) models.JobSpec {
	if spec == nil {
		return models.JobSpec{}
	}
	steps := make([]models.JobStep, 0, len(spec.GetSteps()))
	for _, step := range spec.GetSteps() {
		steps = append(steps, models.JobStep{
			Command: step.GetCommand(),
			Args:    step.GetArgs(),
			Workdir: step.GetWorkdir(),
			Env:     step.GetEnv(),
			Timeout: step.GetTimeout().AsDuration(),
		})
	}
	return models.JobSpec{Steps: steps}
}

// specToProto converts the model's JobSpec into a proto JobSpec. A spec with
// no steps yields a nil proto (so it round-trips to an empty spec).
func specToProto(spec models.JobSpec) *dbpb.JobSpec {
	if len(spec.Steps) == 0 {
		return nil
	}
	steps := make([]*dbpb.JobStep, 0, len(spec.Steps))
	for _, step := range spec.Steps {
		steps = append(steps, &dbpb.JobStep{
			Command: step.Command,
			Args:    step.Args,
			Workdir: step.Workdir,
			Env:     step.Env,
			Timeout: durationpb.New(step.Timeout),
		})
	}
	return &dbpb.JobSpec{Steps: steps}
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
