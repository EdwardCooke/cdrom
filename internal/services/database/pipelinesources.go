package database

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// ---------------------------------------------------------------------------
// Pipeline sources (F-21.5.1 — minimal slice for F-21.3)
// ---------------------------------------------------------------------------
//
// Where a pipeline is pulled from: a kind (git / http / file), a reference
// (the <type>:<spec> locator, F-21.2), a refresh schedule, and a credential
// name. The source is a database concept configured through the API (it is not
// in the file). This minimal slice supports the source-credential guards
// (F-21.3): attaching a source, reading it back, counting the sources that
// reference a credential (so a credential in use cannot be deleted), and
// recording the last sync's bookkeeping (including last_error, so a
// credential that fails to authenticate is diagnosable).

// pipelineSourceKinds is the set of valid source kinds.
var pipelineSourceKinds = map[string]bool{
	"git":  true,
	"http": true,
	"file": true,
}

// toProtoPipelineSource renders a stored source as its Database-service form.
func toProtoPipelineSource(src *models.PipelineSource) *dbpb.PipelineSource {
	return &dbpb.PipelineSource{
		Id:         int64(src.ID),
		PipelineId: int64(src.PipelineID),
		Kind:       src.Kind,
		Reference:  src.Reference,
		Schedule:   src.Schedule,
		Credential: src.Credential,
		CreatedAt:  timestamppb.New(src.CreatedAt),
		UpdatedAt:  timestamppb.New(src.UpdatedAt),
	}
}

// AttachPipelineSource attaches (or replaces) a source on a pipeline
// (F-21.5.1). It rejects an unknown kind, an empty reference, and a missing
// pipeline.
func (s *Server) AttachPipelineSource(ctx context.Context, req *dbpb.AttachPipelineSourceRequest) (*dbpb.PipelineSource, error) {
	if !pipelineSourceKinds[req.GetKind()] {
		return nil, status.Errorf(codes.InvalidArgument, "kind must be one of git, http, or file (got %q)", req.GetKind())
	}
	if req.GetReference() == "" {
		return nil, status.Error(codes.InvalidArgument, "reference is required")
	}
	db := s.db.WithContext(ctx)
	// The pipeline must exist (a source is attached to a pipeline).
	var pipeline models.Pipeline
	if err := db.First(&pipeline, uint(req.GetPipelineId())).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "pipeline not found")
		}
		return nil, grpcErr(err)
	}
	// One source per pipeline: replace the existing row if there is one.
	var existing models.PipelineSource
	if err := db.Where("pipeline_id = ?", pipeline.ID).First(&existing).Error; err == nil {
		existing.Kind = req.GetKind()
		existing.Reference = req.GetReference()
		existing.Schedule = req.GetSchedule()
		existing.Credential = req.GetCredential()
		if err := db.Save(&existing).Error; err != nil {
			return nil, grpcErr(err)
		}
		return toProtoPipelineSource(&existing), nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, grpcErr(err)
	}
	src := &models.PipelineSource{
		PipelineID: pipeline.ID,
		Kind:       req.GetKind(),
		Reference:  req.GetReference(),
		Schedule:   req.GetSchedule(),
		Credential: req.GetCredential(),
	}
	if err := db.Create(src).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoPipelineSource(src), nil
}

// GetPipelineSource returns a pipeline's source (F-21.5.1). A pipeline with no
// source returns NotFound.
func (s *Server) GetPipelineSource(ctx context.Context, req *dbpb.GetPipelineSourceRequest) (*dbpb.PipelineSource, error) {
	var src models.PipelineSource
	if err := s.db.WithContext(ctx).Where("pipeline_id = ?", req.GetPipelineId()).First(&src).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoPipelineSource(&src), nil
}

// CountSourcesByCredential counts the sources that reference the named
// credential (F-21.3.3's delete guard).
func (s *Server) CountSourcesByCredential(ctx context.Context, req *dbpb.CountSourcesByCredentialRequest) (*dbpb.CountSourcesByCredentialResponse, error) {
	var count int64
	if err := s.db.WithContext(ctx).Model(&models.PipelineSource{}).Where("credential = ?", req.GetCredential()).Count(&count).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &dbpb.CountSourcesByCredentialResponse{Count: count}, nil
}

// SetPipelineSourceState records the last sync's bookkeeping for a pipeline's
// source (F-21.5.1 / F-21.5.2): last_synced_at, last_version, file_hash, and
// last_error (set on a sync failure, cleared on success). It upserts the state
// row (one per pipeline).
func (s *Server) SetPipelineSourceState(ctx context.Context, req *dbpb.SetPipelineSourceStateRequest) (*dbpb.PipelineSourceState, error) {
	db := s.db.WithContext(ctx)
	var state models.PipelineSourceState
	err := db.Where("pipeline_id = ?", req.GetPipelineId()).First(&state).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		state = models.PipelineSourceState{PipelineID: uint(req.GetPipelineId())}
	} else if err != nil {
		return nil, grpcErr(err)
	}
	if ts := req.GetLastSyncedAt(); ts != nil {
		state.LastSyncedAt = ts.AsTime()
	} else {
		state.LastSyncedAt = time.Now().UTC()
	}
	state.LastVersion = req.GetLastVersion()
	state.FileHash = req.GetFileHash()
	state.LastError = req.GetLastError()
	if err := db.Save(&state).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &dbpb.PipelineSourceState{
		PipelineId:   int64(state.PipelineID),
		LastSyncedAt: timestamppb.New(state.LastSyncedAt),
		LastVersion:  state.LastVersion,
		FileHash:     state.FileHash,
		LastError:    state.LastError,
	}, nil
}

// DetachPipelineSource removes a pipeline's source (and its state)
// (F-21.5.1). A pipeline with no source returns NotFound.
func (s *Server) DetachPipelineSource(ctx context.Context, req *dbpb.DetachPipelineSourceRequest) (*emptypb.Empty, error) {
	db := s.db.WithContext(ctx)
	var src models.PipelineSource
	if err := db.Where("pipeline_id = ?", req.GetPipelineId()).First(&src).Error; err != nil {
		return nil, grpcErr(err)
	}
	if err := db.Where("pipeline_id = ?", req.GetPipelineId()).Delete(&models.PipelineSourceState{}).Error; err != nil {
		return nil, grpcErr(err)
	}
	if err := db.Delete(&src).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

// GetPipelineSourceState returns a pipeline's source state (F-21.5.1). A
// pipeline with no recorded state returns NotFound.
func (s *Server) GetPipelineSourceState(ctx context.Context, req *dbpb.GetPipelineSourceStateRequest) (*dbpb.PipelineSourceState, error) {
	var state models.PipelineSourceState
	if err := s.db.WithContext(ctx).Where("pipeline_id = ?", req.GetPipelineId()).First(&state).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &dbpb.PipelineSourceState{
		PipelineId:   int64(state.PipelineID),
		LastSyncedAt: timestamppb.New(state.LastSyncedAt),
		LastVersion:  state.LastVersion,
		FileHash:     state.FileHash,
		LastError:    state.LastError,
	}, nil
}
