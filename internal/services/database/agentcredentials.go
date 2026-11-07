// Package database — agent-credential directory (F-30.2).
//
// The Database service OWNS the credential directory: the tracked rows the
// IdP signs JWTs against. The JWT itself never enters the database — its
// signature is the custody proof; the row is the authority for liveness
// (active, unexpired, unrevoked, un-retired). Verification (F-30.3) reduces
// to GetAgentCredential + the row's Active(clock).
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

// CreateAgentCredential stores a newly issued credential. The caller (the
// IdP) chooses the id (which becomes the JWT's jti) and computes the
// deadline; the database assigns created_at. A duplicate id is
// AlreadyExists (the id is the primary key).
func (s *Server) CreateAgentCredential(ctx context.Context, req *dbpb.CreateAgentCredentialRequest) (*dbpb.AgentCredential, error) {
	c := req.GetCredential()
	if c == nil || c.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "credential.id is required")
	}
	if c.GetAgentName() == "" {
		return nil, status.Error(codes.InvalidArgument, "credential.agent_name is required")
	}
	row := &models.AgentCredential{
		ID:          c.GetId(),
		AgentName:   c.GetAgentName(),
		IssuedBy:    c.GetIssuedBy(),
		Description: c.GetDescription(),
	}
	if ts := c.GetExpiresAt(); ts != nil {
		row.ExpiresAt = ts.AsTime()
	}
	db := s.db.WithContext(ctx)
	var existing int64
	if err := db.Model(&models.AgentCredential{}).Where("id = ?", c.GetId()).Count(&existing).Error; err != nil {
		return nil, grpcErr(err)
	}
	if existing > 0 {
		return nil, status.Error(codes.AlreadyExists, "credential id already exists: "+c.GetId())
	}
	if err := db.Create(row).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoAgentCredential(row), nil
}

// GetAgentCredential returns a row by id. A missing row is NotFound.
func (s *Server) GetAgentCredential(ctx context.Context, req *dbpb.GetAgentCredentialRequest) (*dbpb.AgentCredential, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var row models.AgentCredential
	if err := s.db.WithContext(ctx).Where("id = ?", req.GetId()).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "agent credential not found: "+req.GetId())
		}
		return nil, grpcErr(err)
	}
	return toProtoAgentCredential(&row), nil
}

// ListAgentCredentials returns rows ordered by id. When agent_name is set
// only that agent's rows are returned; when empty, all rows in the system.
func (s *Server) ListAgentCredentials(ctx context.Context, req *dbpb.ListAgentCredentialsRequest) (*dbpb.ListAgentCredentialsResponse, error) {
	q := s.db.WithContext(ctx).Order("id")
	if name := req.GetAgentName(); name != "" {
		q = q.Where("agent_name = ?", name)
	}
	var rows []models.AgentCredential
	if err := q.Find(&rows).Error; err != nil {
		return nil, grpcErr(err)
	}
	out := &dbpb.ListAgentCredentialsResponse{Credentials: make([]*dbpb.AgentCredential, 0, len(rows))}
	for i := range rows {
		out.Credentials = append(out.Credentials, toProtoAgentCredential(&rows[i]))
	}
	return out, nil
}

// ExtendAgentCredentialExpiration moves a row's deadline FORWARD to the
// supplied instant (a "renew": the same JWT keeps working — the row is the
// deadline's keeper, not the token). Extending backward is rejected
// (InvalidArgument); a revoked row cannot be revived (FailedPrecondition).
func (s *Server) ExtendAgentCredentialExpiration(ctx context.Context, req *dbpb.ExtendAgentCredentialExpirationRequest) (*dbpb.AgentCredential, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	newExp := req.GetExpiresAt()
	if newExp == nil {
		return nil, status.Error(codes.InvalidArgument, "expires_at is required")
	}
	var row models.AgentCredential
	if err := s.db.WithContext(ctx).Where("id = ?", req.GetId()).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "agent credential not found: "+req.GetId())
		}
		return nil, grpcErr(err)
	}
	if row.RevokedAt != nil || row.ReplacedByID != "" {
		return nil, status.Error(codes.FailedPrecondition, "cannot renew a retired or revoked credential")
	}
	if !row.ExpiresAt.IsZero() && !newExp.AsTime().After(row.ExpiresAt) {
		return nil, status.Error(codes.InvalidArgument, "renew must extend the deadline forward")
	}
	row.ExpiresAt = newExp.AsTime()
	if err := s.db.WithContext(ctx).Save(&row).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoAgentCredential(&row), nil
}

// RetireAgentCredential retires a row (rotation's first half): it chains
// replaced_by_id to the successor and thereby stops governing liveness.
// Retirement is irrevocable and idempotent-friendly: retiring an already
// retired or revoked row is FailedPrecondition (the lifecycle has moved on).
func (s *Server) RetireAgentCredential(ctx context.Context, req *dbpb.RetireAgentCredentialRequest) (*dbpb.AgentCredential, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if req.GetReplacedById() == "" {
		return nil, status.Error(codes.InvalidArgument, "replaced_by_id is required")
	}
	var row models.AgentCredential
	if err := s.db.WithContext(ctx).Where("id = ?", req.GetId()).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "agent credential not found: "+req.GetId())
		}
		return nil, grpcErr(err)
	}
	if row.RevokedAt != nil || row.ReplacedByID != "" {
		return nil, status.Error(codes.FailedPrecondition, "credential is already retired or revoked")
	}
	row.ReplacedByID = req.GetReplacedById()
	if err := s.db.WithContext(ctx).Save(&row).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoAgentCredential(&row), nil
}

// RevokeAgentCredential stamps a row revoked NOW, with a reason. Immediate:
// the row stops governing liveness the moment the write commits. Revoking an
// already-revoked row is idempotent (returns the row); revoking a retired
// row is redundant-but-legal (chains the reason).
func (s *Server) RevokeAgentCredential(ctx context.Context, req *dbpb.RevokeAgentCredentialRequest) (*dbpb.AgentCredential, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var row models.AgentCredential
	if err := s.db.WithContext(ctx).Where("id = ?", req.GetId()).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "agent credential not found: "+req.GetId())
		}
		return nil, grpcErr(err)
	}
	if row.RevokedAt == nil {
		now := time.Now().UTC()
		reason := req.GetReason()
		row.RevokedAt = &now
		row.RevokedReason = reason
		if err := s.db.WithContext(ctx).Save(&row).Error; err != nil {
			return nil, grpcErr(err)
		}
	}
	return toProtoAgentCredential(&row), nil
}

// MarkAgentCredentialSeen bumps last_seen_at to now. Called (throttled) by
// the verifier on a successful presentation (F-30.3). Unknown rows are
// NotFound (the verifier treats that as INVALID, not as a blip).
func (s *Server) MarkAgentCredentialSeen(ctx context.Context, req *dbpb.MarkAgentCredentialSeenRequest) (*emptypb.Empty, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var row models.AgentCredential
	if err := s.db.WithContext(ctx).Where("id = ?", req.GetId()).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "agent credential not found: "+req.GetId())
		}
		return nil, grpcErr(err)
	}
	now := time.Now().UTC()
	row.LastSeenAt = &now
	if err := s.db.WithContext(ctx).Save(&row).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

// toProtoAgentCredential maps a model row to its wire form. Timestamps are
// OMITTED when zero (proto3 omitempty semantics via Has* checks on the
// consumer side).
func toProtoAgentCredential(c *models.AgentCredential) *dbpb.AgentCredential {
	out := &dbpb.AgentCredential{
		Id:            c.ID,
		AgentName:     c.AgentName,
		IssuedBy:      c.IssuedBy,
		Description:   c.Description,
		ReplacedById:  c.ReplacedByID,
		RevokedReason: c.RevokedReason,
	}
	if !c.ExpiresAt.IsZero() {
		out.ExpiresAt = timestamppb.New(c.ExpiresAt)
	}
	if c.RevokedAt != nil {
		out.RevokedAt = timestamppb.New(*c.RevokedAt)
	}
	if c.LastSeenAt != nil {
		out.LastSeenAt = timestamppb.New(*c.LastSeenAt)
	}
	out.CreatedAt = timestamppb.New(c.CreatedAt)
	out.UpdatedAt = timestamppb.New(c.UpdatedAt)
	return out
}
