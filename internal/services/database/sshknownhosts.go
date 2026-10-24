package database

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// ---------------------------------------------------------------------------
// SSH known hosts (F-21.2.4.1)
// ---------------------------------------------------------------------------
//
// Runtime-managed SSH host keys the source-fetcher uses to verify a git-ssh
// server's host key. They live in the database (owned by the Database service)
// and are managed through the API (GET/POST/DELETE /api/ssh-known-hosts), so an
// admin can add a key for a self-hosted or corporate-firewalled git host at
// runtime without a redeployment. The API accumulates them and passes them to
// the fetcher on each git-ssh fetch (FetchSourceRequest.known_hosts); the
// fetcher combines them with its built-in common provider keys.

// toProtoSSHKnownHost renders a stored host key as its Database-service form.
func toProtoSSHKnownHost(h *models.SSHKnownHost) *dbpb.SSHKnownHost {
	return &dbpb.SSHKnownHost{
		Id:          int64(h.ID),
		HostPattern: h.HostPattern,
		KeyType:     h.KeyType,
		Key:         h.Key,
		Description: h.Description,
		CreatedAt:   timestamppb.New(h.CreatedAt),
		UpdatedAt:   timestamppb.New(h.UpdatedAt),
	}
}

// ListSSHKnownHosts returns every stored host key (F-21.2.4.1), ordered by host
// pattern.
func (s *Server) ListSSHKnownHosts(ctx context.Context, req *dbpb.ListSSHKnownHostsRequest) (*dbpb.ListSSHKnownHostsResponse, error) {
	var hosts []models.SSHKnownHost
	if err := s.db.WithContext(ctx).Order("host_pattern").Find(&hosts).Error; err != nil {
		return nil, grpcErr(err)
	}
	resp := &dbpb.ListSSHKnownHostsResponse{}
	for i := range hosts {
		resp.Hosts = append(resp.Hosts, toProtoSSHKnownHost(&hosts[i]))
	}
	return resp, nil
}

// CreateSSHKnownHost adds a host key (F-21.2.4.1). It rejects an empty host
// pattern, an empty key type, an empty key, and a duplicate host pattern (a
// host has one stored key line).
func (s *Server) CreateSSHKnownHost(ctx context.Context, req *dbpb.CreateSSHKnownHostRequest) (*dbpb.SSHKnownHost, error) {
	hostPattern := req.GetHostPattern()
	keyType := req.GetKeyType()
	key := req.GetKey()
	if hostPattern == "" {
		return nil, status.Error(codes.InvalidArgument, "host_pattern is required")
	}
	if keyType == "" {
		return nil, status.Error(codes.InvalidArgument, "key_type is required")
	}
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	var existing models.SSHKnownHost
	if err := s.db.WithContext(ctx).Where("host_pattern = ?", hostPattern).First(&existing).Error; err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "a host key for %q already exists", hostPattern)
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, grpcErr(err)
	}
	host := &models.SSHKnownHost{
		HostPattern: hostPattern,
		KeyType:     keyType,
		Key:         key,
		Description: req.GetDescription(),
	}
	if err := s.db.WithContext(ctx).Create(host).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoSSHKnownHost(host), nil
}

// DeleteSSHKnownHost removes a host key by id (F-21.2.4.1). A missing key
// returns NotFound.
func (s *Server) DeleteSSHKnownHost(ctx context.Context, req *dbpb.DeleteSSHKnownHostRequest) (*emptypb.Empty, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var host models.SSHKnownHost
	if err := s.db.WithContext(ctx).First(&host, uint(req.GetId())).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "ssh known host not found")
		}
		return nil, grpcErr(err)
	}
	if err := s.db.WithContext(ctx).Delete(&host).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}
