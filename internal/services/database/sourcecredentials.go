package database

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// ---------------------------------------------------------------------------
// Source credentials (F-21.3)
// ---------------------------------------------------------------------------
//
// Named, database-stored credentials that authenticate a fetch of a private
// source (a private git repository, or an HTTP/HTTPS endpoint that requires a
// token). They are matched to a source by the longest matching prefix (or an
// explicit credential name on the source), and their secret material is
// encrypted at rest (F-12) when marked sensitive. The Database service stores
// the secret material opaquely (ciphertext or plaintext) and never sees the
// key; the API is the only component that can decrypt it. The source-fetcher
// (F-21.2) holds no credentials at rest — the API decrypts the chosen
// credential for a single fetch and hands the plaintext to the fetcher.

// sourceCredentialKinds is the set of valid credential kinds.
var sourceCredentialKinds = map[string]bool{
	"git-ssh":   true,
	"git-https": true,
	"http":      true,
}

// toProtoSourceCredential renders a stored credential as its Database-service
// form. The secret material is returned as stored (ciphertext or plaintext);
// the API redacts it on read.
func toProtoSourceCredential(c *models.SourceCredential) *dbpb.SourceCredential {
	return &dbpb.SourceCredential{
		Id:          int64(c.ID),
		Name:        c.Name,
		Kind:        c.Kind,
		Prefix:      c.Prefix,
		Description: c.Description,
		Username:    c.Username,
		Token:       c.Token,
		PrivateKey:  c.PrivateKey,
		HostKey:     c.HostKey,
		Headers:     toProtoHeaders(c.Headers),
		CreatedAt:   timestamppb.New(c.CreatedAt),
		UpdatedAt:   timestamppb.New(c.UpdatedAt),
	}
}

// toProtoHeaders converts a stored http header list to its Database-service
// form (the values are returned as stored — ciphertext or plaintext).
func toProtoHeaders(hs []models.HTTPHeader) []*dbpb.HTTPHeader {
	out := make([]*dbpb.HTTPHeader, 0, len(hs))
	for i := range hs {
		out = append(out, &dbpb.HTTPHeader{
			Name:      hs[i].Name,
			Value:     hs[i].Value,
			Sensitive: hs[i].Sensitive,
		})
	}
	return out
}

// fromProtoHeaders converts a Database-service http header list to its stored
// form (the values are stored opaquely — ciphertext or plaintext).
func fromProtoHeaders(hs []*dbpb.HTTPHeader) []models.HTTPHeader {
	out := make([]models.HTTPHeader, 0, len(hs))
	for _, h := range hs {
		out = append(out, models.HTTPHeader{
			Name:      h.GetName(),
			Value:     h.GetValue(),
			Sensitive: h.GetSensitive(),
		})
	}
	return out
}

// validateHeaders rejects an http header list with a duplicate (case-insensitive)
// name or an empty name, so a fetch cannot send two different values for the
// same header.
func validateHeaders(hs []*dbpb.HTTPHeader) error {
	seen := make(map[string]bool, len(hs))
	for _, h := range hs {
		name := strings.TrimSpace(h.GetName())
		if name == "" {
			return errors.New("a header has an empty name")
		}
		key := strings.ToLower(name)
		if seen[key] {
			return errors.New("duplicate header name " + name)
		}
		seen[key] = true
	}
	return nil
}

// ListSourceCredentials returns every stored credential (F-21.3), ordered by
// name.
func (s *Server) ListSourceCredentials(ctx context.Context, req *dbpb.ListSourceCredentialsRequest) (*dbpb.ListSourceCredentialsResponse, error) {
	var creds []models.SourceCredential
	if err := s.db.WithContext(ctx).Order("name").Find(&creds).Error; err != nil {
		return nil, grpcErr(err)
	}
	resp := &dbpb.ListSourceCredentialsResponse{}
	for i := range creds {
		resp.Credentials = append(resp.Credentials, toProtoSourceCredential(&creds[i]))
	}
	return resp, nil
}

// CreateSourceCredential adds a credential (F-21.3). It rejects an empty name,
// an unknown kind, and a duplicate name. The secret material fields carry the
// API-encrypted ciphertext (or plaintext when not marked sensitive); the
// Database service stores them opaquely.
func (s *Server) CreateSourceCredential(ctx context.Context, req *dbpb.CreateSourceCredentialRequest) (*dbpb.SourceCredential, error) {
	name := req.GetName()
	kind := req.GetKind()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if !sourceCredentialKinds[kind] {
		return nil, status.Errorf(codes.InvalidArgument, "kind must be one of git-ssh, git-https, or http (got %q)", kind)
	}
	if err := validateHeaders(req.GetHeaders()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid headers: %v", err)
	}
	var existing models.SourceCredential
	if err := s.db.WithContext(ctx).Where("name = ?", name).First(&existing).Error; err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "a source credential named %q already exists", name)
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, grpcErr(err)
	}
	cred := &models.SourceCredential{
		Name:        name,
		Kind:        kind,
		Prefix:      req.GetPrefix(),
		Description: req.GetDescription(),
		Username:    req.GetUsername(),
		Token:       req.GetToken(),
		PrivateKey:  req.GetPrivateKey(),
		HostKey:     req.GetHostKey(),
		Headers:     fromProtoHeaders(req.GetHeaders()),
	}
	if err := s.db.WithContext(ctx).Create(cred).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoSourceCredential(cred), nil
}

// GetSourceCredential returns the named credential (F-21.3). A missing
// credential returns NotFound.
func (s *Server) GetSourceCredential(ctx context.Context, req *dbpb.GetSourceCredentialRequest) (*dbpb.SourceCredential, error) {
	var cred models.SourceCredential
	if err := s.db.WithContext(ctx).Where("name = ?", req.GetName()).First(&cred).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoSourceCredential(&cred), nil
}

// UpdateSourceCredential edits a credential's prefix, description, and secret
// material (F-21.3). The kind and name are immutable. A missing credential
// returns NotFound.
func (s *Server) UpdateSourceCredential(ctx context.Context, req *dbpb.UpdateSourceCredentialRequest) (*dbpb.SourceCredential, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if err := validateHeaders(req.GetHeaders()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid headers: %v", err)
	}
	db := s.db.WithContext(ctx)
	var cred models.SourceCredential
	if err := db.Where("name = ?", req.GetName()).First(&cred).Error; err != nil {
		return nil, grpcErr(err)
	}
	cred.Prefix = req.GetPrefix()
	cred.Description = req.GetDescription()
	cred.Username = req.GetUsername()
	cred.Token = req.GetToken()
	cred.PrivateKey = req.GetPrivateKey()
	cred.HostKey = req.GetHostKey()
	cred.Headers = fromProtoHeaders(req.GetHeaders())
	if err := db.Save(&cred).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoSourceCredential(&cred), nil
}

// DeleteSourceCredential removes a credential by name (F-21.3). A missing
// credential returns NotFound.
func (s *Server) DeleteSourceCredential(ctx context.Context, req *dbpb.DeleteSourceCredentialRequest) (*emptypb.Empty, error) {
	var cred models.SourceCredential
	if err := s.db.WithContext(ctx).Where("name = ?", req.GetName()).First(&cred).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "source credential not found")
		}
		return nil, grpcErr(err)
	}
	if err := s.db.WithContext(ctx).Delete(&cred).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}
