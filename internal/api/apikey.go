package api

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"cdrom/internal/auth"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// APIKeyClient is the API's proxy to the IdP's API-key surface (F-25). The
// API is a thin bridge: it forwards the UI's API-key management requests and
// the auth middleware's key verifications to the IdP (where the real logic —
// key generation, hashing, verification, and lockout — lives) and returns the
// IdP's response. The requests go to the IdP over gRPC (mTLS when TLS is
// configured), so only the API can reach them. The pepper, failure threshold,
// and lockout duration are supplied by the API (from its auth.api_key config)
// on each operation.
type APIKeyClient interface {
	// CreateAPIKey generates a new `cdrom-…` API key for the owner (by email),
	// and returns the key's metadata (never the hash) plus the plaintext key
	// (shown to the caller exactly once).
	CreateAPIKey(ctx context.Context, ownerEmail, description, expiresIn string, pipelineScope []uint) (*APIKeyResult, string, error)
	// GetAPIKey returns a key's metadata and prefix (never the plaintext or
	// hash) by key id.
	GetAPIKey(ctx context.Context, id string) (*APIKeyResult, error)
	// ListAPIKeys returns API keys. When ownerID is set only that user's keys
	// are returned; when empty, all keys in the system are returned (an admin
	// listing every key).
	ListAPIKeys(ctx context.Context, ownerID string) ([]APIKeyResult, error)
	// UpdateAPIKey edits a key's description, expiration, and/or pipeline
	// scope without changing its secret (a "renew").
	UpdateAPIKey(ctx context.Context, id string, req UpdateAPIKeyRequest) (*APIKeyResult, error)
	// RotateAPIKey generates a brand-new `cdrom-…` secret for a key, and
	// returns the key's metadata plus the new plaintext key (shown to the
	// caller exactly once); the previous key stops working.
	RotateAPIKey(ctx context.Context, id string) (*APIKeyResult, string, error)
	// DeleteAPIKey removes a key by id.
	DeleteAPIKey(ctx context.Context, id string) error
	// VerifyAPIKey checks a presented `username:apikey` credential against the
	// key directory and, on success, returns the owner's authenticated user
	// (with the owner's roles and the key's pipeline scope). A miss is an
	// error with a gRPC code the API maps to a 401.
	VerifyAPIKey(ctx context.Context, username, apiKey string) (auth.User, error)
	// ResetAPIKeyLockout clears a user's API-key lockout state.
	ResetAPIKeyLockout(ctx context.Context, userID string) error
}

// APIKeyResult is the metadata of an API key (never the plaintext or hash).
type APIKeyResult struct {
	ID            string  `json:"id"`
	OwnerID       string  `json:"owner_id"`
	OwnerEmail    string  `json:"owner_email,omitempty"`
	Description   string  `json:"description"`
	KeyPrefix     string  `json:"key_prefix"`
	ExpiresAt     *string `json:"expires_at,omitempty"`
	PipelineScope []uint  `json:"pipeline_scope,omitempty"`
	CreatedAt     *string `json:"created_at,omitempty"`
}

// UpdateAPIKeyRequest is the request to edit a key's description, expiration,
// and/or pipeline scope without changing its secret (a "renew"). The Has*
// flags indicate which fields are applied (a field whose flag is false is
// left unchanged).
type UpdateAPIKeyRequest struct {
	Description    string `json:"description,omitempty"`
	HasDescription bool   `json:"-"`
	ExpiresIn      string `json:"expires_in,omitempty"`
	HasExpiresAt   bool   `json:"-"`
	PipelineScope  []uint `json:"pipeline_scope,omitempty"`
	HasScope       bool   `json:"-"`
}

// grpcAPIKeyClient is an APIKeyClient that talks to the IdP's gRPC service
// (over mTLS when TLS is configured).
type grpcAPIKeyClient struct {
	idp idppb.IdPClient
	// pepper is the secret mixed into the key's hash (from the API's
	// auth.api_key config); it is passed to the IdP on each key operation.
	pepper string
	// maxFailures is the failure count that triggers an API-key lockout.
	maxFailures int
	// lockoutDuration is how long an API-key lockout lasts (0 = permanent).
	lockoutDuration time.Duration
}

// NewAPIKeyClient returns an APIKeyClient that proxies to the IdP's gRPC
// service (idp). pepper, maxFailures, and lockoutDuration come from the API's
// auth.api_key config and are supplied to the IdP on each operation.
func NewAPIKeyClient(idp idppb.IdPClient, pepper string, maxFailures int, lockoutDuration time.Duration) APIKeyClient {
	return &grpcAPIKeyClient{idp: idp, pepper: pepper, maxFailures: maxFailures, lockoutDuration: lockoutDuration}
}

func (c *grpcAPIKeyClient) CreateAPIKey(ctx context.Context, ownerEmail, description, expiresIn string, pipelineScope []uint) (*APIKeyResult, string, error) {
	resp, err := c.idp.CreateAPIKey(ctx, &idppb.CreateAPIKeyRequest{
		OwnerEmail:    ownerEmail,
		Description:   description,
		ExpiresIn:     expiresIn,
		PipelineScope: uintsTo64(pipelineScope),
		Pepper:        c.pepper,
	})
	if err != nil {
		return nil, "", &apiKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return apiKeyResult(resp.GetKey()), resp.GetPlaintext(), nil
}

func (c *grpcAPIKeyClient) GetAPIKey(ctx context.Context, id string) (*APIKeyResult, error) {
	resp, err := c.idp.GetAPIKey(ctx, &idppb.GetAPIKeyRequest{Id: id})
	if err != nil {
		return nil, &apiKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return apiKeyResult(resp), nil
}

func (c *grpcAPIKeyClient) ListAPIKeys(ctx context.Context, ownerID string) ([]APIKeyResult, error) {
	resp, err := c.idp.ListAPIKeys(ctx, &idppb.ListAPIKeysRequest{OwnerId: ownerID})
	if err != nil {
		return nil, &apiKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	out := make([]APIKeyResult, 0, len(resp.GetKeys()))
	for _, k := range resp.GetKeys() {
		out = append(out, *apiKeyResult(k))
	}
	return out, nil
}

func (c *grpcAPIKeyClient) UpdateAPIKey(ctx context.Context, id string, req UpdateAPIKeyRequest) (*APIKeyResult, error) {
	resp, err := c.idp.UpdateAPIKey(ctx, &idppb.UpdateAPIKeyRequest{
		Id:               id,
		Description:      req.Description,
		ExpiresIn:        req.ExpiresIn,
		PipelineScope:    uintsTo64(req.PipelineScope),
		HasDescription:   req.HasDescription,
		HasExpiresAt:     req.HasExpiresAt,
		HasPipelineScope: req.HasScope,
	})
	if err != nil {
		return nil, &apiKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return apiKeyResult(resp), nil
}

func (c *grpcAPIKeyClient) RotateAPIKey(ctx context.Context, id string) (*APIKeyResult, string, error) {
	resp, err := c.idp.RotateAPIKey(ctx, &idppb.RotateAPIKeyRequest{Id: id, Pepper: c.pepper})
	if err != nil {
		return nil, "", &apiKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return apiKeyResult(resp.GetKey()), resp.GetPlaintext(), nil
}

func (c *grpcAPIKeyClient) DeleteAPIKey(ctx context.Context, id string) error {
	_, err := c.idp.DeleteAPIKey(ctx, &idppb.DeleteAPIKeyRequest{Id: id})
	if err != nil {
		return &apiKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return nil
}

func (c *grpcAPIKeyClient) VerifyAPIKey(ctx context.Context, username, apiKey string) (auth.User, error) {
	resp, err := c.idp.VerifyAPIKey(ctx, &idppb.VerifyAPIKeyRequest{
		Username:        username,
		ApiKey:          apiKey,
		Pepper:          c.pepper,
		MaxFailures:     int32(c.maxFailures),
		LockoutDuration: durationpb.New(c.lockoutDuration),
	})
	if err != nil {
		return auth.User{}, &apiKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	owner := resp.GetOwner()
	name := owner.GetFirstName() + " " + owner.GetLastName()
	if name == "  " {
		name = owner.GetEmail()
	}
	user := auth.User{
		Subject: owner.GetId(),
		Name:    name,
		Email:   owner.GetEmail(),
		Roles:   nonNil(owner.GetRoles()),
	}
	if key := resp.GetKey(); key != nil {
		user.PipelineScope = uintsFrom64(key.GetPipelineScope())
	}
	return user, nil
}

func (c *grpcAPIKeyClient) ResetAPIKeyLockout(ctx context.Context, userID string) error {
	_, err := c.idp.ResetAPIKeyLockout(ctx, &idppb.ResetAPIKeyLockoutRequest{UserId: userID})
	if err != nil {
		return &apiKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return nil
}

// apiKeyResult renders a key's metadata (never the plaintext or hash). The
// owner's email is left empty; the API fills it in when it can (the IdP's
// key message carries only the owner's id).
func apiKeyResult(k *dbpb.IDPAPIKey) *APIKeyResult {
	out := &APIKeyResult{
		ID:            k.GetId(),
		OwnerID:       k.GetOwnerId(),
		Description:   k.GetDescription(),
		KeyPrefix:     k.GetKeyPrefix(),
		PipelineScope: uintsFrom64(k.GetPipelineScope()),
	}
	if ts := k.GetExpiresAt(); ts != nil {
		s := ts.AsTime().UTC().Format(time.RFC3339)
		out.ExpiresAt = &s
	}
	if ts := k.GetCreatedAt(); ts != nil {
		s := ts.AsTime().UTC().Format(time.RFC3339)
		out.CreatedAt = &s
	}
	return out
}

// apiKeyError is an error from the IdP's API-key surface, carrying the gRPC
// status code (so the API can map it to an HTTP status) and the message.
type apiKeyError struct {
	code codes.Code
	msg  string
}

func (e *apiKeyError) Error() string {
	return fmt.Sprintf("idp api key: %s: %s", e.code, e.msg)
}

// apiKeyVerifier adapts an APIKeyClient to the auth.APIKeyVerifier interface
// (F-25): the auth middleware calls Verify for a presented
// `Authorization: Bearer <username>:<apikey>` credential, and it forwards the
// check to the IdP (where the real verification and lockout logic lives).
type apiKeyVerifier struct {
	client APIKeyClient
}

// Verify checks the presented `username:apikey` credential and returns the
// owner's authenticated user (with the owner's roles and the key's pipeline
// scope). A miss is an error.
func (v apiKeyVerifier) Verify(ctx context.Context, username, apiKey string) (auth.User, error) {
	return v.client.VerifyAPIKey(ctx, username, apiKey)
}

// NewAPIKeyVerifier returns an auth.APIKeyVerifier that forwards key
// verifications to the IdP through client (F-25). The API attaches it to the
// auth bundle so the middleware accepts `Authorization: Bearer
// <username>:<apikey>` credentials in place of a JWT.
func NewAPIKeyVerifier(client APIKeyClient) auth.APIKeyVerifier {
	return apiKeyVerifier{client: client}
}

// uintsTo64 converts a uint list to an int64 list (the wire's pipeline-scope
// type).
func uintsTo64(in []uint) []int64 {
	out := make([]int64, len(in))
	for i, v := range in {
		out[i] = int64(v)
	}
	return out
}

// uintsFrom64 converts an int64 list to a uint list (the model's
// pipeline-scope type).
func uintsFrom64(in []int64) []uint {
	out := make([]uint, len(in))
	for i, v := range in {
		out[i] = uint(v)
	}
	return out
}
