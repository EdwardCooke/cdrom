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

// ServiceAccountClient is the API's proxy to the IdP's service-account surface
// (F-26). The API is a thin bridge: it forwards the UI's service-account
// management requests and the auth middleware's key verifications to the IdP
// (where the real logic — key generation, salted hashing, verification, and
// lockout — lives) and returns the IdP's response. The requests go to the IdP
// over gRPC (mTLS when TLS is configured), so only the API can reach them. The
// pepper, failure threshold, and lockout duration are supplied by the API (from
// its auth.service_account config) on each operation.
type ServiceAccountClient interface {
	// CreateServiceAccount creates a service account and its two freshly
	// generated key slots (and binds its roles). It returns the account's
	// metadata (never the hashes) plus both plaintext keys (shown to the
	// caller exactly once).
	CreateServiceAccount(ctx context.Context, loginName, displayName, description string, roles []string) (*ServiceAccountResult, []ServiceAccountPlaintextKey, error)
	// GetServiceAccount returns an account's metadata and both slots'
	// non-secret metadata (never the plaintext or hash) by id.
	GetServiceAccount(ctx context.Context, id string) (*ServiceAccountResult, error)
	// ListServiceAccounts returns service accounts. When includeDeleted is
	// false deleted (tombstoned) accounts are excluded.
	ListServiceAccounts(ctx context.Context, includeDeleted bool) ([]ServiceAccountResult, error)
	// UpdateServiceAccount edits an account's display name/description (never
	// its roles, status, or keys). A stale revision is a conflict.
	UpdateServiceAccount(ctx context.Context, id string, req UpdateServiceAccountRequest) (*ServiceAccountResult, error)
	// RotateServiceAccountKey generates a brand-new `cdrom-sa-…` secret for the
	// selected slot (1 or 2) and returns the account's metadata plus the new
	// plaintext key (shown to the caller exactly once); the previous key stops
	// working. A stale revision is a conflict.
	RotateServiceAccountKey(ctx context.Context, id string, slot int, revision int64) (*ServiceAccountResult, *ServiceAccountPlaintextKey, error)
	// DisableServiceAccount temporarily disables an account (rejecting both
	// keys while preserving the hashes and role bindings). A stale revision is
	// a conflict.
	DisableServiceAccount(ctx context.Context, id string, revision int64) (*ServiceAccountResult, error)
	// EnableServiceAccount re-enables a disabled, non-deleted account
	// (restoring both keys with the same keys and current roles). A stale
	// revision is a conflict.
	EnableServiceAccount(ctx context.Context, id string, revision int64) (*ServiceAccountResult, error)
	// DeleteServiceAccount permanently soft-deletes an account (tombstone +
	// zeroed key slots). Repeated delete is idempotent.
	DeleteServiceAccount(ctx context.Context, id string) (*ServiceAccountResult, error)
	// AssignServiceAccountRoles adds role bindings to an account.
	AssignServiceAccountRoles(ctx context.Context, id string, roles []string) (*ServiceAccountResult, error)
	// RemoveServiceAccountRole removes a role binding from an account.
	RemoveServiceAccountRole(ctx context.Context, id, role string) (*ServiceAccountResult, error)
	// VerifyServiceAccountKey checks a presented `login-name:cdrom-sa-…`
	// credential against the account's two key slots and, on success, returns
	// the account's authenticated principal (a service-account kind with the
	// account's roles). A miss is an error with a gRPC code the API maps to a
	// 401.
	VerifyServiceAccountKey(ctx context.Context, loginName, apiKey string) (auth.User, error)
	// ResetServiceAccountLockout clears an account's key lockout state.
	ResetServiceAccountLockout(ctx context.Context, id string) error
}

// ServiceAccountPlaintextKey is a slot number and its plaintext key (returned
// exactly once at creation or rotation).
type ServiceAccountPlaintextKey struct {
	Slot int    `json:"slot"`
	Key  string `json:"key"`
}

// ServiceAccountKeySlot is one of a service account's two key slots (F-26):
// its slot number, a short non-secret prefix, and its generation/rotation
// metadata. It never carries the plaintext or hash.
type ServiceAccountKeySlot struct {
	Slot       int     `json:"slot"`
	KeyPrefix  string  `json:"key_prefix"`
	Generation int     `json:"generation"`
	RotatedAt  *string `json:"rotated_at,omitempty"`
}

// ServiceAccountResult is the metadata of a service account (never the
// plaintext or hash): its identity, editable fields, lifecycle state, roles,
// and its two key slots' non-secret metadata.
type ServiceAccountResult struct {
	ID          string                  `json:"id"`
	LoginName   string                  `json:"login_name"`
	DisplayName string                  `json:"display_name,omitempty"`
	Description string                  `json:"description,omitempty"`
	Disabled    bool                    `json:"disabled"`
	Deleted     bool                    `json:"deleted"`
	DeletedAt   *string                 `json:"deleted_at,omitempty"`
	Roles       []string                `json:"roles"`
	Keys        []ServiceAccountKeySlot `json:"keys"`
	CreatedBy   string                  `json:"created_by,omitempty"`
	UpdatedBy   string                  `json:"updated_by,omitempty"`
	Revision    int64                   `json:"revision"`
	CreatedAt   *string                 `json:"created_at,omitempty"`
	UpdatedAt   *string                 `json:"updated_at,omitempty"`
}

// UpdateServiceAccountRequest is the request to edit an account's display
// name/description (never its roles, status, or keys). The Has* flags indicate
// which fields are applied (a field whose flag is false is left unchanged).
type UpdateServiceAccountRequest struct {
	DisplayName    string `json:"display_name,omitempty"`
	HasDisplayName bool   `json:"-"`
	Description    string `json:"description,omitempty"`
	HasDescription bool   `json:"-"`
	Revision       int64  `json:"revision"`
}

// grpcServiceAccountClient is a ServiceAccountClient that talks to the IdP's
// gRPC service (over mTLS when TLS is configured).
type grpcServiceAccountClient struct {
	idp idppb.IdPClient
	// pepper is the secret mixed into each key's salted hash (from the API's
	// auth.service_account config); it is passed to the IdP on each key
	// operation.
	pepper string
	// maxFailures is the failure count that triggers a service-account lockout.
	maxFailures int
	// lockoutDuration is how long a service-account lockout lasts (0 =
	// permanent).
	lockoutDuration time.Duration
}

// NewServiceAccountClient returns a ServiceAccountClient that proxies to the
// IdP's gRPC service (idp). pepper, maxFailures, and lockoutDuration come from
// the API's auth.service_account config and are supplied to the IdP on each
// operation.
func NewServiceAccountClient(idp idppb.IdPClient, pepper string, maxFailures int, lockoutDuration time.Duration) ServiceAccountClient {
	return &grpcServiceAccountClient{idp: idp, pepper: pepper, maxFailures: maxFailures, lockoutDuration: lockoutDuration}
}

func (c *grpcServiceAccountClient) CreateServiceAccount(ctx context.Context, loginName, displayName, description string, roles []string) (*ServiceAccountResult, []ServiceAccountPlaintextKey, error) {
	resp, err := c.idp.CreateServiceAccount(ctx, &idppb.CreateServiceAccountRequest{
		LoginName:   loginName,
		DisplayName: displayName,
		Description: description,
		Roles:       roles,
		Pepper:      c.pepper,
	})
	if err != nil {
		return nil, nil, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	keys := make([]ServiceAccountPlaintextKey, 0, len(resp.GetKeys()))
	for _, k := range resp.GetKeys() {
		keys = append(keys, ServiceAccountPlaintextKey{Slot: int(k.GetSlot()), Key: k.GetKey()})
	}
	return serviceAccountResult(resp.GetAccount()), keys, nil
}

func (c *grpcServiceAccountClient) GetServiceAccount(ctx context.Context, id string) (*ServiceAccountResult, error) {
	resp, err := c.idp.GetServiceAccount(ctx, &idppb.GetServiceAccountRequest{Id: id})
	if err != nil {
		return nil, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return serviceAccountResult(resp), nil
}

func (c *grpcServiceAccountClient) ListServiceAccounts(ctx context.Context, includeDeleted bool) ([]ServiceAccountResult, error) {
	resp, err := c.idp.ListServiceAccounts(ctx, &idppb.ListServiceAccountsRequest{IncludeDeleted: includeDeleted})
	if err != nil {
		return nil, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	out := make([]ServiceAccountResult, 0, len(resp.GetAccounts()))
	for _, a := range resp.GetAccounts() {
		out = append(out, *serviceAccountResult(a))
	}
	return out, nil
}

func (c *grpcServiceAccountClient) UpdateServiceAccount(ctx context.Context, id string, req UpdateServiceAccountRequest) (*ServiceAccountResult, error) {
	resp, err := c.idp.UpdateServiceAccount(ctx, &idppb.UpdateServiceAccountRequest{
		Id:             id,
		DisplayName:    req.DisplayName,
		Description:    req.Description,
		HasDisplayName: req.HasDisplayName,
		HasDescription: req.HasDescription,
		Revision:       req.Revision,
	})
	if err != nil {
		return nil, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return serviceAccountResult(resp), nil
}

func (c *grpcServiceAccountClient) RotateServiceAccountKey(ctx context.Context, id string, slot int, revision int64) (*ServiceAccountResult, *ServiceAccountPlaintextKey, error) {
	resp, err := c.idp.RotateServiceAccountKey(ctx, &idppb.RotateServiceAccountKeyRequest{
		Id:       id,
		Slot:     int32(slot),
		Pepper:   c.pepper,
		Revision: revision,
	})
	if err != nil {
		return nil, nil, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	var key *ServiceAccountPlaintextKey
	if pk := resp.GetKey(); pk != nil {
		key = &ServiceAccountPlaintextKey{Slot: int(pk.GetSlot()), Key: pk.GetKey()}
	}
	return serviceAccountResult(resp.GetAccount()), key, nil
}

func (c *grpcServiceAccountClient) DisableServiceAccount(ctx context.Context, id string, revision int64) (*ServiceAccountResult, error) {
	resp, err := c.idp.DisableServiceAccount(ctx, &idppb.ServiceAccountStateRequest{Id: id, Revision: revision})
	if err != nil {
		return nil, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return serviceAccountResult(resp), nil
}

func (c *grpcServiceAccountClient) EnableServiceAccount(ctx context.Context, id string, revision int64) (*ServiceAccountResult, error) {
	resp, err := c.idp.EnableServiceAccount(ctx, &idppb.ServiceAccountStateRequest{Id: id, Revision: revision})
	if err != nil {
		return nil, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return serviceAccountResult(resp), nil
}

func (c *grpcServiceAccountClient) DeleteServiceAccount(ctx context.Context, id string) (*ServiceAccountResult, error) {
	resp, err := c.idp.DeleteServiceAccount(ctx, &idppb.DeleteServiceAccountRequest{Id: id})
	if err != nil {
		return nil, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return serviceAccountResult(resp), nil
}

func (c *grpcServiceAccountClient) AssignServiceAccountRoles(ctx context.Context, id string, roles []string) (*ServiceAccountResult, error) {
	resp, err := c.idp.AssignServiceAccountRoles(ctx, &idppb.AssignServiceAccountRolesRequest{Id: id, Roles: roles})
	if err != nil {
		return nil, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return serviceAccountResult(resp), nil
}

func (c *grpcServiceAccountClient) RemoveServiceAccountRole(ctx context.Context, id, role string) (*ServiceAccountResult, error) {
	resp, err := c.idp.RemoveServiceAccountRole(ctx, &idppb.RemoveServiceAccountRoleRequest{Id: id, Role: role})
	if err != nil {
		return nil, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return serviceAccountResult(resp), nil
}

func (c *grpcServiceAccountClient) VerifyServiceAccountKey(ctx context.Context, loginName, apiKey string) (auth.User, error) {
	resp, err := c.idp.VerifyServiceAccountKey(ctx, &idppb.VerifyServiceAccountKeyRequest{
		LoginName:       loginName,
		ApiKey:          apiKey,
		Pepper:          c.pepper,
		MaxFailures:     int32(c.maxFailures),
		LockoutDuration: durationpb.New(c.lockoutDuration),
	})
	if err != nil {
		return auth.User{}, &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	acc := resp.GetAccount()
	name := acc.GetDisplayName()
	if name == "" {
		name = acc.GetLoginName()
	}
	return auth.User{
		Subject: acc.GetId(),
		Kind:    "service-account",
		Name:    name,
		Roles:   nonNil(acc.GetRoles()),
	}, nil
}

func (c *grpcServiceAccountClient) ResetServiceAccountLockout(ctx context.Context, id string) error {
	_, err := c.idp.ResetServiceAccountLockout(ctx, &idppb.ResetServiceAccountLockoutRequest{Id: id})
	if err != nil {
		return &serviceAccountError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return nil
}

// serviceAccountResult renders an account's metadata (never the plaintext or
// hash): its identity, editable fields, lifecycle state, roles, and its two
// key slots' non-secret metadata.
func serviceAccountResult(a *dbpb.ServiceAccount) *ServiceAccountResult {
	out := &ServiceAccountResult{
		ID:          a.GetId(),
		LoginName:   a.GetLoginName(),
		DisplayName: a.GetDisplayName(),
		Description: a.GetDescription(),
		Disabled:    a.GetDisabled(),
		Roles:       nonNil(a.GetRoles()),
		CreatedBy:   a.GetCreatedBy(),
		UpdatedBy:   a.GetUpdatedBy(),
		Revision:    a.GetRevision(),
	}
	if a.GetDeletedAt() != nil {
		out.Deleted = true
		s := a.GetDeletedAt().AsTime().UTC().Format(time.RFC3339)
		out.DeletedAt = &s
	}
	if ts := a.GetCreatedAt(); ts != nil {
		s := ts.AsTime().UTC().Format(time.RFC3339)
		out.CreatedAt = &s
	}
	if ts := a.GetUpdatedAt(); ts != nil {
		s := ts.AsTime().UTC().Format(time.RFC3339)
		out.UpdatedAt = &s
	}
	for _, k := range a.GetKeys() {
		slot := ServiceAccountKeySlot{
			Slot:       int(k.GetSlot()),
			KeyPrefix:  k.GetKeyPrefix(),
			Generation: int(k.GetGeneration()),
		}
		if ts := k.GetRotatedAt(); ts != nil {
			s := ts.AsTime().UTC().Format(time.RFC3339)
			slot.RotatedAt = &s
		}
		out.Keys = append(out.Keys, slot)
	}
	return out
}

// serviceAccountError is an error from the IdP's service-account surface,
// carrying the gRPC status code (so the API can map it to an HTTP status) and
// the message.
type serviceAccountError struct {
	code codes.Code
	msg  string
}

func (e *serviceAccountError) Error() string {
	return fmt.Sprintf("idp service account: %s: %s", e.code, e.msg)
}

// serviceAccountVerifier adapts a ServiceAccountClient to the
// auth.ServiceAccountVerifier interface (F-26): the auth middleware calls
// Verify for a presented `Authorization: Bearer <login-name>:cdrom-sa-…`
// credential, and it forwards the check to the IdP (where the real
// verification and lockout logic lives).
type serviceAccountVerifier struct {
	client ServiceAccountClient
}

// Verify checks the presented `login-name:cdrom-sa-…` credential and returns
// the account's authenticated principal (a service-account kind with the
// account's roles). A miss is an error.
func (v serviceAccountVerifier) Verify(ctx context.Context, loginName, apiKey string) (auth.User, error) {
	return v.client.VerifyServiceAccountKey(ctx, loginName, apiKey)
}

// NewServiceAccountVerifier returns an auth.ServiceAccountVerifier that
// forwards service-account key verifications to the IdP through client (F-26).
// The API attaches it to the auth bundle so the middleware accepts
// `Authorization: Bearer <login-name>:cdrom-sa-…` credentials in place of a
// JWT.
func NewServiceAccountVerifier(client ServiceAccountClient) auth.ServiceAccountVerifier {
	return serviceAccountVerifier{client: client}
}
