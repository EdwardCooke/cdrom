package idp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// Agent credentials (F-30.2). The IdP is the SOLE component that mints the
// JWTs: it signs them with its key ring and tracks their lifecycle in the
// Database service's credential directory. The plaintext JWT is returned
// exactly once (issue / rotate) and is never retrievable afterwards — the
// row (looked up by jti) is the authority for liveness, so revocation is a
// row mutation, not a key ceremony.
//
// Division of labor (the F-25 shape): the store below persists ROWS only
// (never the JWT); the ops layer signs and orchestrates; the gRPC handlers
// in grpc.go are thin adapters.

// agentCredentialJWTPlaceholderLife is the GENEROUS exp placeholder stamped
// on agent-credential JWTs. Nobody enforces it (the API gates on the row's
// deadline, F-30.3); it exists so third-party verifiers doing stock OIDC
// checks see a sane-looking token. Thirty days comfortably outruns any
// realistic renew cycle.
const agentCredentialJWTPlaceholderLife = 30 * 24 * time.Hour

// AgentCredentialRow is the IdP's in-memory view of a tracked credential row
// (the wire form minus the proto wrappers).
type AgentCredentialRow struct {
	ID            string
	AgentName     string
	IssuedBy      string
	Description   string
	ExpiresAt     time.Time
	ReplacedByID  string
	RevokedAt     *time.Time
	RevokedReason string
	LastSeenAt    *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Active reports whether the row governs liveness RIGHT NOW: not revoked, not
// retired (superseded by a successor), and before its deadline. A zero
// ExpiresAt (no deadline) imposes no expiry. This mirrors the model's
// predicate (F-30.3 evaluates it against the wire row).
func (r *AgentCredentialRow) Active(asOf time.Time) bool {
	if r.RevokedAt != nil {
		return false
	}
	if r.ReplacedByID != "" {
		return false
	}
	if r.ExpiresAt.IsZero() {
		return true
	}
	return asOf.Before(r.ExpiresAt)
}

// AgentCredentialStore persists the credential directory so multiple IdP
// replicas share it. Production wires the Database service (F-25 shape);
// tests may substitute an in-memory implementation. Errors carry gRPC
// status codes (NotFound for missing rows, FailedPrecondition for illegal
// transitions).
type AgentCredentialStore interface {
	// Create stores a freshly issued row. The id (== the JWT's jti) is
	// caller-chosen. Duplicate id is AlreadyExists.
	Create(ctx context.Context, row *AgentCredentialRow) (*AgentCredentialRow, error)
	// Get loads a row by id. Missing is NotFound.
	Get(ctx context.Context, id string) (*AgentCredentialRow, error)
	// List loads rows for one agent ("" = all agents), ordered by id.
	List(ctx context.Context, agentName string) ([]*AgentCredentialRow, error)
	// ExtendExpiration moves the deadline FORWARD (a renew). Backward is
	// InvalidArgument; a revoked row is FailedPrecondition.
	ExtendExpiration(ctx context.Context, id string, expiresAt time.Time) (*AgentCredentialRow, error)
	// Retire chains the row to its successor (rotation's first half).
	// Already-retired/revoked rows are FailedPrecondition.
	Retire(ctx context.Context, id, replacedByID string) (*AgentCredentialRow, error)
	// Revoke stamps the row revoked now, with a reason (idempotent).
	Revoke(ctx context.Context, id, reason string) (*AgentCredentialRow, error)
}

// AgentCredentialOps signs agent-credential JWTs and drives their lifecycle
// against the store. Nil (unattached) means the credential RPCs answer
// Unimplemented.
type AgentCredentialOps struct {
	cfg    config.IdPConfig
	km     *KeyManager
	store  AgentCredentialStore
	logger *slog.Logger
}

// NewAgentCredentialOps builds the ops layer. km signs; store persists.
func NewAgentCredentialOps(cfg config.IdPConfig, km *KeyManager, store AgentCredentialStore, logger *slog.Logger) *AgentCredentialOps {
	if logger == nil {
		logger = slog.Default()
	}
	return &AgentCredentialOps{cfg: cfg, km: km, store: store, logger: logger}
}

// drawCredentialID draws a fresh credential id (== the JWT's jti): 16 random
// bytes, hex-encoded (32 chars). Collision probability is negligible; the
// store's duplicate-id check is the backstop.
func drawCredentialID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("idp: draw credential id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// signAgentCredential signs the JWT for a row. Claims: iss/sub/aud/jti/iat/
// exp (the generous placeholder) + token_type. The row's deadline is NOT in
// the token — it lives in the row (that is what makes renew a no-break).
func (o *AgentCredentialOps) signAgentCredential(row *AgentCredentialRow) (string, error) {
	aud := o.cfg.Audiences
	if len(aud) == 0 {
		aud = []string{"cdrom-api"}
	}
	now := time.Now()
	claims := map[string]any{
		"iss":        o.cfg.EffectiveIssuer(),
		"sub":        "agent:" + row.AgentName,
		"aud":        aud,
		"jti":        row.ID,
		"iat":        now.Unix(),
		"exp":        now.Add(agentCredentialJWTPlaceholderLife).Unix(),
		"token_type": "agent_credential",
		"agent_name": row.AgentName,
	}
	token, err := o.km.Sign(claims)
	if err != nil {
		return "", fmt.Errorf("idp: sign agent credential: %w", err)
	}
	return token, nil
}

// Issue signs a NEW credential for the named agent (deadline = now + the
// configured TTL) and stores the row. Returns the row and the plaintext JWT
// (shown exactly once).
func (o *AgentCredentialOps) Issue(ctx context.Context, agentName, issuedBy, description string) (*AgentCredentialRow, string, error) {
	if agentName == "" {
		return nil, "", status.Error(codes.InvalidArgument, "agent_name is required")
	}
	id, err := drawCredentialID()
	if err != nil {
		return nil, "", err
	}
	row := &AgentCredentialRow{
		ID:          id,
		AgentName:   agentName,
		IssuedBy:    issuedBy,
		Description: description,
		ExpiresAt:   time.Now().Add(o.cfg.EffectiveAgentCredentialTTL()),
	}
	token, err := o.signAgentCredential(row)
	if err != nil {
		return nil, "", err
	}
	created, err := o.store.Create(ctx, row)
	if err != nil {
		return nil, "", err
	}
	o.logger.Info("idp: issued agent credential", "credential", created.ID, "agent", agentName,
		"issued_by", issuedBy, "ttl", o.cfg.EffectiveAgentCredentialTTL())
	return created, token, nil
}

// Get returns a row's metadata (never JWT material) by id.
func (o *AgentCredentialOps) Get(ctx context.Context, id string) (*AgentCredentialRow, error) {
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	return o.store.Get(ctx, id)
}

// List returns rows for one agent ("" = all), metadata only.
func (o *AgentCredentialOps) List(ctx context.Context, agentName string) ([]*AgentCredentialRow, error) {
	return o.store.List(ctx, agentName)
}

// Renew extends a row's deadline to now + TTL WITHOUT touching the JWT —
// the same token keeps working (structural no-break). A revoked row cannot
// be renewed (the store rejects it).
func (o *AgentCredentialOps) Renew(ctx context.Context, id string) (*AgentCredentialRow, error) {
	cur, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	next := time.Now().Add(o.cfg.EffectiveAgentCredentialTTL())
	updated, err := o.store.ExtendExpiration(ctx, id, next)
	if err != nil {
		return nil, err
	}
	o.logger.Info("idp: renewed agent credential", "credential", id, "agent", cur.AgentName,
		"previous_expiration", cur.ExpiresAt.Format(time.RFC3339),
		"new_expiration", updated.ExpiresAt.Format(time.RFC3339))
	return updated, nil
}

// Rotate retires a row and signs a SUCCESSOR for the same agent. Successor
// first, retirement second: the overlap window (both rows momentarily
// present) is SAFE because the retired row's Active() is false the moment
// its replaced_by_id is set. Returns the successor row and its plaintext
// JWT (shown exactly once).
func (o *AgentCredentialOps) Rotate(ctx context.Context, id, issuedBy, description string) (*AgentCredentialRow, string, error) {
	old, err := o.store.Get(ctx, id)
	if err != nil {
		return nil, "", err
	}
	if old.RevokedAt != nil || old.ReplacedByID != "" {
		return nil, "", status.Error(codes.FailedPrecondition, "cannot rotate a retired or revoked credential")
	}
	successorID, err := drawCredentialID()
	if err != nil {
		return nil, "", err
	}
	successor := &AgentCredentialRow{
		ID:          successorID,
		AgentName:   old.AgentName,
		IssuedBy:    issuedBy,
		Description: description,
		ExpiresAt:   time.Now().Add(o.cfg.EffectiveAgentCredentialTTL()),
	}
	token, err := o.signAgentCredential(successor)
	if err != nil {
		return nil, "", err
	}
	created, err := o.store.Create(ctx, successor)
	if err != nil {
		return nil, "", err
	}
	retired, err := o.store.Retire(ctx, id, created.ID)
	if err != nil {
		// The successor EXISTS but the predecessor was not retired (the
		// precondition failed between Get and Retire). Surface the error;
		// the operator can revoke the dangling successor explicitly.
		o.logger.Error("idp: rotation retired-predecessor step failed; successor exists but predecessor unretired",
			"credential", id, "successor", created.ID, "err", err)
		return nil, "", err
	}
	_ = retired
	o.logger.Info("idp: rotated agent credential", "retired", id, "successor", created.ID, "agent", old.AgentName)
	return created, token, nil
}

// Revoke stamps a row revoked NOW, with a reason. Immediate effect: the row
// stops governing liveness the moment the write commits.
func (o *AgentCredentialOps) Revoke(ctx context.Context, id, reason string) (*AgentCredentialRow, error) {
	row, err := o.store.Revoke(ctx, id, reason)
	if err != nil {
		return nil, err
	}
	o.logger.Info("idp: revoked agent credential", "credential", id, "agent", row.AgentName, "reason", reason)
	return row, nil
}

// ---- Database-service-backed store ------------------------------------------

// dbAgentCredentialStore implements AgentCredentialStore over the Database
// service's gRPC surface. It translates between the IdP's row view and the
// wire messages and maps gRPC errors through verbatim (statuses are
// preserved).
type dbAgentCredentialStore struct {
	db dbpb.DatabaseClient
}

// NewDBAgentCredentialStore returns an AgentCredentialStore that persists the
// credential directory through the Database service.
func NewDBAgentCredentialStore(db dbpb.DatabaseClient) AgentCredentialStore {
	return &dbAgentCredentialStore{db: db}
}

func (s *dbAgentCredentialStore) Create(ctx context.Context, row *AgentCredentialRow) (*AgentCredentialRow, error) {
	msg := &dbpb.AgentCredential{
		Id:          row.ID,
		AgentName:   row.AgentName,
		IssuedBy:    row.IssuedBy,
		Description: row.Description,
	}
	if !row.ExpiresAt.IsZero() {
		msg.ExpiresAt = timestamppb.New(row.ExpiresAt)
	}
	resp, err := s.db.CreateAgentCredential(ctx, &dbpb.CreateAgentCredentialRequest{Credential: msg})
	if err != nil {
		return nil, err
	}
	return agentCredentialFromProto(resp), nil
}

func (s *dbAgentCredentialStore) Get(ctx context.Context, id string) (*AgentCredentialRow, error) {
	resp, err := s.db.GetAgentCredential(ctx, &dbpb.GetAgentCredentialRequest{Id: id})
	if err != nil {
		return nil, err
	}
	return agentCredentialFromProto(resp), nil
}

func (s *dbAgentCredentialStore) List(ctx context.Context, agentName string) ([]*AgentCredentialRow, error) {
	resp, err := s.db.ListAgentCredentials(ctx, &dbpb.ListAgentCredentialsRequest{AgentName: agentName})
	if err != nil {
		return nil, err
	}
	rows := make([]*AgentCredentialRow, 0, len(resp.GetCredentials()))
	for _, c := range resp.GetCredentials() {
		rows = append(rows, agentCredentialFromProto(c))
	}
	return rows, nil
}

func (s *dbAgentCredentialStore) ExtendExpiration(ctx context.Context, id string, expiresAt time.Time) (*AgentCredentialRow, error) {
	resp, err := s.db.ExtendAgentCredentialExpiration(ctx, &dbpb.ExtendAgentCredentialExpirationRequest{
		Id:        id,
		ExpiresAt: timestamppb.New(expiresAt),
	})
	if err != nil {
		return nil, err
	}
	return agentCredentialFromProto(resp), nil
}

func (s *dbAgentCredentialStore) Retire(ctx context.Context, id, replacedByID string) (*AgentCredentialRow, error) {
	resp, err := s.db.RetireAgentCredential(ctx, &dbpb.RetireAgentCredentialRequest{
		Id:           id,
		ReplacedById: replacedByID,
	})
	if err != nil {
		return nil, err
	}
	return agentCredentialFromProto(resp), nil
}

func (s *dbAgentCredentialStore) Revoke(ctx context.Context, id, reason string) (*AgentCredentialRow, error) {
	resp, err := s.db.RevokeAgentCredential(ctx, &dbpb.RevokeAgentCredentialRequest{Id: id, Reason: reason})
	if err != nil {
		return nil, err
	}
	return agentCredentialFromProto(resp), nil
}

// agentCredentialFromProto maps a wire row to the IdP's in-memory view.
func agentCredentialFromProto(c *dbpb.AgentCredential) *AgentCredentialRow {
	row := &AgentCredentialRow{
		ID:            c.GetId(),
		AgentName:     c.GetAgentName(),
		IssuedBy:      c.GetIssuedBy(),
		Description:   c.GetDescription(),
		ReplacedByID:  c.GetReplacedById(),
		RevokedReason: c.GetRevokedReason(),
	}
	if ts := c.GetExpiresAt(); ts != nil {
		row.ExpiresAt = ts.AsTime()
	}
	if ts := c.GetRevokedAt(); ts != nil {
		v := ts.AsTime()
		row.RevokedAt = &v
	}
	if ts := c.GetLastSeenAt(); ts != nil {
		v := ts.AsTime()
		row.LastSeenAt = &v
	}
	if ts := c.GetCreatedAt(); ts != nil {
		row.CreatedAt = ts.AsTime()
	}
	if ts := c.GetUpdatedAt(); ts != nil {
		row.UpdatedAt = ts.AsTime()
	}
	return row
}
