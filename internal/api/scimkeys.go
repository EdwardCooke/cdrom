package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/audit"
	"cdrom/internal/auth"
	"cdrom/internal/authz"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// scimKeyPruneHorizon is how long a revocation stays undoable before the
// prune sweep physically deletes the row (F-28.2). It mirrors the Database
// service's reinstate window (scimReinstateWindow): the sweep deletes exactly
// the rows whose reinstate window has ELAPSED.
const scimKeyPruneHorizon = 24 * time.Hour

// ScimKeyClient is the API's proxy to the SCIM key pool (F-28.2). The pool
// authenticates the corporate SCIM connector against the IdP's /scim/v2
// surface. The DATA PLANE (persist, query, transitions, prune) speaks to the
// Database service directly — it owns the rows and the pool semantics (cap,
// CAS, prune horizon, bundling). Only DERIVATION detours to the IdP: it is
// the sole pepper-touching site (it draws the plaintext and computes the
// Argon2ID encoding; the pepper never leaves the IdP). The API is a thin
// coordinator: it displays the plaintext exactly once (the 201) and never
// persists it.
type ScimKeyClient interface {
	// Derive asks the IdP to draw a fresh SCIM key and compute its Argon2ID
	// encoding. It returns the plaintext (shown exactly once), the encoding
	// (what gets persisted), and the 8-char prefix.
	Derive(ctx context.Context) (plaintext, encoding, prefix string, err error)
	// Create persists a derived key (encoding + prefix + metadata) through
	// the Database service. Beyond the five-key cap it is a conflict.
	Create(ctx context.Context, encoding, prefix, description, createdBy string, expiresAt *time.Time) (*ScimKeyResult, error)
	// Get returns one key (including a revoked-within-window row, so the
	// caller can distinguish "still reversible" from "gone"). Unknown or
	// pruned is NotFound.
	Get(ctx context.Context, id string) (*ScimKeyResult, error)
	// List returns the non-revoked keys, newest first.
	List(ctx context.Context) ([]ScimKeyResult, error)
	// Update edits a key's description and/or expiration (partial semantics;
	// an explicit clear removes the expiration). Identity columns are frozen.
	// A revoked key is a conflict (reinstate first).
	Update(ctx context.Context, id string, req UpdateScimKeyFields) (*ScimKeyResult, error)
	// Revoke stamps the revocation (validation stops immediately). An
	// already-revoked key is a conflict; a pruned key is NotFound.
	Revoke(ctx context.Context, id string) (*ScimKeyResult, error)
	// Reinstate clears the revocation stamp (allowed only within the 24 h
	// window). A pruned key is NotFound; an elapsed window is a conflict.
	Reinstate(ctx context.Context, id string) (*ScimKeyResult, error)
	// Prune physically deletes rows whose revocation is older than the
	// horizon and returns the count removed.
	Prune(ctx context.Context, horizon time.Duration) (int, error)
}

// UpdateScimKeyFields carries a partial edit of a SCIM key's mutable
// attributes. The Has* flags express INTENT (present = apply, absent =
// unchanged); ClearExpiresAt takes precedence over HasExpiresAt (an explicit
// null clears the expiration).
type UpdateScimKeyFields struct {
	Description    string
	HasDescription bool
	ExpiresAt      *time.Time
	ClearExpiresAt bool
}

// ScimKeyResult is the public shape of a SCIM key (F-28.2): its identifier,
// label, the 8-char recognition prefix, and its lifecycle timestamps. It
// NEVER carries the plaintext or the full encoding — the prefix is the only
// fragment of the secret an operator ever sees again.
type ScimKeyResult struct {
	Identifier  string  `json:"identifier"`
	Description string  `json:"description"`
	Prefix      string  `json:"prefix"`
	CreatedBy   string  `json:"created_by,omitempty"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at,omitempty"`
	ExpiresAt   *string `json:"expires_at"`
	LastUsedAt  *string `json:"last_used_at"`
	Revoked     bool    `json:"revoked"`
	RevokedAt   *string `json:"revoked_at"`
}

// scimKeyResult renders a pool row as its public shape (timestamps as RFC
// 3339 UTC; absent timestamps as null).
func scimKeyResult(k *dbpb.ScimKey) *ScimKeyResult {
	out := &ScimKeyResult{
		Identifier:  k.GetId(),
		Description: k.GetDescription(),
		Prefix:      k.GetPrefix(),
		CreatedBy:   k.GetCreatedBy(),
	}
	if ts := k.GetCreatedAt(); ts != nil {
		s := ts.AsTime().UTC().Format(time.RFC3339)
		out.CreatedAt = s
	}
	if ts := k.GetUpdatedAt(); ts != nil {
		s := ts.AsTime().UTC().Format(time.RFC3339)
		out.UpdatedAt = s
	}
	if ts := k.GetExpiresAt(); ts != nil {
		s := ts.AsTime().UTC().Format(time.RFC3339)
		out.ExpiresAt = &s
	}
	if ts := k.GetLastUsedAt(); ts != nil {
		s := ts.AsTime().UTC().Format(time.RFC3339)
		out.LastUsedAt = &s
	}
	if ts := k.GetRevokedAt(); ts != nil {
		out.Revoked = true
		s := ts.AsTime().UTC().Format(time.RFC3339)
		out.RevokedAt = &s
	}
	return out
}

// grpcScimKeyClient is the production ScimKeyClient: the data plane over the
// Database service, the derivation over the IdP.
type grpcScimKeyClient struct {
	db  dbpb.DatabaseClient
	idp idppb.IdPClient
}

// NewScimKeyClient returns the production ScimKeyClient speaking to the
// Database service (pool rows + semantics) and the IdP (derivation).
func NewScimKeyClient(db dbpb.DatabaseClient, idp idppb.IdPClient) ScimKeyClient {
	return &grpcScimKeyClient{db: db, idp: idp}
}

func (c *grpcScimKeyClient) Derive(ctx context.Context) (string, string, string, error) {
	resp, err := c.idp.DeriveScimKey(ctx, &idppb.DeriveScimKeyRequest{})
	if err != nil {
		return "", "", "", &scimKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return resp.GetKey(), resp.GetEncoding(), resp.GetPrefix(), nil
}

func (c *grpcScimKeyClient) Create(ctx context.Context, encoding, prefix, description, createdBy string, expiresAt *time.Time) (*ScimKeyResult, error) {
	key := &dbpb.ScimKey{
		Hash:        encoding,
		Prefix:      prefix,
		Description: description,
		CreatedBy:   createdBy,
	}
	if expiresAt != nil {
		key.ExpiresAt = timestamppb.New(*expiresAt)
	}
	resp, err := c.db.CreateScimKey(ctx, &dbpb.CreateScimKeyRequest{Key: key})
	if err != nil {
		return nil, &scimKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return scimKeyResult(resp), nil
}

func (c *grpcScimKeyClient) Get(ctx context.Context, id string) (*ScimKeyResult, error) {
	resp, err := c.db.GetScimKey(ctx, &dbpb.GetScimKeyRequest{Id: id})
	if err != nil {
		return nil, &scimKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return scimKeyResult(resp), nil
}

func (c *grpcScimKeyClient) List(ctx context.Context) ([]ScimKeyResult, error) {
	resp, err := c.db.ListScimKeys(ctx, &dbpb.ListScimKeysRequest{})
	if err != nil {
		return nil, &scimKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	out := make([]ScimKeyResult, 0, len(resp.GetKeys()))
	for _, k := range resp.GetKeys() {
		out = append(out, *scimKeyResult(k))
	}
	return out, nil
}

func (c *grpcScimKeyClient) Update(ctx context.Context, id string, req UpdateScimKeyFields) (*ScimKeyResult, error) {
	pb := &dbpb.UpdateScimKeyRequest{
		Id:             id,
		Description:    req.Description,
		HasDescription: req.HasDescription,
		ClearExpiresAt: req.ClearExpiresAt,
	}
	if !req.ClearExpiresAt && req.ExpiresAt != nil {
		pb.HasExpiresAt = true
		pb.ExpiresAt = timestamppb.New(*req.ExpiresAt)
	}
	resp, err := c.db.UpdateScimKey(ctx, pb)
	if err != nil {
		return nil, &scimKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return scimKeyResult(resp), nil
}

func (c *grpcScimKeyClient) Revoke(ctx context.Context, id string) (*ScimKeyResult, error) {
	resp, err := c.db.RevokeScimKey(ctx, &dbpb.RevokeScimKeyRequest{Id: id})
	if err != nil {
		return nil, &scimKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return scimKeyResult(resp), nil
}

func (c *grpcScimKeyClient) Reinstate(ctx context.Context, id string) (*ScimKeyResult, error) {
	resp, err := c.db.ReinstateScimKey(ctx, &dbpb.ReinstateScimKeyRequest{Id: id})
	if err != nil {
		return nil, &scimKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return scimKeyResult(resp), nil
}

func (c *grpcScimKeyClient) Prune(ctx context.Context, horizon time.Duration) (int, error) {
	resp, err := c.db.PruneScimKeys(ctx, &dbpb.PruneScimKeysRequest{Cutoff: durationpb.New(horizon)})
	if err != nil {
		return 0, &scimKeyError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return int(resp.GetDeleted()), nil
}

// scimKeyError carries a gRPC status code (mapped to an HTTP status by
// scimKeyStatus) and the message.
type scimKeyError struct {
	code codes.Code
	msg  string
}

func (e *scimKeyError) Error() string {
	return fmt.Sprintf("scim key: %s: %s", e.code, e.msg)
}

// scimKeyStatus maps an error from the SCIM key pool to an HTTP status: a
// scimKeyError is mapped from its gRPC code, and any other error (transport
// failure) is a 502.
func scimKeyStatus(err error) int {
	if e, ok := err.(*scimKeyError); ok {
		switch e.code {
		case codes.NotFound:
			return http.StatusNotFound
		case codes.InvalidArgument:
			return http.StatusBadRequest
		case codes.AlreadyExists, codes.FailedPrecondition, codes.Aborted:
			return http.StatusConflict
		case codes.Unimplemented:
			return http.StatusNotImplemented
		}
		return http.StatusBadGateway
	}
	return http.StatusBadGateway
}

// requireScimKey reports whether the SCIM key pool is wired (a client is
// attached). When it is not, the endpoints respond 501.
func (s *Server) requireScimKey(w http.ResponseWriter) bool {
	if s.scimKeys == nil {
		httpError(w, http.StatusNotImplemented, "scim key management is not enabled")
		return false
	}
	return true
}

// scimKeyActorKind returns the audit actor kind for the caller (a service
// account administering the pool is recorded as such; humans as users).
func scimKeyActorKind(user auth.User) string {
	if user.Kind == "service-account" {
		return audit.ActorKindServiceAccount
	}
	return audit.ActorKindUser
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// createScimKeyRequest is the body of POST /api/scim/keys: the key's label
// and optional expiration.
type createScimKeyRequest struct {
	Description string  `json:"description"`
	ExpiresAt   *string `json:"expiresAt"`
}

// createScimKey mints a new SCIM key (F-28.2): it asks the IdP to DRAW the
// plaintext and COMPUTE the Argon2ID encoding (the sole pepper-touching
// site), persists the (encoding, prefix) pair through the Database service,
// and returns the plaintext EXACTLY ONCE — in this 201's top-level
// `plaintext` field (beside the `key` object) — never again. Requires
// scim-keys.can-create.
func (s *Server) createScimKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireScimKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermScimKeysCreate, authz.Resource{}) {
		return
	}
	var req createScimKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if n := utf8.RuneCountInString(req.Description); n < 1 || n > 200 {
		httpError(w, http.StatusBadRequest, "description is required (1-200 characters)")
		return
	}
	var expiresAt *time.Time
	if req.ExpiresAt != nil {
		t, err := time.Parse(time.RFC3339, *req.ExpiresAt)
		if err != nil {
			httpError(w, http.StatusBadRequest, "expiresAt must be RFC 3339: %v", err)
			return
		}
		if !t.After(time.Now().UTC()) {
			httpError(w, http.StatusBadRequest, "expiresAt must be strictly in the future")
			return
		}
		expiresAt = &t
	}
	user := auth.UserFromContext(r.Context())
	plaintext, encoding, prefix, err := s.scimKeys.Derive(r.Context())
	if err != nil {
		httpError(w, scimKeyStatus(err), "derive scim key: %v", err)
		return
	}
	result, err := s.scimKeys.Create(r.Context(), encoding, prefix, req.Description, user.Subject, expiresAt)
	if err != nil {
		httpError(w, scimKeyStatus(err), "create scim key: %v", err)
		return
	}
	newValue := map[string]any{"description": req.Description, "prefix": result.Prefix}
	if expiresAt != nil {
		newValue["expires_at"] = expiresAt.UTC().Format(time.RFC3339)
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionScimKeyCreate,
		TargetKind: audit.TargetScimKey,
		TargetID:   result.Identifier,
		TargetName: result.Prefix,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  scimKeyActorKind(user),
		NewValue:   auditJSON(newValue),
	})
	// The plaintext is returned exactly once (here); it is never cached.
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusCreated, map[string]any{"key": result, "plaintext": plaintext})
}

// listScimKeys returns the pool's NON-revoked keys, newest first (identifier,
// description, prefix, timestamps — never the plaintext or the encoding).
// Requires scim-keys.can-view.
func (s *Server) listScimKeys(w http.ResponseWriter, r *http.Request) {
	if !s.requireScimKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermScimKeysView, authz.Resource{}) {
		return
	}
	results, err := s.scimKeys.List(r.Context())
	if err != nil {
		httpError(w, scimKeyStatus(err), "list scim keys: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(results))
}

// getScimKey returns one key's object (the list shape PLUS a `revoked` flag —
// true for a key inside its 24 h post-revocation window, when the row still
// exists). Unknown or already-pruned keys are 404 (indistinguishable by
// design — no existence leak). Requires scim-keys.can-view.
func (s *Server) getScimKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireScimKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermScimKeysView, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "scim key id is required")
		return
	}
	result, err := s.scimKeys.Get(r.Context(), id)
	if err != nil {
		httpError(w, scimKeyStatus(err), "get scim key: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// updateScimKey edits a key's description and/or expiration (F-28.2). The
// identity columns (encoding, prefix, created_at, creator) are frozen — an
// edit cannot launder a key's provenance. Edits act only on NON-revoked keys
// (a revoked key is 409 — reinstate first). Requires scim-keys.can-edit.
func (s *Server) updateScimKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireScimKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermScimKeysEdit, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "scim key id is required")
		return
	}
	// Decode into a raw map: an explicit JSON null for expiresAt must be
	// distinguished from an omitted field (decoded into a typed struct, both
	// arrive as a nil pointer).
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil && !errors.Is(err, io.EOF) {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	fields := UpdateScimKeyFields{}
	if v, ok := raw["description"]; ok {
		var desc string
		if err := json.Unmarshal(v, &desc); err != nil {
			httpError(w, http.StatusBadRequest, "description must be a string")
			return
		}
		if n := utf8.RuneCountInString(desc); n < 1 || n > 200 {
			httpError(w, http.StatusBadRequest, "description must be 1-200 characters")
			return
		}
		fields.Description = desc
		fields.HasDescription = true
	}
	if v, ok := raw["expiresAt"]; ok {
		if strings.TrimSpace(string(v)) == "null" {
			fields.ClearExpiresAt = true
		} else {
			var iso string
			if err := json.Unmarshal(v, &iso); err != nil {
				httpError(w, http.StatusBadRequest, "expiresAt must be an RFC 3339 string or null")
				return
			}
			t, err := time.Parse(time.RFC3339, iso)
			if err != nil {
				httpError(w, http.StatusBadRequest, "expiresAt must be RFC 3339: %v", err)
				return
			}
			if !t.After(time.Now().UTC()) {
				httpError(w, http.StatusBadRequest, "expiresAt must be strictly in the future")
				return
			}
			fields.ExpiresAt = &t
		}
	}
	user := auth.UserFromContext(r.Context())
	result, err := s.scimKeys.Update(r.Context(), id, fields)
	if err != nil {
		httpError(w, scimKeyStatus(err), "update scim key: %v", err)
		return
	}
	newValue := map[string]any{}
	if fields.HasDescription {
		newValue["description"] = fields.Description
	}
	if fields.ClearExpiresAt {
		newValue["expires_at"] = nil
	}
	if fields.ExpiresAt != nil {
		newValue["expires_at"] = fields.ExpiresAt.UTC().Format(time.RFC3339)
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionScimKeyUpdate,
		TargetKind: audit.TargetScimKey,
		TargetID:   result.Identifier,
		TargetName: result.Prefix,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  scimKeyActorKind(user),
		NewValue:   auditJSON(newValue),
	})
	writeJSON(w, http.StatusOK, result)
}

// revokeScimKey stamps a key's revocation (F-28.2): validation stops INSTANTLY
// (the key drops out of the encoding bundle on the next pull). Idempotent in
// spirit, honest in result: an already-revoked key is 409, a pruned one 404.
// Requires scim-keys.can-revoke.
func (s *Server) revokeScimKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireScimKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermScimKeysRevoke, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "scim key id is required")
		return
	}
	user := auth.UserFromContext(r.Context())
	result, err := s.scimKeys.Revoke(r.Context(), id)
	if err != nil {
		httpError(w, scimKeyStatus(err), "revoke scim key: %v", err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionScimKeyRevoke,
		TargetKind: audit.TargetScimKey,
		TargetID:   result.Identifier,
		TargetName: result.Prefix,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  scimKeyActorKind(user),
		NewValue:   auditJSON(map[string]any{"revoked_at": result.RevokedAt}),
	})
	writeJSON(w, http.StatusOK, result)
}

// reinstateScimKey clears a key's revocation stamp (F-28.2) — recovering an
// accidental revocation while the 24 h window lasts. Outside the window (or
// already pruned) it is 404/409. Requires scim-keys.can-reinstate.
func (s *Server) reinstateScimKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireScimKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermScimKeysReinstate, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "scim key id is required")
		return
	}
	user := auth.UserFromContext(r.Context())
	result, err := s.scimKeys.Reinstate(r.Context(), id)
	if err != nil {
		httpError(w, scimKeyStatus(err), "reinstate scim key: %v", err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionScimKeyReinstate,
		TargetKind: audit.TargetScimKey,
		TargetID:   result.Identifier,
		TargetName: result.Prefix,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  scimKeyActorKind(user),
	})
	writeJSON(w, http.StatusOK, result)
}

// ---------------------------------------------------------------------------
// Maintenance (prune sweep)
// ---------------------------------------------------------------------------

// StartScimKeyPruning launches the approximately-hourly sweep that PHYSICALLY
// DELETES SCIM key rows whose revocation is older than the 24 h reinstate
// window (F-28.2). It is started alongside the audit pruner in cmd/api and
// runs until ctx is cancelled. The audit trail retains the revocations, so
// deletion costs no accountability.
func (s *Server) StartScimKeyPruning(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Hour
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.runScimKeyPruneSweep(ctx)
			}
		}
	}()
}

// runScimKeyPruneSweep performs one prune pass (factored out so the sweep's
// behaviour is testable without sleeping on the ticker).
func (s *Server) runScimKeyPruneSweep(ctx context.Context) {
	if s.scimKeys == nil {
		return
	}
	deleted, err := s.scimKeys.Prune(ctx, scimKeyPruneHorizon)
	if err != nil {
		s.logger.Warn("scim keys: prune sweep", "err", err)
		return
	}
	if deleted == 0 {
		return
	}
	s.logger.Info("scim keys: prune sweep", "deleted", deleted)
	if s.audit != nil {
		s.audit.Record(audit.Event{
			Action:     audit.ActionScimKeyPrune,
			TargetKind: audit.TargetScimKey,
			TargetName: "pool",
			Outcome:    audit.OutcomeSuccess,
			Actor:      "scim-key-prune",
			ActorKind:  audit.ActorKindSystem,
			NewValue:   auditJSON(map[string]any{"deleted": deleted}),
		})
	}
}
