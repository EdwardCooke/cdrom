// Package api — agent-credential management (F-30.2, Slice 2).
//
// Thin HTTP proxies to the IdP's agent-credential RPCs (the F-25 shape: the
// real logic — signing, lifecycle orchestration — lives in the IdP; the API
// authorizes, audits, and relays). Humans act by RBAC permission
// (agents.can-view / agents.can-manage); the agent's SELF-SERVICE renew arm
// lands in Slice 3. The plaintext JWT travels only in the issue/rotate
// response bodies and is never returned by list/get.
package api

import (
	"context"
	"encoding/json"
	"net/http"

	"cdrom/internal/audit"
	"cdrom/internal/auth"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// AgentCredentialClient is the API's proxy to the IdP's agent-credential
// surface. The generated idppb.IdPClient satisfies it; the variadic opts keep
// the interface callable with a plain generated client in tests.
type AgentCredentialClient interface {
	IssueAgentCredential(ctx context.Context, agentName, issuedBy, description string) (*dbpb.AgentCredential, string, error)
	GetAgentCredential(ctx context.Context, id string) (*dbpb.AgentCredential, error)
	ListAgentCredentials(ctx context.Context, agentName string) ([]*dbpb.AgentCredential, error)
	RenewAgentCredential(ctx context.Context, id string) (*dbpb.AgentCredential, error)
	RotateAgentCredential(ctx context.Context, id, issuedBy, description string) (*dbpb.AgentCredential, string, error)
	RevokeAgentCredential(ctx context.Context, id, reason string) (*dbpb.AgentCredential, error)
}

// NewAgentCredentialClient returns the gRPC-backed AgentCredentialClient.
func NewAgentCredentialClient(idp idppb.IdPClient) AgentCredentialClient {
	return &grpcAgentCredentialClient{idp: idp}
}

type grpcAgentCredentialClient struct {
	idp idppb.IdPClient
}

func (c *grpcAgentCredentialClient) IssueAgentCredential(ctx context.Context, agentName, issuedBy, description string) (*dbpb.AgentCredential, string, error) {
	resp, err := c.idp.IssueAgentCredential(ctx, &idppb.IssueAgentCredentialRequest{
		AgentName:   agentName,
		IssuedBy:    issuedBy,
		Description: description,
	})
	if err != nil {
		return nil, "", err
	}
	return resp.GetCredential(), resp.GetJwt(), nil
}

func (c *grpcAgentCredentialClient) GetAgentCredential(ctx context.Context, id string) (*dbpb.AgentCredential, error) {
	resp, err := c.idp.GetAgentCredential(ctx, &idppb.GetAgentCredentialRequest{Id: id})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *grpcAgentCredentialClient) ListAgentCredentials(ctx context.Context, agentName string) ([]*dbpb.AgentCredential, error) {
	resp, err := c.idp.ListAgentCredentials(ctx, &idppb.ListAgentCredentialsRequest{AgentName: agentName})
	if err != nil {
		return nil, err
	}
	return resp.GetCredentials(), nil
}

func (c *grpcAgentCredentialClient) RenewAgentCredential(ctx context.Context, id string) (*dbpb.AgentCredential, error) {
	resp, err := c.idp.RenewAgentCredential(ctx, &idppb.RenewAgentCredentialRequest{Id: id})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *grpcAgentCredentialClient) RotateAgentCredential(ctx context.Context, id, issuedBy, description string) (*dbpb.AgentCredential, string, error) {
	resp, err := c.idp.RotateAgentCredential(ctx, &idppb.RotateAgentCredentialRequest{
		Id:          id,
		IssuedBy:    issuedBy,
		Description: description,
	})
	if err != nil {
		return nil, "", err
	}
	return resp.GetCredential(), resp.GetJwt(), nil
}

func (c *grpcAgentCredentialClient) RevokeAgentCredential(ctx context.Context, id, reason string) (*dbpb.AgentCredential, error) {
	resp, err := c.idp.RevokeAgentCredential(ctx, &idppb.RevokeAgentCredentialRequest{Id: id, Reason: reason})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

// agentCredentialUnavailable replies 501 when the IdP is not wired (the
// degrade-to-null convention, F-28.4 S4): the endpoint exists but the
// mechanic is off.
func (s *Server) agentCredentialUnavailable(w http.ResponseWriter) {
	httpError(w, http.StatusNotImplemented, "agent credentials are unavailable (IdP not wired)")
}

// issueAgentCredential issues a new credential for the named agent
// (POST /api/agents/{name}/credentials). Requires agents.can-manage. The
// response carries the row PLUS the plaintext JWT, returned exactly once.
func (s *Server) issueAgentCredential(w http.ResponseWriter, r *http.Request) {
	if s.agentCredentials == nil {
		s.agentCredentialUnavailable(w)
		return
	}
	if !s.requirePermission(w, r, authz.PermAgentsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "agent name is required")
		return
	}
	var body struct {
		Description string `json:"description"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			httpError(w, http.StatusBadRequest, "invalid body: %v", err)
			return
		}
	}
	actor := auth.UserFromContext(r.Context()).Subject
	cred, jwt, err := s.agentCredentials.IssueAgentCredential(r.Context(), name, actor, body.Description)
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAgentCredentialIssue,
		TargetKind: audit.TargetAgentCredential,
		TargetID:   cred.GetId(),
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusCreated, map[string]any{
		"credential": cred,
		"jwt":        jwt, // plaintext, exactly once
	})
}

// listAgentCredentials lists an agent's credential rows
// (GET /api/agents/{name}/credentials). Requires agents.can-view. Metadata
// only — never JWT material.
func (s *Server) listAgentCredentials(w http.ResponseWriter, r *http.Request) {
	if s.agentCredentials == nil {
		s.agentCredentialUnavailable(w)
		return
	}
	if !s.requirePermission(w, r, authz.PermAgentsView, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	creds, err := s.agentCredentials.ListAgentCredentials(r.Context(), name)
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(creds))
}

// renewAgentCredential extends a row's deadline to now + TTL without touching
// the JWT (POST /api/agents/{name}/credentials/{id}/renew). TWO ARMS:
//
//  1. SELF-SERVICE (F-30.2 Slice 3): the agent presents its OWN credential as
//     the Bearer token; the verifier checks signature (JWKS) + row liveness
//     and BINDS it to the path ({name} must equal the credential's agent,
//     {id} must equal its jti) — cross-name/cross-id renew is impossible by
//     construction. Checked FIRST, before RBAC, because an agent principal
//     carries no roles (the RBAC arm would 403 it).
//  2. ADMINISTRATOR: agents.can-manage (any principal with the permission).
//
// The presented credential is NEVER logged or audited (secret-value hygiene);
// the audit event attributes the renewal to the credential's jti.
func (s *Server) renewAgentCredential(w http.ResponseWriter, r *http.Request) {
	if s.agentCredentials == nil {
		s.agentCredentialUnavailable(w)
		return
	}
	name := r.PathValue("name")
	id := r.PathValue("id")
	selfServiced := false
	if s.agentCredentialVerifier != nil {
		if token := httpBearerToken(r); token != "" {
			if jti, agentName, verr := s.agentCredentialVerifier.Verify(r.Context(), token); verr == nil {
				if agentName == name && jti == id {
					selfServiced = true
				}
			}
			// A presented-but-invalid credential does NOT fall through to the
			// RBAC arm: the caller offered a MACHINE credential, so judge it on
			// its merits (401) — smuggling a human JWT past a failed machine
			// check would blur the two regimes.
			if !selfServiced {
				httpError(w, http.StatusUnauthorized, "presented agent credential is not valid for this agent/credential")
				return
			}
		}
	}
	if !selfServiced {
		if !s.requirePermission(w, r, authz.PermAgentsManage, authz.Resource{}) {
			return
		}
	}
	cred, err := s.agentCredentials.RenewAgentCredential(r.Context(), id)
	if err != nil {
		grpcError(w, err)
		return
	}
	event := audit.Event{
		Action:     audit.ActionAgentCredentialRenew,
		TargetKind: audit.TargetAgentCredential,
		TargetID:   cred.GetId(),
		TargetName: cred.GetAgentName(),
		Outcome:    audit.OutcomeSuccess,
	}
	if selfServiced {
		event.Actor = "agent:" + name
		event.ActorKind = "agent"
	}
	s.recordAudit(r, event)
	writeJSON(w, http.StatusOK, cred)
}

// rotateAgentCredential retires a row and issues a successor
// (POST /api/agents/{name}/credentials/{id}/rotate). Requires
// agents.can-manage. The response carries the successor row PLUS the new
// plaintext JWT, returned exactly once.
func (s *Server) rotateAgentCredential(w http.ResponseWriter, r *http.Request) {
	if s.agentCredentials == nil {
		s.agentCredentialUnavailable(w)
		return
	}
	if !s.requirePermission(w, r, authz.PermAgentsManage, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	var body struct {
		Description string `json:"description"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			httpError(w, http.StatusBadRequest, "invalid body: %v", err)
			return
		}
	}
	actor := auth.UserFromContext(r.Context()).Subject
	cred, jwt, err := s.agentCredentials.RotateAgentCredential(r.Context(), id, actor, body.Description)
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAgentCredentialRotate,
		TargetKind: audit.TargetAgentCredential,
		TargetID:   cred.GetId(),
		TargetName: cred.GetAgentName(),
		OldValue:   map[string]any{"retired_id": id},
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"credential": cred,
		"jwt":        jwt, // plaintext, exactly once
	})
}

// revokeAgentCredential revokes a row immediately
// (DELETE /api/agents/{name}/credentials/{id}). Requires agents.can-manage.
// The optional JSON body carries a reason.
func (s *Server) revokeAgentCredential(w http.ResponseWriter, r *http.Request) {
	if s.agentCredentials == nil {
		s.agentCredentialUnavailable(w)
		return
	}
	if !s.requirePermission(w, r, authz.PermAgentsManage, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	var body struct {
		Reason string `json:"reason"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			httpError(w, http.StatusBadRequest, "invalid body: %v", err)
			return
		}
	}
	cred, err := s.agentCredentials.RevokeAgentCredential(r.Context(), id, body.Reason)
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAgentCredentialRevoke,
		TargetKind: audit.TargetAgentCredential,
		TargetID:   cred.GetId(),
		TargetName: cred.GetAgentName(),
		OldValue:   map[string]any{"reason": body.Reason},
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusOK, cred)
}
