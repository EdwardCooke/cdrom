// Package api — agent-credential verification (F-30.2, Slice 3).
//
// The self-service ARM of the renew endpoint: an agent renews its OWN
// credential by presenting it as the request's Bearer token. Verification is
// handler-local (the WEBHOOK-OIDC precedent: a verb-specific alternate
// credential, not a global middleware branch): the JWT's signature is checked
// against the IdP's JWKS (the JobTokenAuth pattern, generalized), and its
// liveness against the credential ROW (F-30.2) through the Database service.
// The presented credential must bind to BOTH the path's agent name AND the
// path's credential id — cross-name and cross-id renewals are impossible by
// construction. Human callers (OIDC JWTs) fail the agent-credential check and
// fall through to the RBAC arm (agents.can-manage).
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	oidc "github.com/coreos/go-oidc/v3/oidc"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// AgentCredentialVerifier verifies an agent credential presented on the
// renew endpoint. It combines cryptographic verification (the IdP's JWKS —
// the signature proves the IdP issued it) with row-liveness (the Database
// service — the row decides life: not revoked, not retired, not expired).
// The JWT's exp claim is a generous PLACEHOLDER and is skipped (mirroring
// JobTokenAuth): the row's deadline is the law.
type AgentCredentialVerifier struct {
	verifier  *oidc.IDTokenVerifier
	audiences []string
	db        dbpb.DatabaseClient
	logger    *slog.Logger
}

// NewAgentCredentialVerifier builds the verifier: OIDC discovery against the
// IdP's HTTP surface (a network call), then a JWKS-backed verifier skipping
// the client-id and expiry checks (agent credentials are not issued to a
// client, and their exp is a placeholder). audiences are the values accepted
// on the aud claim (the API's configured audiences, as for job tokens). db is
// consulted for the row's liveness. client is the HTTP client used for
// discovery (an mTLS client when the IdP serves TLS); a nil client uses the
// default.
func NewAgentCredentialVerifier(ctx context.Context, cfg config.GRPCAuthConfig, client *http.Client, db dbpb.DatabaseClient, logger *slog.Logger) (*AgentCredentialVerifier, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if client == nil {
		client = http.DefaultClient
	}
	base := normalizeIDPURL(cfg.IdPAddress)
	provider, err := oidc.NewProvider(oidc.ClientContext(ctx, client), base)
	if err != nil {
		return nil, fmt.Errorf("api: oidc discovery against idp %q: %w", base, err)
	}
	verifier := provider.Verifier(&oidc.Config{SkipClientIDCheck: true, SkipExpiryCheck: true})
	return &AgentCredentialVerifier{verifier: verifier, audiences: cfg.Audiences, db: db, logger: logger}, nil
}

// Enabled reports whether the verifier is usable.
func (v *AgentCredentialVerifier) Enabled() bool { return v != nil && v.verifier != nil }

// Verify checks the presented token: signature (JWKS), audience, and row
// liveness. It returns the credential's jti and bound agent name on success.
// Any failure (forgery, wrong audience, unknown/revoked/retired/expired row)
// is an error — the caller falls through to the RBAC arm.
func (v *AgentCredentialVerifier) Verify(ctx context.Context, token string) (jti, agentName string, err error) {
	idt, err := v.verifier.Verify(ctx, token)
	if err != nil {
		return "", "", fmt.Errorf("api: verify agent credential: %w", err)
	}
	if !v.audienceOK(idt.Audience) {
		return "", "", fmt.Errorf("api: agent credential audience %v not accepted", idt.Audience)
	}
	sub := idt.Subject // "agent:<name>"
	if len(sub) < len("agent:")+1 || sub[:6] != "agent:" {
		return "", "", fmt.Errorf("api: agent credential subject %q is not an agent principal", sub)
	}
	agentName = sub[len("agent:"):]
	var custom struct {
		JTI string `json:"jti"`
	}
	if err := idt.Claims(&custom); err != nil {
		return "", "", fmt.Errorf("api: read agent credential claims: %w", err)
	}
	jti = custom.JTI
	if jti == "" {
		return "", "", fmt.Errorf("api: agent credential has no jti")
	}
	// Row liveness (F-30.2): the row decides life.
	row, err := v.db.GetAgentCredential(ctx, &dbpb.GetAgentCredentialRequest{Id: jti})
	if err != nil {
		return "", "", fmt.Errorf("api: agent credential row lookup: %w", err)
	}
	if row.GetAgentName() != agentName {
		return "", "", fmt.Errorf("api: agent credential row is bound to %q, not %q", row.GetAgentName(), agentName)
	}
	revoked := row.GetRevokedAt() != nil
	retired := row.GetReplacedById() != ""
	expired := row.GetExpiresAt() != nil && row.GetExpiresAt().AsTime().Before(time.Now().UTC())
	if revoked || retired || expired {
		return "", "", fmt.Errorf("api: agent credential %s is not active (revoked=%v retired=%v expired=%v)", jti, revoked, retired, expired)
	}
	return jti, agentName, nil
}

// audienceOK reports whether the token's audience contains any of the
// configured audiences (shared with JobTokenAuth's semantics).
func (v *AgentCredentialVerifier) audienceOK(aud []string) bool {
	for _, want := range v.audiences {
		for _, got := range aud {
			if got == want {
				return true
			}
		}
	}
	return false
}
