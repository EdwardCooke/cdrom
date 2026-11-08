// Package api — shared token-verification substrate (F-30.3, Slice 1).
//
// Both the JOB-TOKEN path (JobTokenAuth) and the AGENT-CREDENTIAL path
// (AgentCredentialVerifier) verify IdP-signed RS256 JWTs against the same
// JWKS with the same policy: skip the client-id check (these tokens are not
// issued to an OAuth client) and SKIP THE EXP CHECK (cdrom tokens carry a
// long placeholder exp; liveness is gated on DATABASE STATE — the job's live
// status for job tokens, the credential row for agent credentials). This file
// factors that COMMON SUBSTRATE (OIDC discovery + verifier construction +
// audience intersection) so the two paths cannot drift apart.
package api

import (
	"context"
	"fmt"
	"net/http"

	oidc "github.com/coreos/go-oidc/v3/oidc"
)

// newOIDCTokenVerifier performs OIDC discovery against the IdP's HTTP base
// URL and returns an IDTokenVerifier tuned for cdrom's machine tokens:
// client-id and expiry checks are SKIPPED (see package comment). The
// provider caches the JWKS keys it discovers (refreshing on an unknown kid),
// so steady-state verification is a local signature check.
func newOIDCTokenVerifier(ctx context.Context, base string, client *http.Client) (*oidc.IDTokenVerifier, error) {
	if client == nil {
		client = http.DefaultClient
	}
	provider, err := oidc.NewProvider(oidc.ClientContext(ctx, client), base)
	if err != nil {
		return nil, fmt.Errorf("api: oidc discovery against idp %q: %w", base, err)
	}
	return provider.Verifier(&oidc.Config{SkipClientIDCheck: true, SkipExpiryCheck: true}), nil
}

// audienceAccepted reports whether the token's audience INTERSECTS the
// configured audiences (a token is valid for the API when its aud contains
// any of them). Mirrors the semantics both token paths have always used.
func audienceAccepted(configured, presented []string) bool {
	for _, want := range configured {
		for _, got := range presented {
			if got == want {
				return true
			}
		}
	}
	return false
}
