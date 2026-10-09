// Package auth implements OIDC authentication for the API's UI-facing HTTP
// surface and its WebSocket endpoint.
//
// The API is a pure token verifier. The client (the UI, curl, or any other
// HTTP client) performs the OIDC authorization-code + PKCE flow against the
// identity provider itself — using the discovery document the API serves at
// /api/auth/oidc — receives an OAuth token, and presents it on every API and
// WebSocket request as `Authorization: Bearer <token>`. The API verifies that
// token against the IdP's JWKS (signature, issuer, audience, and expiry) and,
// when valid, extracts the authenticated user from its claims. There is no
// server-side session cookie and no server-side code exchange, which is what
// makes it easy for clients (curl, the UI) to authenticate with the token
// they already hold.
//
// Authentication is disabled by default so a local run works with no identity
// provider; enable it via the config (auth.enabled) for deployments that need
// it.
package auth

import (
	"context"
	"encoding/json"
	"net/http"

	oidc "github.com/coreos/go-oidc/v3/oidc"

	"cdrom/internal/config"
)

// Auth bundles the OIDC provider (token verifier) for the API's HTTP surface.
// It is always non-nil; when authentication is disabled the Provider is nil
// and Enabled reports false.
type Auth struct {
	Provider *Provider
}

// New builds the auth components from config. When auth is disabled it
// returns an Auth with a nil Provider. It performs OIDC discovery (a network
// call) when enabled. It uses the default HTTP client; use NewWithClient to
// talk to an IdP that serves TLS.
func New(ctx context.Context, cfg config.AuthConfig) (*Auth, error) {
	return NewWithClient(ctx, cfg, nil)
}

// NewWithClient is New with an explicit HTTP client, used to reach an IdP
// that serves TLS (the client must trust the IdP's CA). A nil client falls
// back to the default.
func NewWithClient(ctx context.Context, cfg config.AuthConfig, httpClient *http.Client) (*Auth, error) {
	if !cfg.Enabled {
		return &Auth{}, nil
	}
	provider, err := NewProviderWithClient(ctx, cfg, httpClient)
	if err != nil {
		return nil, err
	}
	return &Auth{Provider: provider}, nil
}

// Enabled reports whether authentication is active.
func (a *Auth) Enabled() bool {
	return a != nil && a.Provider != nil && a.Provider.Enabled()
}

// User is the authenticated principal extracted from a verified token.
type User struct {
	Subject string `json:"sub"`
	Name    string `json:"name,omitempty"`
	Email   string `json:"email,omitempty"`
	// Roles are the user's role names, stamped onto the token by the IdP
	// (from the user's registered roles). They are empty when the token was
	// not minted for a registered user (e.g. the local OIDC dev flow).
	Roles []string `json:"roles,omitempty"`
}

// HasRole reports whether the user carries the named role.
func (u User) HasRole(role string) bool {
	for _, r := range u.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// userContextKey is the context key under which the authenticated user is
// stored by the middleware.
type userContextKey struct{}

// WithUser returns a context carrying user.
func WithUser(ctx context.Context, user User) context.Context {
	return context.WithValue(ctx, userContextKey{}, user)
}

// UserFromContext returns the authenticated user stored by the middleware,
// or the zero User when absent.
func UserFromContext(ctx context.Context) User {
	if user, ok := ctx.Value(userContextKey{}).(User); ok {
		return user
	}
	return User{}
}

// Middleware wraps next so that, when authentication is enabled, every
// request must carry a valid `Authorization: Bearer <token>` header that
// verifies against the identity provider. When authentication is disabled it
// passes requests through unchanged.
func (a *Auth) Middleware(next http.Handler) http.Handler {
	return a.MiddlewareExempt(next)
}

// MiddlewareExempt is Middleware with a set of unauthenticated paths: requests
// whose path is in exempt bypass the token check (they are reachable without a
// Bearer token even when authentication is enabled). This is how the API keeps
// its sign-in entry points (e.g. /api/login and /api/register) reachable while
// still requiring a token on every other request. When authentication is
// disabled it passes all requests through unchanged.
func (a *Auth) MiddlewareExempt(next http.Handler, exempt ...string) http.Handler {
	if !a.Enabled() {
		return next
	}
	exemptSet := make(map[string]bool, len(exempt))
	for _, p := range exempt {
		exemptSet[p] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exemptSet[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		user, err := a.Provider.UserFromRequest(r)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"authentication required"}`))
			return
		}
		next.ServeHTTP(w, r.WithContext(WithUser(r.Context(), user)))
	})
}

// userFromIDToken extracts the principal from validated OIDC claims, applying
// the configured JWT claim mapping (F-14) to derive additional roles from a
// configurable claim (e.g. an enterprise IdP's group claim). The standard
// roles claim (stamped by the built-in IdP) is always read; the configured
// claim mapping is additive, so a principal can have token-stamped and
// claim-mapped roles at once.
func userFromIDToken(idToken *oidc.IDToken, roles config.RolesConfig) User {
	var claims struct {
		Subject string   `json:"sub"`
		Name    string   `json:"name"`
		Email   string   `json:"email"`
		Roles   []string `json:"roles"`
	}
	_ = idToken.Claims(&claims)
	user := User{Subject: claims.Subject, Name: claims.Name, Email: claims.Email, Roles: claims.Roles}
	// JWT claim mapping (F-14): read the configured claim and map its values
	// to role names, so group membership in an enterprise IdP drives Cdrom
	// roles without Cdrom's user directory knowing the user at all. When the
	// role claim is unset, claim mapping is disabled.
	if roles.RoleClaim != "" {
		for _, value := range claimValues(idToken, roles.RoleClaim) {
			if role := mappedRole(roles, value); role != "" {
				user.Roles = appendRole(user.Roles, role)
			}
		}
	}
	return user
}

// mappedRole returns the role name a claim value maps to under the configured
// mapping: the explicit RoleMappings entry when present, otherwise the value
// itself as a role name when RoleClaimAsNames is set, otherwise "" (ignored).
func mappedRole(roles config.RolesConfig, value string) string {
	if role, ok := roles.RoleMappings[value]; ok {
		return role
	}
	if roles.RoleClaimAsNames {
		return value
	}
	return ""
}

// claimValues returns the values of a token claim, handling both a scalar
// string claim and a list-of-strings claim (e.g. an enterprise IdP's "groups"
// claim). A missing or non-string claim yields no values.
func claimValues(idToken *oidc.IDToken, name string) []string {
	var raw map[string]any
	_ = idToken.Claims(&raw)
	if raw == nil {
		return nil
	}
	switch v := raw[name].(type) {
	case string:
		return []string{v}
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// appendRole appends role to roles if it is not already present.
func appendRole(roles []string, role string) []string {
	for _, r := range roles {
		if r == role {
			return roles
		}
	}
	return append(roles, role)
}

// DiscoveryDocument is the OIDC discovery document the API serves at
// /api/auth/oidc. It points a client at the identity provider's endpoints
// (discovered from the configured issuer) plus the API's own client_id and
// redirect_uri, so the client can run the authorization-code + PKCE flow
// against the IdP and then present the resulting token to the API.
type DiscoveryDocument struct {
	Issuer                string   `json:"issuer"`
	AuthorizationEndpoint string   `json:"authorization_endpoint"`
	TokenEndpoint         string   `json:"token_endpoint"`
	JwksURI               string   `json:"jwks_uri"`
	ClientID              string   `json:"client_id"`
	RedirectURI           string   `json:"redirect_uri"`
	Scopes                []string `json:"scopes"`
	ResponseType          string   `json:"response_type"`
	CodeChallengeMethod   string   `json:"code_challenge_method"`
}

// DiscoveryDocument returns the OIDC discovery document the API serves at
// /api/auth/oidc. It is nil when authentication is disabled.
func (p *Provider) DiscoveryDocument() *DiscoveryDocument {
	if p == nil || !p.Enabled() {
		return nil
	}
	endpoint := p.oidcProvider.Endpoint()
	// The JWKS URL is not exposed by oauth2.Endpoint; read it from the
	// discovery document via the provider's Claims.
	var metadata struct {
		JwksURI string `json:"jwks_uri"`
	}
	_ = p.oidcProvider.Claims(&metadata)
	return &DiscoveryDocument{
		Issuer:                p.cfg.Issuer,
		AuthorizationEndpoint: endpoint.AuthURL,
		TokenEndpoint:         endpoint.TokenURL,
		JwksURI:               metadata.JwksURI,
		ClientID:              p.cfg.ClientID,
		RedirectURI:           p.cfg.RedirectURL,
		Scopes:                p.cfg.EffectiveScopes(),
		ResponseType:          "code",
		CodeChallengeMethod:   "S256",
	}
}

// ServeDiscovery writes the OIDC discovery document to w (or a 404 when
// authentication is disabled).
func (a *Auth) ServeDiscovery(w http.ResponseWriter, r *http.Request) {
	if !a.Enabled() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"authentication is disabled"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(a.Provider.DiscoveryDocument())
}
