// Package auth implements OIDC authentication for the API's UI-facing HTTP
// surface and its WebSocket endpoint.
//
// The flow is the OIDC authorization-code grant with PKCE: the UI redirects
// the browser to /api/auth/login, which bounces to the identity provider; on
// return the provider redirects to /api/auth/callback, where the API exchanges
// the code for tokens, validates the ID token, and sets a signed session
// cookie. Subsequent API and WebSocket requests present that cookie.
//
// Authentication is disabled by default so a local run works with no identity
// provider; enable it via the config (auth.enabled) for deployments that need
// it.
package auth

import (
	"context"
	"net/http"

	"cdrom/internal/config"
)

// Auth bundles the OIDC provider, session manager, and endpoint handler for
// the API's HTTP surface. It is always non-nil; when authentication is
// disabled the Provider is nil and Enabled reports false.
type Auth struct {
	Provider *Provider
	Sessions *SessionManager
	Handler  *Handler
}

// New builds the auth components from config. When auth is disabled it
// returns an Auth with a nil Provider (and a handler that reports the local
// user). It performs OIDC discovery (a network call) when enabled. It uses
// the default HTTP client; use NewWithClient to talk to an IdP that serves
// TLS.
func New(ctx context.Context, cfg config.AuthConfig) (*Auth, error) {
	return NewWithClient(ctx, cfg, nil)
}

// NewWithClient is New with an explicit HTTP client, used to reach an IdP
// that serves TLS (the client must trust the IdP's CA). A nil client falls
// back to the default.
func NewWithClient(ctx context.Context, cfg config.AuthConfig, httpClient *http.Client) (*Auth, error) {
	if !cfg.Enabled {
		return &Auth{Handler: NewHandler(nil, nil)}, nil
	}
	provider, err := NewProviderWithClient(ctx, cfg, httpClient)
	if err != nil {
		return nil, err
	}
	sessions := NewSessionManager(cfg.CookieSecret, cfg.CookieDomain)
	return &Auth{
		Provider: provider,
		Sessions: sessions,
		Handler:  NewHandler(provider, sessions),
	}, nil
}

// Enabled reports whether authentication is active.
func (a *Auth) Enabled() bool {
	return a != nil && a.Provider != nil && a.Provider.Enabled()
}
