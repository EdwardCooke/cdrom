package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	oidc "github.com/coreos/go-oidc/v3/oidc"

	"cdrom/internal/config"
)

// Provider wraps an OIDC identity provider: it performs discovery, builds the
// authorization redirect (with PKCE), and exchanges the callback code for a
// validated ID token.
type Provider struct {
	oidcProvider *oidc.Provider
	verifier     *oidc.IDTokenVerifier
	cfg          config.AuthConfig
	httpClient   *http.Client
}

// NewProvider performs OIDC discovery against cfg.Issuer and returns a ready
// provider. It makes a network call to fetch the provider's metadata and
// signing keys. It uses the default HTTP client; use NewProviderWithClient to
// talk to an IdP that serves TLS.
func NewProvider(ctx context.Context, cfg config.AuthConfig) (*Provider, error) {
	return NewProviderWithClient(ctx, cfg, nil)
}

// NewProviderWithClient is NewProvider with an explicit HTTP client, used to
// reach an IdP that serves TLS (the client must trust the IdP's CA). A nil
// client falls back to the default.
func NewProviderWithClient(ctx context.Context, cfg config.AuthConfig, httpClient *http.Client) (*Provider, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	provider, err := oidc.NewProvider(oidc.ClientContext(ctx, httpClient), cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("auth: oidc discovery %q: %w", cfg.Issuer, err)
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: cfg.ClientID})
	return &Provider{oidcProvider: provider, verifier: verifier, cfg: cfg, httpClient: httpClient}, nil
}

// Enabled reports whether the provider is active.
func (p *Provider) Enabled() bool {
	return p != nil && p.oidcProvider != nil
}

// NewPKCEState generates a random state value and a matching PKCE code
// verifier, packing both into the state so the callback can recover the
// verifier without any server-side pre-auth store. The state format is
// "<random>.<codeVerifier>".
func NewPKCEState() (state, codeVerifier string) {
	random := randomToken(16)
	verifier := randomToken(32)
	return random + "." + verifier, verifier
}

// CodeVerifierFromState recovers the PKCE code verifier that was packed into
// state by NewPKCEState.
func CodeVerifierFromState(state string) string {
	parts := strings.SplitN(state, ".", 2)
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

// AuthURL builds the identity provider's authorization endpoint URL for the
// authorization-code + PKCE flow. state is echoed back to the callback (and
// carries the PKCE verifier); codeVerifier is the matching S256 challenge
// input.
func (p *Provider) AuthURL(state, codeVerifier string) string {
	endpoint := p.oidcProvider.Endpoint()
	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", p.cfg.ClientID)
	q.Set("redirect_uri", p.cfg.RedirectURL)
	q.Set("scope", strings.Join(p.cfg.EffectiveScopes(), " "))
	q.Set("state", state)
	q.Set("code_challenge", codeChallenge(codeVerifier))
	q.Set("code_challenge_method", "S256")
	return endpoint.AuthURL + "?" + q.Encode()
}

// ExchangeCallback exchanges the authorization code from the callback for a
// validated ID token. codeVerifier is the PKCE verifier that matches the
// challenge sent in AuthURL.
func (p *Provider) ExchangeCallback(ctx context.Context, code, codeVerifier string) (*oidc.IDToken, error) {
	endpoint := p.oidcProvider.Endpoint()
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", p.cfg.RedirectURL)
	form.Set("client_id", p.cfg.ClientID)
	form.Set("code_verifier", codeVerifier)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("auth: build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("auth: token exchange: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auth: token exchange: status %d", resp.StatusCode)
	}

	var tokenResp struct {
		IDToken string `json:"id_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("auth: decode token response: %w", err)
	}
	if tokenResp.IDToken == "" {
		return nil, fmt.Errorf("auth: token response missing id_token")
	}
	idToken, err := p.verifier.Verify(ctx, tokenResp.IDToken)
	if err != nil {
		return nil, fmt.Errorf("auth: verify id token: %w", err)
	}
	return idToken, nil
}

// codeChallenge derives the S256 PKCE code challenge from a verifier.
func codeChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// randomToken returns a URL-safe random string of the given byte length.
func randomToken(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failure is fatal; return empty so the error surfaces
		// later rather than panicking.
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}
