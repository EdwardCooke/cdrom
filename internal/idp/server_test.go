package idp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	oidc "github.com/coreos/go-oidc/v3/oidc"
)

// TestE2E_IssuesVerifiableIDTokens drives the full authorization-code + PKCE
// flow against the IdP and verifies the resulting ID token with go-oidc — the
// same library the API uses to validate tokens. This proves the IdP is a
// conforming JWT issuer (discovery, JWKS, and RS256 signatures all check out).
func TestE2E_IssuesVerifiableIDTokens(t *testing.T) {
	cfg := testConfig(time.Hour, 30*time.Minute)
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx := context.Background()

	// The API would do this at startup: OIDC discovery against the issuer.
	provider, err := oidc.NewProvider(ctx, ts.URL)
	if err != nil {
		t.Fatalf("oidc discovery: %v", err)
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: "cdrom-ui"})

	// 1. Authorization: the IdP redirects back with a code.
	authURL := ts.URL + "/auth?" + url.Values{
		"response_type":         {"code"},
		"client_id":             {"cdrom-ui"},
		"redirect_uri":          {"http://localhost:8080/api/auth/callback"},
		"scope":                 {"openid profile email"},
		"state":                 {"st"},
		"code_challenge":        {challengeFor("verifier-123")},
		"code_challenge_method": {"S256"},
		"user":                  {"alice"},
	}.Encode()
	code, err := authorizeCode(t, authURL)
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	// 2. Token: exchange the code (with the PKCE verifier) for an ID token.
	idToken, err := exchangeToken(t, ts.URL+"/token", "cdrom-ui",
		"http://localhost:8080/api/auth/callback", code, "verifier-123")
	if err != nil {
		t.Fatalf("token exchange: %v", err)
	}

	// 3. Verify the ID token exactly as the API would.
	idt, err := verifier.Verify(ctx, idToken)
	if err != nil {
		t.Fatalf("verify id token: %v", err)
	}
	if idt.Subject != "alice" {
		t.Errorf("subject = %q, want alice", idt.Subject)
	}
	var claims struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}
	if err := idt.Claims(&claims); err != nil {
		t.Fatalf("claims: %v", err)
	}
	if claims.Email != "alice@example.com" {
		t.Errorf("email = %q, want alice@example.com", claims.Email)
	}
}

// TestE2E_RotationStillVerifies rotates the signing key and confirms a token
// signed with the new key still verifies (the JWKS now serves the new key).
func TestE2E_RotationStillVerifies(t *testing.T) {
	cfg := testConfig(500*time.Millisecond, 300*time.Millisecond)
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx := context.Background()
	provider, err := oidc.NewProvider(ctx, ts.URL)
	if err != nil {
		t.Fatalf("oidc discovery: %v", err)
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: "cdrom-ui"})

	// Force a rotation so the current key differs from the one at discovery.
	time.Sleep(250 * time.Millisecond)
	km.RotateIfNeeded()

	code, err := authorizeCode(t, ts.URL+"/auth?response_type=code&client_id=cdrom-ui&redirect_uri=http://localhost:8080/api/auth/callback&user=bob")
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	idToken, err := exchangeToken(t, ts.URL+"/token", "cdrom-ui",
		"http://localhost:8080/api/auth/callback", code, "")
	if err != nil {
		t.Fatalf("token exchange: %v", err)
	}
	idt, err := verifier.Verify(ctx, idToken)
	if err != nil {
		t.Fatalf("verify after rotation: %v", err)
	}
	if idt.Subject != "bob" {
		t.Errorf("subject = %q, want bob", idt.Subject)
	}
}

// noRedirectClient does not follow redirects, so the test can read the
// Location header the IdP sets on the authorization response.
var noRedirectClient = &http.Client{
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// authorizeCode performs the authorization request and returns the code the
// IdP redirects back with.
func authorizeCode(t *testing.T, authURL string) (string, error) {
	t.Helper()
	resp, err := noRedirectClient.Get(authURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	loc := resp.Header.Get("Location")
	if loc == "" {
		return "", io.ErrUnexpectedEOF
	}
	u, err := url.Parse(loc)
	if err != nil {
		return "", err
	}
	return u.Query().Get("code"), nil
}

// exchangeToken posts the authorization code to the token endpoint and returns
// the ID token.
func exchangeToken(t *testing.T, tokenURL, clientID, redirectURI, code, verifier string) (string, error) {
	t.Helper()
	form := url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"client_id":    {clientID},
		"redirect_uri": {redirectURI},
	}
	if verifier != "" {
		form.Set("code_verifier", verifier)
	}
	resp, err := http.PostForm(tokenURL, form)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", &tokenError{status: resp.StatusCode, body: string(body)}
	}
	var out struct {
		IDToken string `json:"id_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.IDToken, nil
}

type tokenError struct {
	status int
	body   string
}

func (e *tokenError) Error() string {
	return "token endpoint: status " + itoa(e.status) + " body " + e.body
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// challengeFor derives the S256 PKCE challenge for a verifier.
func challengeFor(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
