package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"cdrom/internal/config"
)

// --- fake OIDC identity provider for tests ---

// fakeIDP is a minimal in-process OIDC provider: it serves the discovery
// document, a JWKS with a freshly generated RSA key, and a token endpoint that
// mints a signed ID token. It lets the login→callback flow be exercised end to
// end without a real identity provider.
type fakeIDP struct {
	server *httptest.Server
	issuer string
	key    *rsa.PrivateKey
	kid    string
}

func newFakeIDP(t *testing.T) *fakeIDP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	kid := "test-key"
	idp := &fakeIDP{key: key, kid: kid}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                idp.issuer,
			"authorization_endpoint":                idp.issuer + "/auth",
			"token_endpoint":                        idp.issuer + "/token",
			"jwks_uri":                              idp.issuer + "/jwks",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jwksJSON(&key.PublicKey, kid)))
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		claims := map[string]any{
			"iss":   idp.issuer,
			"sub":   "test-user",
			"aud":   r.FormValue("client_id"),
			"exp":   time.Now().Add(time.Hour).Unix(),
			"iat":   time.Now().Unix(),
			"name":  "Test User",
			"email": "test@example.com",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "fake-access-token",
			"id_token":     signJWT(t, key, kid, claims),
			"token_type":   "Bearer",
		})
	})

	idp.server = httptest.NewServer(mux)
	idp.issuer = idp.server.URL
	t.Cleanup(idp.server.Close)
	return idp
}

// signJWT builds a compact RS256 JWT signed with key.
func signJWT(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid})
	payload, _ := json.Marshal(claims)
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	hashed := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hashed[:])
	if err != nil {
		t.Fatalf("sign jwt: %v", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// jwksJSON renders the RSA public key as a JWKS document.
func jwksJSON(pub *rsa.PublicKey, kid string) string {
	n := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes())
	b, _ := json.Marshal(map[string]any{
		"keys": []map[string]any{{
			"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256", "n": n, "e": e,
		}},
	})
	return string(b)
}

// testAuthConfig returns a complete, enabled auth config pointing at idp.
func testAuthConfig(idp *fakeIDP) config.AuthConfig {
	return config.AuthConfig{
		Enabled:      true,
		Issuer:       idp.issuer,
		ClientID:     "test-client",
		RedirectURL:  "http://localhost:8080/api/auth/callback",
		CookieSecret: "test-cookie-secret",
	}
}

func TestNewPKCEStateRoundTrip(t *testing.T) {
	state, verifier := NewPKCEState()
	if verifier == "" {
		t.Fatal("empty code verifier")
	}
	if got := CodeVerifierFromState(state); got != verifier {
		t.Errorf("CodeVerifierFromState(%q) = %q, want %q", state, got, verifier)
	}
}

func TestCodeVerifierFromStateMalformed(t *testing.T) {
	if got := CodeVerifierFromState("no-dot-here"); got != "" {
		t.Errorf("CodeVerifierFromState(malformed) = %q, want empty", got)
	}
}

func TestCodeChallenge(t *testing.T) {
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXb"
	sum := sha256.Sum256([]byte(verifier))
	want := base64.RawURLEncoding.EncodeToString(sum[:])
	if got := codeChallenge(verifier); got != want {
		t.Errorf("codeChallenge = %q, want %q", got, want)
	}
}

func TestAuthURL(t *testing.T) {
	idp := newFakeIDP(t)
	p, err := NewProvider(context.Background(), testAuthConfig(idp))
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	state, verifier := NewPKCEState()
	u, err := url.Parse(p.AuthURL(state, verifier))
	if err != nil {
		t.Fatalf("parse auth url: %v", err)
	}
	q := u.Query()
	if q.Get("response_type") != "code" {
		t.Errorf("response_type = %q, want code", q.Get("response_type"))
	}
	if q.Get("client_id") != "test-client" {
		t.Errorf("client_id = %q, want test-client", q.Get("client_id"))
	}
	if q.Get("state") != state {
		t.Errorf("state = %q, want %q", q.Get("state"), state)
	}
	if q.Get("code_challenge_method") != "S256" {
		t.Errorf("code_challenge_method = %q, want S256", q.Get("code_challenge_method"))
	}
	if q.Get("code_challenge") != codeChallenge(verifier) {
		t.Error("code_challenge does not match the verifier")
	}
	if q.Get("redirect_uri") != "http://localhost:8080/api/auth/callback" {
		t.Errorf("redirect_uri = %q", q.Get("redirect_uri"))
	}
}
