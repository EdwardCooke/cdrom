package idp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	oidc "github.com/coreos/go-oidc/v3/oidc"

	"cdrom/internal/config"
)

// jobTestConfig returns a valid IdP config with job-token audiences.
func jobTestConfig() config.IdPConfig {
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Audiences = []string{"cdrom-api"}
	return cfg
}

// mintJobToken posts a job_token grant to the IdP's token endpoint using the
// given HTTP client and returns the minted token. audience may be empty (the
// IdP then stamps its default job-token audiences).
func mintJobToken(t *testing.T, client *http.Client, tokenURL, audience, jobID string) (string, int, error) {
	t.Helper()
	form := url.Values{
		"grant_type": {"job_token"},
		"job_id":     {jobID},
	}
	if audience != "" {
		form.Set("audience", audience)
	}
	resp, err := client.PostForm(tokenURL, form)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", resp.StatusCode, &tokenError{status: resp.StatusCode, body: string(body)}
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", resp.StatusCode, err
	}
	return out.AccessToken, resp.StatusCode, nil
}

// TestE2E_MintJobToken mints a job token via the job_token grant and verifies
// it with go-oidc (the same library the API uses), checking the job-scoped
// claims and audience.
func TestE2E_MintJobToken(t *testing.T) {
	cfg := jobTestConfig()
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx := context.Background()
	token, _, err := mintJobToken(t, http.DefaultClient, ts.URL+"/token", "", "42")
	if err != nil {
		t.Fatalf("mint job token: %v", err)
	}
	if token == "" {
		t.Fatal("minted an empty token")
	}

	provider, err := oidc.NewProvider(ctx, ts.URL)
	if err != nil {
		t.Fatalf("oidc discovery: %v", err)
	}
	verifier := provider.Verifier(&oidc.Config{SkipClientIDCheck: true})
	idt, err := verifier.Verify(ctx, token)
	if err != nil {
		t.Fatalf("verify job token: %v", err)
	}
	if !containsString(idt.Audience, "cdrom-api") {
		t.Errorf("audience = %v, want to contain cdrom-api", idt.Audience)
	}
	var claims struct {
		JobID     string `json:"job_id"`
		Group     string `json:"target_group"`
		TokenType string `json:"token_type"`
	}
	if err := idt.Claims(&claims); err != nil {
		t.Fatalf("claims: %v", err)
	}
	if claims.JobID != "42" {
		t.Errorf("job_id = %q, want 42", claims.JobID)
	}
	if claims.TokenType != "job" {
		t.Errorf("token_type = %q, want job", claims.TokenType)
	}
}

// TestJobTokenAudienceOverride mints a job token with a specific audience and
// confirms the token is stamped for that audience (the exchange path a job
// uses to get a token for an outside resource).
func TestJobTokenAudienceOverride(t *testing.T) {
	cfg := jobTestConfig()
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx := context.Background()
	token, _, err := mintJobToken(t, http.DefaultClient, ts.URL+"/token", "outside-svc", "7")
	if err != nil {
		t.Fatalf("mint job token: %v", err)
	}
	provider, err := oidc.NewProvider(ctx, ts.URL)
	if err != nil {
		t.Fatalf("oidc discovery: %v", err)
	}
	verifier := provider.Verifier(&oidc.Config{SkipClientIDCheck: true})
	idt, err := verifier.Verify(ctx, token)
	if err != nil {
		t.Fatalf("verify job token: %v", err)
	}
	if len(idt.Audience) != 1 || idt.Audience[0] != "outside-svc" {
		t.Errorf("audience = %v, want [outside-svc]", idt.Audience)
	}
	var claims struct {
		JobID string `json:"job_id"`
	}
	if err := idt.Claims(&claims); err != nil {
		t.Fatalf("claims: %v", err)
	}
	if claims.JobID != "7" {
		t.Errorf("job_id = %q, want 7", claims.JobID)
	}
}

// TestJobTokenRequiresClientCert confirms that when the IdP serves TLS, the
// job_token grant rejects a client that did not present a client certificate
// and accepts one that did (the API's mTLS identity).
func TestJobTokenRequiresClientCert(t *testing.T) {
	cfg := jobTestConfig()
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())

	serverTLS, clientWithCert, clientNoCert := genTestTLS(t)
	ts := httptest.NewUnstartedServer(srv.Handler())
	ts.TLS = serverTLS
	ts.StartTLS()
	defer ts.Close()

	// A client without a client certificate is rejected.
	if _, code, err := mintJobToken(t, clientNoCert, ts.URL+"/token", "", "42"); err == nil {
		t.Error("expected an error for a client without a client certificate")
	} else if code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", code, http.StatusUnauthorized)
	}

	// A client with a CA-signed client certificate is accepted.
	token, _, err := mintJobToken(t, clientWithCert, ts.URL+"/token", "", "42")
	if err != nil {
		t.Fatalf("mint with client cert: %v", err)
	}
	if token == "" {
		t.Fatal("minted an empty token")
	}
}

// genTestTLS generates a CA plus a server and a client certificate, and
// returns the IdP's TLS server config (RequestClientCert) and two HTTP
// clients: one presenting the client certificate and one that does not.
func genTestTLS(t *testing.T) (*tls.Config, *http.Client, *http.Client) {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen ca key: %v", err)
	}
	caTmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cdrom-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, &caTmpl, &caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	mkCert := func(cn string, eku []x509.ExtKeyUsage) tls.Certificate {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("gen key: %v", err)
		}
		tmpl := x509.Certificate{
			SerialNumber: big.NewInt(2),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage:  eku,
			DNSNames:     []string{"localhost"},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		}
		der, err := x509.CreateCertificate(rand.Reader, &tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("create cert: %v", err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}

	serverCert := mkCert("cdrom-idp", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCert := mkCert("cdrom-api", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequestClientCert,
		MinVersion:   tls.VersionTLS12,
	}
	clientWithCert := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      caPool,
				Certificates: []tls.Certificate{clientCert},
				MinVersion:   tls.VersionTLS12,
			},
		},
	}
	clientNoCert := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    caPool,
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	return serverTLS, clientWithCert, clientNoCert
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
