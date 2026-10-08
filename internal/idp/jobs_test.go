package idp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	oidc "github.com/coreos/go-oidc/v3/oidc"

	"cdrom/internal/config"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// jobTestConfig returns a valid IdP config with job-token audiences.
func jobTestConfig() config.IdPConfig {
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Audiences = []string{"cdrom-api"}
	return cfg
}

// testIDP is an in-process IdP: an HTTP server (the OIDC surface, used to
// verify minted tokens via discovery + JWKS) and a gRPC server (the API-only
// surface, used to mint job tokens and manage users).
type testIDP struct {
	httpURL string
	client  idppb.IdPClient
	users   UserStore
}

// startTestIDP starts an in-process IdP with the given config and user store
// (nil for no user store) and returns its HTTP base URL (for token
// verification) and gRPC client (for minting / user management). Both are
// closed when the test finishes.
func startTestIDP(t *testing.T, cfg config.IdPConfig, users UserStore) *testIDP {
	t.Helper()
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	// Align the configured issuer with the test server's URL so a token minted
	// over gRPC (iss = the configured issuer) verifies against the test
	// server's discovery document (iss = the test server's URL).
	cfg.Issuer = ts.URL
	grpcSrv := grpc.NewServer()
	idppb.RegisterIdPServer(grpcSrv, NewGRPCServer(cfg, km, users, testLogger()))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go grpcSrv.Serve(lis)
	t.Cleanup(grpcSrv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testIDP{httpURL: ts.URL, client: idppb.NewIdPClient(conn), users: users}
}

// mintJobToken mints a job token for jobID from the IdP over gRPC. audience
// may be empty (the IdP then stamps its default job-token audiences).
func mintJobToken(t *testing.T, idp *testIDP, audience, jobID string) string {
	t.Helper()
	resp, err := idp.client.MintJobToken(context.Background(), &idppb.MintJobTokenRequest{
		JobId:    jobID,
		Audience: audience,
	})
	if err != nil {
		t.Fatalf("mint job token: %v", err)
	}
	return resp.GetAccessToken()
}

// TestE2E_MintJobToken mints a job token over gRPC and verifies it with
// go-oidc (the same library the API uses), checking the job-scoped claims and
// audience.
func TestE2E_MintJobToken(t *testing.T) {
	idp := startTestIDP(t, jobTestConfig(), nil)
	ctx := context.Background()
	token := mintJobToken(t, idp, "", "42")
	if token == "" {
		t.Fatal("minted an empty token")
	}

	provider, err := oidc.NewProvider(ctx, idp.httpURL)
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
	idp := startTestIDP(t, jobTestConfig(), nil)
	ctx := context.Background()
	token := mintJobToken(t, idp, "outside-svc", "7")

	provider, err := oidc.NewProvider(ctx, idp.httpURL)
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

// TestJobTokenTriggerAndUpstreamClaims mints a job token carrying the trigger
// that started the run and the upstream OIDC claims a webhook caller
// presented, and confirms the token is stamped with trigger_name,
// trigger_type, and the upstream claims (prefixed with upstream_) (F-09).
func TestJobTokenTriggerAndUpstreamClaims(t *testing.T) {
	idp := startTestIDP(t, jobTestConfig(), nil)
	ctx := context.Background()
	// The API passes the upstream claims JSON-encoded as a JSON object. A
	// claim that is itself an object (job_config) keeps its structure.
	upstream, _ := json.Marshal(map[string]any{
		"org":        "acme",
		"user":       "alice",
		"job_config": map[string]any{"url": "https://gitlab.example.com/policy.yml", "sha": "abc"},
	})
	resp, err := idp.client.MintJobToken(ctx, &idppb.MintJobTokenRequest{
		JobId:          "42",
		TriggerName:    "github-push",
		TriggerType:    "webhook",
		UpstreamClaims: string(upstream),
	})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	provider, err := oidc.NewProvider(ctx, idp.httpURL)
	if err != nil {
		t.Fatalf("oidc discovery: %v", err)
	}
	verifier := provider.Verifier(&oidc.Config{SkipClientIDCheck: true})
	idt, err := verifier.Verify(ctx, resp.GetAccessToken())
	if err != nil {
		t.Fatalf("verify job token: %v", err)
	}
	var claims struct {
		TriggerName    string `json:"trigger_name"`
		TriggerType    string `json:"trigger_type"`
		UpstreamOrg    string `json:"upstream_org"`
		UpstreamUser   string `json:"upstream_user"`
		UpstreamJobCfg struct {
			URL string `json:"url"`
			SHA string `json:"sha"`
		} `json:"upstream_job_config"`
	}
	if err := idt.Claims(&claims); err != nil {
		t.Fatalf("claims: %v", err)
	}
	if claims.TriggerName != "github-push" {
		t.Errorf("trigger_name = %q, want github-push", claims.TriggerName)
	}
	if claims.TriggerType != "webhook" {
		t.Errorf("trigger_type = %q, want webhook", claims.TriggerType)
	}
	if claims.UpstreamOrg != "acme" {
		t.Errorf("upstream_org = %q, want acme", claims.UpstreamOrg)
	}
	if claims.UpstreamUser != "alice" {
		t.Errorf("upstream_user = %q, want alice", claims.UpstreamUser)
	}
	// A complex upstream claim (an object) is stamped with its structure
	// intact, not flattened to a string.
	if claims.UpstreamJobCfg.SHA != "abc" {
		t.Errorf("upstream_job_config.sha = %q, want abc", claims.UpstreamJobCfg.SHA)
	}
	if claims.UpstreamJobCfg.URL != "https://gitlab.example.com/policy.yml" {
		t.Errorf("upstream_job_config.url = %q, want the policy url", claims.UpstreamJobCfg.URL)
	}
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// jobTokenExpSeconds decodes a compact JWT's payload and returns its exp claim
// (unix seconds).
func jobTokenExpSeconds(t *testing.T, token string) int64 {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token is not a compact JWT: %q", token)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	return claims.Exp
}

// TestJobTokenExpiresIn confirms the gRPC MintJobToken honors the request's
// expires_in (a duration string): the token's exp is stamped to now +
// expires_in. This is how the API gives the main job token a long placeholder
// lifetime (a week) and an exchanged token a short one.
func TestJobTokenExpiresIn(t *testing.T) {
	idp := startTestIDP(t, jobTestConfig(), nil)
	ctx := context.Background()
	now := time.Now().Unix()

	// A custom lifetime is honored.
	resp, err := idp.client.MintJobToken(ctx, &idppb.MintJobTokenRequest{JobId: "42", ExpiresIn: time.Hour.String()})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	exp := jobTokenExpSeconds(t, resp.GetAccessToken())
	if exp < now+3590 || exp > now+3610 {
		t.Errorf("custom lifetime: exp = %d, want ~%d (now+1h)", exp, now+3600)
	}
	if resp.GetExpiresIn() != 3600 {
		t.Errorf("response expires_in = %d, want 3600", resp.GetExpiresIn())
	}

	// Without expires_in the IdP falls back to its configured token lifetime.
	resp2, err := idp.client.MintJobToken(ctx, &idppb.MintJobTokenRequest{JobId: "43"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	exp = jobTokenExpSeconds(t, resp2.GetAccessToken())
	// jobTestConfig sets TokenLifetime to an hour.
	if exp < now+3590 || exp > now+3610 {
		t.Errorf("default lifetime: exp = %d, want ~%d (now+1h)", exp, now+3600)
	}
}
