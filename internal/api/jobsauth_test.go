package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"cdrom/internal/config"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/idp"
)

// startTestIDP starts an in-process IdP (with job-token audiences) backed by
// in-memory stores, and returns its base URL. It is closed when the test
// finishes.
func startTestIDP(t *testing.T) string {
	t.Helper()
	cfg := config.IdPConfig{
		Issuer:        "http://127.0.0.1:7104",
		KeyLifetime:   time.Hour,
		RotateBefore:  30 * time.Minute,
		TokenLifetime: time.Hour,
		CheckInterval: time.Hour,
		Audiences:     []string{"cdrom-api"},
	}
	km, err := idp.NewKeyManager(cfg, idp.NewMemoryKeyStore(), slog.Default())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := idp.NewServer(cfg, km, idp.NewMemoryAuthCodeStore(), slog.Default())
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// mintTestJobToken mints a job token for jobID from the IdP at base.
func mintTestJobToken(t *testing.T, base, jobID string) string {
	t.Helper()
	form := url.Values{
		"grant_type": {"job_token"},
		"job_id":     {jobID},
	}
	resp, err := http.PostForm(base+"/token", form)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("mint: status %d body %s", resp.StatusCode, body)
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode mint: %v", err)
	}
	return out.AccessToken
}

// TestJobTokenAuthVerify mints a job token from a real IdP and verifies it
// with the API's JobTokenAuth (the same code path used on the gRPC surface).
func TestJobTokenAuthVerify(t *testing.T) {
	base := startTestIDP(t)
	cfg := config.GRPCAuthConfig{
		Enabled:    true,
		IdPAddress: base,
		Audiences:  []string{"cdrom-api"},
	}
	jta, err := NewJobTokenAuth(context.Background(), cfg, nil, slog.Default())
	if err != nil {
		t.Fatalf("NewJobTokenAuth: %v", err)
	}
	if !jta.Enabled() {
		t.Fatal("expected job-token auth to be enabled")
	}

	token := mintTestJobToken(t, base, "42")
	jobID, err := jta.Verify(context.Background(), token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if jobID != 42 {
		t.Errorf("jobID = %d, want 42", jobID)
	}
}

// TestCheckJobToken exercises the gRPC server's job-token check: it rejects a
// missing token, accepts a token scoped to the job, and rejects a token scoped
// to a different job.
func TestCheckJobToken(t *testing.T) {
	base := startTestIDP(t)
	cfg := config.GRPCAuthConfig{
		Enabled:    true,
		IdPAddress: base,
		Audiences:  []string{"cdrom-api"},
	}
	jta, err := NewJobTokenAuth(context.Background(), cfg, nil, slog.Default())
	if err != nil {
		t.Fatalf("NewJobTokenAuth: %v", err)
	}
	s := NewGRPCServer(nil, nil, nil, jta, slog.Default())

	// No token -> Unauthenticated.
	if err := s.checkJobToken(context.Background(), 42); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no token: got %v, want Unauthenticated", err)
	}

	// Valid token for job 42 -> ok.
	token := mintTestJobToken(t, base, "42")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	if err := s.checkJobToken(ctx, 42); err != nil {
		t.Errorf("valid token: %v", err)
	}

	// Token for job 42 but requesting job 7 -> PermissionDenied.
	if err := s.checkJobToken(ctx, 7); status.Code(err) != codes.PermissionDenied {
		t.Errorf("wrong job: got %v, want PermissionDenied", err)
	}
}

// TestCheckJobTokenDisabled confirms the check is a no-op when job-token auth
// is disabled (nil JobTokenAuth).
func TestCheckJobTokenDisabled(t *testing.T) {
	s := NewGRPCServer(nil, nil, nil, nil, slog.Default())
	if err := s.checkJobToken(context.Background(), 42); err != nil {
		t.Errorf("disabled: %v, want nil", err)
	}
}

// TestExchangeJobToken exercises the ExchangeJobToken RPC: with job-token auth
// enabled it rejects a missing token, and with a valid token it mints a new
// token for the requested audience. When disabled it is a no-op.
func TestExchangeJobToken(t *testing.T) {
	base := startTestIDP(t)
	cfg := config.GRPCAuthConfig{
		Enabled:    true,
		IdPAddress: base,
		Audiences:  []string{"cdrom-api"},
	}
	jta, err := NewJobTokenAuth(context.Background(), cfg, nil, slog.Default())
	if err != nil {
		t.Fatalf("NewJobTokenAuth: %v", err)
	}
	s := NewGRPCServer(nil, nil, nil, jta, slog.Default())

	// No token -> Unauthenticated.
	_, err = s.ExchangeJobToken(context.Background(), &apipb.ExchangeJobTokenRequest{JobId: 42, Audience: "outside-svc"})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("no token: got %v, want Unauthenticated", err)
	}

	// Valid token for job 42 -> a new token for the requested audience.
	token := mintTestJobToken(t, base, "42")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	resp, err := s.ExchangeJobToken(ctx, &apipb.ExchangeJobTokenRequest{JobId: 42, Audience: "outside-svc"})
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if resp.GetToken() == "" {
		t.Fatal("exchange returned an empty token")
	}
	// The exchanged token is a well-formed JWT (three compact-JWS parts). It
	// is scoped to the requested audience, which is not in the API's accepted
	// set, so the API's own Verify would reject it — that is correct, since
	// the token is meant for the outside resource, not the API.
	if parts := strings.Split(resp.GetToken(), "."); len(parts) != 3 {
		t.Errorf("exchanged token is not a compact JWT: %q", resp.GetToken())
	}
}

// TestExchangeJobTokenDisabled confirms the RPC is a no-op when job-token auth
// is disabled (nil JobTokenAuth).
func TestExchangeJobTokenDisabled(t *testing.T) {
	s := NewGRPCServer(nil, nil, nil, nil, slog.Default())
	resp, err := s.ExchangeJobToken(context.Background(), &apipb.ExchangeJobTokenRequest{JobId: 42, Audience: "outside-svc"})
	if err != nil {
		t.Errorf("disabled: %v, want nil", err)
	}
	if resp.GetToken() != "" {
		t.Errorf("disabled: token = %q, want empty", resp.GetToken())
	}
}
