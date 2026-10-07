package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"cdrom/internal/config"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
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
	s := NewGRPCServer(nil, nil, nil, jta, nil, slog.Default())

	// No token -> Unauthenticated.
	if err := s.checkJobToken(context.Background(), 42, false); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no token: got %v, want Unauthenticated", err)
	}

	// Valid token for job 42 -> ok.
	token := mintTestJobToken(t, base, "42")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	if err := s.checkJobToken(ctx, 42, false); err != nil {
		t.Errorf("valid token: %v", err)
	}

	// Token for job 42 but requesting job 7 -> PermissionDenied.
	if err := s.checkJobToken(ctx, 7, false); status.Code(err) != codes.PermissionDenied {
		t.Errorf("wrong job: got %v, want PermissionDenied", err)
	}
}

// TestCheckJobTokenDisabled confirms the check is a no-op when job-token auth
// is disabled (nil JobTokenAuth).
func TestCheckJobTokenDisabled(t *testing.T) {
	s := NewGRPCServer(nil, nil, nil, nil, nil, slog.Default())
	if err := s.checkJobToken(context.Background(), 42, false); err != nil {
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
	s := NewGRPCServer(nil, nil, nil, jta, nil, slog.Default())

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
	s := NewGRPCServer(nil, nil, nil, nil, nil, slog.Default())
	resp, err := s.ExchangeJobToken(context.Background(), &apipb.ExchangeJobTokenRequest{JobId: 42, Audience: "outside-svc"})
	if err != nil {
		t.Errorf("disabled: %v, want nil", err)
	}
	if resp.GetToken() != "" {
		t.Errorf("disabled: token = %q, want empty", resp.GetToken())
	}
}

// fakeDB is a dbpb.DatabaseClient that returns a fixed job from GetJob and
// fails every other call. It backs the job-status gate in checkJobToken: the
// gate only ever calls GetJob.
type fakeDB struct {
	dbpb.DatabaseClient
	job *dbpb.Job
}

func (f *fakeDB) GetJob(ctx context.Context, in *dbpb.GetJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	if f.job != nil && f.job.GetId() == in.GetId() {
		return f.job, nil
	}
	return nil, status.Errorf(codes.NotFound, "job %d not found", in.GetId())
}

// jobTokenExpSeconds decodes a compact JWT's payload and returns its exp claim
// (unix seconds). It is used to assert the lifetime stamped on a minted token.
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

// TestCheckJobTokenStatusGate confirms that a job token is only accepted while
// its job is still active (pending or running): once the job reaches a
// terminal state in the database, the token is rejected even though its exp
// claim (a long placeholder) has not passed. This is what invalidates a job's
// token when the job is no longer running.
func TestCheckJobTokenStatusGate(t *testing.T) {
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
	token := mintTestJobToken(t, base, "42")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))

	cases := []struct {
		name    string
		status  dbpb.JobStatus
		wantErr codes.Code
	}{
		{"running", dbpb.JobStatus_JOB_STATUS_RUNNING, codes.OK},
		{"pending", dbpb.JobStatus_JOB_STATUS_PENDING, codes.OK},
		{"succeeded", dbpb.JobStatus_JOB_STATUS_SUCCEEDED, codes.PermissionDenied},
		{"failed", dbpb.JobStatus_JOB_STATUS_FAILED, codes.PermissionDenied},
		{"cancelled", dbpb.JobStatus_JOB_STATUS_CANCELLED, codes.PermissionDenied},
		{"timed_out", dbpb.JobStatus_JOB_STATUS_TIMED_OUT, codes.PermissionDenied},
		{"skipped", dbpb.JobStatus_JOB_STATUS_SKIPPED, codes.PermissionDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &fakeDB{job: &dbpb.Job{Id: 42, Status: tc.status}}
			s := NewGRPCServer(db, nil, nil, jta, nil, slog.Default())
			err := s.checkJobToken(ctx, 42, false)
			if tc.wantErr == codes.OK {
				if err != nil {
					t.Fatalf("checkJobToken: %v, want ok", err)
				}
				return
			}
			if status.Code(err) != tc.wantErr {
				t.Fatalf("checkJobToken: got %v, want %v", status.Code(err), tc.wantErr)
			}
		})
	}
}

// TestCheckJobTokenAllowTerminal confirms that allowTerminal relaxes the
// status gate: a read-only poll (the approval gate's CheckApproval) must keep
// working after a rejection has moved the job to a terminal state, so a token
// for a finished job is accepted when allowTerminal is true and rejected when
// it is false.
func TestCheckJobTokenAllowTerminal(t *testing.T) {
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
	token := mintTestJobToken(t, base, "42")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))

	// A failed job (terminal): the token is rejected by default but accepted
	// when allowTerminal is set.
	failed := NewGRPCServer(&fakeDB{job: &dbpb.Job{Id: 42, Status: dbpb.JobStatus_JOB_STATUS_FAILED}}, nil, nil, jta, nil, slog.Default())
	if err := failed.checkJobToken(ctx, 42, false); status.Code(err) != codes.PermissionDenied {
		t.Errorf("failed job, allowTerminal=false: got %v, want PermissionDenied", err)
	}
	if err := failed.checkJobToken(ctx, 42, true); err != nil {
		t.Errorf("failed job, allowTerminal=true: %v, want ok", err)
	}

	// A running job (non-terminal): the token is accepted either way.
	running := NewGRPCServer(&fakeDB{job: &dbpb.Job{Id: 42, Status: dbpb.JobStatus_JOB_STATUS_RUNNING}}, nil, nil, jta, nil, slog.Default())
	if err := running.checkJobToken(ctx, 42, false); err != nil {
		t.Errorf("running job, allowTerminal=false: %v, want ok", err)
	}
	if err := running.checkJobToken(ctx, 42, true); err != nil {
		t.Errorf("running job, allowTerminal=true: %v, want ok", err)
	}
}

// TestExchangeJobTokenExpiresIn confirms the exchanged token's lifetime is the
// request's expires_in when set, and the default (a short window) when unset.
// The main job token is unaffected (it keeps its long placeholder lifetime).
func TestExchangeJobTokenExpiresIn(t *testing.T) {
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
	// A running job so the status gate in checkJobToken passes.
	db := &fakeDB{job: &dbpb.Job{Id: 42, Status: dbpb.JobStatus_JOB_STATUS_RUNNING}}
	s := NewGRPCServer(db, nil, nil, jta, nil, slog.Default())

	token := mintTestJobToken(t, base, "42")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	now := time.Now().Unix()

	// A custom lifetime is honored.
	resp, err := s.ExchangeJobToken(ctx, &apipb.ExchangeJobTokenRequest{
		JobId:     42,
		Audience:  "outside-svc",
		ExpiresIn: durationpb.New(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("exchange (custom): %v", err)
	}
	exp := jobTokenExpSeconds(t, resp.GetToken())
	if exp < now+29*60 || exp > now+31*60 {
		t.Errorf("custom lifetime: exp = %d, want ~%d (now+30m)", exp, now+30*60)
	}

	// An unset lifetime falls back to the default (a short window, not the
	// main token's long placeholder).
	resp, err = s.ExchangeJobToken(ctx, &apipb.ExchangeJobTokenRequest{JobId: 42, Audience: "outside-svc"})
	if err != nil {
		t.Fatalf("exchange (default): %v", err)
	}
	exp = jobTokenExpSeconds(t, resp.GetToken())
	if exp < now+14*60 || exp > now+16*60 {
		t.Errorf("default lifetime: exp = %d, want ~%d (now+15m)", exp, now+15*60)
	}
}

// TestMainJobTokenLongLifetime confirms the main job token (minted on
// dispatch) carries a long placeholder exp (a week), so it effectively never
// expires for the duration of a long-running job.
func TestMainJobTokenLongLifetime(t *testing.T) {
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
	token, err := jta.Mint(context.Background(), 42, 0, "default", "", "", nil)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	exp := jobTokenExpSeconds(t, token)
	now := time.Now().Unix()
	// A week out (allow a small margin for the time between mint and check).
	if exp < now+(7*24*60*60-60) || exp > now+(7*24*60*60+60) {
		t.Errorf("main token exp = %d, want ~%d (now+7d)", exp, now+7*24*60*60)
	}
}
