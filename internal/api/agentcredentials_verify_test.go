package api

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/config"
	"cdrom/internal/idp"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// quietLog silences IdP chatter during the test.
func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeCredDB is a Database client answering ONLY GetAgentCredential (the
// verifier's row-liveness lookup); any other method panics via the embedded
// nil interface (sentinel).
type fakeCredDB struct {
	dbpb.DatabaseClient
	row *dbpb.AgentCredential
	err error
}

func (f *fakeCredDB) GetAgentCredential(_ context.Context, in *dbpb.GetAgentCredentialRequest, _ ...grpc.CallOption) (*dbpb.AgentCredential, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.row == nil || f.row.GetId() != in.GetId() {
		return nil, status.Error(codes.NotFound, "agent credential not found: "+in.GetId())
	}
	return f.row, nil
}

// startTestIDPCredentials stands up an IN-PROCESS IdP whose HTTP surface serves
// JWKS + discovery and whose gRPC surface carries the agent-credential ops
// (memory store). The HTTP listener is bound explicitly so the discovery
// document's issuer (advertised as scheme://host) EXACTLY equals the static
// issuer stamped onto signed credentials (cfg.EffectiveIssuer()) — go-oidc
// enforces iss equality, so the two must agree.
//
// Returns the HTTP base URL (for discovery), a gRPC client (to issue a real
// signed credential), and the aligned config.
func startTestIDPCredentials(t *testing.T) (string, idppb.IdPClient, config.IdPConfig) {
	t.Helper()

	// HTTP (OIDC) listener on a known port.
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("http listen: %v", err)
	}
	base := "http://" + httpLis.Addr().String()

	cfg := config.IdPConfig{
		Issuer:            base, // == discovery-advertised issuer (aligned)
		KeyLifetime:       time.Hour,
		RotateBefore:      30 * time.Minute,
		TokenLifetime:     time.Hour,
		CheckInterval:     time.Hour,
		Audiences:         []string{"cdrom-api"},
		LocalLoginEnabled: true,
	}

	km, err := idp.NewKeyManager(cfg, idp.NewMemoryKeyStore(), quietLog())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	httpSrv := idp.NewServer(cfg, km, idp.NewMemoryAuthCodeStore(), quietLog())
	hs := &http.Server{Handler: httpSrv.Handler()}
	go func() { _ = hs.Serve(httpLis) }()
	t.Cleanup(func() { _ = hs.Shutdown(context.Background()) })

	// gRPC listener for the credential RPCs.
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("grpc listen: %v", err)
	}
	gs := grpc.NewServer()
	ig := idp.NewGRPCServer(cfg, km, nil, quietLog())
	ig.SetAgentCredentialOps(idp.NewAgentCredentialOps(cfg, km, idp.NewMemoryAgentCredentialStore(), quietLog()))
	idppb.RegisterIdPServer(gs, ig)
	go func() { _ = gs.Serve(grpcLis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(grpcLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return base, idppb.NewIdPClient(conn), cfg
}

// credVerifierServer builds an API test server with the agent-credential
// client AND verifier attached (either may be nil).
func credVerifierServer(t *testing.T, fc AgentCredentialClient, v *AgentCredentialVerifier) *httptest.Server {
	t.Helper()
	srv := New(Clients{}, nil)
	if fc != nil {
		srv.SetAgentCredentialClient(fc)
	}
	if v != nil {
		srv.SetAgentCredentialVerifier(v)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// TestAgentCredentialSelfServeRenew is the Slice 3 centerpiece: an agent
// renews its OWN credential by presenting it as the Bearer token (verified
// against the real IdP's JWKS + the row), and the arms are mutually exclusive
// — a failed machine presentation is judged 401 (NO RBAC fall-through), while
// a caller offering no Bearer token takes the RBAC arm.
func TestAgentCredentialSelfServeRenew(t *testing.T) {
	base, idpClient, _ := startTestIDPCredentials(t)

	ctx := context.Background()
	issued, err := idpClient.IssueAgentCredential(ctx, &idppb.IssueAgentCredentialRequest{
		AgentName: "edge-box-1", IssuedBy: "bootstrap",
	})
	if err != nil {
		t.Fatalf("IssueAgentCredential: %v", err)
	}
	row := issued.GetCredential()
	credID := row.GetId()
	jwt := issued.GetJwt()
	if jwt == "" {
		t.Fatal("issue response carried no JWT")
	}

	newVerifier := func(db dbpb.DatabaseClient) *AgentCredentialVerifier {
		v, err := NewAgentCredentialVerifier(ctx,
			config.GRPCAuthConfig{IdPAddress: base, Audiences: []string{"cdrom-api"}},
			nil, db, quietLog())
		if err != nil {
			t.Fatalf("NewAgentCredentialVerifier: %v", err)
		}
		return v
	}

	// A healthy row (live, unexpired, unrevoked) straight from the issue.
	healthyDB := &fakeCredDB{row: row}
	// A REVOKED twin: same id/name/deadline, but stamped revoked.
	revokedRow := &dbpb.AgentCredential{
		Id:        row.GetId(),
		AgentName: row.GetAgentName(),
		ExpiresAt: row.ExpiresAt,
		RevokedAt: timestamppb.New(time.Now().UTC()),
	}
	revokedDB := &fakeCredDB{row: revokedRow}
	// AN UNKNOWN row: the lookup misses (NotFound).
	unknownDB := &fakeCredDB{}

	primaryFC := &fakeAgentCred{}
	revokedFC := &fakeAgentCred{}
	unknownFC := &fakeAgentCred{}
	primary := credVerifierServer(t, primaryFC, newVerifier(healthyDB))
	revokedSrv := credVerifierServer(t, revokedFC, newVerifier(revokedDB))
	unknownSrv := credVerifierServer(t, unknownFC, newVerifier(unknownDB))

	post := func(ts *httptest.Server, token, path string) int {
		req, err := http.NewRequest(http.MethodPost, ts.URL+path, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		io.CopyN(io.Discard, res.Body, 1<<20)
		res.Body.Close()
		return res.StatusCode
	}

	ownPath := "/api/agents/edge-box-1/credentials/" + credID + "/renew"
	otherNamePath := "/api/agents/other-box/credentials/" + credID + "/renew"
	otherIDPath := "/api/agents/edge-box-1/credentials/not-that-id/renew"

	// 1. SELF-SERVE: the agent presents its own credential for its own
	//    name+id ⇒ 200, and the renew is forwarded.
	if code := post(primary, jwt, ownPath); code != http.StatusOK {
		t.Errorf("self-serve renew status = %d, want 200", code)
	}
	if primaryFC.renewedID != credID {
		t.Errorf("renew forwarded id = %q, want %q", primaryFC.renewedID, credID)
	}

	// 2. CROSS-NAME: the same credential presented for a DIFFERENT agent's
	//    path ⇒ 401 (binding fails; no RBAC fall-through).
	if code := post(primary, jwt, otherNamePath); code != http.StatusUnauthorized {
		t.Errorf("cross-name renew status = %d, want 401", code)
	}

	// 3. CROSS-ID: the credential presented for a DIFFERENT credential id ⇒
	//    401.
	if code := post(primary, jwt, otherIDPath); code != http.StatusUnauthorized {
		t.Errorf("cross-id renew status = %d, want 401", code)
	}

	// 4. GARBLE: a nonsense Bearer token ⇒ 401 (judged on its merits).
	if code := post(primary, "garbage.not.a.jwt", ownPath); code != http.StatusUnauthorized {
		t.Errorf("garble renew status = %d, want 401", code)
	}

	// 5. REVOKED ROW: the signature is valid but the row is revoked ⇒ 401,
	//    and NOTHING is forwarded.
	if code := post(revokedSrv, jwt, ownPath); code != http.StatusUnauthorized {
		t.Errorf("revoked-row renew status = %d, want 401", code)
	}
	if revokedFC.renewedID != "" {
		t.Errorf("revoked-row renew forwarded id = %q, want none", revokedFC.renewedID)
	}

	// 6. UNKNOWN ROW: valid signature, but the row lookup is NotFound ⇒ 401,
	//    and NOTHING is forwarded.
	if code := post(unknownSrv, jwt, ownPath); code != http.StatusUnauthorized {
		t.Errorf("unknown-row renew status = %d, want 401", code)
	}
	if unknownFC.renewedID != "" {
		t.Errorf("unknown-row renew forwarded id = %q, want none", unknownFC.renewedID)
	}

	// 7. ADMIN FALLBACK: no Bearer token at all ⇒ the RBAC arm. With RBAC
	//    DISABLED (no SetAuthz) the caller is a synthetic admin ⇒ 200.
	if code := post(primary, "", ownPath); code != http.StatusOK {
		t.Errorf("admin (no bearer) renew status = %d, want 200", code)
	}
}

// TestAgentCredentialVerifierDirect exercises the verifier's Verify() in
// isolation: a genuine credential passes (jti + name recovered); a FORGED
// signature (minted by a different IdP key ring) is rejected.
func TestAgentCredentialVerifierDirect(t *testing.T) {
	base, idpClient, cfg := startTestIDPCredentials(t)
	ctx := context.Background()

	issued, err := idpClient.IssueAgentCredential(ctx, &idppb.IssueAgentCredentialRequest{
		AgentName: "solo", IssuedBy: "x",
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	row := issued.GetCredential()

	v, err := NewAgentCredentialVerifier(ctx,
		config.GRPCAuthConfig{IdPAddress: base, Audiences: []string{"cdrom-api"}},
		nil, &fakeCredDB{row: row}, quietLog())
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	jti, name, err := v.Verify(ctx, issued.GetJwt())
	if err != nil {
		t.Fatalf("Verify(genuine): %v", err)
	}
	if jti != row.GetId() || name != "solo" {
		t.Errorf("Verify = (jti %q, name %q), want (%q, solo)", jti, name, row.GetId())
	}

	// A FORGED token: signed by a FOREIGN key ring (a second IdP), mirroring
	// the genuine claims shape ⇒ the JWKS check must reject it (unknown kid).
	foreignKM, err := idp.NewKeyManager(cfg, idp.NewMemoryKeyStore(), quietLog())
	if err != nil {
		t.Fatalf("foreign keymanager: %v", err)
	}
	now := time.Now()
	forged, err := foreignKM.Sign(map[string]any{
		"iss": cfg.EffectiveIssuer(),
		"sub": "agent:solo",
		"aud": []string{"cdrom-api"},
		"jti": row.GetId(),
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("forge sign: %v", err)
	}
	if _, _, err := v.Verify(ctx, forged); err == nil {
		t.Error("Verify(forged) succeeded, want a signature rejection")
	}
}
