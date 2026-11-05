package idp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/audit"
	"cdrom/internal/config"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// --- test certificate fixtures -------------------------------------------

type testPKI struct {
	ca    *x509.Certificate
	caKey *ecdsa.PrivateKey
	trust *x509.CertPool
}

// newTestPKI mints a self-signed CA (trust anchor) for the break-glass
// peer-chain tests.
func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cdrom-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	trust := x509.NewCertPool()
	trust.AddCert(ca)
	return &testPKI{ca: ca, caKey: key, trust: trust}
}

// issueLeaf mints a CA-signed client certificate (the "API's client cert"
// stand-in) with the given CommonName.
func (pki *testPKI) issueLeaf(t *testing.T, cn string) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, pki.ca, &key.PublicKey, pki.caKey)
	if err != nil {
		t.Fatalf("issue leaf: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return leaf
}

// roguePKI is a SECOND, distrusting CA — its leaves must be rejected.
func roguePKI(t *testing.T) *testPKI { return newTestPKI(t) }

// --- audit capture --------------------------------------------------------

type captureSink struct {
	mu  sync.Mutex
	evs []audit.Event
}

func (s *captureSink) Append(_ context.Context, e audit.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evs = append(s.evs, e)
	return nil
}

func (s *captureSink) events() []audit.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]audit.Event(nil), s.evs...)
}

// --- handler fixture ------------------------------------------------------

// bgConfig returns a break-glass-ready IdP config for tests.
func bgConfig(enabled bool) config.IdPConfig {
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.BreakGlassEnabled = enabled
	cfg.BreakGlassDefaultTTL = 24 * time.Hour
	cfg.BreakGlassMaxTTL = 7 * 24 * time.Hour
	cfg.BreakGlassLiveCap = 3
	return cfg
}

// captureHandler is a slog.Handler that records every record so tests can
// assert on log OUTPUT (the break-glass WARN banner, and the absence of the
// plaintext password).
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(name string) slog.Handler       { return h }

// newCaptureLogger returns a *slog.Logger fed by a capturing handler.
func newCaptureLogger() (*slog.Logger, *captureHandler) {
	h := &captureHandler{}
	return slog.New(h), h
}

// blob concatenates every captured record's message + attrs (for containment
// assertions).
func (h *captureHandler) blob() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var sb strings.Builder
	for _, r := range h.records {
		sb.WriteString(r.Message)
		r.Attrs(func(a slog.Attr) bool {
			sb.WriteString(" ")
			sb.WriteString(a.Value.String())
			return true
		})
		sb.WriteByte('\n')
	}
	return sb.String()
}

// breakGlassFixture wires a Server with the break-glass machinery over an
// in-memory user store, a trusting root pool (or nil), and a capturing audit
// sink.
func breakGlassFixture(t *testing.T, enabled bool, roots *x509.CertPool, users UserStore) (*Server, *captureSink) {
	t.Helper()
	cfg := bgConfig(enabled)
	srv := NewServer(cfg, nil, nil, testLogger())
	sink := &captureSink{}
	srv.SetBreakGlass(NewBreakGlassCoiner(cfg, users, testLogger()), users, roots)
	srv.SetBreakGlassAuditor(audit.NewRecorder(nil, sink, nil, testLogger()))
	return srv, sink
}

// bgRequest builds a POST to the break-glass route with the given TLS peer
// state (nil = plaintext/no TLS).
func bgRequest(body string, cs *tls.ConnectionState) *http.Request {
	req := httptest.NewRequest(http.MethodPost, breakGlassRoute, strings.NewReader(body))
	if cs != nil {
		req.TLS = cs
	}
	return req
}

// --- admission ladder -----------------------------------------------------

func TestBreakGlassAdmissionLadder(t *testing.T) {
	pki := newTestPKI(t)
	rogue := roguePKI(t)
	users := NewMemoryUserStore()
	srv, sink := breakGlassFixture(t, true, pki.trust, users)
	ctx := context.Background()

	seed, err := users.Create(ctx, &User{Username: "benign", Email: "benign@example.com", Roles: []string{"user"}, Active: true})
	if err != nil {
		t.Fatalf("seed benign user: %v", err)
	}

	cases := []struct {
		name string
		req  *http.Request
	}{
		{name: "plaintext (no TLS state)", req: bgRequest(`{}`, nil)},
		{name: "TLS but no peer certificate", req: bgRequest(`{}`, &tls.ConnectionState{})},
		{name: "peer cert from ROGUE CA", req: bgRequest(`{}`, &tls.ConnectionState{PeerCertificates: []*x509.Certificate{rogue.issueLeaf(t, "intruder")}})},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		srv.handleEmergencyBreakGlass(rec, tc.req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("[%s] status = %d, want 403", tc.name, rec.Code)
		}
		if rec.Body.String() != breakGlassDenyBody {
			t.Errorf("[%s] body = %q, want the uniform denial", tc.name, rec.Body.String())
		}
	}

	// WRONG METHOD on the trusted route: uniform 403 (route-mounted for all
	// methods so the mux cannot 405-leak existence).
	getReq := httptest.NewRequest(http.MethodGet, breakGlassRoute, nil)
	getReq.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{pki.issueLeaf(t, "ops")}}
	rec := httptest.NewRecorder()
	srv.handleEmergencyBreakGlass(rec, getReq)
	if rec.Code != http.StatusForbidden {
		t.Errorf("GET method status = %d, want 403 (uniform)", rec.Code)
	}

	// DISABLED TOGGLE: identical uniform 403 even WITH a valid certificate
	// (no state is advertised).
	disabledSrv, _ := breakGlassFixture(t, false, pki.trust, NewMemoryUserStore())
	rec = httptest.NewRecorder()
	disabledSrv.handleEmergencyBreakGlass(rec, bgRequest(`{}`, &tls.ConnectionState{PeerCertificates: []*x509.Certificate{pki.issueLeaf(t, "ops")}}))
	if rec.Code != http.StatusForbidden || rec.Body.String() != breakGlassDenyBody {
		t.Errorf("disabled toggle = %d %q, want uniform 403", rec.Code, rec.Body.String())
	}

	// NOTHING was coined: the directory is unchanged (still just the benign
	// user), and no audit event was emitted (denials are logged, not audited
	// as coinage).
	if ls, _ := users.List(ctx, true); len(ls) != 1 || ls[0].ID != seed.ID {
		t.Errorf("directory after denials = %d users, want only the seed", len(ls))
	}
	if n := len(sink.events()); n != 0 {
		t.Errorf("audit events after denials = %d, want 0", n)
	}
}

// --- successful coinage ---------------------------------------------------

func TestBreakGlassCoinsStrictlyAdditiveAdmin(t *testing.T) {
	pki := newTestPKI(t)
	users := NewMemoryUserStore()
	capLog, capHandler := newCaptureLogger()
	cfg := bgConfig(true)
	srv := NewServer(cfg, nil, nil, capLog)
	sink := &captureSink{}
	srv.SetBreakGlass(NewBreakGlassCoiner(cfg, users, capLog), users, pki.trust)
	srv.SetBreakGlassAuditor(audit.NewRecorder(nil, sink, nil, capLog))
	ctx := context.Background()

	before, err := users.List(ctx, true)
	if err != nil {
		t.Fatalf("list before: %v", err)
	}

	rec := httptest.NewRecorder()
	srv.handleEmergencyBreakGlass(rec, bgRequest(`{}`, &tls.ConnectionState{PeerCertificates: []*x509.Certificate{pki.issueLeaf(t, "platform-ops")}}))
	if rec.Code != http.StatusOK {
		t.Fatalf("coin status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if cc := rec.Result().Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}
	var resp breakGlassCoinResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.HasPrefix(resp.Username, "breakglass-") || !strings.HasSuffix(resp.Username, "@invalid") {
		t.Errorf("username = %q, want the breakglass-…@invalid shape", resp.Username)
	}
	if resp.Password == "" {
		t.Error("password empty, want the once-shown plaintext")
	}
	expiresAt, err := time.Parse(time.RFC3339, resp.ExpiresAt)
	if err != nil {
		t.Fatalf("expires_at %q: %v", resp.ExpiresAt, err)
	}
	if delta := expiresAt.Sub(time.Now()); delta < 23*time.Hour || delta > 25*time.Hour {
		t.Errorf("default ttl ≈ %v, want ≈24h", delta)
	}

	// STRICTLY ADDITIVE: exactly one new row; every pre-existing row
	// untouched (diff asserted).
	after, err := users.List(ctx, true)
	if err != nil {
		t.Fatalf("list after: %v", err)
	}
	if len(after) != len(before)+1 {
		t.Fatalf("directory grew by %d, want exactly 1", len(after)-len(before))
	}
	newRows := 0
	for _, u := range after {
		found := false
		for _, b := range before {
			if b.ID == u.ID {
				found = true
				break
			}
		}
		if !found {
			newRows++
			if u.Username != resp.Username {
				t.Errorf("new row username = %q, want the coined %q", u.Username, resp.Username)
			}
			if !u.Active || u.Provisioned || u.DeletedAt != nil {
				t.Errorf("new row state = active:%v provisioned:%v tombstone:%v, want active local non-tombstoned", u.Active, u.Provisioned, u.DeletedAt)
			}
			if len(u.Roles) != 1 || u.Roles[0] != "admin" {
				t.Errorf("new row roles = %v, want [admin]", u.Roles)
			}
			if u.TempPasswordHash == "" || u.TempPasswordValidUntil == nil {
				t.Error("new row lacks the temporal credential")
			}
		}
	}
	if newRows != 1 {
		t.Errorf("new rows = %d, want 1", newRows)
	}

	// AUDIT: the caller's certificate is attributed, the password is NOT.
	evs := sink.events()
	if len(evs) != 1 {
		t.Fatalf("audit events = %d, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Action != audit.ActionBreakGlassCoin {
		t.Errorf("action = %q, want %q", ev.Action, audit.ActionBreakGlassCoin)
	}
	if !strings.HasPrefix(ev.Actor, "break-glass(") || !strings.Contains(ev.Actor, "cn=platform-ops") {
		t.Errorf("actor = %q, want the caller's certificate leaf", ev.Actor)
	}
	if ev.Outcome != audit.OutcomeSuccess {
		t.Errorf("outcome = %q, want success", ev.Outcome)
	}
	if blob := ev.Actor + ev.TargetID + ev.Details; strings.Contains(blob, resp.Password) {
		t.Error("AUDIT RECORD CONTAINS THE PLAINTEXT PASSWORD")
	}

	// The CONSPICUOUS WARN log fired — and the password is absent from the
	// LOGGED output too (record the event, not the secret).
	logged := capHandler.blob()
	if !strings.Contains(logged, "BREAK GLASS INVOCATION") {
		t.Error("no WARN 'BREAK GLASS INVOCATION' log line captured")
	}
	if strings.Contains(logged, resp.Password) {
		t.Error("LOG CAPTURE CONTAINS THE PLAINTEXT PASSWORD")
	}
}

// --- TTL riding the request ------------------------------------------------

func TestBreakGlassTTLRidesRequestClamped(t *testing.T) {
	pki := newTestPKI(t)
	users := NewMemoryUserStore()
	srv, _ := breakGlassFixture(t, true, pki.trust, users)
	cs := &tls.ConnectionState{PeerCertificates: []*x509.Certificate{pki.issueLeaf(t, "ops")}}
	ctx := context.Background()

	// Short request ttl (under the default): honored.
	rec := httptest.NewRecorder()
	srv.handleEmergencyBreakGlass(rec, bgRequest(`{"ttl_seconds":900}`, cs))
	if rec.Code != http.StatusOK {
		t.Fatalf("short ttl status = %d", rec.Code)
	}
	var shortResp breakGlassCoinResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &shortResp)
	shortExp, _ := time.Parse(time.RFC3339, shortResp.ExpiresAt)
	if d := shortExp.Sub(time.Now()); d < 14*time.Minute || d > 16*time.Minute {
		t.Errorf("short ttl ≈ %v, want ≈15m", d)
	}
	shortUser, _ := users.GetByUsername(ctx, shortResp.Username, true)

	// Oversized request ttl: clamped to MAX (7d).
	rec = httptest.NewRecorder()
	srv.handleEmergencyBreakGlass(rec, bgRequest(`{"ttl_seconds":31536000}`, cs))
	if rec.Code != http.StatusOK {
		t.Fatalf("long ttl status = %d", rec.Code)
	}
	var longResp breakGlassCoinResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &longResp)
	longExp, _ := time.Parse(time.RFC3339, longResp.ExpiresAt)
	if d := longExp.Sub(time.Now()); d < 6*24*time.Hour || d > 8*24*time.Hour {
		t.Errorf("clamped ttl ≈ %v, want ≤7d", d)
	}
	longUser, _ := users.GetByUsername(ctx, longResp.Username, true)

	// The WINDOWS persisted on the rows agree with the responses.
	if shortUser.TempPasswordValidUntil == nil || shortUser.TempPasswordValidUntil.Sub(shortExp) > time.Minute {
		t.Errorf("short persisted window %v disagrees with response %v", shortUser.TempPasswordValidUntil, shortExp)
	}
	if longUser.TempPasswordValidUntil == nil || longUser.TempPasswordValidUntil.Sub(longExp) > time.Minute {
		t.Errorf("long persisted window %v disagrees with response %v", longUser.TempPasswordValidUntil, longExp)
	}
}

// --- live-pool cap ---------------------------------------------------------

func TestBreakGlassLivePoolCap(t *testing.T) {
	pki := newTestPKI(t)
	users := NewMemoryUserStore()
	srv, sink := breakGlassFixture(t, true, pki.trust, users)
	ctx := context.Background()
	cs := &tls.ConnectionState{PeerCertificates: []*x509.Certificate{pki.issueLeaf(t, "sprayer")}}

	// Fill the pool (cap 3) with LIVE break-glass admins.
	future := time.Now().Add(time.Hour)
	for i := 0; i < 3; i++ {
		h, _ := HashPassword("", "seeded-temp")
		if _, err := users.Create(ctx, &User{
			Username:               "breakglass-seeded" + string(rune('a'+i)) + "@invalid",
			Roles:                  []string{"admin"},
			Active:                 true,
			TempPasswordHash:       h,
			TempPasswordValidUntil: &future,
		}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	// A FOURTH live coinage is refused with 429.
	rec := httptest.NewRecorder()
	srv.handleEmergencyBreakGlass(rec, bgRequest(`{}`, cs))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("saturated coin status = %d, want 429 (body %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "wait for existing credentials to expire") {
		t.Errorf("429 body = %q, want the saturation explanation", rec.Body.String())
	}
	// Saturation is AUDITED (failure outcome, certificate attributed).
	evs := sink.events()
	if len(evs) != 1 || evs[0].Outcome != audit.OutcomeFailure {
		t.Errorf("saturation audit = %+v, want one failure event", evs)
	}
	if len(evs) == 1 && !strings.Contains(evs[0].Actor, "cn=sprayer") {
		t.Errorf("saturation actor = %q, want the sprayer's cert", evs[0].Actor)
	}

	// Once SOME expire, coinage resumes.
	past := time.Now().Add(-time.Hour)
	for _, u := range mustList(t, users, ctx) {
		if u.isBreakGlass() {
			_ = users.ExpireTemporaryPassword(ctx, u.ID)
			_ = users.Deactivate(ctx, u.ID)
		}
	}
	rec = httptest.NewRecorder()
	srv.handleEmergencyBreakGlass(rec, bgRequest(`{}`, cs))
	if rec.Code != http.StatusOK {
		t.Fatalf("post-expiry coin status = %d, want 200", rec.Code)
	}
	_ = past
}

func mustList(t *testing.T, users UserStore, ctx context.Context) []*User {
	t.Helper()
	us, err := users.List(ctx, true)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return us
}

// --- hygiene sweep ---------------------------------------------------------

func TestBreakGlassSweepRetiresExpired(t *testing.T) {
	users := NewMemoryUserStore()
	srv, _ := breakGlassFixture(t, true, nil, users)
	ctx := context.Background()

	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)
	h, _ := HashPassword("", "swp")
	expired, err := users.Create(ctx, &User{
		Username: "breakglass-swpx@invalid", Roles: []string{"admin"}, Active: true,
		TempPasswordHash: h, TempPasswordValidUntil: &past,
	})
	if err != nil {
		t.Fatalf("create expired: %v", err)
	}
	live, err := users.Create(ctx, &User{
		Username: "breakglass-swpy@invalid", Roles: []string{"admin"}, Active: true,
		TempPasswordHash: h, TempPasswordValidUntil: &future,
	})
	if err != nil {
		t.Fatalf("create live: %v", err)
	}
	stranger, err := users.Create(ctx, &User{Username: "innocent", Email: "innocent@example.com", Roles: []string{"user"}, Active: true})
	if err != nil {
		t.Fatalf("create stranger: %v", err)
	}

	swept := srv.SweepExpiredBreakGlass(ctx)
	if swept != 1 {
		t.Fatalf("swept = %d, want 1 (only the expired break-glass user)", swept)
	}
	expiredAfter, _ := users.GetByID(ctx, expired.ID, true)
	if expiredAfter.Active {
		t.Error("expired user still active after sweep")
	}
	if expiredAfter.TempPasswordHash != "" {
		t.Error("expired user's temporal hash survives the sweep")
	}
	liveAfter, _ := users.GetByID(ctx, live.ID, true)
	if !liveAfter.Active || liveAfter.TempPasswordHash == "" {
		t.Error("LIVE break-glass user was disturbed by the sweep")
	}
	strangerAfter, _ := users.GetByID(ctx, stranger.ID, true)
	if !strangerAfter.Active {
		t.Error("NON-break-glass user was deactivated by the sweep")
	}

	// Second sweep: idempotent (nothing new to retire).
	if swept := srv.SweepExpiredBreakGlass(ctx); swept != 0 {
		t.Errorf("second sweep retired %d, want 0", swept)
	}
}

// --- Login temporal window -------------------------------------------------

func TestLoginHonorsTemporalWindow(t *testing.T) {
	users := NewMemoryUserStore()
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), users)
	ctx := context.Background()

	pw := "temporal-passphrase"
	h, err := HashPassword("", pw)
	if err != nil {
		t.Fatalf("encode temporal: %v", err)
	}
	inFuture := time.Now().Add(time.Hour)
	inPast := time.Now().Add(-time.Hour)

	// Within the window: the temporal password ADMITS.
	winUser, err := users.Create(ctx, &User{
		Username: "win@glass.invalid", Roles: []string{"admin"}, Active: true,
		TempPasswordHash: h, TempPasswordValidUntil: &inFuture,
	})
	if err != nil {
		t.Fatalf("create win: %v", err)
	}
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: winUser.Username, Password: pw}); err != nil {
		t.Fatalf("login within window: %v", err)
	}

	// Outside the window: REFUSED (generic invalid credentials — no state
	// leakage), and the credential is LAZILY VOIDED (hash zeroed).
	loseUser, err := users.Create(ctx, &User{
		Username: "lose@glass.invalid", Roles: []string{"admin"}, Active: true,
		TempPasswordHash: h, TempPasswordValidUntil: &inPast,
	})
	if err != nil {
		t.Fatalf("create lose: %v", err)
	}
	_, err = idp.client.Login(ctx, &idppb.LoginRequest{Username: loseUser.Username, Password: pw})
	if err == nil {
		t.Fatal("login outside window succeeded, want refused")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("outside-window code = %v, want Unauthenticated (generic)", status.Code(err))
	}
	loseAfter, _ := users.GetByID(ctx, loseUser.ID, true)
	if loseAfter.TempPasswordHash != "" {
		t.Error("lapsed temporal hash not zeroed by the lazy expiry")
	}
}
