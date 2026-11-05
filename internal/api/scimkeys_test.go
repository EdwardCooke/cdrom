package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"cdrom/internal/audit"
	"cdrom/internal/auth"
)

// fakeScimKey is a stub ScimKeyClient that returns canned responses and
// records the calls the API made (F-28.2 slice 2).
type fakeScimKey struct {
	derivePlain, deriveEnc, derivePrefix string
	deriveErr                            error
	createErr                            error
	getErr, listErr                      error
	updateErr, revokeErr, reinstateErr   error
	pruneDeleted                         int
	pruneErr                             error

	createdEnc, createdPrefix, createdDesc, createdBy string
	createdExpires                                    *time.Time
	updated                                           UpdateScimKeyFields
	updatedID                                         string
	pruned                                            bool
	lastPruneHorizon                                  time.Duration

	mu sync.Mutex
}

func (f *fakeScimKey) Derive(_ context.Context) (string, string, string, error) {
	if f.deriveErr != nil {
		return "", "", "", f.deriveErr
	}
	return f.derivePlain, f.deriveEnc, f.derivePrefix, nil
}

func (f *fakeScimKey) Create(_ context.Context, encoding, prefix, description, createdBy string, expiresAt *time.Time) (*ScimKeyResult, error) {
	f.mu.Lock()
	f.createdEnc, f.createdPrefix, f.createdDesc, f.createdBy, f.createdExpires = encoding, prefix, description, createdBy, expiresAt
	f.mu.Unlock()
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &ScimKeyResult{Identifier: "7", Description: description, Prefix: prefix, CreatedBy: createdBy, CreatedAt: "2026-11-01T00:00:00Z"}, nil
}

func (f *fakeScimKey) Get(_ context.Context, id string) (*ScimKeyResult, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &ScimKeyResult{Identifier: id, Description: "d", Prefix: "abcdef12", CreatedAt: "2026-11-01T00:00:00Z", Revoked: id == "dead"}, nil
}

func (f *fakeScimKey) List(_ context.Context) ([]ScimKeyResult, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return []ScimKeyResult{
		{Identifier: "3", Description: "newest", Prefix: "nnnnnnnn", CreatedAt: "2026-11-01T00:00:00Z"},
		{Identifier: "1", Description: "oldest", Prefix: "oooooooo", CreatedAt: "2026-10-01T00:00:00Z"},
	}, nil
}

func (f *fakeScimKey) Update(_ context.Context, id string, req UpdateScimKeyFields) (*ScimKeyResult, error) {
	f.mu.Lock()
	f.updatedID, f.updated = id, req
	f.mu.Unlock()
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	out := &ScimKeyResult{Identifier: id, Description: "edited", Prefix: "abcdef12", CreatedAt: "2026-11-01T00:00:00Z"}
	if req.ExpiresAt != nil {
		s := req.ExpiresAt.UTC().Format(time.RFC3339)
		out.ExpiresAt = &s
	}
	return out, nil
}

func (f *fakeScimKey) Revoke(_ context.Context, id string) (*ScimKeyResult, error) {
	if f.revokeErr != nil {
		return nil, f.revokeErr
	}
	at := "2026-11-01T00:00:00Z"
	return &ScimKeyResult{Identifier: id, Description: "d", Prefix: "abcdef12", CreatedAt: "2026-11-01T00:00:00Z", Revoked: true, RevokedAt: &at}, nil
}

func (f *fakeScimKey) Reinstate(_ context.Context, id string) (*ScimKeyResult, error) {
	if f.reinstateErr != nil {
		return nil, f.reinstateErr
	}
	return &ScimKeyResult{Identifier: id, Description: "d", Prefix: "abcdef12", CreatedAt: "2026-11-01T00:00:00Z", Revoked: false}, nil
}

func (f *fakeScimKey) Prune(_ context.Context, horizon time.Duration) (int, error) {
	f.mu.Lock()
	f.pruned = true
	f.lastPruneHorizon = horizon
	del, err := f.pruneDeleted, f.pruneErr
	f.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return del, nil
}

// recordingSink is an in-memory audit.Sink for asserting event emission.
type recordingSink struct {
	mu     sync.Mutex
	events []audit.Event
}

func (s *recordingSink) Append(_ context.Context, e audit.Event) error {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
	return nil
}

func (s *recordingSink) byAction(action string) []audit.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []audit.Event
	for _, e := range s.events {
		if e.Action == action {
			out = append(out, e)
		}
	}
	return out
}

// scimKeyServer returns an API test server with the given SCIM key client
// attached (and an audit recorder draining into sink, when sink is non-nil).
// Every request is served with user in the context (simulating an
// authenticated caller); RBAC is left off, so the caller acts as a synthetic
// admin (mirrors the F-25 test posture).
func scimKeyServer(t *testing.T, fk ScimKeyClient, user auth.User, sink *recordingSink) *httptest.Server {
	t.Helper()
	srv := New(Clients{}, nil)
	srv.SetLogger(testLogger())
	if fk != nil {
		srv.SetScimKeyClient(fk)
	}
	if sink != nil {
		srv.SetAudit(audit.NewRecorder(nil, sink, nil, slog.New(slog.NewTextHandler(io.Discard, nil))))
	}
	handler := srv.Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user.Subject != "" {
			r = r.WithContext(auth.WithUser(r.Context(), user))
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// testLogger returns a discard slog.Logger for servers built in tests (the
// prune sweep WARNs through it; a nil logger would panic).
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestScimKeyEndpointsDisabled checks that the /api/scim/keys endpoints
// respond 501 when the pool is not wired (no client attached).
func TestScimKeyEndpointsDisabled(t *testing.T) {
	ts := scimKeyServer(t, nil, auth.User{}, nil)
	urls := []struct {
		method, path string
	}{
		{http.MethodGet, "/api/scim/keys"},
		{http.MethodPost, "/api/scim/keys"},
		{http.MethodGet, "/api/scim/keys/1"},
		{http.MethodPatch, "/api/scim/keys/1"},
		{http.MethodPost, "/api/scim/keys/1/revoke"},
		{http.MethodPost, "/api/scim/keys/1/reinstate"},
	}
	for _, u := range urls {
		if code, _ := doRequest(t, u.method, ts.URL+u.path, ""); code != http.StatusNotImplemented {
			t.Errorf("%s %s status = %d, want 501", u.method, u.path, code)
		}
	}
}

// TestScimKeyCreateShowsPlaintextOnce checks the F-28.2 create contract: the
// 201 carries the key OBJECT plus the plaintext (shown exactly once), the
// response is marked no-store, the plaintext is never persisted (the store
// received only the encoding + prefix), and the audit event records the
// PREFIX — never the plaintext.
func TestScimKeyCreateShowsPlaintextOnce(t *testing.T) {
	const plain = "Ab3!xY9^mN2*pQ7=rS4&tU6#wV8+zA1-"
	fk := &fakeScimKey{derivePlain: plain, deriveEnc: "$argon2id$enc", derivePrefix: "Ab3!xY9^"}
	sink := &recordingSink{}
	ts := scimKeyServer(t, fk, auth.User{Subject: "ada@example.com"}, sink)

	code, body := doRequest(t, http.MethodPost, ts.URL+"/api/scim/keys", `{"description":"Okta connector"}`)
	if code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201 (body %s)", code, body)
	}
	var created struct {
		Key       ScimKeyResult `json:"key"`
		Plaintext string        `json:"plaintext"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	if created.Plaintext != plain {
		t.Fatalf("plaintext = %q, want the derived key", created.Plaintext)
	}
	if created.Key.Identifier == "" || created.Key.Prefix != fk.derivePrefix {
		t.Errorf("key object = %+v, want identifier + prefix", created.Key)
	}
	if created.Key.CreatedBy != "ada@example.com" {
		t.Errorf("created_by = %q, want the caller's subject", created.Key.CreatedBy)
	}
	// The persisted material is (encoding, prefix) — never the plaintext.
	fk.mu.Lock()
	enc, prefix := fk.createdEnc, fk.createdPrefix
	fk.mu.Unlock()
	if enc != fk.deriveEnc || prefix != fk.derivePrefix {
		t.Errorf("create persisted (%q,%q), want (%q,%q)", enc, prefix, fk.deriveEnc, fk.derivePrefix)
	}
	// Cache-control forbids intermediaries retaining the one-time plaintext.
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/scim/keys", strings.NewReader(`{"description":"hdr"}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	hdrCh := make(chan string, 1)
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdrCh <- w.Header().Get("Cache-Control")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	_ = req
	_ = hdrCh
	ts2.Close()

	// The audit trail records the prefix, not the plaintext.
	evs := sink.byAction(audit.ActionScimKeyCreate)
	if len(evs) != 1 {
		t.Fatalf("audit create events = %d, want 1", len(evs))
	}
	if evs[0].TargetName != fk.derivePrefix {
		t.Errorf("audit target_name = %q, want the prefix", evs[0].TargetName)
	}
	if strings.Contains(audit.EncodeFieldValue(evs[0].NewValue), plain) {
		t.Error("the plaintext leaked into the audit event")
	}
}

// TestScimKeyCreateHeaderNoStore checks the create response's Cache-Control
// directive (the one-time plaintext must not linger in intermediary caches).
func TestScimKeyCreateHeaderNoStore(t *testing.T) {
	fk := &fakeScimKey{derivePlain: "k", deriveEnc: "e", derivePrefix: "kpfx1234"}
	ts := scimKeyServer(t, fk, auth.User{Subject: "u"}, nil)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/scim/keys", strings.NewReader(`{"description":"hdr"}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
}

// TestScimKeyCreateValidation checks the F-28.2 create guards: required
// description (1-200 chars), RFC 3339 strictly-future expiresAt, and the
// pool-cap conflict surfacing as 409.
func TestScimKeyCreateValidation(t *testing.T) {
	fk := &fakeScimKey{derivePlain: "k", deriveEnc: "e", derivePrefix: "kpfx1234"}
	ts := scimKeyServer(t, fk, auth.User{Subject: "u"}, nil)

	longDesc := strings.Repeat("x", 201)
	cases := []struct {
		body string
		want int
	}{
		{`{}`, http.StatusBadRequest},                                 // no description
		{`{"description":""}`, http.StatusBadRequest},                 // empty description
		{`{"description":"` + longDesc + `"}`, http.StatusBadRequest}, // 201 runes
		{`{"description":"x","expiresAt":"not-a-time"}`, http.StatusBadRequest},
		{`{"description":"x","expiresAt":"2020-01-01T00:00:00Z"}`, http.StatusBadRequest}, // past
	}
	for i, c := range cases {
		if code, body := doRequest(t, http.MethodPost, ts.URL+"/api/scim/keys", c.body); code != c.want {
			t.Errorf("case %d (%s): status = %d, want %d (body %s)", i, c.body, code, c.want, body)
		}
	}

	// A valid future expiration is forwarded to the store.
	code, _ := doRequest(t, http.MethodPost, ts.URL+"/api/scim/keys", `{"description":"x","expiresAt":"2030-01-01T00:00:00Z"}`)
	if code != http.StatusCreated {
		t.Fatalf("future expiresAt: status = %d, want 201", code)
	}
	fk.mu.Lock()
	exp := fk.createdExpires
	fk.mu.Unlock()
	if exp == nil || exp.Year() != 2030 {
		t.Fatalf("expiresAt not forwarded to the store: %v", exp)
	}

	// Beyond the cap: the store's AlreadyExists surfaces as 409.
	fk.createErr = &scimKeyError{code: codes.AlreadyExists, msg: "pool at capacity"}
	if code, _ := doRequest(t, http.MethodPost, ts.URL+"/api/scim/keys", `{"description":"overflow"}`); code != http.StatusConflict {
		t.Fatalf("cap breach: status = %d, want 409", code)
	}
}

// TestScimKeyPatchSemantics checks the F-28.2 PARTIAL edit contract: omitted
// fields are unchanged, an explicit null CLEARS the expiration, a value sets
// it (future-only), and a revoked key is refused with 409.
func TestScimKeyPatchSemantics(t *testing.T) {
	fk := &fakeScimKey{}
	ts := scimKeyServer(t, fk, auth.User{Subject: "u"}, nil)

	// Description only.
	if code, _ := doRequest(t, http.MethodPatch, ts.URL+"/api/scim/keys/1", `{"description":"renamed"}`); code != http.StatusOK {
		t.Fatalf("patch description: status = %d, want 200", code)
	}
	fk.mu.Lock()
	u := fk.updated
	fk.mu.Unlock()
	if !u.HasDescription || u.Description != "renamed" || u.ExpiresAt != nil || u.ClearExpiresAt {
		t.Errorf("patch(description) forwarded %+v, want description-only intent", u)
	}

	// Explicit null clears the expiration.
	if code, _ := doRequest(t, http.MethodPatch, ts.URL+"/api/scim/keys/1", `{"expiresAt":null}`); code != http.StatusOK {
		t.Fatalf("patch(null expiry): status = %d, want 200", code)
	}
	fk.mu.Lock()
	u = fk.updated
	fk.mu.Unlock()
	if !u.ClearExpiresAt || u.HasDescription {
		t.Errorf("patch(null expiry) forwarded %+v, want clear-intent only", u)
	}

	// A value sets it (future-only).
	if code, _ := doRequest(t, http.MethodPatch, ts.URL+"/api/scim/keys/1", `{"expiresAt":"2030-06-01T00:00:00Z"}`); code != http.StatusOK {
		t.Fatalf("patch(value expiry): status = %d, want 200", code)
	}
	fk.mu.Lock()
	u = fk.updated
	fk.mu.Unlock()
	if u.ClearExpiresAt || u.ExpiresAt == nil || u.ExpiresAt.Year() != 2030 {
		t.Errorf("patch(value expiry) forwarded %+v, want the set-intent", u)
	}

	// Past value refused.
	if code, _ := doRequest(t, http.MethodPatch, ts.URL+"/api/scim/keys/1", `{"expiresAt":"2020-01-01T00:00:00Z"}`); code != http.StatusBadRequest {
		t.Fatalf("patch(past expiry): status = %d, want 400", code)
	}

	// An empty body is a legal no-change patch.
	if code, _ := doRequest(t, http.MethodPatch, ts.URL+"/api/scim/keys/1", ``); code != http.StatusOK {
		t.Fatalf("patch(empty body): status = %d, want 200", code)
	}

	// A revoked key is refused (reinstate first).
	fk.updateErr = &scimKeyError{code: codes.FailedPrecondition, msg: "revoked"}
	if code, _ := doRequest(t, http.MethodPatch, ts.URL+"/api/scim/keys/1", `{"description":"late"}`); code != http.StatusConflict {
		t.Fatalf("patch(revoked): status = %d, want 409", code)
	}
}

// TestScimKeyRevokeReinstate checks the F-28.2 lifecycle verbs: both succeed
// (200 + audit), a pruned key is 404, and the reinstate-window conflict is
// 409.
func TestScimKeyRevokeReinstate(t *testing.T) {
	fk := &fakeScimKey{}
	sink := &recordingSink{}
	ts := scimKeyServer(t, fk, auth.User{Subject: "u"}, sink)

	code, body := doRequest(t, http.MethodPost, ts.URL+"/api/scim/keys/1/revoke", "")
	if code != http.StatusOK {
		t.Fatalf("revoke: status = %d, want 200 (body %s)", code, body)
	}
	var obj ScimKeyResult
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("decode revoke: %v", err)
	}
	if !obj.Revoked || obj.RevokedAt == nil {
		t.Errorf("revoke response = %+v, want revoked:true + stamp", obj)
	}
	if len(sink.byAction(audit.ActionScimKeyRevoke)) != 1 {
		t.Errorf("revoke audit events = %d, want 1", len(sink.byAction(audit.ActionScimKeyRevoke)))
	}

	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/scim/keys/1/reinstate", "")
	if code != http.StatusOK {
		t.Fatalf("reinstate: status = %d, want 200 (body %s)", code, body)
	}
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("decode reinstate: %v", err)
	}
	if obj.Revoked {
		t.Errorf("reinstate response = %+v, want revoked:false", obj)
	}
	if len(sink.byAction(audit.ActionScimKeyReinstate)) != 1 {
		t.Errorf("reinstate audit events = %d, want 1", len(sink.byAction(audit.ActionScimKeyReinstate)))
	}

	// Gone (pruned) → 404; window elapsed → 409.
	fk.revokeErr = &scimKeyError{code: codes.NotFound, msg: "pruned"}
	if code, _ := doRequest(t, http.MethodPost, ts.URL+"/api/scim/keys/9/revoke", ""); code != http.StatusNotFound {
		t.Fatalf("revoke(pruned): status = %d, want 404", code)
	}
	fk.reinstateErr = &scimKeyError{code: codes.FailedPrecondition, msg: "window elapsed"}
	if code, _ := doRequest(t, http.MethodPost, ts.URL+"/api/scim/keys/9/reinstate", ""); code != http.StatusConflict {
		t.Fatalf("reinstate(window elapsed): status = %d, want 409", code)
	}
}

// TestScimKeyListGetShapes checks the read contracts: list renders an ARRAY
// (never null when empty) in newest-first order, and get carries the revoked
// flag (404 for unknown ids).
func TestScimKeyListGetShapes(t *testing.T) {
	fk := &fakeScimKey{}
	ts := scimKeyServer(t, fk, auth.User{Subject: "u"}, nil)

	code, body := doRequest(t, http.MethodGet, ts.URL+"/api/scim/keys", "")
	if code != http.StatusOK {
		t.Fatalf("list: status = %d, want 200", code)
	}
	var arr []ScimKeyResult
	if err := json.Unmarshal(body, &arr); err != nil {
		t.Fatalf("list body is not an array: %v (%s)", err, body)
	}
	if len(arr) != 2 || arr[0].Identifier != "3" {
		t.Errorf("list = %+v, want newest-first passthrough", arr)
	}

	// An empty pool still renders [].
	srv := New(Clients{}, nil)
	srv.SetLogger(testLogger())
	srv.SetScimKeyClient(&emptyScimLister{})
	h := srv.Handler()
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(auth.WithUser(r.Context(), auth.User{Subject: "u"}))
		h.ServeHTTP(w, r)
	}))
	defer ts2.Close()
	code, body = doRequest(t, http.MethodGet, ts2.URL+"/api/scim/keys", "")
	if code != http.StatusOK {
		t.Fatalf("empty list: status = %d, want 200", code)
	}
	if string(bytes.TrimSpace(body)) != "[]" {
		t.Errorf("empty list body = %s, want []", body)
	}

	// Get reflects the revoked flag.
	code, body = doRequest(t, http.MethodGet, ts.URL+"/api/scim/keys/dead", "")
	if code != http.StatusOK {
		t.Fatalf("get(dead): status = %d, want 200", code)
	}
	var one ScimKeyResult
	if err := json.Unmarshal(body, &one); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if !one.Revoked {
		t.Errorf("get(dead) = %+v, want revoked:true", one)
	}

	// Unknown id → 404.
	fk.getErr = &scimKeyError{code: codes.NotFound, msg: "gone"}
	if code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/scim/keys/nope", ""); code != http.StatusNotFound {
		t.Fatalf("get(unknown): status = %d, want 404", code)
	}
}

// emptyScimLister is a ScimKeyClient whose List returns an empty pool.
type emptyScimLister struct{}

func (emptyScimLister) Derive(context.Context) (string, string, string, error) {
	return "", "", "", nil
}
func (emptyScimLister) Create(context.Context, string, string, string, string, *time.Time) (*ScimKeyResult, error) {
	return nil, nil
}
func (emptyScimLister) Get(context.Context, string) (*ScimKeyResult, error) { return nil, nil }
func (emptyScimLister) List(context.Context) ([]ScimKeyResult, error)       { return []ScimKeyResult{}, nil }
func (emptyScimLister) Update(context.Context, string, UpdateScimKeyFields) (*ScimKeyResult, error) {
	return nil, nil
}
func (emptyScimLister) Revoke(context.Context, string) (*ScimKeyResult, error) { return nil, nil }
func (emptyScimLister) Reinstate(context.Context, string) (*ScimKeyResult, error) {
	return nil, nil
}
func (emptyScimLister) Prune(context.Context, time.Duration) (int, error) { return 0, nil }

// TestScimKeyPruneSweep checks the maintenance sweep: one pass PRUNEs with
// the 24 h horizon, AUDITS the removal as a system actor, and stays silent
// when nothing was deleted.
func TestScimKeyPruneSweep(t *testing.T) {
	sink := &recordingSink{}
	fk := &fakeScimKey{pruneDeleted: 2}
	srv := New(Clients{}, nil)
	srv.SetLogger(testLogger())
	srv.SetScimKeyClient(fk)
	srv.SetAudit(audit.NewRecorder(nil, sink, nil, slog.New(slog.NewTextHandler(io.Discard, nil))))

	srv.runScimKeyPruneSweep(context.Background())
	fk.mu.Lock()
	pruned, horizon := fk.pruned, fk.lastPruneHorizon
	fk.mu.Unlock()
	if !pruned || horizon != 24*time.Hour {
		t.Fatalf("sweep pruned=%v horizon=%v, want one pass at the 24 h horizon", pruned, horizon)
	}
	evs := sink.byAction(audit.ActionScimKeyPrune)
	if len(evs) != 1 {
		t.Fatalf("prune audit events = %d, want 1", len(evs))
	}
	if evs[0].ActorKind != audit.ActorKindSystem {
		t.Errorf("prune actor kind = %q, want system", evs[0].ActorKind)
	}

	// Silent when nothing was deleted.
	before := len(sink.byAction(audit.ActionScimKeyPrune))
	fk.pruneDeleted = 0
	srv.runScimKeyPruneSweep(context.Background())
	if after := len(sink.byAction(audit.ActionScimKeyPrune)); after != before {
		t.Errorf("silent sweep emitted %d events, want 0", after-before)
	}

	// The LAUNCHER fires at least one pass promptly (interval honoured).
	fk2 := &fakeScimKey{}
	srv2 := New(Clients{}, nil)
	srv2.SetLogger(testLogger())
	srv2.SetScimKeyClient(fk2)
	ctx, cancel := context.WithCancel(context.Background())
	srv2.StartScimKeyPruning(ctx, 20*time.Millisecond)
	defer cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fk2.mu.Lock()
		hit := fk2.pruned
		fk2.mu.Unlock()
		if hit {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("StartScimKeyPruning never ran a sweep within 2 s")
}
