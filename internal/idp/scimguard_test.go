// SCIM caller-authentication validator tests (F-28.2, Slice 3): pool
// resolution, the timing-safe walk, the hot-path memo, stale-bundle
// resilience, failure recording (no key echo), and the guard's 401 contract
// across the whole surface.
package idp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/audit"
	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// --- fakes -----------------------------------------------------------------

// scimPoolFake is the counting fake backend: it serves a scripted bundle
// (rows the test controls) and tallies the handshake calls.
type scimPoolFake struct {
	mu          sync.Mutex
	rows        []*dbpb.ScimKey
	fail        bool
	bundleCalls int
	marks       []string
}

func (f *scimPoolFake) setRows(rows []*dbpb.ScimKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = rows
}

func (f *scimPoolFake) appendRow(row *dbpb.ScimKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, row)
}

func (f *scimPoolFake) failFeed(on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = on
}

func (f *scimPoolFake) bundleCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bundleCalls
}

func (f *scimPoolFake) markIds() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.marks...)
}

func (f *scimPoolFake) BundleScimKeys(_ context.Context, _ *dbpb.BundleScimKeysRequest) (*dbpb.BundleScimKeysResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bundleCalls++
	if f.fail {
		return nil, errScimPoolDown
	}
	return &dbpb.BundleScimKeysResponse{Keys: append([]*dbpb.ScimKey(nil), f.rows...)}, nil
}

func (f *scimPoolFake) MarkScimKeyUsed(_ context.Context, in *dbpb.MarkScimKeyUsedRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, in.GetId())
	return &emptypb.Empty{}, nil
}

var errScimPoolDown = errors.New("simulated database outage")

// flushingValidator exposes a TEST SEAM on *scimValidator: it resets the
// memo (verdicts + bundle freshness) so the next presentation is forced
// cold.
type flushingValidator struct {
	inner *scimValidator
}

func (fv *flushingValidator) Flush() {
	fv.inner.mu.Lock()
	defer fv.inner.mu.Unlock()
	fv.inner.verdicts = make(map[string]*scimVerdict)
	fv.inner.bundleAt = time.Time{}
}

// capturingAuditor records the events handed to Record.
type capturingAuditor struct {
	mu     sync.Mutex
	events []audit.Event
}

func (c *capturingAuditor) Record(e audit.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

func (c *capturingAuditor) byAction(action string) []audit.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []audit.Event
	for _, e := range c.events {
		if e.Action == action {
			out = append(out, e)
		}
	}
	return out
}

// --- helpers -----------------------------------------------------------------

// scimKeyRow builds a pool ROW for a chosen plaintext: the encoding is
// computed with the given pepper (mirrors what the S2 create path stores).
func scimKeyRow(t *testing.T, id, pepper, key string, expires *time.Time) *dbpb.ScimKey {
	t.Helper()
	enc, err := EncodeScimKey(pepper, key)
	if err != nil {
		t.Fatalf("encode key: %v", err)
	}
	row := &dbpb.ScimKey{Id: id, Hash: enc, Prefix: scimKeyPrefixOf(key)}
	if expires != nil {
		row.ExpiresAt = timestamppb.New(*expires)
	}
	return row
}

// scimPoolServer builds an IdP HTTP server with the SCIM surface ENABLED and
// the REAL validator attached over the given backend.
func scimPoolServer(t *testing.T, backend scimPoolBackend, pepper string, auditor scimAuditor) (*httptest.Server, *flushingValidator) {
	t.Helper()
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Scim = config.ScimConfig{Enabled: true}
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	v := NewScimValidator(backend, pepper, auditor, testLogger(), time.Second)
	srv.SetScimAuthorizer(v)
	return httptest.NewServer(srv.Handler()), &flushingValidator{inner: v}
}

// scimPresent issues a GET on the SCIM surface with the given bearer ("" =
// no Authorization header) and returns the status + body.
func scimPresent(t *testing.T, url, key string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, body
}

func ptrTime(tm time.Time) *time.Time { return &tm }

// --- the walk (timing-safe core) ---------------------------------------------

func TestWalkBundle_EvaluatesEveryRowRegardlessOfOutcome(t *testing.T) {
	const pepper = "pepper"
	keys := []string{"alpha-one", "bravo-two", "charlie-three", "delta-four", "echo-five"}
	var bundle []*scimCandidate
	for i, k := range keys {
		row := scimKeyRow(t, strconv.Itoa(i+1), pepper, k, nil)
		bundle = append(bundle, &scimCandidate{id: row.GetId(), encoding: row.GetHash()})
	}

	run := func(presented string) (count, idx int, expired bool) {
		count = 0
		verify := func(enc string) bool {
			count++
			return VerifySecret(enc, pepper, presented)
		}
		idx, expired = walkBundle(bundle, verify, time.Now())
		return
	}

	// Match on the LAST row: the walk must STILL evaluate all five.
	if c, idx, exp := run(keys[4]); idx != 4 || exp || c != len(bundle) {
		t.Errorf("last-row match: idx=%d expired=%v count=%d, want 4/false/%d", idx, exp, c, len(bundle))
	}
	// Match on the FIRST row: same count.
	if c, idx, _ := run(keys[0]); idx != 0 || c != len(bundle) {
		t.Errorf("first-row match: idx=%d count=%d, want 0/%d", idx, c, len(bundle))
	}
	// NO match: still evaluates every row (uniform CPU profile).
	if c, idx, _ := run("attacker-guess"); idx != -1 || c != len(bundle) {
		t.Errorf("miss: idx=%d count=%d, want -1/%d", idx, c, len(bundle))
	}
}

func TestWalkBundle_ExpiryCheckedOnlyOnMatch(t *testing.T) {
	const pepper = "pepper"
	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	mustEnc := func(key string) string { e, _ := EncodeScimKey(pepper, key); return e }
	bundle := []*scimCandidate{
		{id: "past", encoding: mustEnc("k-past"), expiresAt: &past},
		{id: "future", encoding: mustEnc("k-future"), expiresAt: &future},
		{id: "none", encoding: mustEnc("k-none")},
	}
	if idx, expired := walkBundle(bundle, func(enc string) bool { return VerifySecret(enc, pepper, "k-past") }, time.Now()); idx != 0 || !expired {
		t.Errorf("expired match: idx=%d expired=%v, want 0/true", idx, expired)
	}
	if idx, expired := walkBundle(bundle, func(enc string) bool { return VerifySecret(enc, pepper, "k-future") }, time.Now()); idx != 1 || expired {
		t.Errorf("live match: idx=%d expired=%v, want 1/false", idx, expired)
	}
	if idx, expired := walkBundle(bundle, func(enc string) bool { return VerifySecret(enc, pepper, "k-none") }, time.Now()); idx != 2 || expired {
		t.Errorf("no-expiry match: idx=%d expired=%v, want 2/false", idx, expired)
	}
}

// --- the validator over the guard (categories) --------------------------------

func TestScimGuard_CategoryOutcomes(t *testing.T) {
	const pepper = "pepper"
	valid := scimKeyRow(t, "1", pepper, "valid-live-key", nil)
	expiringSoon := scimKeyRow(t, "2", pepper, "soon-expiring-key", ptrTime(time.Now().Add(time.Hour)))
	expired := scimKeyRow(t, "3", pepper, "already-expired-key", ptrTime(time.Now().Add(-time.Minute)))
	pool := &scimPoolFake{}
	pool.setRows([]*dbpb.ScimKey{valid, expiringSoon, expired})
	ts, fv := scimPoolServer(t, pool, pepper, nil)
	defer ts.Close()

	check := func(label, key string, wantStatus int) {
		t.Helper()
		status, body := scimPresent(t, ts.URL+"/scim/v2/ServiceProviderConfig", key)
		if status != wantStatus {
			t.Errorf("%s: status = %d, want %d (%s)", label, status, wantStatus, body)
		}
	}

	check("valid key", "valid-live-key", http.StatusOK)
	check("near-future expiry", "soon-expiring-key", http.StatusOK)
	check("EXPIRED key", "already-expired-key", http.StatusUnauthorized)
	check("UNKNOWN key", "totally-made-up-key", http.StatusUnauthorized)
	check("MISSING header", "", http.StatusUnauthorized)
	// REVOCATION: the service drops the row from the bundle on revoke. Swap
	// the pool and force the next presentation cold.
	pool.setRows([]*dbpb.ScimKey{valid})
	fv.Flush()
	check("REVOKED key (gone from bundle)", "soon-expiring-key", http.StatusUnauthorized)
	// Pepper sensitivity: the same plaintext under a WRONG pepper rejects.
	fv.Flush()
	fv.inner.pepper = "different-pepper"
	check("WRONG pepper", "valid-live-key", http.StatusUnauthorized)
}

// --- the guard's 401 contract --------------------------------------------------

func TestScimGuard_Identical401AcrossCategoriesAndRoutes(t *testing.T) {
	const pepper = "pepper"
	past := time.Now().Add(-time.Minute)
	good := scimKeyRow(t, "1", pepper, "good-key-for-tests", nil)
	expired := scimKeyRow(t, "2", pepper, "expired-key-for-tests", &past)
	pool := &scimPoolFake{}
	pool.setRows([]*dbpb.ScimKey{good, expired})
	ts, fv := scimPoolServer(t, pool, pepper, nil)
	defer ts.Close()

	routes := []string{
		"/scim/v2/ServiceProviderConfig",
		"/scim/v2/Schemas",
		"/scim/v2/ResourceTypes",
		"/scim/v2/ResourceTypes/User",
		"/scim/v2/NoSuchThing", // subtree 404 territory
	}
	// Collect the 401 BODY for each rejection CATEGORY on the first route.
	categoryBodies := map[string][]byte{}
	getCategory := func(cat, key string) {
		t.Helper()
		status, body := scimPresent(t, ts.URL+routes[0], key)
		if status != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d, want 401", cat, status)
		}
		categoryBodies[cat] = body
	}
	getCategory("missing", "")
	getCategory("unknown", "made-up-key")
	getCategory("expired", "expired-key-for-tests")
	// Revoked: drop the good key, force cold, present it.
	pool.setRows([]*dbpb.ScimKey{expired})
	fv.Flush()
	getCategory("revoked", "good-key-for-tests")

	ref := categoryBodies["missing"]
	for cat, body := range categoryBodies {
		if !bytes.Equal(ref, body) {
			t.Errorf("%s 401 body differs from the canonical one:\n%s\nvs\n%s", cat, body, ref)
		}
	}
	// The body is the RFC 7644 error shape.
	var doc map[string]any
	if err := json.Unmarshal(ref, &doc); err != nil {
		t.Fatalf("canonical 401 body is not JSON: %s", ref)
	}
	if doc["status"] != "401" || doc["detail"] != "Unauthorized" {
		t.Errorf("canonical 401 body = %#v, want status 401 / detail Unauthorized", doc)
	}
	// EVERY route 401s identically for an unknown key.
	for _, rt := range routes {
		status, body := scimPresent(t, ts.URL+rt, "made-up-key")
		if status != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", rt, status)
		}
		if !bytes.Equal(categoryBodies["unknown"], body) {
			t.Errorf("%s: 401 body diverges from the canonical one", rt)
		}
	}
	// A GOOD key opens every routable resource (the NoSuchThing route 404s
	// INSIDE the surface — still proof the credential cleared the guard).
	pool.setRows([]*dbpb.ScimKey{good, expired})
	fv.Flush()
	for _, rt := range routes {
		status, _ := scimPresent(t, ts.URL+rt, "good-key-for-tests")
		if status != http.StatusOK && status != http.StatusNotFound {
			t.Errorf("%s with valid key: status = %d, want 200/404", rt, status)
		}
	}
}

func TestScimGuard_RejectsForeignSchemes(t *testing.T) {
	pool := &scimPoolFake{}
	pool.setRows([]*dbpb.ScimKey{scimKeyRow(t, "1", "p", "scheme-test-key", nil)})
	ts, _ := scimPoolServer(t, pool, "p", nil)
	defer ts.Close()

	// Basic (e.g. a user-pass style credential) is foreign to this surface.
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/scim/v2/Schemas", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Basic Zm9vOmJhcg==")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("Basic scheme status = %d, want 401", res.StatusCode)
	}
	// Lowercase "bearer" is a legitimate scheme spelling (case-insensitive).
	req2, _ := http.NewRequest(http.MethodGet, ts.URL+"/scim/v2/Schemas", nil)
	req2.Header.Set("Authorization", "bearer scheme-test-key")
	res2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res2.Body)
	res2.Body.Close()
	if res2.StatusCode != http.StatusOK {
		t.Errorf("lowercase bearer status = %d, want 200", res2.StatusCode)
	}
}

// --- the memo -------------------------------------------------------------------

func TestScimValidator_MemoDecidesWhileFresh(t *testing.T) {
	const pepper = "pepper"
	original := scimKeyRow(t, "1", pepper, "memo-original-key", nil)
	successor := scimKeyRow(t, "2", pepper, "memo-successor-key", nil)
	pool := &scimPoolFake{}
	pool.setRows([]*dbpb.ScimKey{original})
	v := NewScimValidator(pool, pepper, nil, testLogger(), time.Second)
	fv := &flushingValidator{inner: v}

	if _, ok := v.ValidatePresentation(context.Background(), "memo-original-key"); !ok {
		t.Fatalf("cold valid presentation rejected")
	}
	if got := pool.bundleCallCount(); got != 1 {
		t.Fatalf("bundle pulls after cold hit = %d, want 1", got)
	}
	// Swap the POOL underneath: the warm presenter must STILL pass (decided
	// by the memo, not a fresh bundle)…
	pool.setRows([]*dbpb.ScimKey{successor})
	if _, ok := v.ValidatePresentation(context.Background(), "memo-original-key"); !ok {
		t.Errorf("warm presentation of the swapped-away key rejected — memo missed")
	}
	// …while the newcomer must FAIL (the bundle is still fresh and holds no
	// trace of it).
	if _, ok := v.ValidatePresentation(context.Background(), "memo-successor-key"); ok {
		t.Errorf("newcomer key accepted before the bundle refreshed — memo overrun")
	}
	if got := pool.bundleCallCount(); got != 1 {
		t.Errorf("bundle pulls = %d, want 1 (fresh bundle suppresses pulls)", got)
	}
	// Negative memo: a bad key is cached too (blunts probing).
	if _, ok := v.ValidatePresentation(context.Background(), "some-other-key"); ok {
		t.Fatalf("bogus key accepted")
	}
	// Force the next round cold: memo AND bundle reset.
	fv.Flush()
	if _, ok := v.ValidatePresentation(context.Background(), "memo-successor-key"); !ok {
		t.Errorf("successor rejected after cold refresh")
	}
	// Successes bump last_used_at (through the backend).
	if len(pool.markIds()) == 0 {
		t.Errorf("no MarkScimKeyUsed bumps recorded for accepted presentations")
	}
}

// TestScimValidator_CustomMemoTTL proves the CONFIGURED memo cadence (ctor
// argument <- config idp.scim.memo_ttl / CDROM_SCIM_MEMO_TTL) actually
// steers memo warmth: a minute TTL cools the memo almost instantly (forcing
// a re-validation against a changed pool), the default 1 s keeps it warm
// across the same maneuver, and non-positive values normalize to the
// default.
func TestScimValidator_CustomMemoTTL(t *testing.T) {
	ctx := context.Background()
	const pepper = "pepper"
	key := scimKeyRow(t, "1", pepper, "custom-ttl-key", nil)
	stranger := scimKeyRow(t, "2", pepper, "custom-ttl-stranger", nil)

	// Minute TTL: the memo cools faster than the wall-clock delta between
	// two presentations, so a pool swap UNDERNEATH drops the warm key.
	poolTiny := &scimPoolFake{}
	poolTiny.setRows([]*dbpb.ScimKey{key})
	vTiny := NewScimValidator(poolTiny, pepper, nil, testLogger(), time.Nanosecond)
	time.Sleep(time.Millisecond)
	if _, ok := vTiny.ValidatePresentation(ctx, "custom-ttl-key"); !ok {
		t.Fatalf("cold valid presentation rejected")
	}
	poolTiny.setRows([]*dbpb.ScimKey{stranger})
	if _, ok := vTiny.ValidatePresentation(ctx, "custom-ttl-key"); ok {
		t.Errorf("minute TTL: swapped-away key still passed — memo refused to cool")
	}
	if got := poolTiny.bundleCallCount(); got != 2 {
		t.Errorf("minute TTL: bundle pulls = %d, want 2 (cooled memo re-validates)", got)
	}

	// Identical maneuver with the default TTL: the memo stays warm, the
	// swapped-away key still passes, and no extra pull happens.
	poolWarm := &scimPoolFake{}
	poolWarm.setRows([]*dbpb.ScimKey{key})
	vWarm := NewScimValidator(poolWarm, pepper, nil, testLogger(), time.Second)
	if _, ok := vWarm.ValidatePresentation(ctx, "custom-ttl-key"); !ok {
		t.Fatalf("cold valid presentation rejected")
	}
	poolWarm.setRows([]*dbpb.ScimKey{stranger})
	if _, ok := vWarm.ValidatePresentation(ctx, "custom-ttl-key"); !ok {
		t.Errorf("default TTL: warm key lost validity inside the memo window")
	}
	if got := poolWarm.bundleCallCount(); got != 1 {
		t.Errorf("default TTL: bundle pulls = %d, want 1 (warm memo suppresses pulls)", got)
	}

	// Normalization: non-positive TTLs behave exactly like the 1 s default.
	for _, ttl := range []time.Duration{0, -time.Second} {
		poolNorm := &scimPoolFake{}
		poolNorm.setRows([]*dbpb.ScimKey{key})
		v := NewScimValidator(poolNorm, pepper, nil, testLogger(), ttl)
		if _, ok := v.ValidatePresentation(ctx, "custom-ttl-key"); !ok {
			t.Fatalf("ttl=%v: cold valid presentation rejected", ttl)
		}
		poolNorm.setRows([]*dbpb.ScimKey{stranger})
		if _, ok := v.ValidatePresentation(ctx, "custom-ttl-key"); !ok {
			t.Errorf("ttl=%v: mis-normalized (warm key lost validity)", ttl)
		}
	}
}

// --- stale-bundle resilience ------------------------------------------------------

func TestScimValidator_StaleBundleFallback(t *testing.T) {
	const pepper = "pepper"
	living := scimKeyRow(t, "1", pepper, "fallback-key", nil)
	arrival := scimKeyRow(t, "2", pepper, "arrival-key", nil)
	pool := &scimPoolFake{}
	pool.setRows([]*dbpb.ScimKey{living})
	v := NewScimValidator(pool, pepper, nil, testLogger(), time.Second)
	fv := &flushingValidator{inner: v}

	if _, ok := v.ValidatePresentation(context.Background(), "fallback-key"); !ok {
		t.Fatalf("initial presentation rejected")
	}
	// Kill the bundle feed and bust the memo: the NEXT cold presentation must
	// still validate against the remembered (stale) bundle.
	pool.failFeed(true)
	fv.Flush()
	if _, ok := v.ValidatePresentation(context.Background(), "fallback-key"); !ok {
		t.Errorf("dead bundle feed dropped a healthy connector (stale fallback wanted)")
	}
	// Restore the feed; a NEW key appearing in the pool is picked up on the
	// next cold cycle.
	pool.failFeed(false)
	pool.appendRow(arrival)
	fv.Flush()
	if _, ok := v.ValidatePresentation(context.Background(), "arrival-key"); !ok {
		t.Errorf("new key invisible after feed restore")
	}
}

func TestScimValidator_NoEverObtainedBundleDenies(t *testing.T) {
	pool := &scimPoolFake{}
	pool.failFeed(true)
	v := NewScimValidator(pool, "p", nil, testLogger(), time.Second)
	if _, ok := v.ValidatePresentation(context.Background(), "anything"); ok {
		t.Errorf("accepted despite never obtaining a bundle (deny wanted)")
	}
}

// --- failure recording --------------------------------------------------------------

func TestScimValidator_FailuresRecordedWithoutEchoingTheKey(t *testing.T) {
	const pepper = "pepper"
	pool := &scimPoolFake{}
	pool.setRows([]*dbpb.ScimKey{scimKeyRow(t, "1", pepper, "present-good-key", nil)})
	aud := &capturingAuditor{}
	logBuf := &bytes.Buffer{}
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Scim = config.ScimConfig{Enabled: true}
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	bufLogger := slog.New(slog.NewTextHandler(logBuf, nil))
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), bufLogger)
	srv.SetScimAuthorizer(NewScimValidator(pool, pepper, aud, bufLogger, time.Second))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Probe with a distinctive key; the 401 must not mention it, nor must the
	// audit event or the warning log.
	probe := "SUPER-DISTINCTIVE-PROBE-XYZ"
	st, body := scimPresent(t, ts.URL+"/scim/v2/ServiceProviderConfig", probe)
	if st != http.StatusUnauthorized {
		t.Fatalf("probe status = %d, want 401", st)
	}
	if strings.Contains(string(body), probe) {
		t.Errorf("response echoed the presented key: %s", body)
	}
	evs := aud.byAction(audit.ActionScimAuthFail)
	if len(evs) == 0 {
		t.Fatalf("no scim.auth.fail audit event recorded")
	}
	last := evs[len(evs)-1]
	if last.ActorKind != audit.ActorKindSystem || last.Outcome != audit.OutcomeFailure {
		t.Errorf("event actor/outcome = %s/%s, want system/failure", last.ActorKind, last.Outcome)
	}
	if last.TargetKind != audit.TargetScimKey {
		t.Errorf("event target = %s, want scim-key", last.TargetKind)
	}
	if strings.Contains(audit.EncodeFieldValue(last.Details), probe) || strings.Contains(last.Actor, probe) {
		t.Errorf("audit event echoed the presented key: %+v", last)
	}
	det, ok := last.Details.(map[string]any)
	if !ok {
		t.Fatalf("details = %v (%T), want a structured document", last.Details, last.Details)
	}
	if det["reason"] != scimFailReasonRejected || det["path"] != "/scim/v2/ServiceProviderConfig" {
		t.Errorf("details = %#v, want reason=rejected + the path", det)
	}
	// The warning log line names the offense, never the key.
	logText := logBuf.String()
	if !strings.Contains(logText, "unauthorized caller") {
		t.Errorf("warning log missing the failure notice:\n%s", logText)
	}
	if strings.Contains(logText, probe) {
		t.Errorf("warning log echoed the presented key:\n%s", logText)
	}
	// A MISSING header records the coarser category.
	if st, _ := scimPresent(t, ts.URL+"/scim/v2/Schemas", ""); st != http.StatusUnauthorized {
		t.Fatalf("missing-header status = %d, want 401", st)
	}
	evs = aud.byAction(audit.ActionScimAuthFail)
	det, ok = evs[len(evs)-1].Details.(map[string]any)
	if !ok {
		t.Fatalf("details = %v (%T), want a structured document", evs[len(evs)-1].Details, evs[len(evs)-1].Details)
	}
	if det["reason"] != scimFailReasonMissing {
		t.Errorf("missing-header reason = %v, want %s", det["reason"], scimFailReasonMissing)
	}
}

// --- attribution context -------------------------------------------------------------

func TestScimCaller_ContextRoundTrip(t *testing.T) {
	if got := SCIMCallerFromContext(context.Background()); got != "" {
		t.Errorf("plain context = %q, want empty", got)
	}
	ctx := withSCIMCaller(context.Background(), "SCIM connector (#7)")
	if got := SCIMCallerFromContext(ctx); got != "SCIM connector (#7)" {
		t.Errorf("round trip = %q, want the planted identity", got)
	}
}

// --- memo capacity bound ---------------------------------------------------------------

func TestScimValidator_StoreVerdictShedsOldestHalfOverBudget(t *testing.T) {
	v := NewScimValidator(nil, "", nil, testLogger(), time.Second)
	young := time.Now()
	// Plant scimMemoBound+1 YOUNG entries (purge-by-expiry finds nothing).
	for i := 0; i <= scimMemoBound; i++ {
		v.verdicts["pad-key-"+strconv.Itoa(i)] = &scimVerdict{at: young}
	}
	v.storeVerdict("final-entry", &scimVerdict{at: young})
	if len(v.verdicts) > scimMemoBound {
		t.Errorf("memo size = %d, want <= %d after shedding", len(v.verdicts), scimMemoBound)
	}
}

// --- the guard denies when no validator is attached ------------------------------------

func TestScimGuard_DeniesWithoutAttachedValidator(t *testing.T) {
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Scim = config.ScimConfig{Enabled: true}
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	// Intentionally NO SetScimAuthorizer.
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	if st, _ := scimPresent(t, ts.URL+"/scim/v2/Schemas", "whatever"); st != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 (unattached validator ⇒ deny)", st)
	}
}
