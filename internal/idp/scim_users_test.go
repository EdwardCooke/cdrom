package idp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/audit"
	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/metrics"
)

// recordingDBFake is a MINIMAL DatabaseClient: it records the ListIDPUsers /
// CountIDPUsers requests and replies with an EMPTY PAGE / ZERO COUNT. Every
// OTHER inherited method would panic if called (the embedded nil interface) —
// proof that only the overridden paths ran.
type recordingDBFake struct {
	dbpb.DatabaseClient
	lastListReq  *dbpb.ListIDPUsersRequest
	lastCountReq *dbpb.ListIDPUsersRequest
	countReply   int32
}

func (f *recordingDBFake) ListIDPUsers(_ context.Context, in *dbpb.ListIDPUsersRequest, _ ...grpc.CallOption) (*dbpb.ListIDPUsersResponse, error) {
	f.lastListReq = in
	return &dbpb.ListIDPUsersResponse{}, nil
}

func (f *recordingDBFake) CountIDPUsers(_ context.Context, in *dbpb.ListIDPUsersRequest, _ ...grpc.CallOption) (*dbpb.ListIDPUsersResponse, error) {
	f.lastCountReq = in
	return &dbpb.ListIDPUsersResponse{Count: f.countReply}, nil
}

// TestDBUserStore_ListNilConstraints locks the PROD-WIRING REGRESSION the
// e2e boot exposed: internal callers (the break-glass sweep, the user-count
// gauge, the first-admin roll-out) list with a NIL constraint set — the db
// store must treat that as UNCONSTRAINED (zero predicates forwarded), not
// dereference it.
func TestDBUserStore_ListNilConstraints(t *testing.T) {
	fake := &recordingDBFake{}
	store := NewDBUserStore(fake)
	if _, err := store.List(context.Background(), false, nil); err != nil {
		t.Fatalf("List(nil): %v", err)
	}
	if fake.lastListReq == nil {
		t.Fatal("List(nil): the RPC was never reached")
	}
	if got := len(fake.lastListReq.GetGenericPredicate()); got != 0 {
		t.Errorf("List(nil): %d predicates forwarded, want 0", got)
	}
	// A POPULATED set forwards verbatim.
	set := EqualityConstraints{{Attribute: "username", Value: "x.y", CaseSensitive: false}}
	if _, err := store.List(context.Background(), true, &set); err != nil {
		t.Fatalf("List(populated): %v", err)
	}
	ps := fake.lastListReq.GetGenericPredicate()
	if len(ps) != 1 || ps[0].GetAttribute() != "username" || ps[0].GetValue() != "x.y" {
		t.Errorf("List(populated): forwarded %v, want [{username x.y}]", ps)
	}
}

// TestDBUserStore_CountDelegatesToAggregate locks the MEASUREMENT CONTRACT:
// counting the directory (gauges, first-admin roll-outs) must ride the
// CountIDPUsers AGGREGATE (one integer crosses the wire) and forward the SAME
// conditions a List would — never the population-transfer List.
func TestDBUserStore_CountDelegatesToAggregate(t *testing.T) {
	fake := &recordingDBFake{countReply: 42}
	store := NewDBUserStore(fake)
	get, err := store.Count(context.Background(), false, nil)
	if err != nil || get != 42 {
		t.Fatalf("Count(nil) = %d (%v), want 42", get, err)
	}
	if fake.lastCountReq == nil {
		t.Fatal("Count: the CountIDPUsers RPC was never reached")
	}
	if fake.lastListReq != nil {
		t.Error("Count: rode the LIST RPC (population transfer), want the aggregate")
	}
	// Conditions FORWARD VERBATIM (includeDeleted + predicates).
	set := EqualityConstraints{{Attribute: "external_id", Value: "AZ-X", CaseSensitive: true}}
	if _, err := store.Count(context.Background(), true, &set); err != nil {
		t.Fatalf("Count(populated): %v", err)
	}
	cr := fake.lastCountReq
	if !cr.GetIncludeDeleted() || len(cr.GetGenericPredicate()) != 1 || cr.GetGenericPredicate()[0].GetAttribute() != "external_id" {
		t.Errorf("Count(populated): forwarded %v, want includeDeleted + the predicate", cr)
	}
}

// ===========================================================================
// F-28.4 Slice 2 — the SCIM /Users resource
// ===========================================================================

// capSCIMAuditor captures the audit events the /Users handlers record (the
// guard's scimAuditor seam).
type capSCIMAuditor struct {
	events []audit.Event
}

func (a *capSCIMAuditor) Record(ev audit.Event) { a.events = append(a.events, ev) }

// scimUsersFixture builds the /Users test world: an IdP HTTP server with the
// SCIM surface ENABLED, the permissive authorizer (attribution
// "SCIM connector (#test)"), a MemoryUserStore backing, a capturing auditor,
// and a PROMETHEUS REGISTRY (for the operations metric).
func scimUsersFixture(t *testing.T, scimCfg config.ScimConfig) (*httptest.Server, *MemoryUserStore, *capSCIMAuditor, *prometheus.Registry) {
	t.Helper()
	if scimCfg.Enabled {
		// (enabled by default in the configs passed here)
	}
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Scim = scimCfg
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	rec := prometheus.NewRegistry()
	reg := rec
	ms := metrics.NewIDPMetrics(rec)
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	srv.SetMetrics(ms)
	srv.SetScimAuthorizer(&scimPermissiveAuthorizer{})
	store := NewMemoryUserStore()
	aud := &capSCIMAuditor{}
	srv.SetScimUserBacking(store, aud)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, store, aud, reg
}

// scimURL builds a SAFE request URL from a raw query fragment (which may
// contain spaces/quotes in a filter value): it parses the fragment into
// url.Values and RE-ENCODES it, so the outgoing request line is well-formed.
// Assigning to url.RawQuery directly would NOT re-escape (spaces/quotes
// would corrupt the target).
func scimURL(t *testing.T, base, query string) string {
	t.Helper()
	if query == "" {
		return base
	}
	frags := strings.TrimLeft(query, "?")
	vals, err := neturl.ParseQuery(frags)
	if err != nil {
		t.Fatalf("parse query %q: %v", frags, err)
	}
	return base + "?" + vals.Encode()
}

// scimCall issues an authenticated SCIM request (guard: bearer + Accept) and
// returns the status code and decoded JSON body (nil when empty).
func scimCall(t *testing.T, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+scimFrameworkKey)
	req.Header.Set("Accept", "application/scim+json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, url, err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(data) == 0 {
		return res.StatusCode, nil
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("%s %s: decode body (%s): %v", method, url, data, err)
	}
	return res.StatusCode, doc
}

// scimDocField digs a dotted path out of a decoded SCIM document, descending
// into OBJECTS (segment = key) and ARRAYS (segment = numeric index).
func scimDocField(t *testing.T, doc map[string]any, path string) any {
	t.Helper()
	cur := any(doc)
	for _, seg := range strings.Split(path, ".") {
		switch node := cur.(type) {
		case map[string]any:
			nxt, ok := node[seg]
			if !ok {
				t.Fatalf("dig %q: missing key %q", path, seg)
			}
			cur = nxt
		case []any:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(node) {
				t.Fatalf("dig %q: bad array index %q", path, seg)
			}
			cur = node[idx]
		default:
			t.Fatalf("dig %q: cannot descend into %T at %q", path, cur, seg)
		}
	}
	return cur
}

// scimCounter sums a labeled counter's samples from the registry. A
// CounterVec with NO CHILDREN YET is absent from Gather() (Prometheus
// omits empty vecs), so absence is reported as 0 — the caller decides
// whether that is acceptable (baselines) or fatal (post-recording).
func scimCounter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var total float64
	for _, fam := range fams {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

// --- POST: provision --------------------------------------------------------

func TestSCIMUsers_PostProvision(t *testing.T) {
	ts, store, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"

	code, doc := scimCall(t, http.MethodPost, url, map[string]any{
		"schemas":    []string{scimSchemaUser},
		"userName":   "alice.smith",
		"name":       map[string]any{"givenName": "Alice", "familyName": "Smith"},
		"emails":     []map[string]any{{"value": "alice@corp.example", "type": "primary"}},
		"externalId": "az-obj-1",
		"active":     true,
		"groups":     []map[string]any{{"display": "Engineering"}},
	})
	if code != http.StatusCreated {
		t.Fatalf("POST provision = %d, want 201 (%v)", code, doc)
	}
	if got := doc["id"].(string); got == "" {
		t.Fatal("201 body missing id")
	}
	if loc := ts.URL + config.SCIMBasePath + "/Users/" + doc["id"].(string); doc["meta"].(map[string]any)["location"] != loc {
		t.Errorf("meta.location = %v, want %q", doc["meta"].(map[string]any)["location"], loc)
	}
	if got := scimDocField(t, doc, "name.givenName"); got != "Alice" {
		t.Errorf("name.givenName = %v, want Alice", got)
	}
	if got := scimDocField(t, doc, "emails.0.value"); got != "alice@corp.example" {
		t.Errorf("emails[0].value = %v, want corp email", got)
	}
	if got := doc["externalId"]; got != "az-obj-1" {
		t.Errorf("externalId = %v, want az-obj-1", got)
	}
	if got := doc["active"]; got != true {
		t.Errorf("active = %v, want true", got)
	}
	// The STOCK ROLE PLACEHOLDER + provisioned flag (F-28.5 derives real
	// roles from groups later).
	u, err := store.GetByUsername(context.Background(), "alice.smith", false)
	if err != nil {
		t.Fatalf("store lookup: %v", err)
	}
	if len(u.Roles) != 1 || u.Roles[0] != scimStockRolePlaceholder {
		t.Errorf("roles = %v, want [%s]", u.Roles, scimStockRolePlaceholder)
	}
	if !u.Provisioned {
		t.Error("provisioned = false, want true")
	}
	// meta echoes the ROW's timestamps (RFC3339, parseable).
	if _, err := time.Parse(time.RFC3339, doc["meta"].(map[string]any)["created"].(string)); err != nil {
		t.Errorf("meta.created not RFC3339: %v", err)
	}
}

func TestSCIMUsers_PostStrictDuplicate(t *testing.T) {
	ts, _, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	post := map[string]any{"userName": "dup.user", "externalId": "az-dup"}
	if code, doc := scimCall(t, http.MethodPost, url, post); code != http.StatusCreated {
		t.Fatalf("first POST = %d, want 201 (%v)", code, doc)
	}
	// Duplicate userName (different/no externalId) → strict 409 uniqueness.
	code, doc := scimCall(t, http.MethodPost, url, map[string]any{"userName": "Dup.USER"})
	if code != http.StatusConflict {
		t.Fatalf("duplicate POST = %d, want 409 (%v)", code, doc)
	}
	if doc["scimType"] != scimTypeUniqueness {
		t.Errorf("scimType = %v, want %s", doc["scimType"], scimTypeUniqueness)
	}
}

func TestSCIMUsers_PostBornInactiveHonored(t *testing.T) {
	ts, store, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	code, doc := scimCall(t, http.MethodPost, url, map[string]any{"userName": "quiet.user", "active": false})
	if code != http.StatusCreated {
		t.Fatalf("POST = %d, want 201 (%v)", code, doc)
	}
	if doc["active"] != false {
		t.Errorf("active = %v, want false (born inactive per payload)", doc["active"])
	}
	u, err := store.GetByUsername(context.Background(), "quiet.user", false)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if u.Active {
		t.Error("store active = true, want false")
	}
}

func TestSCIMUsers_PostRequiresUserName(t *testing.T) {
	ts, _, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	code, doc := scimCall(t, http.MethodPost, url, map[string]any{"name": map[string]any{"givenName": "Nobody"}})
	if code != http.StatusBadRequest {
		t.Fatalf("POST sans userName = %d, want 400 (%v)", code, doc)
	}
	if doc["scimType"] != scimTypeInvalidValue {
		t.Errorf("scimType = %v, want %s", doc["scimType"], scimTypeInvalidValue)
	}
}

// --- POST: re-provision (rehire) ---------------------------------------------

func TestSCIMUsers_PostRehirePreservesIDHistory(t *testing.T) {
	ts, store, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	code, doc := scimCall(t, http.MethodPost, url, map[string]any{"userName": "rehire.me", "externalId": "az-rh", "name": map[string]any{"givenName": "Orig"}})
	if code != http.StatusCreated {
		t.Fatalf("provision = %d, want 201", code)
	}
	origID := doc["id"].(string)
	// Operator grants an EXTRA role while the user is employed.
	if _, err := store.Update(context.Background(), &User{ID: origID, Roles: []string{scimStockRolePlaceholder, "ops"}}); err != nil {
		t.Fatalf("grant role: %v", err)
	}
	// Deprovision.
	if code, _ := scimCall(t, http.MethodDelete, url+"/"+origID, nil); code != http.StatusNoContent {
		t.Fatalf("DELETE = %d, want 204", code)
	}
	// Item ops on the tombstone are 404.
	if code, _ := scimCall(t, http.MethodGet, url+"/"+origID, nil); code != http.StatusNotFound {
		t.Fatalf("GET tombstone = %d, want 404", code)
	}
	// REHIRED VIA POST (same userName): 200, SAME id, history intact.
	code, rehired := scimCall(t, http.MethodPost, url, map[string]any{"userName": "rehire.me", "externalId": "az-rh", "name": map[string]any{"givenName": "Back"}})
	if code != http.StatusOK {
		t.Fatalf("rehire POST = %d, want 200 (%v)", code, rehired)
	}
	if rehired["id"] != origID {
		t.Fatalf("rehired id = %v, want the SAME %v", rehired["id"], origID)
	}
	if rehired["active"] != true {
		t.Errorf("rehired active = %v, want true", rehired["active"])
	}
	u, err := store.GetByID(context.Background(), origID, false)
	if err != nil {
		t.Fatalf("lookup rehirered: %v", err)
	}
	hasOps := false
	for _, r := range u.Roles {
		if r == "ops" {
			hasOps = true
		}
	}
	if !hasOps {
		t.Errorf("roles after rehire = %v, want the operator-granted 'ops' PRESERVED", u.Roles)
	}
	if u.FirstName != "Back" {
		t.Errorf("givenName after rehire = %q, want Back (repopulated)", u.FirstName)
	}
}

// --- POST: got-married rename -------------------------------------------------

func TestSCIMUsers_PostGotMarriedRename(t *testing.T) {
	ts, store, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	code, doc := scimCall(t, http.MethodPost, url, map[string]any{"userName": "mary.jones", "externalId": "az-mm"})
	if code != http.StatusCreated {
		t.Fatalf("provision mary = %d, want 201", code)
	}
	maryID := doc["id"].(string)
	// She marries: the DIRECTORY reposts her under the new surname, SAME
	// externalId → the row is RENAMED (id/history preserved).
	code, renamed := scimCall(t, http.MethodPost, url, map[string]any{"userName": "mary.smith", "externalId": "az-mm"})
	if code != http.StatusOK {
		t.Fatalf("married POST = %d, want 200 (%v)", code, renamed)
	}
	if renamed["id"] != maryID {
		t.Fatalf("renamed id = %v, want %v", renamed["id"], maryID)
	}
	if renamed["userName"] != "mary.smith" {
		t.Errorf("userName = %v, want mary.smith", renamed["userName"])
	}
	// The OLD name is gone (404), the new name resolves.
	if code, _ := scimCall(t, http.MethodGet, url+"/"+maryID, nil); code != http.StatusOK {
		t.Fatalf("GET by id = %d, want 200", code)
	}
	u, err := store.GetByUsername(context.Background(), "mary.jones", false)
	if err == nil {
		t.Errorf("old name still resolves (%v), want NotFound", u)
	}
}

func TestSCIMUsers_PostGotMarriedCollision(t *testing.T) {
	ts, _, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	// Two DIFFERENT people occupy "taken.name".
	if code, _ := scimCall(t, http.MethodPost, url, map[string]any{"userName": "taken.name", "externalId": "az-a"}); code != http.StatusCreated {
		t.Fatalf("setup = %d, want 201", code)
	}
	// Posting externalId az-b with userName "taken.name" renames az-b's OWNER
	// onto the collision → 409 uniqueness.
	code, doc := scimCall(t, http.MethodPost, url, map[string]any{"userName": "taken.name", "externalId": "az-b"})
	if code != http.StatusConflict {
		t.Fatalf("collision POST = %d, want 409 (%v)", code, doc)
	}
	if doc["scimType"] != scimTypeUniqueness {
		t.Errorf("scimType = %v, want %s", doc["scimType"], scimTypeUniqueness)
	}
}

// --- Items: 404s, PUT definite replace, immutability ---------------------------

func TestSCIMUsers_ItemsUnknownOrTombstoned404(t *testing.T) {
	ts, _, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	methods := map[string]string{
		http.MethodGet:    "",
		http.MethodPut:    `{"userName":"ghost"}`,
		http.MethodPatch:  `{"operations":[{"op":"replace","path":"/active","value":true}]}`,
		http.MethodDelete: "",
	}
	for method, body := range methods {
		var buf io.Reader
		if body != "" {
			buf = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, url+"/does-not-exist", buf)
		req.Header.Set("Authorization", "Bearer "+scimFrameworkKey)
		req.Header.Set("Accept", "application/scim+json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		data, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("%s unknown id = %d, want 404 (%s)", method, res.StatusCode, data)
			continue
		}
		var doc map[string]any
		_ = json.Unmarshal(data, &doc)
		if doc["scimType"] != scimTypeNotFound {
			t.Errorf("%s unknown id: scimType = %v, want %s", method, doc["scimType"], scimTypeNotFound)
		}
	}
}

func TestSCIMUsers_PUT_DefiniteReplaceClearsBlanks(t *testing.T) {
	ts, store, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	code, doc := scimCall(t, http.MethodPost, url, map[string]any{
		"userName": "put.target",
		"name":     map[string]any{"givenName": "Given", "familyName": "Family"},
		"emails":   []map[string]any{{"value": "put@corp.example", "type": "primary"}},
	})
	if code != http.StatusCreated {
		t.Fatalf("provision = %d, want 201", code)
	}
	id := doc["id"].(string)
	// PUT posting ONLY userName (names/email omitted ⇒ DEFAULTS "").
	code, put := scimCall(t, http.MethodPut, url+"/"+id, map[string]any{"userName": "put.target"})
	if code != http.StatusOK {
		t.Fatalf("PUT = %d, want 200 (%v)", code, put)
	}
	if got := scimDocField(t, put, "name.givenName"); got != "" {
		t.Errorf("PUT cleared givenName = %q, want \"\"", got)
	}
	if _, present := put["emails"]; present {
		t.Errorf("PUT cleared emails still present: %v", put["emails"])
	}
	if put["active"] != true {
		t.Errorf("PUT defaulted active = %v, want true", put["active"])
	}
	u, _ := store.GetByID(context.Background(), id, false)
	if u.Email != "" {
		t.Errorf("store email = %q, want cleared", u.Email)
	}
	// Roles are PRESERVED (operator grants are not SCIM's to reset).
	if len(u.Roles) != 1 || u.Roles[0] != scimStockRolePlaceholder {
		t.Errorf("roles after PUT = %v, want preserved [%s]", u.Roles, scimStockRolePlaceholder)
	}
}

func TestSCIMUsers_PUT_ImmutableExternalID(t *testing.T) {
	ts, _, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	code, doc := scimCall(t, http.MethodPost, url, map[string]any{"userName": "sticky.ext", "externalId": "az-fixed"})
	if code != http.StatusCreated {
		t.Fatalf("provision = %d, want 201", code)
	}
	id := doc["id"].(string)
	// Altering the corporate link ⇒ 400.
	code, put := scimCall(t, http.MethodPut, url+"/"+id, map[string]any{"userName": "sticky.ext", "externalId": "az-other"})
	if code != http.StatusBadRequest {
		t.Fatalf("PUT alter externalId = %d, want 400 (%v)", code, put)
	}
	// Equalling it is a no-op (fine).
	code, _ = scimCall(t, http.MethodPut, url+"/"+id, map[string]any{"userName": "sticky.ext", "externalId": "az-fixed"})
	if code != http.StatusOK {
		t.Errorf("PUT equal externalId = %d, want 200", code)
	}
}

func TestSCIMUsers_PATCH_ActiveToggle(t *testing.T) {
	ts, store, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	code, doc := scimCall(t, http.MethodPost, url, map[string]any{"userName": "toggle.me"})
	if code != http.StatusCreated {
		t.Fatalf("provision = %d, want 201", code)
	}
	id := doc["id"].(string)
	// Deactivate.
	code, patched := scimCall(t, http.MethodPatch, url+"/"+id, map[string]any{
		"schemas":    []string{"urn:ietf:params:scim:api:messages:2.0:PatchOp"},
		"operations": []map[string]any{{"op": "replace", "path": "/active", "value": false}},
	})
	if code != http.StatusOK {
		t.Fatalf("PATCH deactivate = %d, want 200 (%v)", code, patched)
	}
	if patched["active"] != false {
		t.Errorf("active = %v, want false", patched["active"])
	}
	u, _ := store.GetByID(context.Background(), id, false)
	if u.Active {
		t.Error("store active = true after deactivate, want false")
	}
	// Reactivate.
	code, patched = scimCall(t, http.MethodPatch, url+"/"+id, map[string]any{
		"operations": []map[string]any{{"op": "replace", "path": "/active", "value": true}},
	})
	if code != http.StatusOK || patched["active"] != true {
		t.Fatalf("PATCH activate = %d/%v, want 200/true", code, patched)
	}
}

func TestSCIMUsers_PATCH_Negatives(t *testing.T) {
	ts, _, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	code, doc := scimCall(t, http.MethodPost, url, map[string]any{"userName": "patch.neg"})
	if code != http.StatusCreated {
		t.Fatalf("provision = %d, want 201", code)
	}
	id := doc["id"].(string)
	cases := []struct {
		label string
		body  map[string]any
	}{
		{"unsupported attribute", map[string]any{"operations": []map[string]any{{"op": "replace", "path": "/phoneNumbers", "value": "555"}}}},
		{"bogus op kind", map[string]any{"operations": []map[string]any{{"op": "explode", "path": "/active", "value": true}}}},
		{"replace without value", map[string]any{"operations": []map[string]any{{"op": "replace", "path": "/active"}}}},
		{"empty operations", map[string]any{"operations": []map[string]any{}}},
		{"active non-boolean", map[string]any{"operations": []map[string]any{{"op": "replace", "path": "/active", "value": "yes"}}}},
		{"remove externalId (immutable)", map[string]any{"operations": []map[string]any{{"op": "remove", "path": "/externalId"}}}},
	}
	for _, tc := range cases {
		code, doc := scimCall(t, http.MethodPatch, url+"/"+id, tc.body)
		if code != http.StatusBadRequest {
			t.Errorf("%s: PATCH = %d, want 400 (%v)", tc.label, code, doc)
		}
	}
	// Remove of a CLEARABLE attribute succeeds (name.givenName → "").
	code, patched := scimCall(t, http.MethodPatch, url+"/"+id, map[string]any{
		"operations": []map[string]any{{"op": "remove", "path": "/name/givenName"}},
	})
	if code != http.StatusOK {
		t.Fatalf("remove givenName = %d, want 200", code)
	}
	if got := scimDocField(t, patched, "name.givenName"); got != "" {
		t.Errorf("givenName after remove = %q, want \"\"", got)
	}
	// The compact STRING name is accepted on patch too (both forms).
	code, patched = scimCall(t, http.MethodPatch, url+"/"+id, map[string]any{
		"operations": []map[string]any{{"op": "replace", "path": "/name", "value": "Ada Lovelace"}},
	})
	if code != http.StatusOK {
		t.Fatalf("PATCH string name = %d, want 200 (%v)", code, patched)
	}
	if g := scimDocField(t, patched, "name.givenName"); g != "Ada" {
		t.Errorf("givenName = %q, want Ada", g)
	}
	if f := scimDocField(t, patched, "name.familyName"); f != "Lovelace" {
		t.Errorf("familyName = %q, want Lovelace", f)
	}
	code, patched = scimCall(t, http.MethodPatch, url+"/"+id, map[string]any{
		"operations": []map[string]any{{"op": "replace", "path": "/name", "value": map[string]any{"givenName": "Grace", "familyName": "Hopper"}}},
	})
	if code != http.StatusOK {
		t.Fatalf("PATCH object name = %d, want 200 (%v)", code, patched)
	}
	if g := scimDocField(t, patched, "name.givenName"); g != "Grace" {
		t.Errorf("givenName = %q, want Grace", g)
	}
	// The compact STRING name is accepted on patch too (both forms).
	code, patched = scimCall(t, http.MethodPatch, url+"/"+id, map[string]any{
		"operations": []map[string]any{{"op": "replace", "path": "/name", "value": "Ada Lovelace"}},
	})
	if code != http.StatusOK {
		t.Fatalf("PATCH string name = %d, want 200 (%v)", code, patched)
	}
	if g := scimDocField(t, patched, "name.givenName"); g != "Ada" {
		t.Errorf("givenName = %q, want Ada", g)
	}
	if f := scimDocField(t, patched, "name.familyName"); f != "Lovelace" {
		t.Errorf("familyName = %q, want Lovelace", f)
	}
	code, patched = scimCall(t, http.MethodPatch, url+"/"+id, map[string]any{
		"operations": []map[string]any{{"op": "replace", "path": "/name", "value": map[string]any{"givenName": "Grace", "familyName": "Hopper"}}},
	})
	if code != http.StatusOK {
		t.Fatalf("PATCH object name = %d, want 200 (%v)", code, patched)
	}
	if g := scimDocField(t, patched, "name.givenName"); g != "Grace" {
		t.Errorf("givenName = %q, want Grace", g)
	}
}

// --- Listing: filters + pagination -------------------------------------------

func TestSCIMUsers_List_FilterPagination(t *testing.T) {
	// Small page bound (3) exercises the COUNT CLAMP.
	ts, _, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true, MaxPageSize: 3})
	base := ts.URL + config.SCIMBasePath + "/Users"
	seed := []map[string]any{
		{"userName": "alpha.one", "externalId": "az-1"},
		{"userName": "beta.two", "externalId": "az-2"},
		{"userName": "gamma.three", "externalId": "az-3"},
		{"userName": "delta.four", "externalId": "az-4"},
		{"userName": "epsilon.five", "externalId": "az-5"},
	}
	for _, s := range seed {
		if code, _ := scimCall(t, http.MethodPost, base, s); code != http.StatusCreated {
			t.Fatalf("seed %v = %d, want 201", s, code)
		}
	}
	// Locate beta.two, then deactivate it (filters + totals hinge on it).
	code, list := scimCall(t, http.MethodGet, scimURL(t, base, `filter=userName eq "beta.two"`), nil)
	if code != http.StatusOK {
		t.Fatalf("filtered list = %d, want 200", code)
	}
	if tot := list["totalResults"].(float64); tot != 1 {
		t.Fatalf("totalResults(beta) = %v, want 1", tot)
	}
	betaID := list["Resources"].([]any)[0].(map[string]any)["id"].(string)
	scimCall(t, http.MethodPatch, base+"/"+betaID, map[string]any{"operations": []map[string]any{{"op": "replace", "path": "/active", "value": false}}})

	getList := func(query string) (int, map[string]any) {
		return scimCall(t, http.MethodGet, scimURL(t, base, strings.TrimPrefix(query, "?")), nil)
	}

	// UNFILTERED: totalResults = 5; PAGE SIZE clamped to the bound (3).
	code, page1 := getList("?startIndex=1")
	if code != http.StatusOK {
		t.Fatalf("list = %d, want 200", code)
	}
	if tot := page1["totalResults"].(float64); tot != 5 {
		t.Errorf("totalResults = %v, want 5", tot)
	}
	if ips := page1["itemsPerPage"].(float64); ips != 3 {
		t.Errorf("itemsPerPage = %v, want 3 (clamped to the bound)", ips)
	}
	// count SMALLER THAN THE BOUND is honored.
	_, pc := getList("?startIndex=1&count=2")
	if ips := pc["itemsPerPage"].(float64); ips != 2 {
		t.Errorf("count=2 itemsPerPage = %v, want 2", ips)
	}
	// Walk the PAGES: advance the START INDEX BY THE RETURNED PAGE LENGTH
	// (stepping by 1 would revisit overlapping windows), collecting every
	// user exactly once.
	seen := map[string]int{}
	si := 1
	for {
		code, pg := getList(fmt.Sprintf("?startIndex=%d", si))
		if code != http.StatusOK {
			t.Fatalf("page %d = %d, want 200", si, code)
		}
		res := pg["Resources"].([]any)
		if len(res) == 0 {
			break
		}
		itemsPerPage := int(pg["itemsPerPage"].(float64))
		for _, r := range res {
			uid := r.(map[string]any)["id"].(string)
			seen[uid]++
		}
		si += itemsPerPage
		if si > 100 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != 5 {
		t.Errorf("walked %d distinct users, want 5", len(seen))
	}
	for uid, n := range seen {
		if n != 1 {
			t.Errorf("user %s visited %d times, want 1 (disjoint pages)", uid, n)
		}
	}
	// FILTERED: active eq false → only beta.
	_, filt := getList("?filter=active eq false")
	if tot := filt["totalResults"].(float64); tot != 1 {
		t.Errorf("active=false totalResults = %v, want 1", tot)
	}
	// CHAINED AND: userName + active.
	_, chained := getList("?filter=userName eq \"alpha.one\" AND active eq true")
	if tot := chained["totalResults"].(float64); tot != 1 {
		t.Errorf("chained totalResults = %v, want 1", tot)
	}
	_, none := getList("?filter=userName eq \"alpha.one\" AND active eq false")
	if tot := none["totalResults"].(float64); tot != 0 {
		t.Errorf("contradictory chain totalResults = %v, want 0", tot)
	}
	// externalId eq.
	_, ext := getList("?filter=externalId eq az-3")
	if tot := ext["totalResults"].(float64); tot != 1 {
		t.Errorf("externalId totalResults = %v, want 1", tot)
	}
}

func TestSCIMUsers_List_UnsupportedFilters400(t *testing.T) {
	ts, _, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	base := ts.URL + config.SCIMBasePath + "/Users"
	bads := []string{
		"?filter=userName co alpha",                   // other comparator
		"?filter=userName ne alpha",                   // negation
		"?filter=passwordNumber eq 1",                 // unknown attribute
		"?filter=(userName eq a) OR (active eq true)", // OR
		"?filter=userName eq \"oops",                  // unterminated quote
		"?filter=name.givenName eq Al",                // dotted path
	}
	for _, q := range bads {
		code, doc := scimCall(t, http.MethodGet, scimURL(t, base, strings.TrimPrefix(q, "?")), nil)
		if code != http.StatusBadRequest {
			t.Errorf("%s: = %d, want 400 (%v)", q, code, doc)
			continue
		}
		if doc["scimType"] != scimTypeInvalidFilter {
			t.Errorf("%s: scimType = %v, want %s", q, doc["scimType"], scimTypeInvalidFilter)
		}
	}
	// Bad count/startIndex shapes.
	for _, q := range []string{"?count=0", "?count=x", "?startIndex=0", "?startIndex=-1"} {
		if code, _ := scimCall(t, http.MethodGet, scimURL(t, base, strings.TrimPrefix(q, "?")), nil); code != http.StatusBadRequest {
			t.Errorf("%s: = %d, want 400", q, code)
		}
	}
	// Positive CONTROL: a quoted value admitting spaces is subset-VALID
	// (RFC 7644: the value is delimited by the quotes, not whitespace) —
	// contrast with the unquoted dangling-term case above.
	if code, _ := scimCall(t, http.MethodGet, scimURL(t, base, `filter=userName EQ "a b c"`), nil); code != http.StatusOK {
		t.Errorf("quoted-space filter = %d, want 200 (subset-valid)", code)
	}
}

// --- Wrong verbs + degradation -------------------------------------------------

func TestSCIMUsers_WrongVerb405(t *testing.T) {
	ts, _, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	base := ts.URL + config.SCIMBasePath + "/Users"
	// DELETE on the COLLECTION (legal verbs: GET/POST).
	req, _ := http.NewRequest(http.MethodDelete, base, nil)
	req.Header.Set("Authorization", "Bearer "+scimFrameworkKey)
	req.Header.Set("Accept", "application/scim+json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /Users = %d, want 405", res.StatusCode)
	}
	allow := res.Header.Get("Allow")
	if !strings.Contains(allow, "POST") || !strings.Contains(allow, "GET") {
		t.Errorf("Allow = %q, want GET+POST advertised", allow)
	}
	// POST on an ITEM (legal: GET/PUT/PATCH/DELETE).
	code, _ := scimCall(t, http.MethodPost, base+"/some-item", map[string]any{"userName": "x"})
	if code != http.StatusMethodNotAllowed {
		t.Errorf("POST /Users/{id} = %d, want 405", code)
	}
}

func TestSCIMUsers_NoBacking503(t *testing.T) {
	// Surface enabled, authorizer attached, but NO user store wired.
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Scim = config.ScimConfig{Enabled: true}
	km, _ := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	srv.SetScimAuthorizer(&scimPermissiveAuthorizer{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	code, _ := scimCall(t, http.MethodGet, ts.URL+config.SCIMBasePath+"/Users", nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("GET /Users without backing = %d, want 503", code)
	}
}

// --- Audit + metrics -----------------------------------------------------------

// auditChangedFlag pulls the `changed` marker out of an event's details
// (STRUCTURED when produced by the /Users handlers; encoded defensively for
// any flavor).
func auditChangedFlag(t *testing.T, ev audit.Event) bool {
	t.Helper()
	switch det := ev.Details.(type) {
	case map[string]any:
		ch, _ := det["changed"].(bool)
		return ch
	case string:
		var doc map[string]any
		if err := json.Unmarshal([]byte(det), &doc); err != nil {
			t.Fatalf("parse details %q: %v", det, err)
		}
		ch, _ := doc["changed"].(bool)
		return ch
	default:
		var doc map[string]any
		raw := audit.EncodeFieldValue(det)
		if err := json.Unmarshal([]byte(raw), &doc); err != nil {
			t.Fatalf("encode+parse details %v: %v", det, err)
		}
		ch, _ := doc["changed"].(bool)
		return ch
	}
}

func TestSCIMUsers_MutationsAuditedConnectorAttributed(t *testing.T) {
	ts, _, aud, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	before := len(aud.events)
	scimCall(t, http.MethodPost, url, map[string]any{"userName": "audit.check"})
	// A LIST read is UNAUDITED: exactly one event (the provision) so far.
	scimCall(t, http.MethodGet, url, nil)
	if got := len(aud.events); got != before+1 {
		t.Fatalf("events after provision+read = %d, want %d (reads unaudited)", got, before+1)
	}
	ev := aud.events[len(aud.events)-1]
	if ev.Action != "scim.provision" {
		t.Errorf("latest action = %q, want scim.provision", ev.Action)
	}
	// A TRANSITION attaches the FULL profile (nothing existed beforehand) —
	// as STRUCTURED documents (parser-friendly, not rendered strings).
	if !auditChangedFlag(t, ev) || ev.OldValue != nil {
		t.Errorf("provision audit = {changed:%v old:%v}, want full (changed=true, empty-old)", auditChangedFlag(t, ev), ev.OldValue)
	}
	prof, ok := ev.NewValue.(map[string]any)
	if !ok || prof["username"] != "audit.check" {
		t.Errorf("provision NewValue = %v (%T), want a structured profile naming audit.check", ev.NewValue, ev.NewValue)
	}
	locate := func(userName string) string {
		_, rs := scimCall(t, http.MethodGet, url, nil)
		arr, _ := rs["Resources"].([]any)
		for _, r := range arr {
			if r.(map[string]any)["userName"] == userName {
				return r.(map[string]any)["id"].(string)
			}
		}
		t.Fatalf("could not locate %s", userName)
		return ""
	}
	id := locate("audit.check")
	// RESYNC CONTACT (no movement): a PUT reposting the SAME profile must
	// record a LEAN event — the contact fact, but NO profile blobs (the
	// anti-noise rule for thousand-scale bulk syncs).
	scimCall(t, http.MethodPut, url+"/"+id, map[string]any{"userName": "audit.check", "active": true})
	ev = aud.events[len(aud.events)-1]
	if ev.Action != "scim.update" {
		t.Errorf("resync action = %q, want scim.update", ev.Action)
	}
	if auditChangedFlag(t, ev) {
		t.Errorf("resync audit claims CHANGED (details=%v)", ev.Details)
	}
	if ev.OldValue != nil || ev.NewValue != nil {
		t.Errorf("resync audit leaked profile blobs (old=%v new=%v), want NONE (lean event)", ev.OldValue, ev.NewValue)
	}
	// REAL CHANGE (movement): renaming the userName attaches the FULL
	// before/after profiles — structured documents showing the transition.
	scimCall(t, http.MethodPut, url+"/"+id, map[string]any{"userName": "audit.moved", "active": true})
	ev = aud.events[len(aud.events)-1]
	if !auditChangedFlag(t, ev) {
		t.Fatalf("change audit claims UNCHANGED (details=%v)", ev.Details)
	}
	oldProf, okO := ev.OldValue.(map[string]any)
	newProf, okN := ev.NewValue.(map[string]any)
	if !okO || !okN || oldProf["username"] != "audit.check" || newProf["username"] != "audit.moved" {
		t.Errorf("change audit = {old:%v new:%v}, want structured before/after spanning the rename", ev.OldValue, ev.NewValue)
	}
	// Attribution invariants hold across ALL events (lean and full alike).
	for _, ev := range aud.events {
		if ev.Actor != "SCIM connector (#test)" {
			t.Errorf("actor = %q, want the presenting connector", ev.Actor)
		}
		if ev.TargetKind != "user" {
			t.Errorf("targetKind = %q, want user", ev.TargetKind)
		}
	}
}

func TestSCIMUsers_OperationMetered(t *testing.T) {
	ts, _, _, reg := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	url := ts.URL + config.SCIMBasePath + "/Users"
	const name = "cdrom_scim_operations_total"
	if got := scimCounter(t, reg, name); got != 0 {
		t.Fatalf("baseline = %v, want 0", got)
	}
	scimCall(t, http.MethodPost, url, map[string]any{"userName": "meter.check"})
	// Patch deactivate (counts as an "activate"/"deactivate" op).
	code, doc := scimCall(t, http.MethodGet, url, nil)
	_ = code
	id := ""
	if rs, ok := doc["Resources"].([]any); ok && len(rs) > 0 {
		for _, r := range rs {
			if r.(map[string]any)["userName"] == "meter.check" {
				id = r.(map[string]any)["id"].(string)
			}
		}
	}
	if id == "" {
		t.Fatal("could not find the metered user")
	}
	scimCall(t, http.MethodPatch, url+"/"+id, map[string]any{"operations": []map[string]any{{"op": "replace", "path": "/active", "value": false}}})
	scimCall(t, http.MethodDelete, url+"/"+id, nil)
	if got := scimCounter(t, reg, name); got != 3 {
		t.Errorf("operations = %v, want 3 (provision + deactivate + deprovision)", got)
	}
}

// --- Parser units ----------------------------------------------------------------

func TestParseSCIMFilterUnits(t *testing.T) {
	type ec = EqualityConstraint
	good := []struct {
		expr string
		want []ec
	}{
		{"", nil},
		{"userName eq alice", []ec{{Attribute: "username", Value: "alice", CaseSensitive: false}}},
		{"userName EQ Alice", []ec{{Attribute: "username", Value: "Alice", CaseSensitive: false}}}, // keyword folds, value preserved
		{"userName eq alice AND active eq true", []ec{{Attribute: "username", Value: "alice"}, {Attribute: "active", Value: "true"}}},
		{"userName eq alice and active eq true", []ec{{Attribute: "username", Value: "alice"}, {Attribute: "active", Value: "true"}}},
		{"userName eq \"a b c\"", []ec{{Attribute: "username", Value: "a b c"}}}, // quoted value with spaces
		{"active eq false", []ec{{Attribute: "active", Value: "false"}}},
		{"userName eq a and externalId eq \"x-Y\"", []ec{{Attribute: "username", Value: "a"}, {Attribute: "external_id", Value: "x-Y", CaseSensitive: true}}},
	}
	for _, tc := range good {
		cs, err := parseSCIMFilter(tc.expr)
		if err != nil {
			t.Errorf("parse %q: %v", tc.expr, err)
			continue
		}
		if tc.want == nil {
			if cs != nil {
				t.Errorf("parse %q: constraints = %v, want nil", tc.expr, *cs)
			}
			continue
		}
		if cs == nil || len(*cs) != len(tc.want) {
			t.Errorf("parse %q: got %#v, want %v", tc.expr, cs, tc.want)
			continue
		}
		for i, w := range tc.want {
			if got := (*cs)[i]; got != w {
				t.Errorf("parse %q[%d]: got {%s|%s|sens=%v}, want {%s|%s|sens=%v}", tc.expr, i, got.Attribute, got.Value, got.CaseSensitive, w.Attribute, w.Value, w.CaseSensitive)
			}
		}
	}
	bads := []string{
		"userName gt alice",
		"password eq hunter2",
		"(userName eq a)",
		"a OR b",
		"userName eq \"broken",
		"name.givenName eq Al",
		"userName eq",
		"active eq maybe",
		"USERNAME eq alice", // attribute names are case-SENSITIVE (camelCase)
		"userName eq a b c", // UNQUOTED spaces tokenize apart → dangling term
	}
	for _, expr := range bads {
		if _, err := parseSCIMFilter(expr); err == nil {
			t.Errorf("parse %q: wanted an error, got none", expr)
		}
	}
}

// TestMemoryUserStore_EqualityPushdown certifies the MEMORY evaluator agrees
// with the Database service's pushdown on the same inputs: casefolded vs
// literal string comparison, boolean literalism, and UNKNOWN-ATTRIBUTE
// rejection (InvalidArgument, mirrored).
func TestMemoryUserStore_EqualityPushdown(t *testing.T) {
	u := &User{Username: "Mixed.Name", ExternalID: "EXT-1", Email: "mail@x", FirstName: "Mi", LastName: "Xe", Active: true, Provisioned: false}
	cases := []struct {
		ec   EqualityConstraint
		want bool
	}{
		{EqualityConstraint{Attribute: "username", Value: "mixed.name"}, true},                       // casefolded (sense=false)
		{EqualityConstraint{Attribute: "username", Value: "mixed.name", CaseSensitive: true}, false}, // literal
		{EqualityConstraint{Attribute: "external_id", Value: "EXT-1", CaseSensitive: true}, true},
		{EqualityConstraint{Attribute: "external_id", Value: "ext-1", CaseSensitive: true}, false},
		{EqualityConstraint{Attribute: "active", Value: "true"}, true},
		{EqualityConstraint{Attribute: "active", Value: "TRUE"}, false}, // booleans compare LITERALLY
		{EqualityConstraint{Attribute: "provisioned", Value: "false"}, true},
		{EqualityConstraint{Attribute: "first_name", Value: "mi"}, true},
	}
	for _, tc := range cases {
		if got := matchesEquality(tc.ec, u); got != tc.want {
			t.Errorf("matchesEquality{%s=%q sens=%v}: got %v, want %v", tc.ec.Attribute, tc.ec.Value, tc.ec.CaseSensitive, got, tc.want)
		}
	}
	// Unknown attributes are REJECTED by List (mirroring the database).
	store := NewMemoryUserStore()
	if _, err := store.Create(context.Background(), &User{Username: "push.mem"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	bogus := EqualityConstraints{{Attribute: "bogus_attribute", Value: "x"}}
	if _, err := store.List(context.Background(), false, &bogus); status.Code(err) != codes.InvalidArgument {
		t.Errorf("unknown attribute: err=%v, want InvalidArgument", err)
	}
	bogusConst := EqualityConstraints{{Attribute: "active", Value: "maybe"}}
	if _, err := store.List(context.Background(), false, &bogusConst); status.Code(err) != codes.InvalidArgument {
		t.Errorf("non-literal boolean constant: err=%v, want InvalidArgument", err)
	}
	// A valid constraint narrows the memory listing.
	hit := EqualityConstraints{{Attribute: "username", Value: "PUSH.MEM"}}
	got, err := store.List(context.Background(), false, &hit)
	if err != nil || len(got) != 1 || got[0].Username != "push.mem" {
		t.Errorf("narrowed list = %v (%v), want [push.mem]", got, err)
	}
	// Literal (case-SENSITIVE) comparison MISSES the differently-cased posting.
	miss := EqualityConstraints{{Attribute: "username", Value: "PUSH.MEM", CaseSensitive: true}}
	got, err = store.List(context.Background(), false, &miss)
	if err != nil || len(got) != 0 {
		t.Errorf("literal-mismatch list = %v (%v), want empty", got, err)
	}
}

func TestDecodeSCIMUserUnits(t *testing.T) {
	// Missing userName.
	if _, err := decodeSCIMUser(map[string]any{"name": map[string]any{}}); err == nil {
		t.Error("decode sans userName: wanted an error")
	}
	// Blank userName.
	if _, err := decodeSCIMUser(map[string]any{"userName": "  "}); err == nil {
		t.Error("decode blank userName: wanted an error")
	}
	// Rich doc: primary email picked, non-primary ignored, extras tolerated.
	p, err := decodeSCIMUser(map[string]any{
		"userName":   "rich.doc",
		"name":       map[string]any{"givenName": "Rich", "familyName": "Doc"},
		"emails":     []any{map[string]any{"value": "sec@x", "type": "work"}, map[string]any{"value": "prim@x", "type": "primary"}},
		"externalId": "az-rich",
		"nickName":   "whatever",
		"groups":     []any{map[string]any{"display": "Eng"}},
	})
	if err != nil {
		t.Fatalf("decode rich: %v", err)
	}
	if p.PrimaryEmail != "prim@x" {
		t.Errorf("PrimaryEmail = %q, want prim@x (PRIMARY, not the work one)", p.PrimaryEmail)
	}
	// F-28.5 Slice 3 switchover: the embedded references are CAPTURED (no
	// longer acknowledged-and-discarded).
	if len(p.EmbeddedGroups) != 1 {
		t.Errorf("EmbeddedGroups = %v, want the one posted reference", p.EmbeddedGroups)
	}
	// Lone untyped email tolerated as primary.
	p2, err := decodeSCIMUser(map[string]any{"userName": "loner.email", "emails": []any{map[string]any{"value": "only@x"}}})
	if err != nil || p2.PrimaryEmail != "only@x" {
		t.Errorf("lone email decode = %v/%v, want only@x", p2, err)
	}
	// active absent ⇒ nil (default territory).
	if p.Active != nil {
		t.Errorf("absent active = %v, want nil", *p.Active)
	}
	// Compact STRING name tolerated: first token ⇒ given, remainder ⇒ family.
	pc, err := decodeSCIMUser(map[string]any{"userName": "compact.name", "name": "Jane Q. Public"})
	if err != nil {
		t.Fatalf("decode compact name: %v", err)
	}
	if pc.GivenName != "Jane" || pc.FamilyName != "Q. Public" {
		t.Errorf("compact name split = %q/%q, want Jane / Q. Public", pc.GivenName, pc.FamilyName)
	}
	ps, err := decodeSCIMUser(map[string]any{"userName": "solo.name", "name": "Solo"})
	if err != nil || ps.GivenName != "Solo" || ps.FamilyName != "" {
		t.Errorf("solo name decode = %+v (%v), want Solo/\"\"", ps, err)
	}
}
