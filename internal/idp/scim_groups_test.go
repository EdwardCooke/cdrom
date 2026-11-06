// F-28.5 Slice 2 — the SCIM /Groups resource.
//
// Coverage: the verb × state matrix, the RFC 7643 Group schema shape
// (members expansion: resolved ⇒ $ref/displayName, unresolvable ⇒ {value}),
// duplicate-displayName legality, unresolvable-value tolerance, the filter
// subset + pagination, and the audit trail (lean no-move contacts).
package idp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"cdrom/internal/audit"
	"cdrom/internal/config"
	"cdrom/internal/metrics"
)

// scimGroupsFixture builds the /Groups test world: an IdP HTTP server with
// the SCIM surface ENABLED, the permissive authorizer (attribution "SCIM
// connector (#test)"), a MemoryGroupStore backing, a MemoryUserStore for
// member RESOLUTION, a capturing auditor, and a prometheus registry. It
// mirrors scimUsersFixture but RETAINS the *Server so the group backing can
// be attached too.
func scimGroupsFixture(t *testing.T, scimCfg config.ScimConfig) (*httptest.Server, *MemoryGroupStore, *MemoryUserStore, *capSCIMAuditor, *prometheus.Registry) {
	t.Helper()
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Scim = scimCfg
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	rec := prometheus.NewRegistry()
	ms := metrics.NewIDPMetrics(rec)
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	srv.SetMetrics(ms)
	srv.SetScimAuthorizer(&scimPermissiveAuthorizer{})
	users := NewMemoryUserStore()
	groups := NewMemoryGroupStore()
	aud := &capSCIMAuditor{}
	srv.SetScimUserBacking(users, aud)
	srv.SetScimGroupBacking(groups)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, groups, users, aud, rec
}

// groupsBase is the absolute /Groups collection URL for a fixture server.
func groupsBase(url string) string { return url + config.SCIMBasePath + "/Groups" }

// sortedMembers extracts the member VALUES from a decoded group doc, sorted.
func sortedMembers(t *testing.T, doc map[string]any) []string {
	t.Helper()
	raw, ok := doc["members"].([]any)
	if !ok {
		t.Fatalf("doc has no members array: %#v", doc)
	}
	out := make([]string, 0, len(raw))
	for _, m := range raw {
		obj, ok := m.(map[string]any)
		if !ok {
			t.Fatalf("member is not an object: %#v", m)
		}
		v, _ := obj["value"].(string)
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// memberObjs renders bare values as SCIM member objects.
func memberObjs(values ...string) []map[string]any {
	out := make([]map[string]any, 0, len(values))
	for _, v := range values {
		out = append(out, map[string]any{"value": v})
	}
	return out
}

// =====================================================================
// Backing degradation
// =====================================================================

// TestSCIMGroups_UnattachedBacking: the /Groups routes degrade to 503 when
// the group directory is unwired (the surface is mounted but its backing is
// missing — a misconfiguration, degraded rather than guessed at). The /Users
// fixture does NOT attach a group backing.
func TestSCIMGroups_UnattachedBacking(t *testing.T) {
	ts, _, _, _ := scimUsersFixture(t, config.ScimConfig{Enabled: true})
	code, _ := scimCall(t, http.MethodGet, groupsBase(ts.URL), nil)
	if code != http.StatusServiceUnavailable {
		t.Errorf("GET /Groups (unattached) = %d, want 503", code)
	}
}

// =====================================================================
// POST /Groups
// =====================================================================

func TestSCIMGroups_Create(t *testing.T) {
	ts, groups, _, _, _ := scimGroupsFixture(t, config.ScimConfig{Enabled: true})
	base := groupsBase(ts.URL)

	code, doc := scimCall(t, http.MethodPost, base, map[string]any{
		"schemas":     []string{scimSchemaGroup},
		"displayName": "Platform Engineering",
		"description": "owns the platform",
		"members": []map[string]any{
			{"$ref": "irrelevant/ref", "value": "ext-alice", "displayName": "should-be-ignored"},
			{"value": "ext-bob"},
			{"value": "ext-alice"}, // DUP — deduplicated
			{"value": "  "},        // BLANK — dropped
		},
	})
	if code != http.StatusCreated {
		t.Fatalf("POST = %d, want 201 (body: %v)", code, doc)
	}
	if doc["displayName"] != "Platform Engineering" {
		t.Errorf("echoed displayName = %v", doc["displayName"])
	}
	if doc["description"] != "owns the platform" {
		t.Errorf("echoed description = %v", doc["description"])
	}
	if got := sortedMembers(t, doc); !reflect.DeepEqual(got, []string{"ext-alice", "ext-bob"}) {
		t.Errorf("members = %v, want [ext-alice ext-bob] (deduped, blanks dropped)", got)
	}
	if rt, _ := doc["meta"].(map[string]any)["resourceType"]; rt != "Group" {
		t.Errorf("meta.resourceType = %v, want Group", rt)
	}
	if loc, _ := doc["meta"].(map[string]any)["location"].(string); !strings.Contains(loc, "/Groups/") {
		t.Errorf("meta.location = %q, want a /Groups/<id> URL", loc)
	}
	stored, err := groups.List(context.Background(), nil)
	if err != nil {
		t.Fatalf("store list: %v", err)
	}
	if len(stored) != 1 || stored[0].ID == "" {
		t.Fatalf("store = %+v, want one group with an id", stored)
	}

	// Malformed: missing displayName ⇒ 400 invalidValue.
	code, doc = scimCall(t, http.MethodPost, base, map[string]any{"members": []any{}})
	if code != http.StatusBadRequest {
		t.Fatalf("POST(no displayName) = %d, want 400", code)
	}
	if st, _ := doc["scimType"].(string); st != scimTypeInvalidValue {
		t.Errorf("scimType = %v, want %s", doc["scimType"], scimTypeInvalidValue)
	}
	// Blank displayName ⇒ 400.
	if code, _ = scimCall(t, http.MethodPost, base, map[string]any{"displayName": "   "}); code != http.StatusBadRequest {
		t.Errorf("POST(blank displayName) = %d, want 400", code)
	}
	// Non-object body ⇒ 400.
	if code, _ = scimCall(t, http.MethodPost, base, []any{"junk"}); code != http.StatusBadRequest {
		t.Errorf("POST(array body) = %d, want 400", code)
	}
}

// =====================================================================
// GET /Groups (list + filter + pagination)
// =====================================================================

func TestSCIMGroups_ListFilterPagination(t *testing.T) {
	ts, _, _, _, _ := scimGroupsFixture(t, config.ScimConfig{Enabled: true})
	base := groupsBase(ts.URL)

	mustCreate := func(name string, members ...string) string {
		code, doc := scimCall(t, http.MethodPost, base, map[string]any{
			"displayName": name, "members": memberObjs(members...),
		})
		if code != http.StatusCreated {
			t.Fatalf("seed %q = %d, want 201", name, code)
		}
		return doc["id"].(string)
	}
	alpha := mustCreate("Alpha Squad", "aa-1", "zz-9")
	beta := mustCreate("Beta Squad", "bb-1")
	gammaDup1 := mustCreate("Gamma Dup", "cc-1")
	mustCreate("Gamma Dup", "dd-1") // SAME display name — LEGAL
	mustCreate("Nobody Home")

	filtered := func(expr string) (int, map[string]any) {
		return scimCall(t, http.MethodGet, scimURL(t, base, "filter="+expr), nil)
	}
	paged := func(rawQuery string) (int, map[string]any) {
		return scimCall(t, http.MethodGet, scimURL(t, base, rawQuery), nil)
	}

	// PLAIN list: all five, ordered by id.
	code, doc := scimCall(t, http.MethodGet, base, nil)
	if code != http.StatusOK {
		t.Fatalf("GET = %d, want 200", code)
	}
	if tot, _ := doc["totalResults"].(float64); int(tot) != 5 {
		t.Errorf("totalResults = %v, want 5", doc["totalResults"])
	}
	ress := doc["Resources"].([]any)
	if len(ress) != 5 {
		t.Fatalf("resources = %d, want 5", len(ress))
	}
	if first := ress[0].(map[string]any); first["id"] != alpha {
		t.Errorf("first resource id = %v, want %s (id order)", first["id"], alpha)
	}

	// Filter: displayName eq (LOOSE by convention).
	code, doc = filtered("displayName eq \"beta squad\"")
	if code != http.StatusOK {
		t.Fatalf("filtered GET = %d, want 200", code)
	}
	if tot, _ := doc["totalResults"].(float64); int(tot) != 1 {
		t.Fatalf("displayName filter totalResults = %v, want 1 (body %v)", doc["totalResults"], doc)
	}
	if got := doc["Resources"].([]any)[0].(map[string]any)["id"]; got != beta {
		t.Errorf("displayName filter = %v, want beta", got)
	}

	// Filter: members[value] eq (EXACT — provider identifiers).
	code, doc = filtered("members[value] eq cc-1")
	if code != http.StatusOK {
		t.Fatalf("members filter GET = %d, want 200", code)
	}
	if tot, _ := doc["totalResults"].(float64); int(tot) != 1 {
		t.Fatalf("members filter totalResults = %v, want 1 (body %v)", doc["totalResults"], doc)
	}
	if got := doc["Resources"].([]any)[0].(map[string]any)["id"]; got != gammaDup1 {
		t.Errorf("members filter = %v, want gammaDup1", got)
	}
	// Exactness: uppercased value must MISS (provider ids are case-meaning).
	code, doc = filtered("members[value] eq CC-1")
	if code != http.StatusOK {
		t.Fatalf("members exact GET = %d, want 200", code)
	}
	if tot, _ := doc["totalResults"].(float64); int(tot) != 0 {
		t.Errorf("members exact (CC-1) totalResults = %v, want 0 (case matters)", tot)
	}

	// AND-chain: displayName + members[value].
	code, doc = filtered("displayName eq \"alpha squad\" and members[value] eq zz-9")
	if code != http.StatusOK {
		t.Fatalf("AND-chain GET = %d, want 200", code)
	}
	if tot, _ := doc["totalResults"].(float64); int(tot) != 1 {
		t.Errorf("AND-chain totalResults = %v, want 1", tot)
	}
	code, doc = filtered("displayName eq \"alpha squad\" and members[value] eq bb-1")
	if code != http.StatusOK {
		t.Fatalf("AND-chain(miss) GET = %d, want 200", code)
	}
	if tot, _ := doc["totalResults"].(float64); int(tot) != 0 {
		t.Errorf("AND-chain(miss) totalResults = %v, want 0", tot)
	}

	// A "*" INSIDE a value is a LITERAL (no wildcard syntax exists — only
	// `eq`; consistent with the /Users filter): it matches nothing, not 400.
	code, doc = filtered("displayName eq al*")
	if code != http.StatusOK {
		t.Fatalf("wildcard-in-value GET = %d, want 200 (literal)", code)
	}
	if tot, _ := doc["totalResults"].(float64); int(tot) != 0 {
		t.Errorf("wildcard-in-value totalResults = %v, want 0 (literal match)", tot)
	}

	// UNSUPPORTED forms ⇒ 400 invalidFilter.
	for _, expr := range []string{
		"displayName ne alpha",      // non-eq comparator
		"displayName eq a or b",     // OR
		"(displayName eq alpha)",    // parens
		"bogusAttr eq x",            // unknown attribute
		"displayName eq \"unclosed", // unterminated quote
		"members eq x",              // wrong bracket form
		"displayName eq",            // missing operand
	} {
		code, doc = filtered(expr)
		if code != http.StatusBadRequest {
			t.Errorf("filter %q = %d, want 400", expr, code)
			continue
		}
		if st, _ := doc["scimType"].(string); st != scimTypeInvalidFilter {
			t.Errorf("filter %q scimType = %v, want %s", expr, doc["scimType"], scimTypeInvalidFilter)
		}
	}

	// PAGINATION: count honored (shrinks the page), startIndex 1-based,
	// totalResults post-filter (FULL population).
	code, doc = paged("count=2&startIndex=1")
	if code != http.StatusOK {
		t.Fatalf("paged GET = %d, want 200", code)
	}
	if ip, _ := doc["itemsPerPage"].(float64); int(ip) != 2 {
		t.Errorf("itemsPerPage = %v, want 2 (count honored)", doc["itemsPerPage"])
	}
	if si, _ := doc["startIndex"].(float64); int(si) != 1 {
		t.Errorf("startIndex = %v, want 1", doc["startIndex"])
	}
	if tot, _ := doc["totalResults"].(float64); int(tot) != 5 {
		t.Errorf("totalResults(post-page) = %v, want 5", tot)
	}
	code, doc = paged("count=2&startIndex=3")
	if code != http.StatusOK {
		t.Fatalf("paged GET(3) = %d, want 200", code)
	}
	if si, _ := doc["startIndex"].(float64); int(si) != 3 {
		t.Errorf("startIndex = %v, want 3", doc["startIndex"])
	}
	if ress := doc["Resources"].([]any); len(ress) != 2 {
		t.Fatalf("page 3 size = %d, want 2", len(ress))
	}
	// Page beyond the population: empty Resources, sane indices.
	code, doc = paged("count=10&startIndex=99")
	if code != http.StatusOK {
		t.Fatalf("overshoot GET = %d, want 200", code)
	}
	if ip, _ := doc["itemsPerPage"].(float64); int(ip) != 0 {
		t.Errorf("overshoot itemsPerPage = %v, want 0", doc["itemsPerPage"])
	}
	// Bad count/startIndex ⇒ 400 invalidValue.
	if code, _ = paged("count=-1"); code != http.StatusBadRequest {
		t.Errorf("count=-1 = %d, want 400", code)
	}
	if code, _ = paged("startIndex=0"); code != http.StatusBadRequest {
		t.Errorf("startIndex=0 = %d, want 400", code)
	}
}

// =====================================================================
// GET /Groups/{id} — expansion + 404
// =====================================================================

func TestSCIMGroups_GetExpansion(t *testing.T) {
	ts, _, users, _, _ := scimGroupsFixture(t, config.ScimConfig{Enabled: true})
	base := groupsBase(ts.URL)

	// Seed a RESOLVABLE user (external id + username) and an unresolvable
	// value.
	seeded, err := users.Create(context.Background(), &User{
		Username: "carol.doe", FirstName: "Carol", LastName: "Doe",
		ExternalID: "ext-exterior", Active: true,
	})
	if err != nil {
		t.Fatalf("seed user: %v", err)
	}
	userLoc := "/Users/" + seeded.ID
	code, doc := scimCall(t, http.MethodPost, base, map[string]any{
		"displayName": "Mixed Crew",
		"members":     memberObjs("ext-exterior", "carol.doe", "mystery-person"),
	})
	if code != http.StatusCreated {
		t.Fatalf("POST = %d, want 201", code)
	}
	id := doc["id"].(string)

	code, doc = scimCall(t, http.MethodGet, base+"/"+id, nil)
	if code != http.StatusOK {
		t.Fatalf("GET = %d, want 200", code)
	}
	mems := doc["members"].([]any)
	if len(mems) != 3 {
		t.Fatalf("members = %d, want 3", len(mems))
	}
	byValue := map[string]map[string]any{}
	for _, m := range mems {
		o := m.(map[string]any)
		byValue[o["value"].(string)] = o
	}
	ext := byValue["ext-exterior"]
	if ref, _ := ext["$ref"].(string); !strings.HasSuffix(ref, userLoc) {
		t.Errorf("$ref(external) = %q, want a %s location", ref, userLoc)
	}
	if dn, _ := ext["displayName"].(string); dn != "Carol Doe" {
		t.Errorf("displayName(external) = %q, want Carol Doe", dn)
	}
	if ref, _ := byValue["carol.doe"]["$ref"].(string); !strings.HasSuffix(ref, userLoc) {
		t.Errorf("$ref(username) = %q, want a %s location", ref, userLoc)
	}
	my := byValue["mystery-person"]
	if _, has := my["$ref"]; has {
		t.Errorf("unresolvable member carries $ref: %v", my)
	}
	if _, has := my["displayName"]; has {
		t.Errorf("unresolvable member carries displayName: %v", my)
	}
	if v, _ := my["value"].(string); v != "mystery-person" {
		t.Errorf("unresolvable value = %v, want mystery-person (KEPT)", v)
	}

	// Unknown id ⇒ 404 notFound.
	code, doc = scimCall(t, http.MethodGet, base+"/9999", nil)
	if code != http.StatusNotFound {
		t.Fatalf("GET(unknown) = %d, want 404", code)
	}
	if st, _ := doc["scimType"].(string); st != scimTypeNotFound {
		t.Errorf("scimType = %v, want %s", doc["scimType"], scimTypeNotFound)
	}
}

// =====================================================================
// PUT /Groups/{id} — definite replace
// =====================================================================

func TestSCIMGroups_PutDefiniteReplace(t *testing.T) {
	ts, _, _, _, _ := scimGroupsFixture(t, config.ScimConfig{Enabled: true})
	base := groupsBase(ts.URL)

	code, doc := scimCall(t, http.MethodPost, base, map[string]any{
		"displayName": "Original", "description": "was here",
		"members": memberObjs("m1", "m2"),
	})
	if code != http.StatusCreated {
		t.Fatalf("POST = %d, want 201", code)
	}
	id := doc["id"].(string)

	// Full replace: new name/desc + swapped membership.
	code, doc = scimCall(t, http.MethodPut, base+"/"+id, map[string]any{
		"displayName": "Renamed", "description": "now here",
		"members": memberObjs("m3"),
	})
	if code != http.StatusOK {
		t.Fatalf("PUT = %d, want 200", code)
	}
	if doc["displayName"] != "Renamed" || doc["description"] != "now here" {
		t.Errorf("PUT echo = %v / %v, want Renamed / now here", doc["displayName"], doc["description"])
	}
	if got := sortedMembers(t, doc); !reflect.DeepEqual(got, []string{"m3"}) {
		t.Errorf("PUT members = %v, want [m3] (WHOLESALE swap)", got)
	}

	// Omitted attributes CLEAR (PUT semantics — definite, not sparse).
	code, doc = scimCall(t, http.MethodPut, base+"/"+id, map[string]any{"displayName": "Lean"})
	if code != http.StatusOK {
		t.Fatalf("PUT(lean) = %d, want 200", code)
	}
	if doc["description"] != nil {
		t.Errorf("PUT(lean) description = %v, want ABSENT (cleared)", doc["description"])
	}
	if got := sortedMembers(t, doc); len(got) != 0 {
		t.Errorf("PUT(lean) members = %v, want EMPTY (omitted ⇒ emptied)", got)
	}

	// PUT on an unknown id ⇒ 404.
	if code, _ := scimCall(t, http.MethodPut, base+"/9999", map[string]any{"displayName": "Ghost"}); code != http.StatusNotFound {
		t.Errorf("PUT(unknown) = %d, want 404", code)
	}
	// Malformed PUT (missing displayName) ⇒ 400.
	if code, _ := scimCall(t, http.MethodPut, base+"/"+id, map[string]any{"members": nil}); code != http.StatusBadRequest {
		t.Errorf("PUT(no displayName) = %d, want 400", code)
	}
}

// =====================================================================
// PATCH /Groups/{id}
// =====================================================================

func TestSCIMGroups_PatchOps(t *testing.T) {
	ts, _, _, _, _ := scimGroupsFixture(t, config.ScimConfig{Enabled: true})
	base := groupsBase(ts.URL)

	code, doc := scimCall(t, http.MethodPost, base, map[string]any{
		"displayName": "Patch Base", "members": memberObjs("p1", "p2"),
	})
	if code != http.StatusCreated {
		t.Fatalf("POST = %d, want 201", code)
	}
	id := doc["id"].(string)

	// members add + remove in ONE op (objects AND bare strings).
	code, doc = scimCall(t, http.MethodPatch, base+"/"+id, map[string]any{
		"Schemas": []string{scimSchemaGroup},
		"Operations": []map[string]any{
			{"op": "add", "path": "members", "value": []any{map[string]any{"value": "p3"}, "p4"}},
			{"op": "remove", "path": "members", "value": "p1"},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("PATCH = %d, want 200 (body %v)", code, doc)
	}
	if got := sortedMembers(t, doc); !reflect.DeepEqual(got, []string{"p2", "p3", "p4"}) {
		t.Errorf("PATCH members = %v, want [p2 p3 p4]", got)
	}

	// displayName replace + description add + members remove (COMPOSED).
	code, doc = scimCall(t, http.MethodPatch, base+"/"+id, map[string]any{
		"Operations": []map[string]any{
			{"op": "replace", "path": "displayName", "value": "Patched Name"},
			{"op": "add", "path": "description", "value": "added by patch"},
			{"op": "remove", "path": "members", "value": []any{"p4"}},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("PATCH(compose) = %d, want 200 (body %v)", code, doc)
	}
	if doc["displayName"] != "Patched Name" || doc["description"] != "added by patch" {
		t.Errorf("PATCH attrs = %v / %v", doc["displayName"], doc["description"])
	}
	if got := sortedMembers(t, doc); !reflect.DeepEqual(got, []string{"p2", "p3"}) {
		t.Errorf("PATCH members = %v, want [p2 p3]", got)
	}

	// description remove clears it.
	code, doc = scimCall(t, http.MethodPatch, base+"/"+id, map[string]any{
		"Operations": []map[string]any{{"op": "remove", "path": "description"}},
	})
	if code != http.StatusOK {
		t.Fatalf("PATCH(desc remove) = %d, want 200", code)
	}
	if doc["description"] != nil {
		t.Errorf("description after remove = %v, want ABSENT", doc["description"])
	}

	// Negatives: unsupported op/path/attr, missing value, bad member shape.
	negatives := []map[string]any{
		{"Operations": []map[string]any{{"op": "explode", "path": "members", "value": "x"}}},
		{"Operations": []map[string]any{{"op": "add", "path": "manager", "value": "x"}}},
		{"Operations": []map[string]any{{"op": "remove", "path": "displayName"}}},
		{"Operations": []map[string]any{{"op": "add", "path": "members", "value": 42}}},
		{"Operations": []map[string]any{{"op": "add", "path": "members", "value": map[string]any{"$ref": "only"}}}},
		{"Operations": nil},
	}
	for i, neg := range negatives {
		if code, doc := scimCall(t, http.MethodPatch, base+"/"+id, neg); code != http.StatusBadRequest {
			t.Errorf("negative[%d] = %d, want 400 (body %v)", i, code, doc)
		}
	}
	// PATCH on an unknown id ⇒ 404.
	if code, _ := scimCall(t, http.MethodPatch, base+"/9999", map[string]any{
		"Operations": []map[string]any{{"op": "add", "path": "members", "value": "x"}},
	}); code != http.StatusNotFound {
		t.Errorf("PATCH(unknown) = %d, want 404", code)
	}
}

// =====================================================================
// DELETE /Groups/{id}
// =====================================================================

func TestSCIMGroups_Delete(t *testing.T) {
	ts, _, _, _, _ := scimGroupsFixture(t, config.ScimConfig{Enabled: true})
	base := groupsBase(ts.URL)

	code, doc := scimCall(t, http.MethodPost, base, map[string]any{
		"displayName": "Demolishable", "members": memberObjs("x1"),
	})
	if code != http.StatusCreated {
		t.Fatalf("POST = %d, want 201", code)
	}
	id := doc["id"].(string)

	if code, _ := scimCall(t, http.MethodDelete, base+"/"+id, nil); code != http.StatusNoContent {
		t.Fatalf("DELETE = %d, want 204", code)
	}
	// Gone: 404 on every item verb.
	if code, _ := scimCall(t, http.MethodGet, base+"/"+id, nil); code != http.StatusNotFound {
		t.Errorf("GET(deleted) = %d, want 404", code)
	}
	if code, _ := scimCall(t, http.MethodDelete, base+"/"+id, nil); code != http.StatusNotFound {
		t.Errorf("DELETE(deleted) = %d, want 404", code)
	}
	// Recreate is LEGAL (hard delete, no tombstone).
	if code, doc = scimCall(t, http.MethodPost, base, map[string]any{"displayName": "Demolishable"}); code != http.StatusCreated {
		t.Errorf("recreate = %d, want 201", code)
	}
	rid := doc["id"].(string)
	if code, _ = scimCall(t, http.MethodDelete, base+"/"+rid, nil); code != http.StatusNoContent {
		t.Errorf("cleanup delete = %d, want 204", code)
	}
}

// =====================================================================
// Audit trail
// =====================================================================

func TestSCIMGroups_AuditTrail(t *testing.T) {
	ts, _, _, aud, _ := scimGroupsFixture(t, config.ScimConfig{Enabled: true})
	base := groupsBase(ts.URL)

	code, doc := scimCall(t, http.MethodPost, base, map[string]any{
		"displayName": "Audited", "members": memberObjs("a1"),
	})
	if code != http.StatusCreated {
		t.Fatalf("POST = %d, want 201", code)
	}
	id := doc["id"].(string)

	// A NO-MOVE contact: adding an EXISTING value (the store dedupes ⇒
	// membership UNCHANGED ⇒ lean record).
	if code, _ := scimCall(t, http.MethodPatch, base+"/"+id, map[string]any{
		"Operations": []map[string]any{{"op": "add", "path": "members", "value": "a1"}},
	}); code != http.StatusOK {
		t.Fatalf("no-move PATCH = %d, want 200", code)
	}
	// A MOVING contact (add a new member).
	if code, _ := scimCall(t, http.MethodPatch, base+"/"+id, map[string]any{
		"Operations": []map[string]any{{"op": "add", "path": "members", "value": "a2"}},
	}); code != http.StatusOK {
		t.Fatalf("moving PATCH = %d, want 200", code)
	}
	// A delete.
	if code, _ := scimCall(t, http.MethodDelete, base+"/"+id, nil); code != http.StatusNoContent {
		t.Fatalf("DELETE = %d, want 204", code)
	}

	evs := aud.events
	if len(evs) != 4 {
		t.Fatalf("audit events = %d, want 4 (create, no-move, moving, delete)", len(evs))
	}
	actions := []string{}
	for _, ev := range evs {
		actions = append(actions, ev.Action)
		if ev.TargetKind != audit.TargetSCIMGroup {
			t.Errorf("target kind = %q, want %q", ev.TargetKind, audit.TargetSCIMGroup)
		}
		if ev.Actor != "SCIM connector (#test)" {
			t.Errorf("actor = %q, want the presenting connector", ev.Actor)
		}
	}
	wantSeq := []string{"scim.group.create", "scim.group.members", "scim.group.members", "scim.group.delete"}
	if !reflect.DeepEqual(actions, wantSeq) {
		t.Errorf("actions = %v, want %v", actions, wantSeq)
	}
	// Lean no-move contact: changed:false, NO blobs.
	noMove := evs[1]
	if dm, ok := noMove.Details.(map[string]any); ok {
		if chg, _ := dm["changed"].(bool); chg {
			t.Errorf("no-move contact marked changed: %v", dm)
		}
	}
	if noMove.OldValue != nil || noMove.NewValue != nil {
		t.Errorf("no-move contact carries blobs: old=%v new=%v", noMove.OldValue, noMove.NewValue)
	}
	// Moving contact: changed:true + blobs.
	mov := evs[2]
	if dm, ok := mov.Details.(map[string]any); ok {
		if chg, _ := dm["changed"].(bool); !chg {
			t.Errorf("moving contact not marked changed: %v", dm)
		}
	}
	if mov.OldValue == nil || mov.NewValue == nil {
		t.Errorf("moving contact missing blobs: old=%v new=%v", mov.OldValue, mov.NewValue)
	}
}

// =====================================================================
// Metrics
// =====================================================================

func TestSCIMGroups_OperationMetrics(t *testing.T) {
	ts, _, _, _, reg := scimGroupsFixture(t, config.ScimConfig{Enabled: true})
	base := groupsBase(ts.URL)

	before := scimCounter(t, reg, "cdrom_scim_operations_total")
	code, doc := scimCall(t, http.MethodPost, base, map[string]any{"displayName": "Metered"})
	if code != http.StatusCreated {
		t.Fatalf("POST = %d, want 201", code)
	}
	id := doc["id"].(string)
	if pc, _ := scimCall(t, http.MethodPatch, base+"/"+id, map[string]any{
		"Operations": []map[string]any{{"op": "add", "path": "members", "value": "m"}},
	}); pc != http.StatusOK {
		t.Fatalf("PATCH = %d, want 200", pc)
	}
	after := scimCounter(t, reg, "cdrom_scim_operations_total")
	if after-before != 2 {
		t.Errorf("metered ops = %.0f, want 2 (create + patch)", after-before)
	}
}
