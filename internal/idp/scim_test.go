package idp

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cdrom/internal/config"
)

// scimTestServer builds an IdP HTTP server with the SCIM surface enabled
// (optionally pinning the public base used in announcements) around a
// throwaway key manager.
func scimTestServer(t *testing.T, publicBase string) *httptest.Server {
	t.Helper()
	return scimTestServerCustom(t, config.ScimConfig{Enabled: true, Credential: "svc-secret", PublicBase: publicBase})
}

// scimTestServerCustom builds an IdP HTTP server with the given SCIM config
// (which must have Enabled set) around a throwaway key manager.
func scimTestServerCustom(t *testing.T, scimCfg config.ScimConfig) *httptest.Server {
	t.Helper()
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Scim = scimCfg
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	return httptest.NewServer(srv.Handler())
}

func scimDisabledTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	cfg := testConfig(time.Hour, 30*time.Minute)
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	srv := NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	return httptest.NewServer(srv.Handler())
}

// doGetSCIM issues a GET expecting scim+json and returns the decoded body.
func doGetSCIM(t *testing.T, ts *httptest.Server, path string) (int, map[string]any) {
	t.Helper()
	res, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	ct := res.Header.Get("Content-Type")
	if ct != "application/scim+json" {
		t.Fatalf("%s: Content-Type = %q, want %q", path, ct, "application/scim+json")
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("%s: decode body (%s): %v", path, body, err)
	}
	return res.StatusCode, doc
}

func TestSCIM_ServiceProviderConfig(t *testing.T) {
	ts := scimTestServer(t, "")
	defer ts.Close()

	status, doc := doGetSCIM(t, ts, "/scim/v2/ServiceProviderConfig")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	schemas, _ := doc["schemas"].([]any)
	if len(schemas) != 1 || schemas[0] != scimSchemaSvcCfg {
		t.Errorf("schemas = %#v, want [%s]", doc["schemas"], scimSchemaSvcCfg)
	}
	filterSup, _ := doc["filter"].(map[string]any)["supported"].(bool)
	if !filterSup {
		t.Errorf("filter.supported = false, want true")
	}
	bulkSup, _ := doc["bulk"].(map[string]any)["supported"].(bool)
	if bulkSup {
		t.Errorf("bulk.supported = true, want false (until Bulk lands)")
	}
	etagSup, _ := doc["etag"].(map[string]any)["supported"].(bool)
	if etagSup {
		t.Errorf("etag.supported = true, want false (until ETag lands)")
	}
	maxRes, _ := doc["pagination"].(map[string]any)["maxResults"].(float64)
	if int(maxRes) != config.DefaultSCIMMaxPageSize {
		t.Errorf("pagination.maxResults = %v, want %d (default bound)", maxRes, config.DefaultSCIMMaxPageSize)
	}
	filterMax, _ := doc["filter"].(map[string]any)["maxResults"].(float64)
	if int(filterMax) != config.DefaultSCIMMaxPageSize {
		t.Errorf("filter.maxResults = %v, want %d (default bound)", filterMax, config.DefaultSCIMMaxPageSize)
	}
	meta, _ := doc["meta"].(map[string]any)
	location, _ := meta["location"].(string)
	wantLocation := ts.URL + "/scim/v2/ServiceProviderConfig"
	if location != wantLocation {
		t.Errorf("meta.location = %q, want %q", location, wantLocation)
	}
	resourceType, _ := meta["resourceType"].(string)
	if resourceType != "ServiceConfiguration" {
		t.Errorf("meta.resourceType = %q, want ServiceConfiguration", resourceType)
	}
}

func TestSCIM_Schemas_ListAndIndividual(t *testing.T) {
	ts := scimTestServer(t, "")
	defer ts.Close()

	status, doc := doGetSCIM(t, ts, "/scim/v2/Schemas")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	listResp, _ := doc["schemas"].([]any)
	if len(listResp) != 1 || listResp[0] != scimSchemaListResponse {
		t.Errorf("schemas = %#v, want [%s]", doc["schemas"], scimSchemaListResponse)
	}
	total, _ := doc["totalResults"].(float64)
	if int(total) != len(scimCoreSchemas) {
		t.Errorf("totalResults = %v, want %d", total, len(scimCoreSchemas))
	}
	resources, _ := doc["Resources"].([]any)
	foundURNs := map[string]bool{}
	for _, resAny := range resources {
		res, _ := resAny.(map[string]any)
		if u, ok := res["id"].(string); ok {
			foundURNs[u] = true
		}
	}
	for _, want := range []string{scimSchemaUser, scimSchemaGroup, scimSchemaError} {
		if !foundURNs[want] {
			t.Errorf("Schemas listing misses %s (got %#v)", want, foundURNs)
		}
	}

	// Individual retrieval by URN.
	st, userDesc := doGetSCIM(t, ts, "/scim/v2/Schemas/"+strings.ReplaceAll(scimSchemaUser, "/", "%2F"))
	if st != http.StatusOK {
		t.Fatalf("individual schema status = %d, want 200", st)
	}
	if userDesc["id"] != scimSchemaUser {
		t.Errorf("individual schema id = %v, want %s", userDesc["id"], scimSchemaUser)
	}

	// Unknown URN → 404 SCIM error.
	res, err := http.Get(ts.URL + "/scim/v2/Schemas/" + strings.ReplaceAll("urn:bogus:nope", "/", "%2F"))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("unknown schema status = %d, want 404", res.StatusCode)
	}
}

func TestSCIM_ResourceTypes_ListAndIndividual(t *testing.T) {
	ts := scimTestServer(t, "")
	defer ts.Close()

	status, doc := doGetSCIM(t, ts, "/scim/v2/ResourceTypes")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	total, _ := doc["totalResults"].(float64)
	if int(total) != len(scimCoreResourceTypes) {
		t.Errorf("totalResults = %v, want %d", total, len(scimCoreResourceTypes))
	}
	endpoints := map[string]string{}
	for _, resAny := range doc["Resources"].([]any) {
		res, _ := resAny.(map[string]any)
		endpoints[res["name"].(string)] = res["endpoint"].(string)
	}
	if endpoints["User"] != "/Users" || endpoints["Group"] != "/Groups" {
		t.Errorf("endpoints = %#v, want User->/Users, Group->/Groups", endpoints)
	}

	st, userType := doGetSCIM(t, ts, "/scim/v2/ResourceTypes/User")
	if st != http.StatusOK {
		t.Fatalf("individual ResourceType status = %d, want 200", st)
	}
	if userType["schema"] != scimSchemaUser {
		t.Errorf("User.schema = %v, want %s", userType["schema"], scimSchemaUser)
	}

	res, err := http.Get(ts.URL + "/scim/v2/ResourceTypes/Wibble")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("unknown resource type status = %d, want 404", res.StatusCode)
	}
}

func TestSCIM_ErrorBodyShape(t *testing.T) {
	ts := scimTestServer(t, "")
	defer ts.Close()

	res, err := http.Get(ts.URL + "/scim/v2/nothing-here")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("decode error body (%s): %v", body, err)
	}
	schemas, _ := doc["schemas"].([]any)
	if len(schemas) != 1 || schemas[0] != scimSchemaError {
		t.Errorf("error schemas = %#v, want [%s]", doc["schemas"], scimSchemaError)
	}
	if doc["status"] != "404" {
		t.Errorf("error status = %v, want \"404\"", doc["status"])
	}
	if _, ok := doc["detail"].(string); !ok {
		t.Errorf("error detail missing/not a string: %#v", doc)
	}
}

func TestSCIM_MethodNotAllowedAndGetOnly(t *testing.T) {
	ts := scimTestServer(t, "")
	defer ts.Close()

	post, err := http.NewRequest(http.MethodPost, ts.URL+"/scim/v2/ServiceProviderConfig", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(post)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d, want 405", res.StatusCode)
	}
	if allow := res.Header.Get("Allow"); allow != http.MethodGet {
		t.Errorf("Allow = %q, want GET", allow)
	}
}

func TestSCIM_AcceptNegotiation(t *testing.T) {
	ts := scimTestServer(t, "")
	defer ts.Close()

	doReq := func(method, path, accept string) int {
		req, err := http.NewRequest(method, ts.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		return res.StatusCode
	}

	if got := doReq(http.MethodGet, "/scim/v2/Schemas", "text/html"); got != http.StatusNotAcceptable {
		t.Errorf("Accept=text/html status = %d, want 406", got)
	}
	if got := doReq(http.MethodGet, "/scim/v2/Schemas", "*/*"); got != http.StatusOK {
		t.Errorf("Accept=*/* status = %d, want 200", got)
	}
	if got := doReq(http.MethodGet, "/scim/v2/Schemas", "application/json"); got != http.StatusOK {
		t.Errorf("Accept=application/json status = %d, want 200", got)
	}
	if got := doReq(http.MethodGet, "/scim/v2/Schemas", ""); got != http.StatusOK {
		t.Errorf("no Accept status = %d, want 200", got)
	}
}

func TestSCIM_DisabledByDefault(t *testing.T) {
	ts := scimDisabledTestServer(t)
	defer ts.Close()

	for _, p := range []string{
		"/scim/v2/ServiceProviderConfig",
		"/scim/v2/Schemas",
		"/scim/v2/ResourceTypes",
		"/scim/v2/Users",
	} {
		res, err := http.Get(ts.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("%s with SCIM disabled: status = %d, want 404", p, res.StatusCode)
		}
	}
}

func TestSCIM_BareBasePathStock404(t *testing.T) {
	ts := scimTestServer(t, "")
	defer ts.Close()

	// The bare base path (no trailing slash) matches no SCIM registration:
	// the mux answers its stock 404. SCIM clients address the subtree
	// (/scim/v2/...), so this is informational, not a SCIM-dialect error.
	res, err := http.Get(ts.URL + "/scim/v2")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("bare base path status = %d, want 404", res.StatusCode)
	}
}

func TestSCIM_PublicBaseAnnouncement(t *testing.T) {
	// Pinned public base: announcements use it (trailing slash tolerated),
	// IGNORING the request's Host — simulating a proxy topology where the
	// outside-world URL differs from what the IdP receives.
	ts := scimTestServer(t, "https://idp.portal.example/ext-idp/")
	defer ts.Close()

	_, doc := doGetSCIM(t, ts, "/scim/v2/ServiceProviderConfig")
	meta, _ := doc["meta"].(map[string]any)
	wantLoc := "https://idp.portal.example/ext-idp/scim/v2/ServiceProviderConfig"
	if got, _ := meta["location"].(string); got != wantLoc {
		t.Errorf("meta.location = %q, want %q", got, wantLoc)
	}
	ci, _ := doc["changeInfo"].(map[string]any)
	if got, _ := ci["location"].(string); got != wantLoc {
		t.Errorf("changeInfo.location = %q, want %q", got, wantLoc)
	}
}

func TestSCIM_WrongVerbOnKnownResource(t *testing.T) {
	ts := scimTestServer(t, "")
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/scim/v2/Schemas", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /Schemas status = %d, want 405", res.StatusCode)
	}
	if allow := res.Header.Get("Allow"); allow != http.MethodGet {
		t.Errorf("Allow = %q, want GET", allow)
	}
	body, _ := io.ReadAll(res.Body)
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("405 body not SCIM-json (%s): %v", body, err)
	}
	if doc["status"] != "405" {
		t.Errorf("405 error status = %v, want \"405\"", doc["status"])
	}
}

func TestSCIM_UnnestedUnknownResources404(t *testing.T) {
	ts := scimTestServer(t, "")
	defer ts.Close()

	for _, p := range []string{
		"/scim/v2/Nothing",       // unknown resource
		"/scim/v2/Schemas/a/b/c", // over-nested under a known resource
	} {
		res, err := http.Get(ts.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("%s status = %d, want 404", p, res.StatusCode)
		}
	}
}

func TestSCIM_ConfigurableMaxPageSize(t *testing.T) {
	// Honor: a raised bound is advertised AND enforced.
	ts := scimTestServerCustom(t, config.ScimConfig{Enabled: true, Credential: "svc-secret", MaxPageSize: 2})
	defer ts.Close()

	_, doc := doGetSCIM(t, ts, "/scim/v2/ServiceProviderConfig")
	adv, _ := doc["pagination"].(map[string]any)["maxResults"].(float64)
	if int(adv) != 2 {
		t.Errorf("advertisement maxResults = %v, want 2", adv)
	}
	// /Schemas holds 3 descriptors; a bound of 2 must clip the first page.
	st, list := doGetSCIM(t, ts, "/scim/v2/Schemas")
	if st != http.StatusOK {
		t.Fatalf("Schemas status = %d, want 200", st)
	}
	resources, _ := list["Resources"].([]any)
	if len(resources) != 2 {
		t.Errorf("page 1 Resources = %d, want 2 (bound clipped)", len(resources))
	}
	if tot, _ := list["totalResults"].(float64); int(tot) != 3 {
		t.Errorf("totalResults = %v, want 3", tot)
	}
	// Page 2 picks up the remainder, 1-based startIndex echoes back.
	st, page2 := doGetSCIM(t, ts, "/scim/v2/Schemas?startIndex=3")
	if st != http.StatusOK {
		t.Fatalf("Schemas?page2 status = %d, want 200", st)
	}
	resources2, _ := page2["Resources"].([]any)
	if len(resources2) != 1 {
		t.Errorf("page 2 Resources = %d, want 1", len(resources2))
	}
	if si, _ := page2["startIndex"].(float64); int(si) != 3 {
		t.Errorf("page 2 startIndex = %v, want 3", si)
	}

	// Cap: a bound above the hard ceiling is clamped in the advertisement.
	tsBig := scimTestServerCustom(t, config.ScimConfig{Enabled: true, Credential: "svc-secret", MaxPageSize: 1 << 20})
	defer tsBig.Close()
	_, bigDoc := doGetSCIM(t, tsBig, "/scim/v2/ServiceProviderConfig")
	bigAdv, _ := bigDoc["pagination"].(map[string]any)["maxResults"].(float64)
	if int(bigAdv) != config.SCIMHardCapMaxPageSize {
		t.Errorf("clamped advertisement = %v, want %d", bigAdv, config.SCIMHardCapMaxPageSize)
	}
}

func TestSCIM_PaginationBounds(t *testing.T) {
	ts := scimTestServer(t, "")
	defer ts.Close()

	// Out-of-range startIndex collapses to the tail (still 200, empty page).
	status, doc := doGetSCIM(t, ts, "/scim/v2/Schemas?startIndex=999")
	if status != http.StatusOK {
		t.Fatalf("startIndex=999 status = %d, want 200", status)
	}
	items, _ := doc["itemsPerPage"].(float64)
	if int(items) != 0 {
		t.Errorf("itemsPerPage = %v, want 0", items)
	}
	// Invalid startIndex → 400.
	res, err := http.Get(ts.URL + "/scim/v2/Schemas?startIndex=bogus")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("startIndex=bogus status = %d, want 400", res.StatusCode)
	}
}
