package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fakeWebhookOIDC is a webhookTokenVerifier that returns canned claims for a
// token, so the webhook handler's claim-matching path can be exercised without
// a live identity provider. When verifyErr is set, verifyToken returns it. The
// claims are the token's full claims as a JSON object (a claim that is itself
// an object or a list keeps its structure).
type fakeWebhookOIDC struct {
	claims    map[string]any
	verifyErr error
}

func (f *fakeWebhookOIDC) verifyToken(ctx context.Context, issuer, token string) (map[string]any, error) {
	if f.verifyErr != nil {
		return nil, f.verifyErr
	}
	return f.claims, nil
}

// webhookOIDCPipeline returns a canned pipeline with a single webhook trigger
// that authenticates by OIDC (the given issuer and required claims) for the
// API's webhook handler tests (F-09).
func webhookOIDCPipeline(id int64, issuer string, claims map[string]string) *dbpb.Pipeline {
	return &dbpb.Pipeline{
		Id:   id,
		Name: "deploy",
		Triggers: []*dbpb.Trigger{
			{Name: "hook", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK,
				OidcIssuer: issuer, OidcClaims: claims,
				Params: map[string]string{"env": "staging"}},
		},
	}
}

// doWebhookToken posts to the pipeline's webhook endpoint with the given
// Bearer token and JSON body.
func doWebhookToken(t *testing.T, ts *httptest.Server, pipelineID int64, token, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+fmt.Sprintf("/api/pipelines/%d/webhook", pipelineID), bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

// newWebhookTestServer builds an API server over the given fakes with the
// webhook OIDC verifier replaced by the given fake, and serves it.
func newWebhookTestServer(t *testing.T, fakeDB *fakeDatabase, fakeSched *fakeScheduler, verifier webhookTokenVerifier) *httptest.Server {
	t.Helper()
	srv := New(Clients{Database: fakeDB, Scheduler: fakeSched}, nil)
	srv.webhookOIDC = verifier
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// TestWebhookOIDCStartsRun verifies that a webhook POST with an OIDC token
// whose claims satisfy the trigger's oidc_claims starts a run (F-09) and that
// the token's flattened claims are recorded on the run as upstream claims.
func TestWebhookOIDCStartsRun(t *testing.T) {
	fakeDB := &fakeDatabase{pipeline: webhookOIDCPipeline(3, "https://idp.example", map[string]string{"org": "acme"})}
	fakeSched := &fakeScheduler{}
	verifier := &fakeWebhookOIDC{claims: map[string]any{"org": "acme", "user": "alice"}}
	ts := newWebhookTestServer(t, fakeDB, fakeSched, verifier)

	resp := doWebhookToken(t, ts, 3, "tok", `{"commit": "abc"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if fakeSched.triggered == nil {
		t.Fatal("scheduler.TriggerRun was not called")
	}
	if fakeSched.triggered.GetTriggerName() != "hook" {
		t.Errorf("trigger = %q, want hook", fakeSched.triggered.GetTriggerName())
	}
	// The token's claims are recorded as upstream claims (as a JSON object).
	upstream := fakeSched.triggered.GetUpstreamClaims().AsMap()
	if got := upstream["org"]; got != "acme" {
		t.Errorf("upstream_claims[org] = %v, want acme", got)
	}
	if got := upstream["user"]; got != "alice" {
		t.Errorf("upstream_claims[user] = %v, want alice", got)
	}
	// The static param and the body param are both present.
	if got := fakeSched.triggered.GetParams()["env"]; got != "staging" {
		t.Errorf("params[env] = %q, want staging", got)
	}
	if got := fakeSched.triggered.GetParams()["commit"]; got != "abc" {
		t.Errorf("params[commit] = %q, want abc", got)
	}
}

// TestWebhookOIDCPreservesComplexClaims verifies that a webhook token whose
// claims include complex values (an object and a list, e.g. GitLab's
// "user_identities" and "job_config") records them on the run with their
// structure intact (F-09), and that a nested claim is matched dot-addressed.
func TestWebhookOIDCPreservesComplexClaims(t *testing.T) {
	// The trigger requires a nested claim (project_path) and a wildcard on a
	// complex claim (user_identities).
	fakeDB := &fakeDatabase{pipeline: webhookOIDCPipeline(3, "https://gitlab.example", map[string]string{
		"project_path":    "my-group/my-project",
		"user_identities": "*",
	})}
	fakeSched := &fakeScheduler{}
	// The token carries a scalar, a list of objects, and a nested object.
	verifier := &fakeWebhookOIDC{claims: map[string]any{
		"project_path": "my-group/my-project",
		"user_identities": []any{
			map[string]any{"provider": "github", "extern_uid": "2435223452345"},
			map[string]any{"provider": "bitbucket", "extern_uid": "john.smith"},
		},
		"job_config": map[string]any{
			"url": "https://gitlab.example.com/my-group/my-project/-/blob/abc/policy.yml",
			"sha": "abc",
		},
	}}
	ts := newWebhookTestServer(t, fakeDB, fakeSched, verifier)

	resp := doWebhookToken(t, ts, 3, "tok", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if fakeSched.triggered == nil {
		t.Fatal("scheduler.TriggerRun was not called")
	}
	upstream := fakeSched.triggered.GetUpstreamClaims().AsMap()
	// A scalar claim keeps its string value.
	if got := upstream["project_path"]; got != "my-group/my-project" {
		t.Errorf("upstream_claims[project_path] = %v, want my-group/my-project", got)
	}
	// A list-of-objects claim keeps its structure (a slice of maps).
	identities, ok := upstream["user_identities"].([]any)
	if !ok || len(identities) != 2 {
		t.Fatalf("upstream_claims[user_identities] = %v (%T), want a 2-element list", upstream["user_identities"], upstream["user_identities"])
	}
	if first, ok := identities[0].(map[string]any); !ok || first["provider"] != "github" {
		t.Errorf("upstream_claims[user_identities][0] = %v, want provider github", identities[0])
	}
	// A nested-object claim keeps its structure (a map).
	if cfg, ok := upstream["job_config"].(map[string]any); !ok || cfg["sha"] != "abc" {
		t.Errorf("upstream_claims[job_config] = %v, want an object with sha=abc", upstream["job_config"])
	}
}

// TestWebhookOIDCRejectsMismatchedClaims verifies that a webhook POST whose
// token's claims do not satisfy the trigger's oidc_claims is rejected with
// 401 (F-09).
func TestWebhookOIDCRejectsMismatchedClaims(t *testing.T) {
	fakeDB := &fakeDatabase{pipeline: webhookOIDCPipeline(3, "https://idp.example", map[string]string{"org": "acme"})}
	fakeSched := &fakeScheduler{}
	// The token's org claim does not match the trigger's required org.
	verifier := &fakeWebhookOIDC{claims: map[string]any{"org": "other"}}
	ts := newWebhookTestServer(t, fakeDB, fakeSched, verifier)

	resp := doWebhookToken(t, ts, 3, "tok", `{"commit": "abc"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if fakeSched.triggered != nil {
		t.Error("scheduler.TriggerRun was called for mismatched claims, want it rejected")
	}
}

// TestWebhookOIDCWildcard verifies that a trigger's oidc_claims value of "*"
// is a wildcard that matches any value for that claim (including the claim
// being absent) (F-09).
func TestWebhookOIDCWildcard(t *testing.T) {
	// The trigger requires org to be present (any value) and user to be
	// exactly "alice".
	fakeDB := &fakeDatabase{pipeline: webhookOIDCPipeline(3, "https://idp.example", map[string]string{"org": "*", "user": "alice"})}
	fakeSched := &fakeScheduler{}
	// org is present with an arbitrary value; user matches.
	verifier := &fakeWebhookOIDC{claims: map[string]any{"org": "acme", "user": "alice"}}
	ts := newWebhookTestServer(t, fakeDB, fakeSched, verifier)

	resp := doWebhookToken(t, ts, 3, "tok", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if fakeSched.triggered == nil {
		t.Fatal("scheduler.TriggerRun was not called")
	}

	// A token whose user claim does not match is rejected even though org is
	// a wildcard.
	fakeSched2 := &fakeScheduler{}
	verifier2 := &fakeWebhookOIDC{claims: map[string]any{"org": "acme", "user": "bob"}}
	ts2 := newWebhookTestServer(t, fakeDB, fakeSched2, verifier2)
	resp2 := doWebhookToken(t, ts2, 3, "tok", "")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp2.StatusCode)
	}
}

// TestWebhookOIDCRequiresToken verifies that a webhook trigger with an
// oidc_issuer set rejects a request that presents no Bearer token (F-09).
func TestWebhookOIDCRequiresToken(t *testing.T) {
	fakeDB := &fakeDatabase{pipeline: webhookOIDCPipeline(3, "https://idp.example", map[string]string{"org": "acme"})}
	fakeSched := &fakeScheduler{}
	verifier := &fakeWebhookOIDC{claims: map[string]any{"org": "acme"}}
	ts := newWebhookTestServer(t, fakeDB, fakeSched, verifier)

	// No Authorization header.
	resp := doWebhookToken(t, ts, 3, "", `{"commit": "abc"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if fakeSched.triggered != nil {
		t.Error("scheduler.TriggerRun was called without a token, want it rejected")
	}
}

// TestWebhookOIDCAndSecret verifies that a webhook trigger that sets both an
// oidc_issuer and a secret requires both to be satisfied (F-09): a matching
// token with the wrong secret is rejected, and both matching starts a run.
func TestWebhookOIDCAndSecret(t *testing.T) {
	pipeline := &dbpb.Pipeline{
		Id:   3,
		Name: "deploy",
		Triggers: []*dbpb.Trigger{
			{Name: "hook", Type: dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK,
				Secret: "s3cret", OidcIssuer: "https://idp.example",
				OidcClaims: map[string]string{"org": "acme"},
				Params:     map[string]string{"env": "staging"}},
		},
	}
	verifier := &fakeWebhookOIDC{claims: map[string]any{"org": "acme"}}

	// Matching token but wrong secret → 401.
	fakeSched := &fakeScheduler{}
	ts := newWebhookTestServer(t, &fakeDatabase{pipeline: pipeline}, fakeSched, verifier)
	resp := doWebhookTokenSecret(t, ts, 3, "tok", "wrong", `{"commit": "abc"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if fakeSched.triggered != nil {
		t.Error("scheduler.TriggerRun was called with a wrong secret, want it rejected")
	}

	// Matching token and secret → 201.
	fakeSched2 := &fakeScheduler{}
	ts2 := newWebhookTestServer(t, &fakeDatabase{pipeline: pipeline}, fakeSched2, verifier)
	resp2 := doWebhookTokenSecret(t, ts2, 3, "tok", "s3cret", `{"commit": "abc"}`)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp2.StatusCode)
	}
	if fakeSched2.triggered == nil {
		t.Fatal("scheduler.TriggerRun was not called")
	}
}

// doWebhookTokenSecret posts to the pipeline's webhook endpoint with the given
// Bearer token, secret header, and JSON body.
func doWebhookTokenSecret(t *testing.T, ts *httptest.Server, pipelineID int64, token, secret, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+fmt.Sprintf("/api/pipelines/%d/webhook", pipelineID), bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if secret != "" {
		req.Header.Set(webhookSecretHeader, secret)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

// TestFlattenClaims verifies that nested OIDC claims are flattened to
// dot-addressed keys (F-09), so a trigger can address a claim a provider
// nests (e.g. GitLab's "resource_access").
func TestFlattenClaims(t *testing.T) {
	claims := map[string]any{
		"sub": "12345",
		"org": map[string]any{
			"name":  "acme",
			"roles": []any{"admin", "dev"},
		},
		"count": 42.0,
		"flag":  true,
	}
	flat := flattenClaims(claims)
	if got := flat["sub"]; got != "12345" {
		t.Errorf("sub = %q, want 12345", got)
	}
	if got := flat["org.name"]; got != "acme" {
		t.Errorf("org.name = %q, want acme", got)
	}
	if got := flat["count"]; got != "42" {
		t.Errorf("count = %q, want 42", got)
	}
	if got := flat["flag"]; got != "true" {
		t.Errorf("flag = %q, want true", got)
	}
	// A list-valued claim is rendered as its JSON encoding.
	if flat["org.roles"] == "" {
		t.Error("org.roles should be rendered as a JSON string")
	}
}

// TestOidcClaimsMatch verifies the wildcard and exact-match semantics of
// claim matching (F-09).
func TestOidcClaimsMatch(t *testing.T) {
	claims := map[string]string{"org": "acme", "user": "alice"}
	cases := []struct {
		name     string
		required map[string]string
		want     bool
	}{
		{"exact match", map[string]string{"org": "acme"}, true},
		{"exact mismatch", map[string]string{"org": "other"}, false},
		{"wildcard present", map[string]string{"org": "*"}, true},
		{"wildcard absent", map[string]string{"missing": "*"}, true},
		{"missing required", map[string]string{"missing": "x"}, false},
		{"multiple", map[string]string{"org": "acme", "user": "alice"}, true},
		{"multiple one wrong", map[string]string{"org": "acme", "user": "bob"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := oidcClaimsMatch(claims, tc.required); got != tc.want {
				t.Errorf("oidcClaimsMatch = %v, want %v", got, tc.want)
			}
		})
	}
}
