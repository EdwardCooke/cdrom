// F-28.4 Slice 3 — the admission gate's ENFORCEMENT SIDE (middleware
// integration).
//
// Rig: an auth.Auth with OIDC DISABLED (auth.New returns the bare &Auth{} —
// the API-key branch is independent of OIDC) + a scripted APIKeyVerifier
// (stands in for the IdP's key directory) + a scripted AdmissionPolicy (stands
// in for the IdP's AdmitPrincipal). Asserts the MIDDLEWARE'S obligations:
// human principals are gated (allowed ⇒ proceed; denied ⇒ 403 with the
// distinct text; infrastructure failure ⇒ 503 fail-closed), service accounts
// are EXEMPT, and the exemption set bypasses everything.
package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/auth"
	"cdrom/internal/config"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// Verdict shorthands (keep the case table legible).
const (
	verdictAdmitted      = idppb.AdmissionVerdict_ADMITTED
	verdictDeactivated   = idppb.AdmissionVerdict_DEACTIVATED
	verdictDeprovisioned = idppb.AdmissionVerdict_DEPROVISIONED
	verdictUnknown       = idppb.AdmissionVerdict_UNKNOWN_REJECTED
	verdictNoRoles       = idppb.AdmissionVerdict_NO_ROLES_REJECTED
)

// constantAdmitter is a scripted admissionClient answering every call with
// one fixed verdict.
type constantAdmitter idppb.AdmissionVerdict

func (c constantAdmitter) AdmitPrincipal(_ context.Context, _ *idppb.AdmitPrincipalRequest, _ ...grpc.CallOption) (*idppb.AdmitPrincipalResponse, error) {
	return &idppb.AdmitPrincipalResponse{Verdict: idppb.AdmissionVerdict(c)}, nil
}

// erroringAdmitter stands in for an UNREACHABLE IdP (the fail-closed leg).
type erroringAdmitter struct{}

func (erroringAdmitter) AdmitPrincipal(_ context.Context, _ *idppb.AdmitPrincipalRequest, _ ...grpc.CallOption) (*idppb.AdmitPrincipalResponse, error) {
	return nil, status.Error(codes.Unavailable, "conn failed")
}

// scriptedKeyVerifier plays the IdP's API-key directory for the test.
type scriptedKeyVerifier struct {
	user auth.User
}

func (v *scriptedKeyVerifier) Verify(_ context.Context, _, _ string) (auth.User, error) {
	return v.user, nil
}

// scriptedSAVerifier plays the service-account directory for the test.
type scriptedSAVerifier struct {
	user auth.User
}

func (v *scriptedSAVerifier) Verify(_ context.Context, _, _ string) (auth.User, error) {
	return v.user, nil
}

// gateHarness mounts the auth middleware over a sentinel handler that
// REPORTS the principal it saw (proof the request proceeded) and returns the
// response.
func gateHarness(t *testing.T, saEnabled bool, policy auth.AdmissionPolicy, bearer string) (int, string, bool) {
	t.Helper()
	a, err := auth.New(context.Background(), config.AuthConfig{}) // OIDC disabled
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	a.SetAPIKeyVerifier(&scriptedKeyVerifier{user: auth.User{Username: "eve.key", Roles: []string{"user"}}}, true)
	if saEnabled {
		a.SetServiceAccountVerifier(&scriptedSAVerifier{user: auth.User{Kind: "service-account", Subject: "sa:builder"}}, true)
	}
	if policy != nil {
		a.SetAdmissionPolicy(policy)
	}
	proceeded := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proceeded = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	h := a.MiddlewareExempt(inner)
	req := httptest.NewRequest(http.MethodGet, "/api/things", nil)
	req.Header.Set("Authorization", "Bearer "+bearer)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, string(body), proceeded
}

// TestAdmission_GatesHumanAPIKeyPrincipals asserts the middleware's
// obligations for a HUMAN principal (here via the API-key channel — the
// OIDC channel converges on the same passAdmission hook): allowed ⇒ proceed;
// denied ⇒ 403 with the DISTINCT text; infrastructure failure ⇒ 503
// (fail-closed, never a disguised 403, never a passthrough).
func TestAdmission_GatesHumanAPIKeyPrincipals(t *testing.T) {
	const bearer = "eve.key:cdrom-k"
	// ALLOWED: the request proceeds to the inner handler.
	code, _, ok := gateHarness(t, false, func(_ context.Context, _ auth.User) auth.AdmissionDecision {
		return auth.AdmissionDecision{Allowed: true}
	}, bearer)
	if code != http.StatusOK || !ok {
		t.Errorf("allowed: code=%d proceeded=%v, want 200/proceeded", code, ok)
	}
	// DENIED: 403 carrying the distinct reason text.
	code, body, ok := gateHarness(t, false, func(_ context.Context, _ auth.User) auth.AdmissionDecision {
		return auth.AdmissionDecision{Reason: "your account is deactivated; contact an administrator"}
	}, bearer)
	if code != http.StatusForbidden || ok {
		t.Errorf("denied: code=%d proceeded=%v, want 403/not-proceeded", code, ok)
	}
	if !strings.Contains(body, "deactivated") {
		t.Errorf("denied body = %q, want the distinct reason", body)
	}
	// INFRASTRUCTURE FAILURE: 503 fail-closed.
	code, body, ok = gateHarness(t, false, func(_ context.Context, _ auth.User) auth.AdmissionDecision {
		return auth.AdmissionDecision{InfrastructureFailure: true}
	}, bearer)
	if code != http.StatusServiceUnavailable || ok {
		t.Errorf("infra: code=%d proceeded=%v, want 503/not-proceeded", code, ok)
	}
	if strings.Contains(body, "deactivated") || !strings.Contains(body, "unavailable") {
		t.Errorf("infra body = %q, want the 503 text (not a denial)", body)
	}
	// NO POLICY ATTACHED: today's behavior (ungated passthrough).
	code, _, ok = gateHarness(t, false, nil, bearer)
	if code != http.StatusOK || !ok {
		t.Errorf("no-policy: code=%d proceeded=%v, want 200/proceeded", code, ok)
	}
}

// TestAdmission_ServiceAccountsExempt asserts MACHINE principals bypass the
// gate entirely (the policy is never consulted).
func TestAdmission_ServiceAccountsExempt(t *testing.T) {
	consulted := false
	policy := func(_ context.Context, _ auth.User) auth.AdmissionDecision {
		consulted = true
		return auth.AdmissionDecision{Allowed: false}
	}
	code, _, ok := gateHarness(t, true, policy, "sa-builder:cdrom-sa-k")
	if code != http.StatusOK || !ok {
		t.Errorf("service account: code=%d proceeded=%v, want 200/proceeded (exempt)", code, ok)
	}
	if consulted {
		t.Error("the gate WAS consulted for a service account — machines must be exempt")
	}
}

// TestAdmission_NewAdmissionPolicyMapsVerdicts asserts the ADAPTER's
// translations: each IdP verdict ⇒ its decision shape (allowed / distinct
// 403 text / 503 on RPC failure), and the empty-subject short-circuit.
func TestAdmission_NewAdmissionPolicyMapsVerdicts(t *testing.T) {
	cases := []struct {
		verdict idppb.AdmissionVerdict
		want    auth.AdmissionDecision
	}{
		{verdictAdmitted, auth.AdmissionDecision{Allowed: true}},
		{verdictDeactivated, auth.AdmissionDecision{Reason: "your account is deactivated; contact an administrator"}},
		{verdictDeprovisioned, auth.AdmissionDecision{Reason: "your account has been deprovisioned"}},
		{verdictUnknown, auth.AdmissionDecision{Reason: "your account is not recognized; contact an administrator"}},
		{verdictNoRoles, auth.AdmissionDecision{Reason: "no roles are mapped for your account; contact an administrator"}},
	}
	for _, tc := range cases {
		policy := NewAdmissionPolicy(constantAdmitter(tc.verdict), nil)
		got := policy(context.Background(), auth.User{Username: "someone"})
		if got != tc.want {
			t.Errorf("verdict %v: got %+v, want %+v", tc.verdict, got, tc.want)
		}
	}
	// RPC FAILURE ⇒ fail closed (infrastructure, not a denial).
	failPolicy := NewAdmissionPolicy(erroringAdmitter{}, nil)
	if got := failPolicy(context.Background(), auth.User{Username: "someone"}); !got.InfrastructureFailure || got.Allowed {
		t.Errorf("rpc failure: got %+v, want InfrastructureFailure", got)
	}
	// EMPTY SUBJECT ⇒ a distinct denial (not a 503 — the gate is healthy).
	emptyPolicy := NewAdmissionPolicy(constantAdmitter(verdictAdmitted), nil)
	if got := emptyPolicy(context.Background(), auth.User{}); got.Allowed || !strings.Contains(got.Reason, "subject") {
		t.Errorf("empty subject: got %+v, want a denial mentioning the subject", got)
	}
}
