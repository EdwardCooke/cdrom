package idp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// newTestCredentialStack builds a KeyManager + ops layer over the in-memory
// store with the config's default TTL.
func newTestCredentialStack(t *testing.T) (*KeyManager, *AgentCredentialOps, *MemoryAgentCredentialStore) {
	t.Helper()
	cfg := testConfig(time.Hour, 30*time.Minute)
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	store := NewMemoryAgentCredentialStore()
	ops := NewAgentCredentialOps(cfg, km, store, testLogger())
	return km, ops, store
}

// decodeJWTParts splits a compact JWT into its header, claims, and signature.
func decodeJWTParts(t *testing.T, tok string) (header, claims map[string]any, sig []byte) {
	t.Helper()
	parts := bytes.Split([]byte(tok), []byte("."))
	if len(parts) != 3 {
		t.Fatalf("compact JWT has %d segments, want 3", len(parts))
	}
	hraw, _ := base64.RawURLEncoding.DecodeString(string(parts[0]))
	craw, _ := base64.RawURLEncoding.DecodeString(string(parts[1]))
	sig, _ = base64.RawURLEncoding.DecodeString(string(parts[2]))
	if err := json.Unmarshal(hraw, &header); err != nil {
		t.Fatalf("unmarshal header: %v", err)
	}
	if err := json.Unmarshal(craw, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	return header, claims, sig
}

// verifyRS256AgainstJWKS cryptographically verifies the JWT's RS256 signature
// against the IdP's JWKS PUBLIC KEY selected by the header's kid. This is the
// "the signature proves the IdP issued it" guarantee — done with stdlib RSA
// (PKCS1v15 + SHA-256 over the ASCII signing input) so the test depends on
// nothing beyond the public key the IdP advertises.
func verifyRS256AgainstJWKS(t *testing.T, km *KeyManager, tok string) {
	t.Helper()
	keys, err := km.JWKS()
	if err != nil {
		t.Fatalf("JWKS: %v", err)
	}
	header, _, sig := decodeJWTParts(t, tok)
	kid, _ := header["kid"].(string)
	var pub *rsa.PublicKey
	for _, k := range keys {
		if k.KeyID == kid {
			rsaPub, ok := k.Key.(*rsa.PublicKey)
			if !ok {
				t.Fatalf("JWKS key %q is %T, want *rsa.PublicKey", kid, k.Key)
			}
			pub = rsaPub
			break
		}
	}
	if pub == nil {
		t.Fatalf("JWKS has no key with kid %q", kid)
	}
	segments := bytes.Split([]byte(tok), []byte("."))
	signingInput := append(segments[0], '.')
	signingInput = append(signingInput, segments[1]...)
	hash := sha256.Sum256(signingInput)
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, hash[:], sig); err != nil {
		t.Fatalf("RS256 signature did not verify against the JWKS public key: %v", err)
	}
}

// TestAgentCredentialIssueClaimsAndPlaintextOnce covers the F-30.2 Slice 1
// headline: the issued JWT carries the specced claims (sub agent:<name>,
// jti == row id, aud, token_type), its signature VERIFIES against the JWKS
// public key, and the plaintext appears in the issue response and nowhere
// else (the store row holds no JWT material).
func TestAgentCredentialIssueClaimsAndPlaintextOnce(t *testing.T) {
	km, ops, store := newTestCredentialStack(t)
	ctx := context.Background()

	row, jwt, err := ops.Issue(ctx, "edge-box-1", "alice", "deploy host")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	verifyRS256AgainstJWKS(t, km, jwt)

	_, claims, _ := decodeJWTParts(t, jwt)
	if sub, _ := claims["sub"].(string); sub != "agent:edge-box-1" {
		t.Errorf("sub = %v, want agent:edge-box-1", sub)
	}
	if jti, _ := claims["jti"].(string); jti != row.ID {
		t.Errorf("jti = %v, want the row id %q", jti, row.ID)
	}
	if tt, _ := claims["token_type"].(string); tt != "agent_credential" {
		t.Errorf("token_type = %v, want agent_credential", tt)
	}
	aud, _ := claims["aud"].([]any)
	if len(aud) != 1 || aud[0] != "cdrom-api" {
		t.Errorf("aud = %#v, want [cdrom-api]", claims["aud"])
	}
	// The exp placeholder is generous (>= 7 days out) — the ROW's deadline is
	// the real one (~30 days from now).
	expFloat, _ := claims["exp"].(float64)
	if int64(expFloat) < time.Now().Add(7*24*time.Hour).Unix() {
		t.Errorf("exp = %v, want the generous placeholder (>= 7d out)", expFloat)
	}
	delta := row.ExpiresAt.Sub(time.Now())
	if delta < 29*24*time.Hour || delta > 31*24*time.Hour {
		t.Errorf("row deadline offset = %v, want ~now+30d (the real deadline)", delta)
	}
	// Metadata-only reads: Get and List return the row with NO jwt field.
	meta, err := ops.Get(ctx, row.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if meta.ID != row.ID || meta.AgentName != "edge-box-1" {
		t.Errorf("Get echo = (%q,%q), want (%q,edge-box-1)", meta.ID, meta.AgentName, row.ID)
	}
	all, err := store.List(ctx, "")
	if err != nil || len(all) != 1 {
		t.Fatalf("store.List = %d rows (err %v), want 1", len(all), err)
	}
}

// TestAgentCredentialValidityTransitions walks the full lifecycle: issue →
// renew (extends; the SAME jwt stays operative) → rotate (successor signed +
// stored, predecessor retired and chained) → revoke (stops governing
// liveness immediately).
func TestAgentCredentialValidityTransitions(t *testing.T) {
	_, ops, _ := newTestCredentialStack(t)
	ctx := context.Background()

	first, jwt1, err := ops.Issue(ctx, "box-2", "bob", "")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	// Renew: deadline moves forward; the SAME jwt1 remains the operative token
	// (no re-signing — structural no-break).
	renewed, err := ops.Renew(ctx, first.ID)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if !renewed.ExpiresAt.After(first.ExpiresAt) {
		t.Errorf("renewed deadline %v not after original %v", renewed.ExpiresAt, first.ExpiresAt)
	}
	if !renewed.Active(time.Now().UTC()) {
		t.Error("renewed row reports inactive, want active")
	}
	_ = jwt1

	// Rotate: successor signed + stored; predecessor retired and chained.
	succ, jwt2, err := ops.Rotate(ctx, first.ID, "bob", "replacement")
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if succ.ID == first.ID {
		t.Fatal("successor id equals the retired id")
	}
	if succ.AgentName != "box-2" {
		t.Errorf("successor agent = %q, want box-2", succ.AgentName)
	}
	retired, err := ops.Get(ctx, first.ID)
	if err != nil {
		t.Fatalf("Get(retired): %v", err)
	}
	if retired.ReplacedByID != succ.ID {
		t.Errorf("retired.replaced_by_id = %q, want successor %q", retired.ReplacedByID, succ.ID)
	}
	if retired.Active(time.Now().UTC()) {
		t.Error("retired row reports Active, want false")
	}
	if !succ.Active(time.Now().UTC()) {
		t.Error("successor row reports inactive, want active")
	}
	// The retired row cannot be renewed or rotated further.
	if _, err := ops.Renew(ctx, first.ID); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("renew retired err = %v, want FailedPrecondition", err)
	}
	if _, _, err := ops.Rotate(ctx, first.ID, "bob", ""); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("rotate retired err = %v, want FailedPrecondition", err)
	}
	// The successor's JWT is a FRESH token (differs from the first).
	if jwt2 == jwt1 {
		t.Error("rotation returned the SAME jwt — successor must be a fresh token")
	}

	// Revoke: the successor stops governing liveness immediately.
	revoked, err := ops.Revoke(ctx, succ.ID, "lost laptop")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if revoked.RevokedAt == nil || revoked.RevokedReason != "lost laptop" {
		t.Errorf("revoked row = (stamp %v, reason %q), want stamped + reasoned", revoked.RevokedAt, revoked.RevokedReason)
	}
	if revoked.Active(time.Now().UTC()) {
		t.Error("revoked row reports Active, want false")
	}
}

// TestAgentCredentialGRPCUnimplemented confirms the gRPC surface answers
// Unimplemented when the ops layer is not attached (the F-25 convention).
func TestAgentCredentialGRPCUnimplemented(t *testing.T) {
	cfg := testConfig(time.Hour, 30*time.Minute)
	g := NewGRPCServer(cfg, nil, NewMemoryUserStore(), testLogger())
	ctx := context.Background()
	if _, err := g.IssueAgentCredential(ctx, &idppb.IssueAgentCredentialRequest{AgentName: "x"}); status.Code(err) != codes.Unimplemented {
		t.Errorf("Issue (unwired) err = %v, want Unimplemented", err)
	}
	if _, err := g.RenewAgentCredential(ctx, &idppb.RenewAgentCredentialRequest{Id: "x"}); status.Code(err) != codes.Unimplemented {
		t.Errorf("Renew (unwired) err = %v, want Unimplemented", err)
	}
	if _, err := g.RevokeAgentCredential(ctx, &idppb.RevokeAgentCredentialRequest{Id: "x"}); status.Code(err) != codes.Unimplemented {
		t.Errorf("Revoke (unwired) err = %v, want Unimplemented", err)
	}
}
