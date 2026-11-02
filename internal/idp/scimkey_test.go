package idp

import (
	"context"
	"strings"
	"testing"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/config"

	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// TestScimKeyShape checks the key's shape contract (F-28.2): 32 characters,
// every one from the documented alphabet (upper/lower/digits + the curated
// specials — no quote/backslash/slash/percent/bracket/parenthesis/space).
func TestScimKeyShape(t *testing.T) {
	alphabet := map[rune]bool{}
	for _, r := range scimKeyAlphabet {
		alphabet[r] = true
	}
	for trial := 0; trial < 50; trial++ {
		key, err := NewScimKey()
		if err != nil {
			t.Fatalf("NewScimKey: %v", err)
		}
		if len(key) != scimKeyLength {
			t.Fatalf("trial %d: key length = %d, want %d", trial, len(key), scimKeyLength)
		}
		for _, r := range key {
			if !alphabet[r] {
				t.Fatalf("trial %d: key contains char %q outside the documented alphabet", trial, r)
			}
			if unicode.IsSpace(r) || strings.ContainsRune("\"'\\/()%[]", r) {
				t.Fatalf("trial %d: key contains a forbidden char %q", trial, r)
			}
		}
	}
	// Two draws differ (entropy).
	a, err := NewScimKey()
	if err != nil {
		t.Fatalf("NewScimKey (a): %v", err)
	}
	b, err := NewScimKey()
	if err != nil {
		t.Fatalf("NewScimKey (b): %v", err)
	}
	if a == b {
		t.Fatal("two drawn SCIM keys are identical")
	}
	if got := scimKeyPrefixOf(a); len(got) != scimKeyPrefixLen || got != a[:scimKeyPrefixLen] {
		t.Fatalf("prefix = %q, want the first %d chars %q", got, scimKeyPrefixLen, a[:scimKeyPrefixLen])
	}
}

// TestScimKeyEncodingProperties checks the Argon2ID encoding's guarantees
// (F-28.2): the encoding verifies under its own pepper, REFUSES a different
// pepper (and an empty one when a pepper was used), and two encodings of the
// SAME plaintext differ (fresh embedded salt each time).
func TestScimKeyEncodingProperties(t *testing.T) {
	key, err := NewScimKey()
	if err != nil {
		t.Fatalf("NewScimKey: %v", err)
	}
	enc, err := EncodeScimKey("pepper-α", key)
	if err != nil {
		t.Fatalf("EncodeScimKey: %v", err)
	}
	if !strings.HasPrefix(enc, "$argon2id$") {
		t.Fatalf("encoding %q lacks the argon2id marker", enc)
	}
	if !VerifySecret(enc, "pepper-α", key) {
		t.Error("key does not verify against its own encoding")
	}
	if VerifySecret(enc, "pepper-β", key) {
		t.Error("key verifies under a WRONG pepper")
	}
	if VerifySecret(enc, "", key) {
		t.Error("key verifies under an EMPTY pepper although one was used")
	}
	// Same plaintext, second draw: DISTINCT encoding (different salt)...
	enc2, err := EncodeScimKey("pepper-α", key)
	if err != nil {
		t.Fatalf("EncodeScimKey (2): %v", err)
	}
	if enc == enc2 {
		t.Error("two encodings of the same key are identical (want distinct salts)")
	}
	// ...but both verify.
	if !VerifySecret(enc2, "pepper-α", key) {
		t.Error("second encoding does not verify")
	}
	// Wrong plaintext never verifies.
	other, err := NewScimKey()
	if err != nil {
		t.Fatalf("NewScimKey (other): %v", err)
	}
	if VerifySecret(enc, "pepper-α", other) {
		t.Error("a DIFFERENT key verifies against this encoding")
	}
}

// TestDeriveScimKey exercises the IdP's derivation RPC end to end (F-28.2):
// it returns the plaintext exactly once, an encoding that verifies under the
// IdP's configured pepper, and the 8-char prefix. It is a PURE derivation —
// nothing is stored, so it works with no stores attached (the bootstrap
// ritual).
func TestDeriveScimKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pepper string
	}{
		{name: "with pepper", pepper: "scim-pepper"},
		{name: "without pepper", pepper: ""},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			idp := startTestIDP(t, config.IdPConfig{ScimKeysPepper: tc.pepper}, nil)
			resp, err := idp.client.DeriveScimKey(context.Background(), &idppb.DeriveScimKeyRequest{})
			if err != nil {
				t.Fatalf("DeriveScimKey: %v", err)
			}
			key := resp.GetKey()
			if len(key) != scimKeyLength {
				t.Fatalf("plaintext length = %d, want %d", len(key), scimKeyLength)
			}
			if resp.GetPrefix() != key[:scimKeyPrefixLen] {
				t.Fatalf("prefix = %q, want %q", resp.GetPrefix(), key[:scimKeyPrefixLen])
			}
			enc := resp.GetEncoding()
			if !strings.HasPrefix(enc, "$argon2id$") {
				t.Fatalf("encoding %q lacks the argon2id marker", enc)
			}
			if !VerifySecret(enc, tc.pepper, key) {
				t.Fatal("encoding does not verify under the IdP's pepper")
			}
			if tc.pepper != "" && VerifySecret(enc, "", key) {
				t.Fatal("encoding verifies under an EMPTY pepper although one was configured")
			}
		})
	}
}

// TestDeriveScimKeyDistinctDraws checks that successive derivations produce
// distinct plaintexts and distinct encodings (fresh salt each time).
func TestDeriveScimKeyDistinctDraws(t *testing.T) {
	idp := startTestIDP(t, config.IdPConfig{ScimKeysPepper: "p"}, nil)
	first, err := idp.client.DeriveScimKey(context.Background(), &idppb.DeriveScimKeyRequest{})
	if err != nil {
		t.Fatalf("DeriveScimKey (1): %v", err)
	}
	second, err := idp.client.DeriveScimKey(context.Background(), &idppb.DeriveScimKeyRequest{})
	if err != nil {
		t.Fatalf("DeriveScimKey (2): %v", err)
	}
	if first.GetKey() == second.GetKey() {
		t.Error("two derivations returned the same plaintext")
	}
	if first.GetEncoding() == second.GetEncoding() {
		t.Error("two derivations returned the same encoding (want distinct salts)")
	}
}

// TestDeriveScimKeyUnknownMethodSanity guards the gRPC surface: the RPC is
// registered (calling it with an absurd expectation would be Internal, not
// Unimplemented).
func TestDeriveScimKeyRegistered(t *testing.T) {
	idp := startTestIDP(t, config.IdPConfig{}, nil)
	_, err := idp.client.DeriveScimKey(context.Background(), &idppb.DeriveScimKeyRequest{})
	if err != nil {
		// A pure derivation never fails; if it did it would be Internal
		// (randomness/KDF), never Unimplemented.
		if status.Code(err) == codes.Unimplemented {
			t.Fatalf("DeriveScimKey is Unimplemented: %v", err)
		}
		t.Fatalf("DeriveScimKey: %v", err)
	}
}
