package idp

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// registerTestUser registers a user with the given email and password through
// the IdP's gRPC surface (so the user exists in the user store the API-key
// store resolves owners against).
func registerTestUser(t *testing.T, idp *testIDP, email, password string) {
	t.Helper()
	if _, err := idp.client.Register(context.Background(), &idppb.RegisterRequest{
		Email: email, Password: password,
	}); err != nil {
		t.Fatalf("register %s: %v", email, err)
	}
}

// TestAPIKeyGenerateAndHash checks the key's shape (cdrom- + 64 alphanumeric
// characters) and that the hash is deterministic and pepper-sensitive.
func TestAPIKeyGenerateAndHash(t *testing.T) {
	key, err := NewAPIKey()
	if err != nil {
		t.Fatalf("NewAPIKey: %v", err)
	}
	if !strings.HasPrefix(key, apiKeyPrefix) {
		t.Errorf("key = %q, want prefix %q", key, apiKeyPrefix)
	}
	if len(key) != len(apiKeyPrefix)+apiKeyLength {
		t.Errorf("key length = %d, want %d", len(key), len(apiKeyPrefix)+apiKeyLength)
	}
	// The random part is alphanumeric.
	for _, r := range key[len(apiKeyPrefix):] {
		if !isAlnum(r) {
			t.Errorf("key contains non-alphanumeric rune %q", r)
			break
		}
	}
	// Two generated keys differ.
	other, err := NewAPIKey()
	if err != nil {
		t.Fatalf("NewAPIKey (2): %v", err)
	}
	if key == other {
		t.Error("two generated keys are identical")
	}
	// The hash is deterministic and pepper-sensitive.
	if HashAPIKey("p", key) != HashAPIKey("p", key) {
		t.Error("hash is not deterministic")
	}
	if HashAPIKey("p", key) == HashAPIKey("q", key) {
		t.Error("hash is not pepper-sensitive")
	}
	if HashAPIKey("", key) == HashAPIKey("p", key) {
		t.Error("hash is not pepper-sensitive (empty pepper)")
	}
}

func isAlnum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// TestAPIKeyCRUD drives the IdP's API-key gRPC surface (F-25): create (with
// the plaintext returned once), get, list (per-owner and all), update (renew,
// secret unchanged), rotate (new plaintext, old stops working), and delete.
func TestAPIKeyCRUD(t *testing.T) {
	users := NewMemoryUserStore()
	keys := NewMemoryAPIKeyStore(users)
	idp := startTestIDPWithKeys(t, testConfig(time.Hour, 30*time.Minute), users, keys)
	ctx := context.Background()
	registerTestUser(t, idp, "ada@example.com", "pw")

	// Create a key: the plaintext is returned once and has the key shape.
	created, err := idp.client.CreateAPIKey(ctx, &idppb.CreateAPIKeyRequest{
		OwnerEmail:    "ada@example.com",
		Description:   "ci",
		PipelineScope: []int64{1, 2},
	})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	plaintext := created.GetPlaintext()
	if !strings.HasPrefix(plaintext, apiKeyPrefix) {
		t.Errorf("plaintext = %q, want prefix %q", plaintext, apiKeyPrefix)
	}
	if created.GetKey().GetId() == "" {
		t.Fatal("created key has no ID")
	}
	if created.GetKey().GetKeyHash() == "" {
		t.Error("created key has no hash")
	}
	if created.GetKey().GetKeyHash() == plaintext {
		t.Error("stored key hash equals the plaintext (the plaintext must not be stored)")
	}
	if len(created.GetKey().GetPipelineScope()) != 2 {
		t.Errorf("scope = %v, want [1 2]", created.GetKey().GetPipelineScope())
	}
	keyID := created.GetKey().GetId()

	// Get returns metadata (never the plaintext).
	got, err := idp.client.GetAPIKey(ctx, &idppb.GetAPIKeyRequest{Id: keyID})
	if err != nil {
		t.Fatalf("GetAPIKey: %v", err)
	}
	if got.GetKeyHash() != created.GetKey().GetKeyHash() {
		t.Errorf("get hash = %q, want %q", got.GetKeyHash(), created.GetKey().GetKeyHash())
	}

	// A missing key is NotFound.
	if _, err := idp.client.GetAPIKey(ctx, &idppb.GetAPIKeyRequest{Id: "999"}); status.Code(err) != codes.NotFound {
		t.Errorf("get missing = %v, want NotFound", err)
	}

	// Create a second key (a user can have any number of keys).
	if _, err := idp.client.CreateAPIKey(ctx, &idppb.CreateAPIKeyRequest{OwnerEmail: "ada@example.com", Description: "second"}); err != nil {
		t.Fatalf("CreateAPIKey second: %v", err)
	}

	// List the owner's keys (two).
	ownerKeys, err := idp.client.ListAPIKeys(ctx, &idppb.ListAPIKeysRequest{OwnerId: created.GetKey().GetOwnerId()})
	if err != nil {
		t.Fatalf("ListAPIKeys owner: %v", err)
	}
	if len(ownerKeys.GetKeys()) != 2 {
		t.Errorf("owner keys = %d, want 2", len(ownerKeys.GetKeys()))
	}

	// List all keys (two).
	allKeys, err := idp.client.ListAPIKeys(ctx, &idppb.ListAPIKeysRequest{})
	if err != nil {
		t.Fatalf("ListAPIKeys all: %v", err)
	}
	if len(allKeys.GetKeys()) != 2 {
		t.Errorf("all keys = %d, want 2", len(allKeys.GetKeys()))
	}

	// Update (renew) the description and pipeline scope; the secret is
	// unchanged (the same plaintext keeps working).
	updated, err := idp.client.UpdateAPIKey(ctx, &idppb.UpdateAPIKeyRequest{
		Id:               keyID,
		Description:      "renamed",
		HasDescription:   true,
		PipelineScope:    []int64{3},
		HasPipelineScope: true,
	})
	if err != nil {
		t.Fatalf("UpdateAPIKey: %v", err)
	}
	if updated.GetDescription() != "renamed" {
		t.Errorf("description = %q, want renamed", updated.GetDescription())
	}
	if len(updated.GetPipelineScope()) != 1 || updated.GetPipelineScope()[0] != 3 {
		t.Errorf("scope = %v, want [3]", updated.GetPipelineScope())
	}
	if updated.GetKeyHash() != created.GetKey().GetKeyHash() {
		t.Errorf("hash after renew = %q, want unchanged", updated.GetKeyHash())
	}
	// The same plaintext still verifies after a renew.
	if _, _, err := keys.Verify(ctx, "ada@example.com", plaintext, "", 0, 0); err != nil {
		t.Errorf("verify after renew = %v, want success", err)
	}

	// Rotate: a new plaintext is returned once and the old one stops working.
	rotated, err := idp.client.RotateAPIKey(ctx, &idppb.RotateAPIKeyRequest{Id: keyID})
	if err != nil {
		t.Fatalf("RotateAPIKey: %v", err)
	}
	newPlaintext := rotated.GetPlaintext()
	if newPlaintext == plaintext {
		t.Error("rotated plaintext equals the old one")
	}
	if rotated.GetKey().GetKeyHash() == created.GetKey().GetKeyHash() {
		t.Error("rotated hash equals the old one")
	}
	// The new plaintext verifies; the old one no longer does.
	if _, _, err := keys.Verify(ctx, "ada@example.com", newPlaintext, "", 0, 0); err != nil {
		t.Errorf("verify new key = %v, want success", err)
	}
	if _, _, err := keys.Verify(ctx, "ada@example.com", plaintext, "", 0, 0); err == nil {
		t.Error("old key still verifies after rotation")
	}

	// Delete the key.
	if _, err := idp.client.DeleteAPIKey(ctx, &idppb.DeleteAPIKeyRequest{Id: keyID}); err != nil {
		t.Fatalf("DeleteAPIKey: %v", err)
	}
	if _, err := idp.client.GetAPIKey(ctx, &idppb.GetAPIKeyRequest{Id: keyID}); status.Code(err) != codes.NotFound {
		t.Errorf("get after delete = %v, want NotFound", err)
	}
}

// TestAPIKeyVerifyAndLockout checks that a presented `username:apikey`
// credential verifies against the stored hash, that a miss increments the
// owner's failure counter and locks the owner out at the configured maximum
// (for the configured duration, or permanently when it is zero), that a
// successful verification clears the counter, and that an admin can reset the
// lockout.
func TestAPIKeyVerifyAndLockout(t *testing.T) {
	users := NewMemoryUserStore()
	keys := NewMemoryAPIKeyStore(users)
	idp := startTestIDPWithKeys(t, testConfig(time.Hour, 30*time.Minute), users, keys)
	ctx := context.Background()
	registerTestUser(t, idp, "ada@example.com", "pw")

	created, err := idp.client.CreateAPIKey(ctx, &idppb.CreateAPIKeyRequest{OwnerEmail: "ada@example.com"})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	plaintext := created.GetPlaintext()

	// A correct key verifies (returns the key and the owner).
	verified, err := idp.client.VerifyAPIKey(ctx, &idppb.VerifyAPIKeyRequest{
		Username:    "ada@example.com",
		ApiKey:      plaintext,
		MaxFailures: 3,
	})
	if err != nil {
		t.Fatalf("VerifyAPIKey: %v", err)
	}
	if verified.GetOwner().GetEmail() != "ada@example.com" {
		t.Errorf("owner = %q, want ada@example.com", verified.GetOwner().GetEmail())
	}
	if !hasRole(verified.GetOwner().GetRoles(), "admin") {
		t.Errorf("owner roles = %v, want to include admin", verified.GetOwner().GetRoles())
	}

	// A wrong key is Unauthenticated and increments the failure counter.
	if _, err := idp.client.VerifyAPIKey(ctx, &idppb.VerifyAPIKeyRequest{
		Username:    "ada@example.com",
		ApiKey:      apiKeyPrefix + "wrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrongwrong",
		MaxFailures: 3,
	}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("wrong key = %v, want Unauthenticated", err)
	}

	// A key presented with the wrong username (owner mismatch) is a miss.
	if _, err := idp.client.VerifyAPIKey(ctx, &idppb.VerifyAPIKeyRequest{
		Username:    "nobody@example.com",
		ApiKey:      plaintext,
		MaxFailures: 3,
	}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("wrong username = %v, want Unauthenticated", err)
	}

	// Lockout: with max_failures=1, a single miss locks the owner out
	// (permanently, since lockout_duration is 0); a correct key is rejected
	// while locked.
	if _, err := idp.client.VerifyAPIKey(ctx, &idppb.VerifyAPIKeyRequest{
		Username:    "ada@example.com",
		ApiKey:      apiKeyPrefix + "wrong",
		MaxFailures: 1,
	}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("lockout trigger = %v, want Unauthenticated", err)
	}
	if _, err := idp.client.VerifyAPIKey(ctx, &idppb.VerifyAPIKeyRequest{
		Username:    "ada@example.com",
		ApiKey:      plaintext,
		MaxFailures: 1,
	}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("verify while locked = %v, want Unauthenticated", err)
	}

	// Reset the lockout: a correct key verifies again.
	if _, err := idp.client.ResetAPIKeyLockout(ctx, &idppb.ResetAPIKeyLockoutRequest{UserId: verified.GetOwner().GetId()}); err != nil {
		t.Fatalf("ResetAPIKeyLockout: %v", err)
	}
	if _, err := idp.client.VerifyAPIKey(ctx, &idppb.VerifyAPIKeyRequest{
		Username:    "ada@example.com",
		ApiKey:      plaintext,
		MaxFailures: 1,
	}); err != nil {
		t.Errorf("verify after reset = %v, want success", err)
	}

	// Resetting a missing user is NotFound.
	if _, err := idp.client.ResetAPIKeyLockout(ctx, &idppb.ResetAPIKeyLockoutRequest{UserId: "999"}); status.Code(err) != codes.NotFound {
		t.Errorf("reset missing user = %v, want NotFound", err)
	}
}

// TestAPIKeyExpiration checks that a key's expiration is validated to be at
// most one year in the future, and that an expired key is rejected.
func TestAPIKeyExpiration(t *testing.T) {
	users := NewMemoryUserStore()
	keys := NewMemoryAPIKeyStore(users)
	idp := startTestIDPWithKeys(t, testConfig(time.Hour, 30*time.Minute), users, keys)
	ctx := context.Background()
	registerTestUser(t, idp, "ada@example.com", "pw")

	// An expiration more than one year in the future is rejected.
	if _, err := idp.client.CreateAPIKey(ctx, &idppb.CreateAPIKeyRequest{
		OwnerEmail: "ada@example.com",
		ExpiresIn:  "400d",
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("create with >1y expiration = %v, want InvalidArgument", err)
	}

	// A key that expires immediately is rejected on verification.
	created, err := idp.client.CreateAPIKey(ctx, &idppb.CreateAPIKeyRequest{
		OwnerEmail: "ada@example.com",
		ExpiresIn:  "1ns",
	})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	// Give the (already-expired) key a moment to be past its expiry.
	time.Sleep(2 * time.Millisecond)
	if _, err := idp.client.VerifyAPIKey(ctx, &idppb.VerifyAPIKeyRequest{
		Username:    "ada@example.com",
		ApiKey:      created.GetPlaintext(),
		MaxFailures: 0,
	}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("verify expired key = %v, want Unauthenticated", err)
	}
}
