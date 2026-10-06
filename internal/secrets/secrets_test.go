package secrets

import (
	"context"
	"encoding/base64"
	"testing"

	"cdrom/internal/config"
)

// fakeNonceSource hands out an increasing sequence of nonces and records how
// many times it was consulted, so a test can assert that each encryption drew
// a fresh nonce.
type fakeNonceSource struct {
	next  uint64
	calls int
}

func (f *fakeNonceSource) NextNonce(context.Context) (uint64, error) {
	f.calls++
	f.next++
	return f.next, nil
}

// testConfig returns a SecretsConfig with a valid 32-byte AES-256 key.
func testConfig() config.SecretsConfig {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	return config.SecretsConfig{Kind: "aes", Key: base64.StdEncoding.EncodeToString(key)}
}

// TestNewStoreNoKeyFallsBackToZeroKey verifies that when no key is configured
// the built-in AES store falls back to an all-zero key (so secrets can be
// exercised in test / local-dev scenarios) and still round-trips a value.
func TestNewStoreNoKeyFallsBackToZeroKey(t *testing.T) {
	nonce := &fakeNonceSource{}
	store, err := NewStore(config.SecretsConfig{}, nonce)
	if err != nil {
		t.Fatalf("NewStore (no key): %v", err)
	}
	if store == nil {
		t.Fatal("NewStore (no key) = nil, want the all-zero-key AES store")
	}
	ctx := context.Background()
	const plaintext = "dev-secret"
	ct, err := store.Encrypt(ctx, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	got, err := store.Decrypt(ctx, ct)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if got != plaintext {
		t.Errorf("Decrypt = %q, want %q", got, plaintext)
	}
}

// TestNewStoreAESRequiresNonceSource verifies that the built-in AES store
// cannot be built without a nonce source, even when no key is configured.
func TestNewStoreAESRequiresNonceSource(t *testing.T) {
	if _, err := NewStore(config.SecretsConfig{}, nil); err == nil {
		t.Fatal("NewStore (no nonce source): expected error, got nil")
	}
}

// TestNewStoreRejectsBadKey verifies that a key that is not 32 bytes is
// rejected for the AES store.
func TestNewStoreRejectsBadKey(t *testing.T) {
	cfg := testConfig()
	cfg.Key = base64.StdEncoding.EncodeToString([]byte("too-short"))
	if _, err := NewStore(cfg, &fakeNonceSource{}); err == nil {
		t.Fatal("NewStore (short key): expected error, got nil")
	}
}

// TestNewStoreRejectsUnknownKind verifies that an unrecognised store kind is
// rejected.
func TestNewStoreRejectsUnknownKind(t *testing.T) {
	cfg := testConfig()
	cfg.Kind = "bogus"
	if _, err := NewStore(cfg, &fakeNonceSource{}); err == nil {
		t.Fatal("NewStore (unknown kind): expected error, got nil")
	}
}

// TestNewStoreUnimplementedKind verifies that the reserved vault/openbao kinds
// are first-class but not yet implemented.
func TestNewStoreUnimplementedKind(t *testing.T) {
	for _, kind := range []string{"vault", "openbao"} {
		cfg := testConfig()
		cfg.Kind = kind
		if _, err := NewStore(cfg, &fakeNonceSource{}); err == nil {
			t.Errorf("NewStore (%s): expected not-implemented error, got nil", kind)
		}
	}
}

// TestAESRoundTrip verifies that a value encrypted by the AES store decrypts
// back to the original plaintext, and that each encryption draws a fresh nonce
// from the source (so two ciphertexts of the same plaintext differ).
func TestAESRoundTrip(t *testing.T) {
	nonce := &fakeNonceSource{}
	store, err := NewStore(testConfig(), nonce)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	ctx := context.Background()

	const plaintext = "s3cr3t-value"
	ct1, err := store.Encrypt(ctx, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if got, err := store.Decrypt(ctx, ct1); err != nil {
		t.Fatalf("Decrypt: %v", err)
	} else if got != plaintext {
		t.Errorf("Decrypt = %q, want %q", got, plaintext)
	}

	// A second encryption of the same plaintext must use a different nonce, so
	// the ciphertext differs (AES-GCM is non-deterministic per nonce).
	ct2, err := store.Encrypt(ctx, plaintext)
	if err != nil {
		t.Fatalf("Encrypt (2): %v", err)
	}
	if ct1 == ct2 {
		t.Error("two encryptions of the same plaintext produced identical ciphertexts")
	}
	if nonce.calls != 2 {
		t.Errorf("nonce source consulted %d times, want 2", nonce.calls)
	}
}

// TestAESDecryptWrongKey verifies that a ciphertext cannot be decrypted with a
// different key (AES-GCM's authentication tag fails).
func TestAESDecryptWrongKey(t *testing.T) {
	store, err := NewStore(testConfig(), &fakeNonceSource{})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	ctx := context.Background()
	ct, err := store.Encrypt(ctx, "value")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	other := testConfig()
	other.Key = base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20})
	wrong, err := NewStore(other, &fakeNonceSource{})
	if err != nil {
		t.Fatalf("NewStore (wrong key): %v", err)
	}
	if _, err := wrong.Decrypt(ctx, ct); err == nil {
		t.Error("Decrypt with the wrong key: expected error, got nil")
	}
}

// TestAESDecryptGarbage verifies that a malformed ciphertext is rejected.
func TestAESDecryptGarbage(t *testing.T) {
	store, err := NewStore(testConfig(), &fakeNonceSource{})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.Decrypt(context.Background(), "not-base64!!!"); err == nil {
		t.Error("Decrypt (garbage): expected error, got nil")
	}
}
