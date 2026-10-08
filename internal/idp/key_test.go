package idp

import (
	"crypto/rsa"
	"log/slog"
	"os"
	"testing"
	"time"

	"cdrom/internal/config"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// testConfig returns a valid IdP config.
func testConfig(keyLifetime, rotateBefore time.Duration) config.IdPConfig {
	return config.IdPConfig{
		Issuer:        "http://127.0.0.1:7104",
		KeyLifetime:   keyLifetime,
		RotateBefore:  rotateBefore,
		TokenLifetime: time.Hour,
		CheckInterval: time.Hour,
	}
}

func TestKeyManagerGenerateAndJWKS(t *testing.T) {
	km, err := NewKeyManager(testConfig(time.Hour, 30*time.Minute), NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	keys, err := km.JWKS()
	if err != nil {
		t.Fatalf("JWKS: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("JWKS has %d keys, want 1", len(keys))
	}
	if keys[0].Algorithm != "RS256" || keys[0].KeyID == "" {
		t.Errorf("unexpected jwk: %+v", keys[0])
	}
	if _, ok := keys[0].Key.(*rsa.PublicKey); !ok {
		t.Errorf("jwk key is %T, want *rsa.PublicKey", keys[0].Key)
	}
}

func TestKeyManagerPersistsAcrossRestart(t *testing.T) {
	cfg := testConfig(time.Hour, 30*time.Minute)
	store := NewMemoryKeyStore()

	km1, err := NewKeyManager(cfg, store, testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	keys1, _ := km1.JWKS()
	kid1 := keys1[0].KeyID

	// A fresh manager over the same store must reuse the persisted key.
	km2, err := NewKeyManager(cfg, store, testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager (reload): %v", err)
	}
	keys2, _ := km2.JWKS()
	if len(keys2) != 1 || keys2[0].KeyID != kid1 {
		t.Fatalf("reload JWKS = %+v, want the same kid %q", keys2, kid1)
	}
}

func TestKeyManagerRotation(t *testing.T) {
	// Short lifetimes so the key enters its rotation window quickly.
	cfg := testConfig(500*time.Millisecond, 300*time.Millisecond)
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	keysBefore, _ := km.JWKS()
	kidBefore := keysBefore[0].KeyID

	// Wait until the current key is within its rotate-before window.
	time.Sleep(250 * time.Millisecond)
	km.RotateIfNeeded()

	keysAfter, _ := km.JWKS()
	if len(keysAfter) != 2 {
		t.Fatalf("after rotation JWKS has %d keys, want 2 (current + predecessor)", len(keysAfter))
	}
	var kidAfter string
	for _, k := range keysAfter {
		if k.KeyID == kidBefore {
			continue
		}
		kidAfter = k.KeyID
	}
	if kidAfter == "" {
		t.Fatal("rotation did not introduce a new key")
	}
	if kidAfter == kidBefore {
		t.Fatal("rotation kept the same kid")
	}

	// The new current key must be able to sign.
	token, err := km.Sign(map[string]any{"sub": "x"})
	if err != nil {
		t.Fatalf("Sign after rotation: %v", err)
	}
	if token == "" {
		t.Error("empty token after rotation")
	}
}

func TestKeyManagerSign(t *testing.T) {
	km, err := NewKeyManager(testConfig(time.Hour, 30*time.Minute), NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	token, err := km.Sign(map[string]any{"iss": "test", "sub": "u", "exp": time.Now().Add(time.Hour).Unix()})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	parts := 0
	for i := 0; i < len(token); i++ {
		if token[i] == '.' {
			parts++
		}
	}
	if parts != 2 {
		t.Errorf("token has %d segments, want 3 (header.payload.sig)", parts+1)
	}
}
