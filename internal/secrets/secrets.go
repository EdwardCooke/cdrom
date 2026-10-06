// Package secrets provides the secret store the API uses to encrypt and
// decrypt pipeline secrets (F-12).
//
// A pipeline declares a set of named secrets (a name → value mapping, like its
// parameters). The API is the only component that holds the key: it encrypts a
// secret's plaintext when the pipeline is created or updated, and decrypts the
// stored ciphertext when it hands a job to an execution target (so a step can
// reference the value as `{{ .secrets.name }}`). The database service stores
// the ciphertext opaquely and never sees the key, so no single service or
// configuration can decrypt a secret on its own.
//
// The store is an interface (Store) so other backends can be added as
// first-class citizens: the built-in "aes" store uses AES-256-GCM with a key
// from configuration, and "vault" / "openbao" are reserved for a future
// HashiCorp Vault / OpenBao backend. The built-in store draws a unique nonce
// for every encryption from a counter kept in the database (via a NonceSource),
// so every API replica shares one sequence and AES-GCM's unique key/nonce
// requirement holds across replicas. The nonce is embedded in the stored
// ciphertext, so decryption needs only the key.
package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"

	"cdrom/internal/config"
)

// StoreKind names a built-in Store implementation.
type StoreKind string

// Built-in store implementations.
const (
	// StoreKindAES is the built-in AES-256-GCM store (the default).
	StoreKindAES StoreKind = "aes"
	// StoreKindVault is a HashiCorp Vault-backed store (not yet implemented).
	StoreKindVault StoreKind = "vault"
	// StoreKindOpenBao is an OpenBao-backed store (not yet implemented).
	StoreKindOpenBao StoreKind = "openbao"
)

// Store encrypts and decrypts secret values. The API is the only caller: it
// encrypts a secret's plaintext on pipeline create/update and decrypts the
// stored ciphertext on dispatch. Implementations must be safe for concurrent
// use.
type Store interface {
	// Encrypt returns the ciphertext for plaintext. The ciphertext is
	// self-describing (it carries everything Decrypt needs, e.g. the nonce)
	// and is safe to store opaquely.
	Encrypt(ctx context.Context, plaintext string) (string, error)
	// Decrypt returns the plaintext for a ciphertext produced by Encrypt.
	Decrypt(ctx context.Context, ciphertext string) (string, error)
}

// NonceSource provides the next nonce for an encryption. The built-in AES
// store draws one from it on each Encrypt, so every encryption uses a unique
// nonce (AES-GCM requires a unique key/nonce pair). The API implements it by
// calling the Database service's NextSecretNonce RPC, so every API replica
// shares one counter and can never reuse a nonce.
type NonceSource interface {
	// NextNonce returns the next nonce and advances the counter.
	NextNonce(ctx context.Context) (uint64, error)
}

// NewStore builds a Store from the configured secret store. cfg selects the
// kind and carries the key material; nonce provides the per-encryption nonce
// (required by the built-in AES store). The built-in AES store is always
// available: when no key is configured it falls back to an all-zero key, so
// secrets can be exercised in test / local-dev scenarios without a real key
// (the zero key provides no real security — set a real key in production).
// A nil nonce source is an error for the AES store, since it needs a nonce
// for every encryption.
func NewStore(cfg config.SecretsConfig, nonce NonceSource) (Store, error) {
	switch StoreKind(strings.ToLower(cfg.EffectiveKind())) {
	case StoreKindAES, "":
		var key []byte
		if cfg.Key != "" {
			var err error
			key, err = base64.StdEncoding.DecodeString(cfg.Key)
			if err != nil {
				return nil, fmt.Errorf("secrets: key is not valid base64: %w", err)
			}
			if len(key) != 32 {
				return nil, fmt.Errorf("secrets: aes key must be 32 bytes (AES-256), got %d", len(key))
			}
		} else {
			// No key configured: fall back to an all-zero key so secrets can be
			// exercised in test / local-dev scenarios. The zero key provides no
			// real security; set a real key in production.
			key = make([]byte, 32)
		}
		if nonce == nil {
			return nil, fmt.Errorf("secrets: the aes store requires a nonce source")
		}
		return &AESStore{key: key, nonce: nonce}, nil
	case StoreKindVault, StoreKindOpenBao:
		return nil, fmt.Errorf("secrets: store kind %q is not implemented yet; use %q", cfg.EffectiveKind(), StoreKindAES)
	default:
		return nil, fmt.Errorf("secrets: unknown store kind %q (want %q, %q, or %q)", cfg.EffectiveKind(), StoreKindAES, StoreKindVault, StoreKindOpenBao)
	}
}

// gcmNonceSize is the AES-GCM nonce length (12 bytes, the recommended size).
const gcmNonceSize = 12

// AESStore is the built-in secret store: AES-256-GCM with a key from
// configuration. Each encryption draws a unique nonce from its NonceSource
// and embeds it in the ciphertext (nonce || gcm-ciphertext, base64-encoded),
// so decryption needs only the key.
type AESStore struct {
	key   []byte
	nonce NonceSource
}

// Compile-time check that AESStore implements Store.
var _ Store = (*AESStore)(nil)

// Encrypt AES-256-GCM-encrypts plaintext using a fresh nonce drawn from the
// store's NonceSource. The returned ciphertext is base64(nonce || gcm-sealed),
// so it is self-describing and safe to store opaquely.
func (s *AESStore) Encrypt(ctx context.Context, plaintext string) (string, error) {
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return "", fmt.Errorf("secrets: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("secrets: new gcm: %w", err)
	}
	counter, err := s.nonce.NextNonce(ctx)
	if err != nil {
		return "", fmt.Errorf("secrets: next nonce: %w", err)
	}
	nonce := nonceFromCounter(counter)
	sealed := gcm.Seal(nil, nonce, []byte(plaintext), nil)
	out := make([]byte, 0, len(nonce)+len(sealed))
	out = append(out, nonce...)
	out = append(out, sealed...)
	return base64.StdEncoding.EncodeToString(out), nil
}

// Decrypt AES-256-GCM-decrypts a ciphertext produced by Encrypt. The nonce is
// read from the front of the ciphertext, so the store's NonceSource is not
// needed to decrypt.
func (s *AESStore) Decrypt(ctx context.Context, ciphertext string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("secrets: ciphertext is not valid base64: %w", err)
	}
	if len(raw) < gcmNonceSize {
		return "", fmt.Errorf("secrets: ciphertext is too short")
	}
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return "", fmt.Errorf("secrets: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("secrets: new gcm: %w", err)
	}
	nonce, sealed := raw[:gcmNonceSize], raw[gcmNonceSize:]
	plaintext, err := gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		return "", fmt.Errorf("secrets: decrypt: %w", err)
	}
	return string(plaintext), nil
}

// nonceFromCounter encodes a nonce counter as a 12-byte AES-GCM nonce: four
// zero bytes followed by the counter as a big-endian uint64. The counter is
// unique per encryption (it is a shared, monotonically increasing sequence),
// so the resulting nonces are unique for a given key.
func nonceFromCounter(counter uint64) []byte {
	nonce := make([]byte, gcmNonceSize)
	binary.BigEndian.PutUint64(nonce[gcmNonceSize-8:], counter)
	return nonce
}
