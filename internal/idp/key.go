// Package idp implements a local OIDC identity provider that acts as a JWT
// issuer for the API's OIDC authentication. It serves the standard OIDC
// discovery document, a JWKS of its signing keys, an authorization endpoint
// (authorization-code + PKCE), and a token endpoint that mints RS256 ID
// tokens.
//
// The IdP signs tokens with an RSA key and rotates it automatically: a new
// key is generated when the current one is within the configured
// rotate-before window of its expiry. The JWKS always serves the current key
// plus any not-yet-expired predecessor keys, so clients keep verifying tokens
// signed with an older key during the overlap window.
//
// The signing keyring and the OIDC authorization codes are persisted through
// the Database service (not on the IdP's local filesystem), so multiple IdP
// replicas share the same keys and codes and can run behind a load balancer.
package idp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/config"
)

// signingKey is a single RSA signing key with its validity window and a
// stable key ID (kid) used in the JWT header and the JWKS. isCurrent marks
// the key the IdP signs new tokens with; it is persisted so a replica that
// reloads the keyring knows which key is current.
type signingKey struct {
	kid       string
	key       *rsa.PrivateKey
	notBefore time.Time
	expiresAt time.Time
	isCurrent bool
}

// KeyStore persists the IdP's signing keyring (the current key plus any
// not-yet-expired predecessor keys). The production implementation stores the
// keyring in the Database service so multiple IdP replicas share it; tests
// use an in-memory implementation.
type KeyStore interface {
	// ListKeys returns the persisted keyring.
	ListKeys(ctx context.Context) ([]*signingKey, error)
	// SetKeys atomically replaces the persisted keyring.
	SetKeys(ctx context.Context, keys []*signingKey) error
}

// dbKeyStore is a KeyStore backed by the Database service.
type dbKeyStore struct {
	db dbpb.DatabaseClient
}

func (s *dbKeyStore) ListKeys(ctx context.Context) ([]*signingKey, error) {
	resp, err := s.db.ListIDPSigningKeys(ctx, &dbpb.ListIDPSigningKeysRequest{})
	if err != nil {
		return nil, fmt.Errorf("idp: list signing keys: %w", err)
	}
	var keys []*signingKey
	for _, key := range resp.GetKeys() {
		sk, err := signingKeyFromProto(key)
		if err != nil {
			continue // skip malformed keys
		}
		keys = append(keys, sk)
	}
	return keys, nil
}

func (s *dbKeyStore) SetKeys(ctx context.Context, keys []*signingKey) error {
	req := &dbpb.SetIDPSigningKeysRequest{}
	for _, key := range keys {
		pk, err := key.toProto()
		if err != nil {
			return err
		}
		req.Keys = append(req.Keys, pk)
	}
	_, err := s.db.SetIDPSigningKeys(ctx, req)
	if err != nil {
		return fmt.Errorf("idp: set signing keys: %w", err)
	}
	return nil
}

// KeyManager owns the IdP's signing keys and rotates the current one
// automatically as it approaches expiry. It keeps the keyring in memory for
// fast signing and JWKS serving, and persists it through the KeyStore so the
// state survives restarts and is shared across replicas. It is safe for
// concurrent use.
type KeyManager struct {
	cfg    config.IdPConfig
	store  KeyStore
	logger *slog.Logger

	mu      sync.RWMutex
	current *signingKey
	keys    []*signingKey // current + not-yet-expired predecessors
}

// NewKeyManager loads the keyring from the store (or generates a fresh key
// when none exists) and returns a ready KeyManager. If the current key is
// already within its rotation window it is rotated immediately.
func NewKeyManager(cfg config.IdPConfig, store KeyStore, logger *slog.Logger) (*KeyManager, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if store == nil {
		return nil, fmt.Errorf("idp: a key store is required")
	}
	k := &KeyManager{cfg: cfg, store: store, logger: logger}
	if err := k.load(); err != nil {
		return nil, err
	}
	return k, nil
}

// load reads the keyring from the store, drops expired keys, ensures a valid
// current key exists (generating one if needed), persists the result, and
// rotates if the current key is in its rotation window.
func (k *KeyManager) load() error {
	stored, err := k.store.ListKeys(context.Background())
	if err != nil {
		return fmt.Errorf("idp: load keyring: %w", err)
	}

	now := time.Now()
	var keys []*signingKey
	var current *signingKey
	for _, key := range stored {
		if key.expiresAt.Before(now) {
			continue // expired
		}
		keys = append(keys, key)
		if key.isCurrent {
			current = key
		}
	}
	if current == nil {
		// Current key missing or expired: generate a fresh one.
		fresh, err := k.generateKey()
		if err != nil {
			return err
		}
		fresh.isCurrent = true
		current = fresh
		keys = append(keys, fresh)
		k.logger.Info("idp: generated new signing key", "kid", fresh.kid, "expires", fresh.expiresAt)
	}

	k.mu.Lock()
	k.keys = keys
	k.current = current
	k.mu.Unlock()

	if err := k.persist(); err != nil {
		return err
	}
	k.RotateIfNeeded()
	return nil
}

// generateKey creates a new 2048-bit RSA signing key valid for the
// configured key lifetime.
func (k *KeyManager) generateKey() (*signingKey, error) {
	kid, err := newKid()
	if err != nil {
		return nil, err
	}
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("idp: generate rsa key: %w", err)
	}
	now := time.Now()
	return &signingKey{
		kid:       kid,
		key:       rsaKey,
		notBefore: now,
		expiresAt: now.Add(k.cfg.KeyLifetime),
	}, nil
}

// RotateIfNeeded rotates the current signing key when it is within the
// configured rotate-before window of its expiry. Rotation keeps the old key
// (as a predecessor) until it expires so the JWKS can still verify tokens it
// signed.
func (k *KeyManager) RotateIfNeeded() {
	k.mu.RLock()
	needs := k.current != nil && time.Until(k.current.expiresAt) <= k.cfg.RotateBefore
	k.mu.RUnlock()
	if !needs {
		return
	}
	if err := k.rotate(); err != nil {
		k.logger.Error("idp: rotate signing key", "err", err)
	}
}

// rotate generates a new current key, demoting the old current key to a
// predecessor (kept until it expires), and persists the keyring.
func (k *KeyManager) rotate() error {
	newKey, err := k.generateKey()
	if err != nil {
		return err
	}
	now := time.Now()
	k.mu.Lock()
	oldKid := k.current.kid
	var keys []*signingKey
	for _, key := range k.keys {
		if key.expiresAt.After(now) {
			keys = append(keys, key)
		}
	}
	if k.current.expiresAt.After(now) && !containsKid(keys, k.current.kid) {
		keys = append(keys, k.current)
	}
	k.keys = keys
	k.current = newKey
	k.mu.Unlock()

	k.logger.Info("idp: rotated signing key",
		"old_kid", oldKid, "new_kid", newKey.kid, "new_expires", newKey.expiresAt)
	return k.persist()
}

// persist writes the current keyring (current + not-yet-expired keys) to the
// store. The current key is marked as such; all others are predecessors.
func (k *KeyManager) persist() error {
	k.mu.Lock()
	now := time.Now()
	keys := make([]*signingKey, 0, len(k.keys)+1)
	for _, key := range k.keys {
		if key.expiresAt.Before(now) {
			continue
		}
		key.isCurrent = false
		keys = append(keys, key)
	}
	if k.current != nil {
		k.current.isCurrent = true
		if !containsKid(keys, k.current.kid) {
			keys = append(keys, k.current)
		}
	}
	k.mu.Unlock()

	return k.store.SetKeys(context.Background(), keys)
}

// JWKS returns the JSON Web Keys to serve: the current key plus any
// not-yet-expired predecessor keys.
func (k *KeyManager) JWKS() ([]jwkJSON, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	now := time.Now()
	var out []jwkJSON
	seen := make(map[string]bool)
	if k.current != nil && k.current.expiresAt.After(now) {
		out = append(out, k.current.jwk())
		seen[k.current.kid] = true
	}
	for _, key := range k.keys {
		if key.expiresAt.Before(now) || seen[key.kid] {
			continue
		}
		out = append(out, key.jwk())
		seen[key.kid] = true
	}
	return out, nil
}

// Sign signs claims as an RS256 JWT using the current signing key.
func (k *KeyManager) Sign(claims map[string]any) (string, error) {
	k.mu.RLock()
	current := k.current
	k.mu.RUnlock()
	if current == nil {
		return "", fmt.Errorf("idp: no current signing key")
	}
	return current.signRS256(claims)
}

// newKid returns a random 16-hex-char key ID.
func newKid() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("idp: generate kid: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func containsKid(keys []*signingKey, kid string) bool {
	for _, key := range keys {
		if key.kid == kid {
			return true
		}
	}
	return false
}

// toProto renders the key as its Database-service form (PKCS8 PEM + metadata).
func (k *signingKey) toProto() (*dbpb.IDPSigningKey, error) {
	der, err := x509.MarshalPKCS8PrivateKey(k.key)
	if err != nil {
		return nil, fmt.Errorf("idp: marshal key %q: %w", k.kid, err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	return &dbpb.IDPSigningKey{
		Kid:       k.kid,
		IsCurrent: k.isCurrent,
		NotBefore: timestamppb.New(k.notBefore),
		ExpiresAt: timestamppb.New(k.expiresAt),
		Pem:       string(pemBytes),
	}, nil
}

// signingKeyFromProto reconstructs an in-memory signing key from its
// Database-service form.
func signingKeyFromProto(pk *dbpb.IDPSigningKey) (*signingKey, error) {
	block, _ := pem.Decode([]byte(pk.GetPem()))
	if block == nil {
		return nil, fmt.Errorf("idp: decode pem for key %q", pk.GetKid())
	}
	der, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("idp: parse key %q: %w", pk.GetKid(), err)
	}
	rsaKey, ok := der.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("idp: key %q is not RSA", pk.GetKid())
	}
	sk := &signingKey{
		kid:       pk.GetKid(),
		key:       rsaKey,
		isCurrent: pk.GetIsCurrent(),
	}
	if ts := pk.GetNotBefore(); ts != nil {
		sk.notBefore = ts.AsTime()
	}
	if ts := pk.GetExpiresAt(); ts != nil {
		sk.expiresAt = ts.AsTime()
	}
	return sk, nil
}
