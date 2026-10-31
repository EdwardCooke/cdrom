package idp

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// API key format (F-25). A key is the string `cdrom-` followed by 64 random
// alphanumeric characters. Only its hash is stored; the plaintext is shown to
// the user exactly once (at creation and at each rotation). A short,
// non-secret prefix of the key is stored so a user can tell keys apart in a
// listing without the plaintext.
const (
	apiKeyPrefix        = "cdrom-"
	apiKeyLength        = 64
	apiKeyDisplayPrefix = 12 // length of the non-secret prefix shown in listings
	// maxAPIKeyLifetime is the longest a key's expiration may be in the
	// future (one year).
	maxAPIKeyLifetime = 365 * 24 * time.Hour
)

// apiKeyAlphabet is the character set a key's random part is drawn from.
var apiKeyAlphabet = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// apiKeyLockoutForever is the instant used to record a permanent API-key
// lockout (one whose configured duration is zero): a far-future time that
// keeps the user locked out until an admin resets the lockout.
var apiKeyLockoutForever = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

// APIKey is an API key owned by a user (F-25). The IdP generates the
// plaintext secret at creation and at each rotation, encodes it (Argon2ID,
// pepper prepended to the KDF input, salt embedded in the encoding), and stores only the encoding and a short
// non-secret prefix. The plaintext is returned to the caller exactly once and
// is never persisted.
type APIKey struct {
	ID            string
	OwnerID       string
	Description   string
	KeyHash       string
	KeyPrefix     string
	ExpiresAt     time.Time
	PipelineScope []uint
	CreatedAt     time.Time
}

// APIKeyStore persists the API-key directory (key hashes, metadata, and the
// owner's lockout state) so that multiple IdP replicas share the same keys.
// The production implementation stores keys in the Database service; tests
// use an in-memory implementation.
type APIKeyStore interface {
	// Create generates a new `cdrom-…` key for the owner (looked up by
	// email), hashes it (mixing in pepper), stores it, and returns the stored
	// key plus the plaintext (shown to the caller exactly once). A key's
	// expiration is validated to be at most one year in the future.
	Create(ctx context.Context, ownerEmail, description string, expiresAt time.Time, pipelineScope []uint, pepper string) (*APIKey, string, error)
	// Get returns a key by id. A missing key returns an error with
	// status.Code == codes.NotFound.
	Get(ctx context.Context, id string) (*APIKey, error)
	// List returns API keys. When ownerID is set only that user's keys are
	// returned; when empty, all keys in the system are returned.
	List(ctx context.Context, ownerID string) ([]*APIKey, error)
	// Update edits a key's description, expiration, and/or pipeline scope
	// without changing its secret (a "renew"). The has* flags indicate which
	// fields are applied (a field whose flag is false is left unchanged):
	// hasDescription applies description, hasExpiresAt applies expiresAt (a
	// zero time means the key never expires), and hasScope applies
	// pipelineScope (an empty value means the key is not limited). A missing
	// key returns an error with status.Code == codes.NotFound.
	Update(ctx context.Context, id, description string, hasDescription bool, expiresAt time.Time, hasExpiresAt bool, hasScope bool, pipelineScope []uint) (*APIKey, error)
	// Rotate generates a new `cdrom-…` secret for the key (hashing it, mixing
	// in pepper), stores it, and returns the stored key plus the new
	// plaintext (shown to the caller exactly once); the previous secret stops
	// working. A missing key returns an error with status.Code == codes.NotFound.
	Rotate(ctx context.Context, id, pepper string) (*APIKey, string, error)
	// Delete removes a key by id. A missing key returns an error with
	// status.Code == codes.NotFound.
	Delete(ctx context.Context, id string) error
	// Verify checks a presented `username:apikey` credential against the
	// stored key directory (hash, expiration, and the owner's lockout). On a
	// miss it increments the owner's failure counter and locks the owner out
	// at maxFailures (for lockoutDuration, or permanently when it is zero).
	// A miss returns an error with status.Code == codes.Unauthenticated.
	Verify(ctx context.Context, username, apiKey, pepper string, maxFailures int, lockoutDuration time.Duration) (*APIKey, *User, error)
	// ResetLockout clears a user's API-key lockout state. A missing user
	// returns an error with status.Code == codes.NotFound.
	ResetLockout(ctx context.Context, userID string) error
}

// NewAPIKey generates a new API key's plaintext: `cdrom-` followed by 64
// random alphanumeric characters.
func NewAPIKey() (string, error) {
	b := make([]rune, apiKeyLength)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(apiKeyAlphabet))))
		if err != nil {
			return "", fmt.Errorf("idp: generate api key: %w", err)
		}
		b[i] = apiKeyAlphabet[n.Int64()]
	}
	return apiKeyPrefix + string(b), nil
}

// apiDisplayPrefix returns the short, non-secret prefix of a key shown in
// listings so a user can tell keys apart without the plaintext.
func apiDisplayPrefix(key string) string {
	if len(key) < apiKeyDisplayPrefix {
		return key
	}
	return key[:apiKeyDisplayPrefix]
}

// EncodeAPIKey returns the Argon2ID encoding (PHC string, embedding its own
// random salt and the API-key parameter profile) of the key's plaintext,
// with the optional pepper prepended to the input. The encoding is what gets
// stored; the plaintext is shown to the user exactly once.
func EncodeAPIKey(pepper, key string) (string, error) {
	return EncodeSecret(kdfAPIKeys, pepper, key)
}

// apiExpired reports whether the key's expiration (if any) has lapsed.
func apiExpired(expiresAt time.Time, now time.Time) bool {
	return !expiresAt.IsZero() && expiresAt.Before(now)
}

// apiKeyFromProto reconstructs an in-memory key from its Database-service form.
func apiKeyFromProto(k *dbpb.IDPAPIKey) *APIKey {
	key := &APIKey{
		ID:            k.GetId(),
		OwnerID:       k.GetOwnerId(),
		Description:   k.GetDescription(),
		KeyHash:       k.GetKeyHash(),
		KeyPrefix:     k.GetKeyPrefix(),
		PipelineScope: uintsFromInt64(k.GetPipelineScope()),
	}
	if ts := k.GetExpiresAt(); ts != nil {
		key.ExpiresAt = ts.AsTime()
	}
	if ts := k.GetCreatedAt(); ts != nil {
		key.CreatedAt = ts.AsTime()
	}
	return key
}

// apiKeyToProto renders an in-memory key as its Database-service form.
func apiKeyToProto(k *APIKey) *dbpb.IDPAPIKey {
	out := &dbpb.IDPAPIKey{
		Id:            k.ID,
		OwnerId:       k.OwnerID,
		Description:   k.Description,
		KeyHash:       k.KeyHash,
		KeyPrefix:     k.KeyPrefix,
		PipelineScope: int64sFromUint(k.PipelineScope),
	}
	if !k.ExpiresAt.IsZero() {
		out.ExpiresAt = timestamppb.New(k.ExpiresAt)
	}
	if !k.CreatedAt.IsZero() {
		out.CreatedAt = timestamppb.New(k.CreatedAt)
	}
	return out
}

// uintsFromInt64 converts a proto int64 list to a uint list (the model's
// pipeline-scope type).
func uintsFromInt64(in []int64) []uint {
	out := make([]uint, len(in))
	for i, v := range in {
		out[i] = uint(v)
	}
	return out
}

// int64sFromUint converts a uint list to a proto int64 list (the wire's
// pipeline-scope type).
func int64sFromUint(in []uint) []int64 {
	out := make([]int64, len(in))
	for i, v := range in {
		out[i] = int64(v)
	}
	return out
}

// dbAPIKeyStore is an APIKeyStore backed by the Database service.
type dbAPIKeyStore struct {
	db dbpb.DatabaseClient
}

// NewDBAPIKeyStore returns an APIKeyStore that persists the key directory
// through the Database service.
func NewDBAPIKeyStore(db dbpb.DatabaseClient) APIKeyStore {
	return &dbAPIKeyStore{db: db}
}

func (s *dbAPIKeyStore) Create(ctx context.Context, ownerEmail, description string, expiresAt time.Time, pipelineScope []uint, pepper string) (*APIKey, string, error) {
	if !expiresAt.IsZero() && expiresAt.After(time.Now().Add(maxAPIKeyLifetime)) {
		return nil, "", status.Error(codes.InvalidArgument, "expiration is more than one year in the future")
	}
	owner, err := s.db.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Email: ownerEmail})
	if err != nil {
		return nil, "", err
	}
	plaintext, err := NewAPIKey()
	if err != nil {
		return nil, "", err
	}
	keyHash, err := EncodeAPIKey(pepper, plaintext)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "idp: hash api key: %v", err)
	}
	key := &dbpb.IDPAPIKey{
		OwnerId:       owner.GetId(),
		Description:   description,
		KeyHash:       keyHash,
		KeyPrefix:     apiDisplayPrefix(plaintext),
		PipelineScope: int64sFromUint(pipelineScope),
	}
	if !expiresAt.IsZero() {
		key.ExpiresAt = timestamppb.New(expiresAt)
	}
	resp, err := s.db.CreateAPIKey(ctx, &dbpb.CreateIDPAPIKeyRequest{Key: key})
	if err != nil {
		return nil, "", err
	}
	return apiKeyFromProto(resp), plaintext, nil
}

func (s *dbAPIKeyStore) Get(ctx context.Context, id string) (*APIKey, error) {
	resp, err := s.db.GetAPIKey(ctx, &dbpb.GetIDPAPIKeyRequest{Id: id})
	if err != nil {
		return nil, err
	}
	return apiKeyFromProto(resp), nil
}

func (s *dbAPIKeyStore) List(ctx context.Context, ownerID string) ([]*APIKey, error) {
	resp, err := s.db.ListAPIKeys(ctx, &dbpb.ListIDPAPIKeysRequest{OwnerId: ownerID})
	if err != nil {
		return nil, err
	}
	out := make([]*APIKey, 0, len(resp.GetKeys()))
	for _, k := range resp.GetKeys() {
		out = append(out, apiKeyFromProto(k))
	}
	return out, nil
}

func (s *dbAPIKeyStore) Update(ctx context.Context, id, description string, hasDescription bool, expiresAt time.Time, hasExpiresAt bool, hasScope bool, pipelineScope []uint) (*APIKey, error) {
	key := &dbpb.IDPAPIKey{
		Id:            id,
		Description:   description,
		PipelineScope: int64sFromUint(pipelineScope),
	}
	if hasExpiresAt {
		if !expiresAt.IsZero() {
			key.ExpiresAt = timestamppb.New(expiresAt)
		}
	}
	resp, err := s.db.UpdateAPIKey(ctx, &dbpb.UpdateIDPAPIKeyRequest{
		Key:              key,
		HasDescription:   hasDescription,
		HasExpiresAt:     hasExpiresAt,
		HasPipelineScope: hasScope,
	})
	if err != nil {
		return nil, err
	}
	return apiKeyFromProto(resp), nil
}

func (s *dbAPIKeyStore) Rotate(ctx context.Context, id, pepper string) (*APIKey, string, error) {
	plaintext, err := NewAPIKey()
	if err != nil {
		return nil, "", err
	}
	keyHash, err := EncodeAPIKey(pepper, plaintext)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "idp: hash api key: %v", err)
	}
	resp, err := s.db.RotateAPIKey(ctx, &dbpb.RotateIDPAPIKeyRequest{
		Id:        id,
		KeyHash:   keyHash,
		KeyPrefix: apiDisplayPrefix(plaintext),
	})
	if err != nil {
		return nil, "", err
	}
	return apiKeyFromProto(resp), plaintext, nil
}

func (s *dbAPIKeyStore) Delete(ctx context.Context, id string) error {
	_, err := s.db.DeleteAPIKey(ctx, &dbpb.DeleteIDPAPIKeyRequest{Id: id})
	return err
}

// Verify checks a presented `username:apikey` credential against the stored
// key directory. Because the keys are Argon2ID encodings (their salt is
// embedded in the stored value), the presented plaintext cannot be reduced
// to a comparable hash UP FRONT: the owner is looked up, the owner's keys
// are enumerated, and each stored encoding is verified against the presented
// key locally (constant-time, per key). Lockout accounting (former home: the
// Database service's VerifyAPIKey) is performed here through
// AdjustAPIKeyLockout. On a miss it increments the owner's failure counter
// and locks the owner out at maxFailures (for lockoutDuration, or
// permanently when it is zero). A miss returns an error with
// status.Code == codes.Unauthenticated.
func (s *dbAPIKeyStore) Verify(ctx context.Context, username, apiKey, pepper string, maxFailures int, lockoutDuration time.Duration) (*APIKey, *User, error) {
	if username == "" || apiKey == "" {
		return nil, nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	owner, err := s.db.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Email: username})
	if err != nil {
		// Unknown user: fail with the same response as a bad key so the
		// endpoint does not reveal which usernames exist.
		if status.Code(err) == codes.NotFound {
			return nil, nil, status.Error(codes.Unauthenticated, "invalid credentials")
		}
		return nil, nil, err
	}
	// While locked, every API-key verification for the owner fails (the
	// counter is not incremented again).
	if lu := owner.GetApiKeyLockedUntil(); lu != nil && lu.AsTime().After(time.Now()) {
		return nil, nil, status.Error(codes.Unauthenticated, "api key access is locked")
	}
	keysResp, err := s.db.ListAPIKeys(ctx, &dbpb.ListIDPAPIKeysRequest{OwnerId: owner.GetId()})
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	matchedIdx := -1
	for i, k := range keysResp.GetKeys() {
		if k.GetKeyHash() == "" {
			continue
		}
		// A nil ExpiresAt means the key never expires. Call AsTime() only on a
		// present timestamp: on a nil one it yields the epoch (year 1970),
		// which apiExpired would wrongly classify as long past.
		if ts := k.GetExpiresAt(); ts != nil && apiExpired(ts.AsTime(), now) {
			continue
		}
		if VerifySecret(k.GetKeyHash(), pepper, apiKey) {
			matchedIdx = i
			break
		}
	}
	if matchedIdx >= 0 {
		// A match: clear the owner's failure counter (and lift any stale
		// lockout).
		if err := s.adjustAPIKeyLockout(ctx, owner.GetId(), -int32(owner.GetApiKeyFailedCount()), nil); err != nil {
			return nil, nil, err
		}
		return apiKeyFromProto(keysResp.GetKeys()[matchedIdx]), userFromProto(owner), nil
	}
	// A miss: increment the owner's failure counter and, at the configured
	// maximum, lock the owner out (for the configured duration, or
	// permanently when it is zero) and reset the counter.
	if maxFailures > 0 {
		count := int(owner.GetApiKeyFailedCount()) + 1
		if count >= maxFailures {
			until := now.Add(lockoutDuration)
			if lockoutDuration == 0 {
				until = apiKeyLockoutForever
			}
			if err := s.adjustAPIKeyLockout(ctx, owner.GetId(), 1, &until); err != nil {
				return nil, nil, err
			}
			return nil, nil, status.Error(codes.Unauthenticated, "invalid credentials")
		}
		if err := s.adjustAPIKeyLockout(ctx, owner.GetId(), 1, nil); err != nil {
			return nil, nil, err
		}
	}
	return nil, nil, status.Error(codes.Unauthenticated, "invalid credentials")
}

// adjustAPIKeyLockout applies a delta to the owner's API-key failure counter
// and adjusts the lockout instant (PRESENCE of lockoutUntil = set it; nil =
// clear it) — the lockout accounting the Database service used to perform
// inside VerifyAPIKey, now relocated because salted KDFs prevent pre-hashing
// the presented key.
func (s *dbAPIKeyStore) adjustAPIKeyLockout(ctx context.Context, userID string, failedDelta int32, lockoutUntil *time.Time) error {
	req := &dbpb.AdjustAPIKeyLockoutRequest{
		UserId:      userID,
		FailedDelta: failedDelta,
	}
	if lockoutUntil != nil {
		req.LockoutUntil = timestamppb.New(*lockoutUntil)
	}
	_, err := s.db.AdjustAPIKeyLockout(ctx, req)
	return err
}

func (s *dbAPIKeyStore) ResetLockout(ctx context.Context, userID string) error {
	_, err := s.db.ResetAPIKeyLockout(ctx, &dbpb.ResetIDPAPIKeyLockoutRequest{UserId: userID})
	return err
}
