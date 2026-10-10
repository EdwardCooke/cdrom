package idp

import (
	"context"
	"crypto/rand"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// Service-account key format (F-26). A key is the string `cdrom-sa-` followed
// by 64 random alphanumeric characters. Only its salted hash is stored; the
// plaintext is shown to the caller exactly once (at creation and at each
// rotation). A short, non-secret prefix of the key is stored so a caller can
// tell keys apart in a listing without the plaintext.
const (
	serviceAccountKeyPrefix        = "cdrom-sa-"
	serviceAccountKeyLength        = 64
	serviceAccountKeyDisplayPrefix = 12 // length of the non-secret prefix shown in listings
	// serviceAccountKeySaltBytes is the size of the per-key salt (in bytes).
	// The salt is unique to every key (generated with the key) and is mixed
	// into the key's hash, so two keys with the same plaintext never collide.
	serviceAccountKeySaltBytes = 32
)

// serviceAccountKeyAlphabet is the character set a key's random part is drawn
// from.
var serviceAccountKeyAlphabet = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// ServiceAccountKey is one of a service account's two API-key slots (F-26).
// The IdP generates the plaintext secret at creation and at each rotation,
// hashes it (SHA-512 of pepper + salt + key, with a per-key salt and an
// optional pepper), and stores only the salted hash, salt, and a short
// non-secret prefix. The plaintext is returned to the caller exactly once and
// is never persisted.
type ServiceAccountKey struct {
	Slot      int
	KeyHash   string
	KeySalt   string
	KeyPrefix string
}

// ServiceAccount is a non-human identity for automation (F-26): a unique login
// name, an editable display name/description, role memberships, a disabled
// flag, a soft-delete tombstone, a key lockout state, and its two key slots.
type ServiceAccount struct {
	ID             string
	LoginName      string
	DisplayName    string
	Description    string
	Disabled       bool
	Deleted        bool
	KeyFailedCount int
	KeyLockedUntil time.Time
	Roles          []string
	Keys           []*ServiceAccountKey
	CreatedBy      string
	UpdatedBy      string
	Revision       int
}

// ServiceAccountStore persists the service-account directory (accounts, their
// two key slots' salted hashes, role bindings, and lockout state) so that
// multiple IdP replicas share the same accounts. The production implementation
// stores accounts in the Database service; tests use an in-memory
// implementation.
type ServiceAccountStore interface {
	// Create generates both of the account's keys (the caller supplies the
	// salted hashes, salts, and prefixes — it generated and hashed the
	// plaintexts), persists the account, both key slots, and its role bindings
	// atomically, and returns the stored account. A duplicate login name is
	// AlreadyExists.
	Create(ctx context.Context, acc *ServiceAccount, roles []string) (*ServiceAccount, error)
	// Get returns an account by id or login name. When includeDeleted is false
	// a deleted account is treated as missing. A missing account returns an
	// error with status.Code == codes.NotFound.
	Get(ctx context.Context, id, loginName string, includeDeleted bool) (*ServiceAccount, error)
	// List returns service accounts. When includeDeleted is false deleted
	// accounts are excluded.
	List(ctx context.Context, includeDeleted bool) ([]*ServiceAccount, error)
	// Update edits an account's display name/description (never its roles,
	// status, or keys). A stale revision is Aborted. A missing account returns
	// an error with status.Code == codes.NotFound.
	Update(ctx context.Context, id, displayName, description string, hasDisplayName, hasDescription bool, revision int64, updatedBy string) (*ServiceAccount, error)
	// RotateKey replaces one slot's salted hash, salt, and prefix (the caller
	// generated and hashed the new plaintext), bumping the slot's generation.
	// A stale revision is Aborted. A missing account returns an error with
	// status.Code == codes.NotFound.
	RotateKey(ctx context.Context, id string, slot int, keyHash, keySalt, keyPrefix string, revision int64, updatedBy string) (*ServiceAccount, error)
	// SetDisabled sets (or clears) the account's disabled flag. A stale
	// revision is Aborted. A missing account returns an error with
	// status.Code == codes.NotFound.
	SetDisabled(ctx context.Context, id string, disabled bool, revision int64, updatedBy string) (*ServiceAccount, error)
	// Delete permanently soft-deletes the account (tombstone + zeroed key
	// slots). Repeated delete is idempotent. A missing account returns an
	// error with status.Code == codes.NotFound.
	Delete(ctx context.Context, id, updatedBy string) (*ServiceAccount, error)
	// AssignRole attaches a role to the account (a RoleBinding row). It is
	// idempotent on (account, role). A missing account returns an error with
	// status.Code == codes.NotFound.
	AssignRole(ctx context.Context, id, role string) (*ServiceAccount, error)
	// RemoveRole removes a role binding from the account (by role name). A
	// missing binding is a no-op. A missing account returns an error with
	// status.Code == codes.NotFound.
	RemoveRole(ctx context.Context, id, role string) (*ServiceAccount, error)
	// RecordKeyAttempt atomically updates the account's key lockout state
	// after a key verification: on a success it resets the failure counter; on
	// a miss it increments the counter and locks the account out at
	// maxFailures (for lockoutDuration, or permanently when it is zero). A
	// missing account returns an error with status.Code == codes.NotFound.
	RecordKeyAttempt(ctx context.Context, id string, success bool, maxFailures int, lockoutDuration time.Duration) (*ServiceAccount, error)
	// ResetLockout clears the account's key lockout state. A missing account
	// returns an error with status.Code == codes.NotFound.
	ResetLockout(ctx context.Context, id string) error
}

// NewServiceAccountKey generates a new service-account key's plaintext:
// `cdrom-sa-` followed by 64 random alphanumeric characters.
func NewServiceAccountKey() (string, error) {
	b := make([]rune, serviceAccountKeyLength)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(serviceAccountKeyAlphabet))))
		if err != nil {
			return "", fmt.Errorf("idp: generate service account key: %w", err)
		}
		b[i] = serviceAccountKeyAlphabet[n.Int64()]
	}
	return serviceAccountKeyPrefix + string(b), nil
}

// NewServiceAccountKeySalt generates a new per-key salt: 32 random bytes,
// hex-encoded. The salt is unique to the key it is generated for and is mixed
// into the key's hash.
func NewServiceAccountKeySalt() (string, error) {
	b := make([]byte, serviceAccountKeySaltBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("idp: generate service account key salt: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// serviceAccountKeyPrefixOf returns the short, non-secret prefix of a key
// shown in listings so a caller can tell keys apart without the plaintext.
func serviceAccountKeyPrefixOf(key string) string {
	if len(key) < serviceAccountKeyDisplayPrefix {
		return key
	}
	return key[:serviceAccountKeyDisplayPrefix]
}

// HashServiceAccountKey returns the salted hash (hex) of a service-account
// key's plaintext: SHA-512 of pepper + salt + key. The per-key salt makes the
// hash unique to the key (so two keys with the same plaintext never collide);
// the pepper (when configured) is mixed in as added security and is kept out
// of the database. A fast hash is used so per-request verification stays
// cheap; the key is high-entropy.
func HashServiceAccountKey(pepper, salt, key string) string {
	h := sha512.New()
	h.Write([]byte(pepper))
	h.Write([]byte(salt))
	h.Write([]byte(key))
	return hex.EncodeToString(h.Sum(nil))
}

// serviceAccountKeyToProto renders a key slot as its Database-service form.
func serviceAccountKeyToProto(k *ServiceAccountKey) *dbpb.ServiceAccountKey {
	return &dbpb.ServiceAccountKey{
		Slot:      int32(k.Slot),
		KeyHash:   k.KeyHash,
		KeySalt:   k.KeySalt,
		KeyPrefix: k.KeyPrefix,
	}
}

// serviceAccountKeyFromProto reconstructs a key slot from its Database-service
// form.
func serviceAccountKeyFromProto(k *dbpb.ServiceAccountKey) *ServiceAccountKey {
	return &ServiceAccountKey{
		Slot:      int(k.GetSlot()),
		KeyHash:   k.GetKeyHash(),
		KeySalt:   k.GetKeySalt(),
		KeyPrefix: k.GetKeyPrefix(),
	}
}

// serviceAccountToProto renders an account as its Database-service form.
func serviceAccountToProto(a *ServiceAccount) *dbpb.ServiceAccount {
	out := &dbpb.ServiceAccount{
		Id:          a.ID,
		LoginName:   a.LoginName,
		DisplayName: a.DisplayName,
		Description: a.Description,
		Disabled:    a.Disabled,
		CreatedBy:   a.CreatedBy,
		UpdatedBy:   a.UpdatedBy,
		Revision:    int64(a.Revision),
		Roles:       a.Roles,
	}
	if a.Deleted {
		out.DeletedAt = timestamppb.New(time.Now())
	}
	if !a.KeyLockedUntil.IsZero() {
		out.KeyLockedUntil = timestamppb.New(a.KeyLockedUntil)
	}
	for _, k := range a.Keys {
		out.Keys = append(out.Keys, serviceAccountKeyToProto(k))
	}
	return out
}

// serviceAccountFromProto reconstructs an in-memory account from its
// Database-service form.
func serviceAccountFromProto(a *dbpb.ServiceAccount) *ServiceAccount {
	out := &ServiceAccount{
		ID:          a.GetId(),
		LoginName:   a.GetLoginName(),
		DisplayName: a.GetDisplayName(),
		Description: a.GetDescription(),
		Disabled:    a.GetDisabled(),
		CreatedBy:   a.GetCreatedBy(),
		UpdatedBy:   a.GetUpdatedBy(),
		Revision:    int(a.GetRevision()),
		Roles:       a.GetRoles(),
	}
	if a.GetDeletedAt() != nil {
		out.Deleted = true
	}
	if ts := a.GetKeyLockedUntil(); ts != nil {
		out.KeyLockedUntil = ts.AsTime()
	}
	for _, k := range a.GetKeys() {
		out.Keys = append(out.Keys, serviceAccountKeyFromProto(k))
	}
	return out
}

// dbServiceAccountStore is a ServiceAccountStore backed by the Database
// service.
type dbServiceAccountStore struct {
	db dbpb.DatabaseClient
}

// NewDBServiceAccountStore returns a ServiceAccountStore that persists the
// service-account directory through the Database service.
func NewDBServiceAccountStore(db dbpb.DatabaseClient) ServiceAccountStore {
	return &dbServiceAccountStore{db: db}
}

func (s *dbServiceAccountStore) Create(ctx context.Context, acc *ServiceAccount, roles []string) (*ServiceAccount, error) {
	account := &dbpb.ServiceAccount{
		LoginName:   acc.LoginName,
		DisplayName: acc.DisplayName,
		Description: acc.Description,
		CreatedBy:   acc.CreatedBy,
	}
	keys := make([]*dbpb.ServiceAccountKey, 0, len(acc.Keys))
	for _, k := range acc.Keys {
		keys = append(keys, serviceAccountKeyToProto(k))
	}
	resp, err := s.db.CreateServiceAccount(ctx, &dbpb.CreateServiceAccountRequest{
		Account: account,
		Keys:    keys,
		Roles:   roles,
	})
	if err != nil {
		return nil, err
	}
	return serviceAccountFromProto(resp), nil
}

func (s *dbServiceAccountStore) Get(ctx context.Context, id, loginName string, includeDeleted bool) (*ServiceAccount, error) {
	resp, err := s.db.GetServiceAccount(ctx, &dbpb.GetServiceAccountRequest{
		Id:             id,
		LoginName:      loginName,
		IncludeDeleted: includeDeleted,
	})
	if err != nil {
		return nil, err
	}
	return serviceAccountFromProto(resp), nil
}

func (s *dbServiceAccountStore) List(ctx context.Context, includeDeleted bool) ([]*ServiceAccount, error) {
	resp, err := s.db.ListServiceAccounts(ctx, &dbpb.ListServiceAccountsRequest{IncludeDeleted: includeDeleted})
	if err != nil {
		return nil, err
	}
	out := make([]*ServiceAccount, 0, len(resp.GetAccounts()))
	for _, a := range resp.GetAccounts() {
		out = append(out, serviceAccountFromProto(a))
	}
	return out, nil
}

func (s *dbServiceAccountStore) Update(ctx context.Context, id, displayName, description string, hasDisplayName, hasDescription bool, revision int64, updatedBy string) (*ServiceAccount, error) {
	resp, err := s.db.UpdateServiceAccount(ctx, &dbpb.UpdateServiceAccountRequest{
		Account:        &dbpb.ServiceAccount{Id: id, DisplayName: displayName, Description: description},
		HasDisplayName: hasDisplayName,
		HasDescription: hasDescription,
		Revision:       revision,
		UpdatedBy:      updatedBy,
	})
	if err != nil {
		return nil, err
	}
	return serviceAccountFromProto(resp), nil
}

func (s *dbServiceAccountStore) RotateKey(ctx context.Context, id string, slot int, keyHash, keySalt, keyPrefix string, revision int64, updatedBy string) (*ServiceAccount, error) {
	resp, err := s.db.RotateServiceAccountKey(ctx, &dbpb.RotateServiceAccountKeyRequest{
		ServiceAccountId: id,
		Slot:             int32(slot),
		KeyHash:          keyHash,
		KeySalt:          keySalt,
		KeyPrefix:        keyPrefix,
		Revision:         revision,
		UpdatedBy:        updatedBy,
	})
	if err != nil {
		return nil, err
	}
	return serviceAccountFromProto(resp), nil
}

func (s *dbServiceAccountStore) SetDisabled(ctx context.Context, id string, disabled bool, revision int64, updatedBy string) (*ServiceAccount, error) {
	req := &dbpb.ServiceAccountStateRequest{Id: id, Revision: revision, UpdatedBy: updatedBy}
	var resp *dbpb.ServiceAccount
	var err error
	if disabled {
		resp, err = s.db.DisableServiceAccount(ctx, req)
	} else {
		resp, err = s.db.EnableServiceAccount(ctx, req)
	}
	if err != nil {
		return nil, err
	}
	return serviceAccountFromProto(resp), nil
}

func (s *dbServiceAccountStore) Delete(ctx context.Context, id, updatedBy string) (*ServiceAccount, error) {
	resp, err := s.db.DeleteServiceAccount(ctx, &dbpb.DeleteServiceAccountRequest{Id: id, UpdatedBy: updatedBy})
	if err != nil {
		return nil, err
	}
	return serviceAccountFromProto(resp), nil
}

func (s *dbServiceAccountStore) AssignRole(ctx context.Context, id, role string) (*ServiceAccount, error) {
	if _, err := s.db.AddRoleBinding(ctx, &dbpb.AddRoleBindingRequest{
		PrincipalKind: "service-account",
		PrincipalId:   id,
		RoleName:      role,
	}); err != nil {
		return nil, err
	}
	return s.Get(ctx, id, "", false)
}

func (s *dbServiceAccountStore) RemoveRole(ctx context.Context, id, role string) (*ServiceAccount, error) {
	resp, err := s.db.ListRoleBindings(ctx, &dbpb.ListRoleBindingsRequest{
		PrincipalKind: "service-account",
		PrincipalId:   id,
		RoleName:      role,
	})
	if err != nil {
		return nil, err
	}
	for _, b := range resp.GetBindings() {
		if _, err := s.db.DeleteRoleBinding(ctx, &dbpb.DeleteRoleBindingRequest{Id: b.GetId()}); err != nil {
			return nil, err
		}
	}
	return s.Get(ctx, id, "", false)
}

func (s *dbServiceAccountStore) RecordKeyAttempt(ctx context.Context, id string, success bool, maxFailures int, lockoutDuration time.Duration) (*ServiceAccount, error) {
	resp, err := s.db.RecordServiceAccountKeyAttempt(ctx, &dbpb.RecordServiceAccountKeyAttemptRequest{
		Id:              id,
		Success:         success,
		MaxFailures:     int32(maxFailures),
		LockoutDuration: durationpb.New(lockoutDuration),
	})
	if err != nil {
		return nil, err
	}
	return serviceAccountFromProto(resp), nil
}

func (s *dbServiceAccountStore) ResetLockout(ctx context.Context, id string) error {
	_, err := s.db.ResetServiceAccountLockout(ctx, &dbpb.ResetServiceAccountLockoutRequest{Id: id})
	return err
}

// verifyServiceAccountKey checks a presented `login-name:cdrom-sa-…`
// credential against the account's two key slots. It fetches the account (with
// its slots' salted hashes and salts), rejects a deleted or disabled account
// and one that is locked out, and compares the presented key's salted hash
// (computed per slot, mixing in the configured pepper) against each slot's
// stored hash in constant time. On a match it resets the account's failure
// counter and returns the matched slot; on a miss it records the failed
// attempt (which may lock the account out) and returns Unauthenticated.
func verifyServiceAccountKey(ctx context.Context, store ServiceAccountStore, loginName, apiKey, pepper string, maxFailures int, lockoutDuration time.Duration) (*ServiceAccount, int, error) {
	acc, err := store.Get(ctx, "", loginName, false)
	if err != nil {
		// Unknown account: fail with the same response as a bad key so the
		// endpoint does not reveal which login names exist.
		return nil, 0, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	// While locked, every key verification for the account fails (the counter
	// is not incremented again).
	if !acc.KeyLockedUntil.IsZero() && acc.KeyLockedUntil.After(time.Now()) {
		return nil, 0, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	// A disabled or deleted account rejects both keys.
	if acc.Disabled || acc.Deleted {
		return nil, 0, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	// Compare the presented key's salted hash against each slot's stored hash
	// in constant time. The salt is unique to the key, so the hash is unique
	// to the (pepper, salt, key) triple.
	for _, k := range acc.Keys {
		if k.KeyHash == "" {
			continue // a zeroed slot (after delete) never matches
		}
		candidate := HashServiceAccountKey(pepper, k.KeySalt, apiKey)
		if subtle.ConstantTimeCompare([]byte(candidate), []byte(k.KeyHash)) == 1 {
			if _, err := store.RecordKeyAttempt(ctx, acc.ID, true, maxFailures, lockoutDuration); err != nil {
				return nil, 0, err
			}
			return acc, k.Slot, nil
		}
	}
	// A miss: record the failed attempt (which may lock the account out).
	if _, err := store.RecordKeyAttempt(ctx, acc.ID, false, maxFailures, lockoutDuration); err != nil {
		return nil, 0, err
	}
	return nil, 0, status.Error(codes.Unauthenticated, "invalid credentials")
}
