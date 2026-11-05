package idp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MemoryKeyStore is an in-memory KeyStore. It is used by tests and is suitable
// for running a single IdP without a Database service; it does not share state
// across processes, so it does not support multiple IdP replicas.
type MemoryKeyStore struct {
	mu   sync.Mutex
	keys []*signingKey
}

// NewMemoryKeyStore returns an empty in-memory KeyStore.
func NewMemoryKeyStore() *MemoryKeyStore {
	return &MemoryKeyStore{}
}

// ListKeys returns the persisted keyring.
func (s *MemoryKeyStore) ListKeys(_ context.Context) ([]*signingKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*signingKey, len(s.keys))
	copy(out, s.keys)
	return out, nil
}

// SetKeys atomically replaces the persisted keyring.
func (s *MemoryKeyStore) SetKeys(_ context.Context, keys []*signingKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = keys
	return nil
}

// MemoryAuthCodeStore is an in-memory AuthCodeStore. It is used by tests and
// is suitable for running a single IdP without a Database service; it does not
// share state across processes, so it does not support multiple IdP replicas.
type MemoryAuthCodeStore struct {
	mu    sync.Mutex
	codes map[string]*authCode
}

// NewMemoryAuthCodeStore returns an empty in-memory AuthCodeStore.
func NewMemoryAuthCodeStore() *MemoryAuthCodeStore {
	return &MemoryAuthCodeStore{codes: make(map[string]*authCode)}
}

// Store persists a newly minted authorization code.
func (s *MemoryAuthCodeStore) Store(_ context.Context, code *authCode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.codes[code.code] = code
	return nil
}

// Consume atomically fetches and deletes a code. A missing code returns an
// error with status.Code == codes.NotFound.
func (s *MemoryAuthCodeStore) Consume(_ context.Context, code string) (*authCode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ac, ok := s.codes[code]
	if !ok {
		return nil, status.Error(codes.NotFound, "authorization code not found")
	}
	delete(s.codes, code)
	return ac, nil
}

// Prune drops codes created before the given instant.
func (s *MemoryAuthCodeStore) Prune(_ context.Context, before time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for code, ac := range s.codes {
		if ac.createdAt.Before(before) {
			delete(s.codes, code)
		}
	}
	return nil
}

// MemoryUserStore is an in-memory UserStore. It is used by tests and is
// suitable for running a single IdP without a Database service; it does not
// share state across processes, so it does not support multiple IdP replicas.
type MemoryUserStore struct {
	mu      sync.Mutex
	byID    map[string]*User
	byUser  map[string]*User // keyed by lower-cased username
	byExtID map[string]*User // keyed by external ID (exact)
}

// NewMemoryUserStore returns an empty in-memory UserStore.
func NewMemoryUserStore() *MemoryUserStore {
	return &MemoryUserStore{
		byID:    make(map[string]*User),
		byUser:  make(map[string]*User),
		byExtID: make(map[string]*User),
	}
}

// Create registers (or updates) a user, keyed by USERNAME (the unique login
// identifier, F-28.4). A new user is given a stable random ID; an existing
// user's profile and roles are updated.
func (s *MemoryUserStore) Create(_ context.Context, user *User) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if user.Username == "" {
		return nil, status.Error(codes.InvalidArgument, "username is required")
	}
	key := strings.ToLower(user.Username)
	if existing, ok := s.byUser[key]; ok {
		existing.FirstName = user.FirstName
		existing.LastName = user.LastName
		existing.Email = user.Email
		existing.Roles = user.Roles
		if user.PasswordHash != "" {
			existing.PasswordHash = user.PasswordHash
		}
		return copyUser(existing), nil
	}
	if user.ID == "" {
		id, err := newUserID()
		if err != nil {
			return nil, err
		}
		user.ID = id
	}
	if user.CreatedAt.IsZero() {
		user.CreatedAt = time.Now()
	}
	// Born active and non-tombstoned (F-28.3): the tombstone and deactivation
	// belong solely to the lifecycle verbs.
	user.Active = true
	user.DeletedAt = nil
	stored := copyUser(user)
	s.index(stored)
	return stored, nil
}

// index adds/removes a user in the auxiliary indices. Caller holds s.mu.
func (s *MemoryUserStore) index(u *User) {
	s.byID[u.ID] = u
	s.byUser[strings.ToLower(u.Username)] = u
	if u.ExternalID != "" {
		s.byExtID[u.ExternalID] = u
	}
}

// GetByUsername returns the user with the given username (case-insensitive).
// A missing user — or a tombstoned one when includeDeleted is false —
// returns a NotFound error.
func (s *MemoryUserStore) GetByUsername(_ context.Context, username string, includeDeleted bool) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byUser[strings.ToLower(username)]
	if !ok || (!includeDeleted && u.DeletedAt != nil) {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	return copyUser(u), nil
}

// FindByExternalID returns the user with the given corporate external ID
// (exact match). A missing user — or a tombstoned one when includeDeleted is
// false — returns a NotFound error.
func (s *MemoryUserStore) FindByExternalID(_ context.Context, externalID string, includeDeleted bool) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byExtID[externalID]
	if !ok || (!includeDeleted && u.DeletedAt != nil) {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	return copyUser(u), nil
}

// GetByID returns the user with the given ID. A missing user — or a
// tombstoned one when includeDeleted is false — returns a NotFound error.
func (s *MemoryUserStore) GetByID(_ context.Context, id string, includeDeleted bool) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byID[id]
	if !ok || (!includeDeleted && u.DeletedAt != nil) {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	return copyUser(u), nil
}

// List returns registered users, ordered by ID. When includeDeleted is false
// (the default) tombstoned users are excluded (F-28.3).
func (s *MemoryUserStore) List(_ context.Context, includeDeleted bool) ([]*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	users := make([]*User, 0, len(s.byID))
	for _, u := range s.byID {
		if !includeDeleted && u.DeletedAt != nil {
			continue
		}
		users = append(users, copyUser(u))
	}
	sort.Slice(users, func(i, j int) bool { return users[i].ID < users[j].ID })
	return users, nil
}

// Update updates a user's profile and/or roles, keyed by ID. A missing user
// — or a tombstoned one (revival is exclusively Reactivate's job) — returns
// a NotFound error.
func (s *MemoryUserStore) Update(_ context.Context, user *User) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.byID[user.ID]
	if !ok || existing.DeletedAt != nil {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	if user.FirstName != "" {
		existing.FirstName = user.FirstName
	}
	if user.LastName != "" {
		existing.LastName = user.LastName
	}
	if user.Email != "" {
		existing.Email = user.Email
	}
	// Username is mutable-but-UNIQUE (F-28.4): a rename onto another user's
	// username is AlreadyExists.
	if nu := user.Username; nu != "" && !strings.EqualFold(nu, existing.Username) {
		if other, taken := s.byUser[strings.ToLower(nu)]; taken && other.ID != existing.ID {
			return nil, status.Error(codes.AlreadyExists, "a user with that username already exists")
		}
		existing.Username = nu
		s.index(existing)
	}
	if len(user.Roles) > 0 {
		existing.Roles = user.Roles
	}
	if user.PasswordHash != "" {
		existing.PasswordHash = user.PasswordHash
	}
	return copyUser(existing), nil
}

// SoftDelete deprovisions a user by ID (F-28.3): it sets the tombstone,
// marks the user inactive, and zeroes the password hash so no leftover
// credential can revive the record. The row (identity, roles, history) is
// retained. A missing user returns a NotFound error; repeating the delete is
// idempotent.
func (s *MemoryUserStore) SoftDelete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byID[id]
	if !ok {
		return status.Error(codes.NotFound, "user not found")
	}
	if u.DeletedAt == nil {
		now := time.Now()
		u.DeletedAt = &now
		u.Active = false
		u.PasswordHash = ""
	}
	return nil
}

// setState applies a lifecycle transition to a user by ID (missing user is
// NotFound).
func (s *MemoryUserStore) setState(id string, prepare func(*User)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byID[id]
	if !ok {
		return status.Error(codes.NotFound, "user not found")
	}
	prepare(u)
	return nil
}

// Activate activates a deactivated user (active=true), restoring
// authentication eligibility; credentials are untouched (F-28.3). Idempotent.
func (s *MemoryUserStore) Activate(ctx context.Context, id string) error {
	return s.setState(id, func(u *User) { u.Active = true })
}

// Deactivate deactivates an active user ("on leave"): active=false, the row
// and its credentials are retained (F-28.3). Idempotent.
func (s *MemoryUserStore) Deactivate(ctx context.Context, id string) error {
	return s.setState(id, func(u *User) { u.Active = false })
}

// Reactivate clears a deprovisioned user's tombstone (rehire): active=true,
// the password hash is zeroed (fresh credentials must be reinstated by the
// rightful source), and the same stable ID and all history are preserved
// (F-28.3). Reactivating a non-tombstoned user is idempotent (it merely
// activates).
func (s *MemoryUserStore) Reactivate(ctx context.Context, id string) error {
	return s.setState(id, func(u *User) {
		u.DeletedAt = nil
		u.Active = true
		u.PasswordHash = ""
	})
}

// ExpireTemporaryPassword zeros a user's TEMPORAL (short-lived) credential
// (F-28.3 break-glass hygiene): the hash and its validity window are cleared
// without touching the user's active state or standing password. A no-op when
// the user has no temporal credential.
func (s *MemoryUserStore) ExpireTemporaryPassword(ctx context.Context, id string) error {
	return s.setState(id, func(u *User) {
		u.TempPasswordHash = ""
		u.TempPasswordValidUntil = nil
	})
}

// newUserID returns a random 16-hex-character user identifier (the token's
// subject).
func newUserID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("idp: generate user id: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// copyUser returns a copy of user (with a copied Roles slice and cloned
// lifecycle-pointer fields) so callers cannot mutate the store's state.
func copyUser(u *User) *User {
	out := *u
	if u.Roles != nil {
		out.Roles = append([]string(nil), u.Roles...)
	}
	if u.DeletedAt != nil {
		t := *u.DeletedAt
		out.DeletedAt = &t
	}
	if u.TempPasswordValidUntil != nil {
		t := *u.TempPasswordValidUntil
		out.TempPasswordValidUntil = &t
	}
	return &out
}

// MemoryAPIKeyStore is an in-memory APIKeyStore. It is used by tests and is
// suitable for running a single IdP without a Database service; it does not
// share state across processes, so it does not support multiple IdP replicas.
// It holds a UserStore (users) so it can resolve a key's owner by username.
type MemoryAPIKeyStore struct {
	mu   sync.Mutex
	keys map[string]*APIKey
	// lockout tracks a user's API-key lockout state, keyed by user ID.
	lockout map[string]*apiKeyLockout
	users   UserStore
}

// apiKeyLockout is a user's API-key lockout state.
type apiKeyLockout struct {
	failedCount int
	lockedUntil time.Time
}

// NewMemoryAPIKeyStore returns an empty in-memory APIKeyStore that resolves
// key owners through users.
func NewMemoryAPIKeyStore(users UserStore) *MemoryAPIKeyStore {
	return &MemoryAPIKeyStore{
		keys:    make(map[string]*APIKey),
		lockout: make(map[string]*apiKeyLockout),
		users:   users,
	}
}

// nextKeyID returns the next key id (a 1-based counter over the store's keys).
func (s *MemoryAPIKeyStore) nextKeyID() string {
	max := 0
	for id := range s.keys {
		if n, err := strconv.Atoi(id); err == nil && n > max {
			max = n
		}
	}
	return strconv.Itoa(max + 1)
}

// storeKey indexes a key by its id. The caller must hold s.mu.
func (s *MemoryAPIKeyStore) storeKey(k *APIKey) {
	s.keys[k.ID] = k
}

// removeKey unindexes a key by its id. The caller must hold s.mu.
func (s *MemoryAPIKeyStore) removeKey(k *APIKey) {
	delete(s.keys, k.ID)
}

func (s *MemoryAPIKeyStore) Create(ctx context.Context, ownerUsername, description string, expiresAt time.Time, pipelineScope []uint, pepper string) (*APIKey, string, error) {
	if !expiresAt.IsZero() && expiresAt.After(time.Now().Add(maxAPIKeyLifetime)) {
		return nil, "", status.Error(codes.InvalidArgument, "expiration is more than one year in the future")
	}
	owner, err := s.users.GetByUsername(ctx, ownerUsername, true)
	if err != nil {
		return nil, "", err
	}
	plaintext, err := NewAPIKey()
	if err != nil {
		return nil, "", err
	}
	keyHash, err := EncodeAPIKey(pepper, plaintext)
	if err != nil {
		return nil, "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := &APIKey{
		ID:            s.nextKeyID(),
		OwnerID:       owner.ID,
		Description:   description,
		KeyHash:       keyHash,
		KeyPrefix:     apiDisplayPrefix(plaintext),
		ExpiresAt:     expiresAt,
		PipelineScope: pipelineScope,
		CreatedAt:     time.Now(),
	}
	s.storeKey(key)
	return copyAPIKey(key), plaintext, nil
}

func (s *MemoryAPIKeyStore) Get(_ context.Context, id string) (*APIKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.keys[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "api key not found")
	}
	return copyAPIKey(k), nil
}

func (s *MemoryAPIKeyStore) List(_ context.Context, ownerID string) ([]*APIKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*APIKey, 0, len(s.keys))
	for _, k := range s.keys {
		if ownerID != "" && k.OwnerID != ownerID {
			continue
		}
		out = append(out, copyAPIKey(k))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *MemoryAPIKeyStore) Update(_ context.Context, id, description string, hasDescription bool, expiresAt time.Time, hasExpiresAt bool, hasScope bool, pipelineScope []uint) (*APIKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.keys[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "api key not found")
	}
	if hasDescription {
		k.Description = description
	}
	if hasExpiresAt {
		k.ExpiresAt = expiresAt
	}
	if hasScope {
		k.PipelineScope = pipelineScope
	}
	return copyAPIKey(k), nil
}

func (s *MemoryAPIKeyStore) Rotate(_ context.Context, id, pepper string) (*APIKey, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.keys[id]
	if !ok {
		return nil, "", status.Error(codes.NotFound, "api key not found")
	}
	plaintext, err := NewAPIKey()
	if err != nil {
		return nil, "", err
	}
	keyHash, err := EncodeAPIKey(pepper, plaintext)
	if err != nil {
		return nil, "", err
	}
	k.KeyHash = keyHash
	k.KeyPrefix = apiDisplayPrefix(plaintext)
	return copyAPIKey(k), plaintext, nil
}

func (s *MemoryAPIKeyStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.keys[id]
	if !ok {
		return status.Error(codes.NotFound, "api key not found")
	}
	s.removeKey(k)
	return nil
}

func (s *MemoryAPIKeyStore) Verify(ctx context.Context, username, apiKey, pepper string, maxFailures int, lockoutDuration time.Duration) (*APIKey, *User, error) {
	owner, err := s.users.GetByUsername(ctx, username, true)
	if err != nil {
		return nil, nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lo := s.lockout[owner.ID]
	if lo != nil && !lo.lockedUntil.IsZero() && lo.lockedUntil.After(time.Now()) {
		return nil, nil, status.Error(codes.Unauthenticated, "api key access is locked")
	}
	now := time.Now()
	for _, k := range s.keys {
		if k.OwnerID != owner.ID || k.KeyHash == "" {
			continue
		}
		if !k.ExpiresAt.IsZero() && k.ExpiresAt.Before(now) {
			continue
		}
		if !VerifySecret(k.KeyHash, pepper, apiKey) {
			continue
		}
		if lo != nil {
			lo.failedCount = 0
		}
		return copyAPIKey(k), copyUser(owner), nil
	}
	// A miss: increment the owner's failure counter and, at the configured
	// maximum, lock the owner out (for the configured duration, or
	// permanently when it is zero) and reset the counter.
	if maxFailures > 0 {
		if lo == nil {
			lo = &apiKeyLockout{}
			s.lockout[owner.ID] = lo
		}
		lo.failedCount++
		if lo.failedCount >= maxFailures {
			if lockoutDuration > 0 {
				lo.lockedUntil = now.Add(lockoutDuration)
			} else {
				lo.lockedUntil = apiKeyLockoutForever
			}
			lo.failedCount = 0
		}
	}
	return nil, nil, status.Error(codes.Unauthenticated, "invalid credentials")
}

func (s *MemoryAPIKeyStore) ResetLockout(ctx context.Context, userID string) error {
	if _, err := s.users.GetByID(ctx, userID, true); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.lockout, userID)
	return nil
}

// copyAPIKey returns a copy of key (with a copied PipelineScope slice) so
// callers cannot mutate the store's state.
func copyAPIKey(k *APIKey) *APIKey {
	out := *k
	if k.PipelineScope != nil {
		out.PipelineScope = append([]uint(nil), k.PipelineScope...)
	}
	return &out
}

// MemoryServiceAccountStore is an in-memory ServiceAccountStore. It is used by
// tests and is suitable for running a single IdP without a Database service;
// it does not share state across processes, so it does not support multiple
// IdP replicas.
type MemoryServiceAccountStore struct {
	mu       sync.Mutex
	accounts map[string]*ServiceAccount // keyed by account id
	byLogin  map[string]*ServiceAccount // keyed by login name
	nextID   int
}

// NewMemoryServiceAccountStore returns an empty in-memory ServiceAccountStore.
func NewMemoryServiceAccountStore() *MemoryServiceAccountStore {
	return &MemoryServiceAccountStore{
		accounts: make(map[string]*ServiceAccount),
		byLogin:  make(map[string]*ServiceAccount),
		nextID:   1,
	}
}

// copyServiceAccount returns a deep copy of an account (with copied keys and
// roles) so callers cannot mutate the store's state.
func copyServiceAccount(a *ServiceAccount) *ServiceAccount {
	out := *a
	out.Roles = append([]string(nil), a.Roles...)
	out.Keys = make([]*ServiceAccountKey, len(a.Keys))
	for i, k := range a.Keys {
		ck := *k
		out.Keys[i] = &ck
	}
	return &out
}

func (s *MemoryServiceAccountStore) Create(_ context.Context, acc *ServiceAccount, roles []string) (*ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byLogin[acc.LoginName]; ok {
		return nil, status.Error(codes.AlreadyExists, "a service account with that login name already exists")
	}
	id := strconv.Itoa(s.nextID)
	s.nextID++
	stored := copyServiceAccount(acc)
	stored.ID = id
	stored.Roles = append([]string(nil), roles...)
	s.accounts[id] = stored
	s.byLogin[acc.LoginName] = stored
	return copyServiceAccount(stored), nil
}

func (s *MemoryServiceAccountStore) Get(_ context.Context, id, loginName string, includeDeleted bool) (*ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var acc *ServiceAccount
	if id != "" {
		acc = s.accounts[id]
	} else {
		acc = s.byLogin[loginName]
	}
	if acc == nil || (!includeDeleted && acc.Deleted) {
		return nil, status.Error(codes.NotFound, "service account not found")
	}
	return copyServiceAccount(acc), nil
}

func (s *MemoryServiceAccountStore) List(_ context.Context, includeDeleted bool) ([]*ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*ServiceAccount, 0, len(s.accounts))
	for _, a := range s.accounts {
		if !includeDeleted && a.Deleted {
			continue
		}
		out = append(out, copyServiceAccount(a))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *MemoryServiceAccountStore) Update(_ context.Context, id, displayName, description string, hasDisplayName, hasDescription bool, revision int64, _ string) (*ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[id]
	if !ok || acc.Deleted {
		return nil, status.Error(codes.NotFound, "service account not found")
	}
	if int64(acc.Revision) != revision {
		return nil, status.Error(codes.Aborted, "revision mismatch: the account was modified concurrently")
	}
	if hasDisplayName {
		acc.DisplayName = displayName
	}
	if hasDescription {
		acc.Description = description
	}
	acc.Revision++
	return copyServiceAccount(acc), nil
}

func (s *MemoryServiceAccountStore) RotateKey(_ context.Context, id string, slot int, keyHash, keyPrefix string, revision int64, _ string) (*ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[id]
	if !ok || acc.Deleted {
		return nil, status.Error(codes.NotFound, "service account not found")
	}
	if int64(acc.Revision) != revision {
		return nil, status.Error(codes.Aborted, "revision mismatch: the account was modified concurrently")
	}
	for _, k := range acc.Keys {
		if k.Slot == slot {
			k.KeyHash = keyHash
			k.KeyPrefix = keyPrefix
			break
		}
	}
	acc.Revision++
	return copyServiceAccount(acc), nil
}

func (s *MemoryServiceAccountStore) SetDisabled(_ context.Context, id string, disabled bool, revision int64, _ string) (*ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[id]
	if !ok || acc.Deleted {
		return nil, status.Error(codes.NotFound, "service account not found")
	}
	if int64(acc.Revision) != revision {
		return nil, status.Error(codes.Aborted, "revision mismatch: the account was modified concurrently")
	}
	acc.Disabled = disabled
	acc.Revision++
	return copyServiceAccount(acc), nil
}

func (s *MemoryServiceAccountStore) Delete(_ context.Context, id, _ string) (*ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "service account not found")
	}
	if !acc.Deleted {
		acc.Deleted = true
		acc.Disabled = true
		acc.Revision++
		// Zero out both key slots: clear the credential-bearing fields so no
		// usable verifier remains.
		for _, k := range acc.Keys {
			k.KeyHash = ""
			k.KeyPrefix = ""
		}
	}
	return copyServiceAccount(acc), nil
}

func (s *MemoryServiceAccountStore) AssignRole(_ context.Context, id, role string) (*ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[id]
	if !ok || acc.Deleted {
		return nil, status.Error(codes.NotFound, "service account not found")
	}
	for _, r := range acc.Roles {
		if r == role {
			return copyServiceAccount(acc), nil // idempotent: already bound
		}
	}
	acc.Roles = append(acc.Roles, role)
	return copyServiceAccount(acc), nil
}

func (s *MemoryServiceAccountStore) RemoveRole(_ context.Context, id, role string) (*ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[id]
	if !ok || acc.Deleted {
		return nil, status.Error(codes.NotFound, "service account not found")
	}
	out := make([]string, 0, len(acc.Roles))
	for _, r := range acc.Roles {
		if r != role {
			out = append(out, r)
		}
	}
	acc.Roles = out
	return copyServiceAccount(acc), nil
}

func (s *MemoryServiceAccountStore) RecordKeyAttempt(_ context.Context, id string, success bool, maxFailures int, lockoutDuration time.Duration) (*ServiceAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "service account not found")
	}
	if success {
		acc.KeyFailedCount = 0
		return copyServiceAccount(acc), nil
	}
	if maxFailures > 0 {
		acc.KeyFailedCount++
		if acc.KeyFailedCount >= maxFailures {
			if lockoutDuration > 0 {
				acc.KeyLockedUntil = time.Now().Add(lockoutDuration)
			} else {
				acc.KeyLockedUntil = apiKeyLockoutForever
			}
			acc.KeyFailedCount = 0
		}
	}
	return copyServiceAccount(acc), nil
}

func (s *MemoryServiceAccountStore) ResetLockout(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[id]
	if !ok {
		return status.Error(codes.NotFound, "service account not found")
	}
	acc.KeyFailedCount = 0
	acc.KeyLockedUntil = time.Time{}
	return nil
}
