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
	mu     sync.Mutex
	byID   map[string]*User
	byMail map[string]*User // keyed by lower-cased email
}

// NewMemoryUserStore returns an empty in-memory UserStore.
func NewMemoryUserStore() *MemoryUserStore {
	return &MemoryUserStore{byID: make(map[string]*User), byMail: make(map[string]*User)}
}

// Create registers (or updates) a user, keyed by email. A new user is given a
// stable random ID; an existing user's profile and roles are updated.
func (s *MemoryUserStore) Create(_ context.Context, user *User) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.ToLower(user.Email)
	if existing, ok := s.byMail[key]; ok {
		existing.FirstName = user.FirstName
		existing.LastName = user.LastName
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
	stored := copyUser(user)
	s.byID[stored.ID] = stored
	s.byMail[key] = stored
	return stored, nil
}

// GetByEmail returns the user with the given email (case-insensitive). A
// missing user returns a NotFound error.
func (s *MemoryUserStore) GetByEmail(_ context.Context, email string) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byMail[strings.ToLower(email)]
	if !ok {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	return copyUser(u), nil
}

// GetByID returns the user with the given ID. A missing user returns a
// NotFound error.
func (s *MemoryUserStore) GetByID(_ context.Context, id string) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byID[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	return copyUser(u), nil
}

// List returns all registered users, ordered by ID.
func (s *MemoryUserStore) List(_ context.Context) ([]*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	users := make([]*User, 0, len(s.byID))
	for _, u := range s.byID {
		users = append(users, copyUser(u))
	}
	sort.Slice(users, func(i, j int) bool { return users[i].ID < users[j].ID })
	return users, nil
}

// Update updates a user's profile and/or roles, keyed by ID. A missing user
// returns a NotFound error.
func (s *MemoryUserStore) Update(_ context.Context, user *User) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.byID[user.ID]
	if !ok {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	if user.FirstName != "" {
		existing.FirstName = user.FirstName
	}
	if user.LastName != "" {
		existing.LastName = user.LastName
	}
	if user.Email != "" {
		delete(s.byMail, strings.ToLower(existing.Email))
		existing.Email = user.Email
		s.byMail[strings.ToLower(existing.Email)] = existing
	}
	if len(user.Roles) > 0 {
		existing.Roles = user.Roles
	}
	if user.PasswordHash != "" {
		existing.PasswordHash = user.PasswordHash
	}
	return copyUser(existing), nil
}

// Delete removes a user by ID. A missing user returns a NotFound error.
func (s *MemoryUserStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.byID[id]
	if !ok {
		return status.Error(codes.NotFound, "user not found")
	}
	delete(s.byID, id)
	delete(s.byMail, strings.ToLower(u.Email))
	return nil
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

// copyUser returns a shallow copy of user (with a copied Roles slice) so
// callers cannot mutate the store's state.
func copyUser(u *User) *User {
	out := *u
	if u.Roles != nil {
		out.Roles = append([]string(nil), u.Roles...)
	}
	return &out
}

// MemoryAPIKeyStore is an in-memory APIKeyStore. It is used by tests and is
// suitable for running a single IdP without a Database service; it does not
// share state across processes, so it does not support multiple IdP replicas.
// It holds a UserStore (users) so it can resolve a key's owner by email.
type MemoryAPIKeyStore struct {
	mu    sync.Mutex
	keys  map[string]*APIKey
	byKey map[string]*APIKey // keyed by key hash
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
		byKey:   make(map[string]*APIKey),
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

// storeKey indexes a key by its id and its hash. The caller must hold s.mu.
func (s *MemoryAPIKeyStore) storeKey(k *APIKey) {
	s.keys[k.ID] = k
	s.byKey[k.KeyHash] = k
}

// removeKey unindexes a key by its id and hash. The caller must hold s.mu.
func (s *MemoryAPIKeyStore) removeKey(k *APIKey) {
	delete(s.keys, k.ID)
	delete(s.byKey, k.KeyHash)
}

func (s *MemoryAPIKeyStore) Create(ctx context.Context, ownerEmail, description string, expiresAt time.Time, pipelineScope []uint, pepper string) (*APIKey, string, error) {
	if !expiresAt.IsZero() && expiresAt.After(time.Now().Add(maxAPIKeyLifetime)) {
		return nil, "", status.Error(codes.InvalidArgument, "expiration is more than one year in the future")
	}
	owner, err := s.users.GetByEmail(ctx, ownerEmail)
	if err != nil {
		return nil, "", err
	}
	plaintext, err := NewAPIKey()
	if err != nil {
		return nil, "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := &APIKey{
		ID:            s.nextKeyID(),
		OwnerID:       owner.ID,
		Description:   description,
		KeyHash:       HashAPIKey(pepper, plaintext),
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
	delete(s.byKey, k.KeyHash)
	k.KeyHash = HashAPIKey(pepper, plaintext)
	k.KeyPrefix = apiDisplayPrefix(plaintext)
	s.byKey[k.KeyHash] = k
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
	owner, err := s.users.GetByEmail(ctx, username)
	if err != nil {
		return nil, nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lo := s.lockout[owner.ID]
	if lo != nil && !lo.lockedUntil.IsZero() && lo.lockedUntil.After(time.Now()) {
		return nil, nil, status.Error(codes.Unauthenticated, "api key access is locked")
	}
	hash := HashAPIKey(pepper, apiKey)
	now := time.Now()
	for _, k := range s.byKey {
		if k.OwnerID != owner.ID || k.KeyHash != hash {
			continue
		}
		if !k.ExpiresAt.IsZero() && k.ExpiresAt.Before(now) {
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
	if _, err := s.users.GetByID(ctx, userID); err != nil {
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
