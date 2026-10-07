package idp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
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
