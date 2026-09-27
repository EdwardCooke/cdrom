package idp

import (
	"context"
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
