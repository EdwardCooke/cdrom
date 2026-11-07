package idp

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MemoryAgentCredentialStore is an in-memory AgentCredentialStore for tests.
// Transitions mirror the Database service's semantics (backward renew and
// double-retire are rejected) so the ops layer is tested honestly.
type MemoryAgentCredentialStore struct {
	mu    sync.Mutex
	rows  map[string]*AgentCredentialRow
	order []string
}

func NewMemoryAgentCredentialStore() *MemoryAgentCredentialStore {
	return &MemoryAgentCredentialStore{rows: map[string]*AgentCredentialRow{}}
}

func cloneRow(r *AgentCredentialRow) *AgentCredentialRow { cp := *r; return &cp }

func (s *MemoryAgentCredentialStore) Create(_ context.Context, row *AgentCredentialRow) (*AgentCredentialRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[row.ID]; ok {
		return nil, status.Error(codes.AlreadyExists, "credential id already exists: "+row.ID)
	}
	stored := cloneRow(row)
	now := time.Now().UTC()
	stored.CreatedAt, stored.UpdatedAt = now, now
	s.rows[stored.ID] = stored
	s.order = append(s.order, stored.ID)
	return cloneRow(stored), nil
}

func (s *MemoryAgentCredentialStore) Get(_ context.Context, id string) (*AgentCredentialRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "agent credential not found: "+id)
	}
	return cloneRow(r), nil
}

func (s *MemoryAgentCredentialStore) List(_ context.Context, agentName string) ([]*AgentCredentialRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*AgentCredentialRow, 0)
	for _, id := range s.order {
		r := s.rows[id]
		if agentName == "" || r.AgentName == agentName {
			out = append(out, cloneRow(r))
		}
	}
	return out, nil
}

func (s *MemoryAgentCredentialStore) ExtendExpiration(_ context.Context, id string, expiresAt time.Time) (*AgentCredentialRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "agent credential not found: "+id)
	}
	if r.RevokedAt != nil || r.ReplacedByID != "" {
		return nil, status.Error(codes.FailedPrecondition, "cannot renew a retired or revoked credential")
	}
	if !r.ExpiresAt.IsZero() && !expiresAt.After(r.ExpiresAt) {
		return nil, status.Error(codes.InvalidArgument, "renew must extend the deadline forward")
	}
	r.ExpiresAt = expiresAt
	r.UpdatedAt = time.Now().UTC()
	return cloneRow(r), nil
}

func (s *MemoryAgentCredentialStore) Retire(_ context.Context, id, replacedByID string) (*AgentCredentialRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "agent credential not found: "+id)
	}
	if r.RevokedAt != nil || r.ReplacedByID != "" {
		return nil, status.Error(codes.FailedPrecondition, "credential is already retired or revoked")
	}
	r.ReplacedByID = replacedByID
	r.UpdatedAt = time.Now().UTC()
	return cloneRow(r), nil
}

func (s *MemoryAgentCredentialStore) Revoke(_ context.Context, id, reason string) (*AgentCredentialRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return nil, status.Error(codes.NotFound, "agent credential not found: "+id)
	}
	if r.RevokedAt == nil {
		now := time.Now().UTC()
		r.RevokedAt = &now
		r.RevokedReason = reason
		r.UpdatedAt = now
	}
	return cloneRow(r), nil
}

