// GroupStore — the SCIM group directory (F-28.5, Slice 2).
//
// The /Groups resource manages GROUPS SYNCED FROM THE CORPORATE DIRECTORY.
// Membership values are OPAQUE: whatever the provider sends in members[].value
// is stored verbatim (trimmed, deduplicated) and is never validated against
// the user directory at write time — unknown or departed members degrade
// gracefully (resolution is best-effort at READ time, in the handlers).
// Groups are hard-deletable (recreation is legal; no tombstone machinery).
//
// Listings carry GENERIC EQUALITY CONSTRAINTS pushed down INTO THE STORE
// (evaluated server-side, AND-ed) — the same store-neutral algebra the user
// directory uses (F-28.4): the canonical attribute vocabulary here is
// display_name / members_value. The production store forwards them to the
// Database service (real SQL narrowing); the memory store evaluates them in
// memory with identical rules.
package idp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// SCIMGroup is a group synced from the corporate directory (F-28.5).
type SCIMGroup struct {
	// ID is the group's stable identifier (decimal string — the SCIM
	// resource id).
	ID string
	// DisplayName is the group's display name (NOT unique — departments
	// routinely share names).
	DisplayName string
	Description string // ExternalID is the corporate directory's stable identifier for the
	// group (e.g. the Azure group objectId) — the re-correlation key when a
	// connector loses its id mapping (groups have no unique LOCAL key to
	// re-find by: displayName is non-unique). CREATE-ONLY and IMMUTABLE
	// after (the provider's group identity is stable for the group's life;
	// a changed identity is a new group). Empty when the provider sent none.
	ExternalID string // MemberValues are the group's VERBATIM membership strings (opaque;
	// usually the user's external_id, occasionally the username).
	MemberValues []string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// cloneSCIMGroup deep-copies a group (defense against aliasing between the
// store and its callers).
func cloneSCIMGroup(g *SCIMGroup) *SCIMGroup {
	cp := *g
	cp.MemberValues = append([]string(nil), g.MemberValues...)
	return &cp
}

// GroupStore persists the SCIM group directory so multiple IdP replicas
// share the same groups. The production implementation stores groups in the
// Database service; tests use an in-memory implementation.
//
// Constraint semantics (List): Attribute is a CANONICAL name (display_name,
// members_value); Value the compared constant; CaseSensitive governs the
// string comparisons. An unknown attribute is an InvalidArgument error (the
// caller translates its own dialect's unsupported expressions upstream).
type GroupStore interface {
	// Create registers a group with an initial (possibly empty) membership.
	// Values are normalized (trimmed, deduplicated). externalID is the
	// corporate directory's group identifier (ACCEPTED ONLY HERE — it is
	// immutable afterwards; empty means the provider sent none). Returns the
	// stored group (with its assigned id).
	Create(ctx context.Context, displayName, description, externalID string, memberValues []string) (*SCIMGroup, error)
	// Get returns the group with the given id. A missing group returns an
	// error with status.Code == codes.NotFound; a malformed id returns
	// codes.InvalidArgument.
	Get(ctx context.Context, id string) (*SCIMGroup, error)
	// List returns groups narrowed by the generic equality constraints
	// (nil/empty ⇒ unconstrained), ordered by id. Canonical attributes:
	// display_name, external_id, members_value.
	List(ctx context.Context, equalities *EqualityConstraints) ([]*SCIMGroup, error)
	// Replace DEFINITELY replaces a group's attributes and its ENTIRE
	// membership (SCIM PUT semantics): displayName/description are SET to
	// the supplied values ("" clears them) and the membership is swapped
	// for memberValues wholesale (empty ⇒ emptied). externalID is
	// IMMUTABLE: a non-empty value differing from the group's current one
	// is codes.InvalidArgument (equal-or-absent is a no-op). A missing
	// group returns codes.NotFound.
	Replace(ctx context.Context, id, displayName, description, externalID string, memberValues []string) (*SCIMGroup, error)
	// PatchMembers atomically adds and/or removes membership values
	// (idempotent both ways; values normalized). At least one side must be
	// non-empty (otherwise InvalidArgument). A missing group returns
	// codes.NotFound.
	PatchMembers(ctx context.Context, id string, add, remove []string) (*SCIMGroup, error)
	// Delete hard-deletes a group. Recreation afterwards is legal. A
	// missing group returns codes.NotFound.
	Delete(ctx context.Context, id string) error
}

// normalizeMemberValues settles the verbatim membership strings to their
// canonical stored form: trimmed, blanks dropped, duplicates collapsed
// (FIRST OCCURRENCE WINS, order preserved). Shared by the production and
// memory stores so both sides agree on every input (the dual-store parity
// rule).
func normalizeMemberValues(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

// ---------------------------------------------------------------------------
// Production store (Database service)
// ---------------------------------------------------------------------------

// dbGroupStore is a GroupStore backed by the Database service.
type dbGroupStore struct {
	db dbpb.DatabaseClient
}

// NewDBGroupStore returns a GroupStore that persists the group directory
// through the Database service.
func NewDBGroupStore(db dbpb.DatabaseClient) GroupStore {
	return &dbGroupStore{db: db}
}

func groupFromProto(g *dbpb.IDPGroup) *SCIMGroup {
	return &SCIMGroup{
		ID:           g.GetId(),
		DisplayName:  g.GetDisplayName(),
		Description:  g.GetDescription(),
		ExternalID:   g.GetExternalId(),
		MemberValues: append([]string(nil), g.GetMemberValues()...),
		CreatedAt:    g.GetCreatedAt().AsTime(),
		UpdatedAt:    g.GetUpdatedAt().AsTime(),
	}
}

func (s *dbGroupStore) Create(ctx context.Context, displayName, description, externalID string, memberValues []string) (*SCIMGroup, error) {
	resp, err := s.db.CreateGroup(ctx, &dbpb.CreateGroupRequest{
		DisplayName:  displayName,
		Description:  description,
		ExternalId:   externalID,
		MemberValues: memberValues,
	})
	if err != nil {
		return nil, fmt.Errorf("idp: create group: %w", err)
	}
	return groupFromProto(resp), nil
}

func (s *dbGroupStore) Get(ctx context.Context, id string) (*SCIMGroup, error) {
	resp, err := s.db.GetGroup(ctx, &dbpb.GetGroupRequest{Id: id})
	if err != nil {
		return nil, err
	}
	return groupFromProto(resp), nil
}

// dbGroupListingRequest renders the store-neutral constraints into the
// Database service's predicate shape (verbatim translation — the canonical
// vocabulary IS the service's; a nil set is unconstrained).
func dbGroupListingRequest(equalities *EqualityConstraints) *dbpb.ListGroupsRequest {
	req := &dbpb.ListGroupsRequest{}
	if equalities != nil {
		for _, ec := range *equalities {
			req.GenericPredicate = append(req.GenericPredicate, &dbpb.ListGroupsRequest_Predicate{
				Attribute:     ec.Attribute,
				CaseSensitive: ec.CaseSensitive,
				Value:         ec.Value,
			})
		}
	}
	return req
}

func (s *dbGroupStore) List(ctx context.Context, equalities *EqualityConstraints) ([]*SCIMGroup, error) {
	resp, err := s.db.ListGroups(ctx, dbGroupListingRequest(equalities))
	if err != nil {
		return nil, fmt.Errorf("idp: list groups: %w", err)
	}
	groups := make([]*SCIMGroup, 0, len(resp.GetGroups()))
	for _, g := range resp.GetGroups() {
		groups = append(groups, groupFromProto(g))
	}
	return groups, nil
}

func (s *dbGroupStore) Replace(ctx context.Context, id, displayName, description, externalID string, memberValues []string) (*SCIMGroup, error) {
	resp, err := s.db.ReplaceGroup(ctx, &dbpb.ReplaceGroupRequest{
		Id:           id,
		DisplayName:  displayName,
		Description:  description,
		ExternalId:   externalID,
		MemberValues: memberValues,
	})
	if err != nil {
		return nil, fmt.Errorf("idp: replace group: %w", err)
	}
	return groupFromProto(resp), nil
}

func (s *dbGroupStore) PatchMembers(ctx context.Context, id string, add, remove []string) (*SCIMGroup, error) {
	resp, err := s.db.PatchGroupMembers(ctx, &dbpb.PatchGroupMembersRequest{
		Id:     id,
		Add:    add,
		Remove: remove,
	})
	if err != nil {
		return nil, fmt.Errorf("idp: patch group members: %w", err)
	}
	return groupFromProto(resp), nil
}

func (s *dbGroupStore) Delete(ctx context.Context, id string) error {
	_, err := s.db.DeleteGroup(ctx, &dbpb.DeleteGroupRequest{Id: id})
	if err != nil {
		return fmt.Errorf("idp: delete group: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Memory store (tests / single-instance dev)
// ---------------------------------------------------------------------------

// MemoryGroupStore is an in-memory GroupStore. It is used by tests and is
// suitable for running a single IdP without a Database service; it does not
// share state across processes.
type MemoryGroupStore struct {
	mu    sync.Mutex
	seq   int
	byID  map[string]*SCIMGroup
	order []string // insertion order (ids)
}

// NewMemoryGroupStore returns an empty in-memory GroupStore.
func NewMemoryGroupStore() *MemoryGroupStore {
	return &MemoryGroupStore{byID: make(map[string]*SCIMGroup)}
}

// validateGroupEqualities closes the generic equality algebra for the MEMORY
// side (paranoia: the production side validates in the Database service, but
// the memory store must reject the SAME inputs — the dual-store agreement
// rule).
func validateGroupEqualities(eq *EqualityConstraints) error {
	if eq == nil {
		return nil
	}
	for _, c := range *eq {
		switch c.Attribute {
		case "display_name", "external_id", "members_value":
		default:
			return status.Errorf(codes.InvalidArgument, "unknown group filter attribute: %q", c.Attribute)
		}
	}
	return nil
}

func (s *MemoryGroupStore) Create(_ context.Context, displayName, description, externalID string, memberValues []string) (*SCIMGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	s.seq++
	id := fmt.Sprintf("%d", s.seq)
	g := &SCIMGroup{
		ID:           id,
		DisplayName:  displayName,
		Description:  description,
		ExternalID:   externalID,
		MemberValues: normalizeMemberValues(memberValues),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	s.byID[id] = g
	s.order = append(s.order, id)
	return cloneSCIMGroup(g), nil
}

func (s *MemoryGroupStore) Get(_ context.Context, id string) (*SCIMGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.byID[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "group %q not found", id)
	}
	return cloneSCIMGroup(g), nil
}

func (s *MemoryGroupStore) List(_ context.Context, equalities *EqualityConstraints) ([]*SCIMGroup, error) {
	if err := validateGroupEqualities(equalities); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*SCIMGroup, 0, len(s.order))
	for _, id := range s.order { // insertion order == id order
		g := s.byID[id]
		if groupMatches(g, equalities) {
			out = append(out, cloneSCIMGroup(g))
		}
	}
	return out, nil
}

// groupMatches evaluates the (validated) constraints against one group.
func groupMatches(g *SCIMGroup, eq *EqualityConstraints) bool {
	if eq == nil {
		return true
	}
	for _, c := range *eq {
		switch c.Attribute {
		case "display_name":
			if c.CaseSensitive {
				if g.DisplayName != c.Value {
					return false
				}
			} else if !strings.EqualFold(g.DisplayName, c.Value) {
				return false
			}
		case "external_id":
			// Provider identifiers are EXACT (case matters) — the
			// translator pins caseSensitive=true for this attribute.
			if g.ExternalID != c.Value {
				return false
			}
		case "members_value":
			found := false
			for _, v := range g.MemberValues {
				if c.CaseSensitive {
					if v == c.Value {
						found = true
						break
					}
				} else if strings.EqualFold(v, c.Value) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func (s *MemoryGroupStore) Replace(_ context.Context, id, displayName, description, externalID string, memberValues []string) (*SCIMGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.byID[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "group %q not found", id)
	}
	// external_id is IMMUTABLE (parity with the DB store's guard).
	if externalID != "" && externalID != g.ExternalID {
		return nil, status.Error(codes.InvalidArgument, "external_id is immutable")
	}
	g.DisplayName = displayName
	g.Description = description
	g.MemberValues = normalizeMemberValues(memberValues)
	g.UpdatedAt = time.Now().UTC()
	return cloneSCIMGroup(g), nil
}

func (s *MemoryGroupStore) PatchMembers(_ context.Context, id string, add, remove []string) (*SCIMGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.byID[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "group %q not found", id)
	}
	addNorm := normalizeMemberValues(add)
	removeNorm := normalizeMemberValues(remove)
	if len(addNorm)+len(removeNorm) == 0 {
		return nil, status.Error(codes.InvalidArgument, "nothing to patch: supply at least one add or remove value")
	}
	removed := make(map[string]bool, len(removeNorm))
	for _, v := range removeNorm {
		removed[v] = true
	}
	// Final set = (existing − removed) ∪ (added − removed), DEDUPED: the
	// membership is a set (parity with the DB store's composite unique index
	// — adding an already-present value is a no-op, not a duplicate row).
	present := make(map[string]int, len(g.MemberValues)) // value ⇒ position
	keep := make([]string, 0, len(g.MemberValues))
	for pos, v := range g.MemberValues {
		if removed[v] {
			continue
		}
		present[v] = pos
		keep = append(keep, v)
	}
	for _, v := range addNorm {
		if removed[v] { // a simultaneous add+remove of the same value nets to REMOVE
			continue
		}
		if _, exists := present[v]; exists {
			continue // idempotent add
		}
		present[v] = len(keep)
		keep = append(keep, v)
	}
	g.MemberValues = keep
	g.UpdatedAt = time.Now().UTC()
	return cloneSCIMGroup(g), nil
}

func (s *MemoryGroupStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[id]; !ok {
		return status.Errorf(codes.NotFound, "group %q not found", id)
	}
	delete(s.byID, id)
	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	return nil
}
