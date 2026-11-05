package idp

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// User is a registered user of the local identity provider. The IdP uses it
// for username/password authentication: it verifies a login against the
// stored password hash and, on success, mints an OIDC token for the user
// (stamping the user's roles onto the token). The password is stored only as
// a salted hash (PasswordHash); the plaintext is never persisted.
//
// Identity anchors (F-28.4): Username is the UNIQUE login identifier (the
// OIDC subject claim and the SCIM userName land here); Email is OPTIONAL and
// non-unique (a subject need not be an email). All lookups run through ID,
// Username, or ExternalID — never email.
//
// Lifecycle (F-28.3): a user is active (full access), deactivated
// (Active=false — "on leave", credentials retained), or deprovisioned
// (DeletedAt tombstone — gone; credentials destroyed, identity and history
// retained). Provenance: Provisioned marks an externally-sourced record
// (SCIM/JIT) versus a locally registered one; it drives the
// authentication-channel lockdown (provisioned users sign in via SSO only).
// ExternalID is the corporate directory's stable identifier (the
// re-provisioning match key).
type User struct {
	ID string
	// Username is the user's unique login identifier (F-28.4).
	Username  string
	FirstName string
	LastName  string
	// Email is the user's optional electronic-mail address (F-28.4); empty
	// when the user has no known email. Informational only — never a lookup
	// key.
	Email        string
	PasswordHash string
	Roles        []string
	CreatedAt    time.Time
	// UpdatedAt is the row's last modification (F-28.4 Slice 2): the SCIM
	// resource's meta.modified echoes this instant (the row's actual
	// timestamp), not the emission time.
	UpdatedAt time.Time
	// Active is the user's activated state (F-28.3); false means deactivated
	// ("on leave").
	Active bool
	// DeletedAt is the deprovisioning tombstone (F-28.3); nil means the user
	// is not deprovisioned.
	DeletedAt *time.Time
	// ExternalID is the corporate directory's stable identifier for the user
	// (F-28.3/F-28.4); empty for locally registered users.
	ExternalID string
	// Provisioned marks an externally-sourced record (F-28.3).
	Provisioned bool
	// TempPasswordHash is the encoding of a SHORT-LIVED password (F-28.3
	// break-glass); distinct from PasswordHash so a temporal credential never
	// masks the user's standing one.
	TempPasswordHash string
	// TempPasswordValidUntil bounds the temporal password's usability; nil
	// means there is no temporal credential.
	TempPasswordValidUntil *time.Time
}

// ProfileReplacement expresses the DESIRED state for a definite profile
// replace (F-28.4 Slice 2, the SCIM PUT semantics). It is intentionally a
// SEPARATE type from User so the presence/default distinctions are explicit
// rather than overloaded onto User's zero-values:
//   - Username is REQUIRED (empty ⇒ the store returns InvalidArgument).
//   - FirstName/LastName/Email are SET to the given value ("" clears them).
//   - Roles is SET to the given slice (nil/empty resets the role set).
//   - ActiveSignaled, when non-nil, is the desired active state; when nil the
//     default (true) applies. A pointer is used because active's default
//     (true) differs from its zero value (false), so a plain bool cannot
//     express "omit".
//   - ExternalID is IMMUTABLE: a non-empty value differing from the row's
//     current external ID is InvalidArgument; empty or equal is a no-op.
type ProfileReplacement struct {
	ID             string
	Username       string
	FirstName      string
	LastName       string
	Email          string
	Roles          []string
	ActiveSignaled *bool
	ExternalID     string
}

// EqualityConstraint is one GENERIC EQUALITY CONSTRAINT expressed in the
// store-neutral ATTRIBUTE VOCABULARY (the canonical lowercase snake_case
// column names: username, external_id, email, first_name, last_name, active,
// provisioned). Translating FROM a caller's dialect (SCIM camelCase, the
// IdP gRPC's camelCase) happens UPSTREAM of the store; translating TO the
// backend (the Database service's pushdown, the memory evaluator) happens
// downstream. Booleans compare literally regardless of CaseSensitive.
type EqualityConstraint struct {
	Attribute     string
	Value         string
	CaseSensitive bool
}

// EqualityConstraints is the AND-ed set of generic equality constraints a
// listing may carry (nil/empty ⇒ unconstrained).
type EqualityConstraints []EqualityConstraint

// UserStore persists the IdP's user directory (registered users, their
// password hashes, and roles) so that multiple IdP replicas share the same
// users. The production implementation stores users in the Database service;
// tests use an in-memory implementation.
//
// Lookup semantics (F-28.3/F-28.4): a tombstoned (deprovisioned) user is
// reported as NotFound unless includeDeleted is set. Reviving a tombstoned
// user is exclusively Reactivate's job. Identifiers (username, external ID)
// are the lookup keys — email is never consulted.
type UserStore interface {
	// Create registers (or updates) a user, keyed by USERNAME (the unique
	// login identifier, F-28.4). A new user is given a stable ID (the
	// token's subject) and is born ACTIVE and non-tombstoned; an existing
	// user's profile and roles are updated. It returns the stored user.
	Create(ctx context.Context, user *User) (*User, error)
	// GetByUsername returns the user with the given username (case-
	// insensitive). A missing user — or a tombstoned one when includeDeleted
	// is false — returns an error with status.Code == codes.NotFound.
	GetByUsername(ctx context.Context, username string, includeDeleted bool) (*User, error)
	// FindByExternalID returns the user with the given corporate external ID
	// (exact match; the re-provisioning match key, F-28.3/F-28.4). A missing
	// user — or a tombstoned one when includeDeleted is false — returns an
	// error with status.Code == codes.NotFound.
	FindByExternalID(ctx context.Context, externalID string, includeDeleted bool) (*User, error)
	// GetByID returns the user with the given ID. A missing user — or a
	// tombstoned one when includeDeleted is false — returns an error with
	// status.Code == codes.NotFound.
	GetByID(ctx context.Context, id string, includeDeleted bool) (*User, error)
	// List returns registered users. When includeDeleted is false (the
	// default) tombstoned users are excluded. Equalities, when non-nil, adds
	// GENERIC EQUALITY CONSTRAINTS pushed down INTO THE STORE (evaluated
	// server-side, AND-ed): Attribute is a canonical lowercase snake_case
	// name (username, external_id, email, first_name, last_name, active,
	// provisioned); Value the compared constant; CaseSensitive governs the
	// string comparisons (booleans compare literally regardless). An unknown
	// attribute is an InvalidArgument error (the caller translates its own
	// dialect's unsupported expressions upstream).
	List(ctx context.Context, includeDeleted bool, equalities *EqualityConstraints) ([]*User, error)
	// Count tallies the users THAT WOULD BE RETURNED by List (same
	// includeDeleted + equalities conditions) WITHOUT transferring them:
	// the production store computes the aggregate in the Database service
	// (one integer crosses the wire, not the population); the memory store
	// walks its own index. Measuring the directory (gauges, first-admin
	// roll-outs) must use this.
	Count(ctx context.Context, includeDeleted bool, equalities *EqualityConstraints) (int, error)
	// Update updates a user's profile and/or roles, keyed by ID. A missing
	// user — or a tombstoned one — returns an error with status.Code ==
	// codes.NotFound.
	Update(ctx context.Context, user *User) (*User, error)
	// ReplaceProfile DEFINITELY replaces the user's tracked profile fields
	// (F-28.4 Slice 2, the SCIM PUT semantics): unlike Update's supplied-
	// value semantics (empty ⇒ leave unchanged), every field is SET to the
	// supplied value — omitted names/email take "" and an omitted role set
	// resets to none. Profile carries the desired state: Username (required),
	// FirstName, LastName, Email, Roles, and ActiveSignaled (present ⇒ the
	// desired active state; absent ⇒ the default, true). ExternalID is
	// immutable: a non-empty Profile.ExternalID differing from the row's
	// current value is an InvalidArgument error. A missing user — or a
	// tombstoned one — returns codes.NotFound. Returns the stored user.
	ReplaceProfile(ctx context.Context, profile *ProfileReplacement) (*User, error)
	// SoftDelete deprovisions a user by ID (F-28.3): it sets the tombstone,
	// marks the user inactive, and zeroes the password hash so no leftover
	// credential can revive the record. The row (identity, roles, history)
	// is retained. A missing user returns an error with status.Code ==
	// codes.NotFound; repeating the delete is idempotent.
	SoftDelete(ctx context.Context, id string) error
	// Activate activates a deactivated user (active=true), restoring
	// authentication eligibility; credentials are untouched. Idempotent.
	Activate(ctx context.Context, id string) error
	// Deactivate deactivates an active user ("on leave"): active=false, the
	// row and its credentials are retained. Idempotent.
	Deactivate(ctx context.Context, id string) error
	// Reactivate clears a deprovisioned user's tombstone (rehire):
	// active=true, the password hash is zeroed (fresh credentials must be
	// reinstated by the rightful source), and the same stable ID and all
	// history are preserved. Reactivating a non-tombstoned user is idempotent
	// (it merely activates).
	Reactivate(ctx context.Context, id string) error
	// ExpireTemporaryPassword zeros a user's TEMPORAL (short-lived) credential
	// (F-28.3 break-glass hygiene): the hash and its validity window are
	// cleared without touching the user's active state or standing password.
	// Called by the hygiene sweep and lazily at login once the window lapses.
	// A no-op when the user has no temporal credential.
	ExpireTemporaryPassword(ctx context.Context, id string) error
}

// dbUserStore is a UserStore backed by the Database service.
type dbUserStore struct {
	db dbpb.DatabaseClient
}

// NewDBUserStore returns a UserStore that persists the user directory through
// the Database service.
func NewDBUserStore(db dbpb.DatabaseClient) UserStore {
	return &dbUserStore{db: db}
}

func (s *dbUserStore) Create(ctx context.Context, user *User) (*User, error) {
	resp, err := s.db.CreateUser(ctx, &dbpb.CreateIDPUserRequest{User: user.toProto()})
	if err != nil {
		return nil, fmt.Errorf("idp: create user: %w", err)
	}
	return userFromProto(resp), nil
}

func (s *dbUserStore) GetByUsername(ctx context.Context, username string, includeDeleted bool) (*User, error) {
	resp, err := s.db.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Username: username, IncludeDeleted: includeDeleted})
	if err != nil {
		return nil, err
	}
	return userFromProto(resp), nil
}

func (s *dbUserStore) FindByExternalID(ctx context.Context, externalID string, includeDeleted bool) (*User, error) {
	resp, err := s.db.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{ExternalId: externalID, IncludeDeleted: includeDeleted})
	if err != nil {
		return nil, err
	}
	return userFromProto(resp), nil
}

func (s *dbUserStore) GetByID(ctx context.Context, id string, includeDeleted bool) (*User, error) {
	resp, err := s.db.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Id: id, IncludeDeleted: includeDeleted})
	if err != nil {
		return nil, err
	}
	return userFromProto(resp), nil
}

// dbUserListingRequest renders the store-neutral listing conditions into the
// Database service's request shape (SHARED by List and Count — the count
// must obey EXACTLY the conditions the list would). The store-neutral
// constraints ARE the service's canonical vocabulary (verbatim translation);
// a NIL set is simply unconstrained (most internal callers — sweeps, gauges,
// roll-outs — operate without constraints).
func dbUserListingRequest(includeDeleted bool, equalities *EqualityConstraints) *dbpb.ListIDPUsersRequest {
	preds := []*dbpb.ListIDPUsersRequest_Predicate{}
	if equalities != nil {
		for _, ec := range *equalities {
			preds = append(preds, &dbpb.ListIDPUsersRequest_Predicate{
				Attribute:     ec.Attribute,
				CaseSensitive: ec.CaseSensitive,
				Value:         ec.Value,
			})
		}
	}
	return &dbpb.ListIDPUsersRequest{IncludeDeleted: includeDeleted, GenericPredicate: preds}
}

func (s *dbUserStore) List(ctx context.Context, includeDeleted bool, equalities *EqualityConstraints) ([]*User, error) {
	resp, err := s.db.ListIDPUsers(ctx, dbUserListingRequest(includeDeleted, equalities))
	if err != nil {
		return nil, fmt.Errorf("idp: list users: %w", err)
	}
	users := make([]*User, 0, len(resp.GetUsers()))
	for _, u := range resp.GetUsers() {
		users = append(users, userFromProto(u))
	}
	return users, nil
}

// Count measures the directory THROUGH THE DATABASE (aggregate — one integer
// crosses the wire, never the population).
func (s *dbUserStore) Count(ctx context.Context, includeDeleted bool, equalities *EqualityConstraints) (int, error) {
	resp, err := s.db.CountIDPUsers(ctx, dbUserListingRequest(includeDeleted, equalities))
	if err != nil {
		return 0, fmt.Errorf("idp: count users: %w", err)
	}
	return int(resp.GetCount()), nil
}

func (s *dbUserStore) Update(ctx context.Context, user *User) (*User, error) {
	resp, err := s.db.UpdateIDPUser(ctx, &dbpb.UpdateIDPUserRequest{User: user.toProto()})
	if err != nil {
		return nil, fmt.Errorf("idp: update user: %w", err)
	}
	return userFromProto(resp), nil
}

// ReplaceProfile delegates to the Database service's definite-replace RPC
// (F-28.4 Slice 2). Presence of the active state is conveyed by the optional
// field (nil ⇒ the default, true).
func (s *dbUserStore) ReplaceProfile(ctx context.Context, p *ProfileReplacement) (*User, error) {
	req := &dbpb.ReplaceIDPUserProfileRequest{
		Id:         p.ID,
		Username:   p.Username,
		FirstName:  p.FirstName,
		LastName:   p.LastName,
		Email:      p.Email,
		Roles:      p.Roles,
		ExternalId: p.ExternalID,
	}
	if p.ActiveSignaled != nil {
		a := *p.ActiveSignaled
		req.Active = &a
	}
	resp, err := s.db.ReplaceIDPUserProfile(ctx, req)
	if err != nil {
		return nil, err
	}
	return userFromProto(resp), nil
}

func (s *dbUserStore) SoftDelete(ctx context.Context, id string) error {
	_, err := s.db.DeleteIDPUser(ctx, &dbpb.DeleteIDPUserRequest{Id: id})
	if err != nil {
		return err
	}
	return nil
}

func (s *dbUserStore) Activate(ctx context.Context, id string) error {
	_, err := s.db.ActivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id})
	if err != nil {
		return err
	}
	return nil
}

func (s *dbUserStore) Deactivate(ctx context.Context, id string) error {
	_, err := s.db.DeactivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id})
	if err != nil {
		return err
	}
	return nil
}

func (s *dbUserStore) Reactivate(ctx context.Context, id string) error {
	_, err := s.db.ReactivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id})
	if err != nil {
		return err
	}
	return nil
}

func (s *dbUserStore) ExpireTemporaryPassword(ctx context.Context, id string) error {
	_, err := s.db.ExpireTemporaryPassword(ctx, &dbpb.ExpireTemporaryPasswordRequest{Id: id})
	if err != nil {
		return err
	}
	return nil
}

// toProto renders the user as its Database-service form.
func (u *User) toProto() *dbpb.IDPUser {
	out := &dbpb.IDPUser{
		Id:                    u.ID,
		Username:              u.Username,
		FirstName:             u.FirstName,
		LastName:              u.LastName,
		Email:                 u.Email,
		PasswordHash:          u.PasswordHash,
		Roles:                 u.Roles,
		CreatedAt:             timestamppb.New(u.CreatedAt),
		Active:                u.Active,
		ExternalId:            u.ExternalID,
		Provisioned:           u.Provisioned,
		TemporaryPasswordHash: u.TempPasswordHash,
	}
	if u.DeletedAt != nil {
		out.DeletedAt = timestamppb.New(*u.DeletedAt)
	}
	if u.TempPasswordValidUntil != nil {
		out.TemporaryPasswordValidUntil = timestamppb.New(*u.TempPasswordValidUntil)
	}
	return out
}

// userFromProto reconstructs an in-memory user from its Database-service form.
func userFromProto(u *dbpb.IDPUser) *User {
	user := &User{
		ID:               u.GetId(),
		Username:         u.GetUsername(),
		FirstName:        u.GetFirstName(),
		LastName:         u.GetLastName(),
		Email:            u.GetEmail(),
		PasswordHash:     u.GetPasswordHash(),
		Roles:            u.GetRoles(),
		Active:           u.GetActive(),
		ExternalID:       u.GetExternalId(),
		Provisioned:      u.GetProvisioned(),
		TempPasswordHash: u.GetTemporaryPasswordHash(),
	}
	if ts := u.GetCreatedAt(); ts != nil {
		user.CreatedAt = ts.AsTime()
	}
	if ts := u.GetUpdatedAt(); ts != nil {
		user.UpdatedAt = ts.AsTime()
	}
	if ts := u.GetDeletedAt(); ts != nil {
		t := ts.AsTime()
		user.DeletedAt = &t
	}
	if ts := u.GetTemporaryPasswordValidUntil(); ts != nil {
		t := ts.AsTime()
		user.TempPasswordValidUntil = &t
	}
	return user
}
