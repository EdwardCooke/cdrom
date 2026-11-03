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
// Lifecycle (F-28.3): a user is active (full access), deactivated
// (Active=false — "on leave", credentials retained), or deprovisioned
// (DeletedAt tombstone — gone; credentials destroyed, identity and history
// retained). Provenance: Provisioned marks an externally-sourced record
// (SCIM/JIT) versus a locally registered one; it drives the
// authentication-channel lockdown (provisioned users sign in via SSO only).
// ExternalID is the corporate directory's stable identifier (the
// re-provisioning match key).
type User struct {
	ID           string
	FirstName    string
	LastName     string
	Email        string
	PasswordHash string
	Roles        []string
	CreatedAt    time.Time
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

// UserStore persists the IdP's user directory (registered users, their
// password hashes, and roles) so that multiple IdP replicas share the same
// users. The production implementation stores users in the Database service;
// tests use an in-memory implementation.
//
// Lookup semantics (F-28.3): a tombstoned (deprovisioned) user is reported
// as NotFound unless includeDeleted is set. Reviving a tombstoned user is
// exclusively Reactivate's job.
type UserStore interface {
	// Create registers (or updates) a user, keyed by email. A new user is
	// given a stable ID (the token's subject) and is born ACTIVE and
	// non-tombstoned; an existing user's profile and roles are updated. It
	// returns the stored user.
	Create(ctx context.Context, user *User) (*User, error)
	// GetByEmail returns the user with the given email (case-insensitive). A
	// missing user — or a tombstoned one when includeDeleted is false —
	// returns an error with status.Code == codes.NotFound.
	GetByEmail(ctx context.Context, email string, includeDeleted bool) (*User, error)
	// GetByID returns the user with the given ID. A missing user — or a
	// tombstoned one when includeDeleted is false — returns an error with
	// status.Code == codes.NotFound.
	GetByID(ctx context.Context, id string, includeDeleted bool) (*User, error)
	// List returns registered users. When includeDeleted is false (the
	// default) tombstoned users are excluded.
	List(ctx context.Context, includeDeleted bool) ([]*User, error)
	// Update updates a user's profile and/or roles, keyed by ID. A missing
	// user — or a tombstoned one — returns an error with status.Code ==
	// codes.NotFound.
	Update(ctx context.Context, user *User) (*User, error)
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

func (s *dbUserStore) GetByEmail(ctx context.Context, email string, includeDeleted bool) (*User, error) {
	resp, err := s.db.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Email: email, IncludeDeleted: includeDeleted})
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

func (s *dbUserStore) List(ctx context.Context, includeDeleted bool) ([]*User, error) {
	resp, err := s.db.ListIDPUsers(ctx, &dbpb.ListIDPUsersRequest{IncludeDeleted: includeDeleted})
	if err != nil {
		return nil, fmt.Errorf("idp: list users: %w", err)
	}
	users := make([]*User, 0, len(resp.GetUsers()))
	for _, u := range resp.GetUsers() {
		users = append(users, userFromProto(u))
	}
	return users, nil
}

func (s *dbUserStore) Update(ctx context.Context, user *User) (*User, error) {
	resp, err := s.db.UpdateIDPUser(ctx, &dbpb.UpdateIDPUserRequest{User: user.toProto()})
	if err != nil {
		return nil, fmt.Errorf("idp: update user: %w", err)
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
		ID:           u.GetId(),
		FirstName:    u.GetFirstName(),
		LastName:     u.GetLastName(),
		Email:        u.GetEmail(),
		PasswordHash: u.GetPasswordHash(),
		Roles:        u.GetRoles(),
		Active:           u.GetActive(),
		ExternalID:       u.GetExternalId(),
		Provisioned:      u.GetProvisioned(),
		TempPasswordHash: u.GetTemporaryPasswordHash(),
	}
	if ts := u.GetCreatedAt(); ts != nil {
		user.CreatedAt = ts.AsTime()
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
