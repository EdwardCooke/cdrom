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
type User struct {
	ID           string
	FirstName    string
	LastName     string
	Email        string
	PasswordHash string
	Roles        []string
	CreatedAt    time.Time
}

// UserStore persists the IdP's user directory (registered users, their
// password hashes, and roles) so that multiple IdP replicas share the same
// users. The production implementation stores users in the Database service;
// tests use an in-memory implementation.
type UserStore interface {
	// Create registers (or updates) a user, keyed by email. A new user is
	// given a stable ID (the token's subject); an existing user's profile and
	// roles are updated. It returns the stored user.
	Create(ctx context.Context, user *User) (*User, error)
	// GetByEmail returns the user with the given email (case-insensitive). A
	// missing user returns an error with status.Code == codes.NotFound.
	GetByEmail(ctx context.Context, email string) (*User, error)
	// GetByID returns the user with the given ID. A missing user returns an
	// error with status.Code == codes.NotFound.
	GetByID(ctx context.Context, id string) (*User, error)
	// List returns all registered users.
	List(ctx context.Context) ([]*User, error)
	// Update updates a user's profile and/or roles, keyed by ID. A missing
	// user returns an error with status.Code == codes.NotFound.
	Update(ctx context.Context, user *User) (*User, error)
	// Delete removes a user by ID. A missing user returns an error with
	// status.Code == codes.NotFound.
	Delete(ctx context.Context, id string) error
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

func (s *dbUserStore) GetByEmail(ctx context.Context, email string) (*User, error) {
	resp, err := s.db.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Email: email})
	if err != nil {
		return nil, err
	}
	return userFromProto(resp), nil
}

func (s *dbUserStore) GetByID(ctx context.Context, id string) (*User, error) {
	resp, err := s.db.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Id: id})
	if err != nil {
		return nil, err
	}
	return userFromProto(resp), nil
}

func (s *dbUserStore) List(ctx context.Context) ([]*User, error) {
	resp, err := s.db.ListIDPUsers(ctx, &dbpb.ListIDPUsersRequest{})
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

func (s *dbUserStore) Delete(ctx context.Context, id string) error {
	_, err := s.db.DeleteIDPUser(ctx, &dbpb.DeleteIDPUserRequest{Id: id})
	if err != nil {
		return err
	}
	return nil
}

// toProto renders the user as its Database-service form.
func (u *User) toProto() *dbpb.IDPUser {
	return &dbpb.IDPUser{
		Id:           u.ID,
		FirstName:    u.FirstName,
		LastName:     u.LastName,
		Email:        u.Email,
		PasswordHash: u.PasswordHash,
		Roles:        u.Roles,
		CreatedAt:    timestamppb.New(u.CreatedAt),
	}
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
	}
	if ts := u.GetCreatedAt(); ts != nil {
		user.CreatedAt = ts.AsTime()
	}
	return user
}
