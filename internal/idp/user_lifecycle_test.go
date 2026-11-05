package idp

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// TestUserLifecycleSignInEnforcement walks the F-28.3 sign-in state machine
// over the gRPC surface (in-memory store): active users log in; deactivated
// and deprovisioned users are denied with DISTINCT messages; a provisioned
// user is denied on the password channel even though a hash exists (the
// channel lockdown fires before any hash compare); activation restores
// access; reactivation clears the tombstone but demands fresh credentials.
func TestUserLifecycleSignInEnforcement(t *testing.T) {
	users := NewMemoryUserStore()
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), users)
	ctx := context.Background()

	ada, err := idp.client.Register(ctx, &idppb.RegisterRequest{
		FirstName: "Ada", LastName: "L", Username: "ada", Email: "ada@example.com", Password: "s3cret",
	})
	if err != nil {
		t.Fatalf("register ada: %v", err)
	}
	id := ada.GetId()

	// Baseline: she CAN log in.
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "ada", Password: "s3cret"}); err != nil {
		t.Fatalf("baseline login: %v", err)
	}

	// Deactivate → denied with her OWN reason (not "invalid credentials").
	if err := users.Deactivate(ctx, id); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "ada", Password: "s3cret"}); err == nil {
		t.Fatal("login while deactivated succeeded, want denied")
	} else if status.Code(err) != codes.PermissionDenied {
		t.Errorf("denied code = %v, want PermissionDenied", status.Code(err))
	} else if msg := status.Convert(err).Message(); msg != "account is deactivated; contact an administrator" {
		t.Errorf("denied message = %q, want the distinct deactivated message", msg)
	}

	// Activation RESTORES access (same password — credentials untouched).
	if err := users.Activate(ctx, id); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "ada", Password: "s3cret"}); err != nil {
		t.Fatalf("login after activation: %v", err)
	}

	// Deprovision → denied with HER OWN reason.
	if err := users.SoftDelete(ctx, id); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "ada", Password: "s3cret"}); err == nil {
		t.Fatal("login while deprovisioned succeeded, want denied")
	} else if status.Code(err) != codes.PermissionDenied {
		t.Errorf("denied code = %v, want PermissionDenied", status.Code(err))
	} else if msg := status.Convert(err).Message(); msg != "account has been deprovisioned" {
		t.Errorf("denied message = %q, want the distinct deprovisioned message", msg)
	}

	// Reactivate → eligible AGAIN, but the hash was zeroed: the old password
	// no longer works (generic invalid-credentials, not a state denial).
	if err := users.Reactivate(ctx, id); err != nil {
		t.Fatalf("reactivate: %v", err)
	}
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "ada", Password: "s3cret"}); err == nil {
		t.Fatal("login with the ZEROED hash succeeded, want invalid credentials")
	} else if status.Code(err) != codes.Unauthenticated {
		t.Errorf("reactivated-login code = %v, want Unauthenticated (hash zeroed)", status.Code(err))
	}

	// UNKNOWN users still collapse to the generic message (enumeration
	// protection is unchanged by the state denials).
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "ghost", Password: "whatever"}); err == nil {
		t.Fatal("login for unknown user succeeded")
	} else if msg := status.Convert(err).Message(); msg != "invalid credentials" {
		t.Errorf("unknown-user message = %q, want the generic 'invalid credentials'", msg)
	}
}

// TestUserProvisionedChannelLockdown proves tooth 3 of the F-28.3 lockdown:
// a provisioned user is refused on the PASSWORD CHANNEL even when a hash
// exists (defect smuggling), with the distinct SSO-directed message — and the
// refusal fires before any hash compare (wrong password gets the IDENTICAL
// denial).
func TestUserProvisionedChannelLockdown(t *testing.T) {
	users := NewMemoryUserStore()
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), users)
	ctx := context.Background()

	// A locally registered user logs in fine (control).
	if _, err := idp.client.Register(ctx, &idppb.RegisterRequest{
		Username: "local", Email: "local@example.com", Password: "pw1",
	}); err != nil {
		t.Fatalf("register local: %v", err)
	}
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "local", Password: "pw1"}); err != nil {
		t.Fatalf("local login: %v", err)
	}

	// A provisioned user with a SMUGGLED hash (simulate legacy state /
	// defect): the password channel must stay shut.
	smuggled, err := users.Create(ctx, &User{
		Username:     "corp",
		Email:        "corp@example.com",
		PasswordHash: "should-be-unused-smuggled-hash",
		Roles:        []string{"user"},
		Provisioned:  true,
	})
	if err != nil {
		t.Fatalf("create provisioned: %v", err)
	}

	wrongDenial, errWrong := idp.client.Login(ctx, &idppb.LoginRequest{Username: "corp", Password: "totally-wrong"})
	rightDenial, errRight := idp.client.Login(ctx, &idppb.LoginRequest{Username: "corp", Password: "correct-smuggled"})
	_ = wrongDenial
	_ = rightDenial
	if errWrong == nil || errRight == nil {
		t.Fatalf("provisioned logins must be DENIED (got %v / %v)", errWrong, errRight)
	}
	if status.Code(errWrong) != codes.PermissionDenied || status.Code(errRight) != codes.PermissionDenied {
		t.Errorf("denial codes = %v / %v, want PermissionDenied both", status.Code(errWrong), status.Code(errRight))
	}
	if mw, mr := status.Convert(errWrong).Message(), status.Convert(errRight).Message(); mw != mr {
		t.Errorf("denial differs by password (%q vs %q) — the check leaked INTO the hash compare", mw, mr)
	}
	if mw := status.Convert(errWrong).Message(); mw != "account is managed by the identity provider; sign in via SSO" {
		t.Errorf("denial message = %q, want the distinct SSO-directed message", mw)
	}
	_ = smuggled
}

// TestUserUpdateProvisionedPasswordRefusal proves tooth 2 of the F-28.3
// lockdown: no WRITE path may attach a password to a provisioned user.
// Profile-only updates succeed; a password attempt is FailedPrecondition
// (the gRPC 409 analogue) and the hash is left untouched.
func TestUserUpdateProvisionedPasswordRefusal(t *testing.T) {
	users := NewMemoryUserStore()
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), users)
	ctx := context.Background()

	prov, err := users.Create(ctx, &User{
		Username:    "scimmed",
		Email:       "scimmed@example.com",
		Roles:       []string{"user"},
		Provisioned: true,
	})
	if err != nil {
		t.Fatalf("create provisioned: %v", err)
	}

	// Attempt to SET a password → refused.
	if _, err := idp.client.UpdateUser(ctx, &idppb.UpdateUserRequest{
		Id: prov.ID, Password: "attacker-chosen",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("update with password = %v, want FailedPrecondition", err)
	}
	after, err := users.GetByID(ctx, prov.ID, false)
	if err != nil {
		t.Fatalf("get after refused update: %v", err)
	}
	if after.PasswordHash != "" {
		t.Errorf("password hash = %q after refused update, want EMPTY", after.PasswordHash)
	}

	// Control: a profile-only update on the SAME user succeeds.
	if _, err := idp.client.UpdateUser(ctx, &idppb.UpdateUserRequest{
		Id: prov.ID, FirstName: "Renamed",
	}); err != nil {
		t.Fatalf("profile-only update: %v", err)
	}
	renamed, err := users.GetByID(ctx, prov.ID, false)
	if err != nil {
		t.Fatalf("get after profile update: %v", err)
	}
	if renamed.FirstName != "Renamed" {
		t.Errorf("first_name = %q, want Renamed", renamed.FirstName)
	}

	// Control: a NON-provisioned user MAY be given a password.
	local, err := users.Create(ctx, &User{Username: "localupd", Email: "local-upd@example.com", Roles: []string{"user"}})
	if err != nil {
		t.Fatalf("create local: %v", err)
	}
	if _, err := idp.client.UpdateUser(ctx, &idppb.UpdateUserRequest{
		Id: local.ID, Password: "granted",
	}); err != nil {
		t.Fatalf("local password update: %v", err)
	}
	localAfter, err := users.GetByID(ctx, local.ID, false)
	if err != nil {
		t.Fatalf("get local after update: %v", err)
	}
	if localAfter.PasswordHash == "" {
		t.Error("local user's password hash empty after permitted update")
	}
}

// TestUserListIncludeDeleted checks the IdP ListUsers flag: tombstoned users
// are hidden by default and revealed (flagged) with include_deleted.
func TestUserListIncludeDeleted(t *testing.T) {
	users := NewMemoryUserStore()
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), users)
	ctx := context.Background()

	if _, err := idp.client.Register(ctx, &idppb.RegisterRequest{Username: "stay", Email: "stay@example.com", Password: "p"}); err != nil {
		t.Fatalf("register stay: %v", err)
	}
	gone, err := idp.client.Register(ctx, &idppb.RegisterRequest{Username: "gone", Email: "gone@example.com", Password: "p"})
	if err != nil {
		t.Fatalf("register gone: %v", err)
	}
	if err := users.SoftDelete(ctx, gone.GetId()); err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	defaultList, err := idp.client.ListUsers(ctx, &idppb.ListUsersRequest{})
	if err != nil {
		t.Fatalf("list default: %v", err)
	}
	if len(defaultList.GetUsers()) != 1 {
		t.Fatalf("default list = %d users, want 1 (tombstoned hidden)", len(defaultList.GetUsers()))
	}

	fullList, err := idp.client.ListUsers(ctx, &idppb.ListUsersRequest{IncludeDeleted: true})
	if err != nil {
		t.Fatalf("list include_deleted: %v", err)
	}
	if len(fullList.GetUsers()) != 2 {
		t.Fatalf("include_deleted list = %d users, want 2", len(fullList.GetUsers()))
	}
	var ghost *dbpb.IDPUser
	for _, u := range fullList.GetUsers() {
		if u.GetUsername() == "gone" {
			ghost = u
		}
	}
	if ghost == nil {
		t.Fatal("tombstoned user missing from include_deleted list")
	}
	if ghost.GetDeletedAt() == nil {
		t.Error("tombstoned user lacks the deleted_at flag")
	}
	if ghost.GetActive() {
		t.Error("tombstoned user active = true, want false")
	}
}

// TestUserRegisterReservation checks the F-28.3 registration reservation: a
// TOMBSTONED username is refused (already-exists), not duplicated — the
// identity is reserved for restoration via reactivation.
func TestUserRegisterReservation(t *testing.T) {
	users := NewMemoryUserStore()
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), users)
	ctx := context.Background()

	res, err := idp.client.Register(ctx, &idppb.RegisterRequest{
		Username: "departed", Email: "departed@example.com", Password: "pw",
	})
	if err != nil {
		t.Fatalf("initial register: %v", err)
	}
	firstID := res.GetId()
	if err := users.SoftDelete(ctx, firstID); err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	// Re-registering the RESERVED username is refused…
	if _, err := idp.client.Register(ctx, &idppb.RegisterRequest{
		Username: "departed", Email: "departed@example.com", Password: "other",
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("register reserved username = %v, want AlreadyExists", err)
	}
	// …and no DUPLICATE row was created.
	if _, err := users.GetByUsername(ctx, "departed", true); err != nil {
		t.Fatalf("look up survivor: %v", err)
	}
	list, err := users.List(ctx, true)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	count := 0
	for _, u := range list {
		if u.Username == "departed" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("rows for reserved username = %d, want exactly 1 (no duplicate)", count)
	}
}
