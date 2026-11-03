package database

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

func mustTimestamp(t *testing.T, s string) *timestamppb.Timestamp {
	t.Helper()
	ts, err := time.Parse("2006-01-02T15:04:05Z", s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return timestamppb.New(ts)
}

// TestIDPUserMigrationDefaults seeds a user the ordinary way (exercising
// AutoMigrate's column defaults) and asserts the F-28.3 baseline: active, no
// tombstone, empty external ID, provisioned=false, no temporal credential —
// on both the WIRE projection and the persisted ROW.
func TestIDPUserMigrationDefaults(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	email := "migration-defaults@example.com"
	created, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{Email: email, PasswordHash: "h", Roles: []string{"user"}},
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	assertWireBaseline(t, created)

	var row models.IDPUser
	if err := db.Where("email = ?", email).First(&row).Error; err != nil {
		t.Fatalf("load row: %v", err)
	}
	assertRowBaseline(t, &row)
}

func assertWireBaseline(t *testing.T, u *dbpb.IDPUser) {
	t.Helper()
	if !u.GetActive() {
		t.Errorf("wire active = false, want true")
	}
	if u.GetDeletedAt() != nil {
		t.Errorf("wire deleted_at = %v, want unset", u.GetDeletedAt())
	}
	if u.GetExternalId() != "" {
		t.Errorf("wire external_id = %q, want empty", u.GetExternalId())
	}
	if u.GetProvisioned() {
		t.Errorf("wire provisioned = true, want false")
	}
	if u.GetTemporaryPasswordHash() != "" {
		t.Errorf("wire temporary_password_hash set, want empty")
	}
	if u.GetTemporaryPasswordValidUntil() != nil {
		t.Errorf("wire temporary_password_valid_until = %v, want unset", u.GetTemporaryPasswordValidUntil())
	}
}

func assertRowBaseline(t *testing.T, row *models.IDPUser) {
	t.Helper()
	if !row.Active {
		t.Errorf("row active = false, want true")
	}
	if row.DeletedAt != nil {
		t.Errorf("row deleted_at = %v, want nil", row.DeletedAt)
	}
	if row.ExternalID != "" {
		t.Errorf("row external_id = %q, want empty", row.ExternalID)
	}
	if row.Provisioned {
		t.Errorf("row provisioned = true, want false")
	}
	if row.TemporaryPasswordHash != "" {
		t.Errorf("row temporary_password_hash set, want empty")
	}
	if row.TemporaryPasswordValidUntil != nil {
		t.Errorf("row temporary_password_valid_until = %v, want nil", row.TemporaryPasswordValidUntil)
	}
}

// TestIDPUserLifecycleGraph walks the FULL F-28.3 state-transition graph
// over the Database service:
//
//	active ──deactivate──▶ deactivated ──activate──▶ active
//	    │                                         ▲
//	deprovision                                   │
//	    ▼                                         │
//	deprovisioned ────────────────reactivate──────┘
//
// asserting the row's fate (retention, tombstone, credential destruction,
// stable ID) at every edge.
func TestIDPUserLifecycleGraph(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	const email = "life-cycle@example.com"
	created, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{Email: email, PasswordHash: "standing-hash", Roles: []string{"admin"}},
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	id := created.GetId()

	row := func(label string) *models.IDPUser {
		t.Helper()
		var r models.IDPUser
		if err := db.Where("id = ?", id).First(&r).Error; err != nil {
			t.Fatalf("%s: load row: %v", label, err)
		}
		return &r
	}

	// --- Edge 1: active → deactivated ------------------------------------
	deact, err := client.DeactivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id})
	if err != nil {
		t.Fatalf("DeactivateIDPUser: %v", err)
	}
	if deact.GetActive() {
		t.Errorf("edge 1: wire active = true, want false")
	}
	r1 := row("edge 1")
	if r1.DeletedAt != nil {
		t.Errorf("edge 1: tombstone set (%v), want nil", r1.DeletedAt)
	}
	if r1.PasswordHash != "standing-hash" {
		t.Errorf("edge 1: password hash altered (%q), want retained", r1.PasswordHash)
	}
	if r1.Active {
		t.Errorf("edge 1: row active = true, want false")
	}

	// Idempotent: deactivating again is a no-op.
	if _, err := client.DeactivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id}); err != nil {
		t.Fatalf("edge 1 (again): %v", err)
	}

	// --- Edge 2: deactivated → active -------------------------------------
	act, err := client.ActivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id})
	if err != nil {
		t.Fatalf("ActivateIDPUser: %v", err)
	}
	if !act.GetActive() {
		t.Errorf("edge 2: wire active = false, want true")
	}
	r2 := row("edge 2")
	if r2.PasswordHash != "standing-hash" {
		t.Errorf("edge 2: password hash altered (%q), want untouched", r2.PasswordHash)
	}
	if !r2.Active {
		t.Errorf("edge 2: row active = false, want true")
	}

	// --- Edge 3: active → deprovisioned (soft delete) --------------------
	del, err := client.DeleteIDPUser(ctx, &dbpb.DeleteIDPUserRequest{Id: id})
	if err != nil {
		t.Fatalf("DeleteIDPUser: %v", err)
	}
	if del.GetActive() {
		t.Errorf("edge 3: wire active = true, want false")
	}
	if del.GetDeletedAt() == nil {
		t.Errorf("edge 3: wire deleted_at unset, want tombstone")
	}
	if del.GetPasswordHash() != "" {
		t.Errorf("edge 3: wire password_hash = %q, want zeroed", del.GetPasswordHash())
	}
	r3 := row("edge 3")
	if r3.DeletedAt == nil {
		t.Errorf("edge 3: row tombstone nil, want set")
	}
	if r3.PasswordHash != "" {
		t.Errorf("edge 3: row password hash = %q, want zeroed", r3.PasswordHash)
	}
	if r3.Active {
		t.Errorf("edge 3: row active = true, want false")
	}
	// Identity/history RETAINED: the row still exists with the same ID,
	// email, and roles.
	if r3.Email != email {
		t.Errorf("edge 3: row email = %q, want %q", r3.Email, email)
	}
	if len(r3.Roles) != 1 || r3.Roles[0] != "admin" {
		t.Errorf("edge 3: row roles = %v, want [admin]", r3.Roles)
	}

	// Idempotent: deleting again returns the tombstoned row unchanged.
	delAgain, err := client.DeleteIDPUser(ctx, &dbpb.DeleteIDPUserRequest{Id: id})
	if err != nil {
		t.Fatalf("edge 3 (again): %v", err)
	}
	if delAgain.GetId() != id || delAgain.GetDeletedAt() == nil {
		t.Errorf("edge 3 (again): unexpected row id=%q tombstone=nil?", delAgain.GetId())
	}

	// Ordinary GET treats the tombstoned user as NotFound.
	if _, err := client.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Id: id}); status.Code(err) != codes.NotFound {
		t.Errorf("edge 3: ordinary get = %v, want NotFound", err)
	}
	// Opt-in GET finds it, flagged.
	found, err := client.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Id: id, IncludeDeleted: true})
	if err != nil {
		t.Fatalf("edge 3: include_deleted get: %v", err)
	}
	if found.GetDeletedAt() == nil {
		t.Errorf("edge 3: include_deleted get lacks the tombstone")
	}

	// UPDATE refuses the tombstoned user (revival is Reactivate's job).
	if _, err := client.UpdateIDPUser(ctx, &dbpb.UpdateIDPUserRequest{
		User: &dbpb.IDPUser{Id: id, FirstName: "Ghost"},
	}); status.Code(err) != codes.NotFound {
		t.Errorf("edge 3: update tombstoned = %v, want NotFound", err)
	}

	// --- Edge 4: deprovisioned → re-provisioned (reactivate) -------------
	reac, err := client.ReactivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id})
	if err != nil {
		t.Fatalf("ReactivateIDPUser: %v", err)
	}
	if reac.GetId() != id {
		t.Errorf("edge 4: id = %q, want %q (STABLE)", reac.GetId(), id)
	}
	if !reac.GetActive() {
		t.Errorf("edge 4: wire active = false, want true")
	}
	if reac.GetDeletedAt() != nil {
		t.Errorf("edge 4: wire deleted_at = %v, want cleared", reac.GetDeletedAt())
	}
	if reac.GetPasswordHash() != "" {
		t.Errorf("edge 4: wire password_hash = %q, want zeroed (fresh credentials required)", reac.GetPasswordHash())
	}
	r4 := row("edge 4")
	if r4.DeletedAt != nil {
		t.Errorf("edge 4: row tombstone = %v, want cleared", r4.DeletedAt)
	}
	if !r4.Active {
		t.Errorf("edge 4: row active = false, want true")
	}
	if r4.PasswordHash != "" {
		t.Errorf("edge 4: row password hash = %q, want zeroed", r4.PasswordHash)
	}
	// History INTACT: email and roles survived the cycle.
	if r4.Email != email {
		t.Errorf("edge 4: row email = %q, want %q", r4.Email, email)
	}
	if len(r4.Roles) != 1 || r4.Roles[0] != "admin" {
		t.Errorf("edge 4: row roles = %v, want [admin]", r4.Roles)
	}

	// Idempotent: reactivating a non-tombstoned user merely activates.
	if _, err := client.ReactivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id}); err != nil {
		t.Fatalf("edge 4 (again): %v", err)
	}
}

// TestIDPUserListingVisibility checks the include_deleted flag on LIST:
// regular listings hide tombstoned rows; the opt-in view returns them
// flagged.
func TestIDPUserListingVisibility(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	a, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{Email: "visible@example.com", PasswordHash: "h", Roles: []string{"user"}},
	})
	if err != nil {
		t.Fatalf("CreateUser a: %v", err)
	}
	b, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{Email: "ghost@example.com", PasswordHash: "h", Roles: []string{"user"}},
	})
	if err != nil {
		t.Fatalf("CreateUser b: %v", err)
	}
	if _, err := client.DeleteIDPUser(ctx, &dbpb.DeleteIDPUserRequest{Id: b.GetId()}); err != nil {
		t.Fatalf("DeleteIDPUser b: %v", err)
	}

	// Regular listing: only the living user.
	living, err := client.ListIDPUsers(ctx, &dbpb.ListIDPUsersRequest{})
	if err != nil {
		t.Fatalf("ListIDPUsers: %v", err)
	}
	if len(living.GetUsers()) != 1 || living.GetUsers()[0].GetId() != a.GetId() {
		t.Errorf("living list = %v, want only [%s]", describeUsers(living.GetUsers()), a.GetId())
	}

	// Opt-in listing: both, ghost flagged with its tombstone.
	all, err := client.ListIDPUsers(ctx, &dbpb.ListIDPUsersRequest{IncludeDeleted: true})
	if err != nil {
		t.Fatalf("ListIDPUsers include_deleted: %v", err)
	}
	if len(all.GetUsers()) != 2 {
		t.Fatalf("all list = %d users, want 2", len(all.GetUsers()))
	}
	var ghost *dbpb.IDPUser
	for _, u := range all.GetUsers() {
		if u.GetId() == b.GetId() {
			ghost = u
		}
	}
	if ghost == nil {
		t.Fatalf("ghost missing from include_deleted listing")
	}
	if ghost.GetDeletedAt() == nil {
		t.Errorf("ghost lacks the tombstone flag")
	}
	if ghost.GetActive() {
		t.Errorf("ghost active = true, want false")
	}
}

// TestIDPUserFieldPersistenceRoundTrip checks that the new wire fields
// round-trip through CREATE (birth values) and that UPDATE respects the
// ownership split: profile fields (incl. external_id) are editable, while
// the lifecycle-owned fields (provisioned, temporal pair) survive a routine
// profile update UNCHANGED.
func TestIDPUserFieldPersistenceRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	windowEnd := mustTimestamp(t, "2026-11-03T12:00:00Z")
	prov, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{
			Email:                       "corp@example.com",
			Roles:                       []string{"user"},
			ExternalId:                  "azure-object-xyz",
			Provisioned:                 true,
			TemporaryPasswordHash:       "temp-hash",
			TemporaryPasswordValidUntil: windowEnd,
		},
	})
	if err != nil {
		t.Fatalf("CreateUser prov: %v", err)
	}
	assertBirthValues(t, prov)

	// Routine profile update (addresses NONE of the lifecycle fields):
	// every lifecycle value must come back bit-for-bit identical.
	upd, err := client.UpdateIDPUser(ctx, &dbpb.UpdateIDPUserRequest{
		User: &dbpb.IDPUser{Id: prov.GetId(), FirstName: "Carol"},
	})
	if err != nil {
		t.Fatalf("UpdateIDPUser: %v", err)
	}
	if upd.GetFirstName() != "Carol" {
		t.Errorf("first_name = %q, want Carol", upd.GetFirstName())
	}
	assertBirthValues(t, upd)

	// UPDATE: overwrite the external ID (editable profile field).
	upd2, err := client.UpdateIDPUser(ctx, &dbpb.UpdateIDPUserRequest{
		User: &dbpb.IDPUser{Id: prov.GetId(), ExternalId: "renamed-object"},
	})
	if err != nil {
		t.Fatalf("UpdateIDPUser ext: %v", err)
	}
	if upd2.GetExternalId() != "renamed-object" {
		t.Errorf("external_id = %q, want renamed-object", upd2.GetExternalId())
	}
}

// assertBirthValues pins the lifecycle fields as recorded at creation.
func assertBirthValues(t *testing.T, u *dbpb.IDPUser) {
	t.Helper()
	if !u.GetProvisioned() {
		t.Errorf("provisioned = false, want true")
	}
	if u.GetTemporaryPasswordHash() != "temp-hash" {
		t.Errorf("temp hash = %q, want temp-hash", u.GetTemporaryPasswordHash())
	}
	if u.GetTemporaryPasswordValidUntil() == nil {
		t.Errorf("temp valid-until unset, want set")
	}
}

// TestIDPUserLifecycleErrors checks the NotFound/InvalidArgument edges of
// the state RPCs.
func TestIDPUserLifecycleErrors(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	stateCalls := map[string]func(string) error{
		"activate": func(id string) error {
			_, e := client.ActivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id})
			return e
		},
		"deactivate": func(id string) error {
			_, e := client.DeactivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id})
			return e
		},
		"reactivate": func(id string) error {
			_, e := client.ReactivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: id})
			return e
		},
		"delete": func(id string) error { _, e := client.DeleteIDPUser(ctx, &dbpb.DeleteIDPUserRequest{Id: id}); return e },
	}
	for name, fn := range stateCalls {
		if err := fn(""); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s(empty id) = %v, want InvalidArgument", name, err)
		}
		if err := fn("bogus"); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s(non-numeric id) = %v, want InvalidArgument", name, err)
		}
		if err := fn("999999"); status.Code(err) != codes.NotFound {
			t.Errorf("%s(missing id) = %v, want NotFound", name, err)
		}
	}
}

func describeUsers(users []*dbpb.IDPUser) []string {
	out := make([]string, 0, len(users))
	for _, u := range users {
		out = append(out, u.GetId()+"("+u.GetEmail()+")")
	}
	return out
}
