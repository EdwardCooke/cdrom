package database

import (
	"context"
	"sort"
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

	username := "migdefaults"
	created, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{Username: username, Email: "migration-defaults@example.com", PasswordHash: "h", Roles: []string{"user"}},
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	assertWireBaseline(t, created)

	var row models.IDPUser
	if err := db.Where("username = ?", username).First(&row).Error; err != nil {
		t.Fatalf("load row: %v", err)
	}
	assertRowBaseline(t, &row)
	if row.Username != username {
		t.Errorf("row username = %q, want %q", row.Username, username)
	}
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

	const username = "lifecycler"
	const email = "life-cycle@example.com"
	created, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{Username: username, Email: email, PasswordHash: "standing-hash", Roles: []string{"admin"}},
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
	// username, email, and roles.
	if r3.Username != username {
		t.Errorf("edge 3: row username = %q, want %q", r3.Username, username)
	}
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
	// History INTACT: username, email, and roles survived the cycle.
	if r4.Username != username {
		t.Errorf("edge 4: row username = %q, want %q", r4.Username, username)
	}
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
		User: &dbpb.IDPUser{Username: "visible", Email: "visible@example.com", PasswordHash: "h", Roles: []string{"user"}},
	})
	if err != nil {
		t.Fatalf("CreateUser a: %v", err)
	}
	b, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{
		User: &dbpb.IDPUser{Username: "ghost", Email: "ghost@example.com", PasswordHash: "h", Roles: []string{"user"}},
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
			Username:                    "corp",
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
		out = append(out, u.GetId()+"("+u.GetUsername()+")")
	}
	return out
}

// TestIDPUserListPushdown certifies the generic equality pushdown (F-28.4
// Slice 2 refinement): predicates evaluate IN THE DATABASE (the caller sees
// only matching rows), case sensitivity is pinned per predicate, booleans
// compare literally, and unknown attributes are InvalidArgument.
func TestIDPUserListPushdown(t *testing.T) {
	client, _ := startServerDB(t)
	ctx := context.Background()
	// All THREE are born active (creation semantics); β is driven inactive
	// afterwards through the OWNING channel (below).
	seed := []dbpb.IDPUser{
		{Username: "alpha.one", FirstName: "Ann", LastName: "Alpha", Email: "ann@a.x", ExternalId: "EXT-A", Provisioned: true},
		{Username: "beta.two", FirstName: "Bob", LastName: "Beta", Email: "bob@b.x", ExternalId: "EXT-B"},
		{Username: "gamma.three", FirstName: "Cy", LastName: "Gamma", Email: "cy@c.x", ExternalId: "EXT-C"},
	}
	for si := range seed {
		if _, err := client.CreateUser(ctx, &dbpb.CreateIDPUserRequest{User: &seed[si]}); err != nil {
			t.Fatalf("CreateUser(%s): %v", seed[si].Username, err)
		}
	}
	// Creation settles EVERYONE active (birth semantics — the state RPCs own
	// the active bit); drive β's deactivation through ITS owning channel.
	deactivated, err := client.DeactivateIDPUser(ctx, &dbpb.UserStateChangeRequest{Id: userIdOf(t, client, ctx, "beta.two")})
	if err != nil || deactivated.GetActive() {
		t.Fatalf("drive β inactive via DeactivateIDPUser: %v (active=%v)", err, deactivated.GetActive())
	}
	pred := func(attr string, cs bool, val string) *dbpb.ListIDPUsersRequest_Predicate {
		return &dbpb.ListIDPUsersRequest_Predicate{Attribute: attr, CaseSensitive: cs, Value: val}
	}
	listWith := func(ps ...*dbpb.ListIDPUsersRequest_Predicate) []string {
		resp, err := client.ListIDPUsers(ctx, &dbpb.ListIDPUsersRequest{GenericPredicate: ps})
		if err != nil {
			t.Fatalf("ListIDPUsers(%v): %v", ps, err)
		}
		got := map[string]bool{}
		for _, u := range resp.GetUsers() {
			got[u.GetUsername()] = true
		}
		return sortedKeys(got)
	}
	check := func(label string, want []string, ps ...*dbpb.ListIDPUsersRequest_Predicate) {
		t.Run(label, func(t *testing.T) {
			if got := listWith(ps...); !sliceEq(got, want) {
				t.Errorf("pushdown %s: got %v, want %v", label, got, want)
			}
		})
	}
	// Baseline: no predicate ⇒ everyone (ordered by id).
	check("none", []string{"alpha.one", "beta.two", "gamma.three"})
	// Case-INSENSITIVE username (loose, the directory default).
	check("username-loose", []string{"alpha.one"}, pred("username", false, "ALPHA.ONE"))
	// Case-SENSITIVE username (tight — misses the differently-cased posting).
	check("username-tight-miss", []string{}, pred("username", true, "Alpha.One"))
	// Exact externalId (always literal).
	check("external-id", []string{"beta.two"}, pred("external_id", true, "EXT-B"))
	// Boolean literalness: "true" ≡ the constant; ANYTHING ELSE is
	// InvalidArgument (garbage must not silently widen the scan).
	check("active-true", []string{"alpha.one", "gamma.three"}, pred("active", false, "true"))
	if _, err := client.ListIDPUsers(ctx, &dbpb.ListIDPUsersRequest{GenericPredicate: []*dbpb.ListIDPUsersRequest_Predicate{pred("active", false, "True")}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("non-literal boolean constant: err=%v, want InvalidArgument", err)
	}
	// Compound AND.
	check("compound-and", []string{"alpha.one"}, pred("active", false, "true"), pred("provisioned", false, "true"))
	// Name-column pushdown (still evaluated in the database).
	check("first-name", []string{"gamma.three"}, pred("first_name", false, "CY"))
	// Unknown attribute ⇒ InvalidArgument (loud, not a widened scan).
	if _, err := client.ListIDPUsers(ctx, &dbpb.ListIDPUsersRequest{GenericPredicate: []*dbpb.ListIDPUsersRequest_Predicate{pred("bogus_attr", false, "x")}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("unknown attribute: err=%v, want InvalidArgument", err)
	}
	// COUNT RIDES THE SAME CONDITIONS (aggregate — no rows transferred):
	// the tally must agree with the list's narrowing on every predicate.
	countOf := func(ps ...*dbpb.ListIDPUsersRequest_Predicate) int32 {
		resp, err := client.CountIDPUsers(ctx, &dbpb.ListIDPUsersRequest{GenericPredicate: ps})
		if err != nil {
			t.Fatalf("CountIDPUsers(%v): %v", ps, err)
		}
		return resp.GetCount()
	}
	if got := countOf(); got != 3 {
		t.Errorf("count(all) = %d, want 3", got)
	}
	if got := countOf(pred("active", false, "true")); got != 2 {
		t.Errorf("count(active=true) = %d, want 2 (α γ)", got)
	}
	if got := countOf(pred("active", false, "true"), pred("provisioned", false, "true")); got != 1 {
		t.Errorf("count(compound AND) = %d, want 1 (α)", got)
	}
	if got := countOf(pred("username", false, "GAMMA.THREE")); got != 1 {
		t.Errorf("count(casefolded username) = %d, want 1 (γ)", got)
	}
	if got := countOf(pred("external_id", true, "EXT-B")); got != 1 {
		t.Errorf("count(exact externalId) = %d, want 1 (β)", got)
	}
}

// userIdOf resolves a username to its stable id (through the PUBLIC getter —
// the test exercises the wire, not the ORM).
func userIdOf(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, username string) string {
	t.Helper()
	resp, err := client.GetIDPUser(ctx, &dbpb.GetIDPUserRequest{Username: username})
	if err != nil {
		t.Fatalf("lookup %s: %v", username, err)
	}
	return resp.GetId()
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sliceEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
