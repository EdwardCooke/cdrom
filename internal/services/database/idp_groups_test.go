package database

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// strSet renders a slice as a comparable sorted set (helper for membership
// assertions, where order is incidental).
func strSet(vals []string) []string {
	out := append([]string(nil), vals...)
	sort.Strings(out)
	return out
}

// TestIDPGroundMigration asserts the F-28.5 tables exist after AutoMigrate.
// The junction table lives under its PINNED name (the sub-query fragments in
// idpGroupPushdownAttributes depend on exactly that); the groups table is
// checked via the model's OWN TableName() (whatever GORM inferred).
func TestIDPGroundMigration(t *testing.T) {
	_, db := startServerDB(t)
	if !db.Migrator().HasTable("idp_group_member") {
		t.Fatal("expected idp_group_member table after migration")
	}
	const table = "idp_groups"
	if !db.Migrator().HasTable(table) {
		t.Fatalf("expected %q table after migration", table)
	}
}

// TestIDPGroupCRUD exercises the group (F-28.5) lifecycle over the REAL gRPC
// client (bufconn): create (trimming + dedup) → duplicate display name is
// LEGAL → get round-trips the membership → list (plain) → replace (wholesale
// membership swap, incl. clearing) → patch (add/remove, idempotent) → delete
// (junction CASCADE) → recreate is legal.
func TestIDPGroupCRUD(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// Create trims blanks and dedupes values; external_id is accepted.
	g, err := client.CreateGroup(ctx, &dbpb.CreateGroupRequest{
		DisplayName:  "  Platform Eng ",
		Description:  "owns the platform",
		ExternalId:   "az-group-uuid-1",
		MemberValues: []string{"ext-alice", "ext-bob", " ext-alice", "", "   "},
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if g.GetDisplayName() != "Platform Eng" {
		t.Errorf("display_name trimmed = %q, want \"Platform Eng\"", g.GetDisplayName())
	}
	if g.GetExternalId() != "az-group-uuid-1" {
		t.Errorf("external_id = %q, want az-group-uuid-1", g.GetExternalId())
	}
	if got := strSet(g.GetMemberValues()); !reflect.DeepEqual(got, []string{"ext-alice", "ext-bob"}) {
		t.Errorf("members = %v, want [ext-alice ext-bob]", got)
	}
	id := g.GetId()
	if id == "" || id == "0" {
		t.Fatalf("unexpected group id %q", id)
	}

	// Duplicate display name is LEGAL (ruling 4) — distinct id, both listed.
	g2, err := client.CreateGroup(ctx, &dbpb.CreateGroupRequest{DisplayName: "Platform Eng"})
	if err != nil {
		t.Fatalf("CreateGroup(dup display name): %v", err)
	}
	if g2.GetId() == id {
		t.Errorf("dup display name got the SAME id %q, want distinct", g2.GetId())
	}
	all, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{})
	if err != nil {
		t.Fatalf("ListGroups(all): %v", err)
	}
	if len(all.GetGroups()) != 2 {
		t.Errorf("list = %d groups, want 2 (duplicate display names coexist)", len(all.GetGroups()))
	}

	// Get round-trips the membership (in insertion order).
	got, err := client.GetGroup(ctx, &dbpb.GetGroupRequest{Id: id})
	if err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	if ss := strSet(got.GetMemberValues()); !reflect.DeepEqual(ss, []string{"ext-alice", "ext-bob"}) {
		t.Errorf("get members = %v, want [ext-alice ext-bob]", got.GetMemberValues())
	}

	// Bad ids: non-numeric ⇒ InvalidArgument; unknown ⇒ NotFound.
	if _, err := client.GetGroup(ctx, &dbpb.GetGroupRequest{Id: "abc"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetGroup(non-numeric) code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := client.GetGroup(ctx, &dbpb.GetGroupRequest{Id: "999999"}); status.Code(err) != codes.NotFound {
		t.Errorf("GetGroup(unknown) code = %v, want NotFound", status.Code(err))
	}

	// Replace: wholesale membership swap + definite attributes.
	rep, err := client.ReplaceGroup(ctx, &dbpb.ReplaceGroupRequest{
		Id:           id,
		DisplayName:  "Renamed Team",
		Description:  "new desc",
		MemberValues: []string{"ext-carol"},
	})
	if err != nil {
		t.Fatalf("ReplaceGroup: %v", err)
	}
	if rep.GetDisplayName() != "Renamed Team" || rep.GetDescription() != "new desc" {
		t.Errorf("replace attrs = %q/%q, want renamed team / new desc", rep.GetDisplayName(), rep.GetDescription())
	}
	if got := rep.GetMemberValues(); len(got) != 1 || got[0] != "ext-carol" {
		t.Errorf("replace members = %v, want [ext-carol] (whole-sale swap)", got)
	}

	// Replace with EMPTY membership clears it (PUT semantics).
	cleared, err := client.ReplaceGroup(ctx, &dbpb.ReplaceGroupRequest{Id: id, DisplayName: "Empty Team"})
	if err != nil {
		t.Fatalf("ReplaceGroup(clear): %v", err)
	}
	if len(cleared.GetMemberValues()) != 0 {
		t.Errorf("cleared members = %v, want none", cleared.GetMemberValues())
	}
	if cleared.GetExternalId() != "az-group-uuid-1" {
		t.Errorf("external_id after replace = %q, want unchanged (immutable)", cleared.GetExternalId())
	}
	// external_id is IMMUTABLE: a differing value ⇒ InvalidArgument; an
	// equal value is a no-op.
	if _, err := client.ReplaceGroup(ctx, &dbpb.ReplaceGroupRequest{
		Id: id, DisplayName: "Empty Team", ExternalId: "DIFFERENT-IDENTITY",
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("ReplaceGroup(diff external_id) code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := client.ReplaceGroup(ctx, &dbpb.ReplaceGroupRequest{
		Id: id, DisplayName: "Empty Team", ExternalId: "az-group-uuid-1",
	}); err != nil {
		t.Errorf("ReplaceGroup(same external_id) = %v, want a no-op", err)
	}
	if _, err := client.ReplaceGroup(ctx, &dbpb.ReplaceGroupRequest{Id: "999999"}); status.Code(err) != codes.NotFound {
		t.Errorf("ReplaceGroup(unknown) code = %v, want NotFound", status.Code(err))
	}

	// Patch: add + remove in one op; idempotent both directions.
	patched, err := client.PatchGroupMembers(ctx, &dbpb.PatchGroupMembersRequest{
		Id:     id,
		Add:    []string{"ext-dave", "ext-erin"},
		Remove: []string{"ghost-value"},
	})
	if err != nil {
		t.Fatalf("PatchGroupMembers: %v", err)
	}
	if got := strSet(patched.GetMemberValues()); !reflect.DeepEqual(got, []string{"ext-dave", "ext-erin"}) {
		t.Errorf("patched members = %v, want [ext-dave ext-erin]", got)
	}
	again, err := client.PatchGroupMembers(ctx, &dbpb.PatchGroupMembersRequest{
		Id: id, Add: []string{"ext-dave"}, Remove: []string{"ext-erin"},
	})
	if err != nil {
		t.Fatalf("PatchGroupMembers(repeat): %v", err)
	}
	if got := strSet(again.GetMemberValues()); !reflect.DeepEqual(got, []string{"ext-dave"}) {
		t.Errorf("repeat patch members = %v, want [ext-dave] (adds idempotent, removes hit)", got)
	}
	if _, err := client.PatchGroupMembers(ctx, &dbpb.PatchGroupMembersRequest{Id: id}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("PatchGroupMembers(empty) code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := client.PatchGroupMembers(ctx, &dbpb.PatchGroupMembersRequest{Id: "999999", Add: []string{"x"}}); status.Code(err) != codes.NotFound {
		t.Errorf("PatchGroupMembers(unknown) code = %v, want NotFound", status.Code(err))
	}

	// Delete: hard, cascades the junction rows; recreate afterwards is legal.
	if _, err := client.DeleteGroup(ctx, &dbpb.DeleteGroupRequest{Id: id}); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	if _, err := client.GetGroup(ctx, &dbpb.GetGroupRequest{Id: id}); status.Code(err) != codes.NotFound {
		t.Errorf("GetGroup(after delete) code = %v, want NotFound", status.Code(err))
	}
	rec, err := client.CreateGroup(ctx, &dbpb.CreateGroupRequest{DisplayName: "Recycled"})
	if err != nil {
		t.Fatalf("CreateGroup(recycle): %v", err)
	}
	if rec.GetId() == id {
		t.Logf("(info) recycled group happened to reuse id %q — acceptable (PK reuse)", rec.GetId())
	}
	if _, err := client.DeleteGroup(ctx, &dbpb.DeleteGroupRequest{Id: "999999"}); status.Code(err) != codes.NotFound {
		t.Errorf("DeleteGroup(unknown) code = %v, want NotFound", status.Code(err))
	}
}

// TestIDPGroupCascade checks the junction cleanup DIRECTLY on the rows (the
// gRPC surface hides the intermediate state): deleting a group leaves no
// dangling member rows.
func TestIDPGroupCascade(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	g, err := client.CreateGroup(ctx, &dbpb.CreateGroupRequest{
		DisplayName: "Cascade Me", MemberValues: []string{"v1", "v2", "v3"},
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	cntBefore := int64(0)
	if err := db.Model(&models.IDPGroupMember{}).Where("group_id = ?", g.GetId()).Count(&cntBefore).Error; err != nil {
		t.Fatalf("count before: %v", err)
	}
	if cntBefore != 3 {
		t.Fatalf("junction rows before delete = %d, want 3", cntBefore)
	}
	if _, err := client.DeleteGroup(ctx, &dbpb.DeleteGroupRequest{Id: g.GetId()}); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	cntAfter := int64(0)
	if err := db.Model(&models.IDPGroupMember{}).Where("group_id = ?", g.GetId()).Count(&cntAfter).Error; err != nil {
		t.Fatalf("count after: %v", err)
	}
	if cntAfter != 0 {
		t.Errorf("junction rows after delete = %d, want 0 (CASCADE)", cntAfter)
	}
}

// TestIDPGroupListPredices asserts the SERVER-SIDE narrowing (ruling 5):
// display_name equality (exact + loose), the relational members_value
// predicate (exact + loose), AND-chaining, and the unknown-attribute
// rejection.
func TestIDPGroupListPredicates(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	names := map[string][]string{
		"Alpha Squad": {"aa-1", "zz-9"},
		"Beta Squad":  {"bb-1"},
		// Mixed-case DISPLAY name (targets the loose display_name arm) and an
		// OVERLAPPING member value aa-1 (targets the members_value arms).
		"GAMMA squad": {"gg-1", "aa-1"},
		"No Members":  {},
	}
	externals := map[string]string{
		"Alpha Squad": "EXT-alpha",
		"Beta Squad":  "EXT-beta",
	}
	ids := map[string]string{}
	for name, members := range names {
		g, err := client.CreateGroup(ctx, &dbpb.CreateGroupRequest{
			DisplayName: name, MemberValues: members, ExternalId: externals[name],
		})
		if err != nil {
			t.Fatalf("seed %q: %v", name, err)
		}
		ids[name] = g.GetId()
	}

	assertIDs := func(label string, preds []*dbpb.ListGroupsRequest_Predicate, want ...string) {
		t.Helper()
		resp, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: preds})
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		var got []string
		for _, grp := range resp.GetGroups() {
			got = append(got, grp.GetId())
		}
		sort.Strings(got)
		wants := append([]string(nil), want...)
		sort.Strings(wants)
		if !reflect.DeepEqual(got, wants) {
			t.Errorf("%s: ids = %v, want %v", label, got, wants)
		}
	}

	// Plain list: everyone.
	assertIDs("plain", nil, ids["Alpha Squad"], ids["Beta Squad"], ids["GAMMA squad"], ids["No Members"])

	// display_name EXACT.
	respExact, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: []*dbpb.ListGroupsRequest_Predicate{
		{Attribute: "display_name", CaseSensitive: true, Value: "GAMMA squad"},
	}})
	if err != nil {
		t.Fatalf("display_name exact: %v", err)
	}
	if len(respExact.GetGroups()) != 1 || respExact.GetGroups()[0].GetId() != ids["GAMMA squad"] {
		t.Errorf("display_name exact = %v, want only GAMMA squad", respExact.GetGroups())
	}

	// display_name LOOSE (uppercase query matches mixed-case row).
	respLoose, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: []*dbpb.ListGroupsRequest_Predicate{
		{Attribute: "display_name", CaseSensitive: false, Value: "gamma squad"},
	}})
	if err != nil {
		t.Fatalf("display_name loose: %v", err)
	}
	if len(respLoose.GetGroups()) != 1 || respLoose.GetGroups()[0].GetId() != ids["GAMMA squad"] {
		t.Errorf("display_name loose = %v, want only GAMMA squad", respLoose.GetGroups())
	}

	// external_id EXACT (provider identifiers are case-meaning): the
	// case-sensitive arm matches; the looser arm does NOT (EXTERNALS are
	// exact — the IdP's translator pins caseSensitive=true for them).
	respExtCS, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: []*dbpb.ListGroupsRequest_Predicate{
		{Attribute: "external_id", CaseSensitive: true, Value: "EXT-alpha"},
	}})
	if err != nil {
		t.Fatalf("external_id CS: %v", err)
	}
	if len(respExtCS.GetGroups()) != 1 || respExtCS.GetGroups()[0].GetId() != ids["Alpha Squad"] {
		t.Errorf("external_id CS = %v, want only Alpha Squad", respExtCS.GetGroups())
	}
	respExtLoose, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: []*dbpb.ListGroupsRequest_Predicate{
		{Attribute: "external_id", CaseSensitive: false, Value: "ext-BETA"},
	}})
	if err != nil {
		t.Fatalf("external_id loose: %v", err)
	}
	if len(respExtLoose.GetGroups()) != 1 || respExtLoose.GetGroups()[0].GetId() != ids["Beta Squad"] {
		t.Errorf("external_id loose = %v, want only Beta squad", respExtLoose.GetGroups())
	}
	// Unknown external_id ⇒ empty (no leak of other groups).
	respExtMiss, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: []*dbpb.ListGroupsRequest_Predicate{
		{Attribute: "external_id", CaseSensitive: true, Value: "EXT-none"},
	}})
	if err != nil {
		t.Fatalf("external_id miss: %v", err)
	}
	if len(respExtMiss.GetGroups()) != 0 {
		t.Errorf("external_id miss = %v, want NONE", respExtMiss.GetGroups())
	}

	// members_value EXACT (relational): aa-1 ∈ Alpha Squad and GAMMA squad.
	mvResp, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: []*dbpb.ListGroupsRequest_Predicate{
		{Attribute: "members_value", CaseSensitive: true, Value: "aa-1"},
	}})
	if err != nil {
		t.Fatalf("members_value exact: %v", err)
	}
	var mvIds []string
	for _, grp := range mvResp.GetGroups() {
		mvIds = append(mvIds, grp.GetId())
	}
	sort.Strings(mvIds)
	if !reflect.DeepEqual(mvIds, []string{ids["Alpha Squad"], ids["GAMMA squad"]}) {
		t.Errorf("members_value exact = %v, want alpha + gamma squads", mvIds)
	}

	// members_value LOOSE (uppercased value matches lowercase row).
	mvLoose, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: []*dbpb.ListGroupsRequest_Predicate{
		{Attribute: "members_value", CaseSensitive: false, Value: "BB-1"},
	}})
	if err != nil {
		t.Fatalf("members_value loose: %v", err)
	}
	if len(mvLoose.GetGroups()) != 1 || mvLoose.GetGroups()[0].GetId() != ids["Beta Squad"] {
		t.Errorf("members_value loose = %v, want beta squad", mvLoose.GetGroups())
	}

	// AND-chain: display_name + members_value intersect.
	andResp, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: []*dbpb.ListGroupsRequest_Predicate{
		{Attribute: "display_name", CaseSensitive: true, Value: "GAMMA squad"},
		{Attribute: "members_value", CaseSensitive: true, Value: "zz-9"},
	}})
	if err != nil {
		t.Fatalf("AND-chain: %v", err)
	}
	if len(andResp.GetGroups()) != 0 {
		t.Errorf("AND-chain (disjoint values) = %v, want NONE", andResp.GetGroups())
	}
	andOk, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: []*dbpb.ListGroupsRequest_Predicate{
		{Attribute: "display_name", CaseSensitive: true, Value: "GAMMA squad"},
		{Attribute: "members_value", CaseSensitive: true, Value: "aa-1"},
	}})
	if err != nil {
		t.Fatalf("AND-chain(ok): %v", err)
	}
	if len(andOk.GetGroups()) != 1 || andOk.GetGroups()[0].GetId() != ids["GAMMA squad"] {
		t.Errorf("AND-chain(ok) = %v, want gamma squad", andOk.GetGroups())
	}

	// Unknown attribute ⇒ InvalidArgument (finite algebra).
	if _, err := client.ListGroups(ctx, &dbpb.ListGroupsRequest{GenericPredicate: []*dbpb.ListGroupsRequest_Predicate{
		{Attribute: "bogus_attr", Value: "x"},
	}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("unknown attribute code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestRoleBindingOrigin asserts the F-28.5 provenance thread: the default
// origin is "manual", "scim_group" round-trips through the RPCs, and an
// unknown origin is rejected (typos must not mint a new bucket).
func TestRoleBindingOrigin(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	def, err := client.AddRoleBinding(ctx, &dbpb.AddRoleBindingRequest{
		PrincipalKind: "user", PrincipalId: "1", RoleName: "operator",
	})
	if err != nil {
		t.Fatalf("AddRoleBinding(default): %v", err)
	}
	if def.GetOrigin() != "manual" {
		t.Errorf("default origin = %q, want \"manual\"", def.GetOrigin())
	}

	scim, err := client.AddRoleBinding(ctx, &dbpb.AddRoleBindingRequest{
		PrincipalKind: "user", PrincipalId: "1", RoleName: "viewer", Origin: "scim_group",
	})
	if err != nil {
		t.Fatalf("AddRoleBinding(scim_group): %v", err)
	}
	if scim.GetOrigin() != "scim_group" {
		t.Errorf("scim origin = %q, want \"scim_group\"", scim.GetOrigin())
	}

	if _, err := client.AddRoleBinding(ctx, &dbpb.AddRoleBindingRequest{
		PrincipalKind: "user", PrincipalId: "1", RoleName: "admin", Origin: "galaxy_brain",
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad origin code = %v, want InvalidArgument", status.Code(err))
	}

	// List reflects origins per binding.
	list, err := client.ListRoleBindings(ctx, &dbpb.ListRoleBindingsRequest{PrincipalId: "1"})
	if err != nil {
		t.Fatalf("ListRoleBindings: %v", err)
	}
	origins := map[string]string{}
	for _, b := range list.GetBindings() {
		origins[b.GetRoleName()] = b.GetOrigin()
	}
	if origins["operator"] != "manual" || origins["viewer"] != "scim_group" {
		t.Errorf("listed origins = %v, want operator=manual viewer=scim_group", origins)
	}
}
