// F-28.5 Slice 4 — the IdP's enriched group-detail RPCs (the home of the
// /api/groups bridge). Coverage: member RESOLUTION (live ⇒ username, ghost
// ⇒ null), the derived_roles PROJECTION (mapped + catalog-present ⇒ listed;
// catalog-missing ⇒ excluded), the Unimplemented degradation (group store
// unwired), and the NotFound passthrough.
package idp

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/config"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// grpDetailWorld boots the gRPC face over a REAL Database client (the role
// catalog) and MEMORY user/group stores (fast, deterministic).
type grpDetailWorld struct {
	t      *testing.T
	g      *GRPCServer
	users  *MemoryUserStore
	groups *MemoryGroupStore
}

func newGRPDetailWorld(t *testing.T, mappings map[string]string) *grpDetailWorld {
	t.Helper()
	db := newRealDBClient(t)
	cfg := config.IdPConfig{Scim: config.ScimConfig{GroupRoleMappings: mappings}}
	users := NewMemoryUserStore()
	groups := NewMemoryGroupStore()
	g := NewGRPCServer(cfg, nil, users, testLogger())
	g.SetGroupStore(groups)
	g.SetDatabaseClient(db)
	return &grpDetailWorld{t: t, g: g, users: users, groups: groups}
}

func (w *grpDetailWorld) addUser(username, externalID string) {
	w.t.Helper()
	if _, err := w.users.Create(context.Background(), &User{
		Username: username, ExternalID: externalID, Roles: []string{scimStockRolePlaceholder}, Active: true,
	}); err != nil {
		w.t.Fatalf("add user %q: %v", username, err)
	}
}

func (w *grpDetailWorld) addGroup(displayName string, members ...string) *SCIMGroup {
	w.t.Helper()
	g, err := w.groups.Create(context.Background(), displayName, "desc", "ext-"+displayName, members)
	if err != nil {
		w.t.Fatalf("add group %q: %v", displayName, err)
	}
	return g
}

// TestGroupDetails_Enrichment proves the two enrichment dimensions: member
// resolution (live username vs null ghost) and the derived_roles projection
// (listed when the mapped role is in the catalog).
func TestGroupDetails_Enrichment(t *testing.T) {
	w := newGRPDetailWorld(t, map[string]string{
		"Pod":     "viewer",  // catalog-present ⇒ projected
		"Orphans": "no-such", // catalog-missing ⇒ excluded (ruling 4)
	})
	w.addUser("alice", "ext-alice")
	w.addGroup("Pod", "ext-alice", "ghost-value") // ghost ⇒ unresolvable
	w.addGroup("Orphans", "ext-alice")

	resp, err := w.g.ListGroupDetails(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListGroupDetails: %v", err)
	}
	if len(resp.GetGroups()) != 2 {
		t.Fatalf("groups = %d, want 2", len(resp.GetGroups()))
	}
	byName := map[string]int{}
	for i, gd := range resp.GetGroups() {
		byName[gd.GetDisplayName()] = i
	}

	// POD: resolution + projection.
	pod := resp.GetGroups()[byName["Pod"]]
	if pod.GetId() == "" || pod.GetDisplayName() != "Pod" || pod.GetExternalId() != "ext-Pod" {
		t.Errorf("pod scalars wrong: id=%q name=%q ext=%q", pod.GetId(), pod.GetDisplayName(), pod.GetExternalId())
	}
	if len(pod.GetMemberValues()) != 2 {
		t.Fatalf("pod member_values = %v, want 2", pod.GetMemberValues())
	}
	if len(pod.GetMembers()) != 2 {
		t.Fatalf("pod members = %d, want 2", len(pod.GetMembers()))
	}
	// Parallel order: [ext-alice, ghost-value].
	if pod.GetMembers()[0].GetValue() != "ext-alice" {
		t.Errorf("members[0].value = %q, want ext-alice", pod.GetMembers()[0].GetValue())
	}
	if u := pod.GetMembers()[0].GetUsername(); u != "alice" {
		t.Errorf("members[0].username = %q, want alice (resolved)", u)
	}
	if pod.GetMembers()[1].GetValue() != "ghost-value" {
		t.Errorf("members[1].value = %q, want ghost-value", pod.GetMembers()[1].GetValue())
	}
	if pod.GetMembers()[1].GetUsername() != "" {
		t.Errorf("members[1].username = %q, want EMPTY (unresolvable ⇒ null)", pod.GetMembers()[1].GetUsername())
	}
	if len(pod.GetDerivedRoles()) != 1 || pod.GetDerivedRoles()[0] != "viewer" {
		t.Errorf("pod derived_roles = %v, want [viewer]", pod.GetDerivedRoles())
	}

	// ORPHANS: mapped role missing from the catalog ⇒ NO projection.
	orphan := resp.GetGroups()[byName["Orphans"]]
	if len(orphan.GetDerivedRoles()) != 0 {
		t.Errorf("orphans derived_roles = %v, want EMPTY (catalog-missing role excluded)", orphan.GetDerivedRoles())
	}
}

// TestGroupDetails_GetById proves the single-item RPC and the NotFound
// passthrough (a missing id surfaces as codes.NotFound, which the API maps
// to 404).
func TestGroupDetails_GetById(t *testing.T) {
	w := newGRPDetailWorld(t, map[string]string{"Solo": "operator"})
	w.addUser("zed", "ext-zed")
	g := w.addGroup("Solo", "ext-zed")

	got, err := w.g.GetGroupDetails(context.Background(), &idppb.GetGroupDetailsRequest{Id: g.ID})
	if err != nil {
		t.Fatalf("GetGroupDetails(hit): %v", err)
	}
	if got.GetId() != g.ID || got.GetDisplayName() != "Solo" {
		t.Errorf("get = %q/%q, want %q/Solo", got.GetId(), got.GetDisplayName(), g.ID)
	}
	if len(got.GetDerivedRoles()) != 1 || got.GetDerivedRoles()[0] != "operator" {
		t.Errorf("solo derived_roles = %v, want [operator]", got.GetDerivedRoles())
	}

	// Miss ⇒ NotFound.
	if _, err := w.g.GetGroupDetails(context.Background(), &idppb.GetGroupDetailsRequest{Id: "999"}); status.Code(err) != codes.NotFound {
		t.Errorf("GetGroupDetails(miss) err = %v, want NotFound", err)
	}
}

// TestGroupDetails_Unimplemented proves the degradation: with the group
// store unwired the RPCs answer Unimplemented (the API maps it to 501; the
// surface is simply not attached — typical when SCIM is disabled).
func TestGroupDetails_Unimplemented(t *testing.T) {
	db := newRealDBClient(t)
	cfg := config.IdPConfig{Scim: config.ScimConfig{GroupRoleMappings: map[string]string{"x": "viewer"}}}
	g := NewGRPCServer(cfg, nil, NewMemoryUserStore(), testLogger())
	g.SetDatabaseClient(db) // db attached, but NO group store
	if _, err := g.ListGroupDetails(context.Background(), nil); status.Code(err) != codes.Unimplemented {
		t.Errorf("ListGroupDetails(no store) err = %v, want Unimplemented", err)
	}
	if _, err := g.GetGroupDetails(context.Background(), &idppb.GetGroupDetailsRequest{Id: "1"}); status.Code(err) != codes.Unimplemented {
		t.Errorf("GetGroupDetails(no store) err = %v, want Unimplemented", err)
	}
}
