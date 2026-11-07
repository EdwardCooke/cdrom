// F-28.5 Slice 3 — the group→role mapping applicator.
//
// Coverage (per the spec's unit matrix): grant-on-join, union across
// groups, keep-on-leaving-one-of-two, strip-on-last-group, operator-grant
// SURVIVAL (manual-origin bindings untouched through a full recompute
// cycle), missing-role skip (warn-logged, never created), zero-diff replay
// publishing NOTHING, group-delete revocation, deprovisioning NEUTRALITY
// (tombstone ⇒ no pruning, no recomputation), and the embedded-`groups`
// switchover (gap-fill on existing groups; phantoms ignored + logged; the
// stock `user` placeholder augmented, never displaced).
//
// The world: a REAL in-process Database service (SQLite, bufconn — the
// origin bookkeeping exercises the actual migration/column) driven through
// the IdP's memory stores, so the applicator's binding lane and the
// role-change publication are genuine.
package idp

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/services/database"
)

// ---------------------------------------------------------------------------
// World
// ---------------------------------------------------------------------------

// pubCounter wraps the Database client counting PublishRoleChange calls
// (the applicator's publications — the ruling-1 observability seam).
type pubCounter struct {
	dbpb.DatabaseClient
	n *int
}

func (p *pubCounter) PublishRoleChange(ctx context.Context, in *dbpb.PublishRoleChangeRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	*p.n++
	return p.DatabaseClient.PublishRoleChange(ctx, in, opts...)
}

// scimMapWorld bundles the applicator's test fixtures: the real Database
// service (bindings + catalog + publications), the memory stores, the
// server, and the HTTP facade.
type scimMapWorld struct {
	t            *testing.T
	db           dbpb.DatabaseClient // DIRECT lane (world helpers bypass the counter)
	srv          *Server
	users        *MemoryUserStore
	groups       *MemoryGroupStore
	aud          *capSCIMAuditor
	ts           *httptest.Server
	publications int
}

// newRealDBClient boots a REAL in-process Database service (SQLite temp
// file, migrated, built-in roles SEEDED — mirroring the database package's
// own test recipe, assembled from its exported surface) over a bufconn
// listener. The applicator's binding lane therefore exercises the ACTUAL
// origin-column bookkeeping (migration + normalization), not a mock.
func newRealDBClient(t *testing.T) dbpb.DatabaseClient {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := database.Open(database.Config{Backend: database.BackendSQLite, SQLitePath: dbPath}, logger)
	if err != nil {
		t.Fatalf("database.Open: %v", err)
	}
	t.Cleanup(func() { _ = database.Close(db) })
	if err := database.Migrate(db, logger); err != nil {
		t.Fatalf("database.Migrate: %v", err)
	}
	srvImpl := database.NewServer(db)
	if err := srvImpl.SeedBuiltInRoles(context.Background()); err != nil {
		t.Fatalf("SeedBuiltInRoles: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	dbpb.RegisterDatabaseServer(grpcSrv, srvImpl)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)
	conn, err := grpc.DialContext(context.Background(), "bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("dial database service: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return dbpb.NewDatabaseClient(conn)
}

// newScimMapWorld boots the world with the given group→role mapping table.
// The applicator's Database client is the PUBLICATION-COUNTING wrapper; the
// world's own helpers use the direct client.
func newScimMapWorld(t *testing.T, mappings map[string]string) *scimMapWorld {
	t.Helper()
	client := newRealDBClient(t)
	w := &scimMapWorld{t: t, db: client}

	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Scim = config.ScimConfig{Enabled: true, GroupRoleMappings: mappings}
	km, err := NewKeyManager(cfg, NewMemoryKeyStore(), testLogger())
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	w.srv = NewServer(cfg, km, NewMemoryAuthCodeStore(), testLogger())
	w.users = NewMemoryUserStore()
	w.groups = NewMemoryGroupStore()
	w.aud = &capSCIMAuditor{}
	w.srv.SetScimUserBacking(w.users, w.aud)
	w.srv.SetScimGroupBacking(w.groups)
	w.srv.SetScimAuthorizer(&scimPermissiveAuthorizer{})
	w.srv.SetDatabaseClient(&pubCounter{DatabaseClient: client, n: &w.publications})
	w.ts = httptest.NewServer(w.srv.Handler())
	t.Cleanup(w.ts.Close)
	return w
}

// seedUser creates a LIVE user (optional external id) with the STOCK
// PLACEHOLDER role (the SCIM provisioning norm) and returns the row.
func (w *scimMapWorld) seedUser(username, externalID string) *User {
	w.t.Helper()
	u, err := w.users.Create(context.Background(), &User{
		Username:   username,
		ExternalID: externalID,
		Roles:      []string{scimStockRolePlaceholder},
		Active:     true,
	})
	if err != nil {
		w.t.Fatalf("seed user %q: %v", username, err)
	}
	return u
}

// seedGroup creates a group with the given membership VALUES.
func (w *scimMapWorld) seedGroup(displayName string, members ...string) *SCIMGroup {
	w.t.Helper()
	g, err := w.groups.Create(context.Background(), displayName, "", "", members)
	if err != nil {
		w.t.Fatalf("seed group %q: %v", displayName, err)
	}
	return g
}

// bindings splits the user's bindings by ORIGIN (role → binding id).
func (w *scimMapWorld) bindings(username string) (manual, scim map[string]int64) {
	w.t.Helper()
	resp, err := w.db.ListRoleBindings(context.Background(), &dbpb.ListRoleBindingsRequest{
		PrincipalKind: "user", PrincipalId: username,
	})
	if err != nil {
		w.t.Fatalf("ListRoleBindings(%s): %v", username, err)
	}
	manual, scim = map[string]int64{}, map[string]int64{}
	for _, b := range resp.GetBindings() {
		if b.GetOrigin() == scimGroupBindingOrigin {
			scim[b.GetRoleName()] = b.GetId()
		} else {
			manual[b.GetRoleName()] = b.GetId()
		}
	}
	return manual, scim
}

// catalogNames snapshots the role catalog.
func (w *scimMapWorld) catalogNames() map[string]bool {
	w.t.Helper()
	resp, err := w.db.ListRoles(context.Background(), &dbpb.ListRolesRequest{})
	if err != nil {
		w.t.Fatalf("ListRoles: %v", err)
	}
	out := map[string]bool{}
	for _, r := range resp.GetRoles() {
		out[r.GetName()] = true
	}
	return out
}

// addCustomRole registers a CUSTOM role in the catalog (the built-in seed
// covers admin/operator/viewer/user; exotic names need this).
func (w *scimMapWorld) addCustomRole(name string) {
	w.t.Helper()
	if _, err := w.db.CreateRole(context.Background(), &dbpb.CreateRoleRequest{
		Name: name, Permissions: []string{"pipelines.can-view"},
	}); err != nil {
		w.t.Fatalf("CreateRole(%s): %v", name, err)
	}
}

// reconcileDirect drives the applicator straight (bypassing HTTP) — the
// SAME entry the group handlers call after a store mutation.
func (w *scimMapWorld) reconcileDirect(groupDisplayName string, joined, left []string) {
	w.t.Helper()
	req := httptest.NewRequest(http.MethodPatch, "/scim/v2/Groups/x", nil)
	w.srv.scimReconcileGroup(req, groupDisplayName, joined, left)
}

// patchDirect simulates the PATCH trigger: membership add/remove, then the
// reconciliation over the movement operands.
func (w *scimMapWorld) patchDirect(id string, add, rem []string) {
	w.t.Helper()
	merged, err := w.groups.PatchMembers(context.Background(), id, add, rem)
	if err != nil {
		w.t.Fatalf("patchDirect PatchMembers: %v", err)
	}
	w.reconcileDirect(merged.DisplayName, add, rem)
}

// removeDirect patches with a removal only.
func (w *scimMapWorld) removeDirect(id, value string) {
	w.t.Helper()
	w.patchDirect(id, nil, []string{value})
}

// ---------------------------------------------------------------------------
// Unit matrix (spec Slice 3)
// ---------------------------------------------------------------------------

// TestSCIMMapping_GrantUnionLeaveStrip walks the CORE matrix on one user:
// join (grant), second group (UNION), zero-diff REPLAY (silent), leave one
// of two (KEEP — survivorship), leave the last (STRIP).
func TestSCIMMapping_GrantUnionLeaveStrip(t *testing.T) {
	w := newScimMapWorld(t, map[string]string{
		"alpha-team": "operator",
		"beta-team":  "viewer",
	})
	w.seedUser("alice", "ext-alice")

	// --- Grant-on-JOIN ----------------------------------------------------
	g1 := w.seedGroup("alpha-team", "ext-alice")
	w.reconcileDirect(g1.DisplayName, g1.MemberValues, nil)
	_, scim := w.bindings("alice")
	if _, ok := scim["operator"]; !ok {
		t.Fatalf("after join alpha-team: alice's scim bindings = %v, want operator", scim)
	}
	if w.publications != 1 {
		t.Errorf("publications after grant = %d, want 1", w.publications)
	}

	// --- Union across groups -----------------------------------------------
	g2 := w.seedGroup("beta-team")
	w.patchDirect(g2.ID, []string{"ext-alice"}, nil)
	_, scim = w.bindings("alice")
	if _, o1 := scim["operator"]; !o1 {
		t.Fatalf("after beta-team join: operator missing (union violated): %v", scim)
	}
	if _, o2 := scim["viewer"]; !o2 {
		t.Fatalf("after beta-team join: viewer missing (union violated): %v", scim)
	}

	// --- Zero-diff REPLAY (silence) -----------------------------------------
	// Alice still belongs to both teams and her lane is CONVERGED: re-driving
	// the reconciliation must publish NOTHING and append NO audit companion.
	beforePub, beforeEv := w.publications, len(w.aud.events)
	w.reconcileDirect("alpha-team", []string{"ext-alice"}, nil)
	if w.publications != beforePub {
		t.Errorf("converged replay PUBLISHED (Δ%d), want silent", w.publications-beforePub)
	}
	if len(w.aud.events) != beforeEv {
		t.Errorf("converged replay APPENDED audit events (Δ%d), want none", len(w.aud.events)-beforeEv)
	}

	// --- Leave ONE of two (keep) --------------------------------------------
	w.removeDirect(g2.ID, "ext-alice")
	_, scim = w.bindings("alice")
	if _, ok := scim["operator"]; !ok {
		t.Errorf("leaving beta-team: operator STRIPPED (want kept — alpha still derives it): %v", scim)
	}
	if _, ok := scim["viewer"]; ok {
		t.Errorf("leaving beta-team: viewer STILL PRESENT (want stripped): %v", scim)
	}

	// --- Leave the LAST (strip) ----------------------------------------------
	w.removeDirect(g1.ID, "ext-alice")
	_, scim = w.bindings("alice")
	if len(scim) != 0 {
		t.Errorf("leaving the last group: scim bindings = %v, want EMPTY", scim)
	}
}

// TestSCIMMapping_OperatorGrantSurvival proves ruling 2: a MANUAL-origin
// binding on a SCIM-managed user survives a FULL recompute cycle (join →
// churn → leave) while the scim_group lane is reconciled underneath it.
func TestSCIMMapping_OperatorGrantSurvival(t *testing.T) {
	w := newScimMapWorld(t, map[string]string{"crew": "operator"})
	w.seedUser("bob", "ext-bob")

	// Operator grants bob "viewer" MANUALLY (the API's addBinding path —
	// origin defaults to "manual").
	if _, err := w.db.AddRoleBinding(context.Background(), &dbpb.AddRoleBindingRequest{
		PrincipalKind: "user", PrincipalId: "bob", RoleName: "viewer",
	}); err != nil {
		t.Fatalf("operator grant: %v", err)
	}
	manual, _ := w.bindings("bob")
	if _, ok := manual["viewer"]; !ok {
		t.Fatalf("setup: manual viewer binding missing: %v", manual)
	}

	// Bob JOINS crew (gets operator via scim_group)…
	g := w.seedGroup("crew", "ext-bob")
	w.reconcileDirect(g.DisplayName, g.MemberValues, nil)
	man1, scim1 := w.bindings("bob")
	if _, ok := man1["viewer"]; !ok {
		t.Fatalf("after join: manual viewer DISPLACED: %v", man1)
	}
	if _, ok := scim1["operator"]; !ok {
		t.Fatalf("after join: scim operator missing: %v", scim1)
	}

	// …CHURNS (a no-motion recompute touching his lane)…
	w.reconcileDirect(g.DisplayName, []string{"ext-bob"}, nil)
	man2, scim2 := w.bindings("bob")
	if _, ok := man2["viewer"]; !ok {
		t.Fatalf("after churn: manual viewer DISAPPEARED: %v", man2)
	}
	if _, ok := scim2["operator"]; !ok {
		t.Fatalf("after churn: scim operator disappeared: %v", scim2)
	}

	// …LEAVES crew (operator stripped; manual viewer INTACT).
	w.removeDirect(g.ID, "ext-bob")
	man3, scim3 := w.bindings("bob")
	if _, ok := man3["viewer"]; !ok {
		t.Errorf("after leave: manual viewer STRIPPED (survival violated): %v", man3)
	}
	if _, ok := scim3["operator"]; ok {
		t.Errorf("after leave: scim operator survived (stripping violated): %v", scim3)
	}
}

// TestSCIMMapping_MissingRoleSkipped proves ruling 4: a mapping pointing at
// a role ABSENT from the catalog is skipped with a warning — never created,
// never fatal — and the user ends up with NO scim bindings.
func TestSCIMMapping_MissingRoleSkipped(t *testing.T) {
	w := newScimMapWorld(t, map[string]string{"ghost-team": "phantom-role"})
	w.seedUser("cara", "ext-cara")
	g := w.seedGroup("ghost-team", "ext-cara")
	w.reconcileDirect(g.DisplayName, g.MemberValues, nil)

	_, scim := w.bindings("cara")
	if len(scim) != 0 {
		t.Errorf("missing mapped role produced bindings: %v, want NONE", scim)
	}
	if w.catalogNames()["phantom-role"] {
		t.Error("phantom-role AUTO-CREATED (violates ruling 4)")
	}
}

// TestSCIMMapping_CustomMappedRole exercises a mapping to a CUSTOM
// (non-built-in) catalog role — the realistic corporate case.
func TestSCIMMapping_CustomMappedRole(t *testing.T) {
	w := newScimMapWorld(t, map[string]string{"release-engineering": "rel-eng"})
	w.addCustomRole("rel-eng")
	w.seedUser("dan", "ext-dan")
	g := w.seedGroup("release-engineering", "ext-dan")
	w.reconcileDirect(g.DisplayName, g.MemberValues, nil)
	_, scim := w.bindings("dan")
	if _, ok := scim["rel-eng"]; !ok {
		t.Errorf("custom mapped role not granted: scim bindings = %v", scim)
	}
}

// TestSCIMMapping_GroupDeleteRevokes proves the DELETE trigger (wired
// THROUGH THE HTTP FACADE): deleting a group recomputes its former members
// and strips the orphaned derivations (survivorship from OTHER groups is
// preserved).
func TestSCIMMapping_GroupDeleteRevokes(t *testing.T) {
	w := newScimMapWorld(t, map[string]string{
		"doomed":  "viewer",
		"eternal": "operator",
	})
	w.seedUser("erin", "ext-erin")
	doomed := w.seedGroup("doomed", "ext-erin")
	eternal := w.seedGroup("eternal")
	w.patchDirect(eternal.ID, []string{"ext-erin"}, nil)
	w.reconcileDirect(doomed.DisplayName, doomed.MemberValues, nil)
	_, scim := w.bindings("erin")
	if _, o1 := scim["viewer"]; !o1 {
		t.Fatalf("setup: erin missing viewer: %v", scim)
	}
	if _, o2 := scim["operator"]; !o2 {
		t.Fatalf("setup: erin missing operator: %v", scim)
	}

	code, _ := scimCall(t, http.MethodDelete, w.ts.URL+"/scim/v2/Groups/"+doomed.ID, nil)
	if code != http.StatusNoContent {
		t.Fatalf("DELETE group = %d, want 204", code)
	}
	_, scim = w.bindings("erin")
	if _, ok := scim["viewer"]; ok {
		t.Errorf("group delete: orphaned viewer SURVIVES: %v", scim)
	}
	if _, ok := scim["operator"]; !ok {
		t.Errorf("group delete: surviving operator LOST (union violated): %v", scim)
	}
}

// TestSCIMMapping_DeprovisionNeutrality proves the tombstone PRUNES NOTHING
// and RECOMPUTES NOTHING: deprovisioning a member leaves her scim bindings
// intact (authentication is blocked regardless; rehire needs no repair —
// the membership values persist).
func TestSCIMMapping_DeprovisionNeutrality(t *testing.T) {
	w := newScimMapWorld(t, map[string]string{"fleet": "operator"})
	fr := w.seedUser("frank", "ext-frank")
	g := w.seedGroup("fleet", "ext-frank")
	w.reconcileDirect(g.DisplayName, g.MemberValues, nil)
	_, scim := w.bindings("frank")
	if len(scim) == 0 {
		t.Fatal("setup: frank should hold operator")
	}

	// Frank is TOMBSTONED via the HTTP façade (the user DELETE handler —
	// no group involvement by design).
	code, _ := scimCall(t, http.MethodDelete, w.ts.URL+"/scim/v2/Users/"+fr.ID, nil)
	if code != http.StatusNoContent {
		t.Fatalf("DELETE user = %d, want 204", code)
	}
	_, scimAfter := w.bindings("frank")
	if len(scimAfter) != len(scim) {
		t.Errorf("tombstone PRUNED bindings (%v → %v); deprovisioning must be neutral", scim, scimAfter)
	}
	if !containsStr(g.MemberValues, "ext-frank") {
		t.Errorf("tombstone ERASED the membership value (rehire would need repair): %v", g.MemberValues)
	}
}

// TestSCIMMapping_EmbeddedGroupsSwitchover proves the user-handler
// switchover: embedded `groups` in a POSTed user DOC (a) GAP-FILL
// membership into an EXISTING group, (b) DRIVE the derivation, (c) IGNORE +
// log UNKNOWN references (no phantom groups), and (d) AUGMENT (never
// displace) the stock `user` placeholder.
func TestSCIMMapping_EmbeddedGroupsSwitchover(t *testing.T) {
	w := newScimMapWorld(t, map[string]string{"pod": "viewer"})
	w.seedGroup("pod") // existing group WITHOUT grace

	code, _ := scimCall(t, http.MethodPost, w.ts.URL+"/scim/v2/Users", map[string]any{
		"schemas":    []string{"urn:ietf:params:scim:schemas:core:2.0:User"},
		"userName":   "grace.hopper",
		"externalId": "ext-grace",
		"groups": []any{
			map[string]any{"displayName": "pod"},         // HIT: gap-fill + derive
			map[string]any{"value": "Nonexistent Corps"}, // MISS: ignored + logged
			"Another Ghost Group",                        // MISS (bare string)
		},
	})
	if code != http.StatusCreated {
		t.Fatalf("POST user w/ embedded groups = %d, want 201", code)
	}

	// (d) Placeholder AUGMENTED, never displaced.
	row, err := w.users.GetByUsername(context.Background(), "grace.hopper", false)
	if err != nil {
		t.Fatalf("lookup grace: %v", err)
	}
	if !containsStr(row.Roles, scimStockRolePlaceholder) {
		t.Errorf("stock placeholder DISPLACED: row roles = %v", row.Roles)
	}

	// (a) Gap-fill: grace is NOW a member of pod (under her external id).
	pods, err := w.groups.List(context.Background(), &EqualityConstraints{{Attribute: "display_name", Value: "pod", CaseSensitive: true}})
	if err != nil || len(pods) != 1 {
		t.Fatalf("look up pod: %v (%d groups)", err, len(pods))
	}
	if !containsStr(pods[0].MemberValues, "ext-grace") {
		t.Errorf("gap-fill FAILED: pod members = %v, want ext-grace present", pods[0].MemberValues)
	}

	// (b) Derivation fired: viewer bound scim_group-origin.
	_, scim := w.bindings("grace.hopper")
	if _, ok := scim["viewer"]; !ok {
		t.Errorf("embedded groups did not DRIVE derivation: scim bindings = %v", scim)
	}

	// (c) NO PHANTOMS: only the pre-existing group exists.
	all, err := w.groups.List(context.Background(), nil)
	if err != nil {
		t.Fatalf("list groups: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("PHANTOM GROUPS CREATED: %d groups exist, want 1", len(all))
	}
}

// TestSCIMMapping_InertWithoutBackings proves graceful DECLINE: with the
// mapping table empty, or the Database client unwired, the applicator does
// nothing — group mutations still succeed (201), no bindings, no crash.
func TestSCIMMapping_InertWithoutBackings(t *testing.T) {
	// (a) No mapping table.
	w := newScimMapWorld(t, nil)
	w.seedUser("ivan", "ext-ivan")
	code, _ := scimCall(t, http.MethodPost, w.ts.URL+"/scim/v2/Groups", map[string]any{
		"displayName": "other-team",
		"members":     []map[string]any{{"value": "ext-ivan"}},
	})
	if code != http.StatusCreated {
		t.Fatalf("group POST (inert world) = %d, want 201", code)
	}
	_, scim := w.bindings("ivan")
	if len(scim) != 0 {
		t.Errorf("inert world produced bindings: %v", scim)
	}

	// (b) Table present but Database CLIENT unwired.
	w2 := newScimMapWorld(t, map[string]string{"team": "viewer"})
	w2.srv.SetDatabaseClient(nil)
	w2.seedUser("judith", "ext-judith")
	g2 := w2.seedGroup("team", "ext-judith")
	w2.reconcileDirect(g2.DisplayName, g2.MemberValues, nil)
	_, scim2 := w2.bindings("judith")
	if len(scim2) != 0 {
		t.Errorf("unwired-db world produced bindings: %v", scim2)
	}
}

// TestSCIMMapping_UserDocumentImmutabilityRegression guards the adjacent
// invariant: a user PUT with a DIFFERING externalId still 400s (the
// switchover didn't loosen the /Users immutability check).
func TestSCIMMapping_UserDocumentImmutabilityRegression(t *testing.T) {
	w := newScimMapWorld(t, map[string]string{"team": "viewer"})
	post, _ := scimCall(t, http.MethodPost, w.ts.URL+"/scim/v2/Users", map[string]any{
		"userName": "kevin.bacon", "externalId": "kb-orig",
	})
	if post != http.StatusCreated {
		t.Fatalf("POST kevin = %d, want 201", post)
	}
	kb, err := w.users.GetByUsername(context.Background(), "kevin.bacon", false)
	if err != nil {
		t.Fatalf("lookup kevin: %v", err)
	}
	put, _ := scimCall(t, http.MethodPut, w.ts.URL+"/scim/v2/Users/"+kb.ID, map[string]any{
		"userName": "kevin.bacon", "externalId": "kb-other",
	})
	if put != http.StatusBadRequest {
		t.Errorf("PUT differing externalId = %d, want 400", put)
	}
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
