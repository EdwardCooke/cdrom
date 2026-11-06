// F-28.4 Slice 3 — the admission gate's BRANCH GRAPH (IdP side).
//
// Covers every arm of AdmitPrincipal against the MemoryUserStore: the
// known-row verdicts (admitted / deactivated / deprovisioned), the mode fork
// (SCIM-enabled ⇒ unknown denied; SCIM-disabled ⇒ JIT), the JIT birth rules
// (claim roles required — empty ⇒ NO_ROLES_REJECTED with NO row; ARBITRARY
// subject shapes trusted verbatim — trust-the-issuer), the returning-JIT
// refresh (name/email refreshed, roles grown MONOTONICALLY — a shrinking
// claim never strips), and the untouched branches (local + SCIM-managed
// rows admitted as-is despite conflicting posted args).
package idp

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// admissionWorld is the gate's test bench: a GRPCServer over a
// MemoryUserStore with the SCIM mode pinned.
type admissionWorld struct {
	server *GRPCServer
	store  *MemoryUserStore
	mode   bool // scim.enabled
}

func newAdmissionWorld(t *testing.T, mode bool) *admissionWorld {
	t.Helper()
	store := NewMemoryUserStore()
	cfg := config.IdPConfig{}
	cfg.Scim.Enabled = mode
	srv := &GRPCServer{cfg: cfg, users: store, logger: testLogger()}
	return &admissionWorld{server: srv, store: store, mode: mode}
}

func (w *admissionWorld) admit(t *testing.T, username, name, email string, roles ...string) (idppb.AdmissionVerdict, *dbpb.IDPUser) {
	t.Helper()
	resp, err := w.server.AdmitPrincipal(context.Background(), &idppb.AdmitPrincipalRequest{
		Username: username, Name: name, Email: email, Roles: roles,
	})
	if err != nil {
		t.Fatalf("AdmitPrincipal(%s): %v", username, err)
	}
	return resp.GetVerdict(), resp.GetUser()
}

func (w *admissionWorld) storeByName(t *testing.T, username string) *User {
	t.Helper()
	u, err := w.store.GetByUsername(context.Background(), username, true)
	if err != nil {
		t.Fatalf("store lookup %s: %v", username, err)
	}
	return u
}

func (w *admissionWorld) storeAbsent(t *testing.T, username string) {
	t.Helper()
	_, err := w.store.GetByUsername(context.Background(), username, true)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected ABSENCE of %s, got err=%v", username, err)
	}
}

func TestAdmit_KnownStates(t *testing.T) {
	for _, mode := range []bool{false, true} { // the known-row verdicts hold in BOTH modes
		w := newAdmissionWorld(t, mode)
		ctx := context.Background()
		admin, _ := w.store.Create(ctx, &User{Username: "boss", Roles: []string{"admin"}})
		joe, _ := w.store.Create(ctx, &User{Username: "joe", Roles: []string{"user"}})
		gone, _ := w.store.Create(ctx, &User{Username: "gone", Roles: []string{"user"}})
		if err := w.store.Deactivate(ctx, joe.ID); err != nil {
			t.Fatalf("deactivate: %v", err)
		}
		if err := w.store.SoftDelete(ctx, gone.ID); err != nil {
			t.Fatalf("delete: %v", err)
		}
		// Known + active ⇒ ADMITTED, untouched (even with conflicting args —
		// the posted name/email/roles must NOT bleed into the row).
		v, u := w.admit(t, "BOSS", "Conflicting", "other@x", "intruder-role")
		if v != idppb.AdmissionVerdict_ADMITTED || u.GetUsername() != "boss" {
			t.Errorf("[mode=%v] boss = %v/%v, want ADMITTED/boss", mode, v, u.GetUsername())
		}
		if got := w.storeByName(t, "boss"); containsRole(got.Roles, "intruder-role") || got.Email != "" || got.FirstName != "" {
			// Untouched: the row retains its ORIGINAL profile.
			t.Errorf("boss row altered by the admission: %v", got)
		}
		// Deactivated ⇒ DEACTIVATED.
		if v, _ = w.admit(t, "joe", "", "", "user"); v != idppb.AdmissionVerdict_DEACTIVATED {
			t.Errorf("[mode=%v] joe = %v, want DEACTIVATED", mode, v)
		}
		// Tombstoned ⇒ DEPROVISIONED.
		if v, _ = w.admit(t, "gone", "", "", "user"); v != idppb.AdmissionVerdict_DEPROVISIONED {
			t.Errorf("[mode=%v] gone = %v, want DEPROVISIONED", mode, v)
		}
		_ = admin
	}
}

func TestAdmit_JITBirth(t *testing.T) {
	w := newAdmissionWorld(t, false) // JIT mode
	// Birth: claim roles REQUIRED, provisioned, stock-free, name split.
	v, u := w.admit(t, "new.hire", "Sam Newbie", "sam@corp", "viewer")
	if v != idppb.AdmissionVerdict_ADMITTED {
		t.Fatalf("birth = %v, want ADMITTED", v)
	}
	row := w.storeByName(t, "new.hire")
	if !row.Provisioned || row.ExternalID != "" {
		t.Errorf("born row = {provisioned:%v external:%q}, want provisioned/no-ext", row.Provisioned, row.ExternalID)
	}
	if row.FirstName != "Sam" || row.LastName != "Newbie" || row.Email != "sam@corp" {
		t.Errorf("born profile = %q/%q/%q, want Sam/Newbie/sam@corp", row.FirstName, row.LastName, row.Email)
	}
	if !containsRole(row.Roles, "viewer") || containsRole(row.Roles, "user") {
		t.Errorf("born roles = %v, want [viewer] (no stock injection)", row.Roles)
	}
	_ = u
	// Return visit: name/email refreshed, roles GROWN monotonically.
	v, _ = w.admit(t, "new.hire", "Sam Renamed", "sam2@corp", "viewer", "deployer")
	if v != idppb.AdmissionVerdict_ADMITTED {
		t.Fatalf("return = %v, want ADMITTED", v)
	}
	row = w.storeByName(t, "new.hire")
	if row.Email != "sam2@corp" {
		t.Errorf("email = %q, want sam2@corp (refreshed)", row.Email)
	}
	if !containsRole(row.Roles, "viewer") || !containsRole(row.Roles, "deployer") {
		t.Errorf("grown roles = %v, want superset {viewer,deployer}", row.Roles)
	}
	// SHRINKING claim: the dropped role SURVIVES (monotonic growth).
	v, _ = w.admit(t, "new.hire", "", "", "viewer")
	if v != idppb.AdmissionVerdict_ADMITTED {
		t.Fatalf("shrinking-return = %v, want ADMITTED", v)
	}
	row = w.storeByName(t, "new.hire")
	if !containsRole(row.Roles, "deployer") {
		t.Errorf("roles lost the surviving grant: %v", row.Roles)
	}
	// Resync contact (nothing moves): admitted, row UNTOUCHED (no write —
	// a gratuitous rewrite would churn UpdatedAt and muddy the audit trail).
	before := w.storeByName(t, "new.hire")
	v, _ = w.admit(t, "new.hire", "", "", "viewer", "deployer")
	if v != idppb.AdmissionVerdict_ADMITTED {
		t.Fatalf("resync = %v, want ADMITTED", v)
	}
	after := w.storeByName(t, "new.hire")
	if !reflect.DeepEqual(before, after) {
		t.Errorf("resync contact rewrote the row: %+v → %+v", before, after)
	}
}

func TestAdmit_JITEmptyRolesNoRow(t *testing.T) {
	w := newAdmissionWorld(t, false)
	v, _ := w.admit(t, "ghost.roleless", "Ghost", "", "") // zero claim roles
	if v != idppb.AdmissionVerdict_NO_ROLES_REJECTED {
		t.Fatalf("empty-roles = %v, want NO_ROLES_REJECTED", v)
	}
	w.storeAbsent(t, "ghost.roleless") // FAIL-CLOSED: no residual row
}

func TestAdmit_ModeFork(t *testing.T) {
	// SCIM ENABLED: unknown ⇒ UNKNOWN_REJECTED (no JIT, no row).
	on := newAdmissionWorld(t, true)
	v, _ := on.admit(t, "shadow.agent", "Shadow", "", "viewer")
	if v != idppb.AdmissionVerdict_UNKNOWN_REJECTED {
		t.Fatalf("scim-on unknown = %v, want UNKNOWN_REJECTED", v)
	}
	on.storeAbsent(t, "shadow.agent")
	// SCIM DISABLED: the SAME subject JITs (above).
	off := newAdmissionWorld(t, false)
	if v, _ := off.admit(t, "shadow.agent", "Shadow", "", "viewer"); v != idppb.AdmissionVerdict_ADMITTED {
		t.Fatalf("scim-off unknown = %v, want ADMITTED (JIT)", v)
	}
}

// Trust-the-issuer (ruled 2026-11-05): the gate honors WHATEVER shape a
// configured issuer presents — Azure UPNs carry '@', email-as-username is
// common, and the subject is treated as an OPAQUE identity. Exotics
// JIT-CREATE verbatim in JIT mode; in SCIM mode the identical shapes are
// denied by the MODE FORK (sole-source-of-truth), not by any sanitation.
// The subject is trimmed at the gate boundary, so padded presentations
// collapse onto the trimmed identity (one row, no shadow twins). Only the
// empty-after-trim subject is a request fault (InvalidArgument), never a
// verdict.
func TestAdmit_TrustArbitrarySubjects(t *testing.T) {
	jit := newAdmissionWorld(t, false)
	for _, exotic := range []string{"carol@example.onmicrosoft.com", "John Doe", "üser.näme", strings.Repeat("y", 200)} {
		v, u := jit.admit(t, exotic, "", "", "viewer")
		if v != idppb.AdmissionVerdict_ADMITTED || u.GetUsername() != exotic {
			t.Fatalf("%q = %v/%q, want ADMITTED with the exact subject", exotic, v, u.GetUsername())
		}
	}
	// Trim-boundary: a PADDED presentation normalizes onto the trimmed
	// identity — the born row carries the trimmed username, and a padded
	// revisit reaches the SAME row (no shadow twins).
	base := "camel@example.onmicrosoft.com"
	v, u := jit.admit(t, "\t"+base+"\t", "", "", "viewer")
	if v != idppb.AdmissionVerdict_ADMITTED || u.GetUsername() != base {
		t.Fatalf("padded birth = %v/%q, want ADMITTED/%q (trimmed)", v, u.GetUsername(), base)
	}
	row := jit.storeByName(t, base)
	if v, _ = jit.admit(t, " "+base+" ", "", "", "viewer"); v != idppb.AdmissionVerdict_ADMITTED {
		t.Fatalf("padded revisit = %v, want ADMITTED", v)
	}
	if got := jit.storeByName(t, base); got.ID != row.ID {
		t.Fatalf("padded revisit drifted from the born row: %v vs %v", got, row)
	}
	// A SPACE-ONLY subject trims to empty ⇒ request fault, not a verdict.
	if _, err := jit.server.AdmitPrincipal(context.Background(), &idppb.AdmitPrincipalRequest{Username: "   "}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("blank subject: err=%v, want InvalidArgument", err)
	}
	scim := newAdmissionWorld(t, true)
	for _, exotic := range []string{"ghost@example.onmicrosoft.com", "Ghost Person"} {
		v, _ := scim.admit(t, exotic, "", "", "viewer")
		if v != idppb.AdmissionVerdict_UNKNOWN_REJECTED {
			t.Fatalf("%q in scim mode = %v, want UNKNOWN_REJECTED (mode fork)", exotic, v)
		}
		scim.storeAbsent(t, exotic)
	}
	if _, err := scim.server.AdmitPrincipal(context.Background(), &idppb.AdmitPrincipalRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty username: err=%v, want InvalidArgument", err)
	}
}

func TestAdmit_LocalAndSCIMManagedUntouched(t *testing.T) {
	w := newAdmissionWorld(t, true)
	ctx := context.Background()
	// LOCAL row (hand-registered): admitted untouched.
	local, _ := w.store.Create(ctx, &User{Username: "handmade", Roles: []string{"user"}})
	if v, _ := w.admit(t, "handmade", "Override", "override@x", "rogue"); v != idppb.AdmissionVerdict_ADMITTED {
		t.Fatalf("local = %v, want ADMITTED", v)
	}
	if got := w.storeByName(t, "handmade"); got.Email != "" || containsRole(got.Roles, "rogue") {
		t.Errorf("local row overwritten: %v", got)
	}
	// SCIM-MANAGED row (provisioned + externalId): admitted untouched.
	scim, _ := w.store.Create(ctx, &User{Username: "managed", ExternalID: "az-1", Provisioned: true, Roles: []string{"user"}})
	if v, _ := w.admit(t, "managed", "Override", "override@x", "rogue"); v != idppb.AdmissionVerdict_ADMITTED {
		t.Fatalf("scim-managed = %v, want ADMITTED", v)
	}
	if got := w.storeByName(t, "managed"); got.Email != "" || containsRole(got.Roles, "rogue") {
		t.Errorf("scim-managed row overwritten: %v", got)
	}
	_ = local
	_ = scim
}

func containsRole(roles []string, want string) bool {
	for _, r := range roles {
		if r == want {
			return true
		}
	}
	return false
}
