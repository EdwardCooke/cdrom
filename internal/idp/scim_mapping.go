// SCIM group→role mapping applicator (F-28.5, Slice 3).
//
// Turns group MEMBERSHIP into F-14 role BINDINGS: the static mapping table
// (idp.scim.group_role_mappings, process-loaded at boot, F-28.1) associates
// a group displayName with a role name; every user belonging to a mapped
// group OWNS that role. Ownership is expressed EXCLUSIVELY through
// RoleBinding rows of origin "scim_group" (the applicator's own lane —
// operator/manual bindings are never touched, so temporary adjustment
// grants survive every recomputation; F-28.5 ruling 2).
//
// DERIVATION (per affected user U):
//
//	D    = ⋃ mapped(g.displayName) for groups g containing U
//	         − names missing from the role catalog   (WARNING log, skipped)
//	have = { b.roleName | b ← ListRoleBindings(U), b.origin == "scim_group" }
//	add(D − have)  /  remove(have − D)
//	if diff ≠ ∅ → PublishRoleChange()                 (cache invalidation)
//
// The diff is computed AGAINST THE STORE (the binding rows), never against
// token claims: effective authorization is the UNION of token-stamped roles
// and stored bindings (internal/authz), so the binding row is the durable,
// cache-invalidatable half. Zero-diff replays converge silently — no event,
// no audit noise (connectors resync the whole directory periodically;
// idempotence is by construction).
//
// Trigger points (wired in scim_groups.go / scim_users.go): group
// POST/PUT/PATCH/DELETE (each DISTINCT resolved member) and user
// POST/PUT/PATCH carrying embedded `groups` (after ensure-membership
// gap-fill). Periodic replays re-drive the same paths and reconverge.
//
// FAILURE POSTURE: the applicator is BEST-EFFORT SUPPLEMENTARY — a
// derivation hiccup (transient DB fault) is logged and swallowed; the SCIM
// response reflects the STORE WRITE, which succeeded. The next sync heals.
package idp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/audit"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// scimGroupBindingOrigin is the RoleBinding.ORIGIN the applicator stamps on
// every write it makes (F-28.5, Slice 1): the recomputation diffs ONLY
// scim_group-origin bindings — that exclusion is what protects operator
// grants (ruling 2) in one line.
const scimGroupBindingOrigin = "scim_group"

// scimGroupDerivedAction is the audit ACTION for the NON-EMPTY role-diff
// companion events (F-28.5 *Auditing & metrics*: "resulting NON-EMPTY role
// diffs append a companion event naming the user, the gained and lost
// roles, and the triggering group"). Lean by nature — a diff is always a
// MOVE (there is no no-move variant of a non-empty diff).
const scimGroupDerivedAction = "scim.group.derived"

// scimAgTargetPrefix marks a mapping TARGET as an F-27 APPROVAL GROUP
// instead of an F-14 role (F-28.5 stretch): a group_role_mappings value of
// "ag:<name>" mirrors the group's membership INTO the approval group named
// <name>; a bare value is a role name. The single-knob discrimination keeps
// the F-28.1 config surface unchanged.
const scimAgTargetPrefix = "ag:"

// derivationDiff is the outcome of one user's recomputation: the roles
// gained and lost THIS PASS (empty when converged — the caller publishes
// and audits NOTHING for a zero diff).
type derivationDiff struct {
	Added   []string
	Removed []string
}

func (d derivationDiff) isEmpty() bool { return len(d.Added) == 0 && len(d.Removed) == 0 }

// scimApplicable reports whether the applicator can do anything: the
// Database client is wired AND the boot-loaded mapping table is non-empty
// (an empty table legitimately derives nothing — the group resource still
// functions, its groups merely stay inert).
func (s *Server) scimApplicable() bool {
	return s.db != nil && len(s.cfg.Scim.GroupRoleMappings) > 0
}

// scimRoleCatalog snapshots the role catalog as a NAME SET (the existence
// lens for missing-mapped-role skips, ruling 4).
func (s *Server) scimRoleCatalog(ctx context.Context) (map[string]bool, error) {
	resp, err := s.db.ListRoles(ctx, &dbpb.ListRolesRequest{})
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(resp.GetRoles()))
	for _, r := range resp.GetRoles() {
		out[r.GetName()] = true
	}
	return out, nil
}

// scimCurrentGroupBindings returns the user's scim_group-origin bindings as
// a role→BINDING-ID map (the remove leg needs the ids; the want leg only
// the names). Bindings of ANY other origin are invisible here — the
// operator-grant protection (ruling 2) distilled to one filter.
func (s *Server) scimCurrentGroupBindings(ctx context.Context, username string) (map[string]int64, error) {
	resp, err := s.db.ListRoleBindings(ctx, &dbpb.ListRoleBindingsRequest{
		PrincipalKind: "user",
		PrincipalId:   username,
	})
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, b := range resp.GetBindings() {
		if b.GetOrigin() == scimGroupBindingOrigin {
			out[b.GetRoleName()] = b.GetId()
		}
	}
	return out, nil
}

// scimGroupsContaining enumerates the user's groups via TWO list queries
// (the store exposes no reverse-membership RPC): one per CANDIDATE VALUE
// (the user's username and external_id) over the members_value predicate —
// the SAME two-arm resolution the group codec uses for expansion. Results
// are deduped by group id. Resolutions/listings that fail are logged and
// treated as "no groups" (best-effort posture; the next sync heals).
// scimGroupsContaining enumerates the user's groups via TWO list queries
// (the store exposes no reverse-membership RPC): one per CANDIDATE VALUE
// (the user's username and external_id) over the members_value predicate —
// the SAME two-arm resolution the group codec uses for expansion. Results
// are deduped by group id.
//
// SAFETY CONTRACT: a TRANSIENT failure (user lookup or listing) is an
// ERROR, not "no groups" — collapsing it to empty would make the caller
// WIPE the user's entire scim_group lane (catastrophic on a blip). Only a
// definitive NotFound (the user is truly gone) yields the empty set.
func (s *Server) scimGroupsContaining(ctx context.Context, username string) ([]*SCIMGroup, error) {
	if s.scimGroups == nil || s.scimUsers == nil {
		return nil, nil
	}
	u, err := s.scimUsers.GetByUsername(ctx, username, false)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil // truly gone: no groups, nothing to protect
		}
		return nil, err
	}
	candidates := map[string]bool{u.Username: true}
	if u.ExternalID != "" {
		candidates[u.ExternalID] = true
	}
	byID := map[string]*SCIMGroup{}
	out := make([]*SCIMGroup, 0, len(byID))
	for v := range candidates {
		cs := &EqualityConstraints{{Attribute: "members_value", Value: v, CaseSensitive: true}}
		gs, lerr := s.scimGroups.List(ctx, cs)
		if lerr != nil {
			return nil, lerr // bail: partial blindness ⇒ refuse to conclude
		}
		for _, g := range gs {
			byID[g.ID] = g
		}
	}
	for _, g := range byID {
		out = append(out, g)
	}
	return out, nil
}

// scimDeriveUser recomputes ONE user's scim_group bindings: union the
// mapped roles over the user's groups (dropping catalog misses — WARN-
// LOGGED, ruling 4), diff against the current scim_group lane, APPLY the
// diff (adds stamped scim_group; removes by collected binding id), and
// PUBLISH iff non-empty (ruling 1 — the event flushes every API replica's
// authorization cache without a restart).
func (s *Server) scimDeriveUser(ctx context.Context, username string, catalog map[string]bool) (derivationDiff, error) {
	var diff derivationDiff
	want := map[string]bool{}
	groups, gerr := s.scimGroupsContaining(ctx, username)
	if gerr != nil {
		return diff, gerr // refuse to conclude on partial blindness (wipe risk)
	}
	for _, g := range groups {
		target, ok := s.cfg.Scim.GroupRoleMappings[g.DisplayName]
		if !ok {
			continue // unmapped group: stored inertly, no authorisation effect
		}
		// STRETCH (F-27): an "ag:"-prefixed target mirrors the user's
		// membership into the named APPROVAL GROUP instead of binding a
		// role (orthogonal channels — no diff, no publish involved).
		if strings.HasPrefix(target, scimAgTargetPrefix) {
			s.scimMirrorApprovalGroup(ctx, username, strings.TrimPrefix(target, scimAgTargetPrefix), g.DisplayName)
			continue
		}
		role := target
		if !catalog[role] {
			// Missing mapped role (ruling 4): SKIP + WARN (role name +
			// contributing group); never created, never fatal — inert by
			// construction (uncatalogued roles confer no permissions).
			s.logger.Warn("scim: mapped role missing from the role catalog; derivation skipped",
				"role", role, "group", g.DisplayName, "user", username)
			continue
		}
		want[role] = true
	}
	haveIDs, err := s.scimCurrentGroupBindings(ctx, username)
	if err != nil {
		return diff, err
	}
	for role := range want {
		if _, present := haveIDs[role]; !present {
			diff.Added = append(diff.Added, role)
		}
		delete(haveIDs, role)
	}
	for role := range haveIDs {
		diff.Removed = append(diff.Removed, role)
	}
	sort.Strings(diff.Added)
	sort.Strings(diff.Removed)
	if diff.isEmpty() {
		return diff, nil // converged: NO publish, NO audit (anti-noise)
	}
	for _, role := range diff.Added {
		if _, err := s.db.AddRoleBinding(ctx, &dbpb.AddRoleBindingRequest{
			PrincipalKind: "user",
			PrincipalId:   username,
			RoleName:      role,
			Origin:        scimGroupBindingOrigin,
		}); err != nil {
			return diff, err
		}
	}
	for _, role := range diff.Removed {
		id, ok := haveIDs[role]
		if !ok {
			continue // raced away concurrently; the lane is already clean
		}
		if _, err := s.db.DeleteRoleBinding(ctx, &dbpb.DeleteRoleBindingRequest{Id: id}); err != nil {
			return diff, err
		}
	}
	if _, err := s.db.PublishRoleChange(ctx, &dbpb.PublishRoleChangeRequest{}); err != nil {
		return diff, err
	}
	return diff, nil
}

// scimReconcileGroup is the GROUP-HANDLER trigger-point entry: after a
// SUCCESSFUL group store mutation, recompute the derivation for each
// DISTINCT resolved member (joined ∪ left, deduped) and append the
// companion audit events for non-empty diffs. The role catalog is fetched
// ONCE for the whole batch. Failures are LOGGED, never propagated
// (best-effort supplementary — the store write stands on its own).
//
// joined/left are RAW membership VALUES (not users): POST passes its
// initial set (nothing left), PUT passes the SYMMETRIC DIFFERENCE (entered
// or exited), PATCH passes its add/remove operands, DELETE passes the
// former full set (everyone left). Overlap is harmless — resolutions are
// deduped before derivation.
func (s *Server) scimReconcileGroup(r *http.Request, groupDisplayName string, joined, left []string) {
	if !s.scimApplicable() {
		return
	}
	ctx := r.Context()
	affected := map[string]bool{}
	for _, v := range append(append([]string{}, joined...), left...) {
		if u, ok := s.userResolutionDirectory().resolveLive(ctx, v); ok {
			affected[u.Username] = true
		}
	}
	if len(affected) == 0 {
		return
	}
	usernames := make([]string, 0, len(affected))
	for u := range affected {
		usernames = append(usernames, u)
	}
	sort.Strings(usernames)
	catalog, err := s.scimRoleCatalog(ctx)
	if err != nil {
		s.logger.Error("scim: fetching the role catalog for derivation failed",
			"group", groupDisplayName, "err", err)
		return
	}
	for _, u := range usernames {
		diff, derr := s.scimDeriveUser(ctx, u, catalog)
		if derr != nil {
			s.logger.Error("scim: group-role derivation failed (will reconverge on next sync)",
				"user", u, "group", groupDisplayName, "err", derr)
			continue
		}
		if !diff.isEmpty() {
			s.scimAuditGroupDerived(r, u, diff, groupDisplayName)
		}
	}
}

// scimAuditGroupDerived appends the COMPANION audit event for one user's
// non-empty derivation diff (F-28.5 *Auditing & metrics*): it names the
// user, the gained and lost roles, and the triggering group. The actor is
// the presenting connector (same attribution as the parent group op).
func (s *Server) scimAuditGroupDerived(r *http.Request, username string, d derivationDiff, groupDisplayName string) {
	if s.scimUsersAuditor == nil {
		return
	}
	s.scimUsersAuditor.Record(audit.Event{
		Actor:      SCIMCallerFromContext(r.Context()),
		ActorKind:  audit.ActorKindSystem,
		Action:     scimGroupDerivedAction,
		TargetKind: audit.TargetSCIMGroup,
		TargetID:   username,
		TargetName: username,
		Outcome:    audit.OutcomeSuccess,
		SourceIP:   remoteIP(r),
		NewValue: map[string]any{
			"user":   username,
			"gained": d.Added,
			"lost":   d.Removed,
			"group":  groupDisplayName,
			"origin": scimGroupBindingOrigin,
		},
		Details: map[string]any{"method": r.Method, "path": r.URL.Path},
	})
}

// scimEnsureEmbeddedGroups is the USER-HANDLER trigger point (the
// SWITCHOVER of F-28.4's acknowledge-and-discard path): the embedded
// `groups` of a posted user document are BEST-EFFORT SUPPLEMENTS to the
// group resource — each reference is matched against EXISTING groups by
// exact displayName (the mapping key) or by numeric Location; a hit
// ENSURES the user's denoting value is present in that group's membership
// (gap-fill via PatchMembers); a miss is ignored + logged (NO PHANTOM
// GROUPS from opaque values). Afterwards the poster's derivation is
// recomputed (covering every healed group PLUS any pre-existing
// membership the store already knew).
//
// Precedence (spec): group-resource writes are AUTHORITATIVE (PUT replaces
// the full set); user-document embeddings supplement. A periodic full
// resync (authoritative group PUTs) heals any ordering skew.
func (s *Server) scimEnsureEmbeddedGroups(r *http.Request, username string, refs []any) {
	if len(refs) == 0 || !s.scimApplicable() {
		return
	}
	ctx := r.Context()
	seen := map[string]bool{} // group ids already considered this pass
	for _, ref := range refs {
		name, loc := scimGroupReferenceParts(ref)
		g, ok := s.scimFindExistingGroup(ctx, name, loc)
		if !ok {
			s.logger.Warn("scim: embedded group reference matches no existing group; ignored (no phantom groups)",
				"user", username, "reference", describeSCIMRef(ref))
			continue
		}
		if seen[g.ID] {
			continue
		}
		seen[g.ID] = true
		u, err := s.scimUsers.GetByUsername(ctx, username, false)
		if err != nil {
			s.logger.Warn("scim: embedded-group gap-fill cannot resolve the user; skipped",
				"user", username, "err", err)
			continue
		}
		preferred := u.ExternalID
		if preferred == "" {
			preferred = u.Username
		}
		if groupHasValue(g.MemberValues, preferred) || groupHasValue(g.MemberValues, u.Username) {
			continue // already a member under either key — nothing to heal
		}
		if _, perr := s.scimGroups.PatchMembers(ctx, g.ID, []string{preferred}, nil); perr != nil {
			s.logger.Warn("scim: embedded-group gap-fill failed (next sync heals)",
				"user", username, "group", g.DisplayName, "err", perr)
			continue
		}
		s.logger.Info("scim: embedded group reference healed membership",
			"user", username, "group", g.DisplayName, "value", preferred)
	}
	// Recompute the poster's derivation ONCE (whether or not anything was
	// healed — the user may already belong to a mapped group).
	catalog, err := s.scimRoleCatalog(ctx)
	if err != nil {
		s.logger.Error("scim: fetching the role catalog for derivation failed",
			"user", username, "err", err)
		return
	}
	diff, derr := s.scimDeriveUser(ctx, username, catalog)
	if derr != nil {
		s.logger.Error("scim: embedded-group derivation failed (will reconverge on next sync)",
			"user", username, "err", derr)
		return
	}
	if !diff.isEmpty() {
		s.scimAuditGroupDerived(r, username, diff, "<embedded>")
	}
}

// scimFindExistingGroup matches an embedded reference against EXISTING
// groups: by EXACT displayName first (the mapping key — the intended
// precision), then by NUMERIC Location (refs shaped like ".../Groups/3").
// Legal duplicate-displayName hits resolve to the FIRST match — the
// healing is membership-preserving, so choosing among equals is harmless.
func (s *Server) scimFindExistingGroup(ctx context.Context, displayName, location string) (*SCIMGroup, bool) {
	if s.scimGroups == nil {
		return nil, false
	}
	if displayName != "" {
		cs := &EqualityConstraints{{Attribute: "display_name", Value: displayName, CaseSensitive: true}}
		if gs, err := s.scimGroups.List(ctx, cs); err == nil && len(gs) > 0 {
			return gs[0], true
		}
	}
	if location != "" {
		if g, err := s.scimGroups.Get(ctx, location); err == nil {
			return g, true
		}
	}
	return nil, false
}

// scimGroupReferenceParts decompose an embedded `groups` entry into its
// displayName hint and its numeric Location hint: a STRING is a bare
// displayName; an OBJECT may carry {value|displayName, $ref}.
func scimGroupReferenceParts(ref any) (displayName, location string) {
	switch v := ref.(type) {
	case string:
		displayName = strings.TrimSpace(v)
	case map[string]any:
		if d, ok := v["displayName"].(string); ok {
			displayName = strings.TrimSpace(d)
		} else if d, ok := v["value"].(string); ok {
			displayName = strings.TrimSpace(d)
		}
		if lr, ok := v["$ref"].(string); ok {
			if idx := strings.LastIndexByte(lr, '/'); idx >= 0 {
				location = strings.TrimRight(lr[idx+1:], "/")
			}
		}
	}
	return displayName, location
}

// describeSCIMRef renders an embedded reference compactly for warn logs.
func describeSCIMRef(ref any) string {
	b, err := json.Marshal(ref)
	if err != nil {
		return fmt.Sprintf("%v", ref)
	}
	return string(b)
}

// groupHasValue reports whether the (normalised) membership carries the
// value.
func groupHasValue(memberValues []string, value string) bool {
	for _, mv := range memberValues {
		if mv == value {
			return true
		}
	}
	return false
}

// scimAGCache is the per-INSTANCE cache of the approval-group NAME SET (the
// existence lens for missing-target skips, ruling 4's twin). Instance-scoped
// (not package-global) so isolated test worlds — each with its own database
// — never bleed a stale set across one another. Approval groups are rarely
// created, so a short TTL keeps the mirror cheap without staleness biting.
type scimAGCache struct {
	mu    sync.Mutex
	names map[string]bool
	at    time.Time
}

const scimAGCacheTTL = 30 * time.Second

// scimApprovalGroupNameSet returns the current approval-group names,
// refreshing the cache past its TTL. A listing failure yields the STALE set
// (or empty on cold failure) — the mirror degrades to a skip, never a fault.
func (s *Server) scimApprovalGroupNameSet(ctx context.Context) map[string]bool {
	cache := s.agCache
	if cache == nil {
		return nil
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.names != nil && time.Since(cache.at) < scimAGCacheTTL {
		return cache.names
	}
	if s.db == nil {
		return cache.names
	}
	resp, err := s.db.ListApprovalGroups(ctx, &dbpb.ListApprovalGroupsRequest{})
	if err != nil {
		s.logger.Warn("scim: approval-group listing failed; mirror degrades to skip", "err", err)
		return cache.names
	}
	names := make(map[string]bool, len(resp.GetGroups()))
	for _, g := range resp.GetGroups() {
		names[g.GetName()] = true
	}
	cache.names = names
	cache.at = time.Now()
	return names
}

// scimMirrorApprovalGroup mirrors ONE user's membership into the named
// APPROVAL GROUP (F-28.5 stretch): the user's USERNAME (the subject the
// gate matches) is added via the idempotent AddApprovalGroupMember RPC.
// A missing approval group is SKIPPED with a warning (ruling 4's twin);
// membership removal is NOT mirrored (departure is governed by the group
// resource's own membership; the mirror is purely additive — replay-safe).
func (s *Server) scimMirrorApprovalGroup(ctx context.Context, username, groupName, scimGroup string) {
	if s.db == nil {
		return
	}
	if !s.scimApprovalGroupNameSet(ctx)[groupName] {
		s.logger.Warn("scim: approval-group mapping target missing; mirror skipped",
			"approval_group", groupName, "scim_group", scimGroup, "user", username)
		return
	}
	if _, err := s.db.AddApprovalGroupMember(ctx, &dbpb.AddApprovalGroupMemberRequest{
		Name:   groupName,
		Member: username,
	}); err != nil {
		s.logger.Warn("scim: approval-group mirror add failed (replay-safe; next sync retries)",
			"approval_group", groupName, "user", username, "err", err)
	}
}
