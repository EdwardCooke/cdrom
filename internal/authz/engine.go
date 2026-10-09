package authz

import (
	"context"
	"sync"
)

// Role is a role's definition as read from the role catalog. It is the
// engine's local form of a role (decoupled from the wire/DB types): its name,
// description, built-in flag, the permissions it grants directly, and the
// roles it includes (composition).
type Role struct {
	Name        string
	Description string
	BuiltIn     bool
	Permissions []string
	Includes    []string
}

// Binding is a stored role binding as read from the catalog: a role attached
// to a principal, optionally scoped to a pipeline. PipelineID of 0 means the
// binding is unscoped (platform-wide).
type Binding struct {
	PrincipalKind string
	PrincipalID   string
	RoleName      string
	PipelineID    int64
}

// RoleSource is how the engine reads the role catalog (roles and bindings).
// The API implements it over the Database service (which owns the rows), so
// every API replica sees the same catalog. The engine caches the catalog
// locally and invalidates it on role/binding change events (F-23).
type RoleSource interface {
	// ListRoles returns every role (built-in and custom).
	ListRoles(ctx context.Context) ([]Role, error)
	// ListBindings returns every stored role binding.
	ListBindings(ctx context.Context) ([]Binding, error)
}

// Principal is the identity being authorized: its kind (user or
// service-account), its identifier (a user's id / the token's subject, or a
// service account's id), and the roles stamped onto its token's roles claim
// (the fast path for users known to the built-in IdP).
type Principal struct {
	Kind       string
	ID         string
	TokenRoles []string
}

// Resource is the resource an action targets. PipelineID of 0 means the
// action is not pipeline-scoped (a platform-wide action, e.g. creating a
// pipeline or managing roles).
type Resource struct {
	PipelineID int64
}

// PermissionSet is a principal's effective permissions, split by scope: the
// permissions granted platform-wide (by the principal's unscoped roles) and,
// per pipeline, the resource-scoped permissions granted for that pipeline (by
// the principal's pipeline-scoped roles).
type PermissionSet struct {
	// PlatformWide is the set of permissions granted platform-wide.
	PlatformWide map[string]bool
	// Scoped is, per pipeline id, the set of resource-scoped permissions
	// granted for that pipeline.
	Scoped map[int64]map[string]bool
}

// roleGrant records how a role is granted to a principal: whether it is
// granted unscoped (platform-wide) and which pipelines it is granted scoped
// to.
type roleGrant struct {
	unscoped  bool
	pipelines map[int64]bool
}

// catalog is the engine's cached view of the role catalog: the roles (by
// name), each role's effective (expanded) permission set, and all stored
// bindings. It is immutable once built; the engine replaces it wholesale on
// invalidation.
type catalog struct {
	roles     map[string]Role
	rolePerms map[string]map[string]bool
	bindings  []Binding
}

// Engine is the authorization engine. It is stateless except for a short-lived
// cache of the role catalog, which it invalidates on role/binding change
// events (F-23) so a grant takes effect on every API replica without a
// restart.
type Engine struct {
	source RoleSource
	mu     sync.Mutex
	cat    *catalog
}

// New creates an authorization engine that reads its role catalog from
// source.
func New(source RoleSource) *Engine {
	return &Engine{source: source}
}

// Invalidate drops the cached role catalog so the next check re-fetches it.
// The API calls it when a role/binding change event arrives on the shared
// event log (F-23), so a role or binding change takes effect on this replica
// within the tail cadence.
func (e *Engine) Invalidate() {
	e.mu.Lock()
	e.cat = nil
	e.mu.Unlock()
}

// catalogLocked returns the cached catalog, fetching and building it if
// absent. The caller must hold e.mu.
func (e *Engine) catalogLocked(ctx context.Context) (*catalog, error) {
	if e.cat != nil {
		return e.cat, nil
	}
	roles, err := e.source.ListRoles(ctx)
	if err != nil {
		return nil, err
	}
	bindings, err := e.source.ListBindings(ctx)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]Role, len(roles))
	for _, r := range roles {
		byName[r.Name] = r
	}
	cat := &catalog{
		roles:     byName,
		rolePerms: make(map[string]map[string]bool, len(roles)),
		bindings:  bindings,
	}
	for name := range byName {
		cat.rolePerms[name] = expandRole(byName, name, map[string]bool{})
	}
	e.cat = cat
	return cat, nil
}

// expandRole returns the effective permission set of the named role: the
// union of the role's own permissions and the effective permissions of every
// role it includes (expanded transitively). visiting guards against include
// cycles (a role that includes itself, directly or indirectly).
func expandRole(roles map[string]Role, name string, visiting map[string]bool) map[string]bool {
	perms := map[string]bool{}
	r, ok := roles[name]
	if !ok {
		return perms // an unknown role contributes no permissions
	}
	if visiting[name] {
		return perms // cycle: stop expanding
	}
	visiting[name] = true
	for _, p := range r.Permissions {
		perms[p] = true
	}
	for _, inc := range r.Includes {
		for p := range expandRole(roles, inc, visiting) {
			perms[p] = true
		}
	}
	return perms
}

// principalGrants returns, for each role that applies to the principal, how
// that role is granted to it: unscoped (platform-wide) and/or scoped to
// specific pipelines. A role applies via the principal's token roles
// (inherently unscoped) and/or its stored bindings (unscoped or scoped).
func principalGrants(cat *catalog, p Principal) map[string]*roleGrant {
	grants := map[string]*roleGrant{}
	grant := func(name string) *roleGrant {
		g, ok := grants[name]
		if !ok {
			g = &roleGrant{pipelines: map[int64]bool{}}
			grants[name] = g
		}
		return g
	}
	for _, r := range p.TokenRoles {
		grant(r).unscoped = true
	}
	for _, b := range cat.bindings {
		if b.PrincipalKind != p.Kind || b.PrincipalID != p.ID {
			continue
		}
		g := grant(b.RoleName)
		if b.PipelineID == 0 {
			g.unscoped = true
		} else {
			g.pipelines[b.PipelineID] = true
		}
	}
	return grants
}

// Check reports whether the principal may perform the named permission on the
// resource. It is deny-by-default: a principal is allowed only if one of its
// roles grants the permission with a scope that covers the resource. A
// resource-scoped permission is satisfied by an unscoped grant of the role or
// a scoped grant on the resource's pipeline; a platform-wide permission is
// satisfied only by an unscoped grant.
func (e *Engine) Check(ctx context.Context, p Principal, permission string, res Resource) (bool, error) {
	e.mu.Lock()
	cat, err := e.catalogLocked(ctx)
	if err != nil {
		e.mu.Unlock()
		return false, err
	}
	ok := checkLocked(cat, p, permission, res)
	e.mu.Unlock()
	return ok, nil
}

func checkLocked(cat *catalog, p Principal, permission string, res Resource) bool {
	grants := principalGrants(cat, p)
	for roleName, g := range grants {
		perms, ok := cat.rolePerms[roleName]
		if !ok || !perms[permission] {
			continue
		}
		if isResourceScoped(permission) {
			// A resource-scoped permission is satisfied by an unscoped grant
			// of the role, or a scoped grant on the resource's pipeline.
			if g.unscoped || (res.PipelineID != 0 && g.pipelines[res.PipelineID]) {
				return true
			}
		} else {
			// A platform-wide permission is satisfied only by an unscoped
			// grant (a scoped binding never grants platform-wide power).
			if g.unscoped {
				return true
			}
		}
	}
	return false
}

// PermissionsFor returns the principal's effective permissions, split by
// scope: the permissions granted platform-wide (by its unscoped roles) and,
// per pipeline, the resource-scoped permissions granted for that pipeline (by
// its pipeline-scoped roles).
func (e *Engine) PermissionsFor(ctx context.Context, p Principal) (*PermissionSet, error) {
	e.mu.Lock()
	cat, err := e.catalogLocked(ctx)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	set := &PermissionSet{
		PlatformWide: map[string]bool{},
		Scoped:       map[int64]map[string]bool{},
	}
	for roleName, g := range principalGrants(cat, p) {
		perms, ok := cat.rolePerms[roleName]
		if !ok {
			continue
		}
		for perm := range perms {
			if g.unscoped {
				set.PlatformWide[perm] = true
			}
			for pid := range g.pipelines {
				if !isResourceScoped(perm) {
					continue
				}
				if set.Scoped[pid] == nil {
					set.Scoped[pid] = map[string]bool{}
				}
				set.Scoped[pid][perm] = true
			}
		}
	}
	e.mu.Unlock()
	return set, nil
}

// CanGrant reports whether the caller may bind the named role to a target
// principal with the given scope (targetPipelineID of 0 means unscoped).
// Granting a role is itself a permission (roles.can-assign), and it is
// bounded: the caller may bind a role only if the caller already holds that
// role (unscoped, or with a scope at least as wide as the one being granted).
// This is what keeps an operator from promoting someone to admin, and a
// pipeline-scoped operator from granting platform-wide operator; the admin
// role is grantable only by a principal that holds it.
func (e *Engine) CanGrant(ctx context.Context, caller Principal, roleName string, targetPipelineID int64) (bool, error) {
	e.mu.Lock()
	cat, err := e.catalogLocked(ctx)
	if err != nil {
		e.mu.Unlock()
		return false, err
	}
	ok := canGrantLocked(cat, caller, roleName, targetPipelineID)
	e.mu.Unlock()
	return ok, nil
}

func canGrantLocked(cat *catalog, caller Principal, roleName string, targetPipelineID int64) bool {
	// The caller must hold roles.can-assign (a platform-wide permission, so
	// it must be granted unscoped).
	if !checkLocked(cat, caller, PermRolesAssign, Resource{}) {
		return false
	}
	// The caller must hold the role being granted, with a scope at least as
	// wide as the one being granted. Only the caller's directly-held roles
	// (token roles and stored bindings) count — not roles reached by
	// composition — so a principal cannot delegate a role it merely inherits.
	g, ok := principalGrants(cat, caller)[roleName]
	if !ok {
		return false
	}
	if targetPipelineID == 0 {
		// Granting platform-wide: the caller must hold the role unscoped.
		return g.unscoped
	}
	// Granting scoped to a pipeline: the caller must hold the role unscoped
	// (which covers every pipeline) or scoped to that same pipeline.
	return g.unscoped || g.pipelines[targetPipelineID]
}
