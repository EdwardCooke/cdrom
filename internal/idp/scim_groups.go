// SCIM /Groups resource (F-28.5, Slice 2): the concrete Group endpoints over
// the existing SCIM frame (F-28.1) and caller guard (F-28.2).
//
// Routes (mounted under the scimGuard in mountSCIM):
//
//	GET    /Groups            paginated, filterable list
//	POST   /Groups            create (embedded members processed; dup
//	                          displayName is LEGAL — no 409)
//	GET    /Groups/{id}       fetch one group, members EXPANDED
//	PUT    /Groups/{id}       DEFINITE replace of attributes + full member set
//	PATCH  /Groups/{id}       ops on members (add/remove) and
//	                          displayName/description (replace)
//	DELETE /Groups/{id}       hard-delete (recreatable; no tombstone)
//
// Serialization follows the RFC 7643 Group schema: displayName⇄DisplayName,
// description⇄Description, members[{value,$ref,displayName}]⇄MemberValues.
// Membership values are OPAQUE (taken verbatim, trimmed, deduplicated) —
// resolution against the user directory is BEST-EFFORT at read time: a value
// resolving to a LIVE user expands to {$ref, value, displayName}; an
// unresolvable value is KEPT, expands to {value} alone, and is WARN-LOGGED
// with the value (troubleshooting aid). Writes are never refused on its
// account. meta.created/meta.modified echo the ROW's actual timestamps; the
// resource id is the stable numeric ID (decimal string).
//
// Filters: the supported subset (displayName eq, members[value] eq,
// AND-chains, quoted values) translates into GENERIC EQUALITY CONSTRAINTS
// pushed down INTO THE STORE (evaluated in the Database service — the IdP
// transfers only matching rows, never the population; F-28.5 ruling 5).
//
// Every mutation is audited (actor = the presenting connector, per the
// guard's context attribution) and metered (cdrom_scim_operations_total,
// group_* op values); reads are neither. The ROLE-DIFF audit companions
// await Slice 3 (the mapping applicator); the hooks are reserved here.
package idp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/audit"
)

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

// userResolutionDirectory is the sliver of the user directory the group
// codec CONSULTS for member expansion (best-effort; nil when unwired —
// expansions then degrade to {value} for every member).
type userResolutionDirectory interface {
	// resolveLive maps a MEMBER VALUE to the LIVE user it denotes: the
	// external_id first (the assumed provider identifier), then the
	// username (providers that stuff the login name in). Tombstoned users
	// count as unresolvable. ok=false ⇒ no live match.
	resolveLive(ctx context.Context, value string) (*User, bool)
}

// dirResolver adapts the UserStore to userResolutionDirectory.
type dirResolver struct {
	store UserStore
}

func (d dirResolver) resolveLive(ctx context.Context, value string) (*User, bool) {
	if d.store == nil || value == "" {
		return nil, false
	}
	if u, err := d.store.FindByExternalID(ctx, value, false); err == nil {
		return u, true
	}
	if u, err := d.store.GetByUsername(ctx, value, false); err == nil {
		return u, true
	}
	return nil, false
}

// scimMemberDisplayName synthesizes a resolved user's display name (the User
// struct carries no such field): "Given Family" when either part is known,
// else the username.
func scimMemberDisplayName(u *User) string {
	if u.FirstName != "" || u.LastName != "" {
		if dn := strings.TrimSpace(u.FirstName + " " + u.LastName); dn != "" {
			return dn
		}
	}
	return u.Username
}

// userResolutionDirectory returns the resolution adapter. ALWAYS non-nil
// (calling a method on a nil interface would panic); when the user directory
// is unwired the adapter's store is nil and resolveLive degrades to
// "unresolvable" for every value ({value}-only expansions).
func (s *Server) userResolutionDirectory() userResolutionDirectory {
	return dirResolver{store: s.scimUsers}
}

// scimGroupDoc renders a stored group as a SCIM Group document (RFC 7643),
// expanding its membership against the USER DIRECTORY (best-effort; see the
// file preamble). The meta block echoes the ROW's actual created/modified
// instants and the public location of the resource.
func (s *Server) scimGroupDoc(r *http.Request, g *SCIMGroup) map[string]any {
	dir := s.userResolutionDirectory()
	members := make([]map[string]any, 0, len(g.MemberValues))
	for _, v := range g.MemberValues {
		entry := map[string]any{"value": v}
		if u, ok := dir.resolveLive(r.Context(), v); ok {
			entry["$ref"] = s.scimLocation(r, "/Users/"+u.ID)
			entry["displayName"] = scimMemberDisplayName(u)
		} else {
			// Unresolvable: KEEP the value, warn-log it (troubleshooting
			// aid — the provider's notion of a member id we cannot match).
			s.logger.Warn("scim: group member value does not resolve to a live user",
				"group", g.ID, "value", v)
		}
		members = append(members, entry)
	}
	doc := map[string]any{
		"schemas":     []string{scimSchemaGroup},
		"id":          g.ID,
		"displayName": g.DisplayName,
		"members":     members,
	}
	if g.Description != "" {
		doc["description"] = g.Description
	}
	if g.ExternalID != "" {
		doc["externalId"] = g.ExternalID
	}
	meta := map[string]any{
		"resourceType": scimRTGroup,
		"location":     s.scimLocation(r, "/Groups/"+g.ID),
	}
	if !g.CreatedAt.IsZero() {
		meta["created"] = g.CreatedAt.UTC().Format(time.RFC3339)
	}
	if !g.UpdatedAt.IsZero() {
		meta["modified"] = g.UpdatedAt.UTC().Format(time.RFC3339)
	}
	doc["meta"] = meta
	return doc
}

// ---------------------------------------------------------------------------
// Payload decoding
// ---------------------------------------------------------------------------

// scimGroupPayload is the decoded inbound Group document (tracked fields).
type scimGroupPayload struct {
	DisplayName string
	Description string
	// ExternalID is the corporate directory's group identifier (azure
	// objectId et al.) — accepted at CREATION, immutable afterwards.
	ExternalID   string
	MemberValues []string
}

// decodeSCIMGroup extracts the tracked fields from a SCIM Group document.
// Lenient about unrecognized attributes (the directory keeps only what it
// models) but strict about the shapes it does: a missing/blank displayName
// yields an error the caller renders as 400 invalidValue. Member values are
// TAKEN VERBATIM (stores trim + deduplicate; here we only extract).
func decodeSCIMGroup(raw map[string]any) (*scimGroupPayload, error) {
	doc := &scimGroupPayload{}
	nameAny, ok := raw["displayName"]
	if !ok {
		return nil, errors.New("displayName is required")
	}
	dname, ok := nameAny.(string)
	if !ok || strings.TrimSpace(dname) == "" {
		return nil, errors.New("displayName must be a non-empty string")
	}
	doc.DisplayName = dname
	if desc, ok := raw["description"].(string); ok {
		doc.Description = desc
	}
	if ext, ok := raw["externalId"].(string); ok {
		doc.ExternalID = ext
	}
	if ms, ok := raw["members"].([]any); ok {
		for _, m := range ms {
			obj, ok := m.(map[string]any)
			if !ok {
				continue
			}
			val, _ := obj["value"].(string)
			if val == "" {
				continue
			}
			// The VALUE is the member's identifier (opaque — stored
			// verbatim). $ref/displayName are ACKNOWLEDGED AND IGNORED:
			// the directory keys membership on the value, never on the ref.
			doc.MemberValues = append(doc.MemberValues, val)
		}
	}
	return doc, nil
}

// ---------------------------------------------------------------------------
// Filters (supported subset)
// ---------------------------------------------------------------------------

// scimGroupFilterTranslator converts the SUPPORTED GROUP FILTER SUBSET
// (F-28.5) into GENERIC EQUALITY CONSTRAINTS enforced SERVER-SIDE (database
// pushdown — the IdP receives only matching rows). Attributes map onto the
// store's canonical vocabulary with CASE FOLDING PINNED PER ATTRIBUTE
// (directory convention: display names fold; member values are EXACT — they
// are provider identifiers); the comparator is restricted to `eq`, chaining
// to AND, values to double-quoted strings (spaces permitted) or bare tokens.
// Anything outside the subset — other comparators, OR, parentheses,
// wildcards, unterminated quotes — is an error the caller renders as 400
// invalidFilter.
var scimGroupFilterTranslator = map[string]struct {
	canonical     string
	caseSensitive bool
}{
	"displayName":    {canonical: "display_name", caseSensitive: false},
	"externalId":     {canonical: "external_id", caseSensitive: true},
	"members[value]": {canonical: "members_value", caseSensitive: true},
}

// parseSCIMGroupFilter parses the supported GROUP filter subset into GENERIC
// EQUALITY CONSTRAINTS (translation: see scimGroupFilterTranslator). An
// empty expression yields nil (unconstrained). Shares the tokenizer with the
// user filter (scimFilterTokens).
func parseSCIMGroupFilter(expr string) (*EqualityConstraints, error) {
	src := strings.TrimSpace(expr)
	if src == "" {
		return nil, nil
	}
	fields, err := scimFilterTokens(src)
	if err != nil {
		return nil, err
	}
	constraints := EqualityConstraints{}
	i := 0
	for i < len(fields) {
		if i+2 >= len(fields) {
			return nil, errors.New("malformed filter term")
		}
		attr := fields[i]
		tr, ok := scimGroupFilterTranslator[attr]
		if !ok {
			return nil, errors.New("unsupported filter attribute: " + attr)
		}
		if !strings.EqualFold(fields[i+1], "eq") {
			return nil, errors.New("only the 'eq' comparator is supported")
		}
		val := fields[i+2]
		// Strip ENCLOSING quotes (kept by the tokenizer so intra-quote
		// spaces survive splitting).
		if len(val) >= 2 && strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") {
			val = val[1 : len(val)-1]
		}
		constraints = append(constraints, EqualityConstraint{
			Attribute:     tr.canonical,
			Value:         val,
			CaseSensitive: tr.caseSensitive,
		})
		i += 3
		if i < len(fields) {
			if !strings.EqualFold(fields[i], "and") {
				return nil, errors.New("only AND-chaining is supported")
			}
			i++
		}
	}
	return &constraints, nil
}

// ---------------------------------------------------------------------------
// Mutation bookkeeping (audit + metrics)
// ---------------------------------------------------------------------------

// auditSCIMGroupOp records a SCIM GROUP mutation attributable to the
// PRESENTING CONNECTOR (the guard's context identity). Anti-noise rule
// (bulk-sync volume, cf. auditSCIMOp): connectors resync the whole directory
// periodically, so most PUT/PATCH contacts move nothing — those are recorded
// LEAN (contact fact + changed:false marker, no blobs). Only when the
// tracked state ACTUALLY MOVED (displayName/description/membership differ)
// are the full before/after documents attached. Pass nil for either side to
// force the full record (create, delete — transitions that always mean
// something happened).
func (s *Server) auditSCIMGroupOp(r *http.Request, op, targetID, targetName string, before, after *SCIMGroup) {
	if s.scimUsersAuditor == nil {
		return
	}
	moved := before == nil || after == nil || scimGroupMoved(before, after)
	var oldVal, newVal any
	if moved {
		oldVal, newVal = scimGroupAuditDoc(before), scimGroupAuditDoc(after)
	}
	s.scimUsersAuditor.Record(audit.Event{
		Actor:      SCIMCallerFromContext(r.Context()),
		ActorKind:  audit.ActorKindSystem,
		Action:     "scim.group." + op,
		TargetKind: audit.TargetSCIMGroup,
		TargetID:   targetID,
		TargetName: targetName,
		Outcome:    audit.OutcomeSuccess,
		SourceIP:   remoteIP(r),
		OldValue:   oldVal,
		NewValue:   newVal,
		Details:    map[string]any{"method": r.Method, "path": r.URL.Path, "changed": moved},
	})
}

// scimGroupMoved reports whether the TRACKED STATE DIFFERS between two group
// snapshots (timestamps shift on every touch, so they are excluded —
// comparing them would defeat the anti-noise rule).
func scimGroupMoved(a, b *SCIMGroup) bool {
	if a.DisplayName != b.DisplayName || a.Description != b.Description {
		return true
	}
	return !equalSortedSets(a.MemberValues, b.MemberValues)
}

// equalSortedSets compares two membership sets ignoring ORDER (multiplicity
// is respected — moot in practice, values are deduplicated at write time).
func equalSortedSets(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	xs := append([]string(nil), x...)
	ys := append([]string(nil), y...)
	sort.Strings(xs)
	sort.Strings(ys)
	for i := range xs {
		if xs[i] != ys[i] {
			return false
		}
	}
	return true
}

// scimGroupAuditDoc renders a group's auditable profile as a STRUCTURED
// DOCUMENT (the audit layer encodes it at the render/store boundary).
func scimGroupAuditDoc(g *SCIMGroup) any {
	if g == nil {
		return nil
	}
	return map[string]any{
		"display_name":  g.DisplayName,
		"description":   g.Description,
		"external_id":   g.ExternalID,
		"member_values": append([]string(nil), g.MemberValues...),
	}
}

// ---------------------------------------------------------------------------
// Item loader
// ---------------------------------------------------------------------------

// loadSCIMGroup loads the item for the {id} route segment. Unknown ids are
// 404 notFound; malformed ids are 400 invalidValue.
func (s *Server) loadSCIMGroup(w http.ResponseWriter, r *http.Request) (*SCIMGroup, bool) {
	store := s.scimGroups
	if store == nil {
		s.scimErrTyped(w, r, http.StatusServiceUnavailable, "", "the group directory is not configured")
		return nil, false
	}
	id := r.PathValue("id")
	g, err := store.Get(r.Context(), id)
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			s.scimErrTyped(w, r, http.StatusNotFound, scimTypeNotFound, "no group with id "+strconv.Quote(id))
		case codes.InvalidArgument:
			s.scimErrTyped(w, r, http.StatusBadRequest, scimTypeInvalidValue, "malformed group id "+strconv.Quote(id))
		default:
			s.scimErrTyped(w, r, http.StatusInternalServerError, "", "loading the group failed")
		}
		return nil, false
	}
	return g, true
}

// ---------------------------------------------------------------------------
// Collection: GET /Groups
// ---------------------------------------------------------------------------

// handleSCIMGroupsList lists groups, applies the filter subset AS GENERIC
// EQUALITY CONSTRAINTS PUSHED DOWN INTO THE STORE (evaluated in the database
// — the IdP transfers only matching rows, never the population; F-28.5
// ruling 5), then paginates (startIndex 1-based, count honored/clamped,
// totalResults post-filter).
func (s *Server) handleSCIMGroupsList(w http.ResponseWriter, r *http.Request) {
	store := s.scimGroups
	if store == nil {
		s.scimErrTyped(w, r, http.StatusServiceUnavailable, "", "the group directory is not configured")
		return
	}
	q := r.URL.Query()
	constraints, err := parseSCIMGroupFilter(q.Get("filter"))
	if err != nil {
		s.scimErrTyped(w, r, http.StatusBadRequest, scimTypeInvalidFilter, "unsupported filter: "+err.Error())
		return
	}
	countHint, err := scimCountParam(q.Get("count"))
	if err != nil {
		s.scimErrTyped(w, r, http.StatusBadRequest, scimTypeInvalidValue, err.Error()+" (count)")
		return
	}
	startIdx, ok := scimPageStart(r)
	if !ok {
		s.scimErrTyped(w, r, http.StatusBadRequest, scimTypeInvalidValue, "startIndex must be a positive integer")
		return
	}
	groups, err := store.List(r.Context(), constraints)
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			s.scimErrTyped(w, r, http.StatusBadRequest, scimTypeInvalidFilter, "unsupported filter attribute")
			return
		}
		s.scimErrTyped(w, r, http.StatusInternalServerError, "", "listing groups failed")
		return
	}
	resources := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		resources = append(resources, s.scimGroupDoc(r, g))
	}
	s.scimWrite(w, r, http.StatusOK, s.scimListDocPaged(resources, startIdx, countHint))
}

// ---------------------------------------------------------------------------
// Collection: POST /Groups
// ---------------------------------------------------------------------------

// handleSCIMGroupsCreate creates a group (embedded members processed;
// unresolvable values stored + logged, per the opacity ruling). Duplicate
// display names are LEGAL (ruling 4 — no 409).
func (s *Server) handleSCIMGroupsCreate(w http.ResponseWriter, r *http.Request) {
	store := s.scimGroups
	if store == nil {
		s.scimErrTyped(w, r, http.StatusServiceUnavailable, "", "the group directory is not configured")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		s.scimErrTyped(w, r, http.StatusBadRequest, scimTypeInvalidValue, "unreadable request body")
		return
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		s.scimErrTyped(w, r, http.StatusBadRequest, scimTypeInvalidValue, "body must be a JSON object")
		return
	}
	payload, err := decodeSCIMGroup(doc)
	if err != nil {
		s.scimErrTyped(w, r, http.StatusBadRequest, scimTypeInvalidValue, err.Error())
		return
	}
	g, err := store.Create(r.Context(), payload.DisplayName, payload.Description, payload.ExternalID, payload.MemberValues)
	if err != nil {
		s.finishSCIMOp(r, "group_create", "failure")
		s.scimErrTyped(w, r, http.StatusInternalServerError, "", "creating the group failed")
		return
	}
	s.finishSCIMOp(r, "group_create", "success")
	s.auditSCIMGroupOp(r, "create", g.ID, g.DisplayName, nil, g)
	w.Header().Set("Location", s.scimLocation(r, "/Groups/"+g.ID))
	s.scimWrite(w, r, http.StatusCreated, s.scimGroupDoc(r, g))
}

// ---------------------------------------------------------------------------
// Items: GET/PUT/PATCH/DELETE /Groups/{id}
// ---------------------------------------------------------------------------

// handleSCIMGroupGet fetches one group (members expanded).
func (s *Server) handleSCIMGroupGet(w http.ResponseWriter, r *http.Request) {
	g, ok := s.loadSCIMGroup(w, r)
	if !ok {
		return
	}
	s.scimWrite(w, r, http.StatusOK, s.scimGroupDoc(r, g))
}

// handleSCIMGroupPut performs the DEFINITE replace: attributes SET to the
// posted values (omitted ⇒ "") and the ENTIRE membership swapped for the
// posted set (omitted/empty ⇒ emptied).
func (s *Server) handleSCIMGroupPut(w http.ResponseWriter, r *http.Request) {
	current, ok := s.loadSCIMGroup(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		s.groupPutPatchMalformed(w, r, "unreadable request body")
		return
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		s.groupPutPatchMalformed(w, r, "body must be a JSON object")
		return
	}
	payload, err := decodeSCIMGroup(doc)
	if err != nil {
		s.groupPutPatchMalformed(w, r, err.Error())
		return
	}
	// externalId is IMMUTABLE (the provider's group identity is stable for
	// the group's life): a differing posted value is 400 (absent or equal is
	// a no-op — the current value is carried through the replace).
	if ext, ok := doc["externalId"].(string); ok && ext != "" && ext != current.ExternalID {
		s.groupPutPatchMalformed(w, r, "externalId is immutable")
		return
	}
	refreshed, err := s.scimGroups.Replace(r.Context(), current.ID, payload.DisplayName, payload.Description, current.ExternalID, payload.MemberValues)
	if err != nil {
		s.finishSCIMOp(r, "group_update", "failure")
		s.renderSCIMGroupMutationError(w, r, err)
		return
	}
	s.finishSCIMOp(r, "group_update", "success")
	s.auditSCIMGroupOp(r, "replace", refreshed.ID, refreshed.DisplayName, current, refreshed)
	s.scimWrite(w, r, http.StatusOK, s.scimGroupDoc(r, refreshed))
}

// handleSCIMGroupPatch applies SCIM operations: add/replace on
// displayName/description and add/remove on members. Ops compose over the
// CURRENT state (sparse), then commit: attribute moves ride the definite
// Replace (carrying the CURRENT membership); membership moves ride the
// ATOMIC PatchMembers (server-side read-modify-write). Last-writer-wins
// (ETags deferred).
func (s *Server) handleSCIMGroupPatch(w http.ResponseWriter, r *http.Request) {
	current, ok := s.loadSCIMGroup(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		s.groupPutPatchMalformed(w, r, "unreadable request body")
		return
	}
	var envelope struct {
		Schemas    []string         `json:"schemas"`
		Operations []map[string]any `json:"operations"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Operations) == 0 {
		s.groupPutPatchMalformed(w, r, "a non-empty operations array is required")
		return
	}
	newName, newDesc := current.DisplayName, current.Description
	attrChanged := false
	addVals, delVals := []string{}, []string{}
	for _, opRaw := range envelope.Operations {
		opKind, _ := opRaw["op"].(string)
		path, _ := opRaw["path"].(string)
		path = strings.TrimPrefix(strings.TrimSpace(path), "/")
		value, hasValue := opRaw["value"]
		switch strings.ToLower(opKind) {
		case "add", "replace":
			if !hasValue {
				s.groupPutPatchMalformed(w, r, "op '"+opKind+"' requires a value")
				return
			}
			switch path {
			case "displayName":
				str, ok := value.(string)
				if !ok || strings.TrimSpace(str) == "" {
					s.groupPutPatchMalformed(w, r, "displayName requires a non-empty string value")
					return
				}
				newName, attrChanged = str, true
			case "description":
				str, _ := value.(string)
				newDesc, attrChanged = str, true
			case "members":
				vs, err := scimPatchMemberValues(value)
				if err != nil {
					s.groupPutPatchMalformed(w, r, err.Error())
					return
				}
				addVals = append(addVals, vs...)
			default:
				s.groupPutPatchMalformed(w, r, "unsupported attribute: "+path)
				return
			}
		case "remove":
			switch path {
			case "members":
				vs, err := scimPatchMemberValues(value)
				if err != nil {
					s.groupPutPatchMalformed(w, r, err.Error())
					return
				}
				delVals = append(delVals, vs...)
			case "description":
				newDesc, attrChanged = "", true
			default:
				s.groupPutPatchMalformed(w, r, "unsupported attribute: "+path)
				return
			}
		default:
			s.groupPutPatchMalformed(w, r, "unsupported op: "+opKind)
			return
		}
	}
	var refreshed *SCIMGroup
	if attrChanged {
		// The attribute leg carries the group's EXTERNAL ID unchanged
		// (immutable — PATCH has no externalId path).
		attrs, err := s.scimGroups.Replace(r.Context(), current.ID, newName, newDesc, current.ExternalID, current.MemberValues)
		if err != nil {
			s.finishSCIMOp(r, "group_update", "failure")
			s.renderSCIMGroupMutationError(w, r, err)
			return
		}
		refreshed = attrs
	} else {
		refreshed = current
	}
	if len(addVals)+len(delVals) > 0 {
		patched, err := s.scimGroups.PatchMembers(r.Context(), current.ID, addVals, delVals)
		if err != nil {
			s.finishSCIMOp(r, "group_update", "failure")
			s.renderSCIMGroupMutationError(w, r, err)
			return
		}
		refreshed = patched
	}
	op := "members"
	if attrChanged {
		op = "replace"
	}
	s.finishSCIMOp(r, "group_update", "success")
	s.auditSCIMGroupOp(r, op, refreshed.ID, refreshed.DisplayName, current, refreshed)
	s.scimWrite(w, r, http.StatusOK, s.scimGroupDoc(r, refreshed))
}

// scimPatchMemberValues extracts the member VALUES from a PATCH op's value:
// a STRING (bare value), a {value:...} OBJECT, or an ARRAY thereof.
func scimPatchMemberValues(value any) ([]string, error) {
	var out []string
	appendEntry := func(entry any) error {
		switch v := entry.(type) {
		case string:
			if strings.TrimSpace(v) == "" {
				return errors.New("member value must be a non-empty string")
			}
			out = append(out, v)
		case map[string]any:
			val, _ := v["value"].(string)
			if val == "" {
				return errors.New("member objects require a non-empty value")
			}
			out = append(out, val)
		default:
			return errors.New("member values must be strings or {value} objects")
		}
		return nil
	}
	switch v := value.(type) {
	case string:
		if err := appendEntry(v); err != nil {
			return nil, err
		}
	case []any:
		for _, e := range v {
			if err := appendEntry(e); err != nil {
				return nil, err
			}
		}
	default:
		if err := appendEntry(value); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// handleSCIMGroupDelete hard-deletes the group (recreatable; no tombstone).
// The ROLE-DIFF revocation awaits Slice 3 (the mapping applicator).
func (s *Server) handleSCIMGroupDelete(w http.ResponseWriter, r *http.Request) {
	current, ok := s.loadSCIMGroup(w, r)
	if !ok {
		return
	}
	if err := s.scimGroups.Delete(r.Context(), current.ID); err != nil {
		s.finishSCIMOp(r, "group_delete", "failure")
		s.scimErrTyped(w, r, http.StatusInternalServerError, "", "deleting the group failed")
		return
	}
	s.finishSCIMOp(r, "group_delete", "success")
	s.auditSCIMGroupOp(r, "delete", current.ID, current.DisplayName, current, nil)
	w.WriteHeader(http.StatusNoContent)
}

// groupPutPatchMalformed renders a PUT/PATCH malformation as 400 (invalidValue
// for shape faults, unsupportedAttribute for unknown paths and immutability
// breaches — MIRRORING the /Users classifier's treatment) and meters the
// failure.
func (s *Server) groupPutPatchMalformed(w http.ResponseWriter, r *http.Request, msg string) {
	scimType := scimTypeInvalidValue
	if strings.Contains(msg, "unsupported") || strings.Contains(msg, "immu") {
		scimType = scimTypeUnsupportedAttribute
	}
	s.finishSCIMOp(r, "group_update", "failure")
	s.scimErrTyped(w, r, http.StatusBadRequest, scimType, msg)
}

// renderSCIMGroupMutationError maps a store/RPC error to its SCIM rendition
// (NotFound ⇒ 404; else 500). The caller has ALREADY metered the failure.
func (s *Server) renderSCIMGroupMutationError(w http.ResponseWriter, r *http.Request, err error) {
	switch status.Code(err) {
	case codes.NotFound:
		s.scimErrTyped(w, r, http.StatusNotFound, scimTypeNotFound, "no such group")
	default:
		s.scimErrTyped(w, r, http.StatusInternalServerError, "", "updating the group failed")
	}
}
