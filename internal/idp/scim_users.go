// SCIM /Users resource (F-28.4, Slice 2): the concrete User endpoints over
// the existing SCIM frame (F-28.1) and caller guard (F-28.2).
//
// Routes (mounted under the scimGuard in mountSCIM):
//
//	GET    /Users            paginated, filterable list (active rows only)
//	POST   /Users            provision | re-provision | rename (conflict matrix)
//	GET    /Users/{id}       fetch one active user
//	PUT    /Users/{id}       DEFINITE replace of the tracked profile fields
//	PATCH  /Users/{id}       apply operations (read-modify-write, LWOW)
//	DELETE /Users/{id}       deprovision (F-28.3 tombstone semantics)
//
// Serialization follows the RFC 7643 User schema mapped onto IDPUser per the
// F-28.4 spec: userName⇄Username (the anchor), name.givenName/familyName⇄
// First/LastName, active⇄Active, externalId⇄ExternalID (sticky),
// emails[type=primary].value⇄Email (informational), groups⇒parsed and
// DISCARDED (debug-logged) until F-28.5 lands group→role application.
// meta.created/meta.modified echo the ROW's actual timestamps (not emission
// time); the resource id is the stable numeric ID (decimal string).
//
// Conflict matrix (POST resolution order: externalId match FIRST, then
// userName):
//
//	no match                           → 201 provision (stock role
//	                                        placeholder, provisioned=true,
//	                                        active per payload or true)
//	tombstoned row matches (ext/name)  → 200 re-provision: Reactivate +
//	                                        definite repopulate, same stable
//	                                        ID/history
//	active row matches externalId      → 200 rename/repopulate (got-married);
//	                                        a new userName colliding with
//	                                        ANOTHER active row ⇒ 409 uniqueness
//	active row matches userName only   → 409 duplicate (strict; never a second
//	                                       row)
//	item ops on unknown/tombstoned id  → 404 notFound
//
// Every mutation is audited (actor = the presenting connector, per the
// guard's context attribution) and metered (cdrom_scim_operations_total);
// reads are neither.
package idp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/audit"
)

const (
	// scimStockRolePlaceholder is the role set a SCIM-provisioned user is born
	// with UNTIL F-28.5 lands group→role derivation (the spec's placeholder).
	scimStockRolePlaceholder = "user"

	// SCIM error TYPE vocabulary (RFC 7644 §3.7), scalar where cheap.
	scimTypeUniqueness           = "uniqueness"
	scimTypeNotFound             = "notFound"
	scimTypeInvalidFilter        = "invalidFilter"
	scimTypeInvalidValue         = "invalidValue"
	scimTypeUnsupportedAttribute = "unsupportedAttribute"
)

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

// scimErrTyped emits an RFC 7644 §3.7-shaped SCIM error body WITH a scalar
// scimType (extended over scimErr, which omits the type).
func (s *Server) scimErrTyped(w http.ResponseWriter, r *http.Request, code int, scimType, detail string) {
	s.logger.Debug("scim: rejecting request", "method", r.Method, "path", r.URL.Path, "status", code, "remote", r.RemoteAddr)
	doc := map[string]any{
		"schemas": []string{scimSchemaError},
		"status":  strconv.Itoa(code),
		"detail":  detail,
	}
	if scimType != "" {
		doc["scimType"] = scimType
	}
	s.scimWrite(w, r, code, doc)
}

// scimListDocPaged wraps resources in the RFC 7644 ListResponse honoring the
// REQUESTED page size (count): the effective page bound is min(requested,
// configured) — RFC 7644 permits the server to shrink the page, never grow
// it. totalResults reflects the FULL (post-filter) population.
func (s *Server) scimListDocPaged(resources []map[string]any, startIndex, countHint int) map[string]any {
	bound := s.cfg.Scim.EffectiveMaxPageSize()
	if countHint > 0 && countHint < bound {
		bound = countHint
	}
	total := len(resources)
	from0, to0 := scimClamp(startIndex-1, startIndex-1+bound, total)
	page := resources[from0:to0]
	return map[string]any{
		"schemas":      []string{scimSchemaListResponse},
		"totalResults": total,
		"itemsPerPage": len(page),
		"startIndex":   from0 + 1,
		"Resources":    page,
	}
}

// ---------------------------------------------------------------------------
// Payload decoding
// ---------------------------------------------------------------------------

// scimUserPayload is the decoded inbound User document (tracked fields).
// Active is a POINTER: absent means "take the default (true)" — the
// distinguishing factor driving the conflict matrix and PUT semantics.
type scimUserPayload struct {
	UserName     string
	GivenName    string
	FamilyName   string
	PrimaryEmail string
	ExternalID   string
	Active       *bool
	HadGroups    bool // groups present (acknowledged, discarded until F-28.5)
}

// decodeSCIMUser extracts the tracked fields from a SCIM User document. It is
// LENIENT about unrecognized attributes (the directory keeps only what it
// models) but STRICT about the shapes it does: a missing/blank/non-string
// userName yields an error the caller renders as 400 invalidValue.
func decodeSCIMUser(raw map[string]any) (*scimUserPayload, error) {
	doc := &scimUserPayload{}
	nameAny, ok := raw["userName"]
	if !ok {
		return nil, errors.New("userName is required")
	}
	uname, ok := nameAny.(string)
	if !ok || strings.TrimSpace(uname) == "" {
		return nil, errors.New("userName must be a non-empty string")
	}
	doc.UserName = uname

	switch nm := raw["name"].(type) {
	case map[string]any:
		if gn, ok := nm["givenName"].(string); ok {
			doc.GivenName = gn
		}
		if fn, ok := nm["familyName"].(string); ok {
			doc.FamilyName = fn
		}
	case string:
		// TOLERATED COMPACT FORM: the full name as a single string.
		doc.GivenName, doc.FamilyName = splitCompactName(nm)
	}
	if es, ok := raw["emails"].([]any); ok {
		for _, e := range es {
			em, ok := e.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := em["type"].(string)
			val, _ := em["value"].(string)
			if val == "" {
				continue
			}
			// The PRIMARY entry fills Email; a lone typed-less entry is
			// tolerated as primary. Non-primary entries are acknowledged and
			// ignored (the directory models one informational email).
			if typ == "primary" || (typ == "" && doc.PrimaryEmail == "") {
				doc.PrimaryEmail = val
			}
		}
	}
	if ext, ok := raw["externalId"].(string); ok {
		doc.ExternalID = ext
	}
	if a, ok := raw["active"].(bool); ok {
		doc.Active = &a
	}
	if _, ok := raw["groups"]; ok {
		doc.HadGroups = true
	}
	return doc, nil
}

// splitCompactName partitions a COMPACT full-name string ("Jane Q. Public")
// into (First, Last): the first whitespace-delimited token is the given name,
// the trimmed remainder the family name; a single token leaves the family
// name empty.
func splitCompactName(full string) (string, string) {
	full = strings.Join(strings.Fields(full), " ")
	sp := strings.IndexByte(full, ' ')
	if sp <= 0 {
		return full, ""
	}
	return full[:sp], full[sp+1:]
}

// scimUserDoc renders a stored user as a SCIM User document (RFC 7643). The
// meta block echoes the ROW's actual created/modified instants (F-28.4: not
// emission time) and the public location of the resource.
func (s *Server) scimUserDoc(r *http.Request, u *User) map[string]any {
	doc := map[string]any{
		"schemas":  []string{scimSchemaUser},
		"id":       u.ID,
		"userName": u.Username,
		"name":     map[string]any{"givenName": u.FirstName, "familyName": u.LastName},
		"active":   u.Active,
	}
	if u.Email != "" {
		doc["emails"] = []map[string]any{{"value": u.Email, "type": "primary", "primary": true}}
	}
	if u.ExternalID != "" {
		doc["externalId"] = u.ExternalID
	}
	meta := map[string]any{
		"resourceType": scimRTUser,
		"location":     s.scimLocation(r, "/Users/"+u.ID),
	}
	if !u.CreatedAt.IsZero() {
		meta["created"] = u.CreatedAt.UTC().Format(time.RFC3339)
	}
	if !u.UpdatedAt.IsZero() {
		meta["modified"] = u.UpdatedAt.UTC().Format(time.RFC3339)
	}
	doc["meta"] = meta
	return doc
}

// ---------------------------------------------------------------------------
// Filters (supported subset) + count
// ---------------------------------------------------------------------------

// scimFilterTranslator converts the supported SCIM filter subset (F-28.4
// Slice 2) into GENERIC EQUALITY CONSTRAINTS enforced SERVER-SIDE (database
// pushdown — the IdP receives only matching rows, so the transfer scales
// with the answer, not the population). Attributes map onto the store's
// canonical vocabulary with CASE FOLDING PINNED PER ATTRIBUTE (directory
// convention: usernames fold; externalIds are exact); the comparator is
// restricted to `eq`, chaining to AND, values to double-quoted strings
// (spaces permitted) or bare tokens. Anything outside the subset — other
// comparators, OR, parentheses, wildcards, dotted paths, unterminated
// quotes — is an error the caller renders as 400 invalidFilter.
var scimFilterTranslator = map[string]struct {
	canonical     string
	caseSensitive bool
	isBoolean     bool
}{
	"userName":   {canonical: "username", caseSensitive: false},
	"externalId": {canonical: "external_id", caseSensitive: true},
	"active":     {canonical: "active", caseSensitive: false, isBoolean: true},
}

// scimFilterTokens splits a filter expression into whitespace-delimited
// tokens, honoring DOUBLE-QUOTED SPANS (whitespace inside quotes is VALUE
// CONTENT, not a separator — `userName eq "a b c"` is three logical tokens).
// An unclosed quote is an error.
func scimFilterTokens(src string) ([]string, error) {
	toks := []string{}
	var cur strings.Builder
	inTok := false
	open := false
	flush := func() {
		if inTok {
			toks = append(toks, cur.String())
			cur.Reset()
			inTok = false
		}
	}
	for _, r := range src {
		switch {
		case r == '"':
			open = !open
			cur.WriteRune('"')
			inTok = true
		case unicode.IsSpace(r):
			if open {
				cur.WriteRune(r) // keep intra-quote spaces
				inTok = true
			} else {
				flush()
			}
		default:
			cur.WriteRune(r)
			inTok = true
		}
	}
	if open {
		return nil, errors.New("unterminated quoted filter value")
	}
	flush()
	return toks, nil
}

// parseSCIMFilter parses the supported filter subset into GENERIC EQUALITY
// CONSTRAINTS (translation: see scimFilterTranslator). An empty expression
// yields nil (unconstrained).
func parseSCIMFilter(expr string) (*EqualityConstraints, error) {
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
		tr, ok := scimFilterTranslator[attr]
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
		if tr.isBoolean && val != "true" && val != "false" {
			return nil, errors.New(attr + " filter expects true or false")
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

// scimCountParam parses the `count` query parameter (requested page size).
// Absent ⇒ 0 (use the configured bound); non-positive/non-integer ⇒ error
// (400 invalidValue). Oversized values clamp DOWN in scimListDocPaged.
func scimCountParam(q string) (int, error) {
	if q == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(q)
	if err != nil || n < 1 {
		return 0, errors.New("count must be a positive integer")
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// Mutation bookkeeping (audit + metrics)
// ---------------------------------------------------------------------------

// finishSCIMOp meters a SCIM operation's outcome (low-cardinality enums:
// op ∈ provision/reprovision/update/activate/deactivate/deprovision;
// result ∈ success/failure). Nil-safe (metrics disabled).
func (s *Server) finishSCIMOp(_ *http.Request, op, result string) {
	if s.metrics != nil {
		s.metrics.RecordScimOperation(op, result)
	}
}

// auditSCIMOp records a SCIM mutation attributable to the PRESENTING
// CONNECTOR (the guard's context identity). Secret material is absent by
// construction (profiles carry no password hashes).
//
// Anti-noise rule (bulk-sync volume): enterprise connectors RESYNC the whole
// directory periodically, so the overwhelming majority of PUT/PATCH/resync
// contacts MOVE NOTHING. Those are recorded LEAN — the contact fact (who,
// which user, which op, when) with an `unchanged` marker and NO profile
// blobs. Only when the TRACKED PROFILE ACTUALLY MOVED (before/after differ
// on username/names/email/active/externalId) are the full before/after
// profiles attached (Old/NewValue). Pass nil for either side to force the
// full record (provision, re-provision, deletion — transitions that always
// mean something happened).
func (s *Server) auditSCIMOp(r *http.Request, op, targetID, targetName string, before, after *User) {
	if s.scimUsersAuditor == nil {
		return
	}
	moved := before == nil || after == nil || scimProfileMoved(before, after)
	var oldVal, newVal any
	if moved {
		oldVal, newVal = scimAuditProfile(before), scimAuditProfile(after)
	}
	s.scimUsersAuditor.Record(audit.Event{
		Actor:      SCIMCallerFromContext(r.Context()),
		ActorKind:  audit.ActorKindSystem,
		Action:     "scim." + op,
		TargetKind: audit.TargetUser,
		TargetID:   targetID,
		TargetName: targetName,
		Outcome:    audit.OutcomeSuccess,
		SourceIP:   remoteIP(r),
		OldValue:   oldVal,
		NewValue:   newVal,
		Details:    map[string]any{"method": r.Method, "path": r.URL.Path, "changed": moved},
	})
}

// scimProfileMoved reports whether the TRACKED STATE DIFFERS between two row
// snapshots (ignoring volatile internals — roles are operator-owned and
// preserved through SCIM writes, and the timestamps shift on every touch, so
// comparing them would defeat the anti-noise rule). A TOMBSTONE TRANSITION
// (rehire / departure) always counts as movement even when the profile
// blobs are byte-identical.
func scimProfileMoved(a, b *User) bool {
	if (a.DeletedAt == nil) != (b.DeletedAt == nil) {
		return true
	}
	return a.Username != b.Username ||
		a.FirstName != b.FirstName ||
		a.LastName != b.LastName ||
		a.Email != b.Email ||
		a.Active != b.Active ||
		a.ExternalID != b.ExternalID
}

// scimAuditProfile renders a row's auditable profile as a STRUCTURED DOCUMENT
// (the audit layer encodes it at the render/store boundary — no secrets by
// construction: profiles carry no password hashes).
func scimAuditProfile(u *User) any {
	if u == nil {
		return nil
	}
	return map[string]any{
		"username": u.Username, "first_name": u.FirstName, "last_name": u.LastName,
		"email": u.Email, "active": u.Active, "external_id": u.ExternalID, "roles": u.Roles,
	}
}

// remoteIP extracts the client IP from RemoteAddr (host only).
func remoteIP(r *http.Request) string {
	host := r.RemoteAddr
	if h, _, ok := strings.Cut(host, ":"); ok {
		return h
	}
	return host
}

// scimTransitionOp names the operation implied by an active-state transition
// (fallback: the neutral "update").
func scimTransitionOp(prev, next bool) string {
	switch {
	case !prev && next:
		return "activate"
	case prev && !next:
		return "deactivate"
	default:
		return "update"
	}
}

// ---------------------------------------------------------------------------
// Collection: GET /Users
// ---------------------------------------------------------------------------

// handleSCIMUsersList lists ACTIVE users (tombstoned rows are invisible —
// revival is exclusively POST's job), applies the filter subset AS GENERIC
// EQUALITY CONSTRAINTS PUSHED DOWN INTO THE STORE (evaluated in the
// database — the IdP transfers only matching rows, never the population),
// then paginates (startIndex 1-based, count honored/clamped, totalResults
// post-filter).
func (s *Server) handleSCIMUsersList(w http.ResponseWriter, r *http.Request) {
	store := s.scimUsers
	if store == nil {
		s.scimErrTyped(w, r, http.StatusServiceUnavailable, "", "the user directory is not configured")
		return
	}
	q := r.URL.Query()
	constraints, err := parseSCIMFilter(q.Get("filter"))
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
	users, err := store.List(r.Context(), false, constraints)
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			s.scimErrTyped(w, r, http.StatusBadRequest, scimTypeInvalidFilter, "unsupported filter attribute")
			return
		}
		s.scimErrTyped(w, r, http.StatusInternalServerError, "", "listing users failed")
		return
	}
	resources := make([]map[string]any, 0, len(users))
	for _, u := range users {
		resources = append(resources, s.scimUserDoc(r, u))
	}
	s.scimWrite(w, r, http.StatusOK, s.scimListDocPaged(resources, startIdx, countHint))
}

// ---------------------------------------------------------------------------
// Collection: POST /Users (the conflict matrix)
// ---------------------------------------------------------------------------

// handleSCIMUsersCreate implements the F-28.4 POST conflict matrix.
// Resolution order: externalId match FIRST (identifiers outrank names), then
// userName match.
func (s *Server) handleSCIMUsersCreate(w http.ResponseWriter, r *http.Request) {
	store := s.scimUsers
	if store == nil {
		s.scimErrTyped(w, r, http.StatusServiceUnavailable, "", "the user directory is not configured")
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
	payload, err := decodeSCIMUser(doc)
	if err != nil {
		s.scimErrTyped(w, r, http.StatusBadRequest, scimTypeInvalidValue, err.Error())
		return
	}
	if payload.HadGroups {
		s.logger.Debug("scim: acknowledging and discarding groups (until F-28.5)", "userName", payload.UserName)
	}
	desiredActive := true
	if payload.Active != nil {
		desiredActive = *payload.Active
	}
	ctx := r.Context()

	// Resolve the match INCLUDING tombstoned rows (the rehire case hinges on
	// seeing them). externalId outranks userName.
	var matched *User
	if payload.ExternalID != "" {
		if u, err := store.FindByExternalID(ctx, payload.ExternalID, true); err == nil {
			matched = u
		} else if status.Code(err) != codes.NotFound {
			s.scimErrTyped(w, r, http.StatusInternalServerError, "", "looking up externalId failed")
			return
		}
	}
	if matched == nil {
		if u, err := store.GetByUsername(ctx, payload.UserName, true); err == nil {
			matched = u
		} else if status.Code(err) != codes.NotFound {
			s.scimErrTyped(w, r, http.StatusInternalServerError, "", "looking up userName failed")
			return
		}
	}

	switch {
	case matched == nil:
		// PROVISION: no row (active or tombstoned) bears this externalId or
		// userName — a fresh row is safe (Create keys on username; the lookups
		// above proved it unclaimed, tombstones included).
		user, err := store.Create(ctx, &User{
			Username:    payload.UserName,
			FirstName:   payload.GivenName,
			LastName:    payload.FamilyName,
			Email:       payload.PrimaryEmail,
			ExternalID:  payload.ExternalID,
			Roles:       []string{scimStockRolePlaceholder},
			Provisioned: true,
			Active:      desiredActive,
		})
		if err != nil {
			if status.Code(err) == codes.AlreadyExists {
				s.finishSCIMOp(r, "provision", "failure")
				s.scimErrTyped(w, r, http.StatusConflict, scimTypeUniqueness, "a user with that userName already exists")
				return
			}
			s.finishSCIMOp(r, "provision", "failure")
			s.scimErrTyped(w, r, http.StatusInternalServerError, "", "creating the user failed")
			return
		}
		// Stores BORN users active (the Register/JIT birth semantics); honor
		// a posted active:false by deactivating the newborn immediately.
		if !desiredActive {
			if err := store.Deactivate(ctx, user.ID); err != nil {
				s.finishSCIMOp(r, "provision", "failure")
				s.scimErrTyped(w, r, http.StatusInternalServerError, "", "setting the active state failed")
				return
			}
			if refreshed, err := store.GetByID(ctx, user.ID, false); err == nil {
				user = refreshed
			}
		}
		s.finishSCIMOp(r, "provision", "success")
		s.auditSCIMOp(r, "provision", user.ID, user.Username, nil, user)
		w.Header().Set("Location", s.scimLocation(r, "/Users/"+user.ID))
		s.scimWrite(w, r, http.StatusCreated, s.scimUserDoc(r, user))

	case matched.DeletedAt != nil:
		// RE-PROVISION (rehire): lift the tombstone (same stable ID, history
		// intact), then definitely repopulate the posted profile. Roles are
		// PRESERVED (operator grants survive the hiatus); Reactivate zeroed
		// the password (fresh credentials come from the rightful source).
		if err := store.Reactivate(ctx, matched.ID); err != nil {
			s.finishSCIMOp(r, "reprovision", "failure")
			s.scimErrTyped(w, r, http.StatusInternalServerError, "", "reactivating the user failed")
			return
		}
		repopulated, err := s.definiteProfile(ctx, store, matched.ID, payload, matched.Roles, desiredActive)
		if err != nil {
			s.finishSCIMOp(r, "reprovision", "failure")
			s.renderSCIMMutationError(w, r, err)
			return
		}
		s.finishSCIMOp(r, "reprovision", "success")
		s.auditSCIMOp(r, "reprovision", repopulated.ID, repopulated.Username, matched, repopulated)
		s.scimWrite(w, r, http.StatusOK, s.scimUserDoc(r, repopulated))

	case payload.ExternalID != "" && matched.ExternalID == payload.ExternalID:
		// RENAME/REPOPULATE (got-married): the corporate link identifies the
		// ACTIVE row; adopt the posted userName (colliding with ANOTHER active
		// row ⇒ 409 uniqueness) and refresh the profile. Roles preserved.
		refreshed, err := s.definiteProfile(ctx, store, matched.ID, payload, matched.Roles, desiredActive)
		if err != nil {
			s.finishSCIMOp(r, "update", "failure")
			s.renderSCIMMutationError(w, r, err)
			return
		}
		s.finishSCIMOp(r, "update", "success")
		s.auditSCIMOp(r, "update", refreshed.ID, refreshed.Username, matched, refreshed)
		s.scimWrite(w, r, http.StatusOK, s.scimUserDoc(r, refreshed))

	default:
		// STRICT DUPLICATE: an active row already bears this userName (and no
		// externalId match preceded it) — never a second row.
		s.finishSCIMOp(r, "provision", "failure")
		s.scimErrTyped(w, r, http.StatusConflict, scimTypeUniqueness, "a user with that userName already exists")
	}
}

// definiteProfile pushes a posted profile ONTO an existing row via the
// DEFINITE REPLACE (ReplaceProfile): names/email/username set to the posted
// values (blanks clear), roles set to the GIVEN set (typically the row's
// current roles — preservation), active set to the desired state.
func (s *Server) definiteProfile(ctx context.Context, store UserStore, id string, p *scimUserPayload, roles []string, desiredActive bool) (*User, error) {
	active := desiredActive
	_, err := store.ReplaceProfile(ctx, &ProfileReplacement{
		ID:             id,
		Username:       p.UserName,
		FirstName:      p.GivenName,
		LastName:       p.FamilyName,
		Email:          p.PrimaryEmail,
		Roles:          roles,
		ActiveSignaled: &active,
		ExternalID:     p.ExternalID,
	})
	if err != nil {
		return nil, err
	}
	return store.GetByID(ctx, id, false)
}

// renderSCIMMutationError maps a store/RPC error to its SCIM rendition
// (AlreadyExists ⇒ 409 uniqueness; NotFound ⇒ 404; else 500). The caller has
// ALREADY metered (finishSCIMOp) the failure.
func (s *Server) renderSCIMMutationError(w http.ResponseWriter, r *http.Request, err error) {
	switch status.Code(err) {
	case codes.AlreadyExists:
		s.scimErrTyped(w, r, http.StatusConflict, scimTypeUniqueness, "userName collides with another active user")
	case codes.NotFound:
		s.scimErrTyped(w, r, http.StatusNotFound, scimTypeNotFound, "no such user")
	default:
		s.scimErrTyped(w, r, http.StatusInternalServerError, "", "updating the user failed")
	}
}

// ---------------------------------------------------------------------------
// Items: GET/PUT/PATCH/DELETE /Users/{id}
// ---------------------------------------------------------------------------

// loadSCIMUser loads the item for the {id} route segment. Unknown or
// tombstoned ids are 404 notFound (revival is exclusively POST's job).
func (s *Server) loadSCIMUser(w http.ResponseWriter, r *http.Request) (*User, bool) {
	store := s.scimUsers
	if store == nil {
		s.scimErrTyped(w, r, http.StatusServiceUnavailable, "", "the user directory is not configured")
		return nil, false
	}
	id := r.PathValue("id")
	u, err := store.GetByID(r.Context(), id, false)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			s.scimErrTyped(w, r, http.StatusNotFound, scimTypeNotFound, "no user with id "+strconv.Quote(id))
		} else {
			s.scimErrTyped(w, r, http.StatusInternalServerError, "", "loading the user failed")
		}
		return nil, false
	}
	return u, true
}

func (s *Server) handleSCIMUserGet(w http.ResponseWriter, r *http.Request) {
	u, ok := s.loadSCIMUser(w, r)
	if !ok {
		return
	}
	s.scimWrite(w, r, http.StatusOK, s.scimUserDoc(r, u))
}

// handleSCIMUserPut performs the DEFINITE replace (matrix: omitted ⇒ defaults
// — names "", email "", active true; userName REQUIRED; externalId immutable;
// ROLES PRESERVED — operator grants are not SCIM's to reset).
func (s *Server) handleSCIMUserPut(w http.ResponseWriter, r *http.Request) {
	current, ok := s.loadSCIMUser(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		s.putPatchMalformed(w, r, "unreadable request body")
		return
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		s.putPatchMalformed(w, r, "body must be a JSON object")
		return
	}
	payload, err := decodeSCIMUser(doc)
	if err != nil {
		s.putPatchMalformed(w, r, err.Error())
		return
	}
	// externalId is IMMUTABLE: a differing posted value is 400 (absent or
	// equal is a no-op).
	if ext, ok := doc["externalId"].(string); ok && ext != "" && ext != current.ExternalID {
		s.putPatchMalformed(w, r, "externalId is immutable")
		return
	}
	desiredActive := true
	if payload.Active != nil {
		desiredActive = *payload.Active
	}
	prevActive := current.Active
	refreshed, err := s.definiteProfile(r.Context(), s.scimUsers, current.ID, payload, current.Roles, desiredActive)
	if err != nil {
		s.finishSCIMOp(r, "update", "failure")
		s.renderSCIMMutationError(w, r, err)
		return
	}
	op := scimTransitionOp(prevActive, refreshed.Active)
	s.finishSCIMOp(r, op, "success")
	s.auditSCIMOp(r, op, refreshed.ID, refreshed.Username, current, refreshed)
	s.scimWrite(w, r, http.StatusOK, s.scimUserDoc(r, refreshed))
}

// handleSCIMUserPatch applies SCIM operations (add/replace/remove) on the
// supported attributes — the usual carrier for active toggles. Read-modify-
// write over the definite replace; last-writer-wins (ETags deferred).
func (s *Server) handleSCIMUserPatch(w http.ResponseWriter, r *http.Request) {
	current, ok := s.loadSCIMUser(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		s.putPatchMalformed(w, r, "unreadable request body")
		return
	}
	var envelope struct {
		Schemas    []string         `json:"schemas"`
		Operations []map[string]any `json:"operations"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Operations) == 0 {
		s.putPatchMalformed(w, r, "a non-empty operations array is required")
		return
	}
	// Accumulate the desired state OVER A COPY OF THE CURRENT PROFILE (sparse
	// ops compose onto the current values). Paths are SLASH-delimited in SCIM
	// ("/name/givenName"); normalize to dots for the attribute table.
	work := &scimUserPayload{
		UserName:     current.Username,
		GivenName:    current.FirstName,
		FamilyName:   current.LastName,
		PrimaryEmail: current.Email,
		ExternalID:   current.ExternalID,
		Active:       &current.Active,
	}
	for _, opRaw := range envelope.Operations {
		opKind, _ := opRaw["op"].(string)
		path, _ := opRaw["path"].(string)
		path = strings.TrimPrefix(strings.TrimSpace(path), "/")
		path = strings.ReplaceAll(path, "/", ".") // SCIM slashes ⇒ dotted attrs
		value, hasValue := opRaw["value"]
		switch strings.ToLower(opKind) {
		case "add", "replace":
			if !hasValue {
				s.putPatchMalformed(w, r, "op '"+opKind+"' requires a value")
				return
			}
			if err := applyPatchOp(work, path, value); err != nil {
				s.putPatchMalformed(w, r, err.Error())
				return
			}
		case "remove":
			if err := removePatchOp(work, path); err != nil {
				s.putPatchMalformed(w, r, err.Error())
				return
			}
		default:
			s.putPatchMalformed(w, r, "unsupported op: "+opKind)
			return
		}
	}
	desiredActive := true
	if work.Active != nil {
		desiredActive = *work.Active
	}
	prevActive := current.Active
	refreshed, err := s.definiteProfile(r.Context(), s.scimUsers, current.ID, work, current.Roles, desiredActive)
	if err != nil {
		s.finishSCIMOp(r, "update", "failure")
		s.renderSCIMMutationError(w, r, err)
		return
	}
	op := scimTransitionOp(prevActive, refreshed.Active)
	s.finishSCIMOp(r, op, "success")
	s.auditSCIMOp(r, op, refreshed.ID, refreshed.Username, current, refreshed)
	s.scimWrite(w, r, http.StatusOK, s.scimUserDoc(r, refreshed))
}

// putPatchMalformed renders a PUT/PATCH malformation as 400 (invalidValue for
// shape faults, unsupportedAttribute for unknown paths/immutability) and
// meters the failure.
func (s *Server) putPatchMalformed(w http.ResponseWriter, r *http.Request, msg string) {
	scimType := scimTypeInvalidValue
	if strings.Contains(msg, "unsupported") || strings.Contains(msg, "immu") {
		scimType = scimTypeUnsupportedAttribute
	}
	s.finishSCIMOp(r, "update", "failure")
	s.scimErrTyped(w, r, http.StatusBadRequest, scimType, msg)
}

// applyPatchOp folds one add/replace op into the accumulating payload.
func applyPatchOp(work *scimUserPayload, path string, value any) error {
	switch path {
	case "active":
		b, ok := value.(bool)
		if !ok {
			return errors.New("active requires a boolean value")
		}
		work.Active = &b
	case "userName":
		str, ok := value.(string)
		if !ok || strings.TrimSpace(str) == "" {
			return errors.New("userName requires a non-empty string value")
		}
		work.UserName = str
	case "name.givenName":
		str, ok := value.(string)
		if !ok {
			return errors.New("name.givenName requires a string value")
		}
		work.GivenName = str
	case "name.familyName":
		str, ok := value.(string)
		if !ok {
			return errors.New("name.familyName requires a string value")
		}
		work.FamilyName = str
	case "name":
		switch v := value.(type) {
		case string:
			work.GivenName, work.FamilyName = splitCompactName(v)
		case map[string]any:
			if gn, ok := v["givenName"].(string); ok {
				work.GivenName = gn
			}
			if fn, ok := v["familyName"].(string); ok {
				work.FamilyName = fn
			}
		default:
			return errors.New("name requires a string or object value")
		}
	case "emails", "emails.value":
		email, ok := coercePatchEmail(value)
		if !ok {
			return errors.New("emails requires a string value (or {value:string})")
		}
		work.PrimaryEmail = email
	case "externalId":
		str, _ := value.(string)
		if str != "" && str != work.ExternalID {
			return errors.New("externalId is immutable")
		}
	case "groups":
		// Acknowledged and discarded until F-28.5 (consistent with POST).
		return nil
	default:
		return errors.New("unsupported attribute: " + path)
	}
	return nil
}

// removePatchOp folds one remove op into the accumulating payload.
func removePatchOp(work *scimUserPayload, path string) error {
	switch path {
	case "name.givenName":
		work.GivenName = ""
	case "name.familyName":
		work.FamilyName = ""
	case "emails", "emails.value":
		work.PrimaryEmail = ""
	case "externalId":
		return errors.New("externalId is immutable")
	case "groups":
		return nil
	default:
		return errors.New("unsupported attribute: " + path)
	}
	return nil
}

// coercePatchEmail accepts a bare string or a {value: "..."} object.
func coercePatchEmail(value any) (string, bool) {
	if str, ok := value.(string); ok {
		return str, true
	}
	if obj, ok := value.(map[string]any); ok {
		if str, ok := obj["value"].(string); ok {
			return str, true
		}
	}
	return "", false
}

// handleSCIMUserDelete deprovisions via the F-28.3 tombstone semantics (the
// record is retained; credentials suspended; the id reserved for rehire).
func (s *Server) handleSCIMUserDelete(w http.ResponseWriter, r *http.Request) {
	current, ok := s.loadSCIMUser(w, r)
	if !ok {
		return
	}
	if err := s.scimUsers.SoftDelete(r.Context(), current.ID); err != nil {
		s.finishSCIMOp(r, "deprovision", "failure")
		s.scimErrTyped(w, r, http.StatusInternalServerError, "", "deprovisioning the user failed")
		return
	}
	s.finishSCIMOp(r, "deprovision", "success")
	s.auditSCIMOp(r, "deprovision", current.ID, current.Username, current, nil)
	w.WriteHeader(http.StatusNoContent)
}
