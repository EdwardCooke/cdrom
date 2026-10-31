// SCIM 2.0 (RFC 7643/7644) service-provider surface (F-28.1).
//
// Mounted on the IdP's HTTP server alongside the OIDC surface under the
// FIXED base path (config.SCIMBasePath, /scim/v2). This file ships the
// frame: the route guard (Accept negotiation + the reserved F-28.2 auth
// seam), the SCIM error encoder, and the three metadata endpoints
// (ServiceProviderConfig, Schemas, ResourceTypes). The resource endpoints
// (/Users, /Groups) arrive with F-28.4/F-28.5. WHERE THE SURFACE IS REACHED
// (routing) is thus fixed; where the OUTSIDE WORLD BELIEVES it lives
// (announcements: meta.location, changeInfo.location, and eventually
// Location headers) is governed by idp.scim.public_base — pinned when set,
// derived per request (Host + X-Forwarded-Proto) when empty.
package idp

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cdrom/internal/config"
)

const (
	// scimMediaType is the SCIM wire format (RFC 7644 §3.2).
	scimMediaType = "application/scim+json"

	// Core SCIM schema URNs (RFC 7643 §3).
	scimSchemaUser   = "urn:ietf:params:scim:schemas:core:2.0:User"
	scimSchemaGroup  = "urn:ietf:params:scim:schemas:core:2.0:Group"
	scimSchemaError  = "urn:ietf:params:scim:schemas:core:2.0:Error"
	scimSchemaSvcCfg = "urn:ietf:params:scim:schemas:core:2.0:ServiceConfiguration"
	// scimSchemaListResponse is the ListResponse envelope URN (RFC 7644 §3).
	scimSchemaListResponse = "urn:ietf:params:scim:api:messages:2.0:ListResponse"

	// Resource type names (RFC 7643 §3.3).
	scimRTUser  = "User"
	scimRTGroup = "Group"
)

// mountSCIM wires the SCIM 2.0 surface (F-28.1) under the fixed base path.
// Native mux patterns own method gating and parameter extraction; the guard
// prepends the cross-cutting obligations (today: Accept negotiation; F-28.2:
// the caller-auth check) to every route. The unrestricted subtree pattern
// is the SCIM-dialect catch-all: because it widens the allowed-method set at
// every covered path, the mux's own 405 synthesis never fires and wrong-verb
// requests land in handleSCIMSubtree, which answers 405/404 in the SCIM
// dialect. The bare base path itself (/scim/v2, no trailing slash) matches no
// pattern and gets the mux's stock 404 — intentional: SCIM clients address
// the subtree, and keeping a single registration per subtree avoids the
// exact-plus-subtree pattern pairing observed to shadow subtree routing.
func (s *Server) mountSCIM(mux *http.ServeMux) {
	base := config.SCIMBasePath
	mux.HandleFunc("GET "+base+"/ServiceProviderConfig", s.scimGuard(s.handleSCIMServiceProviderConfig))
	mux.HandleFunc("GET "+base+"/Schemas", s.scimGuard(s.handleSCIMSchemas))
	mux.HandleFunc("GET "+base+"/Schemas/{urn}", s.scimGuard(s.handleSCIMSchema))
	mux.HandleFunc("GET "+base+"/ResourceTypes", s.scimGuard(s.handleSCIMResourceTypes))
	mux.HandleFunc("GET "+base+"/ResourceTypes/{rtype}", s.scimGuard(s.handleSCIMResourceType))
	mux.Handle(base+"/", s.scimGuard(http.HandlerFunc(s.handleSCIMSubtree)))
}

// scimGuard wraps a SCIM route handler with the obligations every SCIM
// request shares: Accept-header negotiation (406 when the client refuses
// the SCIM media type).
//
// TODO(F-28.2): enforce the caller credential HERE — one choke point for
// the whole surface (parse Authorization: Bearer, resolve the key against
// the database-held SCIM API pool, 401 SCIM-error on miss).
func (s *Server) scimGuard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !acceptsSCIM(r.Header.Get("Accept")) {
			s.scimErr(w, r, http.StatusNotAcceptable, "unsupported Accept; this surface speaks "+scimMediaType)
			return
		}
		next(w, r)
	}
}

// handleSCIMSubtree answers requests the concrete patterns did not claim:
// unknown resources (SCIM-dialect 404) and wrong verbs on known resources
// (405 + Allow, emitted by us — see mountSCIM for why the mux's own
// synthesis is bypassed). Over-nesting under a known resource is a 404.
func (s *Server) handleSCIMSubtree(w http.ResponseWriter, r *http.Request) {
	rel := strings.TrimPrefix(r.URL.Path, config.SCIMBasePath)
	elems := strings.FieldsFunc(rel, func(rc rune) bool { return rc == '/' })
	if len(elems) == 0 {
		s.scimErr(w, r, http.StatusNotFound, "no resource at the SCIM base path; see "+s.scimLocation(r, "/ServiceProviderConfig"))
		return
	}
	switch elems[0] {
	case "ServiceProviderConfig":
		if len(elems) == 1 {
			s.scimWrongVerb(w, r)
			return
		}
	case "Schemas", "ResourceTypes":
		// Depth 1 (the collection) and depth 2 (item) are the GET shapes;
		// anything arriving here had the wrong verb.
		if len(elems) <= 2 {
			s.scimWrongVerb(w, r)
			return
		}
	}
	// Unknown resource (today: /Users and /Groups land with F-28.4/F-28.5)
	// or over-nested junk.
	s.scimErr(w, r, http.StatusNotFound, "unknown SCIM resource "+r.URL.Path)
}

// scimWrongVerb answers a known-resource request with the wrong method.
func (s *Server) scimWrongVerb(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", http.MethodGet)
	s.scimErr(w, r, http.StatusMethodNotAllowed, "method "+r.Method+" not allowed on this resource")
}

// acceptsSCIM reports whether the request's Accept header tolerates the SCIM
// media type. An absent Accept header (or a wildcard) is tolerated.
func acceptsSCIM(header string) bool {
	header = strings.TrimSpace(header)
	if header == "" {
		return true
	}
	for _, part := range strings.Split(header, ",") {
		media := strings.ToLower(strings.TrimSpace(strings.SplitN(part, ";", 2)[0]))
		switch media {
		case "*", "*/*", "application/*", "application/json", scimMediaType:
			return true
		}
	}
	return false
}

// scimPublicBase returns the public base under which the surface announces
// itself: the PINNED idp.scim.public_base (validated absolute URL) when set,
// otherwise DERIVED PER REQUEST from the Host header (behind transparent
// proxies) with X-Forwarded-Proto honored for the scheme.
func (s *Server) scimPublicBase(r *http.Request) string {
	if pb := s.cfg.Scim.NormalizePublicBase(); pb != "" {
		return pb
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	return scheme + "://" + r.Host
}

// scimLocation derives the absolute PUBLIC URL of a SCIM resource for
// meta.location / changeInfo.location (and, from F-28.4 onward, Location
// headers on creations).
func (s *Server) scimLocation(r *http.Request, rel string) string {
	return s.scimPublicBase(r) + config.SCIMBasePath + rel
}

// scimWrite emits a SCIM response in the SCIM media type.
func (s *Server) scimWrite(w http.ResponseWriter, r *http.Request, code int, value any) {
	w.Header().Set("Content-Type", scimMediaType)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

// scimErr emits an RFC 7644 §3.7-shaped SCIM error body.
func (s *Server) scimErr(w http.ResponseWriter, r *http.Request, code int, detail string) {
	s.logger.Debug("scim: rejecting request", "method", r.Method, "path", r.URL.Path, "status", code, "remote", r.RemoteAddr)
	s.scimWrite(w, r, code, map[string]any{
		"schemas": []string{scimSchemaError},
		"status":  strconv.Itoa(code),
		"detail":  detail,
	})
}

// scimMeta builds a resource-meta block (RFC 7643 §3.1).
func scimMeta(location, resourceType string) map[string]any {
	modified := time.Now().UTC().Format(time.RFC3339)
	return map[string]any{
		"location":     location,
		"resourceType": resourceType,
		"created":      modified,
		"modified":     modified,
	}
}

// scimAttr builds a compact schema-attribute descriptor (RFC 7643 §3.1).
func scimAttr(name, typ string, multivalue, required bool, description string) map[string]any {
	attr := map[string]any{
		"name":        name,
		"type":        typ,
		"multiValued": multivalue,
		"description": description,
	}
	if required {
		attr["required"] = true
	}
	return attr
}

// scimSchemaDescriptor builds a schema descriptor (RFC 7643 §3).
func scimSchemaDescriptor(name, urn string, common bool, required []string, attrs ...map[string]any) map[string]any {
	desc := map[string]any{
		"name":       name,
		"id":         urn,
		"schema":     urn,
		"common":     common,
		"attributes": attrs,
	}
	if len(required) > 0 {
		desc["required"] = required
	}
	return desc
}

// scimCoreSchemas are the schema descriptors advertised by /Schemas.
var scimCoreSchemas = []map[string]any{
	scimSchemaDescriptor("User", scimSchemaUser, true, []string{"userName"},
		scimAttr("userName", "string", false, true, "Unique identifier for the User (the login email)."),
		scimAttr("name", "complex", false, false, "Components of the User's name (givenName, familyName)."),
		scimAttr("emails", "complex", true, false, "Electronic mail addresses (type=primary corroborates userName)."),
		scimAttr("active", "boolean", false, false, "Whether the User is active (deactivation suspends authentication)."),
		scimAttr("externalId", "string", false, false, "Corporate-wide unique identifier; the match key for re-provisioning."),
		scimAttr("groups", "complex", true, false, "Groups the User is a member of ($ref to Group)."),
		scimAttr("meta", "complex", false, false, "Resource metadata."),
	),
	scimSchemaDescriptor("Group", scimSchemaGroup, true, []string{"displayName"},
		scimAttr("displayName", "string", false, true, "Localized name of the Group."),
		scimAttr("description", "string", false, false, "Human-readable description for the Group."),
		scimAttr("members", "complex", true, false, "Members of the Group ($ref to User)."),
	),
	scimSchemaDescriptor("Error", scimSchemaError, false, nil,
		scimAttr("schemata", "reference", true, false, "Identifiers of the schemas describing the error."),
		scimAttr("detail", "string", false, false, "Human-readable error description."),
		scimAttr("status", "string", false, false, "HTTP status code of the response."),
	),
}

// scimCoreResourceTypes are the resource-type descriptors advertised by
// /ResourceTypes (RFC 7643 §3.3). Their endpoints are relative to the SCIM
// base path.
var scimCoreResourceTypes = []map[string]any{
	{
		"name":        scimRTUser,
		"plural":      "Users",
		"endpoint":    "/Users",
		"schema":      scimSchemaUser,
		"description": "A User resource.",
	},
	{
		"name":        scimRTGroup,
		"plural":      "Groups",
		"endpoint":    "/Groups",
		"schema":      scimSchemaGroup,
		"description": "A Group resource.",
	},
}

// scimClamp confines [from,to) to [0,total], shrinking toward the lower end
// when the window overshoots.
func scimClamp(from, to, total int) (int, int) {
	if to > total {
		to = total
	}
	if from > total {
		from = total
	}
	if from < 0 {
		from = 0
	}
	if to < from {
		to = from
	}
	return from, to
}

// scimPageStart parses the startIndex query parameter (default 1).
func scimPageStart(r *http.Request) (int, bool) {
	v := r.URL.Query().Get("startIndex")
	if v == "" {
		return 1, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}

// scimListDoc wraps resources in the RFC 7644 ListResponse envelope, clamped
// to the configured page bound (EffectiveMaxPageSize; RFC 7644 mandates no
// universal ceiling, so the bound is ours to set). startIndex is 1-based
// (SCIM): the first item is startIndex=1.
func (s *Server) scimListDoc(resources []map[string]any, startIndex int) map[string]any {
	bound := s.cfg.Scim.EffectiveMaxPageSize()
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

// handleSCIMServiceProviderConfig serves GET /ServiceProviderConfig (RFC
// 7643 §6). Capability ads are conservative: only patch + filter are claimed
// at launch; bulk/etag advertise false until the nice-to-haves land. The
// maxResults figures reflect the CONFIGURED page bound (idp.scim.max_page_size):
// RFC 7644 sets no universal ceiling, so the provider declares its own.
func (s *Server) handleSCIMServiceProviderConfig(w http.ResponseWriter, r *http.Request) {
	loc := s.scimLocation(r, "/ServiceProviderConfig")
	bound := s.cfg.Scim.EffectiveMaxPageSize()
	doc := map[string]any{
		"schemas":    []string{scimSchemaSvcCfg},
		"bulk":       map[string]any{"supported": false, "maxOperations": 0, "maxPayloadSize": 0},
		"filter":     map[string]any{"supported": true, "maxResults": bound},
		"etag":       map[string]any{"supported": false},
		"pagination": map[string]any{"supported": true, "maxResults": bound},
		"sortBy":     map[string]any{"supported": false},
		"changeInfo": map[string]any{
			"location":     loc,
			"verbosity":    "none",
			"lastModified": time.Now().UTC().Format(time.RFC3339),
		},
		"meta": scimMeta(loc, "ServiceConfiguration"),
	}
	s.scimWrite(w, r, http.StatusOK, doc)
}

// handleSCIMSchemas serves GET /Schemas (the ListResponse of schema
// descriptors).
func (s *Server) handleSCIMSchemas(w http.ResponseWriter, r *http.Request) {
	startIdx, ok := scimPageStart(r)
	if !ok {
		s.scimErr(w, r, http.StatusBadRequest, "invalid value for parameter startIndex")
		return
	}
	s.scimWrite(w, r, http.StatusOK, s.scimListDoc(scimCoreSchemas, startIdx))
}

// handleSCIMSchema serves GET /Schemas/{schemaUri}: the mux delivers the
// (percent-decoded) URN fragment as the {urn} wildcard value.
func (s *Server) handleSCIMSchema(w http.ResponseWriter, r *http.Request) {
	urn := r.PathValue("urn")
	for _, desc := range scimCoreSchemas {
		if desc["id"].(string) == urn {
			s.scimWrite(w, r, http.StatusOK, desc)
			return
		}
	}
	s.scimErr(w, r, http.StatusNotFound, "unknown schema "+urn)
}

// handleSCIMResourceTypes serves GET /ResourceTypes (the ListResponse of
// resource-type descriptors).
func (s *Server) handleSCIMResourceTypes(w http.ResponseWriter, r *http.Request) {
	startIdx, ok := scimPageStart(r)
	if !ok {
		s.scimErr(w, r, http.StatusBadRequest, "invalid value for parameter startIndex")
		return
	}
	types := make([]map[string]any, len(scimCoreResourceTypes))
	copy(types, scimCoreResourceTypes)
	for i := range types {
		types[i]["schemas"] = []string{scimSchemaListResponse}
	}
	s.scimWrite(w, r, http.StatusOK, s.scimListDoc(types, startIdx))
}

// handleSCIMResourceType serves GET /ResourceTypes/{resourceType}.
func (s *Server) handleSCIMResourceType(w http.ResponseWriter, r *http.Request) {
	rt := r.PathValue("rtype")
	for _, ty := range scimCoreResourceTypes {
		if ty["name"].(string) == rt {
			doc := make(map[string]any, len(ty)+1)
			for k, v := range ty {
				doc[k] = v
			}
			doc["schemas"] = []string{scimSchemaListResponse}
			s.scimWrite(w, r, http.StatusOK, doc)
			return
		}
	}
	s.scimErr(w, r, http.StatusNotFound, "unknown resource type "+rt)
}
