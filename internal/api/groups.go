// groups.go — the read-only /api/groups admin surface (F-28.5, Slice 4).
//
// GET /api/groups and GET /api/groups/{id} expose the ENRICHED group
// documents (stored attributes + resolved members + the group's projected
// derived role) to the UI. The surface is READ-ONLY BY DESIGN: group
// mutation is SCIM-owned (the corporate connector speaks to /scim/v2
// directly), and groups are expendable (no emergency-valve analogues).
//
// Authorization: users.can-manage (F-14) — adjacent to user-directory
// administration; no new permission is invented. When RBAC is not enforced
// (authentication disabled) the caller acts as a synthetic admin, so a local
// run stays open.
//
// Bridging: like /api/users, the API is a THIN coordinator — the real logic
// (enrichment: member resolution against the live user directory, the
// role-catalog projection) lives in the IdP, reached over gRPC (mTLS when
// configured). Component degradation: the IdP unwired or unreachable ⇒ 503
// (cf. /api/system/info's null-segment habit) — the endpoint reports the
// outage rather than guessing.
package api

import (
	"context"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/auth"
	"cdrom/internal/authz"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// groupDetailsService abstracts the IdP's group-detail RPCs (F-28.5 Slice
// 4). The generated idppb.IdPClient satisfies it; the variadic opts keep the
// signatures faithful to the gRPC surface.
type groupDetailsService interface {
	ListGroupDetails(ctx context.Context, in *idppb.ListGroupDetailsRequest, opts ...grpc.CallOption) (*idppb.ListGroupDetailsResponse, error)
	GetGroupDetails(ctx context.Context, in *idppb.GetGroupDetailsRequest, opts ...grpc.CallOption) (*idppb.GroupDetail, error)
}

// SetGroupDetailsService attaches the IdP client the read-only /api/groups
// surface bridges to. When left nil the endpoints respond 503 (component
// degradation — the IdP is the group directory's home).
func (s *Server) SetGroupDetailsService(d groupDetailsService) {
	s.groupSvc = d
}

// groupDetailDTO is the UI-facing shape of one enriched group (stable field
// names, no secrets). Rendered from the IdP's proto so the wire contract is
// insulated from proto reshuffles.
type groupDetailDTO struct {
	ID           string              `json:"id"`
	DisplayName  string              `json:"display_name"`
	Description  string              `json:"description"`
	ExternalID   string              `json:"external_id,omitempty"`
	MemberValues []string            `json:"member_values"`
	Members      []resolvedMemberDTO `json:"members"`
	DerivedRoles []string            `json:"derived_roles"`
	CreatedAt    string              `json:"created_at,omitempty"`
	UpdatedAt    string              `json:"updated_at,omitempty"`
}

// resolvedMemberDTO pairs a stored membership value with the live username it
// resolves to (null when unresolvable — the value is kept, per the opacity
// ruling).
type resolvedMemberDTO struct {
	Value    string  `json:"value"`
	Username *string `json:"username"`
}

// toGroupDetailDTO reshapes one enriched proto document into the UI DTO.
func toGroupDetailDTO(in *idppb.GroupDetail) groupDetailDTO {
	dto := groupDetailDTO{
		ID:           in.GetId(),
		DisplayName:  in.GetDisplayName(),
		Description:  in.GetDescription(),
		ExternalID:   in.GetExternalId(),
		MemberValues: nonNilStrings(in.GetMemberValues()),
		DerivedRoles: nonNilStrings(in.GetDerivedRoles()),
		CreatedAt:    in.GetCreatedAt(),
		UpdatedAt:    in.GetUpdatedAt(),
	}
	dto.Members = make([]resolvedMemberDTO, 0, len(in.GetMembers()))
	for _, m := range in.GetMembers() {
		dto.Members = append(dto.Members, resolvedMemberDTO{
			Value:    m.GetValue(),
			Username: m.Username, // nil propagates as JSON null
		})
	}
	return dto
}

// requireGroupAccess guards the /api/groups surface: users.can-manage when
// RBAC is enforced; a degenerate-mode admin-role requirement otherwise.
// Reports false (having written the response) when the caller is not
// allowed. Deliberately NOT coupled to the userpass switch — the surface
// tracks the IdP's availability, not username/password auth.
func (s *Server) requireGroupAccess(w http.ResponseWriter, r *http.Request) bool {
	if s.rbacEnabled && s.authz != nil {
		return s.requirePermission(w, r, authz.PermUsersManage, authz.Resource{})
	}
	user := auth.UserFromContext(r.Context())
	if user.Subject == "" {
		httpError(w, http.StatusUnauthorized, "authentication required")
		return false
	}
	if !user.HasRole(adminRole) {
		httpError(w, http.StatusForbidden, "admin role required")
		return false
	}
	return true
}

// listGroups serves GET /api/groups: the enriched group documents, bridged to
// the IdP. IdP unwired/unreachable ⇒ 503.
func (s *Server) listGroups(w http.ResponseWriter, r *http.Request) {
	if !s.requireGroupAccess(w, r) {
		return
	}
	if s.groupSvc == nil {
		httpError(w, http.StatusServiceUnavailable, "the identity provider is not reachable")
		return
	}
	resp, err := s.groupSvc.ListGroupDetails(r.Context(), &idppb.ListGroupDetailsRequest{})
	if err != nil {
		s.writeGroupBridgeError(w, err)
		return
	}
	items := make([]groupDetailDTO, 0, len(resp.GetGroups()))
	for _, gd := range resp.GetGroups() {
		items = append(items, toGroupDetailDTO(gd))
	}
	writeJSON(w, http.StatusOK, items)
}

// getGroup serves GET /api/groups/{id}: one enriched group document. A
// missing group is 404; IdP unwired/unreachable ⇒ 503.
func (s *Server) getGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requireGroupAccess(w, r) {
		return
	}
	if s.groupSvc == nil {
		httpError(w, http.StatusServiceUnavailable, "the identity provider is not reachable")
		return
	}
	id := r.PathValue("id")
	resp, err := s.groupSvc.GetGroupDetails(r.Context(), &idppb.GetGroupDetailsRequest{Id: id})
	if err != nil {
		s.writeGroupBridgeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toGroupDetailDTO(resp))
}

// writeGroupBridgeError maps a bridged IdP error to its HTTP rendition:
// NotFound ⇒ 404, InvalidArgument ⇒ 400, Unimplemented ⇒ 501, and any other
// (transport) failure ⇒ 503 (component degradation).
func (s *Server) writeGroupBridgeError(w http.ResponseWriter, err error) {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			httpError(w, http.StatusNotFound, "%s", st.Message())
			return
		case codes.InvalidArgument:
			httpError(w, http.StatusBadRequest, "%s", st.Message())
			return
		case codes.Unimplemented:
			httpError(w, http.StatusNotImplemented, "%s", st.Message())
			return
		}
	}
	httpError(w, http.StatusServiceUnavailable, "the identity provider is not reachable")
}

// nonNilStrings coerces a proto string slice to a non-nil slice (so the JSON
// renders [] rather than null when empty).
func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}
