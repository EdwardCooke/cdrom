package api

import (
	"context"
	"encoding/json"
	"net/http"

	"cdrom/internal/audit"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// ---------------------------------------------------------------------------
// Approval groups (F-27)
// ---------------------------------------------------------------------------
//
// An approval group is a named set of USERS that a pipeline's approval step
// can reference via an `approvers` entry of `group:<name>`. It is a set of
// PEOPLE, distinct from an F-14 Role (a set of PERMISSIONS); the two bridge
// via `role:<name>` approver references and via a group's optional claim
// derivation. The group rows are owned by the Database service (shared across
// replicas); the API resolves a gate's authorized set when a decision arrives
// and is the only place that authorizes.
//
// Viewing requires approvals.can-view; creating/updating/deleting a group and
// adding/removing members requires approvals.can-manage (F-14). Every mutation
// is audited (F-15) and publishes an approval_groups event so the UI's
// approval-groups view stays in sync.

// approvalGroupRequest is the JSON form of an approval-group create/update. On
// create, name is required. On update, the fields replace the existing values
// (members replace the list only when present; the claim derivation is
// replaced only when either claim field is present).
type approvalGroupRequest struct {
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Members     []string `json:"members,omitempty"`
	// MembersReplace signals (on update) that Members should REPLACE the
	// group's member list (an empty list with MembersReplace true empties the
	// group's manual members). Absent/false leaves the list unchanged.
	MembersReplace bool `json:"members_replace,omitempty"`
	ClaimName      string `json:"claim_name,omitempty"`
	ClaimValue     string `json:"claim_value,omitempty"`
}

// approvalGroupMemberRequest is the JSON body of the incremental member-add
// endpoint (POST /api/approval-groups/{name}/members).
type approvalGroupMemberRequest struct {
	Member string `json:"member"`
}

// listApprovalGroups lists every approval group (F-27). It requires
// approvals.can-view.
func (s *Server) listApprovalGroups(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermApprovalsView, authz.Resource{}) {
		return
	}
	resp, err := s.clients.Database.ListApprovalGroups(r.Context(), &dbpb.ListApprovalGroupsRequest{})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(resp.GetGroups()))
}

// createApprovalGroup adds an approval group (F-27). It requires
// approvals.can-manage. On success it records an audit event and publishes an
// approval_groups event.
func (s *Server) createApprovalGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermApprovalsManage, authz.Resource{}) {
		return
	}
	var req approvalGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Name == "" {
		httpError(w, http.StatusBadRequest, "name is required")
		return
	}
	group, err := s.clients.Database.CreateApprovalGroup(r.Context(), &dbpb.CreateApprovalGroupRequest{
		Name:        req.Name,
		Description: req.Description,
		Members:     req.Members,
		ClaimName:   req.ClaimName,
		ClaimValue:  req.ClaimValue,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionApprovalGroupCreate,
		TargetKind: audit.TargetApprovalGroup,
		TargetID:   group.GetName(),
		TargetName: group.GetName(),
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditApprovalGroupValue(group),
	})
	s.publishApprovalGroupsChanged(r.Context())
	writeJSON(w, http.StatusCreated, group)
}

// getApprovalGroup returns the named approval group (F-27). It requires
// approvals.can-view.
func (s *Server) getApprovalGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermApprovalsView, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "group name is required")
		return
	}
	group, err := s.clients.Database.GetApprovalGroup(r.Context(), &dbpb.GetApprovalGroupRequest{Name: name})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, group)
}

// updateApprovalGroup edits an approval group's description, claim derivation,
// and/or member list (F-27). It requires approvals.can-manage. On success it
// records an audit event and publishes an approval_groups event.
func (s *Server) updateApprovalGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermApprovalsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "group name is required")
		return
	}
	var req approvalGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	group, err := s.clients.Database.UpdateApprovalGroup(r.Context(), &dbpb.UpdateApprovalGroupRequest{
		Name:           name,
		Description:    req.Description,
		ClaimName:      req.ClaimName,
		ClaimValue:     req.ClaimValue,
		Members:        req.Members,
		MembersPresent: req.MembersReplace,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionApprovalGroupUpdate,
		TargetKind: audit.TargetApprovalGroup,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditApprovalGroupValue(group),
	})
	s.publishApprovalGroupsChanged(r.Context())
	writeJSON(w, http.StatusOK, group)
}

// deleteApprovalGroup removes an approval group (F-27). It requires
// approvals.can-manage. Deleting a group does NOT retroactively affect gates
// already awaiting approval (the gate stores the reference; a dangling
// reference simply contributes no members at decision time). On success it
// records an audit event and publishes an approval_groups event.
func (s *Server) deleteApprovalGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermApprovalsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "group name is required")
		return
	}
	if _, err := s.clients.Database.DeleteApprovalGroup(r.Context(), &dbpb.DeleteApprovalGroupRequest{Name: name}); err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionApprovalGroupDelete,
		TargetKind: audit.TargetApprovalGroup,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
	})
	s.publishApprovalGroupsChanged(r.Context())
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// addApprovalGroupMember atomically adds a single member to a group's member
// list (F-27). It requires approvals.can-manage. Idempotent: adding a member
// already present is a no-op. On success it records an audit event and
// publishes an approval_groups event.
func (s *Server) addApprovalGroupMember(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermApprovalsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "group name is required")
		return
	}
	var req approvalGroupMemberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Member == "" {
		httpError(w, http.StatusBadRequest, "member is required")
		return
	}
	group, err := s.clients.Database.AddApprovalGroupMember(r.Context(), &dbpb.AddApprovalGroupMemberRequest{
		Name:   name,
		Member: req.Member,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionApprovalGroupAddMember,
		TargetKind: audit.TargetApprovalGroup,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"member": req.Member}),
	})
	s.publishApprovalGroupsChanged(r.Context())
	writeJSON(w, http.StatusOK, group)
}

// removeApprovalGroupMember atomically removes a single member from a group's
// member list (F-27). It requires approvals.can-manage. Idempotent: removing
// an absent member is a no-op. On success it records an audit event and
// publishes an approval_groups event.
func (s *Server) removeApprovalGroupMember(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermApprovalsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "group name is required")
		return
	}
	member := r.PathValue("member")
	if member == "" {
		httpError(w, http.StatusBadRequest, "member is required")
		return
	}
	group, err := s.clients.Database.RemoveApprovalGroupMember(r.Context(), &dbpb.RemoveApprovalGroupMemberRequest{
		Name:   name,
		Member: member,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionApprovalGroupRemoveMember,
		TargetKind: audit.TargetApprovalGroup,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"member": member}),
	})
	s.publishApprovalGroupsChanged(r.Context())
	writeJSON(w, http.StatusOK, group)
}

// publishApprovalGroupsChanged publishes an approval_groups event carrying the
// current set of approval groups, so the UI's approval-groups view stays in
// sync (locally, and across replicas via the snapshot on reconnect). Best-
// effort: a failure to list (e.g. the Database client is unavailable) suppresses
// the event rather than failing the mutation.
func (s *Server) publishApprovalGroupsChanged(ctx context.Context) {
	if s.hub == nil || s.clients.Database == nil {
		return
	}
	resp, err := s.clients.Database.ListApprovalGroups(ctx, &dbpb.ListApprovalGroupsRequest{})
	if err != nil {
		return
	}
	s.publish(Event{Type: EventApprovalGroups, ApprovalGroups: nonNil(resp.GetGroups())})
}

// auditApprovalGroupValue renders an approval group as a compact JSON document
// for an audit event's new value (F-15).
func auditApprovalGroupValue(group *dbpb.ApprovalGroup) string {
	return auditJSON(map[string]any{
		"name":         group.GetName(),
		"description":  group.GetDescription(),
		"members":      group.GetMembers(),
		"claim_name":   group.GetClaimName(),
		"claim_value":  group.GetClaimValue(),
	})
}
