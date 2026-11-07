package api

import (
	"encoding/json"
	"net/http"

	"cdrom/internal/audit"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// ---------------------------------------------------------------------------
// Agent groups (F-29)
// ---------------------------------------------------------------------------
//
// A agent group is a named group of long-lived agents (a job's target_group
// names it; a agent's group names it). It is a first-class, labelable row: its
// labels are combined onto the runs of the jobs that target it. The row is
// metadata-only — it does not change dispatch (which still keys off the
// agent's declared group string) — so a group that has no row simply
// contributes no labels and jobs targeting it still run on its agents.
//
// Agent groups are stored in the database (the AgentGroup table, owned by
// the Database service) and managed through these endpoints, so the UI can
// label a group at runtime without a redeployment. Viewing requires
// agent-groups.can-view; creating/updating/deleting requires
// agent-groups.can-manage (F-14).

// agentGroupRequest is the JSON form of a agent-group create/update. On
// create, name is required; on update, the fields replace the existing values.
type agentGroupRequest struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	// Labels are the group's labels (F-29): a set of key/value pairs that label
	// the group. When present on an update they replace the group's existing
	// labels; when absent the group's labels are left unchanged.
	Labels map[string]string `json:"labels,omitempty"`
}

// listAgentGroups lists every agent group (F-29), optionally filtered by name
// prefix (the ?name= query param). It requires agent-groups.can-view.
func (s *Server) listAgentGroups(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermAgentGroupsView, authz.Resource{}) {
		return
	}
	resp, err := s.clients.Database.ListAgentGroups(r.Context(), &dbpb.ListAgentGroupsRequest{
		NamePrefix: r.URL.Query().Get("name"),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(resp.GetGroups()))
}

// createAgentGroup adds a agent group (F-29). It requires
// agent-groups.can-manage. It is idempotent on name: a create for an existing
// group is a no-op that returns the existing row. On success it records an
// audit event.
func (s *Server) createAgentGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermAgentGroupsManage, authz.Resource{}) {
		return
	}
	var req agentGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Name == "" {
		httpError(w, http.StatusBadRequest, "name is required")
		return
	}
	group, err := s.clients.Database.CreateAgentGroup(r.Context(), &dbpb.CreateAgentGroupRequest{
		Name:        req.Name,
		Description: req.Description,
		Labels:        req.Labels,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAgentGroupCreate,
		TargetKind: audit.TargetAgentGroup,
		TargetID:   group.GetName(),
		TargetName: group.GetName(),
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditAgentGroupValue(group),
	})
	writeJSON(w, http.StatusCreated, group)
}

// getAgentGroup returns the named agent group (F-29). It requires
// agent-groups.can-view.
func (s *Server) getAgentGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermAgentGroupsView, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "agent group name is required")
		return
	}
	group, err := s.clients.Database.GetAgentGroup(r.Context(), &dbpb.GetAgentGroupRequest{Name: name})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, group)
}

// updateAgentGroup edits a agent group's description and/or labels (F-29). It
// requires agent-groups.can-manage. On success it records an audit event.
func (s *Server) updateAgentGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermAgentGroupsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "agent group name is required")
		return
	}
	var req agentGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	group, err := s.clients.Database.UpdateAgentGroup(r.Context(), &dbpb.UpdateAgentGroupRequest{
		Name:        name,
		Description: req.Description,
		Labels:        req.Labels,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAgentGroupUpdate,
		TargetKind: audit.TargetAgentGroup,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditAgentGroupValue(group),
	})
	writeJSON(w, http.StatusOK, group)
}

// deleteAgentGroup removes a agent group (F-29). It requires
// agent-groups.can-manage. Deleting a group does not affect the agents that
// belong to it or the jobs that target it; it only removes the group's labels.
// On success it records an audit event.
func (s *Server) deleteAgentGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermAgentGroupsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "agent group name is required")
		return
	}
	if _, err := s.clients.Database.DeleteAgentGroup(r.Context(), &dbpb.DeleteAgentGroupRequest{Name: name}); err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAgentGroupDelete,
		TargetKind: audit.TargetAgentGroup,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// auditAgentGroupValue renders a agent group as a compact JSON document for
// an audit event's old/new value (F-15).
func auditAgentGroupValue(group *dbpb.AgentGroup) string {
	return auditJSON(map[string]any{
		"name":        group.GetName(),
		"description": group.GetDescription(),
		"labels":        group.GetLabels(),
	})
}
