package api

import (
	"encoding/json"
	"net/http"

	"cdrom/internal/audit"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// ---------------------------------------------------------------------------
// Worker groups (F-29)
// ---------------------------------------------------------------------------
//
// A worker group is a named group of long-lived workers (a job's target_group
// names it; a worker's group names it). It is a first-class, labelable row: its
// labels are combined onto the runs of the jobs that target it. The row is
// metadata-only — it does not change dispatch (which still keys off the
// worker's declared group string) — so a group that has no row simply
// contributes no labels and jobs targeting it still run on its workers.
//
// Worker groups are stored in the database (the WorkerGroup table, owned by
// the Database service) and managed through these endpoints, so the UI can
// label a group at runtime without a redeployment. Viewing requires
// worker-groups.can-view; creating/updating/deleting requires
// worker-groups.can-manage (F-14).

// workerGroupRequest is the JSON form of a worker-group create/update. On
// create, name is required; on update, the fields replace the existing values.
type workerGroupRequest struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	// Labels are the group's labels (F-29): a set of key/value pairs that label
	// the group. When present on an update they replace the group's existing
	// labels; when absent the group's labels are left unchanged.
	Labels map[string]string `json:"labels,omitempty"`
}

// listWorkerGroups lists every worker group (F-29), optionally filtered by name
// prefix (the ?name= query param). It requires worker-groups.can-view.
func (s *Server) listWorkerGroups(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermWorkerGroupsView, authz.Resource{}) {
		return
	}
	resp, err := s.clients.Database.ListWorkerGroups(r.Context(), &dbpb.ListWorkerGroupsRequest{
		NamePrefix: r.URL.Query().Get("name"),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(resp.GetGroups()))
}

// createWorkerGroup adds a worker group (F-29). It requires
// worker-groups.can-manage. It is idempotent on name: a create for an existing
// group is a no-op that returns the existing row. On success it records an
// audit event.
func (s *Server) createWorkerGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermWorkerGroupsManage, authz.Resource{}) {
		return
	}
	var req workerGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Name == "" {
		httpError(w, http.StatusBadRequest, "name is required")
		return
	}
	group, err := s.clients.Database.CreateWorkerGroup(r.Context(), &dbpb.CreateWorkerGroupRequest{
		Name:        req.Name,
		Description: req.Description,
		Labels:        req.Labels,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionWorkerGroupCreate,
		TargetKind: audit.TargetWorkerGroup,
		TargetID:   group.GetName(),
		TargetName: group.GetName(),
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditWorkerGroupValue(group),
	})
	writeJSON(w, http.StatusCreated, group)
}

// getWorkerGroup returns the named worker group (F-29). It requires
// worker-groups.can-view.
func (s *Server) getWorkerGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermWorkerGroupsView, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "worker group name is required")
		return
	}
	group, err := s.clients.Database.GetWorkerGroup(r.Context(), &dbpb.GetWorkerGroupRequest{Name: name})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, group)
}

// updateWorkerGroup edits a worker group's description and/or labels (F-29). It
// requires worker-groups.can-manage. On success it records an audit event.
func (s *Server) updateWorkerGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermWorkerGroupsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "worker group name is required")
		return
	}
	var req workerGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	group, err := s.clients.Database.UpdateWorkerGroup(r.Context(), &dbpb.UpdateWorkerGroupRequest{
		Name:        name,
		Description: req.Description,
		Labels:        req.Labels,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionWorkerGroupUpdate,
		TargetKind: audit.TargetWorkerGroup,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditWorkerGroupValue(group),
	})
	writeJSON(w, http.StatusOK, group)
}

// deleteWorkerGroup removes a worker group (F-29). It requires
// worker-groups.can-manage. Deleting a group does not affect the workers that
// belong to it or the jobs that target it; it only removes the group's labels.
// On success it records an audit event.
func (s *Server) deleteWorkerGroup(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermWorkerGroupsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "worker group name is required")
		return
	}
	if _, err := s.clients.Database.DeleteWorkerGroup(r.Context(), &dbpb.DeleteWorkerGroupRequest{Name: name}); err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionWorkerGroupDelete,
		TargetKind: audit.TargetWorkerGroup,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// auditWorkerGroupValue renders a worker group as a compact JSON document for
// an audit event's old/new value (F-15).
func auditWorkerGroupValue(group *dbpb.WorkerGroup) string {
	return auditJSON(map[string]any{
		"name":        group.GetName(),
		"description": group.GetDescription(),
		"labels":        group.GetLabels(),
	})
}
