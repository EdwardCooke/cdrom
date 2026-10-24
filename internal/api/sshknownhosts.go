package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"cdrom/internal/audit"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// ---------------------------------------------------------------------------
// SSH known hosts (F-21.2.4.1)
// ---------------------------------------------------------------------------
//
// Runtime-managed SSH host keys the source-fetcher uses to verify a git-ssh
// server's host key. They are stored in the database (owned by the Database
// service) and managed through these endpoints, so an admin can add a key for
// a self-hosted or corporate-firewalled git host at runtime (via the UI)
// without a redeployment. Listing requires ssh-known-hosts.can-view; adding
// and removing require ssh-known-hosts.can-manage. The API accumulates the
// stored keys and passes them to the fetcher on each git-ssh fetch
// (FetchSourceRequest.known_hosts); the fetcher combines them with its
// built-in common provider keys.

// sshKnownHostRequest is the JSON form of an SSH host key create.
type sshKnownHostRequest struct {
	HostPattern string `json:"host_pattern"`
	KeyType     string `json:"key_type"`
	Key         string `json:"key"`
	Description string `json:"description,omitempty"`
}

// listSSHKnownHosts lists every stored SSH host key (F-21.2.4.1). It requires
// ssh-known-hosts.can-view.
func (s *Server) listSSHKnownHosts(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermSSHKnownHostsView, authz.Resource{}) {
		return
	}
	resp, err := s.clients.Database.ListSSHKnownHosts(r.Context(), &dbpb.ListSSHKnownHostsRequest{})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(resp.GetHosts()))
}

// createSSHKnownHost adds an SSH host key (F-21.2.4.1). It requires
// ssh-known-hosts.can-manage. On success it records an audit event.
func (s *Server) createSSHKnownHost(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermSSHKnownHostsManage, authz.Resource{}) {
		return
	}
	var req sshKnownHostRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	host, err := s.clients.Database.CreateSSHKnownHost(r.Context(), &dbpb.CreateSSHKnownHostRequest{
		HostPattern: req.HostPattern,
		KeyType:     req.KeyType,
		Key:         req.Key,
		Description: req.Description,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionSSHKnownHostCreate,
		TargetKind: audit.TargetSSHKnownHost,
		TargetID:   strconv.FormatInt(host.GetId(), 10),
		TargetName: host.GetHostPattern(),
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusCreated, host)
}

// deleteSSHKnownHost removes an SSH host key (F-21.2.4.1). It requires
// ssh-known-hosts.can-manage. On success it records an audit event.
func (s *Server) deleteSSHKnownHost(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermSSHKnownHostsManage, authz.Resource{}) {
		return
	}
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if _, err := s.clients.Database.DeleteSSHKnownHost(r.Context(), &dbpb.DeleteSSHKnownHostRequest{Id: id}); err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionSSHKnownHostDelete,
		TargetKind: audit.TargetSSHKnownHost,
		TargetID:   strconv.FormatInt(id, 10),
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}
