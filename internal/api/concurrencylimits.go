package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"cdrom/internal/audit"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// ---------------------------------------------------------------------------
// Concurrency limits (F-16)
// ---------------------------------------------------------------------------
//
// The concurrency limits that bound how much runs in parallel (the system-wide
// cap, per-worker-group caps, and per-concurrency-key caps) are stored as
// regex-based rules in the database (the ConcurrencyLimit table), not in
// configuration, so they can be edited at runtime via these endpoints without a
// redeployment. A rule has a kind (global / group / key), a regex pattern
// matched against the dimension's name, and a limit (0 = unlimited). A
// concrete resource's effective limit is the limit of the most specific
// (longest) matching rule of its kind; a resource matching no rule falls back
// to a default (a concurrency key defaults to a limit of 1 — a lock; a group
// or the global resource defaults to unlimited).

// concurrencyLimitRequest is the JSON form of a concurrency limit rule
// create/update. On create, kind, pattern, and limit are all present; on
// update, pattern (when non-empty) and limit (when present) replace the
// existing values.
type concurrencyLimitRequest struct {
	Kind    string `json:"kind,omitempty"`
	Pattern string `json:"pattern,omitempty"`
	// Limit is a pointer so an update can distinguish "absent" (leave the
	// existing limit unchanged) from "present" (replace it, including with 0
	// for unlimited).
	Limit *int32 `json:"limit,omitempty"`
}

// listConcurrencyLimits lists every concurrency limit rule (F-16), optionally
// filtered by kind (the ?kind= query param: "global", "group", or "key"). It
// requires concurrency-limits.can-view.
func (s *Server) listConcurrencyLimits(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermConcurrencyLimitsView, authz.Resource{}) {
		return
	}
	resp, err := s.clients.Database.ListConcurrencyLimits(r.Context(), &dbpb.ListConcurrencyLimitsRequest{
		Kind: r.URL.Query().Get("kind"),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(resp.GetLimits()))
}

// createConcurrencyLimit adds a concurrency limit rule (F-16). It requires
// concurrency-limits.can-manage. On success it records an audit event and
// publishes a concurrency_limits event so the UI's limits view stays in sync
// across replicas.
func (s *Server) createConcurrencyLimit(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermConcurrencyLimitsManage, authz.Resource{}) {
		return
	}
	var req concurrencyLimitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Kind == "" {
		httpError(w, http.StatusBadRequest, "kind is required")
		return
	}
	limit := int32(0)
	if req.Limit != nil {
		limit = *req.Limit
	}
	rule, err := s.clients.Database.CreateConcurrencyLimit(r.Context(), &dbpb.CreateConcurrencyLimitRequest{
		Kind:    req.Kind,
		Pattern: req.Pattern,
		Limit:   limit,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionConcurrencyLimitCreate,
		TargetKind: audit.TargetConcurrencyLimit,
		TargetID:   strconv.FormatInt(rule.GetId(), 10),
		TargetName: rule.GetKind() + ":" + rule.GetPattern(),
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditConcurrencyLimitValue(rule),
	})
	s.publishConcurrencyLimitsChange()
	writeJSON(w, http.StatusCreated, rule)
}

// updateConcurrencyLimit edits a concurrency limit rule's pattern and/or limit
// (F-16). It requires concurrency-limits.can-manage. On success it records an
// audit event and publishes a concurrency_limits event.
func (s *Server) updateConcurrencyLimit(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermConcurrencyLimitsManage, authz.Resource{}) {
		return
	}
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req concurrencyLimitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	rule, err := s.clients.Database.UpdateConcurrencyLimit(r.Context(), &dbpb.UpdateConcurrencyLimitRequest{
		Id:      id,
		Pattern: req.Pattern,
		Limit:   req.Limit,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionConcurrencyLimitUpdate,
		TargetKind: audit.TargetConcurrencyLimit,
		TargetID:   strconv.FormatInt(rule.GetId(), 10),
		TargetName: rule.GetKind() + ":" + rule.GetPattern(),
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditConcurrencyLimitValue(rule),
	})
	s.publishConcurrencyLimitsChange()
	writeJSON(w, http.StatusOK, rule)
}

// deleteConcurrencyLimit removes a concurrency limit rule (F-16). It requires
// concurrency-limits.can-manage. On success it records an audit event and
// publishes a concurrency_limits event.
func (s *Server) deleteConcurrencyLimit(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermConcurrencyLimitsManage, authz.Resource{}) {
		return
	}
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if _, err := s.clients.Database.DeleteConcurrencyLimit(r.Context(), &dbpb.DeleteConcurrencyLimitRequest{Id: id}); err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionConcurrencyLimitDelete,
		TargetKind: audit.TargetConcurrencyLimit,
		TargetID:   strconv.FormatInt(id, 10),
		Outcome:    audit.OutcomeSuccess,
	})
	s.publishConcurrencyLimitsChange()
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// publishConcurrencyLimitsChange reads the current set of concurrency limit
// rules (F-16) and publishes them as a concurrency_limits event, so the UI's
// limits view stays in sync across replicas. It is best-effort: a failure to
// read the rules is silently dropped (the UI resynchronizes from the snapshot
// on reconnect).
func (s *Server) publishConcurrencyLimitsChange() {
	if s.hub == nil || s.clients.Database == nil {
		return
	}
	resp, err := s.clients.Database.ListConcurrencyLimits(context.Background(), &dbpb.ListConcurrencyLimitsRequest{})
	if err != nil {
		return
	}
	s.hub.Publish(Event{Type: EventConcurrencyLimits, ConcurrencyLimits: resp.GetLimits()})
}

// auditConcurrencyLimitValue renders a concurrency limit rule as a value
// document for an audit event's new value: its kind, pattern, and limit. It
// returns "" when the rule is nil.
func auditConcurrencyLimitValue(rule *dbpb.ConcurrencyLimit) string {
	if rule == nil {
		return ""
	}
	return auditJSON(map[string]any{
		"kind":    rule.GetKind(),
		"pattern": rule.GetPattern(),
		"limit":   rule.GetLimit(),
	})
}
