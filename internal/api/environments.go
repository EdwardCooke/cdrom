package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"cdrom/internal/audit"
	"cdrom/internal/auth"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// ---------------------------------------------------------------------------
// Environments (F-17)
// ---------------------------------------------------------------------------
//
// Environments are named deployment targets that group execution targets and
// carry the deployment policy that governs how work reaches them: an approval
// policy (requires_approval, an F-13 gate) and an automation policy
// (automatic / manual). They are stored in the database (the Environment
// table, owned by the Database service) and managed through these endpoints,
// so an admin can create `prod-auto`, flip `prod` to manual, or add a worker
// to an environment at runtime without a redeployment. A change publishes an
// environment event so the UI's environments view stays in sync across
// replicas.
//
// Proceeding a run (or job) held by a `manual` environment is a separate,
// more privileged operation (runs.can-proceed): it releases the hold and
// dispatches the run's held jobs.

// environmentRequest is the JSON form of an environment create/update. On
// create, name is required and the remaining fields are present; on update,
// the fields replace the existing values.
type environmentRequest struct {
	Name             string `json:"name,omitempty"`
	Description      string `json:"description,omitempty"`
	TargetGroup      string `json:"target_group,omitempty"`
	RequiresApproval bool   `json:"requires_approval,omitempty"`
	// Automation is the environment's automation policy: "automatic", "manual",
	// or "" (no policy — the job's own setting decides).
	Automation string `json:"automation,omitempty"`
}

// listEnvironments lists every environment (F-17), optionally filtered by name
// prefix (the ?name= query param). It requires environments.can-view.
func (s *Server) listEnvironments(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermEnvironmentsView, authz.Resource{}) {
		return
	}
	resp, err := s.clients.Database.ListEnvironments(r.Context(), &dbpb.ListEnvironmentsRequest{
		NamePrefix: r.URL.Query().Get("name"),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(resp.GetEnvironments()))
}

// createEnvironment adds an environment (F-17). It requires
// environments.can-manage. On success it records an audit event and publishes
// an environment event so the UI's environments view stays in sync across
// replicas.
func (s *Server) createEnvironment(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermEnvironmentsManage, authz.Resource{}) {
		return
	}
	var req environmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Name == "" {
		httpError(w, http.StatusBadRequest, "name is required")
		return
	}
	env, err := s.clients.Database.CreateEnvironment(r.Context(), &dbpb.CreateEnvironmentRequest{
		Name:             req.Name,
		Description:      req.Description,
		TargetGroup:      req.TargetGroup,
		RequiresApproval: req.RequiresApproval,
		Automation:       automationFromName(req.Automation),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionEnvironmentCreate,
		TargetKind: audit.TargetEnvironment,
		TargetID:   env.GetName(),
		TargetName: env.GetName(),
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditEnvironmentValue(env),
	})
	s.publishEnvironmentChange()
	writeJSON(w, http.StatusCreated, env)
}

// getEnvironment returns the named environment (F-17). It requires
// environments.can-view.
func (s *Server) getEnvironment(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermEnvironmentsView, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "environment name is required")
		return
	}
	env, err := s.clients.Database.GetEnvironment(r.Context(), &dbpb.GetEnvironmentRequest{Name: name})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, env)
}

// updateEnvironment edits an environment's description, target group, approval
// policy, and automation policy (F-17). It requires environments.can-manage.
// On success it records an audit event and publishes an environment event.
func (s *Server) updateEnvironment(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermEnvironmentsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "environment name is required")
		return
	}
	var req environmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	env, err := s.clients.Database.UpdateEnvironment(r.Context(), &dbpb.UpdateEnvironmentRequest{
		Name:             name,
		Description:      req.Description,
		TargetGroup:      req.TargetGroup,
		RequiresApproval: req.RequiresApproval,
		Automation:       automationFromName(req.Automation),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionEnvironmentUpdate,
		TargetKind: audit.TargetEnvironment,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditEnvironmentValue(env),
	})
	s.publishEnvironmentChange()
	writeJSON(w, http.StatusOK, env)
}

// deleteEnvironment removes an environment (F-17). It requires
// environments.can-manage. On success it records an audit event and publishes
// an environment event.
func (s *Server) deleteEnvironment(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermEnvironmentsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "environment name is required")
		return
	}
	if _, err := s.clients.Database.DeleteEnvironment(r.Context(), &dbpb.DeleteEnvironmentRequest{Name: name}); err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionEnvironmentDelete,
		TargetKind: audit.TargetEnvironment,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
	})
	s.publishEnvironmentChange()
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// proceedRun releases the jobs of a run that are held by a `manual`
// environment (F-17): it calls the scheduler's ProceedRun, which releases each
// held job (awaiting_proceed) and dispatches it. It requires runs.can-proceed.
// On success it records an audit event.
func (s *Server) proceedRun(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermRunsProceed, authz.Resource{}) {
		return
	}
	runID, ok := pathID(w, r)
	if !ok {
		return
	}
	actor := auth.UserFromContext(r.Context()).Subject
	resp, err := s.clients.Scheduler.ProceedRun(r.Context(), &schedpb.ProceedRunRequest{
		RunId: runID,
		Actor: actor,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionRunProceed,
		TargetKind: audit.TargetRun,
		TargetID:   strconv.FormatInt(runID, 10),
		RunID:      runID,
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"proceeded": resp.GetProceeded()}),
	})
	writeJSON(w, http.StatusOK, resp)
}

// proceedJob releases a single job held by a `manual` environment (F-17): it
// calls the scheduler's ProceedJob, which releases the hold and dispatches the
// job. It requires runs.can-proceed. On success it records an audit event.
func (s *Server) proceedJob(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermRunsProceed, authz.Resource{}) {
		return
	}
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	actor := auth.UserFromContext(r.Context()).Subject
	job, err := s.clients.Scheduler.ProceedJob(r.Context(), &schedpb.ProceedJobRequest{
		Id:    jobID,
		Actor: actor,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionRunProceed,
		TargetKind: audit.TargetJob,
		TargetID:   strconv.FormatInt(jobID, 10),
		TargetName: job.GetName(),
		PipelineID: job.GetPipelineId(),
		RunID:      job.GetRunId(),
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"status": jobStatusName(job.GetStatus())}),
	})
	writeJSON(w, http.StatusOK, job)
}

// publishEnvironmentChange reads the current set of environments (F-17) and
// publishes them as an environment event, so the UI's environments view stays
// in sync across replicas. It is best-effort: a failure to read the
// environments is silently dropped (the UI resynchronizes from the snapshot on
// reconnect).
func (s *Server) publishEnvironmentChange() {
	if s.hub == nil || s.clients.Database == nil {
		return
	}
	resp, err := s.clients.Database.ListEnvironments(context.Background(), &dbpb.ListEnvironmentsRequest{})
	if err != nil {
		return
	}
	s.hub.Publish(Event{Type: EventEnvironment, Environments: resp.GetEnvironments()})
}

// auditEnvironmentValue renders an environment as a value document for an
// audit event's new value: its name, target group, approval flag, and
// automation policy. It returns "" when the environment is nil.
func auditEnvironmentValue(env *dbpb.Environment) string {
	if env == nil {
		return ""
	}
	return auditJSON(map[string]any{
		"name":              env.GetName(),
		"target_group":      env.GetTargetGroup(),
		"requires_approval": env.GetRequiresApproval(),
		"automation":        automationName(env.GetAutomation()),
	})
}

// automationFromName maps a JSON automation name ("automatic", "manual", or
// "") onto the proto enum. An unrecognized or empty name maps to
// AUTOMATION_UNSPECIFIED (no policy).
func automationFromName(name string) dbpb.Automation {
	switch name {
	case "automatic":
		return dbpb.Automation_AUTOMATION_AUTOMATIC
	case "manual":
		return dbpb.Automation_AUTOMATION_MANUAL
	default:
		return dbpb.Automation_AUTOMATION_UNSPECIFIED
	}
}

// automationName maps a proto Automation enum onto its JSON name.
// AUTOMATION_UNSPECIFIED maps to "" (no policy).
func automationName(a dbpb.Automation) string {
	switch a {
	case dbpb.Automation_AUTOMATION_AUTOMATIC:
		return "automatic"
	case dbpb.Automation_AUTOMATION_MANUAL:
		return "manual"
	default:
		return ""
	}
}
