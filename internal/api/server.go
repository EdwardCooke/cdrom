// Package api implements the API/controller layer: an HTTP surface consumed
// by the UI that routes requests to the gRPC services. No business logic
// belongs here — it is a thin bridge between the UI and the service layer.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/audit"
	"cdrom/internal/auth"
	"cdrom/internal/authz"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
	"cdrom/internal/secrets"
)

// Clients are the gRPC clients the API bridges to.
type Clients struct {
	Database  dbpb.DatabaseClient
	Scheduler schedpb.SchedulerClient
	Artifacts artifactspb.ArtifactsClient
}

// Server is the HTTP API server.
type Server struct {
	clients Clients
	hub     *EventHub
	// webhookOIDC verifies the Bearer token a webhook caller presents against
	// a trigger's oidc_issuer and returns its flattened claims. It is the
	// production verifier by default; tests substitute a fake so the
	// claim-matching path can be exercised without a live identity provider.
	webhookOIDC webhookTokenVerifier
	// secretStore encrypts a pipeline's secret plaintexts on create/update and
	// decrypts them at dispatch (F-12). It is nil only in tests that do not
	// configure a store; in production the built-in AES store is always
	// available (falling back to an all-zero key when none is configured).
	secretStore secrets.Store
	// userpass proxies the UI's username/password requests (login, register,
	// user/role management) to the IdP, where the real logic lives. It is nil
	// when username/password authentication is disabled, in which case the
	// /api/login, /api/register, and /api/users endpoints respond 501.
	userpass UserPassClient
	// apikey proxies the UI's API-key management requests (F-25) to the IdP,
	// where the real logic (key generation, hashing, verification, and
	// lockout) lives. It is nil when API-key authentication is disabled, in
	// which case the /api/api-keys endpoints respond 501.
	apikey APIKeyClient
	// serviceAccounts proxies the UI's service-account management requests
	// (F-26) to the IdP, where the real logic (key generation, salted hashing,
	// verification, and lockout) lives. It is nil when service-account
	// authentication is disabled, in which case the /api/service-accounts
	// endpoints respond 501.
	serviceAccounts ServiceAccountClient
	// authz is the authorization engine (F-14, RBAC). It enforces role-based
	// access on the UI-facing HTTP surface: a request is allowed only if the
	// authenticated principal's roles grant the required permission (with a
	// scope that covers the target resource). It is nil when RBAC is not
	// enforced (authentication disabled), in which case requests act as a
	// synthetic admin.
	authz *authz.Engine
	// rbacEnabled reports whether role-based access control is enforced on the
	// UI-facing HTTP surface (authentication enabled). When false, requests
	// act as a synthetic admin (everything is allowed), so a local run with
	// authentication disabled keeps working.
	rbacEnabled bool
	// audit records an audit event (F-15) for each audited action the server
	// handles (a pipeline/run/job/secret/role/user mutation, an approval
	// decision, a login). It is nil when the audit log is not configured (or
	// in tests), in which case no audit events are recorded on the HTTP
	// surface.
	audit *audit.Recorder
}

// SetUserPassClient attaches the IdP username/password proxy client, enabling
// the /api/login, /api/register, and /api/users endpoints. When left nil the
// endpoints are disabled (they respond 501).
func (s *Server) SetUserPassClient(c UserPassClient) {
	s.userpass = c
}

// SetAPIKeyClient attaches the IdP API-key proxy client (F-25), enabling the
// /api/api-keys endpoints. When left nil the endpoints are disabled (they
// respond 501).
func (s *Server) SetAPIKeyClient(c APIKeyClient) {
	s.apikey = c
}

// SetServiceAccountClient attaches the IdP service-account proxy client (F-26),
// enabling the /api/service-accounts endpoints. When left nil the endpoints
// are disabled (they respond 501).
func (s *Server) SetServiceAccountClient(c ServiceAccountClient) {
	s.serviceAccounts = c
}

// SetAuthz attaches the authorization engine (F-14, RBAC) and enables
// role-based access control on the UI-facing HTTP surface. When enabled, every
// request is checked against the authenticated principal's roles before the
// handler acts; a request the principal's roles do not permit is rejected with
// 403. When left unset (authentication disabled), requests act as a synthetic
// admin and RBAC is not enforced.
func (s *Server) SetAuthz(e *authz.Engine, enabled bool) {
	s.authz = e
	s.rbacEnabled = enabled
}

// New creates an API server over the given gRPC clients. hub is the event
// hub that backs the /api/ws WebSocket endpoint; it may be nil to disable
// the WebSocket endpoint. secretStore (optional) is the store that
// encrypts/decrypts pipeline secrets (F-12); when omitted (or nil) the API
// cannot encrypt or decrypt secrets and rejects any pipeline that carries
// them (in production the built-in AES store is always supplied).
func New(clients Clients, hub *EventHub, secretStore ...secrets.Store) *Server {
	var store secrets.Store
	if len(secretStore) > 0 {
		store = secretStore[0]
	}
	return &Server{clients: clients, hub: hub, webhookOIDC: newWebhookOIDCVerifier(), secretStore: store}
}

// Handler builds the HTTP handler for the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Pipelines (via the database service).
	mux.HandleFunc("POST /api/pipelines", s.createPipeline)
	mux.HandleFunc("GET /api/pipelines", s.listPipelines)
	mux.HandleFunc("PUT /api/pipelines/{id}", s.updatePipeline)

	// Pipeline runs (F-07): triggering a pipeline creates a run and its job
	// instances (via the scheduler); runs are listed and fetched via the
	// database service.
	mux.HandleFunc("POST /api/pipelines/{id}/runs", s.createRun)
	mux.HandleFunc("GET /api/pipelines/{id}/runs", s.listRuns)
	mux.HandleFunc("GET /api/runs/{id}", s.getRun)

	// Pipeline versions (F-11): each change to a pipeline produces a new
	// version; a run is bound to the version that was active when it started.
	// Versions are listed and fetched via the database service.
	mux.HandleFunc("GET /api/pipelines/{id}/versions", s.listPipelineVersions)
	mux.HandleFunc("GET /api/pipelines/{id}/versions/{version}", s.getPipelineVersion)

	// Webhook trigger (F-09): an external POST (authenticated with the
	// pipeline's webhook trigger secret) starts a run, passing the request's
	// JSON body as run parameters.
	mux.HandleFunc("POST /api/pipelines/{id}/webhook", s.webhook)

	// Jobs (via the scheduler service).
	mux.HandleFunc("POST /api/jobs", s.submitJob)
	mux.HandleFunc("GET /api/jobs", s.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /api/jobs/{id}/cancel", s.cancelJob)
	mux.HandleFunc("POST /api/jobs/{id}/rerun", s.rerunJob)
	// Approval gates (F-13): an authorized user approves or rejects a job that
	// is awaiting approval. The actor (the authenticated user) and an optional
	// reason are recorded on the job.
	mux.HandleFunc("POST /api/jobs/{id}/approve", s.approveJob)
	mux.HandleFunc("POST /api/jobs/{id}/reject", s.rejectJob)

	// Workers (via the scheduler service).
	mux.HandleFunc("GET /api/workers", s.listWorkers)

	// Artifacts (via the artifacts service).
	mux.HandleFunc("GET /api/jobs/{id}/artifacts", s.listJobArtifacts)

	// Job logs (via the artifacts service). A job's logs are stored as one
	// file per step (step-<n>.log) plus a combined job.log.
	mux.HandleFunc("GET /api/jobs/{id}/logs", s.listJobLogs)
	mux.HandleFunc("GET /api/jobs/{id}/logs/{name}", s.getJobLog)

	// Secrets (F-12): an authenticated user encrypts a value through the API's
	// secret store. The returned ciphertext is what a pipeline's secret
	// declaration stores; the plaintext is never persisted or returned.
	mux.HandleFunc("POST /api/secrets/encrypt", s.encryptSecret)

	// Audit log (F-15): an authenticated caller with the audit.can-view
	// permission queries the shared audit log (who did what, when, with what
	// outcome and what changed). Events are filtered by actor, action, target,
	// pipeline, and time range.
	mux.HandleFunc("GET /api/audit", s.listAudit)

	// Roles (F-14, RBAC): custom role CRUD (built-in roles cannot be edited or
	// deleted), role bindings (principal + role + optional pipeline scope), and
	// the caller's effective permissions (the UI uses it to disable actions the
	// caller lacks).
	mux.HandleFunc("GET /api/roles", s.listRoles)
	mux.HandleFunc("POST /api/roles", s.createRole)
	mux.HandleFunc("GET /api/roles/{name}", s.getRole)
	mux.HandleFunc("PUT /api/roles/{name}", s.updateRole)
	mux.HandleFunc("DELETE /api/roles/{name}", s.deleteRole)
	mux.HandleFunc("GET /api/role-bindings", s.listBindings)
	mux.HandleFunc("POST /api/role-bindings", s.addBinding)
	mux.HandleFunc("DELETE /api/role-bindings/{id}", s.deleteBinding)
	mux.HandleFunc("GET /api/me/permissions", s.myPermissions)

	// Username/password authentication (proxied to the IdP). These are the
	// unauthenticated entry points: login and register require no token, and
	// user/role management requires an authenticated (admin) caller. They are
	// disabled (501) when username/password auth is not enabled.
	mux.HandleFunc("POST /api/login", s.login)
	mux.HandleFunc("POST /api/register", s.register)
	mux.HandleFunc("GET /api/users", s.listUsers)
	mux.HandleFunc("POST /api/users", s.createUser)
	mux.HandleFunc("PUT /api/users/{id}", s.updateUser)
	mux.HandleFunc("DELETE /api/users/{id}", s.deleteUser)

	// API keys (F-25, proxied to the IdP). A user with the api-keys.can-manage
	// permission can create a key for themselves or (an admin) for another
	// user, and list their own keys. Listing every key in the system is a
	// separate, more privileged operation (GET /api/api-keys/all) that requires
	// the api-keys.can-list-all permission (granted to admins). The create and
	// rotate responses return the plaintext key exactly once. They are disabled
	// (501) when API-key auth is not enabled.
	mux.HandleFunc("POST /api/api-keys", s.createAPIKey)
	mux.HandleFunc("GET /api/api-keys", s.listAPIKeys)
	mux.HandleFunc("GET /api/api-keys/all", s.listAllAPIKeys)
	mux.HandleFunc("GET /api/api-keys/{id}", s.getAPIKey)
	mux.HandleFunc("PUT /api/api-keys/{id}", s.updateAPIKey)
	mux.HandleFunc("POST /api/api-keys/{id}/rotate", s.rotateAPIKey)
	mux.HandleFunc("DELETE /api/api-keys/{id}", s.deleteAPIKey)
	mux.HandleFunc("POST /api/api-keys/lockout/reset", s.resetAPIKeyLockout)

	// Service accounts (F-26, proxied to the IdP). A principal with the
	// service-accounts.can-* permissions manages service accounts (non-human
	// identities with two API-key slots). The create and rotate responses
	// return the plaintext key(s) exactly once. They are disabled (501) when
	// service-account auth is not enabled.
	mux.HandleFunc("POST /api/service-accounts", s.createServiceAccount)
	mux.HandleFunc("GET /api/service-accounts", s.listServiceAccounts)
	mux.HandleFunc("GET /api/service-accounts/{id}", s.getServiceAccount)
	mux.HandleFunc("PATCH /api/service-accounts/{id}", s.updateServiceAccount)
	mux.HandleFunc("POST /api/service-accounts/{id}/keys/{slot}/rotate", s.rotateServiceAccountKey)
	mux.HandleFunc("POST /api/service-accounts/{id}/disable", s.disableServiceAccount)
	mux.HandleFunc("POST /api/service-accounts/{id}/enable", s.enableServiceAccount)
	mux.HandleFunc("POST /api/service-accounts/{id}/roles", s.assignServiceAccountRoles)
	mux.HandleFunc("DELETE /api/service-accounts/{id}/roles/{role}", s.removeServiceAccountRole)
	mux.HandleFunc("DELETE /api/service-accounts/{id}", s.deleteServiceAccount)
	mux.HandleFunc("POST /api/service-accounts/{id}/lockout/reset", s.resetServiceAccountLockout)

	// Live event stream (WebSocket).
	if s.hub != nil {
		mux.HandleFunc("GET /api/ws", s.handleWebSocket)
	}

	return mux
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

type pipelineRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	// Jobs are the pipeline's job definitions (F-08): each carries a key (the
	// job's stable name), needs (dependencies, expressed as the keys of other
	// jobs in the pipeline), a target group, and an execution spec. When
	// present the pipeline's DAG is validated (unique keys, known needs, no
	// cycle) before the pipeline is saved.
	Jobs []pipelineJobRequest `json:"jobs,omitempty"`
	// Triggers are the ways a run of the pipeline can be started other than a
	// manual click (F-09): a cron schedule, a webhook, or an event (another
	// pipeline's run reaching a state). When present they are validated
	// (unique names, kind-specific fields present) before the pipeline is
	// saved.
	Triggers []triggerRequest `json:"triggers,omitempty"`
	// Params are the pipeline's parameter declarations (F-10): the named,
	// typed inputs a run can supply. When present they are validated (unique,
	// non-empty names) before the pipeline is saved.
	Params []parameterRequest `json:"params,omitempty"`
	// Secrets are the pipeline's named secrets (F-12): each carries a name and
	// a plaintext value. The API encrypts each value (with the configured key)
	// before persisting it; the plaintext is never stored or returned. When
	// present they are validated (unique, non-empty names) before the pipeline
	// is saved.
	Secrets []secretRequest `json:"secrets,omitempty"`
}

// parameterRequest is the JSON form of a single pipeline parameter (F-10): a
// name, an optional default value, and an optional description. A run
// supplies concrete values for the pipeline's parameters; a parameter not
// supplied falls back to its default.
type parameterRequest struct {
	Name        string `json:"name"`
	Default     string `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// secretRequest is the JSON form of a single pipeline secret (F-12): a name
// and a plaintext value. The API encrypts the value before persisting it; the
// plaintext is never stored or returned to the UI.
type secretRequest struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// triggerRequest is the JSON form of a single pipeline trigger (F-09). Type
// selects the trigger kind ("cron", "webhook", or "event") and the remaining
// fields carry the kind-specific settings: Cron for a cron trigger, Secret and
// OIDCIssuer/OIDCClaims for a webhook trigger, and EventPipeline/EventStatus
// for an event trigger.
type triggerRequest struct {
	Name          string            `json:"name"`
	Type          string            `json:"type"`
	Cron          string            `json:"cron,omitempty"`
	Secret        string            `json:"secret,omitempty"`
	EventPipeline string            `json:"event_pipeline,omitempty"`
	EventStatus   string            `json:"event_status,omitempty"`
	Params        map[string]string `json:"params,omitempty"`
	// OIDCIssuer is the OIDC issuer of the token a webhook caller must
	// present (a webhook trigger); when set the API verifies the caller's
	// Bearer token against it and matches its claims against OIDCClaims.
	OIDCIssuer string `json:"oidc_issuer,omitempty"`
	// OIDCClaims are the claims a webhook caller's OIDC token must carry for
	// the trigger to match (a webhook trigger); a value of "*" is a wildcard.
	OIDCClaims map[string]string `json:"oidc_claims,omitempty"`
}

// pipelineJobRequest is the JSON form of a single job definition in a
// pipeline (F-08). Key is the job's stable, pipeline-scoped identifier; Needs
// lists the keys of the jobs this job depends on; Spec is the execution spec
// to snapshot onto each of the job's run instances.
type pipelineJobRequest struct {
	Key         string          `json:"key"`
	Name        string          `json:"name,omitempty"`
	TargetGroup string          `json:"target_group,omitempty"`
	Needs       []string        `json:"needs,omitempty"`
	Spec        *jobSpecRequest `json:"spec,omitempty"`
}

func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermPipelinesCreate, authz.Resource{}) {
		return
	}
	var req pipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	jobs, err := pipelineJobsToProto(req.Jobs)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid jobs: %v", err)
		return
	}
	secrets, err := s.pipelineSecretsToProto(r.Context(), req.Secrets)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid secrets: %v", err)
		return
	}
	pipeline, err := s.clients.Database.CreatePipeline(r.Context(), &dbpb.CreatePipelineRequest{
		Name:        req.Name,
		Description: req.Description,
		Jobs:        jobs,
		Triggers:    triggersToProto(req.Triggers),
		Params:      pipelineParamsToProto(req.Params),
		Secrets:     secrets,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionPipelineCreate,
		TargetKind: audit.TargetPipeline,
		TargetID:   strconv.FormatInt(pipeline.GetId(), 10),
		TargetName: pipeline.GetName(),
		PipelineID: pipeline.GetId(),
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditPipelineValue(pipeline),
	})
	writeJSON(w, http.StatusCreated, pipeline)
}

func (s *Server) updatePipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesEdit, authz.Resource{PipelineID: pipelineID}) {
		return
	}
	var req pipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	jobs, err := pipelineJobsToProto(req.Jobs)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid jobs: %v", err)
		return
	}
	secrets, err := s.pipelineSecretsToProto(r.Context(), req.Secrets)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid secrets: %v", err)
		return
	}
	// Fetch the pipeline's current state so the audit event can record what
	// changed (old value vs new value, F-15). A failure to fetch it (the
	// pipeline was deleted concurrently) does not block the update.
	oldPipeline, _ := s.clients.Database.GetPipeline(r.Context(), &dbpb.GetPipelineRequest{Id: pipelineID})
	pipeline, err := s.clients.Database.UpdatePipeline(r.Context(), &dbpb.UpdatePipelineRequest{
		Id:          pipelineID,
		Name:        req.Name,
		Description: req.Description,
		Jobs:        jobs,
		Triggers:    triggersToProto(req.Triggers),
		Params:      pipelineParamsToProto(req.Params),
		Secrets:     secrets,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionPipelineUpdate,
		TargetKind: audit.TargetPipeline,
		TargetID:   strconv.FormatInt(pipeline.GetId(), 10),
		TargetName: pipeline.GetName(),
		PipelineID: pipeline.GetId(),
		Outcome:    audit.OutcomeSuccess,
		OldValue:   auditPipelineValue(oldPipeline),
		NewValue:   auditPipelineValue(pipeline),
	})
	writeJSON(w, http.StatusOK, pipeline)
}

// triggersToProto converts the JSON trigger definitions into the proto Trigger
// list carried to the database service (F-09). It returns nil when there are
// no triggers, so an update with no triggers leaves the pipeline's triggers
// unchanged.
func triggersToProto(triggers []triggerRequest) []*dbpb.Trigger {
	if len(triggers) == 0 {
		return nil
	}
	out := make([]*dbpb.Trigger, 0, len(triggers))
	for _, trigger := range triggers {
		out = append(out, &dbpb.Trigger{
			Name:          trigger.Name,
			Type:          triggerTypeFromName(trigger.Type),
			Cron:          trigger.Cron,
			Secret:        trigger.Secret,
			EventPipeline: trigger.EventPipeline,
			EventStatus:   runStatusFromName(trigger.EventStatus),
			Params:        trigger.Params,
			OidcIssuer:    trigger.OIDCIssuer,
			OidcClaims:    trigger.OIDCClaims,
		})
	}
	return out
}

// pipelineParamsToProto converts the JSON parameter declarations into the
// proto Parameter list carried to the database service (F-10). It returns nil
// when there are no params, so an update with no params leaves the pipeline's
// parameters unchanged.
func pipelineParamsToProto(params []parameterRequest) []*dbpb.Parameter {
	if len(params) == 0 {
		return nil
	}
	out := make([]*dbpb.Parameter, 0, len(params))
	for _, param := range params {
		out = append(out, &dbpb.Parameter{
			Name:        param.Name,
			Default:     param.Default,
			Description: param.Description,
		})
	}
	return out
}

// pipelineSecretsToProto converts the JSON secret declarations into the proto
// Secret list carried to the database service (F-12). Each plaintext value is
// encrypted with the API's secret store before it is handed off, so the
// database service only ever sees ciphertext. It returns nil when there are no
// secrets, so an update with no secrets leaves the pipeline's secrets
// unchanged. It is an error to supply secrets when no store is available (the
// API cannot encrypt them).
func (s *Server) pipelineSecretsToProto(ctx context.Context, reqs []secretRequest) ([]*dbpb.Secret, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if s.secretStore == nil {
		return nil, fmt.Errorf("secrets are not supported: no secret store is available")
	}
	seen := make(map[string]struct{}, len(reqs))
	out := make([]*dbpb.Secret, 0, len(reqs))
	for _, req := range reqs {
		if req.Name == "" {
			return nil, fmt.Errorf("secret has an empty name")
		}
		if _, dup := seen[req.Name]; dup {
			return nil, fmt.Errorf("duplicate secret name %q", req.Name)
		}
		seen[req.Name] = struct{}{}
		encrypted, err := s.secretStore.Encrypt(ctx, req.Value)
		if err != nil {
			return nil, fmt.Errorf("encrypt secret %q: %w", req.Name, err)
		}
		out = append(out, &dbpb.Secret{Name: req.Name, Encrypted: encrypted})
	}
	return out, nil
}

// triggerTypeFromName maps a trigger's JSON type to the proto TriggerType
// (F-09).
func triggerTypeFromName(name string) dbpb.TriggerType {
	switch name {
	case "cron":
		return dbpb.TriggerType_TRIGGER_TYPE_CRON
	case "webhook":
		return dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK
	case "event":
		return dbpb.TriggerType_TRIGGER_TYPE_EVENT
	default:
		return dbpb.TriggerType_TRIGGER_TYPE_UNSPECIFIED
	}
}

// runStatusFromName maps a run status name to the proto RunStatus (F-09).
func runStatusFromName(name string) dbpb.RunStatus {
	switch name {
	case "pending":
		return dbpb.RunStatus_RUN_STATUS_PENDING
	case "running":
		return dbpb.RunStatus_RUN_STATUS_RUNNING
	case "succeeded":
		return dbpb.RunStatus_RUN_STATUS_SUCCEEDED
	case "failed":
		return dbpb.RunStatus_RUN_STATUS_FAILED
	case "cancelled":
		return dbpb.RunStatus_RUN_STATUS_CANCELLED
	default:
		return dbpb.RunStatus_RUN_STATUS_UNSPECIFIED
	}
}

// pipelineJobsToProto converts the JSON job definitions into the proto
// JobDefinition list carried to the database service (F-08). It returns nil
// when there are no jobs, so an update with no jobs leaves the pipeline's
// definitions unchanged.
func pipelineJobsToProto(jobs []pipelineJobRequest) ([]*dbpb.JobDefinition, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	defs := make([]*dbpb.JobDefinition, 0, len(jobs))
	for _, job := range jobs {
		spec, err := job.Spec.toProtoSpec()
		if err != nil {
			return nil, fmt.Errorf("job %q: %w", job.Key, err)
		}
		defs = append(defs, &dbpb.JobDefinition{
			Key:         job.Key,
			Name:        job.Name,
			TargetGroup: job.TargetGroup,
			Needs:       job.Needs,
			Spec:        spec,
		})
	}
	return defs, nil
}

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{}) {
		return
	}
	response, err := s.clients.Database.ListPipelines(r.Context(), &dbpb.ListPipelinesRequest{})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetPipelines()))
}

// ---------------------------------------------------------------------------
// Pipeline runs (F-07)
// ---------------------------------------------------------------------------

// runRequest is the body of POST /api/pipelines/{id}/runs. Trigger is how the
// run was started (e.g. "manual"); Params are the run's parameters (F-10);
// PipelineVersion, when set (> 0), is the specific version of the pipeline to
// execute (F-11) — a re-run of an old run against its original version. When
// 0 the run executes the pipeline's current version.
type runRequest struct {
	Trigger         string            `json:"trigger,omitempty"`
	Params          map[string]string `json:"params,omitempty"`
	PipelineVersion int32             `json:"pipeline_version,omitempty"`
}

// createRun triggers a new execution of a pipeline (F-07): it asks the
// scheduler to create a PipelineRun and one job instance per job definition in
// the pipeline, then drive them. The run's overall status is derived from its
// job instances by the scheduler's run-status loop. When the request names a
// specific version (F-11) the run executes that version's definition and
// records it; otherwise it executes the pipeline's current version.
func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermRunsTrigger, authz.Resource{PipelineID: pipelineID}) {
		return
	}
	var req runRequest
	// An empty body is allowed (a run with the default trigger and no params);
	// json.Decoder reports io.EOF for one, which we treat as "no body".
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	run, err := s.clients.Scheduler.CreateRun(r.Context(), &schedpb.CreateRunRequest{
		PipelineId:      pipelineID,
		Trigger:         req.Trigger,
		Params:          req.Params,
		PipelineVersion: req.PipelineVersion,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	trigger := req.Trigger
	if trigger == "" {
		trigger = "manual"
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionRunTrigger,
		TargetKind: audit.TargetRun,
		TargetID:   strconv.FormatInt(run.GetId(), 10),
		PipelineID: pipelineID,
		RunID:      run.GetId(),
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"trigger": trigger, "params": req.Params, "pipeline_version": req.PipelineVersion}),
	})
	s.publish(Event{Type: EventRunStatus, RunID: run.GetId(), Status: runStatusName(run.GetStatus())})
	writeJSON(w, http.StatusCreated, run)
}

// listRuns lists a pipeline's runs (F-07), most recent first.
func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{PipelineID: pipelineID}) {
		return
	}
	response, err := s.clients.Database.ListRuns(r.Context(), &dbpb.ListRunsRequest{PipelineId: pipelineID})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetRuns()))
}

// getRun fetches a single run (F-07).
func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	runID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{}) {
		return
	}
	run, err := s.clients.Database.GetRun(r.Context(), &dbpb.GetRunRequest{Id: runID})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

// listPipelineVersions lists a pipeline's version history (F-11), most recent
// first. Each version is an immutable snapshot of the pipeline's definition at
// that version.
func (s *Server) listPipelineVersions(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{PipelineID: pipelineID}) {
		return
	}
	response, err := s.clients.Database.ListPipelineVersions(r.Context(), &dbpb.ListPipelineVersionsRequest{PipelineId: pipelineID})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetVersions()))
}

// getPipelineVersion fetches the immutable snapshot of one version of a
// pipeline's definition (F-11).
func (s *Server) getPipelineVersion(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{PipelineID: pipelineID}) {
		return
	}
	version, err := strconv.ParseInt(r.PathValue("version"), 10, 32)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid version: %v", err)
		return
	}
	versionProto, err := s.clients.Database.GetPipelineVersion(r.Context(), &dbpb.GetPipelineVersionRequest{
		PipelineId: pipelineID,
		Version:    int32(version),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, versionProto)
}

// webhookSecretHeader is the header a webhook caller presents the pipeline's
// webhook trigger secret in (F-09).
const webhookSecretHeader = "X-Cdrom-Webhook-Secret"

// webhook starts a run of a pipeline from a webhook trigger (F-09): an
// external POST (e.g. from a git host). The request's JSON body (a flat object
// of string values) is recorded as the run's parameters, so the caller can
// pass a commit, branch, or any other context to the run.
//
// The handler looks up the pipeline's webhook triggers and matches the request
// against them. A trigger may authenticate the caller in two independent
// ways, both of which must hold when both are set:
//
//   - a non-empty secret: the presented secret (in the X-Cdrom-Webhook-Secret
//     header) must match it (constant-time, to avoid a timing oracle);
//   - a non-empty oidc_issuer: the caller must present an OIDC Bearer token
//     that verifies against that issuer and whose claims satisfy the
//     trigger's oidc_claims (a value of "*" is a wildcard matching any value,
//     including the claim being absent).
//
// A trigger with neither a secret nor an oidc_issuer is open: any POST starts
// a run. A matched trigger starts a run via the scheduler's TriggerRun RPC
// (which atomically deduplicates it); when the caller authenticated with an
// OIDC token the token's flattened claims are recorded on the run (and its
// job instances) so the API can stamp them (prefixed with upstream_) onto the
// job tokens it hands to execution targets. A pipeline with no webhook
// trigger is rejected with 404.
func (s *Server) webhook(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	pipeline, err := s.clients.Database.GetPipeline(r.Context(), &dbpb.GetPipelineRequest{Id: pipelineID})
	if err != nil {
		grpcError(w, err)
		return
	}
	// Find the pipeline's webhook triggers.
	var webhookTriggers []*dbpb.Trigger
	for _, trigger := range pipeline.GetTriggers() {
		if trigger.GetType() == dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK {
			webhookTriggers = append(webhookTriggers, trigger)
		}
	}
	if len(webhookTriggers) == 0 {
		httpError(w, http.StatusNotFound, "pipeline has no webhook trigger")
		return
	}
	// Match the request against the webhook triggers. A trigger is matched
	// when every credential it sets is satisfied: a set secret must equal the
	// presented secret (constant-time), and a set oidc_issuer must have a
	// verifying Bearer token whose claims satisfy the trigger's oidc_claims.
	// A trigger with neither is open (any request matches it).
	secret := r.Header.Get(webhookSecretHeader)
	token := httpBearerToken(r)
	var matched *dbpb.Trigger
	var upstreamClaims map[string]any
	for _, trigger := range webhookTriggers {
		if !webhookTriggerMatches(trigger, secret, token, r.Context(), s.webhookOIDC, &upstreamClaims) {
			continue
		}
		matched = trigger
		break
	}
	if matched == nil {
		httpError(w, http.StatusUnauthorized, "webhook request matched no trigger")
		return
	}
	// The request body is a flat object of string values, recorded as the
	// run's parameters. An empty body is allowed (a run with the trigger's
	// static params only).
	var payload map[string]string
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	// Merge the trigger's static params (the payload overrides them on a key
	// collision).
	params := make(map[string]string, len(matched.GetParams())+len(payload))
	for k, v := range matched.GetParams() {
		params[k] = v
	}
	for k, v := range payload {
		params[k] = v
	}
	run, err := s.clients.Scheduler.TriggerRun(r.Context(), &schedpb.TriggerRunRequest{
		PipelineId:     pipelineID,
		TriggerName:    matched.GetName(),
		TriggerType:    dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK,
		Params:         params,
		UpstreamClaims: upstreamClaimsToStruct(upstreamClaims),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	// The webhook's actor is the caller's OIDC subject (when the trigger
	// authenticated with an OIDC token) or a synthetic "system:webhook"
	// identity (a secret-authenticated or open trigger).
	webhookActor := "system:webhook"
	webhookActorKind := audit.ActorKindSystem
	if subject, ok := upstreamClaims["sub"].(string); ok && subject != "" {
		webhookActor = subject
		webhookActorKind = audit.ActorKindUser
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionRunTrigger,
		TargetKind: audit.TargetRun,
		TargetID:   strconv.FormatInt(run.GetId(), 10),
		PipelineID: pipelineID,
		RunID:      run.GetId(),
		Outcome:    audit.OutcomeSuccess,
		Actor:      webhookActor,
		ActorKind:  webhookActorKind,
		Details:    auditJSON(map[string]any{"trigger": matched.GetName(), "params": params}),
	})
	s.publish(Event{Type: EventRunStatus, RunID: run.GetId(), Status: runStatusName(run.GetStatus())})
	writeJSON(w, http.StatusCreated, run)
}

// webhookTriggerMatches reports whether a webhook trigger is satisfied by the
// presented secret and OIDC token. A trigger with neither a secret nor an
// oidc_issuer is open and always matches. When the trigger sets an
// oidc_issuer the caller's Bearer token is verified against it and its claims
// (flattened to dot-addressed string values) are matched against the
// trigger's oidc_claims (a value of "*" is a wildcard); on a match the token's
// full structured claims are written to upstreamClaims (so the caller can
// record them on the run). A set secret must additionally equal the presented
// secret (constant-time).
func webhookTriggerMatches(trigger *dbpb.Trigger, secret, token string, ctx context.Context, verifier webhookTokenVerifier, upstreamClaims *map[string]any) bool {
	issuer := trigger.GetOidcIssuer()
	if issuer != "" {
		if token == "" {
			return false
		}
		claims, err := verifier.verifyToken(ctx, issuer, token)
		if err != nil {
			return false
		}
		// Match against the flattened claims (a trigger's oidc_claims are
		// dot-addressed string values); record the raw structured claims so a
		// complex claim (an object or a list) keeps its shape on the run.
		if !oidcClaimsMatch(flattenClaims(claims), trigger.GetOidcClaims()) {
			return false
		}
		if upstreamClaims != nil {
			*upstreamClaims = claims
		}
	}
	if trigger.GetSecret() != "" {
		if subtle.ConstantTimeCompare([]byte(secret), []byte(trigger.GetSecret())) != 1 {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

// paramValueRequest is the JSON form of a single step param value: either a
// scalar String or a list of Strings (by convention, only one is set).
type paramValueRequest struct {
	String  string   `json:"string,omitempty"`
	Strings []string `json:"strings,omitempty"`
}

// jobStepRequest is the JSON form of a single execution step. Type selects the
// step handler that runs the step; empty means the built-in "shell" handler.
// Timeout is a duration string (e.g. "30s", "5m"); empty means no per-step
// timeout. Handler-specific settings live in Params, not in the step's own
// fields, so a new step type can be added without changing the request schema.
// The built-in shell handler reads its command, args, and shell from Params:
//   - "command" (string) — the executable to run (required for the shell
//     handler).
//   - "args" (list of strings) — the command's arguments, in order.
//   - "shell" (string) — when set, the command is run through this shell
//     instead of directly: the target executes `<shell> <args> <command>`
//     (e.g. shell "pwsh", args ["-NoProfile", "-Command"], command
//     "Get-ChildItem"). Empty means run command directly (no shell).
type jobStepRequest struct {
	Type         string                        `json:"type,omitempty"`
	Workdir      string                        `json:"workdir,omitempty"`
	Env          map[string]string             `json:"env,omitempty"`
	Timeout      string                        `json:"timeout,omitempty"`
	Condition    string                        `json:"condition,omitempty"`
	IgnoreFailed bool                          `json:"ignore_failed,omitempty"`
	Params       map[string]*paramValueRequest `json:"params,omitempty"`
}

// jobRequest is the body of POST /api/jobs. Spec is the execution spec to
// snapshot onto the job; when omitted the job has no steps and succeeds
// without doing any work. DependsOn (F-06) is a minimal, single-level
// dependency mechanism ahead of F-08's full DAG/`needs`: when non-empty the
// job is held pending until every dependency succeeds — see
// docs/Architecture.md.
type jobRequest struct {
	PipelineID  int64   `json:"pipeline_id"`
	Name        string  `json:"name"`
	TargetGroup string  `json:"target_group"`
	DependsOn   []int64 `json:"depends_on,omitempty"`
	// Key is the job's stable, pipeline-scoped identifier (F-08); required
	// when the job belongs to a pipeline and is referenced by other jobs'
	// needs.
	Key   string          `json:"key,omitempty"`
	Needs []string        `json:"needs,omitempty"`
	Spec  *jobSpecRequest `json:"spec,omitempty"`
}

// jobSpecRequest is the JSON form of a job's execution spec. Timeout is a
// duration string (e.g. "30s", "5m") bounding the whole job (all steps
// combined); empty means no job-level timeout. Retry is the job's retry
// policy (F-04); when omitted the job is never retried.
type jobSpecRequest struct {
	Steps        []jobStepRequest    `json:"steps,omitempty"`
	Timeout      string              `json:"timeout,omitempty"`
	Retry        *retryPolicyRequest `json:"retry,omitempty"`
	IgnoreFailed bool                `json:"ignore_failed,omitempty"`
}

// retryPolicyRequest is the JSON form of a job's retry policy (F-04).
// MaxAttempts is the number of retries after the initial attempt (0 means the
// job is never retried); Backoff is a duration string (e.g. "10s") to wait
// before each retry (empty means retries are dispatched immediately).
type retryPolicyRequest struct {
	MaxAttempts int    `json:"max_attempts,omitempty"`
	Backoff     string `json:"backoff,omitempty"`
}

// toProtoSpec converts the JSON spec into the proto JobSpec carried to the
// scheduler. It returns nil when the spec is absent and has no steps, no job
// level timeout, and no retry policy.
func (r *jobSpecRequest) toProtoSpec() (*dbpb.JobSpec, error) {
	if r == nil || (len(r.Steps) == 0 && r.Timeout == "" && r.Retry == nil) {
		return nil, nil
	}
	spec := &dbpb.JobSpec{}
	if r.Timeout != "" {
		duration, err := time.ParseDuration(r.Timeout)
		if err != nil {
			return nil, fmt.Errorf("spec: invalid timeout %q: %w", r.Timeout, err)
		}
		spec.Timeout = durationpb.New(duration)
	}
	if r.Retry != nil {
		retry := &dbpb.RetryPolicy{MaxAttempts: int32(r.Retry.MaxAttempts)}
		if r.Retry.Backoff != "" {
			duration, err := time.ParseDuration(r.Retry.Backoff)
			if err != nil {
				return nil, fmt.Errorf("spec: invalid retry backoff %q: %w", r.Retry.Backoff, err)
			}
			retry.Backoff = durationpb.New(duration)
		}
		spec.Retry = retry
	}
	spec.IgnoreFailed = r.IgnoreFailed
	for i, step := range r.Steps {
		// A command is required for the built-in shell handler (the default
		// when type is empty); other step types may carry their work in
		// params instead.
		if step.Type == "" && paramString(step.Params, "command") == "" {
			return nil, fmt.Errorf("spec: step %d: command is required", i)
		}
		protoStep := &dbpb.JobStep{
			Type:         step.Type,
			Workdir:      step.Workdir,
			Env:          step.Env,
			Condition:    step.Condition,
			IgnoreFailed: step.IgnoreFailed,
			Params:       paramsToProto(step.Params),
		}
		if step.Timeout != "" {
			duration, err := time.ParseDuration(step.Timeout)
			if err != nil {
				return nil, fmt.Errorf("spec: step %d: invalid timeout %q: %w", i, step.Timeout, err)
			}
			protoStep.Timeout = durationpb.New(duration)
		}
		spec.Steps = append(spec.Steps, protoStep)
	}
	return spec, nil
}

// paramsToProto converts the JSON params map into the proto params map.
func paramsToProto(in map[string]*paramValueRequest) map[string]*dbpb.ParamValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*dbpb.ParamValue, len(in))
	for k, v := range in {
		out[k] = &dbpb.ParamValue{
			String_: v.String,
			Strings: v.Strings,
		}
	}
	return out
}

// paramString returns the string value of a param, or "" if the param is
// absent or not a string.
func paramString(in map[string]*paramValueRequest, key string) string {
	if v := in[key]; v != nil {
		return v.String
	}
	return ""
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	var req jobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	// A job that belongs to a pipeline is a job definition (pipelines.can-edit);
	// a standalone job triggers work (runs.can-trigger).
	if req.PipelineID > 0 {
		if !s.requirePermission(w, r, authz.PermPipelinesEdit, authz.Resource{PipelineID: req.PipelineID}) {
			return
		}
	} else {
		if !s.requirePermission(w, r, authz.PermRunsTrigger, authz.Resource{}) {
			return
		}
	}
	spec, err := req.Spec.toProtoSpec()
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid spec: %v", err)
		return
	}
	// A job that belongs to a pipeline is a job *definition* (F-08): it is
	// persisted on the pipeline (via the database service) and becomes part of
	// the pipeline's DAG, from which runs create job instances. It is not
	// dispatched on its own. A job with no pipeline is a standalone job and is
	// created and dispatched by the scheduler.
	if req.PipelineID > 0 {
		created, err := s.clients.Database.CreateJob(r.Context(), &dbpb.CreateJobRequest{
			PipelineId:  req.PipelineID,
			Name:        req.Name,
			TargetGroup: req.TargetGroup,
			DependsOn:   req.DependsOn,
			Key:         req.Key,
			Needs:       req.Needs,
			Spec:        spec,
		})
		if err != nil {
			grpcError(w, err)
			return
		}
		s.publish(Event{Type: EventJobStatus, JobID: created.GetId(), Status: jobStatusName(created.GetStatus())})
		writeJSON(w, http.StatusCreated, created)
		return
	}
	job, err := s.clients.Scheduler.SubmitJob(r.Context(), &schedpb.SubmitJobRequest{
		PipelineId:  req.PipelineID,
		Name:        req.Name,
		TargetGroup: req.TargetGroup,
		DependsOn:   req.DependsOn,
		Key:         req.Key,
		Needs:       req.Needs,
		Spec:        spec,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	// A standalone job submit triggers work (F-15): record it as a run trigger
	// with the job as the target.
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionRunTrigger,
		TargetKind: audit.TargetJob,
		TargetID:   strconv.FormatInt(job.GetId(), 10),
		TargetName: job.GetName(),
		PipelineID: job.GetPipelineId(),
		RunID:      job.GetRunId(),
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"trigger": "manual", "target_group": req.TargetGroup}),
	})
	s.publish(Event{Type: EventJobStatus, JobID: job.GetId(), Status: jobStatusName(job.GetStatus())})
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{}) {
		return
	}
	query := r.URL.Query()
	req := &schedpb.ListJobsRequest{}
	if value := query.Get("pipeline_id"); value != "" {
		if id, err := strconv.ParseInt(value, 10, 64); err == nil {
			req.PipelineId = id
		}
	}
	if value := query.Get("status"); value != "" {
		req.Status = jobStatusFromName(value)
	}
	response, err := s.clients.Scheduler.ListJobs(r.Context(), req)
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetJobs()))
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{}) {
		return
	}
	job, err := s.clients.Scheduler.GetJob(r.Context(), &schedpb.GetJobRequest{Id: jobID})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermRunsCancel, authz.Resource{}) {
		return
	}
	job, err := s.clients.Scheduler.CancelJob(r.Context(), &schedpb.CancelJobRequest{Id: jobID})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionJobCancel,
		TargetKind: audit.TargetJob,
		TargetID:   strconv.FormatInt(job.GetId(), 10),
		TargetName: job.GetName(),
		PipelineID: job.GetPipelineId(),
		RunID:      job.GetRunId(),
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"status": jobStatusName(job.GetStatus())}),
	})
	s.publish(Event{Type: EventJobStatus, JobID: job.GetId(), Status: jobStatusName(job.GetStatus())})
	writeJSON(w, http.StatusOK, job)
}

// rerunJob re-runs a finished job (F-04): it resets the job to pending with a
// fresh attempt and re-dispatches it, so the job runs again with the same
// spec. Only jobs in a terminal state can be re-run.
func (s *Server) rerunJob(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermRunsTrigger, authz.Resource{}) {
		return
	}
	job, err := s.clients.Scheduler.RerunJob(r.Context(), &schedpb.RerunJobRequest{Id: jobID})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionJobRerun,
		TargetKind: audit.TargetJob,
		TargetID:   strconv.FormatInt(job.GetId(), 10),
		TargetName: job.GetName(),
		PipelineID: job.GetPipelineId(),
		RunID:      job.GetRunId(),
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"status": jobStatusName(job.GetStatus()), "attempt": job.GetAttempt()}),
	})
	s.publish(Event{Type: EventJobStatus, JobID: job.GetId(), Status: jobStatusName(job.GetStatus())})
	writeJSON(w, http.StatusOK, job)
}

// approvalRequest is the body of POST /api/jobs/{id}/approve and
// /api/jobs/{id}/reject (F-13): an optional free-text reason the user gives
// for the decision.
type approvalRequest struct {
	Reason string `json:"reason,omitempty"`
}

// approveJob approves a job that is awaiting approval (F-13): it records the
// decision (approved), the actor (the authenticated user), and an optional
// reason on the job (via the scheduler, which persists it and appends a
// job_status event to the shared event log, F-23). The execution target waiting
// at the gate observes the decision on its next poll and continues the job.
// Only a job that is still awaiting approval can be approved; a job that is no
// longer awaiting approval (it was cancelled, or the gate was already
// resolved) is left untouched.
func (s *Server) approveJob(w http.ResponseWriter, r *http.Request) {
	s.resolveApproval(w, r, "approved")
}

// rejectJob rejects a job that is awaiting approval (F-13): it records the
// decision (rejected), the actor, and an optional reason on the job. The
// execution target waiting at the gate observes the decision and fails the
// job. Only a job that is still awaiting approval can be rejected.
func (s *Server) rejectJob(w http.ResponseWriter, r *http.Request) {
	s.resolveApproval(w, r, "rejected")
}

// resolveApproval is the shared implementation of the approve/reject
// endpoints (F-13). It records the decision (approved or rejected), the actor
// (the authenticated user, via auth.UserFromContext), and an optional reason
// on the job (via the scheduler's ResolveApproval, which persists it and
// appends a job_status event to the shared event log, F-23). The actor is the
// authenticated user's subject; when authentication is disabled there is no
// actor (the decision is recorded with an empty actor).
func (s *Server) resolveApproval(w http.ResponseWriter, r *http.Request, decision string) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	// Approving requires jobs.can-approve; rejecting requires jobs.can-reject.
	permission := authz.PermJobsApprove
	if decision == "rejected" {
		permission = authz.PermJobsReject
	}
	if !s.requirePermission(w, r, permission, authz.Resource{}) {
		return
	}
	var req approvalRequest
	// The body is optional (a reason is not required); an empty or absent body
	// is fine.
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	actor := auth.UserFromContext(r.Context()).Subject
	job, err := s.clients.Scheduler.ResolveApproval(r.Context(), &schedpb.ResolveApprovalRequest{
		Id:       jobID,
		Decision: decision,
		Actor:    actor,
		Reason:   req.Reason,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	action := audit.ActionJobApprove
	if decision == "rejected" {
		action = audit.ActionJobReject
	}
	s.recordAudit(r, audit.Event{
		Action:     action,
		TargetKind: audit.TargetJob,
		TargetID:   strconv.FormatInt(job.GetId(), 10),
		TargetName: job.GetName(),
		PipelineID: job.GetPipelineId(),
		RunID:      job.GetRunId(),
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"decision": decision, "reason": req.Reason, "status": jobStatusName(job.GetStatus())}),
	})
	s.publish(Event{Type: EventJobStatus, JobID: job.GetId(), Status: jobStatusName(job.GetStatus())})
	writeJSON(w, http.StatusOK, job)
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermWorkersView, authz.Resource{}) {
		return
	}
	response, err := s.clients.Database.ListWorkers(r.Context(), &dbpb.ListWorkersRequest{
		Group: r.URL.Query().Get("group"),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetWorkers()))
}

// ---------------------------------------------------------------------------
// Artifacts
// ---------------------------------------------------------------------------

func (s *Server) listJobArtifacts(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{}) {
		return
	}
	response, err := s.clients.Artifacts.ListArtifacts(r.Context(), &artifactspb.ListArtifactsRequest{
		Namespace: strconv.FormatInt(jobID, 10),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetArtifacts()))
}

// ---------------------------------------------------------------------------
// Job logs
// ---------------------------------------------------------------------------

// listJobLogs lists a job's log files (one per step plus the combined
// job.log) via the artifacts service.
func (s *Server) listJobLogs(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{}) {
		return
	}
	response, err := s.clients.Artifacts.ListLogs(r.Context(), &artifactspb.ListLogsRequest{
		Namespace: strconv.FormatInt(jobID, 10),
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetLogs()))
}

// getJobLog streams a job's log file (a step's output or the combined
// job.log) to the caller. The name is the log file name, e.g. "step-0.log"
// or "job.log".
func (s *Server) getJobLog(w http.ResponseWriter, r *http.Request) {
	jobID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "log name is required")
		return
	}
	stream, err := s.clients.Artifacts.DownloadLog(r.Context(), &artifactspb.DownloadLogRequest{
		Namespace: strconv.FormatInt(jobID, 10),
		Name:      name,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	// The first chunk carries metadata; set the content type from it, then
	// stream the data chunks to the response body.
	first, err := stream.Recv()
	if err != nil {
		grpcError(w, err)
		return
	}
	if contentType := first.GetMetadata().GetContentType(); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(first.GetData()); err != nil {
		return
	}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			return
		}
		if _, err := w.Write(chunk.GetData()); err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

// encryptSecretRequest is the body of POST /api/secrets/encrypt: a plaintext
// value to encrypt with the API's secret store (F-12).
type encryptSecretRequest struct {
	Value string `json:"value"`
}

// encryptSecretResponse is the response of POST /api/secrets/encrypt: the
// ciphertext for the supplied value. The ciphertext is self-describing (it
// carries the nonce) and is what a pipeline's secret declaration stores.
type encryptSecretResponse struct {
	Ciphertext string `json:"ciphertext"`
}

// encryptSecret encrypts a value through the API's secret store (F-12) and
// returns the ciphertext. It lets an authenticated user produce the ciphertext
// for a secret value without it ever being persisted by the API. When no
// secrets key is configured the API cannot encrypt and rejects the request.
func (s *Server) encryptSecret(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermSecretsManage, authz.Resource{}) {
		return
	}
	var req encryptSecretRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if s.secretStore == nil {
		httpError(w, http.StatusBadRequest, "secrets are not supported: no secret store is available")
		return
	}
	ciphertext, err := s.secretStore.Encrypt(r.Context(), req.Value)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "encrypt: %v", err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionSecretEncrypt,
		TargetKind: audit.TargetSecret,
		Outcome:    audit.OutcomeSuccess,
		Details:    auditJSON(map[string]any{"value": "[REDACTED]"}),
	})
	writeJSON(w, http.StatusOK, encryptSecretResponse{Ciphertext: ciphertext})
}

// ---------------------------------------------------------------------------
// Audit log (F-15)
// ---------------------------------------------------------------------------

// listAudit queries the shared audit log (F-15) via the Database service. It
// requires the audit.can-view permission (F-14). The log is append-only: past
// events are immutable (they are only ever pruned by age, by the API's
// retention schedule). Events are returned most recent first, filtered by
// actor, action, target kind, target id, pipeline, and a time range (all
// optional; an empty filter matches everything).
func (s *Server) listAudit(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermAuditView, authz.Resource{}) {
		return
	}
	query := r.URL.Query()
	req := &dbpb.ListAuditEventsRequest{}
	if value := query.Get("actor"); value != "" {
		req.Actor = value
	}
	if value := query.Get("action"); value != "" {
		req.Action = value
	}
	if value := query.Get("target_kind"); value != "" {
		req.TargetKind = value
	}
	if value := query.Get("target_id"); value != "" {
		req.TargetId = value
	}
	if value := query.Get("pipeline_id"); value != "" {
		if id, err := strconv.ParseInt(value, 10, 64); err == nil {
			req.PipelineId = id
		}
	}
	if value := query.Get("from"); value != "" {
		if t, err := time.Parse(time.RFC3339, value); err == nil {
			req.From = timestamppb.New(t)
		}
	}
	if value := query.Get("to"); value != "" {
		if t, err := time.Parse(time.RFC3339, value); err == nil {
			req.To = timestamppb.New(t)
		}
	}
	if value := query.Get("limit"); value != "" {
		if n, err := strconv.ParseInt(value, 10, 32); err == nil {
			req.Limit = int32(n)
		}
	}
	response, err := s.clients.Database.ListAuditEvents(r.Context(), req)
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(response.GetEvents()))
}

// ---------------------------------------------------------------------------
// Username/password authentication (proxied to the IdP)
// ---------------------------------------------------------------------------

// adminRole is the role required to manage users and roles. A user with this
// role can list, create, update, and delete users (and change their roles).
const adminRole = "admin"

// requireUserPass reports whether username/password authentication is enabled
// (a userpass client is attached). When it is not, the endpoints respond 501.
func (s *Server) requireUserPass(w http.ResponseWriter) bool {
	if s.userpass == nil {
		httpError(w, http.StatusNotImplemented, "username/password authentication is not enabled")
		return false
	}
	return true
}

// userPassStatus maps an error from the IdP's userpass surface to an HTTP
// status: a userPassError is mapped from its gRPC status code, and any other
// error (a transport failure reaching the IdP) is a 502.
func userPassStatus(err error) int {
	if e, ok := err.(*userPassError); ok {
		switch e.code {
		case codes.Unauthenticated:
			return http.StatusUnauthorized
		case codes.NotFound:
			return http.StatusNotFound
		case codes.AlreadyExists:
			return http.StatusConflict
		case codes.InvalidArgument:
			return http.StatusBadRequest
		case codes.Unimplemented:
			return http.StatusNotImplemented
		}
		return http.StatusBadGateway
	}
	return http.StatusBadGateway
}

// requireAdmin verifies the caller may manage users (used by the user
// management endpoints). When RBAC is enabled it requires the users.can-manage
// permission (F-14) — which the admin role grants, but a custom role could
// also grant. When RBAC is not enabled it falls back to requiring the admin
// role (the pre-F-14 behavior). It reports false (and writes the response)
// when the caller is not allowed.
func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
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

// loginRequest is the body of POST /api/login: the user's email and password.
type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// login is the unauthenticated entry point for username/password sign-in. It
// proxies the credentials to the IdP, which verifies them and mints an OIDC
// token for the user (stamped with the user's roles). The returned token is
// the same OIDC token the API verifies on every other request, so a client
// can present it as `Authorization: Bearer <access_token>`.
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if !s.requireUserPass(w) {
		return
	}
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Email == "" || req.Password == "" {
		httpError(w, http.StatusUnauthorized, "email and password are required")
		return
	}
	result, err := s.userpass.Login(r.Context(), req.Email, req.Password)
	if err != nil {
		// A failed login is audited too (F-15): the actor is the email the
		// caller presented, and the outcome is failure.
		s.recordAudit(r, audit.Event{
			Action:     audit.ActionLogin,
			TargetKind: audit.TargetLogin,
			TargetID:   req.Email,
			Outcome:    audit.OutcomeFailure,
			Actor:      req.Email,
			ActorKind:  audit.ActorKindUser,
		})
		if userPassStatus(err) == http.StatusUnauthorized {
			httpError(w, http.StatusUnauthorized, "invalid credentials")
			return
		}
		httpError(w, userPassStatus(err), "login: %v", err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionLogin,
		TargetKind: audit.TargetLogin,
		TargetID:   result.Email,
		TargetName: result.Email,
		Outcome:    audit.OutcomeSuccess,
		Actor:      result.Email,
		ActorKind:  audit.ActorKindUser,
		Details:    auditJSON(map[string]any{"roles": result.Roles}),
	})
	writeJSON(w, http.StatusOK, result)
}

// registerRequest is the body of POST /api/register: the required fields to
// create a user (first/last/email and a password).
type registerRequest struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
	Password  string `json:"password"`
}

// register is the unauthenticated entry point for creating a user. It proxies
// the request to the IdP, which hashes the password and stores the user. The
// first user ever registered is given the admin role (so a local run has a
// way in); later users get the default role.
func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	if !s.requireUserPass(w) {
		return
	}
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Email == "" || req.Password == "" {
		httpError(w, http.StatusBadRequest, "email and password are required")
		return
	}
	result, err := s.userpass.Register(r.Context(), RegisterRequest{
		FirstName: req.FirstName,
		LastName:  req.LastName,
		Email:     req.Email,
		Password:  req.Password,
	})
	if err != nil {
		if userPassStatus(err) == http.StatusConflict {
			httpError(w, http.StatusConflict, "a user with that email already exists")
			return
		}
		httpError(w, userPassStatus(err), "register: %v", err)
		return
	}
	// The actor is the user being created (a register is an unauthenticated
	// entry point, so there is no caller identity to attribute it to).
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionRegister,
		TargetKind: audit.TargetUser,
		TargetID:   result.ID,
		TargetName: result.Email,
		Outcome:    audit.OutcomeSuccess,
		Actor:      result.Email,
		ActorKind:  audit.ActorKindUser,
		Details:    auditJSON(map[string]any{"roles": result.Roles}),
	})
	writeJSON(w, http.StatusCreated, result)
}

// listUsers returns all registered users. It requires an authenticated admin
// caller.
func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	if !s.requireUserPass(w) {
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	users, err := s.userpass.ListUsers(r.Context())
	if err != nil {
		httpError(w, userPassStatus(err), "list users: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(users))
}

// createUser creates a user (profile, password, and roles). It requires an
// authenticated admin caller, and — unlike the unauthenticated register — it
// honours the supplied roles.
type createUserRequest struct {
	FirstName string   `json:"first_name"`
	LastName  string   `json:"last_name"`
	Email     string   `json:"email"`
	Password  string   `json:"password"`
	Roles     []string `json:"roles,omitempty"`
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	if !s.requireUserPass(w) {
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	var req createUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Email == "" || req.Password == "" {
		httpError(w, http.StatusBadRequest, "email and password are required")
		return
	}
	result, err := s.userpass.CreateUser(r.Context(), RegisterRequest{
		FirstName: req.FirstName,
		LastName:  req.LastName,
		Email:     req.Email,
		Password:  req.Password,
		Roles:     req.Roles,
	})
	if err != nil {
		if userPassStatus(err) == http.StatusConflict {
			httpError(w, http.StatusConflict, "a user with that email already exists")
			return
		}
		httpError(w, userPassStatus(err), "create user: %v", err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionUserCreate,
		TargetKind: audit.TargetUser,
		TargetID:   result.ID,
		TargetName: result.Email,
		Outcome:    audit.OutcomeSuccess,
		NewValue:   auditJSON(map[string]any{"email": result.Email, "roles": result.Roles}),
	})
	writeJSON(w, http.StatusCreated, result)
}

// updateUser updates a user's profile and/or roles (and password when
// supplied). It requires an authenticated admin caller.
func (s *Server) updateUser(w http.ResponseWriter, r *http.Request) {
	if !s.requireUserPass(w) {
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "user id is required")
		return
	}
	var req UpdateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	// Fetch the user's current profile so the audit event can record what
	// changed (old value vs new value, F-15). A failure to fetch it does not
	// block the update.
	oldValue := ""
	if users, err := s.userpass.ListUsers(r.Context()); err == nil {
		for i := range users {
			if users[i].ID == id {
				oldValue = auditJSON(map[string]any{"email": users[i].Email, "roles": users[i].Roles})
				break
			}
		}
	}
	result, err := s.userpass.UpdateUser(r.Context(), id, req)
	if err != nil {
		if userPassStatus(err) == http.StatusNotFound {
			httpError(w, http.StatusNotFound, "user not found")
			return
		}
		httpError(w, userPassStatus(err), "update user: %v", err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionUserUpdate,
		TargetKind: audit.TargetUser,
		TargetID:   result.ID,
		TargetName: result.Email,
		Outcome:    audit.OutcomeSuccess,
		OldValue:   oldValue,
		NewValue:   auditJSON(map[string]any{"email": result.Email, "roles": result.Roles}),
	})
	writeJSON(w, http.StatusOK, result)
}

// deleteUser removes a user by ID. It requires an authenticated admin caller.
func (s *Server) deleteUser(w http.ResponseWriter, r *http.Request) {
	if !s.requireUserPass(w) {
		return
	}
	if !s.requireAdmin(w, r) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "user id is required")
		return
	}
	// Fetch the user's profile before deleting it so the audit event can
	// record the user that was removed (F-15). A failure to fetch it does not
	// block the delete.
	oldValue := ""
	if users, err := s.userpass.ListUsers(r.Context()); err == nil {
		for i := range users {
			if users[i].ID == id {
				oldValue = auditJSON(map[string]any{"email": users[i].Email, "roles": users[i].Roles})
				break
			}
		}
	}
	if err := s.userpass.DeleteUser(r.Context(), id); err != nil {
		if userPassStatus(err) == http.StatusNotFound {
			httpError(w, http.StatusNotFound, "user not found")
			return
		}
		httpError(w, userPassStatus(err), "delete user: %v", err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionUserDelete,
		TargetKind: audit.TargetUser,
		TargetID:   id,
		Outcome:    audit.OutcomeSuccess,
		OldValue:   oldValue,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// ---------------------------------------------------------------------------
// API keys (F-25)
// ---------------------------------------------------------------------------

// requireAPIKey reports whether API-key authentication is enabled (an API-key
// client is attached). When it is not, the endpoints respond 501.
func (s *Server) requireAPIKey(w http.ResponseWriter) bool {
	if s.apikey == nil {
		httpError(w, http.StatusNotImplemented, "api key authentication is not enabled")
		return false
	}
	return true
}

// apiKeyStatus maps an error from the IdP's API-key surface to an HTTP status:
// an apiKeyError is mapped from its gRPC status code, and any other error (a
// transport failure reaching the IdP) is a 502.
func apiKeyStatus(err error) int {
	if e, ok := err.(*apiKeyError); ok {
		switch e.code {
		case codes.Unauthenticated:
			return http.StatusUnauthorized
		case codes.NotFound:
			return http.StatusNotFound
		case codes.InvalidArgument:
			return http.StatusBadRequest
		case codes.Unimplemented:
			return http.StatusNotImplemented
		}
		return http.StatusBadGateway
	}
	return http.StatusBadGateway
}

// apiKeyOwnerEmail resolves a key's owner id to the owner's email (so the UI
// can show who a key belongs to). It returns "" when the owner cannot be
// resolved (e.g. the user was deleted); a failure to resolve does not block
// the response.
func (s *Server) apiKeyOwnerEmail(ctx context.Context, ownerID string) string {
	if s.userpass == nil || ownerID == "" {
		return ""
	}
	users, err := s.userpass.ListUsers(ctx)
	if err != nil {
		return ""
	}
	for i := range users {
		if users[i].ID == ownerID {
			return users[i].Email
		}
	}
	return ""
}

// fillAPIKeyOwnerEmails resolves each key's owner email (best-effort) so the
// UI can show who a key belongs to.
func (s *Server) fillAPIKeyOwnerEmails(ctx context.Context, keys []APIKeyResult) {
	if s.userpass == nil {
		return
	}
	users, err := s.userpass.ListUsers(ctx)
	if err != nil {
		return
	}
	byID := make(map[string]string, len(users))
	for i := range users {
		byID[users[i].ID] = users[i].Email
	}
	for i := range keys {
		keys[i].OwnerEmail = byID[keys[i].OwnerID]
	}
}

// createAPIKeyRequest is the body of POST /api/api-keys: the key's metadata.
// owner_email is the user the key belongs to; when empty the caller's own
// account is used. An authenticated caller with the api-keys.can-manage
// permission (e.g. an admin) may create a key for another user.
type createAPIKeyRequest struct {
	OwnerEmail    string `json:"owner_email"`
	Description   string `json:"description"`
	ExpiresIn     string `json:"expires_in,omitempty"`
	PipelineScope []uint `json:"pipeline_scope,omitempty"`
}

// createAPIKey creates an API key for the caller (or, when the caller has the
// api-keys.can-manage permission, for another user). The response returns the
// plaintext `cdrom-…` key exactly once; the stored record holds only its hash.
func (s *Server) createAPIKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIKey(w) {
		return
	}
	var req createAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	ownerEmail := req.OwnerEmail
	if ownerEmail == "" {
		ownerEmail = user.Email
	}
	if ownerEmail == "" {
		httpError(w, http.StatusBadRequest, "owner_email is required")
		return
	}
	// Creating a key requires the api-keys.can-manage permission (self-service,
	// granted to the built-in user role). Creating a key for another user
	// additionally requires the api-keys.can-manage-all permission — an
	// explicit grant the admin role carries by default — so a regular user can
	// only ever create keys for themselves.
	if !s.requirePermission(w, r, authz.PermAPIKeysManage, authz.Resource{}) {
		return
	}
	if ownerEmail != user.Email {
		if !s.requirePermission(w, r, authz.PermAPIKeysManageAll, authz.Resource{}) {
			return
		}
	}
	result, plaintext, err := s.apikey.CreateAPIKey(r.Context(), ownerEmail, req.Description, req.ExpiresIn, req.PipelineScope)
	if err != nil {
		httpError(w, apiKeyStatus(err), "create api key: %v", err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAPIKeyCreate,
		TargetKind: audit.TargetAPIKey,
		TargetID:   result.ID,
		TargetName: result.KeyPrefix,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  audit.ActorKindUser,
		NewValue:   auditJSON(map[string]any{"owner": ownerEmail, "description": req.Description, "prefix": result.KeyPrefix, "pipeline_scope": result.PipelineScope}),
	})
	// The plaintext key is returned exactly once (at creation and at each
	// rotation); it is never returned by list/get.
	writeJSON(w, http.StatusCreated, map[string]any{"key": result, "plaintext": plaintext})
}

// listAPIKeys returns the caller's own API keys. It requires the
// api-keys.can-manage permission (which the built-in user role grants for
// self-service). A caller can only ever see their own keys here; listing every
// key in the system is a separate, more privileged operation (listAllAPIKeys).
func (s *Server) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermAPIKeysManage, authz.Resource{}) {
		return
	}
	user := auth.UserFromContext(r.Context())
	keys, err := s.apikey.ListAPIKeys(r.Context(), user.Subject)
	if err != nil {
		httpError(w, apiKeyStatus(err), "list api keys: %v", err)
		return
	}
	s.fillAPIKeyOwnerEmails(r.Context(), keys)
	writeJSON(w, http.StatusOK, nonNil(keys))
}

// listAllAPIKeys returns every API key in the system. It requires the
// api-keys.can-list-all permission — a more privileged, platform-wide grant
// that the admin role carries — so a regular user (who can only ever see their
// own keys via listAPIKeys) cannot enumerate other users' keys.
func (s *Server) listAllAPIKeys(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermAPIKeysListAll, authz.Resource{}) {
		return
	}
	keys, err := s.apikey.ListAPIKeys(r.Context(), "")
	if err != nil {
		httpError(w, apiKeyStatus(err), "list all api keys: %v", err)
		return
	}
	s.fillAPIKeyOwnerEmails(r.Context(), keys)
	writeJSON(w, http.StatusOK, nonNil(keys))
}

// callerCanManageAllAPIKeys reports whether the caller may manage every key in
// the system, as opposed to only their own. It requires the
// api-keys.can-manage-all permission — an explicit grant the admin role
// carries by default — so a regular user (who can only manage their own keys
// via api-keys.can-manage) cannot act on another user's keys. When
// authentication is disabled the caller acts as a synthetic admin and may
// manage every key.
func (s *Server) callerCanManageAllAPIKeys(r *http.Request) bool {
	if !s.rbacEnabled || s.authz == nil {
		// Authentication disabled: the caller acts as a synthetic admin.
		return true
	}
	principal := s.principalFromContext(r.Context())
	ok, err := s.authz.Check(r.Context(), principal, authz.PermAPIKeysManageAll, authz.Resource{})
	if err != nil {
		return false
	}
	return ok
}

// getAPIKey returns a single API key's metadata (never the plaintext or hash).
// A caller may fetch a key only if it is their own or they hold the
// api-keys.can-manage-all permission (which the admin role grants).
func (s *Server) getAPIKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermAPIKeysManage, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "api key id is required")
		return
	}
	result, err := s.apikey.GetAPIKey(r.Context(), id)
	if err != nil {
		httpError(w, apiKeyStatus(err), "get api key: %v", err)
		return
	}
	if !s.callerCanManageAllAPIKeys(r) && result.OwnerID != auth.UserFromContext(r.Context()).Subject {
		httpError(w, http.StatusForbidden, "permission denied: not your api key")
		return
	}
	keys := []APIKeyResult{*result}
	s.fillAPIKeyOwnerEmails(r.Context(), keys)
	writeJSON(w, http.StatusOK, keys[0])
}

// updateAPIKeyRequest is the body of PUT /api/api-keys/{id}: the key's new
// metadata. Only the fields present are changed (a "renew"); the key's secret
// is never changed. A field is applied only when it is present in the body:
// the pointer fields are nil when absent, so the decoder can tell "not
// provided" (leave unchanged) from "provided" (apply, even if empty).
type updateAPIKeyRequest struct {
	Description   *string `json:"description"`
	ExpiresIn     *string `json:"expires_in"`
	PipelineScope *[]uint `json:"pipeline_scope"`
}

// updateAPIKey edits a key's description, expiration, and/or pipeline scope
// without changing its secret (a "renew"); the same `cdrom-…` value keeps
// working. A caller may edit a key only if it is their own or they hold the
// api-keys.can-manage-all permission (which the admin role grants).
func (s *Server) updateAPIKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermAPIKeysManage, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "api key id is required")
		return
	}
	// A caller may edit only their own key unless they may manage every key
	// in the system (an admin).
	if !s.callerCanManageAllAPIKeys(r) {
		current, err := s.apikey.GetAPIKey(r.Context(), id)
		if err != nil {
			httpError(w, apiKeyStatus(err), "get api key: %v", err)
			return
		}
		if current.OwnerID != auth.UserFromContext(r.Context()).Subject {
			httpError(w, http.StatusForbidden, "permission denied: not your api key")
			return
		}
	}
	var req updateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	// A field is applied only when it is present in the body (its pointer is
	// non-nil); a field whose pointer is nil is left unchanged.
	result, err := s.apikey.UpdateAPIKey(r.Context(), id, UpdateAPIKeyRequest{
		Description:    derefString(req.Description),
		HasDescription: req.Description != nil,
		ExpiresIn:      derefString(req.ExpiresIn),
		HasExpiresAt:   req.ExpiresIn != nil,
		PipelineScope:  derefUintSlice(req.PipelineScope),
		HasScope:       req.PipelineScope != nil,
	})
	if err != nil {
		httpError(w, apiKeyStatus(err), "update api key: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAPIKeyUpdate,
		TargetKind: audit.TargetAPIKey,
		TargetID:   id,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  audit.ActorKindUser,
		NewValue:   auditJSON(map[string]any{"description": result.Description, "expires_at": result.ExpiresAt, "pipeline_scope": result.PipelineScope}),
	})
	writeJSON(w, http.StatusOK, result)
}

// derefString returns the value pointed to by p, or "" when p is nil.
func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// derefUintSlice returns the slice pointed to by p, or nil when p is nil.
func derefUintSlice(p *[]uint) []uint {
	if p == nil {
		return nil
	}
	return *p
}

// rotateAPIKey generates a brand-new `cdrom-…` secret for a key and returns it
// exactly once; the previous key stops working. A caller may rotate a key only
// if it is their own or they hold the api-keys.can-manage-all permission
// (which the admin role grants).
func (s *Server) rotateAPIKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermAPIKeysManage, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "api key id is required")
		return
	}
	if !s.callerCanManageAllAPIKeys(r) {
		current, err := s.apikey.GetAPIKey(r.Context(), id)
		if err != nil {
			httpError(w, apiKeyStatus(err), "get api key: %v", err)
			return
		}
		if current.OwnerID != auth.UserFromContext(r.Context()).Subject {
			httpError(w, http.StatusForbidden, "permission denied: not your api key")
			return
		}
	}
	result, plaintext, err := s.apikey.RotateAPIKey(r.Context(), id)
	if err != nil {
		httpError(w, apiKeyStatus(err), "rotate api key: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAPIKeyRotate,
		TargetKind: audit.TargetAPIKey,
		TargetID:   id,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  audit.ActorKindUser,
		NewValue:   auditJSON(map[string]any{"prefix": result.KeyPrefix}),
	})
	// The new plaintext key is returned exactly once; the previous key stops
	// working.
	writeJSON(w, http.StatusOK, map[string]any{"key": result, "plaintext": plaintext})
}

// deleteAPIKey removes a key by id. A caller may delete a key only if it is
// their own or they hold the api-keys.can-manage-all permission (which the
// admin role grants).
func (s *Server) deleteAPIKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermAPIKeysManage, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "api key id is required")
		return
	}
	if !s.callerCanManageAllAPIKeys(r) {
		current, err := s.apikey.GetAPIKey(r.Context(), id)
		if err != nil {
			httpError(w, apiKeyStatus(err), "get api key: %v", err)
			return
		}
		if current.OwnerID != auth.UserFromContext(r.Context()).Subject {
			httpError(w, http.StatusForbidden, "permission denied: not your api key")
			return
		}
	}
	if err := s.apikey.DeleteAPIKey(r.Context(), id); err != nil {
		httpError(w, apiKeyStatus(err), "delete api key: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAPIKeyDelete,
		TargetKind: audit.TargetAPIKey,
		TargetID:   id,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  audit.ActorKindUser,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// resetAPIKeyLockout clears a user's API-key lockout state. It requires the
// api-keys.can-manage permission (e.g. the admin role).
func (s *Server) resetAPIKeyLockout(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIKey(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermAPIKeysManage, authz.Resource{}) {
		return
	}
	var req struct {
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.UserID == "" {
		httpError(w, http.StatusBadRequest, "user_id is required")
		return
	}
	if err := s.apikey.ResetAPIKeyLockout(r.Context(), req.UserID); err != nil {
		httpError(w, apiKeyStatus(err), "reset api key lockout: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionAPIKeyResetLockout,
		TargetKind: audit.TargetAPIKey,
		TargetID:   req.UserID,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  audit.ActorKindUser,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

// ---------------------------------------------------------------------------
// Service accounts (F-26, proxied to the IdP)
// ---------------------------------------------------------------------------

// requireServiceAccount reports whether service-account authentication is
// enabled (a service-account client is attached). When it is not, the
// endpoints respond 501.
func (s *Server) requireServiceAccount(w http.ResponseWriter) bool {
	if s.serviceAccounts == nil {
		httpError(w, http.StatusNotImplemented, "service account authentication is not enabled")
		return false
	}
	return true
}

// serviceAccountStatus maps an error from the IdP's service-account surface to
// an HTTP status: a serviceAccountError is mapped from its gRPC status code,
// and any other error (a transport failure reaching the IdP) is a 502.
func serviceAccountStatus(err error) int {
	if e, ok := err.(*serviceAccountError); ok {
		switch e.code {
		case codes.Unauthenticated:
			return http.StatusUnauthorized
		case codes.NotFound:
			return http.StatusNotFound
		case codes.InvalidArgument:
			return http.StatusBadRequest
		case codes.AlreadyExists, codes.Aborted:
			return http.StatusConflict
		case codes.Unimplemented:
			return http.StatusNotImplemented
		}
		return http.StatusBadGateway
	}
	return http.StatusBadGateway
}

// serviceAccountRevision fetches an account's current revision (for the
// optimistic-concurrency guard on a mutation). It returns 0 when the account
// cannot be fetched (the mutation will then fail with a clear error).
func (s *Server) serviceAccountRevision(ctx context.Context, id string) int64 {
	acc, err := s.serviceAccounts.GetServiceAccount(ctx, id)
	if err != nil {
		return 0
	}
	return acc.Revision
}

// serviceAccountActorKind returns the actor kind for an audit event: the
// caller's principal kind (a service account acting on another account is
// recorded as a service-account actor, a human user as a user actor).
func serviceAccountActorKind(user auth.User) string {
	if user.Kind == "service-account" {
		return audit.ActorKindServiceAccount
	}
	return audit.ActorKindUser
}

// createServiceAccountRequest is the body of POST /api/service-accounts: the
// account's identity and optional initial roles.
type createServiceAccountRequest struct {
	LoginName   string   `json:"login_name"`
	DisplayName string   `json:"display_name,omitempty"`
	Description string   `json:"description,omitempty"`
	Roles       []string `json:"roles,omitempty"`
}

// createServiceAccount creates a service account and its two freshly generated
// key slots (and binds its roles). The response returns both plaintext keys
// exactly once; the stored record holds only their salted hashes. Creating an
// account requires service-accounts.can-create; creating it with roles
// additionally requires service-accounts.can-assign-roles (and the F-14
// delegation rule for each role).
func (s *Server) createServiceAccount(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	var req createServiceAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.LoginName == "" {
		httpError(w, http.StatusBadRequest, "login_name is required")
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsCreate, authz.Resource{}) {
		return
	}
	// Creating with roles additionally requires the assignment permission and
	// the F-14 delegation rule for each role (a caller cannot grant a role it
	// does not hold with sufficient scope).
	if len(req.Roles) > 0 {
		if !s.requirePermission(w, r, authz.PermServiceAccountsAssign, authz.Resource{}) {
			return
		}
		if s.rbacEnabled && s.authz != nil {
			caller := s.principalFromContext(r.Context())
			for _, role := range req.Roles {
				ok, err := s.authz.CanGrantWithPermission(r.Context(), caller, role, 0, authz.PermServiceAccountsAssign)
				if err != nil {
					httpError(w, http.StatusInternalServerError, "authorization: %v", err)
					return
				}
				if !ok {
					httpError(w, http.StatusForbidden, "cannot grant role %q: caller does not hold it with sufficient scope", role)
					return
				}
			}
		}
	}
	result, keys, err := s.serviceAccounts.CreateServiceAccount(r.Context(), req.LoginName, req.DisplayName, req.Description, req.Roles)
	if err != nil {
		httpError(w, serviceAccountStatus(err), "create service account: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionServiceAccountCreate,
		TargetKind: audit.TargetServiceAccount,
		TargetID:   result.ID,
		TargetName: result.LoginName,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  serviceAccountActorKind(user),
		NewValue:   auditJSON(map[string]any{"login_name": result.LoginName, "roles": result.Roles}),
	})
	// The two plaintext keys are returned exactly once (at creation); they are
	// never returned by list/get. The response must not be cached.
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusCreated, map[string]any{"account": result, "keys": keys})
}

// listServiceAccounts returns service accounts (metadata only, never the
// plaintext or hash). Deleted accounts are excluded unless the caller passes
// ?include_deleted=true. It requires service-accounts.can-view.
func (s *Server) listServiceAccounts(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsView, authz.Resource{}) {
		return
	}
	includeDeleted := r.URL.Query().Get("include_deleted") == "true"
	accounts, err := s.serviceAccounts.ListServiceAccounts(r.Context(), includeDeleted)
	if err != nil {
		httpError(w, serviceAccountStatus(err), "list service accounts: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, nonNil(accounts))
}

// getServiceAccount returns a single account's metadata and both slots'
// non-secret metadata (never the plaintext or hash). A deleted account is
// returned only when the caller passes ?include_deleted=true. It requires
// service-accounts.can-view.
func (s *Server) getServiceAccount(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsView, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "service account id is required")
		return
	}
	result, err := s.serviceAccounts.GetServiceAccount(r.Context(), id)
	if err != nil {
		httpError(w, serviceAccountStatus(err), "get service account: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// updateServiceAccountRequest is the body of PATCH /api/service-accounts/{id}:
// the account's new display name/description. Only the fields present are
// changed; a field whose pointer is nil is left unchanged. Protected fields
// (login name, roles, status, keys) cannot be changed here.
type updateServiceAccountRequest struct {
	DisplayName *string `json:"display_name"`
	Description *string `json:"description"`
}

// updateServiceAccount edits an account's display name/description (never its
// roles, status, or keys). It requires service-accounts.can-edit.
func (s *Server) updateServiceAccount(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsEdit, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "service account id is required")
		return
	}
	var req updateServiceAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	result, err := s.serviceAccounts.UpdateServiceAccount(r.Context(), id, UpdateServiceAccountRequest{
		DisplayName:    derefString(req.DisplayName),
		HasDisplayName: req.DisplayName != nil,
		Description:    derefString(req.Description),
		HasDescription: req.Description != nil,
		Revision:       s.serviceAccountRevision(r.Context(), id),
	})
	if err != nil {
		httpError(w, serviceAccountStatus(err), "update service account: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionServiceAccountUpdate,
		TargetKind: audit.TargetServiceAccount,
		TargetID:   result.ID,
		TargetName: result.LoginName,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  serviceAccountActorKind(user),
		NewValue:   auditJSON(map[string]any{"display_name": result.DisplayName, "description": result.Description}),
	})
	writeJSON(w, http.StatusOK, result)
}

// rotateServiceAccountKey generates a brand-new `cdrom-sa-…` secret for the
// selected slot (1 or 2) and returns it exactly once; the previous key stops
// working and the other slot is unchanged. It requires
// service-accounts.can-rotate-keys.
func (s *Server) rotateServiceAccountKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsRotate, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "service account id is required")
		return
	}
	slot, ok := parseServiceAccountSlot(r.PathValue("slot"))
	if !ok {
		httpError(w, http.StatusBadRequest, "slot must be 1 or 2")
		return
	}
	result, key, err := s.serviceAccounts.RotateServiceAccountKey(r.Context(), id, slot, s.serviceAccountRevision(r.Context(), id))
	if err != nil {
		httpError(w, serviceAccountStatus(err), "rotate service account key: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionServiceAccountRotate,
		TargetKind: audit.TargetServiceAccount,
		TargetID:   result.ID,
		TargetName: result.LoginName,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  serviceAccountActorKind(user),
		NewValue:   auditJSON(map[string]any{"slot": slot}),
	})
	// The new plaintext key is returned exactly once; the previous key stops
	// working. The response must not be cached.
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{"account": result, "key": key})
}

// disableServiceAccount temporarily disables an account (rejecting both keys
// while preserving the hashes and role bindings). It requires
// service-accounts.can-disable.
func (s *Server) disableServiceAccount(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsDisable, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "service account id is required")
		return
	}
	result, err := s.serviceAccounts.DisableServiceAccount(r.Context(), id, s.serviceAccountRevision(r.Context(), id))
	if err != nil {
		httpError(w, serviceAccountStatus(err), "disable service account: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionServiceAccountDisable,
		TargetKind: audit.TargetServiceAccount,
		TargetID:   result.ID,
		TargetName: result.LoginName,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  serviceAccountActorKind(user),
	})
	writeJSON(w, http.StatusOK, result)
}

// enableServiceAccount re-enables a disabled, non-deleted account (restoring
// both keys with the same keys and current roles). It requires
// service-accounts.can-enable.
func (s *Server) enableServiceAccount(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsEnable, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "service account id is required")
		return
	}
	result, err := s.serviceAccounts.EnableServiceAccount(r.Context(), id, s.serviceAccountRevision(r.Context(), id))
	if err != nil {
		httpError(w, serviceAccountStatus(err), "enable service account: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionServiceAccountEnable,
		TargetKind: audit.TargetServiceAccount,
		TargetID:   result.ID,
		TargetName: result.LoginName,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  serviceAccountActorKind(user),
	})
	writeJSON(w, http.StatusOK, result)
}

// assignServiceAccountRolesRequest is the body of POST
// /api/service-accounts/{id}/roles: the role names to bind to the account.
type assignServiceAccountRolesRequest struct {
	Roles []string `json:"roles"`
}

// assignServiceAccountRoles adds role bindings to an account. It requires
// service-accounts.can-assign-roles and the F-14 delegation rule for each role
// (a caller cannot grant a role it does not hold with sufficient scope).
func (s *Server) assignServiceAccountRoles(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsAssign, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "service account id is required")
		return
	}
	var req assignServiceAccountRolesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if len(req.Roles) == 0 {
		httpError(w, http.StatusBadRequest, "roles is required")
		return
	}
	// Delegation rule: the caller must hold each role being granted, with a
	// scope at least as wide as the one being granted (unscoped here).
	if s.rbacEnabled && s.authz != nil {
		caller := s.principalFromContext(r.Context())
		for _, role := range req.Roles {
			ok, err := s.authz.CanGrantWithPermission(r.Context(), caller, role, 0, authz.PermServiceAccountsAssign)
			if err != nil {
				httpError(w, http.StatusInternalServerError, "authorization: %v", err)
				return
			}
			if !ok {
				httpError(w, http.StatusForbidden, "cannot grant role %q: caller does not hold it with sufficient scope", role)
				return
			}
		}
	}
	result, err := s.serviceAccounts.AssignServiceAccountRoles(r.Context(), id, req.Roles)
	if err != nil {
		httpError(w, serviceAccountStatus(err), "assign service account roles: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionServiceAccountAssignRole,
		TargetKind: audit.TargetServiceAccount,
		TargetID:   result.ID,
		TargetName: result.LoginName,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  serviceAccountActorKind(user),
		NewValue:   auditJSON(map[string]any{"roles": req.Roles}),
	})
	s.publishRoleChange()
	writeJSON(w, http.StatusOK, result)
}

// removeServiceAccountRole removes a role binding from an account (by role
// name). It requires service-accounts.can-remove-roles.
func (s *Server) removeServiceAccountRole(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsRemove, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "service account id is required")
		return
	}
	role := r.PathValue("role")
	if role == "" {
		httpError(w, http.StatusBadRequest, "role is required")
		return
	}
	result, err := s.serviceAccounts.RemoveServiceAccountRole(r.Context(), id, role)
	if err != nil {
		httpError(w, serviceAccountStatus(err), "remove service account role: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionServiceAccountRemoveRole,
		TargetKind: audit.TargetServiceAccount,
		TargetID:   result.ID,
		TargetName: result.LoginName,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  serviceAccountActorKind(user),
		OldValue:   auditJSON(map[string]any{"role": role}),
	})
	s.publishRoleChange()
	writeJSON(w, http.StatusOK, result)
}

// deleteServiceAccount permanently soft-deletes an account (tombstone + zeroed
// key slots). Repeated delete is idempotent. It requires
// service-accounts.can-delete.
func (s *Server) deleteServiceAccount(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsDelete, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "service account id is required")
		return
	}
	result, err := s.serviceAccounts.DeleteServiceAccount(r.Context(), id)
	if err != nil {
		httpError(w, serviceAccountStatus(err), "delete service account: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionServiceAccountDelete,
		TargetKind: audit.TargetServiceAccount,
		TargetID:   result.ID,
		TargetName: result.LoginName,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  serviceAccountActorKind(user),
	})
	// 204 No Content after the tombstone and key clearing commit (the spec
	// requires no body on delete).
	w.WriteHeader(http.StatusNoContent)
}

// resetServiceAccountLockout clears an account's key lockout state. It requires
// service-accounts.can-disable (lockout is a form of disabling key access).
func (s *Server) resetServiceAccountLockout(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceAccount(w) {
		return
	}
	if !s.requirePermission(w, r, authz.PermServiceAccountsDisable, authz.Resource{}) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		httpError(w, http.StatusBadRequest, "service account id is required")
		return
	}
	if err := s.serviceAccounts.ResetServiceAccountLockout(r.Context(), id); err != nil {
		httpError(w, serviceAccountStatus(err), "reset service account lockout: %v", err)
		return
	}
	user := auth.UserFromContext(r.Context())
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionServiceAccountResetLockout,
		TargetKind: audit.TargetServiceAccount,
		TargetID:   id,
		Outcome:    audit.OutcomeSuccess,
		Actor:      user.Subject,
		ActorKind:  serviceAccountActorKind(user),
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

// parseServiceAccountSlot parses a key slot from the path and reports whether
// it is a valid slot (1 or 2).
func parseServiceAccountSlot(value string) (int, bool) {
	slot, err := strconv.Atoi(value)
	if err != nil {
		return 0, false
	}
	if slot != 1 && slot != 2 {
		return 0, false
	}
	return slot, true
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// publish sends an event to the hub, if one is configured.
func (s *Server) publish(ev Event) {
	if s.hub != nil {
		s.hub.Publish(ev)
	}
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	value := r.PathValue("id")
	jobID, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid job id %q", value)
		return 0, false
	}
	return jobID, true
}

func jobStatusFromName(name string) dbpb.JobStatus {
	switch name {
	case "pending":
		return dbpb.JobStatus_JOB_STATUS_PENDING
	case "running":
		return dbpb.JobStatus_JOB_STATUS_RUNNING
	case "succeeded":
		return dbpb.JobStatus_JOB_STATUS_SUCCEEDED
	case "failed":
		return dbpb.JobStatus_JOB_STATUS_FAILED
	case "cancelled":
		return dbpb.JobStatus_JOB_STATUS_CANCELLED
	case "timed_out":
		return dbpb.JobStatus_JOB_STATUS_TIMED_OUT
	case "skipped":
		return dbpb.JobStatus_JOB_STATUS_SKIPPED
	case "awaiting_approval":
		return dbpb.JobStatus_JOB_STATUS_AWAITING_APPROVAL
	default:
		return dbpb.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

// runStatusName maps a run status enum to the short name the UI uses.
func runStatusName(status dbpb.RunStatus) string {
	switch status {
	case dbpb.RunStatus_RUN_STATUS_PENDING:
		return "pending"
	case dbpb.RunStatus_RUN_STATUS_RUNNING:
		return "running"
	case dbpb.RunStatus_RUN_STATUS_SUCCEEDED:
		return "succeeded"
	case dbpb.RunStatus_RUN_STATUS_FAILED:
		return "failed"
	case dbpb.RunStatus_RUN_STATUS_CANCELLED:
		return "cancelled"
	default:
		return "unknown"
	}
}

// nonNil returns a non-nil slice so that empty results marshal to [] rather
// than null in JSON responses.
func nonNil[T any](slice []T) []T {
	if slice == nil {
		return []T{}
	}
	return slice
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func httpError(w http.ResponseWriter, code int, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf(format, args...)})
}
