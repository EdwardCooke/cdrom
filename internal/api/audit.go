// Package api — audit log wiring (F-15).
//
// The API records each audited action (a pipeline/run/job/secret/role/user
// mutation, an approval decision, a login, a worker lifecycle change) to the
// audit Recorder, which fans it out to the local audit log file (or stdout)
// and to the shared audit log in the database (via the Database service's
// AppendAuditEvent RPC). The database's audit log is the queryable,
// cross-replica store the UI reads (GET /api/audit); old entries are pruned by
// age on a schedule (see the API's StartPruning, driven by the audit config).
//
// Secret values are never recorded: the API builds the old/new value documents
// from the resource's non-secret fields and redacts any secret plaintext
// before recording (see internal/audit's Redactor).
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"

	"cdrom/internal/audit"
	"cdrom/internal/auth"
	"cdrom/internal/config"
)

// auditSink is the API's audit.Sink: it appends an audit event to the shared
// audit log in the database (via the Database service's AppendAuditEvent RPC).
type auditSink struct {
	db dbpb.DatabaseClient
}

func (s *auditSink) Append(ctx context.Context, e audit.Event) error {
	_, err := s.db.AppendAuditEvent(ctx, &dbpb.AppendAuditEventRequest{
		Actor:      e.Actor,
		ActorKind:  e.ActorKind,
		Action:     e.Action,
		TargetKind: e.TargetKind,
		TargetId:   e.TargetID,
		TargetName: e.TargetName,
		PipelineId: e.PipelineID,
		RunId:      e.RunID,
		Outcome:    e.Outcome,
		SourceIp:   e.SourceIP,
		OldValue:   e.OldValue,
		NewValue:   e.NewValue,
		Details:    e.Details,
	})
	return err
}

// auditPruner is the API's audit.Pruner: it prunes the database's audit log of
// entries older than the given instant (via the Database service's
// PruneAuditEvents RPC).
type auditPruner struct {
	db dbpb.DatabaseClient
}

func (p *auditPruner) Prune(ctx context.Context, before time.Time) (int64, error) {
	resp, err := p.db.PruneAuditEvents(ctx, &dbpb.PruneAuditEventsRequest{
		CreatedBefore: timestamppb.New(before),
	})
	if err != nil {
		return 0, err
	}
	return resp.GetPruned(), nil
}

// NewAuditRecorder builds the API's audit Recorder from the audit config: a
// local audit Logger (a rotating file, or stdout when cfg.File is "-") plus a
// sink and pruner backed by the Database service. db may be nil (tests), in
// which case the events go only to the local log.
func NewAuditRecorder(cfg config.AuditConfig, db dbpb.DatabaseClient, logger *slog.Logger) (*audit.Recorder, error) {
	auditCfg := audit.Config{
		File:     cfg.EffectiveFile(),
		Format:   cfg.EffectiveFormat(),
		Rotation: auditRotation(cfg.EffectiveRotation()),
	}
	local, err := audit.NewLogger(auditCfg)
	if err != nil {
		return nil, err
	}
	var sink audit.Sink
	var pruner audit.Pruner
	if db != nil {
		sink = &auditSink{db: db}
		pruner = &auditPruner{db: db}
	}
	return audit.NewRecorder(local, sink, pruner, logger), nil
}

// auditRotation maps the config's rotation string to the audit package's
// Rotation type.
func auditRotation(s string) audit.Rotation {
	if s == "hourly" {
		return audit.RotationHourly
	}
	return audit.RotationDaily
}

// SetAudit attaches the audit Recorder to the HTTP server. When set, the
// server records an audit event for each audited action it handles. When left
// nil (tests, or the audit log is not configured) no audit events are recorded
// on the HTTP surface.
func (s *Server) SetAudit(r *audit.Recorder) {
	s.audit = r
}

// auditActor returns the actor identity (and kind) for an HTTP request: the
// authenticated user's subject when RBAC is enforced, or a synthetic
// "anonymous" principal when authentication is disabled (a local run acts as a
// synthetic admin, so its actions are attributed to "anonymous").
func (s *Server) auditActor(ctx context.Context) (string, string) {
	if s.rbacEnabled {
		return auth.UserFromContext(ctx).Subject, audit.ActorKindUser
	}
	return "anonymous", audit.ActorKindUser
}

// recordAudit records an audit event for an HTTP request, filling in the actor
// (from the request's context) and the source IP (the request's remote
// address) when the event does not already carry them. It is a no-op when the
// audit Recorder is not attached.
func (s *Server) recordAudit(r *http.Request, e audit.Event) {
	if s.audit == nil {
		return
	}
	if e.Actor == "" {
		e.Actor, e.ActorKind = s.auditActor(r.Context())
	}
	if e.SourceIP == "" {
		e.SourceIP = clientIP(r)
	}
	s.audit.Record(e)
}

// clientIP extracts the client's IP address from a request's remote address
// (stripping the port). It returns the raw address when it has no host:port
// form.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// SetAudit attaches the audit Recorder to the gRPC server. When set, the gRPC
// surface records an audit event for each audited action it handles (worker
// lifecycle, job status reports, token exchanges). When left nil no audit
// events are recorded on the gRPC surface.
func (s *GRPCServer) SetAudit(r *audit.Recorder) {
	s.audit = r
}

// recordAuditGRPC records an audit event for a gRPC action. actor and
// actorKind are supplied by the caller (the gRPC surface has no HTTP request
// context to derive them from: a worker action's actor is the worker's name, a
// job action's actor is the job's identity). sourceIP is the peer's address
// (empty for a local or non-network caller).
func (s *GRPCServer) recordAuditGRPC(e audit.Event, actor, actorKind, sourceIP string) {
	if s.audit == nil {
		return
	}
	if e.Actor == "" {
		e.Actor = actor
	}
	if e.ActorKind == "" {
		e.ActorKind = actorKind
	}
	if e.SourceIP == "" {
		e.SourceIP = sourceIP
	}
	s.audit.Record(e)
}

// auditPeerAddr extracts the peer's IP address from a gRPC context's peer
// address (e.g. "127.0.0.1:54321" -> "127.0.0.1"). It returns "" when the
// peer address is absent or has no host part.
func auditPeerAddr(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String()
	}
	return host
}

// auditJSON marshals v to a compact JSON document (for an audit event's
// old/new value or details). It returns "" when v is nil or cannot be
// marshaled.
func auditJSON(v any) string {
	if v == nil {
		return ""
	}
	data, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(data)
}

// auditRedact runs a string through the given redactor (a no-op when the
// redactor is nil).
func auditRedact(r *audit.Redactor, s string) string {
	if r == nil {
		return s
	}
	return r.Redact(s)
}

// failureModeName maps a pipeline's failure mode enum to the short name the
// UI uses (F-15: it is part of a pipeline's audited value document).
func failureModeName(mode dbpb.FailureMode) string {
	switch mode {
	case dbpb.FailureMode_FAILURE_MODE_ALL:
		return "all"
	case dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT:
		return "best_effort"
	case dbpb.FailureMode_FAILURE_MODE_ANY:
		return "any"
	default:
		return "unspecified"
	}
}

// auditSecretsValue renders a secret list as a value document that names each
// secret but never its value (F-15: a secret's value is never stored or
// displayed in the audit log). It returns "" when there are no secrets.
func auditSecretsValue(secrets []*dbpb.Secret) string {
	if len(secrets) == 0 {
		return ""
	}
	names := make([]string, 0, len(secrets))
	for _, s := range secrets {
		names = append(names, s.GetName())
	}
	return auditJSON(map[string]any{"secrets": names})
}

// auditRoleValue renders a role's definition as a value document for an audit
// event's old/new value: its description, permissions, and included roles. It
// returns "" when the role is nil (e.g. it could not be fetched before the
// action).
func auditRoleValue(role *dbpb.Role) string {
	if role == nil {
		return ""
	}
	return auditJSON(map[string]any{
		"description": role.GetDescription(),
		"permissions": role.GetPermissions(),
		"includes":    role.GetIncludes(),
		"built_in":    role.GetBuiltIn(),
	})
}

// auditPipelineValue renders a pipeline's non-secret definition as a value
// document for an audit event's old/new value: its name, description, failure
// mode, version, job keys, trigger names, and parameter names. Secret values
// are excluded (a secret is represented by name only, via auditSecretsValue).
func auditPipelineValue(p *dbpb.Pipeline) string {
	if p == nil {
		return ""
	}
	jobKeys := make([]string, 0, len(p.GetJobs()))
	for _, j := range p.GetJobs() {
		jobKeys = append(jobKeys, j.GetKey())
	}
	triggerNames := make([]string, 0, len(p.GetTriggers()))
	for _, t := range p.GetTriggers() {
		triggerNames = append(triggerNames, t.GetName())
	}
	paramNames := make([]string, 0, len(p.GetParams()))
	for _, prm := range p.GetParams() {
		paramNames = append(paramNames, prm.GetName())
	}
	doc := map[string]any{
		"name":         p.GetName(),
		"description":  p.GetDescription(),
		"failure_mode": failureModeName(p.GetFailureMode()),
		"version":      p.GetVersion(),
		"jobs":         jobKeys,
		"triggers":     triggerNames,
		"params":       paramNames,
	}
	if secrets := auditSecretsValue(p.GetSecrets()); secrets != "" {
		doc["secrets"] = secrets
	}
	return auditJSON(doc)
}
