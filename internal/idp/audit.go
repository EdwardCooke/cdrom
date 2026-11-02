// The IdP's audit recorder (F-15, adopted by F-28.2 Slice 3).
//
// The SCIM caller-auth guard records REJECTED presentations as audit events
// (alongside a warning log line). Following the house pattern each event is
// written TWICE: to a local rotating audit file (or stdout) and to the
// SHARED audit log in the database (the queryable copy the UI reads via
// GET /api/audit). The pruner is deliberately NIL: the retention schedule is
// OWNED by the API's recorder (double-pruning from two components would race
// the same table); the IdP only appends.
package idp

import (
	"context"
	"log/slog"

	"cdrom/internal/audit"
	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// dbAuditSink adapts the Database service's AppendAuditEvent RPC to
// audit.Sink (the sibling of the API's internal/api auditSink).
type dbAuditSink struct {
	db dbpb.DatabaseClient
}

func (s *dbAuditSink) Append(ctx context.Context, e audit.Event) error {
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

// NewAuditRecorder builds the IdP's audit Recorder from the shared audit
// config: a local rotating file (or stdout when cfg's file is "-") plus the
// database sink. db may be nil (tests) — events then go to the local file
// only.
func NewAuditRecorder(cfg config.AuditConfig, db dbpb.DatabaseClient, logger *slog.Logger) (*audit.Recorder, error) {
	local, err := audit.NewLogger(audit.Config{
		File:     cfg.EffectiveFile(),
		Format:   cfg.EffectiveFormat(),
		Rotation: idpAuditRotation(cfg.EffectiveRotation()),
	})
	if err != nil {
		return nil, err
	}
	var sink audit.Sink
	if db != nil {
		sink = &dbAuditSink{db: db}
	}
	return audit.NewRecorder(local, sink, nil, logger), nil
}

// idpAuditRotation maps the config's rotation string onto the audit
// package's enum (mirrors the API's mapper).
func idpAuditRotation(s string) audit.Rotation {
	if s == "hourly" {
		return audit.RotationHourly
	}
	return audit.RotationDaily
}
