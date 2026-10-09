// Package scheduler — audit recording for trigger-fired runs (F-15).
//
// The scheduler's cron and event loops start runs directly via the Database
// service's TriggerRun RPC (they do not go through the API, per the topology
// rule: the scheduler talks only to the Database service). To keep the audit
// log complete, each trigger-fired run is recorded as an audit event with a
// synthetic "system:scheduler" actor, appended straight to the shared audit
// log in the database (via AppendAuditEvent). Recording is best-effort: a
// failure to append is logged and dropped, so an audit-log outage can never
// take down a trigger loop.
package scheduler

import (
	"context"
	"encoding/json"
	"strconv"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// auditActorScheduler is the synthetic actor identity recorded for a run
// started by the scheduler's cron or event loop (F-15).
const auditActorScheduler = "system:scheduler"

// recordTriggerRun appends an audit event for a run started by a trigger loop
// (F-15). run is the created run; triggerName and triggerType name the
// trigger that fired. It is a no-op when the Database client is nil (tests) or
// the run is nil. A failure to append is logged and dropped (best-effort).
func (s *Server) recordTriggerRun(ctx context.Context, run *dbpb.PipelineRun, triggerName string, triggerType dbpb.TriggerType) {
	if s.db == nil || run == nil {
		return
	}
	triggerTypeName := "unknown"
	switch triggerType {
	case dbpb.TriggerType_TRIGGER_TYPE_CRON:
		triggerTypeName = "cron"
	case dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK:
		triggerTypeName = "webhook"
	case dbpb.TriggerType_TRIGGER_TYPE_EVENT:
		triggerTypeName = "event"
	}
	details := map[string]any{
		"trigger":      triggerName,
		"trigger_type": triggerTypeName,
	}
	if run.GetSourceRunId() > 0 {
		details["source_run"] = run.GetSourceRunId()
	}
	event := &dbpb.AppendAuditEventRequest{
		Actor:      auditActorScheduler,
		ActorKind:  "system",
		Action:     "run.trigger",
		TargetKind: "run",
		TargetId:   strconv.FormatInt(run.GetId(), 10),
		TargetName: run.GetPipelineName(),
		PipelineId: run.GetPipelineId(),
		RunId:      run.GetId(),
		Outcome:    "success",
		Details:    marshalAuditDetails(details),
	}
	if _, err := s.db.AppendAuditEvent(ctx, event); err != nil {
		s.logger.Warn("scheduler: audit trigger run", "run", run.GetId(), "pipeline", run.GetPipelineId(), "err", err)
	}
}

// marshalAuditDetails renders a details map as a compact JSON document. It
// returns "" when the value is nil or cannot be marshaled.
func marshalAuditDetails(v any) string {
	if v == nil {
		return ""
	}
	data, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(data)
}
