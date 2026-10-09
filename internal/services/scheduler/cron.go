// Package scheduler — cron trigger loop (F-09).
//
// A pipeline may carry cron triggers: each names a standard five-field cron
// expression (minute, hour, day-of-month, month, day-of-week). The cron loop
// runs on exactly one scheduler replica at a time (leader-gated, like the
// other background loops) and, on each tick, checks every pipeline's cron
// triggers and starts a run of the pipeline (via TriggerRun) at each
// scheduled time.
//
// The loop keeps, per (pipeline, trigger), the parsed schedule and the next
// fire time. When the next fire time is due it calls TriggerRun, which
// atomically claims the trigger's fire window in the database (a run started
// by the same trigger within the window suppresses a duplicate), so a racing
// replica can never fire the same scheduled time twice. A scheduler restart
// naturally avoids re-firing the same time: on restart the loop recomputes
// the next fire as schedule.Next(now), which is the *next* scheduled time
// after now, so a time that already passed is not re-fired. The database's
// dedup is the authoritative guard against the racing-replica case.
package scheduler

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// cronTickInterval is how often the cron loop checks for due triggers. It is a
// variable (not a constant) so tests can shorten it. A one-second cadence is
// fine: a cron expression's resolution is a minute, so firing within a second
// of the scheduled time is accurate enough.
var cronTickInterval = time.Second

// cronDedupWindow is how long a run claimed by a cron trigger suppresses a
// duplicate run of the same pipeline started by the same trigger (F-09). It
// is deliberately shorter than the minimum cron period (one minute): a racing
// replica fires within a few seconds of the scheduled time (well inside the
// window, so it is suppressed), while the next legitimate fire is a full
// period later (outside the window, so it is not suppressed). It is a variable
// so tests can shorten it.
var cronDedupWindow = 30 * time.Second

// cronDB is the subset of the Database client the cron loop uses to list
// pipelines (to read their cron triggers) and to claim a trigger-fired run.
// The concrete dbpb.DatabaseClient satisfies it; tests inject a fake.
type cronDB interface {
	ListPipelines(ctx context.Context, in *dbpb.ListPipelinesRequest, opts ...grpc.CallOption) (*dbpb.ListPipelinesResponse, error)
	TriggerRun(ctx context.Context, in *dbpb.TriggerRunRequest, opts ...grpc.CallOption) (*dbpb.TriggerRunResponse, error)
}

// cronTriggerState is the loop's in-memory state for one cron trigger: the
// parsed schedule and the next time the trigger should fire. It is rebuilt
// from the database on every tick (a pipeline's triggers can change), so it
// is only a fast-path cache — the database's dedup is authoritative.
type cronTriggerState struct {
	schedule cron.Schedule
	next     time.Time
}

// StartCronLoop runs the cron trigger loop in the background until ctx is
// cancelled. It periodically checks every pipeline's cron triggers and starts
// a run of the pipeline at each scheduled time (F-09). It is a no-op when the
// Database client is nil (e.g. in tests that do not exercise the loop).
func (s *Server) StartCronLoop(ctx context.Context) {
	if s.db == nil {
		return
	}
	var mu sync.Mutex
	state := make(map[string]*cronTriggerState) // key: pipelineID|triggerName
	go func() {
		ticker := time.NewTicker(cronTickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.processCronTriggers(ctx, s.db, &mu, state)
			}
		}
	}()
}

// processCronTriggers refreshes the loop's in-memory state from the database
// (the pipelines that carry a cron trigger) and fires any trigger whose next
// fire time is due. db is the loop's dependency (the concrete client in
// production, a fake in tests).
func (s *Server) processCronTriggers(ctx context.Context, db cronDB, mu *sync.Mutex, state map[string]*cronTriggerState) {
	// Ask the database for only the pipelines that carry a cron trigger (F-09),
	// so the loop does not fetch (or parse the triggers of) every pipeline.
	response, err := db.ListPipelines(ctx, &dbpb.ListPipelinesRequest{TriggerType: dbpb.TriggerType_TRIGGER_TYPE_CRON})
	if err != nil {
		s.logger.Warn("scheduler: cron list pipelines", "err", err)
		return
	}
	now := time.Now()
	seen := make(map[string]struct{}, len(state))
	for _, pipeline := range response.GetPipelines() {
		for _, trigger := range pipeline.GetTriggers() {
			if trigger.GetType() != dbpb.TriggerType_TRIGGER_TYPE_CRON {
				continue
			}
			key := cronStateKey(pipeline.GetId(), trigger.GetName())
			seen[key] = struct{}{}
			schedule, err := cron.ParseStandard(trigger.GetCron())
			if err != nil {
				// A malformed cron expression is rejected when the pipeline is
				// saved, so this should not happen; skip it rather than fail the
				// whole tick.
				s.logger.Warn("scheduler: cron trigger has invalid expression",
					"pipeline", pipeline.GetId(), "trigger", trigger.GetName(), "err", err)
				continue
			}
			mu.Lock()
			entry, ok := state[key]
			if !ok {
				// First time seeing this trigger: the next fire is the next
				// scheduled time after now (the loop does not fire for times
				// that passed before it started watching the trigger).
				entry = &cronTriggerState{schedule: schedule, next: schedule.Next(now)}
				state[key] = entry
			} else {
				// The trigger's expression may have changed; re-parse it.
				entry.schedule = schedule
			}
			if entry.next.After(now) {
				mu.Unlock()
				continue
			}
			// The trigger is due: claim the run and advance to the next fire.
			due := entry.next
			entry.next = entry.schedule.Next(due)
			mu.Unlock()
			s.fireCronTrigger(ctx, db, pipeline.GetId(), trigger.GetName(), trigger.GetParams())
		}
	}
	// Drop state for triggers that no longer exist (a pipeline was deleted or
	// its triggers changed), so the map does not grow without bound.
	mu.Lock()
	for key := range state {
		if _, ok := seen[key]; !ok {
			delete(state, key)
		}
	}
	mu.Unlock()
}

// fireCronTrigger claims a cron-triggered run of the pipeline (F-09). The
// database's TriggerRun atomically deduplicates it against a run the same
// trigger already started for the window, so a racing replica cannot fire the
// same scheduled time twice. A failure to claim is logged and left to the next
// tick (or the database's dedup) to reconcile.
func (s *Server) fireCronTrigger(ctx context.Context, db cronDB, pipelineID int64, triggerName string, params map[string]string) {
	resp, err := db.TriggerRun(ctx, &dbpb.TriggerRunRequest{
		PipelineId:  pipelineID,
		TriggerName: triggerName,
		TriggerType: dbpb.TriggerType_TRIGGER_TYPE_CRON,
		Params:      params,
		DedupWindow: durationpb.New(cronDedupWindow),
	})
	if err != nil {
		s.logger.Warn("scheduler: cron trigger run", "pipeline", pipelineID, "trigger", triggerName, "err", err)
		return
	}
	s.logger.Info("scheduler: cron trigger fired", "pipeline", pipelineID, "trigger", triggerName)
	// Record an audit event only when this call created the run (a racing
	// replica that lost the dedup claim is a no-op and is not re-audited).
	if resp.GetCreated() {
		s.recordTriggerRun(ctx, resp.GetRun(), triggerName, dbpb.TriggerType_TRIGGER_TYPE_CRON)
	}
}

// cronStateKey is the in-memory state key for a pipeline's cron trigger.
func cronStateKey(pipelineID int64, triggerName string) string {
	return strconv.FormatInt(pipelineID, 10) + "|" + triggerName
}
