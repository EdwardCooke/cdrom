// Package scheduler — event trigger loop (F-09).
//
// A pipeline may carry event triggers: each names another pipeline (by name)
// and a run status, and says "when a run of that pipeline reaches that status,
// start a run of this pipeline" (chaining). The event loop runs on exactly one
// scheduler replica at a time (leader-gated, like the other background loops)
// and, on each tick, scans recently-finished runs and, for each run whose
// pipeline and status match an event trigger, starts a run of the triggered
// pipeline (via TriggerRun).
//
// The loop keeps a lookback window (how far back it scans for finished runs)
// and an in-memory set of source run ids it has already fired, so a run that
// finished a moment ago is fired once. The authoritative dedup is in the
// database: TriggerRun records the source run's id on the run it creates, and
// the same source run can never start the same downstream run twice. So a
// scheduler restart (which loses the in-memory set) re-scans the lookback
// window and re-fires, but the database makes each re-fire a no-op.
package scheduler

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// eventTriggerInterval is how often the event loop scans for finished runs
// that match an event trigger. It is a variable (not a constant) so tests can
// shorten it.
var eventTriggerInterval = 5 * time.Second

// eventTriggerLookback is how far back the event loop scans for finished runs
// (F-09). It is deliberately generous so a scheduler that was down (or just
// restarted) still catches runs that finished while it was away, within the
// window. The database's dedup (by source run id) makes re-firing a no-op, so
// a wide window is safe. It is a variable so tests can shorten it.
var eventTriggerLookback = time.Hour

// eventProcessedCap bounds the in-memory set of source run ids the event loop
// has already fired. When it is exceeded the set is cleared; the database's
// dedup makes re-firing a cleared entry a no-op, so clearing is safe.
const eventProcessedCap = 10000

// eventTriggerDB is the subset of the Database client the event loop uses to
// list pipelines (to read their event triggers), list recently-finished runs,
// and claim a trigger-fired run. The concrete dbpb.DatabaseClient satisfies
// it; tests inject a fake.
type eventTriggerDB interface {
	ListPipelines(ctx context.Context, in *dbpb.ListPipelinesRequest, opts ...grpc.CallOption) (*dbpb.ListPipelinesResponse, error)
	ListRuns(ctx context.Context, in *dbpb.ListRunsRequest, opts ...grpc.CallOption) (*dbpb.ListRunsResponse, error)
	TriggerRun(ctx context.Context, in *dbpb.TriggerRunRequest, opts ...grpc.CallOption) (*dbpb.TriggerRunResponse, error)
}

// eventRule is one event trigger, flattened for matching: the name of the
// pipeline it watches (event_pipeline), the run status that fires it, the id
// of the pipeline to run when it fires, the trigger's name, and its static
// params.
type eventRule struct {
	watchedName      string
	status           dbpb.RunStatus
	targetPipelineID int64
	triggerName      string
	params           map[string]string
}

// StartEventTriggerLoop runs the event trigger loop in the background until
// ctx is cancelled. It periodically scans recently-finished runs and starts a
// run of a pipeline when a run of another pipeline reaches the status an event
// trigger watches (F-09). It is a no-op when the Database client is nil (e.g.
// in tests that do not exercise the loop).
func (s *Server) StartEventTriggerLoop(ctx context.Context) {
	if s.db == nil {
		return
	}
	processed := make(map[int64]struct{}) // source run id already fired
	go func() {
		ticker := time.NewTicker(eventTriggerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.processEventTriggers(ctx, s.db, processed)
			}
		}
	}()
}

// processEventTriggers scans recently-finished runs and, for each run whose
// pipeline and status match an event trigger, starts a run of the triggered
// pipeline (F-09). db is the loop's dependency (the concrete client in
// production, a fake in tests); processed is the in-memory set of source run
// ids already fired (an optimization — the database's dedup is authoritative).
func (s *Server) processEventTriggers(ctx context.Context, db eventTriggerDB, processed map[int64]struct{}) {
	// Ask the database for only the pipelines that carry an event trigger
	// (F-09), so the loop does not fetch (or parse the triggers of) every
	// pipeline. The pipelines being *watched* need not carry a trigger, so a
	// finished run is matched to a rule by the run's own pipeline name (the
	// database preloads it onto the run) rather than by a pipeline id lookup.
	pipelines, err := db.ListPipelines(ctx, &dbpb.ListPipelinesRequest{TriggerType: dbpb.TriggerType_TRIGGER_TYPE_EVENT})
	if err != nil {
		s.logger.Warn("scheduler: event trigger list pipelines", "err", err)
		return
	}
	var rules []eventRule
	for _, pipeline := range pipelines.GetPipelines() {
		for _, trigger := range pipeline.GetTriggers() {
			if trigger.GetType() != dbpb.TriggerType_TRIGGER_TYPE_EVENT {
				continue
			}
			rules = append(rules, eventRule{
				watchedName:      trigger.GetEventPipeline(),
				status:           trigger.GetEventStatus(),
				targetPipelineID: pipeline.GetId(),
				triggerName:      trigger.GetName(),
				params:           trigger.GetParams(),
			})
		}
	}
	if len(rules) == 0 {
		return
	}
	// Scan recently-finished runs (all statuses; a run's status is matched
	// against the rules below).
	since := timestamppb.New(time.Now().Add(-eventTriggerLookback))
	runs, err := db.ListRuns(ctx, &dbpb.ListRunsRequest{FinishedAfter: since})
	if err != nil {
		s.logger.Warn("scheduler: event trigger list runs", "err", err)
		return
	}
	for _, run := range runs.GetRuns() {
		// The name of the pipeline this run executed (preloaded by the
		// database); a run whose pipeline name is unknown cannot be matched.
		sourcePipeline := run.GetPipelineName()
		if sourcePipeline == "" {
			continue
		}
		for _, rule := range rules {
			// The rule fires when a run of the watched pipeline (by name)
			// reaches the watched status.
			if rule.watchedName != sourcePipeline || rule.status != run.GetStatus() {
				continue
			}
			if _, done := processed[run.GetId()]; done {
				continue
			}
			s.fireEventTrigger(ctx, db, rule, run.GetId(), sourcePipeline)
			processed[run.GetId()] = struct{}{}
			if len(processed) > eventProcessedCap {
				// Bound the set; the database's dedup makes re-firing a
				// cleared entry a no-op.
				for k := range processed {
					delete(processed, k)
				}
			}
		}
	}
}

// fireEventTrigger starts a run of the triggered pipeline from an event
// trigger (F-09). The database's TriggerRun records the source run's id on the
// run it creates and deduplicates against it, so the same source run can never
// start the same downstream run twice. A failure to claim is logged and left
// to the next tick to reconcile.
func (s *Server) fireEventTrigger(ctx context.Context, db eventTriggerDB, rule eventRule, sourceRunID int64, sourcePipeline string) {
	params := make(map[string]string, len(rule.params)+2)
	for k, v := range rule.params {
		params[k] = v
	}
	params[models.ParamKeySourceRun] = strconv.FormatInt(sourceRunID, 10)
	params[models.ParamKeySourcePipeline] = sourcePipeline
	resp, err := db.TriggerRun(ctx, &dbpb.TriggerRunRequest{
		PipelineId:  rule.targetPipelineID,
		TriggerName: rule.triggerName,
		TriggerType: dbpb.TriggerType_TRIGGER_TYPE_EVENT,
		Params:      params,
	})
	if err != nil {
		s.logger.Warn("scheduler: event trigger run", "pipeline", rule.targetPipelineID, "trigger", rule.triggerName, "source_run", sourceRunID, "err", err)
		return
	}
	s.logger.Info("scheduler: event trigger fired", "pipeline", rule.targetPipelineID, "trigger", rule.triggerName, "source_run", sourceRunID, "source_pipeline", sourcePipeline)
	// Record an audit event only when this call created the run (a re-fire
	// that the database's dedup suppresses is a no-op and is not re-audited).
	if resp.GetCreated() {
		s.recordTriggerRun(ctx, resp.GetRun(), rule.triggerName, dbpb.TriggerType_TRIGGER_TYPE_EVENT)
	}
}
