// Package api — event-log tail loop (F-23, high availability).
//
// The tail loop is the API pod's fast path for delivering events to its local
// workers and UI clients. It periodically tails the shared event log (the
// Database service's TailEvents RPC) and fans each new event out to the
// pod's local state:
//
//   - an "assignment" event nudges the local workers in the job's target
//     group (a JobNudge down their WatchJobs streams); the worker then
//     fetches the job via GetJob and claims it.
//   - a "cancel" event delivers a JobCancellation down every local worker's
//     stream (the worker that is running the job interrupts it, F-05).
//   - a "job_status" / "run_status" / "worker" event is published to the
//     local EventHub (the UI's WebSocket fan-out).
//   - a "job_log_updated" event triggers a range read of the log delta from
//     the artifacts store, which is published to the local EventHub as a
//     job_log event (cross-pod near-live logs).
//
// The pod keeps a per-pod cursor (its position in the global log). Events the
// pod itself published (tracked in the published set) are not re-published to
// the local EventHub (the pod already published them directly when it
// observed the state change), avoiding duplicate UI events.
package api

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// tailInterval is how often the tail loop polls the event log. It is a
// variable (not a constant) so tests can shorten it.
var tailInterval = 50 * time.Millisecond

// tailBatchSize bounds how many events the tail loop fetches per poll.
const tailBatchSize = 1000

// logCoalesceWindow is how long the log coalescer waits before flushing a
// pending job_log_updated event (F-23). At most one event per (job, log) is
// emitted per window, keeping the event log lean when a job produces many log
// chunks in a short time.
var logCoalesceWindow = 200 * time.Millisecond

// wasPublished reports whether this pod published the event with the given id
// (and so should not re-publish it to the local EventHub when it sees it in
// the tail).
func (s *GRPCServer) wasPublished(id int64) bool {
	s.publishedMu.Lock()
	defer s.publishedMu.Unlock()
	_, ok := s.published[id]
	return ok
}

// markPublished records that this pod published the event with the given id,
// so the tail loop does not re-publish it to the local EventHub.
func (s *GRPCServer) markPublished(id int64) {
	s.publishedMu.Lock()
	defer s.publishedMu.Unlock()
	s.published[id] = struct{}{}
}

// ---------------------------------------------------------------------------
// Event payload types (the JSON objects stored in the event log's payload
// column). They mirror the data the API needs to handle each event kind.
// ---------------------------------------------------------------------------

type assignmentPayload struct {
	JobID int64 `json:"job_id"`
}

type cancelPayload struct {
	JobID int64 `json:"job_id"`
}

type jobStatusPayload struct {
	JobID       int64  `json:"job_id"`
	Status      string `json:"status"`
	Attempt     int32  `json:"attempt"`
	MaxAttempts int32  `json:"max_attempts"`
}

type runStatusPayload struct {
	RunID  int64  `json:"run_id"`
	Status string `json:"status"`
}

type workerPayload struct {
	Name   string `json:"name"`
	Action string `json:"action"`
}

type logUpdatedPayload struct {
	JobID     int64  `json:"job_id"`
	Log       string `json:"log"`
	StepIndex int32  `json:"step_index"`
	Stream    string `json:"stream"`
	Size      int64  `json:"size"`
}

// ---------------------------------------------------------------------------
// Log coalescer
// ---------------------------------------------------------------------------

// logCoalescer coalesces job_log_updated events: at most one event per
// (job, log) per window. This keeps the event log lean when a job produces
// many log chunks in a short time. The consumer tracks a byte offset, so
// coalescing is safe: no matter how many appends happened in the window, the
// consumer's range read picks up everything that accumulated.
type logCoalescer struct {
	mu      sync.Mutex
	pending map[string]*logUpdate // key: jobID|log
	window  time.Duration
}

type logUpdate struct {
	JobID     int64
	Log       string
	StepIndex int32
	Stream    string
	Size      int64
	created   time.Time
}

func newLogCoalescer(window time.Duration) *logCoalescer {
	return &logCoalescer{
		pending: make(map[string]*logUpdate),
		window:  window,
	}
}

// record notes that a log has grown to size. It updates the pending entry
// (keeping the latest size) but does not flush: the tail loop's periodic
// flushDue call emits an entry once its window has elapsed.
func (c *logCoalescer) record(jobID int64, log string, stepIndex int32, stream string, size int64) {
	key := logKey(jobID, log)
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.pending[key]
	if !ok {
		c.pending[key] = &logUpdate{JobID: jobID, Log: log, StepIndex: stepIndex, Stream: stream, Size: size, created: time.Now()}
		return
	}
	entry.Size = size
	entry.StepIndex = stepIndex
	entry.Stream = stream
}

// flushDue returns and clears the pending log updates whose window has
// elapsed (at most one event per (job, log) per window). Entries still
// inside their window are kept, so the window is preserved even when flushDue
// is called on a faster cadence than the window itself.
func (c *logCoalescer) flushDue(now time.Time) []*logUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*logUpdate
	for key, u := range c.pending {
		if now.Sub(u.created) >= c.window {
			out = append(out, u)
			delete(c.pending, key)
		}
	}
	return out
}

// flushAll returns and clears every pending log update regardless of its
// window. It is used at shutdown so no coalesced update is lost.
func (c *logCoalescer) flushAll() []*logUpdate {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return nil
	}
	out := make([]*logUpdate, 0, len(c.pending))
	for _, u := range c.pending {
		out = append(out, u)
	}
	c.pending = make(map[string]*logUpdate)
	return out
}

func logKey(jobID int64, log string) string {
	return itoa(jobID) + "|" + log
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// ---------------------------------------------------------------------------
// Tail loop
// ---------------------------------------------------------------------------

// StartEventLogTail runs the event-log tail loop in the background until ctx
// is cancelled. On each tick it (a) tails the shared event log and fans new
// events out to the pod's local workers and UI clients, and (b) flushes any
// due coalesced job_log_updated events to the shared event log (so other pods
// can range-read the log deltas this pod wrote). It is a no-op when the
// Database client is nil (e.g. in tests that do not exercise the tail loop).
func (s *GRPCServer) StartEventLogTail(ctx context.Context) {
	if s.db == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(tailInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Flush any pending coalesced log updates so none are lost.
				s.flushLogCoalescer(ctx, s.coalescer.flushAll())
				return
			case <-ticker.C:
				s.flushLogCoalescer(ctx, s.coalescer.flushDue(time.Now()))
				s.tailEvents(ctx)
			}
		}
	}()
}

// flushLogCoalescer publishes a job_log_updated pointer event to the shared
// event log for each coalesced log update (F-23). The event carries only the
// job id, log name, step index, stream, and the log's total size — never the
// log bytes. Other API pods tail the event and range-read the delta from the
// shared artifacts store.
func (s *GRPCServer) flushLogCoalescer(ctx context.Context, updates []*logUpdate) {
	if s.db == nil || len(updates) == 0 {
		return
	}
	for _, u := range updates {
		s.publishLogUpdated(ctx, u.JobID, u.Log, u.StepIndex, u.Stream, u.Size)
	}
}

// tailEvents fetches new events from the shared event log (since the pod's
// cursor) and fans them out to the pod's local workers and UI clients.
func (s *GRPCServer) tailEvents(ctx context.Context) {
	resp, err := s.db.TailEvents(ctx, &dbpb.TailEventsRequest{
		Cursor: s.tailCursor.Load(),
		Limit:  tailBatchSize,
	})
	if err != nil {
		s.logger.Warn("api: tail events", "err", err)
		return
	}
	for _, event := range resp.GetEvents() {
		s.handleEvent(ctx, event)
	}
	if len(resp.GetEvents()) > 0 {
		s.tailCursor.Store(resp.GetCursor())
	}
}

// handleEvent fans a single event out to the pod's local state.
func (s *GRPCServer) handleEvent(ctx context.Context, event *dbpb.Event) {
	// Skip events this pod published itself (it already published them
	// directly to the local EventHub when it observed the state change).
	if s.wasPublished(event.GetId()) {
		return
	}
	switch event.GetName() {
	case "assignment":
		var p assignmentPayload
		if err := json.Unmarshal([]byte(event.GetPayload()), &p); err != nil {
			s.logger.Warn("api: bad assignment payload", "err", err)
			return
		}
		s.nudgeWorkers(ctx, p.JobID, event.GetWorkerGroup())
	case "cancel":
		var p cancelPayload
		if err := json.Unmarshal([]byte(event.GetPayload()), &p); err != nil {
			s.logger.Warn("api: bad cancel payload", "err", err)
			return
		}
		s.broadcastCancellation(p.JobID)
	case "job_status":
		var p jobStatusPayload
		if err := json.Unmarshal([]byte(event.GetPayload()), &p); err != nil {
			s.logger.Warn("api: bad job_status payload", "err", err)
			return
		}
		s.publish(Event{
			Type:        EventJobStatus,
			JobID:       p.JobID,
			Status:      p.Status,
			Attempt:     p.Attempt,
			MaxAttempts: p.MaxAttempts,
		})
	case "run_status":
		var p runStatusPayload
		if err := json.Unmarshal([]byte(event.GetPayload()), &p); err != nil {
			s.logger.Warn("api: bad run_status payload", "err", err)
			return
		}
		s.publish(Event{
			Type:   EventRunStatus,
			RunID:  p.RunID,
			Status: p.Status,
		})
	case "worker":
		var p workerPayload
		if err := json.Unmarshal([]byte(event.GetPayload()), &p); err != nil {
			s.logger.Warn("api: bad worker payload", "err", err)
			return
		}
		s.publish(Event{
			Type:   EventWorker,
			Worker: p.Name,
			Group:  event.GetWorkerGroup(),
			Action: p.Action,
		})
	case "job_log_updated":
		var p logUpdatedPayload
		if err := json.Unmarshal([]byte(event.GetPayload()), &p); err != nil {
			s.logger.Warn("api: bad job_log_updated payload", "err", err)
			return
		}
		s.handleLogUpdated(ctx, &p)
	}
}

// nudgeWorkers delivers a JobNudge to every local worker in the group. The
// worker that receives it fetches the job via GetJob and claims it.
func (s *GRPCServer) nudgeWorkers(ctx context.Context, jobID int64, group string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, worker := range s.live {
		if worker.group != group {
			continue
		}
		channel, ok := s.watchers[name]
		if !ok {
			continue
		}
		nudge := &apipb.WatchMessage{
			Message: &apipb.WatchMessage_Nudge{
				Nudge: &apipb.JobNudge{JobId: jobID},
			},
		}
		select {
		case channel <- nudge:
		default:
			s.logger.Warn("api: nudge queue full", "worker", name, "job", jobID)
		}
	}
}

// broadcastCancellation delivers a JobCancellation to every local worker's
// stream (the worker that is running the job interrupts it, F-05).
func (s *GRPCServer) broadcastCancellation(jobID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, channel := range s.watchers {
		select {
		case channel <- &apipb.WatchMessage{
			Message: &apipb.WatchMessage_Cancellation{
				Cancellation: &apipb.JobCancellation{JobId: jobID},
			},
		}:
		default:
			s.logger.Warn("api: cancel queue full", "worker", name, "job", jobID)
		}
	}
}

// handleLogUpdated range-reads the log delta from the artifacts store and
// publishes it to the local EventHub as a job_log event (cross-pod near-live
// logs). It is skipped entirely when this pod has no UI subscribers (the log
// bytes are durable in the artifacts store, so a UI client that connects later
// resynchronizes from the persisted log). If the artifacts service is
// unreachable the event is dropped (the UI resyncs from the persisted log on
// reconnect).
func (s *GRPCServer) handleLogUpdated(ctx context.Context, p *logUpdatedPayload) {
	if s.artifacts == nil {
		return
	}
	if s.hub == nil || !s.hub.HasSubscribers() {
		// No UI client on this pod is watching; skip the artifacts range read.
		return
	}
	jobIDStr := itoa(p.JobID)
	cursor := s.logCursor(jobIDStr, p.Log)
	if p.Size <= cursor {
		return // no new data
	}
	stream, err := s.artifacts.DownloadLog(ctx, &artifactspb.DownloadLogRequest{
		Namespace: jobIDStr,
		Name:      p.Log,
		Offset:    cursor,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			// The log file is gone (job cleaned up); drop the tail.
			return
		}
		s.logger.Warn("api: range read log", "job", p.JobID, "log", p.Log, "err", err)
		return
	}
	var data []byte
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}
		data = append(data, chunk.GetData()...)
	}
	if len(data) == 0 {
		return
	}
	s.logAdvance(jobIDStr, p.Log, p.Size)
	s.publish(Event{
		Type:      EventJobLog,
		JobID:     p.JobID,
		StepIndex: p.StepIndex,
		Stream:    p.Stream,
		Data:      string(data),
	})
}

// logCursor returns the byte offset the pod has already read for a (job, log)
// pair.
func (s *GRPCServer) logCursor(jobID, log string) int64 {
	key := jobID + "|" + log
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.logCursors[key]; ok {
		return c
	}
	return 0
}

// logAdvance records the new byte offset for a (job, log) pair.
func (s *GRPCServer) logAdvance(jobID, log string, size int64) {
	key := jobID + "|" + log
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logCursors[key] = size
}
