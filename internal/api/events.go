package api

import (
	"sync"
	"time"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// Event is a single change notification broadcast to WebSocket subscribers.
type Event struct {
	Type  string `json:"type"`
	JobID int64  `json:"job_id,omitempty"`
	// RunID is the pipeline run a run_status event belongs to (F-07); 0 means
	// the field is unset.
	RunID  int64  `json:"run_id,omitempty"`
	Status string `json:"status,omitempty"`
	Worker string `json:"worker,omitempty"`
	Group  string `json:"group,omitempty"`
	Action string `json:"action,omitempty"`
	// StepName is the step's name a job_log event belongs to.
	StepName string `json:"step_name,omitempty"`
	// Stream is the output stream a job_log event came from ("stdout" or
	// "stderr").
	Stream string `json:"stream,omitempty"`
	// Data is the log text a job_log event carries.
	Data string `json:"data,omitempty"`
	// Attempt is the 1-based attempt number of a job_status event (F-04); 0
	// means the field is unset.
	Attempt int32 `json:"attempt,omitempty"`
	// MaxAttempts is the job's retry budget (0 means the job is never
	// retried), so the UI can render "attempt N of M" on a job_status event.
	MaxAttempts int32 `json:"max_attempts,omitempty"`
	// Queue is the pending-queue state a queue event carries (F-16): the
	// pending jobs (with the resource(s) blocking each), the per-group and
	// global load, and the per-key load. It is set only on queue events.
	Queue *dbpb.QueueStatus `json:"queue,omitempty"`
	// ConcurrencyLimits is the set of concurrency limit rules a
	// concurrency_limits event carries (F-16). It is set only on
	// concurrency_limits events (published when a limit rule is created,
	// updated, or deleted).
	ConcurrencyLimits []*dbpb.ConcurrencyLimit `json:"concurrency_limits,omitempty"`
	// Environments is the set of environments an environment event carries
	// (F-17). It is set only on environment events (published when an
	// environment is created, updated, or deleted), so the UI's environments
	// view stays in sync across replicas.
	Environments []*dbpb.Environment `json:"environments,omitempty"`
	// SourceCredentials is the set of source credentials a source_credential
	// event carries (F-21.3). It is set only on source_credential events
	// (published when a credential is created, updated, or deleted), so the
	// UI's credentials view stays in sync across replicas. The secret material
	// is redacted before the event is published.
	SourceCredentials []*dbpb.SourceCredential `json:"source_credentials,omitempty"`
	At                int64                    `json:"at"`
}

// Event type constants.
const (
	EventJobStatus = "job_status"
	EventWorker    = "worker"
	EventJobLog    = "job_log"
	// EventRunStatus is a pipeline run status change (F-07); the event carries
	// the run's id and its derived status.
	EventRunStatus = "run_status"
	// EventQueue is a pending-queue refresh (F-16); the event carries the
	// current queue status (pending jobs, per-group/global load, per-key
	// load). It is published whenever a job's status changes, since that is
	// what frees or takes a concurrency slot.
	EventQueue = "queue"
	// EventConcurrencyLimits is a concurrency-limit change (F-16); the event
	// carries the current set of concurrency limit rules. It is published when
	// a limit rule is created, updated, or deleted, so the UI's limits view
	// stays in sync across replicas.
	EventConcurrencyLimits = "concurrency_limits"
	// EventEnvironment is an environment change (F-17); the event carries the
	// current set of environments. It is published when an environment is
	// created, updated, or deleted, so the UI's environments view stays in
	// sync across replicas.
	EventEnvironment = "environment"
	// EventSourceCredential is a source-credential change (F-21.3); the event
	// carries the current set of source credentials (secret material redacted).
	// It is published when a credential is created, updated, or deleted, so
	// the UI's credentials view stays in sync across replicas.
	EventSourceCredential = "source_credential"
)

// Job log stream names (the Event.Stream values for job_log events).
const (
	JobLogStreamStdout = "stdout"
	JobLogStreamStderr = "stderr"
)

// Worker action constants.
const (
	WorkerRegistered   = "registered"
	WorkerDeregistered = "deregistered"
	WorkerWatching     = "watching"
)

// EventHub is a fan-out of change events to WebSocket subscribers. Each
// subscriber gets a buffered channel; events that cannot be delivered (full
// buffer) are dropped, and the UI resynchronizes from the snapshot it receives
// on (re)connect.
type EventHub struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

// NewEventHub creates an empty hub.
func NewEventHub() *EventHub {
	return &EventHub{subs: make(map[chan Event]struct{})}
}

// Subscribe registers a new subscriber and returns its channel plus a cancel
// func that unregisters and closes it.
func (h *EventHub) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 256)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	cancel := func() {
		h.mu.Lock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
		h.mu.Unlock()
	}
	return ch, cancel
}

// Publish sends ev to every subscriber, dropping it for any subscriber whose
// buffer is full.
func (h *EventHub) Publish(ev Event) {
	if ev.At == 0 {
		ev.At = time.Now().UnixMilli()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- ev:
		default:
			// Subscriber is slow; drop the event. The UI resyncs from the
			// snapshot on reconnect.
		}
	}
}

// HasSubscribers reports whether any WebSocket subscriber is connected to the
// hub. The event-log tail loop uses it to skip the artifacts range read for a
// job_log_updated event when no UI client on this pod is watching (F-23):
// the log bytes are durable in the artifacts store, so a UI client that
// connects later resynchronizes from the persisted log.
func (h *EventHub) HasSubscribers() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs) > 0
}

// jobStatusName maps a job status enum to the short name the UI uses.
func jobStatusName(status dbpb.JobStatus) string {
	switch status {
	case dbpb.JobStatus_JOB_STATUS_PENDING:
		return "pending"
	case dbpb.JobStatus_JOB_STATUS_RUNNING:
		return "running"
	case dbpb.JobStatus_JOB_STATUS_SUCCEEDED:
		return "succeeded"
	case dbpb.JobStatus_JOB_STATUS_FAILED:
		return "failed"
	case dbpb.JobStatus_JOB_STATUS_CANCELLED:
		return "cancelled"
	case dbpb.JobStatus_JOB_STATUS_TIMED_OUT:
		return "timed_out"
	case dbpb.JobStatus_JOB_STATUS_SKIPPED:
		return "skipped"
	case dbpb.JobStatus_JOB_STATUS_AWAITING_APPROVAL:
		return "awaiting_approval"
	default:
		return "unknown"
	}
}
