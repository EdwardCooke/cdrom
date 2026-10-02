package api

import (
	"sync"
	"time"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// Event is a single change notification broadcast to WebSocket subscribers.
type Event struct {
	Type   string `json:"type"`
	JobID  int64  `json:"job_id,omitempty"`
	Status string `json:"status,omitempty"`
	Worker string `json:"worker,omitempty"`
	Group  string `json:"group,omitempty"`
	Action string `json:"action,omitempty"`
	// StepIndex is the 0-based index of the step a job_log event belongs to.
	StepIndex int32 `json:"step_index,omitempty"`
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
	At          int64 `json:"at"`
}

// Event type constants.
const (
	EventJobStatus = "job_status"
	EventWorker    = "worker"
	EventJobLog    = "job_log"
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
	default:
		return "unknown"
	}
}
