package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// wsUpgrader upgrades HTTP requests to WebSocket connections. CheckOrigin
// accepts same-origin and cross-origin requests; the API relies on the
// Bearer token (verified by the auth middleware) for authorization, so origin
// checking is intentionally permissive for the UI.
var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// wsMessage is the envelope sent over the WebSocket. On connect the first
// message is a snapshot of current state; subsequent messages are individual
// events.
type wsMessage struct {
	Snapshot *wsSnapshot `json:"snapshot,omitempty"`
	Event    *Event      `json:"event,omitempty"`
}

// wsSnapshot is the state sent to a subscriber on connect so it can
// resynchronize without replaying history.
type wsSnapshot struct {
	Jobs    []wsJob    `json:"jobs"`
	Workers []wsWorker `json:"workers"`
}

type wsJob struct {
	ID          int64  `json:"id"`
	PipelineID  int64  `json:"pipeline_id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	TargetGroup string `json:"target_group"`
	// Attempt is the 1-based attempt number of the execution (F-04); 0 means
	// the field is unset.
	Attempt int32 `json:"attempt,omitempty"`
	// MaxAttempts is the job's retry budget (0 means the job is never
	// retried), so the UI can render "attempt N of M".
	MaxAttempts int32 `json:"max_attempts,omitempty"`
}

type wsWorker struct {
	Name  string `json:"name"`
	Group string `json:"group"`
}

// handleWebSocket upgrades the connection, sends a state snapshot, then
// streams events from the hub until the connection closes.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Send the current state so the client can resynchronize.
	if snapshot, err := s.snapshot(); err == nil {
		_ = conn.WriteJSON(wsMessage{Snapshot: snapshot})
	}

	events, cancel := s.hub.Subscribe()
	defer cancel()

	// Ping periodically to keep the connection alive and detect dead peers.
	pingDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pingDone:
				return
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					return
				}
			}
		}
	}()

	// Drain inbound messages (the UI sends none, but we must read to detect
	// close frames and keep the connection healthy).
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				close(pingDone)
				return
			}
		}
	}()

	for ev := range events {
		if err := conn.WriteJSON(wsMessage{Event: &ev}); err != nil {
			return
		}
	}
}

// snapshot builds the current jobs and workers for a WebSocket subscriber.
// Clients that are nil are skipped, so the endpoint works even when only a
// subset of the services is wired up.
func (s *Server) snapshot() (*wsSnapshot, error) {
	ctx := context.Background()
	snap := &wsSnapshot{Jobs: []wsJob{}, Workers: []wsWorker{}}

	if s.clients.Scheduler != nil {
		jobsResp, err := s.clients.Scheduler.ListJobs(ctx, &schedpb.ListJobsRequest{})
		if err != nil {
			return nil, err
		}
		for _, job := range jobsResp.GetJobs() {
			snap.Jobs = append(snap.Jobs, wsJob{
				ID:          job.GetId(),
				PipelineID:  job.GetPipelineId(),
				Name:        job.GetName(),
				Status:      jobStatusName(job.GetStatus()),
				TargetGroup: job.GetTargetGroup(),
				Attempt:     job.GetAttempt(),
				MaxAttempts: job.GetMaxAttempts(),
			})
		}
	}

	if s.clients.Database != nil {
		workersResp, err := s.clients.Database.ListWorkers(ctx, &dbpb.ListWorkersRequest{})
		if err != nil {
			return nil, err
		}
		for _, worker := range workersResp.GetWorkers() {
			snap.Workers = append(snap.Workers, wsWorker{
				Name:  worker.GetName(),
				Group: worker.GetGroup(),
			})
		}
	}

	return snap, nil
}
