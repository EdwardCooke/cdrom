package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestWebSocketSnapshotAndEvent dials the /api/ws endpoint, asserts the first
// message is a state snapshot, then publishes an event and asserts it is
// streamed to the client.
func TestWebSocketSnapshotAndEvent(t *testing.T) {
	hub := NewEventHub()
	srv := New(Clients{}, hub)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	// The first message is the state snapshot.
	var first wsMessage
	if err := conn.ReadJSON(&first); err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if first.Snapshot == nil {
		t.Fatal("expected the first message to be a snapshot")
	}
	if first.Snapshot.Jobs == nil || first.Snapshot.Workers == nil {
		t.Error("expected non-nil jobs and workers slices in the snapshot")
	}

	// Give the server a moment to register its subscription, then publish.
	time.Sleep(100 * time.Millisecond)
	hub.Publish(Event{Type: EventJobStatus, JobID: 42, Status: "succeeded"})

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var second wsMessage
	if err := conn.ReadJSON(&second); err != nil {
		t.Fatalf("read event: %v", err)
	}
	if second.Event == nil {
		t.Fatal("expected the second message to be an event")
	}
	if second.Event.JobID != 42 || second.Event.Status != "succeeded" {
		t.Errorf("event = %+v", second.Event)
	}
}

func TestWebSocketDisabledWithoutHub(t *testing.T) {
	srv := New(Clients{}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/ws")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (no hub configured)", resp.StatusCode)
	}
}
