package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TestGetQueue verifies that GET /api/queue (F-16) returns the pending queue
// from the database service: the pending jobs (each with the resource(s)
// blocking it), the per-group and global load, and the per-key load.
func TestGetQueue(t *testing.T) {
	fake := &fakeDatabase{queue: &dbpb.QueueStatus{
		GlobalRunning: 1,
		GlobalLimit:   2,
		Pending: []*dbpb.QueuePendingJob{
			{Id: 10, Name: "deploy", TargetGroup: "prod", BlockedBy: []string{"prod-db"}},
		},
		Groups: []*dbpb.QueueGroupLoad{
			{Group: "prod", Running: 1, Limit: 1},
		},
		Keys: []*dbpb.QueueKeyLoad{
			{Key: "prod-db", Holders: 1, Limit: 1},
		},
	}}
	srv := New(Clients{Database: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/queue")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, data)
	}

	var queue dbpb.QueueStatus
	if err := json.NewDecoder(resp.Body).Decode(&queue); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if queue.GetGlobalRunning() != 1 || queue.GetGlobalLimit() != 2 {
		t.Errorf("global = running %d limit %d, want running 1 limit 2", queue.GetGlobalRunning(), queue.GetGlobalLimit())
	}
	if got := len(queue.GetPending()); got != 1 {
		t.Fatalf("pending = %d, want 1", got)
	}
	pending := queue.GetPending()[0]
	if pending.GetId() != 10 || pending.GetName() != "deploy" {
		t.Errorf("pending = id %d name %q, want id 10 name deploy", pending.GetId(), pending.GetName())
	}
	if len(pending.GetBlockedBy()) != 1 || pending.GetBlockedBy()[0] != "prod-db" {
		t.Errorf("pending blocked_by = %v, want [prod-db]", pending.GetBlockedBy())
	}
	if got := len(queue.GetGroups()); got != 1 || queue.GetGroups()[0].GetGroup() != "prod" {
		t.Errorf("groups = %+v, want one group prod", queue.GetGroups())
	}
	if got := len(queue.GetKeys()); got != 1 || queue.GetKeys()[0].GetKey() != "prod-db" {
		t.Errorf("keys = %+v, want one key prod-db", queue.GetKeys())
	}
}

// TestGetQueueEmpty verifies that GET /api/queue returns an empty (but valid)
// queue when there is no backlog and no load (F-16).
func TestGetQueueEmpty(t *testing.T) {
	fake := &fakeDatabase{} // no canned queue: GetQueueStatus returns an empty QueueStatus
	srv := New(Clients{Database: fake}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/queue")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	var queue dbpb.QueueStatus
	if err := json.NewDecoder(resp.Body).Decode(&queue); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(queue.GetPending()) != 0 || len(queue.GetGroups()) != 0 || len(queue.GetKeys()) != 0 {
		t.Errorf("queue = %+v, want empty", &queue)
	}
}
