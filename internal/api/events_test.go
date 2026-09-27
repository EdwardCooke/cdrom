package api

import (
	"testing"
	"time"
)

func TestEventHubPublishSubscribe(t *testing.T) {
	hub := NewEventHub()
	ch, cancel := hub.Subscribe()
	defer cancel()

	hub.Publish(Event{Type: EventJobStatus, JobID: 1, Status: "running"})

	select {
	case ev := <-ch:
		if ev.Type != EventJobStatus {
			t.Errorf("type = %q, want %q", ev.Type, EventJobStatus)
		}
		if ev.JobID != 1 || ev.Status != "running" {
			t.Errorf("event = %+v", ev)
		}
		if ev.At == 0 {
			t.Error("expected Publish to stamp At")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the published event")
	}
}

func TestEventHubFanOut(t *testing.T) {
	hub := NewEventHub()
	ch1, cancel1 := hub.Subscribe()
	defer cancel1()
	ch2, cancel2 := hub.Subscribe()
	defer cancel2()

	hub.Publish(Event{Type: EventWorker, Worker: "w1", Action: WorkerRegistered})

	for _, ch := range []<-chan Event{ch1, ch2} {
		select {
		case ev := <-ch:
			if ev.Worker != "w1" {
				t.Errorf("worker = %q, want w1", ev.Worker)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for a fan-out event")
		}
	}
}

func TestEventHubCancelStopsDelivery(t *testing.T) {
	hub := NewEventHub()
	ch, cancel := hub.Subscribe()
	cancel()

	// Publishing after cancel must not panic or deliver.
	hub.Publish(Event{Type: EventWorker, Worker: "w"})
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected the subscriber channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected the subscriber channel to be closed")
	}
}

func TestEventHubDropOnFull(t *testing.T) {
	hub := NewEventHub()
	ch, cancel := hub.Subscribe()
	defer cancel()

	const buffer = 256
	for i := 0; i < buffer+50; i++ {
		hub.Publish(Event{Type: EventJobStatus, JobID: int64(i)})
	}

	count := 0
	for {
		select {
		case <-ch:
			count++
		default:
			if count != buffer {
				t.Errorf("drained %d events, want %d (the buffer size)", count, buffer)
			}
			return
		}
	}
}
