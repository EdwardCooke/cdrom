// Package database — Prometheus metrics refresh (F-20).
//
// The Database service is the only component that owns the durable global
// state the fleet-wide gauges describe: the shared event log (F-23), the
// pending queue (F-16), and the concurrency slots (F-16). It exposes those as
// point-in-time gauges (cdrom_db_event_log_size, cdrom_db_queue_depth,
// cdrom_db_concurrency_slots) and refreshes them on a short interval in a
// background loop. The per-RPC latency (cdrom_db_rpc_duration_seconds) is
// recorded by the shared gRPC StatsHandler, not here.
package database

import (
	"context"
	"time"

	"cdrom/internal/metrics"
	"cdrom/internal/models"
)

// metricsRefreshInterval is how often the metrics loop refreshes the
// global-state gauges. It is a variable (not a constant) so tests can shorten
// it.
var metricsRefreshInterval = 5 * time.Second

// StartMetricsLoop runs the Database service's metrics refresh loop in the
// background until ctx is cancelled. It periodically re-derives the
// point-in-time gauges of the global state the database owns (the shared
// event log's size, the pending queue's depth per group, and the durable
// concurrency slots held per resource) and records them. It is a no-op when
// the metric set is nil (metrics disabled).
func (s *Server) StartMetricsLoop(ctx context.Context) {
	if s.metrics == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(metricsRefreshInterval)
		defer ticker.Stop()
		// Refresh once immediately so the gauges are populated without waiting
		// a full interval after startup.
		s.refreshMetrics(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.refreshMetrics(ctx)
			}
		}
	}()
}

// refreshMetrics re-derives and records the global-state gauges (F-20): the
// shared event log's row count, the pending queue's depth per group, and the
// durable concurrency slots held per resource. A query failure leaves the
// corresponding gauge at its last value (the gauges are point-in-time and a
// transient database hiccup should not zero them).
func (s *Server) refreshMetrics(ctx context.Context) {
	db := s.db.WithContext(ctx)

	// Event-log size (F-23): the row count of the shared event log. It feeds
	// the event-log retention open item.
	var eventLogSize int64
	if err := db.Model(&models.Event{}).Count(&eventLogSize).Error; err == nil {
		s.metrics.SetEventLogSize(eventLogSize)
	}

	// Queue depth per group (F-16): the number of pending, not-yet-admitted
	// jobs per target group (the empty group is the global/ephemeral-agent
	// queue). An admitted pending job is about to be dispatched, so it is not
	// counted as waiting.
	type groupDepth struct {
		TargetGroup string
		Pending     int64
	}
	var depths []groupDepth
	if err := db.Model(&models.Job{}).
		Where("status = ? AND concurrency_admitted = ?", models.JobStatusPending, false).
		Select("COALESCE(target_group, '') as target_group, count(*) as pending").
		Group("COALESCE(target_group, '')").
		Scan(&depths).Error; err == nil {
		depth := make(map[string]int, len(depths))
		for _, d := range depths {
			if d.Pending > 0 {
				depth[d.TargetGroup] = int(d.Pending)
			}
		}
		s.metrics.UpdateQueueDepth(depth)
	}

	// Concurrency slots (F-16): the durable slots held per resource. A
	// resource's kind is derived from its key: "global" is the system-wide
	// cap, "group:<name>" is a worker group's cap, and anything else is a
	// concurrency key.
	var resources []models.ConcurrencyResource
	if err := db.Find(&resources).Error; err == nil {
		slots := make([]metrics.Slot, 0, len(resources))
		for i := range resources {
			if resources[i].Held <= 0 {
				continue
			}
			kind, resource := slotKindAndResource(resources[i].Key)
			slots = append(slots, metrics.Slot{Kind: kind, Resource: resource, Held: resources[i].Held})
		}
		s.metrics.UpdateConcurrencySlots(slots)
	}
}

// slotKindAndResource maps a concurrency resource's key to its kind
// (global / group / key) and name (F-16): "global" is the system-wide cap,
// "group:<name>" is a worker group's cap, and anything else is a concurrency
// key.
func slotKindAndResource(key string) (kind, resource string) {
	switch {
	case key == globalResourceKey:
		return "global", globalResourceKey
	case len(key) > len(groupResourcePrefix) && key[:len(groupResourcePrefix)] == groupResourcePrefix:
		return "group", key[len(groupResourcePrefix):]
	default:
		return "key", key
	}
}
