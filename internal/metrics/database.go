package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// DatabaseMetrics is the Database service's metric set (F-20). It is
// constructed against a Collector's registry (NewDatabaseMetrics) and records
// the point-in-time gauges of the global state the database owns (the shared
// event log's size, the pending queue's depth per group, and the durable
// concurrency slots held per resource). The per-RPC latency
// (cdrom_db_rpc_duration_seconds) is recorded by the shared gRPC StatsHandler
// (via NewStatsHandlerExtra), not here.
//
// HA (F-23): these are point-in-time gauges of global state, exposed by the
// component that owns the state (the database service). They are refreshed by
// the database service on a short interval (see the database server's
// metrics loop).
//
// Every method is nil-safe: when metrics are disabled the database service
// passes a nil *DatabaseMetrics and all recording is a no-op.
type DatabaseMetrics struct {
	// EventLogSize is the row count of the shared event log (F-23); it feeds
	// the event-log retention open item.
	EventLogSize prometheus.Gauge
	// QueueDepth is the number of pending jobs per group (empty label = the
	// global queue), from the durable queue state (F-16).
	QueueDepth *prometheus.GaugeVec
	// ConcurrencySlots is the durable slots held per concurrency resource
	// (kind: global / group / key; resource: the resource's name), (F-16).
	ConcurrencySlots *prometheus.GaugeVec
}

// NewDatabaseMetrics builds the database metric set, registering its
// collectors with reg.
func NewDatabaseMetrics(reg prometheus.Registerer) *DatabaseMetrics {
	m := &DatabaseMetrics{
		EventLogSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_db_event_log_size",
			Help: "Row count of the shared event log (F-23).",
		}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: Prefix + "_db_queue_depth",
			Help: "Pending jobs per group (empty label = the global queue), from the durable queue state (F-16).",
		}, []string{"group"}),
		ConcurrencySlots: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: Prefix + "_db_concurrency_slots",
			Help: "Durable slots held per concurrency resource (F-16).",
		}, []string{"kind", "resource"}),
	}
	reg.MustRegister(m.EventLogSize, m.QueueDepth, m.ConcurrencySlots)
	return m
}

// SetEventLogSize sets the shared event log's row count.
func (m *DatabaseMetrics) SetEventLogSize(n int64) {
	if m == nil || n < 0 {
		return
	}
	m.EventLogSize.Set(float64(n))
}

// UpdateQueueDepth recomputes the per-group pending-job gauge from the given
// group -> pending-count map. A group with no pending jobs is dropped (the
// gauge is reset each refresh).
func (m *DatabaseMetrics) UpdateQueueDepth(depth map[string]int) {
	if m == nil {
		return
	}
	m.QueueDepth.Reset()
	for group, n := range depth {
		if n <= 0 {
			continue
		}
		m.QueueDepth.WithLabelValues(group).Set(float64(n))
	}
}

// Slot is one concurrency resource's kind and name, for the slots gauge.
type Slot struct {
	Kind     string // global / group / key
	Resource string // the resource's name (e.g. a group name or a key)
	Held     int
}

// UpdateConcurrencySlots recomputes the per-resource held-slots gauge from the
// given slots. A resource holding no slots is dropped (the gauge is reset
// each refresh).
func (m *DatabaseMetrics) UpdateConcurrencySlots(slots []Slot) {
	if m == nil {
		return
	}
	m.ConcurrencySlots.Reset()
	for _, s := range slots {
		if s.Held <= 0 {
			continue
		}
		m.ConcurrencySlots.WithLabelValues(s.Kind, s.Resource).Set(float64(s.Held))
	}
}
