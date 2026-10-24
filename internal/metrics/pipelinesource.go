package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PipelineSourceMetrics is the source-fetcher service's metric set (F-20). It
// is constructed against a Collector's registry (NewPipelineSourceMetrics)
// and records the fetch operation counters (by kind and gRPC status code),
// the bytes fetched, the fetch durations, and the git repository cache's
// state (entries, and the clone / fetch / resync / prune counts).
//
// Every method is nil-safe: when metrics are disabled the service passes a nil
// *PipelineSourceMetrics and all recording is a no-op.
type PipelineSourceMetrics struct {
	// Fetches counts FetchSource operations by kind (git / http / file) and
	// gRPC status code.
	Fetches *prometheus.CounterVec
	// Bytes is the bytes fetched, by kind.
	Bytes *prometheus.CounterVec
	// Duration is the fetch duration, by kind.
	Duration *prometheus.HistogramVec
	// CacheEntries is the number of repositories currently in the git
	// repository cache.
	CacheEntries prometheus.Gauge
	// CacheOperations counts git cache operations (clone / fetch / resync /
	// prune) by outcome.
	CacheOperations *prometheus.CounterVec
}

// NewPipelineSourceMetrics builds the source-fetcher metric set, registering
// its collectors with reg.
func NewPipelineSourceMetrics(reg prometheus.Registerer) *PipelineSourceMetrics {
	m := &PipelineSourceMetrics{
		Fetches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_pipelinesource_fetches_total",
			Help: "Source-fetcher FetchSource operations, by kind and status code.",
		}, []string{"kind", "code"}),
		Bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_pipelinesource_bytes_total",
			Help: "Bytes fetched by the source-fetcher, by kind.",
		}, []string{"kind"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_pipelinesource_fetch_duration_seconds",
			Help:    "Source-fetcher FetchSource duration, by kind.",
			Buckets: RequestBuckets,
		}, []string{"kind"}),
		CacheEntries: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_pipelinesource_cache_entries",
			Help: "Repositories currently in the git repository cache.",
		}),
		CacheOperations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_pipelinesource_cache_operations_total",
			Help: "Git repository cache operations, by op (clone/fetch/resync/prune) and outcome.",
		}, []string{"op", "outcome"}),
	}
	reg.MustRegister(m.Fetches, m.Bytes, m.Duration, m.CacheEntries, m.CacheOperations)
	return m
}

// RecordFetch counts a FetchSource operation by kind and gRPC status code.
func (m *PipelineSourceMetrics) RecordFetch(kind, code string) {
	if m == nil || kind == "" {
		return
	}
	m.Fetches.WithLabelValues(kind, code).Inc()
}

// AddBytes counts bytes fetched by kind.
func (m *PipelineSourceMetrics) AddBytes(kind string, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.Bytes.WithLabelValues(kind).Add(float64(n))
}

// ObserveDuration observes a fetch's duration by kind.
func (m *PipelineSourceMetrics) ObserveDuration(kind string, d time.Duration) {
	if m == nil || kind == "" || d < 0 {
		return
	}
	m.Duration.WithLabelValues(kind).Observe(d.Seconds())
}

// SetCacheEntries sets the number of repositories in the cache.
func (m *PipelineSourceMetrics) SetCacheEntries(n int) {
	if m == nil {
		return
	}
	m.CacheEntries.Set(float64(n))
}

// RecordCacheOperation counts a git cache operation by op and outcome.
func (m *PipelineSourceMetrics) RecordCacheOperation(op, outcome string) {
	if m == nil || op == "" {
		return
	}
	m.CacheOperations.WithLabelValues(op, outcome).Inc()
}
