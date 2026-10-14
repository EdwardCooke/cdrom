package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ArtifactsMetrics is the Artifacts service's metric set (F-20). It is
// constructed against a Collector's registry (NewArtifactsMetrics) and
// records the operation counters, bytes moved, and durations for the
// artifacts service's operations (upload / download / append_log / list).
//
// Every method is nil-safe: when metrics are disabled the artifacts service
// passes a nil *ArtifactsMetrics and all recording is a no-op.
type ArtifactsMetrics struct {
	// Operations counts operations by op (upload / download / append_log /
	// list) and gRPC status code.
	Operations *prometheus.CounterVec
	// Bytes is the bytes moved, by op.
	Bytes *prometheus.CounterVec
	// Duration is the operation duration, by op.
	Duration *prometheus.HistogramVec
}

// NewArtifactsMetrics builds the artifacts metric set, registering its
// collectors with reg.
func NewArtifactsMetrics(reg prometheus.Registerer) *ArtifactsMetrics {
	m := &ArtifactsMetrics{
		Operations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_artifacts_operations_total",
			Help: "Artifacts operations, by op and status code.",
		}, []string{"op", "code"}),
		Bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_artifacts_bytes_total",
			Help: "Bytes moved by the artifacts service, by op.",
		}, []string{"op"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_artifacts_duration_seconds",
			Help:    "Artifacts operation duration, by op.",
			Buckets: RequestBuckets,
		}, []string{"op"}),
	}
	reg.MustRegister(m.Operations, m.Bytes, m.Duration)
	return m
}

// RecordOperation counts an operation by op and gRPC status code.
func (m *ArtifactsMetrics) RecordOperation(op, code string) {
	if m == nil || op == "" {
		return
	}
	m.Operations.WithLabelValues(op, code).Inc()
}

// AddBytes counts bytes moved by op.
func (m *ArtifactsMetrics) AddBytes(op string, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.Bytes.WithLabelValues(op).Add(float64(n))
}

// ObserveDuration observes an operation's duration by op.
func (m *ArtifactsMetrics) ObserveDuration(op string, d time.Duration) {
	if m == nil || op == "" || d < 0 {
		return
	}
	m.Duration.WithLabelValues(op).Observe(d.Seconds())
}
