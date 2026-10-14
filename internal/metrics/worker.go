package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// WorkerMetrics is the execution target's (worker or agent) metric set
// (F-20). It is constructed against a Collector's registry
// (NewWorkerMetrics) and recorded into at the target's hot spots: the jobs it
// completes (by terminal status) and their durations, the steps it runs (by
// step type and status) and their durations, its in-flight job count
// (utilization), its WatchJobs stream state (worker only), its pending poll
// (worker only), its heartbeat failures (worker only), and the log bytes it
// streams to the API.
//
// The worker and the agent share this set: a worker records the watch-state,
// poll, and heartbeat metrics; an agent (which holds no WatchJobs stream and
// polls no queue) records only the job/step/in-flight/log metrics.
//
// Every method is nil-safe: when metrics are disabled the target passes a nil
// *WorkerMetrics and all recording is a no-op.
type WorkerMetrics struct {
	// Jobs counts jobs this target completed, by terminal status.
	Jobs *prometheus.CounterVec
	// JobDuration is a job's duration, by terminal status.
	JobDuration *prometheus.HistogramVec
	// Steps counts steps by step type and status.
	Steps *prometheus.CounterVec
	// StepDuration is a step's duration, by step type.
	StepDuration *prometheus.HistogramVec
	// Inflight is the number of jobs currently running on this target
	// (utilization: in-flight vs. the target's limit of one).
	Inflight prometheus.Gauge
	// WatchState is the WatchJobs stream's state (connected / reconnecting);
	// the target's link to the control plane. Recorded by workers only.
	WatchState *prometheus.GaugeVec
	// Polls counts the periodic pending poll by outcome (idle / claimed).
	// Recorded by workers only.
	Polls *prometheus.CounterVec
	// HeartbeatFailures counts failed heartbeats. Recorded by workers only.
	HeartbeatFailures prometheus.Counter
	// LogBytes is the log bytes streamed to the API (F-02).
	LogBytes prometheus.Counter
}

// NewWorkerMetrics builds the execution-target metric set, registering its
// collectors with reg.
func NewWorkerMetrics(reg prometheus.Registerer) *WorkerMetrics {
	m := &WorkerMetrics{
		Jobs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_worker_jobs_total",
			Help: "Jobs this target completed, by terminal status.",
		}, []string{"status"}),
		JobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_worker_job_duration_seconds",
			Help:    "Job duration on this target, by terminal status.",
			Buckets: JobRunBuckets,
		}, []string{"status"}),
		Steps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_worker_steps_total",
			Help: "Steps run on this target, by step type and status.",
		}, []string{"step_type", "status"}),
		StepDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_worker_step_duration_seconds",
			Help:    "Step duration on this target, by step type.",
			Buckets: RequestBuckets,
		}, []string{"step_type"}),
		Inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_worker_inflight",
			Help: "Jobs currently running on this target.",
		}),
		WatchState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: Prefix + "_worker_watch_state",
			Help: "WatchJobs stream state (connected / reconnecting).",
		}, []string{"state"}),
		Polls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_worker_polls_total",
			Help: "Periodic pending polls, by outcome (idle / claimed).",
		}, []string{"outcome"}),
		HeartbeatFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: Prefix + "_worker_heartbeat_failures_total",
			Help: "Failed heartbeats.",
		}),
		LogBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: Prefix + "_worker_log_bytes_total",
			Help: "Log bytes streamed to the API (F-02).",
		}),
	}
	reg.MustRegister(
		m.Jobs, m.JobDuration, m.Steps, m.StepDuration, m.Inflight,
		m.WatchState, m.Polls, m.HeartbeatFailures, m.LogBytes,
	)
	return m
}

// RecordJob counts a completed job by terminal status and observes its
// duration.
func (m *WorkerMetrics) RecordJob(status string, d time.Duration) {
	if m == nil || status == "" {
		return
	}
	m.Jobs.WithLabelValues(status).Inc()
	if d >= 0 {
		m.JobDuration.WithLabelValues(status).Observe(d.Seconds())
	}
}

// RecordStep counts a step by type and status and observes its duration.
func (m *WorkerMetrics) RecordStep(stepType, status string, d time.Duration) {
	if m == nil || stepType == "" {
		return
	}
	if status != "" {
		m.Steps.WithLabelValues(stepType, status).Inc()
	}
	if d >= 0 {
		m.StepDuration.WithLabelValues(stepType).Observe(d.Seconds())
	}
}

// SetInflight sets the in-flight job gauge.
func (m *WorkerMetrics) SetInflight(n int) {
	if m == nil {
		return
	}
	m.Inflight.Set(float64(n))
}

// SetWatchState sets the WatchJobs stream-state gauge (connected /
// reconnecting).
func (m *WorkerMetrics) SetWatchState(state string) {
	if m == nil || state == "" {
		return
	}
	m.WatchState.WithLabelValues(state).Set(1)
}

// RecordPoll counts a pending poll by outcome (idle / claimed).
func (m *WorkerMetrics) RecordPoll(outcome string) {
	if m == nil || outcome == "" {
		return
	}
	m.Polls.WithLabelValues(outcome).Inc()
}

// RecordHeartbeatFailure counts a failed heartbeat.
func (m *WorkerMetrics) RecordHeartbeatFailure() {
	if m == nil {
		return
	}
	m.HeartbeatFailures.Inc()
}

// AddLogBytes counts log bytes streamed to the API.
func (m *WorkerMetrics) AddLogBytes(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.LogBytes.Add(float64(n))
}
