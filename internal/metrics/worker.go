package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// AgentMetrics is the execution target's (agent or agent) metric set.
// It is constructed against a Collector's registry
// (NewAgentMetrics) and recorded into at the target's hot spots: the jobs it
// completes (by terminal status) and their durations, the steps it runs (by
// step type and status) and their durations, its in-flight job count
// (utilization), its WatchJobs stream state (agent only), its pending poll
// (agent only), its heartbeat failures (agent only), and the log bytes it
// streams to the API.
//
// The agent and the worker share this set: an agent records the watch-state,
// poll, and heartbeat metrics; an agent (which holds no WatchJobs stream and
// polls no queue) records only the job/step/in-flight/log metrics.
//
// Every method is nil-safe: when metrics are disabled the target passes a nil
// *AgentMetrics and all recording is a no-op.
type AgentMetrics struct {
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
	// the target's link to the control plane. Recorded by agents only.
	WatchState *prometheus.GaugeVec
	// Polls counts the periodic pending poll by outcome (idle / claimed).
	// Recorded by agents only.
	Polls *prometheus.CounterVec
	// HeartbeatFailures counts failed heartbeats. Recorded by agents only.
	HeartbeatFailures prometheus.Counter
	// LogBytes is the log bytes streamed to the API.
	LogBytes prometheus.Counter
}

// NewAgentMetrics builds the execution-target metric set, registering its
// collectors with reg.
func NewAgentMetrics(reg prometheus.Registerer) *AgentMetrics {
	m := &AgentMetrics{
		Jobs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_agent_jobs_total",
			Help: "Jobs this target completed, by terminal status.",
		}, []string{"status"}),
		JobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_agent_job_duration_seconds",
			Help:    "Job duration on this target, by terminal status.",
			Buckets: JobRunBuckets,
		}, []string{"status"}),
		Steps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_agent_steps_total",
			Help: "Steps run on this target, by step type and status.",
		}, []string{"step_type", "status"}),
		StepDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_agent_step_duration_seconds",
			Help:    "Step duration on this target, by step type.",
			Buckets: RequestBuckets,
		}, []string{"step_type"}),
		Inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_agent_inflight",
			Help: "Jobs currently running on this target.",
		}),
		WatchState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: Prefix + "_agent_watch_state",
			Help: "WatchJobs stream state (connected / reconnecting).",
		}, []string{"state"}),
		Polls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_agent_polls_total",
			Help: "Periodic pending polls, by outcome (idle / claimed).",
		}, []string{"outcome"}),
		HeartbeatFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: Prefix + "_agent_heartbeat_failures_total",
			Help: "Failed heartbeats.",
		}),
		LogBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: Prefix + "_agent_log_bytes_total",
			Help: "Log bytes streamed to the API.",
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
func (m *AgentMetrics) RecordJob(status string, d time.Duration) {
	if m == nil || status == "" {
		return
	}
	m.Jobs.WithLabelValues(status).Inc()
	if d >= 0 {
		m.JobDuration.WithLabelValues(status).Observe(d.Seconds())
	}
}

// RecordStep counts a step by type and status and observes its duration.
func (m *AgentMetrics) RecordStep(stepType, status string, d time.Duration) {
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
func (m *AgentMetrics) SetInflight(n int) {
	if m == nil {
		return
	}
	m.Inflight.Set(float64(n))
}

// SetWatchState sets the WatchJobs stream-state gauge (connected /
// reconnecting).
func (m *AgentMetrics) SetWatchState(state string) {
	if m == nil || state == "" {
		return
	}
	m.WatchState.WithLabelValues(state).Set(1)
}

// RecordPoll counts a pending poll by outcome (idle / claimed).
func (m *AgentMetrics) RecordPoll(outcome string) {
	if m == nil || outcome == "" {
		return
	}
	m.Polls.WithLabelValues(outcome).Inc()
}

// RecordHeartbeatFailure counts a failed heartbeat.
func (m *AgentMetrics) RecordHeartbeatFailure() {
	if m == nil {
		return
	}
	m.HeartbeatFailures.Inc()
}

// AddLogBytes counts log bytes streamed to the API.
func (m *AgentMetrics) AddLogBytes(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.LogBytes.Add(float64(n))
}
