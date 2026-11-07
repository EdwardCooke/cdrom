package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// SchedulerMetrics is the scheduler service's metric set. It is
// constructed against a Collector's registry (NewSchedulerMetrics) and
// recorded into at the scheduler's hot spots: dispatch outcomes, the
// leader-gated admission loop, queue wait, run status/duration/lead-time, the
// watchdog / retry / dependency / trigger counters, the leader gauge, and the
// per-loop tick durations.
//
// HA: counters are recorded by exactly one replica per event — the
// replica that handled the event (a dispatch is handled by one scheduler
// replica) or the leader for leader-gated work (the admission loop, the
// run-status loop) — so summing the per-pod series gives the fleet total.
//
// Every method is nil-safe: when metrics are disabled the scheduler passes a
// nil *SchedulerMetrics and all recording is a no-op.
type SchedulerMetrics struct {
	// Dispatches counts dispatch decisions by outcome: dispatched /
	// blocked_limit / blocked_key / held_manual / skipped.
	Dispatches *prometheus.CounterVec
	// AdmissionChecks counts the admission loop's checks.
	AdmissionChecks prometheus.Counter
	// AdmissionDuration is the admission loop's per-pass duration.
	AdmissionDuration prometheus.Histogram
	// QueueWait is a job's pending→running wait (the queue's wait time),
	// recorded by the replica that dispatches it.
	QueueWait *prometheus.HistogramVec
	// Runs counts run status transitions from the run-status loop, by
	// status, trigger (manual / cron / webhook / event), and environment (the
	// highest environment the run reached, empty when it targets none).
	Runs *prometheus.CounterVec
	// RunDuration is a run's start → finish duration.
	RunDuration *prometheus.HistogramVec
	// RunLeadTime is a run's creation (trigger) → completion; the
	// lead-time-for-changes signal.
	RunLeadTime *prometheus.HistogramVec
	// WatchdogReaps counts jobs reaped as timed out.
	WatchdogReaps prometheus.Counter
	// Retries counts retry re-dispatches.
	Retries prometheus.Counter
	// DependenciesResolved counts dependency resolutions by outcome (released /
	// skipped).
	DependenciesResolved *prometheus.CounterVec
	// TriggersFired counts trigger fires by kind (cron / webhook / event).
	TriggersFired *prometheus.CounterVec
	// Leader is 1 while this replica holds the leader lease.
	Leader prometheus.Gauge
	// LoopDuration is a background loop's per-tick duration, by loop name
	// (watchdog / retry / dependencies / run_status / job_status / admission /
	// cron / event).
	LoopDuration *prometheus.HistogramVec
}

// NewSchedulerMetrics builds the scheduler metric set, registering its
// collectors with reg.
func NewSchedulerMetrics(reg prometheus.Registerer) *SchedulerMetrics {
	m := &SchedulerMetrics{
		Dispatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_scheduler_dispatches_total",
			Help: "Scheduler dispatch decisions, by outcome.",
		}, []string{"outcome"}),
		AdmissionChecks: prometheus.NewCounter(prometheus.CounterOpts{
			Name: Prefix + "_scheduler_admission_checks_total",
			Help: "Admission-loop checks.",
		}),
		AdmissionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    Prefix + "_scheduler_admission_duration_seconds",
			Help:    "Admission-loop pass duration.",
			Buckets: RequestBuckets,
		}),
		QueueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_scheduler_queue_wait_seconds",
			Help:    "A job's pending→running wait (the queue's wait time).",
			Buckets: QueueWaitBuckets,
		}, []string{"group"}),
		Runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_scheduler_runs_total",
			Help: "Run status transitions from the run-status loop, by status, trigger, and environment.",
		}, []string{"status", "trigger", "environment"}),
		RunDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_scheduler_run_duration_seconds",
			Help:    "Run start → finish duration.",
			Buckets: JobRunBuckets,
		}, []string{"status", "trigger", "environment"}),
		RunLeadTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_scheduler_run_lead_time_seconds",
			Help:    "Run creation (trigger) → completion; the lead-time-for-changes signal.",
			Buckets: JobRunBuckets,
		}, []string{"trigger", "environment"}),
		WatchdogReaps: prometheus.NewCounter(prometheus.CounterOpts{
			Name: Prefix + "_scheduler_watchdog_reaps_total",
			Help: "Jobs reaped as timed out.",
		}),
		Retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: Prefix + "_scheduler_retries_total",
			Help: "Retry re-dispatches.",
		}),
		DependenciesResolved: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_scheduler_dependencies_resolved_total",
			Help: "Dependency resolutions, by outcome (released / skipped).",
		}, []string{"outcome"}),
		TriggersFired: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_scheduler_triggers_fired_total",
			Help: "Trigger fires, by kind (cron / webhook / event).",
		}, []string{"kind"}),
		Leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_scheduler_leader",
			Help: "1 while this replica holds the leader lease.",
		}),
		LoopDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_scheduler_loop_duration_seconds",
			Help:    "Background-loop tick duration, by loop.",
			Buckets: RequestBuckets,
		}, []string{"loop"}),
	}
	reg.MustRegister(
		m.Dispatches, m.AdmissionChecks, m.AdmissionDuration, m.QueueWait,
		m.Runs, m.RunDuration, m.RunLeadTime, m.WatchdogReaps, m.Retries,
		m.DependenciesResolved, m.TriggersFired, m.Leader, m.LoopDuration,
	)
	return m
}

// RecordDispatch counts a dispatch decision by outcome.
func (m *SchedulerMetrics) RecordDispatch(outcome string) {
	if m == nil || outcome == "" {
		return
	}
	m.Dispatches.WithLabelValues(outcome).Inc()
}

// RecordAdmission counts an admission-loop check and observes the pass
// duration.
func (m *SchedulerMetrics) RecordAdmission(d time.Duration) {
	if m == nil {
		return
	}
	m.AdmissionChecks.Inc()
	m.AdmissionDuration.Observe(d.Seconds())
}

// ObserveQueueWait observes a job's pending→running wait, by target group
// (empty for a job with no group).
func (m *SchedulerMetrics) ObserveQueueWait(group string, d time.Duration) {
	if m == nil || d < 0 {
		return
	}
	m.QueueWait.WithLabelValues(group).Observe(d.Seconds())
}

// RecordRun counts a run status transition, by status, trigger, and
// environment.
func (m *SchedulerMetrics) RecordRun(status, trigger, environment string) {
	if m == nil || status == "" {
		return
	}
	m.Runs.WithLabelValues(status, trigger, environment).Inc()
}

// ObserveRunDuration observes a run's start → finish duration.
func (m *SchedulerMetrics) ObserveRunDuration(status, trigger, environment string, d time.Duration) {
	if m == nil || d < 0 {
		return
	}
	m.RunDuration.WithLabelValues(status, trigger, environment).Observe(d.Seconds())
}

// ObserveRunLeadTime observes a run's creation → completion duration.
func (m *SchedulerMetrics) ObserveRunLeadTime(trigger, environment string, d time.Duration) {
	if m == nil || d < 0 {
		return
	}
	m.RunLeadTime.WithLabelValues(trigger, environment).Observe(d.Seconds())
}

// RecordWatchdogReap counts a job reaped as timed out.
func (m *SchedulerMetrics) RecordWatchdogReap() {
	if m == nil {
		return
	}
	m.WatchdogReaps.Inc()
}

// RecordRetry counts a retry re-dispatch.
func (m *SchedulerMetrics) RecordRetry() {
	if m == nil {
		return
	}
	m.Retries.Inc()
}

// RecordDependencyResolution counts a dependency resolution by outcome (released / skipped).
func (m *SchedulerMetrics) RecordDependencyResolution(outcome string) {
	if m == nil || outcome == "" {
		return
	}
	m.DependenciesResolved.WithLabelValues(outcome).Inc()
}

// RecordTriggerFired counts a trigger fire by kind (cron / webhook / event).
func (m *SchedulerMetrics) RecordTriggerFired(kind string) {
	if m == nil || kind == "" {
		return
	}
	m.TriggersFired.WithLabelValues(kind).Inc()
}

// SetLeader sets the leader gauge (1 while this replica holds the lease).
func (m *SchedulerMetrics) SetLeader(isLeader bool) {
	if m == nil {
		return
	}
	if isLeader {
		m.Leader.Set(1)
	} else {
		m.Leader.Set(0)
	}
}

// ObserveLoopDuration observes a background loop's tick duration, by loop
// name.
func (m *SchedulerMetrics) ObserveLoopDuration(loop string, d time.Duration) {
	if m == nil || loop == "" {
		return
	}
	m.LoopDuration.WithLabelValues(loop).Observe(d.Seconds())
}
