package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// WorkerState is one worker's identity and state, as reported to the API's
// workers gauge (cdrom_api_workers). Group is the worker's group; Environments
// are the environments it belongs to (F-17; empty when it belongs to none);
// State is "registered" or "watching" (whether it holds an open WatchJobs
// stream).
type WorkerState struct {
	Group        string
	Environments []string
	State        string
}

// APIMetrics is the API (control plane) metric set (F-20). It is constructed
// against a Collector's registry (NewAPIMetrics) and recorded into at the
// control plane's hot spots: the UI HTTP surface, the gRPC surface (via the
// shared StatsHandler), the WebSocket hub, the worker hub, the event-log tail
// loop, job status reports, the artifact proxy, log streaming, token
// minting/exchange, secret decryption, and notification sends.
//
// Every method is nil-safe: when metrics are disabled the API passes a nil
// *APIMetrics and all recording is a no-op.
type APIMetrics struct {
	// HTTPRequests counts the UI HTTP surface's requests by route (the mux
	// pattern, not the raw path), method, and status code.
	HTTPRequests *prometheus.CounterVec
	// HTTPDuration is the UI HTTP surface's request duration by route and
	// method.
	HTTPDuration *prometheus.HistogramVec
	// AuthFailures counts 401/403 responses on the UI surface (reason:
	// unauthenticated / forbidden / lockout).
	AuthFailures *prometheus.CounterVec
	// WSClients is the number of live WebSocket UI clients on this pod.
	WSClients prometheus.Gauge
	// Workers is the number of workers registered on this pod, by group,
	// environment, and state (registered / watching).
	Workers *prometheus.GaugeVec
	// EventLogLag is how far this pod's event-log tail cursor is behind the
	// shared event log's head (F-23) — the key HA health signal. It is ~0 on a
	// healthy pod and grows when the tail is stopped.
	EventLogLag prometheus.Gauge
	// JobStatus counts job status transitions as reported by targets (running /
	// succeeded / failed / cancelled / timed_out / skipped), recorded by the
	// pod that receives the report.
	JobStatus *prometheus.CounterVec
	// JobDuration is a job's duration (from its start to finish timestamps),
	// observed when a job reaches a terminal state, by terminal status.
	JobDuration *prometheus.HistogramVec
	// ArtifactBytes is the bytes moved through the API's artifact proxy, by
	// operation (upload / download) and kind (artifact / log).
	ArtifactBytes *prometheus.CounterVec
	// LogStreamBytes is the job log bytes persisted (F-02).
	LogStreamBytes prometheus.Counter
	// TokensMinted counts job tokens minted or exchanged (kind: job /
	// exchanged).
	TokensMinted *prometheus.CounterVec
	// SecretsDecrypted counts secret decryptions at dispatch (F-12).
	SecretsDecrypted prometheus.Counter
	// Notifications counts email/webhook sends (F-18) by kind (email /
	// webhook) and outcome (sent / failed).
	Notifications *prometheus.CounterVec
	// SourceSyncs counts source-refresh syncs (F-21.5.3) by outcome:
	// success (a changed file was re-imported), unchanged (a no-op), error
	// (fetch/compile/re-import failed), deferred_peer (another replica held
	// the cross-replica sync lease), skipped_busy (the loop's own
	// skip-if-in-flight guard declined a due fire), and skipped_throttled
	// (the loop's max-parallel throttle deferred a due fire to the next tick).
	SourceSyncs *prometheus.CounterVec
	// SourceSyncDuration is a source sync's wall-clock duration (F-21.5.3),
	// by outcome — the latency signal that reveals a source whose syncs
	// outrun its own interval.
	SourceSyncDuration *prometheus.HistogramVec
	// SourceSyncInFlight is the number of source syncs currently in flight on
	// this pod (F-21.5.3) — the saturation signal.
	SourceSyncInFlight prometheus.Gauge
	// LeaderState is 1 while this pod holds the source-refresh leader lease
	// (F-21.5.3 / F-23), 0 otherwise — parity with the scheduler's leader
	// metric; a stuck 0 on every pod means nobody is polling.
	LeaderState prometheus.Gauge
	// Restorations counts resumption (rollback) initiations (F-22) by door
	// (armed — a rollback step handler's mediation — / manual — a user's
	// restart endpoint) and result (initiated, cooldown, no_anchor, rearmed,
	// invalid_cut_point, error).
	Restorations *prometheus.CounterVec
}

// NewAPIMetrics builds the API metric set, registering its collectors with
// reg.
func NewAPIMetrics(reg prometheus.Registerer) *APIMetrics {
	m := &APIMetrics{
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_api_http_requests_total",
			Help: "UI HTTP requests, by route, method, and status code.",
		}, []string{"route", "method", "code"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_api_http_request_duration_seconds",
			Help:    "UI HTTP request duration, by route and method.",
			Buckets: RequestBuckets,
		}, []string{"route", "method"}),
		AuthFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_api_auth_failures_total",
			Help: "UI HTTP authentication/authorization failures, by reason.",
		}, []string{"reason"}),
		WSClients: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_api_ws_clients",
			Help: "Live WebSocket UI clients on this pod.",
		}),
		Workers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: Prefix + "_api_workers",
			Help: "Workers registered on this pod, by group, environment, and state.",
		}, []string{"group", "environment", "state"}),
		EventLogLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_api_event_log_lag",
			Help: "How far this pod's event-log tail cursor is behind the shared event log's head (F-23).",
		}),
		JobStatus: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_api_job_status_total",
			Help: "Job status transitions as reported by targets, by status.",
		}, []string{"status"}),
		JobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_api_job_duration_seconds",
			Help:    "Job duration (start to finish), by terminal status.",
			Buckets: JobRunBuckets,
		}, []string{"status"}),
		ArtifactBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_api_artifact_bytes_total",
			Help: "Bytes moved through the API's artifact proxy, by operation and kind.",
		}, []string{"op", "kind"}),
		LogStreamBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: Prefix + "_api_log_stream_bytes_total",
			Help: "Job log bytes persisted (F-02).",
		}),
		TokensMinted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_api_tokens_minted_total",
			Help: "Job tokens minted or exchanged, by kind.",
		}, []string{"kind"}),
		SecretsDecrypted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: Prefix + "_api_secrets_decrypted_total",
			Help: "Secret decryptions at dispatch (F-12).",
		}),
		Notifications: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_api_notifications_total",
			Help: "Email/webhook notification sends (F-18), by kind and outcome.",
		}, []string{"kind", "outcome"}),
		SourceSyncs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_api_source_syncs_total",
			Help: "Source-refresh syncs (F-21.5.3), by outcome.",
		}, []string{"outcome"}),
		SourceSyncDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_api_source_sync_duration_seconds",
			Help:    "Source-refresh sync duration (F-21.5.3), by outcome.",
			Buckets: JobRunBuckets,
		}, []string{"outcome"}),
		SourceSyncInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_api_source_sync_in_flight",
			Help: "Source-refresh syncs currently in flight on this pod (F-21.5.3).",
		}),
		LeaderState: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: Prefix + "_api_leader_state",
			Help: "1 while this pod holds the source-refresh leader lease (F-21.5.3), 0 otherwise.",
		}),
		Restorations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_api_restorations_total",
			Help: "Resumption (rollback) initiations (F-22), by door and result.",
		}, []string{"door", "result"}),
	}
	reg.MustRegister(
		m.HTTPRequests, m.HTTPDuration, m.AuthFailures, m.WSClients, m.Workers,
		m.EventLogLag, m.JobStatus, m.JobDuration, m.ArtifactBytes,
		m.LogStreamBytes, m.TokensMinted, m.SecretsDecrypted, m.Notifications,
		m.SourceSyncs, m.SourceSyncDuration, m.SourceSyncInFlight, m.LeaderState,
		m.Restorations,
	)
	return m
}

// statusRecorder captures the response status code so the metrics middleware
// can label the request with it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Middleware wraps next (the API's inner HTTP mux) to record
// cdrom_api_http_requests_total and cdrom_api_http_request_duration_seconds
// (route = the mux pattern, not the raw path) and, for a 403 response,
// cdrom_api_auth_failures_total{forbidden}. It is applied inside the auth
// middleware, so it observes requests that passed authentication; a 401
// (rejected by the auth middleware) is counted separately by the outer
// auth-failure wrapper.
func (m *APIMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m == nil {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = r.URL.Path
		}
		m.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(rec.status)).Inc()
		m.HTTPDuration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
		if rec.status == http.StatusForbidden {
			m.AuthFailures.WithLabelValues("forbidden").Inc()
		}
	})
}

// AuthFailureMiddleware wraps next (the auth middleware) to count 401
// responses as cdrom_api_auth_failures_total{unauthenticated}. It is applied
// outside the auth middleware, so it sees the 401 the auth middleware returns
// for a missing or invalid token.
func (m *APIMetrics) AuthFailureMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m == nil {
			next.ServeHTTP(w, r)
			return
		}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		switch rec.status {
		case http.StatusUnauthorized:
			m.AuthFailures.WithLabelValues("unauthenticated").Inc()
		case http.StatusLocked:
			m.AuthFailures.WithLabelValues("lockout").Inc()
		}
	})
}

// SetWSClients sets the live WebSocket UI client gauge.
func (m *APIMetrics) SetWSClients(n int) {
	if m == nil {
		return
	}
	m.WSClients.Set(float64(n))
}

// UpdateWorkers recomputes the workers gauge from the given worker states. For
// each worker it counts the worker under (group, environment, state) for each
// environment it belongs to (or the empty environment when it belongs to
// none), so a worker in several environments is counted under each.
func (m *APIMetrics) UpdateWorkers(states []WorkerState) {
	if m == nil {
		return
	}
	counts := make(map[string]float64)
	for _, s := range states {
		envs := s.Environments
		if len(envs) == 0 {
			envs = []string{""}
		}
		for _, env := range envs {
			key := s.Group + "\x00" + env + "\x00" + s.State
			counts[key]++
		}
	}
	m.Workers.Reset()
	for key, n := range counts {
		group, env, state := splitWorkerKey(key)
		m.Workers.WithLabelValues(group, env, state).Set(n)
	}
}

func splitWorkerKey(key string) (group, env, state string) {
	parts := splitN(key, 3)
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}
	return key, "", ""
}

func splitN(s string, n int) []string {
	var out []string
	for i := 0; i < n-1 && s != ""; i++ {
		idx := indexByte(s, '\x00')
		if idx < 0 {
			out = append(out, s)
			s = ""
			break
		}
		out = append(out, s[:idx])
		s = s[idx+1:]
	}
	out = append(out, s)
	return out
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// SetEventLogLag sets the event-log tail lag gauge (F-23).
func (m *APIMetrics) SetEventLogLag(n int64) {
	if m == nil || n < 0 {
		return
	}
	m.EventLogLag.Set(float64(n))
}

// RecordJobStatus counts a job status transition as reported by a target.
func (m *APIMetrics) RecordJobStatus(status string) {
	if m == nil || status == "" {
		return
	}
	m.JobStatus.WithLabelValues(status).Inc()
}

// ObserveJobDuration observes a job's duration (start to finish) by terminal
// status.
func (m *APIMetrics) ObserveJobDuration(status string, d time.Duration) {
	if m == nil || status == "" || d < 0 {
		return
	}
	m.JobDuration.WithLabelValues(status).Observe(d.Seconds())
}

// AddArtifactBytes counts bytes moved through the artifact proxy (op: upload /
// download; kind: artifact / log).
func (m *APIMetrics) AddArtifactBytes(op, kind string, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.ArtifactBytes.WithLabelValues(op, kind).Add(float64(n))
}

// AddLogStreamBytes counts job log bytes persisted.
func (m *APIMetrics) AddLogStreamBytes(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.LogStreamBytes.Add(float64(n))
}

// RecordTokenMinted counts a job token minted (kind "job") or exchanged (kind
// "exchanged").
func (m *APIMetrics) RecordTokenMinted(kind string) {
	if m == nil {
		return
	}
	m.TokensMinted.WithLabelValues(kind).Inc()
}

// AddSecretsDecrypted counts secret decryptions at dispatch.
func (m *APIMetrics) AddSecretsDecrypted(n int) {
	if m == nil || n <= 0 {
		return
	}
	m.SecretsDecrypted.Add(float64(n))
}

// RecordNotification counts an email/webhook send (kind: email / webhook;
// outcome: sent / failed).
// RecordRestoration counts a resumption (rollback) initiation (F-22) by door
// (armed / manual) and result (initiated / cooldown / no_anchor / rearmed /
// invalid_cut_point / error).
func (m *APIMetrics) RecordRestoration(door, result string) {
	if m == nil {
		return
	}
	m.Restorations.WithLabelValues(door, result).Inc()
}

func (m *APIMetrics) RecordNotification(kind, outcome string) {
	if m == nil {
		return
	}
	m.Notifications.WithLabelValues(kind, outcome).Inc()
}
