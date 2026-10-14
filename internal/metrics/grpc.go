package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// StatsHandler is a gRPC server-side stats.Handler that records, on every
// service server (including streaming RPCs such as WatchJobs and
// StreamJobLogs):
//
//   - cdrom_grpc_server_started_total{method} — RPCs started;
//   - cdrom_grpc_server_handled_total{method,code} — RPCs completed, by gRPC
//     status code;
//   - cdrom_grpc_server_handling_seconds{method} — RPC duration.
//
// The grpc_prometheus interceptor is deprecated, so this small handler is the
// supported way to instrument the gRPC surface. It is attached to a gRPC
// server with grpc.StatsHandler(metrics.NewStatsHandler(reg)).
//
// The method label is the RPC's full method with the leading slash stripped
// (e.g. cdrom.api.v1.API/WatchJobs) — a low-cardinality, bounded set. The
// code label is the gRPC status code name (e.g. OK, Unauthenticated).
type StatsHandler struct {
	started    *prometheus.CounterVec
	handled    *prometheus.CounterVec
	processing *prometheus.HistogramVec
	// extra, when set, is an additional per-method histogram (e.g. the
	// database service's cdrom_db_rpc_duration_seconds) that is observed on
	// every RPC in addition to the shared handling histogram.
	extra *prometheus.HistogramVec
}

// NewStatsHandler builds a StatsHandler that records into reg.
func NewStatsHandler(reg prometheus.Registerer) *StatsHandler {
	return NewStatsHandlerExtra(reg, nil)
}

// NewStatsHandlerExtra builds a StatsHandler that records into reg and, when
// extra is non-nil, also observes extra (a per-method histogram) on every
// RPC. The database service uses it to record cdrom_db_rpc_duration_seconds
// alongside the shared cdrom_grpc_server_* metrics.
func NewStatsHandlerExtra(reg prometheus.Registerer, extra *prometheus.HistogramVec) *StatsHandler {
	h := &StatsHandler{
		started: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_grpc_server_started_total",
			Help: "gRPC server RPCs started, by method.",
		}, []string{"method"}),
		handled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: Prefix + "_grpc_server_handled_total",
			Help: "gRPC server RPCs completed, by method and status code.",
		}, []string{"method", "code"}),
		processing: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    Prefix + "_grpc_server_handling_seconds",
			Help:    "gRPC server RPC duration, by method.",
			Buckets: RequestBuckets,
		}, []string{"method"}),
		extra: extra,
	}
	reg.MustRegister(h.started, h.handled, h.processing)
	if extra != nil {
		reg.MustRegister(extra)
	}
	return h
}

type rpcMethodKey struct{}

// TagRPC implements stats.Handler: it records the RPC's method name in the
// context so HandleRPC can label the metrics with it.
func (h *StatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	method := info.FullMethodName
	if len(method) > 0 && method[0] == '/' {
		method = method[1:]
	}
	return context.WithValue(ctx, rpcMethodKey{}, method)
}

// HandleRPC implements stats.Handler: it records the started counter on the
// RPC's Begin event and the handled counter and handling histogram on its End
// event. Client-side events are ignored (the handler is server-side).
func (h *StatsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if s.IsClient() {
		return
	}
	method, _ := ctx.Value(rpcMethodKey{}).(string)
	switch e := s.(type) {
	case *stats.Begin:
		h.started.WithLabelValues(method).Inc()
	case *stats.End:
		code := codes.OK
		if e.Error != nil {
			if st, _ := status.FromError(e.Error); st != nil {
				code = st.Code()
			}
		}
		h.handled.WithLabelValues(method, code.String()).Inc()
		if !e.BeginTime.IsZero() {
			d := e.EndTime.Sub(e.BeginTime).Seconds()
			h.processing.WithLabelValues(method).Observe(d)
			if h.extra != nil {
				h.extra.WithLabelValues(method).Observe(d)
			}
		}
	}
}

// TagConn implements stats.Handler: connection events are not recorded, so the
// context is returned unchanged.
func (h *StatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn implements stats.Handler: connection events are not recorded.
func (h *StatsHandler) HandleConn(_ context.Context, _ stats.ConnStats) {}
