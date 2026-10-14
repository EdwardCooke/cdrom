package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Prefix is the prefix every cdrom metric name carries.
const Prefix = "cdrom"

// Collector is the shared metrics plumbing for one component: a self-contained
// prometheus.Registry (with the Go runtime and process collectors registered)
// plus a promhttp /metrics handler and a /healthz handler. It also exposes the
// two process-wide gauges every component serves: cdrom_up (1 while the
// process is serving) and cdrom_build_info (1, labeled by version and commit).
//
// A component constructs exactly one Collector at startup. Its per-component
// metric set (see api.go, scheduler.go, ...) is built against the Collector's
// Registry, so a component's metric set is self-contained and a test can use a
// fresh Collector.
type Collector struct {
	Registry *prometheus.Registry
	Handler  http.Handler

	up        prometheus.Gauge
	buildInfo *prometheus.GaugeVec
}

// NewCollector builds a Collector with a fresh registry. The Go runtime
// (go_*) and process (process_*) collectors are registered into the same
// registry, so process health (goroutines, heap, GC, CPU, file descriptors)
// comes for free. cdrom_up and cdrom_build_info are registered and set.
func NewCollector() *Collector {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	c := &Collector{
		Registry:  reg,
		Handler:   promhttp.HandlerFor(reg, promhttp.HandlerOpts{}),
		up:        prometheus.NewGauge(prometheus.GaugeOpts{Name: Prefix + "_up", Help: "1 while the process is serving."}),
		buildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: Prefix + "_build_info", Help: "Build information, labeled by version and commit."}, []string{"version", "commit"}),
	}
	reg.MustRegister(c.up, c.buildInfo)
	c.up.Set(1)
	version, commit := buildInfo()
	c.buildInfo.WithLabelValues(version, commit).Set(1)
	return c
}

// buildInfo returns the build's version and commit, from runtime/debug's build
// info. When the binary was not built with module version info (the common
// case for a local build) both fall back to "unknown".
func buildInfo() (version, commit string) {
	version, commit = "unknown", "unknown"
	if bi, ok := debug.ReadBuildInfo(); ok {
		if bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			version = bi.Main.Version
		}
		for _, setting := range bi.Settings {
			if setting.Key == "vcs.revision" {
				commit = setting.Value
				break
			}
		}
	}
	return version, commit
}

// Healthz is the /healthz handler: it reports 200 while the process is
// serving. It is served on the dedicated metrics port (not behind the auth
// middleware), so a probe needs no token.
func (c *Collector) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// StartMetrics starts a small HTTP server on addr serving only /metrics and
// /healthz, and blocks until ctx is cancelled (then it shuts the server down).
// It is the dedicated metrics port: separate from the component's service
// ports, so a deployment can firewall the service ports (gRPC, the UI HTTP,
// the IdP's OIDC surface) and expose only this port to a scraper. The metrics
// port is not behind the auth middleware — a scraper needs no token — which is
// safe precisely because it is a separate, low-trust surface.
//
// It returns the listener's error (e.g. the port is already in use) before
// serving begins, so a caller can fail fast. It returns nil once the server
// has shut down cleanly on ctx cancellation.
func (c *Collector) StartMetrics(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", c.Handler)
	mux.HandleFunc("/healthz", c.Healthz)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", addr, err)
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// The server stopped on its own (not a clean shutdown); surface it
			// so the process can react. A clean shutdown returns ErrServerClosed.
			fmt.Printf("metrics: serve %s: %v\n", addr, err)
		}
	}()
	return nil
}

// DefaultMetricsAddr returns the component's default dedicated metrics address
// (one per component, 127.0.0.1 for local development). It is used when the
// config's metrics.addr is empty.
func DefaultMetricsAddr(component string) string {
	switch component {
	case "worker", "agent":
		return "127.0.0.1:9100"
	case "database":
		return "127.0.0.1:9101"
	case "scheduler":
		return "127.0.0.1:9102"
	case "artifacts":
		return "127.0.0.1:9103"
	case "api":
		return "127.0.0.1:9104"
	case "idp":
		return "127.0.0.1:9105"
	default:
		return "127.0.0.1:9100"
	}
}
