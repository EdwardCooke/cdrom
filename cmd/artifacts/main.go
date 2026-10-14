// Command artifacts is the Cdrom artifacts service.
//
// It stores and retrieves job artifacts and job logs and exposes them over
// gRPC with streamed uploads and downloads. The storage backend is
// configurable (artifacts_store / CDROM_ARTIFACTS_STORE); the built-in
// default is the local filesystem.
package main

import (
	"context"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"

	"cdrom/internal/config"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
	"cdrom/internal/metrics"
	"cdrom/internal/services/artifacts"
)

func main() {
	logger := logging.New()

	configFile, err := config.ParseFlags(os.Args[1:])
	if err != nil {
		logger.Error("config: parse flags", "err", err)
		os.Exit(1)
	}
	cfg, err := config.LoadWithFile(configFile)
	if err != nil {
		logger.Error("config: invalid", "err", err)
		os.Exit(1)
	}
	addr := cfg.ListenAddress
	if addr == "" {
		addr = config.DefaultArtifactsAddress
	}

	ctx := context.Background()

	// Metrics (F-20): the Artifacts service serves /metrics and /healthz on a
	// dedicated metrics port (separate from its gRPC port). When disabled
	// (metrics.enabled: false) the listener is not started and recording is a
	// no-op (the metric set is nil).
	var artMetrics *metrics.ArtifactsMetrics
	var artStatsHandler stats.Handler
	metricsAddr := cfg.Metrics.Addr
	if metricsAddr == "" {
		metricsAddr = metrics.DefaultMetricsAddr("artifacts")
	}
	if cfg.Metrics.Enabled {
		collector := metrics.NewCollector()
		artMetrics = metrics.NewArtifactsMetrics(collector.Registry)
		artStatsHandler = metrics.NewStatsHandler(collector.Registry)
		go func() {
			if err := collector.StartMetrics(ctx, metricsAddr); err != nil {
				logger.Error("metrics: start", "addr", metricsAddr, "err", err)
			}
		}()
		logger.Info("cdrom artifacts metrics starting", "addr", metricsAddr)
	}

	store, err := artifacts.NewStore(artifacts.StoreKind(cfg.ArtifactsStore), cfg.ArtifactsRoot)
	if err != nil {
		logger.Error("artifacts: init store", "store", cfg.ArtifactsStore, "err", err)
		os.Exit(1)
	}
	srvImpl := artifacts.NewServer(store)
	// Attach the Artifacts service's Prometheus metric set (F-20) so the
	// operations record their counters, bytes, and durations.
	srvImpl.SetMetrics(artMetrics)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("listen", "addr", addr, "err", err)
		os.Exit(1)
	}

	creds, err := grpcutil.ServerCreds(cfg.TLS)
	if err != nil {
		logger.Error("tls: server creds", "err", err)
		os.Exit(1)
	}
	srvOpts := []grpc.ServerOption{grpc.Creds(creds)}
	if artStatsHandler != nil {
		srvOpts = append(srvOpts, grpc.StatsHandler(artStatsHandler))
	}
	srv := grpc.NewServer(srvOpts...)
	artifactspb.RegisterArtifactsServer(srv, srvImpl)

	logger.Info("cdrom artifacts service starting", "addr", addr, "store", cfg.ArtifactsStore, "root", cfg.ArtifactsRoot)
	if err := grpcutil.Serve(lis, srv, logger); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
