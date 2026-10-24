// Command pipelinesource is the Cdrom source-fetcher service (F-21.2).
//
// It performs the actual fetch of a pipeline definition file named by a
// reference (a "<type>:<spec>" string — git / http / file) and exposes it over
// gRPC (FetchSource). It is dialed only by the API. It holds no pipeline
// definitions, no credentials, and no encryption key at rest; its only on-disk
// state is a transient git repository cache (F-21.2.3).
package main

import (
	"context"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"

	"cdrom/internal/config"
	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
	"cdrom/internal/metrics"
	"cdrom/internal/services/pipelinesource"
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
		addr = config.DefaultPipelineSourceAddress
	}

	ctx := context.Background()

	// Metrics (F-20): the source-fetcher serves /metrics and /healthz on a
	// dedicated metrics port (separate from its gRPC port). When disabled
	// (metrics.enabled: false) the listener is not started and recording is a
	// no-op (the metric set is nil).
	var psMetrics *metrics.PipelineSourceMetrics
	var psStatsHandler stats.Handler
	metricsAddr := cfg.Metrics.Addr
	if metricsAddr == "" {
		metricsAddr = metrics.DefaultMetricsAddr("pipelinesource")
	}
	if cfg.Metrics.Enabled {
		collector := metrics.NewCollector()
		psMetrics = metrics.NewPipelineSourceMetrics(collector.Registry)
		psStatsHandler = metrics.NewStatsHandler(collector.Registry)
		go func() {
			if err := collector.StartMetrics(ctx, metricsAddr); err != nil {
				logger.Error("metrics: start", "addr", metricsAddr, "err", err)
			}
		}()
		logger.Info("cdrom pipelinesource metrics starting", "addr", metricsAddr)
	}

	srvImpl, err := pipelinesource.NewServer(cfg.PipelineSourceRepoCacheDir, cfg.PipelineSourceFileRoot, logger)
	if err != nil {
		logger.Error("pipelinesource: init", "err", err)
		os.Exit(1)
	}
	srvImpl.SetMetrics(psMetrics)
	stopCleanup := srvImpl.StartCleanup(ctx)
	defer stopCleanup()

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
	if psStatsHandler != nil {
		srvOpts = append(srvOpts, grpc.StatsHandler(psStatsHandler))
	}
	srv := grpc.NewServer(srvOpts...)
	pipelinesourcepb.RegisterPipelineSourceServer(srv, srvImpl)

	logger.Info("cdrom pipelinesource service starting", "addr", addr, "cache_root", srvImpl.CacheRoot(), "file_root", cfg.PipelineSourceFileRoot)
	if err := grpcutil.Serve(lis, srv, logger); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
