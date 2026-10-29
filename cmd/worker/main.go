// Command worker is the Cdrom long-lived worker.
//
// A worker is a resident process on a deployment target. Workers belong to a
// group; jobs can be dispatched to workers by group name. A worker talks
// only to the API service. Must run on both Windows and Linux.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"cdrom/internal/config"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
	"cdrom/internal/metrics"
	"cdrom/internal/worker"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Metrics (F-20): the worker serves /metrics and /healthz on a dedicated
	// metrics port (separate from the API's service ports). When disabled
	// (metrics.enabled: false) the listener is not started and recording is a
	// no-op (the metric set is nil).
	var workerMetrics *metrics.WorkerMetrics
	metricsAddr := cfg.Metrics.Addr
	if metricsAddr == "" {
		metricsAddr = metrics.DefaultMetricsAddr("worker")
	}
	if cfg.Metrics.Enabled {
		collector := metrics.NewCollector()
		workerMetrics = metrics.NewWorkerMetrics(collector.Registry)
		go func() {
			if err := collector.StartMetrics(ctx, metricsAddr); err != nil {
				logger.Error("metrics: start", "addr", metricsAddr, "err", err)
			}
		}()
		logger.Info("cdrom worker metrics starting", "addr", metricsAddr)
	}

	apiConn, err := grpcutil.Dial(ctx, cfg.APIAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial api", "addr", cfg.APIAddress, "err", err)
		os.Exit(1)
	}
	defer apiConn.Close()

	w := worker.New(cfg.WorkerName, cfg.WorkerGroup, cfg.WorkerEnvironments, cfg.WorkerTags, worker.Dependencies{
		API:      apipb.NewAPIClient(apiConn),
		WorkRoot: cfg.WorkRoot,
	}, logger)
	// Attach the worker's Prometheus metric set (F-20) so it records its jobs,
	// steps, in-flight count, watch state, polls, and heartbeat failures.
	w.SetMetrics(workerMetrics)

	logger.Info("cdrom worker starting", "name", cfg.WorkerName, "group", cfg.WorkerGroup,
		"environments", cfg.WorkerEnvironments, "tags", cfg.WorkerTags, "api", cfg.APIAddress)
	if err := w.Run(ctx); err != nil {
		logger.Error("worker: run", "err", err)
		os.Exit(1)
	}
	logger.Info("cdrom worker stopped")
}
