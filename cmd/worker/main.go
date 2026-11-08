// Command worker is the Cdrom ephemeral worker.
//
// A worker is a short-lived process spawned in Kubernetes to execute a single
// one-off job, then terminate. It talks only to the API service. Must run on
// both Windows and Linux.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"cdrom/internal/worker"
	"cdrom/internal/config"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
	"cdrom/internal/metrics"
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
	if cfg.WorkerJobID == "" {
		logger.Error("worker: worker_job_id is required (config file or CDROM_WORKER_JOB_ID)")
		os.Exit(1)
	}
	jobID, err := strconv.ParseInt(cfg.WorkerJobID, 10, 64)
	if err != nil {
		logger.Error("worker: invalid worker_job_id", "value", cfg.WorkerJobID, "err", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// Metrics (F-20): the worker serves /metrics and /healthz on a dedicated
	// metrics port (separate from the API's service ports). When disabled
	// (metrics.enabled: false) the listener is not started and recording is a
	// no-op (the metric set is nil). The worker is short-lived, so the metrics
	// server lives only for the duration of the job.
	var workerMetrics *metrics.AgentMetrics
	metricsAddr := cfg.Metrics.Addr
	if metricsAddr == "" {
		metricsAddr = metrics.DefaultMetricsAddr("worker")
	}
	if cfg.Metrics.Enabled {
		collector := metrics.NewCollector()
		workerMetrics = metrics.NewAgentMetrics(collector.Registry)
		go func() {
			if err := collector.StartMetrics(ctx, metricsAddr); err != nil {
				logger.Error("metrics: start", "addr", metricsAddr, "err", err)
			}
		}()
		logger.Info("cdrom worker metrics starting", "addr", metricsAddr)
	}

	// F-30.3: the worker connects to the API WITHOUT a client certificate —
	// the target leg is TLS-with-server-verification + the BEARER job token
	// (the worker's identity is the job token the agent handed it, not a mesh
	// cert). Plaintext when the mesh has no CA (local dev).
	apiConn, err := grpcutil.DialNoClientCert(ctx, cfg.APIAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial api", "addr", cfg.APIAddress, "err", err)
		os.Exit(1)
	}
	defer apiConn.Close()

	a := worker.New(cfg.WorkerName, jobID, worker.Dependencies{
		API:      apipb.NewAPIClient(apiConn),
		AgentRoot:  cfg.AgentRoot,
	}, logger)
	// Attach the worker's Prometheus metric set (F-20) so it records its job,
	// steps, in-flight count, and log bytes.
	a.SetMetrics(workerMetrics)

	logger.Info("cdrom worker starting", "job", jobID, "name", cfg.WorkerName, "api", cfg.APIAddress)
	status, err := a.Run(ctx)
	if err != nil {
		logger.Error("worker: run", "err", err)
		os.Exit(1)
	}
	logger.Info("cdrom worker finished", "job", jobID, "status", fmt.Sprint(status))
	if status == dbpb.JobStatus_JOB_STATUS_FAILED || status == dbpb.JobStatus_JOB_STATUS_TIMED_OUT {
		os.Exit(1)
	}
}
