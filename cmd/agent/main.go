// Command agent is the Cdrom ephemeral agent.
//
// An agent is a short-lived process spawned in Kubernetes to execute a single
// one-off job, then terminate. It talks only to the API service. Must run on
// both Windows and Linux.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"cdrom/internal/agent"
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
	if cfg.AgentJobID == "" {
		logger.Error("agent: agent_job_id is required (config file or CDROM_AGENT_JOB_ID)")
		os.Exit(1)
	}
	jobID, err := strconv.ParseInt(cfg.AgentJobID, 10, 64)
	if err != nil {
		logger.Error("agent: invalid agent_job_id", "value", cfg.AgentJobID, "err", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// Metrics (F-20): the agent serves /metrics and /healthz on a dedicated
	// metrics port (separate from the API's service ports). When disabled
	// (metrics.enabled: false) the listener is not started and recording is a
	// no-op (the metric set is nil). The agent is short-lived, so the metrics
	// server lives only for the duration of the job.
	var agentMetrics *metrics.WorkerMetrics
	metricsAddr := cfg.Metrics.Addr
	if metricsAddr == "" {
		metricsAddr = metrics.DefaultMetricsAddr("agent")
	}
	if cfg.Metrics.Enabled {
		collector := metrics.NewCollector()
		agentMetrics = metrics.NewWorkerMetrics(collector.Registry)
		go func() {
			if err := collector.StartMetrics(ctx, metricsAddr); err != nil {
				logger.Error("metrics: start", "addr", metricsAddr, "err", err)
			}
		}()
		logger.Info("cdrom agent metrics starting", "addr", metricsAddr)
	}

	apiConn, err := grpcutil.Dial(ctx, cfg.APIAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial api", "addr", cfg.APIAddress, "err", err)
		os.Exit(1)
	}
	defer apiConn.Close()

	a := agent.New(cfg.AgentName, jobID, agent.Dependencies{
		API:      apipb.NewAPIClient(apiConn),
		WorkerRoot: cfg.WorkerRoot,
	}, logger)
	// Attach the agent's Prometheus metric set (F-20) so it records its job,
	// steps, in-flight count, and log bytes.
	a.SetMetrics(agentMetrics)

	logger.Info("cdrom agent starting", "job", jobID, "name", cfg.AgentName, "api", cfg.APIAddress)
	status, err := a.Run(ctx)
	if err != nil {
		logger.Error("agent: run", "err", err)
		os.Exit(1)
	}
	logger.Info("cdrom agent finished", "job", jobID, "status", fmt.Sprint(status))
	if status == dbpb.JobStatus_JOB_STATUS_FAILED || status == dbpb.JobStatus_JOB_STATUS_TIMED_OUT {
		os.Exit(1)
	}
}
