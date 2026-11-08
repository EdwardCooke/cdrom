// Command agent is the Cdrom long-lived agent.
//
// An agent is a resident process on a deployment target. Agents belong to a
// group; jobs can be dispatched to agents by group name. An agent talks
// only to the API service. Must run on both Windows and Linux.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"cdrom/internal/config"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/agent"
	"cdrom/internal/credfile"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Metrics (F-20): the agent serves /metrics and /healthz on a dedicated
	// metrics port (separate from the API's service ports). When disabled
	// (metrics.enabled: false) the listener is not started and recording is a
	// no-op (the metric set is nil).
	var agentMetrics *metrics.AgentMetrics
	metricsAddr := cfg.Metrics.Addr
	if metricsAddr == "" {
		metricsAddr = metrics.DefaultMetricsAddr("agent")
	}
	if cfg.Metrics.Enabled {
		collector := metrics.NewCollector()
		agentMetrics = metrics.NewAgentMetrics(collector.Registry)
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

	// F-30.3: the agent's CREDENTIAL is SOURCED FROM ITS CREDENTIAL FILE —
	// the file is the ONLY legal source (never the environment: process-
	// listing exposure + inheritance into worker subprocesses, F-30.4).
	// A MISSING or UNPARSABLE file is a BOOT REFUSAL with a diagnostic
	// (non-zero exit; the process supervisor — systemd / Windows service /
	// k8s — restarts the agent, which SETTLES once a valid credential
	// exists). Once booted, the read is LAZY (consulted on every identity
	// call), so a rotated or late-arriving token is adopted without a
	// restart.
	credPath := cfg.CredentialFile
	if credPath == "" {
		logger.Error("agent: no credential file configured; refusing to start",
			"hint", "set credential.file (CDROM_AGENT_CREDENTIAL_FILE) to the agent's credential file")
		os.Exit(1)
	}
	if _, err := credfile.Read(credPath); err != nil {
		logger.Error("agent: cannot read credential file; refusing to start", "file", credPath, "err", err)
		os.Exit(1)
	}
	credentialSupplier := func() (string, error) { return credfile.Read(credPath) }

	w := agent.New(cfg.AgentName, cfg.AgentGroup, cfg.AgentEnvironments, cfg.AgentLabels, agent.Dependencies{
		API:                apipb.NewAPIClient(apiConn),
		AgentRoot:          cfg.AgentRoot,
		CredentialSupplier: credentialSupplier,
	}, logger)
	// Attach the agent's Prometheus metric set (F-20) so it records its jobs,
	// steps, in-flight count, watch state, polls, and heartbeat failures.
	w.SetMetrics(agentMetrics)

	logger.Info("cdrom agent starting", "name", cfg.AgentName, "group", cfg.AgentGroup,
		"environments", cfg.AgentEnvironments, "labels", cfg.AgentLabels, "api", cfg.APIAddress)
	if err := w.Run(ctx); err != nil {
		logger.Error("agent: run", "err", err)
		os.Exit(1)
	}
	logger.Info("cdrom agent stopped")
}
