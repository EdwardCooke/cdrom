// Command db is the Cdrom database service.
//
// It owns the storage backend (SQLite in development, PostgreSQL in
// deployment) and exposes all persisted pipeline data over gRPC. Every other
// component reads and writes data exclusively through this service.
package main

import (
	"context"
	"net"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
	"cdrom/internal/metrics"
	"cdrom/internal/services/database"
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
		addr = config.DefaultDBAddress
	}

	ctx := context.Background()

	db, err := database.Open(cfg.DB, logger)
	if err != nil {
		logger.Error("database: open", "err", err)
		os.Exit(1)
	}
	defer database.Close(db)
	if err := database.Migrate(db, logger); err != nil {
		logger.Error("database: migrate", "err", err)
		os.Exit(1)
	}
	// The Database service owns the concurrency slot accounting (F-16): it
	// enforces the global, per-group, and per-key limits on the scheduler's
	// dispatch path. The limits are stored as regex-based rules in the
	// database (the ConcurrencyLimit table), not in configuration, so they can
	// be edited at runtime via the API without a redeployment.
	server := database.NewServer(db)

	// Metrics (F-20): the Database service serves /metrics and /healthz on a
	// dedicated metrics port (separate from its gRPC port). When disabled
	// (metrics.enabled: false) the listener is not started, the global-state
	// gauges are not refreshed, and recording is a no-op (the metric set is
	// nil). The per-RPC latency is recorded by the shared gRPC StatsHandler,
	// which also records the database's own cdrom_db_rpc_duration_seconds.
	var dbMetrics *metrics.DatabaseMetrics
	var dbStatsHandler stats.Handler
	metricsAddr := cfg.Metrics.Addr
	if metricsAddr == "" {
		metricsAddr = metrics.DefaultMetricsAddr("database")
	}
	if cfg.Metrics.Enabled {
		collector := metrics.NewCollector()
		dbMetrics = metrics.NewDatabaseMetrics(collector.Registry)
		dbRPCDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    metrics.Prefix + "_db_rpc_duration_seconds",
			Help:    "Database service RPC duration, by method.",
			Buckets: metrics.RequestBuckets,
		}, []string{"method"})
		dbStatsHandler = metrics.NewStatsHandlerExtra(collector.Registry, dbRPCDuration)
		go func() {
			if err := collector.StartMetrics(ctx, metricsAddr); err != nil {
				logger.Error("metrics: start", "addr", metricsAddr, "err", err)
			}
		}()
		logger.Info("cdrom db metrics starting", "addr", metricsAddr)
	}
	server.SetMetrics(dbMetrics)
	// Refresh the global-state gauges (event-log size, queue depth,
	// concurrency slots) on a short interval. It is a no-op when metrics are
	// disabled.
	server.StartMetricsLoop(ctx)

	// Seed the built-in roles (F-14, RBAC) into the role catalog so every API
	// replica sees the same built-in roles (admin, operator, viewer, user).
	// Idempotent: it only inserts a built-in role that is not already present.
	if err := server.SeedBuiltInRoles(context.Background()); err != nil {
		logger.Error("database: seed built-in roles", "err", err)
		os.Exit(1)
	}

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
	if dbStatsHandler != nil {
		srvOpts = append(srvOpts, grpc.StatsHandler(dbStatsHandler))
	}
	srv := grpc.NewServer(srvOpts...)
	dbpb.RegisterDatabaseServer(srv, server)

	logger.Info("cdrom db service starting", "addr", addr, "backend", cfg.DB.Backend)
	if err := grpcutil.Serve(lis, srv, logger); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
