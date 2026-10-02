// Command scheduler is the Cdrom scheduler service.
//
// It manages job lifecycle and dispatch. When a job targets a group of
// long-lived workers, the scheduler pushes it to the API, which fans it out
// to the live workers in that group. Jobs with an empty target group are
// queued for ephemeral Kubernetes agents. All durable state is persisted
// through the Database service.
package main

import (
	"context"
	"net"
	"os"

	"google.golang.org/grpc"

	"cdrom/internal/config"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
	"cdrom/internal/services/scheduler"
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
		addr = config.DefaultSchedulerAddress
	}

	ctx := context.Background()
	dbConn, err := grpcutil.Dial(ctx, cfg.DBAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial database service", "addr", cfg.DBAddress, "err", err)
		os.Exit(1)
	}
	defer dbConn.Close()
	apiConn, err := grpcutil.Dial(ctx, cfg.APIAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial api service", "addr", cfg.APIAddress, "err", err)
		os.Exit(1)
	}
	defer apiConn.Close()

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
	sched := scheduler.NewServer(
		dbpb.NewDatabaseClient(dbConn), apipb.NewAPIClient(apiConn), logger)
	// Start the job-timeout watchdog (F-03): it reaps running jobs that have
	// exceeded their declared timeout even if the execution target goes
	// silent. It runs until the process shuts down.
	watchdogCtx, stopWatchdog := context.WithCancel(ctx)
	defer stopWatchdog()
	sched.StartWatchdog(watchdogCtx)
	// Start the job-retry loop (F-04): it re-dispatches failed jobs that
	// still have retries remaining, after their policy's backoff. It runs
	// until the process shuts down.
	retryCtx, stopRetry := context.WithCancel(ctx)
	defer stopRetry()
	sched.StartRetryLoop(retryCtx)
	// Start the job-dependency resolver (F-06): it skips a pending job whose
	// dependency did not succeed, and dispatches a pending job once every
	// dependency has succeeded. It runs until the process shuts down.
	dependencyCtx, stopDependencyResolver := context.WithCancel(ctx)
	defer stopDependencyResolver()
	sched.StartDependencyResolver(dependencyCtx)
	srv := grpc.NewServer(grpc.Creds(creds))
	schedpb.RegisterSchedulerServer(srv, sched)

	logger.Info("cdrom scheduler service starting", "addr", addr, "db", cfg.DBAddress, "api", cfg.APIAddress)
	if err := grpcutil.Serve(lis, srv, logger); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
