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
	srv := grpc.NewServer(grpc.Creds(creds))
	schedpb.RegisterSchedulerServer(srv, scheduler.NewServer(
		dbpb.NewDatabaseClient(dbConn), apipb.NewAPIClient(apiConn), logger))

	logger.Info("cdrom scheduler service starting", "addr", addr, "db", cfg.DBAddress, "api", cfg.APIAddress)
	if err := grpcutil.Serve(lis, srv, logger); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
