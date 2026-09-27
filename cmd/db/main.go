// Command db is the Cdrom database service.
//
// It owns the storage backend (SQLite in development, PostgreSQL in
// deployment) and exposes all persisted pipeline data over gRPC. Every other
// component reads and writes data exclusively through this service.
package main

import (
	"net"
	"os"

	"google.golang.org/grpc"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
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
	dbpb.RegisterDatabaseServer(srv, database.NewServer(db))

	logger.Info("cdrom db service starting", "addr", addr, "backend", cfg.DB.Backend)
	if err := grpcutil.Serve(lis, srv, logger); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
