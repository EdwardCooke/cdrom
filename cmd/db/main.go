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
	// The Database service owns the concurrency slot accounting (F-16): it
	// enforces the global, per-group, and per-key limits on the scheduler's
	// dispatch path. The limits are stored as regex-based rules in the
	// database (the ConcurrencyLimit table), not in configuration, so they can
	// be edited at runtime via the API without a redeployment.
	server := database.NewServer(db)
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
	srv := grpc.NewServer(grpc.Creds(creds))
	dbpb.RegisterDatabaseServer(srv, server)

	logger.Info("cdrom db service starting", "addr", addr, "backend", cfg.DB.Backend)
	if err := grpcutil.Serve(lis, srv, logger); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
