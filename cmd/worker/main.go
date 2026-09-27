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

	apiConn, err := grpcutil.Dial(ctx, cfg.APIAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial api", "addr", cfg.APIAddress, "err", err)
		os.Exit(1)
	}
	defer apiConn.Close()

	w := worker.New(cfg.WorkerName, cfg.WorkerGroup, worker.Dependencies{
		API: apipb.NewAPIClient(apiConn),
	}, logger)

	logger.Info("cdrom worker starting", "name", cfg.WorkerName, "group", cfg.WorkerGroup,
		"api", cfg.APIAddress)
	if err := w.Run(ctx); err != nil {
		logger.Error("worker: run", "err", err)
		os.Exit(1)
	}
	logger.Info("cdrom worker stopped")
}
