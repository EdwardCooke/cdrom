// Command artifacts is the Cdrom artifacts service.
//
// It stores and retrieves job artifacts on the local filesystem and exposes
// them over gRPC with streamed uploads and downloads.
package main

import (
	"net"
	"os"

	"google.golang.org/grpc"

	"cdrom/internal/config"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
	"cdrom/internal/services/artifacts"
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
		addr = config.DefaultArtifactsAddress
	}

	srvImpl, err := artifacts.NewServer(cfg.ArtifactsRoot)
	if err != nil {
		logger.Error("artifacts: init", "err", err)
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
	artifactspb.RegisterArtifactsServer(srv, srvImpl)

	logger.Info("cdrom artifacts service starting", "addr", addr, "root", cfg.ArtifactsRoot)
	if err := grpcutil.Serve(lis, srv, logger); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
