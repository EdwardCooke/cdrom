// Command api is the Cdrom API/controller service.
//
// It is the single control plane. It exposes the HTTP API consumed by the UI
// and a gRPC API consumed by execution targets (workers, agents) and the
// scheduler. It routes requests to the service layer (database, scheduler,
// artifacts) and holds the workers' WatchJobs streams. No business logic
// belongs here.
package main

import (
	"context"
	"net"
	"net/http"
	"os"

	"google.golang.org/grpc"

	"cdrom/internal/api"
	"cdrom/internal/auth"
	"cdrom/internal/config"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
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

	// The API's own gRPC listen address (dial target for workers, agents,
	// and the scheduler).
	grpcAddr := cfg.ListenAddress
	if grpcAddr == "" {
		grpcAddr = config.DefaultAPIAddress
	}
	// The API's HTTP listen address (for the UI).
	httpAddr := cfg.APIHTTPAddress
	if httpAddr == "" {
		httpAddr = config.DefaultAPIHTTPAddress
	}

	ctx := context.Background()
	dbConn, err := grpcutil.Dial(ctx, cfg.DBAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial database", "addr", cfg.DBAddress, "err", err)
		os.Exit(1)
	}
	defer dbConn.Close()
	schedConn, err := grpcutil.Dial(ctx, cfg.SchedulerAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial scheduler", "addr", cfg.SchedulerAddress, "err", err)
		os.Exit(1)
	}
	defer schedConn.Close()
	artConn, err := grpcutil.Dial(ctx, cfg.ArtifactsAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial artifacts", "addr", cfg.ArtifactsAddress, "err", err)
		os.Exit(1)
	}
	defer artConn.Close()

	clients := api.Clients{
		Database:  dbpb.NewDatabaseClient(dbConn),
		Scheduler: schedpb.NewSchedulerClient(schedConn),
		Artifacts: artifactspb.NewArtifactsClient(artConn),
	}

	// Event hub backing the /api/ws WebSocket endpoint; both the HTTP server
	// (snapshot + stream) and the gRPC server (publish) share it.
	hub := api.NewEventHub()

	// HTTP client for talking to the IdP (OIDC discovery, code exchange, and
	// job-token minting). When TLS is configured it trusts the shared CA and
	// presents the API's client certificate, so it can reach an IdP that
	// serves TLS and authenticate to its job-token endpoint.
	idpClient, err := grpcutil.HTTPClient(cfg.TLS)
	if err != nil {
		logger.Error("idp: http client", "err", err)
		os.Exit(1)
	}

	// Authentication for the UI-facing HTTP surface. Disabled by default; when
	// enabled it performs OIDC discovery (a network call) here. The API is a
	// pure token verifier: clients present the OAuth token they obtained from
	// the IdP as `Authorization: Bearer <token>`.
	authBundle, err := auth.NewWithClient(ctx, cfg.Auth, idpClient)
	if err != nil {
		logger.Error("auth: init", "err", err)
		os.Exit(1)
	}

	// The API's HTTP handler, wrapped by the auth middleware (which verifies
	// the Bearer token when auth is enabled). The /api/auth/oidc discovery
	// endpoint is registered on a parent mux so it is reachable without a
	// token — a client needs it to start the sign-in flow. Go's ServeMux
	// prefers the more specific /api/auth/oidc pattern over the /api/ subtree
	// handler.
	apiHandler := authBundle.Middleware(api.New(clients, hub).Handler())
	mux := http.NewServeMux()
	mux.Handle("/api/", apiHandler)
	mux.HandleFunc("GET /api/auth/oidc", authBundle.ServeDiscovery)

	// HTTP server (UI).
	httpSrv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		logger.Info("cdrom api http starting", "addr", httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http serve", "err", err)
		}
	}()

	// Job-token auth for the gRPC surface (workers, agents). Disabled by
	// default; when enabled the API mints job tokens via the IdP (the only
	// caller of its job-token endpoint, authenticated with its mTLS client
	// certificate) and verifies them on job-scoped calls. It performs OIDC
	// discovery against the IdP.
	var jobAuth *api.JobTokenAuth
	if cfg.GRPCAuth.Enabled {
		jobAuth, err = api.NewJobTokenAuth(ctx, cfg.GRPCAuth, idpClient, logger)
		if err != nil {
			logger.Error("grpc_auth: init", "err", err)
			os.Exit(1)
		}
	}

	// gRPC server (workers, agents, scheduler).
	creds, err := grpcutil.ServerCreds(cfg.TLS)
	if err != nil {
		logger.Error("tls: server creds", "err", err)
		os.Exit(1)
	}
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("listen", "addr", grpcAddr, "err", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer(grpc.Creds(creds))
	grpcServer := api.NewGRPCServer(clients.Database, clients.Artifacts, hub, jobAuth, logger)
	apipb.RegisterAPIServer(grpcSrv, grpcServer)

	// Event-log tail loop (F-23): tails the shared event log and fans new
	// events out to this pod's local workers and UI clients, so a job
	// published by any pod reaches the workers this pod holds streams for. It
	// also flushes this pod's coalesced job_log_updated events to the shared
	// log. It runs until the gRPC server shuts down (signal received).
	tailCtx, stopTail := context.WithCancel(ctx)
	grpcServer.StartEventLogTail(tailCtx)

	logger.Info("cdrom api starting", "grpc", grpcAddr, "http", httpAddr,
		"db", cfg.DBAddress, "scheduler", cfg.SchedulerAddress, "artifacts", cfg.ArtifactsAddress,
		"auth", authBundle.Enabled(), "grpc_auth", jobAuth.Enabled())
	if err := grpcutil.Serve(lis, grpcSrv, logger); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
	// gRPC server shut down (signal received); stop the tail loop and the
	// HTTP server too.
	stopTail()
	_ = httpSrv.Shutdown(context.Background())
}
