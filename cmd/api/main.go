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
	"cdrom/internal/authz"
	"cdrom/internal/config"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
	"cdrom/internal/secrets"
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

	// The secret store (F-12): encrypts a pipeline's secret plaintexts on
	// create/update and decrypts them at dispatch. The built-in AES store is
	// always available: when no key is configured it falls back to an all-zero
	// key, so secrets can be exercised in test / local-dev scenarios without a
	// real key (the zero key provides no real security). It draws each nonce
	// from a counter kept in the database (via the Database service's
	// NextSecretNonce RPC), so every API replica shares one sequence and can
	// never reuse a nonce.
	secretStore, err := secrets.NewStore(cfg.Secrets, &dbNonceSource{db: clients.Database})
	if err != nil {
		logger.Error("secrets: init", "err", err)
		os.Exit(1)
	}
	if cfg.Secrets.Key == "" {
		logger.Error("================================================================")
		logger.Error("SECURITY WARNING: NO SECRETS ENCRYPTION KEY IS CONFIGURED")
		logger.Error("================================================================")
		logger.Error("The API is encrypting pipeline secrets with an ALL-ZERO key.")
		logger.Error("This provides NO real security: anyone who can read the stored")
		logger.Error("ciphertext can decrypt every secret. Set CDROM_SECRETS_KEY (or the")
		logger.Error("'secrets.key' config) to a real 32-byte base64 AES-256 key before")
		logger.Error("running in production. Generate one with: openssl rand -base64 32")
		logger.Error("================================================================")
	}

	// Event hub backing the /api/ws WebSocket endpoint; both the HTTP server
	// (snapshot + stream) and the gRPC server (publish) share it.
	hub := api.NewEventHub()

	// HTTP client for talking to the IdP's OIDC surface (discovery and code
	// exchange). When TLS is configured it trusts the shared CA and presents
	// the API's client certificate, so it can reach an IdP that serves TLS.
	idpHTTPClient, err := grpcutil.HTTPClient(cfg.TLS)
	if err != nil {
		logger.Error("idp: http client", "err", err)
		os.Exit(1)
	}

	// gRPC client for the IdP's API-only surface (job-token minting and user
	// management). When TLS is configured it uses mTLS, so only the API (a
	// CA-signed client) can mint job tokens or manage users. The dial is lazy
	// (grpc.NewClient), so it does not require the IdP to be up yet.
	idpConn, err := grpcutil.Dial(ctx, cfg.IdPGRPCAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial idp", "addr", cfg.IdPGRPCAddress, "err", err)
		os.Exit(1)
	}
	defer idpConn.Close()
	idpClient := idppb.NewIdPClient(idpConn)

	// Authentication for the UI-facing HTTP surface. Disabled by default; when
	// enabled it performs OIDC discovery (a network call) here. The API is a
	// pure token verifier: clients present the OAuth token they obtained from
	// the IdP as `Authorization: Bearer <token>`.
	authBundle, err := auth.NewWithClient(ctx, cfg.Auth, idpHTTPClient)
	if err != nil {
		logger.Error("auth: init", "err", err)
		os.Exit(1)
	}

	// The API's HTTP server. When username/password authentication is enabled
	// (the default for local development) the API attaches a proxy client to
	// the IdP so the UI can sign in via /api/login and register via
	// /api/register — the real logic (password verification, token minting,
	// user storage) lives in the IdP. The IdP's base URL is the configured
	// auth issuer, falling back to the default IdP address.
	apiServer := api.New(clients, hub, secretStore)

	// Role-based access control (F-14). When authentication is enabled the API
	// enforces RBAC: a request is allowed only if the authenticated
	// principal's roles grant the required permission (with a scope that
	// covers the target resource). The engine reads the role catalog from the
	// Database service and caches it locally, invalidating the cache when a
	// role_change event arrives on the shared event log (F-23), so a role or
	// binding change takes effect on this replica without a restart. The same
	// engine instance is shared by the HTTP server (which enforces it) and the
	// gRPC server's tail loop (which invalidates it). When authentication is
	// disabled the API is open (requests act as a synthetic admin) and RBAC is
	// not enforced.
	var authzEngine *authz.Engine
	if authBundle.Enabled() {
		authzEngine = authz.New(api.NewDBRoleSource(clients.Database))
		apiServer.SetAuthz(authzEngine, true)
	}
	if cfg.Auth.UserPassEnabled {
		// The OIDC issuer the API's verifier checks on a presented token
		// (auth.issuer, falling back to the default IdP address). It is passed
		// to the IdP so a login token is stamped with an issuer the API will
		// accept.
		idpBase := cfg.Auth.Issuer
		if idpBase == "" {
			idpBase = config.DefaultIdPAddress
		}
		// The audience the API's OIDC verifier checks on a presented token
		// (token_audience when set, else client_id). It is passed to the IdP
		// so a login token is stamped with an audience the API will accept.
		audience := cfg.Auth.TokenAudience
		if audience == "" {
			audience = cfg.Auth.ClientID
		}
		apiServer.SetUserPassClient(api.NewUserPassClient(idpClient, audience, idpBase))
	}

	// The API's HTTP handler, wrapped by the auth middleware (which verifies
	// the Bearer token when auth is enabled). The /api/auth/oidc discovery
	// endpoint is registered on a parent mux so it is reachable without a
	// token — a client needs it to start the sign-in flow. Go's ServeMux
	// prefers the more specific /api/auth/oidc pattern over the /api/ subtree
	// handler. When username/password auth is enabled the sign-in entry points
	// (/api/login, /api/register) are exempted from the token check so they
	// stay reachable without a token.
	var exempt []string
	if cfg.Auth.UserPassEnabled {
		exempt = []string{"/api/login", "/api/register"}
	}
	apiHandler := authBundle.MiddlewareExempt(apiServer.Handler(), exempt...)
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
		jobAuth, err = api.NewJobTokenAuth(ctx, cfg.GRPCAuth, idpHTTPClient, idpClient, logger)
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
	grpcServer := api.NewGRPCServer(clients.Database, clients.Artifacts, hub, jobAuth, secretStore, logger)
	// Share the authorization engine (F-14) with the gRPC server's event-log
	// tail loop so a role_change event invalidates this replica's cache (the
	// same instance the HTTP server enforces).
	if authzEngine != nil {
		grpcServer.SetAuthz(authzEngine)
	}
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

// dbNonceSource adapts the Database service's NextSecretNonce RPC to the
// secrets.NonceSource interface (F-12). Every API replica draws its per-
// encryption nonce from this single shared counter, so no two encryptions —
// even across replicas — ever reuse a nonce.
type dbNonceSource struct {
	db dbpb.DatabaseClient
}

func (n *dbNonceSource) NextNonce(ctx context.Context) (uint64, error) {
	resp, err := n.db.NextSecretNonce(ctx, &dbpb.NextSecretNonceRequest{})
	if err != nil {
		return 0, err
	}
	return uint64(resp.GetNonce()), nil
}
