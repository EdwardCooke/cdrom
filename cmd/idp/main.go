// Command idp is the Cdrom local OIDC identity provider.
//
// It acts as a JWT issuer for the API's OIDC authentication: it serves the
// OIDC discovery document, a JWKS of its signing keys, an authorization
// endpoint (authorization-code + PKCE), and a token endpoint that mints RS256
// ID tokens. It signs with an RSA key and rotates it automatically as the
// key approaches expiry, keeping predecessor keys in the JWKS until they
// expire so clients keep verifying older tokens.
//
// Point the API's auth config at it:
//
//	auth:
//	  enabled: true
//	  issuer: http://127.0.0.1:7104
//	  client_id: cdrom-ui
//	  redirect_url: http://127.0.0.1:8080/api/auth/callback
//
// Clients sign in against the IdP (authorization-code + PKCE) and then call
// the API with `Authorization: Bearer <access_token>`.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/idp"
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
	if err := cfg.IdP.Validate(); err != nil {
		logger.Error("config: invalid idp", "err", err)
		os.Exit(1)
	}

	// Pepper warnings (F-24/F-28.2). All credential secrets are Argon2ID
	// encodings of pepper || secret; the pepper is the defense that keeps a
	// database dump from yielding usable credentials offline. Warn prominently
	// when it is absent (house pattern, cf. the API's secrets/pepper banners).
	if cfg.IdP.PasswordPepper == "" {
		logger.Error("================================================================")
		logger.Error("SECURITY WARNING: NO USER-PASSWORD PEPPER IS CONFIGURED")
		logger.Error("================================================================")
		logger.Error("User passwords are Argon2ID-hashed without a pepper. A pepper")
		logger.Error("(prepended to the KDF input) ensures a database leak does not")
		logger.Error("reveal usable passwords. Set CDROM_IDP_PASSWORD_PEPPER (or the")
		logger.Error("'idp.password_pepper' config) to a long random secret before")
		logger.Error("running in production.")
		logger.Error("================================================================")
	}
	if cfg.IdP.Scim.Enabled && cfg.IdP.ScimKeysPepper == "" {
		logger.Error("================================================================")
		logger.Error("SECURITY WARNING: NO SCIM KEY PEPPER IS CONFIGURED")
		logger.Error("================================================================")
		logger.Error("SCIM API keys are Argon2ID-hashed without a pepper. A pepper")
		logger.Error("(prepended to the KDF input) ensures a database leak does not")
		logger.Error("reveal usable SCIM keys. Set CDROM_IDP_SCIM_KEYS_PEPPER (or the")
		logger.Error("'idp.scim_keys_pepper' config) to a long random secret before")
		logger.Error("running in production.")
		logger.Error("================================================================")
	}

	addr := cfg.ListenAddress
	if addr == "" {
		addr = config.DefaultIdPAddress
	}

	// The IdP persists its signing keyring and OIDC authorization codes
	// through the Database service, so multiple IdP replicas share the same
	// keys and codes and can run behind a load balancer.
	ctx := context.Background()
	dbConn, err := grpcutil.Dial(ctx, cfg.DBAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial database service", "addr", cfg.DBAddress, "err", err)
		os.Exit(1)
	}
	defer dbConn.Close()
	dbClient := dbpb.NewDatabaseClient(dbConn)

	km, err := idp.NewKeyManager(cfg.IdP, idp.NewDBKeyStore(dbClient), logger)
	if err != nil {
		logger.Error("idp: init key manager", "err", err)
		os.Exit(1)
	}

	// The HTTP server serves the standard OIDC surface (discovery, JWKS, the
	// authorization endpoint, and the authorization_code token grant) to
	// browsers and the API's discovery/code-exchange.
	srv := idp.NewServer(cfg.IdP, km, idp.NewDBAuthCodeStore(dbClient), logger)
	// The gRPC server serves the IdP's API-only surface (job-token minting,
	// user management, and API-key management) to the API, which authenticates
	// with its mTLS client certificate (when TLS is configured), so only the
	// API can reach it.
	userStore := idp.NewDBUserStore(dbClient)
	// The API-key store (F-25) persists the key directory (hashes, metadata,
	// and the owner's lockout state) through the Database service, so multiple
	// IdP replicas share it.
	apiKeyStore := idp.NewDBAPIKeyStore(dbClient)
	// The service-account store (F-26) persists the service-account directory
	// (accounts, their two key slots' salted hashes, role bindings, and
	// lockout state) through the Database service, so multiple IdP replicas
	// share it.
	serviceAccountStore := idp.NewDBServiceAccountStore(dbClient)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Metrics (F-20): the IdP serves /metrics and /healthz on a dedicated
	// metrics port (separate from its OIDC HTTP surface and its gRPC port).
	// When disabled (metrics.enabled: false) the listener is not started and
	// recording is a no-op (the metric set is nil). The metric set is shared
	// by the key manager (keyring size, rotations), the HTTP OIDC surface
	// (UI tokens minted), and the gRPC surface (job tokens, logins, users).
	var idpMetrics *metrics.IDPMetrics
	var idpStatsHandler stats.Handler
	metricsAddr := cfg.Metrics.Addr
	if metricsAddr == "" {
		metricsAddr = metrics.DefaultMetricsAddr("idp")
	}
	if cfg.Metrics.Enabled {
		collector := metrics.NewCollector()
		idpMetrics = metrics.NewIDPMetrics(collector.Registry)
		idpStatsHandler = metrics.NewStatsHandler(collector.Registry)
		go func() {
			if err := collector.StartMetrics(ctx, metricsAddr); err != nil {
				logger.Error("metrics: start", "addr", metricsAddr, "err", err)
			}
		}()
		logger.Info("cdrom idp metrics starting", "addr", metricsAddr)
	}
	km.SetMetrics(idpMetrics)
	srv.SetMetrics(idpMetrics)
	// The keyring was loaded before metrics were attached, so refresh the
	// keyring-size gauge now that the metric set is available.
	km.RefreshMetrics()

	go srv.StartRotation(ctx)

	// When TLS is configured the IdP's HTTP server serves HTTPS: it presents
	// its certificate. The OIDC flow (browsers, the API's discovery and code
	// exchange) does not present a client certificate. The gRPC server uses
	// mTLS (grpcutil.ServerCreds), requiring the API's CA-signed client
	// certificate.
	var tlsCfg *tls.Config
	if cfg.TLS.Enabled() {
		tlsCfg, err = idp.TLSConfig(cfg.TLS)
		if err != nil {
			logger.Error("idp: tls config", "err", err)
			os.Exit(1)
		}
	}

	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler(), TLSConfig: tlsCfg}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	// The gRPC server's listen address (the API dials it for job-token
	// minting and user management). Empty means the default IdP gRPC address.
	grpcAddr := cfg.IdP.GRPCAddress
	if grpcAddr == "" {
		grpcAddr = config.DefaultIdPGRPCAddress
	}
	creds, err := grpcutil.ServerCreds(cfg.TLS)
	if err != nil {
		logger.Error("idp: tls creds", "err", err)
		os.Exit(1)
	}
	grpcSrvOpts := []grpc.ServerOption{grpc.Creds(creds)}
	if idpStatsHandler != nil {
		grpcSrvOpts = append(grpcSrvOpts, grpc.StatsHandler(idpStatsHandler))
	}
	grpcSrv := grpc.NewServer(grpcSrvOpts...)
	idpGRPC := idp.NewGRPCServer(cfg.IdP, km, userStore, logger)
	idpGRPC.SetAPIKeyStore(apiKeyStore)
	idpGRPC.SetServiceAccountStore(serviceAccountStore)
	// Attach the IdP's Prometheus metric set (F-20) so the gRPC surface
	// records job-token minting, logins, and the user-directory size.
	idpGRPC.SetMetrics(idpMetrics)
	idppb.RegisterIdPServer(grpcSrv, idpGRPC)
	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("idp: listen grpc", "addr", grpcAddr, "err", err)
		os.Exit(1)
	}
	go func() {
		<-ctx.Done()
		grpcSrv.GracefulStop()
	}()
	go func() {
		if err := grpcSrv.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("idp: grpc serve", "err", err)
			os.Exit(1)
		}
	}()

	startLogArgs := []any{"addr", addr, "grpc", grpcAddr, "issuer", cfg.IdP.EffectiveIssuer(),
		"db", cfg.DBAddress, "key_lifetime", cfg.IdP.KeyLifetime,
		"rotate_before", cfg.IdP.RotateBefore, "tls", cfg.TLS.Enabled()}
	if cfg.IdP.Scim.Enabled {
		startLogArgs = append(startLogArgs, "scim_public_base", cfg.IdP.Scim.PublicBase)
	}
	logger.Info("cdrom idp starting", startLogArgs...)
	var serveErr error
	if tlsCfg != nil {
		serveErr = httpSrv.ListenAndServeTLS("", "")
	} else {
		serveErr = httpSrv.ListenAndServe()
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		logger.Error("http serve", "err", serveErr)
		os.Exit(1)
	}
}
