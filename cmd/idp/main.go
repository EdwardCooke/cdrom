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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/idp"
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
	if err := cfg.IdP.Validate(); err != nil {
		logger.Error("config: invalid idp", "err", err)
		os.Exit(1)
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

	srv := idp.NewServer(cfg.IdP, km, idp.NewDBAuthCodeStore(dbClient), logger).
		WithUsers(idp.NewDBUserStore(dbClient))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go srv.StartRotation(ctx)

	// When TLS is configured the IdP serves HTTPS: it presents its
	// certificate and accepts clients with or without a client certificate
	// (RequestClientCert). The OIDC flow (browsers, the API's discovery
	// and code exchange) works without a client cert; job-token minting
	// additionally requires the caller to have presented a CA-signed client
	// certificate (the API's), which the token handler enforces.
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

	logger.Info("cdrom idp starting", "addr", addr, "issuer", cfg.IdP.EffectiveIssuer(),
		"db", cfg.DBAddress, "key_lifetime", cfg.IdP.KeyLifetime,
		"rotate_before", cfg.IdP.RotateBefore, "tls", cfg.TLS.Enabled())
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
