// Package grpcutil provides shared gRPC plumbing for all cdrom binaries:
// client dialing and server lifecycle (serve until signal, graceful stop).
package grpcutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"cdrom/internal/config"
)

// Dial opens a gRPC connection to addr. When tls is enabled (a CA
// certificate is configured) the connection uses mutual TLS; otherwise it
// is plaintext.
func Dial(ctx context.Context, addr string, tls config.TLSConfig) (*grpc.ClientConn, error) {
	creds, err := ClientCreds(tls)
	if err != nil {
		return nil, fmt.Errorf("grpcutil: dial %q: %w", addr, err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("grpcutil: dial %q: %w", addr, err)
	}
	return conn, nil
}

// WithBearerToken returns a copy of ctx carrying an "authorization: Bearer
// <token>" metadata entry, so a gRPC call presents the token to the server.
// An empty token returns ctx unchanged.
func WithBearerToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

// HTTPClient returns an *http.Client for talking to an HTTPS service that
// presents a certificate signed by the CA in tls (e.g. the IdP when it
// serves TLS). When tls is not enabled it returns a client suitable for
// plaintext HTTP. When tls is enabled the client verifies the server against
// the CA and presents its own client certificate (from tls.CertFile/KeyFile),
// so a service that requires a client certificate (the IdP's job-token
// endpoint) sees a CA-signed peer.
func HTTPClient(tlsCfg config.TLSConfig) (*http.Client, error) {
	if !tlsCfg.Enabled() {
		return &http.Client{Timeout: 10 * time.Second}, nil
	}
	ca, err := LoadCA(tlsCfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("grpcutil: http client: %w", err)
	}
	cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("grpcutil: http client load cert %s: %w", tlsCfg.CertFile, err)
	}
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      ca,
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			},
		},
	}, nil
}

// Serve runs srv on lis until the process receives SIGINT or SIGTERM, then
// shuts the server down gracefully. It is the single entry point every
// gRPC service binary uses to run its server.
func Serve(lis net.Listener, srv *grpc.Server, logger *slog.Logger) error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpcutil: serve: %w", err)
		}
	}()
	logger.Info("grpc: serving", "addr", lis.Addr().String())

	select {
	case <-stop:
		logger.Info("grpc: shutting down")
		srv.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}
