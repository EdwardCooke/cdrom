package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"cdrom/internal/config"
)

// ServerCreds returns the gRPC transport credentials a service server should
// use. When cfg.TLS is enabled it returns mTLS credentials: the server
// presents the certificate and key from cfg.TLS and requires clients to
// present a certificate signed by the CA in cfg.TLS.CAFile. When TLS is not
// enabled it returns insecure (plaintext) credentials.
func ServerCreds(cfg config.TLSConfig) (credentials.TransportCredentials, error) {
	if !cfg.Enabled() {
		return insecure.NewCredentials(), nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("grpcutil: load server cert %s: %w", cfg.CertFile, err)
	}
	ca, err := loadCA(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    ca,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}), nil
}

// ClientCreds returns the gRPC transport credentials a client should use to
// dial a service. When cfg.TLS is enabled it returns mTLS credentials: the
// client presents the certificate and key from cfg.TLS and verifies the
// server against the CA in cfg.TLS.CAFile. When TLS is not enabled it
// returns insecure (plaintext) credentials.
func ClientCreds(cfg config.TLSConfig) (credentials.TransportCredentials, error) {
	if !cfg.Enabled() {
		return insecure.NewCredentials(), nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("grpcutil: load client cert %s: %w", cfg.CertFile, err)
	}
	ca, err := loadCA(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      ca,
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// ServerCredsRelaxed is the API's gRPC serving credentials UNDER F-30.3:
// the agent↔API leg drops MUTUAL TLS in favor of TLS-with-server-
// verification + the bearer agent credential (the identity guarantee is the
// credential, not the cipher suite). The server does NOT REQUIRE a client
// certificate (targets connect anonymously), but a PRESENTED certificate is
// still VERIFIED against the shared CA (VerifyClientCertIfGiven) — internal
// mesh peers keep their integrity, and a target cannot smuggle a bogus cert.
// When TLS is not enabled it returns plaintext credentials, as ServerCreds
// does (legitimate for local dev: the credential CHECK, not the cipher
// suite, is the identity guarantee).
func ServerCredsRelaxed(cfg config.TLSConfig) (credentials.TransportCredentials, error) {
	if !cfg.Enabled() {
		return insecure.NewCredentials(), nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("grpcutil: load server cert %s: %w", cfg.CertFile, err)
	}
	ca, err := loadCA(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    ca,
		// Anonymous (targets) OR certified (internal mesh) clients are both
		// ADMITTED; a PRESENTED certificate is still VERIFIED against the CA
		// (VerifyClientCertIfGiven: no client cert required, but one that is
		// offered must be legit — mesh peers keep their integrity, and a
		// target cannot smuggle in a bogus cert).
		ClientAuth: tls.VerifyClientCertIfGiven,
	}), nil
}

// DialNoClientCert opens a gRPC connection that VERIFIES THE SERVER against
// the shared CA but presents NO client certificate — the target-side dial
// (F-30.3): the agent/worker authenticate with the BEARER credential, not a
// mesh certificate. When TLS is not enabled it dials plaintext (matching
// Dial's local-dev posture).
func DialNoClientCert(ctx context.Context, addr string, tlsCfg config.TLSConfig) (*grpc.ClientConn, error) {
	var creds credentials.TransportCredentials
	if !tlsCfg.Enabled() {
		creds = insecure.NewCredentials()
	} else {
		ca, err := loadCA(tlsCfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("grpcutil: dial-no-client-cert %q: %w", addr, err)
		}
		creds = credentials.NewTLS(&tls.Config{
			RootCAs:    ca,
			MinVersion: tls.VersionTLS12,
		})
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("grpcutil: dial-no-client-cert %q: %w", addr, err)
	}
	return conn, nil
}

// LoadCA reads the PEM-encoded CA certificate at path and returns a pool
// containing it. It is exported so non-gRPC components (the IdP's HTTP
// server) can verify client certificates against the same shared CA.
func LoadCA(path string) (*x509.CertPool, error) {
	return loadCA(path)
}

// loadCA reads the PEM-encoded CA certificate at path and returns a pool
// containing it.
func loadCA(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("grpcutil: read CA %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("grpcutil: no valid CA certificate in %s", path)
	}
	return pool, nil
}
