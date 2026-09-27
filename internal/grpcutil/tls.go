package grpcutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

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
