package idp

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"cdrom/internal/config"
)

// TLSConfig returns the *tls.Config the IdP's HTTP server should use when
// cfg.TLS is enabled (a CA certificate is configured), or nil for plaintext.
//
// The bulk of the IdP's HTTP surface is the standard OIDC protocol
// (discovery, JWKS, the authorization endpoint, and the authorization_code
// token grant), served to browsers and the API's discovery/code-exchange —
// none of which present a client certificate. The listener therefore REQUESTS
// (but does not demand) a client certificate: handshake-transparent to
// browsers, which simply decline. Exactly ONE route — the break-glass escape
// hatch, POST /emergency/break-glass (F-28.3) — DEMANDS a CA-signed client
// certificate and verifies the peer chain itself (verifyPeerChain); everyone
// else reaches the IdP's API-only operations over gRPC (GRPCServer,
// grpcutil.ServerCreds).
//
// ClientCAs is REQUIRED: without it a presenting client's certificate is
// negotiated but unverifiable, and the break-glass route would deny EVERYONE
// (even the legitimate holder of the API's client certificate).
func TLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("idp: load server cert %s: %w", cfg.CertFile, err)
	}
	pool := x509.NewCertPool()
	caBytes, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("idp: load CA %s: %w", cfg.CAFile, err)
	}
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("idp: no certificates parsed from CA file %s", cfg.CAFile)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		ClientAuth:   tls.RequestClientCert,
		ClientCAs:    pool,
	}, nil
}

// LoadCACertPool reads a PEM CA bundle and returns the x509 pool of root
// certificates it describes (nil-safe: an unreadable or empty file yields an
// error). The break-glass route (F-28.3) verifies presenter chains against
// this pool.
func LoadCACertPool(caFile string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("idp: load CA %s: %w", caFile, err)
	}
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("idp: no certificates parsed from CA file %s", caFile)
	}
	return pool, nil
}
