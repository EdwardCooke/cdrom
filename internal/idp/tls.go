package idp

import (
	"crypto/tls"
	"fmt"

	"cdrom/internal/config"
	"cdrom/internal/grpcutil"
)

// TLSConfig returns the *tls.Config the IdP's HTTP server should use when
// cfg.TLS is enabled (a CA certificate is configured), or nil for plaintext.
//
// The IdP serves both the OIDC flow (browsers, the API's discovery and code
// exchange — no client certificate) and job-token minting (the API, which
// presents its mTLS client certificate). The listener therefore uses
// RequestClientCert: the handshake succeeds with or without a client
// certificate, and the job-token grant handler rejects any TLS client that
// did not present one. This keeps the OIDC flow working for cert-less
// clients while ensuring only a CA-signed client (the API) can mint job
// tokens.
func TLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("idp: load server cert %s: %w", cfg.CertFile, err)
	}
	ca, err := grpcutil.LoadCA(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    ca,
		ClientAuth:   tls.RequestClientCert,
		MinVersion:   tls.VersionTLS12,
	}, nil
}
