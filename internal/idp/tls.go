package idp

import (
	"crypto/tls"
	"fmt"

	"cdrom/internal/config"
)

// TLSConfig returns the *tls.Config the IdP's HTTP server should use when
// cfg.TLS is enabled (a CA certificate is configured), or nil for plaintext.
//
// The IdP's HTTP surface is the standard OIDC protocol (discovery, JWKS, the
// authorization endpoint, and the authorization_code token grant), which is
// served to browsers and the API's discovery/code-exchange — none of which
// present a client certificate. The IdP's API-only operations (job-token
// minting and user management) are served over gRPC by GRPCServer, which
// authenticates the API with its mTLS client certificate (see
// grpcutil.ServerCreds). The HTTP listener therefore only presents the IdP's
// server certificate and does not request a client certificate.
func TLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("idp: load server cert %s: %w", cfg.CertFile, err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}
