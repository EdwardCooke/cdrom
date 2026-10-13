// Package webhook provides the HTTP client the API uses to send webhook
// notifications (F-18). A webhook may point at any endpoint — a public HTTPS
// service signed by a public CA or an internal service signed by the shared
// CA — so the client verifies the server certificate against the system
// certificate store plus the shared CA. It is separate from grpcutil because
// it has no gRPC involvement: it is a plain outbound HTTP client.
package webhook

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"

	"cdrom/internal/config"
)

// NewClient returns an *http.Client for the API to send webhooks (F-18). It
// sets no global timeout: each request is bounded by its own (clamped)
// timeout, so a step's per-request timeout is honored. A webhook may point at
// any endpoint — a public HTTPS service signed by a public CA or an internal
// service signed by the shared CA — so its root CA pool is the system
// certificate store plus the shared CA (see rootCAs). When tls is enabled it
// also presents the API's client certificate, so it can reach an HTTPS
// webhook that requires a client certificate.
func NewClient(tlsCfg config.TLSConfig) (*http.Client, error) {
	if !tlsCfg.Enabled() {
		return &http.Client{}, nil
	}
	cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("webhook: load client cert %s: %w", tlsCfg.CertFile, err)
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      rootCAs(tlsCfg.CAFile),
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			},
		},
	}, nil
}

// rootCAs returns the root CA pool the webhook client uses to verify a
// webhook's TLS server certificate: the system certificate store (so a
// webhook to a public HTTPS endpoint signed by a public CA is accepted) plus,
// when path is set, the shared CA certificate (so a webhook to an internal
// service signed by the shared CA is also accepted). A missing or invalid
// shared CA file does not prevent public webhooks from being reached: the
// system store is the baseline and the shared CA is only an addition.
func rootCAs(path string) *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if path != "" {
		if pem, err := os.ReadFile(path); err == nil {
			_ = pool.AppendCertsFromPEM(pem)
		}
	}
	return pool
}
