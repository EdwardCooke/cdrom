package pipelinesource

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"

	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
)

// httpClient fetches a single file from an HTTP/HTTPS endpoint (F-21.2.4.2).
type httpClient struct {
	client *http.Client
}

// newHTTPClient creates an http client with a bounded timeout.
func newHTTPClient() *httpClient {
	return &httpClient{client: &http.Client{Timeout: 60 * time.Second}}
}

// fetch does a GET of ref.URL and returns the response body (the file's
// content), the ETag/Last-Modified (the version identifier), and the body's
// SHA-256 (hex). Request headers come from the source's http credential
// (F-21.3); a credential is optional — an endpoint that needs no
// authentication is fetched with a plain GET.
func (h *httpClient) fetch(ctx context.Context, ref *Reference, cred *httpCredential) ([]byte, string, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ref.URL, nil)
	if err != nil {
		return nil, "", "", fmt.Errorf("http: build request: %w", err)
	}
	if cred != nil {
		// Each header's value carries the secret (the API decrypts it when it
		// is marked sensitive, F-21.3); send every header as-is on the GET.
		for _, hdr := range cred.Headers {
			if hdr.Name != "" && hdr.Value != "" {
				req.Header.Set(hdr.Name, hdr.Value)
			}
		}
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, "", "", fmt.Errorf("http: get %q: %w", ref.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", "", fmt.Errorf("http: get %q: unexpected status %s", ref.URL, resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", "", fmt.Errorf("http: read body: %w", err)
	}
	version := resp.Header.Get("ETag")
	if version == "" {
		version = resp.Header.Get("Last-Modified")
	}
	sum := sha256.Sum256(body)
	return body, version, hex.EncodeToString(sum[:]), nil
}

// httpHeader is one header an http fetch sends (F-21.3): a name and a value.
// The value carries the secret (the API decrypts it when it is marked
// sensitive before handing it to the fetcher).
type httpHeader struct {
	Name  string
	Value string
}

// httpCredential is the plaintext secret material for an http fetch (F-21.3):
// the list of headers to send on the GET.
type httpCredential struct {
	Headers []httpHeader
}

// toHTTPCredential converts a proto SourceCredential to an httpCredential. It
// returns nil for a non-http or empty credential.
func toHTTPCredential(c *pipelinesourcepb.SourceCredential) *httpCredential {
	if c == nil || c.GetKind() != "http" {
		return nil
	}
	cred := &httpCredential{}
	for _, h := range c.GetHeaders() {
		cred.Headers = append(cred.Headers, httpHeader{Name: h.GetName(), Value: h.GetValue()})
	}
	return cred
}
