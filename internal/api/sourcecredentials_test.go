package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TestSourceCredentialEndpoints verifies the source-credential endpoints
// (F-21.3): GET lists credentials (secret material redacted), POST creates one
// (the API encrypts the plaintext before storing), GET/{name} fetches one
// (redacted), PUT updates (re-encrypts), and DELETE removes one.
func TestSourceCredentialEndpoints(t *testing.T) {
	fake := &fakeDatabase{
		sourceCredentials: &dbpb.ListSourceCredentialsResponse{
			Credentials: []*dbpb.SourceCredential{
				{
					Id:       1,
					Name:     "github-bot",
					Kind:     "git-https",
					Prefix:   "https://github.com/myorg/",
					Username: "bot",
					Token:    "ciphertext",
				},
				{
					Id:     2,
					Name:   "api-endpoint",
					Kind:   "http",
					Prefix: "https://api.example.com/",
					Headers: []*dbpb.HTTPHeader{
						{Name: "Authorization", Value: "ciphertext", Sensitive: true},
					},
				},
			},
		},
	}
	srv := New(Clients{Database: fake}, nil, testSecretStore(t))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// GET lists the credentials with the secret material redacted.
	resp, err := http.Get(ts.URL + "/api/source-credentials")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	var views []*sourceCredentialView
	if err := json.NewDecoder(resp.Body).Decode(&views); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("views = %d, want 2", len(views))
	}
	// The git-https token is redacted; the username is shown.
	if views[0].Token != "[REDACTED]" || views[0].Username != "bot" {
		t.Fatalf("git-https view = %+v, want token redacted + username shown", views[0])
	}
	// The sensitive http header value is redacted; the header name is shown.
	if len(views[1].Headers) != 1 || views[1].Headers[0].Value != "[REDACTED]" || views[1].Headers[0].Name != "Authorization" || !views[1].Headers[0].Sensitive {
		t.Fatalf("http view = %+v, want header value redacted + name shown", views[1])
	}

	// POST creates a credential; the API encrypts the plaintext token before
	// storing (the stored token differs from the plaintext).
	body, _ := json.Marshal(sourceCredentialRequest{
		Name:     "github-bot",
		Kind:     "git-https",
		Prefix:   "https://github.com/myorg/",
		Username: "bot",
		Token:    "plaintext-token",
	})
	resp, err = http.Post(ts.URL+"/api/source-credentials", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST status = %d, want 201 (body %s)", resp.StatusCode, data)
	}
	if fake.createdSourceCred == nil || fake.createdSourceCred.GetToken() == "plaintext-token" {
		t.Fatalf("create not recorded with an encrypted token: %+v", fake.createdSourceCred)
	}
	if fake.createdSourceCred.GetToken() == "" {
		t.Fatal("create token is empty, want an encrypted ciphertext")
	}

	// GET/{name} fetches one (redacted).
	fake.getSourceCred = &dbpb.SourceCredential{
		Id: 1, Name: "github-bot", Kind: "git-https", Prefix: "https://github.com/myorg/",
		Username: "bot", Token: "ciphertext",
	}
	resp, err = http.Get(ts.URL + "/api/source-credentials/github-bot")
	if err != nil {
		t.Fatalf("GET/{name}: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET/{name} status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	var view sourceCredentialView
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if view.Token != "[REDACTED]" || view.Username != "bot" {
		t.Fatalf("GET/{name} view = %+v, want token redacted + username shown", view)
	}

	// PUT updates (re-encrypts the secret material).
	putBody, _ := json.Marshal(sourceCredentialRequest{
		Prefix:   "https://github.com/myorg/",
		Username: "bot",
		Token:    "new-plaintext-token",
	})
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/source-credentials/github-bot", bytes.NewReader(putBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	if fake.updatedSourceCred == nil || fake.updatedSourceCred.GetToken() == "new-plaintext-token" {
		t.Fatalf("update not recorded with an encrypted token: %+v", fake.updatedSourceCred)
	}

	// DELETE removes one.
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/api/source-credentials/github-bot", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("DELETE status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	if fake.deletedSourceCred != "github-bot" {
		t.Fatalf("delete name = %q, want github-bot", fake.deletedSourceCred)
	}
}

// TestSourceCredentialNonSensitiveHeader verifies that an http credential's
// headers are returned per their own sensitivity (F-21.3): a header marked
// sensitive is redacted, while a non-sensitive header is returned in plaintext
// so it is easily viewed and modified.
func TestSourceCredentialNonSensitiveHeader(t *testing.T) {
	fake := &fakeDatabase{
		getSourceCred: &dbpb.SourceCredential{
			Id: 1, Name: "plain-header", Kind: "http", Prefix: "https://api.example.com/",
			Headers: []*dbpb.HTTPHeader{
				{Name: "X-Api-Key", Value: "visible-key", Sensitive: false},
				{Name: "Authorization", Value: "ciphertext", Sensitive: true},
			},
		},
	}
	srv := New(Clients{Database: fake}, nil, testSecretStore(t))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/source-credentials/plain-header")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET status = %d, want 200 (body %s)", resp.StatusCode, data)
	}
	var view sourceCredentialView
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(view.Headers) != 2 {
		t.Fatalf("headers = %d, want 2", len(view.Headers))
	}
	// The non-sensitive header is returned in plaintext.
	if view.Headers[0].Name != "X-Api-Key" || view.Headers[0].Value != "visible-key" || view.Headers[0].Sensitive {
		t.Fatalf("non-sensitive header = %+v, want the plaintext (not redacted)", view.Headers[0])
	}
	// The sensitive header is redacted.
	if view.Headers[1].Name != "Authorization" || view.Headers[1].Value != "[REDACTED]" || !view.Headers[1].Sensitive {
		t.Fatalf("sensitive header = %+v, want the value redacted", view.Headers[1])
	}
}

// TestSourceCredentialDeleteGuard verifies the delete guard (F-21.3.3): a
// credential that a source references by name cannot be deleted (409), while
// an unreferenced credential can.
func TestSourceCredentialDeleteGuard(t *testing.T) {
	// A credential referenced by a source is rejected (409).
	fake := &fakeDatabase{sourceRefCount: 2}
	srv := New(Clients{Database: fake}, nil, testSecretStore(t))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/api/source-credentials/in-use", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("DELETE referenced credential status = %d, want 409 (body %s)", resp.StatusCode, data)
	}
	if fake.deletedSourceCred != "" {
		t.Fatalf("referenced credential was deleted: %q", fake.deletedSourceCred)
	}

	// An unreferenced credential is deleted.
	fake2 := &fakeDatabase{sourceRefCount: 0}
	srv2 := New(Clients{Database: fake2}, nil, testSecretStore(t))
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	req2, _ := http.NewRequest(http.MethodDelete, ts2.URL+"/api/source-credentials/free", nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp2.Body)
		t.Fatalf("DELETE unreferenced credential status = %d, want 200 (body %s)", resp2.StatusCode, data)
	}
	if fake2.deletedSourceCred != "free" {
		t.Fatalf("delete name = %q, want free", fake2.deletedSourceCred)
	}
}

// TestSyncPipelineSourceFailureSurfaces verifies that a credential that fails
// to authenticate surfaces as a sync failure on the source state (F-21.3.2 /
// F-21.5.2): the fetch error is recorded on last_error and the refresh
// endpoint reports it.
func TestSyncPipelineSourceFailureSurfaces(t *testing.T) {
	store := testSecretStore(t)
	// Encrypt the header value so the decrypt step succeeds and the test
	// reaches the fetcher (which returns the 401).
	headerCiphertext, err := store.Encrypt(context.Background(), "Bearer stale-token")
	if err != nil {
		t.Fatalf("encrypt header value: %v", err)
	}
	fake := &fakeDatabase{
		pipelineSource: &dbpb.PipelineSource{
			Id: 1, PipelineId: 7, Kind: "http",
			Reference:  "http://127.0.0.1:1/file.yaml",
			Credential: "stale",
		},
		getSourceCred: &dbpb.SourceCredential{
			Id: 1, Name: "stale", Kind: "http",
			Headers: []*dbpb.HTTPHeader{
				{Name: "Authorization", Value: headerCiphertext, Sensitive: true},
			},
		},
	}
	ps := &fakePipelineSource{err: status.Error(codes.Unauthenticated, "401 unauthorized")}
	srv := New(Clients{Database: fake, PipelineSource: ps}, nil, store)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/pipelines/7/source/refresh", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("refresh status = %d, want 422 (body %s)", resp.StatusCode, data)
	}
	// The failure was recorded on the source state's last_error.
	if fake.pipelineSourceState == nil || fake.pipelineSourceState.GetLastError() == "" {
		t.Fatalf("last_error not recorded on the source state: %+v", fake.pipelineSourceState)
	}
	if !strings.Contains(fake.pipelineSourceState.GetLastError(), "401") {
		t.Fatalf("last_error = %q, want it to carry the fetch error", fake.pipelineSourceState.GetLastError())
	}
}

// TestSelectSourceCredential verifies the credential matching (F-21.3.2): an
// explicit credential name wins; otherwise the longest matching prefix wins; a
// source matching no credential uses no credential.
func TestSelectSourceCredential(t *testing.T) {
	fake := &fakeDatabase{
		sourceCredentials: &dbpb.ListSourceCredentialsResponse{
			Credentials: []*dbpb.SourceCredential{
				{Name: "github", Kind: "git-https", Prefix: "https://github.com/"},
				{Name: "myorg", Kind: "git-https", Prefix: "https://github.com/myorg/"},
				{Name: "api", Kind: "http", Prefix: "https://api.example.com/"},
			},
		},
	}
	srv := New(Clients{Database: fake}, nil)

	ctx := context.Background()

	// An explicit credential name wins.
	fake.getSourceCred = &dbpb.SourceCredential{Name: "explicit", Kind: "git-https"}
	cred, err := srv.selectSourceCredential(ctx, "explicit", "https://github.com/myorg/repo.git")
	if err != nil {
		t.Fatalf("explicit name: %v", err)
	}
	if cred.GetName() != "explicit" {
		t.Fatalf("explicit name = %q, want explicit", cred.GetName())
	}

	// The longest matching prefix wins (myorg over github).
	cred, err = srv.selectSourceCredential(ctx, "", "https://github.com/myorg/repo.git")
	if err != nil {
		t.Fatalf("prefix match: %v", err)
	}
	if cred.GetName() != "myorg" {
		t.Fatalf("prefix match = %q, want myorg (longest)", cred.GetName())
	}

	// A source matching no credential uses no credential.
	cred, err = srv.selectSourceCredential(ctx, "", "https://gitlab.com/other/repo.git")
	if err != nil {
		t.Fatalf("no match: %v", err)
	}
	if cred != nil {
		t.Fatalf("no match = %q, want nil (public source)", cred.GetName())
	}
}

// TestDecryptSourceCredential verifies that the API decrypts a stored
// credential's secret material for a single fetch (F-21.3.2): the git-https
// token, the git-ssh key + host key, and a sensitive http header value are
// decrypted, while a non-sensitive http header value is used as-is.
func TestDecryptSourceCredential(t *testing.T) {
	store := testSecretStore(t)
	enc := func(t *testing.T, v string) string {
		t.Helper()
		c, err := store.Encrypt(context.Background(), v)
		if err != nil {
			t.Fatalf("encrypt %q: %v", v, err)
		}
		return c
	}
	srv := New(Clients{}, nil, store)
	ctx := context.Background()

	// A git-https credential: the token is decrypted.
	git, err := srv.decryptSourceCredential(ctx, &dbpb.SourceCredential{
		Name: "github-bot", Kind: "git-https", Username: "bot", Token: enc(t, "tok123"),
	})
	if err != nil {
		t.Fatalf("decrypt git-https: %v", err)
	}
	if git.GetToken() != "tok123" || git.GetUsername() != "bot" {
		t.Fatalf("git-https = %+v, want token tok123 + username bot", git)
	}

	// A git-ssh credential: the private key + host key are decrypted.
	ssh, err := srv.decryptSourceCredential(ctx, &dbpb.SourceCredential{
		Name: "corp-git", Kind: "git-ssh", PrivateKey: enc(t, "PEM-KEY"), HostKey: enc(t, "HOSTKEY"),
	})
	if err != nil {
		t.Fatalf("decrypt git-ssh: %v", err)
	}
	if ssh.GetPrivateKey() != "PEM-KEY" || ssh.GetHostKey() != "HOSTKEY" {
		t.Fatalf("git-ssh = %+v, want decrypted key + host key", ssh)
	}

	// An http credential with multiple headers, each with its own sensitivity:
	// the sensitive one is decrypted, the plain one is used as-is.
	httpMulti, err := srv.decryptSourceCredential(ctx, &dbpb.SourceCredential{
		Name: "api", Kind: "http",
		Headers: []*dbpb.HTTPHeader{
			{Name: "Authorization", Value: enc(t, "Bearer secret"), Sensitive: true},
			{Name: "X-Request-Id", Value: "req-123", Sensitive: false},
		},
	})
	if err != nil {
		t.Fatalf("decrypt http (multi): %v", err)
	}
	if len(httpMulti.GetHeaders()) != 2 {
		t.Fatalf("http (multi) = %+v, want 2 headers", httpMulti)
	}
	if httpMulti.GetHeaders()[0].GetName() != "Authorization" || httpMulti.GetHeaders()[0].GetValue() != "Bearer secret" {
		t.Fatalf("http (multi) sensitive header = %+v, want decrypted value", httpMulti.GetHeaders()[0])
	}
	if httpMulti.GetHeaders()[1].GetName() != "X-Request-Id" || httpMulti.GetHeaders()[1].GetValue() != "req-123" {
		t.Fatalf("http (multi) plain header = %+v, want the plaintext value", httpMulti.GetHeaders()[1])
	}

	// An http credential with a non-sensitive header: the header value is used
	// as-is (stored in plaintext).
	httpPlain, err := srv.decryptSourceCredential(ctx, &dbpb.SourceCredential{
		Name: "api", Kind: "http",
		Headers: []*dbpb.HTTPHeader{
			{Name: "X-Api-Key", Value: "visible", Sensitive: false},
		},
	})
	if err != nil {
		t.Fatalf("decrypt http (plain): %v", err)
	}
	if len(httpPlain.GetHeaders()) != 1 || httpPlain.GetHeaders()[0].GetValue() != "visible" {
		t.Fatalf("http (plain) = %+v, want the plaintext header value", httpPlain)
	}

	// A nil credential (a public source) returns nil.
	if got, err := srv.decryptSourceCredential(ctx, nil); err != nil || got != nil {
		t.Fatalf("nil credential = (%v, %v), want (nil, nil)", got, err)
	}
}
