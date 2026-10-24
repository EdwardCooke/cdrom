package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"cdrom/internal/audit"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
)

// ---------------------------------------------------------------------------
// Source credentials (F-21.3)
// ---------------------------------------------------------------------------
//
// Named, database-stored credentials that authenticate a fetch of a private
// source (a private git repository, or an HTTP/HTTPS endpoint that requires a
// token). They are matched to a source by the longest matching prefix (or an
// explicit credential name on the source), and their secret material is
// encrypted at rest (F-12) when marked sensitive. The API is the only
// component that holds the key: it encrypts the plaintext on create/update and
// redacts it on read, so the plaintext is never returned to the UI. Listing
// requires source-credentials.can-view; create/update/delete require
// source-credentials.can-manage. A change publishes a source_credential WS
// event so the UI stays current.

// sourceCredentialRedacted is the marker the API returns in place of a
// credential's secret material on read (F-21.3).
const sourceCredentialRedacted = "[REDACTED]"

// httpHeader is the JSON form of one http header on a source credential
// (F-21.3): a name, a value, and a per-header sensitive flag. On a request the
// value carries the plaintext; on a view the value is redacted when the header
// is marked sensitive (returned in plaintext otherwise).
type httpHeader struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Sensitive bool   `json:"sensitive"`
}

// sourceCredentialRequest is the JSON form of a source-credential create/update.
// The secret material fields carry the plaintext; the API encrypts them before
// storing. On update the fields replace the existing values (a full
// replacement).
type sourceCredentialRequest struct {
	Name        string       `json:"name,omitempty"`
	Kind        string       `json:"kind,omitempty"`
	Prefix      string       `json:"prefix,omitempty"`
	Description string       `json:"description,omitempty"`
	Username    string       `json:"username,omitempty"`
	Token       string       `json:"token,omitempty"`
	PrivateKey  string       `json:"private_key,omitempty"`
	HostKey     string       `json:"host_key,omitempty"`
	Headers     []httpHeader `json:"headers,omitempty"`
}

// sourceCredentialView is the JSON form of a source credential returned to the
// UI (F-21.3): the non-secret fields plus the secret material redacted (or,
// for an http header, returned in plaintext when it is not marked sensitive).
type sourceCredentialView struct {
	Id          int64        `json:"id"`
	Name        string       `json:"name"`
	Kind        string       `json:"kind"`
	Prefix      string       `json:"prefix"`
	Description string       `json:"description,omitempty"`
	Username    string       `json:"username,omitempty"`
	Token       string       `json:"token"`
	PrivateKey  string       `json:"private_key"`
	HostKey     string       `json:"host_key"`
	Headers     []httpHeader `json:"headers,omitempty"`
}

// toSourceCredentialView renders a stored credential for the UI (F-21.3): the
// secret material is redacted, except an http header value that is not marked
// sensitive (returned in plaintext).
func toSourceCredentialView(c *dbpb.SourceCredential) *sourceCredentialView {
	v := &sourceCredentialView{
		Id:          c.GetId(),
		Name:        c.GetName(),
		Kind:        c.GetKind(),
		Prefix:      c.GetPrefix(),
		Description: c.GetDescription(),
		Username:    c.GetUsername(),
		Token:       sourceCredentialRedacted,
		PrivateKey:  sourceCredentialRedacted,
		HostKey:     sourceCredentialRedacted,
	}
	// Each http header's value is redacted only when it is marked sensitive; a
	// non-sensitive header is stored in plaintext and returned as-is, so it is
	// easily viewed and modified.
	for _, h := range c.GetHeaders() {
		value := sourceCredentialRedacted
		if !h.GetSensitive() {
			value = h.GetValue()
		}
		v.Headers = append(v.Headers, httpHeader{Name: h.GetName(), Value: value, Sensitive: h.GetSensitive()})
	}
	return v
}

// listSourceCredentials lists every source credential (F-21.3), secret material
// redacted. It requires source-credentials.can-view.
func (s *Server) listSourceCredentials(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermSourceCredentialsView, authz.Resource{}) {
		return
	}
	resp, err := s.clients.Database.ListSourceCredentials(r.Context(), &dbpb.ListSourceCredentialsRequest{})
	if err != nil {
		grpcError(w, err)
		return
	}
	views := make([]*sourceCredentialView, 0, len(resp.GetCredentials()))
	for _, c := range resp.GetCredentials() {
		views = append(views, toSourceCredentialView(c))
	}
	writeJSON(w, http.StatusOK, views)
}

// getSourceCredential returns the named source credential (F-21.3), secret
// material redacted. It requires source-credentials.can-view.
func (s *Server) getSourceCredential(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermSourceCredentialsView, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "credential name is required")
		return
	}
	cred, err := s.clients.Database.GetSourceCredential(r.Context(), &dbpb.GetSourceCredentialRequest{Name: name})
	if err != nil {
		grpcError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSourceCredentialView(cred))
}

// createSourceCredential adds a source credential (F-21.3). The body carries
// the plaintext secret material, which the API encrypts before storing. It
// requires source-credentials.can-manage. On success it records an audit event
// and publishes a source_credential event.
func (s *Server) createSourceCredential(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermSourceCredentialsManage, authz.Resource{}) {
		return
	}
	var req sourceCredentialRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.Name == "" {
		httpError(w, http.StatusBadRequest, "name is required")
		return
	}
	encrypted, err := s.sourceCredentialSecretsToProto(r.Context(), &req)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid secret material: %v", err)
		return
	}
	cred, err := s.clients.Database.CreateSourceCredential(r.Context(), &dbpb.CreateSourceCredentialRequest{
		Name:        req.Name,
		Kind:        req.Kind,
		Prefix:      req.Prefix,
		Description: req.Description,
		Username:    req.Username,
		Token:       encrypted.token,
		PrivateKey:  encrypted.privateKey,
		HostKey:     encrypted.hostKey,
		Headers:     encrypted.headers,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionSourceCredentialCreate,
		TargetKind: audit.TargetSourceCredential,
		TargetID:   cred.GetName(),
		TargetName: cred.GetName(),
		Outcome:    audit.OutcomeSuccess,
	})
	s.publishSourceCredentialChange()
	writeJSON(w, http.StatusCreated, toSourceCredentialView(cred))
}

// updateSourceCredential edits a source credential's prefix, description, and
// secret material (F-21.3). The body carries the new plaintext secret material,
// which the API re-encrypts before storing (a full replacement). It requires
// source-credentials.can-manage. On success it records an audit event and
// publishes a source_credential event.
func (s *Server) updateSourceCredential(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermSourceCredentialsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "credential name is required")
		return
	}
	var req sourceCredentialRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	encrypted, err := s.sourceCredentialSecretsToProto(r.Context(), &req)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid secret material: %v", err)
		return
	}
	cred, err := s.clients.Database.UpdateSourceCredential(r.Context(), &dbpb.UpdateSourceCredentialRequest{
		Name:        name,
		Prefix:      req.Prefix,
		Description: req.Description,
		Username:    req.Username,
		Token:       encrypted.token,
		PrivateKey:  encrypted.privateKey,
		HostKey:     encrypted.hostKey,
		Headers:     encrypted.headers,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionSourceCredentialUpdate,
		TargetKind: audit.TargetSourceCredential,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
	})
	s.publishSourceCredentialChange()
	writeJSON(w, http.StatusOK, toSourceCredentialView(cred))
}

// deleteSourceCredential removes a source credential by name (F-21.3). It
// requires source-credentials.can-manage. On success it records an audit event
// and publishes a source_credential event.
func (s *Server) deleteSourceCredential(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermSourceCredentialsManage, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		httpError(w, http.StatusBadRequest, "credential name is required")
		return
	}
	// A credential in use by a source cannot be deleted (F-21.3.3): deleting it
	// would leave the source unable to authenticate. The count is over the
	// sources that name the credential explicitly (a source matched by prefix
	// does not name it, so it does not block the delete).
	count, err := s.clients.Database.CountSourcesByCredential(r.Context(), &dbpb.CountSourcesByCredentialRequest{Credential: name})
	if err != nil {
		grpcError(w, err)
		return
	}
	if count.GetCount() > 0 {
		httpError(w, http.StatusConflict, "credential %q is referenced by %d source(s); detach them first", name, count.GetCount())
		return
	}
	if _, err := s.clients.Database.DeleteSourceCredential(r.Context(), &dbpb.DeleteSourceCredentialRequest{Name: name}); err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionSourceCredentialDelete,
		TargetKind: audit.TargetSourceCredential,
		TargetID:   name,
		TargetName: name,
		Outcome:    audit.OutcomeSuccess,
	})
	s.publishSourceCredentialChange()
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// sourceCredentialSecrets is the encrypted form of a credential's secret
// material (the values to hand to the Database service).
type sourceCredentialSecrets struct {
	token      string
	privateKey string
	hostKey    string
	headers    []*dbpb.HTTPHeader
}

// sourceCredentialSecretsToProto encrypts a credential's plaintext secret
// material with the API's secret store (F-12) before it is handed to the
// Database service, so the database only ever sees ciphertext. It encrypts the
// token, the git-ssh private key, and the git-ssh host key; for an http
// credential it encrypts each header's value only when that header is marked
// sensitive (otherwise the header value is stored in plaintext). It is an error
// to supply secret material when no store is available (the API cannot encrypt
// it).
func (s *Server) sourceCredentialSecretsToProto(ctx context.Context, req *sourceCredentialRequest) (*sourceCredentialSecrets, error) {
	out := &sourceCredentialSecrets{}
	needsStore := req.Token != "" || req.PrivateKey != "" || req.HostKey != ""
	for _, h := range req.Headers {
		if req.Kind == "http" && h.Sensitive && h.Value != "" {
			needsStore = true
			break
		}
	}
	if !needsStore {
		out.headers = toProtoHeaders(req.Headers)
		return out, nil
	}
	if s.secretStore == nil {
		return nil, fmt.Errorf("no secret store is available")
	}
	if req.Token != "" {
		v, err := s.secretStore.Encrypt(ctx, req.Token)
		if err != nil {
			return nil, fmt.Errorf("encrypt token: %w", err)
		}
		out.token = v
	}
	if req.PrivateKey != "" {
		v, err := s.secretStore.Encrypt(ctx, req.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("encrypt private key: %w", err)
		}
		out.privateKey = v
	}
	if req.HostKey != "" {
		v, err := s.secretStore.Encrypt(ctx, req.HostKey)
		if err != nil {
			return nil, fmt.Errorf("encrypt host key: %w", err)
		}
		out.hostKey = v
	}
	// Each http header's value is encrypted only when it is marked sensitive;
	// a non-sensitive header is stored in plaintext.
	for _, h := range req.Headers {
		value := h.Value
		if req.Kind == "http" && h.Sensitive && h.Value != "" {
			v, err := s.secretStore.Encrypt(ctx, h.Value)
			if err != nil {
				return nil, fmt.Errorf("encrypt header %q value: %w", h.Name, err)
			}
			value = v
		}
		out.headers = append(out.headers, &dbpb.HTTPHeader{Name: h.Name, Value: value, Sensitive: h.Sensitive})
	}
	return out, nil
}

// toProtoHeaders converts a request's http header list (plaintext values) to
// its Database-service form, leaving the values as given (the caller encrypts
// the sensitive ones first).
func toProtoHeaders(hs []httpHeader) []*dbpb.HTTPHeader {
	out := make([]*dbpb.HTTPHeader, 0, len(hs))
	for i := range hs {
		out = append(out, &dbpb.HTTPHeader{Name: hs[i].Name, Value: hs[i].Value, Sensitive: hs[i].Sensitive})
	}
	return out
}

// publishSourceCredentialChange reads the current set of source credentials
// (F-21.3) and publishes them as a source_credential event (secret material
// redacted), so the UI's credentials view stays in sync across replicas. It is
// best-effort: a failure to read the credentials is silently dropped (the UI
// resynchronizes from the snapshot on reconnect).
func (s *Server) publishSourceCredentialChange() {
	if s.hub == nil || s.clients.Database == nil {
		return
	}
	resp, err := s.clients.Database.ListSourceCredentials(context.Background(), &dbpb.ListSourceCredentialsRequest{})
	if err != nil {
		return
	}
	s.hub.Publish(Event{Type: EventSourceCredential, SourceCredentials: resp.GetCredentials()})
}

// selectSourceCredential selects the credential to use for a source (F-21.3.2):
// if the source names a credential, that credential is used; otherwise the
// source's reference is matched against every credential's prefix and the
// longest matching prefix wins. A source matching no credential uses no
// credential (a public source). It returns nil (and no error) when no
// credential applies.
func (s *Server) selectSourceCredential(ctx context.Context, credentialName, reference string) (*dbpb.SourceCredential, error) {
	if credentialName != "" {
		cred, err := s.clients.Database.GetSourceCredential(ctx, &dbpb.GetSourceCredentialRequest{Name: credentialName})
		if err != nil {
			return nil, fmt.Errorf("source names credential %q: %w", credentialName, err)
		}
		return cred, nil
	}
	resp, err := s.clients.Database.ListSourceCredentials(ctx, &dbpb.ListSourceCredentialsRequest{})
	if err != nil {
		return nil, err
	}
	var best *dbpb.SourceCredential
	bestLen := -1
	for _, c := range resp.GetCredentials() {
		if strings.HasPrefix(reference, c.GetPrefix()) && len(c.GetPrefix()) > bestLen {
			best = c
			bestLen = len(c.GetPrefix())
		}
	}
	return best, nil
}

// decryptSourceCredential decrypts a stored credential's secret material (F-12)
// and renders it as the source-fetcher's plaintext form (F-21.3.2): the token,
// the git-ssh private key + host key, and — for an http credential — the list
// of headers (each value decrypted when it was marked sensitive). It is called
// for a single fetch (the sync routine, F-21.5.2); the plaintext is handed to
// the fetcher over the gRPC call and is not persisted anywhere else and is not
// logged. A nil credential (a public source) returns nil.
func (s *Server) decryptSourceCredential(ctx context.Context, cred *dbpb.SourceCredential) (*pipelinesourcepb.SourceCredential, error) {
	if cred == nil {
		return nil, nil
	}
	out := &pipelinesourcepb.SourceCredential{
		Kind:     cred.GetKind(),
		Username: cred.GetUsername(),
	}
	// The token (git-https) is always secret material (encrypted at rest).
	if cred.GetToken() != "" {
		if s.secretStore == nil {
			return nil, fmt.Errorf("no secret store is available to decrypt the credential's token")
		}
		token, err := s.secretStore.Decrypt(ctx, cred.GetToken())
		if err != nil {
			return nil, fmt.Errorf("decrypt credential %q token: %w", cred.GetName(), err)
		}
		out.Token = token
	}
	// The git-ssh private key + host key are secret material (encrypted at
	// rest).
	if cred.GetPrivateKey() != "" || cred.GetHostKey() != "" {
		if s.secretStore == nil {
			return nil, fmt.Errorf("no secret store is available to decrypt the credential's key")
		}
		if cred.GetPrivateKey() != "" {
			key, err := s.secretStore.Decrypt(ctx, cred.GetPrivateKey())
			if err != nil {
				return nil, fmt.Errorf("decrypt credential %q private key: %w", cred.GetName(), err)
			}
			out.PrivateKey = key
		}
		if cred.GetHostKey() != "" {
			host, err := s.secretStore.Decrypt(ctx, cred.GetHostKey())
			if err != nil {
				return nil, fmt.Errorf("decrypt credential %q host key: %w", cred.GetName(), err)
			}
			out.HostKey = host
		}
	}
	// Each http header's value is secret material only when it was marked
	// sensitive (encrypted at rest); otherwise it is stored in plaintext and is
	// used as-is.
	if cred.GetKind() == "http" {
		for _, h := range cred.GetHeaders() {
			value := h.GetValue()
			if h.GetSensitive() && h.GetValue() != "" {
				if s.secretStore == nil {
					return nil, fmt.Errorf("no secret store is available to decrypt the credential's header value")
				}
				v, err := s.secretStore.Decrypt(ctx, h.GetValue())
				if err != nil {
					return nil, fmt.Errorf("decrypt credential %q header %q value: %w", cred.GetName(), h.GetName(), err)
				}
				value = v
			}
			out.Headers = append(out.Headers, &pipelinesourcepb.HTTPHeader{Name: h.GetName(), Value: value})
		}
	}
	return out, nil
}
