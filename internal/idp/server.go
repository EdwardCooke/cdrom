package idp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// codeTTL is how long an issued authorization code stays valid.
const codeTTL = 10 * time.Minute

// defaultSubject is the OIDC subject issued when the authorization request
// does not specify one.
const defaultSubject = "dev-user"

// authCode is a single-use authorization code handed to the client at the
// authorization endpoint and exchanged for tokens at the token endpoint.
type authCode struct {
	code          string
	clientID      string
	redirectURI   string
	codeChallenge string // empty when PKCE was not used
	subject       string
	name          string
	email         string
	createdAt     time.Time
}

// AuthCodeStore persists the IdP's single-use OIDC authorization codes so
// that the /auth request that mints a code and the /token request that
// redeems it can be served by different IdP replicas. The production
// implementation stores codes in the Database service; tests use an
// in-memory implementation.
type AuthCodeStore interface {
	// Store persists a newly minted authorization code.
	Store(ctx context.Context, code *authCode) error
	// Consume atomically fetches and deletes a code so it can be redeemed by
	// at most one replica. A missing code returns an error with
	// status.Code == codes.NotFound.
	Consume(ctx context.Context, code string) (*authCode, error)
	// Prune drops codes created before the given instant.
	Prune(ctx context.Context, before time.Time) error
}

// dbAuthCodeStore is an AuthCodeStore backed by the Database service.
type dbAuthCodeStore struct {
	db dbpb.DatabaseClient
}

func (s *dbAuthCodeStore) Store(ctx context.Context, code *authCode) error {
	req := &dbpb.StoreIDPAuthCodeRequest{Code: &dbpb.IDPAuthCode{
		Code:          code.code,
		ClientId:      code.clientID,
		RedirectUri:   code.redirectURI,
		CodeChallenge: code.codeChallenge,
		Subject:       code.subject,
		Name:          code.name,
		Email:         code.email,
		CreatedAt:     timestamppb.New(code.createdAt),
	}}
	if _, err := s.db.StoreIDPAuthCode(ctx, req); err != nil {
		return fmt.Errorf("idp: store auth code: %w", err)
	}
	return nil
}

func (s *dbAuthCodeStore) Consume(ctx context.Context, code string) (*authCode, error) {
	resp, err := s.db.ConsumeIDPAuthCode(ctx, &dbpb.ConsumeIDPAuthCodeRequest{Code: code})
	if err != nil {
		return nil, err
	}
	c := resp
	return &authCode{
		code:          c.GetCode(),
		clientID:      c.GetClientId(),
		redirectURI:   c.GetRedirectUri(),
		codeChallenge: c.GetCodeChallenge(),
		subject:       c.GetSubject(),
		name:          c.GetName(),
		email:         c.GetEmail(),
		createdAt:     time.Now(),
	}, nil
}

func (s *dbAuthCodeStore) Prune(ctx context.Context, before time.Time) error {
	_, err := s.db.PruneIDPAuthCodes(ctx, &dbpb.PruneIDPAuthCodesRequest{CreatedBefore: timestamppb.New(before)})
	if err != nil {
		return fmt.Errorf("idp: prune auth codes: %w", err)
	}
	return nil
}

// Server is the local OIDC identity provider's HTTP handler.
type Server struct {
	cfg    config.IdPConfig
	issuer string
	km     *KeyManager
	codes  AuthCodeStore
	logger *slog.Logger
}

// NewServer creates the IdP HTTP server over the given key manager and
// authorization-code store.
func NewServer(cfg config.IdPConfig, km *KeyManager, codes AuthCodeStore, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		cfg:    cfg,
		issuer: cfg.EffectiveIssuer(),
		km:     km,
		codes:  codes,
		logger: logger,
	}
}

// Handler builds the IdP's HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.handleIndex)
	mux.HandleFunc("GET /.well-known/openid-configuration", s.handleDiscovery)
	mux.HandleFunc("GET /jwks", s.handleJWKS)
	mux.HandleFunc("GET /auth", s.handleAuthorize)
	mux.HandleFunc("POST /token", s.handleToken)
	return mux
}

// StartRotation runs the key-rotation loop, checking every CheckInterval
// whether the current signing key needs rotating. It blocks until ctx is
// cancelled.
func (s *Server) StartRotation(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.km.RotateIfNeeded()
		}
	}
}

// handleDiscovery serves the OIDC discovery document. The issuer is derived
// from the request (scheme + host) so the document is correct no matter which
// host or port the IdP is reached on.
func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	issuer := s.issuerFor(r)
	doc := map[string]any{
		"issuer":                                issuer,
		"authorization_endpoint":                issuer + "/auth",
		"token_endpoint":                        issuer + "/token",
		"jwks_uri":                              issuer + "/jwks",
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
		"scopes_supported":                      []string{"openid", "profile", "email"},
		"code_challenge_methods_supported":      []string{"S256"},
		"token_endpoint_auth_methods_supported": []string{"none"},
	}
	writeJSON(w, http.StatusOK, doc)
}

// issuerFor returns the issuer to advertise for a request: the configured
// issuer when it matches the request host, otherwise the request's own
// scheme + host. Deriving it from the request keeps the discovery document
// and the token iss claim consistent with the host the client actually used.
func (s *Server) issuerFor(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	return scheme + "://" + r.Host
}

// handleJWKS serves the current signing keys (current + not-yet-expired
// predecessors).
func (s *Server) handleJWKS(w http.ResponseWriter, r *http.Request) {
	keys, err := s.km.JWKS()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

// handleAuthorize is the authorization endpoint. It validates the request,
// mints a single-use authorization code, and redirects the client back to its
// redirect_uri with the code and the echoed state.
func (s *Server) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	redirectURI := q.Get("redirect_uri")
	state := q.Get("state")
	clientID := q.Get("client_id")
	challenge := q.Get("code_challenge")
	challengeMethod := q.Get("code_challenge_method")
	subject := q.Get("user")
	if subject == "" {
		subject = defaultSubject
	}
	if redirectURI == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "redirect_uri is required"})
		return
	}
	if challengeMethod != "" && challengeMethod != "S256" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported code_challenge_method"})
		return
	}

	code, err := randomToken(24)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// Prune expired codes opportunistically, then store the new one.
	_ = s.codes.Prune(r.Context(), time.Now().Add(-codeTTL))
	if err := s.codes.Store(r.Context(), &authCode{
		code:          code,
		clientID:      clientID,
		redirectURI:   redirectURI,
		codeChallenge: challenge,
		subject:       subject,
		name:          subject,
		email:         subject + "@example.com",
		createdAt:     time.Now(),
	}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	loc, err := url.Parse(redirectURI)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid redirect_uri"})
		return
	}
	vals := loc.Query()
	vals.Set("code", code)
	if state != "" {
		vals.Set("state", state)
	}
	loc.RawQuery = vals.Encode()
	s.logger.Info("idp: issued authorization code", "client", clientID, "user", subject)
	http.Redirect(w, r, loc.String(), http.StatusFound)
}

// handleToken is the token endpoint. It exchanges an authorization code for
// an ID token (verifying PKCE when a challenge was provided).
func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid form"})
		return
	}
	switch r.FormValue("grant_type") {
	case "authorization_code":
		if err := s.handleAuthCodeGrant(w, r); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		return
	case "job_token":
		s.handleJobTokenGrant(w, r)
		return
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported grant_type"})
		return
	}
}

// handleAuthCodeGrant exchanges an authorization code for an ID token
// (verifying PKCE when a challenge was provided).
func (s *Server) handleAuthCodeGrant(w http.ResponseWriter, r *http.Request) error {
	code := r.FormValue("code")
	clientID := r.FormValue("client_id")
	redirectURI := r.FormValue("redirect_uri")
	verifier := r.FormValue("code_verifier")

	// Consume the code atomically (single use). A missing code is a NotFound.
	ac, err := s.codes.Consume(r.Context(), code)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return fmt.Errorf("invalid authorization code")
		}
		return fmt.Errorf("consume authorization code: %w", err)
	}
	if ac.clientID != clientID || ac.redirectURI != redirectURI {
		return fmt.Errorf("client/redirect mismatch")
	}
	if ac.codeChallenge != "" {
		if verifier == "" || !verifyPKCE(ac.codeChallenge, verifier) {
			return fmt.Errorf("PKCE verification failed")
		}
	}

	now := time.Now()
	claims := map[string]any{
		"iss":   s.issuerFor(r),
		"sub":   ac.subject,
		"aud":   clientID,
		"exp":   now.Add(s.cfg.TokenLifetime).Unix(),
		"iat":   now.Unix(),
		"name":  ac.name,
		"email": ac.email,
	}
	idToken, err := s.km.Sign(claims)
	if err != nil {
		return fmt.Errorf("sign id token: %w", err)
	}
	// The access token is a real signed JWT carrying the same claims as the
	// id token, so clients can present it as `Authorization: Bearer <token>`
	// to the API, which verifies it against the IdP's JWKS. This is what lets
	// clients (curl, the UI) authenticate without a session cookie.
	accessToken, err := s.km.Sign(claims)
	if err != nil {
		return fmt.Errorf("sign access token: %w", err)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token": accessToken,
		"token_type":   "Bearer",
		"expires_in":   int(s.cfg.TokenLifetime.Seconds()),
		"id_token":     idToken,
	})
	return nil
}

// handleJobTokenGrant mints a job token. The only caller is the API, which
// authenticates with its mTLS client certificate (when the IdP serves TLS);
// in plaintext mode (local development) the grant is open. The token is
// scoped to the requested job and audience, so the execution target can
// present it back to the API and to outside resources for the duration of the
// job. An optional audience overrides the IdP's default job-token audiences —
// this is how a job exchanges its token for one with a different audience.
//
// The token's lifetime comes from the request's expires_in (a duration
// string) when present, else the IdP's configured token lifetime. The API
// passes a long placeholder (a week) for the main job token — so it
// effectively never expires, its validity being gated on the job's live
// status in the database — and a short lifetime for an exchanged token.
func (s *Server) handleJobTokenGrant(w http.ResponseWriter, r *http.Request) {
	// When the IdP serves TLS, job-token minting requires the caller to have
	// presented a client certificate (the API's). The listener uses
	// RequireAnyClientCert so browsers and the API's OIDC flow can complete
	// the handshake without a cert; this check then rejects any TLS client
	// that did not present one. In plaintext mode (r.TLS == nil, local dev)
	// the grant is open.
	if r.TLS != nil && len(r.TLS.PeerCertificates) == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "job token minting requires a client certificate"})
		return
	}
	jobID := r.FormValue("job_id")
	if jobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id is required"})
		return
	}
	pipelineID := r.FormValue("pipeline_id")
	targetGroup := r.FormValue("target_group")
	audience := r.FormValue("audience")
	triggerName := r.FormValue("trigger_name")
	triggerType := r.FormValue("trigger_type")

	var aud []string
	if audience != "" {
		aud = []string{audience}
	} else {
		aud = s.jobTokenAudiences()
	}

	now := time.Now()
	// The token's lifetime. The API passes expires_in explicitly: the main job
	// token carries a long placeholder (a week) so it effectively never
	// expires — the API gates its validity on the job's live status in the
	// database, not the exp claim — while an exchanged token carries a short
	// lifetime. When expires_in is absent the IdP falls back to its configured
	// token lifetime.
	lifetime := s.cfg.TokenLifetime
	if v := r.FormValue("expires_in"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			lifetime = d
		}
	}
	claims := map[string]any{
		"iss":          s.issuerFor(r),
		"sub":          "job:" + jobID,
		"aud":          aud,
		"exp":          now.Add(lifetime).Unix(),
		"iat":          now.Unix(),
		"job_id":       jobID,
		"pipeline_id":  pipelineID,
		"target_group": targetGroup,
		"token_type":   "job",
	}
	// The trigger that started the run (F-09): let a job's steps — and the
	// outside resources they call — see how the run was started.
	if triggerName != "" {
		claims["trigger_name"] = triggerName
	}
	if triggerType != "" {
		claims["trigger_type"] = triggerType
	}
	// The upstream OIDC claims a webhook caller presented (F-09), prefixed
	// with upstream_ so they are distinguishable from the job's own claims.
	// The API passes them JSON-encoded as a JSON object; each claim keeps its
	// structure (a claim that is itself an object or a list, e.g. GitLab's
	// "user_identities" or "job_config", is stamped as that object/list, not
	// flattened to a string). A value that is not a JSON object is ignored.
	if encoded := r.FormValue("upstream_claims"); encoded != "" {
		var upstream map[string]any
		if err := json.Unmarshal([]byte(encoded), &upstream); err == nil {
			for k, v := range upstream {
				claims["upstream_"+k] = v
			}
		}
	}
	token, err := s.km.Sign(claims)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.logger.Info("idp: minted job token", "job", jobID, "pipeline", pipelineID, "group", targetGroup, "audience", audience, "trigger", triggerName, "trigger_type", triggerType, "lifetime", lifetime)
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token": token,
		"token_type":   "Bearer",
		"expires_in":   int(lifetime.Seconds()),
	})
}

// jobTokenAudiences returns the audience values to stamp on job tokens: the
// configured audiences, or a default when none are set.
func (s *Server) jobTokenAudiences() []string {
	if len(s.cfg.Audiences) > 0 {
		return s.cfg.Audiences
	}
	return []string{"cdrom-api"}
}

// handleIndex is a small landing page for humans hitting the IdP root.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "cdrom local OIDC identity provider\nissuer: %s\n\nendpoints:\n  GET  /.well-known/openid-configuration\n  GET  /jwks\n  GET  /auth\n  POST /token\n", s.issuer)
}

// verifyPKCE reports whether verifier produces the S256 challenge.
func verifyPKCE(challenge, verifier string) bool {
	sum := sha256.Sum256([]byte(verifier))
	computed := base64.RawURLEncoding.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(computed), []byte(challenge)) == 1
}

// randomToken returns a URL-safe hex random string of n bytes.
func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("idp: generate token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
