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
	// users is the user directory for username/password authentication
	// (register, login, and role management). It is nil when the IdP is not
	// configured with a user store, in which case the user endpoints are
	// disabled (they respond 501).
	users  UserStore
	logger *slog.Logger
}

// NewServer creates the IdP HTTP server over the given key manager and
// authorization-code store. The username/password endpoints (register, login,
// user management) are disabled until a user store is attached with
// WithUsers.
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

// WithUsers attaches a user store, enabling the username/password endpoints
// (register, login, and user/role management). It returns the server so it
// can be chained: idp.NewServer(...).WithUsers(idp.NewDBUserStore(db)).
func (s *Server) WithUsers(users UserStore) *Server {
	s.users = users
	return s
}

// Handler builds the IdP's HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", s.handleDiscovery)
	mux.HandleFunc("GET /jwks", s.handleJWKS)
	mux.HandleFunc("GET /auth", s.handleAuthorize)
	mux.HandleFunc("POST /token", s.handleToken)
	// Username/password authentication (register, login) and user/role
	// management. The API proxies its /api/login, /api/register, and
	// /api/users endpoints to these; the real logic (password verification,
	// token minting, user storage) lives here.
	mux.HandleFunc("POST /register", s.handleRegister)
	mux.HandleFunc("POST /login", s.handleLogin)
	mux.HandleFunc("GET /users", s.handleListUsers)
	mux.HandleFunc("POST /users", s.handleCreateUser)
	mux.HandleFunc("PUT /users/{id}", s.handleUpdateUser)
	mux.HandleFunc("DELETE /users/{id}", s.handleDeleteUser)
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

// ---------------------------------------------------------------------------
// Username/password authentication and user management
// ---------------------------------------------------------------------------

// requireUsers reports whether the IdP has a user store configured. When it
// does not, the username/password endpoints are disabled and respond 501.
func (s *Server) requireUsers(w http.ResponseWriter) bool {
	if s.users == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "user store is not configured"})
		return false
	}
	return true
}

// userTokenClaims builds the OIDC claims for a user's token: the standard
// identity claims (subject, name, email) plus the user's roles, so the API can
// enforce role-based access from the token.
func (s *Server) userTokenClaims(r *http.Request, user *User, audience string) map[string]any {
	name := user.FirstName + " " + user.LastName
	if name == "  " {
		name = user.Email
	}
	claims := map[string]any{
		"iss":   s.issuerFor(r),
		"sub":   user.ID,
		"aud":   audience,
		"exp":   time.Now().Add(s.cfg.TokenLifetime).Unix(),
		"iat":   time.Now().Unix(),
		"name":  name,
		"email": user.Email,
	}
	if len(user.Roles) > 0 {
		claims["roles"] = user.Roles
	}
	return claims
}

// registerRequest is the JSON body of POST /register: the required fields to
// create a user (first name, last name, email, and password) plus an optional
// initial role set.
type registerRequest struct {
	FirstName string   `json:"first_name"`
	LastName  string   `json:"last_name"`
	Email     string   `json:"email"`
	Password  string   `json:"password"`
	Roles     []string `json:"roles,omitempty"`
}

// registerResponse is the response of POST /register: the created user's
// profile (never the password hash).
type registerResponse struct {
	ID        string   `json:"id"`
	FirstName string   `json:"first_name"`
	LastName  string   `json:"last_name"`
	Email     string   `json:"email"`
	Roles     []string `json:"roles"`
}

// handleRegister creates a new user from the request's first/last/email and
// password. The password is hashed (bcrypt) before it is stored; the plaintext
// is never persisted. A duplicate email is an error (409).
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsers(w) {
		return
	}
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body: " + err.Error()})
		return
	}
	if req.Email == "" || req.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "email and password are required"})
		return
	}
	// Reject a user that already exists (checked before hashing so a duplicate
	// does not clobber the existing user's password).
	if _, err := s.users.GetByEmail(r.Context(), req.Email); err == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "a user with that email already exists"})
		return
	}
	hash, err := HashPassword(req.Password)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// The unauthenticated register path does not honour client-supplied roles
	// (that would let anyone self-assign a role). Instead it assigns a
	// default: the first user ever registered becomes an admin (so a local
	// run has a way in), and every later user gets the default "user" role.
	// An admin can change a user's roles afterwards via the user-management
	// endpoints.
	roles := s.defaultRoles(r.Context())
	user, err := s.users.Create(r.Context(), &User{
		FirstName:    req.FirstName,
		LastName:     req.LastName,
		Email:        req.Email,
		PasswordHash: hash,
		Roles:        roles,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "a user with that email already exists"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.logger.Info("idp: registered user", "email", user.Email, "roles", user.Roles)
	writeJSON(w, http.StatusCreated, registerResponse{
		ID:        user.ID,
		FirstName: user.FirstName,
		LastName:  user.LastName,
		Email:     user.Email,
		Roles:     nonNil(user.Roles),
	})
}

// defaultRoles returns the role set assigned to a user created via the
// unauthenticated register endpoint: the first user ever registered becomes
// an admin (so a local run has a way in), and every later user gets the
// default "user" role.
func (s *Server) defaultRoles(ctx context.Context) []string {
	users, err := s.users.List(ctx)
	if err == nil && len(users) == 0 {
		return []string{"admin"}
	}
	return []string{"user"}
}

// loginRequest is the JSON body of POST /login: the user's email and password.
type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	// Audience is the audience to stamp on the minted token (the audience the
	// caller's token verifier checks). When empty the IdP uses its default
	// audience. The API passes its own verifier audience (client_id or
	// token_audience) so the token it receives is one it will accept.
	Audience string `json:"audience,omitempty"`
}

// loginResponse is the response of POST /login: the OIDC token the IdP minted
// for the user (the same shape the authorization-code flow returns), plus the
// user's profile and roles.
type loginResponse struct {
	AccessToken string   `json:"access_token"`
	TokenType   string   `json:"token_type"`
	ExpiresIn   int      `json:"expires_in"`
	IDToken     string   `json:"id_token"`
	ID          string   `json:"id"`
	FirstName   string   `json:"first_name"`
	LastName    string   `json:"last_name"`
	Email       string   `json:"email"`
	Roles       []string `json:"roles"`
}

// handleLogin verifies the user's credentials (email + password) against the
// stored password hash and, on success, mints an OIDC token for the user
// (stamped with the user's roles) and returns it. A bad password or unknown
// user is a 401. The token is verified by the API against the IdP's JWKS, so
// the login flow is the same OIDC token the API already accepts.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsers(w) {
		return
	}
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body: " + err.Error()})
		return
	}
	if req.Email == "" || req.Password == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "email and password are required"})
		return
	}
	user, err := s.users.GetByEmail(r.Context(), req.Email)
	if err != nil {
		// Unknown user: fail with the same response as a bad password so the
		// endpoint does not reveal which emails are registered.
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	if !VerifyPassword(user.PasswordHash, req.Password) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	// The token is issued for the caller's verifier audience: the audience the
	// caller passes (the API passes its own client_id / token_audience so the
	// token it receives is one it will accept), or the IdP's default audience
	// when none is supplied.
	audience := req.Audience
	if audience == "" {
		if len(s.cfg.Audiences) > 0 {
			audience = s.cfg.Audiences[0]
		} else {
			audience = "cdrom-api"
		}
	}
	claims := s.userTokenClaims(r, user, audience)
	token, err := s.km.Sign(claims)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.logger.Info("idp: user login", "email", user.Email, "roles", user.Roles)
	writeJSON(w, http.StatusOK, loginResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   int(s.cfg.TokenLifetime.Seconds()),
		IDToken:     token,
		ID:          user.ID,
		FirstName:   user.FirstName,
		LastName:    user.LastName,
		Email:       user.Email,
		Roles:       nonNil(user.Roles),
	})
}

// userResponse is the JSON form of a user for the management endpoints: the
// profile and roles, never the password hash.
type userResponse struct {
	ID        string   `json:"id"`
	FirstName string   `json:"first_name"`
	LastName  string   `json:"last_name"`
	Email     string   `json:"email"`
	Roles     []string `json:"roles"`
}

func toUserResponse(u *User) userResponse {
	return userResponse{
		ID:        u.ID,
		FirstName: u.FirstName,
		LastName:  u.LastName,
		Email:     u.Email,
		Roles:     nonNil(u.Roles),
	}
}

// handleListUsers returns all registered users (profile + roles, no password
// hash).
func (s *Server) handleListUsers(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsers(w) {
		return
	}
	users, err := s.users.List(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	out := make([]userResponse, 0, len(users))
	for _, u := range users {
		out = append(out, toUserResponse(u))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleCreateUser creates a user from the request's profile, password, and
// roles (the same as /register, but also usable to create a user with an
// explicit role set).
func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsers(w) {
		return
	}
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body: " + err.Error()})
		return
	}
	if req.Email == "" || req.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "email and password are required"})
		return
	}
	if _, err := s.users.GetByEmail(r.Context(), req.Email); err == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "a user with that email already exists"})
		return
	}
	hash, err := HashPassword(req.Password)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	user, err := s.users.Create(r.Context(), &User{
		FirstName:    req.FirstName,
		LastName:     req.LastName,
		Email:        req.Email,
		PasswordHash: hash,
		Roles:        req.Roles,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, toUserResponse(user))
}

// updateUserRequest is the JSON body of PUT /users/{id}: the profile fields
// and/or roles to change. An empty password leaves the existing password
// untouched.
type updateUserRequest struct {
	FirstName string   `json:"first_name,omitempty"`
	LastName  string   `json:"last_name,omitempty"`
	Email     string   `json:"email,omitempty"`
	Password  string   `json:"password,omitempty"`
	Roles     []string `json:"roles,omitempty"`
}

// handleUpdateUser updates a user's profile and/or roles (and password when
// supplied). The user is identified by the {id} path parameter.
func (s *Server) handleUpdateUser(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsers(w) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user id is required"})
		return
	}
	var req updateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body: " + err.Error()})
		return
	}
	update := &User{ID: id, FirstName: req.FirstName, LastName: req.LastName, Email: req.Email, Roles: req.Roles}
	if req.Password != "" {
		hash, err := HashPassword(req.Password)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		update.PasswordHash = hash
	}
	user, err := s.users.Update(r.Context(), update)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "user not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, toUserResponse(user))
}

// handleDeleteUser removes a user by the {id} path parameter.
func (s *Server) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	if !s.requireUsers(w) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "user id is required"})
		return
	}
	if err := s.users.Delete(r.Context(), id); err != nil {
		if status.Code(err) == codes.NotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "user not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
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
