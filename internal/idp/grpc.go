package idp

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
	"cdrom/internal/metrics"
)

// GRPCServer is the IdP's gRPC surface: its API-only operations (minting job
// tokens and managing the user directory). The API is the sole caller and
// authenticates to the IdP with its mTLS client certificate (when TLS is
// configured), so only a CA-signed client (the API) can mint job tokens or
// manage users. The standard OIDC surface (discovery, JWKS, /auth, and the
// authorization_code token grant) is served over HTTP by Server, because it is
// a browser-based protocol that cannot be expressed as gRPC.
type GRPCServer struct {
	idppb.UnimplementedIdPServer
	cfg             config.IdPConfig
	km              *KeyManager
	users           UserStore
	keys            APIKeyStore
	serviceAccounts ServiceAccountStore
	// groups is the SCIM group directory (F-28.5) serving the read-only
	// group-detail RPCs (ListGroupDetails / GetGroupDetails). When nil those
	// RPCs respond Unimplemented (the surface is unwired — typically SCIM
	// disabled).
	groups GroupStore
	// agentCredentials is the agent-credential ops layer (F-30.2): it signs
	// the JWTs and drives their lifecycle in the Database service's
	// credential directory. When nil the credential RPCs respond
	// Unimplemented (typically the IdP is unwired for credentials).
	agentCredentials *AgentCredentialOps
	// db is the shared Database client the group-detail enrichment consults
	// for the role catalog (the derived_roles projection check). When nil
	// the projection degrades to empty (stored attributes still render).
	db      dbpb.DatabaseClient
	logger  *slog.Logger
	metrics *metrics.IDPMetrics
}

// NewGRPCServer creates the IdP's gRPC server over the given key manager and
// user store. The user store may be nil, in which case the user-management
// RPCs (Login, Register, and the user CRUD) respond with Unimplemented.
func NewGRPCServer(cfg config.IdPConfig, km *KeyManager, users UserStore, logger *slog.Logger) *GRPCServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &GRPCServer{cfg: cfg, km: km, users: users, logger: logger}
}

// SetAPIKeyStore attaches the API-key store (F-25). When left nil the
// API-key RPCs respond with Unimplemented.
func (s *GRPCServer) SetAPIKeyStore(keys APIKeyStore) {
	s.keys = keys
}

// SetServiceAccountStore attaches the service-account store (F-26). When left
// nil the service-account RPCs respond with Unimplemented.
func (s *GRPCServer) SetServiceAccountStore(store ServiceAccountStore) {
	s.serviceAccounts = store
}

// SetGroupStore attaches the SCIM group directory (F-28.5) the read-only
// group-detail RPCs serve. When left nil those RPCs respond Unimplemented.
func (s *GRPCServer) SetGroupStore(store GroupStore) {
	s.groups = store
}

// SetDatabaseClient attaches the shared Database client the group-detail
// enrichment consults for the role catalog. When left nil the derived_roles
// projection degrades to empty (the stored attributes still render).
func (s *GRPCServer) SetDatabaseClient(db dbpb.DatabaseClient) {
	s.db = db
}

// SetAgentCredentialOps attaches the agent-credential ops layer (F-30.2). When
// left nil the credential RPCs (Issue/Get/List/Renew/Rotate/Revoke) respond
// Unimplemented.
func (s *GRPCServer) SetAgentCredentialOps(ops *AgentCredentialOps) {
	s.agentCredentials = ops
}

// SetMetrics attaches the IdP's Prometheus metric set (F-20). The gRPC surface
// uses it to count job-token minting and username/password sign-ins, and to
// keep the user-directory size gauge current. When left nil (metrics
// disabled) all recording is a no-op.
func (s *GRPCServer) SetMetrics(m *metrics.IDPMetrics) {
	s.metrics = m
}

// MintJobToken mints a job token for a dispatched job. The token is scoped to
// the job (job_id, pipeline_id, target_group) and to the API plus any outside
// resources a job may call (audience). An empty audience makes the IdP use its
// default job-token audiences. The token's lifetime is expires_in when set,
// else the IdP's configured token lifetime. The issuer is the request's issuer
// (the caller's configured issuer, so the token verifies against the caller's
// discovery document), falling back to the IdP's own configured issuer.
func (s *GRPCServer) MintJobToken(ctx context.Context, req *idppb.MintJobTokenRequest) (*idppb.MintJobTokenResponse, error) {
	jobID := req.GetJobId()
	if jobID == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	audience := req.GetAudience()
	var aud []string
	if audience != "" {
		aud = []string{audience}
	} else {
		aud = s.jobTokenAudiences()
	}
	issuer := req.GetIssuer()
	if issuer == "" {
		issuer = s.cfg.EffectiveIssuer()
	}
	now := time.Now()
	lifetime := s.cfg.TokenLifetime
	if v := req.GetExpiresIn(); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			lifetime = d
		}
	}
	claims := map[string]any{
		"iss":          issuer,
		"sub":          "job:" + jobID,
		"aud":          aud,
		"exp":          now.Add(lifetime).Unix(),
		"iat":          now.Unix(),
		"job_id":       jobID,
		"pipeline_id":  req.GetPipelineId(),
		"target_group": req.GetTargetGroup(),
		"token_type":   "job",
	}
	if tn := req.GetTriggerName(); tn != "" {
		claims["trigger_name"] = tn
	}
	if tt := req.GetTriggerType(); tt != "" {
		claims["trigger_type"] = tt
	}
	// F-30.4: the agent that started the execution this token was minted for
	// (attribution: the worker spending the token is anonymous to the API
	// apart from it, so its reports credit the agent's JobExecution via this
	// claim). Absent for exchange tokens and pre-F-30.4 styles.
	if an := req.GetAgentName(); an != "" {
		claims["agent_name"] = an
	}
	if encoded := req.GetUpstreamClaims(); encoded != "" {
		var upstream map[string]any
		if err := json.Unmarshal([]byte(encoded), &upstream); err == nil {
			for k, v := range upstream {
				claims["upstream_"+k] = v
			}
		}
	}
	token, err := s.km.Sign(claims)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: sign job token: %v", err)
	}
	s.logger.Info("idp: minted job token", "job", jobID, "pipeline", req.GetPipelineId(),
		"group", req.GetTargetGroup(), "audience", audience, "trigger", req.GetTriggerName(),
		"trigger_type", req.GetTriggerType(), "lifetime", lifetime)
	if s.metrics != nil {
		s.metrics.RecordTokenMinted("job")
	}
	return &idppb.MintJobTokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   int64(lifetime.Seconds()),
	}, nil
}

// jobTokenAudiences returns the audience values to stamp on job tokens: the
// configured audiences, or a default when none are set.
func (s *GRPCServer) jobTokenAudiences() []string {
	if len(s.cfg.Audiences) > 0 {
		return s.cfg.Audiences
	}
	return []string{"cdrom-api"}
}

// Login verifies a user's credentials (username + password) against the
// stored password hash and, on success, mints an OIDC token for the user
// (stamped with the user's roles) and returns it with the user's profile. A
// bad password or unknown user is Unauthenticated. The token is issued for
// the request's audience (the caller's verifier audience) and issuer (the
// caller's configured issuer), so the token the caller receives is one it will
// accept.
func (s *GRPCServer) Login(ctx context.Context, req *idppb.LoginRequest) (*idppb.LoginResponse, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	username := req.GetUsername()
	if username == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.Unauthenticated, "username and password are required")
	}
	user, err := s.users.GetByUsername(ctx, username, true)
	if err != nil {
		// Unknown user: fail with the same response as a bad password so the
		// endpoint does not reveal which usernames are registered.
		if s.metrics != nil {
			s.metrics.RecordLogin("invalid_username")
		}
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	// Sign-in enforcement (F-28.3): state-based rejections fire BEFORE any
	// hash comparison, with distinct messages (the spec mandates them) so
	// the user learns why sign-in is refused. Unknown users still collapse
	// to the generic "invalid credentials" above (no enumeration).
	switch {
	case user.DeletedAt != nil:
		if s.metrics != nil {
			s.metrics.RecordLogin("deprovisioned")
		}
		return nil, status.Error(codes.PermissionDenied, "account has been deprovisioned")
	case !user.Active:
		if s.metrics != nil {
			s.metrics.RecordLogin("deactivated")
		}
		return nil, status.Error(codes.PermissionDenied, "account is deactivated; contact an administrator")
	case user.Provisioned:
		// Channel lockdown (F-28.3): a provisioned user signs in via SSO
		// only — the password channel is closed even if a hash somehow
		// exists (smuggled by legacy state or defect).
		if s.metrics != nil {
			s.metrics.RecordLogin("sso_only")
		}
		return nil, status.Error(codes.PermissionDenied, "account is managed by the identity provider; sign in via SSO")
	}
	// Credential match (F-28.3): the STANDING hash decides, PLUS a
	// TEMPORAL (break-glass) password is accepted only INSIDE its window.
	// Evaluation is CONSTANT-WORK: whichever hash(es) exist are ALWAYS
	// verified (the wall clock only gates ACCEPTANCE, never WHETHER we
	// verify), so a lapse contributes no observable timing difference.
	// CHANNEL MARKING (F-28.4 Slice 4): which credential satisfied the login
	// (standing vs temporal) is reported on the response so the API's
	// local-login switch can admit only temporal sign-ins.
	temporalWindow := user.TempPasswordValidUntil != nil && time.Now().Before(*user.TempPasswordValidUntil)
	var matched bool
	var channel string
	switch {
	case user.PasswordHash != "" && user.TempPasswordHash != "":
		// Standing + temporal: accept EITHER (temporal only inside its
		// window). Both are verified.
		standingMatches := VerifyPassword(s.cfg.PasswordPepper, user.PasswordHash, req.GetPassword())
		temporalMatches := VerifyPassword(s.cfg.PasswordPepper, user.TempPasswordHash, req.GetPassword())
		matched = standingMatches || (temporalWindow && temporalMatches)
		channel = "standing"
		if !standingMatches && temporalMatches {
			channel = "temporal"
		}
	case user.PasswordHash != "":
		matched = VerifyPassword(s.cfg.PasswordPepper, user.PasswordHash, req.GetPassword())
		channel = "standing"
	default:
		// Temporal-only (the break-glass shape): accept iff the presented
		// password matches AND the window holds.
		temporalMatches := VerifyPassword(s.cfg.PasswordPepper, user.TempPasswordHash, req.GetPassword())
		matched = temporalWindow && temporalMatches
		channel = "temporal"
		if temporalMatches && !temporalWindow {
			// Lazy hygiene: the window LAPSED — void the credential now so
			// the periodic sweep (the backstop) catches up promptly.
			// Detached context: a cancelling client must not strand it.
			if serr := s.users.ExpireTemporaryPassword(context.WithoutCancel(ctx), user.ID); serr != nil {
				s.logger.Warn("idp: expire lapsed temporal password", "id", user.ID, "err", serr)
			}
		}
	}
	if !matched {
		if s.metrics != nil {
			s.metrics.RecordLogin("invalid_password")
		}
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	// Local-login switch (F-28.4 Slice 4): when OFF the IdP is SSO-ONLY —
	// standing-channel sign-ins are refused (a distinct denial), while
	// TEMPORAL (break-glass) holders still get through. Registration is
	// likewise refused (see Register).
	if !s.cfg.LocalLoginEnabled && channel == "standing" {
		if s.metrics != nil {
			s.metrics.RecordLogin("local_login_disabled")
		}
		return nil, status.Error(codes.PermissionDenied, "local login is disabled; sign in via SSO")
	}
	audience := req.GetAudience()
	if audience == "" {
		if len(s.cfg.Audiences) > 0 {
			audience = s.cfg.Audiences[0]
		} else {
			audience = "cdrom-api"
		}
	}
	issuer := req.GetIssuer()
	if issuer == "" {
		issuer = s.cfg.EffectiveIssuer()
	}
	now := time.Now()
	claims := map[string]any{
		"iss": issuer,
		// The token's SUBJECT is the user's USERNAME (F-28.4): the admission
		// gate and the approver-set matching correlate on it. The numeric ID
		// travels as a convenience claim.
		"sub":     user.Username,
		"user_id": user.ID,
		"aud":     audience,
		"exp":     now.Add(s.cfg.TokenLifetime).Unix(),
		"iat":     now.Unix(),
		"name":    userName(user),
		"email":   user.Email,
	}
	if len(user.Roles) > 0 {
		claims["roles"] = user.Roles
	}
	token, err := s.km.Sign(claims)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: sign user token: %v", err)
	}
	s.logger.Info("idp: user login", "username", user.Username, "email", user.Email, "roles", user.Roles)
	if s.metrics != nil {
		s.metrics.RecordLogin("success")
		s.metrics.RecordTokenMinted("ui")
	}
	return &idppb.LoginResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   int64(s.cfg.TokenLifetime.Seconds()),
		IdToken:     token,
		User:        userToProto(user),
		Channel:     channel,
	}, nil
}

// userName returns the display name for a user's token: the first and last
// name, or the USERNAME when the name is blank (falling back to the email
// only when the username is also empty — unusual, but harmless).
func userName(u *User) string {
	name := u.FirstName + " " + u.LastName
	if name == "  " {
		if u.Username != "" {
			return u.Username
		}
		return u.Email
	}
	return name
}

// Register creates a new user from the request's username, first/last, and
// password (email is optional). The password is encoded with Argon2ID (pepper
// prepended) before it is stored; the plaintext is never persisted. A
// duplicate username is AlreadyExists. The unauthenticated register path does
// not honour client-supplied roles (that would let anyone self-assign a
// role); instead it assigns a default: the first user ever registered becomes
// an admin (so a local run has a way in), and every later user gets the
// default "user" role.
func (s *GRPCServer) Register(ctx context.Context, req *idppb.RegisterRequest) (*dbpb.IDPUser, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	username := req.GetUsername()
	if username == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "username and password are required")
	}
	// Local-login switch (F-28.4 Slice 4): when OFF the IdP is SSO-ONLY — no
	// NEW local accounts may be created (standing sign-in is equally
	// refused); break-glass coinage bypasses Register entirely.
	if !s.cfg.LocalLoginEnabled {
		return nil, status.Error(codes.PermissionDenied, "local login is disabled; sign in via SSO")
	}
	// Reservation (F-28.3): a tombstoned username is refused, not recreated —
	// the identity is reserved for restoration via reactivation.
	if _, err := s.users.GetByUsername(ctx, username, true); err == nil {
		return nil, status.Error(codes.AlreadyExists, "a user with that username already exists")
	} else if status.Code(err) != codes.NotFound {
		return nil, status.Errorf(codes.Internal, "idp: look up user: %v", err)
	}
	hash, err := HashPassword(s.cfg.PasswordPepper, req.GetPassword())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: hash password: %v", err)
	}
	roles := s.defaultRoles(ctx)
	user, err := s.users.Create(ctx, &User{
		Username:     username,
		FirstName:    req.GetFirstName(),
		LastName:     req.GetLastName(),
		Email:        req.GetEmail(),
		PasswordHash: hash,
		Roles:        roles,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil, status.Error(codes.AlreadyExists, "a user with that username already exists")
		}
		return nil, status.Errorf(codes.Internal, "idp: create user: %v", err)
	}
	s.logger.Info("idp: registered user", "username", user.Username, "email", user.Email, "roles", user.Roles)
	s.refreshUserMetrics(ctx)
	return userToProto(user), nil
}

// defaultRoles returns the role set assigned to a user created via the
// unauthenticated register endpoint: the first user ever registered becomes an
// admin (so a local run has a way in), and every later user gets the default
// "user" role.
func (s *GRPCServer) defaultRoles(ctx context.Context) []string {
	// MEASURE, DON'T FETCH: the directory size is a single aggregate counted
	// in the database (CountIDPUsers) — hauling the whole user table across
	// gRPC just to peek at its length wastes bandwidth proportional to the
	// population.
	count, err := s.users.Count(ctx, false, nil)
	if err == nil && count == 0 {
		return []string{"admin"}
	}
	return []string{"user"}
}

// refreshUserMetrics sets the user-directory size gauge (F-20) from the
// current user directory. It is a no-op when metrics are disabled or the user
// store is not configured. The measurement is a database COUNT (one integer
// crosses the wire, never the population).
func (s *GRPCServer) refreshUserMetrics(ctx context.Context) {
	if s.metrics == nil || s.users == nil {
		return
	}
	count, err := s.users.Count(ctx, false, nil)
	if err != nil {
		return
	}
	s.metrics.SetUsers(count)
}

// grpcUserFilterAlphabet translates the IdP gRPC's camelCase filter
// attributes onto the store's CANONICAL vocabulary (snake_case), pinning
// the case folding per attribute (directory convention: usernames fold;
// externalIds are exact; booleans compare literally regardless). Finite by
// design — an unknown attribute is InvalidArgument.
var grpcUserFilterAlphabet = map[string]struct {
	canonical     string
	caseSensitive bool
}{
	"userName":    {canonical: "username", caseSensitive: false},
	"externalId":  {canonical: "external_id", caseSensitive: true},
	"email":       {canonical: "email", caseSensitive: false},
	"firstName":   {canonical: "first_name", caseSensitive: false},
	"lastName":    {canonical: "last_name", caseSensitive: false},
	"active":      {canonical: "active", caseSensitive: false},
	"provisioned": {canonical: "provisioned", caseSensitive: false},
}

// translateGrpcUserFilter maps one gRPC filter attribute to its canonical
// counterpart (ok=false ⇒ unknown attribute).
func translateGrpcUserFilter(attribute string) (canonical string, caseSensitive bool, ok bool) {
	entry, found := grpcUserFilterAlphabet[attribute]
	if !found {
		return "", false, false
	}
	return entry.canonical, entry.caseSensitive, true
}

// ListUsers returns registered users (profile + roles, no password hash).
// Tombstoned (deprovisioned) users are excluded unless include_deleted is
// set (F-28.3). generic_filter narrows the result set SERVER-SIDE (equality
// predicates, AND-ed — translated to the store's canonical vocabulary and
// pushed down into the backend; unknown attributes are InvalidArgument).
func (s *GRPCServer) ListUsers(ctx context.Context, req *idppb.ListUsersRequest) (*idppb.ListUsersResponse, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	var constraints *EqualityConstraints
	if gf := req.GetGenericFilter(); len(gf) > 0 {
		translated := EqualityConstraints{}
		for _, fp := range gf {
			canon, cs, ok := translateGrpcUserFilter(fp.GetAttribute())
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "unknown user filter attribute: %q", fp.GetAttribute())
			}
			translated = append(translated, EqualityConstraint{Attribute: canon, Value: fp.GetValue(), CaseSensitive: cs})
		}
		constraints = &translated
	}
	users, err := s.users.List(ctx, req.GetIncludeDeleted(), constraints)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: list users: %v", err)
	}
	out := &idppb.ListUsersResponse{Users: make([]*dbpb.IDPUser, 0, len(users))}
	for _, u := range users {
		out.Users = append(out.Users, userToProto(u))
	}
	return out, nil
}

// CreateUser creates a user from the request's profile, password, and roles
// (unlike Register, it honours the supplied role set). A duplicate username
// is AlreadyExists.
func (s *GRPCServer) CreateUser(ctx context.Context, req *idppb.CreateUserRequest) (*dbpb.IDPUser, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	username := req.GetUsername()
	if username == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "username and password are required")
	}
	if _, err := s.users.GetByUsername(ctx, username, true); err == nil {
		return nil, status.Error(codes.AlreadyExists, "a user with that username already exists")
	} else if status.Code(err) != codes.NotFound {
		return nil, status.Errorf(codes.Internal, "idp: look up user: %v", err)
	}
	hash, err := HashPassword(s.cfg.PasswordPepper, req.GetPassword())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: hash password: %v", err)
	}
	user, err := s.users.Create(ctx, &User{
		Username:     username,
		FirstName:    req.GetFirstName(),
		LastName:     req.GetLastName(),
		Email:        req.GetEmail(),
		PasswordHash: hash,
		Roles:        req.GetRoles(),
		Active:       true,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: create user: %v", err)
	}
	s.refreshUserMetrics(ctx)
	return userToProto(user), nil
}

// UpdateUser updates a user's profile and/or roles (and password when
// supplied), keyed by user ID. A missing user is NotFound. A renamed
// username that clashes with another user's is AlreadyExists (F-28.4).
func (s *GRPCServer) UpdateUser(ctx context.Context, req *idppb.UpdateUserRequest) (*dbpb.IDPUser, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	id := req.GetId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "user id is required")
	}
	// Resolve the target FIRST (F-28.3): the write-side channel lockdown
	// needs to know the user's provenance before deciding whether a
	// password may be attached at all.
	target, err := s.users.GetByID(ctx, id, false)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: look up user: %v", err)
	}
	if req.GetPassword() != "" && target.Provisioned {
		// No write path may attach a password to a provisioned user (F-28.3
		// channel lockdown): the password channel is closed at both doors.
		return nil, status.Error(codes.FailedPrecondition, "cannot set a password on a provisioned user; it is managed by the identity provider")
	}
	update := &User{ID: id, Username: req.GetUsername(), FirstName: req.GetFirstName(), LastName: req.GetLastName(), Email: req.GetEmail(), Roles: req.GetRoles()}
	if pw := req.GetPassword(); pw != "" {
		hash, err := HashPassword(s.cfg.PasswordPepper, pw)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "idp: hash password: %v", err)
		}
		update.PasswordHash = hash
	}
	user, err := s.users.Update(ctx, update)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		if status.Code(err) == codes.AlreadyExists {
			return nil, status.Error(codes.AlreadyExists, "a user with that username already exists")
		}
		return nil, status.Errorf(codes.Internal, "idp: update user: %v", err)
	}
	s.refreshUserMetrics(ctx)
	return userToProto(user), nil
}

// DeleteUser deprovisions a user by ID (F-28.3 soft delete): the row is
// retained (tombstone, active=false, password hash zeroed) so historical
// actor references stay resolvable. A missing user is NotFound.
func (s *GRPCServer) DeleteUser(ctx context.Context, req *idppb.DeleteUserRequest) (*idppb.DeleteUserResponse, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	id := req.GetId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "user id is required")
	}
	if err := s.users.SoftDelete(ctx, id); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: delete user: %v", err)
	}
	s.refreshUserMetrics(ctx)
	return &idppb.DeleteUserResponse{Status: "deleted"}, nil
}

// ---------------------------------------------------------------------------
// Group details (F-28.5 Slice 4) — the enriched documents behind the API's
// read-only /api/groups admin surface.
// ---------------------------------------------------------------------------

// ListGroupDetails returns the ENRICHED group documents (READ-ONLY — group
// mutation is SCIM-owned; the corporate connector speaks to /scim/v2
// directly). Enrichment beyond the stored row: (a) RESOLVED MEMBERS — each
// stored value paired with the live username it denotes (null when
// unresolvable; the value is kept, per the opacity ruling); (b) DERIVED
// ROLES — the group's projected role set (its mapped role, present only
// when it exists in the role catalog — missing mapped roles are excluded,
// ruling 4). Unimplemented when the group surface is not attached.
func (s *GRPCServer) ListGroupDetails(ctx context.Context, _ *idppb.ListGroupDetailsRequest) (*idppb.ListGroupDetailsResponse, error) {
	if s.groups == nil {
		return nil, status.Error(codes.Unimplemented, "group store is not configured")
	}
	groups, err := s.groups.List(ctx, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: list groups: %v", err)
	}
	users, catalog := s.enrichmentIndexes(ctx)
	out := &idppb.ListGroupDetailsResponse{Groups: make([]*idppb.GroupDetail, 0, len(groups))}
	for _, g := range groups {
		out.Groups = append(out.Groups, s.buildGroupDetail(users, catalog, g))
	}
	return out, nil
}

// GetGroupDetails returns ONE enriched group document by id. A missing
// group is NotFound; a malformed id is InvalidArgument.
func (s *GRPCServer) GetGroupDetails(ctx context.Context, req *idppb.GetGroupDetailsRequest) (*idppb.GroupDetail, error) {
	if s.groups == nil {
		return nil, status.Error(codes.Unimplemented, "group store is not configured")
	}
	g, err := s.groups.Get(ctx, req.GetId())
	if err != nil {
		return nil, err // the store's statuses (NotFound / InvalidArgument) propagate
	}
	users, catalog := s.enrichmentIndexes(ctx)
	return s.buildGroupDetail(users, catalog, g), nil
}

// groupDetailIndexes preloads the two LOOKUPS the enrichment needs: the
// live-user index (username + external_id lenses for member resolution) and
// the role-catalog name set (the derived-role projection check). Either
// failure degrades the dependent enrichment (unresolved members / no
// projection) rather than failing the read — the stored attributes always
// render.
func (s *GRPCServer) enrichmentIndexes(ctx context.Context) (map[string]*User, map[string]bool) {
	users := map[string]*User{}
	if s.users != nil {
		if ls, err := s.users.List(ctx, false, nil); err == nil {
			for _, u := range ls {
				if u.Username != "" {
					users[u.Username] = u
				}
				if u.ExternalID != "" {
					users[u.ExternalID] = u
				}
			}
		}
	}
	catalog := map[string]bool{}
	if s.db != nil {
		if resp, err := s.db.ListRoles(ctx, &dbpb.ListRolesRequest{}); err == nil {
			for _, r := range resp.GetRoles() {
				catalog[r.GetName()] = true
			}
		}
	}
	return users, catalog
}

// buildGroupDetail renders one enriched document from a stored group plus
// the preloaded indexes.
func (s *GRPCServer) buildGroupDetail(users map[string]*User, catalog map[string]bool, g *SCIMGroup) *idppb.GroupDetail {
	detail := &idppb.GroupDetail{
		Id:           g.ID,
		DisplayName:  g.DisplayName,
		Description:  g.Description,
		ExternalId:   g.ExternalID,
		MemberValues: append([]string(nil), g.MemberValues...),
		CreatedAt:    scimTimeOrNil(g.CreatedAt),
		UpdatedAt:    scimTimeOrNil(g.UpdatedAt),
	}
	// Resolved members (parallel arrays, same order as member_values).
	for _, v := range g.MemberValues {
		rm := &idppb.ResolvedGroupMember{Value: v}
		if u, ok := users[v]; ok {
			rm.Username = &u.Username
		}
		detail.Members = append(detail.Members, rm)
	}
	// Derived roles: the group's mapped role, present ONLY when it exists in
	// the catalog (projection — the applicator skips missing roles at
	// application time, ruling 4).
	if role, ok := s.cfg.Scim.GroupRoleMappings[g.DisplayName]; ok && catalog[role] {
		detail.DerivedRoles = append(detail.DerivedRoles, role)
	}
	return detail
}

// scimTimeOrNil renders a timestamp as an RFC3339 string ("" when unset) —
// the flat-string convention this proto uses for times.
func scimTimeOrNil(tm time.Time) string {
	if tm.IsZero() {
		return ""
	}
	return tm.UTC().Format(time.RFC3339)
}

// ---------------------------------------------------------------------------
// Admission gate (F-28.4 Slice 3)
// ---------------------------------------------------------------------------

// TRUST-THE-ISSUER (ruled 2026-11-05): the gate places NO charset or shape
// constraint on subject-derived usernames. Subjects arrive from the
// CONFIGURED issuers the operator elected to honor (enterprise SSO, the
// built-in IdP, the dev shim); Azure AD UPNs carry "@" and operators
// routinely elect emails AS usernames. The directory treats usernames as
// opaque unique keys (varchar(255) UNIQUE in the schema; nothing downstream
// interprets their shape), so a narrower bound would silently bar legitimate
// corporations — including SCIM-synced identities the SCIM path gladly
// stores. The subject IS the identity: exotics are ordinary rows (inert —
// nobody looks them up), and the subject is normalized by TRIMMING at the
// gate boundary, so padded presentations collapse onto the trimmed identity
// (one row, no shadow twins). The only refusal is EMPTY-AFTER-TRIM: a
// malformed request (InvalidArgument), not a verdict.

// AdmitPrincipal adjudicates a presented principal against the user
// directory — the CONSOLIDATED ADMISSION GATE the API's auth middleware
// invokes PER REQUEST (no caching) for every human user principal. Judgment
// is ORIGIN-INDEPENDENT (weighs the claims, not the signer). The BRANCH GRAPH:
//
//	known + tombstoned                    ⇒ DEPROVISIONED
//	known + deactivated                   ⇒ DEACTIVATED
//	known + active, JIT-born (prov && no
//	  externalId)                          ⇒ refresh name/email from the args
//	                                      (provided ones) + GROW roles
//	                                      monotonically (stored ∪ incoming) ⇒ ADMITTED
//	known + active, otherwise (local or
//	  SCIM-managed)                        ⇒ admitted TOUCHED (no overwrite)
//	unknown + scim.enabled                ⇒ UNKNOWN_REJECTED (no JIT)
//	unknown + jit + empty claim roles     ⇒ NO_ROLES_REJECTED (no row)
//	unknown + jit                         ⇒ CREATE (provisioned, stock-free
//	                                      roles) ⇒ ADMITTED
//
// Denials are RESPONSES (the RPC succeeds; the verdict carries the reason) so
// the caller meters every outcome uniformly.
func (s *GRPCServer) AdmitPrincipal(ctx context.Context, req *idppb.AdmitPrincipalRequest) (*idppb.AdmitPrincipalResponse, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	username := strings.TrimSpace(req.GetUsername())
	if username == "" {
		return nil, status.Error(codes.InvalidArgument, "username is required")
	}
	existing, err := s.users.GetByUsername(ctx, username, true)
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, err
	}
	if existing != nil {
		return s.admitKnown(ctx, existing, req)
	}
	// UNKNOWN: the mode fork.
	if s.cfg.Scim.Enabled {
		// The corporate directory is the SOLE source of truth — no JIT.
		return admitDenial(idppb.AdmissionVerdict_UNKNOWN_REJECTED), nil
	}
	roles := dedupeRoles(req.GetRoles())
	if len(roles) == 0 {
		// Fail-closed: a JIT birth demands claim-derived roles (no stock-role
		// injection); the operator grants a role mapping first.
		return admitDenial(idppb.AdmissionVerdict_NO_ROLES_REJECTED), nil
	}
	first, last := splitCompactName(req.GetName())
	created, err := s.users.Create(ctx, &User{
		Username:    username,
		FirstName:   first,
		LastName:    last,
		Email:       req.GetEmail(),
		Roles:       roles,
		Provisioned: true,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			// Replication race: another admittance created the row meanwhile —
			// re-judge it as a known row.
			if refetched, rerr := s.users.GetByUsername(ctx, username, true); rerr == nil {
				return s.admitKnown(ctx, refetched, req)
			}
		}
		return nil, err
	}
	s.logger.Info("idp: JIT-provisioned user", "username", username, "roles", roles)
	return s.admitGranted(created), nil
}

// admitKnown adjudicates an EXISTING row (any state).
func (s *GRPCServer) admitKnown(ctx context.Context, u *User, req *idppb.AdmitPrincipalRequest) (*idppb.AdmitPrincipalResponse, error) {
	switch {
	case u.DeletedAt != nil:
		return admitDenial(idppb.AdmissionVerdict_DEPROVISIONED), nil
	case !u.Active:
		return admitDenial(idppb.AdmissionVerdict_DEACTIVATED), nil
	}
	// ACTIVE. JIT-born rows (provisioned, no externalId) REFRESH name/email
	// from the args (provided ones only — Update's supplied-value semantics
	// leave absences untouched) and GROW roles monotonically (stored ∪
	// incoming — a claim dropping off never strips an operator grant). The
	// write happens ONLY WHEN SOMETHING MOVES (a resync contact that changes
	// nothing is a pure read — no gratuitous UpdatedAt churn). Local and
	// SCIM-managed rows are admitted TOUCHED.
	if u.Provisioned && u.ExternalID == "" {
		union := mergeRoles(u.Roles, dedupeRoles(req.GetRoles()))
		first, last := splitCompactName(req.GetName())
		nameMoves := req.GetName() != "" && (first != u.FirstName || last != u.LastName)
		emailMoves := req.GetEmail() != "" && req.GetEmail() != u.Email
		if nameMoves || emailMoves || !sameStrings(union, u.Roles) {
			refreshed, err := s.users.Update(ctx, &User{
				ID:        u.ID,
				FirstName: first,
				LastName:  last,
				Email:     req.GetEmail(),
				Roles:     union,
			})
			if err != nil {
				return nil, err
			}
			u = refreshed
		}
	}
	return s.admitGranted(u), nil
}

func (s *GRPCServer) admitGranted(u *User) *idppb.AdmitPrincipalResponse {
	return &idppb.AdmitPrincipalResponse{Verdict: idppb.AdmissionVerdict_ADMITTED, User: userToProto(u)}
}

func admitDenial(v idppb.AdmissionVerdict) *idppb.AdmitPrincipalResponse {
	return &idppb.AdmitPrincipalResponse{Verdict: v}
}

// dedupeRoles collapses a role set to its unique NON-EMPTY members (stable
// order).
func dedupeRoles(in []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, r := range in {
		r = strings.TrimSpace(r)
		if r == "" || seen[r] {
			continue
		}
		seen[r] = true
		out = append(out, r)
	}
	return out
}

// mergeRoles unions base ∪ incoming (base order first, newcomers appended).
func mergeRoles(base, incoming []string) []string {
	merged := append([]string{}, base...)
	for _, r := range incoming {
		found := false
		for _, b := range merged {
			if b == r {
				found = true
				break
			}
		}
		if !found {
			merged = append(merged, r)
		}
	}
	return merged
}

// sameStrings reports whether two string slices hold the same ELEMENTS
// (multiset-insensitive, order-insensitive).
func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := map[string]int{}
	for _, s := range a {
		counts[s]++
	}
	for _, s := range b {
		counts[s]--
		if counts[s] < 0 {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// API keys (F-25)
// ---------------------------------------------------------------------------

// CreateAPIKey generates a new `cdrom-…` API key for a user, hashes it (mixing
// in the request's pepper), stores it, and returns the plaintext exactly once
// (in the response's plaintext field). The key's expiration is validated to be
// at most one year in the future. The RPC is Unimplemented when no API-key
// store is attached.
func (s *GRPCServer) CreateAPIKey(ctx context.Context, req *idppb.CreateAPIKeyRequest) (*idppb.CreateAPIKeyResponse, error) {
	if s.keys == nil {
		return nil, status.Error(codes.Unimplemented, "api key store is not configured")
	}
	if req.GetOwnerUsername() == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_username is required")
	}
	var expiresAt time.Time
	if v := req.GetExpiresIn(); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid expires_in: %v", err)
		}
		if d < 0 {
			return nil, status.Error(codes.InvalidArgument, "expires_in must not be negative")
		}
		expiresAt = time.Now().Add(d)
	}
	key, plaintext, err := s.keys.Create(ctx, req.GetOwnerUsername(), req.GetDescription(), expiresAt, uintsFromInt64(req.GetPipelineScope()), req.GetPepper())
	if err != nil {
		return nil, err
	}
	s.logger.Info("idp: created api key", "key", key.ID, "owner", req.GetOwnerUsername(), "description", req.GetDescription())
	return &idppb.CreateAPIKeyResponse{Key: apiKeyToProto(key), Plaintext: plaintext}, nil
}

// GetAPIKey returns a key's metadata and prefix (never the plaintext or hash)
// by key id. A missing key is NotFound.
func (s *GRPCServer) GetAPIKey(ctx context.Context, req *idppb.GetAPIKeyRequest) (*dbpb.IDPAPIKey, error) {
	if s.keys == nil {
		return nil, status.Error(codes.Unimplemented, "api key store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	key, err := s.keys.Get(ctx, req.GetId())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "api key not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: get api key: %v", err)
	}
	return apiKeyToProto(key), nil
}

// ListAPIKeys returns API keys. When owner_id is set only that user's keys are
// returned; when empty, all keys in the system are returned (an admin listing
// every key). The plaintext is never returned.
func (s *GRPCServer) ListAPIKeys(ctx context.Context, req *idppb.ListAPIKeysRequest) (*idppb.ListAPIKeysResponse, error) {
	if s.keys == nil {
		return nil, status.Error(codes.Unimplemented, "api key store is not configured")
	}
	keys, err := s.keys.List(ctx, req.GetOwnerId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: list api keys: %v", err)
	}
	out := &idppb.ListAPIKeysResponse{Keys: make([]*dbpb.IDPAPIKey, 0, len(keys))}
	for _, k := range keys {
		out.Keys = append(out.Keys, apiKeyToProto(k))
	}
	return out, nil
}

// UpdateAPIKey edits a key's description, expiration, and/or pipeline scope
// without changing its secret (a "renew"); the same `cdrom-…` value keeps
// working. A missing key is NotFound.
func (s *GRPCServer) UpdateAPIKey(ctx context.Context, req *idppb.UpdateAPIKeyRequest) (*dbpb.IDPAPIKey, error) {
	if s.keys == nil {
		return nil, status.Error(codes.Unimplemented, "api key store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var expiresAt time.Time
	if req.GetHasExpiresAt() {
		if v := req.GetExpiresIn(); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid expires_in: %v", err)
			}
			if d < 0 {
				return nil, status.Error(codes.InvalidArgument, "expires_in must not be negative")
			}
			expiresAt = time.Now().Add(d)
		}
	}
	key, err := s.keys.Update(ctx, req.GetId(), req.GetDescription(), req.GetHasDescription(), expiresAt, req.GetHasExpiresAt(), req.GetHasPipelineScope(), uintsFromInt64(req.GetPipelineScope()))
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "api key not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: update api key: %v", err)
	}
	return apiKeyToProto(key), nil
}

// RotateAPIKey generates a brand-new `cdrom-…` secret for a key (hashing it,
// mixing in the request's pepper), returns it exactly once (in the response's
// plaintext field), and invalidates the previous one. A missing key is
// NotFound.
func (s *GRPCServer) RotateAPIKey(ctx context.Context, req *idppb.RotateAPIKeyRequest) (*idppb.RotateAPIKeyResponse, error) {
	if s.keys == nil {
		return nil, status.Error(codes.Unimplemented, "api key store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	key, plaintext, err := s.keys.Rotate(ctx, req.GetId(), req.GetPepper())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "api key not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: rotate api key: %v", err)
	}
	s.logger.Info("idp: rotated api key", "key", key.ID)
	return &idppb.RotateAPIKeyResponse{Key: apiKeyToProto(key), Plaintext: plaintext}, nil
}

// DeleteAPIKey removes a key by id. A missing key is NotFound.
func (s *GRPCServer) DeleteAPIKey(ctx context.Context, req *idppb.DeleteAPIKeyRequest) (*idppb.DeleteAPIKeyResponse, error) {
	if s.keys == nil {
		return nil, status.Error(codes.Unimplemented, "api key store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if err := s.keys.Delete(ctx, req.GetId()); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "api key not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: delete api key: %v", err)
	}
	return &idppb.DeleteAPIKeyResponse{Status: "deleted"}, nil
}

// VerifyAPIKey checks a presented `username:apikey` credential against the
// stored key directory (hash, expiration, and the owner's lockout). On a miss
// it increments the owner's failure counter and locks the owner out at the
// configured maximum. A miss is Unauthenticated.
func (s *GRPCServer) VerifyAPIKey(ctx context.Context, req *idppb.VerifyAPIKeyRequest) (*idppb.VerifyAPIKeyResponse, error) {
	if s.keys == nil {
		return nil, status.Error(codes.Unimplemented, "api key store is not configured")
	}
	if req.GetUsername() == "" || req.GetApiKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "username and api_key are required")
	}
	key, owner, err := s.keys.Verify(ctx, req.GetUsername(), req.GetApiKey(), req.GetPepper(), int(req.GetMaxFailures()), req.GetLockoutDuration().AsDuration())
	if err != nil {
		if status.Code(err) == codes.Unauthenticated {
			return nil, status.Error(codes.Unauthenticated, "invalid credentials")
		}
		return nil, status.Errorf(codes.Internal, "idp: verify api key: %v", err)
	}
	return &idppb.VerifyAPIKeyResponse{Key: apiKeyToProto(key), Owner: userToProto(owner)}, nil
}

// ResetAPIKeyLockout clears a user's API-key lockout state (the failed
// counter and the lockout instant). A missing user is NotFound.
func (s *GRPCServer) ResetAPIKeyLockout(ctx context.Context, req *idppb.ResetAPIKeyLockoutRequest) (*idppb.ResetAPIKeyLockoutResponse, error) {
	if s.keys == nil {
		return nil, status.Error(codes.Unimplemented, "api key store is not configured")
	}
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if err := s.keys.ResetLockout(ctx, req.GetUserId()); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: reset api key lockout: %v", err)
	}
	return &idppb.ResetAPIKeyLockoutResponse{Status: "reset"}, nil
}

// ---------------------------------------------------------------------------
// Agent credentials (F-30.2)
// ---------------------------------------------------------------------------

// IssueAgentCredential signs a NEW agent credential for the named agent
// (deadline = now + the configured TTL) and stores the row. The response
// carries the row PLUS the plaintext JWT, which is returned EXACTLY ONCE.
// Unimplemented when no credential ops layer is attached.
func (s *GRPCServer) IssueAgentCredential(ctx context.Context, req *idppb.IssueAgentCredentialRequest) (*idppb.IssueAgentCredentialResponse, error) {
	if s.agentCredentials == nil {
		return nil, status.Error(codes.Unimplemented, "agent credential store is not configured")
	}
	row, token, err := s.agentCredentials.Issue(ctx, req.GetAgentName(), req.GetIssuedBy(), req.GetDescription())
	if err != nil {
		return nil, err
	}
	return &idppb.IssueAgentCredentialResponse{Credential: agentCredentialToProto(row), Jwt: token}, nil
}

// GetAgentCredential returns a row's METADATA (never JWT material) by id.
// A missing row is NotFound.
func (s *GRPCServer) GetAgentCredential(ctx context.Context, req *idppb.GetAgentCredentialRequest) (*dbpb.AgentCredential, error) {
	if s.agentCredentials == nil {
		return nil, status.Error(codes.Unimplemented, "agent credential store is not configured")
	}
	row, err := s.agentCredentials.Get(ctx, req.GetId())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "agent credential not found")
		}
		return nil, err
	}
	return agentCredentialToProto(row), nil
}

// ListAgentCredentials returns an agent's rows (or all rows when agent_name
// is empty) — metadata only, never JWT material.
func (s *GRPCServer) ListAgentCredentials(ctx context.Context, req *idppb.ListAgentCredentialsRequest) (*idppb.ListAgentCredentialsResponse, error) {
	if s.agentCredentials == nil {
		return nil, status.Error(codes.Unimplemented, "agent credential store is not configured")
	}
	rows, err := s.agentCredentials.List(ctx, req.GetAgentName())
	if err != nil {
		return nil, err
	}
	out := &idppb.ListAgentCredentialsResponse{Credentials: make([]*dbpb.AgentCredential, 0, len(rows))}
	for _, r := range rows {
		out.Credentials = append(out.Credentials, agentCredentialToProto(r))
	}
	return out, nil
}

// RenewAgentCredential EXTENDS a row's deadline to now + TTL without touching
// the JWT (the same token keeps working — structural no-break). A revoked row
// is FailedPrecondition; a missing row is NotFound.
func (s *GRPCServer) RenewAgentCredential(ctx context.Context, req *idppb.RenewAgentCredentialRequest) (*dbpb.AgentCredential, error) {
	if s.agentCredentials == nil {
		return nil, status.Error(codes.Unimplemented, "agent credential store is not configured")
	}
	row, err := s.agentCredentials.Renew(ctx, req.GetId())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "agent credential not found")
		}
		return nil, err
	}
	return agentCredentialToProto(row), nil
}

// RotateAgentCredential RETIRES a row (chaining replaced_by_id) and signs a
// SUCCESSOR for the same agent. The response carries the successor row PLUS
// the new plaintext JWT, returned exactly once.
func (s *GRPCServer) RotateAgentCredential(ctx context.Context, req *idppb.RotateAgentCredentialRequest) (*idppb.IssueAgentCredentialResponse, error) {
	if s.agentCredentials == nil {
		return nil, status.Error(codes.Unimplemented, "agent credential store is not configured")
	}
	row, token, err := s.agentCredentials.Rotate(ctx, req.GetId(), req.GetIssuedBy(), req.GetDescription())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "agent credential not found")
		}
		return nil, err
	}
	return &idppb.IssueAgentCredentialResponse{Credential: agentCredentialToProto(row), Jwt: token}, nil
}

// RevokeAgentCredential stamps a row revoked NOW, with a reason. Immediate:
// the row stops governing liveness the moment the write commits.
func (s *GRPCServer) RevokeAgentCredential(ctx context.Context, req *idppb.RevokeAgentCredentialRequest) (*dbpb.AgentCredential, error) {
	if s.agentCredentials == nil {
		return nil, status.Error(codes.Unimplemented, "agent credential store is not configured")
	}
	row, err := s.agentCredentials.Revoke(ctx, req.GetId(), req.GetReason())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "agent credential not found")
		}
		return nil, err
	}
	return agentCredentialToProto(row), nil
}

// agentCredentialToProto maps an IdP row to its wire form (metadata only —
// the JWT never crosses this boundary).
func agentCredentialToProto(r *AgentCredentialRow) *dbpb.AgentCredential {
	out := &dbpb.AgentCredential{
		Id:            r.ID,
		AgentName:     r.AgentName,
		IssuedBy:      r.IssuedBy,
		Description:   r.Description,
		ReplacedById:  r.ReplacedByID,
		RevokedReason: r.RevokedReason,
	}
	if !r.ExpiresAt.IsZero() {
		out.ExpiresAt = timestamppb.New(r.ExpiresAt)
	}
	if r.RevokedAt != nil {
		out.RevokedAt = timestamppb.New(*r.RevokedAt)
	}
	if r.LastSeenAt != nil {
		out.LastSeenAt = timestamppb.New(*r.LastSeenAt)
	}
	if !r.CreatedAt.IsZero() {
		out.CreatedAt = timestamppb.New(r.CreatedAt)
	}
	if !r.UpdatedAt.IsZero() {
		out.UpdatedAt = timestamppb.New(r.UpdatedAt)
	}
	return out
}

// userToProto renders a user as its protobuf form (mirrors the
// Database-service representation; the password hash is included because the
// API relays profiles to the UI minus the hash via its own shaping).
func userToProto(u *User) *dbpb.IDPUser {
	out := &dbpb.IDPUser{
		Id:           u.ID,
		Username:     u.Username,
		FirstName:    u.FirstName,
		LastName:     u.LastName,
		Email:        u.Email,
		PasswordHash: u.PasswordHash,
		Roles:        u.Roles,
		CreatedAt:    timestamppb.New(u.CreatedAt),
		Active:       u.Active,
		ExternalId:   u.ExternalID,
		Provisioned:  u.Provisioned,
	}
	if u.DeletedAt != nil {
		out.DeletedAt = timestamppb.New(*u.DeletedAt)
	}
	return out
}

// ---------------------------------------------------------------------------
// Service accounts (F-26)
// ---------------------------------------------------------------------------

// CreateServiceAccount creates a service account and its two freshly generated
// key slots, and binds its roles. The IdP generates each key's plaintext
// (`cdrom-sa-…`), encodes them with Argon2ID (the pepper prepended to the
// KDF input; the salt is embedded in the encoding), and persists the
// encodings through the Database service. The
// response returns both plaintext keys exactly once. The RPC is Unimplemented
// when no service-account store is attached.
func (s *GRPCServer) CreateServiceAccount(ctx context.Context, req *idppb.CreateServiceAccountRequest) (*idppb.CreateServiceAccountResponse, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetLoginName() == "" {
		return nil, status.Error(codes.InvalidArgument, "login_name is required")
	}
	// Generate both keys' plaintexts and their Argon2ID encodings. A failure
	// of randomness, hashing, or storage fails the whole operation.
	keys := make([]*ServiceAccountKey, 0, 2)
	plaintexts := make([]*idppb.ServiceAccountPlaintextKey, 0, 2)
	for slot := 1; slot <= 2; slot++ {
		plaintext, err := NewServiceAccountKey()
		if err != nil {
			return nil, err
		}
		encoded, err := EncodeServiceAccountKey(req.GetPepper(), plaintext)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "idp: hash service account key: %v", err)
		}
		keys = append(keys, &ServiceAccountKey{
			Slot:      slot,
			KeyHash:   encoded,
			KeyPrefix: serviceAccountKeyPrefixOf(plaintext),
		})
		plaintexts = append(plaintexts, &idppb.ServiceAccountPlaintextKey{Slot: int32(slot), Key: plaintext})
	}
	acc := &ServiceAccount{
		LoginName:   req.GetLoginName(),
		DisplayName: req.GetDisplayName(),
		Description: req.GetDescription(),
		CreatedBy:   req.GetCreatedBy(),
		UpdatedBy:   req.GetCreatedBy(),
		Keys:        keys,
	}
	created, err := s.serviceAccounts.Create(ctx, acc, req.GetRoles())
	if err != nil {
		return nil, err
	}
	s.logger.Info("idp: created service account", "login", created.LoginName, "roles", created.Roles)
	return &idppb.CreateServiceAccountResponse{
		Account: serviceAccountToProto(created),
		Keys:    plaintexts,
	}, nil
}

// GetServiceAccount returns an account's metadata and both slots' non-secret
// metadata (never the plaintext or hash) by id or login name. A missing
// account is NotFound.
func (s *GRPCServer) GetServiceAccount(ctx context.Context, req *idppb.GetServiceAccountRequest) (*dbpb.ServiceAccount, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetId() == "" && req.GetLoginName() == "" {
		return nil, status.Error(codes.InvalidArgument, "id or login_name is required")
	}
	acc, err := s.serviceAccounts.Get(ctx, req.GetId(), req.GetLoginName(), req.GetIncludeDeleted())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "service account not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: get service account: %v", err)
	}
	return serviceAccountToProto(acc), nil
}

// ListServiceAccounts returns service accounts. When include_deleted is false
// (the default) deleted accounts are excluded.
func (s *GRPCServer) ListServiceAccounts(ctx context.Context, req *idppb.ListServiceAccountsRequest) (*idppb.ListServiceAccountsResponse, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	accounts, err := s.serviceAccounts.List(ctx, req.GetIncludeDeleted())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: list service accounts: %v", err)
	}
	out := &idppb.ListServiceAccountsResponse{Accounts: make([]*dbpb.ServiceAccount, 0, len(accounts))}
	for _, a := range accounts {
		out.Accounts = append(out.Accounts, serviceAccountToProto(a))
	}
	return out, nil
}

// UpdateServiceAccount edits an account's display name/description (never its
// roles, status, or keys). A stale revision is Aborted.
func (s *GRPCServer) UpdateServiceAccount(ctx context.Context, req *idppb.UpdateServiceAccountRequest) (*dbpb.ServiceAccount, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	acc, err := s.serviceAccounts.Update(ctx, req.GetId(), req.GetDisplayName(), req.GetDescription(), req.GetHasDisplayName(), req.GetHasDescription(), req.GetRevision(), req.GetUpdatedBy())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "service account not found")
		}
		if status.Code(err) == codes.Aborted {
			return nil, status.Error(codes.Aborted, "revision mismatch: the account was modified concurrently")
		}
		return nil, status.Errorf(codes.Internal, "idp: update service account: %v", err)
	}
	return serviceAccountToProto(acc), nil
}

// RotateServiceAccountKey generates a brand-new `cdrom-sa-…` secret for the
// selected slot (hashing it, mixing in the request's pepper and a fresh
// per-key salt), returns it exactly once (in the response's key field), and
// invalidates the previous one. A stale revision is Aborted.
func (s *GRPCServer) RotateServiceAccountKey(ctx context.Context, req *idppb.RotateServiceAccountKeyRequest) (*idppb.RotateServiceAccountKeyResponse, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if req.GetSlot() != 1 && req.GetSlot() != 2 {
		return nil, status.Error(codes.InvalidArgument, "slot must be 1 or 2")
	}
	plaintext, err := NewServiceAccountKey()
	if err != nil {
		return nil, err
	}
	encoded, err := EncodeServiceAccountKey(req.GetPepper(), plaintext)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: hash service account key: %v", err)
	}
	acc, err := s.serviceAccounts.RotateKey(ctx, req.GetId(), int(req.GetSlot()), encoded, serviceAccountKeyPrefixOf(plaintext), req.GetRevision(), req.GetUpdatedBy())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "service account not found")
		}
		if status.Code(err) == codes.Aborted {
			return nil, status.Error(codes.Aborted, "revision mismatch: the account was modified concurrently")
		}
		return nil, status.Errorf(codes.Internal, "idp: rotate service account key: %v", err)
	}
	s.logger.Info("idp: rotated service account key", "login", acc.LoginName, "slot", req.GetSlot())
	return &idppb.RotateServiceAccountKeyResponse{
		Account: serviceAccountToProto(acc),
		Key:     &idppb.ServiceAccountPlaintextKey{Slot: req.GetSlot(), Key: plaintext},
	}, nil
}

// DisableServiceAccount temporarily disables an account (rejecting both keys
// while preserving the hashes and role bindings). A stale revision is Aborted.
func (s *GRPCServer) DisableServiceAccount(ctx context.Context, req *idppb.ServiceAccountStateRequest) (*dbpb.ServiceAccount, error) {
	return s.setServiceAccountState(ctx, req, true)
}

// EnableServiceAccount re-enables a disabled, non-deleted account (restoring
// both keys with the same keys and current roles). A stale revision is Aborted.
func (s *GRPCServer) EnableServiceAccount(ctx context.Context, req *idppb.ServiceAccountStateRequest) (*dbpb.ServiceAccount, error) {
	return s.setServiceAccountState(ctx, req, false)
}

// setServiceAccountState sets (or clears) an account's disabled flag.
func (s *GRPCServer) setServiceAccountState(ctx context.Context, req *idppb.ServiceAccountStateRequest, disabled bool) (*dbpb.ServiceAccount, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	acc, err := s.serviceAccounts.SetDisabled(ctx, req.GetId(), disabled, req.GetRevision(), req.GetUpdatedBy())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "service account not found")
		}
		if status.Code(err) == codes.Aborted {
			return nil, status.Error(codes.Aborted, "revision mismatch: the account was modified concurrently")
		}
		return nil, status.Errorf(codes.Internal, "idp: set service account state: %v", err)
	}
	return serviceAccountToProto(acc), nil
}

// DeleteServiceAccount permanently soft-deletes an account (tombstone + zeroed
// key slots). Repeated delete is idempotent.
func (s *GRPCServer) DeleteServiceAccount(ctx context.Context, req *idppb.DeleteServiceAccountRequest) (*dbpb.ServiceAccount, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	acc, err := s.serviceAccounts.Delete(ctx, req.GetId(), req.GetUpdatedBy())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "service account not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: delete service account: %v", err)
	}
	s.logger.Info("idp: deleted service account", "login", acc.LoginName)
	return serviceAccountToProto(acc), nil
}

// AssignServiceAccountRoles adds role bindings to an account.
func (s *GRPCServer) AssignServiceAccountRoles(ctx context.Context, req *idppb.AssignServiceAccountRolesRequest) (*dbpb.ServiceAccount, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var acc *ServiceAccount
	var err error
	for _, role := range req.GetRoles() {
		if role == "" {
			continue
		}
		acc, err = s.serviceAccounts.AssignRole(ctx, req.GetId(), role)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return nil, status.Error(codes.NotFound, "service account not found")
			}
			return nil, status.Errorf(codes.Internal, "idp: assign service account role: %v", err)
		}
	}
	if acc == nil {
		acc, err = s.serviceAccounts.Get(ctx, req.GetId(), "", false)
		if err != nil {
			return nil, err
		}
	}
	return serviceAccountToProto(acc), nil
}

// RemoveServiceAccountRole removes a role binding from an account.
func (s *GRPCServer) RemoveServiceAccountRole(ctx context.Context, req *idppb.RemoveServiceAccountRoleRequest) (*dbpb.ServiceAccount, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if req.GetRole() == "" {
		return nil, status.Error(codes.InvalidArgument, "role is required")
	}
	acc, err := s.serviceAccounts.RemoveRole(ctx, req.GetId(), req.GetRole())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "service account not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: remove service account role: %v", err)
	}
	return serviceAccountToProto(acc), nil
}

// VerifyServiceAccountKey checks a presented `login-name:cdrom-sa-…`
// credential against the account's two key slots (salted hash, disabled, and
// deleted state). On a miss it increments the account's failure counter and
// locks the account out at the configured maximum. A miss is Unauthenticated.
func (s *GRPCServer) VerifyServiceAccountKey(ctx context.Context, req *idppb.VerifyServiceAccountKeyRequest) (*idppb.VerifyServiceAccountKeyResponse, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetLoginName() == "" || req.GetApiKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "login_name and api_key are required")
	}
	acc, slot, err := verifyServiceAccountKey(ctx, s.serviceAccounts, req.GetLoginName(), req.GetApiKey(), req.GetPepper(), int(req.GetMaxFailures()), req.GetLockoutDuration().AsDuration())
	if err != nil {
		if status.Code(err) == codes.Unauthenticated {
			return nil, status.Error(codes.Unauthenticated, "invalid credentials")
		}
		return nil, status.Errorf(codes.Internal, "idp: verify service account key: %v", err)
	}
	return &idppb.VerifyServiceAccountKeyResponse{
		Account: serviceAccountToProto(acc),
		Slot:    int32(slot),
	}, nil
}

// ResetServiceAccountLockout clears an account's key lockout state (the failed
// counter and the lockout instant). A missing account is NotFound.
func (s *GRPCServer) ResetServiceAccountLockout(ctx context.Context, req *idppb.ResetServiceAccountLockoutRequest) (*idppb.ResetServiceAccountLockoutResponse, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if err := s.serviceAccounts.ResetLockout(ctx, req.GetId()); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "service account not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: reset service account lockout: %v", err)
	}
	return &idppb.ResetServiceAccountLockoutResponse{Status: "reset"}, nil
}

// ---------------------------------------------------------------------------
// SCIM keys (F-28.2)
// ---------------------------------------------------------------------------

// DeriveScimKey draws a fresh SCIM connector key (32 characters from the
// documented alphabet, crypto/rand) and computes its Argon2ID encoding with
// the IdP's configured SCIM pepper (prepended to the KDF input; the salt is
// drawn and embedded by the KDF). It returns the plaintext exactly once
// together with the encoding and the 8-char prefix. NOTHING is stored here:
// this is a pure derivation (the API persists the (encoding, prefix) pair
// through the Database service), so the RPC is available even before any pool
// rows exist (the bootstrap ritual). The IdP is the ONLY pepper-touching site
// — the pepper never leaves this process.
// DescribeSystem (F-28.4 Slice 4) reports the IdP's OWN enablement flags —
// a strictly non-secret whitelist feeding the API's GET /api/system/info
// aggregator (which drives the UI's show/hide decisions). It reveals
// CAPABILITY, never material: no addresses, keys, peppers, or DSNs travel
// here.
func (s *GRPCServer) DescribeSystem(ctx context.Context, req *idppb.DescribeSystemRequest) (*idppb.DescribeSystemResponse, error) {
	return &idppb.DescribeSystemResponse{
		ScimEnabled:       s.cfg.Scim.Enabled,
		LocalLoginEnabled: s.cfg.LocalLoginEnabled,
		BreakGlassEnabled: s.cfg.BreakGlassEnabled,
	}, nil
}

func (s *GRPCServer) DeriveScimKey(ctx context.Context, req *idppb.DeriveScimKeyRequest) (*idppb.DeriveScimKeyResponse, error) {
	plaintext, err := NewScimKey()
	if err != nil {
		return nil, err
	}
	encoding, err := EncodeScimKey(s.cfg.ScimKeysPepper, plaintext)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: encode scim key: %v", err)
	}
	return &idppb.DeriveScimKeyResponse{
		Key:      plaintext,
		Encoding: encoding,
		Prefix:   scimKeyPrefixOf(plaintext),
	}, nil
}
