package idp

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
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
	cfg    config.IdPConfig
	km     *KeyManager
	users  UserStore
	logger *slog.Logger
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

// Login verifies a user's credentials (email + password) against the stored
// password hash and, on success, mints an OIDC token for the user (stamped
// with the user's roles) and returns it with the user's profile. A bad
// password or unknown user is Unauthenticated. The token is issued for the
// request's audience (the caller's verifier audience) and issuer (the
// caller's configured issuer), so the token the caller receives is one it will
// accept.
func (s *GRPCServer) Login(ctx context.Context, req *idppb.LoginRequest) (*idppb.LoginResponse, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	if req.GetEmail() == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.Unauthenticated, "email and password are required")
	}
	user, err := s.users.GetByEmail(ctx, req.GetEmail())
	if err != nil {
		// Unknown user: fail with the same response as a bad password so the
		// endpoint does not reveal which emails are registered.
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
	if !VerifyPassword(user.PasswordHash, req.GetPassword()) {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
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
		"iss":   issuer,
		"sub":   user.ID,
		"aud":   audience,
		"exp":   now.Add(s.cfg.TokenLifetime).Unix(),
		"iat":   now.Unix(),
		"name":  userName(user),
		"email": user.Email,
	}
	if len(user.Roles) > 0 {
		claims["roles"] = user.Roles
	}
	token, err := s.km.Sign(claims)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: sign user token: %v", err)
	}
	s.logger.Info("idp: user login", "email", user.Email, "roles", user.Roles)
	return &idppb.LoginResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   int64(s.cfg.TokenLifetime.Seconds()),
		IdToken:     token,
		User:        userToProto(user),
	}, nil
}

// userName returns the display name for a user's token: the first and last
// name, or the email when the name is blank.
func userName(u *User) string {
	name := u.FirstName + " " + u.LastName
	if name == "  " {
		return u.Email
	}
	return name
}

// Register creates a new user from the request's first/last/email and
// password. The password is hashed (bcrypt) before it is stored; the plaintext
// is never persisted. A duplicate email is AlreadyExists. The unauthenticated
// register path does not honour client-supplied roles (that would let anyone
// self-assign a role); instead it assigns a default: the first user ever
// registered becomes an admin (so a local run has a way in), and every later
// user gets the default "user" role.
func (s *GRPCServer) Register(ctx context.Context, req *idppb.RegisterRequest) (*dbpb.IDPUser, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	if req.GetEmail() == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "email and password are required")
	}
	if _, err := s.users.GetByEmail(ctx, req.GetEmail()); err == nil {
		return nil, status.Error(codes.AlreadyExists, "a user with that email already exists")
	}
	hash, err := HashPassword(req.GetPassword())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: hash password: %v", err)
	}
	roles := s.defaultRoles(ctx)
	user, err := s.users.Create(ctx, &User{
		FirstName:    req.GetFirstName(),
		LastName:     req.GetLastName(),
		Email:        req.GetEmail(),
		PasswordHash: hash,
		Roles:        roles,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil, status.Error(codes.AlreadyExists, "a user with that email already exists")
		}
		return nil, status.Errorf(codes.Internal, "idp: create user: %v", err)
	}
	s.logger.Info("idp: registered user", "email", user.Email, "roles", user.Roles)
	return userToProto(user), nil
}

// defaultRoles returns the role set assigned to a user created via the
// unauthenticated register endpoint: the first user ever registered becomes an
// admin (so a local run has a way in), and every later user gets the default
// "user" role.
func (s *GRPCServer) defaultRoles(ctx context.Context) []string {
	users, err := s.users.List(ctx)
	if err == nil && len(users) == 0 {
		return []string{"admin"}
	}
	return []string{"user"}
}

// ListUsers returns all registered users (profile + roles, no password hash).
func (s *GRPCServer) ListUsers(ctx context.Context, _ *idppb.ListUsersRequest) (*idppb.ListUsersResponse, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	users, err := s.users.List(ctx)
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
// (unlike Register, it honours the supplied role set). A duplicate email is
// AlreadyExists.
func (s *GRPCServer) CreateUser(ctx context.Context, req *idppb.CreateUserRequest) (*dbpb.IDPUser, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	if req.GetEmail() == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "email and password are required")
	}
	if _, err := s.users.GetByEmail(ctx, req.GetEmail()); err == nil {
		return nil, status.Error(codes.AlreadyExists, "a user with that email already exists")
	}
	hash, err := HashPassword(req.GetPassword())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: hash password: %v", err)
	}
	user, err := s.users.Create(ctx, &User{
		FirstName:    req.GetFirstName(),
		LastName:     req.GetLastName(),
		Email:        req.GetEmail(),
		PasswordHash: hash,
		Roles:        req.GetRoles(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "idp: create user: %v", err)
	}
	return userToProto(user), nil
}

// UpdateUser updates a user's profile and/or roles (and password when
// supplied), keyed by user ID. A missing user is NotFound.
func (s *GRPCServer) UpdateUser(ctx context.Context, req *idppb.UpdateUserRequest) (*dbpb.IDPUser, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	id := req.GetId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "user id is required")
	}
	update := &User{ID: id, FirstName: req.GetFirstName(), LastName: req.GetLastName(), Email: req.GetEmail(), Roles: req.GetRoles()}
	if pw := req.GetPassword(); pw != "" {
		hash, err := HashPassword(pw)
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
		return nil, status.Errorf(codes.Internal, "idp: update user: %v", err)
	}
	return userToProto(user), nil
}

// DeleteUser removes a user by ID. A missing user is NotFound.
func (s *GRPCServer) DeleteUser(ctx context.Context, req *idppb.DeleteUserRequest) (*idppb.DeleteUserResponse, error) {
	if s.users == nil {
		return nil, status.Error(codes.Unimplemented, "user store is not configured")
	}
	id := req.GetId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "user id is required")
	}
	if err := s.users.Delete(ctx, id); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return nil, status.Errorf(codes.Internal, "idp: delete user: %v", err)
	}
	return &idppb.DeleteUserResponse{Status: "deleted"}, nil
}

// userToProto renders a user as its protobuf form.
func userToProto(u *User) *dbpb.IDPUser {
	return &dbpb.IDPUser{
		Id:           u.ID,
		FirstName:    u.FirstName,
		LastName:     u.LastName,
		Email:        u.Email,
		PasswordHash: u.PasswordHash,
		Roles:        u.Roles,
		CreatedAt:    timestamppb.New(u.CreatedAt),
	}
}
