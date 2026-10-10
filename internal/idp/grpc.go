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
	cfg             config.IdPConfig
	km              *KeyManager
	users           UserStore
	keys            APIKeyStore
	serviceAccounts ServiceAccountStore
	logger          *slog.Logger
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
	if req.GetOwnerEmail() == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_email is required")
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
	key, plaintext, err := s.keys.Create(ctx, req.GetOwnerEmail(), req.GetDescription(), expiresAt, uintsFromInt64(req.GetPipelineScope()), req.GetPepper())
	if err != nil {
		return nil, err
	}
	s.logger.Info("idp: created api key", "key", key.ID, "owner", req.GetOwnerEmail(), "description", req.GetDescription())
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

// ---------------------------------------------------------------------------
// Service accounts (F-26)
// ---------------------------------------------------------------------------

// CreateServiceAccount creates a service account and its two freshly generated
// key slots, and binds its roles. The IdP generates each key's plaintext
// (`cdrom-sa-…`), hashes it (mixing in a fresh per-key salt and the request's
// pepper), and persists the salted hashes through the Database service. The
// response returns both plaintext keys exactly once. The RPC is Unimplemented
// when no service-account store is attached.
func (s *GRPCServer) CreateServiceAccount(ctx context.Context, req *idppb.CreateServiceAccountRequest) (*idppb.CreateServiceAccountResponse, error) {
	if s.serviceAccounts == nil {
		return nil, status.Error(codes.Unimplemented, "service account store is not configured")
	}
	if req.GetLoginName() == "" {
		return nil, status.Error(codes.InvalidArgument, "login_name is required")
	}
	// Generate both keys' plaintexts and their salted hashes. A failure of
	// randomness, hashing, or storage fails the whole operation.
	keys := make([]*ServiceAccountKey, 0, 2)
	plaintexts := make([]*idppb.ServiceAccountPlaintextKey, 0, 2)
	for slot := 1; slot <= 2; slot++ {
		plaintext, err := NewServiceAccountKey()
		if err != nil {
			return nil, err
		}
		salt, err := NewServiceAccountKeySalt()
		if err != nil {
			return nil, err
		}
		keys = append(keys, &ServiceAccountKey{
			Slot:      slot,
			KeyHash:   HashServiceAccountKey(req.GetPepper(), salt, plaintext),
			KeySalt:   salt,
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
	salt, err := NewServiceAccountKeySalt()
	if err != nil {
		return nil, err
	}
	acc, err := s.serviceAccounts.RotateKey(ctx, req.GetId(), int(req.GetSlot()), HashServiceAccountKey(req.GetPepper(), salt, plaintext), salt, serviceAccountKeyPrefixOf(plaintext), req.GetRevision(), req.GetUpdatedBy())
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
