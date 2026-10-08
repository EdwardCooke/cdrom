package api

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// UserPassClient is the API's proxy to the IdP's username/password surface.
// The API is a thin bridge: it forwards the UI's login, register, and
// user-management requests to the IdP (where the real logic — password
// verification, token minting, and user storage — lives) and returns the
// IdP's response. The token a login returns is an OIDC token the API verifies
// against the IdP's JWKS, the same way it verifies any other token. The
// requests go to the IdP over gRPC (mTLS when TLS is configured), so only the
// API can reach them.
type UserPassClient interface {
	// Login verifies the user's credentials and returns the OIDC token the
	// IdP minted for the user (plus the user's profile and roles).
	Login(ctx context.Context, email, password string) (*LoginResult, error)
	// Register creates a new user from the request's profile and password.
	Register(ctx context.Context, req RegisterRequest) (*UserResult, error)
	// ListUsers returns all registered users (profile + roles, no password).
	ListUsers(ctx context.Context) ([]UserResult, error)
	// CreateUser creates a user (the same as Register, also usable to create
	// a user with an explicit role set).
	CreateUser(ctx context.Context, req RegisterRequest) (*UserResult, error)
	// UpdateUser updates a user's profile and/or roles (and password when
	// supplied), keyed by user ID.
	UpdateUser(ctx context.Context, id string, req UpdateUserRequest) (*UserResult, error)
	// DeleteUser removes a user by ID.
	DeleteUser(ctx context.Context, id string) error
}

// LoginResult is the result of a successful login: the OIDC token the IdP
// minted for the user (present it as `Authorization: Bearer <access_token>`),
// plus the user's profile and roles.
type LoginResult struct {
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

// UserResult is the profile of a registered user (never the password hash).
type UserResult struct {
	ID        string   `json:"id"`
	FirstName string   `json:"first_name"`
	LastName  string   `json:"last_name"`
	Email     string   `json:"email"`
	Roles     []string `json:"roles"`
}

// RegisterRequest is the request to create a user: the required profile
// fields (first/last/email) and password, plus an optional initial role set.
type RegisterRequest struct {
	FirstName string   `json:"first_name"`
	LastName  string   `json:"last_name"`
	Email     string   `json:"email"`
	Password  string   `json:"password"`
	Roles     []string `json:"roles,omitempty"`
}

// UpdateUserRequest is the request to update a user's profile and/or roles.
// Empty fields are left unchanged; an empty password leaves the existing
// password untouched.
type UpdateUserRequest struct {
	FirstName string   `json:"first_name,omitempty"`
	LastName  string   `json:"last_name,omitempty"`
	Email     string   `json:"email,omitempty"`
	Password  string   `json:"password,omitempty"`
	Roles     []string `json:"roles,omitempty"`
}

// grpcUserPassClient is a UserPassClient that talks to the IdP's gRPC service
// (over mTLS when TLS is configured).
type grpcUserPassClient struct {
	idp idppb.IdPClient
	// audience is the audience the API's OIDC verifier checks on a presented
	// token (its client_id, or token_audience when set). It is passed to the
	// IdP at login so the minted token's aud matches and the API accepts it.
	audience string
	// issuer is the OIDC issuer the API's verifier checks on a presented
	// token (its configured IdP base URL). It is passed to the IdP at login
	// so the minted token's iss matches the API's discovery document.
	issuer string
}

// NewUserPassClient returns a UserPassClient that proxies to the IdP's gRPC
// service (idp). audience is the audience the API's verifier checks on a
// token (its client_id, or token_audience when set); it is stamped on the
// login token so the API accepts it. issuer is the OIDC issuer the API's
// verifier checks (its configured IdP base URL); it is stamped on the login
// token so it verifies against the API's discovery document. An empty
// audience or issuer lets the IdP use its own defaults.
func NewUserPassClient(idp idppb.IdPClient, audience, issuer string) UserPassClient {
	return &grpcUserPassClient{idp: idp, audience: audience, issuer: issuer}
}

func (c *grpcUserPassClient) Login(ctx context.Context, email, password string) (*LoginResult, error) {
	resp, err := c.idp.Login(ctx, &idppb.LoginRequest{
		Email:    email,
		Password: password,
		Audience: c.audience,
		Issuer:   c.issuer,
	})
	if err != nil {
		return nil, &userPassError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	u := resp.GetUser()
	return &LoginResult{
		AccessToken: resp.GetAccessToken(),
		TokenType:   resp.GetTokenType(),
		ExpiresIn:   int(resp.GetExpiresIn()),
		IDToken:     resp.GetIdToken(),
		ID:          u.GetId(),
		FirstName:   u.GetFirstName(),
		LastName:    u.GetLastName(),
		Email:       u.GetEmail(),
		Roles:       nonNil(u.GetRoles()),
	}, nil
}

func (c *grpcUserPassClient) Register(ctx context.Context, req RegisterRequest) (*UserResult, error) {
	u, err := c.idp.Register(ctx, &idppb.RegisterRequest{
		FirstName: req.FirstName,
		LastName:  req.LastName,
		Email:     req.Email,
		Password:  req.Password,
		Roles:     req.Roles,
	})
	if err != nil {
		return nil, &userPassError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return userResult(u), nil
}

func (c *grpcUserPassClient) ListUsers(ctx context.Context) ([]UserResult, error) {
	resp, err := c.idp.ListUsers(ctx, &idppb.ListUsersRequest{})
	if err != nil {
		return nil, &userPassError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	out := make([]UserResult, 0, len(resp.GetUsers()))
	for _, u := range resp.GetUsers() {
		out = append(out, *userResult(u))
	}
	return out, nil
}

func (c *grpcUserPassClient) CreateUser(ctx context.Context, req RegisterRequest) (*UserResult, error) {
	u, err := c.idp.CreateUser(ctx, &idppb.CreateUserRequest{
		FirstName: req.FirstName,
		LastName:  req.LastName,
		Email:     req.Email,
		Password:  req.Password,
		Roles:     req.Roles,
	})
	if err != nil {
		return nil, &userPassError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return userResult(u), nil
}

func (c *grpcUserPassClient) UpdateUser(ctx context.Context, id string, req UpdateUserRequest) (*UserResult, error) {
	u, err := c.idp.UpdateUser(ctx, &idppb.UpdateUserRequest{
		Id:        id,
		FirstName: req.FirstName,
		LastName:  req.LastName,
		Email:     req.Email,
		Password:  req.Password,
		Roles:     req.Roles,
	})
	if err != nil {
		return nil, &userPassError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return userResult(u), nil
}

func (c *grpcUserPassClient) DeleteUser(ctx context.Context, id string) error {
	_, err := c.idp.DeleteUser(ctx, &idppb.DeleteUserRequest{Id: id})
	if err != nil {
		return &userPassError{code: status.Code(err), msg: status.Convert(err).Message()}
	}
	return nil
}

// userResult renders a user's profile (never the password hash).
func userResult(u *dbpb.IDPUser) *UserResult {
	return &UserResult{
		ID:        u.GetId(),
		FirstName: u.GetFirstName(),
		LastName:  u.GetLastName(),
		Email:     u.GetEmail(),
		Roles:     nonNil(u.GetRoles()),
	}
}

// userPassError is an error from the IdP's userpass surface, carrying the
// gRPC status code (so the API can map it to an HTTP status) and the message.
type userPassError struct {
	code codes.Code
	msg  string
}

func (e *userPassError) Error() string {
	return fmt.Sprintf("idp userpass: %s: %s", e.code, e.msg)
}
