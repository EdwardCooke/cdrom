package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// UserPassClient is the API's proxy to the IdP's username/password
// endpoints. The API is a thin bridge: it forwards the UI's login, register,
// and user-management requests to the IdP (where the real logic — password
// verification, token minting, and user storage — lives) and returns the
// IdP's response. The token a login returns is an OIDC token the API verifies
// against the IdP's JWKS, the same way it verifies any other token.
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

// httpUserPassClient is a UserPassClient that talks to the IdP over HTTP.
type httpUserPassClient struct {
	base string
	http *http.Client
	// audience is the audience the API's OIDC verifier checks on a presented
	// token (its client_id, or token_audience when set). It is passed to the
	// IdP at login so the minted token's aud matches and the API accepts it.
	audience string
}

// NewUserPassClient returns a UserPassClient that proxies to the IdP at
// baseURL (the IdP's base URL, e.g. http://127.0.0.1:7104). httpClient is
// used for the requests; when it is an mTLS client (see grpcutil.HTTPClient)
// it can reach an IdP that serves TLS. A nil httpClient uses the default.
// audience is the audience the API's verifier checks on a token (its
// client_id, or token_audience when set); it is stamped on the login token so
// the API accepts it. An empty audience lets the IdP use its default.
func NewUserPassClient(baseURL string, httpClient *http.Client, audience string) UserPassClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &httpUserPassClient{base: normalizeIDPURL(baseURL), http: httpClient, audience: audience}
}

func (c *httpUserPassClient) Login(ctx context.Context, email, password string) (*LoginResult, error) {
	body := map[string]string{"email": email, "password": password}
	if c.audience != "" {
		body["audience"] = c.audience
	}
	var out LoginResult
	if err := c.postJSON(ctx, "/login", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *httpUserPassClient) Register(ctx context.Context, req RegisterRequest) (*UserResult, error) {
	var out UserResult
	if err := c.postJSON(ctx, "/register", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *httpUserPassClient) ListUsers(ctx context.Context) ([]UserResult, error) {
	var out []UserResult
	if err := c.getJSON(ctx, "/users", &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *httpUserPassClient) CreateUser(ctx context.Context, req RegisterRequest) (*UserResult, error) {
	var out UserResult
	if err := c.postJSON(ctx, "/users", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *httpUserPassClient) UpdateUser(ctx context.Context, id string, req UpdateUserRequest) (*UserResult, error) {
	var out UserResult
	if err := c.putJSON(ctx, "/users/"+id, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *httpUserPassClient) DeleteUser(ctx context.Context, id string) error {
	return c.doJSON(ctx, http.MethodDelete, "/users/"+id, nil, nil)
}

// postJSON posts body to base+path and decodes the JSON response into out.
func (c *httpUserPassClient) postJSON(ctx context.Context, path string, body, out any) error {
	return c.doJSON(ctx, http.MethodPost, path, body, out)
}

// putJSON PUTs body to base+path and decodes the JSON response into out.
func (c *httpUserPassClient) putJSON(ctx context.Context, path string, body, out any) error {
	return c.doJSON(ctx, http.MethodPut, path, body, out)
}

// getJSON GETs base+path and decodes the JSON response into out.
func (c *httpUserPassClient) getJSON(ctx context.Context, path string, out any) error {
	return c.doJSON(ctx, http.MethodGet, path, nil, out)
}

// doJSON performs a JSON request to the IdP and, on a 2xx response, decodes
// the body into out (when out is non-nil). A non-2xx response is an error
// carrying the IdP's message.
func (c *httpUserPassClient) doJSON(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("api: encode userpass request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return fmt.Errorf("api: build userpass request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("api: userpass request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("api: read userpass response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &userPassError{status: resp.StatusCode, body: messageFromJSON(data)}
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("api: decode userpass response: %w", err)
		}
	}
	return nil
}

// userPassError is a non-2xx response from the IdP's userpass endpoints.
type userPassError struct {
	status int
	body   string
}

func (e *userPassError) Error() string {
	return fmt.Sprintf("idp userpass: status %d: %s", e.status, e.body)
}

// messageFromJSON extracts the "error" field from a JSON error body, falling
// back to the raw body when it is not a JSON object.
func messageFromJSON(data []byte) string {
	var m struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(data, &m); err == nil && m.Error != "" {
		return m.Error
	}
	return strings.TrimSpace(string(data))
}
