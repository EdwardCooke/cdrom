package idp

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	oidc "github.com/coreos/go-oidc/v3/oidc"

	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// TestUserRegisterAndLogin drives the full username/password flow over gRPC:
// register a user, log in, and verify the returned token with go-oidc (the
// same library the API uses). It checks the token's subject, email, and roles
// claims.
func TestUserRegisterAndLogin(t *testing.T) {
	users := NewMemoryUserStore()
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), users)
	ctx := context.Background()

	// The API would do this at startup: OIDC discovery against the issuer.
	provider, err := oidc.NewProvider(ctx, idp.httpURL)
	if err != nil {
		t.Fatalf("oidc discovery: %v", err)
	}
	verifier := provider.Verifier(&oidc.Config{SkipClientIDCheck: true})

	// Register the first user (it should be given the admin role).
	reg, err := idp.client.Register(ctx, &idppb.RegisterRequest{
		FirstName: "Ada", LastName: "Lovelace", Email: "ada@example.com", Password: "s3cret",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if reg.GetEmail() != "ada@example.com" {
		t.Errorf("register email = %q, want ada@example.com", reg.GetEmail())
	}
	if !hasRole(reg.GetRoles(), "admin") {
		t.Errorf("first user roles = %v, want to include admin", reg.GetRoles())
	}

	// Log in with the correct password.
	login, err := idp.client.Login(ctx, &idppb.LoginRequest{Email: "ada@example.com", Password: "s3cret"})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if login.GetAccessToken() == "" {
		t.Fatal("login returned no access_token")
	}
	if !hasRole(login.GetUser().GetRoles(), "admin") {
		t.Errorf("login roles = %v, want to include admin", login.GetUser().GetRoles())
	}

	// Verify the token exactly as the API would.
	idt, err := verifier.Verify(ctx, login.GetAccessToken())
	if err != nil {
		t.Fatalf("verify token: %v", err)
	}
	if idt.Subject != reg.GetId() {
		t.Errorf("token subject = %q, want %q", idt.Subject, reg.GetId())
	}
	var claims struct {
		Email string   `json:"email"`
		Name  string   `json:"name"`
		Roles []string `json:"roles"`
	}
	if err := idt.Claims(&claims); err != nil {
		t.Fatalf("claims: %v", err)
	}
	if claims.Email != "ada@example.com" {
		t.Errorf("token email = %q, want ada@example.com", claims.Email)
	}
	if !hasRole(claims.Roles, "admin") {
		t.Errorf("token roles = %v, want to include admin", claims.Roles)
	}

	// The user is stored with a password hash, not the plaintext.
	stored, err := users.GetByEmail(ctx, "ada@example.com")
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if stored.PasswordHash == "" {
		t.Error("stored user has no password hash")
	}
	if stored.PasswordHash == "s3cret" {
		t.Error("stored user password is the plaintext, not a hash")
	}
}

// TestUserLoginWrongPassword checks that a login with a bad password is
// rejected (Unauthenticated) and that an unknown user is rejected the same
// way.
func TestUserLoginWrongPassword(t *testing.T) {
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), NewMemoryUserStore())
	ctx := context.Background()

	if _, err := idp.client.Register(ctx, &idppb.RegisterRequest{
		FirstName: "Bob", LastName: "Jones", Email: "bob@example.com", Password: "right",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Wrong password.
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Email: "bob@example.com", Password: "wrong"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("wrong password code = %v, want Unauthenticated", status.Code(err))
	}

	// Unknown user.
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Email: "nobody@example.com", Password: "x"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("unknown user code = %v, want Unauthenticated", status.Code(err))
	}
}

// TestUserRegisterDuplicate checks that registering a user with an existing
// email is rejected (AlreadyExists).
func TestUserRegisterDuplicate(t *testing.T) {
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), NewMemoryUserStore())
	ctx := context.Background()

	req := &idppb.RegisterRequest{FirstName: "Ada", LastName: "L", Email: "ada@example.com", Password: "s3cret"}
	if _, err := idp.client.Register(ctx, req); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if _, err := idp.client.Register(ctx, req); status.Code(err) != codes.AlreadyExists {
		t.Errorf("duplicate register code = %v, want AlreadyExists", status.Code(err))
	}
}

// TestUserRolesManagement checks the user/role management RPCs: list, update
// roles, and delete.
func TestUserRolesManagement(t *testing.T) {
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), NewMemoryUserStore())
	ctx := context.Background()

	// Create two users (first is admin, second is user).
	if _, err := idp.client.Register(ctx, &idppb.RegisterRequest{
		FirstName: "Ada", LastName: "L", Email: "ada@example.com", Password: "s3cret",
	}); err != nil {
		t.Fatalf("register ada: %v", err)
	}
	bob, err := idp.client.Register(ctx, &idppb.RegisterRequest{
		FirstName: "Bob", LastName: "J", Email: "bob@example.com", Password: "pw",
	})
	if err != nil {
		t.Fatalf("register bob: %v", err)
	}
	if !hasRole(bob.GetRoles(), "user") {
		t.Errorf("second user roles = %v, want to include user", bob.GetRoles())
	}

	// List users.
	listed, err := idp.client.ListUsers(ctx, &idppb.ListUsersRequest{})
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	if len(listed.GetUsers()) != 2 {
		t.Errorf("list users returned %d users, want 2", len(listed.GetUsers()))
	}

	// Update bob's roles to admin.
	updated, err := idp.client.UpdateUser(ctx, &idppb.UpdateUserRequest{Id: bob.GetId(), Roles: []string{"admin"}})
	if err != nil {
		t.Fatalf("update user: %v", err)
	}
	if !hasRole(updated.GetRoles(), "admin") {
		t.Errorf("updated roles = %v, want to include admin", updated.GetRoles())
	}

	// Delete bob.
	if _, err := idp.client.DeleteUser(ctx, &idppb.DeleteUserRequest{Id: bob.GetId()}); err != nil {
		t.Fatalf("delete user: %v", err)
	}

	// A second delete is a NotFound.
	if _, err := idp.client.DeleteUser(ctx, &idppb.DeleteUserRequest{Id: bob.GetId()}); status.Code(err) != codes.NotFound {
		t.Errorf("second delete code = %v, want NotFound", status.Code(err))
	}
}

// TestUserLoginAudienceOverride checks that a login with an explicit audience
// stamps the token's aud with that audience — this is how the API requests a
// token whose aud matches the audience its OIDC verifier checks (its
// client_id or token_audience), so the API accepts the login token.
func TestUserLoginAudienceOverride(t *testing.T) {
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), NewMemoryUserStore())
	ctx := context.Background()

	provider, err := oidc.NewProvider(ctx, idp.httpURL)
	if err != nil {
		t.Fatalf("oidc discovery: %v", err)
	}
	// The API's verifier checks aud against its client_id ("cdrom-ui").
	verifier := provider.Verifier(&oidc.Config{ClientID: "cdrom-ui"})

	if _, err := idp.client.Register(ctx, &idppb.RegisterRequest{
		FirstName: "Ada", LastName: "L", Email: "ada@example.com", Password: "s3cret",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Login requesting the audience the API's verifier checks.
	login, err := idp.client.Login(ctx, &idppb.LoginRequest{
		Email: "ada@example.com", Password: "s3cret", Audience: "cdrom-ui",
	})
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	// The token verifies against the API's verifier (aud == client_id).
	idt, err := verifier.Verify(ctx, login.GetAccessToken())
	if err != nil {
		t.Fatalf("verify token: %v", err)
	}
	if len(idt.Audience) != 1 || idt.Audience[0] != "cdrom-ui" {
		t.Errorf("audience = %v, want [cdrom-ui]", idt.Audience)
	}
}

// hasRole reports whether roles contains role.
func hasRole(roles []string, role string) bool {
	for _, r := range roles {
		if r == role {
			return true
		}
	}
	return false
}
