// locallogin_test.go — F-28.4 Slice 4: the local-login switch, login-channel
// marking, and DescribeSystem.
//
// Covers: (a) which credential channel a login reports (standing vs temporal);
// (b) the switch OFF ⇒ SSO-only (standing logins and registration refused with
// a distinct denial, temporal/break-glass logins still admitted); (c) the
// switch ON (production default) ⇒ everything behaves as before; and
// (d) DescribeSystem reflecting the IdP's own flags.
package idp

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// standingUser crafts an active user with a permanent (standing) password.
func standingUser(t *testing.T, store *MemoryUserStore, username, password string) *User {
	t.Helper()
	h, err := HashPassword("", password)
	if err != nil {
		t.Fatalf("hash standing: %v", err)
	}
	u, err := store.Create(context.Background(), &User{
		Username: username, Roles: []string{"user"}, Active: true, PasswordHash: h,
	})
	if err != nil {
		t.Fatalf("create standing user: %v", err)
	}
	return u
}

// temporalUser crafts an active user with a BREAK-GLASS (temporal) password
// inside its validity window.
func temporalUser(t *testing.T, store *MemoryUserStore, username, password string) *User {
	t.Helper()
	h, err := HashPassword("", password)
	if err != nil {
		t.Fatalf("hash temporal: %v", err)
	}
	until := time.Now().Add(time.Hour)
	u, err := store.Create(context.Background(), &User{
		Username: username, Roles: []string{"admin"}, Active: true,
		TempPasswordHash: h, TempPasswordValidUntil: &until,
	})
	if err != nil {
		t.Fatalf("create temporal user: %v", err)
	}
	return u
}

// TestLoginChannelStamped verifies the login response reports WHICH credential
// satisfied it: a permanent password ⇒ "standing"; a break-glass password
// inside its window ⇒ "temporal".
func TestLoginChannelStamped(t *testing.T) {
	users := NewMemoryUserStore()
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), users)
	ctx := context.Background()

	standingUser(t, users, "steve.standing", "pw-standing")
	temporalUser(t, users, "tess.temporal", "pw-temporal")

	// Standing credential ⇒ channel "standing".
	res, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "steve.standing", Password: "pw-standing"})
	if err != nil {
		t.Fatalf("standing login: %v", err)
	}
	if res.GetChannel() != "standing" {
		t.Errorf("standing channel = %q, want standing", res.GetChannel())
	}

	// Temporal credential (within window) ⇒ channel "temporal".
	res, err = idp.client.Login(ctx, &idppb.LoginRequest{Username: "tess.temporal", Password: "pw-temporal"})
	if err != nil {
		t.Fatalf("temporal login: %v", err)
	}
	if res.GetChannel() != "temporal" {
		t.Errorf("temporal channel = %q, want temporal", res.GetChannel())
	}
}

// TestLocalLoginSwitchOff verifies SSO-only mode: with the switch OFF, a
// STANDING login and registration are refused (distinct PermissionDenied),
// while a TEMPORAL (break-glass) login is still admitted.
func TestLocalLoginSwitchOff(t *testing.T) {
	users := NewMemoryUserStore()
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.LocalLoginEnabled = false
	idp := startTestIDP(t, cfg, users)
	ctx := context.Background()

	// Pre-populate (creation happened "earlier", before the switch went off).
	standingUser(t, users, "held.out", "pw")
	temporalUser(t, users, "breaker.in", "pw-bg")

	// Standing login ⇒ REFUSED (distinct denial, not generic invalid creds).
	_, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "held.out", Password: "pw"})
	if err == nil {
		t.Fatal("standing login with switch off succeeded, want refused")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("standing-denied code = %v, want PermissionDenied", status.Code(err))
	}

	// Registration ⇒ REFUSED.
	_, err = idp.client.Register(ctx, &idppb.RegisterRequest{Username: "late.joiner", Password: "pw"})
	if err == nil {
		t.Fatal("register with switch off succeeded, want refused")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("register-refused code = %v, want PermissionDenied", status.Code(err))
	}

	// Temporal (break-glass) login ⇒ STILL ADMITTED (the guaranteed back-door).
	res, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "breaker.in", Password: "pw-bg"})
	if err != nil {
		t.Fatalf("temporal login with switch off: %v", err)
	}
	if res.GetChannel() != "temporal" {
		t.Errorf("temporal channel = %q, want temporal", res.GetChannel())
	}
}

// TestLocalLoginSwitchOn verifies the production default (ON): standing and
// temporal logins and registration all succeed.
func TestLocalLoginSwitchOn(t *testing.T) {
	users := NewMemoryUserStore()
	idp := startTestIDP(t, testConfig(time.Hour, 30*time.Minute), users) // LocalLoginEnabled=true
	ctx := context.Background()

	if _, err := idp.client.Register(ctx, &idppb.RegisterRequest{Username: "new.person", Password: "pw"}); err != nil {
		t.Fatalf("register with switch on: %v", err)
	}
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "new.person", Password: "pw"}); err != nil {
		t.Fatalf("standing login with switch on: %v", err)
	}
	temporalUser(t, users, "bg.open", "pw-bg")
	if _, err := idp.client.Login(ctx, &idppb.LoginRequest{Username: "bg.open", Password: "pw-bg"}); err != nil {
		t.Fatalf("temporal login with switch on: %v", err)
	}
}

// TestDescribeSystem verifies the IdP's DescribeSystem RPC reports its OWN
// enablement flags (capability only — no material).
func TestDescribeSystem(t *testing.T) {
	users := NewMemoryUserStore()
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.Scim.Enabled = true
	cfg.LocalLoginEnabled = false
	cfg.BreakGlassEnabled = true
	idp := startTestIDP(t, cfg, users)

	desc, err := idp.client.DescribeSystem(context.Background(), &idppb.DescribeSystemRequest{})
	if err != nil {
		t.Fatalf("DescribeSystem: %v", err)
	}
	if desc.GetScimEnabled() != true {
		t.Errorf("scim_enabled = %v, want true", desc.GetScimEnabled())
	}
	if desc.GetLocalLoginEnabled() != false {
		t.Errorf("local_login_enabled = %v, want false", desc.GetLocalLoginEnabled())
	}
	if desc.GetBreakGlassEnabled() != true {
		t.Errorf("break_glass_enabled = %v, want true", desc.GetBreakGlassEnabled())
	}
}

// TestDescribeSystemReflectsBaseline verifies DescribeSystem faithfully
// REPORTS the config it was built from (here: local login ON via testConfig,
// break-glass ON set explicitly, SCIM OFF) — i.e. it is a truthful mirror, not
// a hardcoded answer.
func TestDescribeSystemReflectsBaseline(t *testing.T) {
	users := NewMemoryUserStore()
	cfg := testConfig(time.Hour, 30*time.Minute)
	cfg.BreakGlassEnabled = true
	idp := startTestIDP(t, cfg, users)
	desc, err := idp.client.DescribeSystem(context.Background(), &idppb.DescribeSystemRequest{})
	if err != nil {
		t.Fatalf("DescribeSystem: %v", err)
	}
	if !desc.GetLocalLoginEnabled() {
		t.Error("local_login_enabled = false, want true (testConfig mirrors production)")
	}
	if !desc.GetBreakGlassEnabled() {
		t.Error("break_glass_enabled = false, want true")
	}
	if desc.GetScimEnabled() {
		t.Error("scim_enabled = true, want false")
	}
}
