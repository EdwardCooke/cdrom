package auth

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSessionRoundTrip(t *testing.T) {
	m := NewSessionManager("secret", "")
	cookie := m.NewCookie(User{Subject: "s", Name: "n", Email: "e"}, time.Hour)
	if cookie == nil {
		t.Fatal("NewCookie returned nil")
	}
	if !cookie.HttpOnly {
		t.Error("expected an HttpOnly cookie")
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(cookie)
	user, err := m.UserFromRequest(req)
	if err != nil {
		t.Fatalf("UserFromRequest: %v", err)
	}
	if user.Subject != "s" || user.Name != "n" || user.Email != "e" {
		t.Errorf("user = %+v", user)
	}
}

func TestSessionMissingCookie(t *testing.T) {
	m := NewSessionManager("secret", "")
	if _, err := m.UserFromRequest(httptest.NewRequest(http.MethodGet, "/", nil)); err == nil {
		t.Error("expected an error for a missing cookie")
	}
}

func TestSessionTamperedPayload(t *testing.T) {
	m := NewSessionManager("secret", "")
	cookie := m.NewCookie(User{Subject: "s"}, time.Hour)
	parts := strings.SplitN(cookie.Value, ".", 2)
	if len(parts) != 2 {
		t.Fatalf("unexpected cookie format: %q", cookie.Value)
	}
	payload, _ := base64.RawURLEncoding.DecodeString(parts[0])
	var session Session
	if err := json.Unmarshal(payload, &session); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	session.Subject = "attacker"
	newPayload, _ := json.Marshal(&session)
	tampered := base64.RawURLEncoding.EncodeToString(newPayload) + "." + parts[1]
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: tampered})
	if _, err := m.UserFromRequest(req); err == nil {
		t.Error("expected an error for a tampered cookie")
	}
}

func TestSessionWrongSecret(t *testing.T) {
	signer := NewSessionManager("secret-a", "")
	verifier := NewSessionManager("secret-b", "")
	cookie := signer.NewCookie(User{Subject: "s"}, time.Hour)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(cookie)
	if _, err := verifier.UserFromRequest(req); err == nil {
		t.Error("expected an error when verifying with the wrong secret")
	}
}

func TestSessionExpired(t *testing.T) {
	m := NewSessionManager("secret", "")
	cookie := m.NewCookie(User{Subject: "s"}, -time.Hour)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(cookie)
	if _, err := m.UserFromRequest(req); err == nil {
		t.Error("expected an error for an expired session")
	}
}
