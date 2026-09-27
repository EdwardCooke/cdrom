package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestMiddlewareDisabledPassthrough(t *testing.T) {
	a := &Auth{Handler: NewHandler(nil, nil)}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	rec := httptest.NewRecorder()
	a.Middleware(next).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("body = %q, want ok", rec.Body.String())
	}
}

func TestMiddlewareEnabled(t *testing.T) {
	idp := newFakeIDP(t)
	a, err := New(context.Background(), testAuthConfig(idp))
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	if !a.Enabled() {
		t.Fatal("expected auth to be enabled")
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(UserFromContext(r.Context()).Subject))
	})
	h := a.Middleware(next)

	// No cookie → 401.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no cookie: status = %d, want 401", rec.Code)
	}

	// Valid cookie → 200 with the user's subject.
	cookie := a.Sessions.NewCookie(User{Subject: "u1", Name: "U"}, time.Hour)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(cookie)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusOK {
		t.Errorf("cookie: status = %d, want 200", rec2.Code)
	}
	if rec2.Body.String() != "u1" {
		t.Errorf("body = %q, want u1", rec2.Body.String())
	}
}

func TestLoginDisabledRedirectsHome(t *testing.T) {
	h := NewHandler(nil, nil)
	rec := httptest.NewRecorder()
	h.Login(rec, httptest.NewRequest(http.MethodGet, "/api/auth/login", nil))
	if rec.Code != http.StatusFound {
		t.Errorf("status = %d, want 302", rec.Code)
	}
	if loc := rec.Result().Header.Get("Location"); loc != "/" {
		t.Errorf("Location = %q, want /", loc)
	}
}

// TestCallbackE2E drives the full authorization-code + PKCE flow against the
// fake identity provider: login → (browser to IdP) → callback → session cookie.
func TestCallbackE2E(t *testing.T) {
	idp := newFakeIDP(t)
	cfg := testAuthConfig(idp)
	provider, err := NewProvider(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	sessions := NewSessionManager(cfg.CookieSecret, "")
	handler := NewHandler(provider, sessions)

	// 1. Login: capture the auth redirect and the state cookie.
	loginRec := httptest.NewRecorder()
	handler.Login(loginRec, httptest.NewRequest(http.MethodGet, "/api/auth/login", nil))
	if loginRec.Code != http.StatusFound {
		t.Fatalf("login status = %d, want 302", loginRec.Code)
	}
	authURL := loginRec.Result().Header.Get("Location")
	if authURL == "" {
		t.Fatal("login did not set a Location header")
	}
	var stateCookie *http.Cookie
	for _, c := range loginRec.Result().Cookies() {
		if c.Name == stateCookieName {
			stateCookie = c
		}
	}
	if stateCookie == nil {
		t.Fatal("login did not set the state cookie")
	}
	u, err := url.Parse(authURL)
	if err != nil {
		t.Fatalf("parse auth url: %v", err)
	}
	state := u.Query().Get("state")
	if state == "" {
		t.Fatal("auth url is missing the state parameter")
	}

	// 2. Callback: the IdP redirects back with a code and the state.
	cbReq := httptest.NewRequest(http.MethodGet,
		"/api/auth/callback?code=fake-code&state="+url.QueryEscape(state), nil)
	cbReq.AddCookie(stateCookie)
	cbRec := httptest.NewRecorder()
	handler.Callback(cbRec, cbReq)
	if cbRec.Code != http.StatusFound {
		t.Fatalf("callback status = %d, want 302 (body: %s)", cbRec.Code, cbRec.Body.String())
	}
	var sessionCookie *http.Cookie
	for _, c := range cbRec.Result().Cookies() {
		if c.Name == SessionCookieName {
			sessionCookie = c
		}
	}
	if sessionCookie == nil {
		t.Fatal("callback did not set the session cookie")
	}

	// 3. The session cookie must verify to the IdP's user.
	verifyReq := httptest.NewRequest(http.MethodGet, "/", nil)
	verifyReq.AddCookie(sessionCookie)
	user, err := sessions.UserFromRequest(verifyReq)
	if err != nil {
		t.Fatalf("verify session: %v", err)
	}
	if user.Subject != "test-user" {
		t.Errorf("subject = %q, want test-user", user.Subject)
	}
	if user.Email != "test@example.com" {
		t.Errorf("email = %q, want test@example.com", user.Email)
	}
}

func TestCallbackStateMismatch(t *testing.T) {
	idp := newFakeIDP(t)
	provider, err := NewProvider(context.Background(), testAuthConfig(idp))
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	handler := NewHandler(provider, NewSessionManager("secret", ""))

	req := httptest.NewRequest(http.MethodGet,
		"/api/auth/callback?code=fake-code&state=evil-state", nil)
	// A state cookie that does not match the state in the query.
	req.AddCookie(&http.Cookie{Name: stateCookieName, Value: "different-state"})
	rec := httptest.NewRecorder()
	handler.Callback(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}
