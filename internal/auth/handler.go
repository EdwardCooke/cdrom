package auth

import (
	"context"
	"net/http"
	"time"

	oidc "github.com/coreos/go-oidc/v3/oidc"
)

// sessionTTL is how long a session cookie stays valid.
const sessionTTL = 12 * time.Hour

// stateCookieName is the short-lived cookie that carries the OIDC state
// value back from the identity provider to prevent CSRF.
const stateCookieName = "cdrom_auth_state"

// Handler implements the OIDC authorization-code + PKCE endpoints:
//
//	GET /api/auth/login     → redirect to the identity provider
//	GET /api/auth/callback  → exchange the code, set the session cookie
//	GET /api/auth/logout    → clear the session cookie
type Handler struct {
	provider *Provider
	sessions *SessionManager
	secure   bool
}

// NewHandler creates the auth endpoint handler. provider and sessions may be
// nil when authentication is disabled.
func NewHandler(provider *Provider, sessions *SessionManager) *Handler {
	return &Handler{provider: provider, sessions: sessions}
}

// SetSecure marks the session cookie as Secure (set when the API is served
// over TLS).
func (h *Handler) SetSecure(secure bool) {
	h.secure = secure
	if h.sessions != nil {
		// secure is applied at cookie-creation time via the handler.
	}
}

// Login starts the OIDC flow: it generates a state + PKCE verifier, stores
// the state in a short-lived cookie, and redirects the browser to the
// identity provider.
func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	if h.provider == nil {
		// Auth disabled: nothing to do.
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	state, verifier := NewPKCEState()
	http.SetCookie(w, &http.Cookie{
		Name:     stateCookieName,
		Value:    state,
		Path:     "/",
		MaxAge:   120,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, h.provider.AuthURL(state, verifier), http.StatusFound)
}

// Callback completes the OIDC flow: it validates the state, exchanges the
// code for a validated ID token, sets the session cookie, and redirects to
// the app.
func (h *Handler) Callback(w http.ResponseWriter, r *http.Request) {
	if h.provider == nil {
		http.NotFound(w, r)
		return
	}
	q := r.URL.Query()
	code := q.Get("code")
	state := q.Get("state")
	if code == "" || state == "" {
		h.authError(w, "missing code or state")
		return
	}
	// Verify the state against the cookie set in Login (CSRF protection).
	stateCookie, err := r.Cookie(stateCookieName)
	if err != nil || stateCookie.Value != state {
		h.authError(w, "state mismatch")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: stateCookieName, Value: "", Path: "/", MaxAge: -1})

	idToken, err := h.provider.ExchangeCallback(r.Context(), code, CodeVerifierFromState(state))
	if err != nil {
		h.authError(w, err.Error())
		return
	}
	user := userFromIDToken(idToken)
	cookie := h.sessions.NewCookie(user, sessionTTL)
	cookie.Secure = h.secure
	http.SetCookie(w, cookie)
	http.Redirect(w, r, "/", http.StatusFound)
}

// Logout clears the session cookie and redirects to the login flow.
func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: SessionCookieName, Value: "", Path: "/", MaxAge: -1})
	if h.provider != nil {
		http.Redirect(w, r, "/api/auth/login", http.StatusFound)
		return
	}
	http.Redirect(w, r, "/", http.StatusFound)
}

func (h *Handler) authError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"error":"` + msg + `"}`))
}

// userFromIDToken extracts the principal from validated OIDC claims.
func userFromIDToken(idToken *oidc.IDToken) User {
	var claims struct {
		Subject string `json:"sub"`
		Name    string `json:"name"`
		Email   string `json:"email"`
	}
	_ = idToken.Claims(&claims)
	return User{Subject: claims.Subject, Name: claims.Name, Email: claims.Email}
}

// userContextKey is the context key under which the authenticated user is
// stored by the middleware.
type userContextKey struct{}

// WithUser returns a context carrying user.
func WithUser(ctx context.Context, user User) context.Context {
	return context.WithValue(ctx, userContextKey{}, user)
}

// UserFromContext returns the authenticated user stored by the middleware,
// or the zero User when absent.
func UserFromContext(ctx context.Context) User {
	if user, ok := ctx.Value(userContextKey{}).(User); ok {
		return user
	}
	return User{}
}

// Middleware wraps next so that, when authentication is enabled, every
// request must carry a valid session cookie. When authentication is disabled
// it passes requests through unchanged.
func (a *Auth) Middleware(next http.Handler) http.Handler {
	if !a.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, err := a.Sessions.UserFromRequest(r)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"authentication required"}`))
			return
		}
		next.ServeHTTP(w, r.WithContext(WithUser(r.Context(), user)))
	})
}
