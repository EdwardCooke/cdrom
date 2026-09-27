package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// SessionCookieName is the name of the API's session cookie.
const SessionCookieName = "cdrom_session"

// User is the authenticated principal carried in a session.
type User struct {
	Subject string `json:"sub"`
	Name    string `json:"name,omitempty"`
	Email   string `json:"email,omitempty"`
}

// Session is the signed payload stored in the session cookie.
type Session struct {
	User      `json:",inline"`
	ExpiresAt int64 `json:"exp"` // unix seconds
}

// SessionManager signs and verifies the API's session cookie. The cookie
// value is base64(payload) + "." + base64(HMAC-SHA256(payload, secret)), so a
// client cannot forge a session without the secret.
type SessionManager struct {
	secret []byte
	domain string
}

// NewSessionManager creates a session manager. secret signs the cookie;
// domain optionally scopes it (empty means the request host).
func NewSessionManager(secret, domain string) *SessionManager {
	return &SessionManager{secret: []byte(secret), domain: domain}
}

// NewSession builds a signed cookie value for user that expires after
// ttl.
func (m *SessionManager) NewCookie(user User, ttl time.Duration) *http.Cookie {
	session := Session{User: user, ExpiresAt: time.Now().Add(ttl).Unix()}
	payload, err := json.Marshal(&session)
	if err != nil {
		return nil
	}
	value := m.sign(payload)
	cookie := &http.Cookie{
		Name:     SessionCookieName,
		Value:    value,
		Path:     "/",
		MaxAge:   int(ttl.Seconds()),
		Expires:  time.Now().Add(ttl),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Domain:   m.domain,
	}
	// Secure is set by the caller when the API is served over TLS.
	return cookie
}

// UserFromRequest extracts and verifies the session cookie from r, returning
// the authenticated user. It returns an error when the cookie is missing,
// malformed, expired, or has an invalid signature.
func (m *SessionManager) UserFromRequest(r *http.Request) (User, error) {
	cookie, err := r.Cookie(SessionCookieName)
	if err != nil {
		return User{}, fmt.Errorf("auth: missing session cookie")
	}
	user, err := m.verify(cookie.Value)
	if err != nil {
		return User{}, err
	}
	return user, nil
}

// sign returns base64(payload) + "." + base64(hmac).
func (m *SessionManager) sign(payload []byte) string {
	mac := hmac.New(sha256.New, m.secret)
	mac.Write(payload)
	sig := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// verify splits the cookie value, re-computes the HMAC, and decodes the
// payload. It rejects expired sessions.
func (m *SessionManager) verify(value string) (User, error) {
	parts := strings.SplitN(value, ".", 2)
	if len(parts) != 2 {
		return User{}, fmt.Errorf("auth: malformed session cookie")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return User{}, fmt.Errorf("auth: decode session payload: %w", err)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return User{}, fmt.Errorf("auth: decode session signature: %w", err)
	}
	mac := hmac.New(sha256.New, m.secret)
	mac.Write(payload)
	expected := mac.Sum(nil)
	if !hmac.Equal(sig, expected) {
		return User{}, fmt.Errorf("auth: invalid session signature")
	}
	var session Session
	if err := json.Unmarshal(payload, &session); err != nil {
		return User{}, fmt.Errorf("auth: decode session: %w", err)
	}
	if time.Now().Unix() > session.ExpiresAt {
		return User{}, fmt.Errorf("auth: session expired")
	}
	return session.User, nil
}
