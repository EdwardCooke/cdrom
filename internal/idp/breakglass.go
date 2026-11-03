package idp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cdrom/internal/audit"
	"cdrom/internal/config"
)

// Break-glass administration (F-28.3, Slice 3).
//
// The escape hatch: when SSO/SCIM is the broken link and no human can
// authenticate, an operator presents the API's CA-signed CLIENT certificate
// to POST /emergency/break-glass on THIS (the IdP's) HTTP listener and
// receives an EPHEMERAL, RANDOM-IDENTITY platform admin whose password
// SELF-EXPIRES. Coinage is strictly ADDITIVE — it inserts a brand-new user
// and never modifies any existing row — and every invocation is audited with
// the caller's certificate attributed.
//
// Trust posture (stated plainly in the spec): whoever holds the API's client
// certificate can coin ephemeral platform admins — equivalent to possessing
// the API's TLS secrets. Compensation: short TTL, a small live-pool cap, and
// full audit with certificate attribution.

// breakGlassRoute is the FIXED URL path of the break-glass endpoint. It is
// intentionally not configurable (cf. SCIMBasePath): the mount is a flat
// literal so the drill is predictable.
const breakGlassRoute = "/emergency/break-glass"

// breakGlassAlphabet is the drawing alphabet for the randomized login name:
// lowercase letters + digits (36 symbols × 16 draws ≈ 75 bits of entropy).
const breakGlassAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

// breakGlassDrawLength is the number of alphabet draws in the login name.
const breakGlassDrawLength = 16

// breakGlassPasswordLength is the length of the coined password (lowercase
// letters + digits; 24 chars ≈ 113 bits).
const breakGlassPasswordLength = 24

// breakGlassRetryBudget bounds the redraw attempts on the (practically
// impossible) unique-email collision: failing CLOSED, never touching
// somebody else's row.
const breakGlassRetryBudget = 5

// breakGlassSweepCadence is how often the hygiene sweep runs.
const breakGlassSweepCadence = time.Minute

// breakGlassDenyBody is the UNIFORM denial response: no-valid-certificate and
// disabled-toggle cases are INDISTINGUISHABLE (no state is advertised).
const breakGlassDenyBody = `{"error":"forbidden"}`

// breakGlassPasswordCharset is the charset of the coined password.
const breakGlassPasswordCharset = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

// breakGlassCoinResponse is the 200 body of a successful coinage. The
// PLAINTEXT password appears exactly once (Cache-Control: no-store); it is
// never logged and never audited (house rule shared with F-25/F-26/F-28.2:
// record the event, not the secret).
type breakGlassCoinResponse struct {
	Email     string `json:"email"`
	Password  string `json:"password"`
	ExpiresAt string `json:"expires_at"`
}

// breakGlassCoiner coins break-glass admins. Production is *coinBreakGlass;
// tests inject doubles.
type breakGlassCoiner interface {
	coin(ctx context.Context, ttl time.Duration) (*breakGlassCoinResponse, error)
}

// ErrBreakGlassCapReached is returned when the live-pool cap is exhausted;
// the handler maps it to 429.
type ErrBreakGlassCapReached struct{ Cap int }

func (e ErrBreakGlassCapReached) Error() string {
	return fmt.Sprintf("break glass: live-pool cap of %d reached; wait for existing credentials to expire", e.Cap)
}

// coinBreakGlass is the production breakGlassCoiner: it draws a random
// identity, mints a temporal password, and INSERTS a new user — strictly
// additive, never modifying an existing row.
type coinBreakGlass struct {
	cfg    config.IdPConfig
	users  UserStore
	logger *slog.Logger
}

// NewBreakGlassCoiner constructs the production breakGlassCoiner over the
// given user store (F-28.3, Slice 3).
func NewBreakGlassCoiner(cfg config.IdPConfig, users UserStore, logger *slog.Logger) breakGlassCoiner {
	if logger == nil {
		logger = slog.Default()
	}
	return &coinBreakGlass{cfg: cfg, users: users, logger: logger}
}

func (c *coinBreakGlass) coin(ctx context.Context, ttl time.Duration) (*breakGlassCoinResponse, error) {
	liveCap := c.cfg.EffectiveBreakGlassLiveCap()
	// Live-pool cap (anti-spam, F-28.2 pool shape): count the LIVE
	// (unexpired) break-glass admins among the tombstone-aware listing.
	users, err := c.users.List(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("break glass: list users: %w", err)
	}
	now := time.Now()
	live := 0
	for _, u := range users {
		if u.isBreakGlass() && u.unexpired(now) {
			live++
		}
	}
	if live >= liveCap {
		return nil, ErrBreakGlassCapReached{Cap: liveCap}
	}
	password, err := drawRandom(breakGlassPasswordCharset, breakGlassPasswordLength)
	if err != nil {
		return nil, fmt.Errorf("break glass: draw password: %w", err)
	}
	tempHash, err := HashPassword(c.cfg.PasswordPepper, password)
	if err != nil {
		return nil, fmt.Errorf("break glass: encode temporal password: %w", err)
	}
	validUntil := now.Add(ttl)
	coined := &User{
		Email:                  "", // filled per attempt
		Roles:                  []string{"admin"},
		Active:                 true,
		Provisioned:            false, // a LOCAL user: outside SCIM governance
		TempPasswordHash:       tempHash,
		TempPasswordValidUntil: &validUntil,
	}
	var lastErr error
	for attempt := 0; attempt < breakGlassRetryBudget; attempt++ {
		draw, derr := drawRandom(breakGlassAlphabet, breakGlassDrawLength)
		if derr != nil {
			return nil, fmt.Errorf("break glass: draw identity: %w", derr)
		}
		coined.Email = "breakglass-" + draw + "@invalid"
		created, cerr := c.users.Create(ctx, coined)
		if cerr == nil {
			c.logger.Info("break glass: coined ephemeral admin", "email", created.Email, "ttl", ttl.Round(time.Second))
			return &breakGlassCoinResponse{
				Email:     created.Email,
				Password:  password,
				ExpiresAt: validUntil.UTC().Format(time.RFC3339),
			}, nil
		}
		lastErr = cerr
		// A collision on our own @invalid namespace is retriable (redraw);
		// anything else fails closed.
		if !strings.HasSuffix(coined.Email, "@invalid") {
			break
		}
	}
	return nil, fmt.Errorf("break glass: coin identity (budget exhausted): %w", lastErr)
}

// isBreakGlass identifies a break-glass-coined user: the randomized
// @invalid login name (structural — SCIM cannot mint it, and no local
// registrant picks it).
func (u *User) isBreakGlass() bool {
	return strings.HasPrefix(u.Email, "breakglass-") && strings.HasSuffix(u.Email, "@invalid")
}

// unexpired reports whether the user's TEMPORAL credential is still inside
// its window (a user with no temporal credential is considered expired).
func (u *User) unexpired(now time.Time) bool {
	return u.TempPasswordValidUntil != nil && now.Before(*u.TempPasswordValidUntil)
}

// needsRetirement reports whether a break-glass user is DUE for the hygiene
// sweep (F-28.3): its temporal window has lapsed (or was already voided) and
// it is not YET fully retired (inactive + credential-free). A fully retired
// user is skipped, so successive sweeps are idempotent.
func (u *User) needsRetirement(now time.Time) bool {
	if !u.isBreakGlass() {
		return false
	}
	// Fully retired: no temporal credential and inactive.
	if u.TempPasswordHash == "" && !u.Active {
		return false
	}
	// Window still open: not yet due.
	if u.unexpired(now) {
		return false
	}
	return true
}

// SweepExpiredBreakGlass deactivates expired break-glass users and zeroes
// their temporal hash — symmetric with "soft delete is the only delete"
// (the row is KEPT, the credential VOIDED; accrual is incident-frequency and
// benign). Returns the number of users swept. Errors are logged, not fatal:
// the sweep is hygiene, and a transient blip costs nothing (next minute).
func (s *Server) SweepExpiredBreakGlass(ctx context.Context) int {
	if s.bgUsers == nil {
		return 0
	}
	users, err := s.bgUsers.List(ctx, true)
	if err != nil {
		s.logger.Warn("break glass: sweep list", "err", err)
		return 0
	}
	now := time.Now()
	swept := 0
	for _, u := range users {
		if !u.needsRetirement(now) {
			continue
		}
		if u.TempPasswordHash != "" {
			if err := s.bgUsers.ExpireTemporaryPassword(ctx, u.ID); err != nil {
				s.logger.Warn("break glass: sweep expire", "id", u.ID, "err", err)
				continue
			}
		}
		if u.Active {
			if err := s.bgUsers.Deactivate(ctx, u.ID); err != nil {
				s.logger.Warn("break glass: sweep deactivate", "id", u.ID, "err", err)
				continue
			}
		}
		swept++
		s.logger.Info("break glass: swept expired credential", "email", u.Email)
	}
	return swept
}

// StartBreakGlassSweep runs the hygiene sweep on its cadence until ctx is
// cancelled. It performs an immediate first sweep (so a restart promptly
// retires leftovers), then ticks.
func (s *Server) StartBreakGlassSweep(ctx context.Context) {
	s.SweepExpiredBreakGlass(ctx)
	ticker := time.NewTicker(breakGlassSweepCadence)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.SweepExpiredBreakGlass(ctx)
		}
	}
}

// SetBreakGlass attaches the break-glass machinery (F-28.3, Slice 3):
// the coiner (production: NewBreakGlassCoiner; tests: doubles), the user
// STORE the hygiene sweep operates on, and the ROOT-CERTIFICATE pool the
// peer-chain check trusts (nil when TLS is not configured — the route then
// denies uniformly). Leaving bg nil denies every request.
func (s *Server) SetBreakGlass(bg breakGlassCoiner, users UserStore, roots *x509.CertPool) {
	s.bgc = bg
	s.bgUsers = users
	s.bgRoots = roots
}

// SetBreakGlassAuditor attaches the audit recorder for break-glass events.
// A nil recorder skips cleanly (unit tests without an audit sink).
func (s *Server) SetBreakGlassAuditor(rec *audit.Recorder) {
	s.bgAuditor = rec
}

// handleEmergencyBreakGlass serves POST /emergency/break-glass. Admission
// ladder (uniform 403 at every rung — no state is advertised):
//
//  1. Method must be POST.
//  2. The hatch must be enabled (toggle OFF → same 403 as no cert).
//  3. The peer must present a VALID CA-signed client certificate chain
//     (verifyPeerChain). Plaintext listeners negotiate no peer cert → 403.
//  4. The live-pool cap must permit coinage (429 when saturated).
//
// Success: 200 with {email, password, expires_at}; the password plaintext is
// shown exactly once (Cache-Control: no-store) and is NEVER logged or
// audited. Every invocation (success or saturation) writes an F-15 audit
// event attributing the caller's certificate leaf, plus a conspicuous WARN
// log.
func (s *Server) handleEmergencyBreakGlass(w http.ResponseWriter, r *http.Request) {
	deny := func() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(breakGlassDenyBody))
	}
	if r.Method != http.MethodPost {
		deny()
		return
	}
	if !s.cfg.BreakGlassEnabled {
		deny()
		return
	}
	if s.bgc == nil {
		deny()
		return
	}
	leaf, err := s.verifyPeerChain(r)
	if err != nil {
		s.logger.Warn("break glass: denied (no valid client certificate)")
		deny()
		return
	}
	actor := breakGlassActorLeaf(leaf)

	// Optional ttl_seconds in the JSON body; clamp to the configured band.
	var body struct {
		TTLSeconds int64 `json:"ttl_seconds"`
	}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<10)).Decode(&body); err != nil {
			// Malformed bodies are tolerated: the DEFAULT ttl applies.
			body.TTLSeconds = 0
		}
	}
	// The request's ttl_seconds OVERRIDES the default when present; the
	// result is then clamped DOWN to the configured ceiling (F-28.3).
	ttl := s.cfg.EffectiveBreakGlassDefaultTTL()
	if body.TTLSeconds > 0 {
		ttl = time.Duration(body.TTLSeconds) * time.Second
	}
	if max := s.cfg.EffectiveBreakGlassMaxTTL(); ttl > max {
		ttl = max
	}

	resp, err := s.bgc.coin(r.Context(), ttl)
	if err != nil {
		var capErr ErrBreakGlassCapReached
		if errors.As(err, &capErr) {
			s.warnBreakGlass(actor, "saturation", "cap="+strconv.Itoa(capErr.Cap))
			s.recordBreakGlass(actor, "saturation", "", "")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprintf(w, `{"error":"too many live break-glass administrators (%d); wait for existing credentials to expire"}`, capErr.Cap)
			return
		}
		s.logger.Error("break glass: coinage failed", "err", err)
		s.recordBreakGlass(actor, "failure", "", "")
		deny()
		return
	}
	s.warnBreakGlass(actor, "coin", "email="+maskEmail(resp.Email)+" ttl="+strconv.Itoa(int(ttl/time.Second)))
	s.recordBreakGlass(actor, "success", maskEmail(resp.Email), resp.ExpiresAt)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	payload, _ := json.Marshal(resp)
	_, _ = w.Write(payload)
}

// warnBreakGlass is the CONSPICUOUS operational log line for every admitted
// invocation (WARN severity: this is a break-the-glass moment). The password
// NEVER appears here.
func (s *Server) warnBreakGlass(actor, phase, detail string) {
	s.logger.Warn("BREAK GLASS INVOCATION", "phase", phase, "actor_cert", actor, "detail", detail)
}

// recordBreakGlass writes the F-15 audit event: actor = the caller's
// certificate leaf, target = the coined user (masked email), outcome. The
// password appears nowhere.
func (s *Server) recordBreakGlass(actor, outcome, maskedEmail, expiresAt string) {
	if s.bgAuditor == nil {
		return
	}
	ev := audit.Event{
		Actor:      actor,
		ActorKind:  audit.ActorKindSystem,
		Action:     audit.ActionBreakGlassCoin,
		TargetKind: audit.TargetUser,
		TargetID:   maskedEmail,
		Outcome:    audit.OutcomeSuccess,
	}
	if outcome == "saturation" {
		ev.Outcome = audit.OutcomeFailure
	}
	if expiresAt != "" {
		ev.Details = jsonString(map[string]any{"expires_at": expiresAt, "phase": outcome})
	} else {
		ev.Details = jsonString(map[string]any{"phase": outcome})
	}
	s.bgAuditor.Record(ev)
}

// verifyPeerChain validates the TLS peer certificate CHAIN against the
// listener's roots (r.TLS.RootCAs — the ClientCAs pool TLSConfig populates
// from the CA file). It returns the verified LEAF on success. Requests
// arriving without a verified peer (plaintext listener, declined cert, or an
// untrusted chain) fail.
// verifyPeerChain validates the TLS peer certificate CHAIN against the
// trusted root pool (s.bgRoots — the CA bundle, set by SetBreakGlass). It
// returns the verified LEAF on success. Requests arriving without a verified
// peer (plaintext listener, declined cert, or an untrusted chain) fail.
func (s *Server) verifyPeerChain(r *http.Request) (*x509.Certificate, error) {
	if s.bgRoots == nil {
		return nil, fmt.Errorf("no trusted root pool (TLS not configured?)")
	}
	if r.TLS == nil {
		return nil, fmt.Errorf("no TLS state (listener is plaintext?)")
	}
	if len(r.TLS.PeerCertificates) == 0 {
		return nil, fmt.Errorf("no client certificate presented")
	}
	opts := x509.VerifyOptions{
		Roots:     s.bgRoots,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}
	if _, err := r.TLS.PeerCertificates[0].Verify(opts); err != nil {
		return nil, fmt.Errorf("verify peer chain: %w", err)
	}
	return r.TLS.PeerCertificates[0], nil
}

// breakGlassActorLeaf renders the VERIFIED leaf as the audit actor:
// `break-glass(cn=<CommonName>|san=<DNS-names>|subj=<DER-prefix>)`.
func breakGlassActorLeaf(leaf *x509.Certificate) string {
	var parts []string
	if cn := strings.TrimSpace(leaf.Subject.CommonName); cn != "" {
		parts = append(parts, "cn="+cn)
	}
	if dns := leaf.DNSNames; len(dns) > 0 {
		parts = append(parts, "san="+dns[0])
	}
	if ip := leaf.IPAddresses; len(ip) > 0 {
		parts = append(parts, "ip="+ip[0].String())
	}
	if len(parts) == 0 {
		sum := sha256.Sum256(leaf.RawSubject)
		parts = append(parts, "subj="+hex.EncodeToString(sum[:6]))
	}
	return "break-glass(" + strings.Join(parts, "|") + ")"
}

// maskEmail obscures the local-part of an email for LOG/AUDIT contexts
// (keeps the domain so incidents correlate; the coined @invalid domain is
// uninformative anyway).
func maskEmail(email string) string {
	at := strings.LastIndexByte(email, '@')
	if at <= 0 {
		return "***"
	}
	local := email[:at]
	if len(local) <= 4 {
		return "****" + email[at:]
	}
	return local[:2] + "…" + local[len(local)-2:] + email[at:]
}

// drawRandom draws n characters from charset using crypto/rand.
func drawRandom(charset string, n int) (string, error) {
	buf := make([]byte, n)
	for i := range buf {
		idx, err := randInt(big.NewInt(int64(len(charset))))
		if err != nil {
			return "", err
		}
		buf[i] = charset[idx.Int64()]
	}
	return string(buf), nil
}

// randInt returns a cryptographically secure integer in [0, mod).
func randInt(mod *big.Int) (*big.Int, error) {
	x, err := rand.Int(rand.Reader, mod)
	if err != nil {
		return nil, err
	}
	return x, nil
}

// jsonString renders a compact JSON document (audit payloads).
func jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
