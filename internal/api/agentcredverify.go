// Package api — agent-credential verification (F-30.2 Slice 3, hardened by
// F-30.3 Slice 1).
//
// The self-service ARM of the renew endpoint: an agent renews its OWN
// credential by presenting it as the request's Bearer token. Verification is
// handler-local (the WEBHOOK-OIDC precedent: a verb-specific alternate
// credential, not a global middleware branch): the JWT's signature is checked
// against the IdP's JWKS (the shared token-verifier substrate, F-30.3 S1),
// and its liveness against the credential ROW (F-30.2) through the Database
// service. The presented credential must bind to BOTH the path's agent name
// AND the path's credential id — cross-name and cross-id renewals are
// impossible by construction. Human callers (OIDC JWTs) fail the
// agent-credential check and fall through to the RBAC arm (agents.can-manage).
//
// LIVINESS CACHE (F-30.3): the row lookup is the expensive hop (a gRPC call
// to the Database service on every presentation). Positive results are
// cached for a SHORT TTL (bounded staleness: a revoked/retired/expired row is
// respected within the TTL); NEGATIVE results are NEVER cached (a flipped row
// must be reconsidered immediately). Steady state therefore costs one local
// signature check + one map read per presentation. The cache remembers the
// row's BOUND NAME alongside the verdict, so a hit is only honored when the
// presenting token's subject agrees — the binding check is never weakened by
// the shortcut.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	oidc "github.com/coreos/go-oidc/v3/oidc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// livelinessPosTTL bounds how long a VERIFIED-ACTIVE row is remembered. Kept
// short on purpose: revocation is the emergency path and its worst-case delay
// is this TTL (plus, on the target legs, one heartbeat interval).
const livelinessPosTTL = 15 * time.Second

// livelinessPosCeiling is the HARD upper bound on a positive cache entry's
// age (F-30.3/F-30.4): each successful lookup SLIDES the entry by the TTL
// (cheap freshness for a healthy row), but the slide is CEILINGED so a
// prolonged Database busy-storm (lookups erroring, the last positive
// retained) cannot freeze a retired/revoked credential's shield indefinitely
// — the entry dies at the ceiling no matter how recently it slid. The
// ceiling equals the spec's "within one heartbeat + CACHE TTL" bound (30 s),
// so worst-case teardown stays at cache TTL + one heartbeat.
const livelinessPosCeiling = 30 * time.Second

// lastSeenThrottle bounds how OFTEN a verified presentation bumps the row's
// last_seen_at (a chatty agent would otherwise hammer the Database service on
// every RPC). The UI's "seen recently" indicator tolerates this easily.
const lastSeenThrottle = 60 * time.Second

// AgentCredentialVerifier verifies an agent credential presented on the
// renew endpoint (today) and, from F-30.3 onward, on the target-surface gRPC
// verbs. It combines cryptographic verification (the IdP's JWKS — the
// signature proves the IdP issued it) with row-liveness (the Database
// service — the row decides life: not revoked, not retired, not expired).
// The JWT's exp claim is a generous PLACEHOLDER and is skipped (mirror the
// job-token policy): the row's deadline is the law.
type AgentCredentialVerifier struct {
	verifier  *oidc.IDTokenVerifier
	audiences []string
	db        dbpb.DatabaseClient
	logger    *slog.Logger

	// clock is injectable so tests can MEASURE cache TTL behavior
	// deterministically (advance the clock past the TTL, observe a refetch).
	clock func() time.Time

	cacheMu sync.Mutex
	pos     map[string]posEntry // jti -> cached ACTIVE verdict + bound name
}

// posEntry is a cached positive liveness verdict. name is the row's bound
// agent name: a cache HIT is honored only when the presenting token's
// subject matches it, so the binding check survives the shortcut.
type posEntry struct {
	name string
	exp  time.Time // sliding expiry (hit-granted)
	ceil time.Time // hard ceiling (birth-granted; the slide's wall)
	born time.Time // first population (memoization stats)
}

// NewAgentCredentialVerifier builds the verifier: OIDC discovery against the
// IdP's HTTP surface (a network call), then a JWKS-backed verifier skipping
// the client-id and expiry checks (agent credentials are not issued to a
// client, and their exp is a placeholder). audiences are the values accepted
// on the aud claim (the API's configured audiences, as for job tokens). db is
// consulted for the row's liveness (cached positively, briefly). client is
// the HTTP client used for discovery (an mTLS client when the IdP serves
// TLS); a nil client uses the default.
func NewAgentCredentialVerifier(ctx context.Context, cfg config.GRPCAuthConfig, client *http.Client, db dbpb.DatabaseClient, logger *slog.Logger) (*AgentCredentialVerifier, error) {
	if logger == nil {
		logger = slog.Default()
	}
	base := normalizeIDPURL(cfg.IdPAddress)
	verifier, err := newOIDCTokenVerifier(ctx, base, client)
	if err != nil {
		return nil, err
	}
	return &AgentCredentialVerifier{
		verifier:  verifier,
		audiences: cfg.Audiences,
		db:        db,
		logger:    logger,
		clock:     time.Now,
		pos:       make(map[string]posEntry),
	}, nil
}

// Enabled reports whether the verifier is usable.
func (v *AgentCredentialVerifier) Enabled() bool { return v != nil && v.verifier != nil }

// setClock overrides the wall clock (tests measuring cache TTL).
func (v *AgentCredentialVerifier) setClock(fn func() time.Time) { v.clock = fn }

// Verify checks the presented token: signature (JWKS), audience, and row
// liveness (positively cached, briefly). It returns the credential's jti and
// bound agent name on success. Any failure (forgery, wrong audience,
// unknown/revoked/retired/expired row) is an error — the caller judges it on
// its merits (the renew handler's 401; the target-leg guard's
// UNAUTHENTICATED).
func (v *AgentCredentialVerifier) Verify(ctx context.Context, token string) (jti, agentName string, err error) {
	idt, err := v.verifier.Verify(ctx, token)
	if err != nil {
		return "", "", fmt.Errorf("api: verify agent credential: %w", err)
	}
	if !audienceAccepted(v.audiences, idt.Audience) {
		return "", "", fmt.Errorf("api: agent credential audience %v not accepted", idt.Audience)
	}
	sub := idt.Subject // "agent:<name>"
	if len(sub) < len("agent:")+1 || sub[:6] != "agent:" {
		return "", "", fmt.Errorf("api: agent credential subject %q is not an agent principal", sub)
	}
	agentName = sub[len("agent:"):]
	var custom struct {
		JTI string `json:"jti"`
	}
	if err := idt.Claims(&custom); err != nil {
		return "", "", fmt.Errorf("api: read agent credential claims: %w", err)
	}
	jti = custom.JTI
	if jti == "" {
		return "", "", fmt.Errorf("api: agent credential has no jti")
	}
	if ok, err := v.active(ctx, jti, agentName); err != nil {
		return "", "", err
	} else if !ok {
		return "", "", fmt.Errorf("api: agent credential %s is not active (revoked, retired, expired, or unbound)", jti)
	}
	return jti, agentName, nil
}

// active answers "is row jti active RIGHT NOW, bound to name?". Consults the
// positive cache first (honored only on a name match); negatives are computed
// fresh every time (UNCACHED — a flipped row is reconsidered immediately).
// The error is nonzero only for LOOKUP troubles (distinct from a definitive
// inactive verdict); callers deny conservatively on both.
func (v *AgentCredentialVerifier) active(ctx context.Context, jti, name string) (bool, error) {
	now := v.clock()
	v.cacheMu.Lock()
	if e, ok := v.pos[jti]; ok && now.Before(e.ceil) && e.name == name {
		v.cacheMu.Unlock()
		return true, nil
	}
	v.cacheMu.Unlock()

	row, err := v.fetchRow(ctx, jti)
	if err != nil {
		return false, fmt.Errorf("api: agent credential row lookup: %w", err)
	}
	if row.GetAgentName() != name {
		return false, nil
	}
	revoked := row.GetRevokedAt() != nil
	retired := row.GetReplacedById() != ""
	expiresTs := row.GetExpiresAt()
	expired := expiresTs != nil && expiresTs.AsTime().Before(now.UTC())
	if revoked || retired || expired {
		v.cacheMu.Lock()
		delete(v.pos, jti) // a fresh negative clears any stale positive
		v.cacheMu.Unlock()
		return false, nil
	}
	v.markSeenThrottled(ctx, jti, row.GetLastSeenAt())
	entry := posEntry{name: name, exp: now.Add(livelinessPosTTL)}
	v.cacheMu.Lock()
	if prev, ok := v.pos[jti]; ok {
		// Slide the MEMO, ceiling the LIFE: a healthy row keeps its entry
		// fresh (short-lived memos), but the entry can never outlive the
		// ceiling from its birth — a DB busy-storm (successful lookups
		// interleaved with errors) cannot freeze a retired credential's
		// shield past livelinessPosCeiling.
		entry.born = prev.born
		entry.ceil = prev.born.Add(livelinessPosCeiling)
		if now.After(entry.ceil) {
			entry.ceil = now.Add(livelinessPosCeiling)
		}
	} else {
		entry.born = now
		entry.ceil = now.Add(livelinessPosCeiling)
	}
	v.pos[jti] = entry
	// Opportunistic sweep: drop entries past their HARD ceiling (tiny map;
	// avoids unbounded growth across a fleet's worth of rotated credentials).
	for k, e := range v.pos {
		if !now.Before(e.ceil) {
			delete(v.pos, k)
		}
	}
	v.cacheMu.Unlock()
	return true, nil
}

// fetchRow pulls the row from the Database service. Transport errors are
// propagated (the CALLER decides the posture: the renew handler treats a
// lookup failure as "not self-serviced" → 401; F-30.3's target-leg guard
// will distinguish transient vs definitive).
func (v *AgentCredentialVerifier) fetchRow(ctx context.Context, jti string) (*dbpb.AgentCredential, error) {
	return v.db.GetAgentCredential(ctx, &dbpb.GetAgentCredentialRequest{Id: jti})
}

// markSeenThrottled bumps last_seen_at at MOST once per throttle window.
// Best-effort: a failure here NEVER affects the verdict (liveness is the
// row's state, not its bookkeeping).
func (v *AgentCredentialVerifier) markSeenThrottled(ctx context.Context, jti string, lastSeen *timestamppb.Timestamp) {
	var prev time.Time
	if lastSeen != nil {
		prev = lastSeen.AsTime()
	}
	if v.clock().Sub(prev) < lastSeenThrottle {
		return
	}
	if _, err := v.db.MarkAgentCredentialSeen(ctx, &dbpb.MarkAgentCredentialSeenRequest{Id: jti}); err != nil {
		v.logger.Debug("api: mark agent credential seen", "jti", jti, "err", err)
	}
}
