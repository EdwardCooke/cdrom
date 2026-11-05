// SCIM caller authentication (F-28.2, Slice 3): the pool validator the
// scimGuard invokes on every request under the SCIM base path.
//
// The validator resolves a presented `Bearer <key>` against the database-held
// SCIM key pool WITHOUT the presented key ever travelling to the Database
// service: it pulls the ENCODING BUNDLE (the non-revoked rows' Argon2ID
// encodings + expiry timestamps, at most five by the pool cap) and verifies
// the key against EACH encoding locally, walking every row regardless of the
// outcome (uniform CPU profile — no oracle on key existence or shape), and
// compares tags in constant time (VerifySecret). A presentation is accepted
// iff some encoding verifies AND that row's expiry (optional) has not lapsed.
//
// Hot-path memo (spec): verdicts are cached ~1 s keyed by the presented key —
// POSITIVES and NEGATIVES alike (negative caching blunts brute-force
// probing) — and the bundle is refreshed on the same cadence. A warm request
// is a map lookup; a cold one pays one bundle pull + the per-row walk
// (≤5 × one Argon2ID, ≈7 ms each). Worst-case staleness after a revoke is
// therefore ~1 s.
//
// Recording (spec): a FAILURE produces a warning log entry (timestamp,
// remote address, path — never the presented key) and an audit event
// attributed to the unauthenticated SCIM caller. A SUCCESS marks the matched
// key's last_used_at (throttled, best-effort). Downstream SCIM activity is
// attributed to "SCIM connector (#<id>)" by the guard attaching the matched
// key's identity to the request context.
package idp

import (
	"context"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"cdrom/internal/audit"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

const (
	// scimDefaultMemoTTL is the FALLBACK hot-path memo cadence when the
	// operator hasn't configured one (config idp.scim.memo_ttl /
	// CDROM_SCIM_MEMO_TTL). It governs both halves of the guard's freshness:
	// how long a validation VERDICT (positive or negative) stays hot in the
	// memo (negatives blunt probing; positives absorb the connector's steady
	// traffic), and the minimum spacing between encoding-BUNDLE pulls — the
	// two are deliberately coupled, so revoking or expiring a key loses
	// effect after at most one interval. ~1 s per the spec.
	scimDefaultMemoTTL = time.Second

	// scimMarkUsedThrottle suppresses a last_used_at bump closer than this to
	// the previous one (the Database service enforces it; a suppressed bump
	// is a quiet no-op). Five minutes keeps the column informative without
	// churning writes on a busy channel.
	scimMarkUsedThrottle = 5 * time.Minute

	// scimMemoBound caps the verdict memo's resident entries; on excess the
	// expired entries are purged (then, failing that, the oldest half).
	// Probing with millions of DISTINCT keys must not grow the map forever.
	scimMemoBound = 4096

	// scimAuditorIdentity is the ACTOR recorded for a failed SCIM auth
	// attempt: the caller is BY DEFINITION unauthenticated (there is no key
	// identity to attribute), so the event names the channel, not a person.
	scimAuditorIdentity = "scim-connector(unauthenticated)"

	// Failure reason categories carried in the warning log + audit event.
	// Deliberately COARSE: the 401 response reveals none of this to the
	// caller (unknown, revoked, and expired all collapse to the identical
	// body); the distinction exists for observability only.
	scimFailReasonMissing  = "missing"  // no Authorization header / not Bearer / empty token
	scimFailReasonRejected = "rejected" // walked the pool; nothing verified (or the match was expired)
)

// scimAuditor records audit events. *audit.Recorder satisfies it (a nil
// recorder skips cleanly); tests supply a capturing fake. Kept narrow so the
// validator depends on the event TYPE, not the recorder's machinery.
type scimAuditor interface {
	Record(event audit.Event)
}

// scimAuthorizer is the caller-authentication capability the scimGuard
// demands of whatever is attached: resolve a presented key against the pool
// and record failures. *scimValidator is the production implementation;
// tests attach fakes.
type scimAuthorizer interface {
	ValidatePresentation(ctx context.Context, presented string) (*scimMatch, bool)
	RecordFailure(reason, remoteAddr, path string)
}

// scimCandidate is one row of the encoding bundle: the validation material
// for a single non-revoked key. ExpiresAt is nil when the key has no
// expiration (NULL column — remember: nil means "none", never year-1).
type scimCandidate struct {
	id        string
	encoding  string
	expiresAt *time.Time
}

// scimMatch describes an accepted presentation: the matched candidate plus
// the downstream attribution identity ("SCIM connector (#<id>)").
type scimMatch struct {
	candidate  *scimCandidate
	attributed string
}

// scimPoolBackend is the sliver of the Database service the validator uses
// (both directions of the pool handshake). Production passes a
// dbScimPoolBackend (wrapping the full gRPC client); tests pass a fake.
// Narrowing the dependency keeps the validator's trust surface minimal.
type scimPoolBackend interface {
	BundleScimKeys(ctx context.Context, in *dbpb.BundleScimKeysRequest) (*dbpb.BundleScimKeysResponse, error)
	MarkScimKeyUsed(ctx context.Context, in *dbpb.MarkScimKeyUsedRequest) (*emptypb.Empty, error)
}

// dbScimPoolBackend adapts the generated gRPC DatabaseClient (whose methods
// carry variadic grpc.CallOptions) onto scimPoolBackend.
type dbScimPoolBackend struct {
	db dbpb.DatabaseClient
}

// NewDBScimPoolBackend wraps a Database service client for the SCIM
// validator (production wiring).
func NewDBScimPoolBackend(db dbpb.DatabaseClient) scimPoolBackend {
	return &dbScimPoolBackend{db: db}
}

func (b *dbScimPoolBackend) BundleScimKeys(ctx context.Context, in *dbpb.BundleScimKeysRequest) (*dbpb.BundleScimKeysResponse, error) {
	return b.db.BundleScimKeys(ctx, in)
}

func (b *dbScimPoolBackend) MarkScimKeyUsed(ctx context.Context, in *dbpb.MarkScimKeyUsedRequest) (*emptypb.Empty, error) {
	return b.db.MarkScimKeyUsed(ctx, in)
}

// scimValidator validates presented SCIM keys against the pool. Safe for
// concurrent use. A nil backend denies everything (the guard treats an
// unwired validator as unreachable — deny by default).
type scimValidator struct {
	backend scimPoolBackend
	pepper  string
	auditor scimAuditor
	logger  *slog.Logger
	// memoTTL is the configured hot-path memo cadence (normalized: <=0 ->
	// scimDefaultMemoTTL). See the constant's doc for what it controls.
	memoTTL time.Duration

	mu       sync.Mutex
	bundle   []*scimCandidate
	bundleAt time.Time
	verdicts map[string]*scimVerdict
}

// scimVerdict is one memo entry: the cached outcome for a presented key.
// Accepted verdicts carry the matched candidate (warm hits skip the walk).
type scimVerdict struct {
	accepted bool
	cand     *scimCandidate
	at       time.Time
}

// NewScimValidator builds the pool validator. backend is the Database
// service client (bundle pulls + mark-used); pepper is the IdP's SCIM-key
// pepper (the ONLY pepper-touching site besides derivation — evaluation
// runs argon2id(pepper ‖ key) against each stored encoding); auditor records
// failed attempts (nil = no audit events); logger is nil-safe; memoTTL is
// the hot-path memo cadence (config idp.scim.memo_ttl /
// CDROM_SCIM_MEMO_TTL; <=0 falls back to the 1-second default).
func NewScimValidator(backend scimPoolBackend, pepper string, auditor scimAuditor, logger *slog.Logger, memoTTL time.Duration) *scimValidator {
	if logger == nil {
		logger = slog.Default()
	}
	if memoTTL <= 0 {
		memoTTL = scimDefaultMemoTTL
	}
	return &scimValidator{
		backend:  backend,
		pepper:   pepper,
		auditor:  auditor,
		logger:   logger,
		memoTTL:  memoTTL,
		verdicts: make(map[string]*scimVerdict),
	}
}

// ValidatePresentation resolves presented against the pool. It returns the
// match (attributable identity + candidate) when accepted, or (nil,false)
// when rejected — unknown, revoked (absent from the bundle), and expired
// presentations are INDISTINGUISHABLE to the caller. On acceptance it fires
// the throttled last_used_at bump (best-effort, OUTSIDE the memo lock so a
// slow database never serializes the request path). The presented key is
// never logged or audited here.
func (v *scimValidator) ValidatePresentation(ctx context.Context, presented string) (*scimMatch, bool) {
	now := time.Now()

	v.mu.Lock()
	var ent *scimVerdict
	if e, ok := v.verdicts[presented]; ok && now.Sub(e.at) < v.memoTTL {
		ent = e // warm hit: the memo decides, no walk
	} else {
		// Cold path: refresh the bundle if due (falling back to the STALE
		// bundle on a pull failure — a transient database hiccup must not
		// 401 a healthy connector; the memo's cadence retries promptly).
		if v.bundleAt.IsZero() || now.Sub(v.bundleAt) >= v.memoTTL {
			if b, err := v.pullBundle(detachedContext(ctx)); err != nil {
				v.logger.Warn("scim: pulling the key bundle failed; evaluating against the stale bundle", "err", err)
			} else {
				v.bundle, v.bundleAt = b, now
			}
		}
		verifyRow := func(encoding string) bool {
			return VerifySecret(encoding, v.pepper, presented)
		}
		idx, matchedExpiry := walkBundle(v.bundle, verifyRow, now)
		accepted := idx >= 0 && !matchedExpiry
		var cand *scimCandidate
		if accepted {
			cand = v.bundle[idx]
		}
		ent = &scimVerdict{accepted: accepted, cand: cand, at: now}
		v.storeVerdict(presented, ent)
	}
	v.mu.Unlock()

	if !ent.accepted {
		return nil, false
	}
	v.markUsed(ent.cand)
	return &scimMatch{candidate: ent.cand, attributed: scimAttribution(ent.cand)}, true
}

// walkBundle verifies presented against EVERY bundled encoding (NO EARLY
// EXIT — the CPU profile is independent of which row, if any, matches) and
// reports the matching row's index (-1 when none) together with whether THAT
// row's expiry has lapsed. Expiration is checked only on the matched row:
// an expired-but-verifiable key is rejected identically to an unknown one.
// verify is the per-row predicate (production: VerifySecret with the
// configured pepper); injecting it lets tests COUNT the evaluations.
func walkBundle(bundle []*scimCandidate, verify func(encoding string) bool, now time.Time) (idx int, matchedExpired bool) {
	idx = -1
	for i, cand := range bundle {
		if verify(cand.encoding) {
			idx = i
		}
	}
	if idx < 0 {
		return -1, false
	}
	exp := bundle[idx].expiresAt
	matchedExpired = exp != nil && !exp.After(now)
	return idx, matchedExpired
}

// storeVerdict records a verdict, evicting expired entries (then the oldest
// half) when the memo exceeds scimMemoBound. Callers hold v.mu.
func (v *scimValidator) storeVerdict(presented string, ent *scimVerdict) {
	v.verdicts[presented] = ent
	if len(v.verdicts) <= scimMemoBound {
		return
	}
	now := time.Now()
	for k, e := range v.verdicts {
		if now.Sub(e.at) >= v.memoTTL {
			delete(v.verdicts, k)
		}
	}
	if len(v.verdicts) <= scimMemoBound {
		return
	}
	// Still over budget (everything is young): shed the oldest half.
	type kv struct {
		k string
		a time.Time
	}
	all := make([]kv, 0, len(v.verdicts))
	for k, e := range v.verdicts {
		all = append(all, kv{k, e.at})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].a.Before(all[j].a) })
	for _, e := range all[:len(all)/2] {
		delete(v.verdicts, e.k)
	}
}

// pullBundle fetches the encoding bundle from the Database service and
// normalizes it: the non-revoked rows (the service governs candidacy),
// sorted by id so the walk ORDER is deterministic across replicas.
func (v *scimValidator) pullBundle(ctx context.Context) ([]*scimCandidate, error) {
	resp, err := v.backend.BundleScimKeys(ctx, &dbpb.BundleScimKeysRequest{})
	if err != nil {
		return nil, err
	}
	keys := resp.GetKeys()
	out := make([]*scimCandidate, 0, len(keys))
	for _, k := range keys {
		var exp *time.Time
		if ts := k.GetExpiresAt(); ts != nil {
			t := ts.AsTime()
			exp = &t
		}
		out = append(out, &scimCandidate{id: k.GetId(), encoding: k.GetHash(), expiresAt: exp})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out, nil
}

// markUsed bumps the matched key's last_used_at (throttled at the Database
// service). Best-effort: a failure is a debug note, never a denial. It runs
// on a DETACHED context (the request may already be gone) with a tight
// deadline so a wedged database cannot stall the response.
func (v *scimValidator) markUsed(cand *scimCandidate) {
	if v.backend == nil || cand == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := v.backend.MarkScimKeyUsed(ctx, &dbpb.MarkScimKeyUsedRequest{
		Id:       cand.id,
		Throttle: durationpb.New(scimMarkUsedThrottle),
	}); err != nil {
		v.logger.Debug("scim: marking key used", "id", cand.id, "err", err)
	}
}

// RecordFailure records a rejected SCIM presentation: a warning log entry
// (remote + path + reason category — NEVER the presented key) and an audit
// event attributed to the unauthenticated SCIM caller.
func (v *scimValidator) RecordFailure(reason, remoteAddr, path string) {
	v.logger.Warn("scim: unauthorized caller", "reason", reason, "remote", remoteAddr, "path", path)
	if v.auditor == nil {
		return
	}
	ip := remoteAddr
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		ip = host
	}
	v.auditor.Record(audit.Event{
		Actor:      scimAuditorIdentity,
		ActorKind:  audit.ActorKindSystem,
		Action:     audit.ActionScimAuthFail,
		TargetKind: audit.TargetScimKey,
		Outcome:    audit.OutcomeFailure,
		SourceIP:   ip,
		Details:    map[string]any{"reason": reason, "path": path},
	})
}

// scimAttribution renders the downstream identity for activity authorized by
// the matched key: "SCIM connector (#<id>)".
func scimAttribution(cand *scimCandidate) string {
	return "SCIM connector (#" + cand.id + ")"
}

// detachedContext severs the lifetime of background work (bundle pulls,
// mark-used bumps) from the request's cancellation while PRESERVing values
// (logging context, deadlines propagate naturally).
func detachedContext(parent context.Context) context.Context {
	if parent == nil {
		return context.Background()
	}
	return context.WithoutCancel(parent)
}

// Request-context attribution of the presenting SCIM key (consumed by the
// F-28.4/F-28.5 resource handlers' audit actors).
type scimCallerContextKey struct{}

// withSCIMCaller extends parent, attributing SCIM activity to the given
// identity ("SCIM connector (#<id>)").
func withSCIMCaller(parent context.Context, identity string) context.Context {
	return context.WithValue(parent, scimCallerContextKey{}, identity)
}

// SCIMCallerFromContext returns the presenting key's attribution identity,
// or "" when the request did not pass the SCIM guard.
func SCIMCallerFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(scimCallerContextKey{}).(string)
	return v
}
