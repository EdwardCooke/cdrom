package api

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/config"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// freezeClock installs a MANUAL clock on the verifier (and aligns the fake DB
// so its LastSeenAt stamps use the same timeline) and returns an advance
// function. Deterministic TTL measurements (F-30.3 S1 demonstrable: "cache
// TTL behavior measured").
func freezeClock(t *testing.T, v *AgentCredentialVerifier, db *fakeCredDB) func(time.Duration) {
	t.Helper()
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	cur := now
	fn := func() time.Time { return cur }
	v.setClock(fn)
	db.mu.Lock()
	db.clk = fn
	db.mu.Unlock()
	return func(d time.Duration) { cur = cur.Add(d) }
}

// TestLivenessCachePositiveWithinTTL: three consecutive genuine verifications
// cost EXACTLY ONE row lookup (steady state = local signature + map read);
// advancing the clock past the TTL forces a refetch.
func TestLivenessCachePositiveWithinTTL(t *testing.T) {
	base, idpClient, _ := startTestIDPCredentials(t)
	ctx := context.Background()
	issued, err := idpClient.IssueAgentCredential(ctx, &idppb.IssueAgentCredentialRequest{
		AgentName: "cache-me", IssuedBy: "test",
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	row := issued.GetCredential()
	db := &fakeCredDB{row: row}
	v, err := NewAgentCredentialVerifier(ctx,
		config.GRPCAuthConfig{IdPAddress: base, Audiences: []string{"cdrom-api"}},
		nil, db, quietLog())
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	adv := freezeClock(t, v, db)

	jti, name, err := v.Verify(ctx, issued.GetJwt())
	if err != nil {
		t.Fatalf("Verify #1: %v", err)
	}
	if jti != row.GetId() || name != "cache-me" {
		t.Fatalf("Verify #1 = (%q,%q)", jti, name)
	}
	for i := 2; i <= 3; i++ {
		if _, _, err := v.Verify(ctx, issued.GetJwt()); err != nil {
			t.Fatalf("Verify #%d: %v", i, err)
		}
	}
	if got := db.getCount(); got != 1 {
		t.Fatalf("row lookups after 3 verifies = %d, want 1 (positive cache MISS)", got)
	}

	adv(31 * time.Second) // past the 30 s positive TTL
	if _, _, err := v.Verify(ctx, issued.GetJwt()); err != nil {
		t.Fatalf("Verify after TTL: %v", err)
	}
	if got := db.getCount(); got != 2 {
		t.Fatalf("row lookups after TTL advance = %d, want 2 (cache expired)", got)
	}
}

// TestNegativesAreUncached: a REVOKED row is recomputed on EVERY attempt
// (flipping it back to healthy is honored immediately — no sticky negative),
// and a stale POSITIVE is discarded the moment a refetch sees the row
// revoked.
func TestNegativesAreUncached(t *testing.T) {
	base, idpClient, _ := startTestIDPCredentials(t)
	ctx := context.Background()
	issued, err := idpClient.IssueAgentCredential(ctx, &idppb.IssueAgentCredentialRequest{
		AgentName: "flip-flop", IssuedBy: "test",
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	healthy := issued.GetCredential()
	revoked := &dbpb.AgentCredential{
		Id: healthy.GetId(), AgentName: healthy.GetAgentName(),
		ExpiresAt: healthy.ExpiresAt, RevokedAt: timestamppb.New(time.Now().UTC()),
	}

	// Healthy first: one lookup, cached positive.
	dbHealthy := &fakeCredDB{row: healthy}
	v, err := NewAgentCredentialVerifier(ctx,
		config.GRPCAuthConfig{IdPAddress: base, Audiences: []string{"cdrom-api"}},
		nil, dbHealthy, quietLog())
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	adv := freezeClock(t, v, dbHealthy)
	if _, _, err := v.Verify(ctx, issued.GetJwt()); err != nil {
		t.Fatalf("warm-up Verify: %v", err)
	}
	if got := dbHealthy.getCount(); got != 1 {
		t.Fatalf("warm-up lookups = %d, want 1", got)
	}

	// Expire the positive, THEN reveal the row is revoked: the refetch must
	// happen (stale positive defeated) and the verdict must be DENIED.
	adv(31 * time.Second)
	dbHealthy.setRow(revoked)
	if _, _, err := v.Verify(ctx, issued.GetJwt()); err == nil {
		t.Fatal("Verify after revocation succeeded, want denial")
	}
	if got := dbHealthy.getCount(); got != 2 {
		t.Fatalf("lookups after revocation = %d, want 2 (stale positive forced a refetch)", got)
	}

	// Fresh-negative stickiness: a revoked row is re-checked EVERY attempt —
	// flipping it BACK to healthy is honored immediately (no cached negative).
	dbRevoked := &fakeCredDB{row: revoked}
	v2, err := NewAgentCredentialVerifier(ctx,
		config.GRPCAuthConfig{IdPAddress: base, Audiences: []string{"cdrom-api"}},
		nil, dbRevoked, quietLog())
	if err != nil {
		t.Fatalf("verifier2: %v", err)
	}
	adv2 := freezeClock(t, v2, dbRevoked)
	for i := 1; i <= 3; i++ {
		if _, _, err := v2.Verify(ctx, issued.GetJwt()); err == nil {
			t.Fatalf("denial #%d unexpectedly succeeded", i)
		}
	}
	if got := dbRevoked.getCount(); got != 3 {
		t.Fatalf("lookups across 3 denials = %d, want 3 (NEGATIVES ARE UNCACHED)", got)
	}
	adv2(time.Millisecond)
	dbRevoked.setRow(healthy)
	if _, _, err := v2.Verify(ctx, issued.GetJwt()); err != nil {
		t.Fatalf("Verify after un-revocation: %v (sticky negative?)", err)
	}
}

// TestWarmCacheHonorsBoundName plants a POISONED positive entry (same jti,
// different bound name) and proves the name-guard defeats it: the verifier
// refetches and verifies against the TRUE row.
func TestWarmCacheHonorsBoundName(t *testing.T) {
	base, idpClient, _ := startTestIDPCredentials(t)
	ctx := context.Background()
	issued, err := idpClient.IssueAgentCredential(ctx, &idppb.IssueAgentCredentialRequest{
		AgentName: "impersonate", IssuedBy: "test",
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	row := issued.GetCredential()
	db := &fakeCredDB{row: row}
	v, err := NewAgentCredentialVerifier(ctx,
		config.GRPCAuthConfig{IdPAddress: base, Audiences: []string{"cdrom-api"}},
		nil, db, quietLog())
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	freezeClock(t, v, db)
	if _, _, err := v.Verify(ctx, issued.GetJwt()); err != nil {
		t.Fatalf("warm-up: %v", err)
	}
	// Poison: pretend someone else's name is cached for THIS jti.
	v.cacheMu.Lock()
	v.pos[row.GetId()] = posEntry{name: "evil-other", exp: v.clock().Add(time.Hour)}
	v.cacheMu.Unlock()
	if _, _, err := v.Verify(ctx, issued.GetJwt()); err != nil {
		t.Fatalf("Verify with poisoned cache: %v (name-guard failed to force refetch)", err)
	}
	if got := db.getCount(); got != 2 {
		t.Fatalf("lookups = %d, want 2 (poisoned entry defeated by the name guard)", got)
	}
}

// TestLastSeenThrottle: verified presentations bump last_seen_at at MOST once
// per throttle window — a chatty agent cannot hammer the Database service.
func TestLastSeenThrottle(t *testing.T) {
	base, idpClient, _ := startTestIDPCredentials(t)
	ctx := context.Background()
	issued, err := idpClient.IssueAgentCredential(ctx, &idppb.IssueAgentCredentialRequest{
		AgentName: "chatty", IssuedBy: "test",
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	row := issued.GetCredential()
	db := &fakeCredDB{row: row}
	v, err := NewAgentCredentialVerifier(ctx,
		config.GRPCAuthConfig{IdPAddress: base, Audiences: []string{"cdrom-api"}},
		nil, db, quietLog())
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	adv := freezeClock(t, v, db)

	// t0: first verify → mark #1.
	if _, _, err := v.Verify(ctx, issued.GetJwt()); err != nil {
		t.Fatalf("verify t0: %v", err)
	}
	if got := db.markCount(row.GetId()); got != 1 {
		t.Fatalf("marks at t0 = %d, want 1", got)
	}
	// t0+30s: cache MISSES (past the 30 s positive TTL) but the mark is
	// SUPPRESSED (inside the 60 s throttle window).
	adv(30 * time.Second)
	if _, _, err := v.Verify(ctx, issued.GetJwt()); err != nil {
		t.Fatalf("verify t0+30s: %v", err)
	}
	if got := db.markCount(row.GetId()); got != 1 {
		t.Fatalf("marks at t0+30s = %d, want 1 (THROTTLED)", got)
	}
	if got := db.getCount(); got != 2 {
		t.Fatalf("lookups at t0+30s = %d, want 2 (TTL expired, refetch occurred)", got)
	}
	// t0+90s: both the cache AND the throttle have elapsed → mark #2.
	adv(60 * time.Second)
	if _, _, err := v.Verify(ctx, issued.GetJwt()); err != nil {
		t.Fatalf("verify t0+90s: %v", err)
	}
	if got := db.markCount(row.GetId()); got != 2 {
		t.Fatalf("marks at t0+90s = %d, want 2", got)
	}
}
