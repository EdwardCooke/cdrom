package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/protobuf/types/known/durationpb"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// ---------------------------------------------------------------------------
// Periodic source-refresh loop (F-21.5.3)
// ---------------------------------------------------------------------------
//
// The API's leader-gated background loop that keeps externally-sourced
// pipelines current on their cadence. Exactly one API replica leads (a named
// lease on the Database service, like the scheduler's loops, F-23); the
// leader polls every srcRefreshPollInterval and syncs each source that is
// DUE at its EFFECTIVE cadence:
//
//   - the pipeline's file-declared periodicRefresh (spec.periodicRefresh)
//     WHEN the admin gate allows it (global config knob ∧ per-pipeline flag):
//     its schedule cron when set (PRECISE timing — e.g. after-hours syncs
//     when load is minimal), else its interval (due at last_attempt +
//     interval; an honored interval of 0 OPTS OUT of periodic polling);
//   - otherwise the SYSTEM DEFAULT cadence (pipeline_source.refresh_interval,
//     default 2 minutes) — denial restores the safe default, it never halts
//     polling.
//
// Pacing is from the last ATTEMPT (not the last success): a failing source
// is retried on its own cadence — not hammered every tick — and last_error
// keeps it diagnosable. Missed fires (a leadership gap or outage) collapse
// into a single catch-up sync on recovery; there is no backlog replay
// (definitions converge, and refresh-on-run already guards kickoff
// freshness). A sync is serialized per pipeline (locally by the sync lock,
// CROSS-REPLICA by a per-pipeline lease), so two replicas can never
// double-apply a change.
//
// SCALE RULES (hundreds/thousands of sources):
//
//   - SKIP-IF-IN-FLIGHT: a due source that is ALREADY syncing (its previous
//     sync has not finished) is SKIPPED for this fire — the next due instant
//     (one interval later) is tried again, unless it is still syncing.
//     Overshoot therefore NEVER accumulates: a 3m sync on a 2m interval
//     yields a steady ~4m rhythm (skip, run, skip, run…), with no queued
//     second sync and no parked goroutines.
//   - PER-SYNC TIMEOUT: every sync runs under a bounded context
//     (pipeline_source.sync_timeout, default 5m), so a blackholed origin
//     cannot hold a source in flight forever — zombies are impossible by
//     construction.
//   - THROTTLABLE CONCURRENCY: pipeline_source.max_parallel (default 0 =
//     UNLIMITED — goroutines are cheap and the fetcher self-regulates via
//     its per-repo locks) caps simultaneous in-flight syncs; a due source
//     that cannot get a slot is DEFERRED to the next tick (delayed, never
//     coupled or starved).
//   - ANTI-PHASE JITTER: interval-mode due instants are offset by a stable
//     per-source fraction (centered, ±⅛ of the interval) so fleets of
//     identical intervals do not march in lockstep (decorrelating the
//     mass-due pulses); cron mode is left exact (precision is its purpose).
//   - METRICS: cdrom_api_source_syncs_total{outcome},
//     cdrom_api_source_sync_duration_seconds{outcome},
//     cdrom_api_source_sync_in_flight, cdrom_api_leader_state (F-20).

const (
	// srcRefreshLeaseName is the named lease guarding the loop (F-23).
	srcRefreshLeaseName = "api-src-refresh"
	// srcRefreshLeaseTTL is the lease's backstop validity from its last
	// renewal (mirrors the scheduler's leader-election tuning).
	srcRefreshLeaseTTL = 5 * time.Minute
	// srcRefreshLeaseHeartbeatTTL is the liveness window: a peer may take
	// over once the holder's last heartbeat is older than this.
	srcRefreshLeaseHeartbeatTTL = 30 * time.Second
	// srcRefreshPollInterval is how often the leader walks the sources. It is
	// well under the smallest sensible cadence (seconds-scale vs. minutes),
	// so a due source is picked up promptly; it is a variable so tests can
	// shorten it.
	srcRefreshPollInterval = 10 * time.Second
	// srcSyncLeaseTTL bounds the per-pipeline cross-replica sync lease: a
	// sync is short (a fetch + maybe a re-import), so 5 minutes is ample
	// margin; a crashed syncer's lease lapses and a peer may resume.
	srcSyncLeaseTTL = 5 * time.Minute
	// srcSlowSyncWarnCooldown rate-limits the "sync took longer than its own
	// interval" warning (per source): at most one per cooldown.
	srcSlowSyncWarnCooldown = 10 * time.Minute
)

// ErrSyncDeferred signals that ANOTHER replica is currently syncing this
// pipeline's source (the per-pipeline cross-replica lease was held): the
// caller should skip (the in-flight sync will settle the state). Manual
// refresh surfaces it as 409 Conflict.
var ErrSyncDeferred = errors.New("another replica is syncing this pipeline's source")

// srcRefreshIdentity returns a stable, unique identity for this API replica
// to use as the lease holder (hostname + pid + random component, so two
// replicas on the same host do not collide). Mirrors the scheduler's
// leaderIdentity.
func srcRefreshIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(buf[:]))
}

// StartSourceRefreshLoop boots the leader-gated periodic source-refresh loop
// (F-21.5.3) in the background until ctx is cancelled. It is a no-op when the
// source-fetcher is not configured (nothing to refresh). Leadership is via a
// named lease on the Database service: the loop polls only while THIS
// replica holds the lease, and releases it on shutdown so a peer can take
// over immediately.
func (s *Server) StartSourceRefreshLoop(ctx context.Context) {
	if s.clients.Database == nil || s.clients.PipelineSource == nil {
		return
	}
	go s.runSourceRefreshLoop(ctx, srcRefreshIdentity())
}

// runSourceRefreshLoop is the blocking loop: acquire/renew the leader lease
// on every tick, and — while leading — poll the due sources.
func (s *Server) runSourceRefreshLoop(ctx context.Context, holder string) {
	log := s.serverLogger()
	tracker := &srcAttemptTracker{}
	inFlight := s.srcInFlight
	isLeader := false
	ticker := time.NewTicker(srcRefreshPollInterval)
	defer ticker.Stop()

	reconcile := func(acquired bool, err error) {
		if err != nil {
			// Unreachable DB: treat as a LOSS of leadership (demote) so the
			// loop stops rather than running on a stale belief (F-23).
			acquired = false
		}
		switch {
		case acquired && !isLeader:
			isLeader = true
			s.noteLeaderState(true)
			log.Info("api: acquired source-refresh leader lease", "holder", holder)
		case !acquired && isLeader:
			isLeader = false
			s.noteLeaderState(false)
			log.Info("api: lost source-refresh leader lease", "holder", holder)
		}
	}

	maxParallel := s.srcMaxParallel()
	resp, err := s.acquireSrcRefreshLease(ctx, holder)
	reconcile(resp != nil && resp.GetAcquired(), err)

	for {
		select {
		case <-ctx.Done():
			if isLeader {
				s.releaseSrcRefreshLease(holder)
				s.noteLeaderState(false)
			}
			return
		case <-ticker.C:
			resp, err := s.acquireSrcRefreshLease(ctx, holder)
			reconcile(resp != nil && resp.GetAcquired(), err)
			if !isLeader {
				continue
			}
			s.pollDueSources(ctx, tracker, inFlight, maxParallel)
		}
	}
}

// noteLeaderState mirrors the loop's leadership into the F-20 leader metric
// (nil-safe).
func (s *Server) noteLeaderState(active bool) {
	if s.metrics == nil {
		return
	}
	if active {
		s.metrics.LeaderState.Set(1)
	} else {
		s.metrics.LeaderState.Set(0)
	}
}

// srcMaxParallel is the loop's in-flight sync cap (F-21.5.3): the configured
// value clamped to >= 0 (0 = unlimited; negatives clamp to 0).
func (s *Server) srcMaxParallel() int {
	if s.cfg == nil || s.cfg.PipelineSourceMaxParallel <= 0 {
		return 0
	}
	return s.cfg.PipelineSourceMaxParallel
}

// srcSyncTimeout is the per-sync context bound (F-21.5.3): the configured
// value, floored to the built-in default when unset/non-positive.
func (s *Server) srcSyncTimeout() time.Duration {
	if s.cfg != nil && s.cfg.PipelineSourceSyncTimeout > 0 {
		return s.cfg.PipelineSourceSyncTimeout
	}
	return config.DefaultPipelineSourceSyncTimeout
}

// inFlightCounter tallies the loop's in-flight syncs (feeds the throttle and
// the in-flight gauge).
type inFlightCounter struct {
	mu    sync.Mutex
	count int
}

func (c *inFlightCounter) Inc() {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
}

func (c *inFlightCounter) Dec() {
	c.mu.Lock()
	c.count--
	c.mu.Unlock()
}

func (c *inFlightCounter) Get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

// slowSyncWarner rate-limits the "a sync took longer than its own interval"
// warning (per source, at most one per cooldown) so a chronically slow source
// informs without flooding.
type slowSyncWarner struct {
	mu       sync.Mutex
	cooldown time.Duration
	last     map[int64]time.Time
}

// note records a slow sync for a source, emitting the warning at most once
// per cooldown. It reports whether the warning WAS emitted (test hook).
func (w *slowSyncWarner) note(log *slog.Logger, pipelineID int64, span, elapsed time.Duration) bool {
	if span <= 0 || elapsed <= span {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.last == nil {
		w.last = make(map[int64]time.Time)
	}
	now := time.Now()
	if next, ok := w.last[pipelineID]; ok && now.Sub(next) < w.cooldown {
		return false
	}
	w.last[pipelineID] = now
	log.Warn("api: source sync took longer than its own interval (fires will be skipped until it fits)",
		"pipeline", pipelineID, "elapsed", elapsed.Round(time.Second), "interval", span.Round(time.Second))
	return true
}

// acquireSrcRefreshLease acquires (or renews + heartbeats) the loop's lease.
func (s *Server) acquireSrcRefreshLease(ctx context.Context, holder string) (*dbpb.AcquireLeaseResponse, error) {
	return s.clients.Database.AcquireLease(ctx, &dbpb.AcquireLeaseRequest{
		Name:         srcRefreshLeaseName,
		Holder:       holder,
		Ttl:          durationpb.New(srcRefreshLeaseTTL),
		HeartbeatTtl: durationpb.New(srcRefreshLeaseHeartbeatTTL),
	})
}

// releaseSrcRefreshLease releases the loop's lease on shutdown (best-effort)
// so a peer can take over immediately rather than waiting for the TTL.
func (s *Server) releaseSrcRefreshLease(holder string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = s.clients.Database.ReleaseLease(ctx, &dbpb.ReleaseLeaseRequest{Name: srcRefreshLeaseName, Holder: holder})
}

// srcAttemptTracker remembers each source's pacing state (in-memory, per
// leader tenure):
//
//   - lastAttempt: when the source's current sync was ISSUED (reserved).
//     Pacing from issuance (not completion) guarantees a full interval of
//     quiet after every sync BEGINS — an overshooting sync (longer than its
//     interval) yields skipped intermediate fires, never back-to-back syncs
//     and never piled-up debt.
//   - expectedFinish: lastAttempt + the estimated span (the effective
//     interval; the system default in cron mode). While now < expectedFinish
//     the source is presumed IN FLIGHT and a due fire is SKIPPED
//     (skip-if-in-flight). The estimate is conservative: a sync that runs
//     longer than its estimate is still protected by the per-pipeline mutex
//     (a late fire parks rather than overlaps) and by the per-sync timeout
//     (no infinite drags).
//
// A cold start primes every source to "now" so the first wave respects a
// full cadence (avoiding a thundering herd of simultaneous syncs at loop
// start).
type srcAttemptTracker struct {
	mu          sync.Mutex
	lastAttempt map[int64]time.Time
	expectedFin map[int64]time.Time
	ready       bool
}

// prime seeds every source to now (cold start).
func (t *srcAttemptTracker) prime(sourceIDs ...int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if t.lastAttempt == nil {
		t.lastAttempt = make(map[int64]time.Time)
	}
	for _, id := range sourceIDs {
		t.lastAttempt[id] = now
	}
	t.ready = true
}

// last returns a source's last issuance (zero when never issued).
func (t *srcAttemptTracker) last(sourceID int64) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastAttempt[sourceID]
}

// inFlightUntil reports the source's expected finish (zero when not tracked
// / finished).
func (t *srcAttemptTracker) inFlightUntil(sourceID int64) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.expectedFin[sourceID]
}

// reserve records an issuance: the pacing instant and the expected finish
// (issuance + estSpan).
func (t *srcAttemptTracker) reserve(sourceID int64, at, estSpan time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastAttempt == nil {
		t.lastAttempt = make(map[int64]time.Time)
	}
	if t.expectedFin == nil {
		t.expectedFin = make(map[int64]time.Time)
	}
	t.lastAttempt[sourceID] = at
	t.expectedFin[sourceID] = estSpan
}

// forget clears a source's expectation (completion).
func (t *srcAttemptTracker) forget(sourceID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.expectedFin, sourceID)
}

// primed reports whether the tracker has been seeded (cold-start guard).
func (t *srcAttemptTracker) primed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ready
}

// jitterOffset decorrelates sources sharing an identical interval (F-21.5.3):
// a STABLE per-source phase offset in [-3/8, +3/8] of the interval (seven
// centered eighths, indexed by a hash of the source id), so a fleet of
// same-interval sources spreads its due instants organically instead of
// marching in lockstep. Deterministic: the same source keeps the same phase.
func jitterOffset(sourceID int64, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	h := fnv32a(uint32(sourceID))
	return time.Duration(int64(float64(interval) * (float64(int(h%7)-3) / 8.0)))
}

// fnv32a is a small, allocation-free 32-bit FNV-1a hash (phase indexing only).
func fnv32a(x uint32) uint32 {
	const (
		offset32 uint32 = 2166136261
		prime32  uint32 = 16777619
	)
	h := offset32
	for i := 0; i < 4; i++ {
		h ^= x & 0xff
		h *= prime32
		x >>= 8
	}
	return h
}

// pollDueSources walks every attached source, resolves its EFFECTIVE cadence
// (file override when the admin gate allows it, else the system default),
// and issues whatever is due. Issued syncs run asynchronously; one slow
// source cannot stall the others. Scale guards (F-21.5.3):
//
//   - SKIP-IF-IN-FLIGHT: a due source whose previous sync is still presumed
//     running is skipped (counted skipped_busy) — the next due instant is
//     tried again. No queued second sync, no parked goroutines.
//   - THROTTLE: when maxParallel > 0 and the in-flight count has reached the
//     cap, a due source is deferred to the next tick (counted
//     skipped_throttled) — delayed, never blocked or starved.
//   - JITTER: interval-mode due instants carry a stable per-source phase
//     offset (±⅛ of the interval); cron mode stays exact.
func (s *Server) pollDueSources(ctx context.Context, tracker *srcAttemptTracker, inFlight *inFlightCounter, maxParallel int) {
	sources, err := s.clients.Database.ListPipelineSources(ctx, &dbpb.ListPipelineSourcesRequest{})
	if err != nil {
		s.serverLogger().Warn("api: source-refresh poll: list sources", "err", err)
		return
	}
	list := sources.GetSources()
	if len(list) == 0 {
		return
	}
	// Cold start: prime every source to now so the first wave waits a full
	// cadence (bootstrap-on-attach already gave each a chance at an initial
	// sync; this avoids a synchronized burst at loop start).
	if !tracker.primed() {
		ids := make([]int64, len(list))
		for i, src := range list {
			ids[i] = src.GetPipelineId()
		}
		tracker.prime(ids...)
		return
	}
	pipelines, err := s.clients.Database.ListPipelines(ctx, &dbpb.ListPipelinesRequest{})
	if err != nil {
		s.serverLogger().Warn("api: source-refresh poll: list pipelines", "err", err)
		return
	}
	byID := make(map[int64]*dbpb.Pipeline, len(pipelines.GetPipelines()))
	for _, pl := range pipelines.GetPipelines() {
		byID[pl.GetId()] = pl
	}
	now := time.Now()
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	systemDefault := s.systemDefaultInterval()
	for _, src := range list {
		pipelineID := src.GetPipelineId()
		pl := byID[pipelineID]
		if pl == nil {
			continue // the pipeline vanished (race with a delete)
		}
		honored := s.globalOverridesAllowed() && pl.GetPeriodicRefreshOverridesAllowed()
		mode, intervalStr, schedule, optedOut := effectiveCadence(honored, pl.GetPeriodicRefreshInterval(), pl.GetPeriodicRefreshSchedule(), systemDefault)
		if optedOut {
			continue
		}
		last := tracker.last(pipelineID)
		var due time.Time
		var estSpan time.Duration // the in-flight presumption horizon
		switch mode {
		case "cron":
			sp, perr := parser.Parse(schedule)
			if perr != nil {
				// A stored cron that suddenly fails to parse (should not
				// happen — the compiler validates it): fall back to the
				// system default cadence rather than skipping the source.
				if last.IsZero() {
					due = now
				} else {
					due = last.Add(systemDefault)
				}
				estSpan = systemDefault
			} else {
				anchor := now
				if !last.IsZero() {
					anchor = last
				}
				due = sp.Next(anchor)
				// Precision is cron's purpose: no jitter. The in-flight
				// horizon assumes a typical sync (the system default).
				estSpan = systemDefault
			}
		default: // interval
			d, derr := time.ParseDuration(intervalStr)
			if derr != nil || d <= 0 {
				d = systemDefault
			}
			estSpan = d
			if last.IsZero() {
				due = now
			} else {
				due = last.Add(d + jitterOffset(pipelineID, d))
			}
		}
		if due.After(now) {
			continue // not yet due
		}
		// SKIP-IF-IN-FLIGHT: the previous sync is still presumed running —
		// skip this fire (the next due instant is tried again). This is what
		// keeps an overshooting sync from accumulating debt.
		if fin := tracker.inFlightUntil(pipelineID); !fin.IsZero() && fin.After(now) {
			s.observeSyncOutcome(outcomeSkippedBusy)
			continue
		}
		// THROTTLE: the in-flight cap (finite only) is saturated — defer to
		// the next tick (delayed, never blocked).
		if maxParallel > 0 && inFlight.Get() >= maxParallel {
			s.observeSyncOutcome(outcomeSkippedThrottled)
			continue
		}
		// Reserve the attempt NOW (pacing from issuance) and fire
		// asynchronously.
		at := now
		tracker.reserve(pipelineID, at, at.Add(estSpan))
		inFlight.Inc()
		go func(id int64) {
			defer tracker.forget(id)
			defer inFlight.Dec()
			if _, err := s.syncPipelineSource(ctx, id); err != nil && !errors.Is(err, ErrSyncDeferred) {
				// The failure is already recorded on the source state
				// (last_error); log for operability.
				s.serverLogger().Warn("api: periodic source sync failed", "pipeline", id, "err", err)
			}
		}(pipelineID)
	}
}

// acquireSrcSyncLease grabs the per-pipeline CROSS-REPLICA sync lease
// (F-21.5.2 serialization beyond the local lock): it returns true when the
// lease was acquired (caller must release it), false when another replica is
// already syncing this pipeline (caller should skip — ErrSyncDeferred).
func (s *Server) acquireSrcSyncLease(ctx context.Context, pipelineID int64, holder string) bool {
	resp, err := s.clients.Database.AcquireLease(ctx, &dbpb.AcquireLeaseRequest{
		Name:         fmt.Sprintf("src-sync-%d", pipelineID),
		Holder:       holder,
		Ttl:          durationpb.New(srcSyncLeaseTTL),
		HeartbeatTtl: durationpb.New(srcRefreshLeaseHeartbeatTTL),
	})
	if err != nil {
		// The DB is unreachable: the sync itself would fail anyway — refuse
		// to contend (treat as "held") so we do not pile on.
		return false
	}
	return resp.GetAcquired()
}

// releaseSrcSyncLease releases the per-pipeline sync lease (best-effort).
func (s *Server) releaseSrcSyncLease(pipelineID int64, holder string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = s.clients.Database.ReleaseLease(ctx, &dbpb.ReleaseLeaseRequest{
		Name:   fmt.Sprintf("src-sync-%d", pipelineID),
		Holder: holder,
	})
}
