package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// Leader-election tuning (F-23). The lease TTL is how long a lease stays
// valid after the last renewal; the renewal interval is how often the holder
// re-acquires it. The interval is well under the TTL so a healthy holder
// never lapses, while a crashed or partitioned holder lapses within the TTL
// and another replica can take over.
const (
	// leaseName is the name of the scheduler's leader lease.
	leaseName = "scheduler"
	// leaseTTL is how long a lease is valid from its last renewal.
	leaseTTL = 10 * time.Second
	// leaseRenewInterval is how often the holder renews the lease.
	leaseRenewInterval = 3 * time.Second
)

// leaderIdentity returns a stable, unique identity for this scheduler replica
// to use as the lease holder. It combines the hostname and process id with a
// random component so two replicas on the same host (e.g. in a test) do not
// collide.
func leaderIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// A collision is far better than a panic; fall back to the pid.
		return fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(buf[:]))
}

// StartLeaderElection runs the scheduler's leader-election loop in the
// background until ctx is cancelled (F-23). It acquires the named lease on
// the Database service, renews it periodically, and runs the four background
// loops only while this replica holds the lease.
//
// onAcquire is called exactly once when this replica transitions from
// non-leader to leader (it should start the loops); onLose is called exactly
// once when it transitions from leader to non-leader (it should stop them).
// Both callbacks run on the election's goroutine. If onAcquire or onLose is
// nil the corresponding transition is a no-op.
//
// The election is best-effort: a failure to reach the Database service on a
// renewal is treated as "not acquired" for that tick, so a transient outage
// demotes this replica and another (or the same, once the DB is reachable
// again) can lead. The conditional acquire on the Database side means two
// replicas can never both believe they hold the lease for an unexpired term.
func (s *Server) StartLeaderElection(ctx context.Context, onAcquire, onLose func()) {
	if s.db == nil {
		return
	}
	go s.runLeaderElection(ctx, onAcquire, onLose)
}

// runLeaderElection is the blocking election loop, run in a goroutine by
// StartLeaderElection. It acquires the lease immediately (so a fresh leader
// starts the loops without waiting a full renewal interval), then renews it
// periodically until ctx is cancelled.
func (s *Server) runLeaderElection(ctx context.Context, onAcquire, onLose func()) {
	holder := leaderIdentity()
	isLeader := false
	ticker := time.NewTicker(leaseRenewInterval)
	defer ticker.Stop()

	s.tryAcquire(ctx, holder, &isLeader, onAcquire, onLose)

	for {
		select {
		case <-ctx.Done():
			// On shutdown, release the lease if we hold it so a peer can take
			// over immediately rather than waiting for the TTL to lapse.
			if isLeader {
				s.releaseLease(ctx, holder)
			}
			return
		case <-ticker.C:
			s.tryAcquire(ctx, holder, &isLeader, onAcquire, onLose)
		}
	}
}

// tryAcquire attempts to acquire (or renew) the lease and fires the
// onAcquire / onLose callbacks on a leadership transition.
func (s *Server) tryAcquire(ctx context.Context, holder string, isLeader *bool, onAcquire, onLose func()) {
	resp, err := s.db.AcquireLease(ctx, &dbpb.AcquireLeaseRequest{
		Name:   leaseName,
		Holder: holder,
		Ttl:    durationpb.New(leaseTTL),
	})
	if err != nil {
		s.logger.Warn("scheduler: leader election acquire", "holder", holder, "err", err)
		// A failure to reach the DB is treated as a loss of leadership so the
		// loops stop rather than run on a stale belief of leadership.
		if *isLeader {
			*isLeader = false
			if onLose != nil {
				onLose()
			}
		}
		return
	}
	acquired := resp.GetAcquired()
	switch {
	case acquired && !*isLeader:
		*isLeader = true
		s.logger.Info("scheduler: acquired leader lease", "holder", holder)
		if onAcquire != nil {
			onAcquire()
		}
	case !acquired && *isLeader:
		*isLeader = false
		s.logger.Info("scheduler: lost leader lease", "holder", holder)
		if onLose != nil {
			onLose()
		}
	}
}

// releaseLease releases the lease on shutdown so a peer can take over
// immediately. It is best-effort: a failure here only means a peer waits for
// the TTL to lapse.
func (s *Server) releaseLease(ctx context.Context, holder string) {
	if _, err := s.db.ReleaseLease(ctx, &dbpb.ReleaseLeaseRequest{Name: leaseName, Holder: holder}); err != nil {
		s.logger.Warn("scheduler: leader election release", "holder", holder, "err", err)
	}
}
