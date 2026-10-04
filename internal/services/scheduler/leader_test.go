package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fakeLeaseDB is a stub Database client for the leader-election tests. It
// records the AcquireLease requests it receives and returns a scripted
// acquired result (or error) for each.
type fakeLeaseDB struct {
	dbpb.DatabaseClient // nil
	mu                  sync.Mutex
	requests            []*dbpb.AcquireLeaseRequest
	releases            []*dbpb.ReleaseLeaseRequest
	acquired            bool // the result returned for AcquireLease
	acquireErr          error
}

func (f *fakeLeaseDB) AcquireLease(ctx context.Context, in *dbpb.AcquireLeaseRequest, opts ...grpc.CallOption) (*dbpb.AcquireLeaseResponse, error) {
	f.mu.Lock()
	f.requests = append(f.requests, in)
	f.mu.Unlock()
	if f.acquireErr != nil {
		return nil, f.acquireErr
	}
	return &dbpb.AcquireLeaseResponse{Acquired: f.acquired}, nil
}

func (f *fakeLeaseDB) ReleaseLease(ctx context.Context, in *dbpb.ReleaseLeaseRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	f.releases = append(f.releases, in)
	f.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (f *fakeLeaseDB) numRequests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

// TestLeaderElectionAcquiresAndHeartbeats verifies that the election loop
// acquires the lease (firing onAcquire) and keeps renewing it (heartbeating)
// on each tick, passing both the TTL backstop and the heartbeat window.
func TestLeaderElectionAcquiresAndHeartbeats(t *testing.T) {
	db := &fakeLeaseDB{acquired: true}
	s := &Server{db: db, logger: testLogger()}

	acquired := make(chan struct{})
	var acquireCount int
	onAcquire := func() {
		acquireCount++
		select {
		case acquired <- struct{}{}:
		default:
		}
	}

	oldInterval := leaseRenewInterval
	leaseRenewInterval = 20 * time.Millisecond
	defer func() { leaseRenewInterval = oldInterval }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartLeaderElection(ctx, onAcquire, nil)

	// Wait for the leader to acquire the lease.
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not acquire the lease in time")
	}

	// Let a few renewal ticks run so the leader heartbeats.
	time.Sleep(80 * time.Millisecond)
	cancel()

	// The leader should have acquired and then renewed (heartbeated) at least
	// once more.
	if got := db.numRequests(); got < 2 {
		t.Errorf("AcquireLease called %d times, want >= 2 (initial + renewals)", got)
	}

	// The first request carries the TTL backstop and the heartbeat window.
	db.mu.Lock()
	first := db.requests[0]
	db.mu.Unlock()
	if first.GetTtl().AsDuration() != leaseTTL {
		t.Errorf("ttl = %s, want %s", first.GetTtl().AsDuration(), leaseTTL)
	}
	if first.GetHeartbeatTtl().AsDuration() != leaseHeartbeatTTL {
		t.Errorf("heartbeat_ttl = %s, want %s", first.GetHeartbeatTtl().AsDuration(), leaseHeartbeatTTL)
	}
	if first.GetName() != leaseName {
		t.Errorf("name = %q, want %q", first.GetName(), leaseName)
	}
	if acquireCount != 1 {
		t.Errorf("onAcquire fired %d times, want 1", acquireCount)
	}
}

// TestLeaderElectionLosesLease verifies that when the Database reports the
// lease is no longer held (another replica took it over), the election loop
// fires onLose and stops believing it is the leader.
func TestLeaderElectionLosesLease(t *testing.T) {
	// The first acquire succeeds (the replica becomes leader); subsequent
	// acquires report the lease was lost to a peer.
	s := &Server{db: &flipLeaseDB{}, logger: testLogger()}

	lostCh := make(chan struct{})
	onLose := func() {
		select {
		case lostCh <- struct{}{}:
		default:
		}
	}

	oldInterval := leaseRenewInterval
	leaseRenewInterval = 20 * time.Millisecond
	defer func() { leaseRenewInterval = oldInterval }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartLeaderElection(ctx, nil, onLose)

	// Wait for the leader to lose the lease on a subsequent tick.
	select {
	case <-lostCh:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not lose the lease in time")
	}
	cancel()
}

// TestLeaderElectionDBErrorDemotes verifies that a failure to reach the
// Database on a renewal demotes the leader (onLose fires) so the loops stop
// rather than run on a stale belief of leadership.
func TestLeaderElectionDBErrorDemotes(t *testing.T) {
	db := &fakeLeaseDB{acquired: true}
	s := &Server{db: db, logger: testLogger()}

	lostCh := make(chan struct{})
	onLose := func() {
		select {
		case lostCh <- struct{}{}:
		default:
		}
	}

	oldInterval := leaseRenewInterval
	leaseRenewInterval = 20 * time.Millisecond
	defer func() { leaseRenewInterval = oldInterval }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartLeaderElection(ctx, nil, onLose)

	// Let the leader acquire, then make the DB unreachable.
	time.Sleep(40 * time.Millisecond)
	db.mu.Lock()
	db.acquireErr = context.DeadlineExceeded
	db.mu.Unlock()

	// The next renewal fails and demotes the leader.
	select {
	case <-lostCh:
	case <-time.After(2 * time.Second):
		t.Fatal("leader was not demoted after a DB error")
	}
	cancel()
}

// flipLeaseDB is a stub Database client that returns acquired=true for the
// first AcquireLease call and acquired=false for all subsequent calls,
// simulating a replica that acquires the lease and then loses it to a peer.
type flipLeaseDB struct {
	dbpb.DatabaseClient // nil; only the methods below are used
	seen                bool
}

func (f *flipLeaseDB) AcquireLease(ctx context.Context, in *dbpb.AcquireLeaseRequest, opts ...grpc.CallOption) (*dbpb.AcquireLeaseResponse, error) {
	if !f.seen {
		f.seen = true
		return &dbpb.AcquireLeaseResponse{Acquired: true}, nil
	}
	return &dbpb.AcquireLeaseResponse{Acquired: false}, nil
}

func (f *flipLeaseDB) ReleaseLease(ctx context.Context, in *dbpb.ReleaseLeaseRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
