// Command scheduler is the Cdrom scheduler service.
//
// It manages job lifecycle and dispatch. For high availability (F-23) it
// writes to the shared Database service only: it persists the job and appends
// an "assignment" event to the shared event log, which every API pod tails and
// fans out to its local workers. It no longer dials a specific API pod, so a
// job is delivered to whichever pod a worker happens to be connected to. Jobs
// with an empty target group are queued for ephemeral Kubernetes agents. All
// durable state is persisted through the Database service.
//
// The four background loops (watchdog, retry, dependency resolver, run-status)
// run on exactly one replica at a time: the scheduler runs a leader election
// (a lease on the Database service) and starts the loops only while it holds
// the lease, stopping them if it loses it.
package main

import (
	"context"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"

	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
	"cdrom/internal/grpcutil"
	"cdrom/internal/logging"
	"cdrom/internal/services/scheduler"
)

func main() {
	logger := logging.New()

	configFile, err := config.ParseFlags(os.Args[1:])
	if err != nil {
		logger.Error("config: parse flags", "err", err)
		os.Exit(1)
	}
	cfg, err := config.LoadWithFile(configFile)
	if err != nil {
		logger.Error("config: invalid", "err", err)
		os.Exit(1)
	}
	addr := cfg.ListenAddress
	if addr == "" {
		addr = config.DefaultSchedulerAddress
	}

	ctx := context.Background()
	dbConn, err := grpcutil.Dial(ctx, cfg.DBAddress, cfg.TLS)
	if err != nil {
		logger.Error("dial database service", "addr", cfg.DBAddress, "err", err)
		os.Exit(1)
	}
	defer dbConn.Close()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("listen", "addr", addr, "err", err)
		os.Exit(1)
	}

	creds, err := grpcutil.ServerCreds(cfg.TLS)
	if err != nil {
		logger.Error("tls: server creds", "err", err)
		os.Exit(1)
	}
	sched := scheduler.NewServer(dbpb.NewDatabaseClient(dbConn), logger)

	// The four background loops (F-03 watchdog, F-04 retry, F-06 dependency
	// resolver, F-07 run-status) must run on exactly one scheduler replica at
	// a time, or they would publish duplicate events (F-23). They are started
	// when this replica wins the leader election and stopped when it loses the
	// lease, so leadership can move between replicas without a restart.
	loops := newLoopManager(ctx, sched)
	electionCtx, stopElection := context.WithCancel(ctx)
	defer stopElection()
	sched.StartLeaderElection(electionCtx, loops.start, loops.stop)

	srv := grpc.NewServer(grpc.Creds(creds))
	schedpb.RegisterSchedulerServer(srv, sched)

	logger.Info("cdrom scheduler service starting", "addr", addr, "db", cfg.DBAddress)
	if err := grpcutil.Serve(lis, srv, logger); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
	// Serve returned on a shutdown signal. Stop the leader election (which
	// releases the lease so a peer can take over) and the loops, then exit.
	stopElection()
	loops.stop()
}

// loopManager starts and stops the scheduler's four background loops, gated
// by leader election (F-23). It is safe for concurrent use: the
// leader-election goroutine (onAcquire / onLose) and the shutdown path both
// call into it.
type loopManager struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	sched  *scheduler.Server
	ctx    context.Context
}

func newLoopManager(ctx context.Context, sched *scheduler.Server) *loopManager {
	return &loopManager{ctx: ctx, sched: sched}
}

// start begins the four background loops on a fresh context, stopping any
// that are already running (a re-acquired leader restarts them cleanly).
func (m *loopManager) start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		m.cancel()
	}
	loopsCtx, cancel := context.WithCancel(m.ctx)
	m.cancel = cancel
	m.sched.StartWatchdog(loopsCtx)
	m.sched.StartRetryLoop(loopsCtx)
	m.sched.StartDependencyResolver(loopsCtx)
	m.sched.StartRunStatusLoop(loopsCtx)
	m.sched.StartJobStatusLoop(loopsCtx)
}

// stop halts the running loops, if any.
func (m *loopManager) stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
}
