// Package agent — the LAUNCHER SEAM (F-30.4).
//
// The agent's supervision CORE (launch ledger, live children, observation,
// metric roll-up, the supervision channel) is substrate-INDEPENDENT: it
// speaks only the Child and Launcher contracts defined here. Bringing a
// worker UP is delegated to a Launcher. The default (launcher_local.go)
// forks a local subprocess with the canonical env contract; a future
// Kubernetes launcher realizes the SAME contracts by emitting a Pod spec —
// the agent core is unchanged. This is the seam that makes the k8s piece a
// launcher swap, not a rewrite.
package agent

import "time"

// Mission describes ONE worker launch (substrate-agnostic): the coordinate
// the agent reserved, the job token the worker presents to the API, and the
// ambient inputs the launcher materializes into the worker's environment.
type Mission struct {
	Coord      spawnCoordinate
	JobToken   string
	APIAddress string // the API gRPC address (CDROM_API_ADDR)
	TLSCAFile  string // the CA bundle (CDROM_TLS_CA_FILE); "" = plaintext leg
	WorkerRoot string // the worker's staged-work-area base (CDROM_WORKER_ROOT)
	// ChannelAddr is the supervision channel's advertised address
	// (CDROM_PARENT_GRPC_ADDR); "" = the worker runs channel-less (TOTAL
	// DEGRADATION — the channel never gates the job).
	ChannelAddr string
	// ExtraEnv is operator-supplied passthrough (e.g. test steering).
	ExtraEnv []string
}

// Child is one supervised worker (substrate-agnostic). The agent observes a
// child's liveness and books its outcome; it never inspects the substrate
// (a local *exec.Cmd, a k8s Pod, …).
type Child interface {
	// Observe blocks until the child exits and records the outcome. Called
	// by EXACTLY ONE goroutine (the agent's observeChild).
	Observe() error
	// ExitCode is the child's termination code, readable AFTER Observe
	// returns (rich codes: 0 succeeded, 1 failed, 3 cancelled, 4 timed_out;
	// -1/other when unclassifiable).
	ExitCode() int
	// ID is the child's substrate-native identity as a STRING (informative):
	// a local process id, a k8s Pod UID, … Substrate-specific formats vary
	// widely (UUIDs, numeric pids, opaque uids), so the contract exposes it
	// opaquely — callers log/report it, never parse it.
	ID() string
	// WorkerLog is the child's OWN log file (when it could not append live
	// to the agent's log); the agent folds it in after exit. "" = none.
	WorkerLog() string
	// SpawnedAt is when the child was launched (UTC) — the basis of the
	// agent's wall-clock duration roll-up.
	SpawnedAt() time.Time
	// AwaitExited is closed when the child has exited (observation-complete).
	AwaitExited() <-chan struct{}
	// Interrupt politely asks the child to stop (slice 3's graceful-stop
	// ORDERING: drain → INTERRUPT → kill). Best-effort; a no-op when the
	// substrate cannot be signalled cooperatively.
	Interrupt()
	// Terminate forcibly stops the child (slice 3's kill-on-shutdown).
	// Best-effort; a no-op when the substrate cannot be signalled.
	Terminate()
}

// Launcher brings workers UP. The agent delegates every fork/emission to it
// so the supervision core stays substrate-blind.
type Launcher interface {
	Enabled() bool
	Launch(Mission) (Child, error)
}
