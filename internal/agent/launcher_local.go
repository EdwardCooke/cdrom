// Package agent — the LOCAL LAUNCHER (F-30.4): the os/exec realization of
// the Launcher seam. The agent's supervision CORE (launch ledger, live
// children, observation, metric roll-up, the supervision channel) speaks only
// the abstract Child/Launcher contracts (launcher.go); THIS file knows HOW a
// local worker subprocess is brought up — the canonical env contract, the
// privileged-variable strip, the platform detachment, and the worker's log
// placement. Swapping the launcher (a Kubernetes realization that emits a
// Pod spec instead of forking) requires NO change to the agent core.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// LocalLauncher brings up worker PROCESSES on the agent's own host. It is
// the default launcher (installed by cmd/agent); an empty Binary disables
// launching (misconfiguration surfaced loudly).
type LocalLauncher struct {
	Binary string
	// Args are appended to the worker invocation (production: none — the
	// mandate rides the environment; tests: e.g. ["-c","sleep 5"] on a shell
	// to steer the child's lifespan/effects). Substrate-specific, hence on
	// the launcher rather than the substrate-agnostic Mission.
	Args []string
	log  *slog.Logger
}

// NewLocalLauncher wraps the worker executable path (tests may pass a shell
// + args to steer the child's lifespan/effects).
func NewLocalLauncher(binary string, args []string, log *slog.Logger) *LocalLauncher {
	if log == nil {
		log = slog.Default()
	}
	return &LocalLauncher{Binary: binary, Args: args, log: log}
}

// Enabled reports whether launches are possible (the binary is configured).
func (ll *LocalLauncher) Enabled() bool { return ll != nil && ll.Binary != "" }

// Launch forks the worker subprocess with the CANONICAL ENV CONTRACT
// (F-30.4): the mandate rides the ENVIRONMENT ONLY — argv is world-readable
// (process listings, /proc/*/cmdline); env is not. The child DETACHES from
// the launcher's lifetime (its context is independent): it finishes its job
// and reports to the API even if the agent exits (controlled drain/kill
// ordering is slice 3).
func (ll *LocalLauncher) Launch(m Mission) (Child, error) {
	if !ll.Enabled() {
		ll.log.Warn("agent: worker binary not configured; job NOT executed", "job", m.Coord.jobID, "attempt", m.Coord.attempt)
		return nil, fmt.Errorf("worker binary not configured")
	}
	cmd := exec.CommandContext(context.Background(), ll.Binary, ll.Args...)
	cmd.Env = buildWorkerEnv(m)
	// Detach the child into its own process group (platform-specific): a
	// later kill-on-shutdown (slice 3) can take the WHOLE TREE (the worker's
	// own grandchild commands) with one signal.
	attachDetachment(cmd)
	// STDIO IS INHERITED (nil): the worker's logs flow to the agent's log
	// destination (CDROM_LOG_FILE / the service journal) — one coherent log
	// per target host, distinguishable by the "worker:"/"agent:" prefixes.
	// (The worker's own logging convention, CDROM_LOG_*, is inherited too.)
	if err := cmd.Start(); err != nil {
		ll.log.Error("agent: spawn worker", "job", m.Coord.jobID, "attempt", m.Coord.attempt, "err", err)
		return nil, err
	}
	// House style: all STORED/measured times are UTC (duration arithmetic is
	// location-immute either way; UTC keeps exhibits comparable).
	return &localChild{
		cmd:       cmd,
		pid:       cmd.Process.Pid,
		workerLog: cmdEnvValue(cmd.Env, "CDROM_LOG_FILE"),
		spawnedAt: time.Now().UTC(),
		exited:    make(chan struct{}),
	}, nil
}

// buildWorkerEnv assembles the worker's environment: the INHERITED
// environment MINUS the agent's private secrets (the agent credential file
// must NOT propagate into the worker — the worker never needs it), PLUS the
// canonical mandate. The mission's variables OVERRIDE any inherited ones
// (stripped from the inherited set first).
func buildWorkerEnv(m Mission) []string {
	privileged := map[string]struct{}{
		"CDROM_AGENT_CREDENTIAL_FILE": {}, // the agent's credential (private)
		"CDROM_WORKER_JOB_ID":         {}, // overridden below
		"CDROM_WORKER_JOB_TOKEN":      {}, // overridden below
	}
	env := make([]string, 0, 16)
	for _, kv := range os.Environ() {
		k := kv
		if i := strings.IndexByte(kv, '='); i > 0 {
			k = kv[:i]
		}
		if _, priv := privileged[k]; priv {
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"CDROM_WORKER_JOB_ID="+strconv.FormatInt(m.Coord.jobID, 10),
		"CDROM_WORKER_JOB_TOKEN="+m.JobToken,
		"CDROM_API_ADDR="+m.APIAddress,
	)
	// The supervision channel (F-30.4 slice 2): advertise the channel's
	// address so the worker can dial it (Signals + Telemetry). Absent when
	// the channel is degraded (listen failed) — the worker then runs
	// channel-less and the agent infers its metrics from the exit code.
	if m.ChannelAddr != "" {
		env = append(env, "CDROM_PARENT_GRPC_ADDR="+m.ChannelAddr)
	}
	if m.WorkerRoot != "" {
		env = append(env, "CDROM_WORKER_ROOT="+m.WorkerRoot)
	}
	if m.TLSCAFile != "" {
		env = append(env, "CDROM_TLS_CA_FILE="+m.TLSCAFile)
	}
	// Operator-supplied passthrough (e.g. test steering).
	env = append(env, m.ExtraEnv...)
	// The worker's log DESTINATION (F-30.4): when the agent's own stdout is
	// a REGULAR FILE (the norm: harness/service-manager redirection), the
	// worker APPENDS THERE LIVE (concurrent O_APPEND writers) — the
	// worker's lines are visible the instant they are written, so a reader
	// that waits for the job's terminal status never races a post-exit fold.
	// Otherwise the worker writes to its OWN file and the agent folds it in
	// after the child exits (the only case with a small settle window).
	dest := agentLogDest()
	var wl string
	if dest != "" {
		wl = dest
	} else {
		wl = workerLogPath(m.Coord.jobID, randHex())
	}
	env = append(env, "CDROM_LOG_FILE="+wl)
	return env
}

// cmdEnvValue returns the value of KEY in an env KV slice ("" when absent).
func cmdEnvValue(env []string, key string) string {
	pre := key + "="
	for _, kv := range env {
		if strings.HasPrefix(kv, pre) {
			return kv[len(pre):]
		}
	}
	return ""
}

// localChild is one supervised worker SUBPROCESS (the local realization of
// Child).
type localChild struct {
	cmd       *exec.Cmd
	pid       int
	workerLog string // the child's OWN log file ("" when appending live)
	spawnedAt time.Time
	exited    chan struct{} // closed when Wait returns (observation-complete)
	waitErr   error         // set by Observe (the single waiter)
}

func (c *localChild) ID() string                   { return strconv.Itoa(c.pid) }
func (c *localChild) WorkerLog() string            { return c.workerLog }
func (c *localChild) SpawnedAt() time.Time         { return c.spawnedAt }
func (c *localChild) AwaitExited() <-chan struct{} { return c.exited }

// Observe waits on the child (portable Cmd.Wait), records the outcome, and
// CLOSES its exited channel. Exactly one caller (the agent's observeChild
// goroutine).
func (c *localChild) Observe() error {
	err := c.cmd.Wait()
	c.waitErr = err
	close(c.exited)
	return err
}

// ExitCode maps the observed outcome to the rich exit code (0 succeeded,
// 1 failed, 3 cancelled, 4 timed_out; a signaled/killed child or any other
// non-error is classified by the agent's classifyExit). Reads the wait error
// recorded by Observe (same goroutine — no race).
func (c *localChild) ExitCode() int {
	if c.waitErr == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(c.waitErr, &ee) {
		return ee.ExitCode()
	}
	return -1
}

// Interrupt politely asks the child to stop (slice 3's graceful-stop
// ordering: drain → INTerrupt → kill). Delegated to the PLATFORM PRIMITIVE
// (interruptProcessTree): on Linux the child's whole process group gets
// SIGINT (Setpgid at spawn ⇒ group id = the child's pid — the worker's own
// grandchild commands hear it too); on Windows a CtrlBreak event is
// generated for the child's process group (CREATE_NEW_PROCESS_GROUP at
// spawn); elsewhere the direct child gets SIGINT. Process.Signal(os.Interrupt)
// is NOT usable here: it is unimplemented on Windows (returns
// ErrUnsupportedSignal — a silent no-op under best-effort) and on Linux it
// reaches only the DIRECT child, not the tree. Best-effort: the kill stage
// (Terminate) is the guarantee.
func (c *localChild) Interrupt() {
	_ = interruptProcessTree(c.cmd.Process)
}

// Terminate forcibly stops the child (slice 3's kill-on-shutdown; tests use
// it for teardown hygiene). Direct-process kill: on Linux the group-leader
// dies and the group's orphans reparent (the interrupt stage already warned
// the whole tree); on Windows Process.Kill is TerminateProcess. Best-effort.
func (c *localChild) Terminate() {
	if c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
}
