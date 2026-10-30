// stage_area.go — the job's staged working area (F-21.6): a per-job-instance
// scratch directory the artifact step handlers use as their default home
// (download destinations, extraction targets, publish scan roots, zip
// assembly). It is provisioned LAZILY (created on first use, so ordinary
// jobs touch no extra disk), exported to every step of the job as
// executor.StageAreaEnv (CDROM_STAGE_DIR), and removed BEST-EFFORT once the
// job finishes.
//
// The area is unique per job INSTANCE AND PER TARGET: fan-out workers each
// stage their own copy (there is no cross-worker sharing), and the directory
// sits under the CONFIGURED WORKER ROOT (MkdirTemp-style) so concurrent jobs
// never contend. The worker root defaults to the OS temp dir; operators point
// it at disk-backed storage (config worker_root / CDROM_WORKER_ROOT) when the
// temp dir is RAM-backed.
package target

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// stageArea memoizes the job's staged working directory.
type stageArea struct {
	mu     sync.Mutex
	dir    string
	root   string // the worker root the stage dir is created under ("" = os.TempDir)
	logger *slog.Logger
}

// newStageArea builds the lazy provisioner the target hangs on the
// executor's context (executor.ContextWithStageArea). workerRoot is the base
// directory stage dirs are created under ("" = the OS temp dir).
func newStageArea(workerRoot string, logger *slog.Logger) *stageArea {
	if logger == nil {
		logger = slog.Default()
	}
	return &stageArea{root: workerRoot, logger: logger}
}

// Dir creates (on first call) and returns the stage directory. Safe for
// concurrent use; the executor invokes it once per step that needs it.
func (s *stageArea) Dir() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dir != "" {
		return s.dir, nil
	}
	root := s.root
	if root == "" {
		root = os.TempDir()
	}
	// Make sure the root exists (an operator-configured path may not yet).
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp(root, "cdrom-stage-")
	if err != nil {
		return "", err
	}
	s.dir = dir
	s.logger.Info("target: staged working area created", "dir", dir, "root", root)
	return dir, nil
}

// WarnTmpfsWorkerRoot logs a WARNING when the effective worker root (workerRoot, or
// the OS temp dir when it is empty) resides on a RAM-backed (tmpfs/ramfs)
// mount: staged files then consume MEMORY, and a large extraction or
// download can starve the host. Detection is best-effort and LINUX-ONLY
// (other platforms report false — see tmpfs_stub.go); a missing or
// unreadable mounts table is likewise non-alarming. Called ONCE at target
// startup (worker/agent construction), not per job.
func WarnTmpfsWorkerRoot(workerRoot string, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	root := workerRoot
	if root == "" {
		root = os.TempDir()
	}
	root = filepath.Clean(root)
	if workerRootIsTmpfs(root) {
		logger.Warn("target: the staged-working-area root is on a RAM-backed (tmpfs) mount — staged files consume MEMORY and can starve the host; point worker_root / CDROM_WORKER_ROOT at disk-backed storage",
			"root", root)
	}
}

// Clean removes the stage directory (best-effort: a failed removal is
// logged, not surfaced). Invoked once, after the job reaches a terminal
// state.
func (s *stageArea) Clean() {
	s.mu.Lock()
	dir := s.dir
	s.dir = ""
	s.mu.Unlock()
	if dir == "" {
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		s.logger.Warn("target: remove staged working area (ignored)", "dir", dir, "err", err)
	}
}
