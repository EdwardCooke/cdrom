//go:build !linux && !windows

package agent

import (
	"os"
	"os/exec"
	"syscall"
)

// attachDetachment is a no-op on unsupported platforms (the child shares the
// agent's process group). Owned by the LOCAL LAUNCHER (launcher_local.go).
func attachDetachment(cmd *exec.Cmd) {}

// interruptProcessTree signals the DIRECT child only (no group isolation on
// this platform) — best-effort; the kill stage is the guarantee.
func interruptProcessTree(p *os.Process) error {
	if p == nil {
		return nil
	}
	return p.Signal(syscall.SIGINT)
}
