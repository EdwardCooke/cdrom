//go:build linux

package agent

import (
	"os"
	"os/exec"
	"syscall"
)

// attachDetachment puts the child in its own process group (Setpgid): a later
// interrupt/kill (shutdown ordering / the slice-3 death semantics) can take
// the WHOLE TREE (the worker's own grandchild commands) with one signal to
// the negated pgid. Owned by the LOCAL LAUNCHER (launcher_local.go) — the
// agent core never touches the substrate.
func attachDetachment(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// interruptProcessTree politely signals the child's ENTIRE process group
// (SETgid at spawn ⇒ the group id IS the child's pid; the negated pid
// targets the group, so the worker's own grandchild commands hear it too).
// ESRCH (the group already gone) is a benign "too late" — the caller's kill
// stage is the guarantee.
func interruptProcessTree(p *os.Process) error {
	if p == nil {
		return nil
	}
	return syscall.Kill(-p.Pid, syscall.SIGINT)
}
