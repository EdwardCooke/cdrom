//go:build linux

package agent

import (
	"os/exec"
	"syscall"
)

// attachDetachment puts the child in its own process group (Setpgid): a later
// kill (shutdown ordering / the slice-3 death semantics) can take the WHOLE
// TREE (the worker's own grandchild commands) with one signal to the negated
// pgid.
func (w *Agent) attachDetachment(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
