//go:build windows

package agent

import (
	"os/exec"
	"syscall"
)

// Stable Win32 creation flags (golang.org/x/sys/windows values, defined
// locally to avoid widening the dependency surface for two constants):
// CREATE_NEW_PROCESS_GROUP puts the child in its own group (Ctrl+C /
// terminable grouping); DETACHED_PROCESS detaches it from the agent's
// console.
const (
	createNewProcessGroup uint32 = 0x00000200
	detachedProcess       uint32 = 0x00000008
)

// attachDetachment isolates the child on Windows: CREATE_NEW_PROCESS_GROUP
// gives it a terminable group, DETACHED_PROCESS keeps it off the agent's
// console.
func (w *Agent) attachDetachment(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: createNewProcessGroup | detachedProcess,
	}
}
