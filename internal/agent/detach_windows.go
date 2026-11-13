//go:build windows

package agent

import (
	"os"
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
// gives it a terminable group (the group id IS the child's pid),
// DETACHED_PROCESS keeps it off the agent's console. Owned by the LOCAL
// LAUNCHER (launcher_local.go) — the agent core never touches the substrate.
func attachDetachment(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: createNewProcessGroup | detachedProcess,
	}
}

// ctrlBreakEvent is the GenerateConsoleCtrlEvent kind that reaches a whole
// process group (kernel32's CTRL_BREAK_EVENT = 0). Defined locally (as with
// the creation flags above) to avoid widening the dependency surface.
const ctrlBreakEvent uint32 = 0

// generateConsoleCtrlEventPtr is kernel32!GenerateConsoleCtrlEvent
// (lazy-loaded — stdlib syscall does not export it, and the house rule is
// no new dependencies for two constants).
var generateConsoleCtrlEventPtr = syscall.NewLazyDLL("kernel32.dll").NewProc("GenerateConsoleCtrlEvent")

// interruptProcessTree sends a CtrlBreak EVENT to the child's process group
// (the group id is the child's pid — CREATE_NEW_PROCESS_GROUP at spawn).
// BEST-EFFORT: a DETACHED child may lack the console affinity the event
// delivery presumes; the caller's kill stage (TerminateProcess-grade) is
// the guarantee. Note: Go maps CtrlBreak to SIGQUIT in the child — a
// cooperative worker that traps it exits cleanly; one that doesn't dies
// with Go's default SIGQUIT disposition (stack dump, exit 2 — classified
// failed on the agent side, which is the truthful outcome for an aborted
// job).
func interruptProcessTree(p *os.Process) error {
	if p == nil {
		return nil
	}
	p.Signal(os.Signal(syscall.SIGQUIT))
	// dwProcessGroupId is the process GROUP IDENTIFIER — the child's pid
	// (CREATE_NEW_PROCESS_GROUP at spawn made the group id the leader's pid).
	_, _, err := generateConsoleCtrlEventPtr.Call(uintptr(ctrlBreakEvent), uintptr(p.Pid))
	return err
}
