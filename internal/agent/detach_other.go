//go:build !linux && !windows

package agent

import "os/exec"

// attachDetachment is a no-op on unsupported platforms (the child shares the
// agent's process group).
func (w *Agent) attachDetachment(cmd *exec.Cmd) {}
