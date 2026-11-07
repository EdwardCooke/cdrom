//go:build !linux

package target

// agentRootIsTmpfs is the non-Linux counterpart of the /proc/mounts check:
// mounting introspection is not available portably (Windows has no
// /proc/mounts; BSD/macOS would need sysctl), so the tmpfs warning is
// SUPPRESSED on those platforms. The agent root itself (agent_root /
// CDROM_AGENT_ROOT) remains fully configurable everywhere.
func agentRootIsTmpfs(path string) bool {
	return false
}
