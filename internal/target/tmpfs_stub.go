//go:build !linux

package target

// workerRootIsTmpfs is the non-Linux counterpart of the /proc/mounts check:
// mounting introspection is not available portably (Windows has no
// /proc/mounts; BSD/macOS would need sysctl), so the tmpfs warning is
// SUPPRESSED on those platforms. The worker root itself (worker_root /
// CDROM_WORKER_ROOT) remains fully configurable everywhere.
func workerRootIsTmpfs(path string) bool {
	return false
}
