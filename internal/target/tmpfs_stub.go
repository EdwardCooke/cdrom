//go:build !linux

package target

// workRootIsTmpfs is the non-Linux counterpart of the /proc/mounts check:
// mounting introspection is not available portably (Windows has no
// /proc/mounts; BSD/macOS would need sysctl), so the tmpfs warning is
// SUPPRESSED on those platforms. The work root itself (work_root /
// CDROM_WORK_ROOT) remains fully configurable everywhere.
func workRootIsTmpfs(path string) bool {
	return false
}
