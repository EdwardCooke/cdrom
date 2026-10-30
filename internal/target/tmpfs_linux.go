//go:build linux

package target

import (
	"os"
	"strings"
)

// workerRootIsTmpfs reports whether path lies on a RAM-backed (tmpfs/ramfs)
// mount, consulting /proc/mounts. A missing or unreadable mounts table
// yields false — the warning is advisory, never alarming.
func workerRootIsTmpfs(path string) bool {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false
	}
	return mountsIndicateTmpfs(string(data), path)
}

// mountsIndicateTmpfs is the PURE decision over a /proc/mounts document:
// among the mount POINTS that enclose path, the DEEPEST decides (mounts
// nest: a disk bind-mount under a tmpfs / is disk-backed). Kernel octal
// escapes in mount points ("\040" space, "\011" tab, "\012" newline, "\134"
// backslash) are undone. Factored out of workerRootIsTmpfs for testability.
func mountsIndicateTmpfs(doc, path string) bool {
	bestLen := -1
	bestTmpfs := false
	for _, line := range strings.Split(doc, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		mp := unescapeMountPoint(fields[1])
		fstype := fields[2]
		if !encloses(mp, path) {
			continue
		}
		if len(mp) > bestLen {
			bestLen = len(mp)
			bestTmpfs = fstype == "tmpfs" || fstype == "ramfs"
		}
	}
	return bestTmpfs
}

// encloses reports whether mount point mp contains path (mp == path, or mp
// is a path-prefix at a '/' boundary — "/mnt" does NOT enclose "/mntx").
func encloses(mp, path string) bool {
	if mp == path {
		return true
	}
	if mp == "/" {
		return true
	}
	return strings.HasPrefix(path, mp+"/")
}

// unescapeMountPoint undoes the kernel's octal escaping of special
// characters in mount points ("\040" space, "\011" tab, "\012" newline,
// "\134" backslash — BACKSLASH PLUS THREE OCTAL DIGITS, literally).
func unescapeMountPoint(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			switch s[i+1 : i+4] {
			case "040":
				b.WriteByte(' ')
				i += 3
				continue
			case "011":
				b.WriteByte('\t')
				i += 3
				continue
			case "012":
				b.WriteByte('\n')
				i += 3
				continue
			case "134":
				b.WriteByte('\\')
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
