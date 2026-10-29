//go:build linux

package target

import (
	"os"
	"path/filepath"
	"testing"
)

// TestEncloses pins the enclosure relation (boundary-safe prefixes).
func TestEncloses(t *testing.T) {
	cases := []struct {
		mp, path string
		want     bool
	}{
		{"/", "/", true},
		{"/", "/tmp", true},
		{"/tmp", "/tmp", true},
		{"/tmp", "/tmp/x", true},
		{"/tmp", "/tmp/x/y/z", true},
		{"/tmp", "/tmpx", false}, // boundary: /tmp does not enclose /tmpx
		{"/tmp", "/etc", false},
		{"/tmp/foo", "/tmp", false}, // deeper-than-path never encloses
	}
	for _, c := range cases {
		if got := encloses(c.mp, c.path); got != c.want {
			t.Errorf("encloses(%q, %q) = %v, want %v", c.mp, c.path, got, c.want)
		}
	}
}

// TestMountsIndicateTmpfs pins the deepest-mount-wins decision over a
// fabricated /proc/mounts document.
func TestMountsIndicateTmpfs(t *testing.T) {
	doc := "" +
		"/dev/root / ext4 rw 0 0\n" +
		"udev /dev tmpfs rw,nodatacow 0 0\n" +
		"shm /dev/shm tmpfs rw 0 0\n" +
		"overlay /var/lib/docker/overlay2/ext4-dir overlay rw 0 0\n" +
		"diskbind /srv/disk ext4 ro 0 0\n"
	cases := []struct {
		path string
		want bool
	}{
		{"/", false},              // root is ext4
		{"/dev", true},            // tmpfs
		{"/dev/shm", true},        // tmpfs (deepest enclosing)
		{"/dev/shm/socket", true}, // under the shm tmpfs
		{"/var/lib/docker/overlay2/ext4-dir", false}, // overlay (disk-backed)
		{"/srv/disk", false},                         // ext4 bind
		{"/srv/disk/sub/dir", false},                 // under the ext4 bind
		{"/srv", false},                              // nearest enclosing is / (ext4)
		{"/tmp", false},                              // not mounted → / (ext4)
	}
	for _, c := range cases {
		if got := mountsIndicateTmpfs(doc, c.path); got != c.want {
			t.Errorf("mountsIndicateTmpfs(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

// TestUnmountsNestedTmpfsOverDisk: a disk mount UNDER a tmpfs root — the
// deepest enclosing mount (disk) must win.
func TestUnmountsNestedTmpfsOverDisk(t *testing.T) {
	doc := "" +
		"tmpfs / tmpfs rw 0 0\n" +
		"diskvol /tank ext4 rw 0 0\n"
	if !mountsIndicateTmpfs(doc, "/somewhere") {
		t.Error(`/somewhere under tmpfs / = false, want true`)
	}
	if mountsIndicateTmpfs(doc, "/tank/backed") {
		t.Error(`/tank/backed under disk /tank = true, want false (deepest wins)`)
	}
}

// TestUnmountKernelEscapes: a mount point with an ESCAPED SPACE must still
// enclose its children.
func TestUnmountKernelEscapes(t *testing.T) {
	doc := "tmpfs /mnt/my\\040share tmpfs rw 0 0\n"
	if !mountsIndicateTmpfs(doc, "/mnt/my share/pkg") {
		t.Error("escaped-space mount point not recognized")
	}
	if mountsIndicateTmpfs(doc, "/mnt/share") {
		t.Error("unrelated path wrongly classified tmpfs")
	}
}

// TestWorkRootIsTmpfsRealProcMounts: the LIVE check against this host's
// /proc/mounts — / must classify as SOME definite answer (whatever it is),
// and a nonsense path must not blow up. (Asserting a specific answer would
// couple the test to the host's mount layout.)
func TestWorkRootIsTmpfsRealProcMounts(t *testing.T) {
	_ = workRootIsTmpfs("/")
	_ = workRootIsTmpfs("")
	_ = workRootIsTmpfs("/definitely/not/a/path")
}

// TestStageAreaUsesWorkRoot: the stage dir is created UNDER the configured
// work root (and the root is CREATED when absent).
func TestStageAreaUsesWorkRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "custom-work-root")
	sa := newStageArea(root, nil)
	dir, err := sa.Dir()
	if err != nil {
		t.Fatalf("Dir: %v", err)
	}
	if filepath.Dir(dir) != root {
		t.Fatalf("stage dir %q is not under root %q", dir, root)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("work root was not created: %v", err)
	}
	// Memoized: a second call returns the same dir.
	again, err := sa.Dir()
	if err != nil || again != dir {
		t.Fatalf("Dir() again = (%q, %v), want (%q, nil)", again, err, dir)
	}
	// Clean removes it.
	sa.Clean()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("Clean left the stage dir behind (err=%v)", err)
	}
}

// TestStageAreaDefaultRoot: an EMPTY work root falls back to the OS temp dir.
func TestStageAreaDefaultRoot(t *testing.T) {
	sa := newStageArea("", nil)
	dir, err := sa.Dir()
	if err != nil {
		t.Fatalf("Dir: %v", err)
	}
	if filepath.Dir(dir) != os.TempDir() {
		t.Fatalf("stage dir %q is not under the OS temp dir %q", dir, os.TempDir())
	}
	sa.Clean()
}
