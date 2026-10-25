//go:build !windows

package pipelinesource

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestFileClientRejectsSymlinkEscape verifies the symlink guard (F-21.4
// containment): a symlink INSIDE the root that points at a file OUTSIDE the
// root is refused — os.Stat/os.ReadFile follow symlinks, so the lexical
// containment check alone would miss it.
func TestFileClientRejectsSymlinkEscape(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	// A secret file outside the root.
	outside := filepath.Join(base, "outside-secret.yaml")
	writeFile(t, outside, "top: secret\n")
	// A symlink inside the root pointing at it.
	link := filepath.Join(root, "innocuous.yaml")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("could not create symlink (restricted FS?): %v", err)
	}

	fc := newFileClient(root)
	// The lexical path is inside the root; only symlink resolution reveals
	// the escape.
	ref := &Reference{Kind: KindFile, Path: "/innocuous.yaml"}
	if _, _, _, err := fc.fetch(context.Background(), ref); err == nil {
		t.Fatal("a symlink escaping the root should be rejected")
	}
}

// TestFileClientAllowsBenignSymlinkInsideRoot verifies the guard is not overly
// broad: a symlink inside the root that points AT ANOTHER LOCATION INSIDE THE
// ROOT is readable.
func TestFileClientAllowsBenignSymlinkInsideRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	real := filepath.Join(root, "real-vars.yaml")
	writeFile(t, real, "a: 1\n")
	link := filepath.Join(root, "link-vars.yaml")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("could not create symlink (restricted FS?): %v", err)
	}

	fc := newFileClient(root)
	ref := &Reference{Kind: KindFile, Path: "/link-vars.yaml"}
	content, _, _, err := fc.fetch(context.Background(), ref)
	if err != nil {
		t.Fatalf("benign in-root symlink should be readable: %v", err)
	}
	if string(content) != "a: 1\n" {
		t.Fatalf("content = %q", content)
	}
}

// TestFileClientRejectsNonRegularFile verifies a directory (a non-regular
// file) is refused rather than read.
func TestFileClientRejectsNonRegularFile(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "adir")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	fc := newFileClient(root)
	ref := &Reference{Kind: KindFile, Path: "/adir"}
	if _, _, _, err := fc.fetch(context.Background(), ref); err == nil {
		t.Fatal("reading a directory should be rejected")
	}
}
