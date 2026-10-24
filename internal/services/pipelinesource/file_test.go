package pipelinesource

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestFileClientReadsRootedPath(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "shared-variables.yaml"), "a: 1\n")

	fc := newFileClient(root)
	ref := &Reference{Kind: KindFile, Path: "/shared-variables.yaml"}
	content, version, hash, err := fc.fetch(context.Background(), ref)
	if err != nil {
		t.Fatalf("fetch error: %v", err)
	}
	if string(content) != "a: 1\n" {
		t.Fatalf("content = %q", content)
	}
	if version == "" {
		t.Fatal("version (mtime) is empty")
	}
	if hash == "" {
		t.Fatal("hash is empty")
	}
}

func TestFileClientRejectsPathTraversal(t *testing.T) {
	root := t.TempDir()
	// A file outside the root that a traversal would reach.
	outside := filepath.Join(t.TempDir(), "secret.yaml")
	writeFile(t, outside, "top: secret\n")

	fc := newFileClient(root)
	// ../secret.yaml resolves outside the root and must be rejected.
	ref := &Reference{Kind: KindFile, Path: "../secret.yaml"}
	if _, _, _, err := fc.fetch(context.Background(), ref); err == nil {
		t.Fatal("path traversal should be rejected")
	}
}

// TestFileClientRejectsNestedTraversal verifies that a path with INTERNAL ".."
// components that resolves outside the root is rejected: filepath.Join cleans
// the path (collapsing "an/.." away), so the resolved location — not the raw
// string — is what the containment check sees.
func TestFileClientRejectsNestedTraversal(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// A file at <base>/escaped/secret.yaml — outside the root, reachable via
	// "an/../../escaped/secret.yaml" (which cleans to "../escaped/secret.yaml").
	outside := filepath.Join(base, "escaped", "secret.yaml")
	writeFile(t, outside, "top: secret\n")

	fc := newFileClient(root)
	ref := &Reference{Kind: KindFile, Path: "an/../../escaped/secret.yaml"}
	if _, _, _, err := fc.fetch(context.Background(), ref); err == nil {
		t.Fatal("nested path traversal should be rejected")
	}
}

// TestFileClientInternalDotDotInsideRoot verifies that a path with internal
// ".." components that RESOLVE inside the root is allowed: the guard validates
// the resolved location, so "a/b/../file.yaml" (→ "a/file.yaml") is fine.
func TestFileClientInternalDotDotInsideRoot(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a", "file.yaml"), "ok\n")

	fc := newFileClient(root)
	ref := &Reference{Kind: KindFile, Path: "a/b/../file.yaml"}
	content, _, _, err := fc.fetch(context.Background(), ref)
	if err != nil {
		t.Fatalf("internal .. resolving inside the root should be allowed: %v", err)
	}
	if string(content) != "ok\n" {
		t.Fatalf("content = %q", content)
	}
}

func TestFileClientDisabledWithoutRoot(t *testing.T) {
	fc := newFileClient("")
	ref := &Reference{Kind: KindFile, Path: "/anything.yaml"}
	if _, _, _, err := fc.fetch(context.Background(), ref); err == nil {
		t.Fatal("file kind should be disabled when no root is configured")
	}
}

func TestFileClientMissingFile(t *testing.T) {
	root := t.TempDir()
	fc := newFileClient(root)
	ref := &Reference{Kind: KindFile, Path: "/does-not-exist.yaml"}
	if _, _, _, err := fc.fetch(context.Background(), ref); err == nil {
		t.Fatal("missing file should error")
	}
}

// TestFileClientOSNativeSeparators verifies the file client resolves a reference
// path that uses the OS's native separator (forward slash on Linux, backslash on
// Windows) rather than assuming "/". The path is built with filepath.Join so it
// carries the host's separator, which is the case that broke on Windows.
func TestFileClientOSNativeSeparators(t *testing.T) {
	root := t.TempDir()
	// A nested file, written with OS-native separators.
	nested := filepath.Join(root, "vars", "shared-variables.yaml")
	writeFile(t, nested, "a: 1\n")

	fc := newFileClient(root)
	// The reference path uses the OS separator (filepath.Join), not a hardcoded
	// "/". On Windows this is "vars\shared-variables.yaml".
	ref := &Reference{Kind: KindFile, Path: filepath.Join("vars", "shared-variables.yaml")}
	content, _, _, err := fc.fetch(context.Background(), ref)
	if err != nil {
		t.Fatalf("fetch with OS-native separators: %v", err)
	}
	if string(content) != "a: 1\n" {
		t.Fatalf("content = %q", content)
	}
}
