//go:build windows

package pipelinesource

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// TestFileClientWindowsDriveLetterDoesNotEscape verifies that a reference path
// carrying a Windows drive letter (e.g. `d:\sensitive.yaml`) does NOT escape
// the configured root. filepath.Join treats the drive-letter path as a literal
// element under the root (it never re-interprets the second argument's volume
// prefix as a new drive), so the resolved path stays under the root and the
// containment guard holds; the fetch then errors because no such literal path
// exists under the root.
func TestFileClientWindowsDriveLetterDoesNotEscape(t *testing.T) {
	root := t.TempDir()
	ref := &Reference{Kind: KindFile, Path: `d:\sensitive.yaml`}

	// The resolved path must remain under the root (Rel succeeds on the same
	// volume and yields no ".." prefix).
	full := filepath.Join(root, ref.Path)
	rel, err := filepath.Rel(root, full)
	if err != nil {
		t.Fatalf("Rel: %v", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("drive-letter path escaped the root: rel=%q", rel)
	}

	// And the fetch must not read a file outside the root (it errors, since no
	// such literal path exists under the root).
	fc := newFileClient(root)
	if _, _, _, err := fc.fetch(context.Background(), ref); err == nil {
		t.Fatal("drive-letter path should not resolve to a readable file outside the root")
	}
}

// TestFileClientWindowsUNCPathDoesNotEscape verifies that a reference path
// carrying a UNC prefix (e.g. `\\server\share\path.yaml`) does NOT escape the
// configured root. A UNC prefix is only recognized at the start of a path
// (VolumeName), and filepath.Join starts the result with the root — so the
// "\\server" in the reference is collapsed by Clean into a literal element
// under the root (join even strips leading slashes from a following element
// when the accumulated path ends in a separator, specifically to avoid
// creating a UNC path from non-UNC elements). The resolved path stays under
// the root and the read fails because no such literal path exists there.
func TestFileClientWindowsUNCPathDoesNotEscape(t *testing.T) {
	root := t.TempDir()
	ref := &Reference{Kind: KindFile, Path: `\\server\share\path.yaml`}

	// The resolved path must remain under the root (Rel succeeds on the same
	// volume and yields no ".." prefix).
	full := filepath.Join(root, ref.Path)
	rel, err := filepath.Rel(root, full)
	if err != nil {
		t.Fatalf("Rel: %v", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("UNC path escaped the root: rel=%q", rel)
	}

	// And the fetch must not read a file outside the root (it errors, since no
	// such literal path exists under the root).
	fc := newFileClient(root)
	if _, _, _, err := fc.fetch(context.Background(), ref); err == nil {
		t.Fatal("UNC path should not resolve to a readable file outside the root")
	}
}
