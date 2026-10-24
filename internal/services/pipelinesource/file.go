package pipelinesource

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// fileClient reads a single file from the fetcher's host, resolved against a
// configured local-filesystem root (F-21.2.4.3). The root keeps the fetcher
// from reading arbitrary files on its host; when no root is configured the
// file kind is disabled.
type fileClient struct {
	root string // empty means the file kind is disabled
}

// newFileClient creates a file client rooted at root. An empty root disables
// the file kind.
func newFileClient(root string) *fileClient {
	return &fileClient{root: strings.TrimRight(root, "/")}
}

// enabled reports whether the file kind is configured (a root is set).
func (f *fileClient) enabled() bool { return f.root != "" }

// fetch reads the file at ref.Path from <root><path> and returns its content,
// its mtime (the version identifier), and its SHA-256 (hex). The resolved path
// must stay within the root.
func (f *fileClient) fetch(_ context.Context, ref *Reference) ([]byte, string, string, error) {
	if !f.enabled() {
		return nil, "", "", fmt.Errorf("file: the file kind is disabled (no local-filesystem root configured; set CDROM_PIPELINE_SOURCE_FILE_ROOT)")
	}
	// Resolve the path against the root and ensure it stays within it.
	// filepath.Join is OS-aware: it normalizes the path and collapses any
	// leading separator (forward or back slash), so the reference path works
	// whether the caller used / or \ as separators. It never treats the
	// reference path as absolute, so it cannot escape the root on its own.
	full := filepath.Join(f.root, ref.Path)
	// Guard against path traversal: the resolved path must be under the root.
	// filepath.Rel returns an OS-separator path, so compare against the OS
	// separator (not a hardcoded "/").
	rel, err := filepath.Rel(f.root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, "", "", fmt.Errorf("file: path %q escapes the configured root", ref.Path)
	}
	info, err := os.Stat(full)
	if err != nil {
		return nil, "", "", fmt.Errorf("file: stat %q: %w", ref.Path, err)
	}
	content, err := os.ReadFile(full)
	if err != nil {
		return nil, "", "", fmt.Errorf("file: read %q: %w", ref.Path, err)
	}
	sum := sha256.Sum256(content)
	return content, info.ModTime().UTC().Format(time.RFC3339Nano), hex.EncodeToString(sum[:]), nil
}
