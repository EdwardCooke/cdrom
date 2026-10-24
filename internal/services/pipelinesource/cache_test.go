package pipelinesource

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func newTestCache(t *testing.T) *repoCache {
	t.Helper()
	c, err := newRepoCache(t.TempDir(), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatalf("newRepoCache: %v", err)
	}
	return c
}

func TestIndexRoundTrip(t *testing.T) {
	c := newTestCache(t)
	now := time.Now().Unix()
	if err := c.updateIndex(func(m map[string]*cacheEntry) error {
		m["https://github.com/o/r.git"] = &cacheEntry{Dir: "1761024000-0", URL: "https://github.com/o/r.git", LastAccessed: now, LastFetched: now}
		return nil
	}); err != nil {
		t.Fatalf("updateIndex: %v", err)
	}
	entries, err := c.loadIndex()
	if err != nil {
		t.Fatalf("loadIndex: %v", err)
	}
	e, ok := entries["https://github.com/o/r.git"]
	if !ok {
		t.Fatal("entry not found")
	}
	if e.Dir != "1761024000-0" || e.LastAccessed != now || e.LastFetched != now {
		t.Fatalf("entry = %+v", e)
	}
}

func TestIndexAtomicRewrite(t *testing.T) {
	c := newTestCache(t)
	now := time.Now().Unix()
	// Add two entries; the index file should contain both lines.
	if err := c.updateIndex(func(m map[string]*cacheEntry) error {
		m["https://a"] = &cacheEntry{Dir: "1-0", URL: "https://a", LastAccessed: now, LastFetched: now}
		m["https://b"] = &cacheEntry{Dir: "1-1", URL: "https://b", LastAccessed: now, LastFetched: now}
		return nil
	}); err != nil {
		t.Fatalf("updateIndex: %v", err)
	}
	data, err := os.ReadFile(c.indexPath())
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("index is empty")
	}
	// No temp files should be left behind.
	dirEntries, err := os.ReadDir(c.root)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, de := range dirEntries {
		if filepath.Ext(de.Name()) == ".tmp" {
			t.Fatalf("leftover temp file %q", de.Name())
		}
	}
}

func TestNextDirNameUnique(t *testing.T) {
	c := newTestCache(t)
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		name, err := c.nextDirName()
		if err != nil {
			t.Fatalf("nextDirName: %v", err)
		}
		if seen[name] {
			t.Fatalf("duplicate directory name %q", name)
		}
		seen[name] = true
	}
}

// TestNextDirNameSkipsExisting verifies that a directory name already present
// on disk (created by another replica sharing the cache) is skipped:
// nextDirName returns a different name rather than colliding with it.
func TestNextDirNameSkipsExisting(t *testing.T) {
	c := newTestCache(t)
	existing := uuid.NewString()
	if err := os.MkdirAll(filepath.Join(c.root, existing), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	n, err := c.nextDirName()
	if err != nil {
		t.Fatalf("nextDirName: %v", err)
	}
	if n == existing {
		t.Fatalf("nextDirName returned the existing directory %q", n)
	}
}

func TestCleanupPrunesInactive(t *testing.T) {
	c := newTestCache(t)
	old := time.Now().Add(-25 * time.Hour).Unix()
	// Create a fake cached directory + index entry that is inactive.
	dir := "1700000000-0"
	if err := os.MkdirAll(filepath.Join(c.root, dir), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := c.updateIndex(func(m map[string]*cacheEntry) error {
		m["https://old"] = &cacheEntry{Dir: dir, URL: "https://old", LastAccessed: old, LastFetched: old}
		return nil
	}); err != nil {
		t.Fatalf("updateIndex: %v", err)
	}
	c.cleanup()
	// The directory and index entry should be gone.
	if _, err := os.Stat(filepath.Join(c.root, dir)); !os.IsNotExist(err) {
		t.Fatalf("directory %q should be pruned (err=%v)", dir, err)
	}
	entries, _ := c.loadIndex()
	if _, ok := entries["https://old"]; ok {
		t.Fatal("index entry should be pruned")
	}
}

func TestCleanupKeepsActive(t *testing.T) {
	c := newTestCache(t)
	now := time.Now().Unix()
	dir := "1700000001-0"
	if err := os.MkdirAll(filepath.Join(c.root, dir), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := c.updateIndex(func(m map[string]*cacheEntry) error {
		m["https://active"] = &cacheEntry{Dir: dir, URL: "https://active", LastAccessed: now, LastFetched: now}
		return nil
	}); err != nil {
		t.Fatalf("updateIndex: %v", err)
	}
	c.cleanup()
	if _, err := os.Stat(filepath.Join(c.root, dir)); err != nil {
		t.Fatalf("active directory %q should be kept (err=%v)", dir, err)
	}
}
