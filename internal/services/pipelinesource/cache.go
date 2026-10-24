package pipelinesource

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// cacheRetention is how long a cached repository is kept before it is pruned
// (F-21.2.3): a repository is pruned only after this long without use.
const cacheRetention = 24 * time.Hour

// cleanupInterval is how often the cleanup thread walks the cache.
const cleanupInterval = time.Hour

// staleLockTimeout is how long a lock's heartbeat may go quiet before another
// replica takes the lock over (F-21.2.3.3). It bounds how long a crashed
// replica can wedge the shared cache.
const staleLockTimeout = 5 * time.Minute

// lockHeartbeat is how often a lock holder rewrites its heartbeat.
const lockHeartbeat = 30 * time.Second

// indexFileName is the name of the cache's text index file at the cache root.
const indexFileName = "index.tsv"

// cacheEntry is one line of the cache index: which directory holds a
// repository, the repository URL, and the last-accessed / last-fetched Unix
// epoch timestamps.
type cacheEntry struct {
	Dir          string
	URL          string
	LastAccessed int64
	LastFetched  int64
}

// repoCache is the fetcher's on-disk git repository cache (F-21.2.3). It keeps
// a bare object store per repository URL under a shared root, tracks them in an
// atomically updated text index, locks each repository while it is
// cloned/fetched/resynced (so multiple replicas sharing the cache never race),
// and prunes inactive repositories on a background thread. Every request does
// an incremental fetch (no debounce): a fetch is a no-op when the branch is
// unchanged and transfers only the delta when it moved.
type repoCache struct {
	root      string
	logger    *slog.Logger
	mu        sync.Mutex // serializes index mutations within this process
	cleanupCh chan struct{}
}

// newRepoCache creates a repository cache rooted at root (creating it if
// needed).
func newRepoCache(root string, logger *slog.Logger) (*repoCache, error) {
	if root == "" {
		root = filepath.Join(os.TempDir(), "repositories")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("cache: create root %q: %w", root, err)
	}
	return &repoCache{root: root, logger: logger, cleanupCh: make(chan struct{})}, nil
}

// indexPath is the path of the cache's index file.
func (c *repoCache) indexPath() string { return filepath.Join(c.root, indexFileName) }

// lockPath is the path of a repository's lock file.
func (c *repoCache) lockPath(dir string) string { return filepath.Join(c.root, dir+".lock") }

// startCleanup starts the background cleanup thread (F-21.2.3.4). It returns a
// stop function.
func (c *repoCache) startCleanup(ctx context.Context) func() {
	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.cleanupCh:
			case <-ticker.C:
			}
			c.cleanup()
		}
	}()
	return func() { close(c.cleanupCh) }
}

// loadIndex reads the index file into a map keyed by repository URL. A missing
// index file yields an empty map.
func (c *repoCache) loadIndex() (map[string]*cacheEntry, error) {
	out := map[string]*cacheEntry{}
	f, err := os.Open(c.indexPath())
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, fmt.Errorf("cache: open index: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 4 {
			continue // skip a malformed line
		}
		lastAccessed, _ := strconv.ParseInt(fields[2], 10, 64)
		lastFetched, _ := strconv.ParseInt(fields[3], 10, 64)
		e := &cacheEntry{Dir: fields[0], URL: fields[1], LastAccessed: lastAccessed, LastFetched: lastFetched}
		out[e.URL] = e
	}
	return out, sc.Err()
}

// saveIndex writes the index map to the index file atomically (temp file +
// rename). The caller must hold c.mu.
func (c *repoCache) saveIndex(entries map[string]*cacheEntry) error {
	// Sort by URL for a stable, diff-friendly index.
	urls := make([]string, 0, len(entries))
	for u := range entries {
		urls = append(urls, u)
	}
	sort.Strings(urls)
	var b strings.Builder
	for _, u := range urls {
		e := entries[u]
		b.WriteString(e.Dir)
		b.WriteByte('\t')
		b.WriteString(e.URL)
		b.WriteByte('\t')
		b.WriteString(strconv.FormatInt(e.LastAccessed, 10))
		b.WriteByte('\t')
		b.WriteString(strconv.FormatInt(e.LastFetched, 10))
		b.WriteByte('\n')
	}
	tmp, err := os.CreateTemp(c.root, "index-*.tmp")
	if err != nil {
		return fmt.Errorf("cache: create temp index: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once renamed
	if _, err := tmp.WriteString(b.String()); err != nil {
		tmp.Close()
		return fmt.Errorf("cache: write temp index: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cache: close temp index: %w", err)
	}
	if err := os.Rename(tmpName, c.indexPath()); err != nil {
		return fmt.Errorf("cache: rename index: %w", err)
	}
	return nil
}

// updateIndex mutates the index under the process lock: it loads the current
// entries, applies fn, and saves the result atomically.
func (c *repoCache) updateIndex(fn func(entries map[string]*cacheEntry) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, err := c.loadIndex()
	if err != nil {
		return err
	}
	if err := fn(entries); err != nil {
		return err
	}
	return c.saveIndex(entries)
}

// nextDirName returns a new, unique repository directory name: a random UUID
// (v4). The name is returned only if its directory does not already exist, so
// a collision (astronomically unlikely) is retried with a fresh UUID. No
// shared counter state is needed, so the method is safe to call concurrently
// without holding c.mu.
func (c *repoCache) nextDirName() (string, error) {
	for {
		name := uuid.NewString()
		_, err := os.Stat(filepath.Join(c.root, name))
		if os.IsNotExist(err) {
			return name, nil
		}
		if err != nil {
			return "", fmt.Errorf("cache: stat %q: %w", name, err)
		}
		// The directory already exists (astronomically unlikely); try again.
	}
}

// cleanup prunes the cache (F-21.2.3.4): it removes index entries whose
// last-accessed timestamp is older than the retention window (deleting the
// index entry before the directory), skips repositories that are currently
// locked, and sweeps orphaned directories that have no index entry.
func (c *repoCache) cleanup() {
	now := time.Now().Unix()
	entries, err := c.loadIndex()
	if err != nil {
		c.logger.Warn("cache: cleanup: load index", "err", err)
		return
	}
	// Prune inactive repositories.
	pruned := 0
	for url, e := range entries {
		if now-e.LastAccessed < int64(cacheRetention.Seconds()) {
			continue
		}
		if isLocked(c.lockPath(e.Dir)) {
			continue // a fetch is in progress; retry next pass
		}
		if err := c.updateIndex(func(m map[string]*cacheEntry) error {
			delete(m, url)
			return nil
		}); err != nil {
			c.logger.Warn("cache: cleanup: remove index entry", "dir", e.Dir, "err", err)
			continue
		}
		if err := os.RemoveAll(filepath.Join(c.root, e.Dir)); err != nil {
			c.logger.Warn("cache: cleanup: remove directory", "dir", e.Dir, "err", err)
			continue
		}
		os.Remove(c.lockPath(e.Dir))
		pruned++
	}
	// Build the set of directories referenced by an index entry (the index is
	// keyed by URL, so a directory is "known" when some entry's Dir matches it).
	knownDirs := map[string]bool{}
	for _, e := range entries {
		knownDirs[e.Dir] = true
	}
	// Sweep orphaned directories (present under the root but with no index
	// entry), skipping any that are locked.
	if dirEntries, err := os.ReadDir(c.root); err == nil {
		for _, de := range dirEntries {
			if !de.IsDir() {
				continue
			}
			if knownDirs[de.Name()] {
				continue
			}
			if isLocked(c.lockPath(de.Name())) {
				continue
			}
			if err := os.RemoveAll(filepath.Join(c.root, de.Name())); err != nil {
				c.logger.Warn("cache: cleanup: remove orphaned directory", "dir", de.Name(), "err", err)
				continue
			}
			os.Remove(c.lockPath(de.Name()))
			pruned++
		}
	}
	if pruned > 0 {
		c.logger.Info("cache: cleanup pruned repositories", "count", pruned)
	}
}

// isLocked reports whether the lock file at path is currently held (its
// heartbeat is fresh). A missing or stale lock file is not locked.
func isLocked(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	// The lock file holds "<pid>\t<heartbeat-epoch>".
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return false
	}
	hb, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return false
	}
	return time.Now().Unix()-hb < int64(staleLockTimeout.Seconds())
}
