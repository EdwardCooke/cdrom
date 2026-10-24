package pipelinesource

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// repoLock is a per-repository lock (F-21.2.3.3) held while a repository is
// cloned, fetched, or resynced. It is a lock file in the cache root containing
// "<pid>\t<heartbeat-epoch>"; a background goroutine refreshes the heartbeat
// while the lock is held. A lock whose heartbeat is older than
// staleLockTimeout is considered stale and may be taken over, so a crashed
// replica cannot wedge the shared cache.
//
// The lock is portable across Windows and Linux: it relies only on atomic
// file creation (O_EXCL) and file reads/writes, not on platform-specific
// advisory locks.
type repoLock struct {
	path      string
	heartbeat int64
	stop      chan struct{}
	done      chan struct{}
}

// acquireLock takes the lock at path, waiting for a fresh lock held by another
// replica and taking over a stale one. It returns the held lock (call its
// Release) or an error if ctx is cancelled while waiting.
func acquireLock(ctx context.Context, path string) (*repoLock, error) {
	for {
		// If the lock file exists and is fresh, wait and retry.
		if isLocked(path) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}
		// If the lock file exists but is stale (its holder crashed), take it
		// over.
		if data, err := os.ReadFile(path); err == nil {
			if hb, ok := parseHeartbeat(data); ok && time.Now().Unix()-hb >= int64(staleLockTimeout.Seconds()) {
				_ = os.Remove(path)
				continue
			}
		}
		// Create the lock file atomically.
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			if os.IsExist(err) {
				// Another replica created it first; loop and wait/take over.
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(500 * time.Millisecond):
					continue
				}
			}
			return nil, fmt.Errorf("lock: create %q: %w", path, err)
		}
		l := &repoLock{path: path, heartbeat: time.Now().Unix(), stop: make(chan struct{}), done: make(chan struct{})}
		if err := l.write(); err != nil {
			f.Close()
			os.Remove(path)
			return nil, err
		}
		f.Close()
		go l.heartbeatLoop()
		return l, nil
	}
}

// write (re)writes the lock file's heartbeat.
func (l *repoLock) write() error {
	data := fmt.Sprintf("%d\t%d\n", os.Getpid(), l.heartbeat)
	return os.WriteFile(l.path, []byte(data), 0o600)
}

// heartbeatLoop refreshes the heartbeat until Release is called.
func (l *repoLock) heartbeatLoop() {
	defer close(l.done)
	ticker := time.NewTicker(lockHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			l.heartbeat = time.Now().Unix()
			if err := l.write(); err != nil {
				// A failed heartbeat write is non-fatal: the lock is still
				// held; the next tick retries.
			}
		}
	}
}

// Release stops the heartbeat and removes the lock file.
func (l *repoLock) Release() {
	close(l.stop)
	<-l.done
	os.Remove(l.path)
}

// parseHeartbeat parses a lock file's "<pid>\t<heartbeat-epoch>" content.
func parseHeartbeat(data []byte) (int64, bool) {
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return 0, false
	}
	hb, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return hb, true
}
