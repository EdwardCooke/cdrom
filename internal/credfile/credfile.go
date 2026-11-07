// Package credfile is the ATOMIC DISTRIBUTION PRIMITIVE for the agent's
// credential file (F-30.2 "Distribution"; consumed by F-30.3's agent).
//
// The agent's JWT rests in a single file the operator provisions (issue →
// persist) and rotates (overwrite). Because the file is THE credential —
// readable by whoever can read it — every write must satisfy:
//
//  1. RESTRICTIVE PERMISSIONS: the file is created `0600` (owner rw only),
//     so the token is not world-readable. (On Windows the ACL honors the
//     owner-only bits Go requests; the guarantee is "not broader than the
//     creator" on POSIX and "creator-controlled" on NTFS.)
//  2. ATOMICITY: readers (the agent's boot load and its file WATCHER) must
//     never observe a torn write — a partially overwritten JWT is worse than
//     no JWT (it poisons a live adopt). Writes therefore go to a TEMP file
//     in the SAME DIRECTORY and are published with a rename: the reader sees
//     either the old content or the new, never a fragment.
//  3. WATCHER QUIESCENCE: the temp file is DOT-PREFIXED (`.cred-XXXXXX.tmp`)
//     so a sane watcher (polling or fsnotify) ignores in-flight writes and
//     reacts only to the RENAMED destination appearing/changing.
//
// Reads are TOLERANT BY DESIGN: trimming trailing newline/carriage-return
// (operators paste the JWT into editors that add one) and rejecting EMPTY
// content with a clear error (an empty file is a misconfiguration, not a
// valid credential). The file's ABSENCE is a distinct, diagnosable error —
// F-30.3's agent refuses to boot on it.
//
// Portability: only `os`/`io/fs`/`path/filepath` — no unix-only syscalls, so
// the agent ships for Windows and Linux alike (repository rule).
package credfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// permMode is the credential file's permission: owner read/write only.
const permMode = 0o600

// ErrNotExist wraps os.ErrNotExist for a MISSING credential file, keeping
// the sentinel comparable (errors.Is(err, os.ErrNotExist)).
var ErrNotExist = errors.New("credfile: credential file does not exist")

// WriteAtomically publishes token to path: a dot-prefixed temp file in
// path's directory receives the bytes (written 0600, flushed, then chmod'd
// defensively in case umask raced the open), and a rename swaps it into
// place. On any failure the temp file is removed and the ORIGINAL content
// (when present) is left intact. The parent directory must already exist —
// operators provision the location; the primitive does not invent trees.
//
// The token argument is the PLAINTEXT JWT; it is written verbatim plus a
// terminating newline (POSIX-text-friendly; Read strips it back off).
func WriteAtomically(path, token string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".cred-*.tmp")
	if err != nil {
		return fmt.Errorf("credfile: prepare temp in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	fail := func(err error) error {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if _, err := tmp.WriteString(strings.TrimRight(token, "\r\n") + "\n"); err != nil {
		return fail(fmt.Errorf("credfile: write temp: %w", err))
	}
	if err := tmp.Sync(); err != nil {
		return fail(fmt.Errorf("credfile: sync temp: %w", err))
	}
	if err := tmp.Chmod(permMode); err != nil {
		return fail(fmt.Errorf("credfile: chmod temp: %w", err))
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("credfile: close temp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("credfile: rename into place: %w", err)
	}
	// Belt-and-suspenders: the rename carries the temp's mode, but a hostile
	// umask or filesystem quirk could widen it — clamp to 0600 regardless.
	if err := os.Chmod(path, permMode); err != nil {
		return fmt.Errorf("credfile: clamp perms on %s: %w", path, err)
	}
	return nil
}

// Read loads the credential at path and returns the trimmed token. A MISSING
// file returns an error satisfying errors.Is(err, os.ErrNotExist) AND
// errors.Is(err, ErrNotExist). Empty/whitespace-only content is a distinct
// error (misconfigured, not absent).
func Read(path string) (string, error) {
	if _, serr := os.Stat(path); serr != nil {
		if os.IsNotExist(serr) {
			// Wrap BOTH sentinels: callers may recognize absence via the
			// generic os.ErrNotExist or the package-specific ErrNotExist.
			return "", fmt.Errorf("%w (underlying: %w)", ErrNotExist, serr)
		}
		return "", fmt.Errorf("credfile: stat %s: %w", path, serr)
	}
	// Existence is the precondition; the CREATOR (the agent/operator) owns
	// the file, so a 0600 file is readable by exactly the party that wrote
	// it. A genuinely unreadable file surfaces here as a diagnosable error.
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("credfile: read %s: %w", path, err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", fmt.Errorf("credfile: %s is empty (no credential)", path)
	}
	return token, nil
}
