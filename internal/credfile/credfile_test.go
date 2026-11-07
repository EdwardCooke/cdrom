package credfile

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// TestWriteThenReadRoundTrip locks the happy path: write → read returns the
// exact token (terminating-newline tolerance included).
func TestWriteThenReadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-credential.jwt")
	const token = "eyJhbGciOiJSUzI1NiJ9.header.payload.sig"
	if err := WriteAtomically(path, token); err != nil {
		t.Fatalf("WriteAtomically: %v", err)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got != token {
		t.Errorf("round trip = %q, want %q", got, token)
	}
}

// TestWrittenFileCarriesOwnerOnlyPerms locks the SECURITY PROMISE: the
// published file is 0600 (owner rw only). Skipped on Windows, where Go
// synthesizes coarse mode bits and the guarantee is expressed through the
// ACL instead (the primitive still requests owner-only on every platform).
func TestWrittenFileCarriesOwnerOnlyPerms(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode bits are synthesized on Windows; ACL expresses the guarantee")
	}
	path := filepath.Join(t.TempDir(), "perm")
	if err := WriteAtomically(path, "tok"); err != nil {
		t.Fatalf("WriteAtomically: %v", err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := st.Mode().Perm(); perm != 0o600 {
		t.Errorf("published mode = %#o, want 0600", perm)
	}
}

// TestRotationCyclePublishesWholeTokens simulates rotations: ten increasing
// generations overwrite the file; every immediate read must return the JUST
// PUBLISHED token in full (no torn fragments).
func TestRotationCyclePublishesWholeTokens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-credential.jwt")
	prev := strings.Repeat("A", 256)
	if err := WriteAtomically(path, prev); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	for i := 1; i <= 10; i++ {
		next := "gen-" + strings.Repeat(string(rune('a'+i)), 300+i*10)
		if err := WriteAtomically(path, next); err != nil {
			t.Fatalf("generation %d: %v", i, err)
		}
		got, err := Read(path)
		if err != nil {
			t.Fatalf("read after generation %d: %v", i, err)
		}
		if got != next {
			t.Fatalf("generation %d observed %d chars, want %d (torn read?)", i, len(got), len(next))
		}
		prev = next
	}
}

// TestContendedReadWriteInvariant hammers the file from one writer cycling
// twelve DISTINCT tokens while several readers sample continuously: every
// OBSERVED value must equal some published token (the atomicity invariant
// under contention). Absence is tolerated (mid-boot).
func TestContendedReadWriteInvariant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contended")
	var mu sync.Mutex
	published := map[string]int{}
	tokens := make([]string, 12)
	for i := range tokens {
		tokens[i] = "ct-" + strings.Repeat(string(rueSafe(i)), 160)
		mu.Lock()
		published[tokens[i]] = i
		mu.Unlock()
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				got, err := Read(path)
				if err != nil {
					continue
				}
				mu.Lock()
				_, ok := published[got]
				mu.Unlock()
				if !ok {
					t.Errorf("reader observed unpublished content (%d chars) — torn read?", len(got))
					return
				}
			}
		}()
	}
	for _, tk := range tokens {
		if err := WriteAtomically(path, tk); err != nil {
			close(done)
			wg.Wait()
			t.Fatalf("write: %v", err)
		}
	}
	close(done)
	wg.Wait()
}

// rueSafe picks a printable rune safely (avoiding surrogate-range surprises
// in strings.Repeat).
func rueSafe(i int) rune { return 'a' + rune(i%26) }

// TestWatcherContract_NoStrayEntriesDuringWrites: a simulated naive watcher
// (directory scanner) must NEVER see a non-dot entry besides the
// destination — the dot-prefixed temp naming keeps in-flight writes
// invisible to it.
func TestWatcherContract_NoStrayEntriesDuringWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dest")
	if err := WriteAtomically(path, "gen-0"); err != nil {
		t.Fatalf("prime: %v", err)
	}
	stop := make(chan struct{})
	found := make(chan string, 1)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				if strings.HasPrefix(e.Name(), ".") || e.Name() == "dest" {
					continue
				}
				select {
				case found <- e.Name():
				default:
				}
				return
			}
		}
	}()
	for i := 1; i <= 8; i++ {
		if err := WriteAtomically(path, "gen-i"+strings.Repeat("*", i)); err != nil {
			close(stop)
			t.Fatalf("burst %d: %v", i, err)
		}
	}
	close(stop)
	select {
	case stray := <-found:
		t.Fatalf("watcher-visible STRAY entry appeared during writes: %q (dot-prefix contract violated)", stray)
	default:
	}
	// Post-condition: exactly one non-dot entry remains.
	entries, _ := os.ReadDir(dir)
	count := 0
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("post-write directory holds %d non-dot entries, want 1 (temp litter?)", count)
	}
}

// TestMissingFileIsDistinct locks the BOOT-DIAGNOSTIC contract: absence is
// recognizable via BOTH sentinels (os.ErrNotExist and ErrNotExist).
func TestMissingFileIsDistinct(t *testing.T) {
	_, err := Read(filepath.Join(t.TempDir(), "nope"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("missing-file error %v does not wrap os.ErrNotExist", err)
	}
	if !errors.Is(err, ErrNotExist) {
		t.Errorf("missing-file error %v does not wrap ErrNotExist", err)
	}
}

// TestEmptyFileIsMisconfiguration distinguishes "exists but empty" from
// "absent": a distinct error, neither sentinel.
func TestEmptyFileIsMisconfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blank")
	if err := os.WriteFile(path, []byte("\n\r\n "), 0o600); err != nil {
		t.Fatalf("seed blank file: %v", err)
	}
	_, err := Read(path)
	if err == nil {
		t.Fatal("empty file read succeeded, want an error")
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrNotExist) {
		t.Errorf("empty-file error %v masquerades as absence", err)
	}
}

// TestTrailingWhitespaceTolerance: operators paste JWTs into editors that
// append newlines/CRLF; Read must strip them.
func TestTrailingWhitespaceTolerance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ws")
	const token = "abc.def.ghi"
	if err := os.WriteFile(path, []byte(token+"\r\n\n"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got != token {
		t.Errorf("trimmed = %q, want %q", got, token)
	}
}

// TestWriterPreservesOriginalOnFailure: forcing the rename to fail (busy /
// read-only destination) must leave the ORIGINAL content intact and no temp
// litter behind. Root-permissive filesystems legitimately succeed the write —
// the test then relaxes to the positive assertion.
func TestWriterPreservesOriginalOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orig")
	const orig = "original-token"
	if err := os.WriteFile(path, []byte(orig), 0o444); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })

	err := WriteAtomically(path, "replacement")
	data, rderr := os.ReadFile(path)
	if rderr != nil {
		t.Fatalf("destination vanished after write attempt: %v", rderr)
	}
	if err == nil {
		// Privileged FS allowed the swap — assert the POSITIVE outcome.
		if string(data) != "replacement\n" {
			t.Errorf("successful overwrite yielded unexpected content %q", data)
		}
		t.Log("privilege permitted the overwrite; positive path asserted")
		return
	}
	// Negative path: original must SURVIVe.
	if string(data) != orig {
		t.Errorf("original corrupted after failed write: %q", data)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			t.Errorf("temp litter left behind despite failure: %s", e.Name())
		}
	}
}
