package pipelinesource

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// makeTwoCommitSource creates a source repository at dir whose HEAD tracks
// "main" and which has two commits (so the source has more than one commit of
// history). It returns the repository and the second commit's hash.
func makeTwoCommitSource(t *testing.T, dir string) (*gogit.Repository, plumbing.Hash) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, filepath.Join(dir, "file.yaml"), "hello: world\n")
	r, err := gogit.PlainInit(dir, false)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	if err := r.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))); err != nil {
		t.Fatalf("set HEAD -> main: %v", err)
	}
	w, err := r.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	if _, err := w.Add("."); err != nil {
		t.Fatalf("Add: %v", err)
	}
	sig := &object.Signature{Name: "test", Email: "test@example.com", When: time.Now()}
	if _, err := w.Commit("first", &gogit.CommitOptions{Author: sig}); err != nil {
		t.Fatalf("Commit first: %v", err)
	}
	writeFile(t, filepath.Join(dir, "file.yaml"), "hello: world\nv2\n")
	if _, err := w.Add("."); err != nil {
		t.Fatalf("Add: %v", err)
	}
	second, err := w.Commit("second", &gogit.CommitOptions{Author: sig})
	if err != nil {
		t.Fatalf("Commit second: %v", err)
	}
	return r, second
}

// TestGitClientRefetchAfterRemoteAdvances verifies that a fetch on a cached
// (shallow) store picks up multiple new commits on the remote: the content is
// the latest, the version is the new tip, and the store stays shallow
// (incremental — it does not pull the full history).
func TestGitClientRefetchAfterRemoteAdvances(t *testing.T) {
	src := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r, _ := makeTwoCommitSource(t, src)
	w, _ := r.Worktree()
	sig := &object.Signature{Name: "test", Email: "test@example.com", When: time.Now()}

	cache, err := newRepoCache(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("newRepoCache: %v", err)
	}
	gc := newGitClient(cache, logger)
	ref, err := ParseReference(fmt.Sprintf("git:%s@main[/file.yaml]", src))
	if err != nil {
		t.Fatalf("ParseReference: %v", err)
	}

	// First fetch: shallow clone.
	content, version1, _, err := gc.fetch(context.Background(), ref, nil, nil)
	if err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	if string(content) != "hello: world\nv2\n" {
		t.Fatalf("first fetch content = %q", content)
	}

	// Advance the source by 3 more commits (v3, v4, v5).
	for i := 3; i <= 5; i++ {
		writeFile(t, filepath.Join(src, "file.yaml"), fmt.Sprintf("hello: world\nv%d\n", i))
		if _, err := w.Add("."); err != nil {
			t.Fatalf("Add v%d: %v", i, err)
		}
		if _, err := w.Commit(fmt.Sprintf("v%d", i), &gogit.CommitOptions{Author: sig}); err != nil {
			t.Fatalf("Commit v%d: %v", i, err)
		}
	}
	newTip, err := r.Reference(plumbing.NewBranchReferenceName("main"), true)
	if err != nil {
		t.Fatalf("Reference main: %v", err)
	}

	// Second fetch: the store is cached, so this takes the fetch path. Every
	// request fetches (no debounce), so the new commits are picked up.
	content2, version2, _, err := gc.fetch(context.Background(), ref, nil, nil)
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if string(content2) != "hello: world\nv5\n" {
		t.Fatalf("second fetch content = %q, want v5", content2)
	}
	if version2 != newTip.Hash().String() {
		t.Fatalf("second fetch version = %s, want new tip %s", version2, newTip.Hash())
	}
	if version1 == version2 {
		t.Fatal("version did not change after the remote advanced")
	}

	// The store is still shallow (a shallow boundary file exists).
	entries, err := cache.loadIndex()
	if err != nil {
		t.Fatalf("loadIndex: %v", err)
	}
	store := filepath.Join(cache.root, entries[ref.Repo].Dir)
	if _, err := os.Stat(filepath.Join(store, "shallow")); err != nil {
		t.Fatalf("store is no longer shallow after refetch: %v", err)
	}

	// The history is the original tip + the 3 new commits = 4 (incremental,
	// not the full 5-commit history).
	cr, err := gogit.PlainOpen(store)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	branchRef, err := cr.Reference(plumbing.NewRemoteReferenceName("origin", "main"), true)
	if err != nil {
		t.Fatalf("Reference origin/main: %v", err)
	}
	it, err := cr.Log(&gogit.LogOptions{From: branchRef.Hash()})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	count := 0
	for {
		_, err := it.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != 4 {
		t.Fatalf("history should be the original tip + 3 new commits = 4, got %d", count)
	}
	t.Logf("refetch: history depth = %d (tip=%s)", count, branchRef.Hash())
}

// TestGitClientBranchSwitch verifies that fetching a DIFFERENT branch than the
// one the store was cloned with returns that branch's file and tip — the case
// the old worktree+debounce design got wrong (it served the stale branch's
// content within the debounce window).
func TestGitClientBranchSwitch(t *testing.T) {
	src := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Source repo with two branches: main (file=main-v1) and test (file=test-v1).
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	r, err := gogit.PlainInit(src, false)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	if err := r.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))); err != nil {
		t.Fatalf("set HEAD -> main: %v", err)
	}
	w, err := r.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	sig := &object.Signature{Name: "test", Email: "test@example.com", When: time.Now()}

	writeFile(t, filepath.Join(src, "file.yaml"), "main-v1\n")
	if _, err := w.Add("."); err != nil {
		t.Fatalf("Add: %v", err)
	}
	cMain, err := w.Commit("main v1", &gogit.CommitOptions{Author: sig})
	if err != nil {
		t.Fatalf("Commit main: %v", err)
	}
	// Branch "test" off main with different content.
	if err := r.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("test"), cMain)); err != nil {
		t.Fatalf("create test branch: %v", err)
	}
	if err := r.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("test"))); err != nil {
		t.Fatalf("set HEAD -> test: %v", err)
	}
	writeFile(t, filepath.Join(src, "file.yaml"), "test-v1\n")
	if _, err := w.Add("."); err != nil {
		t.Fatalf("Add: %v", err)
	}
	cTest, err := w.Commit("test v1", &gogit.CommitOptions{Author: sig})
	if err != nil {
		t.Fatalf("Commit test: %v", err)
	}

	cache, err := newRepoCache(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("newRepoCache: %v", err)
	}
	gc := newGitClient(cache, logger)

	// 1) Fetch main (clone).
	refMain, _ := ParseReference(fmt.Sprintf("git:%s@main[/file.yaml]", src))
	c1, v1, _, err := gc.fetch(context.Background(), refMain, nil, nil)
	if err != nil {
		t.Fatalf("fetch main: %v", err)
	}
	if string(c1) != "main-v1\n" || v1 != cMain.String() {
		t.Fatalf("fetch main: content=%q version=%s, want main-v1 / %s", c1, v1, cMain)
	}

	// 2) Fetch test IMMEDIATELY (no debounce to wait out). It must return
	// test's file and test's tip, not main's.
	refTest, _ := ParseReference(fmt.Sprintf("git:%s@test[/file.yaml]", src))
	c2, v2, _, err := gc.fetch(context.Background(), refTest, nil, nil)
	if err != nil {
		t.Fatalf("fetch test: %v", err)
	}
	if string(c2) != "test-v1\n" {
		t.Fatalf("fetch test content = %q, want test-v1 (branch switch served the wrong branch)", c2)
	}
	if v2 != cTest.String() {
		t.Fatalf("fetch test version = %s, want test tip %s", v2, cTest)
	}

	// 3) Fetch main again — it must return main's file (no cross-branch leak).
	c3, v3, _, err := gc.fetch(context.Background(), refMain, nil, nil)
	if err != nil {
		t.Fatalf("fetch main again: %v", err)
	}
	if string(c3) != "main-v1\n" || v3 != cMain.String() {
		t.Fatalf("fetch main again: content=%q version=%s, want main-v1 / %s", c3, v3, cMain)
	}
}
