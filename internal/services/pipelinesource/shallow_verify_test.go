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

// TestGitClientShallowClone verifies the clone is a shallow (depth 1) bare
// object store: it has a shallow boundary file and its history contains only
// the tip commit, even when the source repository has several commits.
func TestGitClientShallowClone(t *testing.T) {
	src := t.TempDir()
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, filepath.Join(src, "file.yaml"), "hello: world\n")
	r, err := gogit.PlainInit(src, false)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	// Make HEAD track "main" so the commits advance the branch.
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
	// A second commit so the source has more than one commit of history.
	writeFile(t, filepath.Join(src, "file.yaml"), "hello: world\nv2\n")
	if _, err := w.Add("."); err != nil {
		t.Fatalf("Add: %v", err)
	}
	second, err := w.Commit("second", &gogit.CommitOptions{Author: sig})
	if err != nil {
		t.Fatalf("Commit second: %v", err)
	}

	cache, err := newRepoCache(t.TempDir(), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatalf("newRepoCache: %v", err)
	}
	gc := newGitClient(cache, slog.New(slog.NewTextHandler(os.Stderr, nil)))

	ref, err := ParseReference(fmt.Sprintf("git:%s@main[/file.yaml]", src))
	if err != nil {
		t.Fatalf("ParseReference: %v", err)
	}
	content, version, _, err := gc.fetch(context.Background(), ref, nil, nil)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if string(content) != "hello: world\nv2\n" {
		t.Fatalf("content = %q", content)
	}
	if version != second.String() {
		t.Fatalf("version = %s, want the branch tip %s", version, second)
	}

	// Find the object-store directory from the cache index.
	entries, err := cache.loadIndex()
	if err != nil {
		t.Fatalf("loadIndex: %v", err)
	}
	entry, ok := entries[ref.Repo]
	if !ok {
		t.Fatal("cache entry missing after fetch")
	}
	store := filepath.Join(cache.root, entry.Dir)

	// A shallow bare store records its shallow boundary at <store>/shallow.
	if _, err := os.Stat(filepath.Join(store, "shallow")); err != nil {
		t.Fatalf("expected a shallow store (shallow file missing): %v", err)
	}

	// The store's history (from the branch tip) must contain only the tip
	// commit.
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
	if count != 1 {
		t.Fatalf("shallow store should have exactly 1 commit of history, got %d", count)
	}
}
