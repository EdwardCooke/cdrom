package pipelinesource

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	xssh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// makeSourceRepo creates a local git repository at dir with a single commit
// containing file.yaml, on branch "main". It uses go-git (not the git binary)
// so the tests are self-contained.
func makeSourceRepo(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, filepath.Join(dir, "file.yaml"), "hello: world\n")
	r, err := gogit.PlainInit(dir, false)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	w, err := r.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	if _, err := w.Add("."); err != nil {
		t.Fatalf("Add: %v", err)
	}
	sig := &object.Signature{Name: "test", Email: "test@example.com", When: time.Now()}
	if _, err := w.Commit("initial", &gogit.CommitOptions{Author: sig}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// Point a "main" branch at the new commit and make HEAD track it, so the
	// tests can fetch branch "main" regardless of the default init branch.
	head, err := r.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if err := r.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), head.Hash())); err != nil {
		t.Fatalf("SetReference main: %v", err)
	}
	if err := r.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))); err != nil {
		t.Fatalf("SetReference HEAD: %v", err)
	}
}

func TestGitClientFetchFromLocalRepo(t *testing.T) {
	src := t.TempDir()
	makeSourceRepo(t, src)

	cache, err := newRepoCache(t.TempDir(), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatalf("newRepoCache: %v", err)
	}
	gc := newGitClient(cache, slog.New(slog.NewTextHandler(os.Stderr, nil)))

	ref, err := ParseReference(fmt.Sprintf("git:%s@main[/file.yaml]", src))
	if err != nil {
		t.Fatalf("ParseReference: %v", err)
	}
	content, version, hash, err := gc.fetch(context.Background(), ref, nil, nil)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if string(content) != "hello: world\n" {
		t.Fatalf("content = %q", content)
	}
	if version == "" {
		t.Fatal("version (commit hash) is empty")
	}
	if hash == "" {
		t.Fatal("hash is empty")
	}

	// A second fetch should be served from the cache (a hit) and return the
	// same content.
	content2, version2, _, err := gc.fetch(context.Background(), ref, nil, nil)
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if string(content2) != "hello: world\n" {
		t.Fatalf("second content = %q", content2)
	}
	if version2 != version {
		t.Fatalf("version changed: %q vs %q", version2, version)
	}
}

func TestGitClientMissingFile(t *testing.T) {
	src := t.TempDir()
	makeSourceRepo(t, src)
	cache, err := newRepoCache(t.TempDir(), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatalf("newRepoCache: %v", err)
	}
	gc := newGitClient(cache, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ref, err := ParseReference(fmt.Sprintf("git:%s@main[/nope.yaml]", src))
	if err != nil {
		t.Fatalf("ParseReference: %v", err)
	}
	if _, _, _, err := gc.fetch(context.Background(), ref, nil, nil); err == nil {
		t.Fatal("missing file should error")
	}
}

// TestCommonKnownHostsParse verifies the embedded common provider keys are a
// valid known_hosts file (so a malformed embedded file is caught at test time,
// not at first use).
func TestCommonKnownHostsParse(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "common-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := f.Write(commonKnownHosts); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := knownhosts.New(f.Name()); err != nil {
		t.Fatalf("embedded common known_hosts does not parse: %v", err)
	}
}

// TestHostKeyCallbackStrict verifies the SSH host-key verification is strict:
// a host NOT in the built-in common set is rejected (no lenient accept-new),
// so only the built-in keys and the configured (runtime / per-credential) keys
// are ever accepted.
func TestHostKeyCallbackStrict(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache, err := newRepoCache(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("newRepoCache: %v", err)
	}
	gc := newGitClient(cache, logger)
	if gc.combinedCallback == nil {
		t.Fatal("combinedCallback is nil; the embedded common keys failed to build")
	}
	// A git-ssh credential with no per-credential host key and no runtime keys
	// falls back to the built-in common set (strict).
	cb, err := gc.hostKeyCallback(&gitCredential{Kind: "git-ssh"}, nil)
	if err != nil {
		t.Fatalf("hostKeyCallback: %v", err)
	}
	// A real (but unknown) public key: the knownhosts callback dereferences the
	// key, so a nil key would panic rather than return a clean rejection.
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	sshPub, err := xssh.NewPublicKey(pub)
	if err != nil {
		t.Fatalf("new public key: %v", err)
	}
	// A non-nil net.Addr and a host:port address: knownhosts dereferences the
	// address (remote.String()) and SplitHostPorts both, so the lookup reaches
	// the real "unknown key" rejection rather than a parse error.
	if err := cb("unknown.example:22", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22}, sshPub); err == nil {
		t.Fatal("a host not in the built-in set should be rejected (strict), got nil")
	}
}

// TestCombinedWithRuntimeReadsOpenFile proves that combinedWithRuntime builds
// a correct callback even though knownhosts.New reads the temp file while it
// is still open (f.Close() is deferred until after the parse). The callback
// must accept the host key it was given and reject a mismatch for the same
// host; if the file were not readable while open, the parse would yield an
// empty database and the accept case would fail.
func TestCombinedWithRuntimeReadsOpenFile(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache, err := newRepoCache(t.TempDir(), logger)
	if err != nil {
		t.Fatalf("newRepoCache: %v", err)
	}
	gc := newGitClient(cache, logger)

	// A known_hosts line for host "test.example" with a specific key.
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	sshPub, err := xssh.NewPublicKey(pub)
	if err != nil {
		t.Fatalf("new public key: %v", err)
	}
	line := "test.example " + sshPub.Type() + " " + base64.StdEncoding.EncodeToString(sshPub.Marshal())

	cb, err := gc.hostKeyCallback(&gitCredential{Kind: "git-ssh"}, []string{line})
	if err != nil {
		t.Fatalf("hostKeyCallback: %v", err)
	}

	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22}
	// The matching host + key must be accepted.
	if err := cb("test.example:22", addr, sshPub); err != nil {
		t.Fatalf("matching host key should be accepted, got: %v", err)
	}
	// A different key for the same host must be rejected (the MITM case).
	otherPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate other key: %v", err)
	}
	otherSSH, err := xssh.NewPublicKey(otherPub)
	if err != nil {
		t.Fatalf("new other public key: %v", err)
	}
	if err := cb("test.example:22", addr, otherSSH); err == nil {
		t.Fatal("a mismatched host key should be rejected, got nil")
	}
}
