package pipelinesource

import (
	"context"
	"crypto/sha256"
	_ "embed" // required by the //go:embed directive below
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	gogithttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gogitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	xssh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
)

// commonKnownHosts holds the built-in SSH host keys for common public git
// providers (F-21.2.4.1), embedded in the binary so these hosts get strict
// host-key verification with no configuration.
//
//go:embed common_known_hosts
var commonKnownHosts []byte

// gitCredential is the plaintext secret material for a git fetch (F-21.3),
// passed in for one call and never persisted or logged.
type gitCredential struct {
	Kind       string // "git-ssh" | "git-https"
	Username   string
	Token      string
	PrivateKey string
	HostKey    string
}

// gitClient fetches a single file at a branch ref from a repository served by
// the on-disk cache (F-21.2.3). It uses go-git (a pure-Go git implementation)
// so the fetcher has no external dependency on the git binary.
type gitClient struct {
	cache  *repoCache
	logger *slog.Logger
	// combinedCallback verifies SSH host keys strictly against the built-in
	// common provider keys. It is nil when the set could not be built, in
	// which case a git-ssh fetch with no other configured host key fails.
	combinedCallback xssh.HostKeyCallback
}

// newGitClient creates a git client over the given cache.
func newGitClient(cache *repoCache, logger *slog.Logger) *gitClient {
	g := &gitClient{cache: cache, logger: logger}
	cb, err := buildCommonKnownHosts()
	if err != nil {
		logger.Error("git: could not build the common known_hosts callback; git-ssh host-key verification will fail", "err", err)
	} else {
		g.combinedCallback = cb
	}
	return g
}

// fetch retrieves the file at ref.Path from ref.Repo at ref.Branch, serving the
// repository from the cache. knownHosts are the runtime-managed SSH host keys
// (F-21.2.4.1, OpenSSH known_hosts lines) the API passes in; they are combined
// with the built-in common provider keys for git-ssh host-key verification. It
// returns the file's content, the branch's commit hash (the version
// identifier), and the content's SHA-256 (hex).
//
// The cache holds a bare object store per repository URL (shared across
// branches). The file is read straight from the objects at the requested
// branch's tip — no worktree is checked out and no reset is performed — so
// switching branches is a cheap incremental fetch and the disk cost is ~1x the
// working tree, deduplicated across branches.
func (g *gitClient) fetch(ctx context.Context, ref *Reference, cred *gitCredential, knownHosts []string) ([]byte, string, string, error) {
	now := time.Now().Unix()
	entries, err := g.cache.loadIndex()
	if err != nil {
		return nil, "", "", err
	}
	entry, hit := entries[ref.Repo]

	var dir string
	if !hit {
		// Cache miss: choose a directory, lock it, clone a bare depth-1 object
		// store for the requested branch, and add an index entry.
		dir, err = g.cache.nextDirName()
		if err != nil {
			return nil, "", "", err
		}
		lock, err := acquireLock(ctx, g.cache.lockPath(dir))
		if err != nil {
			return nil, "", "", fmt.Errorf("git: lock %q: %w", dir, err)
		}
		defer lock.Release()
		if err := g.clone(ctx, ref.Repo, ref.Branch, dir, cred, knownHosts); err != nil {
			return nil, "", "", fmt.Errorf("git: clone %q: %w", ref.Repo, err)
		}
		if err := g.cache.updateIndex(func(m map[string]*cacheEntry) error {
			m[ref.Repo] = &cacheEntry{Dir: dir, URL: ref.Repo, LastAccessed: now, LastFetched: now}
			return nil
		}); err != nil {
			return nil, "", "", err
		}
	} else {
		// Cache hit: lock the existing store, then re-read the index under the
		// lock so we act on the freshest state (effective across replicas
		// sharing the cache).
		dir = entry.Dir
		lock, err := acquireLock(ctx, g.cache.lockPath(dir))
		if err != nil {
			return nil, "", "", fmt.Errorf("git: lock %q: %w", dir, err)
		}
		defer lock.Release()
		fresh, err := g.cache.loadIndex()
		if err != nil {
			return nil, "", "", err
		}
		if _, ok := fresh[ref.Repo]; !ok {
			// The entry was pruned while we waited for the lock; clone fresh.
			if err := g.clone(ctx, ref.Repo, ref.Branch, dir, cred, knownHosts); err != nil {
				return nil, "", "", fmt.Errorf("git: clone %q: %w", ref.Repo, err)
			}
		} else {
			// Fetch the requested branch into the shared object store. This is
			// incremental (a no-op when the branch is unchanged) and works for a
			// different branch than the one the store was cloned with.
			if err := g.refetch(ctx, ref.Repo, ref.Branch, dir, cred, knownHosts); err != nil {
				// Resync on fetch failure (F-21.2.3.2): discard the store and
				// re-clone from upstream.
				g.logger.Warn("git: refetch failed; resyncing", "repo", ref.Repo, "err", err)
				if err := g.resync(ctx, ref.Repo, ref.Branch, dir, cred, knownHosts); err != nil {
					return nil, "", "", fmt.Errorf("git: resync %q: %w", ref.Repo, err)
				}
			}
		}
		_ = g.cache.updateIndex(func(m map[string]*cacheEntry) error {
			if e, ok := m[ref.Repo]; ok {
				e.LastAccessed = now
				e.LastFetched = now
			}
			return nil
		})
	}

	// Read the file from the object store at the requested branch's tip (no
	// worktree). The branch's commit hash is the version identifier.
	content, commit, err := g.readBranchFile(dir, ref.Branch, ref.Path)
	if err != nil {
		return nil, "", "", err
	}
	sum := sha256.Sum256(content)
	return content, commit, hex.EncodeToString(sum[:]), nil
}

// clone creates a bare, shallow (depth 1) object store for repo at branch in
// dir. The store holds only the objects (no worktree), so switching branches
// later is a cheap incremental fetch and the disk cost is ~1x the working
// tree. The destination is cleared first so a stale directory (e.g. from a
// pruned entry) cannot make the clone fail.
func (g *gitClient) clone(ctx context.Context, repo, branch, dir string, cred *gitCredential, knownHosts []string) error {
	dest := filepath.Join(g.cache.root, dir)
	if err := os.RemoveAll(dest); err != nil {
		return fmt.Errorf("git: clear %q: %w", dir, err)
	}
	auth, err := g.gitAuth(cred, knownHosts)
	if err != nil {
		return err
	}
	if _, err := gogit.PlainCloneContext(ctx, dest, true, &gogit.CloneOptions{
		URL:           repo,
		ReferenceName: plumbing.NewBranchReferenceName(branch),
		SingleBranch:  true,
		Depth:         1,
		Auth:          auth,
	}); err != nil {
		return fmt.Errorf("git: clone %q: %w", repo, err)
	}
	return nil
}

// refetch fetches the branch ref into the bare object store at dir. It is
// incremental: when the branch is unchanged the server sends only the ref
// advertisement (no objects), and when it moved it sends only the new commits.
// It works for a different branch than the one the store was cloned with (the
// explicit refspec overrides the clone's single-branch config). No worktree is
// checked out and no reset is performed — the file is read from the objects.
func (g *gitClient) refetch(ctx context.Context, repo, branch, dir string, cred *gitCredential, knownHosts []string) error {
	dest := filepath.Join(g.cache.root, dir)
	r, err := gogit.PlainOpen(dest)
	if err != nil {
		return fmt.Errorf("git: open %q: %w", dir, err)
	}
	auth, err := g.gitAuth(cred, knownHosts)
	if err != nil {
		return err
	}
	// Fetch the branch ref into its remote-tracking ref. The leading "+" allows
	// a force push to be absorbed. "Already up-to-date" is a success (nothing
	// new upstream).
	err = r.FetchContext(ctx, &gogit.FetchOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{config.RefSpec("+refs/heads/" + branch + ":refs/remotes/origin/" + branch)},
		Auth:       auth,
	})
	if err != nil && !errors.Is(err, gogit.NoErrAlreadyUpToDate) {
		return fmt.Errorf("git: fetch %q: %w", repo, err)
	}
	return nil
}

// resync discards the object store at dir and re-clones the repository from
// upstream (a full resync of the requested branch).
func (g *gitClient) resync(ctx context.Context, repo, branch, dir string, cred *gitCredential, knownHosts []string) error {
	dest := filepath.Join(g.cache.root, dir)
	if err := os.RemoveAll(dest); err != nil {
		return fmt.Errorf("remove store: %w", err)
	}
	return g.clone(ctx, repo, branch, dir, cred, knownHosts)
}

// readBranchFile reads the file at path (a repository-absolute path, e.g.
// "/some/file.yaml") from the object store at dir, at the requested branch's
// tip commit. It returns the file's content and the branch's commit hash (the
// version identifier). No worktree is involved: the content comes straight
// from the blob objects.
func (g *gitClient) readBranchFile(dir, branch, path string) ([]byte, string, error) {
	dest := filepath.Join(g.cache.root, dir)
	r, err := gogit.PlainOpen(dest)
	if err != nil {
		return nil, "", fmt.Errorf("git: open %q: %w", dir, err)
	}
	ref, err := r.Reference(plumbing.NewRemoteReferenceName("origin", branch), true)
	if err != nil {
		return nil, "", fmt.Errorf("git: resolve %q: %w", branch, err)
	}
	commit, err := object.GetCommit(r.Storer, ref.Hash())
	if err != nil {
		return nil, "", fmt.Errorf("git: get commit %s: %w", ref.Hash(), err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, "", fmt.Errorf("git: get tree: %w", err)
	}
	f, err := tree.File(strings.TrimPrefix(path, "/"))
	if err != nil {
		return nil, "", fmt.Errorf("git: file %q: %w", path, err)
	}
	content, err := f.Contents()
	if err != nil {
		return nil, "", fmt.Errorf("git: read %q: %w", path, err)
	}
	return []byte(content), commit.Hash.String(), nil
}

// gitAuth builds the go-git auth method for a git credential. It returns a nil
// auth for a public repository (no credential). The credential material is
// never logged. knownHosts are the runtime-managed SSH host keys (F-21.2.4.1)
// combined with the built-in common provider keys for git-ssh host-key
// verification.
func (g *gitClient) gitAuth(cred *gitCredential, knownHosts []string) (transport.AuthMethod, error) {
	if cred == nil || cred.Kind == "" {
		return nil, nil
	}
	switch cred.Kind {
	case "git-https":
		if cred.Token == "" {
			return nil, nil
		}
		user := cred.Username
		if user == "" {
			user = "x-access-token"
		}
		// HTTP basic auth — equivalent to the old https://user:token@host form.
		return &gogithttp.BasicAuth{Username: user, Password: cred.Token}, nil
	case "git-ssh":
		user := cred.Username
		if user == "" {
			user = gogitssh.DefaultUsername // "git"
		}
		pk, err := gogitssh.NewPublicKeys(user, []byte(cred.PrivateKey), "")
		if err != nil {
			return nil, fmt.Errorf("git-ssh: parse key: %w", err)
		}
		cb, err := g.hostKeyCallback(cred, knownHosts)
		if err != nil {
			return nil, err
		}
		pk.HostKeyCallback = cb
		return pk, nil
	default:
		return nil, fmt.Errorf("git: unknown credential kind %q", cred.Kind)
	}
}

// hostKeyCallback returns the SSH host-key verification callback for a git-ssh
// credential. When an expected host key is provided it is verified strictly (it
// takes precedence over the combined set). Otherwise the combined set (built-in
// common provider keys + the runtime known_hosts the API passes in) is used:
// only a host whose key is in the set is accepted; a host NOT in the set is
// rejected, and a key MISMATCH for a known host is rejected (the MITM case).
func (g *gitClient) hostKeyCallback(cred *gitCredential, knownHosts []string) (xssh.HostKeyCallback, error) {
	if cred.HostKey != "" {
		f, err := os.CreateTemp("", "cdrom-git-known_hosts-*")
		if err != nil {
			return nil, fmt.Errorf("git-ssh: create known_hosts: %w", err)
		}
		name := f.Name()
		// The file is disposable: knownhosts.New reads it into memory and the
		// callback operates on the in-memory database. Close and remove it on
		// every exit path (best effort). Close runs before Remove so the file
		// is not open when it is deleted (required on Windows).
		defer func() {
			f.Close()
			os.Remove(name)
		}()
		if _, err := f.WriteString(cred.HostKey + "\n"); err != nil {
			return nil, fmt.Errorf("git-ssh: write known_hosts: %w", err)
		}
		cb, err := knownhosts.New(name)
		if err != nil {
			return nil, fmt.Errorf("git-ssh: known_hosts: %w", err)
		}
		return cb, nil
	}
	if len(knownHosts) > 0 {
		// Combine the built-in common keys with the runtime keys the API
		// passes in.
		return g.combinedWithRuntime(knownHosts)
	}
	if g.combinedCallback != nil {
		return g.combinedCallback, nil
	}
	return nil, fmt.Errorf("git-ssh: no host keys configured; refusing to connect to an unverified host (add the host key via the API's /api/ssh-known-hosts or to the credential)")
}

// combinedWithRuntime returns a strict host-key callback over the combined set
// (the built-in common provider keys + the runtime known_hosts lines). The
// combined content is written to a temp file, parsed into memory by
// knownhosts.New, and removed on return — the callback operates on the
// in-memory database, so no file is retained.
func (g *gitClient) combinedWithRuntime(runtime []string) (xssh.HostKeyCallback, error) {
	f, err := os.CreateTemp("", "cdrom-git-known_hosts-*")
	if err != nil {
		return nil, fmt.Errorf("known_hosts: create temp file: %w", err)
	}
	name := f.Name()
	// The file is disposable: knownhosts.New reads it into memory and the
	// callback operates on the in-memory database. Close and remove it on
	// every exit path (best effort) so we don't leak a file per fetch. Close
	// runs before Remove so the file is not open when it is deleted (required
	// on Windows). knownhosts.New reads the file while it is still open here,
	// which is safe because os.File writes are unbuffered (the data is on
	// disk before Close).
	defer func() {
		f.Close()
		if err := os.Remove(name); err != nil {
			g.logger.Warn("git: could not remove combined known_hosts temp file", "file", name, "err", err)
		}
	}()
	if _, err := f.Write(commonKnownHosts); err != nil {
		return nil, fmt.Errorf("known_hosts: write embedded keys: %w", err)
	}
	for _, line := range runtime {
		if line == "" {
			continue
		}
		if _, err := f.Write([]byte("\n" + line + "\n")); err != nil {
			return nil, fmt.Errorf("known_hosts: append runtime keys: %w", err)
		}
	}
	cb, err := knownhosts.New(name)
	if err != nil {
		return nil, fmt.Errorf("known_hosts: parse: %w", err)
	}
	return cb, nil
}

// buildCommonKnownHosts writes the embedded common provider keys to a temp
// file, parses them into memory via knownhosts.New, removes the file on
// return, and returns a strict host-key callback over the in-memory database.
func buildCommonKnownHosts() (xssh.HostKeyCallback, error) {
	f, err := os.CreateTemp("", "cdrom-git-common-known_hosts-*")
	if err != nil {
		return nil, fmt.Errorf("known_hosts: create temp file: %w", err)
	}
	name := f.Name()
	// The file is disposable: knownhosts.New reads it into memory and the
	// callback operates on the in-memory database. Close and remove it on
	// every exit path (best effort). Close runs before Remove so the file is
	// not open when it is deleted (required on Windows). knownhosts.New reads
	// the file while it is still open here, which is safe because os.File
	// writes are unbuffered (the data is on disk before Close).
	defer func() {
		f.Close()
		os.Remove(name)
	}()
	if _, err := f.Write(commonKnownHosts); err != nil {
		return nil, fmt.Errorf("known_hosts: write embedded keys: %w", err)
	}
	cb, err := knownhosts.New(name)
	if err != nil {
		return nil, fmt.Errorf("known_hosts: parse: %w", err)
	}
	return cb, nil
}

// toGitCredential converts a proto SourceCredential to a gitCredential (the
// git kinds only). It returns nil for a non-git or empty credential.
func toGitCredential(c *pipelinesourcepb.SourceCredential) *gitCredential {
	if c == nil || c.GetKind() == "" {
		return nil
	}
	switch c.GetKind() {
	case "git-ssh", "git-https":
		return &gitCredential{
			Kind:       c.GetKind(),
			Username:   c.GetUsername(),
			Token:      c.GetToken(),
			PrivateKey: c.GetPrivateKey(),
			HostKey:    c.GetHostKey(),
		}
	default:
		return nil
	}
}
