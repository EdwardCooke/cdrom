// Package sourceref owns the source reference format (F-21.2.2): a
// "<type>:<spec>" string that names the kind of a source (git / http / file)
// and the kind-specific locator. It is shared by the source-fetcher service
// (which parses a reference to dispatch a fetch) and the pipeline compiler
// (F-21.4), which resolves a relative import locator against the current
// file's reference.
package sourceref

import (
	"fmt"
	"net/url"
	"strings"
)

// The reference kinds (F-21.2.2). A reference is a "<type>:<spec>" string; the
// type selects the kind and the spec carries the kind-specific locator.
const (
	KindGit  = "git"
	KindHTTP = "http"
	KindFile = "file"
)

// Reference is a parsed source reference: the kind and the kind-specific
// locator. For git, Repo/Branch/Path are set; for http, URL is set; for file,
// Path is set.
type Reference struct {
	Kind   string
	Repo   string // git: the git remote
	Branch string // git: the branch ref to track
	Path   string // git: path in repo / file: path under the root
	URL    string // http: the endpoint URL
}

// ParseReference parses a "<type>:<spec>" reference into a Reference. It
// returns a clear error for an unknown type or a malformed reference.
func ParseReference(ref string) (*Reference, error) {
	if ref == "" {
		return nil, fmt.Errorf("reference: empty reference")
	}
	// The type is the text before the first ":". The spec is the remainder.
	// (A git SSH remote uses "user@host:..." but that is inside the spec, after
	// the leading "git:"; the type itself never contains a colon.)
	idx := strings.IndexByte(ref, ':')
	if idx < 0 {
		return nil, fmt.Errorf("reference: %q is missing the <type>: separator", ref)
	}
	kind := ref[:idx]
	spec := ref[idx+1:]
	switch kind {
	case KindGit:
		return parseGitReference(spec)
	case KindHTTP:
		return parseHTTPReference(spec)
	case KindFile:
		return parseFileReference(spec)
	default:
		return nil, fmt.Errorf("reference: unknown type %q (want git, http, or file)", kind)
	}
}

// String renders the reference back into its "<type>:<spec>" form. It is the
// inverse of ParseReference (round-trips a parsed reference).
func (r *Reference) String() string {
	switch r.Kind {
	case KindGit:
		return "git:" + escape(r.Repo) + "@" + escape(r.Branch) + "[" + escape(r.Path) + "]"
	case KindHTTP:
		return "http:" + r.URL
	case KindFile:
		return "file:" + r.Path
	default:
		return r.Kind
	}
}

// ResolveRelative resolves a locator against this reference. If locator is a
// full reference (a "<type>:<spec>" string), it is parsed and returned as-is.
// Otherwise it is treated as a path relative to THIS reference's file's
// directory: for a git reference, relative to the file's path in the repo (the
// fetcher fetches the referenced file from the same repo/branch); for an http
// reference, relative to the file's URL (standard URL resolution); for a file
// reference, relative to the file's path under the root.
func (r *Reference) ResolveRelative(locator string) (*Reference, error) {
	if locator == "" {
		return nil, fmt.Errorf("reference: empty locator")
	}
	// A full reference is used directly.
	if ref, err := ParseReference(locator); err == nil {
		return ref, nil
	}
	dir := dirname(r.Path)
	switch r.Kind {
	case KindGit:
		// Repo paths are absolute within the repo: guarantee a leading slash.
		p := joinPath(dir, locator)
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
		return &Reference{Kind: KindGit, Repo: r.Repo, Branch: r.Branch, Path: p}, nil
	case KindHTTP:
		return &Reference{Kind: KindHTTP, URL: joinURL(r.URL, locator)}, nil
	case KindFile:
		return &Reference{Kind: KindFile, Path: joinPath(dir, locator)}, nil
	default:
		return nil, fmt.Errorf("reference: cannot resolve a relative locator against kind %q", r.Kind)
	}
}

// dirname returns the directory portion of a slash-separated path (everything
// up to, excluding, the last "/"). A path with no "/" yields "" (the root).
func dirname(p string) string {
	idx := strings.LastIndexByte(p, '/')
	if idx < 0 {
		return ""
	}
	return p[:idx]
}

// parseGitReference parses a git spec of the form <repo>@<branch>[<path>],
// honoring backslash escapes. The first unescaped '@' is the branch separator;
// the first unescaped '[' after it opens the path; the first unescaped ']'
// after the opener closes it; the reference must end immediately after the
// closing ']'.
func parseGitReference(spec string) (*Reference, error) {
	// Walk the spec left to right, tracking escapes, to find the first
	// unescaped '@' (the branch separator).
	at := -1
	for i := 0; i < len(spec); i++ {
		switch spec[i] {
		case '\\':
			i++ // skip the escaped character
		case '@':
			at = i
			i = len(spec) // stop at the first unescaped '@'
		}
	}
	if at < 0 {
		return nil, fmt.Errorf("reference: git spec %q is missing the branch separator '@'", spec)
	}
	repo, err := unescape(spec[:at])
	if err != nil {
		return nil, fmt.Errorf("reference: git repo: %w", err)
	}
	rest := spec[at+1:]

	// Find the first unescaped '[' (the path opener).
	open := -1
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '\\':
			i++
		case '[':
			open = i
			i = len(rest)
		}
	}
	if open < 0 {
		return nil, fmt.Errorf("reference: git spec %q is missing the path opener '['", spec)
	}
	branch, err := unescape(rest[:open])
	if err != nil {
		return nil, fmt.Errorf("reference: git branch: %w", err)
	}
	afterOpen := rest[open+1:]

	// Find the first unescaped ']' (the path closer).
	close := -1
	for i := 0; i < len(afterOpen); i++ {
		switch afterOpen[i] {
		case '\\':
			i++
		case ']':
			close = i
			i = len(afterOpen)
		}
	}
	if close < 0 {
		return nil, fmt.Errorf("reference: git spec %q is missing the path closer ']'", spec)
	}
	if tail := afterOpen[close+1:]; tail != "" {
		return nil, fmt.Errorf("reference: git spec %q has trailing content %q after the closing ']'", spec, tail)
	}
	path, err := unescape(afterOpen[:close])
	if err != nil {
		return nil, fmt.Errorf("reference: git path: %w", err)
	}
	if repo == "" {
		return nil, fmt.Errorf("reference: git spec %q has an empty repository", spec)
	}
	if branch == "" {
		return nil, fmt.Errorf("reference: git spec %q has an empty branch", spec)
	}
	return &Reference{Kind: KindGit, Repo: repo, Branch: branch, Path: path}, nil
}

// parseHTTPReference parses an http spec: the full URL of the endpoint that
// serves the file.
func parseHTTPReference(spec string) (*Reference, error) {
	if spec == "" {
		return nil, fmt.Errorf("reference: http spec is empty")
	}
	return &Reference{Kind: KindHTTP, URL: spec}, nil
}

// parseFileReference parses a file spec: the path of the file, resolved
// against the fetcher's configured local-filesystem root (F-21.2.4.3).
func parseFileReference(spec string) (*Reference, error) {
	if spec == "" {
		return nil, fmt.Errorf("reference: file spec is empty")
	}
	return &Reference{Kind: KindFile, Path: spec}, nil
}

// escape backslash-escapes the structurally significant characters (@, [, ],
// and \) so they appear literally in a reference's spec. It is the inverse of
// unescape.
func escape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '@', '[', ']', '\\':
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	return b.String()
}

// Unescape resolves backslash escapes in s. The structurally significant
// characters (@, [, ], and \) are escaped to appear literally; any other
// escaped character is returned as the character itself. It is exported so the
// source-fetcher's reference-parser tests can exercise it directly.
func Unescape(s string) (string, error) {
	return unescape(s)
}

// unescape resolves backslash escapes in s. The structurally significant
// characters (@, [, ], and \) are escaped to appear literally; any other
// escaped character is returned as the character itself.
func unescape(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\\' {
			if i+1 >= len(s) {
				return "", fmt.Errorf("trailing backslash in %q", s)
			}
			b.WriteByte(s[i+1])
			i++
			continue
		}
		b.WriteByte(c)
	}
	return b.String(), nil
}

// joinPath joins a base path and a relative path into a single clean path,
// resolving "." and ".." segments. A leading slash on the base is preserved.
func joinPath(base, rel string) string {
	var segs []string
	for _, s := range strings.Split(base, "/") {
		switch s {
		case "", ".":
		case "..":
			if len(segs) > 0 {
				segs = segs[:len(segs)-1]
			}
		default:
			segs = append(segs, s)
		}
	}
	for _, s := range strings.Split(rel, "/") {
		switch s {
		case "", ".":
		case "..":
			if len(segs) > 0 {
				segs = segs[:len(segs)-1]
			}
		default:
			segs = append(segs, s)
		}
	}
	out := strings.Join(segs, "/")
	if strings.HasPrefix(base, "/") {
		out = "/" + out
	}
	return out
}

// joinURL resolves a relative URL against a base URL (handling "." and ".."
// path segments). A base that does not parse as a URL is joined lexically.
func joinURL(base, rel string) string {
	u, err := url.Parse(base)
	if err != nil {
		return strings.TrimSuffix(base, "/") + "/" + rel
	}
	ref, err := url.Parse(rel)
	if err != nil {
		return strings.TrimSuffix(base, "/") + "/" + rel
	}
	return u.ResolveReference(ref).String()
}
