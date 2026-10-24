package pipelinesource

import (
	"fmt"
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
