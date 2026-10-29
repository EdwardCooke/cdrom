// globx.go — double-star globbing for the publish_artifact step handler
// (F-21.6).
//
// Based on github.com/yargevad/filepathx (master, fetched 2026-10-27),
// MIT Licensed:
//
//	Copyright 2016 The filepathx Authors
//
//	Permission is hereby granted, free of charge, to any person obtaining a
//	copy of this software and associated documentation files (the "Software"),
//	to deal in the Software without restriction, including without limitation
//	the rights to use, copy, modify, merge, publish, distribute, sublicense,
//	and/or sell copies of the Software, and to permit persons to whom the
//	Software is furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included
//	in all copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
//	OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
//	MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
//	IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
//	CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
//	TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
//	SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
// Local adaptations (contracted in
// docs/features/f-21.6-artifact-package-references.md, "Glob portability"):
//
//   - Rooted evaluation (ours): the pattern is evaluated relative to root
//     (never to the process's working directory — a worker runs many jobs
//     concurrently, so a global Chdir is unsafe).
//   - Separator normalization (ours): on Windows a "/" in a pattern is
//     treated as a path separator (translated to "\" before matching); on
//     other operating systems "\" keeps its literal meaning. Patterns are
//     authored with "/" on every platform.
//   - Upstream PR #6: an empty fragment (the head of a leading "**/", e.g.
//     "**/*.txt") acts as "." instead of matching nothing, so leading-**
//     patterns work.
//   - Upstream PR #7: accumulated matches are combined with the next
//     fragment via filepath.Join rather than string concatenation.
//   - Upstream PR #4: glob metacharacters occurring in REAL path names are
//     neutralized (?) when a matched path is fed forward as the prefix of
//     the next fragment's glob, so a literal name cannot be reinterpreted
//     as a wildcard mid-expansion. Adaptation: the neutralized form is used
//     only in the accumulation chain; the RESULTS returned are the pristine
//     paths (the upstream PR mutates the recorded results, which would make
//     them un-openable).
package stephandlers

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// GlobX evaluates the glob pattern against the filesystem rooted at root.
// "**" represents a recursive wildcard matching zero-or-more directory
// levels deep (gitignore/zsh style): the pattern's fragments are applied
// cumulatively, descending arbitrarily deep after each fragment, so
// "a/**/b" matches a/b, a/x/b, a/x/y/b, .... A pattern without "**"
// delegates to the standard library (evaluated against root).
//
// root must be absolute. Matches are returned as absolute paths beneath
// root (native separators), deduplicated, in walk order. Directories are
// included in the results (callers that care about files filter them).
func GlobX(root, pattern string) ([]string, error) {
	norm := globNormalizeSeparators(pattern)
	if !strings.Contains(norm, "**") {
		// Passthrough to the core package if no double-star.
		return filepath.Glob(filepath.Join(root, norm))
	}
	// Process every fragment (including the head) cumulatively: the current
	// frontier is the set of positions descended-so-far; each fragment narrows
	// it, and the descent below every hit prepares the next fragment to match
	// at any depth underneath (the upstream algorithm's walk-downward step).
	curRes := []string{root}
	curFed := []string{globShield(root)}
	for _, frag := range strings.Split(norm, "**") {
		if frag == "" {
			// An empty fragment arises from a leading "**/" (or adjacent
			// "**"); it contributes no path element (upstream PR #6).
			frag = "."
		}
		var nextRes, nextFed []string
		seen := map[string]bool{}
		for i, fed := range curFed {
			paths, err := filepath.Glob(filepath.Join(fed, frag)) // PR #7
			if err != nil {
				return nil, err
			}
			for _, hit := range paths {
				err := filepath.Walk(hit, func(wp string, _ os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if !seen[wp] {
						seen[wp] = true
						nextRes = append(nextRes, wp)
						nextFed = append(nextFed, globShield(wp)) // PR #4
					}
					return nil
				})
				if err != nil {
					return nil, err
				}
			}
			_ = i
		}
		curRes, curFed = nextRes, nextFed
	}
	return curRes, nil
}

// globNormalizeSeparators translates pattern separators to the OS-native
// form: on Windows both "/" and "\" are path separators ("/" → "\", no-op
// for "\"); elsewhere "/" is the separator and "\" is left as a literal
// character (leaving it literal preserves the metacharacter-shielding
// guarantees for unusual file names).
func globNormalizeSeparators(pattern string) string {
	if runtime.GOOS == "windows" {
		return strings.ReplaceAll(pattern, "/", "\\")
	}
	return pattern
}

// globShield neutralizes glob metacharacters in a REAL path so the path can
// be fed forward as the prefix of another glob without its characters being
// reinterpreted as wildcards. "?" matches any single character (including
// the literal one), so the neutralized prefix still matches the original
// path exactly. (Upstream PR #4.)
func globShield(path string) string {
	return strings.NewReplacer("*", "?", "[", "?", "]", "?").Replace(path)
}
