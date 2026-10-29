// publish_pack.go — the publish_artifact step handler's collection and
// packing core (F-21.6).
//
// PackCollect gathers the FILES matched by multiline glob patterns (positive
// and "-"-negated), sanitizes their entry names for Windows compatibility,
// and allocates collision-free zip entry names (counter suffix before the
// extension). PackZip writes the resulting zip.
package stephandlers

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// PackPattern is one line of the patterns param.
type PackPattern struct {
	Negative bool
	Raw      string // the line as authored (minus the leading '-')
}

// ParsePackPatterns interprets the multiline patterns param: one pattern per
// line; a line whose first character is '-' is a negative (excluding)
// pattern; blank lines and '#' comments are ignored.
func ParsePackPatterns(blob string) ([]PackPattern, error) {
	var out []PackPattern
	for ln, line := range strings.Split(blob, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		neg := false
		if strings.HasPrefix(line, "-") {
			neg = true
			line = strings.TrimSpace(line[1:])
			if line == "" {
				return nil, fmt.Errorf("line %d: a negative pattern must name a pattern (-)", ln+1)
			}
		}
		out = append(out, PackPattern{Negative: neg, Raw: line})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("patterns must name at least one glob")
	}
	return out, nil
}

// PackEntry is one file destined for the zip: its final (sanitized,
// collision-free) entry name (slash-separated, the canonical zip convention)
// and the source path on the target.
type PackEntry struct {
	RelName string
	Src     string
}

// PackStats summarizes a collection (reported in the handler's log line).
type PackStats struct {
	Root      string
	Matched   int // files surviving the positive+negative patterns
	Excluded  int // files removed by negative patterns
	Sanitized int // character/device transforms applied to entry names
	Renamed   int // collision counters assigned
	ZipSize   int64
}

// PackCollect scans root for the pattern matches and plans the zip contents.
// Patterns are evaluated relative to root (a leading '/' is redundant but
// tolerated). Files only (directories are not packed on their own); symlinks
// are skipped (matching the extract refusal stance).
func PackCollect(ctx context.Context, root string, pats []PackPattern) (*PackStats, []PackEntry, error) {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return nil, nil, err
	}
	fi, err := os.Stat(rootAbs)
	if err != nil {
		return nil, nil, fmt.Errorf("no such root: %s", root)
	}
	if !fi.IsDir() {
		return nil, nil, fmt.Errorf("root is not a directory: %s", root)
	}
	stats := &PackStats{Root: rootAbs}

	// Two-pass accumulation (gitignore-flavored):
	//   - positives ADD files to the bag (a directory hit contributes
	//     nothing on its own — directories are not packed);
	//   - negatives PRUNE: a negative hit that is a FILE removes exactly
	//     that file; a negative hit that is a DIRECTORY removes the WHOLE
	//     SUBTREE beneath it (authoring "-dist/tmp/" must not leave
	//     dist/tmp/scrap.dat behind).
	posMul := map[string]int{} // absolute file path -> positive multiplicity
	var negFiles, negDirs []string
	for _, pt := range pats {
		if ctx.Err() != nil {
			return stats, nil, ctx.Err()
		}
		// A leading '/' roots the pattern at root redundantly; tolerate it.
		pattern := strings.TrimPrefix(pt.Raw, "/")
		matches, err := GlobX(rootAbs, pattern)
		if err != nil {
			return stats, nil, fmt.Errorf("invalid pattern %q: %w", pt.Raw, err)
		}
		for _, m := range matches {
			abs, err := filepath.Abs(m)
			if err != nil {
				continue
			}
			if pt.Negative {
				fi, err := os.Stat(abs)
				if err != nil {
					continue
				}
				if fi.IsDir() {
					negDirs = append(negDirs, abs)
				} else {
					negFiles = append(negFiles, abs)
				}
				continue
			}
			posMul[abs]++
		}
	}
	negFileSet := make(map[string]bool, len(negFiles))
	for _, nf := range negFiles {
		negFileSet[nf] = true
	}
	pruned := func(p string) bool {
		if negFileSet[p] {
			return true
		}
		for _, nd := range negDirs {
			if p == nd || strings.HasPrefix(p, nd+string(os.PathSeparator)) {
				return true
			}
		}
		return false
	}
	// Survivors: positive multiplicity, regular files, no symlinks, not
	// pruned by a negative. A pruned REGULAR FILE counts toward Excluded
	// (directories are not packed, so pruning one is not a file exclusion).
	var files []string
	for p, mul := range posMul {
		if mul <= 0 {
			continue
		}
		lfi, err := os.Lstat(p)
		if err != nil {
			continue // vanished meanwhile
		}
		if lfi.Mode()&os.ModeSymlink != 0 || !lfi.Mode().IsRegular() {
			continue
		}
		if pruned(p) {
			stats.Excluded++
			continue
		}
		files = append(files, p)
	}
	sort.Strings(files)
	stats.Matched = len(files)
	if len(files) == 0 {
		return stats, nil, fmt.Errorf("no files matched the given patterns")
	}

	// Stage 1: sanitize each file's entry name (segments, slash-joined).
	type contender struct {
		orig      string // absolute source path
		final     string // sanitized slash-joined relative name
		leafEqual bool   // original leaf == sanitized leaf (no char rewrite)
		devFixed  bool
	}
	contenders := make([]contender, 0, len(files))
	groups := map[string][]int{} // final name -> contender indices
	order := []string{}
	for _, fp := range files {
		rel, err := filepath.Rel(rootAbs, fp)
		if err != nil {
			return stats, nil, err
		}
		segs := strings.Split(filepath.ToSlash(rel), "/")
		var rewrote bool
		for i, sg := range segs {
			sg2, nChars := sanitizeSegment(sg, i == len(segs)-1)
			if nChars > 0 {
				rewrote = true
				stats.Sanitized += nChars
			}
			segs[i] = sg2
		}
		final := strings.Join(segs, "/")
		leafOrig := filepath.Base(fp)
		leafFin := segs[len(segs)-1]
		idx := len(contenders)
		contenders = append(contenders, contender{
			orig:      fp,
			final:     final,
			leafEqual: leafOrig == leafFin,
			devFixed:  rewrote,
		})
		if _, ok := groups[final]; !ok {
			order = append(order, final)
		}
		groups[final] = append(groups[final], idx)
	}
	sort.Strings(order)

	// Stage 2: allocate collision-free names. Every group's bare name is
	// occupied from the outset (a counter must never squat on a legitimate
	// file); within a group the uncontested name goes to the contender whose
	// original leaf needed no rewriting (else lowest original path), and the
	// remainder take stem<k>.ext counters (smallest free k, per group).
	occupied := make(map[string]bool, len(order))
	for _, gn := range order {
		occupied[gn] = true
	}
	names := make([]string, len(contenders))
	for _, gn := range order {
		members := groups[gn]
		sort.Slice(members, func(i, j int) bool {
			a, b := contenders[members[i]], contenders[members[j]]
			if a.leafEqual != b.leafEqual {
				return a.leafEqual
			}
			return a.orig < b.orig
		})
		for rank, mi := range members {
			if rank == 0 {
				names[mi] = gn
				continue
			}
			stem, ext := splitLeaf(gn)
			k := 1
			for {
				cand := fmt.Sprintf("%s%d.%s", stem, k, ext)
				if !occupied[cand] {
					occupied[cand] = true
					names[mi] = cand
					stats.Renamed++
					break
				}
				k++
			}
		}
	}
	entries := make([]PackEntry, 0, len(contenders))
	for i, c := range contenders {
		entries = append(entries, PackEntry{RelName: names[i], Src: c.orig})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].RelName < entries[j].RelName })
	return stats, entries, nil
}

// sanitizeSegment rewrites one path segment's characters that Windows
// forbids in file names (*) ? " < > | :, the ASCII control characters
// U+0000–U+001F) to '_', and prefixes '_' when the (extension-less) basename
// is a reserved device name. It returns the rewritten segment and the number
// of transforms applied.
func sanitizeSegment(seg string, isLeaf bool) (string, int) {
	n := 0
	sb := strings.Builder{}
	for _, r := range seg {
		if r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' || r == ':' || (r >= 0 && r <= 0x1F) {
			sb.WriteByte('_')
			n++
		} else {
			sb.WriteRune(r)
		}
	}
	out := sb.String()
	if isLeaf && isReservedDeviceName(stripExt(out)) {
		out = "_" + out
		n++
	}
	return out, n
}

// isReservedDeviceName reports whether base (extension stripped, uppercased
// by the caller convention) is a Windows reserved device name.
func isReservedDeviceName(base string) bool {
	u := strings.ToUpper(base)
	switch u {
	case "NUL", "CON", "PRN", "AUX":
		return true
	}
	if len(u) == 4 && u[:3] == "COM" || len(u) == 4 && u[:3] == "LPT" {
		digit := u[3]
		return digit >= '1' && digit <= '9'
	}
	return false
}

// stripExt drops the final extension (a '.'-suffix group) for the device-name
// comparison.
func stripExt(leaf string) string {
	i := strings.LastIndexByte(leaf, '.')
	if i <= 0 {
		return leaf
	}
	return leaf[:i]
}

// splitLeaf splits a slash-joined entry name into (stem-with-directories,
// extension).
func splitLeaf(final string) (string, string) {
	leaf := final
	if i := strings.LastIndexByte(final, '/'); i >= 0 {
		leaf = final[i+1:]
	}
	ext := ""
	if i := strings.LastIndexByte(leaf, '.'); i > 0 {
		ext = leaf[i+1:]
		leaf = leaf[:i]
	}
	dir := ""
	if i := strings.LastIndexByte(final, '/'); i >= 0 {
		dir = final[:i+1]
	}
	return dir + leaf, ext
}

// PackZip writes entries into a zip at zipDest (parent dirs created; an
// existing file is replaced). Entry order is the given order; modification
// times are the sources' mtimes.
func PackZip(ctx context.Context, zipDest string, entries []PackEntry) error {
	if err := os.MkdirAll(filepath.Dir(zipDest), 0o755); err != nil {
		return err
	}
	zf, err := os.Create(zipDest)
	if err != nil {
		return err
	}
	defer zf.Close()
	zw := zip.NewWriter(zf)
	for _, e := range entries {
		if ctx.Err() != nil {
			zw.Close()
			return ctx.Err()
		}
		fi, err := os.Stat(e.Src)
		if err != nil {
			return fmt.Errorf("stat %s: %w", e.Src, err)
		}
		hdr := &zip.FileHeader{Name: e.RelName, Method: zip.Deflate}
		hdr.Modified = fi.ModTime()
		w, err := zw.CreateHeader(hdr)
		if err != nil {
			return err
		}
		sf, err := os.Open(e.Src)
		if err != nil {
			return err
		}
		_, cerr := io.Copy(w, sf)
		sf.Close()
		if cerr != nil {
			return fmt.Errorf("zip %s: %w", e.RelName, cerr)
		}
	}
	return zw.Close()
}
