// archive_extract.go — the extract step handler's unpacking core (F-21.6).
//
// ExtractArchive unpacks an archive file (auto-detected from magic bytes:
// gzipped tar, zip, or plain tar) into dest. Member names are treated
// hostilely (archives are data someone else wrote):
//
//   - Members whose normalized path would ESCAPE dest (absolute names, drive
//     letters, .. sequences) are refused: the step fails with
//     "refusing member <name>".
//   - Link-typed members (symlinks / hardlinks) are refused outright — the
//     portable stance, closing the classic "symlink escapes dest" hole.
//   - Extraction into a populated dest OVERWRITES conflicting files (last
//     writer wins); documented, not an error.
//
// The function honors ctx for cancellation (checked between members; a
// cancelled ctx unwinds promptly and the partial extraction is left in
// place — the caller cleans up).
package stephandlers

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ArchiveFormat names the detected archive format (reported in the handler's
// log line).
type ArchiveFormat string

const (
	FormatTar ArchiveFormat = "tar"
	FormatTGZ ArchiveFormat = "targz"
	FormatZIP ArchiveFormat = "zip"
)

// ExtractStats summarizes an extraction (reported in the handler's log line).
type ExtractStats struct {
	Format   ArchiveFormat
	Dest     string
	Members  int
	Stripped int
}

// extractOptions tunes ExtractArchive.
type extractOptions struct {
	// StripComponents drops that many leading path components from every
	// member name (GNU tar --strip-components semantics; naïve segment
	// stripping for zip members). 0 = no stripping.
	StripComponents int
}

// detectArchive inspects the first bytes of data and names the format.
// Magic-byte order matters: a gzipped tar begins with the gzip header.
func detectArchive(data []byte) (ArchiveFormat, error) {
	switch {
	case len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b:
		return FormatTGZ, nil
	case len(data) >= 4 && data[0] == 'P' && data[1] == 'K' && data[2] == 0x03 && data[3] == 0x04:
		return FormatZIP, nil
	default:
		return FormatTar, nil
	}
}

// ExtractArchive unpacks the archive at src into dest (creating dest if
// missing). A relative src is resolved against baseDir.
func ExtractArchive(ctx context.Context, src, baseDir, dest string, opts extractOptions) (*ExtractStats, error) {
	stripComponents := opts.StripComponents
	fullSrc := src
	if !filepath.IsAbs(fullSrc) {
		fullSrc = filepath.Join(baseDir, src)
	}
	fi, err := os.Stat(fullSrc)
	if err != nil {
		return nil, fmt.Errorf("no such file: %s", src)
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("not an archive (is a directory): %s", src)
	}
	destAbs, err := filepath.Abs(dest)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(destAbs, 0o755); err != nil {
		return nil, fmt.Errorf("create dest: %w", err)
	}

	f, err := os.Open(fullSrc)
	if err != nil {
		return nil, fmt.Errorf("open archive: %w", err)
	}
	defer f.Close()

	headerBuf := make([]byte, 512)
	n, _ := io.ReadFull(f, headerBuf)
	format, derr := detectArchive(headerBuf[:n])
	if derr != nil {
		return nil, derr
	}
	if _, serr := f.Seek(0, io.SeekStart); serr != nil {
		return nil, serr
	}

	stats := &ExtractStats{Format: format, Dest: destAbs, Stripped: stripComponents}
	switch format {
	case FormatTGZ:
		gr, err := gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("unsupported or corrupt archive (gzip header invalid)")
		}
		defer gr.Close()
		tr := tar.NewReader(bufio.NewReader(gr))
		for {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("unsupported or corrupt archive (tar: %v)", err)
			}
			if err := writeMember(ctx, tr, hdr.Name, hdr.Typeflag, hdr.Linkname, destAbs, stripComponents); err != nil {
				return stats, err
			}
			stats.Members++
		}
	case FormatZIP:
		zr, err := zip.NewReader(f, fi.Size())
		if err != nil {
			return nil, fmt.Errorf("unsupported or corrupt archive (zip: %v)", err)
		}
		for _, zf := range zr.File {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			rc, err := zf.Open()
			if err != nil {
				return nil, fmt.Errorf("unsupported or corrupt archive (zip member %s: %v)", zf.Name, err)
			}
			isLink := zf.Mode()&fs.ModeSymlink != 0
			if err := writeMember(ctx, rc, zf.Name, 0, "", destAbs, stripComponents, withLink(isLink)); err != nil {
				rc.Close()
				return stats, err
			}
			rc.Close()
			stats.Members++
		}
	default: // FormatTar
		tr := tar.NewReader(bufio.NewReader(f))
		for {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("unsupported or corrupt archive (tar: %v)", err)
			}
			if err := writeMember(ctx, tr, hdr.Name, hdr.Typeflag, hdr.Linkname, destAbs, stripComponents); err != nil {
				return stats, err
			}
			stats.Members++
		}
	}
	return stats, nil
}

// memberOpt tweaks writeMember's link handling.
type memberOpt func(*memberFlags)

type memberFlags struct {
	zipSymlink bool
}

func withLink(isLink bool) memberOpt {
	return func(m *memberFlags) { m.zipSymlink = isLink }
}

// writeMember materializes one archive member under destAbs. It refuses
// escaping paths and link-typed members (the step fails with
// "refusing member <name>") and overwrites conflicting files.
func writeMember(ctx context.Context, r io.Reader, name string, typeflag byte, linkname string, destAbs string, strip int, opts ...memberOpt) error {
	flags := &memberFlags{}
	for _, o := range opts {
		o(flags)
	}
	// Normalize separators (zip entries use '/', tars occasionally '\'):
	// treat both as path separators when shaping the member's relative path.
	rel := strings.ReplaceAll(name, "\\", "/")
	rel = strings.TrimLeft(rel, "/") // absolute member names are rooted at dest
	if strip > 0 {
		parts := strings.Split(rel, "/")
		if len(parts) <= strip {
			rel = ""
		} else {
			rel = strings.Join(parts[strip:], "/")
		}
	}
	if rel == "" || rel == "." {
		return nil // a directory entry reduced to nothing; nothing to write
	}
	isDir := strings.HasSuffix(rel, "/") || typeflag == tar.TypeDir
	if typeflag == tar.TypeSymlink || typeflag == tar.TypeLink || flags.zipSymlink {
		return fmt.Errorf("refusing member %s (link)", name)
	}
	target := filepath.Join(destAbs, filepath.FromSlash(rel))
	// Containment: the target must stay under destAbs.
	contained, err := containedWithin(destAbs, target)
	if err != nil || !contained {
		return fmt.Errorf("refusing member %s (escapes dest)", name)
	}
	if isDir {
		if err := os.MkdirAll(target, 0o755); err != nil {
			return fmt.Errorf("create dir %s: %w", name, err)
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("create parent for %s: %w", name, err)
	}
	out, err := os.Create(target)
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	defer out.Close()
	buf := make([]byte, 256<<10)
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		read, rerr := r.Read(buf)
		if read > 0 {
			if _, werr := out.Write(buf[:read]); werr != nil {
				return fmt.Errorf("write %s: %w", name, werr)
			}
		}
		if rerr == io.EOF {
			break
		} else if rerr != nil {
			return fmt.Errorf("read %s: %w", name, rerr)
		}
	}
	return nil
}

// containedWithin reports whether target lies inside base (both absolute).
func containedWithin(base, target string) (bool, error) {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false, err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return false, nil
	}
	return true, nil
}
