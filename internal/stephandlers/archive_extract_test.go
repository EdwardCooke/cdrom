// archive_extract_test.go — the extract core (F-21.6): format detection,
// structure fidelity, stripComponents, and the hostile-input defenses
// (escaping paths, link members, corrupt archives).
package stephandlers

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// buildTGZ builds a gzipped tar in memory from (name → content) pairs.
func buildTGZ(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	for name, content := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	tw.Close()
	gw.Close()
	return buf.Bytes()
}

// buildZIP builds a zip in memory from (name → content) pairs.
func buildZIP(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	zw.Close()
	return buf.Bytes()
}

// buildTar builds a plain tar in memory from (name → content) pairs.
func buildTar(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	tw.Close()
	return buf.Bytes()
}

func extractAndAssert(t *testing.T, data []byte, wantFormat ArchiveFormat, wantMembers int, wantFiles map[string]string) {
	t.Helper()
	src := filepath.Join(t.TempDir(), "pkg.unknown-ext") // name must NOT hint the format
	if err := os.WriteFile(src, data, 0o644); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	stats, err := ExtractArchive(context.Background(), src, filepath.Dir(src), dest, extractOptions{})
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if stats.Format != wantFormat {
		t.Errorf("format = %s, want %s", stats.Format, wantFormat)
	}
	if stats.Members != wantMembers {
		t.Errorf("members = %d, want %d", stats.Members, wantMembers)
	}
	for name, content := range wantFiles {
		got, err := os.ReadFile(filepath.Join(dest, filepath.FromSlash(name)))
		if err != nil {
			t.Errorf("missing %s: %v", name, err)
			continue
		}
		if string(got) != content {
			t.Errorf("%s = %q, want %q", name, got, content)
		}
	}
}

func TestExtractFormatsDetectedByMagicBytes(t *testing.T) {
	tree := map[string]string{
		"d/a.txt":   "alpha",
		"d/b/c.txt": "beta",
	}
	extractAndAssert(t, buildTGZ(t, tree), FormatTGZ, 2, tree)
	extractAndAssert(t, buildZIP(t, tree), FormatZIP, 2, tree)
	extractAndAssert(t, buildTar(t, tree), FormatTar, 2, tree)
}

func TestExtractCorruptInputsFail(t *testing.T) {
	cases := map[string][]byte{
		"garbage":   []byte("hello, definitely not an archive"),
		"truncated": (truncate(buildTGZ(t, map[string]string{"a.txt": "x"}), 10)),
	}
	for name, data := range cases {
		src := filepath.Join(t.TempDir(), name+".bin")
		if err := os.WriteFile(src, data, 0o644); err != nil {
			t.Fatal(err)
		}
		// garbage is sniffed as tar (fallback) and fails mid-parse; a
		// truncated gzip fails at the gzip header. Either way: an error.
		if _, err := ExtractArchive(context.Background(), src, filepath.Dir(src), t.TempDir(), extractOptions{}); err == nil {
			t.Errorf("%s: expected a failure", name)
		}
	}
}

func truncate(b []byte, n int) []byte {
	if n < len(b) {
		return b[:n]
	}
	return b
}

func TestExtractStripComponents(t *testing.T) {
	tree := map[string]string{
		"dist/pkg-1.0/app.bin": "payload",
		"dist/pkg-1.0/doc.txt": "docs",
	}
	src := filepath.Join(t.TempDir(), "pkg.tgz")
	if err := os.WriteFile(src, buildTGZ(t, tree), 0o644); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	stats, err := ExtractArchive(context.Background(), src, filepath.Dir(src), dest, extractOptions{StripComponents: 2})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Stripped != 2 {
		t.Errorf("stripped = %d, want 2", stats.Stripped)
	}
	for _, want := range []string{"app.bin", "doc.txt"} {
		if _, err := os.Stat(filepath.Join(dest, want)); err != nil {
			t.Errorf("strip-2 missing %s: %v", want, err)
		}
	}
}

func TestExtractRefusesEscapingMember(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "../../evil.txt", Mode: 0o644, Size: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("boo")); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	src := filepath.Join(t.TempDir(), "evil.tgz")
	if err := os.WriteFile(src, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	_, err := ExtractArchive(context.Background(), src, filepath.Dir(src), dest, extractOptions{})
	if err == nil {
		t.Fatal("expected a refusal for an escaping member")
	}
	if !strings.Contains(err.Error(), "refusing member") {
		t.Errorf("error %q does not name the refusal", err)
	}
	if _, serr := os.Stat(filepath.Join(dest, "..", "..", "evil.txt")); serr == nil {
		t.Error("the evil file escaped dest!")
	}
}

func TestExtractRefusesSymlinkMember(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "link.txt", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	src := filepath.Join(t.TempDir(), "linky.tgz")
	if err := os.WriteFile(src, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := ExtractArchive(context.Background(), src, filepath.Dir(src), t.TempDir(), extractOptions{})
	if err == nil {
		t.Fatal("expected a refusal for a symlink member")
	}
	if !strings.Contains(err.Error(), "refusing member") {
		t.Errorf("error %q does not name the refusal", err)
	}
}
