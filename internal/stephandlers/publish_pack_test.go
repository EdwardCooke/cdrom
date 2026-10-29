// publish_pack_test.go — the publish_artifact collection/planning core
// (F-21.6): multiline pattern parsing, positive/negative accumulation,
// entry-name sanitization, and collision-counter allocation.
package stephandlers

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
)

func writeFile(t *testing.T, root, rel string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestParsePackPatterns(t *testing.T) {
	pats, err := ParsePackPatterns("dist/**\n-test_\n\n# comment\n- tmp/junk*\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(pats) != 3 {
		t.Fatalf("parsed %d patterns, want 3: %+v", len(pats), pats)
	}
	if pats[0].Negative || pats[0].Raw != "dist/**" {
		t.Errorf("pat0 = %+v", pats[0])
	}
	if !pats[1].Negative || pats[1].Raw != "test_" {
		t.Errorf("pat1 = %+v", pats[1])
	}
	if !pats[2].Negative || pats[2].Raw != "tmp/junk*" {
		t.Errorf("pat2 = %+v", pats[2])
	}
	if _, err := ParsePackPatterns("\n# only comments\n"); err == nil {
		t.Fatal("expected an error for a patterns blob with no patterns")
	}
	if _, err := ParsePackPatterns("-\n"); err == nil {
		t.Fatal("expected an error for a bare negative marker")
	}
}

func TestPackCollectBasicSelection(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "dist/release/app.bin")
	writeFile(t, root, "dist/release/notes.txt")
	writeFile(t, root, "dist/test/unit.test")
	writeFile(t, root, "dist/tmp/scrap.dat")
	writeFile(t, root, "loose.txt")

	pats, err := ParsePackPatterns("dist/**\n-dist/test/\n-dist/tmp/\n-loose.txt\n")
	if err != nil {
		t.Fatal(err)
	}
	stats, entries, err := PackCollect(context.Background(), root, pats)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Matched != 2 {
		t.Errorf("matched = %d, want 2", stats.Matched)
	}
	// Excluded counts pruned REGULAR files: unit.test (via -dist/test/) and
	// scrap.dat (via -dist/tmp/). loose.txt was never positively matched (it
	// appeared only as a negative), so it is not tallied.
	if stats.Excluded != 2 {
		t.Errorf("excluded = %d, want 2 (unit.test, scrap.dat)", stats.Excluded)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.RelName)
	}
	sort.Strings(names)
	want := []string{"dist/release/app.bin", "dist/release/notes.txt"}
	if !slices.Equal(names, want) {
		t.Errorf("entries = %v, want %v", names, want)
	}
}

func TestPackCollectEmptyFails(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt")
	pats, _ := ParsePackPatterns("nomatch-*\n")
	if _, _, err := PackCollect(context.Background(), root, pats); err == nil {
		t.Fatal("expected a no-files-matched error")
	}
	// Negatives excluding everything also fail.
	pats, _ = ParsePackPatterns("*.txt\n-a.txt\n")
	if _, _, err := PackCollect(context.Background(), root, pats); err == nil {
		t.Fatal("expected a no-files-matched error when negatives exclude all")
	}
}

func TestPackCollectSanitization(t *testing.T) {
	root := t.TempDir()
	// Windows-forbidden characters in a real file name (legal on Linux).
	writeFile(t, root, "bad:name?.cfg")
	writeFile(t, root, "quo\"te.txt")
	writeFile(t, root, "normal.txt")

	pats, _ := ParsePackPatterns("**\n")
	stats, entries, err := PackCollect(context.Background(), root, pats)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Sanitized == 0 {
		t.Error("expected sanitization transforms to be counted")
	}
	byName := map[string]string{}
	for _, e := range entries {
		byName[e.RelName] = e.Src
	}
	if _, ok := byName["bad_name_.cfg"]; !ok {
		t.Errorf("'bad:name?.cfg' not sanitized to 'bad_name_.cfg': %v", byName)
	}
	if _, ok := byName["quo_te.txt"]; !ok {
		t.Errorf("'quo\"te.txt' not sanitized to 'quo_te.txt': %v", byName)
	}
	if _, ok := byName["normal.txt"]; !ok {
		t.Errorf("'normal.txt' unexpectedly transformed: %v", byName)
	}
}

func TestPackCollectReservedDeviceName(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "aux.txt") // "aux" is a reserved device name
	pats, _ := ParsePackPatterns("*.txt\n")
	_, entries, err := PackCollect(context.Background(), root, pats)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].RelName != "_aux.txt" {
		t.Errorf("device-name fixup failed: %+v", entries)
	}
}

func TestPackCollectCollisionAllocation(t *testing.T) {
	root := t.TempDir()
	// Collision family: du*p.txt sanitizes to du_p.txt, equal to the real
	// du_p.txt. The LEGITIMATE file keeps the bare name; the star file gets
	// the counter.
	writeFile(t, root, "du*p.txt")
	writeFile(t, root, "du_p.txt")
	// Independent family: x*y.dat + x_y.dat.
	writeFile(t, root, "x*y.dat")
	writeFile(t, root, "x_y.dat")
	// Anti-squat: a REAL a_b1.txt must not be stolen by the counter.
	writeFile(t, root, "a*b.txt")
	writeFile(t, root, "a_b.txt")
	writeFile(t, root, "a_b1.txt")

	pats, _ := ParsePackPatterns("**\n")
	stats, entries, err := PackCollect(context.Background(), root, pats)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Renamed != 3 {
		t.Errorf("renamed = %d, want 3 (du*p, x*y, a*b)", stats.Renamed)
	}
	got := map[string]string{}
	for _, e := range entries {
		got[e.RelName] = filepath.Base(e.Src)
	}
	expect := map[string]string{
		"du_p.txt":  "du_p.txt",
		"du_p1.txt": "du*p.txt",
		"x_y.dat":   "x_y.dat",
		"x_y1.dat":  "x*y.dat",
		"a_b.txt":   "a_b.txt",
		"a_b1.txt":  "a_b1.txt", // the real file keeps its name
		"a_b2.txt":  "a*b.txt",  // the counter skips the occupied a_b1
	}
	if len(got) != len(expect) {
		t.Fatalf("allocated %d names, want %d: %v", len(got), len(expect), got)
	}
	for name, wantBase := range expect {
		if got[name] != wantBase {
			t.Errorf("entry %q = %q, want %q", name, got[name], wantBase)
		}
	}
}

func TestPackZipRoundTrip(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "d1/f1.txt")
	writeFile(t, root, "d1/d2/f2.txt")
	pats, _ := ParsePackPatterns("**\n")
	_, entries, err := PackCollect(context.Background(), root, pats)
	if err != nil {
		t.Fatal(err)
	}
	zipDest := filepath.Join(t.TempDir(), "out.zip")
	if err := PackZip(context.Background(), zipDest, entries); err != nil {
		t.Fatal(err)
	}
	// Unpack with the extract core and verify the structure round-trips.
	dest := t.TempDir()
	stats, err := ExtractArchive(context.Background(), zipDest, root, dest, extractOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Format != FormatZIP {
		t.Errorf("format = %s, want zip", stats.Format)
	}
	for _, want := range []string{"d1/f1.txt", "d1/d2/f2.txt"} {
		if _, err := os.Stat(filepath.Join(dest, want)); err != nil {
			t.Errorf("round-trip missing %s: %v", want, err)
		}
	}
}
