// globx_test.go — the vendored double-star globber (F-21.6): the upstream
// baseline plus the three adopted PRs (#6 leading **/, #7 Join, #4
// metacharacter shielding) and the rooting contract.
package stephandlers

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
)

// globFixtureTree builds a small tree under root and returns it.
func globFixtureTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	files := []string{
		"a/x.txt",
		"a/b/y.txt",
		"a/b/c/z.bin",
		"keep.me",
		"junk.tmp",
		"odd/star*.cfg",
		"odd/sub/inner.txt",
	}
	for _, f := range files {
		p := filepath.Join(root, f)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(f), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// sortedUnder relativizes matches to root and sorts them (walk order is not
// contractual; the SET is).
func sortedUnder(t *testing.T, root string, got []string) []string {
	t.Helper()
	out := make([]string, 0, len(got))
	for _, p := range got {
		rel, err := filepath.Rel(root, p)
		if err != nil {
			t.Fatalf("unexpected match outside root: %s", p)
		}
		out = append(out, filepath.ToSlash(rel))
	}
	sort.Strings(out)
	return out
}

func TestGlobXBasics(t *testing.T) {
	root := globFixtureTree(t)
	cases := []struct {
		pat  string
		want []string
	}{
		// Plain (stdlib delegation, rooted).
		{"*.txt", nil}, // no top-level .txt files
		{"keep.me", []string{"keep.me"}},
		{"a/x.txt", []string{"a/x.txt"}},
		// Double-star basics.
		{"**/*.txt", []string{"a/b/y.txt", "a/x.txt", "odd/sub/inner.txt"}},
		{"*/x.txt", []string{"a/x.txt"}},
		{"**/b/**", []string{"a/b", "a/b/c", "a/b/c/z.bin", "a/b/y.txt"}},
		{"a/**", []string{"a", "a/b", "a/b/c", "a/b/c/z.bin", "a/b/y.txt", "a/x.txt"}},
		// PR #6: a LEADING **/ must work (master silently matches nothing).
		{"**/keep.me", []string{"keep.me"}},
		{"**/a/x.txt", []string{"a/x.txt"}},
		// Absolute-looking patterns root at the scan root (tolerated).
		{"/a/x.txt", []string{"a/x.txt"}},
	}
	for _, c := range cases {
		got, err := GlobX(root, c.pat)
		if err != nil {
			t.Fatalf("%q: %v", c.pat, err)
		}
		want := c.want
		if want == nil {
			want = []string{}
		}
		if !slices.Equal(sortedUnder(t, root, got), want) {
			t.Errorf("%q = %v, want %v", c.pat, sortedUnder(t, root, got), want)
		}
	}
}

// TestGlobXMetalCharacterShielding pins PR #4's adoption: a real file whose
// name carries glob metacharacters (star*.cfg) must (a) be matched
// pristinely and (b) not derail a DEEPER fragment (odd/**/inner.txt crosses
// the weird name as a fed-forward prefix).
func TestGlobXMetalCharacterShielding(t *testing.T) {
	root := globFixtureTree(t)
	got, err := GlobX(root, "odd/**")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "odd", "star*.cfg")
	found := false
	for _, p := range got {
		if p == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("star*.cfg not matched pristinely: %v", got)
	}
	got, err = GlobX(root, "odd/**/inner.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sortedUnder(t, root, got), []string{"odd/sub/inner.txt"}) {
		t.Fatalf("cross-weird-name descent failed: %v", got)
	}
}

// TestGlobXMalformedPattern ensures a syntactically invalid pattern surfaces
// the underlying glob error (the handler fails the step with it).
func TestGlobXMalformedPattern(t *testing.T) {
	root := globFixtureTree(t)
	if _, err := GlobX(root, "[unclosed"); err == nil {
		t.Fatal("expected an error for a malformed bracket expression")
	}
}
