package sourceref

import (
	"testing"
)

// TestParseReferenceRoundTrip verifies ParseReference(String()) round-trips a
// parsed reference (the String renderer is the inverse of the parser).
func TestParseReferenceRoundTrip(t *testing.T) {
	cases := []string{
		"git:https://github.com/org/repo.git@main[/path/to/file.yaml]",
		"git:git\\@github.com:user/repo.git@develop[f.yaml]",
		"http:https://cdn.example.com/pipelines/webapp.yaml",
		"file:/shared/pipelines/webapp.yaml",
	}
	for _, c := range cases {
		ref, err := ParseReference(c)
		if err != nil {
			t.Fatalf("ParseReference(%q): %v", c, err)
		}
		if got := ref.String(); got != c {
			t.Errorf("String() = %q, want %q (round trip)", got, c)
		}
	}
}

// TestResolveRelativeFullReference verifies a full reference (a "<type>:<spec>"
// string) is returned as-is regardless of the base.
func TestResolveRelativeFullReference(t *testing.T) {
	base, err := ParseReference("git:https://github.com/org/repo.git@main[/pipelines/webapp.yaml]")
	if err != nil {
		t.Fatal(err)
	}
	full := "git:https://github.com/other/team.git@main[/shared/steps/build.yaml]"
	got, err := base.ResolveRelative(full)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != full {
		t.Errorf("ResolveRelative(full) = %q, want %q (unchanged)", got.String(), full)
	}
}

// TestResolveRelativeSameRepo verifies a relative path in a git reference
// resolves against the CURRENT FILE'S DIRECTORY (not the file itself), staying
// in the same repo and branch (F-21.4: the fetcher fetches the referenced file
// from the same repo/branch).
func TestResolveRelativeSameRepo(t *testing.T) {
	base, err := ParseReference("git:https://github.com/org/repo.git@main[/pipelines/webapp.yaml]")
	if err != nil {
		t.Fatal(err)
	}
	// Same-directory sibling.
	got, err := base.ResolveRelative("../shared/steps/build.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if want := "git:https://github.com/org/repo.git@main[/shared/steps/build.yaml]"; got.String() != want {
		t.Errorf("ResolveRelative(..) = %q, want %q", got.String(), want)
	}
	if got.Repo != base.Repo || got.Branch != base.Branch {
		t.Errorf("repo/branch changed: got %q/%q, want %q/%q", got.Repo, got.Branch, base.Repo, base.Branch)
	}
	// Descendant.
	got, err = base.ResolveRelative("./extra/vars.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if want := "git:https://github.com/org/repo.git@main[/pipelines/extra/vars.yaml]"; got.String() != want {
		t.Errorf("ResolveRelative(descendant) = %q, want %q", got.String(), want)
	}
	// A leading slash is folded in lexically (consistent with the other kinds):
	// it joins against the current file's directory.
	got, err = base.ResolveRelative("/shared/jobs/deploy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if want := "git:https://github.com/org/repo.git@main[/pipelines/shared/jobs/deploy.yaml]"; got.String() != want {
		t.Errorf("ResolveRelative(/..) = %q, want %q", got.String(), want)
	}
}

// TestResolveRelativeToRootFile verifies a relative path from a file at the
// repo root (no directory) resolves against the root.
func TestResolveRelativeToRootFile(t *testing.T) {
	base, err := ParseReference("git:https://github.com/org/repo.git@main[/webapp.yaml]")
	if err != nil {
		t.Fatal(err)
	}
	got, err := base.ResolveRelative("shared/steps/build.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if want := "git:https://github.com/org/repo.git@main[/shared/steps/build.yaml]"; got.String() != want {
		t.Errorf("ResolveRelative = %q, want %q", got.String(), want)
	}
}

// TestResolveRelativeHTTP verifies a relative path in an http reference
// resolves against the file's URL (standard URL resolution, F-21.4).
func TestResolveRelativeHTTP(t *testing.T) {
	base, err := ParseReference("http:https://cdn.example.com/pipelines/webapp.yaml")
	if err != nil {
		t.Fatal(err)
	}
	got, err := base.ResolveRelative("../shared/steps/build.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if want := "http:https://cdn.example.com/shared/steps/build.yaml"; got.String() != want {
		t.Errorf("ResolveRelative(http ..) = %q, want %q", got.String(), want)
	}
	got, err = base.ResolveRelative("extra/vars.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if want := "http:https://cdn.example.com/pipelines/extra/vars.yaml"; got.String() != want {
		t.Errorf("ResolveRelative(http descendant) = %q, want %q", got.String(), want)
	}
}

// TestResolveRelativeFile verifies a relative path in a file reference
// resolves against the file's directory under the root (F-21.4).
func TestResolveRelativeFile(t *testing.T) {
	base, err := ParseReference("file:/shared/pipelines/webapp.yaml")
	if err != nil {
		t.Fatal(err)
	}
	got, err := base.ResolveRelative("../units/steps/build.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if want := "file:/shared/units/steps/build.yaml"; got.String() != want {
		t.Errorf("ResolveRelative(file ..) = %q, want %q", got.String(), want)
	}
}

// TestResolveRelativeMaliciousTraversal verifies a relative path that climbs
// above the repo root clamps at the root (joinPath drops excess ".." segments),
// so an import cannot escape the repo.
func TestResolveRelativeClampAboveRoot(t *testing.T) {
	base, err := ParseReference("git:https://github.com/org/repo.git@main[/a.yaml]")
	if err != nil {
		t.Fatal(err)
	}
	got, err := base.ResolveRelative("../../../../evil.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if want := "git:https://github.com/org/repo.git@main[/evil.yaml]"; got.String() != want {
		t.Errorf("ResolveRelative(climb) = %q, want %q", got.String(), want)
	}
}

// TestUnescape verifies backslash-escape resolution (the structural characters
// @ [ ] \ are escaped to appear literally; any other escaped character resolves
// to itself).
func TestUnescape(t *testing.T) {
	cases := []struct{ in, want string }{
		{"a\\@b", "a@b"},
		{"a\\[b", "a[b"},
		{"a\\]b", "a]b"},
		{"a\\\\b", "a\\b"},
		{"plain", "plain"},
		{"a\\xb", "axb"},
	}
	for _, c := range cases {
		got, err := Unescape(c.in)
		if err != nil {
			t.Fatalf("Unescape(%q): %v", c.in, err)
		}
		if got != c.want {
			t.Errorf("Unescape(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	if _, err := Unescape("trailing\\"); err == nil {
		t.Error("Unescape(trailing backslash) should error")
	}
}
