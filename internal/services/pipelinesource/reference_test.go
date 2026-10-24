package pipelinesource

import (
	"testing"
)

func TestParseReferenceGit(t *testing.T) {
	cases := []struct {
		name    string
		ref     string
		repo    string
		branch  string
		path    string
		wantErr bool
	}{
		{
			name:   "https simple",
			ref:    "git:https://github.com/edwardcooke/cdrom.git@main[/some/path/file.yaml]",
			repo:   "https://github.com/edwardcooke/cdrom.git",
			branch: "main",
			path:   "/some/path/file.yaml",
		},
		{
			name:   "ssh remote with escaped @",
			ref:    "git:git\\@github.com:EdwardCooke/cdrom.git@main[/path/to/file.yaml]",
			repo:   "git@github.com:EdwardCooke/cdrom.git",
			branch: "main",
			path:   "/path/to/file.yaml",
		},
		{
			name:   "branch with slash",
			ref:    "git:https://github.com/o/r.git@feature/x[/f.yaml]",
			repo:   "https://github.com/o/r.git",
			branch: "feature/x",
			path:   "/f.yaml",
		},
		{
			name:   "literal @ in repo path and branch, literal ] in path",
			ref:    "git:https://somegit.com/edward\\@test/cdrom.git@branch\\@name[/test/aweird\\]path/file.yaml]",
			repo:   "https://somegit.com/edward@test/cdrom.git",
			branch: "branch@name",
			path:   "/test/aweird]path/file.yaml",
		},
		{
			name:    "missing branch separator",
			ref:     "git:https://github.com/o/r.git[/f.yaml]",
			wantErr: true,
		},
		{
			name:    "missing path opener",
			ref:     "git:https://github.com/o/r.git@main/f.yaml",
			wantErr: true,
		},
		{
			name:    "missing path closer",
			ref:     "git:https://github.com/o/r.git@main[/f.yaml",
			wantErr: true,
		},
		{
			name:    "trailing content after closer",
			ref:     "git:https://github.com/o/r.git@main[/f.yaml]extra",
			wantErr: true,
		},
		{
			name:    "empty repo",
			ref:     "git:@main[/f.yaml]",
			wantErr: true,
		},
		{
			name:    "empty branch",
			ref:     "git:https://github.com/o/r.git@[/f.yaml]",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := ParseReference(tc.ref)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseReference(%q) = %+v, want error", tc.ref, ref)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseReference(%q) error: %v", tc.ref, err)
			}
			if ref.Kind != KindGit {
				t.Fatalf("kind = %q, want %q", ref.Kind, KindGit)
			}
			if ref.Repo != tc.repo {
				t.Errorf("repo = %q, want %q", ref.Repo, tc.repo)
			}
			if ref.Branch != tc.branch {
				t.Errorf("branch = %q, want %q", ref.Branch, tc.branch)
			}
			if ref.Path != tc.path {
				t.Errorf("path = %q, want %q", ref.Path, tc.path)
			}
		})
	}
}

func TestParseReferenceHTTP(t *testing.T) {
	ref, err := ParseReference("http:https://www.example.com/some/file.yaml")
	if err != nil {
		t.Fatalf("ParseReference error: %v", err)
	}
	if ref.Kind != KindHTTP {
		t.Fatalf("kind = %q, want %q", ref.Kind, KindHTTP)
	}
	if ref.URL != "https://www.example.com/some/file.yaml" {
		t.Fatalf("url = %q", ref.URL)
	}
	if _, err := ParseReference("http:"); err == nil {
		t.Fatal("empty http spec should error")
	}
}

func TestParseReferenceFile(t *testing.T) {
	ref, err := ParseReference("file:/shared-variables.yaml")
	if err != nil {
		t.Fatalf("ParseReference error: %v", err)
	}
	if ref.Kind != KindFile {
		t.Fatalf("kind = %q, want %q", ref.Kind, KindFile)
	}
	if ref.Path != "/shared-variables.yaml" {
		t.Fatalf("path = %q", ref.Path)
	}
}

func TestParseReferenceUnknownType(t *testing.T) {
	if _, err := ParseReference("svn:https://example.com/repo"); err == nil {
		t.Fatal("unknown type should error")
	}
	if _, err := ParseReference("no-type-here"); err == nil {
		t.Fatal("missing type separator should error")
	}
	if _, err := ParseReference(""); err == nil {
		t.Fatal("empty reference should error")
	}
}

func TestUnescape(t *testing.T) {
	cases := []struct{ in, want string }{
		{"a\\@b", "a@b"},
		{"a\\[b", "a[b"},
		{"a\\]b", "a]b"},
		{"a\\\\b", "a\\b"},
		{"plain", "plain"},
		{"a\\xb", "axb"}, // any escaped char resolves to the char itself
	}
	for _, tc := range cases {
		got, err := unescape(tc.in)
		if err != nil {
			t.Fatalf("unescape(%q) error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("unescape(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if _, err := unescape("trailing\\"); err == nil {
		t.Fatal("trailing backslash should error")
	}
}
