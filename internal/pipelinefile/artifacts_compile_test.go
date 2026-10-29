// artifacts_compile_test.go — the spec.artifacts compiler (F-21.6) and the
// three new step types' recognition in the file format.
package pipelinefile

import (
	"testing"
)

const artifactsDoc = `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: pkg-demo
spec:
  artifacts:
    - name: xyz
      package:
        name: somecoolapi
        tag: "6.6.6"
    - name: util
      package:
        useLatest: true
        allowPreRelease: true
        preReleaseRegex: "^rc"
  jobs:
    build:
      steps:
        fetch:
          download_artifact:
            artifact: xyz
        unpack:
          extract:
            archive: somecoolapi-6.6.6.tar.gz
            dest: dist
            stripComponents: 1
        ship:
          publish_artifact:
            patterns: |-
              dist/**
              -dist/tmp/
            fileName: out-${version}.zip
`

func TestCompileSpecArtifacts(t *testing.T) {
	doc, err := Parse("pkg-demo.yaml", []byte(artifactsDoc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	artsVal, ok := doc.Spec.Get("artifacts")
	if !ok {
		t.Fatal("spec.artifacts missing")
	}
	refs, errs := CompilePackageArtifacts("pkg-demo.yaml", artsVal)
	if len(errs) > 0 {
		t.Fatalf("compile: %v", errs)
	}
	if len(refs) != 2 {
		t.Fatalf("refs = %d, want 2", len(refs))
	}
	x := refs[0]
	if x.GetName() != "xyz" || x.GetPackageName() != "somecoolapi" || x.GetTag() != "6.6.6" || x.GetUseLatest() {
		t.Errorf("ref0 = %+v", x)
	}
	u := refs[1]
	if u.GetName() != "util" || u.GetPackageName() != "" || !u.GetUseLatest() || !u.GetAllowPreRelease() || u.GetPreReleaseRegex() != "^rc" {
		t.Errorf("ref1 = %+v", u)
	}
}

func TestCompileSpecArtifactsValidation(t *testing.T) {
	// Wrong TYPE (a map, not a list).
	mapVal := Value{Type: Map, Entries: []Entry{{Key: "name", Value: Value{Type: Scalar, String: "x"}}}}
	if _, errs := CompilePackageArtifacts("t.yaml", mapVal); len(errs) == 0 {
		t.Error("a map is not a list: expected an error")
	}
	// An empty LIST is valid (declares no packages).
	if refs, errs := CompilePackageArtifacts("t.yaml", Value{Type: List}); len(errs) != 0 || refs != nil {
		t.Errorf("empty list should compile to (nil,nil): %v %v", refs, errs)
	}
	// A MISSING key (absent value) is valid too.
	if refs, errs := CompilePackageArtifacts("t.yaml", Value{}); len(errs) != 0 || refs != nil {
		t.Errorf("absent value should compile to (nil,nil): %v %v", refs, errs)
	}
	// Entry-level violations.
	badCases := []struct {
		name  string
		entry Value
	}{
		{"missing name", Value{Type: Map, Entries: []Entry{
			{Key: "package", Value: Value{Type: Map, Entries: []Entry{{Key: "tag", Value: Value{Type: Scalar, String: "1.0"}}}}},
		}}},
		{"pinned without tag", Value{Type: Map, Entries: []Entry{
			{Key: "name", Value: Value{Type: Scalar, String: "x"}},
		}}},
		{"unknown key", Value{Type: Map, Entries: []Entry{
			{Key: "name", Value: Value{Type: Scalar, String: "x"}},
			{Key: "bogus", Value: Value{Type: Scalar, String: "1"}},
		}}},
		{"unknown package key", Value{Type: Map, Entries: []Entry{
			{Key: "name", Value: Value{Type: Scalar, String: "x"}},
			{Key: "package", Value: Value{Type: Map, Entries: []Entry{
				{Key: "tag", Value: Value{Type: Scalar, String: "1.0"}},
				{Key: "flavor", Value: Value{Type: Scalar, String: "hot"}},
			}}},
		}}},
	}
	for _, c := range badCases {
		if _, errs := CompilePackageArtifacts("t.yaml", Value{Type: List, Items: []Value{c.entry}}); len(errs) == 0 {
			t.Errorf("%s: expected a compile error", c.name)
		}
	}
}

func TestCompileArtifactStepTypes(t *testing.T) {
	doc, err := Parse("pkg-demo.yaml", []byte(artifactsDoc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	jobsVal, _ := doc.Spec.Get("jobs")
	buildVal, _ := jobsVal.Get("build")
	stepsVal, _ := buildVal.Get("steps")
	if stepsVal.Type != Map || len(stepsVal.Entries) != 3 {
		t.Fatalf("steps = %+v, want a 3-entry map", stepsVal)
	}
	wantTypes := map[string]string{
		"fetch":  "download_artifact",
		"unpack": "extract",
		"ship":   "publish_artifact",
	}
	for _, e := range stepsVal.Entries {
		step, sErrs := CompileStep("pkg-demo.yaml", NewPath("spec", "jobs", "build", "steps").Extend(e.Key), e.Key, e.Value)
		if len(sErrs) > 0 {
			t.Fatalf("step %s: %v", e.Key, sErrs)
		}
		if step.GetType() != wantTypes[e.Key] {
			t.Errorf("step %s type = %q, want %q", e.Key, step.GetType(), wantTypes[e.Key])
		}
	}
	// The publish_artifact step's MULTILINE patterns param survived
	// compilation verbatim (the compiler does not interpret handler params).
	shipVal, _ := stepsVal.Get("ship")
	shipStep, sErrs := CompileStep("pkg-demo.yaml", NewPath("spec", "jobs", "build", "steps").Extend("ship"), "ship", shipVal)
	if len(sErrs) > 0 {
		t.Fatalf("ship step: %v", sErrs)
	}
	patterns := shipStep.GetParams()["patterns"]
	if patterns == nil || patterns.GetString_() != "dist/**\n-dist/tmp/" {
		t.Errorf("patterns param = %v, want the multiline blob verbatim", patterns)
	}
}
