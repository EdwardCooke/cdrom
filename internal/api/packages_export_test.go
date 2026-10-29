// packages_export_test.go — the spec.artifacts EMIT side of the file-format
// round trip (F-21.6): a pipeline carrying package references exports them
// under spec.artifacts, and the export re-imports faithfully.
package api

import (
	"testing"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/pipelinefile"
)

func TestExportEmitsSpecArtifacts(t *testing.T) {
	pipeline := &dbpb.Pipeline{
		Id:   7,
		Name: "pkg-demo",
		Jobs: []*dbpb.JobDefinition{{
			Key:         "build",
			TargetGroup: "default",
			Spec: &dbpb.JobSpec{
				Steps: []*dbpb.JobStep{{
					Name: "fetch",
					Type: "download_artifact",
					Params: map[string]*dbpb.ParamValue{
						"artifact": {String_: "xyz"},
					},
				}},
			},
		}},
		Artifacts: []*dbpb.PackageArtifact{
			{Name: "xyz", PackageName: "somecoolapi", Tag: "6.6.6"},
			{Name: "util", UseLatest: true, AllowPreRelease: true, PreReleaseRegex: "^rc"},
		},
	}
	doc := pipelineToDocument(pipeline)
	spec, _ := doc.Get("spec")
	arts, ok := spec.Get("artifacts")
	if !ok {
		t.Fatal("spec.artifacts not emitted")
	}
	if arts.Type != pipelinefile.List || len(arts.Items) != 2 {
		t.Fatalf("artifacts = %+v, want a 2-item list", arts)
	}
	first, _ := arts.Items[0].Get("name")
	if first.AsString() != "xyz" {
		t.Errorf("first name = %q, want xyz", first.AsString())
	}
	blk, _ := arts.Items[1].Get("package")
	if blk.Type != pipelinefile.Map {
		t.Fatalf("second package block = %+v, want a map", blk)
	}
	if s, ok := blk.StringValue("useLatest"); !ok || s != "true" {
		t.Errorf("useLatest = %q (ok=%v), want true", s, ok)
	}
	if s, ok := blk.StringValue("preReleaseRegex"); !ok || s != "^rc" {
		t.Errorf("preReleaseRegex = %q (ok=%v), want ^rc", s, ok)
	}
}
