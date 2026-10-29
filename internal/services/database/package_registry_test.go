// package_registry_test.go — F-21.6 (storage-redesign era): the package
// registry's record-based resolution and the run-creation snapshot path.
// Integration-style: a real SQLite database served over bufconn. Records are
// seeded DIRECTLY (INSERT + explicit CreatedAt), so "latest by upload time"
// is deterministic without any clock games.
package database

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
	"cdrom/internal/services/artifacts"
)

// seedFiling plants a CONFIRMED registry filing record with an explicit
// created_at (the "upload time" resolution orders by).
func seedFiling(t *testing.T, db *gorm.DB, pkg, version string, preRelease bool, when time.Time) {
	t.Helper()
	name := artifacts.ComposePackageNameVersion(pkg, version, ".zip")
	row := models.StorageObject{
		UID:           "fld-" + pkg + "-" + version,
		Kind:          models.StorageObjKindArtifact,
		Namespace:     PackageRegistryNamespace,
		Name:          name,
		Size:          100,
		ContentSHA256: "aa",
		IsPackage:     true,
		PackageName:   pkg,
		Version:       version,
		IsPreRelease:  preRelease,
		UploadedBy:    "tester",
		CreatedAt:     when,
		Confirmed:     true,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatalf("seed filing %s/%s: %v", pkg, version, err)
	}
}

// resolveViaRPC resolves one reference through the gRPC facade.
func resolveViaRPC(t *testing.T, client dbpb.DatabaseClient, req *dbpb.ResolvePackageReferenceRequest) *dbpb.StorageObject {
	t.Helper()
	rec, err := client.ResolvePackageReference(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolvePackageReference(%+v): %v", req, err)
	}
	return rec
}

func TestResolvePackageReferenceMatrix(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()
	day1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	day3 := day1.Add(48 * time.Hour)

	// coolapi: 1.0.0 (day1), 1.0.1 (day2), 2.0.0-rc.1 (day3, PRE-RELEASE).
	seedFiling(t, db, "coolapi", "1.0.0", false, day1)
	seedFiling(t, db, "coolapi", "1.0.1", false, day2)
	seedFiling(t, db, "coolapi", "2.0.0-rc.1", true, day3)

	// use_latest, releases only → 1.0.1 (the newest RELEASE, even though a
	// newer pre-release exists — the FLAG governs, not the version string).
	rec := resolveViaRPC(t, client, &dbpb.ResolvePackageReferenceRequest{PackageName: "coolapi", UseLatest: true})
	if rec.GetVersion() != "1.0.1" {
		t.Fatalf("use_latest(releases) = %q, want 1.0.1", rec.GetVersion())
	}

	// use_latest + allow_pre_release → 2.0.0-rc.1 (the newest OVERALL).
	rec = resolveViaRPC(t, client, &dbpb.ResolvePackageReferenceRequest{PackageName: "coolapi", UseLatest: true, AllowPreRelease: true})
	if rec.GetVersion() != "2.0.0-rc.1" {
		t.Fatalf("use_latest(all channels) = %q, want 2.0.0-rc.1", rec.GetVersion())
	}

	// use_latest + allow_pre_release + regex restricting to ^beta → no
	// eligible pre-release (rc≠beta) and no release qualifies?? NO: releases
	// are ALWAYS eligible; the regex narrows the ADDED pre-release pool. So
	// the winner is the newest release: 1.0.1.
	rec = resolveViaRPC(t, client, &dbpb.ResolvePackageReferenceRequest{PackageName: "coolapi", UseLatest: true, AllowPreRelease: true, PreReleaseRegex: `^beta`})
	if rec.GetVersion() != "1.0.1" {
		t.Fatalf("use_latest(regex ^beta) = %q, want 1.0.1 (rc.1 excluded)", rec.GetVersion())
	}

	// Regex admitting the rc → 2.0.0-rc.1 wins (newest by date).
	rec = resolveViaRPC(t, client, &dbpb.ResolvePackageReferenceRequest{PackageName: "coolapi", UseLatest: true, AllowPreRelease: true, PreReleaseRegex: `^(rc|beta)`})
	if rec.GetVersion() != "2.0.0-rc.1" {
		t.Fatalf("use_latest(regex ^(rc|beta)) = %q, want 2.0.0-rc.1", rec.GetVersion())
	}

	// Pinned tag → exact version regardless of channel.
	rec = resolveViaRPC(t, client, &dbpb.ResolvePackageReferenceRequest{PackageName: "coolapi", Tag: "2.0.0-rc.1"})
	if rec.GetVersion() != "2.0.0-rc.1" {
		t.Fatalf("pinned = %q, want 2.0.0-rc.1", rec.GetVersion())
	}

	// Pinned MISSING version → NotFound.
	_, err := client.ResolvePackageReference(ctx, &dbpb.ResolvePackageReferenceRequest{PackageName: "coolapi", Tag: "9.9.9"})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Fatalf("pinned missing = %v, want NotFound", err)
	}

	// UNKNOWN package → NotFound.
	_, err = client.ResolvePackageReference(ctx, &dbpb.ResolvePackageReferenceRequest{PackageName: "ghost", UseLatest: true})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Fatalf("unknown package = %v, want NotFound", err)
	}

	// use_latest with NEITHER channel having eligibles is impossible here
	// (releases always qualify), but a package with ONLY pre-releases and no
	// opt-in must fail:
	seedFiling(t, db, "prenav", "0.1-alpha", true, day1)
	_, err = client.ResolvePackageReference(ctx, &dbpb.ResolvePackageReferenceRequest{PackageName: "prenav", UseLatest: true})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Fatalf("pre-release-only package, releases-only = %v, want NotFound", err)
	}
	rec = resolveViaRPC(t, client, &dbpb.ResolvePackageReferenceRequest{PackageName: "prenav", UseLatest: true, AllowPreRelease: true})
	if rec.GetVersion() != "0.1-alpha" {
		t.Fatalf("pre-release-only package, opted in = %q, want 0.1-alpha", rec.GetVersion())
	}

	// Date-order dominance: a LOWER version uploaded LATER wins use_latest.
	seedFiling(t, db, "latecomer", "0.9.0", false, day3)
	seedFiling(t, db, "latecomer", "1.0.0", false, day1)
	rec = resolveViaRPC(t, client, &dbpb.ResolvePackageReferenceRequest{PackageName: "latecomer", UseLatest: true})
	if rec.GetVersion() != "0.9.0" {
		t.Fatalf("use_latest(date dominates) = %q, want 0.9.0", rec.GetVersion())
	}
}

// TestRunCreationSnapshotsRegistryFilings proves the run-creation path: a
// pipeline declaring spec.artifacts resolves its references against the
// registry RECORDS and pins the winning FILINGS' display names on the run
// (denormalized onto the job instances) — no per-run copying.
func TestRunCreationSnapshotsRegistryFilings(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()
	day1 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	seedFiling(t, db, "widgetry", "1.0.0", false, day1)
	seedFiling(t, db, "widgetry", "1.0.1", false, day2)

	createResp, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "snappy",
		Jobs: []*dbpb.JobDefinition{{
			Name: "consume",
			Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Type: "shell", Params: map[string]*dbpb.ParamValue{"command": {String_: "true"}}}}},
		}},
		Artifacts: []*dbpb.PackageArtifact{
			{Name: "widgets", PackageName: "widgetry", UseLatest: true},
			{Name: "old-widgets", PackageName: "widgetry", Tag: "1.0.0"},
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	runResp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: createResp.GetId()})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	snap := runResp.GetRun().GetArtifacts()
	if len(snap) != 2 {
		t.Fatalf("snapshot = %v, want 2 entries", snap)
	}
	if got := snap["widgets"]; got != "widgetry-1.0.1.zip" {
		t.Fatalf("snapshot[widgets] = %q, want widgetry-1.0.1.zip (use_latest)", got)
	}
	if got := snap["old-widgets"]; got != "widgetry-1.0.0.zip" {
		t.Fatalf("snapshot[old-widgets] = %q, want widgetry-1.0.0.zip (pinned)", got)
	}
	// Denormalized onto the job instance.
	jobs := runResp.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}
	if got := jobs[0].GetArtifacts()["widgets"]; got != "widgetry-1.0.1.zip" {
		t.Fatalf("job.artifacts[widgets] = %q, want widgetry-1.0.1.zip", got)
	}

	// Snapshot independence: publish a NEWER version, kick ANOTHER run — the
	// first run's snapshot is frozen (it still names 1.0.1), the second
	// resolves the new latest.
	seedFiling(t, db, "widgetry", "1.1.0", false, day2.Add(24*time.Hour))
	run2, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: createResp.GetId()})
	if err != nil {
		t.Fatalf("second CreateRun: %v", err)
	}
	if got := run2.GetRun().GetArtifacts()["widgets"]; got != "widgetry-1.1.0.zip" {
		t.Fatalf("run2 snapshot[widgets] = %q, want widgetry-1.1.0.zip", got)
	}
	// The ORIGINAL run's snapshot is unchanged (frozen at its creation).
	again, err := client.GetRun(ctx, &dbpb.GetRunRequest{Id: runResp.GetRun().GetId()})
	if err != nil {
		t.Fatalf("GetRun(original): %v", err)
	}
	if got := again.GetArtifacts()["widgets"]; got != "widgetry-1.0.1.zip" {
		t.Fatalf("original run snapshot drifted to %q, want widgetry-1.0.1.zip", got)
	}
}

// TestRunCreationAbortsOnUnresolvableReferences proves that a pipeline whose
// spec.artifacts reference a MISSING package fails run creation cleanly
// (InvalidArgument), leaving no half-created run.
func TestRunCreationAbortsOnUnresolvableReferences(t *testing.T) {
	client, _ := startServerDB(t)
	ctx := context.Background()
	createResp, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "doomed",
		Jobs: []*dbpb.JobDefinition{{
			Name: "consume",
			Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Type: "shell", Params: map[string]*dbpb.ParamValue{"command": {String_: "true"}}}}},
		}},
		Artifacts: []*dbpb.PackageArtifact{{Name: "phantom", PackageName: "does-not-exist", UseLatest: true}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	_, err = client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: createResp.GetId()})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("CreateRun(unresolvable) = %v, want InvalidArgument", err)
	}
}

// TestValidatePublicationChecksTable exercises the publish-time coordinate
// rules (charset + round-trip) the API's publish endpoint enforces.
func TestValidatePublicationChecksTable(t *testing.T) {
	valid := []struct{ pkg, ver, ext string }{
		{"coolapi", "1.0.0", ".zip"},
		{"coolapi", "6.6.6-rc.1", ".tar.gz"},
		{"my_pkg", "2.0_beta", ".zip"},
		{"a-b-c", "1.2.3", ".deb"},
		{"weird..dots", "1.0", ".zip"},
	}
	for _, c := range valid {
		if err := ValidatePublicationChecks(c.pkg, c.ver, c.ext); err != nil {
			t.Errorf("checks(%q,%q,%q) = %v, want nil", c.pkg, c.ver, c.ext, err)
		}
	}
	invalid := []struct{ pkg, ver, ext string }{
		{"bad name", "1.0", ".zip"},  // space in the name
		{"good", "1.0 beta", ".zip"}, // space in the version
		{"x-1", "2", ".zip"},         // "x-1-2.zip" re-splits as (x, 1-2) — ambiguous
	}
	for _, c := range invalid {
		if err := ValidatePublicationChecks(c.pkg, c.ver, c.ext); err == nil {
			t.Errorf("checks(%q,%q,%q) = nil, want error", c.pkg, c.ver, c.ext)
		}
	}
}

// TestComposePackageNameVersion pins the display-name synthesis (incl. the
// empty-ext corner).
func TestComposePackageNameVersion(t *testing.T) {
	if got := artifacts.ComposePackageNameVersion("a", "1.0", ".zip"); got != "a-1.0.zip" {
		t.Errorf("compose(.zip) = %q, want a-1.0.zip", got)
	}
	if got := artifacts.ComposePackageNameVersion("a", "1.0", "zip"); got != "a-1.0.zip" {
		t.Errorf("compose(zip) = %q, want a-1.0.zip", got)
	}
	if got := artifacts.ComposePackageNameVersion("a", "1.0", ""); got != "a-1.0" {
		t.Errorf("compose(\"\") = %q, want a-1.0", got)
	}
	if got := artifacts.ComposePackageNameVersion("a", "1.0", ".tar.gz"); got != "a-1.0.tar.gz" {
		t.Errorf("compose(.tar.gz) = %q, want a-1.0.tar.gz", got)
	}
}
