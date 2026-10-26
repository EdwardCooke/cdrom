package database

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// ---------------------------------------------------------------------------
// F-21.5.1 — database-side behaviors: the periodic-refresh policy + admin
// gate on the Pipeline row, the stored source_file/source_hash, the
// kickoff-provenance stamp on the run row, and the ListPipelineSources RPC.
// Integration-style (real SQLite via Open + Migrate, bufconn gRPC).
// ---------------------------------------------------------------------------

// TestPipelineStoresPeriodicRefreshPolicy verifies CREATE persists the
// file-declared policy + gate, and PARTIAL UPDATES leave them untouched while
// HAS_PERIODIC_REFRESH replaces them (removal included).
func TestPipelineStoresPeriodicRefreshPolicy(t *testing.T) {
	client := startSourceCredentialServer(t)
	ctx := context.Background()

	created, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:                            "pf-policy",
		PeriodicRefreshInterval:         "5m",
		PeriodicRefreshSchedule:         "0 3 * * *",
		PeriodicRefreshOverridesAllowed: true,
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if created.GetPeriodicRefreshInterval() != "5m" || created.GetPeriodicRefreshSchedule() != "0 3 * * *" {
		t.Errorf("create echoed policy = {%q %q}, want {5m \"0 3 * * *\"}", created.GetPeriodicRefreshInterval(), created.GetPeriodicRefreshSchedule())
	}
	if !created.GetPeriodicRefreshOverridesAllowed() {
		t.Error("create echoed gate = false, want true")
	}

	// A PARTIAL update (no has_periodic_refresh) must NOT touch the policy.
	partial, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:          created.GetId(),
		Description: "edited",
	})
	if err != nil {
		t.Fatalf("partial UpdatePipeline: %v", err)
	}
	if partial.GetPeriodicRefreshInterval() != "5m" || partial.GetPeriodicRefreshSchedule() != "0 3 * * *" {
		t.Errorf("partial update PERTURBED the policy = {%q %q}, want {5m \"0 3 * * *\"}", partial.GetPeriodicRefreshInterval(), partial.GetPeriodicRefreshSchedule())
	}

	// A REMOVAL (has_periodic_refresh with empty values) clears the policy.
	removed, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:                 created.GetId(),
		HasPeriodicRefresh: true,
	})
	if err != nil {
		t.Fatalf("removing UpdatePipeline: %v", err)
	}
	if removed.GetPeriodicRefreshInterval() != "" || removed.GetPeriodicRefreshSchedule() != "" {
		t.Errorf("removal left policy = {%q %q}, want empty", removed.GetPeriodicRefreshInterval(), removed.GetPeriodicRefreshSchedule())
	}
}

// TestPipelineGateFalsePersists verifies an EXPLICIT FALSE gate survives the
// round-trip (the pointer field distinguishes unset from false).
func TestPipelineGateFalsePersists(t *testing.T) {
	client := startSourceCredentialServer(t)
	ctx := context.Background()

	created, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:                            "pf-denied",
		PeriodicRefreshInterval:         "1h",
		PeriodicRefreshOverridesAllowed: false,
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if created.GetPeriodicRefreshOverridesAllowed() {
		t.Error("gate = true, want the explicit false to persist")
	}
	got, err := client.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	if got.GetPeriodicRefreshOverridesAllowed() {
		t.Error("GetPipeline: gate = true, want false (persists)")
	}
}

// TestPipelineStoresSourceFile verifies the stored RAW TEXT + HASH round-trip
// (F-21.5.1): a re-import carrying source_file/source_hash persists them, and
// a regular update (omitting them) leaves them intact.
func TestPipelineStoresSourceFile(t *testing.T) {
	client := startSourceCredentialServer(t)
	ctx := context.Background()

	created, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "pf-yaml"})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	const file = "apiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: pf-yaml\nspec: {}\n"
	const hash = "deadbeef"
	upd, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:         created.GetId(),
		SourceFile: file,
		SourceHash: hash,
	})
	if err != nil {
		t.Fatalf("UpdatePipeline (source_file): %v", err)
	}
	if upd.GetSourceFile() != file || upd.GetSourceHash() != hash {
		t.Errorf("echoed source_file/hash = {%q %q}, want the stored values", upd.GetSourceFile(), upd.GetSourceHash())
	}
	// A REGULAR update (no source_file) must NOT wipe the stored file.
	reg, err := client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{Id: created.GetId(), Description: "x"})
	if err != nil {
		t.Fatalf("regular UpdatePipeline: %v", err)
	}
	if reg.GetSourceFile() != file || reg.GetSourceHash() != hash {
		t.Errorf("regular update WIPED the stored file = {%q %q}", reg.GetSourceFile(), reg.GetSourceHash())
	}
}

// TestVersionSnapshotCapturesPolicy verifies the F-11 VERSION SNAPSHOT
// captures the policy (history: the cadence a run bound to that version saw).
func TestVersionSnapshotCapturesPolicy(t *testing.T) {
	client := startSourceCredentialServer(t)
	ctx := context.Background()

	created, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:                    "pf-snap",
		PeriodicRefreshInterval: "9m",
		PeriodicRefreshSchedule: "*/10 * * * *",
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	// Editing the policy produces a NEW version; the OLD version retains the
	// old policy.
	_, err = client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id:                      created.GetId(),
		HasPeriodicRefresh:      true,
		PeriodicRefreshInterval: "11m",
	})
	if err != nil {
		t.Fatalf("UpdatePipeline: %v", err)
	}
	v1, err := client.GetPipelineVersion(ctx, &dbpb.GetPipelineVersionRequest{PipelineId: created.GetId(), Version: 1})
	if err != nil {
		t.Fatalf("GetPipelineVersion(1): %v", err)
	}
	if v1.GetPeriodicRefreshInterval() != "9m" || v1.GetPeriodicRefreshSchedule() != "*/10 * * * *" {
		t.Errorf("v1 policy = {%q %q}, want {9m \"*/10 * * * *\"}", v1.GetPeriodicRefreshInterval(), v1.GetPeriodicRefreshSchedule())
	}
	v2, err := client.GetPipelineVersion(ctx, &dbpb.GetPipelineVersionRequest{PipelineId: created.GetId(), Version: 2})
	if err != nil {
		t.Fatalf("GetPipelineVersion(2): %v", err)
	}
	if v2.GetPeriodicRefreshInterval() != "11m" {
		t.Errorf("v2 policy = %q, want 11m", v2.GetPeriodicRefreshInterval())
	}
}

// TestRunRowStampedWithProvenance verifies the KICKOFF PROVENANCE STAMP
// (F-21.5.1): a run of an externally-sourced pipeline carries
// source_kind / source_reference / source_version / source_file_hash (from
// the source + its last-good state), and a run of a UI-authored pipeline
// carries NONE.
func TestRunRowStampedWithProvenance(t *testing.T) {
	client := startSourceCredentialServer(t)
	ctx := context.Background()

	// UI-authored pipeline: a run carries no provenance.
	ui, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "pf-noprovenance"})
	if err != nil {
		t.Fatalf("CreatePipeline(ui): %v", err)
	}
	uiResp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: ui.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun(ui): %v", err)
	}
	if uiResp.GetRun().GetSourceKind() != "" || uiResp.GetRun().GetSourceReference() != "" {
		t.Errorf("UI-authored run carries provenance = {%q %q}, want empty", uiResp.GetRun().GetSourceKind(), uiResp.GetRun().GetSourceReference())
	}

	// Externally-sourced pipeline: attach a source + seed the last-good state.
	ext, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "pf-provenance"})
	if err != nil {
		t.Fatalf("CreatePipeline(ext): %v", err)
	}
	if _, err := client.AttachPipelineSource(ctx, &dbpb.AttachPipelineSourceRequest{
		PipelineId: ext.GetId(), Kind: "git",
		Reference: "git:org/repo@main[pipelines/prod.yaml]",
	}); err != nil {
		t.Fatalf("AttachPipelineSource: %v", err)
	}
	if _, err := client.SetPipelineSourceState(ctx, &dbpb.SetPipelineSourceStateRequest{
		PipelineId:   ext.GetId(),
		LastSyncedAt: tsPtr(time.Now().UTC()),
		LastVersion:  "abcdef1234",
		FileHash:     "sha-sum",
	}); err != nil {
		t.Fatalf("SetPipelineSourceState: %v", err)
	}
	extResp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: ext.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun(ext): %v", err)
	}
	run := extResp.GetRun()
	if run.GetSourceKind() != "git" {
		t.Errorf("source_kind = %q, want git", run.GetSourceKind())
	}
	if run.GetSourceReference() != "git:org/repo@main[pipelines/prod.yaml]" {
		t.Errorf("source_reference = %q, want the locator", run.GetSourceReference())
	}
	if run.GetSourceVersion() != "abcdef1234" {
		t.Errorf("source_version = %q, want abcdef1234", run.GetSourceVersion())
	}
	if run.GetSourceFileHash() != "sha-sum" {
		t.Errorf("source_file_hash = %q, want sha-sum", run.GetSourceFileHash())
	}
}

// TestListPipelineSources verifies the LIST SOURCES RPC (F-21.5.3): the
// refresh loop's population walk.
func TestListPipelineSources(t *testing.T) {
	client := startSourceCredentialServer(t)
	ctx := context.Background()

	before, err := client.ListPipelineSources(ctx, &dbpb.ListPipelineSourcesRequest{})
	if err != nil {
		t.Fatalf("ListPipelineSources(before): %v", err)
	}
	nBefore := len(before.GetSources())

	p1, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "pf-list-1"})
	if err != nil {
		t.Fatalf("CreatePipeline(1): %v", err)
	}
	if _, err := client.AttachPipelineSource(ctx, &dbpb.AttachPipelineSourceRequest{
		PipelineId: p1.GetId(), Kind: "http", Reference: "http://a/x.yaml",
	}); err != nil {
		t.Fatalf("Attach(1): %v", err)
	}
	p2, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "pf-list-2"})
	if err != nil {
		t.Fatalf("CreatePipeline(2): %v", err)
	}
	if _, err := client.AttachPipelineSource(ctx, &dbpb.AttachPipelineSourceRequest{
		PipelineId: p2.GetId(), Kind: "file", Reference: "file:/y.yaml",
	}); err != nil {
		t.Fatalf("Attach(2): %v", err)
	}

	after, err := client.ListPipelineSources(ctx, &dbpb.ListPipelineSourcesRequest{})
	if err != nil {
		t.Fatalf("ListPipelineSources(after): %v", err)
	}
	if got := len(after.GetSources()); got != nBefore+2 {
		t.Fatalf("listed %d sources, want %d", got, nBefore+2)
	}
	found := map[int64]string{}
	for _, src := range after.GetSources() {
		found[src.GetPipelineId()] = src.GetKind()
	}
	if found[p1.GetId()] != "http" || found[p2.GetId()] != "file" {
		t.Errorf("listing missing/mismatched sources: %v", found)
	}
}

// TestSetSourceStatePreservesSurvivors verifies the DATABASE SERVICE's
// preserve-last-good semantics (F-21.5.2): a FAILURE RECORD (empty
// version/hash) does not wipe the last-good survivors — the
// kickoff-provenance fallback leans on them.
func TestSetSourceStatePreservesSurvivors(t *testing.T) {
	client := startSourceCredentialServer(t)
	ctx := context.Background()

	p, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "pf-survive"})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if _, err := client.AttachPipelineSource(ctx, &dbpb.AttachPipelineSourceRequest{
		PipelineId: p.GetId(), Kind: "http", Reference: "http://a/x.yaml",
	}); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	// A successful sync seeds the survivors.
	ok, err := client.SetPipelineSourceState(ctx, &dbpb.SetPipelineSourceStateRequest{
		PipelineId:   p.GetId(),
		LastSyncedAt: tsPtr(time.Now().UTC()),
		LastVersion:  "tip-commit",
		FileHash:     "good-hash",
	})
	if err != nil {
		t.Fatalf("Set(success): %v", err)
	}
	if ok.GetLastVersion() != "tip-commit" || ok.GetFileHash() != "good-hash" {
		t.Fatalf("success state = {%q %q}, want the survivors", ok.GetLastVersion(), ok.GetFileHash())
	}
	// A FAILURE record (empty version/hash, an error) must SURVIVE them.
	fail, err := client.SetPipelineSourceState(ctx, &dbpb.SetPipelineSourceStateRequest{
		PipelineId:   p.GetId(),
		LastSyncedAt: tsPtr(time.Now().UTC()),
		LastError:    "boom",
	})
	if err != nil {
		t.Fatalf("Set(failure): %v", err)
	}
	if fail.GetLastVersion() != "tip-commit" || fail.GetFileHash() != "good-hash" {
		t.Errorf("failure record WIPED the survivors = {%q %q}, want {tip-commit good-hash}", fail.GetLastVersion(), fail.GetFileHash())
	}
	if fail.GetLastError() != "boom" {
		t.Errorf("last_error = %q, want boom", fail.GetLastError())
	}
	// The NEXT success clears the error and advances the survivors.
	recover, err := client.SetPipelineSourceState(ctx, &dbpb.SetPipelineSourceStateRequest{
		PipelineId:   p.GetId(),
		LastSyncedAt: tsPtr(time.Now().UTC()),
		LastVersion:  "new-tip",
		FileHash:     "new-hash",
	})
	if err != nil {
		t.Fatalf("Set(recover): %v", err)
	}
	if recover.GetLastError() != "" {
		t.Errorf("recover did not clear last_error: %q", recover.GetLastError())
	}
	if recover.GetLastVersion() != "new-tip" || recover.GetFileHash() != "new-hash" {
		t.Errorf("recover state = {%q %q}, want {new-tip new-hash}", recover.GetLastVersion(), recover.GetFileHash())
	}
}

// TestDetachedSourceClearsState verifies DETACH removes the source + state
// (F-21.5.1): a detached pipeline's run thereafter carries no provenance.
func TestDetachedSourceClearsState(t *testing.T) {
	client := startSourceCredentialServer(t)
	ctx := context.Background()

	p, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "pf-detach"})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if _, err := client.AttachPipelineSource(ctx, &dbpb.AttachPipelineSourceRequest{
		PipelineId: p.GetId(), Kind: "git", Reference: "git:o/r@m[x.yaml]",
	}); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if _, err := client.DetachPipelineSource(ctx, &dbpb.DetachPipelineSourceRequest{PipelineId: p.GetId()}); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if _, err := client.GetPipelineSource(ctx, &dbpb.GetPipelineSourceRequest{PipelineId: p.GetId()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetPipelineSource after detach = %v, want NotFound", err)
	}
	resp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: p.GetId(), Trigger: "manual"})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if resp.GetRun().GetSourceKind() != "" {
		t.Errorf("detached pipeline's run carries provenance %q, want empty", resp.GetRun().GetSourceKind())
	}
}

// tsPtr is a small helper for Timestamp pointers in test literals.
func tsPtr(tm time.Time) *timestamppb.Timestamp {
	return timestamppb.New(tm)
}
