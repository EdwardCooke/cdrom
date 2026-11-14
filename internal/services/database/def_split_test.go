package database

import (
	"context"

	"testing"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// TestJobDefinitionsLiveInTheirOwnTable pins the F-31 §1 separation: a
// pipeline's job definitions persist in the job_definitions table (addressed
// by (pipeline_id, key)), and NO definition-shaped row (pipeline_id set,
// run_id NULL) exists in the jobs table. The legacy borrowing — definitions
// wearing `status = pending` perpetually — is gone.
func TestJobDefinitionsLiveInTheirOwnTable(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	pipe, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "sep-pipe",
		Jobs: []*dbpb.JobDefinition{
			{Key: "build", Name: "build-it", TargetGroup: "g1"},
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if len(pipe.GetJobs()) != 1 {
		t.Fatalf("pipeline jobs = %d, want 1", len(pipe.GetJobs()))
	}

	// The definition row exists in its own table...
	var defs []models.JobDefinition
	if err := db.Where("pipeline_id = ?", pipe.GetId()).Find(&defs).Error; err != nil {
		t.Fatalf("find definitions: %v", err)
	}
	if len(defs) != 1 || defs[0].Key != "build" {
		t.Fatalf("job_definitions = %#v, want one row keyed %q", defs, "build")
	}
	// ...and NO definition-shaped row remains in the jobs table.
	var legacy int64
	if err := db.Model(&models.Job{}).
		Where("pipeline_id IS NOT NULL AND run_id IS NULL").Count(&legacy).Error; err != nil {
		t.Fatalf("count legacy rows: %v", err)
	}
	if legacy != 0 {
		t.Errorf("jobs table still carries %d definition-shaped rows; the split must leave none", legacy)
	}
}

// TestGetJobRoundTripServesDefinitions pins the F-31 §1 round-trip guarantee:
// a definition forged via CreateJob (pipeline-linked) responds in the job's
// wire form, and GetJob on THAT id serves the definition back in the same
// dress — the split must not regress fetching what the caller was told it
// created.
func TestGetJobRoundTripServesDefinitions(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	pipe, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "roundtrip"})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	created, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{
		PipelineId:  pipe.GetId(),
		Name:        "compile",
		Key:         "compile",
		TargetGroup: "linux-pool",
	})
	if err != nil {
		t.Fatalf("CreateJob(definition): %v", err)
	}
	if created.GetId() == 0 {
		t.Fatal("created definition id = 0, want non-zero")
	}
	fetched, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("GetJob(definition id): %v", err)
	}
	if fetched.GetId() != created.GetId() {
		t.Errorf("fetched id = %d, want %d", fetched.GetId(), created.GetId())
	}
	if fetched.GetName() != "compile" || fetched.GetPipelineId() != pipe.GetId() {
		t.Errorf("fetched = name %q pipeline %d, want compile/%d", fetched.GetName(), fetched.GetPipelineId(), pipe.GetId())
	}
}
