package database

import (
	"context"
	"testing"

	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// tagRows returns the raw Tag rows for the given owner type and id, so a test
// can assert that tags are stored normalized in the Tag table (F-29) rather
// than as a JSON column on the parent row.
func tagRows(t *testing.T, db *gorm.DB, ownerType string, ownerID uint) map[string]string {
	t.Helper()
	var rows []models.Tag
	if err := db.Where("owner_type = ? AND owner_id = ?", ownerType, ownerID).Find(&rows).Error; err != nil {
		t.Fatalf("query Tag table: %v", err)
	}
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		out[r.Key] = r.Value
	}
	return out
}

// TestTagsStoredNormalized verifies that a resource's tags are persisted as
// one row per key/value pair in the Tag table (F-29) — not as a JSON document
// on the parent row — so they can be searched by key and/or value. It checks
// the pipeline, a job definition, an environment, a worker group, and a
// worker.
func TestTagsStoredNormalized(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	// A worker group with tags.
	if _, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{
		Name: "pool",
		Tags: map[string]string{"team": "platform"},
	}); err != nil {
		t.Fatalf("CreateWorkerGroup: %v", err)
	}
	// An environment with tags.
	env, err := client.CreateEnvironment(ctx, &dbpb.CreateEnvironmentRequest{
		Name: "staging",
		Tags: map[string]string{"release": "train-2026-10"},
	})
	if err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}
	// A worker with tags.
	if _, err := client.RegisterWorker(ctx, &dbpb.RegisterWorkerRequest{
		Name:  "w1",
		Group: "pool",
		Tags:  map[string]string{"rack": "7"},
	}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	// A pipeline with tags and a job with its own tags.
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "build",
		Tags: map[string]string{"cost": "1234"},
		Jobs: []*dbpb.JobDefinition{{
			Key:  "compile",
			Name: "compile",
			Tags: map[string]string{"stage": "build"},
		}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	// The pipeline's tags are one row per pair in the Tag table.
	if got := tagRows(t, db, models.TagOwnerPipeline, uint(pipeline.GetId())); len(got) != 1 || got["cost"] != "1234" {
		t.Errorf("pipeline tag rows = %v, want {cost:1234}", got)
	}
	// The environment's tags.
	if got := tagRows(t, db, models.TagOwnerEnvironment, uint(env.GetId())); len(got) != 1 || got["release"] != "train-2026-10" {
		t.Errorf("environment tag rows = %v, want {release:train-2026-10}", got)
	}
	// The worker's tags.
	var worker models.Worker
	if err := db.Where("name = ?", "w1").First(&worker).Error; err != nil {
		t.Fatalf("load worker: %v", err)
	}
	if got := tagRows(t, db, models.TagOwnerWorker, worker.ID); len(got) != 1 || got["rack"] != "7" {
		t.Errorf("worker tag rows = %v, want {rack:7}", got)
	}
	// The worker group's tags.
	var group models.WorkerGroup
	if err := db.Where("name = ?", "pool").First(&group).Error; err != nil {
		t.Fatalf("load group: %v", err)
	}
	if got := tagRows(t, db, models.TagOwnerWorkerGroup, group.ID); len(got) != 1 || got["team"] != "platform" {
		t.Errorf("worker group tag rows = %v, want {team:platform}", got)
	}
	// The job definition's tags.
	var def models.Job
	if err := db.Where("pipeline_id = ? AND run_id IS NULL", uint(pipeline.GetId())).First(&def).Error; err != nil {
		t.Fatalf("load job definition: %v", err)
	}
	if got := tagRows(t, db, models.TagOwnerJob, def.ID); len(got) != 1 || got["stage"] != "build" {
		t.Errorf("job definition tag rows = %v, want {stage:build}", got)
	}
}

// TestWorkerGroupCRUD exercises the worker-group management RPCs (F-29):
// create, get, list, update, and delete, and that a missing group returns
// NotFound.
func TestWorkerGroupCRUD(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// Create a group with tags.
	created, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{
		Name:        "prod-fleet",
		Description: "production workers",
		Tags:        map[string]string{"team": "platform", "region": "eu-west"},
	})
	if err != nil {
		t.Fatalf("CreateWorkerGroup: %v", err)
	}
	if created.GetName() != "prod-fleet" {
		t.Errorf("name = %q, want %q", created.GetName(), "prod-fleet")
	}
	if got := created.GetTags()["team"]; got != "platform" {
		t.Errorf("tags[team] = %q, want %q", got, "platform")
	}

	// A create for an existing group is a no-op (returns the existing row).
	again, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{Name: "prod-fleet", Tags: map[string]string{"x": "y"}})
	if err != nil {
		t.Fatalf("CreateWorkerGroup (existing): %v", err)
	}
	if _, ok := again.GetTags()["x"]; ok {
		t.Errorf("existing group tags were replaced on create: %v", again.GetTags())
	}

	// Get the group.
	got, err := client.GetWorkerGroup(ctx, &dbpb.GetWorkerGroupRequest{Name: "prod-fleet"})
	if err != nil {
		t.Fatalf("GetWorkerGroup: %v", err)
	}
	if got.GetDescription() != "production workers" {
		t.Errorf("description = %q, want %q", got.GetDescription(), "production workers")
	}

	// Update the group's tags.
	updated, err := client.UpdateWorkerGroup(ctx, &dbpb.UpdateWorkerGroupRequest{
		Name: "prod-fleet",
		Tags: map[string]string{"team": "backend"},
	})
	if err != nil {
		t.Fatalf("UpdateWorkerGroup: %v", err)
	}
	if updated.GetTags()["team"] != "backend" {
		t.Errorf("updated tags[team] = %q, want %q", updated.GetTags()["team"], "backend")
	}
	if _, ok := updated.GetTags()["region"]; ok {
		t.Errorf("update replaced the whole tag set; region should be gone: %v", updated.GetTags())
	}

	// List groups.
	list, err := client.ListWorkerGroups(ctx, &dbpb.ListWorkerGroupsRequest{})
	if err != nil {
		t.Fatalf("ListWorkerGroups: %v", err)
	}
	if len(list.GetGroups()) != 1 {
		t.Fatalf("list returned %d groups, want 1", len(list.GetGroups()))
	}

	// Delete the group.
	if _, err := client.DeleteWorkerGroup(ctx, &dbpb.DeleteWorkerGroupRequest{Name: "prod-fleet"}); err != nil {
		t.Fatalf("DeleteWorkerGroup: %v", err)
	}
	if _, err := client.GetWorkerGroup(ctx, &dbpb.GetWorkerGroupRequest{Name: "prod-fleet"}); err == nil {
		t.Errorf("GetWorkerGroup after delete: expected NotFound, got nil")
	}
}

// TestEnvironmentTags verifies that an environment's tags are settable through
// the create/update RPCs and are returned on get (F-29).
func TestEnvironmentTags(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	env, err := client.CreateEnvironment(ctx, &dbpb.CreateEnvironmentRequest{
		Name: "prod",
		Tags: map[string]string{"release": "train-2026-10"},
	})
	if err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}
	if env.GetTags()["release"] != "train-2026-10" {
		t.Errorf("env tags[release] = %q, want %q", env.GetTags()["release"], "train-2026-10")
	}

	updated, err := client.UpdateEnvironment(ctx, &dbpb.UpdateEnvironmentRequest{
		Name: "prod",
		Tags: map[string]string{"release": "train-2026-11"},
	})
	if err != nil {
		t.Fatalf("UpdateEnvironment: %v", err)
	}
	if updated.GetTags()["release"] != "train-2026-11" {
		t.Errorf("updated env tags[release] = %q, want %q", updated.GetTags()["release"], "train-2026-11")
	}
}

// TestRunTagCombining verifies that when a run is created, each job instance's
// stored tags are the union of the tags of the pipeline, the job, the
// environment it targets, and the worker group it targets, with the more
// specific source winning on a key collision (F-29).
func TestRunTagCombining(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// A worker group with tags.
	if _, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{
		Name: "linux-pool",
		Tags: map[string]string{"team": "platform", "rack": "1"},
	}); err != nil {
		t.Fatalf("CreateWorkerGroup: %v", err)
	}
	// An environment with tags.
	if _, err := client.CreateEnvironment(ctx, &dbpb.CreateEnvironmentRequest{
		Name: "staging",
		Tags: map[string]string{"team": "platform", "release": "train-2026-10"},
	}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	// A pipeline with tags and a job that targets the environment and group,
	// with its own tags. The job's tags override the pipeline's on a
	// collision; the environment's and group's tags override the job's.
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "build",
		Tags: map[string]string{"team": "platform", "cost": "1234"},
		Jobs: []*dbpb.JobDefinition{{
			Key:         "compile",
			Name:        "compile",
			TargetGroup: "linux-pool",
			Environment: "staging",
			Tags:        map[string]string{"team": "backend", "stage": "build"},
		}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	// Create a run of the pipeline.
	_, err = client.CreateRun(ctx, &dbpb.CreateRunRequest{
		PipelineId: pipeline.GetId(),
		Trigger:    "manual",
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Fetch the run's job instance and check its combined tags.
	jobs, err := client.ListJobs(ctx, &dbpb.ListJobsRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	// The pipeline has one definition; the run created one instance. Find the
	// instance (run_id != 0).
	var instance *dbpb.Job
	for _, j := range jobs.GetJobs() {
		if j.GetRunId() != 0 {
			instance = j
			break
		}
	}
	if instance == nil {
		t.Fatalf("no run instance found in %v", jobs.GetJobs())
	}
	tags := instance.GetTags()
	// team: pipeline=platform, job=backend, environment=platform, group=platform
	// -> most specific (group) wins: platform.
	if tags["team"] != "platform" {
		t.Errorf("tags[team] = %q, want %q", tags["team"], "platform")
	}
	// cost: only the pipeline sets it.
	if tags["cost"] != "1234" {
		t.Errorf("tags[cost] = %q, want %q", tags["cost"], "1234")
	}
	// stage: only the job sets it.
	if tags["stage"] != "build" {
		t.Errorf("tags[stage] = %q, want %q", tags["stage"], "build")
	}
	// release: only the environment sets it.
	if tags["release"] != "train-2026-10" {
		t.Errorf("tags[release] = %q, want %q", tags["release"], "train-2026-10")
	}
	// rack: only the worker group sets it.
	if tags["rack"] != "1" {
		t.Errorf("tags[rack] = %q, want %q", tags["rack"], "1")
	}
}

// TestRunTagCombiningPrecedence verifies the key-collision precedence
// (worker group > environment > job > pipeline) with a key set by every
// source (F-29).
func TestRunTagCombiningPrecedence(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	if _, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{
		Name: "pool",
		Tags: map[string]string{"k": "group"},
	}); err != nil {
		t.Fatalf("CreateWorkerGroup: %v", err)
	}
	if _, err := client.CreateEnvironment(ctx, &dbpb.CreateEnvironmentRequest{
		Name: "env",
		Tags: map[string]string{"k": "environment"},
	}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "p",
		Tags: map[string]string{"k": "pipeline"},
		Jobs: []*dbpb.JobDefinition{{
			Key:         "j",
			Name:        "j",
			TargetGroup: "pool",
			Environment: "env",
			Tags:        map[string]string{"k": "job"},
		}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if _, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	jobs, err := client.ListJobs(ctx, &dbpb.ListJobsRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	var instance *dbpb.Job
	for _, j := range jobs.GetJobs() {
		if j.GetRunId() != 0 {
			instance = j
			break
		}
	}
	if instance == nil {
		t.Fatalf("no run instance found")
	}
	// The worker group is the most specific static source, so its value wins.
	if got := instance.GetTags()["k"]; got != "group" {
		t.Errorf("tags[k] = %q, want %q (group > environment > job > pipeline)", got, "group")
	}
}

// TestStartJobExecutionMergesWorkerTags verifies that when a worker starts a
// job execution, the execution's tags are the job instance's base tag set
// merged with the worker's tags (the worker's tags win on a collision) (F-29).
func TestStartJobExecutionMergesWorkerTags(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// A worker group (so the job has a base tag set from the group).
	if _, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{
		Name: "pool",
		Tags: map[string]string{"team": "platform"},
	}); err != nil {
		t.Fatalf("CreateWorkerGroup: %v", err)
	}
	// A worker in the group, with its own tags.
	if _, err := client.RegisterWorker(ctx, &dbpb.RegisterWorkerRequest{
		Name:  "w1",
		Group: "pool",
		Tags:  map[string]string{"rack": "7", "team": "backend"},
	}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "p",
		Tags: map[string]string{"cost": "1234"},
		Jobs: []*dbpb.JobDefinition{{
			Key:         "j",
			Name:        "j",
			TargetGroup: "pool",
		}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	if _, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipeline.GetId(), Trigger: "manual"}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	jobs, err := client.ListJobs(ctx, &dbpb.ListJobsRequest{PipelineId: pipeline.GetId()})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	var instance *dbpb.Job
	for _, j := range jobs.GetJobs() {
		if j.GetRunId() != 0 {
			instance = j
			break
		}
	}
	if instance == nil {
		t.Fatalf("no run instance found")
	}

	// The worker starts the job execution.
	resp, err := client.StartJobExecution(ctx, &dbpb.StartJobExecutionRequest{
		JobId:      instance.GetId(),
		WorkerName: "w1",
	})
	if err != nil {
		t.Fatalf("StartJobExecution: %v", err)
	}
	if !resp.GetStarted() {
		t.Fatalf("StartJobExecution: started = false, want true")
	}
	tags := resp.GetExecution().GetTags()
	// cost: from the pipeline (base set).
	if tags["cost"] != "1234" {
		t.Errorf("tags[cost] = %q, want %q", tags["cost"], "1234")
	}
	// team: base set has platform (group); the worker's team=backend wins.
	if tags["team"] != "backend" {
		t.Errorf("tags[team] = %q, want %q (worker wins)", tags["team"], "backend")
	}
	// rack: only the worker sets it.
	if tags["rack"] != "7" {
		t.Errorf("tags[rack] = %q, want %q", tags["rack"], "7")
	}
}
