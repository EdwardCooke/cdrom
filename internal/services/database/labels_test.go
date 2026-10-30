package database

import (
	"context"
	"testing"

	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// labelRows returns the raw Tag rows for the given owner type and id, so a test
// can assert that labels are stored normalized in the Label table (F-29) rather
// than as a JSON column on the parent row.
func labelRows(t *testing.T, db *gorm.DB, ownerType string, ownerID uint) map[string]string {
	t.Helper()
	var rows []models.Label
	if err := db.Where("owner_type = ? AND owner_id = ?", ownerType, ownerID).Find(&rows).Error; err != nil {
		t.Fatalf("query Label table: %v", err)
	}
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		out[r.Key] = r.Value
	}
	return out
}

// TestLabelsStoredNormalized verifies that a resource's labels are persisted
// as one row per key/value pair in the Label table (F-29) — not as a JSON
// document on the parent row — so they can be searched by key and/or value.
// It checks the pipeline, a job definition, an environment, a worker group,
// and a worker.
func TestLabelsStoredNormalized(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	// A worker group with labels.
	if _, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{
		Name: "pool",
		Labels: map[string]string{"team": "platform"},
	}); err != nil {
		t.Fatalf("CreateWorkerGroup: %v", err)
	}
	// An environment with labels.
	env, err := client.CreateEnvironment(ctx, &dbpb.CreateEnvironmentRequest{
		Name: "staging",
		Labels: map[string]string{"release": "train-2026-10"},
	})
	if err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}
	// A worker with labels.
	if _, err := client.RegisterWorker(ctx, &dbpb.RegisterWorkerRequest{
		Name:  "w1",
		Group: "pool",
		Labels:  map[string]string{"rack": "7"},
	}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	// A pipeline with labels and a job with its own labels.
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "build",
		Labels: map[string]string{"cost": "1234"},
		// Free-form annotations (F-21.7): stored on the pipeline row (a JSON
		// column, unlike the normalized Label table) for UI searchability.
		Annotations: map[string]string{"owner": "platform-team"},
		Jobs: []*dbpb.JobDefinition{{
			Key:  "compile",
			Name: "compile",
			Labels: map[string]string{"stage": "build"},
		}},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	// The annotations round-trip on the create response.
	if pipeline.GetAnnotations()["owner"] != "platform-team" {
		t.Errorf("pipeline annotations = %v, want {owner:platform-team}", pipeline.GetAnnotations())
	}

	// The pipeline's labels are one row per pair in the Label table.
	if got := labelRows(t, db, models.LabelOwnerPipeline, uint(pipeline.GetId())); len(got) != 1 || got["cost"] != "1234" {
		t.Errorf("pipeline label rows = %v, want {cost:1234}", got)
	}
	// The environment's labels.
	if got := labelRows(t, db, models.LabelOwnerEnvironment, uint(env.GetId())); len(got) != 1 || got["release"] != "train-2026-10" {
		t.Errorf("environment label rows = %v, want {release:train-2026-10}", got)
	}
	// The worker's labels.
	var worker models.Worker
	if err := db.Where("name = ?", "w1").First(&worker).Error; err != nil {
		t.Fatalf("load worker: %v", err)
	}
	if got := labelRows(t, db, models.LabelOwnerWorker, worker.ID); len(got) != 1 || got["rack"] != "7" {
		t.Errorf("worker label rows = %v, want {rack:7}", got)
	}
	// The worker group's labels.
	var group models.WorkerGroup
	if err := db.Where("name = ?", "pool").First(&group).Error; err != nil {
		t.Fatalf("load group: %v", err)
	}
	if got := labelRows(t, db, models.LabelOwnerWorkerGroup, group.ID); len(got) != 1 || got["team"] != "platform" {
		t.Errorf("worker group label rows = %v, want {team:platform}", got)
	}
	// The job definition's labels.
	var def models.Job
	if err := db.Where("pipeline_id = ? AND run_id IS NULL", uint(pipeline.GetId())).First(&def).Error; err != nil {
		t.Fatalf("load job definition: %v", err)
	}
	if got := labelRows(t, db, models.LabelOwnerJob, def.ID); len(got) != 1 || got["stage"] != "build" {
		t.Errorf("job definition label rows = %v, want {stage:build}", got)
	}
}

// TestWorkerGroupCRUD exercises the worker-group management RPCs (F-29):
// create, get, list, update, and delete, and that a missing group returns
// NotFound.
func TestWorkerGroupCRUD(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// Create a group with labels.
	created, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{
		Name:        "prod-fleet",
		Description: "production workers",
		Labels:        map[string]string{"team": "platform", "region": "eu-west"},
	})
	if err != nil {
		t.Fatalf("CreateWorkerGroup: %v", err)
	}
	if created.GetName() != "prod-fleet" {
		t.Errorf("name = %q, want %q", created.GetName(), "prod-fleet")
	}
	if got := created.GetLabels()["team"]; got != "platform" {
		t.Errorf("labels[team] = %q, want %q", got, "platform")
	}

	// A create for an existing group is a no-op (returns the existing row).
	again, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{Name: "prod-fleet", Labels: map[string]string{"x": "y"}})
	if err != nil {
		t.Fatalf("CreateWorkerGroup (existing): %v", err)
	}
	if _, ok := again.GetLabels()["x"]; ok {
		t.Errorf("existing group labels were replaced on create: %v", again.GetLabels())
	}

	// Get the group.
	got, err := client.GetWorkerGroup(ctx, &dbpb.GetWorkerGroupRequest{Name: "prod-fleet"})
	if err != nil {
		t.Fatalf("GetWorkerGroup: %v", err)
	}
	if got.GetDescription() != "production workers" {
		t.Errorf("description = %q, want %q", got.GetDescription(), "production workers")
	}

	// Update the group's labels.
	updated, err := client.UpdateWorkerGroup(ctx, &dbpb.UpdateWorkerGroupRequest{
		Name: "prod-fleet",
		Labels: map[string]string{"team": "backend"},
	})
	if err != nil {
		t.Fatalf("UpdateWorkerGroup: %v", err)
	}
	if updated.GetLabels()["team"] != "backend" {
		t.Errorf("updated labels[team] = %q, want %q", updated.GetLabels()["team"], "backend")
	}
	if _, ok := updated.GetLabels()["region"]; ok {
		t.Errorf("update replaced the whole label set; region should be gone: %v", updated.GetLabels())
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

// TestEnvironmentLabels verifies that an environment's labels are settable through
// the create/update RPCs and are returned on get (F-29).
func TestEnvironmentLabels(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	env, err := client.CreateEnvironment(ctx, &dbpb.CreateEnvironmentRequest{
		Name: "prod",
		Labels: map[string]string{"release": "train-2026-10"},
	})
	if err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}
	if env.GetLabels()["release"] != "train-2026-10" {
		t.Errorf("env labels[release] = %q, want %q", env.GetLabels()["release"], "train-2026-10")
	}

	updated, err := client.UpdateEnvironment(ctx, &dbpb.UpdateEnvironmentRequest{
		Name: "prod",
		Labels: map[string]string{"release": "train-2026-11"},
	})
	if err != nil {
		t.Fatalf("UpdateEnvironment: %v", err)
	}
	if updated.GetLabels()["release"] != "train-2026-11" {
		t.Errorf("updated env labels[release] = %q, want %q", updated.GetLabels()["release"], "train-2026-11")
	}
}

// TestRunTagCombining verifies that when a run is created, each job instance's
// stored labels are the union of the labels of the pipeline, the job, the
// environment it targets, and the worker group it targets, with the more
// specific source winning on a key collision (F-29).
func TestRunLabelCombining(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// A worker group with labels.
	if _, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{
		Name: "linux-pool",
		Labels: map[string]string{"team": "platform", "rack": "1"},
	}); err != nil {
		t.Fatalf("CreateWorkerGroup: %v", err)
	}
	// An environment with labels.
	if _, err := client.CreateEnvironment(ctx, &dbpb.CreateEnvironmentRequest{
		Name: "staging",
		Labels: map[string]string{"team": "platform", "release": "train-2026-10"},
	}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	// A pipeline with labels and a job that targets the environment and group,
	// with its own labels. The job's labels override the pipeline's on a
	// collision; the environment's and group's labels override the job's.
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "build",
		Labels: map[string]string{"team": "platform", "cost": "1234"},
		Jobs: []*dbpb.JobDefinition{{
			Key:         "compile",
			Name:        "compile",
			TargetGroup: "linux-pool",
			Environment: "staging",
			Labels:        map[string]string{"team": "backend", "stage": "build"},
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

	// Fetch the run's job instance and check its combined labels.
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
	labels := instance.GetLabels()
	// team: pipeline=platform, job=backend, environment=platform, group=platform
	// -> most specific (group) wins: platform.
	if labels["team"] != "platform" {
		t.Errorf("labels[team] = %q, want %q", labels["team"], "platform")
	}
	// cost: only the pipeline sets it.
	if labels["cost"] != "1234" {
		t.Errorf("labels[cost] = %q, want %q", labels["cost"], "1234")
	}
	// stage: only the job sets it.
	if labels["stage"] != "build" {
		t.Errorf("labels[stage] = %q, want %q", labels["stage"], "build")
	}
	// release: only the environment sets it.
	if labels["release"] != "train-2026-10" {
		t.Errorf("labels[release] = %q, want %q", labels["release"], "train-2026-10")
	}
	// rack: only the worker group sets it.
	if labels["rack"] != "1" {
		t.Errorf("labels[rack] = %q, want %q", labels["rack"], "1")
	}
}

// TestRunTagCombiningPrecedence verifies the key-collision precedence
// (worker group > environment > job > pipeline) with a key set by every
// source (F-29).
func TestRunLabelCombiningPrecedence(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	if _, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{
		Name: "pool",
		Labels: map[string]string{"k": "group"},
	}); err != nil {
		t.Fatalf("CreateWorkerGroup: %v", err)
	}
	if _, err := client.CreateEnvironment(ctx, &dbpb.CreateEnvironmentRequest{
		Name: "env",
		Labels: map[string]string{"k": "environment"},
	}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}
	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "p",
		Labels: map[string]string{"k": "pipeline"},
		Jobs: []*dbpb.JobDefinition{{
			Key:         "j",
			Name:        "j",
			TargetGroup: "pool",
			Environment: "env",
			Labels:        map[string]string{"k": "job"},
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
	if got := instance.GetLabels()["k"]; got != "group" {
		t.Errorf("labels[k] = %q, want %q (group > environment > job > pipeline)", got, "group")
	}
}

// TestStartJobExecutionMergesWorkerLabels verifies that when a worker starts a
// job execution, the execution's labels are the job instance's base label set
// merged with the worker's labels (the worker's labels win on a collision) (F-29).
func TestStartJobExecutionMergesWorkerLabels(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// A worker group (so the job has a base label set from the group).
	if _, err := client.CreateWorkerGroup(ctx, &dbpb.CreateWorkerGroupRequest{
		Name: "pool",
		Labels: map[string]string{"team": "platform"},
	}); err != nil {
		t.Fatalf("CreateWorkerGroup: %v", err)
	}
	// A worker in the group, with its own labels.
	if _, err := client.RegisterWorker(ctx, &dbpb.RegisterWorkerRequest{
		Name:  "w1",
		Group: "pool",
		Labels:  map[string]string{"rack": "7", "team": "backend"},
	}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	pipeline, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name: "p",
		Labels: map[string]string{"cost": "1234"},
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
	labels := resp.GetExecution().GetLabels()
	// cost: from the pipeline (base set).
	if labels["cost"] != "1234" {
		t.Errorf("labels[cost] = %q, want %q", labels["cost"], "1234")
	}
	// team: base set has platform (group); the worker's team=backend wins.
	if labels["team"] != "backend" {
		t.Errorf("labels[team] = %q, want %q (worker wins)", labels["team"], "backend")
	}
	// rack: only the worker sets it.
	if labels["rack"] != "7" {
		t.Errorf("labels[rack] = %q, want %q", labels["rack"], "7")
	}
}
