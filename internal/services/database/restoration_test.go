// Package database — F-22 restoration integration tests.
//
// These tests exercise the Database service's restoration primitives against
// a real (temporary) SQLite database: PreviousSuccessfulRun (anchor
// selection), LatestRollbackRun (cooldown input), and RestartRun (the
// transactional clone of an anchor run's job rows — carry-over prefix vs.
// fresh suffix, snapshot fidelity, provenance columns).
package database

import (
	"context"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// seedAnchorRun seeds a pipeline with TWO runs: an older successful run (the
// ANCHOR) and a newer FAILED run (simulating a bad deploy). The anchor run's
// jobs form a small DAG:
//
//	build (root) ─┬─► deploy (cut point) ► verify
//	              │
//	              └─► fixture (diamond sibling of deploy)
//
// so the tests can assert closure/partition behavior on a non-linear graph.
func seedAnchorRun(t *testing.T, client dbpb.DatabaseClient) (pipelineID, anchorRunID, failedRunID int64) {
	t.Helper()
	ctx := context.Background()

	// The DAG in one shot: build is the root; fixture and deploy both feed on
	// build (a DIAMOND); verify is the leaf. build carries a concurrency key
	// so snapshot fidelity can be asserted on it. The pipeline DECLARES a
	// "stage" parameter (F-10) so the runs' concrete values denormalize onto
	// the job instances (undeclared params are dropped at run creation).
	pipe, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{
		Name:   "site",
		Params: []*dbpb.Parameter{{Name: "stage", Default: "dev"}},
		Jobs: []*dbpb.JobDefinition{
			{Key: "build", Name: "build", TargetGroup: "pool", Spec: &dbpb.JobSpec{ConcurrencyKeys: []string{"pk:build"}}},
			{Key: "fixture", Name: "fixture", TargetGroup: "pool", Needs: []string{"build"}, Spec: &dbpb.JobSpec{}},
			{Key: "deploy", Name: "deploy", TargetGroup: "pool", Needs: []string{"build", "fixture"}, Spec: &dbpb.JobSpec{}},
			{Key: "verify", Name: "verify", TargetGroup: "pool", Needs: []string{"deploy"}, Spec: &dbpb.JobSpec{}},
		},
	})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	pipelineID = pipe.GetId()

	// The ANCHOR run: succeeded.
	anchorResp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipelineID, Trigger: "manual", Params: map[string]string{"stage": "prod"}})
	if err != nil {
		t.Fatalf("CreateRun (anchor): %v", err)
	}
	anchorRunID = anchorResp.GetRun().GetId()
	markRunJobsSucceeded(t, client, anchorResp.GetJobs())

	// The FAILED run: simulates the bad deploy (newer than the anchor).
	failedResp, err := client.CreateRun(ctx, &dbpb.CreateRunRequest{PipelineId: pipelineID, Trigger: "manual", Params: map[string]string{"stage": "prod"}})
	if err != nil {
		t.Fatalf("CreateRun (failed): %v", err)
	}
	failedRunID = failedResp.GetRun().GetId()
	for _, j := range failedResp.GetJobs() {
		if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{Id: j.GetId(), Status: dbpb.JobStatus_JOB_STATUS_FAILED}); err != nil {
			t.Fatalf("UpdateJob (failed run): %v", err)
		}
	}
	if _, err := client.UpdateRun(ctx, &dbpb.UpdateRunRequest{Id: failedRunID, Status: dbpb.RunStatus_RUN_STATUS_FAILED}); err != nil {
		t.Fatalf("UpdateRun (failed): %v", err)
	}
	return pipelineID, anchorRunID, failedRunID
}

// markRunJobsSucceeded brings a freshly-created run's jobs (and the run
// itself) to succeeded, mimicking a completed deployment.
func markRunJobsSucceeded(t *testing.T, client dbpb.DatabaseClient, jobs []*dbpb.Job) {
	t.Helper()
	ctx := context.Background()
	for _, j := range jobs {
		if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
			Id:          j.GetId(),
			Status:      dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
			Outputs:     map[string]string{"built": "yes"},
			StepResults: []*dbpb.StepResult{{Name: "s1", Status: dbpb.StepStatus_STEP_STATUS_SUCCEEDED}},
		}); err != nil {
			t.Fatalf("UpdateJob (succeed): %v", err)
		}
	}
	// Derive the run's status the way the scheduler's run-status loop does.
	var runID int64
	if len(jobs) > 0 {
		runID = jobs[0].GetRunId()
	}
	if _, err := client.UpdateRun(ctx, &dbpb.UpdateRunRequest{Id: runID, Status: dbpb.RunStatus_RUN_STATUS_SUCCEEDED}); err != nil {
		t.Fatalf("UpdateRun (succeed): %v", err)
	}
}

// clearRunDependencies simulates the scheduler releasing every job in a
// completed run: DependsOn is cleared and the stable dependency keys/outcomes
// are retained in DependencyObservations.
func clearRunDependencies(t *testing.T, client dbpb.DatabaseClient, runID int64) {
	t.Helper()
	ctx := context.Background()
	resp, err := client.ListJobs(ctx, &dbpb.ListJobsRequest{RunId: runID})
	if err != nil {
		t.Fatalf("ListJobs(run %d): %v", runID, err)
	}
	byID := make(map[int64]*dbpb.Job, len(resp.GetJobs()))
	for _, job := range resp.GetJobs() {
		byID[job.GetId()] = job
	}
	for _, job := range resp.GetJobs() {
		observations := make([]*dbpb.DependencyObservation, 0, len(job.GetDependsOn()))
		for _, dependencyID := range job.GetDependsOn() {
			dependency := byID[dependencyID]
			if dependency == nil {
				t.Fatalf("job %q depends on missing run job %d", job.GetKey(), dependencyID)
			}
			observations = append(observations, &dbpb.DependencyObservation{
				Key:     dependency.GetKey(),
				Status:  dependency.GetStatus(),
				Outputs: dependency.GetOutputs(),
			})
		}
		if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
			Id:                     job.GetId(),
			ClearDependsOn:         true,
			DependencyObservations: observations,
		}); err != nil {
			t.Fatalf("release job %q: %v", job.GetKey(), err)
		}
	}
}

// TestPreviousSuccessfulRunExcludesRollbackOrigin verifies anchor selection
// (F-22): the latest SUCCESSFUL, NON-ROLLBACK-ORIGIN run is returned; a
// newer successful rollback-origin run must NOT shadow it.
func TestPreviousSuccessfulRunExcludesRollbackOrigin(t *testing.T) {
	client, _ := startServerDB(t)
	ctx := context.Background()
	pipelineID, anchorRunID, _ := seedAnchorRun(t, client)

	// Baseline: the anchor is the latest successful run.
	prev, err := client.PreviousSuccessfulRun(ctx, &dbpb.PreviousSuccessfulRunRequest{PipelineId: pipelineID})
	if err != nil {
		t.Fatalf("PreviousSuccessfulRun: %v", err)
	}
	if prev.GetId() != anchorRunID {
		t.Fatalf("anchor = %d, want %d", prev.GetId(), anchorRunID)
	}

	// Perform a REAL resumption (spawned by RestartRun: trigger "rollback",
	// restores_run_id = the anchor) and bring it to success: it must be
	// EXCLUDED from anchor selection — a rollback never rolls back to a
	// rollback.
	rbResp, err := client.RestartRun(ctx, &dbpb.RestartRunRequest{AnchorRunId: anchorRunID, FromJobKey: "verify"})
	if err != nil {
		t.Fatalf("RestartRun (simulate rollback): %v", err)
	}
	rbRunID := rbResp.GetRun().GetId()
	if rbRunID == 0 {
		t.Fatal("RestartRun returned no run id")
	}
	for _, j := range rbResp.GetJobs() {
		if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{Id: j.GetId(), Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED}); err != nil {
			t.Fatalf("bring resumption job to success: %v", err)
		}
	}
	if _, err := client.UpdateRun(ctx, &dbpb.UpdateRunRequest{Id: rbRunID, Status: dbpb.RunStatus_RUN_STATUS_SUCCEEDED}); err != nil {
		t.Fatalf("bring resumption to success: %v", err)
	}
	prev, err = client.PreviousSuccessfulRun(ctx, &dbpb.PreviousSuccessfulRunRequest{PipelineId: pipelineID})
	if err != nil {
		t.Fatalf("PreviousSuccessfulRun (after rollback run): %v", err)
	}
	if prev.GetId() != anchorRunID {
		t.Fatalf("anchor = %d, want %d (a rollback-origin run must not become the anchor)", prev.GetId(), anchorRunID)
	}

	// LatestRollbackRun finds the rollback-origin run (cooldown input).
	latest, err := client.LatestRollbackRun(ctx, &dbpb.LatestRollbackRunRequest{PipelineId: pipelineID})
	if err != nil {
		t.Fatalf("LatestRollbackRun: %v", err)
	}
	if latest.GetId() != rbRunID {
		t.Fatalf("latest rollback run = %d, want %d", latest.GetId(), rbRunID)
	}
	if latest.GetTrigger() != "rollback" {
		t.Fatalf("latest rollback run trigger = %q, want rollback", latest.GetTrigger())
	}
	if latest.GetRestoresRunId() != anchorRunID {
		t.Fatalf("latest rollback run restores_run_id = %d, want %d", latest.GetRestoresRunId(), anchorRunID)
	}
}

// TestPreviousSuccessfulRunNotFound verifies the no-anchor case (F-22): a
// pipeline with no successful run reports NotFound (nothing to restore to).
func TestPreviousSuccessfulRunNotFound(t *testing.T) {
	client, _ := startServerDB(t)
	ctx := context.Background()
	pipe, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "lonely"})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	_, err = client.PreviousSuccessfulRun(ctx, &dbpb.PreviousSuccessfulRunRequest{PipelineId: pipe.GetId()})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("PreviousSuccessfulRun = %v, want NotFound", err)
	}
}

// TestRestartRunPartitionAndProvenance verifies the resumption clone (F-22):
// cutting at "deploy" carries over build+fixture (terminal, outputs copied)
// and re-executes deploy+verify (pending, attempt reset); provenance columns
// are stamped on the run and every job.
func TestRestartRunPartitionAndProvenance(t *testing.T) {
	client, _ := startServerDB(t)
	ctx := context.Background()
	_, anchorRunID, _ := seedAnchorRun(t, client)
	clearRunDependencies(t, client, anchorRunID)

	resp, err := client.RestartRun(ctx, &dbpb.RestartRunRequest{AnchorRunId: anchorRunID, FromJobKey: "deploy"})
	if err != nil {
		t.Fatalf("RestartRun: %v", err)
	}
	newRun := resp.GetRun()
	if newRun.GetId() == 0 {
		t.Fatal("RestartRun returned no run id")
	}
	if newRun.GetTrigger() != "rollback" {
		t.Errorf("new run trigger = %q, want rollback", newRun.GetTrigger())
	}
	if newRun.GetRestoresRunId() != anchorRunID {
		t.Errorf("new run restores_run_id = %d, want %d", newRun.GetRestoresRunId(), anchorRunID)
	}
	if newRun.GetStatus() != dbpb.RunStatus_RUN_STATUS_PENDING {
		t.Errorf("new run status = %v, want PENDING", newRun.GetStatus())
	}
	if len(resp.GetJobs()) != 4 {
		t.Fatalf("resumption jobs = %d, want 4", len(resp.GetJobs()))
	}
	byKey := map[string]*dbpb.Job{}
	for _, j := range resp.GetJobs() {
		byKey[j.GetKey()] = j
	}
	// Carry-over prefix: terminal, outputs/results copied, carried_over set.
	for _, key := range []string{"build", "fixture"} {
		j := byKey[key]
		if j == nil {
			t.Fatalf("missing carry-over job %q", key)
		}
		if j.GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
			t.Errorf("%s status = %v, want SUCCEEDED (replicated from the anchor)", key, j.GetStatus())
		}
		if !j.GetCarriedOver() {
			t.Errorf("%s carried_over = false, want true", key)
		}
		if j.GetOutputs()["built"] != "yes" {
			t.Errorf("%s outputs = %v, want the anchor's outputs copied", key, j.GetOutputs())
		}
		if len(j.GetStepResults()) == 0 {
			t.Errorf("%s step_results empty, want the anchor's results copied", key)
		}
		if j.GetRestoresJobId() == 0 {
			t.Errorf("%s restores_job_id = 0, want the anchor job's id", key)
		}
	}
	// Remapped internal edges: fixture's dependency is the NEW run's build
	// instance (not the anchor's), proving the depends_on remap crossed runs.
	newBuildID := byKey["build"].GetId()
	fixtureDeps := byKey["fixture"].GetDependsOn()
	if len(fixtureDeps) != 1 || fixtureDeps[0] != newBuildID {
		t.Errorf("fixture depends_on = %v, want [%d] (the new run's build)", fixtureDeps, newBuildID)
	}
	// Fresh suffix: pending, attempt 1, no prior results.
	for _, key := range []string{"deploy", "verify"} {
		j := byKey[key]
		if j == nil {
			t.Fatalf("missing suffix job %q", key)
		}
		if j.GetStatus() != dbpb.JobStatus_JOB_STATUS_PENDING {
			t.Errorf("%s status = %v, want PENDING (fresh)", key, j.GetStatus())
		}
		if j.GetAttempt() != 1 {
			t.Errorf("%s attempt = %d, want 1 (reset)", key, j.GetAttempt())
		}
		if j.GetCarriedOver() {
			t.Errorf("%s carried_over = true, want false (fresh suffix)", key)
		}
		if len(j.GetOutputs()) != 0 {
			t.Errorf("%s outputs = %v, want empty (fresh)", key, j.GetOutputs())
		}
		if j.GetRestoresJobId() == 0 {
			t.Errorf("%s restores_job_id = 0, want the anchor job's id", key)
		}
	}
	// The suffix's internal dependency (verify → deploy) is remapped to the
	// NEW run's deploy instance, not the anchor's.
	newDeployID := byKey["deploy"].GetId()
	verifyDeps := byKey["verify"].GetDependsOn()
	foundRemap := false
	for _, d := range verifyDeps {
		if d == newDeployID {
			foundRemap = true
		}
	}
	if !foundRemap {
		t.Errorf("verify depends_on = %v, want a remap to the new run's deploy (%d)", verifyDeps, newDeployID)
	}
}

// TestRestartRunDiamondClosure verifies the PARTITION is ancestry-based
// (F-22): cutting at "verify" carries over build, fixture, AND deploy (three
// ancestors via the diamond) and re-executes only verify.
func TestRestartRunDiamondClosure(t *testing.T) {
	client, _ := startServerDB(t)
	ctx := context.Background()
	_, anchorRunID, _ := seedAnchorRun(t, client)

	resp, err := client.RestartRun(ctx, &dbpb.RestartRunRequest{AnchorRunId: anchorRunID, FromJobKey: "verify"})
	if err != nil {
		t.Fatalf("RestartRun: %v", err)
	}
	byKey := map[string]*dbpb.Job{}
	for _, j := range resp.GetJobs() {
		byKey[j.GetKey()] = j
	}
	wantCarried := map[string]bool{"build": true, "fixture": true, "deploy": true}
	for key, carried := range wantCarried {
		j := byKey[key]
		if j == nil {
			t.Fatalf("missing job %q", key)
		}
		if j.GetCarriedOver() != carried {
			t.Errorf("%s carried_over = %v, want %v (diamond closure)", key, j.GetCarriedOver(), carried)
		}
	}
	if byKey["verify"].GetCarriedOver() {
		t.Error("verify carried_over = true, want false (the cut point re-executes)")
	}
	if byKey["verify"].GetStatus() != dbpb.JobStatus_JOB_STATUS_PENDING {
		t.Errorf("verify status = %v, want PENDING", byKey["verify"].GetStatus())
	}
}

// TestRestartRunValidation verifies the loud failures (F-22): a cut point
// absent from the ANCHOR's jobs is NotFound; a non-succeeded anchor is
// FailedPrecondition.
func TestRestartRunValidation(t *testing.T) {
	client, _ := startServerDB(t)
	ctx := context.Background()
	_, anchorRunID, failedRunID := seedAnchorRun(t, client)

	// Bad cut point (not among the anchor's jobs).
	_, err := client.RestartRun(ctx, &dbpb.RestartRunRequest{AnchorRunId: anchorRunID, FromJobKey: "ghost"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("RestartRun (bad cut point) = %v, want NotFound", err)
	}
	// Non-succeeded anchor.
	_, err = client.RestartRun(ctx, &dbpb.RestartRunRequest{AnchorRunId: failedRunID, FromJobKey: "deploy"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RestartRun (failed anchor) = %v, want FailedPrecondition", err)
	}
}

// TestRestartRunSnapshotFidelity verifies the resumption inherits NOTHING
// from the world of today (F-22): the fresh suffix clones the anchor's
// CONCRETE run params, pre-rendered concurrency keys, and resolved artifacts
// verbatim — and a pipeline EDIT made after the anchor does not leak in.
func TestRestartRunSnapshotFidelity(t *testing.T) {
	client, _ := startServerDB(t)
	ctx := context.Background()
	pipelineID, anchorRunID, _ := seedAnchorRun(t, client)

	// Capture the ANCHOR's job rows BEFORE mutating the pipeline: the
	// resumption must clone THESE (the anchor's snapshot), not the pipeline's
	// current definitions.
	anchorJobs, err := client.ListJobs(ctx, &dbpb.ListJobsRequest{RunId: anchorRunID})
	if err != nil {
		t.Fatalf("ListJobs (anchor): %v", err)
	}
	anchorByKey := map[string]*dbpb.Job{}
	for _, j := range anchorJobs.GetJobs() {
		anchorByKey[j.GetKey()] = j
	}

	// Mutate the PIPELINE after the anchor (retarget a job, bump the version):
	// the resumption must NOT pick this up.
	_, err = client.UpdatePipeline(ctx, &dbpb.UpdatePipelineRequest{
		Id: pipelineID,
		Jobs: []*dbpb.JobDefinition{
			{Key: "build", Name: "build", TargetGroup: "OTHER-PACKAGE", Spec: &dbpb.JobSpec{}},
			{Key: "fixture", Name: "fixture", TargetGroup: "pool", Needs: []string{"build"}, Spec: &dbpb.JobSpec{}},
			{Key: "deploy", Name: "deploy", TargetGroup: "pool", Needs: []string{"build", "fixture"}, Spec: &dbpb.JobSpec{}},
			{Key: "verify", Name: "verify", TargetGroup: "pool", Needs: []string{"deploy"}, Spec: &dbpb.JobSpec{}},
		},
	})
	if err != nil {
		t.Fatalf("UpdatePipeline (mutation): %v", err)
	}

	resp, err := client.RestartRun(ctx, &dbpb.RestartRunRequest{AnchorRunId: anchorRunID, FromJobKey: "deploy"})
	if err != nil {
		t.Fatalf("RestartRun: %v", err)
	}
	byKey := map[string]*dbpb.Job{}
	for _, j := range resp.GetJobs() {
		byKey[j.GetKey()] = j
	}
	// The mutated target group must NOT leak into the clone: the clone carries
	// the ANCHOR's target group, not the pipeline's current one.
	if tg := byKey["build"].GetTargetGroup(); tg != anchorByKey["build"].GetTargetGroup() {
		t.Errorf("build target_group = %q, want the ANCHOR's (%q) — the post-anchor pipeline edit must not leak", tg, anchorByKey["build"].GetTargetGroup())
	}
	// The anchor's concrete run params are cloned verbatim (equality with the
	// anchor job's params — the true invariant, immune to seeding quirks).
	if got, want := byKey["deploy"].GetRunParams(), anchorByKey["deploy"].GetRunParams(); !reflect.DeepEqual(got, want) {
		t.Errorf("deploy run_params = %v, want the anchor's %v", got, want)
	}
	// The anchor's pre-rendered concurrency keys are cloned (not re-rendered
	// against the new run's identity).
	if got, want := byKey["build"].GetConcurrencyKeys(), anchorByKey["build"].GetConcurrencyKeys(); !reflect.DeepEqual(got, want) || len(want) == 0 {
		t.Errorf("build concurrency_keys = %v, want the anchor's %v (cloned, not re-rendered)", got, want)
	}
}
