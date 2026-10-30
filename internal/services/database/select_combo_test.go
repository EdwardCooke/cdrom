package database

import (
	"context"
	"testing"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TestUpdateJobClearsDepsAndPersistsObservations isolates the F-22 release
// write: a single UpdateJob carrying BOTH clear_depends_on and
// dependency_observations must persist BOTH (a chained-Select bug would drop
// one, wedging the resolver or starving the target of observations).
func TestUpdateJobClearsDepsAndPersistsObservations(t *testing.T) {
	client, _ := startServerDB(t)
	ctx := context.Background()

	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{
		Name:        "sel-job",
		TargetGroup: "pool",
		DependsOn:   []int64{999},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if len(job.GetDependsOn()) != 1 {
		t.Fatalf("created job depends_on = %v, want [999]", job.GetDependsOn())
	}
	_, err = client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:                     job.GetId(),
		ClearDependsOn:         true,
		DependencyObservations: []*dbpb.DependencyObservation{{Key: "feed", Status: dbpb.JobStatus_JOB_STATUS_FAILED}},
	})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	got, err := client.GetJob(ctx, &dbpb.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	t.Logf("RESULT depends_on=%v observations=%+v", got.GetDependsOn(), got.GetDependencyObservations())
	if len(got.GetDependsOn()) != 0 {
		t.Errorf("depends_on = %v, want CLEARED (the compound update dropped the DependsOn write)", got.GetDependsOn())
	}
	obs := got.GetDependencyObservations()
	if len(obs) != 1 || obs[0].GetKey() != "feed" || obs[0].GetStatus() != dbpb.JobStatus_JOB_STATUS_FAILED {
		t.Errorf("dependency_observations = %+v, want [{feed FAILED}] (the compound update dropped the observations write)", obs)
	}
}
