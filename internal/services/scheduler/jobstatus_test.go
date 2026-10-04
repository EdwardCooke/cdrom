package scheduler

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// ---------------------------------------------------------------------------
// deriveJobStatus
// ---------------------------------------------------------------------------

func execWithStatus(jobID, id int64, worker string, status dbpb.JobStatus) *dbpb.JobExecution {
	return &dbpb.JobExecution{Id: id, JobId: jobID, WorkerName: worker, Status: status}
}

func TestDeriveJobStatus(t *testing.T) {
	cases := []struct {
		name       string
		executions []*dbpb.JobExecution
		mode       models.FailureMode
		want       models.JobStatus
	}{
		// ALL (default)
		{"all: all succeeded", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_SUCCEEDED)}, models.FailureModeAll, models.JobStatusSucceeded},
		{"all: one failed", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_FAILED)}, models.FailureModeAll, models.JobStatusFailed},
		{"all: one timed out", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_TIMED_OUT)}, models.FailureModeAll, models.JobStatusFailed},
		{"all: one running", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_RUNNING)}, models.FailureModeAll, models.JobStatusRunning},
		{"all: all running", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_RUNNING), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_RUNNING)}, models.FailureModeAll, models.JobStatusRunning},
		{"all: empty mode defaults to all", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_FAILED)}, "", models.JobStatusFailed},

		// BEST_EFFORT
		{"best: all succeeded", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_SUCCEEDED)}, models.FailureModeBestEffort, models.JobStatusSucceeded},
		{"best: one failed one succeeded", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_FAILED)}, models.FailureModeBestEffort, models.JobStatusSucceeded},
		{"best: one running", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_RUNNING)}, models.FailureModeBestEffort, models.JobStatusRunning},
		{"best: all failed", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_FAILED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_FAILED)}, models.FailureModeBestEffort, models.JobStatusSucceeded},

		// ANY
		{"any: one succeeded", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_FAILED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_SUCCEEDED)}, models.FailureModeAny, models.JobStatusSucceeded},
		{"any: none succeeded some running", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_FAILED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_RUNNING)}, models.FailureModeAny, models.JobStatusRunning},
		{"any: all failed", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_FAILED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_TIMED_OUT)}, models.FailureModeAny, models.JobStatusFailed},
		{"any: all succeeded", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED), execWithStatus(1, 2, "w2", dbpb.JobStatus_JOB_STATUS_SUCCEEDED)}, models.FailureModeAny, models.JobStatusSucceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveJobStatus(tc.executions, tc.mode)
			if got != tc.want {
				t.Errorf("deriveJobStatus(%v, %v) = %v, want %v", tc.executions, tc.mode, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// aliveWorkers
// ---------------------------------------------------------------------------

func TestAliveWorkers(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name    string
		workers []*dbpb.Worker
		want    map[string]bool
	}{
		{"no workers", nil, map[string]bool{}},
		{"alive worker", []*dbpb.Worker{{Name: "w1", LastSeenAt: timestamppb.New(now)}}, map[string]bool{"w1": true}},
		{"dead worker", []*dbpb.Worker{{Name: "w1", LastSeenAt: timestamppb.New(now.Add(-2 * time.Minute))}}, map[string]bool{}},
		{"no last seen", []*dbpb.Worker{{Name: "w1"}}, map[string]bool{}},
		{"mixed", []*dbpb.Worker{
			{Name: "w1", LastSeenAt: timestamppb.New(now)},
			{Name: "w2", LastSeenAt: timestamppb.New(now.Add(-2 * time.Minute))},
			{Name: "w3"},
		}, map[string]bool{"w1": true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := aliveWorkers(tc.workers)
			if len(got) != len(tc.want) {
				t.Fatalf("aliveWorkers(%v) = %v (len %d), want %v (len %d)", tc.workers, got, len(got), tc.want, len(tc.want))
			}
			for name, want := range tc.want {
				if got[name] != want {
					t.Errorf("aliveWorkers(%v)[%s] = %v, want %v", tc.workers, name, got[name], want)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// latestExecutionPerWorker
// ---------------------------------------------------------------------------

func TestLatestExecutionPerWorker(t *testing.T) {
	alive := map[string]bool{"w1": true, "w2": true}
	cases := []struct {
		name       string
		executions []*dbpb.JobExecution
		alive      map[string]bool
		want       map[string]int64 // worker -> expected execution id
	}{
		{"single execution", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_RUNNING)}, alive, map[string]int64{"w1": 1}},
		{"latest wins", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_FAILED), execWithStatus(1, 2, "w1", dbpb.JobStatus_JOB_STATUS_RUNNING)}, alive, map[string]int64{"w1": 2}},
		{"dead worker dropped", []*dbpb.JobExecution{execWithStatus(1, 1, "w1", dbpb.JobStatus_JOB_STATUS_RUNNING), execWithStatus(1, 2, "w3", dbpb.JobStatus_JOB_STATUS_RUNNING)}, alive, map[string]int64{"w1": 1}},
		{"all dead", []*dbpb.JobExecution{execWithStatus(1, 1, "w3", dbpb.JobStatus_JOB_STATUS_RUNNING)}, alive, map[string]int64{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := latestExecutionPerWorker(tc.executions, tc.alive)
			if len(got) != len(tc.want) {
				t.Fatalf("latestExecutionPerWorker = %v (len %d), want %v (len %d)", got, len(got), tc.want, len(tc.want))
			}
			for _, execution := range got {
				wantID, ok := tc.want[execution.GetWorkerName()]
				if !ok {
					t.Errorf("unexpected worker %s in result", execution.GetWorkerName())
					continue
				}
				if execution.GetId() != wantID {
					t.Errorf("worker %s execution id = %d, want %d", execution.GetWorkerName(), execution.GetId(), wantID)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// job-status loop (processJobStatuses)
// ---------------------------------------------------------------------------

// fakeJobStatusDB is a stub Database client for the job-status loop. It serves
// a fixed set of running jobs, a fixed set of executions (filtered by job id),
// and a fixed set of workers (filtered by group), and records the UpdateJob
// calls it receives.
type fakeJobStatusDB struct {
	dbpb.DatabaseClient // nil
	jobs                []*dbpb.Job
	executions          []*dbpb.JobExecution
	workers             []*dbpb.Worker
	updated             []*dbpb.UpdateJobRequest
}

func (f *fakeJobStatusDB) ListJobs(ctx context.Context, in *dbpb.ListJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error) {
	response := &dbpb.ListJobsResponse{}
	for _, job := range f.jobs {
		if in.GetStatus() != dbpb.JobStatus_JOB_STATUS_UNSPECIFIED && job.GetStatus() != in.GetStatus() {
			continue
		}
		response.Jobs = append(response.Jobs, job)
	}
	return response, nil
}

func (f *fakeJobStatusDB) ListJobExecutions(ctx context.Context, in *dbpb.ListJobExecutionsRequest, opts ...grpc.CallOption) (*dbpb.ListJobExecutionsResponse, error) {
	response := &dbpb.ListJobExecutionsResponse{}
	for _, execution := range f.executions {
		if in.GetJobId() != 0 && execution.GetJobId() != in.GetJobId() {
			continue
		}
		response.Executions = append(response.Executions, execution)
	}
	return response, nil
}

func (f *fakeJobStatusDB) ListWorkers(ctx context.Context, in *dbpb.ListWorkersRequest, opts ...grpc.CallOption) (*dbpb.ListWorkersResponse, error) {
	response := &dbpb.ListWorkersResponse{}
	for _, worker := range f.workers {
		if in.GetGroup() != "" && worker.GetGroup() != in.GetGroup() {
			continue
		}
		response.Workers = append(response.Workers, worker)
	}
	return response, nil
}

func (f *fakeJobStatusDB) UpdateJob(ctx context.Context, in *dbpb.UpdateJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	f.updated = append(f.updated, in)
	return &dbpb.Job{Id: in.GetId(), Status: in.GetStatus()}, nil
}

// fakeJobStatusPublisher is a stub Database publisher for the job-status
// loop. It records the "job_status" events the scheduler appends to the
// shared event log (F-23).
type fakeJobStatusPublisher struct {
	notified []*dbpb.PublishJobStatusRequest
}

func (f *fakeJobStatusPublisher) PublishJobStatus(ctx context.Context, in *dbpb.PublishJobStatusRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	f.notified = append(f.notified, in)
	return &dbpb.PublishEventResponse{}, nil
}

func recentWorker(name, group string) *dbpb.Worker {
	return &dbpb.Worker{Name: name, Group: group, LastSeenAt: timestamppb.New(time.Now())}
}

func deadWorker(name, group string) *dbpb.Worker {
	return &dbpb.Worker{Name: name, Group: group, LastSeenAt: timestamppb.New(time.Now().Add(-2 * time.Minute))}
}

// TestJobStatusLoopSucceedsJob verifies that a running group job whose workers
// all succeeded is re-derived as succeeded, persisted (with a finish time),
// and fanned out to the UI.
func TestJobStatusLoopSucceedsJob(t *testing.T) {
	db := &fakeJobStatusDB{
		jobs: []*dbpb.Job{
			{Id: 1, TargetGroup: "pool-a", Status: dbpb.JobStatus_JOB_STATUS_RUNNING, FailureMode: dbpb.FailureMode_FAILURE_MODE_ALL},
		},
		executions: []*dbpb.JobExecution{
			execWithStatus(1, 10, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED),
			execWithStatus(1, 11, "w2", dbpb.JobStatus_JOB_STATUS_SUCCEEDED),
		},
		workers: []*dbpb.Worker{recentWorker("w1", "pool-a"), recentWorker("w2", "pool-a")},
	}
	publisher := &fakeJobStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	s.processJobStatuses(context.Background(), db, publisher)

	if len(db.updated) != 1 {
		t.Fatalf("UpdateJob called %d times, want 1", len(db.updated))
	}
	update := db.updated[0]
	if update.GetId() != 1 {
		t.Errorf("UpdateJob id = %d, want 1", update.GetId())
	}
	if update.GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("UpdateJob status = %v, want SUCCEEDED", update.GetStatus())
	}
	if update.GetFinishedAt() == nil {
		t.Error("UpdateJob finished_at = nil, want set (job reached a terminal state)")
	}
	if len(publisher.notified) != 1 || publisher.notified[0].GetJobId() != 1 || publisher.notified[0].GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("PublishJobStatus = %+v, want job 1 SUCCEEDED", publisher.notified)
	}
}

// TestJobStatusLoopFailsJob verifies that a running group job with failure
// mode ALL that has one worker fail is re-derived as failed.
func TestJobStatusLoopFailsJob(t *testing.T) {
	db := &fakeJobStatusDB{
		jobs: []*dbpb.Job{
			{Id: 2, TargetGroup: "pool-a", Status: dbpb.JobStatus_JOB_STATUS_RUNNING, FailureMode: dbpb.FailureMode_FAILURE_MODE_ALL},
		},
		executions: []*dbpb.JobExecution{
			execWithStatus(2, 20, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED),
			execWithStatus(2, 21, "w2", dbpb.JobStatus_JOB_STATUS_FAILED),
		},
		workers: []*dbpb.Worker{recentWorker("w1", "pool-a"), recentWorker("w2", "pool-a")},
	}
	publisher := &fakeJobStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	s.processJobStatuses(context.Background(), db, publisher)

	if len(db.updated) != 1 {
		t.Fatalf("UpdateJob called %d times, want 1", len(db.updated))
	}
	if db.updated[0].GetStatus() != dbpb.JobStatus_JOB_STATUS_FAILED {
		t.Errorf("UpdateJob status = %v, want FAILED", db.updated[0].GetStatus())
	}
}

// TestJobStatusLoopBestEffort verifies that a running group job with failure
// mode BEST_EFFORT that has one worker fail and one succeed is re-derived as
// succeeded (a per-worker failure does not fail the job).
func TestJobStatusLoopBestEffort(t *testing.T) {
	db := &fakeJobStatusDB{
		jobs: []*dbpb.Job{
			{Id: 3, TargetGroup: "pool-a", Status: dbpb.JobStatus_JOB_STATUS_RUNNING, FailureMode: dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT},
		},
		executions: []*dbpb.JobExecution{
			execWithStatus(3, 30, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED),
			execWithStatus(3, 31, "w2", dbpb.JobStatus_JOB_STATUS_FAILED),
		},
		workers: []*dbpb.Worker{recentWorker("w1", "pool-a"), recentWorker("w2", "pool-a")},
	}
	publisher := &fakeJobStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	s.processJobStatuses(context.Background(), db, publisher)

	if len(db.updated) != 1 {
		t.Fatalf("UpdateJob called %d times, want 1", len(db.updated))
	}
	if db.updated[0].GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("UpdateJob status = %v, want SUCCEEDED (best_effort)", db.updated[0].GetStatus())
	}
}

// TestJobStatusLoopAny verifies that a running group job with failure mode ANY
// that has one worker succeed is re-derived as succeeded (one success is
// enough).
func TestJobStatusLoopAny(t *testing.T) {
	db := &fakeJobStatusDB{
		jobs: []*dbpb.Job{
			{Id: 4, TargetGroup: "pool-a", Status: dbpb.JobStatus_JOB_STATUS_RUNNING, FailureMode: dbpb.FailureMode_FAILURE_MODE_ANY},
		},
		executions: []*dbpb.JobExecution{
			execWithStatus(4, 40, "w1", dbpb.JobStatus_JOB_STATUS_FAILED),
			execWithStatus(4, 41, "w2", dbpb.JobStatus_JOB_STATUS_SUCCEEDED),
		},
		workers: []*dbpb.Worker{recentWorker("w1", "pool-a"), recentWorker("w2", "pool-a")},
	}
	publisher := &fakeJobStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	s.processJobStatuses(context.Background(), db, publisher)

	if len(db.updated) != 1 {
		t.Fatalf("UpdateJob called %d times, want 1", len(db.updated))
	}
	if db.updated[0].GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("UpdateJob status = %v, want SUCCEEDED (any)", db.updated[0].GetStatus())
	}
}

// TestJobStatusLoopNoopWhenUnchanged verifies that a running group job whose
// stored status already matches its derived status is left untouched (no
// update, no notification).
func TestJobStatusLoopNoopWhenUnchanged(t *testing.T) {
	db := &fakeJobStatusDB{
		jobs: []*dbpb.Job{
			{Id: 5, TargetGroup: "pool-a", Status: dbpb.JobStatus_JOB_STATUS_RUNNING, FailureMode: dbpb.FailureMode_FAILURE_MODE_ALL},
		},
		executions: []*dbpb.JobExecution{
			execWithStatus(5, 50, "w1", dbpb.JobStatus_JOB_STATUS_RUNNING),
			execWithStatus(5, 51, "w2", dbpb.JobStatus_JOB_STATUS_RUNNING),
		},
		workers: []*dbpb.Worker{recentWorker("w1", "pool-a"), recentWorker("w2", "pool-a")},
	}
	publisher := &fakeJobStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	// Both workers are still running: the derived status is running, which
	// matches the stored status, so nothing is persisted or fanned out.
	s.processJobStatuses(context.Background(), db, publisher)

	if len(db.updated) != 0 {
		t.Errorf("UpdateJob called %d times, want 0 (status unchanged)", len(db.updated))
	}
	if len(publisher.notified) != 0 {
		t.Errorf("PublishJobStatus called %d times, want 0 (status unchanged)", len(publisher.notified))
	}
}

// TestJobStatusLoopDropsDeadWorkers verifies that a dead worker's in-progress
// execution does not block the job: the job is derived from the alive workers
// only.
func TestJobStatusLoopDropsDeadWorkers(t *testing.T) {
	db := &fakeJobStatusDB{
		jobs: []*dbpb.Job{
			{Id: 6, TargetGroup: "pool-a", Status: dbpb.JobStatus_JOB_STATUS_RUNNING, FailureMode: dbpb.FailureMode_FAILURE_MODE_ALL},
		},
		executions: []*dbpb.JobExecution{
			execWithStatus(6, 60, "w1", dbpb.JobStatus_JOB_STATUS_SUCCEEDED),
			execWithStatus(6, 61, "w2", dbpb.JobStatus_JOB_STATUS_RUNNING), // w2 is dead
		},
		workers: []*dbpb.Worker{
			recentWorker("w1", "pool-a"),
			deadWorker("w2", "pool-a"), // w2 has not heartbeated recently
		},
	}
	publisher := &fakeJobStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	// w2 is dead, so only w1's execution (succeeded) is considered. The job
	// is derived as succeeded (all alive workers succeeded).
	s.processJobStatuses(context.Background(), db, publisher)

	if len(db.updated) != 1 {
		t.Fatalf("UpdateJob called %d times, want 1", len(db.updated))
	}
	if db.updated[0].GetStatus() != dbpb.JobStatus_JOB_STATUS_SUCCEEDED {
		t.Errorf("UpdateJob status = %v, want SUCCEEDED (dead worker dropped)", db.updated[0].GetStatus())
	}
}

// TestJobStatusLoopSkipsAgentJobs verifies that a job with an empty target
// group (an ephemeral agent) is not processed by the job-status loop (it
// reports its status directly to the job row).
func TestJobStatusLoopSkipsAgentJobs(t *testing.T) {
	db := &fakeJobStatusDB{
		jobs: []*dbpb.Job{
			{Id: 7, TargetGroup: "", Status: dbpb.JobStatus_JOB_STATUS_RUNNING},
		},
	}
	publisher := &fakeJobStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	s.processJobStatuses(context.Background(), db, publisher)

	if len(db.updated) != 0 {
		t.Errorf("UpdateJob called %d times, want 0 (agent job skipped)", len(db.updated))
	}
	if len(publisher.notified) != 0 {
		t.Errorf("PublishJobStatus called %d times, want 0 (agent job skipped)", len(publisher.notified))
	}
}

// TestJobStatusLoopNoAliveWorkers verifies that a job whose workers are all
// dead is left running (no update): the watchdog reaps it if it has a
// timeout.
func TestJobStatusLoopNoAliveWorkers(t *testing.T) {
	db := &fakeJobStatusDB{
		jobs: []*dbpb.Job{
			{Id: 8, TargetGroup: "pool-a", Status: dbpb.JobStatus_JOB_STATUS_RUNNING, FailureMode: dbpb.FailureMode_FAILURE_MODE_ALL},
		},
		executions: []*dbpb.JobExecution{
			execWithStatus(8, 80, "w1", dbpb.JobStatus_JOB_STATUS_RUNNING),
		},
		workers: []*dbpb.Worker{
			deadWorker("w1", "pool-a"),
		},
	}
	publisher := &fakeJobStatusPublisher{}
	s := &Server{db: db, logger: testLogger()}

	// w1 is dead, so there are no alive workers with executions. The job is
	// left running (the watchdog reaps it if it has a timeout).
	s.processJobStatuses(context.Background(), db, publisher)

	if len(db.updated) != 0 {
		t.Errorf("UpdateJob called %d times, want 0 (no alive workers)", len(db.updated))
	}
	if len(publisher.notified) != 0 {
		t.Errorf("PublishJobStatus called %d times, want 0 (no alive workers)", len(publisher.notified))
	}
}
