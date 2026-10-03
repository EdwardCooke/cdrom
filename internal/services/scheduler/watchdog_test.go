package scheduler

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// testLogger returns a silent logger so test output stays clean.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fakeDB is a stub DatabaseClient for the watchdog tests.
type fakeDB struct {
	running []*dbpb.Job
	reaped  map[int64]bool // job id -> whether ReapJob reported reaped
}

func (f *fakeDB) ListJobs(ctx context.Context, in *dbpb.ListJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error) {
	if in.GetStatus() != dbpb.JobStatus_JOB_STATUS_RUNNING {
		return &dbpb.ListJobsResponse{}, nil
	}
	resp := &dbpb.ListJobsResponse{}
	for _, job := range f.running {
		resp.Jobs = append(resp.Jobs, job)
	}
	return resp, nil
}

func (f *fakeDB) ReapJob(ctx context.Context, in *dbpb.ReapJobRequest, opts ...grpc.CallOption) (*dbpb.ReapJobResponse, error) {
	reaped := f.reaped[in.GetId()]
	if reaped {
		// A reaped job leaves the running set.
		for i, job := range f.running {
			if job.GetId() == in.GetId() {
				f.running = append(f.running[:i], f.running[i+1:]...)
				break
			}
		}
	}
	return &dbpb.ReapJobResponse{Reaped: reaped}, nil
}

// fakeStatusPublisher is a stub Database publisher that records the
// "job_status" events the watchdog appends to the shared event log (F-23).
type fakeStatusPublisher struct {
	notified map[int64]dbpb.JobStatus
}

func (f *fakeStatusPublisher) PublishJobStatus(ctx context.Context, in *dbpb.PublishJobStatusRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	if f.notified == nil {
		f.notified = make(map[int64]dbpb.JobStatus)
	}
	f.notified[in.GetJobId()] = in.GetStatus()
	return &dbpb.PublishEventResponse{}, nil
}

// TestEffectiveTimeout verifies the effective-timeout rule: a job-level
// timeout wins when set; otherwise the longest per-step timeout is used; a
// job with neither has no timeout (zero).
func TestEffectiveTimeout(t *testing.T) {
	cases := []struct {
		name string
		job  *dbpb.Job
		want time.Duration
	}{
		{"job-level wins", &dbpb.Job{Spec: &dbpb.JobSpec{
			Timeout: durationOf(10 * time.Second),
			Steps:   []*dbpb.JobStep{{Timeout: durationOf(5 * time.Second)}},
		}}, 10 * time.Second},
		{"falls back to longest step", &dbpb.Job{Spec: &dbpb.JobSpec{
			Steps: []*dbpb.JobStep{
				{Timeout: durationOf(5 * time.Second)},
				{Timeout: durationOf(30 * time.Second)},
				{Timeout: durationOf(10 * time.Second)},
			},
		}}, 30 * time.Second},
		{"no timeout", &dbpb.Job{Spec: &dbpb.JobSpec{
			Steps: []*dbpb.JobStep{{Params: map[string]*dbpb.ParamValue{"command": {String_: "go"}}}},
		}}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveTimeout(tc.job); got != tc.want {
				t.Errorf("effectiveTimeout = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestJobDeadline verifies the deadline is started_at + effective timeout, and
// that a job with no timeout (or no start time) has no deadline.
func TestJobDeadline(t *testing.T) {
	started := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	job := &dbpb.Job{
		StartedAt: timestamppb.New(started),
		Spec:      &dbpb.JobSpec{Timeout: durationOf(10 * time.Second)},
	}
	deadline, ok := jobDeadline(job)
	if !ok {
		t.Fatal("job with a timeout has no deadline, want one")
	}
	if want := started.Add(10 * time.Second); !deadline.Equal(want) {
		t.Errorf("deadline = %s, want %s", deadline, want)
	}

	// No timeout: no deadline.
	if _, ok := jobDeadline(&dbpb.Job{StartedAt: timestamppb.New(started)}); ok {
		t.Error("job with no timeout has a deadline, want none")
	}
	// No start time: no deadline.
	if _, ok := jobDeadline(&dbpb.Job{Spec: &dbpb.JobSpec{Timeout: durationOf(10 * time.Second)}}); ok {
		t.Error("job with no start time has a deadline, want none")
	}
}

// TestReapTimedOutJobs verifies the watchdog reaps a running job that has
// exceeded its timeout (and notifies the API), leaves a job still within its
// timeout alone, and never reaps a job with no timeout.
func TestReapTimedOutJobs(t *testing.T) {
	now := time.Now()
	db := &fakeDB{
		running: []*dbpb.Job{
			// Past its deadline (started 100s ago, 10s timeout).
			{Id: 1, StartedAt: timestamppb.New(now.Add(-100 * time.Second)),
				Spec: &dbpb.JobSpec{Timeout: durationOf(10 * time.Second)}},
			// Still within its deadline (started 1s ago, 10s timeout).
			{Id: 2, StartedAt: timestamppb.New(now.Add(-1 * time.Second)),
				Spec: &dbpb.JobSpec{Timeout: durationOf(10 * time.Second)}},
			// No timeout: runs unbounded, never reaped.
			{Id: 3, StartedAt: timestamppb.New(now.Add(-100 * time.Second))},
		},
		reaped: map[int64]bool{1: true}, // only job 1 is reaped by the db
	}
	publisher := &fakeStatusPublisher{}
	s := &Server{logger: testLogger()}

	s.reapTimedOutJobs(context.Background(), db, publisher)

	if _, ok := publisher.notified[1]; !ok {
		t.Error("job 1 (timed out) was not published")
	}
	if status, ok := publisher.notified[1]; !ok || status != dbpb.JobStatus_JOB_STATUS_TIMED_OUT {
		t.Errorf("job 1 published status = %v, want TIMED_OUT", status)
	}
	if _, ok := publisher.notified[2]; ok {
		t.Error("job 2 (within timeout) was published, want it left alone")
	}
	if _, ok := publisher.notified[3]; ok {
		t.Error("job 3 (no timeout) was published, want it left alone")
	}
}

// TestReapTimedOutJobsNoAPI verifies the watchdog still reaps (via the db)
// when the API client is nil (e.g. a test or a degraded control plane): the
// notification is skipped but the job is still reaped.
func TestReapTimedOutJobsNoAPI(t *testing.T) {
	now := time.Now()
	db := &fakeDB{
		running: []*dbpb.Job{
			{Id: 1, StartedAt: timestamppb.New(now.Add(-100 * time.Second)),
				Spec: &dbpb.JobSpec{Timeout: durationOf(10 * time.Second)}},
		},
		reaped: map[int64]bool{1: true},
	}
	s := &Server{logger: testLogger()}

	s.reapTimedOutJobs(context.Background(), db, nil)

	if len(db.running) != 0 {
		t.Errorf("running jobs after reap = %d, want 0 (job 1 should be reaped)", len(db.running))
	}
}

// durationOf wraps a duration in a proto Duration.
func durationOf(d time.Duration) *durationpb.Duration {
	return durationpb.New(d)
}
