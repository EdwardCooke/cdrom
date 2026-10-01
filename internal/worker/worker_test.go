package worker

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// testLogger returns a silent logger so test output stays clean.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fakeWatchStream is a grpc.ServerStreamingClient[WatchMessage] backed by a
// channel: the test pushes WatchMessages into the channel and the worker's
// watch loop receives them. It embeds a nil grpc.ClientStream to satisfy the
// interface's other methods (Context, Header, Trailer, SendMsg, RecvMsg,
// CloseSend), which the worker never calls.
type fakeWatchStream struct {
	grpc.ClientStream
	ch chan *apipb.WatchMessage
}

func (f *fakeWatchStream) Recv() (*apipb.WatchMessage, error) {
	msg, ok := <-f.ch
	if !ok {
		return nil, io.EOF
	}
	return msg, nil
}

// fakeAPIClient is a stub APIClient for worker tests. Only the methods a
// worker uses are implemented; the rest are never called. StreamJobLogs fails
// so the log sink (best-effort) does not block the job.
type fakeAPIClient struct {
	apipb.APIClient // nil; only the methods below are used by the worker
	watchCh         chan *apipb.WatchMessage
	mu              sync.Mutex
	statuses        []dbpb.JobStatus
}

func (f *fakeAPIClient) RegisterWorker(ctx context.Context, in *apipb.RegisterWorkerRequest, opts ...grpc.CallOption) (*apipb.Worker, error) {
	return &apipb.Worker{Name: in.GetName(), Group: in.GetGroup()}, nil
}

func (f *fakeAPIClient) DeregisterWorker(ctx context.Context, in *apipb.DeregisterWorkerRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeAPIClient) Heartbeat(ctx context.Context, in *apipb.HeartbeatRequest, opts ...grpc.CallOption) (*apipb.HeartbeatResponse, error) {
	return &apipb.HeartbeatResponse{}, nil
}

func (f *fakeAPIClient) WatchJobs(ctx context.Context, in *apipb.WatchJobsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[apipb.WatchMessage], error) {
	return &fakeWatchStream{ch: f.watchCh}, nil
}

func (f *fakeAPIClient) ReportJobStatus(ctx context.Context, in *apipb.ReportJobStatusRequest, opts ...grpc.CallOption) (*apipb.Job, error) {
	f.mu.Lock()
	f.statuses = append(f.statuses, in.GetStatus())
	f.mu.Unlock()
	return &apipb.Job{Id: in.GetJobId(), Status: in.GetStatus()}, nil
}

func (f *fakeAPIClient) StreamJobLogs(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[apipb.JobLogChunk, apipb.StreamJobLogsResponse], error) {
	return nil, status.Error(codes.Unavailable, "streaming disabled in test")
}

// waitForStatus polls the fake API client until the job reports the wanted
// status (or the deadline elapses).
func waitForStatus(t *testing.T, api *fakeAPIClient, want dbpb.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		api.mu.Lock()
		for _, s := range api.statuses {
			if s == want {
				api.mu.Unlock()
				return
			}
		}
		api.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	api.mu.Lock()
	got := append([]dbpb.JobStatus(nil), api.statuses...)
	api.mu.Unlock()
	t.Fatalf("timed out waiting for status %v (reported %v)", want, got)
}

// TestWorkerCancelInterruptsRunningJob verifies cancellation propagation
// (F-05): a JobCancellation delivered on the worker's WatchJobs stream
// interrupts the running step (terminating the command) and the worker
// reports the job as cancelled.
func TestWorkerCancelInterruptsRunningJob(t *testing.T) {
	// A step that blocks until its context is done: a stand-in for a long
	// command that must be interrupted on cancel.
	executor.RegisterStepType("cancel-blocker", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		<-ctx.Done()
		return ctx.Err()
	})

	watchCh := make(chan *apipb.WatchMessage, 8)
	t.Cleanup(func() { close(watchCh) }) // unblock the watch loop on test end
	api := &fakeAPIClient{watchCh: watchCh}
	w := New("w1", "pool-a", Dependencies{API: api}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	// Dispatch a job that blocks until it is cancelled.
	job := &apipb.Job{
		Id:   42,
		Name: "long",
		Spec: &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Type: "cancel-blocker"}}},
	}
	watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Assignment{
		Assignment: &apipb.JobAssignment{Job: job, WorkerName: "w1"},
	}}

	// Wait for the worker to report the job running.
	waitForStatus(t, api, dbpb.JobStatus_JOB_STATUS_RUNNING)

	// Cancel the job: the API delivers a JobCancellation down the stream.
	watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Cancellation{
		Cancellation: &apipb.JobCancellation{JobId: 42},
	}}

	// The worker interrupts the running step and reports the job cancelled.
	waitForStatus(t, api, dbpb.JobStatus_JOB_STATUS_CANCELLED)
}

// TestWorkerCancelForUnknownJobIsNoop verifies that a cancellation for a job
// that is not running on the worker is ignored (the job's status is
// reconciled by the database), and does not disturb the worker.
func TestWorkerCancelForUnknownJobIsNoop(t *testing.T) {
	watchCh := make(chan *apipb.WatchMessage, 8)
	t.Cleanup(func() { close(watchCh) })
	api := &fakeAPIClient{watchCh: watchCh}
	w := New("w1", "pool-a", Dependencies{API: api}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	// A cancellation for a job the worker is not running is a no-op.
	watchCh <- &apipb.WatchMessage{Message: &apipb.WatchMessage_Cancellation{
		Cancellation: &apipb.JobCancellation{JobId: 99},
	}}

	// The worker keeps running (no status is reported for the unknown job).
	time.Sleep(100 * time.Millisecond)
	api.mu.Lock()
	got := append([]dbpb.JobStatus(nil), api.statuses...)
	api.mu.Unlock()
	if len(got) != 0 {
		t.Errorf("worker reported statuses %v for an unknown job, want none", got)
	}
}
