package api

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	"cdrom/internal/services/artifacts"
)

// startArtifactsServer boots a real Artifacts gRPC server on an in-process
// bufconn listener rooted at a temp dir, and returns a connected client.
func startArtifactsServer(t *testing.T) artifactspb.ArtifactsClient {
	t.Helper()
	srvImpl, err := artifacts.NewFilesystemServer(t.TempDir())
	if err != nil {
		t.Fatalf("NewFilesystemServer: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	artifactspb.RegisterArtifactsServer(srv, srvImpl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.DialContext(context.Background(), "bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return artifactspb.NewArtifactsClient(conn)
}

// startAPIServer boots a real API gRPC server on an in-process bufconn
// listener and returns a connected client.
func startAPIServer(t *testing.T, artifacts artifactspb.ArtifactsClient, hub *EventHub) apipb.APIClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	apipb.RegisterAPIServer(srv, NewGRPCServer(nil, artifacts, hub, nil, nil))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.DialContext(context.Background(), "bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return apipb.NewAPIClient(conn)
}

// readLog streams a job's log from the artifacts service and returns its text.
func readLog(t *testing.T, client artifactspb.ArtifactsClient, jobID, name string) string {
	t.Helper()
	stream, err := client.DownloadLog(context.Background(), &artifactspb.DownloadLogRequest{Namespace: jobID, Name: name})
	if err != nil {
		t.Fatalf("DownloadLog %s: %v", name, err)
	}
	var out []byte
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv %s: %v", name, err)
		}
		out = append(out, chunk.GetData()...)
	}
	return string(out)
}

// TestStreamJobLogsPersistsAndPublishes streams a job's output to the API and
// verifies the API (a) persists it to the artifacts service — one file per
// step plus the combined job.log — and (b) fans it out to the UI over the
// event hub as a job_log event.
func TestStreamJobLogsPersistsAndPublishes(t *testing.T) {
	artifacts := startArtifactsServer(t)
	hub := NewEventHub()
	events, cancel := hub.Subscribe()
	defer cancel()

	client := startAPIServer(t, artifacts, hub)
	ctx := context.Background()

	stream, err := client.StreamJobLogs(ctx)
	if err != nil {
		t.Fatalf("StreamJobLogs: %v", err)
	}
	// Step 0 stdout: the first chunk carries metadata.
	if err := stream.Send(&apipb.JobLogChunk{
		Metadata: &apipb.JobLogMetadata{JobId: 42, StepIndex: 0, Stream: apipb.JobLogStream_JOB_LOG_STREAM_STDOUT},
		Data:     []byte("hello "),
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	// Step 0 stdout continued (no metadata).
	if err := stream.Send(&apipb.JobLogChunk{Data: []byte("world")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	// Step 1 stderr.
	if err := stream.Send(&apipb.JobLogChunk{
		Metadata: &apipb.JobLogMetadata{JobId: 42, StepIndex: 1, Stream: apipb.JobLogStream_JOB_LOG_STREAM_STDERR},
		Data:     []byte("boom"),
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if want := int64(len("hello ") + len("world") + len("boom")); resp.GetReceived() != want {
		t.Errorf("received = %d, want %d", resp.GetReceived(), want)
	}

	// The API persisted one file per step plus the combined job.log.
	if got := readLog(t, artifacts, "42", "step-0.log"); got != "hello world" {
		t.Errorf("step-0.log = %q, want %q", got, "hello world")
	}
	if got := readLog(t, artifacts, "42", "step-1.log"); got != "boom" {
		t.Errorf("step-1.log = %q, want %q", got, "boom")
	}
	if got := readLog(t, artifacts, "42", jobLogName); got != "hello worldboom" {
		t.Errorf("job.log = %q, want %q", got, "hello worldboom")
	}

	// The API published a job_log event per chunk.
	var sawStep0, sawStep1 bool
	for i := 0; i < 3; i++ {
		ev := <-events
		if ev.Type != EventJobLog || ev.JobID != 42 {
			t.Fatalf("event = %+v, want a job_log event for job 42", ev)
		}
		if ev.StepIndex == 0 && ev.Stream == JobLogStreamStdout {
			sawStep0 = true
		}
		if ev.StepIndex == 1 && ev.Stream == JobLogStreamStderr {
			sawStep1 = true
		}
	}
	if !sawStep0 {
		t.Error("did not see a step-0 stdout job_log event")
	}
	if !sawStep1 {
		t.Error("did not see a step-1 stderr job_log event")
	}
}

// TestStreamJobLogsRequiresMetadata verifies the first chunk must carry
// metadata with a job_id.
func TestStreamJobLogsRequiresMetadata(t *testing.T) {
	client := startAPIServer(t, startArtifactsServer(t), nil)
	stream, err := client.StreamJobLogs(context.Background())
	if err != nil {
		t.Fatalf("StreamJobLogs: %v", err)
	}
	if err := stream.Send(&apipb.JobLogChunk{Data: []byte("x")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Error("expected an error for a first chunk without metadata")
	}
}

// fakeArtifacts wraps a real artifacts client and can make AppendLog fail (to
// simulate an artifacts-service outage). All other methods delegate to the
// embedded real client.
type fakeArtifacts struct {
	artifactspb.ArtifactsClient
	mu         sync.Mutex
	failAppend bool
}

func (f *fakeArtifacts) setFailAppend(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAppend = fail
}

func (f *fakeArtifacts) AppendLog(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[artifactspb.ArtifactChunk, artifactspb.UploadArtifactResponse], error) {
	f.mu.Lock()
	fail := f.failAppend
	f.mu.Unlock()
	if fail {
		return nil, status.Error(codes.Unavailable, "artifacts service unavailable")
	}
	return f.ArtifactsClient.AppendLog(ctx, opts...)
}

// TestHandleLogChunkRidesOutArtifactsOutage verifies that a transient
// artifacts-service outage does not tear down a target's StreamJobLogs stream:
// handleLogChunk retries persisting a chunk (appendLogWithRetry) and, if the
// outage outlasts the retry budget, drops the chunk and returns nil (so the
// stream continues). Chunks sent after the outage is over are persisted.
func TestHandleLogChunkRidesOutArtifactsOutage(t *testing.T) {
	// Shorten the retry budget so the test is fast.
	origAttempts, origDelay := appendLogAttempts, appendLogRetryDelay
	appendLogAttempts, appendLogRetryDelay = 2, 10*time.Millisecond
	t.Cleanup(func() { appendLogAttempts, appendLogRetryDelay = origAttempts, origDelay })

	real := startArtifactsServer(t)
	fake := &fakeArtifacts{ArtifactsClient: real}
	srv := NewGRPCServer(nil, fake, nil, nil, nil)
	ctx := context.Background()

	var received int64
	// A chunk that persists fine (artifacts is up).
	if err := srv.handleLogChunk(ctx, 50, &apipb.JobLogMetadata{JobId: 50, StepIndex: 0, Stream: apipb.JobLogStream_JOB_LOG_STREAM_STDOUT}, []byte("ok\n"), &received); err != nil {
		t.Fatalf("handleLogChunk (ok): %v", err)
	}
	// Simulate the artifacts service going down.
	fake.setFailAppend(true)
	// A chunk sent during the outage: the API retries, exhausts the budget,
	// and drops it — but handleLogChunk returns nil, so the stream survives.
	if err := srv.handleLogChunk(ctx, 50, nil, []byte("lost\n"), &received); err != nil {
		t.Fatalf("handleLogChunk (lost) must not fail the stream: %v", err)
	}
	// The artifacts service recovers.
	fake.setFailAppend(false)
	// A chunk sent after recovery is persisted.
	if err := srv.handleLogChunk(ctx, 50, nil, []byte("back\n"), &received); err != nil {
		t.Fatalf("handleLogChunk (back): %v", err)
	}

	// The dropped chunk is absent; the pre- and post-outage chunks are present.
	if got := readLog(t, real, "50", "step-0.log"); got != "ok\nback\n" {
		t.Errorf("step-0.log = %q, want %q", got, "ok\nback\n")
	}
	// Every chunk is counted as received, even the dropped one.
	if want := int64(len("ok\n") + len("lost\n") + len("back\n")); received != want {
		t.Errorf("received = %d, want %d", received, want)
	}
}
