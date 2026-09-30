package logstream

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"cdrom/internal/api"
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
func startAPIServer(t *testing.T, artifacts artifactspb.ArtifactsClient) apipb.APIClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	apipb.RegisterAPIServer(srv, api.NewGRPCServer(nil, artifacts, nil, nil, nil))
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

// tryReadLog streams a job's log from the artifacts service and returns its
// text, or an error if the log does not exist yet.
func tryReadLog(client artifactspb.ArtifactsClient, jobID, name string) (string, error) {
	stream, err := client.DownloadLog(context.Background(), &artifactspb.DownloadLogRequest{Namespace: jobID, Name: name})
	if err != nil {
		return "", err
	}
	var out []byte
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		out = append(out, chunk.GetData()...)
	}
	return string(out), nil
}

// readLog streams a job's log from the artifacts service and returns its text,
// failing the test on error.
func readLog(t *testing.T, client artifactspb.ArtifactsClient, jobID, name string) string {
	t.Helper()
	got, err := tryReadLog(client, jobID, name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return got
}

// waitForLog polls the log until it returns want (the API persists streamed
// chunks asynchronously, so the log may not be complete right after the sink
// closes) or the deadline elapses.
func waitForLog(t *testing.T, client artifactspb.ArtifactsClient, jobID, name, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last string
	for {
		got, err := tryReadLog(client, jobID, name)
		if err == nil && got == want {
			return
		}
		if err == nil {
			last = got
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s = %q after 5s, want %q", name, last, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSinkStreamsToAPI verifies that a sink opened against the API streams a
// job's step output to the API, which persists it to the artifacts service
// (one file per step plus the combined job.log). This exercises the full
// sink -> API -> artifacts path (F-02).
func TestSinkStreamsToAPI(t *testing.T) {
	artifacts := startArtifactsServer(t)
	apiClient := startAPIServer(t, artifacts)
	ctx := context.Background()

	sink, err := NewSink(apiClient, ctx, 7, "", nil)
	if err != nil {
		t.Fatalf("NewSink: %v", err)
	}
	sink.WriteStepOutput(0, "stdout", []byte("step0 out\n"))
	sink.WriteStepOutput(0, "stderr", []byte("step0 err\n"))
	sink.WriteStepOutput(1, "stdout", []byte("step1 out\n"))
	sink.Close()

	waitForLog(t, artifacts, "7", "step-0.log", "step0 out\nstep0 err\n")
	waitForLog(t, artifacts, "7", "step-1.log", "step1 out\n")
	waitForLog(t, artifacts, "7", "job.log", "step0 out\nstep0 err\nstep1 out\n")
}

// TestSinkNilAPI verifies that a sink with a nil API is a no-op (streaming
// disabled) and never panics.
func TestSinkNilAPI(t *testing.T) {
	sink, err := NewSink(nil, context.Background(), 1, "", nil)
	if err != nil {
		t.Fatalf("NewSink(nil): %v", err)
	}
	if sink != nil {
		t.Fatalf("NewSink(nil) = %v, want nil", sink)
	}
	// A nil sink must be safe to use.
	sink.WriteStepOutput(0, "stdout", []byte("x"))
	sink.Close()
}

// ---------------------------------------------------------------------------
// Reconnection (deterministic, via a fake API client)
// ---------------------------------------------------------------------------

// fakeLogStream is a StreamJobLogs client stream that records successful
// Sends and fails them once the API tears it down (simulating the API going
// away). It implements grpc.ClientStreamingClient[JobLogChunk,
// StreamJobLogsResponse].
type fakeLogStream struct {
	id     int
	client *fakeAPIClient
	mu     sync.Mutex
	down   bool // set when the API tears this stream down
}

func (s *fakeLogStream) tearDown() {
	s.mu.Lock()
	s.down = true
	s.mu.Unlock()
}

func (s *fakeLogStream) Send(chunk *apipb.JobLogChunk) error {
	s.mu.Lock()
	down := s.down
	s.mu.Unlock()
	if down {
		return status.Error(codes.Unavailable, "stream broken")
	}
	s.client.record(s.id, string(chunk.GetData()))
	return nil
}

func (s *fakeLogStream) CloseAndRecv() (*apipb.StreamJobLogsResponse, error) {
	return &apipb.StreamJobLogsResponse{}, nil
}
func (s *fakeLogStream) CloseSend() error             { return nil }
func (s *fakeLogStream) Context() context.Context     { return context.Background() }
func (s *fakeLogStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeLogStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeLogStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *fakeLogStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *fakeLogStream) SendMsg(m any) error          { return nil }
func (s *fakeLogStream) RecvMsg(m any) error          { return io.EOF }

// deliveredChunk is a chunk the sink successfully sent, tagged with the
// stream it went over.
type deliveredChunk struct {
	streamID int
	data     string
}

// fakeAPIClient is an apipb.APIClient whose StreamJobLogs can be toggled
// down/up to simulate the API going away and coming back. All other methods
// are unused by the sink (the embedded interface is nil).
type fakeAPIClient struct {
	apipb.APIClient // nil; only StreamJobLogs is used by the sink
	mu              sync.Mutex
	down            bool
	nextID          int
	streams         []*fakeLogStream
	delivered       []deliveredChunk
}

// setDown toggles the API down/up. When going down it tears down every open
// stream (the API went away); when coming up new StreamJobLogs calls succeed.
func (f *fakeAPIClient) setDown(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
	if down {
		for _, s := range f.streams {
			s.tearDown()
		}
	}
}

func (f *fakeAPIClient) StreamJobLogs(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[apipb.JobLogChunk, apipb.StreamJobLogsResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return nil, status.Error(codes.Unavailable, "api unavailable")
	}
	f.nextID++
	s := &fakeLogStream{id: f.nextID, client: f}
	f.streams = append(f.streams, s)
	return s, nil
}

func (f *fakeAPIClient) record(streamID int, data string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = append(f.delivered, deliveredChunk{streamID: streamID, data: data})
}

// streamIDFor returns the id of the stream that first delivered want, or 0.
func (f *fakeAPIClient) streamIDFor(want string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, d := range f.delivered {
		if d.data == want {
			return d.streamID
		}
	}
	return 0
}

// deliveredOnNewerStream reports whether want was delivered on a stream opened
// after minID (i.e. on a reconnected stream).
func (f *fakeAPIClient) deliveredOnNewerStream(want string, minID int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, d := range f.delivered {
		if d.data == want && d.streamID > minID {
			return true
		}
	}
	return false
}

func (f *fakeAPIClient) allDelivered() []deliveredChunk {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]deliveredChunk(nil), f.delivered...)
}

// TestSinkReconnectsAfterAPIRestart verifies that when the API goes away
// mid-job the sink reopens a fresh StreamJobLogs stream once the API is back
// and resumes delivering output on the new stream. This is the resilience a
// K8s deployment needs: an API pod restart must not permanently kill a
// running job's log streaming. It uses a fake API client (whose stream fails
// immediately when "down") so the reconnection logic is exercised
// deterministically, without real-gRPC transport timing.
func TestSinkReconnectsAfterAPIRestart(t *testing.T) {
	api := &fakeAPIClient{}
	ctx := context.Background()

	sink, err := NewSink(api, ctx, 9, "", nil)
	if err != nil {
		t.Fatalf("NewSink: %v", err)
	}

	// API up: "before" is delivered on the first stream.
	sink.WriteStepOutput(0, "stdout", []byte("before\n"))
	deadline := time.Now().Add(5 * time.Second)
	for api.streamIDFor("before\n") == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("did not observe %q delivered; delivered=%v", "before\n", api.allDelivered())
		}
		time.Sleep(20 * time.Millisecond)
	}
	beforeID := api.streamIDFor("before\n")

	// Simulate the API pod going away: tear down the open stream.
	api.setDown(true)

	// Output while the API is down. The sink's stream breaks on the next send
	// and it waits for the API to come back.
	sink.WriteStepOutput(0, "stdout", []byte("during\n"))
	// Give the drain a moment to consume "during" and hit the broken stream.
	time.Sleep(100 * time.Millisecond)

	// Bring the API back (a redeploy).
	api.setDown(false)

	// Output after the restart is delivered once the sink reconnects.
	sink.WriteStepOutput(0, "stdout", []byte("after\n"))

	// Wait for the sink to reconnect and deliver "after" on a NEW stream
	// (proof it reopened the stream after the API came back).
	deadline = time.Now().Add(5 * time.Second)
	for !api.deliveredOnNewerStream("after\n", beforeID) {
		if time.Now().After(deadline) {
			t.Fatalf("sink did not reconnect and resume; delivered=%v", api.allDelivered())
		}
		time.Sleep(20 * time.Millisecond)
	}

	sink.Close()
}
