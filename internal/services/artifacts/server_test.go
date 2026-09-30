package artifacts

import (
	"context"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
)

// startServer boots a real Artifacts gRPC server on an in-process bufconn
// listener rooted at a temp dir, and returns a connected client.
func startServer(t *testing.T) artifactspb.ArtifactsClient {
	t.Helper()
	srvImpl, err := NewFilesystemServer(t.TempDir())
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

// upload streams a single-chunk artifact (metadata + data) and returns the
// stored artifact.
func upload(t *testing.T, client artifactspb.ArtifactsClient, jobID, name, data string) *artifactspb.Artifact {
	t.Helper()
	stream, err := client.UploadArtifact(context.Background())
	if err != nil {
		t.Fatalf("UploadArtifact: %v", err)
	}
	chunk := &artifactspb.ArtifactChunk{
		Metadata: &artifactspb.Artifact{Namespace: jobID, Name: name},
		Data:     []byte(data),
	}
	if err := stream.Send(chunk); err != nil {
		t.Fatalf("send: %v", err)
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	return response.GetArtifact()
}

func TestUploadDownloadRoundTrip(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	const payload = "hello, artifacts"
	artifact := upload(t, client, "job-1", "out.txt", payload)
	if artifact.GetSize() != int64(len(payload)) {
		t.Fatalf("size = %d, want %d", artifact.GetSize(), len(payload))
	}

	stream, err := client.DownloadArtifact(ctx, &artifactspb.DownloadArtifactRequest{Namespace: "job-1", Name: "out.txt"})
	if err != nil {
		t.Fatalf("DownloadArtifact: %v", err)
	}
	firstChunk, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv metadata: %v", err)
	}
	if firstChunk.GetMetadata() == nil || firstChunk.GetMetadata().GetName() != "out.txt" {
		t.Fatalf("first chunk metadata = %v", firstChunk.GetMetadata())
	}
	var downloaded []byte
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		downloaded = append(downloaded, chunk.GetData()...)
	}
	if string(downloaded) != payload {
		t.Fatalf("downloaded = %q, want %q", downloaded, payload)
	}
}

func TestListAndDelete(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	upload(t, client, "job-1", "a.txt", "aaa")
	upload(t, client, "job-1", "b.txt", "bbb")
	upload(t, client, "job-2", "c.txt", "ccc")

	response, err := client.ListArtifacts(ctx, &artifactspb.ListArtifactsRequest{Namespace: "job-1"})
	if err != nil {
		t.Fatalf("ListArtifacts: %v", err)
	}
	if got := len(response.GetArtifacts()); got != 2 {
		t.Fatalf("job-1 artifacts = %d, want 2", got)
	}

	if _, err := client.DeleteArtifact(ctx, &artifactspb.DeleteArtifactRequest{Namespace: "job-1", Name: "a.txt"}); err != nil {
		t.Fatalf("DeleteArtifact: %v", err)
	}
	if _, err := client.GetArtifact(ctx, &artifactspb.GetArtifactRequest{Namespace: "job-1", Name: "a.txt"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetArtifact after delete = %v, want NotFound", err)
	}
}

func TestUploadRejectsBadMetadata(t *testing.T) {
	client := startServer(t)
	stream, err := client.UploadArtifact(context.Background())
	if err != nil {
		t.Fatalf("UploadArtifact: %v", err)
	}
	// First chunk without metadata must be rejected.
	if err := stream.Send(&artifactspb.ArtifactChunk{Data: []byte("x")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("close = %v, want InvalidArgument", err)
	}
}

// appendLog streams data to a job's log and returns the updated metadata.
func appendLog(t *testing.T, client artifactspb.ArtifactsClient, jobID, name, data string) *artifactspb.Artifact {
	t.Helper()
	stream, err := client.AppendLog(context.Background())
	if err != nil {
		t.Fatalf("AppendLog: %v", err)
	}
	if err := stream.Send(&artifactspb.ArtifactChunk{
		Metadata: &artifactspb.Artifact{Namespace: jobID, Name: name},
		Data:     []byte(data),
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	return response.GetArtifact()
}

// downloadLog streams a job's log back and returns its text.
func downloadLog(t *testing.T, client artifactspb.ArtifactsClient, jobID, name string) string {
	t.Helper()
	stream, err := client.DownloadLog(context.Background(), &artifactspb.DownloadLogRequest{Namespace: jobID, Name: name})
	if err != nil {
		t.Fatalf("DownloadLog: %v", err)
	}
	var out []byte
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		out = append(out, chunk.GetData()...)
	}
	return string(out)
}

// TestAppendLogGrowsAndReplays verifies that a log is appended to as a job
// progresses (so it can be read while running) and replayed in full after it
// finishes: two appends to the same log accumulate, and the combined content
// is returned on download.
func TestAppendLogGrowsAndReplays(t *testing.T) {
	client := startServer(t)

	// A step's output arrives in chunks as the step runs.
	if artifact := appendLog(t, client, "job-1", "step-0.log", "line one\n"); artifact.GetSize() != int64(len("line one\n")) {
		t.Fatalf("size after first append = %d, want %d", artifact.GetSize(), len("line one\n"))
	}
	// A second append grows the same log (the step is still running).
	if artifact := appendLog(t, client, "job-1", "step-0.log", "line two\n"); artifact.GetSize() != int64(len("line one\nline two\n")) {
		t.Fatalf("size after second append = %d, want %d", artifact.GetSize(), len("line one\nline two\n"))
	}
	if got := downloadLog(t, client, "job-1", "step-0.log"); got != "line one\nline two\n" {
		t.Fatalf("log = %q, want %q", got, "line one\nline two\n")
	}
}

// TestListLogsAndGetLog verifies that a job's logs are listed (one per step
// plus the combined job.log) and that a single log's metadata is retrievable.
func TestListLogsAndGetLog(t *testing.T) {
	client := startServer(t)

	appendLog(t, client, "job-1", "step-0.log", "a")
	appendLog(t, client, "job-1", "step-1.log", "b")
	appendLog(t, client, "job-1", "job.log", "ab")
	appendLog(t, client, "job-2", "step-0.log", "c")

	response, err := client.ListLogs(context.Background(), &artifactspb.ListLogsRequest{Namespace: "job-1"})
	if err != nil {
		t.Fatalf("ListLogs: %v", err)
	}
	if got := len(response.GetLogs()); got != 3 {
		t.Fatalf("job-1 logs = %d, want 3", got)
	}

	artifact, err := client.GetLog(context.Background(), &artifactspb.GetLogRequest{Namespace: "job-1", Name: "step-0.log"})
	if err != nil {
		t.Fatalf("GetLog: %v", err)
	}
	if artifact.GetSize() != 1 {
		t.Errorf("step-0.log size = %d, want 1", artifact.GetSize())
	}

	// A log that was never written is NotFound.
	if _, err := client.GetLog(context.Background(), &artifactspb.GetLogRequest{Namespace: "job-1", Name: "step-9.log"}); status.Code(err) != codes.NotFound {
		t.Errorf("GetLog missing = %v, want NotFound", err)
	}
}

// TestLogsAndArtifactsAreSeparate verifies that a job's logs and artifacts do
// not see each other's files: listing artifacts excludes logs and vice versa.
func TestLogsAndArtifactsAreSeparate(t *testing.T) {
	client := startServer(t)

	upload(t, client, "job-1", "out.txt", "artifact")
	appendLog(t, client, "job-1", "step-0.log", "log")

	artifacts, err := client.ListArtifacts(context.Background(), &artifactspb.ListArtifactsRequest{Namespace: "job-1"})
	if err != nil {
		t.Fatalf("ListArtifacts: %v", err)
	}
	if got := len(artifacts.GetArtifacts()); got != 1 {
		t.Fatalf("job-1 artifacts = %d, want 1 (logs must not appear)", got)
	}
	logs, err := client.ListLogs(context.Background(), &artifactspb.ListLogsRequest{Namespace: "job-1"})
	if err != nil {
		t.Fatalf("ListLogs: %v", err)
	}
	if got := len(logs.GetLogs()); got != 1 {
		t.Fatalf("job-1 logs = %d, want 1 (artifacts must not appear)", got)
	}
}
