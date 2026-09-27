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
	srvImpl, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
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
		Metadata: &artifactspb.Artifact{JobId: jobID, Name: name},
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

	stream, err := client.DownloadArtifact(ctx, &artifactspb.DownloadArtifactRequest{JobId: "job-1", Name: "out.txt"})
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

	response, err := client.ListArtifacts(ctx, &artifactspb.ListArtifactsRequest{JobId: "job-1"})
	if err != nil {
		t.Fatalf("ListArtifacts: %v", err)
	}
	if got := len(response.GetArtifacts()); got != 2 {
		t.Fatalf("job-1 artifacts = %d, want 2", got)
	}

	if _, err := client.DeleteArtifact(ctx, &artifactspb.DeleteArtifactRequest{JobId: "job-1", Name: "a.txt"}); err != nil {
		t.Fatalf("DeleteArtifact: %v", err)
	}
	if _, err := client.GetArtifact(ctx, &artifactspb.GetArtifactRequest{JobId: "job-1", Name: "a.txt"}); status.Code(err) != codes.NotFound {
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
