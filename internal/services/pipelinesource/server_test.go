package pipelinesource

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"

	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
)

// startServer starts a PipelineSource gRPC server on a random port and returns
// a client connected to it.
func startServer(t *testing.T, cacheRoot, fileRoot string) (pipelinesourcepb.PipelineSourceClient, func()) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	srv, err := NewServer(cacheRoot, fileRoot, logger)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	grpcSrv := grpc.NewServer()
	pipelinesourcepb.RegisterPipelineSourceServer(grpcSrv, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go grpcSrv.Serve(lis)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		grpcSrv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return pipelinesourcepb.NewPipelineSourceClient(conn), func() {
		conn.Close()
		grpcSrv.Stop()
	}
}

func TestFetchSourceFile(t *testing.T) {
	fileRoot := t.TempDir()
	writeFile(t, filepath.Join(fileRoot, "vars.yaml"), "x: 1\n")
	client, stop := startServer(t, t.TempDir(), fileRoot)
	defer stop()

	resp, err := client.FetchSource(context.Background(), &pipelinesourcepb.FetchSourceRequest{
		Reference: "file:/vars.yaml",
	})
	if err != nil {
		t.Fatalf("FetchSource: %v", err)
	}
	if string(resp.GetContent()) != "x: 1\n" {
		t.Fatalf("content = %q", resp.GetContent())
	}
	if resp.GetVersion() == "" || resp.GetHash() == "" {
		t.Fatalf("version/hash empty: %+v", resp)
	}
}

func TestFetchSourceFileDisabled(t *testing.T) {
	client, stop := startServer(t, t.TempDir(), "")
	defer stop()
	if _, err := client.FetchSource(context.Background(), &pipelinesourcepb.FetchSourceRequest{
		Reference: "file:/vars.yaml",
	}); err == nil {
		t.Fatal("file fetch should fail when no root is configured")
	}
}

func TestFetchSourceHTTP(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok123" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer tok123")
		}
		if got := r.Header.Get("X-Request-Id"); got != "req-42" {
			t.Errorf("X-Request-Id = %q, want %q", got, "req-42")
		}
		w.Header().Set("ETag", `"v1"`)
		io.WriteString(w, "http: body\n")
	}))
	defer ts.Close()

	client, stop := startServer(t, t.TempDir(), "")
	defer stop()
	resp, err := client.FetchSource(context.Background(), &pipelinesourcepb.FetchSourceRequest{
		Reference: "http:" + ts.URL + "/file.yaml",
		Credential: &pipelinesourcepb.SourceCredential{
			Kind: "http",
			Headers: []*pipelinesourcepb.HTTPHeader{
				{Name: "Authorization", Value: "Bearer tok123"},
				{Name: "X-Request-Id", Value: "req-42"},
			},
		},
	})
	if err != nil {
		t.Fatalf("FetchSource: %v", err)
	}
	if string(resp.GetContent()) != "http: body\n" {
		t.Fatalf("content = %q", resp.GetContent())
	}
	if resp.GetVersion() != `"v1"` {
		t.Fatalf("version = %q, want ETag", resp.GetVersion())
	}
}

func TestFetchSourceUnknownType(t *testing.T) {
	client, stop := startServer(t, t.TempDir(), "")
	defer stop()
	if _, err := client.FetchSource(context.Background(), &pipelinesourcepb.FetchSourceRequest{
		Reference: "svn:https://example.com/repo",
	}); err == nil {
		t.Fatal("unknown type should error")
	}
}
