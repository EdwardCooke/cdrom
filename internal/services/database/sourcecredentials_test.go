package database

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// startSourceCredentialServer opens a SQLite database, migrates it, and serves
// the Database gRPC service over an in-process bufconn listener. It returns the
// gRPC client.
func startSourceCredentialServer(t *testing.T) dbpb.DatabaseClient {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(Config{Backend: BackendSQLite, SQLitePath: dbPath}, logger)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = Close(db) })
	if err := Migrate(db, logger); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	dbpb.RegisterDatabaseServer(srv, NewServer(db))
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
	return dbpb.NewDatabaseClient(conn)
}

// TestSourceCredentialCRUD exercises the source-credential CRUD (F-21.3):
// create, list, get, update, duplicate rejection, unknown-kind rejection, and
// delete. The secret material is stored opaquely (the Database service never
// sees the key — the API encrypts it before this call).
func TestSourceCredentialCRUD(t *testing.T) {
	client := startSourceCredentialServer(t)
	ctx := context.Background()

	// Create a git-https credential (the secret material is stored as given —
	// the API would have encrypted it before this call).
	created, err := client.CreateSourceCredential(ctx, &dbpb.CreateSourceCredentialRequest{
		Name:        "github-bot",
		Kind:        "git-https",
		Prefix:      "https://github.com/myorg/",
		Description: "bot token for myorg",
		Username:    "bot",
		Token:       "ciphertext-token",
	})
	if err != nil {
		t.Fatalf("CreateSourceCredential: %v", err)
	}
	if created.GetName() != "github-bot" || created.GetKind() != "git-https" || created.GetPrefix() != "https://github.com/myorg/" {
		t.Fatalf("created = %+v", created)
	}
	if created.GetToken() != "ciphertext-token" {
		t.Fatalf("token not stored opaquely: %q", created.GetToken())
	}

	// A duplicate name is rejected.
	if _, err := client.CreateSourceCredential(ctx, &dbpb.CreateSourceCredentialRequest{
		Name:   "github-bot",
		Kind:   "git-https",
		Prefix: "https://github.com/other/",
	}); err == nil {
		t.Fatal("duplicate name should be rejected")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.AlreadyExists {
		t.Fatalf("duplicate should be AlreadyExists, got: %v", err)
	}

	// An unknown kind is rejected.
	if _, err := client.CreateSourceCredential(ctx, &dbpb.CreateSourceCredentialRequest{
		Name: "bad-kind",
		Kind: "svn",
	}); err == nil {
		t.Fatal("unknown kind should be rejected")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("unknown kind should be InvalidArgument, got: %v", err)
	}

	// Create an http credential with multiple headers, each with its own
	// sensitivity (the values are stored opaquely — the API would have
	// encrypted the sensitive one before this call).
	if _, err := client.CreateSourceCredential(ctx, &dbpb.CreateSourceCredentialRequest{
		Name:   "api-endpoint",
		Kind:   "http",
		Prefix: "https://api.example.com/",
		Headers: []*dbpb.HTTPHeader{
			{Name: "Authorization", Value: "ciphertext-header", Sensitive: true},
			{Name: "X-Request-Id", Value: "visible-id", Sensitive: false},
		},
	}); err != nil {
		t.Fatalf("CreateSourceCredential (http): %v", err)
	}

	// A duplicate header name is rejected.
	if _, err := client.CreateSourceCredential(ctx, &dbpb.CreateSourceCredentialRequest{
		Name:   "dup-headers",
		Kind:   "http",
		Prefix: "https://api.example.com/",
		Headers: []*dbpb.HTTPHeader{
			{Name: "Authorization", Value: "a"},
			{Name: "authorization", Value: "b"},
		},
	}); err == nil {
		t.Fatal("duplicate header name should be rejected")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("duplicate header should be InvalidArgument, got: %v", err)
	}

	// List returns both, ordered by name.
	list, err := client.ListSourceCredentials(ctx, &dbpb.ListSourceCredentialsRequest{})
	if err != nil {
		t.Fatalf("ListSourceCredentials: %v", err)
	}
	if len(list.GetCredentials()) != 2 {
		t.Fatalf("list = %d, want 2", len(list.GetCredentials()))
	}
	if list.GetCredentials()[0].GetName() != "api-endpoint" || list.GetCredentials()[1].GetName() != "github-bot" {
		t.Fatalf("list order = %v, %v", list.GetCredentials()[0].GetName(), list.GetCredentials()[1].GetName())
	}

	// Get by name.
	got, err := client.GetSourceCredential(ctx, &dbpb.GetSourceCredentialRequest{Name: "api-endpoint"})
	if err != nil {
		t.Fatalf("GetSourceCredential: %v", err)
	}
	if got.GetKind() != "http" || len(got.GetHeaders()) != 2 {
		t.Fatalf("got = %+v", got)
	}
	if got.GetHeaders()[0].GetName() != "Authorization" || got.GetHeaders()[0].GetValue() != "ciphertext-header" || !got.GetHeaders()[0].GetSensitive() {
		t.Fatalf("sensitive header = %+v", got.GetHeaders()[0])
	}
	if got.GetHeaders()[1].GetName() != "X-Request-Id" || got.GetHeaders()[1].GetValue() != "visible-id" || got.GetHeaders()[1].GetSensitive() {
		t.Fatalf("plain header = %+v", got.GetHeaders()[1])
	}

	// A missing credential returns NotFound.
	if _, err := client.GetSourceCredential(ctx, &dbpb.GetSourceCredentialRequest{Name: "nope"}); err == nil {
		t.Fatal("missing credential should be NotFound")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Fatalf("missing should be NotFound, got: %v", err)
	}

	// Update the git-https credential's prefix and token.
	updated, err := client.UpdateSourceCredential(ctx, &dbpb.UpdateSourceCredentialRequest{
		Name:        "github-bot",
		Prefix:      "https://github.com/myorg/",
		Description: "updated",
		Username:    "bot",
		Token:       "ciphertext-token-2",
	})
	if err != nil {
		t.Fatalf("UpdateSourceCredential: %v", err)
	}
	if updated.GetDescription() != "updated" || updated.GetToken() != "ciphertext-token-2" {
		t.Fatalf("updated = %+v", updated)
	}

	// Delete the http credential.
	if _, err := client.DeleteSourceCredential(ctx, &dbpb.DeleteSourceCredentialRequest{Name: "api-endpoint"}); err != nil {
		t.Fatalf("DeleteSourceCredential: %v", err)
	}
	if _, err := client.GetSourceCredential(ctx, &dbpb.GetSourceCredentialRequest{Name: "api-endpoint"}); err == nil {
		t.Fatal("deleted credential should be NotFound")
	}

	// Deleting a missing credential returns NotFound.
	if _, err := client.DeleteSourceCredential(ctx, &dbpb.DeleteSourceCredentialRequest{Name: "api-endpoint"}); err == nil {
		t.Fatal("deleting a missing credential should be NotFound")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Fatalf("delete missing should be NotFound, got: %v", err)
	}
}

// TestPipelineSourceCRUD exercises the minimal pipeline-source slice (F-21.5.1,
// for F-21.3): attach, get, count-by-credential, and the source-state
// upsert/get.
func TestPipelineSourceCRUD(t *testing.T) {
	client := startSourceCredentialServer(t)
	ctx := context.Background()

	// A pipeline must exist to attach a source to.
	pipe, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "sourced-pipeline"})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}

	// Attach a source that names a credential.
	src, err := client.AttachPipelineSource(ctx, &dbpb.AttachPipelineSourceRequest{
		PipelineId: pipe.GetId(),
		Kind:       "http",
		Reference:  "http:https://api.example.com/file.yaml",
		Credential: "api-endpoint",
	})
	if err != nil {
		t.Fatalf("AttachPipelineSource: %v", err)
	}
	if src.GetKind() != "http" || src.GetCredential() != "api-endpoint" {
		t.Fatalf("src = %+v", src)
	}

	// An unknown kind is rejected.
	if _, err := client.AttachPipelineSource(ctx, &dbpb.AttachPipelineSourceRequest{
		PipelineId: pipe.GetId(), Kind: "svn", Reference: "svn:x",
	}); err == nil {
		t.Fatal("unknown kind should be rejected")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("unknown kind should be InvalidArgument, got: %v", err)
	}

	// Get the source back.
	got, err := client.GetPipelineSource(ctx, &dbpb.GetPipelineSourceRequest{PipelineId: pipe.GetId()})
	if err != nil {
		t.Fatalf("GetPipelineSource: %v", err)
	}
	if got.GetReference() != "http:https://api.example.com/file.yaml" {
		t.Fatalf("got = %+v", got)
	}

	// Count the sources that reference the credential (the delete guard).
	count, err := client.CountSourcesByCredential(ctx, &dbpb.CountSourcesByCredentialRequest{Credential: "api-endpoint"})
	if err != nil {
		t.Fatalf("CountSourcesByCredential: %v", err)
	}
	if count.GetCount() != 1 {
		t.Fatalf("count = %d, want 1", count.GetCount())
	}

	// Record a sync failure on the source state (last_error).
	state, err := client.SetPipelineSourceState(ctx, &dbpb.SetPipelineSourceStateRequest{
		PipelineId: pipe.GetId(),
		LastError:  "401 unauthorized",
	})
	if err != nil {
		t.Fatalf("SetPipelineSourceState: %v", err)
	}
	if state.GetLastError() != "401 unauthorized" {
		t.Fatalf("state = %+v", state)
	}

	// Read the state back.
	read, err := client.GetPipelineSourceState(ctx, &dbpb.GetPipelineSourceStateRequest{PipelineId: pipe.GetId()})
	if err != nil {
		t.Fatalf("GetPipelineSourceState: %v", err)
	}
	if read.GetLastError() != "401 unauthorized" {
		t.Fatalf("read = %+v", read)
	}

	// A pipeline with no source returns NotFound.
	if _, err := client.GetPipelineSource(ctx, &dbpb.GetPipelineSourceRequest{PipelineId: 99999}); err == nil {
		t.Fatal("missing source should be NotFound")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Fatalf("missing source should be NotFound, got: %v", err)
	}
}
