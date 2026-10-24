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

// startSSHKnownHostServer opens a SQLite database, migrates it, and serves the
// Database gRPC service over an in-process bufconn listener. It returns the
// gRPC client.
func startSSHKnownHostServer(t *testing.T) dbpb.DatabaseClient {
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

// TestSSHKnownHostCRUD exercises the runtime-managed SSH host key CRUD
// (F-21.2.4.1): create, list, duplicate rejection, and delete.
func TestSSHKnownHostCRUD(t *testing.T) {
	client := startSSHKnownHostServer(t)
	ctx := context.Background()

	// Create a host key.
	created, err := client.CreateSSHKnownHost(ctx, &dbpb.CreateSSHKnownHostRequest{
		HostPattern: "git.example.com",
		KeyType:     "ssh-ed25519",
		Key:         "AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl",
		Description: "corporate git",
	})
	if err != nil {
		t.Fatalf("CreateSSHKnownHost: %v", err)
	}
	if created.GetHostPattern() != "git.example.com" || created.GetKeyType() != "ssh-ed25519" {
		t.Fatalf("created = %+v", created)
	}

	// A duplicate host pattern is rejected.
	if _, err := client.CreateSSHKnownHost(ctx, &dbpb.CreateSSHKnownHostRequest{
		HostPattern: "git.example.com",
		KeyType:     "ssh-ed25519",
		Key:         "AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl",
	}); err == nil {
		t.Fatal("duplicate host pattern should be rejected")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.AlreadyExists {
		t.Fatalf("duplicate should be AlreadyExists, got: %v", err)
	}

	// An empty key is rejected.
	if _, err := client.CreateSSHKnownHost(ctx, &dbpb.CreateSSHKnownHostRequest{
		HostPattern: "git.example.com",
		KeyType:     "ssh-ed25519",
		Key:         "",
	}); err == nil {
		t.Fatal("empty key should be rejected")
	}

	// List returns the created key.
	list, err := client.ListSSHKnownHosts(ctx, &dbpb.ListSSHKnownHostsRequest{})
	if err != nil {
		t.Fatalf("ListSSHKnownHosts: %v", err)
	}
	if len(list.GetHosts()) != 1 || list.GetHosts()[0].GetId() != created.GetId() {
		t.Fatalf("list = %+v", list.GetHosts())
	}

	// Delete removes it.
	if _, err := client.DeleteSSHKnownHost(ctx, &dbpb.DeleteSSHKnownHostRequest{Id: created.GetId()}); err != nil {
		t.Fatalf("DeleteSSHKnownHost: %v", err)
	}
	list, err = client.ListSSHKnownHosts(ctx, &dbpb.ListSSHKnownHostsRequest{})
	if err != nil {
		t.Fatalf("ListSSHKnownHosts after delete: %v", err)
	}
	if len(list.GetHosts()) != 0 {
		t.Fatalf("expected no host keys after delete, got %d", len(list.GetHosts()))
	}

	// Deleting a missing key returns NotFound.
	if _, err := client.DeleteSSHKnownHost(ctx, &dbpb.DeleteSSHKnownHostRequest{Id: created.GetId()}); err == nil {
		t.Fatal("deleting a missing key should error")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.NotFound {
		t.Fatalf("missing delete should be NotFound, got: %v", err)
	}
}
