package artifacts

import (
	"context"
	"fmt"
	"io"
	"strings"

	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
)

// Store is the storage backend for the artifacts service: a general-purpose,
// namespaced file store. It stores two kinds of data:
//
//   - artifacts: immutable files (one per namespace/name); and
//   - logs: append-only files that grow over time (for a job: one per step
//     plus a combined file), so a log can be read while it is still being
//     written (tail) and replayed in full afterwards.
//
// Every file lives in a *namespace*: an opaque scope string that groups
// related files. The store is agnostic about what a namespace means — it is
// up to the caller. A job's artifacts and logs use the job's id as the
// namespace, while a deployed release's artifacts might use a release
// identifier.
//
// The gRPC Server (server.go) handles the streaming/chunking protocol and
// delegates the actual I/O to a Store. Implementations must be safe for
// concurrent use.
type Store interface {
	// UploadArtifact stores data as an artifact named name in namespace,
	// replacing any existing artifact with the same name. It returns the
	// stored artifact's metadata.
	UploadArtifact(ctx context.Context, namespace, name string, r io.Reader) (*artifactspb.Artifact, error)
	// DownloadArtifact opens the artifact named name in namespace for
	// reading. The caller must close the returned reader.
	DownloadArtifact(ctx context.Context, namespace, name string) (*artifactspb.Artifact, io.ReadCloser, error)
	// GetArtifact returns the metadata of the artifact named name in
	// namespace.
	GetArtifact(ctx context.Context, namespace, name string) (*artifactspb.Artifact, error)
	// ListArtifacts lists the artifacts in namespace (all namespaces when
	// namespace is "").
	ListArtifacts(ctx context.Context, namespace string) ([]*artifactspb.Artifact, error)
	// DeleteArtifact deletes the artifact named name in namespace.
	DeleteArtifact(ctx context.Context, namespace, name string) error

	// AppendLog appends data to the log named name in namespace, creating the
	// log if it does not exist. It returns the updated log's metadata.
	AppendLog(ctx context.Context, namespace, name string, data []byte) (*artifactspb.Artifact, error)
	// DownloadLog opens the log named name in namespace for reading. The
	// caller must close the returned reader. Reading a log that is still
	// being written returns the output captured so far. When offset is
	// greater than zero the reader is positioned at that byte offset (a
	// range read of the log's tail); when it is zero the whole log is
	// returned from the start. An offset beyond the log's current size
	// yields an empty reader.
	DownloadLog(ctx context.Context, namespace, name string, offset int64) (*artifactspb.Artifact, io.ReadCloser, error)
	// GetLog returns the metadata of the log named name in namespace.
	GetLog(ctx context.Context, namespace, name string) (*artifactspb.Artifact, error)
	// ListLogs lists the logs in namespace (all namespaces when namespace is
	// "").
	ListLogs(ctx context.Context, namespace string) ([]*artifactspb.Artifact, error)
}

// StoreKind names a built-in Store implementation.
type StoreKind string

// Built-in store implementations.
const (
	// StoreKindFilesystem is the local-filesystem store (the default).
	StoreKindFilesystem StoreKind = "filesystem"
	// StoreKindS3 is an S3-backed store (not yet implemented).
	StoreKindS3 StoreKind = "s3"
	// StoreKindAzureBlob is an Azure Blob-backed store (not yet implemented).
	StoreKindAzureBlob StoreKind = "azureblob"
)

// NewStore builds a Store of the named kind rooted at root. The kind is
// case-insensitive. Only the filesystem store is implemented today; other
// kinds return an error so a misconfiguration fails fast at startup.
func NewStore(kind StoreKind, root string) (Store, error) {
	switch StoreKind(strings.ToLower(string(kind))) {
	case StoreKindFilesystem:
		return NewFilesystemStore(root)
	case StoreKindS3:
		return nil, fmt.Errorf("artifacts: store kind %q is not implemented yet; use %q", kind, StoreKindFilesystem)
	case StoreKindAzureBlob:
		return nil, fmt.Errorf("artifacts: store kind %q is not implemented yet; use %q", kind, StoreKindFilesystem)
	default:
		return nil, fmt.Errorf("artifacts: unknown store kind %q (want %q, %q, or %q)", kind, StoreKindFilesystem, StoreKindS3, StoreKindAzureBlob)
	}
}
