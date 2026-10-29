package artifacts

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// Store is the storage backend for the artifacts service: a dumb blob store
// keyed by opaque ids. Bytes in, bytes out — no names, no namespaces, no
// metadata derivation. All file ATTRIBUTES live in the Database service's
// storage_object records (F-21.6 storage redesign); a Store implementation
// must treat an id as an opaque string and derive nothing from it.
//
// Semantics:
//
//   - Put: write the WHOLE content of a blob to a fresh id. Double-writes to
//     an id that already holds a NON-EMPTY blob fail (AlreadyExists);
//     rewriting an EMPTY blob (a put that ended with zero bytes) is allowed
//     (adopt-the-slot recovery).
//   - Get: read a blob's bytes, optionally from a byte offset (range reads
//     power cross-pod log tailing, F-23). Offset >= size yields an empty
//     reader.
//   - Append: grow a blob in place (LOGS). Absent blobs are created.
//   - Copy: replicate a blob to another id SERVER-SIDE (promotion).
//   - Delete: remove a blob outright (missing ids are a no-op).
//
// Implementations must be safe for concurrent use.
type Store interface {
	// Put stores data as the blob at id, replacing any EMPTY blob sitting
	// there. It returns the number of bytes stored.
	Put(ctx context.Context, id string, r io.Reader) (int64, error)
	// Get opens the blob at id for reading from offset (0 = from the
	// start). It returns the blob's total size and a reader the caller must
	// close.
	Get(ctx context.Context, id string, offset int64) (int64, io.ReadCloser, error)
	// Stat returns the blob's size without transferring its bytes.
	Stat(ctx context.Context, id string) (int64, error)
	// Append appends data to the blob at id (creating it when absent). It
	// returns the blob's total size after the append.
	Append(ctx context.Context, id string, data []byte) (int64, error)
	// Copy replicates the blob at src to dst (dst must not hold a non-empty
	// blob). It returns the copied size.
	Copy(ctx context.Context, src, dst string) (int64, error)
	// Delete removes the blob at id. A missing id is a no-op.
	Delete(ctx context.Context, id string) error
}

// StoreKind names a built-in Store implementation.
type StoreKind string

// Built-in store implementations.
const (
	// StoreKindFilesystem is the local-filesystem blob store (the default).
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
		return NewBlobFSStore(root)
	case StoreKindS3:
		return nil, fmt.Errorf("artifacts: store kind %q is not implemented yet; use %q", kind, StoreKindFilesystem)
	case StoreKindAzureBlob:
		return nil, fmt.Errorf("artifacts: store kind %q is not implemented yet; use %q", kind, StoreKindFilesystem)
	default:
		return nil, fmt.Errorf("artifacts: unknown store kind %q (want %q, %q, or %q)", kind, StoreKindFilesystem, StoreKindS3, StoreKindAzureBlob)
	}
}

// ErrInvalidBlobID guards the degenerate case of an empty or illegal id.
var ErrInvalidBlobID = fmt.Errorf("artifacts: invalid blob id")

// ValidateBlobID checks that id is a legal blob identifier: non-empty, at
// most 64 characters, drawn from [0-9a-zA-Z-]. Typically a UUID (36 chars,
// lowercase hex + dashes) issued by the Database service's record allocator.
func ValidateBlobID(id string) error {
	if id == "" {
		return ErrInvalidBlobID
	}
	if len(id) > 64 {
		return fmt.Errorf("%w: %d characters exceed the 64-character limit", ErrInvalidBlobID, len(id))
	}
	for _, r := range id {
		ok := (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '-'
		if !ok {
			return fmt.Errorf("%w: %q contains an illegal character", ErrInvalidBlobID, id)
		}
	}
	return nil
}

// blobShardPrefix extracts the two-letter shard prefix of a blob id (the
// first two characters lowercased). Sharding spreads millions of blobs over
// thousands of directories so enumeration and fsck-scale maintenance stay
// cheap on any filesystem.
func blobShardPrefix(id string) string {
	prefix := id
	if len(prefix) > 2 {
		prefix = prefix[:2]
	}
	return strings.ToLower(prefix)
}
