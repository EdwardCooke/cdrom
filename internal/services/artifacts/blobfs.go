package artifacts

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BlobFSStore is the default Store implementation: it stores blobs as files
// under a root directory, sharded flat by id prefix:
//
//	<root>/blobs/<two-letter prefix of the id>/<id>
//
// The id is validated (ValidateBlobID) before ANY path manipulation, so no
// escaping is needed and traversal is impossible. Temporary writes happen in
// the SHARD DIRECTORY beside the final file and are renamed into place, so
// a blob is either wholly present or wholly absent to concurrent readers.
type BlobFSStore struct {
	blobsDir string
}

// NewBlobFSStore creates a BlobFSStore rooted at root, creating the blobs
// directory if needed.
func NewBlobFSStore(root string) (*BlobFSStore, error) {
	if root == "" {
		return nil, fmt.Errorf("artifacts: root must not be empty")
	}
	blobsDir := filepath.Join(root, "blobs")
	if err := os.MkdirAll(blobsDir, 0o755); err != nil {
		return nil, fmt.Errorf("artifacts: create blobs dir: %w", err)
	}
	return &BlobFSStore{blobsDir: blobsDir}, nil
}

// blobPath resolves and validates the on-disk path for the blob at id.
func (s *BlobFSStore) blobPath(id string) (string, error) {
	if err := ValidateBlobID(id); err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	shard := blobShardPrefix(id)
	path := filepath.Join(s.blobsDir, shard, id)
	// Defense in depth: the join must stay inside the blobs dir.
	root := filepath.Clean(s.blobsDir)
	resolved := filepath.Clean(path)
	if resolved != root && !strings.HasPrefix(resolved, root+string(filepath.Separator)) {
		return "", status.Error(codes.InvalidArgument, "blob id escapes the store root")
	}
	return resolved, nil
}

// tempName renders the deterministic temp-file name for a final blob path.
// A FIXED name (rather than a random one) means a crashed partial write is
// OVERWRITTEN by the next put to the same id, bounding leakage to one temp
// per id regardless of how often a put crashes mid-way. Concurrent puts to
// one id are a protocol violation (allocation grants a single writer), so
// the shared temp cannot corrupt a legitimate flow.
func tempName(final string) string { return final + ".~part" }

// existingNonEmpty reports whether the blob at id exists and HOLDS DATA
// (an empty file — a put that ended with zero bytes — does not count).
func existingNonEmpty(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.Mode().IsDir() && info.Size() > 0
}

// writeFileAtomic streams r into a temp file in the blob's shard directory
// and renames it to path. It returns the number of bytes written.
func writeFileAtomic(path string, r io.Reader) (int64, error) {
	tmp := tempName(path)
	file, err := os.Create(tmp)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "artifacts: create temp: %v", err)
	}
	n, err := io.Copy(file, r)
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return 0, status.Errorf(codes.Internal, "artifacts: write blob: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return 0, status.Errorf(codes.Internal, "artifacts: rename blob: %v", err)
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// Store methods
// ---------------------------------------------------------------------------

// Put stores data as the blob at id (see Store.Put).
func (s *BlobFSStore) Put(_ context.Context, id string, r io.Reader) (int64, error) {
	path, err := s.blobPath(id)
	if err != nil {
		return 0, err
	}
	if existingNonEmpty(path) {
		return 0, status.Errorf(codes.AlreadyExists, "blob %s already holds data", id)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, status.Errorf(codes.Internal, "artifacts: mkdir shard: %v", err)
	}
	return writeFileAtomic(path, r)
}

// Get opens the blob at id for reading from offset (see Store.Get).
func (s *BlobFSStore) Get(_ context.Context, id string, offset int64) (int64, io.ReadCloser, error) {
	path, err := s.blobPath(id)
	if err != nil {
		return 0, nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, status.Errorf(codes.NotFound, "blob %s not found", id)
		}
		return 0, nil, status.Errorf(codes.Internal, "artifacts: stat blob: %v", err)
	}
	size := info.Size()
	file, err := os.Open(path)
	if err != nil {
		return 0, nil, status.Errorf(codes.Internal, "artifacts: open blob: %v", err)
	}
	if offset > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			file.Close()
			return 0, nil, status.Errorf(codes.Internal, "artifacts: seek blob: %v", err)
		}
	}
	return size, file, nil
}

// Stat returns the blob's size (see Store.Stat).
func (s *BlobFSStore) Stat(_ context.Context, id string) (int64, error) {
	path, err := s.blobPath(id)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, status.Errorf(codes.NotFound, "blob %s not found", id)
		}
		return 0, status.Errorf(codes.Internal, "artifacts: stat blob: %v", err)
	}
	return info.Size(), nil
}

// Append grows the blob at id (see Store.Append).
func (s *BlobFSStore) Append(_ context.Context, id string, data []byte) (int64, error) {
	path, err := s.blobPath(id)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, status.Errorf(codes.Internal, "artifacts: mkdir shard: %v", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "artifacts: open blob for append: %v", err)
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return 0, status.Errorf(codes.Internal, "artifacts: append blob: %v", err)
	}
	if err := file.Close(); err != nil {
		return 0, status.Errorf(codes.Internal, "artifacts: close blob: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "artifacts: stat blob: %v", err)
	}
	return info.Size(), nil
}

// Copy replicates the blob at src to dst (see Store.Copy).
func (s *BlobFSStore) Copy(_ context.Context, src, dst string) (int64, error) {
	srcPath, err := s.blobPath(src)
	if err != nil {
		return 0, err
	}
	dstPath, err := s.blobPath(dst)
	if err != nil {
		return 0, err
	}
	in, err := os.Open(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, status.Errorf(codes.NotFound, "blob %s not found", src)
		}
		return 0, status.Errorf(codes.Internal, "artifacts: open source blob: %v", err)
	}
	defer in.Close()
	if existingNonEmpty(dstPath) {
		return 0, status.Errorf(codes.AlreadyExists, "blob %s already holds data", dst)
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return 0, status.Errorf(codes.Internal, "artifacts: mkdir shard: %v", err)
	}
	return writeFileAtomic(dstPath, in)
}

// Delete removes the blob at id (see Store.Delete).
func (s *BlobFSStore) Delete(_ context.Context, id string) error {
	path, err := s.blobPath(id)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return status.Errorf(codes.Internal, "artifacts: delete blob: %v", err)
	}
	return nil
}
