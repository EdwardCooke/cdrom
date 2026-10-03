package artifacts

import (
	"context"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
)

// FilesystemStore is the default Store implementation: it stores artifacts and
// logs as files under a root directory.
//
// Layout:
//
//	<root>/<namespace>/<name>          artifacts (immutable)
//	<root>/<namespace>/logs/<name>     logs (append-only, grow over time)
//
// Logs live in a per-namespace "logs" subdirectory so listing artifacts and
// listing logs never see each other's files. Metadata (size, modified time,
// content type) is derived from the file itself, so the store is stateless
// and portable across Windows and Linux.
type FilesystemStore struct {
	root string
}

// NewFilesystemStore creates a FilesystemStore rooted at root, creating the
// directory if needed.
func NewFilesystemStore(root string) (*FilesystemStore, error) {
	if root == "" {
		return nil, fmt.Errorf("artifacts: root must not be empty")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("artifacts: create root: %w", err)
	}
	return &FilesystemStore{root: root}, nil
}

// safeSegment matches a single path segment: no separators, no "..".
var safeSegment = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// ---------------------------------------------------------------------------
// Artifacts
// ---------------------------------------------------------------------------

func (s *FilesystemStore) UploadArtifact(_ context.Context, namespace, name string, r io.Reader) (*artifactspb.Artifact, error) {
	path, err := s.artifactPath(namespace, name)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "artifacts: mkdir: %v", err)
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "artifacts: create: %v", err)
	}
	if _, err := io.Copy(file, r); err != nil {
		file.Close()
		return nil, status.Errorf(codes.Internal, "artifacts: write: %v", err)
	}
	if err := file.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "artifacts: close: %v", err)
	}
	return s.statArtifact(namespace, name)
}

func (s *FilesystemStore) DownloadArtifact(_ context.Context, namespace, name string) (*artifactspb.Artifact, io.ReadCloser, error) {
	path, err := s.artifactPath(namespace, name)
	if err != nil {
		return nil, nil, err
	}
	artifact, err := s.statArtifact(namespace, name)
	if err != nil {
		return nil, nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "artifacts: open: %v", err)
	}
	return artifact, file, nil
}

func (s *FilesystemStore) GetArtifact(_ context.Context, namespace, name string) (*artifactspb.Artifact, error) {
	if namespace == "" || name == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace and name are required")
	}
	return s.statArtifact(namespace, name)
}

func (s *FilesystemStore) ListArtifacts(_ context.Context, namespace string) ([]*artifactspb.Artifact, error) {
	baseDir := s.root
	if namespace != "" {
		dir, err := s.artifactPath(namespace, "")
		if err != nil {
			return nil, err
		}
		baseDir = dir
	}
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, status.Errorf(codes.Internal, "artifacts: list: %v", err)
	}
	var out []*artifactspb.Artifact
	for _, entry := range entries {
		if entry.IsDir() {
			if namespace != "" {
				continue // a namespace directory contains files (and the logs subdir), not artifact subdirs
			}
			nsDir := entry.Name()
			files, err := os.ReadDir(filepath.Join(baseDir, nsDir))
			if err != nil {
				return nil, status.Errorf(codes.Internal, "artifacts: list: %v", err)
			}
			for _, file := range files {
				if file.IsDir() {
					continue
				}
				if artifact, err := s.statArtifact(nsDir, file.Name()); err == nil {
					out = append(out, artifact)
				}
			}
			continue
		}
		if namespace == "" {
			continue
		}
		if artifact, err := s.statArtifact(namespace, entry.Name()); err == nil {
			out = append(out, artifact)
		}
	}
	return out, nil
}

func (s *FilesystemStore) DeleteArtifact(_ context.Context, namespace, name string) error {
	path, err := s.artifactPath(namespace, name)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return status.Error(codes.NotFound, "artifact not found")
		}
		return status.Errorf(codes.Internal, "artifacts: delete: %v", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

func (s *FilesystemStore) AppendLog(_ context.Context, namespace, name string, data []byte) (*artifactspb.Artifact, error) {
	path, err := s.logPath(namespace, name)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "artifacts: mkdir: %v", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "artifacts: open log: %v", err)
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return nil, status.Errorf(codes.Internal, "artifacts: append log: %v", err)
	}
	if err := file.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "artifacts: close log: %v", err)
	}
	return s.statLog(namespace, name)
}

func (s *FilesystemStore) DownloadLog(_ context.Context, namespace, name string, offset int64) (*artifactspb.Artifact, io.ReadCloser, error) {
	path, err := s.logPath(namespace, name)
	if err != nil {
		return nil, nil, err
	}
	artifact, err := s.statLog(namespace, name)
	if err != nil {
		return nil, nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "artifacts: open log: %v", err)
	}
	if offset > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			file.Close()
			return nil, nil, status.Errorf(codes.Internal, "artifacts: seek log: %v", err)
		}
	}
	return artifact, file, nil
}

func (s *FilesystemStore) GetLog(_ context.Context, namespace, name string) (*artifactspb.Artifact, error) {
	if namespace == "" || name == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace and name are required")
	}
	return s.statLog(namespace, name)
}

func (s *FilesystemStore) ListLogs(_ context.Context, namespace string) ([]*artifactspb.Artifact, error) {
	baseDir := s.root
	if namespace != "" {
		dir, err := s.logPath(namespace, "")
		if err != nil {
			return nil, err
		}
		baseDir = dir
	}
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, status.Errorf(codes.Internal, "artifacts: list logs: %v", err)
	}
	var out []*artifactspb.Artifact
	for _, entry := range entries {
		if entry.IsDir() {
			if namespace != "" {
				continue
			}
			nsDir := entry.Name()
			logsDir := filepath.Join(baseDir, nsDir, "logs")
			files, err := os.ReadDir(logsDir)
			if err != nil {
				if os.IsNotExist(err) {
					continue // this namespace has no logs
				}
				return nil, status.Errorf(codes.Internal, "artifacts: list logs: %v", err)
			}
			for _, file := range files {
				if file.IsDir() {
					continue
				}
				if artifact, err := s.statLog(nsDir, file.Name()); err == nil {
					out = append(out, artifact)
				}
			}
			continue
		}
		if namespace == "" {
			continue
		}
		if artifact, err := s.statLog(namespace, entry.Name()); err == nil {
			out = append(out, artifact)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// artifactPath resolves and validates the on-disk path for an artifact. name
// may be empty to address the namespace directory itself.
func (s *FilesystemStore) artifactPath(namespace, name string) (string, error) {
	if !safeSegment.MatchString(namespace) {
		return "", status.Error(codes.InvalidArgument, "invalid namespace")
	}
	if name != "" && !safeSegment.MatchString(name) {
		return "", status.Error(codes.InvalidArgument, "invalid artifact name")
	}
	path := filepath.Join(s.root, namespace)
	if name != "" {
		path = filepath.Join(path, name)
	}
	return s.insideRoot(path)
}

// logPath resolves and validates the on-disk path for a log. name may be
// empty to address the namespace's logs directory itself.
func (s *FilesystemStore) logPath(namespace, name string) (string, error) {
	if !safeSegment.MatchString(namespace) {
		return "", status.Error(codes.InvalidArgument, "invalid namespace")
	}
	if name != "" && !safeSegment.MatchString(name) {
		return "", status.Error(codes.InvalidArgument, "invalid log name")
	}
	path := filepath.Join(s.root, namespace, "logs")
	if name != "" {
		path = filepath.Join(path, name)
	}
	return s.insideRoot(path)
}

// insideRoot is defense in depth: the resolved path must stay inside the root.
func (s *FilesystemStore) insideRoot(path string) (string, error) {
	root := filepath.Clean(s.root)
	resolved := filepath.Clean(path)
	if resolved != root && !strings.HasPrefix(resolved, root+string(filepath.Separator)) {
		return "", status.Error(codes.InvalidArgument, "path escapes artifact root")
	}
	return resolved, nil
}

// statArtifact derives artifact metadata from the file on disk.
func (s *FilesystemStore) statArtifact(namespace, name string) (*artifactspb.Artifact, error) {
	path, err := s.artifactPath(namespace, name)
	if err != nil {
		return nil, err
	}
	return statFile(namespace, name, path)
}

// statLog derives log metadata from the file on disk.
func (s *FilesystemStore) statLog(namespace, name string) (*artifactspb.Artifact, error) {
	path, err := s.logPath(namespace, name)
	if err != nil {
		return nil, err
	}
	return statFile(namespace, name, path)
}

// statFile derives metadata for the file at path.
func statFile(namespace, name, path string) (*artifactspb.Artifact, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.NotFound, "not found")
		}
		return nil, status.Errorf(codes.Internal, "artifacts: stat: %v", err)
	}
	contentType := mime.TypeByExtension(filepath.Ext(name))
	return &artifactspb.Artifact{
		Namespace:   namespace,
		Name:        name,
		Size:        info.Size(),
		ContentType: contentType,
		CreatedAt:   timestamppb.New(info.ModTime()),
	}, nil
}
