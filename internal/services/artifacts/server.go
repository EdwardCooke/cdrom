// Package artifacts implements the cdrom.artifacts.v1.Artifacts gRPC
// service: storage and retrieval of job artifacts on the local filesystem.
//
// Artifacts are stored as files under a root directory, one subdirectory
// per job: <root>/<job_id>/<name>. Metadata (size, creation time, content
// type) is derived from the file itself, so the service is stateless and
// portable across Windows and Linux.
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
)

// maxChunkSize bounds a single streamed chunk to keep memory bounded.
const maxChunkSize = 4 << 20 // 4 MiB

// safeSegment matches a single path segment: no separators, no "..".
var safeSegment = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// Server implements the Artifacts gRPC service over a filesystem root.
type Server struct {
	artifactspb.UnimplementedArtifactsServer
	root string
}

// NewServer creates an Artifacts gRPC service storing artifacts under root.
func NewServer(root string) (*Server, error) {
	if root == "" {
		return nil, fmt.Errorf("artifacts: root must not be empty")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("artifacts: create root: %w", err)
	}
	return &Server{root: root}, nil
}

// UploadArtifact consumes a client stream of chunks. The first chunk must
// carry metadata (job_id and name); the rest carry data.
func (s *Server) UploadArtifact(stream grpc.ClientStreamingServer[artifactspb.ArtifactChunk, artifactspb.UploadArtifactResponse]) error {
	firstChunk, err := stream.Recv()
	if err != nil {
		return grpcErr(err)
	}
	metadata := firstChunk.GetMetadata()
	if metadata == nil || metadata.GetJobId() == "" || metadata.GetName() == "" {
		return status.Error(codes.InvalidArgument, "first chunk must carry metadata with job_id and name")
	}
	path, err := s.artifactPath(metadata.GetJobId(), metadata.GetName())
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return status.Errorf(codes.Internal, "artifacts: mkdir: %v", err)
	}
	file, err := os.Create(path)
	if err != nil {
		return status.Errorf(codes.Internal, "artifacts: create: %v", err)
	}
	defer file.Close()

	if _, err := file.Write(firstChunk.GetData()); err != nil {
		return status.Errorf(codes.Internal, "artifacts: write: %v", err)
	}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return grpcErr(err)
		}
		if size := len(chunk.GetData()); size > maxChunkSize {
			return status.Errorf(codes.InvalidArgument, "chunk of %d bytes exceeds limit of %d", size, maxChunkSize)
		}
		if _, err := file.Write(chunk.GetData()); err != nil {
			return status.Errorf(codes.Internal, "artifacts: write: %v", err)
		}
	}
	if err := file.Close(); err != nil {
		return status.Errorf(codes.Internal, "artifacts: close: %v", err)
	}
	artifact, err := s.stat(metadata.GetJobId(), metadata.GetName())
	if err != nil {
		return err
	}
	return stream.SendAndClose(&artifactspb.UploadArtifactResponse{Artifact: artifact})
}

// DownloadArtifact streams an artifact back: the first chunk carries
// metadata, subsequent chunks carry data.
func (s *Server) DownloadArtifact(req *artifactspb.DownloadArtifactRequest, stream artifactspb.Artifacts_DownloadArtifactServer) error {
	jobID, name := req.GetJobId(), req.GetName()
	if jobID == "" || name == "" {
		return status.Error(codes.InvalidArgument, "job_id and name are required")
	}
	path, err := s.artifactPath(jobID, name)
	if err != nil {
		return err
	}
	artifact, err := s.stat(jobID, name)
	if err != nil {
		return err
	}
	if err := stream.Send(&artifactspb.ArtifactChunk{Metadata: artifact}); err != nil {
		return grpcErr(err)
	}
	file, err := os.Open(path)
	if err != nil {
		return status.Errorf(codes.Internal, "artifacts: open: %v", err)
	}
	defer file.Close()

	buffer := make([]byte, maxChunkSize)
	for {
		read, err := file.Read(buffer)
		if read > 0 {
			if err := stream.Send(&artifactspb.ArtifactChunk{Data: append([]byte(nil), buffer[:read]...)}); err != nil {
				return grpcErr(err)
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "artifacts: read: %v", err)
		}
	}
}

func (s *Server) GetArtifact(_ context.Context, req *artifactspb.GetArtifactRequest) (*artifactspb.Artifact, error) {
	if req.GetJobId() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id and name are required")
	}
	return s.stat(req.GetJobId(), req.GetName())
}

func (s *Server) ListArtifacts(_ context.Context, req *artifactspb.ListArtifactsRequest) (*artifactspb.ListArtifactsResponse, error) {
	response := &artifactspb.ListArtifactsResponse{}
	baseDir := s.root
	if req.GetJobId() != "" {
		jobDir, err := s.artifactPath(req.GetJobId(), "")
		if err != nil {
			return nil, err
		}
		baseDir = jobDir
	}
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return response, nil
		}
		return nil, status.Errorf(codes.Internal, "artifacts: list: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			if req.GetJobId() != "" {
				continue // a job directory contains files, not subdirectories
			}
			jobID := entry.Name()
			files, err := os.ReadDir(filepath.Join(baseDir, jobID))
			if err != nil {
				return nil, status.Errorf(codes.Internal, "artifacts: list: %v", err)
			}
			for _, file := range files {
				if file.IsDir() {
					continue
				}
				if artifact, err := s.stat(jobID, file.Name()); err == nil {
					response.Artifacts = append(response.Artifacts, artifact)
				}
			}
			continue
		}
		if req.GetJobId() == "" {
			continue
		}
		if artifact, err := s.stat(req.GetJobId(), entry.Name()); err == nil {
			response.Artifacts = append(response.Artifacts, artifact)
		}
	}
	return response, nil
}

func (s *Server) DeleteArtifact(_ context.Context, req *artifactspb.DeleteArtifactRequest) (*emptypb.Empty, error) {
	if req.GetJobId() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id and name are required")
	}
	path, err := s.artifactPath(req.GetJobId(), req.GetName())
	if err != nil {
		return nil, err
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.NotFound, "artifact not found")
		}
		return nil, status.Errorf(codes.Internal, "artifacts: delete: %v", err)
	}
	return &emptypb.Empty{}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// artifactPath resolves and validates the on-disk path for an artifact.
// name may be empty to address the job directory itself.
func (s *Server) artifactPath(jobID, name string) (string, error) {
	if !safeSegment.MatchString(jobID) {
		return "", status.Error(codes.InvalidArgument, "invalid job_id")
	}
	if name != "" && !safeSegment.MatchString(name) {
		return "", status.Error(codes.InvalidArgument, "invalid artifact name")
	}
	path := filepath.Join(s.root, jobID)
	if name != "" {
		path = filepath.Join(path, name)
	}
	// Defense in depth: the resolved path must stay inside the root.
	root := filepath.Clean(s.root)
	resolved := filepath.Clean(path)
	if resolved != root && !strings.HasPrefix(resolved, root+string(filepath.Separator)) {
		return "", status.Error(codes.InvalidArgument, "path escapes artifact root")
	}
	return resolved, nil
}

// stat derives artifact metadata from the file on disk.
func (s *Server) stat(jobID, name string) (*artifactspb.Artifact, error) {
	path, err := s.artifactPath(jobID, name)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.NotFound, "artifact not found")
		}
		return nil, status.Errorf(codes.Internal, "artifacts: stat: %v", err)
	}
	contentType := mime.TypeByExtension(filepath.Ext(name))
	return &artifactspb.Artifact{
		JobId:       jobID,
		Name:        name,
		Size:        info.Size(),
		ContentType: contentType,
		CreatedAt:   timestamppb.New(info.ModTime()),
	}, nil
}

func grpcErr(err error) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		return status.Error(codes.Internal, "unexpected end of stream")
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Errorf(codes.Internal, "%v", err)
}
