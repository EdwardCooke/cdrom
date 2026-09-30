// This file implements the Artifacts gRPC service: it handles the
// streaming/chunking protocol and delegates the actual I/O to a Store.
package artifacts

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
)

// maxChunkSize bounds a single streamed chunk to keep memory bounded.
const maxChunkSize = 4 << 20 // 4 MiB

// Server implements the Artifacts gRPC service over a Store.
type Server struct {
	artifactspb.UnimplementedArtifactsServer
	store Store
}

// NewServer creates an Artifacts gRPC service over the given store.
func NewServer(store Store) *Server {
	return &Server{store: store}
}

// NewFilesystemServer creates an Artifacts gRPC service backed by a local
// filesystem store rooted at root.
func NewFilesystemServer(root string) (*Server, error) {
	store, err := NewFilesystemStore(root)
	if err != nil {
		return nil, err
	}
	return &Server{store: store}, nil
}

// ---------------------------------------------------------------------------
// Artifacts
// ---------------------------------------------------------------------------

// UploadArtifact consumes a client stream of chunks. The first chunk must
// carry metadata (namespace and name); the rest carry data. The data is
// written to the store as it arrives (streamed, not buffered in memory).
func (s *Server) UploadArtifact(stream grpc.ClientStreamingServer[artifactspb.ArtifactChunk, artifactspb.UploadArtifactResponse]) error {
	firstChunk, err := stream.Recv()
	if err != nil {
		return grpcErr(err)
	}
	metadata := firstChunk.GetMetadata()
	if metadata == nil || metadata.GetNamespace() == "" || metadata.GetName() == "" {
		return status.Error(codes.InvalidArgument, "first chunk must carry metadata with namespace and name")
	}
	reader := &streamReader{stream: stream, first: firstChunk}
	artifact, err := s.store.UploadArtifact(stream.Context(), metadata.GetNamespace(), metadata.GetName(), reader)
	if err != nil {
		return err
	}
	return stream.SendAndClose(&artifactspb.UploadArtifactResponse{Artifact: artifact})
}

// DownloadArtifact streams an artifact back: the first chunk carries
// metadata, subsequent chunks carry data.
func (s *Server) DownloadArtifact(req *artifactspb.DownloadArtifactRequest, stream grpc.ServerStreamingServer[artifactspb.ArtifactChunk]) error {
	namespace, name := req.GetNamespace(), req.GetName()
	if namespace == "" || name == "" {
		return status.Error(codes.InvalidArgument, "namespace and name are required")
	}
	artifact, reader, err := s.store.DownloadArtifact(stream.Context(), namespace, name)
	if err != nil {
		return err
	}
	defer reader.Close()
	if err := stream.Send(&artifactspb.ArtifactChunk{Metadata: artifact}); err != nil {
		return grpcErr(err)
	}
	return streamFromReader(stream, reader)
}

func (s *Server) GetArtifact(ctx context.Context, req *artifactspb.GetArtifactRequest) (*artifactspb.Artifact, error) {
	if req.GetNamespace() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace and name are required")
	}
	return s.store.GetArtifact(ctx, req.GetNamespace(), req.GetName())
}

func (s *Server) ListArtifacts(ctx context.Context, req *artifactspb.ListArtifactsRequest) (*artifactspb.ListArtifactsResponse, error) {
	artifacts, err := s.store.ListArtifacts(ctx, req.GetNamespace())
	if err != nil {
		return nil, err
	}
	return &artifactspb.ListArtifactsResponse{Artifacts: artifacts}, nil
}

func (s *Server) DeleteArtifact(ctx context.Context, req *artifactspb.DeleteArtifactRequest) (*emptypb.Empty, error) {
	if req.GetNamespace() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace and name are required")
	}
	if err := s.store.DeleteArtifact(ctx, req.GetNamespace(), req.GetName()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

// AppendLog consumes a client stream of chunks and appends them to a log.
// The first chunk must carry metadata (namespace and name); the rest carry
// data. The server returns the updated log's metadata once the stream ends.
func (s *Server) AppendLog(stream grpc.ClientStreamingServer[artifactspb.ArtifactChunk, artifactspb.UploadArtifactResponse]) error {
	firstChunk, err := stream.Recv()
	if err != nil {
		return grpcErr(err)
	}
	metadata := firstChunk.GetMetadata()
	if metadata == nil || metadata.GetNamespace() == "" || metadata.GetName() == "" {
		return status.Error(codes.InvalidArgument, "first chunk must carry metadata with namespace and name")
	}
	var artifact *artifactspb.Artifact
	if len(firstChunk.GetData()) > 0 {
		artifact, err = s.store.AppendLog(stream.Context(), metadata.GetNamespace(), metadata.GetName(), firstChunk.GetData())
		if err != nil {
			return err
		}
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
		if len(chunk.GetData()) == 0 {
			continue
		}
		artifact, err = s.store.AppendLog(stream.Context(), metadata.GetNamespace(), metadata.GetName(), chunk.GetData())
		if err != nil {
			return err
		}
	}
	if artifact == nil {
		// No data was sent; report the (possibly newly created) log's state.
		artifact, err = s.store.GetLog(stream.Context(), metadata.GetNamespace(), metadata.GetName())
		if err != nil {
			return err
		}
	}
	return stream.SendAndClose(&artifactspb.UploadArtifactResponse{Artifact: artifact})
}

// DownloadLog streams a log back: the first chunk carries metadata,
// subsequent chunks carry data. Reading a log that is still being written
// returns the output captured so far.
func (s *Server) DownloadLog(req *artifactspb.DownloadLogRequest, stream grpc.ServerStreamingServer[artifactspb.ArtifactChunk]) error {
	namespace, name := req.GetNamespace(), req.GetName()
	if namespace == "" || name == "" {
		return status.Error(codes.InvalidArgument, "namespace and name are required")
	}
	artifact, reader, err := s.store.DownloadLog(stream.Context(), namespace, name)
	if err != nil {
		return err
	}
	defer reader.Close()
	if err := stream.Send(&artifactspb.ArtifactChunk{Metadata: artifact}); err != nil {
		return grpcErr(err)
	}
	return streamFromReader(stream, reader)
}

func (s *Server) GetLog(ctx context.Context, req *artifactspb.GetLogRequest) (*artifactspb.Artifact, error) {
	if req.GetNamespace() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace and name are required")
	}
	return s.store.GetLog(ctx, req.GetNamespace(), req.GetName())
}

func (s *Server) ListLogs(ctx context.Context, req *artifactspb.ListLogsRequest) (*artifactspb.ListLogsResponse, error) {
	logs, err := s.store.ListLogs(ctx, req.GetNamespace())
	if err != nil {
		return nil, err
	}
	return &artifactspb.ListLogsResponse{Logs: logs}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// streamReader adapts a client-streaming gRPC stream to an io.Reader so the
// store can consume the upload as it arrives (streamed, not buffered). The
// first chunk (already received) is served before subsequent Recv calls.
type streamReader struct {
	stream grpc.ClientStreamingServer[artifactspb.ArtifactChunk, artifactspb.UploadArtifactResponse]
	first  *artifactspb.ArtifactChunk
	buf    []byte
}

func (r *streamReader) Read(p []byte) (int, error) {
	if len(r.buf) == 0 {
		chunk, err := r.next()
		if err != nil {
			return 0, err
		}
		if len(chunk) == 0 {
			return 0, io.EOF
		}
		r.buf = chunk
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// next returns the next chunk's data, or (nil, io.EOF) at the end of the
// stream.
func (r *streamReader) next() ([]byte, error) {
	if r.first != nil {
		chunk := r.first
		r.first = nil
		return chunk.GetData(), nil
	}
	chunk, err := r.stream.Recv()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, grpcErr(err)
	}
	return chunk.GetData(), nil
}

// streamFromReader reads from reader in bounded chunks and sends each as a
// data chunk on stream.
func streamFromReader(stream grpc.ServerStreamingServer[artifactspb.ArtifactChunk], reader io.Reader) error {
	buffer := make([]byte, maxChunkSize)
	for {
		read, err := reader.Read(buffer)
		if read > 0 {
			if sendErr := stream.Send(&artifactspb.ArtifactChunk{Data: append([]byte(nil), buffer[:read]...)}); sendErr != nil {
				return grpcErr(sendErr)
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
