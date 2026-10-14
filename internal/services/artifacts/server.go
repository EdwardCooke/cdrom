// This file implements the Artifacts gRPC service: it handles the
// streaming/chunking protocol and delegates the actual I/O to a Store.
package artifacts

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	"cdrom/internal/metrics"
)

// maxChunkSize bounds a single streamed chunk to keep memory bounded.
const maxChunkSize = 4 << 20 // 4 MiB

// Server implements the Artifacts gRPC service over a Store.
type Server struct {
	artifactspb.UnimplementedArtifactsServer
	store Store
	// metrics records the Artifacts service's Prometheus metrics (F-20): the
	// operation counters (by op and gRPC status code), the bytes moved, and
	// the operation durations. It is nil when metrics are disabled, in which
	// case all recording is a no-op.
	metrics *metrics.ArtifactsMetrics
}

// NewServer creates an Artifacts gRPC service over the given store.
func NewServer(store Store) *Server {
	return &Server{store: store}
}

// SetMetrics attaches the Artifacts service's Prometheus metric set (F-20).
// When left nil (metrics disabled) all recording is a no-op.
func (s *Server) SetMetrics(m *metrics.ArtifactsMetrics) {
	s.metrics = m
}

// recordOp records an artifacts operation's outcome (F-20): the operation
// counter (by op and gRPC status code) and, when bytes > 0, the bytes moved.
// It is a no-op when metrics are disabled.
func (s *Server) recordOp(op string, err error, bytes int64) {
	if s.metrics == nil {
		return
	}
	code := codes.OK
	if err != nil {
		if st, _ := status.FromError(err); st != nil {
			code = st.Code()
		}
	}
	s.metrics.RecordOperation(op, code.String())
	if bytes > 0 {
		s.metrics.AddBytes(op, bytes)
	}
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
func (s *Server) UploadArtifact(stream grpc.ClientStreamingServer[artifactspb.ArtifactChunk, artifactspb.UploadArtifactResponse]) (err error) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.ObserveDuration("upload", time.Since(start))
		}
	}()
	firstChunk, recvErr := stream.Recv()
	if recvErr != nil {
		recvErr = grpcErr(recvErr)
		s.recordOp("upload", recvErr, 0)
		return recvErr
	}
	metadata := firstChunk.GetMetadata()
	if metadata == nil || metadata.GetNamespace() == "" || metadata.GetName() == "" {
		invalidErr := status.Error(codes.InvalidArgument, "first chunk must carry metadata with namespace and name")
		s.recordOp("upload", invalidErr, 0)
		return invalidErr
	}
	reader := &streamReader{stream: stream, first: firstChunk}
	artifact, uploadErr := s.store.UploadArtifact(stream.Context(), metadata.GetNamespace(), metadata.GetName(), reader)
	if uploadErr != nil {
		s.recordOp("upload", uploadErr, 0)
		return uploadErr
	}
	s.recordOp("upload", nil, artifact.GetSize())
	return stream.SendAndClose(&artifactspb.UploadArtifactResponse{Artifact: artifact})
}

// DownloadArtifact streams an artifact back: the first chunk carries
// metadata, subsequent chunks carry data.
func (s *Server) DownloadArtifact(req *artifactspb.DownloadArtifactRequest, stream grpc.ServerStreamingServer[artifactspb.ArtifactChunk]) (err error) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.ObserveDuration("download", time.Since(start))
		}
	}()
	namespace, name := req.GetNamespace(), req.GetName()
	if namespace == "" || name == "" {
		invalidErr := status.Error(codes.InvalidArgument, "namespace and name are required")
		s.recordOp("download", invalidErr, 0)
		return invalidErr
	}
	artifact, reader, dlErr := s.store.DownloadArtifact(stream.Context(), namespace, name)
	if dlErr != nil {
		s.recordOp("download", dlErr, 0)
		return dlErr
	}
	defer reader.Close()
	if err := stream.Send(&artifactspb.ArtifactChunk{Metadata: artifact}); err != nil {
		s.recordOp("download", grpcErr(err), 0)
		return grpcErr(err)
	}
	s.recordOp("download", nil, artifact.GetSize())
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
	s.recordOp("list", err, 0)
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
func (s *Server) AppendLog(stream grpc.ClientStreamingServer[artifactspb.ArtifactChunk, artifactspb.UploadArtifactResponse]) (err error) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.ObserveDuration("append_log", time.Since(start))
		}
	}()
	firstChunk, recvErr := stream.Recv()
	if recvErr != nil {
		recvErr = grpcErr(recvErr)
		s.recordOp("append_log", recvErr, 0)
		return recvErr
	}
	metadata := firstChunk.GetMetadata()
	if metadata == nil || metadata.GetNamespace() == "" || metadata.GetName() == "" {
		invalidErr := status.Error(codes.InvalidArgument, "first chunk must carry metadata with namespace and name")
		s.recordOp("append_log", invalidErr, 0)
		return invalidErr
	}
	var artifact *artifactspb.Artifact
	var appended int64
	if len(firstChunk.GetData()) > 0 {
		appended += int64(len(firstChunk.GetData()))
		var appendErr error
		artifact, appendErr = s.store.AppendLog(stream.Context(), metadata.GetNamespace(), metadata.GetName(), firstChunk.GetData())
		if appendErr != nil {
			s.recordOp("append_log", appendErr, 0)
			return appendErr
		}
	}
	for {
		chunk, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			recvErr = grpcErr(recvErr)
			s.recordOp("append_log", recvErr, 0)
			return recvErr
		}
		if size := len(chunk.GetData()); size > maxChunkSize {
			tooLargeErr := status.Errorf(codes.InvalidArgument, "chunk of %d bytes exceeds limit of %d", size, maxChunkSize)
			s.recordOp("append_log", tooLargeErr, 0)
			return tooLargeErr
		}
		if len(chunk.GetData()) == 0 {
			continue
		}
		appended += int64(len(chunk.GetData()))
		var appendErr error
		artifact, appendErr = s.store.AppendLog(stream.Context(), metadata.GetNamespace(), metadata.GetName(), chunk.GetData())
		if appendErr != nil {
			s.recordOp("append_log", appendErr, 0)
			return appendErr
		}
	}
	if artifact == nil {
		// No data was sent; report the (possibly newly created) log's state.
		var getErr error
		artifact, getErr = s.store.GetLog(stream.Context(), metadata.GetNamespace(), metadata.GetName())
		if getErr != nil {
			s.recordOp("append_log", getErr, 0)
			return getErr
		}
	}
	s.recordOp("append_log", nil, appended)
	return stream.SendAndClose(&artifactspb.UploadArtifactResponse{Artifact: artifact})
}

// DownloadLog streams a log back: the first chunk carries metadata,
// subsequent chunks carry data. Reading a log that is still being written
// returns the output captured so far. When the request's offset is greater
// than zero the stream begins at that byte offset (a range read of the log's
// tail); this is how a consumer that has already read up to a size fetches
// only the new bytes (F-23 cross-pod near-live logs).
func (s *Server) DownloadLog(req *artifactspb.DownloadLogRequest, stream grpc.ServerStreamingServer[artifactspb.ArtifactChunk]) (err error) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.ObserveDuration("download", time.Since(start))
		}
	}()
	namespace, name := req.GetNamespace(), req.GetName()
	if namespace == "" || name == "" {
		invalidErr := status.Error(codes.InvalidArgument, "namespace and name are required")
		s.recordOp("download", invalidErr, 0)
		return invalidErr
	}
	artifact, reader, dlErr := s.store.DownloadLog(stream.Context(), namespace, name, req.GetOffset())
	if dlErr != nil {
		s.recordOp("download", dlErr, 0)
		return dlErr
	}
	defer reader.Close()
	if err := stream.Send(&artifactspb.ArtifactChunk{Metadata: artifact}); err != nil {
		s.recordOp("download", grpcErr(err), 0)
		return grpcErr(err)
	}
	s.recordOp("download", nil, artifact.GetSize())
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
