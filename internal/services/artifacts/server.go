// server.go — the Artifacts gRPC service (F-21.6 storage redesign): a dumb
// blob store keyed by opaque ids. Streaming/chunking protocol lives HERE;
// the actual I/O is delegated to a Store (blobfs.go by default).
//
// Frame conventions:
//
//   - PutBlob / AppendBlob are CLIENT streams. The FIRST chunk must carry
//     the blob's id (later chunks carry data only; an id-less first chunk is
//     InvalidArgument). Subsequent oversized chunks are rejected.
//   - GetBlob is a SERVER stream. The FIRST chunk carries the blob's total
//     size (so a ranged reader learns the ceiling without a separate stat);
//     later chunks carry data.
//
// Existence predicates (matching the Store): putting/copying to an id that
// already holds a NON-EMPTY blob fails with AlreadyExists; getting/statting
// a missing id fails with NotFound; deleting a missing id is a no-op.
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
// filesystem blob store rooted at root.
func NewFilesystemServer(root string) (*Server, error) {
	store, err := NewBlobFSStore(root)
	if err != nil {
		return nil, err
	}
	return &Server{store: store}, nil
}

// ---------------------------------------------------------------------------
// Put / Append (client streams)
// ---------------------------------------------------------------------------

// framed is the slice of the chunk API firstFrame needs (generics do not
// expose accessor methods implicitly, so the constraint spells them out).
type framed interface{ GetId() string }

// firstFrame pulls the first chunk off a client stream and validates its id.
func firstFrame[T framed](recvd func() (T, error)) (T, error) {
	frame, err := recvd()
	if err != nil {
		return frame, grpcErr(err)
	}
	if frame.GetId() == "" {
		return frame, status.Error(codes.InvalidArgument, "first chunk must carry the blob id")
	}
	if err := ValidateBlobID(frame.GetId()); err != nil {
		return frame, status.Error(codes.InvalidArgument, err.Error())
	}
	return frame, nil
}

// PutBlob consumes a client stream of chunks and writes them to the blob at
// the id carried in the first chunk (streamed, not buffered in memory).
func (s *Server) PutBlob(stream grpc.ClientStreamingServer[artifactspb.BlobChunk, artifactspb.BlobStat]) (err error) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.ObserveDuration("put_blob", time.Since(start))
		}
	}()
	recvd := stream.Recv
	first, err := firstFrame(recvd)
	if err != nil {
		s.recordOp("put_blob", err, 0)
		return err
	}
	id := first.GetId()
	putter := &putStreamer{stream: stream, first: first}
	stored, putErr := s.store.Put(stream.Context(), id, putter)
	if putErr != nil {
		s.recordOp("put_blob", putErr, 0)
		return putErr
	}
	s.recordOp("put_blob", nil, stored)
	return stream.SendAndClose(&artifactspb.BlobStat{Size: stored})
}

// AppendBlob consumes a client stream of chunks and appends them to the blob
// at the id carried in the first chunk (creating it when absent). Returns
// the blob's total size after the append.
func (s *Server) AppendBlob(stream grpc.ClientStreamingServer[artifactspb.BlobChunk, artifactspb.BlobStat]) (err error) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.ObserveDuration("append_blob", time.Since(start))
		}
	}()
	recvd := stream.Recv
	first, err := firstFrame(recvd)
	if err != nil {
		s.recordOp("append_blob", err, 0)
		return err
	}
	id := first.GetId()
	total, err := s.store.Append(stream.Context(), id, first.GetData())
	if err != nil {
		s.recordOp("append_blob", err, 0)
		return err
	}
	for {
		chunk, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			recvErr = grpcErr(recvErr)
			s.recordOp("append_blob", recvErr, 0)
			return recvErr
		}
		if size := len(chunk.GetData()); size > maxChunkSize {
			tooLargeErr := status.Errorf(codes.InvalidArgument, "chunk of %d bytes exceeds limit of %d", size, maxChunkSize)
			s.recordOp("append_blob", tooLargeErr, 0)
			return tooLargeErr
		}
		if len(chunk.GetData()) == 0 {
			continue
		}
		total, err = s.store.Append(stream.Context(), id, chunk.GetData())
		if err != nil {
			s.recordOp("append_blob", err, 0)
			return err
		}
	}
	s.recordOp("append_blob", nil, total)
	return stream.SendAndClose(&artifactspb.BlobStat{Size: total})
}

// putStreamer adapts a client-streaming gRPC stream to an io.Reader for the
// store's Put. The first chunk (already received and id-validated) is served
// before subsequent Recv calls; an id-less LATER chunk is a protocol
// violation (rejected), as is an oversize chunk.
type putStreamer struct {
	stream      grpc.ClientStreamingServer[artifactspb.BlobChunk, artifactspb.BlobStat]
	first       *artifactspb.BlobChunk
	firstServed bool
	buf         []byte
}

func (r *putStreamer) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		chunk, err := r.next()
		if err != nil {
			return 0, err // io.EOF at the end of the stream
		}
		if len(chunk) == 0 {
			continue // an empty data chunk advances to the next one
		}
		r.buf = chunk
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// next returns the next chunk's data, or (nil, io.EOF) at the end of the
// stream.
func (r *putStreamer) next() ([]byte, error) {
	if !r.firstServed {
		r.firstServed = true
		return r.first.GetData(), nil
	}
	chunk, err := r.stream.Recv()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, grpcErr(err)
	}
	if chunk.GetId() != "" {
		return nil, status.Error(codes.InvalidArgument, "blob id may only be carried on the first chunk")
	}
	if size := len(chunk.GetData()); size > maxChunkSize {
		return nil, status.Errorf(codes.InvalidArgument, "chunk of %d bytes exceeds limit of %d", size, maxChunkSize)
	}
	return chunk.GetData(), nil
}

// ---------------------------------------------------------------------------
// Get / Stat (reads)
// ---------------------------------------------------------------------------

// GetBlob streams a blob's bytes back: the first chunk carries the blob's
// total size, subsequent chunks carry data. A non-zero offset begins the
// stream at that byte (range reads power cross-pod log tailing, F-23).
func (s *Server) GetBlob(req *artifactspb.BlobRef, stream grpc.ServerStreamingServer[artifactspb.BlobChunk]) (err error) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.ObserveDuration("get_blob", time.Since(start))
		}
	}()
	id := req.GetId()
	offset := req.GetOffset()
	if offset < 0 {
		invalidErr := status.Error(codes.InvalidArgument, "offset must not be negative")
		s.recordOp("get_blob", invalidErr, 0)
		return invalidErr
	}
	size, reader, dlErr := s.store.Get(stream.Context(), id, offset)
	if dlErr != nil {
		s.recordOp("get_blob", dlErr, 0)
		return dlErr
	}
	defer reader.Close()
	if err := stream.Send(&artifactspb.BlobChunk{Size: size}); err != nil {
		s.recordOp("get_blob", grpcErr(err), 0)
		return grpcErr(err)
	}
	s.recordOp("get_blob", nil, size)
	return streamChunks(stream, reader)
}

// StatBlob returns a blob's size (without transferring its bytes).
func (s *Server) StatBlob(ctx context.Context, req *artifactspb.BlobRef) (*artifactspb.BlobStat, error) {
	size, err := s.store.Stat(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &artifactspb.BlobStat{Size: size}, nil
}

// ---------------------------------------------------------------------------
// Copy / Delete
// ---------------------------------------------------------------------------

// CopyBlob replicates one blob to another id SERVER-SIDE (no bytes pass
// through the caller). Powers promotion (F-21.6/former F-19).
func (s *Server) CopyBlob(ctx context.Context, req *artifactspb.CopyBlobRequest) (*artifactspb.BlobStat, error) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.ObserveDuration("copy_blob", time.Since(start))
		}
	}()
	copied, err := s.store.Copy(ctx, req.GetSrc(), req.GetDst())
	s.recordOp("copy_blob", err, copied)
	if err != nil {
		return nil, err
	}
	return &artifactspb.BlobStat{Size: copied}, nil
}

// DeleteBlob removes a blob (nothing else). A missing id is a no-op.
func (s *Server) DeleteBlob(ctx context.Context, req *artifactspb.BlobRef) (*emptypb.Empty, error) {
	if err := s.store.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// streamChunks reads from reader in bounded chunks and sends each as a data
// chunk on stream.
func streamChunks(stream grpc.ServerStreamingServer[artifactspb.BlobChunk], reader io.Reader) error {
	buffer := make([]byte, maxChunkSize)
	for {
		read, err := reader.Read(buffer)
		if read > 0 {
			if sendErr := stream.Send(&artifactspb.BlobChunk{Data: append([]byte(nil), buffer[:read]...)}); sendErr != nil {
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

// grpcErr normalizes a gRPC/stream error into a status error (EOF mid-frame
// is unexpected and reported as Internal).
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
