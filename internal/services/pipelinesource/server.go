package pipelinesource

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
	"cdrom/internal/metrics"
)

// Server implements the PipelineSource gRPC service (F-21.2.1). It parses a
// reference (F-21.2.2) and dispatches the fetch to the matching client (the
// git client over the repository cache, F-21.2.3; the http client; or the
// rooted file client, F-21.2.4). It holds no pipeline definitions, no
// credentials, and no encryption key at rest; a credential is passed in for one
// fetch and is never persisted or logged.
type Server struct {
	pipelinesourcepb.UnimplementedPipelineSourceServer
	git   *gitClient
	http  *httpClient
	file  *fileClient
	cache *repoCache
	// metrics records the source-fetcher's Prometheus metrics (F-20). It is
	// nil when metrics are disabled, in which case all recording is a no-op.
	metrics *metrics.PipelineSourceMetrics
}

// NewServer creates a PipelineSource gRPC service. cacheRoot is the git
// repository cache root (empty = the default), fileRoot is the local-filesystem
// root for file: references (empty = the file kind disabled), and logger is the
// process logger.
func NewServer(cacheRoot, fileRoot string, logger *slog.Logger) (*Server, error) {
	cache, err := newRepoCache(cacheRoot, logger)
	if err != nil {
		return nil, err
	}
	return &Server{
		git:   newGitClient(cache, logger),
		http:  newHTTPClient(),
		file:  newFileClient(fileRoot),
		cache: cache,
	}, nil
}

// SetMetrics attaches the source-fetcher's Prometheus metric set (F-20). When
// left nil (metrics disabled) all recording is a no-op.
func (s *Server) SetMetrics(m *metrics.PipelineSourceMetrics) { s.metrics = m }

// StartCleanup starts the cache's background cleanup thread (F-21.2.3.4) and
// returns a stop function.
func (s *Server) StartCleanup(ctx context.Context) func() {
	return s.cache.startCleanup(ctx)
}

// CacheRoot returns the effective git repository cache root (the configured
// root, or the default when none was set).
func (s *Server) CacheRoot() string { return s.cache.root }

// FetchSource retrieves the file named by the request's reference and returns
// its content, version identifier, and content hash.
func (s *Server) FetchSource(ctx context.Context, req *pipelinesourcepb.FetchSourceRequest) (*pipelinesourcepb.FetchSourceResponse, error) {
	ref, err := ParseReference(req.GetReference())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	start := time.Now()
	var content []byte
	var version string
	var hash string
	switch ref.Kind {
	case KindGit:
		// Pass the runtime-managed SSH host keys (F-21.2.4.1) the API
		// accumulates from its database; the git client combines them with its
		// built-in common provider keys for host-key verification.
		content, version, hash, err = s.git.fetch(ctx, ref, toGitCredential(req.GetCredential()), req.GetKnownHosts())
	case KindHTTP:
		content, version, hash, err = s.http.fetch(ctx, ref, toHTTPCredential(req.GetCredential()))
	case KindFile:
		content, version, hash, err = s.file.fetch(ctx, ref)
	default:
		err = fmt.Errorf("unknown kind %q", ref.Kind)
	}
	s.record(ref.Kind, err, len(content), time.Since(start))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pipelinesourcepb.FetchSourceResponse{
		Content: content,
		Version: version,
		Hash:    hash,
	}, nil
}

// record records a fetch's outcome (F-20): the operation counter (by kind and
// gRPC status code), the bytes fetched, and the duration. It is a no-op when
// metrics are disabled.
func (s *Server) record(kind string, err error, n int, d time.Duration) {
	if s.metrics == nil {
		return
	}
	code := codes.OK
	if err != nil {
		if st, ok := status.FromError(err); ok {
			code = st.Code()
		} else {
			code = codes.Internal
		}
	}
	s.metrics.RecordFetch(kind, code.String())
	s.metrics.AddBytes(kind, int64(n))
	s.metrics.ObserveDuration(kind, d)
}
