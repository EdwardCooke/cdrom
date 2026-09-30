// Package logstream implements an executor.LogSink that streams a job's step
// output (stdout/stderr) to the API's StreamJobLogs RPC in near-real-time.
//
// Both the long-lived worker and the ephemeral agent use it: they open a sink
// for a job, set it in the executor's context (executor.ContextWithLogSink),
// and run the job's spec. As each step produces output, the built-in shell
// handler writes it to the sink, which sends it to the API. The API persists
// the output to the artifacts service and fans it out to the UI over the
// WebSocket event hub.
//
// The sink is best-effort and never blocks the job: output is buffered in a
// bounded channel and a background goroutine drains it to the gRPC stream. If
// the API is slow the channel fills and output is dropped (consistent with the
// EventHub drop-and-resync behavior) — the persisted log the API writes is the
// source of truth for replay, and the UI resynchronizes from it on reconnect.
//
// The sink is resilient to the API going away mid-job (a pod restart or scale
// event): if the stream breaks, the drain goroutine reopens a fresh
// StreamJobLogs stream and resumes sending. Chunks queued while the stream was
// down are resent (they carry per-chunk metadata, so a reopened stream is
// self-describing); if the API is still down when the queue fills, chunks are
// dropped. If the stream cannot be opened at all, output is simply not
// streamed (the job still runs and its local logging is unaffected).
package logstream

import (
	"context"
	"log/slog"
	"sync"
	"time"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/grpcutil"
)

// sinkQueueSize bounds how many output chunks can be queued between the
// command and the API. When it is full (the API is slower than the command
// produces output) new chunks are dropped.
const sinkQueueSize = 1024

// sinkReconnectDelay is how long the drain goroutine waits before reopening a
// broken StreamJobLogs stream (the API is down or restarting).
const sinkReconnectDelay = 500 * time.Millisecond

// Sink streams a single job's step output to the API. It is safe for
// concurrent use (a step's stdout and stderr are written from different
// goroutines).
type Sink struct {
	api    apipb.APIClient
	jobID  int64
	token  string
	ctx    context.Context
	logger *slog.Logger

	queue  chan *apipb.JobLogChunk
	done   chan struct{} // closed when the drain goroutine exits
	once   sync.Once
	mu     sync.Mutex
	closed bool
}

// NewSink opens a StreamJobLogs client stream to the API for jobID and
// returns a Sink that sends the job's output over it. token is the job token
// the target presents to the API (empty when job-token auth is disabled);
// logger is used for reconnection diagnostics (nil → slog.Default). It
// returns (nil, nil) when api is nil (streaming disabled).
func NewSink(api apipb.APIClient, ctx context.Context, jobID int64, token string, logger *slog.Logger) (*Sink, error) {
	if api == nil {
		return nil, nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	s := &Sink{
		api:    api,
		jobID:  jobID,
		token:  token,
		ctx:    ctx,
		logger: logger,
		queue:  make(chan *apipb.JobLogChunk, sinkQueueSize),
		done:   make(chan struct{}),
	}
	go s.drain()
	return s, nil
}

// WriteStepOutput records a chunk of a step's output. It is non-blocking: the
// chunk is queued for the background drain goroutine, and dropped when the
// queue is full (the API is slower than the command produces output, or is
// down and the queue has filled). It is a no-op once the sink is closed. It
// never returns an error: streaming is best-effort and must never fail or
// block the job.
func (s *Sink) WriteStepOutput(stepIndex int, stream string, data []byte) {
	if s == nil || len(data) == 0 {
		return
	}
	chunk := &apipb.JobLogChunk{
		Metadata: &apipb.JobLogMetadata{
			JobId:     s.jobID,
			StepIndex: int32(stepIndex),
			Stream:    streamToEnum(stream),
		},
		Data: data,
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return
	}
	select {
	case s.queue <- chunk:
	default:
		// Queue full: drop the chunk. The persisted log (written by the API
		// from the chunks it did receive) is the source of truth for replay.
	}
}

// Close ends the sink. It flushes any queued output (bounded by a short
// deadline so shutdown never blocks on a slow or down API), signals end of
// stream to the API, and is idempotent and safe to call multiple times.
func (s *Sink) Close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.queue) // the drain goroutine flushes the remainder, then exits
		// Bound the flush: if the API is down the drain goroutine is
		// reconnecting and will not drain the queue, so waiting on done
		// unconditionally would hang shutdown.
		select {
		case <-s.done:
		case <-time.After(2 * time.Second):
		}
	})
}

// drain sends queued chunks to the API until the queue is closed and drained,
// the sink is closed, or the context is cancelled. If the stream breaks (the
// API went away) it waits for the API to come back and reopens a fresh
// StreamJobLogs stream; chunks queued while the stream was down are resent
// (they carry per-chunk metadata, so a reopened stream is self-describing).
func (s *Sink) drain() {
	defer close(s.done)
	stream := s.openStream()
	for {
		if stream == nil {
			// The stream is down (the API is unreachable). Wait for it to
			// come back before pulling the next chunk, so queued output is
			// not dropped while the API is down.
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(sinkReconnectDelay):
			}
			if s.isClosed() {
				return
			}
			stream = s.openStream()
			if stream == nil {
				continue // still down; the queue is left untouched
			}
		}
		chunk, ok := <-s.queue
		if !ok {
			// Queue closed and drained: signal end of stream.
			_ = stream.CloseSend()
			return
		}
		if err := stream.Send(chunk); err != nil {
			// The stream broke mid-send (e.g. the API restarted). The chunk
			// is dropped (best-effort); reopen on the next iteration.
			s.logger.Warn("logstream: send failed; reconnecting", "job", s.jobID, "err", err)
			stream = nil
		}
	}
}

// isClosed reports whether Close has been called.
func (s *Sink) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// openStream opens a fresh StreamJobLogs client stream to the API, presenting
// the job token. It returns nil when the stream cannot be opened (the API is
// down or the context is cancelled); the caller retries.
func (s *Sink) openStream() apipb.API_StreamJobLogsClient {
	ctx := grpcutil.WithBearerToken(s.ctx, s.token)
	stream, err := s.api.StreamJobLogs(ctx)
	if err != nil {
		return nil
	}
	return stream
}

// streamToEnum maps an output stream name to the proto enum.
func streamToEnum(stream string) apipb.JobLogStream {
	switch stream {
	case "stderr":
		return apipb.JobLogStream_JOB_LOG_STREAM_STDERR
	default:
		return apipb.JobLogStream_JOB_LOG_STREAM_STDOUT
	}
}
