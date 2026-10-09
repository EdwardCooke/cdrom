// Package audit — the Recorder: the single entry point the API (and the
// scheduler) use to record an audited action (F-15).
//
// A Recorder fans each Event out to two sinks:
//
//   - the local audit Logger (a rotating file, or stdout), and
//   - a Sink (the database's audit log, via the Database service's
//     AppendAuditEvent RPC), which is the queryable, cross-replica store the
//     UI reads.
//
// Recording is best-effort and never blocks or fails the caller: a failure to
// write to either sink is dropped (the action itself has already happened), so
// an audit-log outage can never take down the API.
//
// StartPruning runs a background loop that periodically prunes the database's
// audit log of entries older than the configured retention, so the table does
// not grow without bound.
package audit

import (
	"context"
	"log/slog"
	"time"
)

// Sink persists an audit Event to a durable, queryable store. The production
// implementation is the database service's AppendAuditEvent RPC (see the API's
// databaseSink); tests use an in-memory fake.
type Sink interface {
	Append(ctx context.Context, e Event) error
}

// Pruner prunes a store's audit entries older than the given instant. The
// production implementation is the database service's PruneAuditEvents RPC.
type Pruner interface {
	Prune(ctx context.Context, before time.Time) (int64, error)
}

// Recorder records audit events to a local Logger and a Sink. It is safe for
// concurrent use. A nil Logger or Sink is skipped, so a Recorder can be
// configured with either, both, or (in tests) neither.
type Recorder struct {
	logger *Logger
	sink   Sink
	pruner Pruner
	log    *slog.Logger
}

// NewRecorder creates a Recorder that writes to logger (the local audit log)
// and sink (the durable store). Either may be nil. log is used to report
// (non-fatal) recording failures; a nil log uses slog.Default().
func NewRecorder(logger *Logger, sink Sink, pruner Pruner, log *slog.Logger) *Recorder {
	if log == nil {
		log = slog.Default()
	}
	return &Recorder{logger: logger, sink: sink, pruner: pruner, log: log}
}

// Record writes e to both sinks. It never returns an error: a failure to write
// to a sink is logged and dropped, so recording an audit event can never fail
// (or block) the action it is auditing. A zero e.Time is stamped with the
// current time.
func (r *Recorder) Record(e Event) {
	if r == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	if r.logger != nil {
		if err := r.logger.Log(e); err != nil {
			r.log.Warn("audit: write local log", "err", err)
		}
	}
	if r.sink != nil {
		// A short timeout bounds how long a slow or down database can hold up
		// the caller; the event is dropped on failure (the action already
		// happened).
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.sink.Append(ctx, e); err != nil {
			r.log.Warn("audit: append to store", "action", e.Action, "err", err)
		}
	}
}

// StartPruning runs a background loop that prunes the durable store's audit
// entries older than the retention, every pruneInterval, until ctx is
// cancelled. It is a no-op when the pruner is nil, the retention is zero
// (entries are never pruned by age), or the prune interval is zero.
func (r *Recorder) StartPruning(ctx context.Context, retention, pruneInterval time.Duration) {
	if r == nil || r.pruner == nil || retention <= 0 || pruneInterval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(pruneInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.pruneOnce(retention)
			}
		}
	}()
}

// Close closes the Recorder's local audit Logger (releasing its file handle).
// It is a no-op when the Recorder is nil or has no local logger. It is
// idempotent.
func (r *Recorder) Close() error {
	if r == nil {
		return nil
	}
	return r.logger.Close()
}

// pruneOnce prunes the store's audit entries older than the retention and
// logs the result.
func (r *Recorder) pruneOnce(retention time.Duration) {
	cutoff := time.Now().Add(-retention)
	n, err := r.pruner.Prune(context.Background(), cutoff)
	if err != nil {
		r.log.Warn("audit: prune", "err", err)
		return
	}
	if n > 0 {
		r.log.Info("audit: pruned old entries", "pruned", n, "cutoff", cutoff)
	}
}
