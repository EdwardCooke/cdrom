// Package scheduler — watchdog (F-03).
//
// The watchdog is the scheduler-side safety valve for job timeouts. The
// execution target (worker or agent) enforces a job's timeout by cancelling
// the running step and reporting timed_out; but if the target goes silent
// (a dead worker, a crashed agent, a lost stream) it never reports, and the
// job would sit "running" forever. The watchdog closes that gap: it
// periodically reaps running jobs that have exceeded their declared timeout,
// even when the target is unreachable.
//
// A job's effective timeout is its job-level timeout (spec.timeout) when set,
// otherwise the longest per-step timeout among its steps. A job with no
// timeout (job-level and per-step) runs unbounded and is never reaped. The
// deadline is measured from the job's started_at timestamp.
//
// Reaping is conditional (Database.ReapJob): it marks the job timed_out only
// if it is still pending or running, so a job that already reported a terminal
// status is left untouched. When a job is reaped the watchdog tells the API
// (NotifyJobStatus) to fan the status change out to the UI over the WebSocket
// event hub, mirroring the job_status events a target's report would produce.
package scheduler

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// watchdogInterval is how often the watchdog scans for timed-out jobs. It is
// a variable (not a constant) so tests can shorten it.
var watchdogInterval = 5 * time.Second

// jobReaper is the subset of the Database client the watchdog uses to list
// running jobs and conditionally reap them. The concrete dbpb.DatabaseClient
// satisfies it; tests inject a fake.
type jobReaper interface {
	ListJobs(ctx context.Context, in *dbpb.ListJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error)
	ReapJob(ctx context.Context, in *dbpb.ReapJobRequest, opts ...grpc.CallOption) (*dbpb.ReapJobResponse, error)
}

// jobNotifier is the subset of the API client the watchdog uses to fan a
// reaped job's status change out to the UI. The concrete apipb.APIClient
// satisfies it; tests inject a fake.
type jobNotifier interface {
	NotifyJobStatus(ctx context.Context, in *apipb.NotifyJobStatusRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

// StartWatchdog runs the job-timeout watchdog in the background until ctx is
// cancelled. It periodically reaps running jobs that have exceeded their
// declared timeout (F-03), covering the case where the execution target goes
// silent and never reports. It is a no-op when the Database client is nil
// (e.g. in tests that do not exercise the watchdog).
func (s *Server) StartWatchdog(ctx context.Context) {
	if s.db == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(watchdogInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.reapTimedOutJobs(ctx, s.db, s.api)
			}
		}
	}()
}

// reapTimedOutJobs lists the running jobs and reaps any that have exceeded
// their effective timeout. A job with no timeout is never reaped. Reaping is
// conditional (Database.ReapJob), so a job that reported a terminal status
// between the list and the reap is left untouched. db and api are the
// watchdog's dependencies (the concrete clients in production, fakes in
// tests); api may be nil to skip the UI notification.
func (s *Server) reapTimedOutJobs(ctx context.Context, db jobReaper, api jobNotifier) {
	response, err := db.ListJobs(ctx, &dbpb.ListJobsRequest{Status: dbpb.JobStatus_JOB_STATUS_RUNNING})
	if err != nil {
		s.logger.Warn("scheduler: watchdog list running jobs", "err", err)
		return
	}
	now := time.Now()
	for _, job := range response.GetJobs() {
		deadline, ok := jobDeadline(job)
		if !ok {
			// No timeout declared: the job runs unbounded.
			continue
		}
		if now.Before(deadline) {
			continue
		}
		if s.reap(ctx, db, api, job.GetId(), deadline) {
			s.logger.Info("scheduler: reaped timed-out job", "job", job.GetId(), "deadline", deadline)
		}
	}
}

// reap marks a job timed_out (conditionally) and, if it was reaped, tells the
// API to fan the status change out to the UI. It returns whether the job was
// reaped.
func (s *Server) reap(ctx context.Context, db jobReaper, api jobNotifier, jobID int64, deadline time.Time) bool {
	resp, err := db.ReapJob(ctx, &dbpb.ReapJobRequest{Id: jobID})
	if err != nil {
		s.logger.Warn("scheduler: reap job", "job", jobID, "err", err)
		return false
	}
	if !resp.GetReaped() {
		// The job already reached a terminal state; nothing to do.
		return false
	}
	// Fan the status change out to the UI. A nil api client (e.g. in tests)
	// simply skips the notification — the persisted status is the source of
	// truth.
	if api != nil {
		if _, err := api.NotifyJobStatus(ctx, &apipb.NotifyJobStatusRequest{
			JobId:  jobID,
			Status: dbpb.JobStatus_JOB_STATUS_TIMED_OUT,
		}); err != nil {
			s.logger.Warn("scheduler: notify api of reaped job", "job", jobID, "err", err)
		}
	}
	return true
}

// jobDeadline returns the deadline (started_at + effective timeout) for a job,
// and whether the job has a timeout at all. A job with no job-level timeout
// falls back to its longest per-step timeout; a job with neither has no
// deadline (ok is false) and runs unbounded. A job that has not started yet
// (no started_at) has no deadline either.
func jobDeadline(job *dbpb.Job) (time.Time, bool) {
	timeout := effectiveTimeout(job)
	if timeout <= 0 {
		return time.Time{}, false
	}
	started := job.GetStartedAt()
	if started == nil {
		return time.Time{}, false
	}
	return started.AsTime().Add(timeout), true
}

// effectiveTimeout returns a job's effective timeout: its job-level timeout
// (spec.timeout) when set, otherwise the longest per-step timeout among its
// steps. A job with no job-level timeout and no per-step timeouts has an
// effective timeout of zero (it runs unbounded).
func effectiveTimeout(job *dbpb.Job) time.Duration {
	if d := job.GetSpec().GetTimeout().AsDuration(); d > 0 {
		return d
	}
	var max time.Duration
	for _, step := range job.GetSpec().GetSteps() {
		if d := step.GetTimeout().AsDuration(); d > max {
			max = d
		}
	}
	return max
}
