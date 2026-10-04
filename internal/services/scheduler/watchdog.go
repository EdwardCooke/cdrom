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
// status is left untouched. When a job is reaped the watchdog appends a
// "job_status" event to the shared event log (F-23), which every API pod tails
// and fans out to its UI clients, mirroring the job_status events a target's
// report would produce.
package scheduler

import (
	"context"
	"time"

	"google.golang.org/grpc"

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

// jobStatusPublisher is the subset of the Database client the watchdog uses
// to append a "job_status" event for a reaped job to the shared event log
// (F-23). The concrete dbpb.DatabaseClient satisfies it; tests inject a fake.
type jobStatusPublisher interface {
	PublishJobStatus(ctx context.Context, in *dbpb.PublishJobStatusRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error)
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
				s.reapTimedOutJobs(ctx, s.db, s.db)
			}
		}
	}()
}

// reapTimedOutJobs lists the running jobs and reaps any that have exceeded
// their effective timeout. A job with no timeout is never reaped. Reaping is
// conditional (Database.ReapJob), so a job that reported a terminal status
// between the list and the reap is left untouched. db and publisher are the
// watchdog's dependencies (the concrete clients in production, fakes in
// tests); publisher may be nil to skip the event append.
func (s *Server) reapTimedOutJobs(ctx context.Context, db jobReaper, publisher jobStatusPublisher) {
	response, err := db.ListJobs(ctx, &dbpb.ListJobsRequest{Status: dbpb.JobStatus_JOB_STATUS_RUNNING})
	if err != nil {
		s.logger.Warn("scheduler: watchdog list running jobs", "err", err)
		return
	}
	now := time.Now()
	for _, job := range response.GetJobs() {
		if job.GetTargetGroup() != "" {
			// A job that targets a worker group runs on every worker in the
			// group (fan-out). Each worker enforces its own execution's timeout
			// and reports it to its execution; the job's overall status is
			// derived from the executions by the job-status loop, which drops a
			// worker that has gone quiet (a dead worker's in-progress execution
			// does not block the job). The job-level watchdog would wrongly reap
			// the whole job as timed_out while other workers are still running,
			// so group jobs are not reaped here.
			continue
		}
		deadline, ok := jobDeadline(job)
		if !ok {
			// No timeout declared: the job runs unbounded.
			continue
		}
		if now.Before(deadline) {
			continue
		}
		if s.reap(ctx, db, publisher, job, deadline) {
			s.logger.Info("scheduler: reaped timed-out job", "job", job.GetId(), "deadline", deadline)
		}
	}
}

// reap marks a job timed_out (conditionally) and, if it was reaped, appends a
// "job_status" event to the shared event log (F-23) so every API pod can fan
// the status change out to its UI clients. It returns whether the job was
// reaped.
func (s *Server) reap(ctx context.Context, db jobReaper, publisher jobStatusPublisher, job *dbpb.Job, deadline time.Time) bool {
	jobID := job.GetId()
	resp, err := db.ReapJob(ctx, &dbpb.ReapJobRequest{Id: jobID})
	if err != nil {
		s.logger.Warn("scheduler: reap job", "job", jobID, "err", err)
		return false
	}
	if !resp.GetReaped() {
		// The job already reached a terminal state; nothing to do.
		return false
	}
	// Append the status change to the shared event log (F-23). A nil publisher
	// (e.g. in tests) simply skips the event — the persisted status is the
	// source of truth.
	if publisher != nil {
		if _, err := publisher.PublishJobStatus(ctx, &dbpb.PublishJobStatusRequest{
			JobId:       jobID,
			Status:      dbpb.JobStatus_JOB_STATUS_TIMED_OUT,
			Attempt:     job.GetAttempt(),
			MaxAttempts: job.GetMaxAttempts(),
		}); err != nil {
			s.logger.Warn("scheduler: publish reaped job status", "job", jobID, "err", err)
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
