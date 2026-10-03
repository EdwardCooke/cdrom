// Package scheduler — retry loop (F-04).
//
// The retry loop is the scheduler-side mechanism that re-dispatches a failed
// job up to its retry budget. The execution target (worker or agent) reports
// a job's terminal status to the API, which persists it through the Database
// service; the retry loop closes the loop by periodically listing failed
// jobs and, for each that still has retries remaining, atomically claiming
// the next attempt (Database.ClaimJobRetry) and, after the policy's backoff,
// appending a "job_status" (reset to pending) and an "assignment" event to
// the shared event log (F-23), which every API pod tails and fans out to its
// local workers.
//
// A retry is a new attempt on the same Job row (not a new Job), so the
// pipeline run stays coherent: the job's attempt counter is incremented and
// the job is reset to pending, and the UI can show "attempt N of M". A job
// with no retry policy (max_attempts 0) or one that has exhausted its retries
// is never re-dispatched.
package scheduler

import (
	"context"
	"time"

	"google.golang.org/grpc"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// retryInterval is how often the retry loop scans for failed jobs to retry.
// It is a variable (not a constant) so tests can shorten it.
var retryInterval = 5 * time.Second

// retryClaimer is the subset of the Database client the retry loop uses to
// list the jobs that still have retries remaining and atomically claim their
// next retry attempt. The concrete dbpb.DatabaseClient satisfies it; tests
// inject a fake.
type retryClaimer interface {
	ListRetriableJobs(ctx context.Context, in *dbpb.ListRetriableJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error)
	ClaimJobRetry(ctx context.Context, in *dbpb.ClaimJobRetryRequest, opts ...grpc.CallOption) (*dbpb.ClaimJobRetryResponse, error)
}

// retryPublisher is the subset of the Database client the retry loop uses to
// append events to the shared event log (F-23): a "job_status" event for the
// reset to pending (so the UI sees the job is being retried) and an
// "assignment" event (so the workers in the group are nudged). The concrete
// dbpb.DatabaseClient satisfies it; tests inject a fake.
type retryPublisher interface {
	PublishJobStatus(ctx context.Context, in *dbpb.PublishJobStatusRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error)
	PublishAssignment(ctx context.Context, in *dbpb.PublishAssignmentRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error)
}

// StartRetryLoop runs the job-retry loop in the background until ctx is
// cancelled. It periodically re-dispatches failed jobs that still have
// retries remaining (F-04). It is a no-op when the Database client is nil
// (e.g. in tests that do not exercise the retry loop).
func (s *Server) StartRetryLoop(ctx context.Context) {
	if s.db == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(retryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.retryFailedJobs(ctx, s.db, s.db)
			}
		}
	}()
}

// retryFailedJobs lists the jobs that still have retries remaining (the
// database filters out jobs with no retry policy and jobs that have exhausted
// their budget) and, for each, claims the next attempt (atomically) and
// re-dispatches it after the policy's backoff. db and publisher are the
// loop's dependencies (the concrete clients in production, fakes in tests);
// publisher may be nil to skip the event appends.
func (s *Server) retryFailedJobs(ctx context.Context, db retryClaimer, publisher retryPublisher) {
	response, err := db.ListRetriableJobs(ctx, &dbpb.ListRetriableJobsRequest{})
	if err != nil {
		s.logger.Warn("scheduler: retry list retriable jobs", "err", err)
		return
	}
	for _, job := range response.GetJobs() {
		s.claimAndDispatch(ctx, db, publisher, job)
	}
}

// claimAndDispatch claims the next retry attempt for a job (atomically) and,
// on success, appends a "job_status" event for the reset to pending (so the
// UI sees the job is being retried) and re-dispatches the job after the
// policy's backoff. It returns whether the job was claimed. The caller
// (retryFailedJobs) only passes jobs the database reported as still having
// retries remaining; the atomic ClaimJobRetry is the final guard, so a job
// that changed state (or exhausted its budget) between the list and the claim
// is left untouched.
func (s *Server) claimAndDispatch(ctx context.Context, db retryClaimer, publisher retryPublisher, job *dbpb.Job) bool {
	resp, err := db.ClaimJobRetry(ctx, &dbpb.ClaimJobRetryRequest{Id: job.GetId()})
	if err != nil {
		s.logger.Warn("scheduler: claim retry", "job", job.GetId(), "err", err)
		return false
	}
	if !resp.GetClaimed() {
		// The job changed state (or exhausted its budget) between the list and
		// the claim; it was not retried.
		return false
	}
	job.Attempt = resp.GetAttempt()
	s.logger.Info("scheduler: retrying job", "job", job.GetId(), "attempt", resp.GetAttempt(), "max", job.GetMaxAttempts())
	if publisher == nil {
		return true
	}
	// Append a "job_status" event for the reset to pending so the UI sees the
	// job is being retried (the persisted status is the source of truth; this
	// only publishes the event). The attempt and max_attempts are passed so
	// the UI can render "attempt N of M".
	if _, err := publisher.PublishJobStatus(ctx, &dbpb.PublishJobStatusRequest{
		JobId:       job.GetId(),
		Status:      dbpb.JobStatus_JOB_STATUS_PENDING,
		Attempt:     job.GetAttempt(),
		MaxAttempts: job.GetMaxAttempts(),
	}); err != nil {
		s.logger.Warn("scheduler: publish retried job status", "job", job.GetId(), "err", err)
	}
	backoff := job.GetSpec().GetRetry().GetBackoff().AsDuration()
	if backoff <= 0 {
		// No backoff: dispatch immediately.
		s.dispatchForRetry(ctx, publisher, job)
		return true
	}
	// Back off before re-dispatching. The goroutine is detached from the tick
	// so a long backoff does not block the loop; it is cancelled with the
	// loop's context.
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		s.dispatchForRetry(ctx, publisher, job)
	}()
	return true
}

// dispatchForRetry appends an "assignment" event for a retried job to the
// shared event log (F-23), which every API pod tails and fans out to its
// local workers in the job's target group. A job with an empty target group
// is for an ephemeral agent and is not nudged.
func (s *Server) dispatchForRetry(ctx context.Context, publisher retryPublisher, job *dbpb.Job) {
	if job.GetTargetGroup() == "" {
		return
	}
	if _, err := publisher.PublishAssignment(ctx, &dbpb.PublishAssignmentRequest{JobId: job.GetId(), TargetGroup: job.GetTargetGroup()}); err != nil {
		s.logger.Warn("scheduler: publish retried job assignment", "job", job.GetId(), "group", job.GetTargetGroup(), "err", err)
	}
}
