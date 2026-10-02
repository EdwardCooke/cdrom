// Package scheduler — dependency resolver (F-06).
//
// A job may declare DependsOn (a list of job ids, F-06): it is held pending
// (never dispatched by SubmitJob) until every dependency reaches a terminal
// state. The resolver closes that loop: it periodically lists pending jobs,
// and for each with unresolved dependencies checks each dependency's status:
//
//   - every dependency succeeded -> the job is released: its DependsOn is
//     cleared (so it is not reconsidered) and it is dispatched exactly like a
//     job with no dependencies;
//   - any dependency reached a terminal state other than succeeded (failed,
//     cancelled, timed_out, or itself skipped) -> the job is marked skipped
//     (Database.SkipJob) instead of running, and the scheduler fans the
//     status change out to the UI (API.NotifyJobStatus);
//   - otherwise (a dependency is still pending or running) -> the job is left
//     alone and reconsidered on the resolver's next tick.
//
// This is a minimal, polling-based, single-level dependency check — enough to
// implement "skip a job whose dependency failed" (F-06) ahead of the full
// parallel DAG resolver (named `needs`, cycle validation at save time, F-08).
package scheduler

import (
	"context"
	"time"

	"google.golang.org/grpc"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// dependencyResolverInterval is how often the resolver scans pending jobs
// for unresolved dependencies. It is a variable (not a constant) so tests can
// shorten it.
var dependencyResolverInterval = 5 * time.Second

// jobDependencyChecker is the subset of the Database client the dependency
// resolver uses to list pending jobs, look up a dependency's status, skip a
// job whose dependency did not succeed, and release a job whose dependencies
// all succeeded. The concrete dbpb.DatabaseClient satisfies it; tests inject
// a fake.
type jobDependencyChecker interface {
	ListJobs(ctx context.Context, in *dbpb.ListJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error)
	GetJob(ctx context.Context, in *dbpb.GetJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error)
	SkipJob(ctx context.Context, in *dbpb.SkipJobRequest, opts ...grpc.CallOption) (*dbpb.SkipJobResponse, error)
	UpdateJob(ctx context.Context, in *dbpb.UpdateJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error)
}

// StartDependencyResolver runs the job-dependency resolver in the background
// until ctx is cancelled. It periodically resolves pending jobs that declare
// dependencies (F-06). It is a no-op when the Database client is nil (e.g. in
// tests that do not exercise the resolver).
func (s *Server) StartDependencyResolver(ctx context.Context) {
	if s.db == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(dependencyResolverInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.resolveJobDependencies(ctx, s.db, s.api)
			}
		}
	}()
}

// resolveJobDependencies lists pending jobs and resolves the dependencies of
// each one that declares any (DependsOn). db and api are the resolver's
// dependencies (the concrete clients in production, fakes in tests); api may
// be nil to skip the UI notification and dispatch.
func (s *Server) resolveJobDependencies(ctx context.Context, db jobDependencyChecker, api jobRelayer) {
	response, err := db.ListJobs(ctx, &dbpb.ListJobsRequest{Status: dbpb.JobStatus_JOB_STATUS_PENDING})
	if err != nil {
		s.logger.Warn("scheduler: dependency resolver list pending jobs", "err", err)
		return
	}
	for _, job := range response.GetJobs() {
		if len(job.GetDependsOn()) == 0 {
			continue
		}
		s.resolveJob(ctx, db, api, job)
	}
}

// resolveJob checks every one of job's dependencies and either skips the job
// (a dependency did not succeed), releases it (every dependency succeeded),
// or leaves it pending (a dependency is still pending or running).
func (s *Server) resolveJob(ctx context.Context, db jobDependencyChecker, api jobRelayer, job *dbpb.Job) {
	allSucceeded := true
	for _, depID := range job.GetDependsOn() {
		dep, err := db.GetJob(ctx, &dbpb.GetJobRequest{Id: depID})
		if err != nil {
			s.logger.Warn("scheduler: dependency resolver get dependency", "job", job.GetId(), "depends_on", depID, "err", err)
			// Try again next tick rather than guessing at the dependency's
			// state.
			return
		}
		switch dep.GetStatus() {
		case dbpb.JobStatus_JOB_STATUS_SUCCEEDED:
			continue
		case dbpb.JobStatus_JOB_STATUS_FAILED, dbpb.JobStatus_JOB_STATUS_TIMED_OUT:
			// A dependency that failed (or timed out) blocks the job — unless it
			// has ignore_failed set (F-06), in which case its failure does not
			// propagate and the dependency counts as satisfied.
			if dep.GetIgnoreFailed() {
				continue
			}
			// A dependency reached a terminal state other than succeeded: the
			// job never runs (F-06). This short-circuits the remaining
			// dependencies — any one of them failing is enough to skip.
			s.skipJob(ctx, db, api, job)
			return
		case dbpb.JobStatus_JOB_STATUS_CANCELLED, dbpb.JobStatus_JOB_STATUS_SKIPPED:
			// A cancelled or skipped dependency always blocks the job (skip
			// propagates; a cancellation is not something ignore_failed can
			// override): the job never runs (F-06).
			s.skipJob(ctx, db, api, job)
			return
		default:
			// Still pending or running: wait and check again next tick.
			allSucceeded = false
		}
	}
	if !allSucceeded {
		return
	}
	s.releaseJob(ctx, db, api, job)
}

// skipJob conditionally marks job as skipped (Database.SkipJob) and, if it
// was skipped, fans the status change out to the UI. It is a no-op if the job
// already started (or finished) between the list and the skip.
func (s *Server) skipJob(ctx context.Context, db jobDependencyChecker, api jobRelayer, job *dbpb.Job) {
	resp, err := db.SkipJob(ctx, &dbpb.SkipJobRequest{Id: job.GetId()})
	if err != nil {
		s.logger.Warn("scheduler: skip job", "job", job.GetId(), "err", err)
		return
	}
	if !resp.GetSkipped() {
		// The job already started (or finished); nothing to do.
		return
	}
	s.logger.Info("scheduler: job skipped (a dependency did not succeed)", "job", job.GetId(), "depends_on", job.GetDependsOn())
	if api == nil {
		return
	}
	if _, err := api.NotifyJobStatus(ctx, &apipb.NotifyJobStatusRequest{
		JobId:       job.GetId(),
		Status:      dbpb.JobStatus_JOB_STATUS_SKIPPED,
		Attempt:     job.GetAttempt(),
		MaxAttempts: job.GetMaxAttempts(),
	}); err != nil {
		s.logger.Warn("scheduler: notify api of skipped job", "job", job.GetId(), "err", err)
	}
}

// releaseJob clears job's DependsOn (so it is not reconsidered on the next
// tick) and dispatches it exactly like SubmitJob would have, had the job
// never had dependencies. Clearing DependsOn first is best-effort: if it
// fails the resolver simply reconsiders the job again next tick instead of
// risking a job that is dispatched more than once.
func (s *Server) releaseJob(ctx context.Context, db jobDependencyChecker, api jobRelayer, job *dbpb.Job) {
	if _, err := db.UpdateJob(ctx, &dbpb.UpdateJobRequest{Id: job.GetId(), ClearDependsOn: true}); err != nil {
		s.logger.Warn("scheduler: clear job dependencies", "job", job.GetId(), "err", err)
		return
	}
	s.logger.Info("scheduler: job dependencies satisfied", "job", job.GetId())
	if api == nil {
		return
	}
	if job.GetTargetGroup() == "" {
		s.logger.Info("scheduler: job queued for ephemeral agent", "job", job.GetId())
		return
	}
	if _, err := api.DispatchJob(ctx, &apipb.DispatchJobRequest{Job: toAPIJob(toProtoJob(job))}); err != nil {
		s.logger.Warn("scheduler: dispatch to api failed; job left pending",
			"job", job.GetId(), "group", job.GetTargetGroup(), "err", err)
	}
}
