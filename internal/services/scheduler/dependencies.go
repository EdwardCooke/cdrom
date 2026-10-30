// Package scheduler — dependency resolver (F-06).
//
// A job may declare DependsOn (a list of job ids, F-06): it is held pending
// (never dispatched by SubmitJob) until every dependency reaches a terminal
// state. The resolver closes that loop: it periodically lists pending jobs,
// and for each with unresolved dependencies checks each dependency's status:
//
//   - every dependency succeeded -> the job is released: its DependsOn is
//     cleared (so it is not reconsidered) and an "assignment" event is
//     appended to the shared event log (F-23), exactly like a job with no
//     dependencies;
//   - any dependency reached a terminal state other than succeeded (failed,
//     cancelled, timed_out, or itself skipped) -> the job is marked skipped
//     (Database.SkipJob) instead of running, and a "job_status" event is
//     appended to the shared event log (F-23) so the UI sees the skip;
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

// dependencyPublisher is the subset of the Database client the dependency
// resolver uses to append events to the shared event log (F-23): a
// "job_status" event for a skipped job and an "assignment" event for a
// released job. The concrete dbpb.DatabaseClient satisfies it; tests inject a
// fake.
type dependencyPublisher interface {
	PublishJobStatus(ctx context.Context, in *dbpb.PublishJobStatusRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error)
	PublishAssignment(ctx context.Context, in *dbpb.PublishAssignmentRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error)
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
				s.observeLoopTick("dependencies", func() {
					s.resolveJobDependencies(ctx, s.db, s.db)
				})
			}
		}
	}()
}

// resolveJobDependencies lists pending jobs and resolves the dependencies of
// each one that declares any (DependsOn). db and publisher are the resolver's
// dependencies (the concrete clients in production, fakes in tests); publisher
// may be nil to skip the event appends.
func (s *Server) resolveJobDependencies(ctx context.Context, db jobDependencyChecker, publisher dependencyPublisher) {
	response, err := db.ListJobs(ctx, &dbpb.ListJobsRequest{Status: dbpb.JobStatus_JOB_STATUS_PENDING})
	if err != nil {
		s.logger.Warn("scheduler: dependency resolver list pending jobs", "err", err)
		return
	}
	for _, job := range response.GetJobs() {
		if len(job.GetDependsOn()) == 0 {
			continue
		}
		s.resolveJob(ctx, db, publisher, job)
	}
}

// resolveJob checks every one of job's dependencies and either skips the job
// (a dependency did not succeed and the job does not branch), releases it
// (every dependency is terminal and none forbids release), or leaves it
// pending (a dependency is still pending or running).
//
// On release it captures each dependency's terminal outcome (status +
// outputs, keyed by the dependency's stable key) as a DependencyObservation
// and persists them atomically with the depends_on clearance (F-22): the
// clearance destroys the only record of WHICH jobs were depended upon, so the
// observations must accompany it. The API later hands them to the execution
// target so a branch job's step conditions can route on
// {{ .deps.<key>.status }} / {{ .deps.<key>.outputs.<name> }}. Capture is
// UNIVERSAL (every released job with dependencies), not limited to branch
// jobs — the cost is one field on the release update and the data is inert
// for non-branch jobs.
func (s *Server) resolveJob(ctx context.Context, db jobDependencyChecker, publisher dependencyPublisher, job *dbpb.Job) {
	observations := make([]*dbpb.DependencyObservation, 0, len(job.GetDependsOn()))
	hasBlocker := false
	for _, depID := range job.GetDependsOn() {
		dep, err := db.GetJob(ctx, &dbpb.GetJobRequest{Id: depID})
		if err != nil {
			s.logger.Warn("scheduler: dependency resolver get dependency", "job", job.GetId(), "depends_on", depID, "err", err)
			// Try again next tick rather than guessing at the dependency's
			// state.
			return
		}
		observations = append(observations, &dbpb.DependencyObservation{
			Key:     dep.GetKey(),
			Status:  dep.GetStatus(),
			Outputs: dep.GetOutputs(),
		})
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
			hasBlocker = true
		case dbpb.JobStatus_JOB_STATUS_CANCELLED:
			// A cancelled dependency ALWAYS blocks the job (a cancellation is
			// not something ignore_failed or branch can override — aborting a
			// lineage is not revivable): the job never runs (F-06).
			s.skipJob(ctx, db, publisher, job)
			return
		case dbpb.JobStatus_JOB_STATUS_SKIPPED:
			// A skipped dependency blocks the job (skip propagates, F-06).
			hasBlocker = true
		default:
			// Still pending or running: wait and check again next tick.
			return
		}
	}
	if hasBlocker && !job.GetBranch() {
		// A dependency reached a terminal state other than succeeded and the
		// job does not branch: the job never runs (F-06).
		s.skipJob(ctx, db, publisher, job)
		return
	}
	s.releaseJob(ctx, db, job, observations)
}

// skipJob conditionally marks job as skipped (Database.SkipJob) and, if it
// was skipped, appends a "job_status" event to the shared event log (F-23) so
// every API pod can fan the skip out to its UI clients. It is a no-op if the
// job already started (or finished) between the list and the skip.
func (s *Server) skipJob(ctx context.Context, db jobDependencyChecker, publisher dependencyPublisher, job *dbpb.Job) {
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
	if s.metrics != nil {
		s.metrics.RecordDependencyResolution("skipped")
	}
	if publisher == nil {
		return
	}
	if _, err := publisher.PublishJobStatus(ctx, &dbpb.PublishJobStatusRequest{
		JobId:       job.GetId(),
		Status:      dbpb.JobStatus_JOB_STATUS_SKIPPED,
		Attempt:     job.GetAttempt(),
		MaxAttempts: job.GetMaxAttempts(),
	}); err != nil {
		s.logger.Warn("scheduler: publish skipped job status", "job", job.GetId(), "err", err)
	}
}

// releaseJob clears job's DependsOn (so it is not reconsidered on the next
// tick) and leaves the job pending for the concurrency admission loop (F-16)
// to dispatch: the admission loop admits the job (acquiring its concurrency
// keys / group / global slots) and dispatches it once it fits within the
// limits. Clearing DependsOn first is best-effort: if it fails the resolver
// simply reconsiders the job again next tick instead of risking a job that is
// dispatched more than once. The job is not dispatched here: dispatching
// directly would bypass the admission gate, so the admission loop is the
// single dispatch path for a dependency-released job.
func (s *Server) releaseJob(ctx context.Context, db jobDependencyChecker, job *dbpb.Job, observations []*dbpb.DependencyObservation) {
	// The observations (F-22) travel with the clearance: the database
	// persists both in one selective update, so the capture and the
	// destruction of the depends_on record are atomic.
	if _, err := db.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:                     job.GetId(),
		ClearDependsOn:         true,
		DependencyObservations: observations,
	}); err != nil {
		s.logger.Warn("scheduler: clear job dependencies", "job", job.GetId(), "err", err)
		return
	}
	s.logger.Info("scheduler: job dependencies satisfied", "job", job.GetId(), "branch", job.GetBranch(), "observations", len(observations))
	if s.metrics != nil {
		s.metrics.RecordDependencyResolution("released")
	}
}
