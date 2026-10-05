// Package scheduler — job-status loop (fan-out).
//
// A job that targets a worker group runs on every worker in the group (fan-out):
// each worker's run is a separate JobExecution, and the worker reports its
// outcome to its own execution (not the job row). The job row's status is the
// *derived* overall status, maintained by this loop: it periodically re-derives
// each in-flight group job's status from the per-worker outcomes of the workers
// that are alive, per the job's failure mode, and persists the change
// (Database.UpdateJob) and appends a "job_status" event to the shared event log
// (F-23), which every API pod tails and fans out to its UI clients.
//
// A worker that is not alive (it has not heartbeated within the liveness
// window) is dropped from the derivation: its in-progress execution does not
// block the job. This is what lets a job complete when a worker in the group
// dies mid-run, and it is the job-level counterpart to the step barrier's
// "barrier only among workers alive at step start" rule.
package scheduler

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// jobStatusInterval is how often the job-status loop re-derives in-flight
// group jobs' overall status from their per-worker executions. It is a
// variable (not a constant) so tests can shorten it.
var jobStatusInterval = 5 * time.Second

// workerAliveThreshold is how recent a worker's last_seen_at must be for it to
// be considered alive. A worker that has not heartbeated within this window is
// considered dead and is dropped from a job's fan-out derivation (its
// in-progress execution does not block the job). The worker heartbeats every
// 10s, so the threshold is a few missed heartbeats. It is a variable (not a
// constant) so tests can shorten it.
var workerAliveThreshold = 30 * time.Second

// jobStatusDB is the subset of the Database client the job-status loop uses to
// list in-flight group jobs, read a job's executions and the group's workers,
// and persist a job's derived status. The concrete dbpb.DatabaseClient
// satisfies it; tests inject a fake.
type jobStatusDB interface {
	ListJobs(ctx context.Context, in *dbpb.ListJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error)
	ListJobExecutions(ctx context.Context, in *dbpb.ListJobExecutionsRequest, opts ...grpc.CallOption) (*dbpb.ListJobExecutionsResponse, error)
	ListWorkers(ctx context.Context, in *dbpb.ListWorkersRequest, opts ...grpc.CallOption) (*dbpb.ListWorkersResponse, error)
	UpdateJob(ctx context.Context, in *dbpb.UpdateJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error)
}

// jobStatusEventPublisher is the subset of the Database client the job-status
// loop uses to append a "job_status" event to the shared event log (F-23) so
// every API pod can fan the job's status change out to its UI clients. The
// concrete dbpb.DatabaseClient satisfies it; tests inject a fake.
type jobStatusEventPublisher interface {
	PublishJobStatus(ctx context.Context, in *dbpb.PublishJobStatusRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error)
}

// StartJobStatusLoop runs the job-status loop in the background until ctx is
// cancelled. It periodically re-derives each in-flight group job's overall
// status from the per-worker outcomes of the workers that are alive, per the
// job's failure mode. It is a no-op when the Database client is nil (e.g. in
// tests that do not exercise the loop).
func (s *Server) StartJobStatusLoop(ctx context.Context) {
	if s.db == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(jobStatusInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.processJobStatuses(ctx, s.db, s.db)
			}
		}
	}()
}

// processJobStatuses re-derives the overall status of every in-flight group
// job (a running job that targets a worker group) from its per-worker
// executions and, when the derived status differs from the stored one,
// persists it and appends a "job_status" event to the shared event log (F-23).
// Jobs with an empty target group (ephemeral agents) report their status
// directly to the job row and are not derived here. db and publisher are the
// loop's dependencies (the concrete clients in production, fakes in tests);
// publisher may be nil to skip the event append.
func (s *Server) processJobStatuses(ctx context.Context, db jobStatusDB, publisher jobStatusEventPublisher) {
	response, err := db.ListJobs(ctx, &dbpb.ListJobsRequest{Status: dbpb.JobStatus_JOB_STATUS_RUNNING})
	if err != nil {
		s.logger.Warn("scheduler: job-status list running jobs", "err", err)
		return
	}
	for _, job := range response.GetJobs() {
		if job.GetTargetGroup() == "" {
			// An agent job reports its status directly to the job row.
			continue
		}
		s.processJob(ctx, db, publisher, job)
	}
}

// processJob re-derives a single group job's overall status from the
// per-worker outcomes of the workers that are alive and, if it changed,
// persists the change (recording the finish time when the job reaches a
// terminal status) and appends a "job_status" event to the shared event log
// (F-23).
func (s *Server) processJob(ctx context.Context, db jobStatusDB, publisher jobStatusEventPublisher, job *dbpb.Job) {
	executionsResp, err := db.ListJobExecutions(ctx, &dbpb.ListJobExecutionsRequest{JobId: job.GetId()})
	if err != nil {
		s.logger.Warn("scheduler: job-status list executions", "job", job.GetId(), "err", err)
		return
	}
	workersResp, err := db.ListWorkers(ctx, &dbpb.ListWorkersRequest{Group: job.GetTargetGroup()})
	if err != nil {
		s.logger.Warn("scheduler: job-status list workers", "job", job.GetId(), "group", job.GetTargetGroup(), "err", err)
		return
	}
	alive := aliveWorkers(workersResp.GetWorkers())
	// Only the most recent execution per alive worker is considered: a worker
	// that restarted and re-ran the job supersedes its abandoned (pre-restart)
	// execution, and a worker that is not alive is dropped entirely (its
	// in-progress execution does not block the job).
	latest := latestExecutionPerWorker(executionsResp.GetExecutions(), alive)
	if len(latest) == 0 {
		// No alive worker has an execution for this job (e.g. every worker in
		// the group is down). Leave the job running; the watchdog reaps it if
		// it has a timeout.
		return
	}
	derived := deriveJobStatus(latest, failureModeFromProto(job.GetFailureMode()))
	if derived == jobStatusFromProto(job.GetStatus()) {
		// The job's status is already up to date; nothing to do.
		return
	}
	// The derived status differs from the stored one: persist it. Record the
	// finish time when the job first reaches a terminal status.
	update := &dbpb.UpdateJobRequest{Id: job.GetId(), Status: jobStatusToProto(derived)}
	if isTerminalJobStatus(derived) && job.GetFinishedAt() == nil {
		update.FinishedAt = timestamppb.Now()
	}
	if _, err := db.UpdateJob(ctx, update); err != nil {
		s.logger.Warn("scheduler: job-status update job", "job", job.GetId(), "err", err)
		return
	}
	s.logger.Info("scheduler: job status derived", "job", job.GetId(), "status", derived, "failure_mode", job.GetFailureMode())
	if publisher == nil {
		return
	}
	if _, err := publisher.PublishJobStatus(ctx, &dbpb.PublishJobStatusRequest{
		JobId:       job.GetId(),
		Status:      jobStatusToProto(derived),
		Attempt:     job.GetAttempt(),
		MaxAttempts: job.GetMaxAttempts(),
	}); err != nil {
		s.logger.Warn("scheduler: publish job status", "job", job.GetId(), "err", err)
	}
}

// aliveWorkers returns the set of worker names in the group that are alive: a
// worker is alive when its last_seen_at is within workerAliveThreshold (it has
// heartbeated recently). A worker with no last_seen_at, or one that has gone
// quiet beyond the threshold, is not alive.
func aliveWorkers(workers []*dbpb.Worker) map[string]bool {
	now := time.Now()
	alive := make(map[string]bool, len(workers))
	for _, worker := range workers {
		lastSeen := worker.GetLastSeenAt()
		if lastSeen == nil {
			continue
		}
		if now.Sub(lastSeen.AsTime()) <= workerAliveThreshold {
			alive[worker.GetName()] = true
		}
	}
	return alive
}

// latestExecutionPerWorker returns, for each alive worker that has an
// execution, the worker's most recent execution (highest id). Executions of
// workers that are not alive are dropped (a dead worker's in-progress
// execution does not block the job).
func latestExecutionPerWorker(executions []*dbpb.JobExecution, alive map[string]bool) []*dbpb.JobExecution {
	best := make(map[string]*dbpb.JobExecution)
	for _, execution := range executions {
		if !alive[execution.GetWorkerName()] {
			continue
		}
		if current, ok := best[execution.GetWorkerName()]; !ok || execution.GetId() > current.GetId() {
			best[execution.GetWorkerName()] = execution
		}
	}
	out := make([]*dbpb.JobExecution, 0, len(best))
	for _, execution := range best {
		out = append(out, execution)
	}
	return out
}

// deriveJobStatus derives a group job's overall status from the per-worker
// outcomes (the most recent execution per alive worker) per the job's failure
// mode:
//
//   - ALL: failed if any worker's run failed or timed out; running while any
//     is still running; succeeded only if every worker's run succeeded.
//   - BEST_EFFORT: running while any worker's run is still running; succeeded
//     once every worker's run reaches a terminal state (a per-worker failure
//     or timeout is recorded but does not fail the job).
//   - ANY: succeeded as soon as one worker's run succeeds; running while none
//     has succeeded and some are still running; failed only if every worker's
//     run failed or timed out.
func deriveJobStatus(executions []*dbpb.JobExecution, mode models.FailureMode) models.JobStatus {
	anySucceeded := false
	anyFailed := false
	anyTimedOut := false
	anyRunning := false
	for _, execution := range executions {
		switch execution.GetStatus() {
		case dbpb.JobStatus_JOB_STATUS_SUCCEEDED:
			anySucceeded = true
		case dbpb.JobStatus_JOB_STATUS_FAILED:
			anyFailed = true
		case dbpb.JobStatus_JOB_STATUS_TIMED_OUT:
			anyTimedOut = true
		case dbpb.JobStatus_JOB_STATUS_PENDING, dbpb.JobStatus_JOB_STATUS_RUNNING:
			anyRunning = true
		}
	}
	switch mode {
	case models.FailureModeBestEffort:
		if anyRunning {
			return models.JobStatusRunning
		}
		return models.JobStatusSucceeded
	case models.FailureModeAny:
		if anySucceeded {
			return models.JobStatusSucceeded
		}
		if anyRunning {
			return models.JobStatusRunning
		}
		// No worker succeeded and none is running: the job is a failure. A
		// concrete failure takes precedence over a timeout; a job that only
		// timed out is reported as timed_out (distinct from failed) so the UI
		// can tell a hung job apart from one that ran and errored.
		if anyFailed {
			return models.JobStatusFailed
		}
		if anyTimedOut {
			return models.JobStatusTimedOut
		}
		return models.JobStatusFailed
	default: // FailureModeAll (and the empty/unset default)
		if anyFailed {
			return models.JobStatusFailed
		}
		if anyTimedOut {
			return models.JobStatusTimedOut
		}
		if anyRunning {
			return models.JobStatusRunning
		}
		return models.JobStatusSucceeded
	}
}

// isTerminalJobStatus reports whether a job status is terminal (the job will
// not change again): succeeded, failed, cancelled, timed_out, or skipped.
func isTerminalJobStatus(status models.JobStatus) bool {
	switch status {
	case models.JobStatusSucceeded, models.JobStatusFailed, models.JobStatusCancelled, models.JobStatusTimedOut, models.JobStatusSkipped:
		return true
	default:
		return false
	}
}

// jobStatusFromProto converts a proto JobStatus to the model's JobStatus.
func jobStatusFromProto(status dbpb.JobStatus) models.JobStatus {
	switch status {
	case dbpb.JobStatus_JOB_STATUS_PENDING:
		return models.JobStatusPending
	case dbpb.JobStatus_JOB_STATUS_RUNNING:
		return models.JobStatusRunning
	case dbpb.JobStatus_JOB_STATUS_SUCCEEDED:
		return models.JobStatusSucceeded
	case dbpb.JobStatus_JOB_STATUS_FAILED:
		return models.JobStatusFailed
	case dbpb.JobStatus_JOB_STATUS_CANCELLED:
		return models.JobStatusCancelled
	case dbpb.JobStatus_JOB_STATUS_TIMED_OUT:
		return models.JobStatusTimedOut
	case dbpb.JobStatus_JOB_STATUS_SKIPPED:
		return models.JobStatusSkipped
	default:
		return ""
	}
}

// jobStatusToProto converts the model's JobStatus to a proto JobStatus.
func jobStatusToProto(status models.JobStatus) dbpb.JobStatus {
	switch status {
	case models.JobStatusPending:
		return dbpb.JobStatus_JOB_STATUS_PENDING
	case models.JobStatusRunning:
		return dbpb.JobStatus_JOB_STATUS_RUNNING
	case models.JobStatusSucceeded:
		return dbpb.JobStatus_JOB_STATUS_SUCCEEDED
	case models.JobStatusFailed:
		return dbpb.JobStatus_JOB_STATUS_FAILED
	case models.JobStatusCancelled:
		return dbpb.JobStatus_JOB_STATUS_CANCELLED
	case models.JobStatusTimedOut:
		return dbpb.JobStatus_JOB_STATUS_TIMED_OUT
	case models.JobStatusSkipped:
		return dbpb.JobStatus_JOB_STATUS_SKIPPED
	default:
		return dbpb.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

// failureModeFromProto converts a proto FailureMode to the model's
// FailureMode. UNSPECIFIED yields FailureModeAll (the default).
func failureModeFromProto(mode dbpb.FailureMode) models.FailureMode {
	switch mode {
	case dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT:
		return models.FailureModeBestEffort
	case dbpb.FailureMode_FAILURE_MODE_ANY:
		return models.FailureModeAny
	case dbpb.FailureMode_FAILURE_MODE_ALL:
		return models.FailureModeAll
	default:
		return models.FailureModeAll
	}
}
