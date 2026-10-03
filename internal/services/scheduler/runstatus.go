// Package scheduler — run-status loop (F-07).
//
// A pipeline run's overall status is derived from the statuses of its job
// instances. The execution targets report each job's status to the API, which
// persists it through the Database service; the run-status loop closes the
// loop by periodically re-deriving each in-flight run's status from its job
// instances and persisting the change (Database.UpdateRun) and fanning it out
// to the UI (API.NotifyRunStatus).
//
// Derivation: a run is succeeded only if every non-skipped job succeeded; it
// is failed if any job failed or timed out, cancelled if any job was
// cancelled, and running while any job is still pending or running. A run
// with no job instances (a pipeline with no job definitions) is succeeded.
package scheduler

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// runStatusInterval is how often the run-status loop re-derives in-flight
// runs' statuses. It is a variable (not a constant) so tests can shorten it.
var runStatusInterval = 5 * time.Second

// runStatusDB is the subset of the Database client the run-status loop uses
// to list in-flight runs, read a run's job instances, and persist a run's
// derived status. The concrete dbpb.DatabaseClient satisfies it; tests inject
// a fake.
type runStatusDB interface {
	ListRuns(ctx context.Context, in *dbpb.ListRunsRequest, opts ...grpc.CallOption) (*dbpb.ListRunsResponse, error)
	ListJobs(ctx context.Context, in *dbpb.ListJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error)
	GetRun(ctx context.Context, in *dbpb.GetRunRequest, opts ...grpc.CallOption) (*dbpb.PipelineRun, error)
	UpdateRun(ctx context.Context, in *dbpb.UpdateRunRequest, opts ...grpc.CallOption) (*dbpb.PipelineRun, error)
}

// runStatusAPI is the subset of the API client the run-status loop uses to
// fan a run's status change out to the UI. The concrete apipb.APIClient
// satisfies it; tests inject a fake.
type runStatusAPI interface {
	NotifyRunStatus(ctx context.Context, in *apipb.NotifyRunStatusRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

// StartRunStatusLoop runs the pipeline-run status loop in the background
// until ctx is cancelled. It periodically re-derives each in-flight run's
// status from its job instances (F-07). It is a no-op when the Database
// client is nil (e.g. in tests that do not exercise the loop).
func (s *Server) StartRunStatusLoop(ctx context.Context) {
	if s.db == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(runStatusInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.processRunStatuses(ctx, s.db, s.api)
			}
		}
	}()
}

// processRunStatuses re-derives the status of every in-flight run (pending or
// running) from its job instances and, when the derived status differs from
// the stored one, persists it and fans it out to the UI. db and api are the
// loop's dependencies (the concrete clients in production, fakes in tests);
// api may be nil to skip the UI notification.
func (s *Server) processRunStatuses(ctx context.Context, db runStatusDB, api runStatusAPI) {
	runs, err := s.inFlightRuns(ctx, db)
	if err != nil {
		s.logger.Warn("scheduler: run-status list runs", "err", err)
		return
	}
	for _, run := range runs {
		s.processRun(ctx, db, api, run)
	}
}

// inFlightRuns lists the runs that are not yet in a terminal state (pending
// or running). The filter is applied in the database so the loop never pulls
// every historical run over the wire as the system grows.
func (s *Server) inFlightRuns(ctx context.Context, db runStatusDB) ([]*dbpb.PipelineRun, error) {
	var runs []*dbpb.PipelineRun
	for _, status := range []dbpb.RunStatus{dbpb.RunStatus_RUN_STATUS_PENDING, dbpb.RunStatus_RUN_STATUS_RUNNING} {
		response, err := db.ListRuns(ctx, &dbpb.ListRunsRequest{Status: status})
		if err != nil {
			return nil, err
		}
		runs = append(runs, response.GetRuns()...)
	}
	return runs, nil
}

// processRun re-derives a single run's status from its job instances and, if
// it changed, persists the change (recording the run's start/finish times on
// the relevant transitions) and fans it out to the UI.
func (s *Server) processRun(ctx context.Context, db runStatusDB, api runStatusAPI, run *dbpb.PipelineRun) {
	jobsResp, err := db.ListJobs(ctx, &dbpb.ListJobsRequest{RunId: run.GetId()})
	if err != nil {
		s.logger.Warn("scheduler: run-status list jobs", "run", run.GetId(), "err", err)
		return
	}
	derived := deriveRunStatus(jobsResp.GetJobs())
	if derived == runStatusFromProto(run.GetStatus()) {
		// The run's status is already up to date; nothing to do.
		return
	}
	// The derived status differs from the stored one: persist it. Record the
	// start time when the run first leaves pending and the finish time when it
	// first reaches a terminal state.
	now := timestamppb.Now()
	update := &dbpb.UpdateRunRequest{Id: run.GetId(), Status: runStatusToProto(derived)}
	if derived != models.RunStatusPending && run.GetStartedAt() == nil {
		update.StartedAt = now
	}
	if isTerminalRunStatus(derived) && run.GetFinishedAt() == nil {
		update.FinishedAt = now
	}
	if _, err := db.UpdateRun(ctx, update); err != nil {
		s.logger.Warn("scheduler: run-status update run", "run", run.GetId(), "err", err)
		return
	}
	s.logger.Info("scheduler: run status changed", "run", run.GetId(), "status", derived)
	if api == nil {
		return
	}
	if _, err := api.NotifyRunStatus(ctx, &apipb.NotifyRunStatusRequest{
		RunId:  run.GetId(),
		Status: runStatusToProto(derived),
	}); err != nil {
		s.logger.Warn("scheduler: notify api of run status", "run", run.GetId(), "err", err)
	}
}

// deriveRunStatus derives a run's overall status from the statuses of its job
// instances (F-07):
//
//   - a run with no job instances is succeeded (a pipeline with no jobs);
//   - failed if any job failed or timed out;
//   - cancelled if any job was cancelled (and none failed/timed out);
//   - running while any job is still pending or running;
//   - succeeded otherwise (every job is succeeded or skipped, i.e. every
//     non-skipped job succeeded).
func deriveRunStatus(jobs []*dbpb.Job) models.RunStatus {
	if len(jobs) == 0 {
		return models.RunStatusSucceeded
	}
	anyCancelled := false
	anyInFlight := false
	for _, job := range jobs {
		switch job.GetStatus() {
		case dbpb.JobStatus_JOB_STATUS_FAILED, dbpb.JobStatus_JOB_STATUS_TIMED_OUT:
			return models.RunStatusFailed
		case dbpb.JobStatus_JOB_STATUS_CANCELLED:
			anyCancelled = true
		case dbpb.JobStatus_JOB_STATUS_PENDING, dbpb.JobStatus_JOB_STATUS_RUNNING:
			anyInFlight = true
			// SUCCEEDED and SKIPPED are terminal and do not, on their own, fail
			// the run.
		}
	}
	if anyCancelled {
		return models.RunStatusCancelled
	}
	if anyInFlight {
		return models.RunStatusRunning
	}
	return models.RunStatusSucceeded
}

// isTerminalRunStatus reports whether a run status is terminal (the run will
// not change again): succeeded, failed, or cancelled.
func isTerminalRunStatus(status models.RunStatus) bool {
	switch status {
	case models.RunStatusSucceeded, models.RunStatusFailed, models.RunStatusCancelled:
		return true
	default:
		return false
	}
}

// runStatusFromProto converts a proto RunStatus to the model's RunStatus.
func runStatusFromProto(status dbpb.RunStatus) models.RunStatus {
	switch status {
	case dbpb.RunStatus_RUN_STATUS_PENDING:
		return models.RunStatusPending
	case dbpb.RunStatus_RUN_STATUS_RUNNING:
		return models.RunStatusRunning
	case dbpb.RunStatus_RUN_STATUS_SUCCEEDED:
		return models.RunStatusSucceeded
	case dbpb.RunStatus_RUN_STATUS_FAILED:
		return models.RunStatusFailed
	case dbpb.RunStatus_RUN_STATUS_CANCELLED:
		return models.RunStatusCancelled
	default:
		return models.RunStatusPending
	}
}

// runStatusToProto converts the model's RunStatus to a proto RunStatus.
func runStatusToProto(status models.RunStatus) dbpb.RunStatus {
	switch status {
	case models.RunStatusPending:
		return dbpb.RunStatus_RUN_STATUS_PENDING
	case models.RunStatusRunning:
		return dbpb.RunStatus_RUN_STATUS_RUNNING
	case models.RunStatusSucceeded:
		return dbpb.RunStatus_RUN_STATUS_SUCCEEDED
	case models.RunStatusFailed:
		return dbpb.RunStatus_RUN_STATUS_FAILED
	case models.RunStatusCancelled:
		return dbpb.RunStatus_RUN_STATUS_CANCELLED
	default:
		return dbpb.RunStatus_RUN_STATUS_UNSPECIFIED
	}
}
