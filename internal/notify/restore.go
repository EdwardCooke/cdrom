// Package notify — the API-backed Restorer (F-22).
//
// A rollback step handler (the built-in "rollback" handler) renders its
// params and asks the API to SPAWN A RESUMPTION of the job's own pipeline:
// the API validates the presenter's job token (establishing the caller's
// run → pipeline), applies the server-side guards (containment to the
// caller's own pipeline, no re-arm for rollback-origin callers, cooldown),
// finds the anchor (the latest successful, non-rollback-origin run of the
// caller's pipeline), and asks the scheduler to restart from the requested
// cut point. The target never chooses the anchor. While the step waits on
// the resumption's verdict it polls the API's GetRun RPC.
//
// The long-lived worker and the ephemeral agent both use this: the shared
// target path (internal/target) sets it in the executor's context (via
// executor.ContextWithRestorer) before running a job's spec. It is the
// single shared implementation, so the worker and agent do not each carry a
// copy.
package notify

import (
	"context"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
)

// Restorer is the API-backed implementation of executor.Restorer. It calls
// the API's InitiateRestore / GetRun RPCs, presenting the job's own token.
type Restorer struct {
	api   apipb.APIClient
	jobID int64
	token string
}

// NewRestorer builds a Restorer for job, presenting token on its API calls.
func NewRestorer(api apipb.APIClient, jobID int64, token string) *Restorer {
	return &Restorer{api: api, jobID: jobID, token: token}
}

// InitiateRestore implements executor.Restorer: it asks the API to spawn a
// resumption of this job's own pipeline, cutting at fromJob (a stable job
// key, F-08). It returns the new resumption run's id.
func (r *Restorer) InitiateRestore(ctx context.Context, fromJob string) (int64, error) {
	resp, err := r.api.InitiateRestore(grpcutil.WithBearerToken(ctx, r.token), &apipb.InitiateRestoreRequest{
		JobId:   r.jobID,
		FromJob: fromJob,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetRunId(), nil
}

// RunStatus implements executor.Restorer: it reports a run's current status
// (the polling aid for a rollback step that waits on its resumption's
// verdict).
func (r *Restorer) RunStatus(ctx context.Context, runID int64) (string, error) {
	resp, err := r.api.GetRun(grpcutil.WithBearerToken(ctx, r.token), &apipb.GetRunRequest{
		RunId: runID,
		JobId: r.jobID,
	})
	if err != nil {
		return "", err
	}
	return runStatusName(resp.GetStatus()), nil
}

// runStatusName maps a run-status enum to its lowercase name (matching the
// job-status names the condition context uses).
func runStatusName(st dbpb.RunStatus) string {
	switch st {
	case dbpb.RunStatus_RUN_STATUS_PENDING:
		return "pending"
	case dbpb.RunStatus_RUN_STATUS_RUNNING:
		return "running"
	case dbpb.RunStatus_RUN_STATUS_SUCCEEDED:
		return "succeeded"
	case dbpb.RunStatus_RUN_STATUS_FAILED:
		return "failed"
	case dbpb.RunStatus_RUN_STATUS_CANCELLED:
		return "cancelled"
	default:
		return "unknown"
	}
}
