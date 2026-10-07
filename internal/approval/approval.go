// Package approval provides the API-backed implementation of the executor's
// Approval interface: it lets a step handler pause a job at an approval gate
// (F-13) by reporting the job as awaiting approval (with a message shown to
// the user) and polling the API for the gate's decision (approved or rejected).
//
// Both the long-lived worker and the ephemeral agent use it: each sets it in
// the executor's context (via executor.ContextWithApproval) before running a
// job's spec, so a step handler (the built-in "approval" handler) can pause the
// job at a gate. It is the single shared implementation, so the worker and
// agent do not each carry a copy.
package approval

import (
	"context"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/grpcutil"
)

// Gate is the API-backed implementation of executor.Approval. It reports the
// job as awaiting approval (via the API's ReportJobStatus) and polls the gate's
// decision (via the API's CheckApproval), presenting the job's own token on
// both calls.
type Gate struct {
	api   apipb.APIClient
	jobID int64
	token string
}

// New builds a Gate for job, presenting token on its API calls.
func New(api apipb.APIClient, jobID int64, token string) *Gate {
	return &Gate{api: api, jobID: jobID, token: token}
}

// ReportAwaitingApproval implements executor.Approval: it reports the job as
// awaiting approval (F-13), carrying the message shown to the user. The API
// persists the status and message on the job so the UI can show the gate.
func (g *Gate) ReportAwaitingApproval(ctx context.Context, message string) error {
	_, err := g.api.ReportJobStatus(grpcutil.WithBearerToken(ctx, g.token), &apipb.ReportJobStatusRequest{
		JobId:           g.jobID,
		Status:          dbpb.JobStatus_JOB_STATUS_AWAITING_APPROVAL,
		ApprovalMessage: message,
	})
	return err
}

// CheckApproval implements executor.Approval: it asks the API for the current
// state of the job's approval gate (F-13) — whether it has been resolved
// (approved or rejected) or whether the job has been cancelled.
func (g *Gate) CheckApproval(ctx context.Context) (executor.CheckApprovalResult, error) {
	resp, err := g.api.CheckApproval(grpcutil.WithBearerToken(ctx, g.token), &apipb.CheckApprovalRequest{
		JobId: g.jobID,
	})
	if err != nil {
		return executor.CheckApprovalResult{}, err
	}
	return executor.CheckApprovalResult{
		Resolved:  resp.GetResolved(),
		Decision:  resp.GetDecision(),
		Cancelled: resp.GetCancelled(),
	}, nil
}
