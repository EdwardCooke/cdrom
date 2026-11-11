package target

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/grpcutil"
)

// stepBarrierPollInterval is how often a target waiting at a step barrier
// re-checks the barrier (F-23-style polling as the authoritative catch-up
// path). It is a variable (not a constant) so tests can shorten the interval.
var stepBarrierPollInterval = 1 * time.Second

// stepBarrier is the TARGET's implementation of executor.StepBarrier (F-30.4):
// it synchronizes the participants of a job that fans out to an agent group at
// each step boundary. Participants are ACCOUNTED BY AGENT NAME (the API's
// barrier participant set is keyed on the agent that started the execution),
// so BOTH the agent and its supervised worker participate under the SAME name
// (the worker learns it from its job token's agent_name claim). After a
// participant completes a step it reports the completion to the API (which
// records a StepCompletion row for the job's current attempt) and then polls
// the API's CheckStepBarrier until every participant alive at the step's
// start has completed it (satisfied), the job is cancelled, or the job's
// context is done. A dead participant is dropped from the barrier (it is no
// longer alive), so a dead participant cannot wedge the job.
type stepBarrier struct {
	api         apipb.APIClient
	participant string // the AGENT NAME participation is accounted under
	jobID       int64
	token       string
	logger      *slog.Logger
	pollEvery   time.Duration
}

// NewStepBarrier builds a step barrier for job, presenting token on its API
// calls and participating UNDER participant (the agent's registered name —
// the worker decodes it from its job token's agent_name claim, F-30.4).
// pollEvery is the barrier's polling interval (stepBarrierPollInterval when
// zero).
func NewStepBarrier(api apipb.APIClient, participant string, jobID int64, token string, logger *slog.Logger, pollEvery time.Duration) *stepBarrier {
	if pollEvery <= 0 {
		pollEvery = stepBarrierPollInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &stepBarrier{
		api:         api,
		participant: participant,
		jobID:       jobID,
		token:       token,
		logger:      logger,
		pollEvery:   pollEvery,
	}
}

// SyncStep implements executor.StepBarrier. It records that this participant
// completed stepName and then blocks until the barrier for that step is
// satisfied (every participant alive at the step's start has completed it),
// the job is cancelled (it returns executor.ErrCancelled, wrapped with the
// context), or ctx is done (it returns ctx.Err()).
func (b *stepBarrier) SyncStep(ctx context.Context, stepName string) error {
	// Report this participant's completion of the step. A transient failure to
	// report is not fatal: the barrier is still satisfied by the other
	// participants' completions, and this participant's own completion is
	// re-checked on the next poll (the API's CheckStepBarrier reads the
	// recorded rows).
	if err := b.reportCompletion(ctx, stepName); err != nil {
		b.logger.Warn("target: report step completion", "job", b.jobID, "step", stepName, "err", err)
	}
	// Poll the barrier until it is satisfied, the job is cancelled, or the
	// job's context is done.
	ticker := time.NewTicker(b.pollEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		resp, err := b.checkBarrier(ctx, stepName)
		if err != nil {
			// A transient API failure does not wedge the job: keep polling so
			// the barrier is re-checked once the API recovers.
			b.logger.Warn("target: check step barrier", "job", b.jobID, "step", stepName, "err", err)
			continue
		}
		if resp.GetCancelled() {
			// The job was cancelled while this participant waited at the
			// barrier: surface it as a cancellation so the target reports the
			// job cancelled (distinct from a failure).
			return fmt.Errorf("%w: job %d cancelled while waiting at step %q barrier", executor.ErrCancelled, b.jobID, stepName)
		}
		if resp.GetSatisfied() {
			return nil
		}
	}
}

// reportCompletion tells the API that this participant completed stepName of
// the job, presenting the job token. Participation is accounted under the
// PARTICIPANT'S NAME (the agent's registered name, F-30.4) — the worker
// participates under the same name the agent's execution was recorded with.
func (b *stepBarrier) reportCompletion(ctx context.Context, stepName string) error {
	_, err := b.api.ReportStepCompletion(grpcutil.WithBearerToken(ctx, b.token), &apipb.ReportStepCompletionRequest{
		JobId:     b.jobID,
		AgentName: b.participant,
		StepName:  stepName,
	})
	return err
}

// checkBarrier asks the API whether the barrier for stepName is satisfied or
// the job has been cancelled, presenting the job token.
func (b *stepBarrier) checkBarrier(ctx context.Context, stepName string) (*apipb.CheckStepBarrierResponse, error) {
	return b.api.CheckStepBarrier(grpcutil.WithBearerToken(ctx, b.token), &apipb.CheckStepBarrierRequest{
		JobId:    b.jobID,
		StepName: stepName,
	})
}
