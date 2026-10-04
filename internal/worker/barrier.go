package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cdrom/internal/executor"
	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/grpcutil"
)

// stepBarrierPollInterval is how often a worker waiting at a step barrier
// re-checks the barrier (F-23-style polling as the authoritative catch-up
// path). It is a variable (not a constant) so tests can shorten it.
var stepBarrierPollInterval = 1 * time.Second

// stepBarrier is the worker's implementation of executor.StepBarrier: it
// synchronizes the workers of a job that fans out to a worker group at each
// step boundary. After a worker completes a step it reports the completion to
// the API (which records a StepCompletion row for the job's current attempt)
// and then polls the API's CheckStepBarrier until every worker alive at the
// step's start has completed it (satisfied), the job is cancelled, or the
// job's context is done. A worker that dies mid-step is dropped from the
// barrier (it is no longer alive), so a dead worker cannot wedge the job.
type stepBarrier struct {
	api       apipb.APIClient
	worker    string
	jobID     int64
	token     string
	logger    *slog.Logger
	pollEvery time.Duration
}

// newStepBarrier builds a step barrier for job, presenting token on its API
// calls. pollEvery is the barrier's polling interval (stepBarrierPollInterval
// when zero).
func newStepBarrier(api apipb.APIClient, worker string, jobID int64, token string, logger *slog.Logger, pollEvery time.Duration) *stepBarrier {
	if pollEvery <= 0 {
		pollEvery = stepBarrierPollInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &stepBarrier{
		api:       api,
		worker:    worker,
		jobID:     jobID,
		token:     token,
		logger:    logger,
		pollEvery: pollEvery,
	}
}

// SyncStep implements executor.StepBarrier. It records that this worker
// completed stepIndex and then blocks until the barrier for that step is
// satisfied (every worker alive at the step's start has completed it), the job
// is cancelled (it returns executor.ErrCancelled, wrapped with the context),
// or ctx is done (it returns ctx.Err()).
func (b *stepBarrier) SyncStep(ctx context.Context, stepIndex int) error {
	// Report this worker's completion of the step. A transient failure to
	// report is not fatal: the barrier is still satisfied by the other
	// workers' completions, and this worker's own completion is re-checked on
	// the next poll (the API's CheckStepBarrier reads the recorded rows).
	if err := b.reportCompletion(ctx, stepIndex); err != nil {
		b.logger.Warn("worker: report step completion", "job", b.jobID, "step", stepIndex, "err", err)
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
		resp, err := b.checkBarrier(ctx, stepIndex)
		if err != nil {
			// A transient API failure does not wedge the job: keep polling so
			// the barrier is re-checked once the API recovers.
			b.logger.Warn("worker: check step barrier", "job", b.jobID, "step", stepIndex, "err", err)
			continue
		}
		if resp.GetCancelled() {
			// The job was cancelled while this worker waited at the barrier:
			// surface it as a cancellation so the worker reports the job
			// cancelled (distinct from a failure).
			return fmt.Errorf("%w: job %d cancelled while waiting at step %d barrier", executor.ErrCancelled, b.jobID, stepIndex)
		}
		if resp.GetSatisfied() {
			return nil
		}
	}
}

// reportCompletion tells the API that this worker completed stepIndex of the
// job, presenting the job token.
func (b *stepBarrier) reportCompletion(ctx context.Context, stepIndex int) error {
	_, err := b.api.ReportStepCompletion(grpcutil.WithBearerToken(ctx, b.token), &apipb.ReportStepCompletionRequest{
		JobId:      b.jobID,
		WorkerName: b.worker,
		StepIndex:  int32(stepIndex),
	})
	return err
}

// checkBarrier asks the API whether the barrier for stepIndex is satisfied or
// the job has been cancelled, presenting the job token.
func (b *stepBarrier) checkBarrier(ctx context.Context, stepIndex int) (*apipb.CheckStepBarrierResponse, error) {
	return b.api.CheckStepBarrier(grpcutil.WithBearerToken(ctx, b.token), &apipb.CheckStepBarrierRequest{
		JobId:     b.jobID,
		StepIndex: int32(stepIndex),
	})
}
