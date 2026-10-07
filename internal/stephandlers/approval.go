package stephandlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypeApproval is the step type of the built-in approval handler (F-13). A
// step of this type pauses the job at an approval gate: it reports the job as
// awaiting approval (with a message shown to the user) and then waits until an
// authorized user approves or rejects it. An approval approves the job (the
// step succeeds and the job continues to the next step); a rejection fails the
// job (the step fails and the job stops).
//
// The handler reads its settings from the step's Params map:
//   - "message" (string) — an optional message shown to the user while the job
//     is awaiting approval. It is a Go template rendered against the step's
//     condition context (its own env, the prior steps' status/outputs, the
//     upstream jobs' status/outputs, the job's identity, the run's parameters,
//     and the pipeline's named secrets as plaintext) — the same interpolation
//     a step's condition (F-06) and the run's spec fields (F-10) use. So a
//     message can reference a secret (`{{ .secrets.name }}`), a prior step's
//     output, a run parameter, or the job's identity. When empty, no message
//     is shown.
//
// The step's own Timeout bounds how long the job may wait at the gate: when it
// expires the job is reported timed_out (like any other step that exceeds its
// timeout). A job-level timeout (the spec's Timeout) also bounds the wait.
// Cancelling the job while it waits at the gate reports it cancelled.
//
// The handler obtains the approval gate from the execution target through the
// executor's Approval (set in the context by the worker or agent, which calls
// the API's ReportJobStatus to report awaiting_approval and CheckApproval to
// poll for the decision).
const TypeApproval = executor.TypeApproval

// Param key the approval handler reads from a step's Params map.
const ParamMessage = "message"

// approvalPollInterval is how often the approval handler polls the API for the
// gate's decision while the job waits. It is a variable (not a constant) so
// tests can shorten it.
var approvalPollInterval = 1 * time.Second

func init() {
	executor.RegisterStepType(TypeApproval, runApprovalStep)
}

// runApprovalStep is the built-in "approval" step handler (F-13). It pauses
// the job at an approval gate: it renders the step's message against the step's
// condition context, reports the job as awaiting approval (so the UI can show
// the gate and its message), and then polls the gate until an authorized user
// approves or rejects it (or the step's context is done). An approval succeeds
// the step (the job continues); a rejection fails the job.
func runApprovalStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	gate := executor.ApprovalFromContext(ctx)
	if gate == nil {
		return fmt.Errorf("approval is not supported by this execution target")
	}

	// Render the approval message against the step's condition context (F-13):
	// the same data a step's condition (F-06) and the run's spec fields (F-10)
	// are rendered against (the step's env, the prior steps' status/outputs,
	// the upstream jobs' status/outputs, the job's identity, the run's
	// parameters, and the pipeline's named secrets as plaintext). The message
	// is a template, so it can reference a secret, a prior step's output, a run
	// parameter, or the job's identity. When the step sets no message, none is
	// shown.
	message := ""
	if raw := paramString(step, ParamMessage); raw != "" {
		rendered, err := executor.RenderTemplate(raw, executor.ConditionContextFromContext(ctx))
		if err != nil {
			return fmt.Errorf("approval message: %w", err)
		}
		message = rendered
	}

	// Report the job as awaiting approval (F-13): the API persists the status
	// and message so the UI can show the gate, and the execution target's
	// status is now awaiting_approval (a non-terminal, in-flight state).
	if err := gate.ReportAwaitingApproval(ctx, message); err != nil {
		return fmt.Errorf("report awaiting approval: %w", err)
	}
	sink := executor.LogSinkFromContext(ctx)
	if sink != nil {
		sink.WriteStepOutput(executor.StepIndexFromContext(ctx), "stdout", []byte("awaiting approval"))
		if message != "" {
			sink.WriteStepOutput(executor.StepIndexFromContext(ctx), "stdout", []byte("message: "+message))
		}
	}

	// Poll the gate until it is resolved (approved or rejected) or the job has
	// been cancelled, or the step's context is done (a timeout, a cancellation
	// that cancelled the context, or a target shutdown).
	for {
		result, err := gate.CheckApproval(ctx)
		if err != nil {
			// A transient failure to check the gate is not a decision: keep
			// polling (the gate's state is durable in the database). But if the
			// step's context is done, stop.
			if ctx.Err() != nil {
				return approvalContextDone(ctx)
			}
			continue
		}
		if result.Cancelled {
			// The job was cancelled while it waited at the gate: report it
			// cancelled (distinct from a failure or a timeout).
			return fmt.Errorf("%w: job cancelled while awaiting approval", executor.ErrCancelled)
		}
		if result.Resolved {
			if result.Decision == "approved" {
				if sink != nil {
					sink.WriteStepOutput(executor.StepIndexFromContext(ctx), "stdout", []byte("approval granted"))
				}
				return nil
			}
			if sink != nil {
				sink.WriteStepOutput(executor.StepIndexFromContext(ctx), "stdout", []byte("approval rejected"))
			}
			return fmt.Errorf("approval rejected")
		}
		// The gate is not resolved yet; wait before polling again.
		select {
		case <-ctx.Done():
			return approvalContextDone(ctx)
		case <-time.After(approvalPollInterval):
		}
	}
}

// approvalContextDone maps a done step context (while waiting at an approval
// gate) to the job's outcome: a deadline (the step's per-step timeout or the
// job-level timeout) is a timeout, and anything else (a cancellation that
// cancelled the context, or a target shutdown) is reported as-is (the target's
// FinishJob reports a cancellation as cancelled).
func approvalContextDone(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("%w: approval timed out", executor.ErrTimeout)
	}
	return fmt.Errorf("approval interrupted: %w", ctx.Err())
}
