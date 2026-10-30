// Package stephandlers — the built-in "rollback" step handler (F-22).
//
// A rollback step ASKS THE API TO SPAWN A RESUMPTION of the job's own
// pipeline: the API validates the presenter's job token (establishing the
// caller's run → pipeline), applies the server-side GUARDS (containment to
// the caller's own pipeline, no re-arm for rollback-origin callers, cooldown
// against flapping), finds the ANCHOR (the latest successful,
// non-rollback-origin run of the caller's pipeline), and asks the scheduler
// to restart from the requested cut point (a stable job key, F-08). The
// target never chooses the anchor — that is a property of the mediation.
//
// The handler reads its settings from the step's Params map (lazily rendered
// against the step's condition context, so the cut point may cite a run
// parameter):
//   - "from_job" (string) — the CUT POINT, a stable job key (F-08) of the
//     anchor run (REQUIRED). This job and everything downstream of it
//     re-execute fresh; its strict ancestors are carried over.
//   - "wait" (bool string) — BLOCK the step until the resumption run
//     terminates. Default "true".
//   - "wait_deadline" (duration string) — the MAXIMUM block; when exceeded
//     the step FAILS citing the still-live resumption run id (the resumption
//     continues unattached). Default "30m", clamped by the step's own
//     Timeout.
//
// Verdict mapping (with wait): resumption `succeeded` → the step SUCCEEDS;
// `failed` / `timed_out` / `cancelled` → the step FAILS (a BOTCHED ROLLBACK
// must fail the enclosing run loudly, never pass silently). Absent Restorer
// capability (an execution target that does not wire it) → a step error,
// parity with the email handler.
//
// Step outputs (F-06): "restore_run_id" (the resumption run's id, published
// as SOON AS IT IS INITIATED — before any waiting) and, when waited,
// "restore_status" (its terminal status).
package stephandlers

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypeRollback is the step type of the built-in rollback handler (F-22).
const TypeRollback = executor.TypeRollback

// Param keys the rollback handler reads from a step's Params map.
const (
	ParamRollbackFromJob    = "from_job"
	ParamRollbackWait       = "wait"
	ParamRollbackWaitDeadln = "wait_deadline"
)

// Rollback defaults.
//
// defaultRollbackWait is FALSE: the step INITIATES the resumption and returns
// (publishing its run id as an output); the resumption's health is tracked in
// the run timeline, not by blocking the step. Blocking (wait: true) is OPT-IN
// because it carries a DEADLOCK HAZARD: a rollback step that waits holds its
// worker's execution slot, and the resumption it waits on may need THAT SLOT
// to run its own jobs (especially on a single-worker group, or a saturated
// pool). Such a waiter starves the very resumption it is monitoring. Choose
// wait: true only when you can guarantee the resumption has OTHER capacity to
// run on.
const (
	defaultRollbackWait       = false
	defaultRollbackWaitDeadln = 30 * time.Minute
)

// rollbackPollInterval is how often the rollback step polls the resumption
// run's status while waiting (F-22). It is a VARIABLE (not a constant) so
// tests can shorten it.
var rollbackPollInterval = 5 * time.Second

func init() {
	executor.RegisterStepType(TypeRollback, runRollbackStep)
}

// runRollbackStep is the built-in "rollback" step handler (F-22). It
// initiates a resumption of the job's own pipeline through the API (guards
// applied server-side) and, when waiting, polls the resumption run's status
// until it terminates or the wait deadline lapses.
func runRollbackStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	restorer := executor.RestorerFromContext(ctx)
	if restorer == nil {
		return fmt.Errorf("rollback is not supported by this execution target")
	}
	condCtx := executor.ConditionContextFromContext(ctx)

	fromJob, err := renderRollbackScalar(condCtx, step, ParamRollbackFromJob)
	if err != nil {
		return err
	}
	if fromJob == "" {
		return fmt.Errorf("rollback: from_job is required")
	}
	wait := defaultRollbackWait
	if w, err := renderRollbackScalar(condCtx, step, ParamRollbackWait); err == nil && w != "" {
		parsed, err := strconv.ParseBool(strings.TrimSpace(w))
		if err != nil {
			return fmt.Errorf("rollback: wait %q is not a boolean", w)
		}
		wait = parsed
	}
	waitDeadline := defaultRollbackWaitDeadln
	if wd, err := renderRollbackScalar(condCtx, step, ParamRollbackWaitDeadln); err == nil && wd != "" {
		d, err := time.ParseDuration(wd)
		if err != nil {
			return fmt.Errorf("rollback: wait_deadline %q: %w", wd, err)
		}
		if d <= 0 {
			return fmt.Errorf("rollback: wait_deadline must be positive")
		}
		waitDeadline = d
	}
	if stepTimeout := step.GetTimeout().AsDuration(); stepTimeout > 0 && waitDeadline > stepTimeout {
		waitDeadline = stepTimeout
	}

	// Initiate FIRST (the server-side guards reject a re-arm or a cooled-down
	// pipeline before any waiting begins), then publish the resumption run id
	// as a step output so it is visible even if the step subsequently fails.
	runID, err := restorer.InitiateRestore(ctx, fromJob)
	if err != nil {
		return fmt.Errorf("rollback: initiate restore from %q: %w", fromJob, err)
	}
	_ = writeStepOutput(step, "restore_run_id", strconv.FormatInt(runID, 10))
	logger.Info("executor: rollback initiated", "step", rollbackStepName(step), "from_job", fromJob, "restore_run", runID, "wait", wait)

	if !wait {
		return nil
	}

	// Wait on the resumption's verdict, bounded by waitDeadline (and the
	// step's context, which the executor bounds further by the step timeout).
	deadline := time.Now().Add(waitDeadline)
	for {
		statusStr, err := restorer.RunStatus(ctx, runID)
		if err != nil {
			// Transient read failure: retry until the deadline/context ends.
			sleepEarly(ctx, rollbackPollInterval)
			if time.Now().After(deadline) {
				return fmt.Errorf("rollback: wait deadline (%s) reached; resumption run %d is still in flight (last status unreadable)", waitDeadline, runID)
			}
			continue
		}
		switch statusStr {
		case "succeeded":
			_ = writeStepOutput(step, "restore_status", statusStr)
			logger.Info("executor: rollback resumption succeeded", "step", rollbackStepName(step), "restore_run", runID)
			return nil
		case "failed", "timed_out", "cancelled":
			_ = writeStepOutput(step, "restore_status", statusStr)
			logger.Warn("executor: rollback resumption ended badly", "step", rollbackStepName(step), "restore_run", runID, "status", statusStr)
			return fmt.Errorf("rollback: resumption run %d ended %s", runID, statusStr)
		default:
			// pending / running: keep polling.
			sleepDur := rollbackPollInterval
			if until := time.Until(deadline); until < sleepDur {
				if until <= 0 {
					_ = writeStepOutput(step, "restore_status", statusStr)
					return fmt.Errorf("rollback: wait deadline (%s) reached; resumption run %d is still %s (it continues unattached)", waitDeadline, runID, statusStr)
				}
				sleepDur = until
			}
			sleepEarly(ctx, sleepDur)
		}
	}
}

// sleepEarly sleeps for d, waking EARLY when ctx is done (so a cancelled job
// unwinds the wait loop promptly).
func sleepEarly(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// rollbackStepName returns the step's name (falling back to its type) for
// logs.
func rollbackStepName(step *dbpb.JobStep) string {
	if n := step.GetName(); n != "" {
		return n
	}
	return step.GetType()
}

// renderRollbackScalar renders the STRING param named key against the step's
// condition context (lazy rendering, F-22). Absent/empty yields "".
func renderRollbackScalar(condCtx map[string]any, step *dbpb.JobStep, key string) (string, error) {
	raw, ok := step.GetParams()[key]
	if !ok || raw == nil {
		return "", nil
	}
	val := raw.GetString_()
	if val == "" {
		return "", nil
	}
	rendered, err := executor.RenderTemplate(val, condCtx)
	if err != nil {
		return "", fmt.Errorf("rollback: %s: %w", key, err)
	}
	return strings.TrimSpace(rendered), nil
}
