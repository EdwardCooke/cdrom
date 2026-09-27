// Package executor runs a job's execution spec: an ordered list of steps.
// Each step is agnostic about *how* it runs: a step's Type selects a
// StepHandler that executes it. A step with no type is dispatched to the
// handler registered under DefaultType (the built-in "shell" handler, which
// runs a command — directly, or through a user-chosen shell — in a working
// directory with an environment). A target (or a plugin) can register
// additional step types via RegisterStepType (e.g. "ansible", "terraform",
// "argo"); a step whose type is not registered on the target fails the job
// with a clear error.
//
// The concrete handlers live in internal/stephandlers (and, later, in
// plugins); this package is the generic dispatch engine and knows only the
// DefaultType name, not the handlers themselves. The package is shared by the
// long-lived worker and the ephemeral agent, and must run on both Windows and
// Linux.
package executor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// Execute runs the steps of spec in order. It returns nil when every step
// succeeds, or the error from the first step that fails — subsequent steps
// are not run. A nil or empty spec succeeds without doing any work (a job
// with no execution spec is a no-op).
//
// The provided ctx bounds the whole job; a per-step timeout (when set)
// additionally bounds an individual step.
func Execute(ctx context.Context, spec *dbpb.JobSpec, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if spec == nil {
		return nil
	}
	for i, step := range spec.GetSteps() {
		if err := runStep(ctx, i, step, logger); err != nil {
			return err
		}
	}
	return nil
}

// StepHandler executes a single step of a job. A handler is selected by the
// step's Type (an empty Type selects the handler registered under
// DefaultType). A handler receives the step's context — already bounded by
// the step's per-step timeout, when set — and the step itself, and returns a
// descriptive error when the step fails.
//
// Handlers are expected to be safe for concurrent use: a worker may run steps
// from different jobs on different goroutines.
type StepHandler func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error

// DefaultType is the step type a step is dispatched to when its Type is
// empty. The built-in shell handler (internal/stephandlers) registers itself
// under this name, so a step with no type runs through the shell handler.
const DefaultType = "shell"

// stepTypes maps a step type to its handler. Handlers register themselves via
// RegisterStepType (the built-in shell handler does so at package init in
// internal/stephandlers); targets and plugins add more the same way.
var stepTypes sync.Map // map[string]StepHandler

// RegisterStepType registers a handler for a step type so steps of that type
// can be executed. The built-in "shell" handler is always registered; this is
// how a target (or a plugin) adds new step types such as "ansible",
// "terraform", or "argo". Registering a type that is already registered
// replaces its handler.
func RegisterStepType(name string, handler StepHandler) {
	if name == "" || handler == nil {
		return
	}
	stepTypes.Store(name, handler)
}

// runStep executes a single step by dispatching to the handler registered for
// the step's type (the built-in "shell" handler when the type is empty). It
// returns a descriptive error when the step fails or its type is not
// registered on this target.
func runStep(ctx context.Context, index int, step *dbpb.JobStep, logger *slog.Logger) error {
	name := step.GetType()
	if name == "" {
		name = DefaultType
	}
	handler, ok := stepTypes.Load(name)
	if !ok {
		return fmt.Errorf("executor: step %d: unknown step type %q (no handler registered on this target)", index, name)
	}

	// A per-step timeout derives a context that expires after the step's
	// timeout. A zero timeout means no per-step limit; the parent ctx still
	// bounds the step.
	stepCtx := ctx
	if timeout := step.GetTimeout().AsDuration(); timeout > 0 {
		var cancel context.CancelFunc
		stepCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if err := handler.(StepHandler)(stepCtx, step, logger); err != nil {
		return fmt.Errorf("executor: step %d (type %q): %w", index, name, err)
	}
	return nil
}
