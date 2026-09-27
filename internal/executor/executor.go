// Package executor runs a job's execution spec: an ordered list of steps,
// each a command (with optional arguments, working directory, environment
// variables, and per-step timeout). It is shared by the long-lived worker and
// the ephemeral agent, and must run on both Windows and Linux.
//
// Portability contract: a step's command is resolved and executed directly by
// the target's OS — no shell is involved. This means the same spec executes
// identically on Windows and Linux: there are no shell-specific assumptions
// (no `&&`, no pipes, no globbing, no `$VAR` expansion). A step that needs
// shell behavior must invoke a shell explicitly (e.g. command "bash" with
// args ["-c", "..."], or "pwsh" with args ["-NoProfile", "-Command", "..."]).
// A step's workdir is interpreted with the target's native path separator; a
// relative workdir is resolved against the target's current working directory.
package executor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strings"

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

// runStep executes a single step and returns a descriptive error when it
// fails. The step's stdout and stderr are inherited from the execution target
// (the worker/agent's own stdout/stderr, which go to stdout or the log file)
// — there is no central logs service, and streaming output to the API is a
// later concern (F-02). Inheriting (rather than capturing into pipes) also
// means a per-step timeout returns promptly: killing the step's process does
// not leave open pipes that would block the wait.
func runStep(ctx context.Context, index int, step *dbpb.JobStep, logger *slog.Logger) error {
	command := step.GetCommand()
	if command == "" {
		return fmt.Errorf("executor: step %d: command is required", index)
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

	cmd := exec.CommandContext(stepCtx, command, step.GetArgs()...)
	if workdir := step.GetWorkdir(); workdir != "" {
		cmd.Dir = workdir
	}
	cmd.Env = mergeEnv(os.Environ(), step.GetEnv())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	logger.Info("executor: running step",
		"step", index, "command", command, "args", step.GetArgs(), "workdir", step.GetWorkdir())

	err := cmd.Run()

	if err != nil {
		// Distinguish a per-step timeout from any other failure. A timeout
		// manifests as the derived context hitting its deadline; a parent
		// cancellation surfaces as context.Canceled instead.
		if stepCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("executor: step %d (%q) timed out after %s", index, command, step.GetTimeout().AsDuration())
		}
		return fmt.Errorf("executor: step %d (%q): %w", index, command, err)
	}
	return nil
}

// mergeEnv returns the base environment with the extra variables applied. An
// extra variable overrides a base variable with the same name. Extra
// variables are appended in sorted order so the result is deterministic.
func mergeEnv(base []string, extra map[string]string) []string {
	if len(extra) == 0 {
		return base
	}
	env := make([]string, 0, len(base)+len(extra))
	for _, kv := range base {
		name := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			name = kv[:i]
		}
		if _, overridden := extra[name]; overridden {
			continue
		}
		env = append(env, kv)
	}
	names := make([]string, 0, len(extra))
	for name := range extra {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		env = append(env, name+"="+extra[name])
	}
	return env
}
