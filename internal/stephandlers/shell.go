// Package stephandlers holds the built-in step handlers that the executor
// dispatches to. The executor (internal/executor) is the generic dispatch
// engine: it selects a handler by a step's type and runs it. This package
// provides the concrete handlers. The built-in "shell" handler is registered
// under executor.DefaultType, so a step with no type runs through it.
//
// A target (or a plugin) adds more step types by calling
// executor.RegisterStepType with a new handler — the same mechanism a future
// plugin architecture will use. The worker and agent blank-import this
// package so the built-in handlers are registered before any job runs.
package stephandlers

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strings"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypeShell is the step type of the built-in shell handler. It is also the
// executor's default: a step with no type is dispatched to it.
//
// Portability contract: a step's command is resolved and executed directly by
// the target's OS — no shell is involved. This means the same spec executes
// identically on Windows and Linux: there are no shell-specific assumptions
// (no `&&`, no pipes, no globbing, no `$VAR` expansion). A step that needs
// shell behavior must invoke a shell explicitly (e.g. command "bash" with
// args ["-c", "..."], or "pwsh" with args ["-NoProfile", "-Command", "..."]).
// A step's workdir is interpreted with the target's native path separator; a
// relative workdir is resolved against the target's current working directory.
//
// Shell override: a step may set Shell to run through a user-chosen shell
// instead of executing Command directly. When Shell is set the target
// executes `<Shell> <Args> <Command>` — Command is passed as the final
// argument to the shell (e.g. Shell "pwsh", Args ["-NoProfile", "-Command"],
// Command "Get-ChildItem"). This is the portable way to opt a step into shell
// behavior with a specific shell rather than a platform default. When Shell
// is empty the step runs Command directly, preserving the no-implicit-shell
// contract.
const TypeShell = executor.DefaultType

func init() {
	executor.RegisterStepType(TypeShell, runShellStep)
}

// runShellStep is the built-in "shell" step handler. It runs the step's
// command, directly or through the step's shell. The step's stdout and stderr
// are inherited from the execution target (the worker/agent's own
// stdout/stderr, which go to stdout or the log file) — there is no central
// logs service, and streaming output to the API is a later concern (F-02).
// Inheriting (rather than capturing into pipes) also means a per-step timeout
// returns promptly: killing the step's process does not leave open pipes that
// would block the wait.
func runShellStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	command := step.GetCommand()
	if command == "" {
		return fmt.Errorf("command is required")
	}

	// Resolve the executable and its arguments. By default the step runs
	// `command` directly with `args` (no shell). When the step sets a shell,
	// it instead runs `<shell> <args> <command>`: the shell is the executable,
	// `args` are the shell's own flags, and `command` is passed as the final
	// argument. This lets a step opt into shell behavior with a user-chosen
	// shell (e.g. "pwsh" on Windows) instead of a platform default.
	executable := command
	args := step.GetArgs()
	if shell := step.GetShell(); shell != "" {
		executable = shell
		args = append(append([]string{}, args...), command)
	}

	cmd := exec.CommandContext(ctx, executable, args...)
	if workdir := step.GetWorkdir(); workdir != "" {
		cmd.Dir = workdir
	}
	cmd.Env = mergeEnv(os.Environ(), step.GetEnv())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	logger.Info("executor: running step",
		"command", command, "args", args, "shell", step.GetShell(), "workdir", step.GetWorkdir())

	err := cmd.Run()

	if err != nil {
		// Distinguish a per-step timeout from any other failure. A timeout
		// manifests as the derived context hitting its deadline; a parent
		// cancellation surfaces as context.Canceled instead.
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("%q timed out after %s", command, step.GetTimeout().AsDuration())
		}
		return fmt.Errorf("%q: %w", command, err)
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
