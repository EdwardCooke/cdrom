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
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypeShell is the step type of the built-in shell handler. It is also the
// executor's default: a step with no type is dispatched to it.
//
// The handler reads its settings from the step's Params map:
//   - "command" (string) — the executable to run (required).
//   - "args" (list of strings) — the command's arguments, in order.
//   - "shell" (string) — when set, the command is run through this shell
//     instead of directly.
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
// Shell override: a step may set the "shell" param to run through a
// user-chosen shell instead of executing the command directly. When it is set
// the target executes `<shell> <args> <command>` — the command is passed as
// the final argument to the shell (e.g. shell "pwsh", args
// ["-NoProfile", "-Command"], command "Get-ChildItem"). This is the portable
// way to opt a step into shell behavior with a specific shell rather than a
// platform default. When shell is empty the step runs the command directly,
// preserving the no-implicit-shell contract.
const TypeShell = executor.DefaultType

// Param keys the shell handler reads from a step's Params map.
const (
	ParamCommand = "command"
	ParamArgs    = "args"
	ParamShell   = "shell"
)

func init() {
	executor.RegisterStepType(TypeShell, runShellStep)
}

// runShellStep is the built-in "shell" step handler. It runs the step's
// command, directly or through the step's shell.
//
// Output handling: the step's stdout and stderr are always written to the
// execution target's own stdout/stderr (local logging). When the step's
// context carries a LogSink (set by a target that streams logs to the API,
// see F-02), the output is additionally captured and streamed to the sink in
// near-real-time: the command's stdout/stderr are read from pipes and teed to
// both the target's local streams and the sink. When no sink is present the
// command's streams are inherited directly from the target — inheriting
// (rather than capturing into pipes) means a per-step timeout returns
// promptly: killing the step's process does not leave open pipes that would
// block the wait.
func runShellStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	command := paramString(step, ParamCommand)
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
	args := paramStrings(step, ParamArgs)
	if shell := paramString(step, ParamShell); shell != "" {
		executable = shell
		args = append(append([]string{}, args...), command)
	}

	cmd := exec.CommandContext(ctx, executable, args...)
	if workdir := step.GetWorkdir(); workdir != "" {
		cmd.Dir = workdir
	}
	cmd.Env = mergeEnv(os.Environ(), step.GetEnv())

	sink := executor.LogSinkFromContext(ctx)
	stepIndex := executor.StepIndexFromContext(ctx)

	// When a sink is present, capture the command's output into pipes and tee
	// it to both the target's local streams and the sink. Otherwise inherit
	// the target's streams directly (no capture, prompt timeout).
	var stdout, stderr io.ReadCloser
	if sink != nil {
		var err error
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			return fmt.Errorf("%q: stdout pipe: %w", command, err)
		}
		stderr, err = cmd.StderrPipe()
		if err != nil {
			return fmt.Errorf("%q: stderr pipe: %w", command, err)
		}
	} else {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	logger.Info("executor: running step",
		"command", command, "args", args, "shell", paramString(step, ParamShell), "workdir", step.GetWorkdir())

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%q: %w", command, err)
	}

	// Copy the command's output to the target's local streams and (when a
	// sink is present) to the sink. Both copies run concurrently so output is
	// streamed as it is produced. The copies finish when the command's pipes
	// close (on exit or on the context killing the process).
	var wg sync.WaitGroup
	if sink != nil {
		wg.Add(2)
		go func() {
			defer wg.Done()
			teeToSink(stdout, os.Stdout, sink, stepIndex, executor.StreamStdout)
		}()
		go func() {
			defer wg.Done()
			teeToSink(stderr, os.Stderr, sink, stepIndex, executor.StreamStderr)
		}()
	}

	// When capturing, finish reading the command's pipes before Wait: with
	// StdoutPipe/StderrPipe, Wait closes the pipes, so it must not run before
	// the reads complete (or output would be lost). The tee goroutines finish
	// when the command exits and its pipes close, so wg.Wait() blocks until
	// the command has run to completion (or been killed by its context).
	if sink != nil {
		wg.Wait()
	}
	err := cmd.Wait()

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

// teeToSink copies from src to both local (the target's own stream) and to
// sink (attributed to stepIndex/stream). It runs until src is exhausted (the
// command's pipe closes). Writes to the sink are best-effort: the sink drops
// output it cannot keep up with, so this never blocks the command.
func teeToSink(src io.Reader, local io.Writer, sink executor.LogSink, stepIndex int, stream string) {
	buffer := make([]byte, 32*1024)
	for {
		n, err := src.Read(buffer)
		if n > 0 {
			data := buffer[:n]
			// Local logging first (the target's own stdout/stderr).
			_, _ = local.Write(data)
			// Stream to the API (best-effort, non-blocking).
			sink.WriteStepOutput(stepIndex, stream, data)
		}
		if err != nil {
			return
		}
	}
}

// paramString returns the string value of a step param, or "" if the param is
// absent or not a string.
func paramString(step *dbpb.JobStep, key string) string {
	if v := step.GetParams()[key]; v != nil {
		return v.GetString_()
	}
	return ""
}

// paramStrings returns the list value of a step param, or nil if the param is
// absent or not a list.
func paramStrings(step *dbpb.JobStep, key string) []string {
	if v := step.GetParams()[key]; v != nil {
		return v.GetStrings()
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
