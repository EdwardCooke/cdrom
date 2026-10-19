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
	"runtime"
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
// Portability contract: a step's command is resolved and executed by the
// target's OS. When a step sets no "shell" param and no "args", its command is
// a one-line shell command and is run through the target OS's default shell
// (`bash -c <command>` on Linux when bash is on the PATH, else `sh -c
// <command>`, and on Windows `pwsh -Command <command>` when PowerShell Core is
// on the PATH, else `powershell -Command <command>`) — this is what makes the
// file format's `shell: <string>` shorthand (a plain-string command) portable.
// When a step sets "args" (it is constructing the invocation
// explicitly) the command is run directly with those args and no shell is
// involved, so the same spec executes identically on Windows and Linux with no
// shell-specific assumptions (no `&&`, no pipes, no globbing, no `$VAR`
// expansion). A step's workdir is interpreted with the target's native path
// separator; a relative workdir is resolved against the target's current
// working directory.
//
// Shell override: a step may set the "shell" param to run through a
// user-chosen shell instead of the platform default. When it is set the target
// executes `<shell> <args> <command>` — the command is passed as the final
// argument to the shell (e.g. shell "pwsh", args
// ["-NoProfile", "-Command"], command "Get-ChildItem"). This is the portable
// way to opt a step into shell behavior with a specific shell rather than the
// platform default.
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

	// Resolve the executable and its arguments. When the step sets a shell, it
	// runs `<shell> <args> <command>`: the shell is the executable, `args` are
	// the shell's own flags, and `command` is passed as the final argument
	// (this lets a step opt into shell behavior with a user-chosen shell, e.g.
	// "pwsh" on Windows, instead of the platform default). When the step sets
	// no shell and no args, its command is a one-line shell command and is run
	// through the target OS's default shell (bash -c on Linux when bash is on
	// the PATH, else sh -c, and on Windows pwsh -Command when PowerShell Core
	// is on the PATH, else powershell -Command). When the step sets args (no
	// shell), the command is run directly with those args (no shell).
	executable := command
	args := paramStrings(step, ParamArgs)
	if shell := paramString(step, ParamShell); shell != "" {
		executable = shell
		args = append(append([]string{}, args...), command)
	} else if len(args) == 0 {
		executable, args = defaultShellInvocation(command)
	}

	cmd := exec.CommandContext(ctx, executable, args...)
	if workdir := step.GetWorkdir(); workdir != "" {
		cmd.Dir = workdir
	}
	cmd.Env = mergeEnv(os.Environ(), step.GetEnv())

	sink := executor.LogSinkFromContext(ctx)
	stepName := executor.StepNameFromContext(ctx)

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
		sink.WriteStepOutput(stepName, "stdout", []byte("command: "+command))
		for _, arg := range args {
			sink.WriteStepOutput(stepName, "stdout", []byte("arg: "+arg))
		}
		sink.WriteStepOutput(stepName, "stdout", []byte("shell: "+paramString(step, ParamShell)))
		sink.WriteStepOutput(stepName, "stdout", []byte("workdir: "+step.GetWorkdir()))
	}

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
			sendToLogs(stdout, sink, stepName, executor.StreamStdout)
		}()
		go func() {
			defer wg.Done()
			sendToLogs(stderr, sink, stepName, executor.StreamStderr)
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
		// Distinguish a timeout from any other failure. A timeout manifests as
		// the step's context hitting its deadline — either the step's own
		// per-step timeout or the job-level timeout inherited from the parent
		// context (F-03). A parent cancellation (e.g. a job cancel) surfaces
		// as context.Canceled instead. Wrapping executor.ErrTimeout lets the
		// worker/agent report the job as timed_out rather than failed.
		if ctx.Err() == context.DeadlineExceeded {
			if timeout := step.GetTimeout().AsDuration(); timeout > 0 {
				return fmt.Errorf("%w: %q timed out after %s", executor.ErrTimeout, command, timeout)
			}
			return fmt.Errorf("%w: %q timed out", executor.ErrTimeout, command)
		}
		return fmt.Errorf("%q: %w", command, err)
	}
	return nil
}

// sendToLogs copies from src to both local (the target's own stream) and to
// sink (attributed to stepName/stream). It runs until src is exhausted (the
// command's pipe closes). Writes to the sink are best-effort: the sink drops
// output it cannot keep up with, so this never blocks the command.
func sendToLogs(src io.Reader, sink executor.LogSink, stepName string, stream string) {
	buffer := make([]byte, 32*1024)
	for {
		n, err := src.Read(buffer)
		if n > 0 {
			data := buffer[:n]
			// Stream to the API (best-effort, non-blocking).
			sink.WriteStepOutput(stepName, stream, data)
		}
		if err != nil {
			return
		}
	}
}

// defaultShellInvocation returns the executable and arguments that run
// command through the target OS's default shell: `bash -c <command>` on Linux
// (and other Unixes) when bash is on the PATH, falling back to
// `sh -c <command>` when it is not, and, on Windows, `pwsh -Command
// <command>` when PowerShell Core (pwsh) is on the PATH, falling back to
// `powershell -Command <command>` (Windows PowerShell) when it is not. It is
// used for a shell step that sets no "shell" param and no "args" — a one-line
// shell command (the file format's `shell: <string>` shorthand) — so the
// command is parsed by a shell rather than executed as a single executable
// name.
func defaultShellInvocation(command string) (string, []string) {
	if runtime.GOOS == "windows" {
		return windowsDefaultShellInvocation(command, exec.LookPath)
	}
	return unixDefaultShellInvocation(command, exec.LookPath)
}

// unixDefaultShellInvocation returns the executable and arguments that run
// command through the Unix default shell: `bash -c <command>` when bash is on
// the PATH, falling back to `sh -c <command>` when it is not. The lookup
// function is a parameter so the bash/sh selection can be tested on any
// platform.
func unixDefaultShellInvocation(command string, lookPath func(string) (string, error)) (string, []string) {
	if _, err := lookPath("bash"); err == nil {
		return "bash", []string{"-c", command}
	}
	return "sh", []string{"-c", command}
}

// windowsDefaultShellInvocation returns the executable and arguments that run
// command through the Windows default shell: `pwsh -Command <command>` when
// PowerShell Core (pwsh) is on the PATH, falling back to
// `powershell -Command <command>` (Windows PowerShell) when it is not. The
// lookup function is a parameter so the pwsh/powershell selection can be
// tested on any platform.
func windowsDefaultShellInvocation(command string, lookPath func(string) (string, error)) (string, []string) {
	if _, err := lookPath("pwsh"); err == nil {
		return "pwsh", []string{"-Command", command}
	}
	return "powershell", []string{"-Command", command}
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
