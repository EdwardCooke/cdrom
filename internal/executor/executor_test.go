package executor

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// testLogger returns a silent logger so test output stays clean.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// shellStep builds a step that runs script in the platform's shell. The
// executor's portability contract is that a command is executed directly (no
// implicit shell); a step that needs shell behavior invokes a shell
// explicitly, which is exactly what this helper does.
func shellStep(t *testing.T, script string) *dbpb.JobStep {
	t.Helper()
	if runtime.GOOS == "windows" {
		return &dbpb.JobStep{Command: "cmd", Args: []string{"/c", script}}
	}
	return &dbpb.JobStep{Command: "sh", Args: []string{"-c", script}}
}

// TestExecuteNilAndEmptySpec verifies that a nil or empty spec is a no-op
// that succeeds.
func TestExecuteNilAndEmptySpec(t *testing.T) {
	if err := Execute(context.Background(), nil, testLogger()); err != nil {
		t.Errorf("nil spec: %v", err)
	}
	if err := Execute(context.Background(), &dbpb.JobSpec{}, testLogger()); err != nil {
		t.Errorf("empty spec: %v", err)
	}
}

// TestExecuteMultiStepInOrder verifies that steps run in order: step 1 writes
// a file and step 2 reads it, so step 2 can only succeed if step 1 ran first.
func TestExecuteMultiStepInOrder(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "marker.txt")

	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		shellStep(t, "printf 'step1' > "+shellQuote(marker)),
		shellStep(t, "cat "+shellQuote(marker)+" > "+shellQuote(filepath.Join(dir, "read.txt"))),
	}}
	if err := Execute(context.Background(), spec, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "read.txt"))
	if err != nil {
		t.Fatalf("read step 2 output: %v", err)
	}
	if string(data) != "step1" {
		t.Errorf("step 2 read %q, want %q (steps did not run in order)", string(data), "step1")
	}
}

// TestExecuteFailingStepStops verifies that a failing step marks the job
// failed and stops subsequent steps: step 2 fails, so step 3 (which would
// write a file) must not run.
func TestExecuteFailingStepStops(t *testing.T) {
	dir := t.TempDir()
	never := filepath.Join(dir, "never.txt")

	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		shellStep(t, "exit 0"),
		shellStep(t, "exit 3"),
		shellStep(t, "printf 'ran' > "+shellQuote(never)),
	}}
	err := Execute(context.Background(), spec, testLogger())
	if err == nil {
		t.Fatal("expected an error from the failing step, got nil")
	}
	if _, statErr := os.Stat(never); !os.IsNotExist(statErr) {
		t.Errorf("step 3 ran after a failure: %v exists", never)
	}
}

// TestExecuteEnvVars verifies that a step's environment variables are
// available to the command.
func TestExecuteEnvVars(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "env.txt")

	var step *dbpb.JobStep
	if runtime.GOOS == "windows" {
		step = &dbpb.JobStep{
			Command: "cmd",
			Args:    []string{"/c", "echo %MY_VAR% > " + shellQuote(out)},
			Env:     map[string]string{"MY_VAR": "hello"},
		}
	} else {
		step = &dbpb.JobStep{
			Command: "sh",
			Args:    []string{"-c", `printf '%s' "$MY_VAR" > ` + shellQuote(out)},
			Env:     map[string]string{"MY_VAR": "hello"},
		}
	}
	if err := Execute(context.Background(), &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read env output: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "hello" {
		t.Errorf("env var = %q, want %q", got, "hello")
	}
}

// TestExecuteWorkdir verifies that a step runs in its working directory.
func TestExecuteWorkdir(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// The command writes "here" using a relative path; it must land in the
	// step's workdir, not the process's current directory.
	step := shellStep(t, "printf 'here' > relative.txt")
	step.Workdir = sub
	if err := Execute(context.Background(), &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := os.Stat(filepath.Join(sub, "relative.txt")); err != nil {
		t.Errorf("expected relative.txt in workdir %s: %v", sub, err)
	}
}

// TestExecutePerStepTimeout verifies that a step exceeding its timeout is
// terminated and reported as a timeout. The step is the long-running process
// directly (no shell wrapper) so killing it is clean and leaves no orphaned
// child holding the inherited output pipes.
func TestExecutePerStepTimeout(t *testing.T) {
	var step *dbpb.JobStep
	if runtime.GOOS == "windows" {
		// ping is the portable Windows sleep idiom (timeout needs a console).
		step = &dbpb.JobStep{Command: "ping", Args: []string{"-n", "6", "127.0.0.1"}}
	} else {
		step = &dbpb.JobStep{Command: "sleep", Args: []string{"5"}}
	}
	step.Timeout = durationpb.New(100 * time.Millisecond)

	start := time.Now()
	err := Execute(context.Background(), &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger())
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("error = %q, want a timeout error", err)
	}
	if elapsed >= 5*time.Second {
		t.Errorf("step was not terminated at its timeout (elapsed %s)", elapsed)
	}
}

// shellQuote quotes a path for use inside a shell script, portably.
func shellQuote(path string) string {
	if runtime.GOOS == "windows" {
		return `"` + path + `"`
	}
	return "'" + strings.ReplaceAll(path, "'", `'\''`) + "'"
}
