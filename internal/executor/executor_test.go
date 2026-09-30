package executor

import (
	"context"
	"errors"
	"log/slog"
	"os"
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

// TestExecuteDefaultTypeDispatch verifies that a step with no type is
// dispatched to the handler registered under DefaultType. This test package
// cannot import the built-in shell handler (it would be an import cycle), so
// it registers a temporary handler under DefaultType and checks the dispatch.
// The real shell handler's behavior is covered in internal/stephandlers.
func TestExecuteDefaultTypeDispatch(t *testing.T) {
	dir := t.TempDir()
	out := dir + "/out.txt"

	RegisterStepType(DefaultType, func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return os.WriteFile(out, []byte("default-handler-ran"), 0o644)
	})

	step := &dbpb.JobStep{} // no type: must dispatch to the DefaultType handler
	if err := Execute(context.Background(), &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if got := string(data); got != "default-handler-ran" {
		t.Errorf("output = %q, want %q (empty type did not dispatch to DefaultType)", got, "default-handler-ran")
	}
}

// TestExecuteCustomStepType verifies the step-type registry: a target can
// register a handler for a new step type, and a step of that type is
// dispatched to it. The handler reads the step's Params, proving that
// handler-specific configuration flows through the spec.
func TestExecuteCustomStepType(t *testing.T) {
	dir := t.TempDir()
	out := dir + "/out.txt"

	RegisterStepType("marker", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		// A stand-in for a non-shell step type (e.g. "ansible"): it does its
		// own work from the step's params rather than running a command.
		return os.WriteFile(out, []byte(step.GetParams()["value"].GetString_()), 0o644)
	})

	step := &dbpb.JobStep{
		Type: "marker",
		Params: map[string]*dbpb.ParamValue{
			"value": {String_: "custom-type-ran"},
		},
	}
	if err := Execute(context.Background(), &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read custom-type output: %v", err)
	}
	if got := string(data); got != "custom-type-ran" {
		t.Errorf("custom-type output = %q, want %q (handler not dispatched)", got, "custom-type-ran")
	}
}

// TestExecuteUnknownStepType verifies that a step whose type has no handler
// registered on the target fails the job with a clear error, rather than
// silently doing nothing.
func TestExecuteUnknownStepType(t *testing.T) {
	step := &dbpb.JobStep{Type: "does-not-exist"}
	err := Execute(context.Background(), &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger())
	if err == nil {
		t.Fatal("expected an error for an unknown step type, got nil")
	}
	if !strings.Contains(err.Error(), "unknown step type") {
		t.Errorf("error = %q, want an unknown-step-type error", err)
	}
}

// TestExecuteJobLevelTimeout verifies that a job-level timeout (spec.Timeout)
// bounds the whole job: a step that would run past the job's deadline is
// cancelled and Execute returns ErrTimeout (F-03). The step itself declares
// no per-step timeout, so only the job-level timeout applies.
func TestExecuteJobLevelTimeout(t *testing.T) {
	RegisterStepType("blocker", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		// Block until the context (carrying the job-level timeout) is done.
		<-ctx.Done()
		return ctx.Err()
	})

	spec := &dbpb.JobSpec{
		Timeout: durationpb.New(150 * time.Millisecond),
		Steps:   []*dbpb.JobStep{{Type: "blocker"}},
	}
	start := time.Now()
	err := Execute(context.Background(), spec, testLogger())
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("error = %q, want it to wrap ErrTimeout", err)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("job was not terminated at its timeout (elapsed %s)", elapsed)
	}
}

// TestExecuteNoTimeoutUnbounded verifies that a job with no job-level timeout
// and no per-step timeout runs unbounded (bounded only by the caller's
// context): the executor imposes no deadline of its own. The test cancels the
// caller's context to unblock the step; a cancellation must not be reported
// as a timeout (ErrTimeout).
func TestExecuteNoTimeoutUnbounded(t *testing.T) {
	RegisterStepType("blocker", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		<-ctx.Done()
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel the caller's context after a short delay to unblock the step.
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{{Type: "blocker"}}}
	err := Execute(ctx, spec, testLogger())
	if err == nil {
		t.Fatal("expected an error when the caller's context was cancelled, got nil")
	}
	if errors.Is(err, ErrTimeout) {
		t.Errorf("error = %q, want a cancellation (not a timeout) for a job with no timeout", err)
	}
}
