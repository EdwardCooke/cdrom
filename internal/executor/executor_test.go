package executor

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

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
		return os.WriteFile(out, []byte(step.GetParams()["value"]), 0o644)
	})

	step := &dbpb.JobStep{
		Type:   "marker",
		Params: map[string]string{"value": "custom-type-ran"},
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
