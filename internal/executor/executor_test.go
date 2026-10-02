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

// TestExecuteConditionTrueRunsStep verifies that a step whose Condition
// renders to true runs normally and is reported as succeeded.
func TestExecuteConditionTrueRunsStep(t *testing.T) {
	var ran bool
	RegisterStepType("condition-marker", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		ran = true
		return nil
	})

	collector := &StepResultCollector{}
	ctx := ContextWithStepStatusReporter(context.Background(), collector)
	step := &dbpb.JobStep{Type: "condition-marker", Condition: "{{ eq 1 1 }}"}
	if err := Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !ran {
		t.Error("step with a true condition did not run")
	}
	results := collector.All()
	if len(results) != 1 || results[0].Status != StepStatusSucceeded {
		t.Fatalf("results = %+v, want one succeeded result", results)
	}
}

// TestExecuteConditionFalseSkipsStep verifies that a step whose Condition
// renders to false is skipped (does not run, does not fail the job) and is
// reported as skipped (F-06).
func TestExecuteConditionFalseSkipsStep(t *testing.T) {
	var ran bool
	RegisterStepType("condition-marker-2", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		ran = true
		return nil
	})

	collector := &StepResultCollector{}
	ctx := ContextWithStepStatusReporter(context.Background(), collector)
	step := &dbpb.JobStep{Type: "condition-marker-2", Condition: "{{ eq 1 2 }}"}
	if err := Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ran {
		t.Error("step with a false condition ran, want it skipped")
	}
	results := collector.All()
	if len(results) != 1 || results[0].Status != StepStatusSkipped {
		t.Fatalf("results = %+v, want one skipped result", results)
	}
}

// TestExecuteConditionMalformedFailsJob verifies that a Condition that fails
// to render or does not parse as a boolean is treated as a spec error: the
// job fails (the step is not skipped, and later steps do not run).
func TestExecuteConditionMalformedFailsJob(t *testing.T) {
	var laterRan bool
	RegisterStepType("condition-marker-3", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		laterRan = true
		return nil
	})

	step := &dbpb.JobStep{Type: "condition-marker-3", Condition: "not-a-boolean"}
	later := &dbpb.JobStep{Type: "condition-marker-3"}
	err := Execute(context.Background(), &dbpb.JobSpec{Steps: []*dbpb.JobStep{step, later}}, testLogger())
	if err == nil {
		t.Fatal("expected an error for a malformed condition, got nil")
	}
	if laterRan {
		t.Error("a later step ran after a malformed condition failed the job")
	}
}

// TestExecuteConditionEmptyNeverSkips verifies that a step with no Condition
// always runs (the common case, with no behavior change from before F-06).
func TestExecuteConditionEmptyNeverSkips(t *testing.T) {
	var ran bool
	RegisterStepType("condition-marker-4", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		ran = true
		return nil
	})

	step := &dbpb.JobStep{Type: "condition-marker-4"}
	if err := Execute(context.Background(), &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !ran {
		t.Error("step with no condition did not run")
	}
}

// TestStepResultCollectorToProto verifies the proto conversion used to
// attach collected step results to a ReportJobStatusRequest (F-06).
func TestStepResultCollectorToProto(t *testing.T) {
	collector := &StepResultCollector{}
	collector.ReportStepStatus(0, StepStatusSucceeded, "")
	collector.ReportStepStatus(1, StepStatusSkipped, "")
	collector.ReportStepStatus(2, StepStatusFailed, "boom")

	proto := collector.ToProto()
	if len(proto) != 3 {
		t.Fatalf("len(proto) = %d, want 3", len(proto))
	}
	if proto[0].GetStatus() != dbpb.StepStatus_STEP_STATUS_SUCCEEDED {
		t.Errorf("proto[0].Status = %v, want SUCCEEDED", proto[0].GetStatus())
	}
	if proto[1].GetStatus() != dbpb.StepStatus_STEP_STATUS_SKIPPED {
		t.Errorf("proto[1].Status = %v, want SKIPPED", proto[1].GetStatus())
	}
	if proto[2].GetStatus() != dbpb.StepStatus_STEP_STATUS_FAILED || proto[2].GetError() != "boom" {
		t.Errorf("proto[2] = %+v, want FAILED with error %q", proto[2], "boom")
	}

	empty := &StepResultCollector{}
	if got := empty.ToProto(); got != nil {
		t.Errorf("ToProto() with no results = %v, want nil", got)
	}
}
