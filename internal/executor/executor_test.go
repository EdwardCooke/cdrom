package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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
	collector.ReportStepStatus(0, StepStatusSucceeded, "", nil)
	collector.ReportStepStatus(1, StepStatusSkipped, "", nil)
	collector.ReportStepStatus(2, StepStatusFailed, "boom", nil)

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

// TestExecuteIgnoreFailedContinues verifies that a step that fails (or times
// out) with IgnoreFailed set does not stop the job: the failure is recorded
// and the next step still runs (F-06).
func TestExecuteIgnoreFailedContinues(t *testing.T) {
	var laterRan bool
	RegisterStepType("boom", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return errors.New("boom")
	})
	RegisterStepType("after", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		laterRan = true
		return nil
	})

	collector := &StepResultCollector{}
	ctx := ContextWithStepStatusReporter(context.Background(), collector)
	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		{Type: "boom", IgnoreFailed: true},
		{Type: "after"},
	}}
	if err := Execute(ctx, spec, testLogger()); err != nil {
		t.Fatalf("Execute: %v (an ignore_failed step must not fail the job)", err)
	}
	if !laterRan {
		t.Error("a later step did not run after an ignore_failed step failed")
	}
	results := collector.All()
	if len(results) != 2 {
		t.Fatalf("results = %+v, want two results", results)
	}
	if results[0].Status != StepStatusFailed {
		t.Errorf("results[0].Status = %v, want failed", results[0].Status)
	}
	if results[1].Status != StepStatusSucceeded {
		t.Errorf("results[1].Status = %v, want succeeded", results[1].Status)
	}
}

// TestExecuteFailedWithoutIgnoreFailedStopsJob verifies that a step that fails
// without IgnoreFailed stops the job (the default, unchanged behavior).
func TestExecuteFailedWithoutIgnoreFailedStopsJob(t *testing.T) {
	var laterRan bool
	RegisterStepType("boom2", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return errors.New("boom")
	})
	RegisterStepType("after2", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		laterRan = true
		return nil
	})

	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		{Type: "boom2"},
		{Type: "after2"},
	}}
	if err := Execute(context.Background(), spec, testLogger()); err == nil {
		t.Fatal("expected an error when a non-ignore_failed step fails, got nil")
	}
	if laterRan {
		t.Error("a later step ran after a failing (non-ignore_failed) step")
	}
}

// TestExecuteStepOutputs verifies that a step that declares outputs writes its
// output files into the per-step output directory (exposed via
// StepOutputDirEnv) and that the executor reads them back into the step's
// results and the job's aggregated outputs (F-06).
func TestExecuteStepOutputs(t *testing.T) {
	RegisterStepType("producer", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		dir := step.GetEnv()[StepOutputDirEnv]
		if dir == "" {
			return errors.New("StepOutputDirEnv not set")
		}
		if err := os.WriteFile(filepath.Join(dir, "version"), []byte("1.2.3\n"), 0o644); err != nil {
			return err
		}
		// "missing" is declared but never written: it must be recorded as "".
		return nil
	})

	collector := &StepResultCollector{}
	ctx := ContextWithStepStatusReporter(context.Background(), collector)
	step := &dbpb.JobStep{Type: "producer", Outputs: []string{"version", "missing"}}
	if err := Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	results := collector.All()
	if len(results) != 1 {
		t.Fatalf("results = %+v, want one result", results)
	}
	outputs := results[0].Outputs
	if outputs["version"] != "1.2.3" {
		t.Errorf("outputs[version] = %q, want %q (trimmed)", outputs["version"], "1.2.3")
	}
	if outputs["missing"] != "" {
		t.Errorf("outputs[missing] = %q, want empty (declared but not written)", outputs["missing"])
	}
	jobOutputs := collector.JobOutputs()
	if jobOutputs["version"] != "1.2.3" {
		t.Errorf("jobOutputs[version] = %q, want %q", jobOutputs["version"], "1.2.3")
	}
}

// TestExecuteConditionReferencesPriorStep verifies that a step's condition can
// reference the status and outputs of a prior step in the same job (F-06).
func TestExecuteConditionReferencesPriorStep(t *testing.T) {
	RegisterStepType("emit", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		dir := step.GetEnv()[StepOutputDirEnv]
		if dir == "" {
			return errors.New("StepOutputDirEnv not set")
		}
		return os.WriteFile(filepath.Join(dir, "ready"), []byte("true"), 0o644)
	})
	var gatedRan bool
	RegisterStepType("gated", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		gatedRan = true
		return nil
	})

	collector := &StepResultCollector{}
	ctx := ContextWithStepStatusReporter(context.Background(), collector)
	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		{Type: "emit", Outputs: []string{"ready"}},
		// Runs only if the prior step succeeded and produced ready == "true".
		{Type: "gated", Condition: `{{ if and (eq (index .steps 0).Status "succeeded") (eq (index .steps 0).Outputs.ready "true") }}true{{ else }}false{{ end }}`},
	}}
	if err := Execute(ctx, spec, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !gatedRan {
		t.Error("gated step did not run although the prior step succeeded with ready=true")
	}
}

// TestExecuteConditionReferencesUpstreamJob verifies that a step's condition
// can reference the status and outputs of an upstream job higher in the
// pipeline chain (F-06).
func TestExecuteConditionReferencesUpstreamJob(t *testing.T) {
	var gatedRan bool
	RegisterStepType("gated2", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		gatedRan = true
		return nil
	})

	upstream := []*dbpb.UpstreamJob{
		{Id: 1, Name: "build", Status: dbpb.JobStatus_JOB_STATUS_SUCCEEDED, Outputs: map[string]string{"artifact": "v9"}},
	}
	collector := &StepResultCollector{}
	ctx := ContextWithStepStatusReporter(context.Background(), collector)
	ctx = ContextWithUpstreamJobs(ctx, upstream)
	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		// Runs only if the upstream "build" job succeeded.
		{Type: "gated2", Condition: `{{ if eq (index .jobs 0).Status "succeeded" }}true{{ else }}false{{ end }}`},
	}}
	if err := Execute(ctx, spec, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !gatedRan {
		t.Error("gated step did not run although the upstream job succeeded")
	}
}

// TestExecuteConditionUpstreamJobFailedSkips verifies that a step whose
// condition references a failed upstream job is skipped (F-06).
func TestExecuteConditionUpstreamJobFailedSkips(t *testing.T) {
	var gatedRan bool
	RegisterStepType("gated3", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		gatedRan = true
		return nil
	})

	upstream := []*dbpb.UpstreamJob{
		{Id: 1, Name: "build", Status: dbpb.JobStatus_JOB_STATUS_FAILED},
	}
	collector := &StepResultCollector{}
	ctx := ContextWithStepStatusReporter(context.Background(), collector)
	ctx = ContextWithUpstreamJobs(ctx, upstream)
	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		{Type: "gated3", Condition: `{{ if eq (index .jobs 0).Status "succeeded" }}true{{ else }}false{{ end }}`},
	}}
	if err := Execute(ctx, spec, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gatedRan {
		t.Error("gated step ran although the upstream job failed")
	}
	results := collector.All()
	if len(results) != 1 || results[0].Status != StepStatusSkipped {
		t.Fatalf("results = %+v, want one skipped result", results)
	}
}

// fakeStepBarrier is a StepBarrier for tests: it records the step indices it
// is asked to synchronize on and returns a configurable error (to simulate a
// cancellation or failure while waiting at the barrier).
type fakeStepBarrier struct {
	synced []int
	err    error
}

func (b *fakeStepBarrier) SyncStep(ctx context.Context, stepIndex int) error {
	b.synced = append(b.synced, stepIndex)
	return b.err
}

// TestExecuteStepBarrierSyncsBetweenSteps verifies that, when a step barrier is
// set in the context, Execute calls SyncStep after each step that is followed
// by another step (not after the last step), and that the job succeeds when
// the barrier is satisfied.
func TestExecuteStepBarrierSyncsBetweenSteps(t *testing.T) {
	RegisterStepType("barrier-step", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return nil
	})
	barrier := &fakeStepBarrier{}
	ctx := ContextWithStepBarrier(context.Background(), barrier)
	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		{Type: "barrier-step"},
		{Type: "barrier-step"},
		{Type: "barrier-step"},
	}}
	if err := Execute(ctx, spec, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// SyncStep is called after steps 0 and 1 (each is followed by another
	// step), but not after step 2 (the last step).
	if len(barrier.synced) != 2 || barrier.synced[0] != 0 || barrier.synced[1] != 1 {
		t.Errorf("barrier.synced = %v, want [0 1]", barrier.synced)
	}
}

// TestExecuteStepBarrierNoBarrierWhenUnset verifies that, when no step barrier
// is set in the context, Execute does not synchronize between steps (a job on
// a single target runs its steps unbarriered).
func TestExecuteStepBarrierNoBarrierWhenUnset(t *testing.T) {
	RegisterStepType("barrier-step2", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return nil
	})
	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		{Type: "barrier-step2"},
		{Type: "barrier-step2"},
	}}
	// No barrier in the context: Execute must not attempt to synchronize.
	if err := Execute(context.Background(), spec, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
}

// TestExecuteStepBarrierCancelledStopsJob verifies that a barrier that reports
// the job was cancelled (ErrCancelled) stops the job with that error, so the
// worker can report the job cancelled (distinct from a failure).
func TestExecuteStepBarrierCancelledStopsJob(t *testing.T) {
	RegisterStepType("barrier-step3", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		return nil
	})
	barrier := &fakeStepBarrier{err: fmt.Errorf("%w: job cancelled at barrier", ErrCancelled)}
	ctx := ContextWithStepBarrier(context.Background(), barrier)
	spec := &dbpb.JobSpec{Steps: []*dbpb.JobStep{
		{Type: "barrier-step3"},
		{Type: "barrier-step3"},
	}}
	err := Execute(ctx, spec, testLogger())
	if err == nil {
		t.Fatal("expected an error when the barrier reports cancellation, got nil")
	}
	if !errors.Is(err, ErrCancelled) {
		t.Errorf("error = %v, want it to wrap ErrCancelled", err)
	}
}
