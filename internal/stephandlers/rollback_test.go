// Package stephandlers — F-22 rollback handler tests.
//
// These tests exercise the built-in "rollback" step handler against a FAKE
// Restorer (stubbing the API): initiation publishes the resumption run id as
// a step output; the wait loop maps the resumption's verdict onto the step's
// outcome (succeeded → step success; failed → step failure); wait-disabled
// returns immediately after initiation.
package stephandlers

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fakeRestorer is a stub executor.Restorer for the rollback handler tests.
type fakeRestorer struct {
	restoreCalled bool
	fromJob       string
	failInit      error
	statusSeq     []string // successive RunStatus readings
	read          int
}

func (f *fakeRestorer) InitiateRestore(ctx context.Context, fromJob string) (int64, error) {
	f.restoreCalled = true
	f.fromJob = fromJob
	if f.failInit != nil {
		return 0, f.failInit
	}
	return 4242, nil
}

func (f *fakeRestorer) RunStatus(ctx context.Context, runID int64) (string, error) {
	if runID != 4242 {
		return "", fmt.Errorf("unexpected run id %d", runID)
	}
	i := f.read
	if i >= len(f.statusSeq) {
		i = len(f.statusSeq) - 1
	}
	f.read++
	return f.statusSeq[i], nil
}

func rollbackLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}

// shortenRollbackPoll speeds up the wait-loop tests: the poll interval is a
// package variable so tests shrink it (house convention, cf. the resolver's
// dependencyResolverInterval).
func shortenRollbackPoll(t *testing.T) {
	t.Helper()
	old := rollbackPollInterval
	rollbackPollInterval = 5 * time.Millisecond
	t.Cleanup(func() { rollbackPollInterval = old })
}

// TestRollbackInitiatesDetachedByDefault verifies the DEFAULT SAFETY POSTURE
// (F-22): the handler initiates the resumption and RETURNS WITHOUT POLLING —
// the default is wait: false, because a waiting rollback step holds its
// worker's execution slot and can STARVE the resumption it monitors (the
// deadlock hazard). The resumption's health lives in the run timeline.
func TestRollbackInitiatesDetachedByDefault(t *testing.T) {
	fr := &fakeRestorer{statusSeq: []string{"pending", "running", "succeeded"}}
	ctx := executor.ContextWithRestorer(context.Background(), fr)

	outDir := t.TempDir()
	step := &dbpb.JobStep{
		Type: TypeRollback,
		Env:  map[string]string{executor.StepOutputDirEnv: outDir},
		Params: map[string]*dbpb.ParamValue{
			"from_job": {String_: "deploy"},
		},
	}
	if err := runRollbackStep(ctx, step, rollbackLogger()); err != nil {
		t.Fatalf("runRollbackStep = %v, want success (detached initiation)", err)
	}
	if !fr.restoreCalled || fr.fromJob != "deploy" {
		t.Fatalf("InitiateRestore(fromJob=%q, called=%v), want (deploy, true)", fr.fromJob, fr.restoreCalled)
	}
	if fr.read != 0 {
		t.Errorf("RunStatus read %d times, want 0 (the default must not poll — deadlock hazard)", fr.read)
	}
	data, err := os.ReadFile(filepath.Join(outDir, "restore_run_id"))
	if err != nil {
		t.Fatalf("restore_run_id output missing: %v", err)
	}
	if string(data) != "4242" {
		t.Errorf("restore_run_id = %q, want 4242", string(data))
	}
}

// TestRollbackOptInWaitSucceeds verifies the OPT-IN wait (F-22): with
// wait: true the handler polls the resumption and succeeds on its success.
func TestRollbackOptInWaitSucceeds(t *testing.T) {
	shortenRollbackPoll(t)
	fr := &fakeRestorer{statusSeq: []string{"pending", "running", "succeeded"}}
	ctx := executor.ContextWithRestorer(context.Background(), fr)

	outDir := t.TempDir()
	step := &dbpb.JobStep{
		Type: TypeRollback,
		Env:  map[string]string{executor.StepOutputDirEnv: outDir},
		Params: map[string]*dbpb.ParamValue{
			"from_job": {String_: "deploy"},
			"wait":     {String_: "true"},
		},
	}
	if err := runRollbackStep(ctx, step, rollbackLogger()); err != nil {
		t.Fatalf("runRollbackStep = %v, want success (the resumption succeeded)", err)
	}
	if fr.read < 3 {
		t.Errorf("RunStatus read %d times, want ≥3 (the opt-in wait polls to the verdict)", fr.read)
	}
	data, err := os.ReadFile(filepath.Join(outDir, "restore_status"))
	if err != nil {
		t.Fatalf("restore_status output missing: %v", err)
	}
	if string(data) != "succeeded" {
		t.Errorf("restore_status = %q, want succeeded", string(data))
	}
}

// TestRollbackBotchedResumptionFails verifies the LOUDNESS RULE (F-22): a
// resumption that FAILS makes the rollback step FAIL (a botched rollback must
// fail the enclosing run, never pass silently).
func TestRollbackBotchedResumptionFails(t *testing.T) {
	shortenRollbackPoll(t)
	fr := &fakeRestorer{statusSeq: []string{"running", "failed"}}
	ctx := executor.ContextWithRestorer(context.Background(), fr)
	step := &dbpb.JobStep{
		Type: TypeRollback,
		Params: map[string]*dbpb.ParamValue{
			"from_job": {String_: "deploy"},
			"wait":     {String_: "true"},
		},
	}
	err := runRollbackStep(ctx, step, rollbackLogger())
	if err == nil {
		t.Fatal("runRollbackStep = nil, want a failure (the resumption failed)")
	}
	if fr.read < 2 {
		t.Errorf("RunStatus read %d times, want ≥2 (the handler waited on the verdict)", fr.read)
	}
}

// TestRollbackFireAndForget verifies wait: false RETURNS IMMEDIATELY after
// initiation (the resumption continues unattached; its verdict is not the
// step's verdict).
func TestRollbackFireAndForget(t *testing.T) {
	fr := &fakeRestorer{statusSeq: []string{"pending"}}
	ctx := executor.ContextWithRestorer(context.Background(), fr)
	step := &dbpb.JobStep{
		Type: TypeRollback,
		Params: map[string]*dbpb.ParamValue{
			"from_job": {String_: "deploy"},
			"wait":     {String_: "false"},
		},
	}
	if err := runRollbackStep(ctx, step, rollbackLogger()); err != nil {
		t.Fatalf("runRollbackStep = %v, want success (fire-and-forget)", err)
	}
	if !fr.restoreCalled {
		t.Fatal("InitiateRestore was not called")
	}
	if fr.read != 0 {
		t.Errorf("RunStatus read %d times, want 0 (wait: false must not poll)", fr.read)
	}
}

// TestRollbackInitiationGuardSurfaces verifies that a GUARD REJECTION from
// the API (e.g. no re-arm, cooldown) SURFACES as a step failure with the
// server's message (the operator sees WHY the rollback was refused).
func TestRollbackInitiationGuardSurfaces(t *testing.T) {
	fr := &fakeRestorer{failInit: fmt.Errorf("rollbacks do not re-arm")}
	ctx := executor.ContextWithRestorer(context.Background(), fr)
	step := &dbpb.JobStep{
		Type:   TypeRollback,
		Params: map[string]*dbpb.ParamValue{"from_job": {String_: "deploy"}},
	}
	err := runRollbackStep(ctx, step, rollbackLogger())
	if err == nil {
		t.Fatal("runRollbackStep = nil, want the guard rejection")
	}
	if got := err.Error(); got == "" {
		t.Fatal("empty error")
	}
}

// TestRollbackUnsupportedTarget verifies the PARITY RULE (F-22): an execution
// target that does not wire a Restorer reports a clear "not supported" error
// (like the email handler does without a Notifier).
func TestRollbackUnsupportedTarget(t *testing.T) {
	step := &dbpb.JobStep{
		Type:   TypeRollback,
		Params: map[string]*dbpb.ParamValue{"from_job": {String_: "deploy"}},
	}
	err := runRollbackStep(context.Background(), step, rollbackLogger())
	if err == nil {
		t.Fatal("runRollbackStep = nil, want 'not supported by this execution target'")
	}
}

// TestRollbackTemplatedCutPoint verifies the cut point is TEMPLATE-ADDRESSABLE
// (F-22): a from_job citing a run parameter renders before initiation.
func TestRollbackTemplatedCutPoint(t *testing.T) {
	fr := &fakeRestorer{statusSeq: []string{"succeeded"}}
	ctx := executor.ContextWithRestorer(context.Background(), fr)
	// Stand-alone handler test: the executor normally builds the condition
	// context (run params, secrets, …); here we attach it DIRECTLY so the
	// lazy-rendered from_job can cite a run parameter.
	ctx = executor.ContextWithConditionContext(ctx, map[string]any{
		"params": map[string]string{"seg": "prod"},
	})
	step := &dbpb.JobStep{
		Type: TypeRollback,
		Params: map[string]*dbpb.ParamValue{
			"from_job": {String_: "{{ .params.seg }}-green"},
		},
	}
	if err := runRollbackStep(ctx, step, rollbackLogger()); err != nil {
		t.Fatalf("runRollbackStep = %v, want success", err)
	}
	if fr.fromJob != "prod-green" {
		t.Errorf("InitiateRestore cut point = %q, want the rendered prod-green", fr.fromJob)
	}
}
