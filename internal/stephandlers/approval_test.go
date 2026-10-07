package stephandlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fakeApproval is a stub executor.Approval for step-handler tests: it records
// the message each ReportAwaitingApproval reported and returns a configured
// CheckApprovalResult (so a test can simulate an approval, a rejection, or a
// cancellation).
type fakeApproval struct {
	messages []string
	result   executor.CheckApprovalResult
	checkErr error
}

func (f *fakeApproval) ReportAwaitingApproval(ctx context.Context, message string) error {
	f.messages = append(f.messages, message)
	return nil
}

func (f *fakeApproval) CheckApproval(ctx context.Context) (executor.CheckApprovalResult, error) {
	if f.checkErr != nil {
		return executor.CheckApprovalResult{}, f.checkErr
	}
	return f.result, nil
}

// approvalStep builds an approval step with an optional message param.
func approvalStep(message string) *dbpb.JobStep {
	params := map[string]*dbpb.ParamValue{}
	if message != "" {
		params[ParamMessage] = stringParam(message)
	}
	return &dbpb.JobStep{Type: TypeApproval, Params: params}
}

// TestApprovalStepApproved verifies that an approval step that is approved
// reports the job as awaiting approval (with the rendered message) and then
// succeeds (the job continues).
func TestApprovalStepApproved(t *testing.T) {
	gate := &fakeApproval{result: executor.CheckApprovalResult{Resolved: true, Decision: "approved"}}
	collector := &executor.StepResultCollector{}
	ctx := executor.ContextWithApproval(context.Background(), gate)
	ctx = executor.ContextWithStepStatusReporter(ctx, collector)

	step := approvalStep("please approve")
	if err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(gate.messages) != 1 || gate.messages[0] != "please approve" {
		t.Errorf("reported messages = %v, want [please approve]", gate.messages)
	}
	results := collector.All()
	if len(results) != 1 {
		t.Fatalf("results = %+v, want one result", results)
	}
	if results[0].Status != executor.StepStatusSucceeded {
		t.Errorf("step status = %q, want succeeded", results[0].Status)
	}
}

// TestApprovalStepRejected verifies that an approval step that is rejected
// fails the job (the step fails and the job stops).
func TestApprovalStepRejected(t *testing.T) {
	gate := &fakeApproval{result: executor.CheckApprovalResult{Resolved: true, Decision: "rejected"}}
	ctx := executor.ContextWithApproval(context.Background(), gate)

	step := approvalStep("please approve")
	if err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err == nil {
		t.Fatal("expected an error for a rejected approval, got nil")
	}
}

// TestApprovalStepCancelled verifies that a job cancelled while waiting at the
// gate is reported cancelled (executor.ErrCancelled), distinct from a failure.
func TestApprovalStepCancelled(t *testing.T) {
	gate := &fakeApproval{result: executor.CheckApprovalResult{Cancelled: true}}
	ctx := executor.ContextWithApproval(context.Background(), gate)

	step := approvalStep("please approve")
	err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger())
	if err == nil {
		t.Fatal("expected an error for a cancelled approval, got nil")
	}
	if !errors.Is(err, executor.ErrCancelled) {
		t.Errorf("error = %v, want it to wrap executor.ErrCancelled", err)
	}
}

// TestApprovalStepRequiresGate verifies that an approval step fails when the
// execution target provides no Approval (the handler cannot pause the job on
// the target's behalf).
func TestApprovalStepRequiresGate(t *testing.T) {
	// No Approval in the context.
	step := approvalStep("please approve")
	if err := executor.Execute(context.Background(), &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err == nil {
		t.Fatal("expected an error when no Approval is set, got nil")
	}
}

// TestApprovalStepMessageReferencesSecret verifies that the approval message
// can reference a pipeline secret ({{ .secrets.name }}), the same interpolation
// the run's spec fields (F-10) use.
func TestApprovalStepMessageReferencesSecret(t *testing.T) {
	gate := &fakeApproval{result: executor.CheckApprovalResult{Resolved: true, Decision: "approved"}}
	ctx := executor.ContextWithApproval(context.Background(), gate)
	ctx = executor.ContextWithRunInfo(ctx, executor.RunInfo{Secrets: map[string]string{"deploy_key": "s3cr3t"}})

	step := approvalStep("deploying with key {{ .secrets.deploy_key }}")
	if err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(gate.messages) != 1 || gate.messages[0] != "deploying with key s3cr3t" {
		t.Errorf("reported messages = %v, want [deploying with key s3cr3t]", gate.messages)
	}
}

// TestApprovalStepMessageReferencesJob verifies that the approval message can
// reference the job's identity ({{ .job.ID }}), the same interpolation a step's
// condition (F-06) uses.
func TestApprovalStepMessageReferencesJob(t *testing.T) {
	gate := &fakeApproval{result: executor.CheckApprovalResult{Resolved: true, Decision: "approved"}}
	ctx := executor.ContextWithApproval(context.Background(), gate)
	ctx = executor.ContextWithJobIdentity(ctx, executor.JobIdentity{ID: 42, Name: "deploy"})

	step := approvalStep("approving job {{ .job.ID }} ({{ .job.Name }})")
	if err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(gate.messages) != 1 || gate.messages[0] != "approving job 42 (deploy)" {
		t.Errorf("reported messages = %v, want [approving job 42 (deploy)]", gate.messages)
	}
}

// TestApprovalStepTimeout verifies that an approval step whose step timeout
// expires while waiting at the gate is reported timed_out (executor.ErrTimeout),
// like any other step that exceeds its timeout.
func TestApprovalStepTimeout(t *testing.T) {
	// The gate never resolves, so the handler keeps polling until the step's
	// timeout expires.
	gate := &fakeApproval{result: executor.CheckApprovalResult{}}
	ctx := executor.ContextWithApproval(context.Background(), gate)

	step := approvalStep("please approve")
	step.Timeout = durationpb.New(50 * time.Millisecond)
	err := executor.Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger())
	if err == nil {
		t.Fatal("expected an error for a timed-out approval, got nil")
	}
	if !errors.Is(err, executor.ErrTimeout) {
		t.Errorf("error = %v, want it to wrap executor.ErrTimeout", err)
	}
}
