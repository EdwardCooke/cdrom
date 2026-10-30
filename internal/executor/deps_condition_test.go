// Package executor — F-22 dependency-observation condition tests.
//
// These tests verify that a job's DEPENDENCIES' terminal outcomes (captured
// by the scheduler's dependency resolver and handed to the target as
// UpstreamJob entries WITH KEYS) are exposed to step conditions as
// {{ .deps.<key>.status }} / {{ .deps.<key>.outputs.<name> }} — the routing
// substrate a BRANCH job (F-22) uses to react to a failed verification.
package executor

import (
	"context"
	"log/slog"
	"testing"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TestExecuteDepsObservationRoutesOnStatus verifies that a step's condition
// CAN ROUTE ON A DEPENDENCY'S FAILURE (F-22): with an upstream observation
// (key "deploy", status FAILED), a condition that matches the failure runs
// the step, and one that demands success skips it.
func TestExecuteDepsObservationRoutesOnStatus(t *testing.T) {
	var ranMatch, ranMismatch int
	RegisterStepType("match-fail", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		ranMatch++
		return nil
	})
	RegisterStepType("need-ok", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		ranMismatch++
		return nil
	})

	upstream := []*dbpb.UpstreamJob{{
		Key:     "deploy",
		Name:    "deploy",
		Status:  dbpb.JobStatus_JOB_STATUS_FAILED,
		Outputs: map[string]string{"exit": "1"},
	}}
	ctx := ContextWithUpstreamJobs(context.Background(), upstream)

	// A step conditioned on the failure RUNS (the rollback arming case).
	match := &dbpb.JobStep{
		Type:      "match-fail",
		Condition: `{{ eq .deps.deploy.Status "failed" }}`,
	}
	if err := Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{match}}, testLogger()); err != nil {
		t.Fatalf("Execute (match-fail): %v", err)
	}
	if ranMatch != 1 {
		t.Errorf("match-fail ran %d times, want 1 (the condition saw the dependency's FAILED status)", ranMatch)
	}

	// A step demanding success SKIPS (the celebration case).
	needOk := &dbpb.JobStep{
		Type:      "need-ok",
		Condition: `{{ eq .deps.deploy.Status "succeeded" }}`,
	}
	if err := Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{needOk}}, testLogger()); err != nil {
		t.Fatalf("Execute (need-ok): %v", err)
	}
	if ranMismatch != 0 {
		t.Errorf("need-ok ran %d times, want 0 (the dependency failed)", ranMismatch)
	}
}

// TestExecuteDepsObservationExposesOutputs verifies that a dependency's
// OBSERVED OUTPUTS are template-addressable INSIDE A HANDLER (F-22): the
// handler renders {{ .deps.<key>.outputs.<name> }} against the step's
// condition context (the same data its condition uses). This is the path the
// probe/rollback handlers take (lazy-rendered params).
func TestExecuteDepsObservationExposesOutputs(t *testing.T) {
	var got string
	RegisterStepType("read-dep-output", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		condCtx := ConditionContextFromContext(ctx)
		rendered, err := RenderTemplate(`{{ .deps.build.Outputs.image }}`, condCtx)
		if err != nil {
			return err
		}
		got = rendered
		return nil
	})
	upstream := []*dbpb.UpstreamJob{{
		Key:     "build",
		Name:    "build",
		Status:  dbpb.JobStatus_JOB_STATUS_SUCCEEDED,
		Outputs: map[string]string{"image": "repo/img:v1"},
	}}
	ctx := ContextWithUpstreamJobs(context.Background(), upstream)
	step := &dbpb.JobStep{Type: "read-dep-output"}
	if err := Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute (read-dep-output): %v", err)
	}
	if got != "repo/img:v1" {
		t.Errorf("rendered = %q, want the dependency's observed output (repo/img:v1)", got)
	}
}

// TestEvaluateConditionUnknownDepKeySafeNoOp verifies the SAFE-DIRECTION
// semantics for a TYPO'D dependency key (F-22): Go templates resolve a
// missing map key to the ZERO VALUE (parity with the existing .jobs.<name>
// addressing), so a condition like {{ eq .deps.typo.Status "failed" }}
// renders FALSE and the step is SKIPPED — the rollback DISARMs rather than
// FIRES on a typo. The opposite accident (a typo arming a rollback) is
// impossible, which is the direction that matters for a destructive action.
func TestEvaluateConditionUnknownDepKeySafeNoOp(t *testing.T) {
	var ran int
	RegisterStepType("typo-guard", func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
		ran++
		return nil
	})
	step := &dbpb.JobStep{
		Type:      "typo-guard",
		Condition: `{{ eq .deps.typo.Status "failed" }}`,
	}
	upstream := []*dbpb.UpstreamJob{{Key: "deploy", Name: "deploy", Status: dbpb.JobStatus_JOB_STATUS_FAILED}}
	ctx := ContextWithUpstreamJobs(context.Background(), upstream)
	if err := Execute(ctx, &dbpb.JobSpec{Steps: []*dbpb.JobStep{step}}, testLogger()); err != nil {
		t.Fatalf("Execute = %v, want a clean skip (a typo'd key must not crash the job)", err)
	}
	if ran != 0 {
		t.Errorf("typo-guard ran %d times, want 0 (a typo'd key renders false — safe no-op)", ran)
	}
}
