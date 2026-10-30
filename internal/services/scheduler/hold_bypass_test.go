// Package scheduler — F-22 hold-bypass tests.
//
// These tests verify the SLICE-A RULING on environment gates meeting a
// resumed branch: a rollback-origin job is EXEMPT from a manual
// environment's proceed hold (the initiation IS the prompt; re-holding would
// strand an unattended automatic rollback and would prevent the branch from
// reaching the approval gate, which is still honored at hand-off), while an
// ORDINARY job targeting the same manual environment is held.
package scheduler

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// holdBypassDB is a minimal DatabaseClient stub for the hold-bypass tests: it
// serves GetEnvironment (the only read shouldHoldForProceed performs).
type holdBypassDB struct {
	dbpb.DatabaseClient // nil; only GetEnvironment is used
	auto                dbpb.Automation
}

func (f *holdBypassDB) GetEnvironment(ctx context.Context, in *dbpb.GetEnvironmentRequest, opts ...grpc.CallOption) (*dbpb.Environment, error) {
	return &dbpb.Environment{Name: in.GetName(), Automation: f.auto}, nil
}

// TestShouldHoldForProceedRollbackOriginExempt verifies the F-22 CARVE-OUT:
// a job whose denormalized TriggerType is "rollback" is NOT held by a manual
// environment.
func TestShouldHoldForProceedRollbackOriginExempt(t *testing.T) {
	s := NewServer(&holdBypassDB{auto: dbpb.Automation_AUTOMATION_MANUAL}, testLogger())
	job := &dbpb.Job{
		Id:          1,
		Environment: "prod",
		TriggerType: "rollback",
		Automation:  dbpb.Automation_AUTOMATION_UNSPECIFIED,
	}
	if s.shouldHoldForProceed(context.Background(), job) {
		t.Fatal("shouldHoldForProceed = true for a rollback-origin job; a resumption must not stall at a proceed hold (it would never reach the approval gate)")
	}
}

// TestShouldHoldForProceedOrdinaryJobStillHeld verifies the carve-out is
// LIMITED to rollback origins: an ordinary job targeting a manual environment
// is held (F-17 behavior preserved).
func TestShouldHoldForProceedOrdinaryJobStillHeld(t *testing.T) {
	s := NewServer(&holdBypassDB{auto: dbpb.Automation_AUTOMATION_MANUAL}, testLogger())
	job := &dbpb.Job{
		Id:          1,
		Environment: "prod",
		TriggerType: "manual",
		Automation:  dbpb.Automation_AUTOMATION_UNSPECIFIED,
	}
	if !s.shouldHoldForProceed(context.Background(), job) {
		t.Fatal("shouldHoldForProceed = false for an ordinary job in a manual environment; the F-17 hold must still apply")
	}
}

// TestShouldHoldForProceedAutomaticEnvNeverHolds verifies the baseline (F-17):
// an automatic environment holds nothing, rollback-origin or not.
func TestShouldHoldForProceedAutomaticEnvNeverHolds(t *testing.T) {
	s := NewServer(&holdBypassDB{auto: dbpb.Automation_AUTOMATION_AUTOMATIC}, testLogger())
	for _, tt := range []struct {
		name        string
		triggerType string
	}{
		{"ordinary", "manual"},
		{"rollback-origin", "rollback"},
	} {
		job := &dbpb.Job{
			Id:          1,
			Environment: "dev",
			TriggerType: tt.triggerType,
			Automation:  dbpb.Automation_AUTOMATION_UNSPECIFIED,
		}
		if s.shouldHoldForProceed(context.Background(), job) {
			t.Errorf("[%s] shouldHoldForProceed = true in an automatic environment; want false", tt.name)
		}
	}
}
