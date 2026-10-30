// Package api — F-22 restoration mediation tests.
//
// These tests exercise the API's InitiateRestore gRPC handler (the ARMED door
// into restoration) against stubbed Database/Scheduler clients: job-token
// containment, the no-re-arm guard, the cooldown guard, anchor absence, and
// the happy path. Job-token auth is DISABLED in these tests (checkJobToken is
// a no-op), so the guards under test are the SEMANTIC ones, not the crypto.
package api

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/config"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	schedpb "cdrom/internal/gen/cdrom/scheduler/v1"
)

// restorationStubDB extends fakeDatabase with the canned responses the
// restoration mediation reads.
type restorationStubDB struct {
	fakeDatabase
	callJob   *dbpb.Job
	callRun   *dbpb.PipelineRun
	anchor    *dbpb.PipelineRun
	anchorErr error
	latestRB  *dbpb.PipelineRun
	latestErr error
}

func (f *restorationStubDB) GetJob(ctx context.Context, in *dbpb.GetJobRequest, opts ...grpc.CallOption) (*dbpb.Job, error) {
	if f.callJob != nil {
		return f.callJob, nil
	}
	return &dbpb.Job{Id: in.GetId()}, nil
}

func (f *restorationStubDB) GetRun(ctx context.Context, in *dbpb.GetRunRequest, opts ...grpc.CallOption) (*dbpb.PipelineRun, error) {
	if f.callRun != nil {
		return f.callRun, nil
	}
	return &dbpb.PipelineRun{Id: in.GetId(), Trigger: "manual"}, nil
}

func (f *restorationStubDB) PreviousSuccessfulRun(ctx context.Context, in *dbpb.PreviousSuccessfulRunRequest, opts ...grpc.CallOption) (*dbpb.PipelineRun, error) {
	if f.anchorErr != nil {
		return nil, f.anchorErr
	}
	if f.anchor != nil {
		return f.anchor, nil
	}
	return &dbpb.PipelineRun{Id: 100, PipelineId: in.GetPipelineId()}, nil
}

func (f *restorationStubDB) LatestRollbackRun(ctx context.Context, in *dbpb.LatestRollbackRunRequest, opts ...grpc.CallOption) (*dbpb.PipelineRun, error) {
	if f.latestErr != nil {
		return nil, f.latestErr
	}
	return f.latestRB, nil
}

// newRestorationServer builds a GRPCServer wired with the restoration stubs.
func newRestorationServer(t *testing.T, db *restorationStubDB, sched *fakeScheduler) *GRPCServer {
	t.Helper()
	s := NewGRPCServer(db, nil, nil, nil, nil, slog.New(slog.NewTextHandler(bytes.NewBuffer(make([]byte, 0, 1<<20)), nil)))
	s.SetSchedulerClient(sched)
	s.SetRollbackConfig(config.RollbackConfig{Cooldown: time.Minute})
	return s
}

// TestInitiateRestoreHappyPath verifies the armed door's HAPPY PATH (F-22):
// the anchor is derived from the CALLER's pipeline (containment), the
// scheduler is asked to restart from the requested cut point, and the
// response carries the new run and the anchor.
func TestInitiateRestoreHappyPath(t *testing.T) {
	db := &restorationStubDB{
		fakeDatabase: fakeDatabase{},
		callJob:      &dbpb.Job{Id: 7, RunId: 50, PipelineId: 9},
		callRun:      &dbpb.PipelineRun{Id: 50, Trigger: "manual"},
		anchor:       &dbpb.PipelineRun{Id: 100, PipelineId: 9},
	}
	sched := &fakeScheduler{run: &schedpb.Run{Id: 200, Trigger: "rollback"}}
	s := newRestorationServer(t, db, sched)

	resp, err := s.InitiateRestore(context.Background(), &apipb.InitiateRestoreRequest{JobId: 7, FromJob: "deploy"})
	if err != nil {
		t.Fatalf("InitiateRestore: %v", err)
	}
	if resp.GetRunId() != 200 {
		t.Errorf("run_id = %d, want 200 (the scheduler's new run)", resp.GetRunId())
	}
	if resp.GetAnchorRunId() != 100 {
		t.Errorf("anchor_run_id = %d, want 100", resp.GetAnchorRunId())
	}
	// Containment: the scheduler was asked about the ANCHOR (100), not the
	// caller's run (50), and with the requested cut point.
	if sched.lastRestart == nil {
		t.Fatal("scheduler.RestartRun was not called")
	}
	if sched.lastRestart.AnchorRunId != 100 {
		t.Errorf("RestartRun anchor = %d, want 100", sched.lastRestart.AnchorRunId)
	}
	if sched.lastRestart.FromJobKey != "deploy" {
		t.Errorf("RestartRun from_job = %q, want deploy", sched.lastRestart.FromJobKey)
	}
}

// TestInitiateRestoreNoReArm verifies the DEPTH≤1 GUARD (F-22): a caller whose
// run is itself rollback-origin is refused — a resumption re-executes the
// anchor's suffix (which includes the armed rollback job), so permitting it
// would descend generations.
func TestInitiateRestoreNoReArm(t *testing.T) {
	db := &restorationStubDB{
		fakeDatabase: fakeDatabase{},
		callJob:      &dbpb.Job{Id: 7, RunId: 50, PipelineId: 9},
		callRun:      &dbpb.PipelineRun{Id: 50, Trigger: "rollback"},
	}
	sched := &fakeScheduler{}
	s := newRestorationServer(t, db, sched)

	_, err := s.InitiateRestore(context.Background(), &apipb.InitiateRestoreRequest{JobId: 7, FromJob: "deploy"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("InitiateRestore (rollback-origin caller) = %v, want FailedPrecondition", err)
	}
	if sched.lastRestart != nil {
		t.Errorf("scheduler.RestartRun WAS called; a re-arm must be refused before spawning")
	}
}

// TestInitiateRestoreCooldown verifies the FLAP-SUPPRESSION GUARD (F-22): a
// rollback-origin run started MORE recently than the cooldown is refused.
func TestInitiateRestoreCooldown(t *testing.T) {
	recent := time.Now().Add(-30 * time.Second)
	db := &restorationStubDB{
		fakeDatabase: fakeDatabase{},
		callJob:      &dbpb.Job{Id: 7, RunId: 50, PipelineId: 9},
		callRun:      &dbpb.PipelineRun{Id: 50, Trigger: "manual"},
		latestRB:     &dbpb.PipelineRun{Id: 150, StartedAt: ts(recent)},
	}
	sched := &fakeScheduler{}
	s := newRestorationServer(t, db, sched)

	_, err := s.InitiateRestore(context.Background(), &apipb.InitiateRestoreRequest{JobId: 7, FromJob: "deploy"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("InitiateRestore (within cooldown) = %v, want FailedPrecondition", err)
	}
	if sched.lastRestart != nil {
		t.Errorf("scheduler.RestartRun WAS called; a cooled-down pipeline must be refused")
	}
}

// TestInitiateRestoreCooldownExpired verifies that a rollback OLDER than the
// cooldown does NOT block a new initiation (the guard is a spacing floor, not
// a latch).
func TestInitiateRestoreCooldownExpired(t *testing.T) {
	old := time.Now().Add(-10 * time.Minute)
	db := &restorationStubDB{
		fakeDatabase: fakeDatabase{},
		callJob:      &dbpb.Job{Id: 7, RunId: 50, PipelineId: 9},
		callRun:      &dbpb.PipelineRun{Id: 50, Trigger: "manual"},
		latestRB:     &dbpb.PipelineRun{Id: 150, StartedAt: ts(old)},
	}
	sched := &fakeScheduler{run: &schedpb.Run{Id: 200}}
	s := newRestorationServer(t, db, sched)

	if _, err := s.InitiateRestore(context.Background(), &apipb.InitiateRestoreRequest{JobId: 7, FromJob: "deploy"}); err != nil {
		t.Fatalf("InitiateRestore (expired cooldown) = %v, want success", err)
	}
	if sched.lastRestart == nil {
		t.Fatal("scheduler.RestartRun was not called")
	}
}

// TestInitiateRestoreNoAnchor verifies the NO-ANCHOR CASE (F-22): a pipeline
// with no successful, non-rollback-origin run reports NotFound (nothing to
// restore to — e.g. the first-ever deploy).
func TestInitiateRestoreNoAnchor(t *testing.T) {
	db := &restorationStubDB{
		fakeDatabase: fakeDatabase{},
		callJob:      &dbpb.Job{Id: 7, RunId: 50, PipelineId: 9},
		callRun:      &dbpb.PipelineRun{Id: 50, Trigger: "manual"},
		anchorErr:    status.Error(codes.NotFound, "pipeline 9 has no successful run to restore to"),
	}
	sched := &fakeScheduler{}
	s := newRestorationServer(t, db, sched)

	_, err := s.InitiateRestore(context.Background(), &apipb.InitiateRestoreRequest{JobId: 7, FromJob: "deploy"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("InitiateRestore (no anchor) = %v, want NotFound", err)
	}
	if sched.lastRestart != nil {
		t.Errorf("scheduler.RestartRun WAS called; no anchor means no resumption")
	}
}

// TestInitiateRestoreBadCutPoint verifies the BAD-CUT-POINT PROPAGATION (F-22):
// a from_job absent from the anchor's jobs surfaces as NotFound (the
// scheduler/database reject it).
func TestInitiateRestoreBadCutPoint(t *testing.T) {
	db := &restorationStubDB{
		fakeDatabase: fakeDatabase{},
		callJob:      &dbpb.Job{Id: 7, RunId: 50, PipelineId: 9},
		callRun:      &dbpb.PipelineRun{Id: 50, Trigger: "manual"},
		anchor:       &dbpb.PipelineRun{Id: 100, PipelineId: 9},
	}
	sched := &fakeScheduler{restartErr: status.Error(codes.NotFound, `cut point "ghost" not found among the jobs of run #100`)}
	s := newRestorationServer(t, db, sched)

	_, err := s.InitiateRestore(context.Background(), &apipb.InitiateRestoreRequest{JobId: 7, FromJob: "ghost"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("InitiateRestore (bad cut point) = %v, want NotFound", err)
	}
}

// TestInitiateRestoreCallerOutsideARun verifies the STANDALONE-JOB REJECTION
// (F-22): a job that is not part of a pipeline run cannot restore (there is no
// pipeline to anchor against).
func TestInitiateRestoreCallerOutsideARun(t *testing.T) {
	db := &restorationStubDB{
		fakeDatabase: fakeDatabase{},
		callJob:      &dbpb.Job{Id: 7, RunId: 0, PipelineId: 9},
	}
	sched := &fakeScheduler{}
	s := newRestorationServer(t, db, sched)

	_, err := s.InitiateRestore(context.Background(), &apipb.InitiateRestoreRequest{JobId: 7, FromJob: "deploy"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("InitiateRestore (standalone job) = %v, want FailedPrecondition", err)
	}
}

func ts(tm time.Time) *timestamppb.Timestamp {
	return timestamppb.New(tm)
}
