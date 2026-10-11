// Package scheduler — concurrency admission (F-16).
//
// The admission gate is the scheduler-side mechanism that bounds how much
// runs in parallel. Before a job is dispatched, the scheduler asks the
// Database service to admit it (AcquireJobSlots): the database holds one slot
// per resource the job consumes (its concurrency keys, its target group's cap
// when the group has a limit, and the global cap when set) and marks the job
// admitted. A job is dispatched only when it is admitted (every resource it
// consumes has room); when a resource is at its limit the job is left pending
// and the background admission loop re-checks it once a slot frees.
//
// The gate applies to every dispatch path (SubmitJob, CreateRun/TriggerRun,
// the dependency resolver's release, the retry loop, and RerunJob) and to
// both worker-group jobs and ephemeral-agent jobs. The Database service owns
// the durable, atomic slot accounting, so multiple scheduler replicas can
// never double-allocate the last free slot, and a restart re-derives held
// slots from the live job rows.
package scheduler

import (
	"context"
	"time"

	"google.golang.org/grpc"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// admissionInterval is how often the admission loop re-checks pending jobs
// (to dispatch the ones whose concurrency keys / group / global slots have
// freed) and reconciles (to release the slots of jobs that reached a terminal
// state but still hold them). It is a variable (not a constant) so tests can
// shorten it.
var admissionInterval = 5 * time.Second

// concurrencyAdmitter is the subset of the Database client the admission gate
// and loop use: to admit a job (AcquireJobSlots), to reconcile (release the
// slots of stuck jobs), to list pending jobs, and to append an "assignment"
// event for a dispatched job. The concrete dbpb.DatabaseClient satisfies it;
// tests inject a fake.
type concurrencyAdmitter interface {
	AcquireJobSlots(ctx context.Context, in *dbpb.AcquireJobSlotsRequest, opts ...grpc.CallOption) (*dbpb.AcquireJobSlotsResponse, error)
	ReconcileConcurrency(ctx context.Context, in *dbpb.ReconcileConcurrencyRequest, opts ...grpc.CallOption) (*dbpb.ReconcileConcurrencyResponse, error)
	ListJobs(ctx context.Context, in *dbpb.ListJobsRequest, opts ...grpc.CallOption) (*dbpb.ListJobsResponse, error)
	PublishAssignment(ctx context.Context, in *dbpb.PublishAssignmentRequest, opts ...grpc.CallOption) (*dbpb.PublishEventResponse, error)
}

// tryAdmit asks the Database service to admit a job (F-16) and reports whether
// it was admitted by this call. A job is admitted only when it fits within
// every limit it is subject to (its concurrency keys, its target group's cap,
// and the global cap); when a resource is at its limit the job is not admitted
// (it stays pending and the background admission loop re-checks it once a slot
// frees). When the job is already admitted (an idempotent no-op) it is not
// admitted again, so the caller must not dispatch it a second time. It returns
// true (admitted, dispatch the job) only when this call performed the
// admission. It is a no-op (returns true) when the Database client is nil
// (tests that do not exercise admission, or a deployment with no limits).
func (s *Server) tryAdmit(ctx context.Context, job *dbpb.Job) bool {
	if s.db == nil {
		return true
	}
	resp, err := s.db.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{
		JobId:   job.GetId(),
		Attempt: job.GetAttempt(),
	})
	if err != nil {
		s.logger.Warn("scheduler: admit job", "job", job.GetId(), "err", err)
		return false
	}
	if !resp.GetAcquired() {
		// The job was not admitted by this call: either it is blocked by a
		// resource at its limit (it stays pending for the admission loop) or
		// it was already admitted (a no-op; do not dispatch again).
		if len(resp.GetBlockedBy()) > 0 {
			s.logger.Info("scheduler: job held pending concurrency", "job", job.GetId(), "blocked_by", resp.GetBlockedBy())
		}
		return false
	}
	return true
}

// StartAdmissionLoop runs the concurrency admission loop in the background
// until ctx is cancelled. It is leader-gated (started only while the
// scheduler holds the leader lease, like the other background loops): it
// periodically reconciles (releasing the slots of any job that reached a
// terminal state but still holds them, a safety net for a missed release) and
// admits pending, dependency-free jobs that are not yet admitted (dispatching
// the ones that fit within the limits). It is a no-op when the Database
// client is nil (e.g. in tests that do not exercise the loop).
func (s *Server) StartAdmissionLoop(ctx context.Context) {
	if s.db == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(admissionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.runAdmission(ctx, s.db)
			}
		}
	}()
}

// runAdmission performs one pass of the admission loop: it reconciles (releases
// the slots of stuck jobs) and then admits every pending, dependency-free job
// that is not yet admitted. db is the Database client (the concrete client in
// production, a fake in tests).
func (s *Server) runAdmission(ctx context.Context, db concurrencyAdmitter) {
	// Reconcile first: release the slots of any job that reached a terminal
	// state but still holds them (a missed release, e.g. a crash between the
	// status change and the release), so a stuck job cannot wedge a resource.
	if _, err := db.ReconcileConcurrency(ctx, &dbpb.ReconcileConcurrencyRequest{}); err != nil {
		s.logger.Warn("scheduler: reconcile concurrency", "err", err)
	}
	// Admit the pending, dependency-free jobs that are not yet admitted. A job
	// with dependencies is held by the dependency resolver (it is not
	// admitted until its dependencies succeed); a job that is already admitted
	// is skipped (it was dispatched by a dispatch path or an earlier pass).
	response, err := db.ListJobs(ctx, &dbpb.ListJobsRequest{Status: dbpb.JobStatus_JOB_STATUS_PENDING})
	if err != nil {
		s.logger.Warn("scheduler: admission list pending jobs", "err", err)
		return
	}
	for _, job := range response.GetJobs() {
		if len(job.GetDependsOn()) > 0 {
			continue // held by the dependency resolver
		}
		if job.GetConcurrencyAdmitted() {
			continue // already dispatched
		}
		if !s.tryAdmit(ctx, job) {
			continue // blocked by a resource at its limit; re-check next pass
		}
		if job.GetTargetGroup() != "" {
			db.PublishAssignment(ctx, &dbpb.PublishAssignmentRequest{JobId: job.GetId(), TargetGroup: job.GetTargetGroup()})
		} else {
			s.logger.Info("scheduler: run instance queued for ephemeral agent", "job", job.GetId(), "run", job.GetRunId())
		}
	}
}
