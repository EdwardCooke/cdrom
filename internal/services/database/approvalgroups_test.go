package database

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TestApprovalGroupCRUD exercises the approval-group (F-27) lifecycle over the
// REAL gRPC client (bufconn): create → duplicate is AlreadyExists → get-round-
// trips the JSON-serializer members column → list → update (partial + member
// replace) → delete → NotFound.
func TestApprovalGroupCRUD(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	group, err := client.CreateApprovalGroup(ctx, &dbpb.CreateApprovalGroupRequest{
		Name:        "release-managers",
		Description: "may approve prod releases",
		Members:     []string{"alice@example.com", "bob@example.com"},
	})
	if err != nil {
		t.Fatalf("CreateApprovalGroup: %v", err)
	}
	if group.GetName() != "release-managers" || len(group.GetMembers()) != 2 {
		t.Errorf("create echoed group = %+v, want name + 2 members", group)
	}

	// Duplicate name ⇒ AlreadyExists.
	if _, err := client.CreateApprovalGroup(ctx, &dbpb.CreateApprovalGroupRequest{Name: "release-managers"}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateApprovalGroup(dup) code = %v, want AlreadyExists", status.Code(err))
	}

	// Missing name ⇒ InvalidArgument.
	if _, err := client.CreateApprovalGroup(ctx, &dbpb.CreateApprovalGroupRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateApprovalGroup(no name) code = %v, want InvalidArgument", status.Code(err))
	}

	// Get round-trips the members (proves the serializer:json column survived).
	got, err := client.GetApprovalGroup(ctx, &dbpb.GetApprovalGroupRequest{Name: "release-managers"})
	if err != nil {
		t.Fatalf("GetApprovalGroup: %v", err)
	}
	want := map[string]bool{"alice@example.com": true, "bob@example.com": true}
	if len(got.GetMembers()) != 2 {
		t.Fatalf("get members = %v, want 2", got.GetMembers())
	}
	for _, m := range got.GetMembers() {
		if !want[m] {
			t.Errorf("get members unexpected %q", m)
		}
	}

	// List finds it.
	list, err := client.ListApprovalGroups(ctx, &dbpb.ListApprovalGroupsRequest{})
	if err != nil {
		t.Fatalf("ListApprovalGroups: %v", err)
	}
	if len(list.GetGroups()) != 1 || list.GetGroups()[0].GetName() != "release-managers" {
		t.Errorf("list = %+v, want the one group", list.GetGroups())
	}

	// Partial update: description only (members left unchanged).
	upd, err := client.UpdateApprovalGroup(ctx, &dbpb.UpdateApprovalGroupRequest{
		Name:        "release-managers",
		Description: "renamed description",
	})
	if err != nil {
		t.Fatalf("UpdateApprovalGroup(desc): %v", err)
	}
	if upd.GetDescription() != "renamed description" || len(upd.GetMembers()) != 2 {
		t.Errorf("desc update = %+v, want new description + 2 members intact", upd)
	}

	// Member replace (members_present): swap the whole list.
	rep, err := client.UpdateApprovalGroup(ctx, &dbpb.UpdateApprovalGroupRequest{
		Name:           "release-managers",
		Members:        []string{"carol@example.com"},
		MembersPresent: true,
	})
	if err != nil {
		t.Fatalf("UpdateApprovalGroup(replace): %v", err)
	}
	if len(rep.GetMembers()) != 1 || rep.GetMembers()[0] != "carol@example.com" {
		t.Errorf("replace members = %v, want [carol]", rep.GetMembers())
	}

	// Delete, then NotFound.
	if _, err := client.DeleteApprovalGroup(ctx, &dbpb.DeleteApprovalGroupRequest{Name: "release-managers"}); err != nil {
		t.Fatalf("DeleteApprovalGroup: %v", err)
	}
	if _, err := client.GetApprovalGroup(ctx, &dbpb.GetApprovalGroupRequest{Name: "release-managers"}); status.Code(err) != codes.NotFound {
		t.Errorf("GetApprovalGroup(deleted) code = %v, want NotFound", status.Code(err))
	}
}

// TestApprovalGroupAtomicMembers verifies the incremental member add/remove
// endpoints (F-27): add is idempotent, remove is idempotent, and both mutate
// the stored list.
func TestApprovalGroupAtomicMembers(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	if _, err := client.CreateApprovalGroup(ctx, &dbpb.CreateApprovalGroupRequest{Name: "ops"}); err != nil {
		t.Fatalf("CreateApprovalGroup: %v", err)
	}

	added, err := client.AddApprovalGroupMember(ctx, &dbpb.AddApprovalGroupMemberRequest{Name: "ops", Member: "dave@example.com"})
	if err != nil {
		t.Fatalf("AddApprovalGroupMember: %v", err)
	}
	if len(added.GetMembers()) != 1 || added.GetMembers()[0] != "dave@example.com" {
		t.Errorf("after add members = %v, want [dave]", added.GetMembers())
	}

	// Idempotent add: same member again leaves the list at one.
	again, err := client.AddApprovalGroupMember(ctx, &dbpb.AddApprovalGroupMemberRequest{Name: "ops", Member: "dave@example.com"})
	if err != nil {
		t.Fatalf("AddApprovalGroupMember(idempotent): %v", err)
	}
	if len(again.GetMembers()) != 1 {
		t.Errorf("idempotent add members = %v, want 1", again.GetMembers())
	}

	removed, err := client.RemoveApprovalGroupMember(ctx, &dbpb.RemoveApprovalGroupMemberRequest{Name: "ops", Member: "dave@example.com"})
	if err != nil {
		t.Fatalf("RemoveApprovalGroupMember: %v", err)
	}
	if len(removed.GetMembers()) != 0 {
		t.Errorf("after remove members = %v, want empty", removed.GetMembers())
	}

	// Removing an absent member is a no-op (still empty, no error).
	nope, err := client.RemoveApprovalGroupMember(ctx, &dbpb.RemoveApprovalGroupMemberRequest{Name: "ops", Member: "ghost@example.com"})
	if err != nil {
		t.Fatalf("RemoveApprovalGroupMember(absent): %v", err)
	}
	if len(nope.GetMembers()) != 0 {
		t.Errorf("remove-absent members = %v, want empty", nope.GetMembers())
	}

	// Operations on a missing group ⇒ NotFound.
	if _, err := client.AddApprovalGroupMember(ctx, &dbpb.AddApprovalGroupMemberRequest{Name: "nope", Member: "x"}); status.Code(err) != codes.NotFound {
		t.Errorf("Add(member) missing group code = %v, want NotFound", status.Code(err))
	}
}

// TestEffectiveApprovalGroupMembers verifies a group's EFFECTIVE membership is
// the union of its manual members and every principal observed to carry its
// claim derivation (F-27).
func TestEffectiveApprovalGroupMembers(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	if _, err := client.CreateApprovalGroup(ctx, &dbpb.CreateApprovalGroupRequest{
		Name:       "corporate-team",
		Members:    []string{"manual@example.com"},
		ClaimName:  "teams",
		ClaimValue: "payments",
	}); err != nil {
		t.Fatalf("CreateApprovalGroup: %v", err)
	}

	// Observe two principals carrying the claim (and one NOT).
	for _, subj := range []string{"claire@example.com", "frank@example.com"} {
		if _, err := client.RecordObservedClaim(ctx, &dbpb.RecordObservedClaimRequest{
			PrincipalSubject: subj, ClaimName: "teams", ClaimValue: "payments",
		}); err != nil {
			t.Fatalf("RecordObservedClaim(%s): %v", subj, err)
		}
	}
	if _, err := client.RecordObservedClaim(ctx, &dbpb.RecordObservedClaimRequest{
		PrincipalSubject: "other@example.com", ClaimName: "teams", ClaimValue: "infra",
	}); err != nil {
		t.Fatalf("RecordObservedClaim(other): %v", err)
	}

	resp, err := client.EffectiveApprovalGroupMembers(ctx, &dbpb.EffectiveApprovalGroupMembersRequest{Name: "corporate-team"})
	if err != nil {
		t.Fatalf("EffectiveApprovalGroupMembers: %v", err)
	}
	got := map[string]bool{}
	for _, m := range resp.GetMembers() {
		got[m] = true
	}
	for _, want := range []string{"manual@example.com", "claire@example.com", "frank@example.com"} {
		if !got[want] {
			t.Errorf("effective members missing %q (have %v)", want, resp.GetMembers())
		}
	}
	if got["other@example.com"] {
		t.Errorf("effective members wrongly include other@example.com (different claim value)")
	}
}

// TestObservedClaimsFor verifies ObservedClaimsFor returns the distinct values
// observed for a principal under a claim name (F-27).
func TestObservedClaimsFor(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	for _, v := range []string{"alpha", "beta"} {
		if _, err := client.RecordObservedClaim(ctx, &dbpb.RecordObservedClaimRequest{
			PrincipalSubject: "gina@example.com", ClaimName: "team", ClaimValue: v,
		}); err != nil {
			t.Fatalf("RecordObservedClaim(%s): %v", v, err)
		}
	}
	// Idempotent re-record of alpha.
	if _, err := client.RecordObservedClaim(ctx, &dbpb.RecordObservedClaimRequest{
		PrincipalSubject: "gina@example.com", ClaimName: "team", ClaimValue: "alpha",
	}); err != nil {
		t.Fatalf("RecordObservedClaim(alpha dup): %v", err)
	}
	resp, err := client.ObservedClaimsFor(ctx, &dbpb.ObservedClaimsForRequest{
		PrincipalSubject: "gina@example.com", ClaimName: "team",
	})
	if err != nil {
		t.Fatalf("ObservedClaimsFor: %v", err)
	}
	vals := map[string]bool{}
	for _, v := range resp.GetValues() {
		vals[v] = true
	}
	if len(vals) != 2 || !vals["alpha"] || !vals["beta"] {
		t.Errorf("ObservedClaimsFor = %v, want {alpha beta}", resp.GetValues())
	}
}

// TestCastApprovalVote verifies the quorum vote ledger (F-27): a vote is
// recorded while the job awaits, casting the same decision again is a no-op
// (recorded=false), FLIPPING a vote supersedes the prior row (still counted
// once), and a vote on a NON-AWAITING job is refused (recorded=false).
func TestCastApprovalVote(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// Craft a job parked at an approval gate (awaiting_approval) with a quorum.
	jobID := seedAwaitingApprovalJob(t, client, ctx, "gate-job", 2)
	// First approval by alice: recorded.
	r1, err := client.CastApprovalVote(ctx, &dbpb.CastApprovalVoteRequest{
		JobId: jobID, Step: "deploy", Attempt: 1, Actor: "alice", Decision: "approved",
	})
	if err != nil {
		t.Fatalf("CastApprovalVote(alice): %v", err)
	}
	if !r1.GetRecorded() {
		t.Errorf("cast(alice) recorded = false, want true")
	}

	// Alice approves AGAIN: idempotent (recorded=false), ledger still one vote.
	r2, err := client.CastApprovalVote(ctx, &dbpb.CastApprovalVoteRequest{
		JobId: jobID, Step: "deploy", Attempt: 1, Actor: "alice", Decision: "approved",
	})
	if err != nil {
		t.Fatalf("CastApprovalVote(alice dup): %v", err)
	}
	if r2.GetRecorded() {
		t.Errorf("dup cast(alice) recorded = true, want false (idempotent)")
	}
	if len(r2.GetVotes()) != 1 {
		t.Errorf("duplicate cast votes = %d, want 1", len(r2.GetVotes()))
	}

	// Bob approves: recorded, ledger now two distinct approvals.
	r3, err := client.CastApprovalVote(ctx, &dbpb.CastApprovalVoteRequest{
		JobId: jobID, Step: "deploy", Attempt: 1, Actor: "bob", Decision: "approved",
	})
	if err != nil {
		t.Fatalf("CastApprovalVote(bob): %v", err)
	}
	if !r3.GetRecorded() {
		t.Errorf("cast(bob) recorded = false, want true")
	}
	if len(r3.GetVotes()) != 2 {
		t.Errorf("after bob votes = %d, want 2", len(r3.GetVotes()))
	}

	// Carol REJECTS: recorded, ledger now three votes (mixed).
	r4, err := client.CastApprovalVote(ctx, &dbpb.CastApprovalVoteRequest{
		JobId: jobID, Step: "deploy", Attempt: 1, Actor: "carol", Decision: "rejected",
	})
	if err != nil {
		t.Fatalf("CastApprovalVote(carol): %v", err)
	}
	if !r4.GetRecorded() || len(r4.GetVotes()) != 3 {
		t.Errorf("after carol votes = %d (recorded=%v), want 3 (true)", len(r4.GetVotes()), r4.GetRecorded())
	}

	// Flip: carol changes her vote to approved. Ledger stays THREE distinct
	// actors (carol's prior row superseded), and she is now counted approved.
	r5, err := client.CastApprovalVote(ctx, &dbpb.CastApprovalVoteRequest{
		JobId: jobID, Step: "deploy", Attempt: 1, Actor: "carol", Decision: "approved",
	})
	if err != nil {
		t.Fatalf("CastApprovalVote(carol flip): %v", err)
	}
	if len(r5.GetVotes()) != 3 {
		t.Errorf("after flip votes = %d, want 3 (supersede, not append)", len(r5.GetVotes()))
	}
	approved := 0
	for _, v := range r5.GetVotes() {
		if v.GetDecision() == "approved" {
			approved++
		}
	}
	if approved != 3 {
		t.Errorf("after flip approved = %d, want 3", approved)
	}

	// A DIFFERENT attempt is a distinct gate: its ledger is empty.
	r6, err := client.CastApprovalVote(ctx, &dbpb.CastApprovalVoteRequest{
		JobId: jobID, Step: "deploy", Attempt: 2, Actor: "dana", Decision: "approved",
	})
	if err != nil {
		t.Fatalf("CastApprovalVote(attempt 2): %v", err)
	}
	if len(r6.GetVotes()) != 1 {
		t.Errorf("attempt-2 votes = %d, want 1 (isolated from attempt 1)", len(r6.GetVotes()))
	}
}

// TestCastApprovalVoteNonAwaiting verifies a vote on a job that is NOT awaiting
// approval is refused (recorded=false) — a late vote on a resolved gate.
func TestCastApprovalVoteNonAwaiting(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()
	jobID := seedAwaitingApprovalJob(t, client, ctx, "settled-job", 1)

	// Resolve the gate (move it out of awaiting_approval).
	if _, err := client.ResolveApproval(ctx, &dbpb.ResolveApprovalRequest{
		JobId: jobID, Decision: "approved", Actor: "sys",
	}); err != nil {
		t.Fatalf("ResolveApproval: %v", err)
	}

	// A vote now arrives for the (no-longer awaiting) gate: refused.
	resp, err := client.CastApprovalVote(ctx, &dbpb.CastApprovalVoteRequest{
		JobId: jobID, Step: "deploy", Attempt: 1, Actor: "late", Decision: "approved",
	})
	if err != nil {
		t.Fatalf("CastApprovalVote(after resolve): %v", err)
	}
	if resp.GetRecorded() {
		t.Errorf("late vote recorded = true, want false (gate no longer awaiting)")
	}
}

// seedAwaitingApprovalJob creates a job and parks it at an approval gate
// (awaiting_approval) with the given quorum, returning its id. It drives the
// real gRPC client (CreateJob + UpdateJob) so the test exercises the genuine
// gate-entry path.
func seedAwaitingApprovalJob(t *testing.T, client dbpb.DatabaseClient, ctx context.Context, name string, quorum int32) int64 {
	t.Helper()
	pipe, err := client.CreatePipeline(ctx, &dbpb.CreatePipelineRequest{Name: "vpipe-" + name})
	if err != nil {
		t.Fatalf("CreatePipeline: %v", err)
	}
	job, err := client.CreateJob(ctx, &dbpb.CreateJobRequest{
		PipelineId: pipe.GetId(), Name: name, TargetGroup: "default",
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	// Park the job at the gate (awaiting_approval) with the gate's F-27 fields.
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{
		Id:                job.GetId(),
		Status:            dbpb.JobStatus_JOB_STATUS_AWAITING_APPROVAL,
		ApprovalMessage:   "hold",
		ApprovalApprovers: []string{"group:g"},
		ApprovalQuorum:    quorum,
		ApprovalStep:      "deploy",
	}); err != nil {
		t.Fatalf("UpdateJob(awaiting): %v", err)
	}
	return job.GetId()
}
