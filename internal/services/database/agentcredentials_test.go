package database

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// mkAgentCredentialMsg builds a wire row for create.
func mkAgentCredentialMsg(id, agentName, issuedBy string, expiresIn time.Duration) *dbpb.AgentCredential {
	return &dbpb.AgentCredential{
		Id:          id,
		AgentName:   agentName,
		IssuedBy:    issuedBy,
		Description: "test credential",
		ExpiresAt:   timestamppb.New(time.Now().UTC().Add(expiresIn)),
	}
}

// TestAgentCredentialLifecycle exercises the F-30.2 directory against a real
// SQLite-backed Database service (REAL gRPC client): create → get → list →
// renew (extend forward; backward and revoked-renew rejected) → rotate
// (retire + successor; double-retire rejected) → revoke (immediate +
// idempotent) → mark-seen. The JWT never crosses this boundary (the row has
// no JWT field — custody-proof vs liveness separation, F-30.2).
func TestAgentCredentialLifecycle(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	const (
		credID  = "cred-alpha"
		agentNm = "edge-box-1"
	)

	// Create.
	created, err := client.CreateAgentCredential(ctx, &dbpb.CreateAgentCredentialRequest{
		Credential: mkAgentCredentialMsg(credID, agentNm, "admin@example.com", 72*time.Hour),
	})
	if err != nil {
		t.Fatalf("CreateAgentCredential: %v", err)
	}
	if created.GetId() != credID || created.GetAgentName() != agentNm {
		t.Errorf("create echo = (%q,%q), want (%q,%q)", created.GetId(), created.GetAgentName(), credID, agentNm)
	}
	// Duplicate id is AlreadyExists.
	if _, err := client.CreateAgentCredential(ctx, &dbpb.CreateAgentCredentialRequest{
		Credential: mkAgentCredentialMsg(credID, agentNm, "other", 72*time.Hour),
	}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("duplicate create err = %v, want AlreadyExists", err)
	}
	// Validation: missing id / agent_name.
	if _, err := client.CreateAgentCredential(ctx, &dbpb.CreateAgentCredentialRequest{
		Credential: &dbpb.AgentCredential{AgentName: "x"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("create without id err = %v, want InvalidArgument", err)
	}
	if _, err := client.CreateAgentCredential(ctx, &dbpb.CreateAgentCredentialRequest{
		Credential: &dbpb.AgentCredential{Id: "cred-beta"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("create without agent_name err = %v, want InvalidArgument", err)
	}

	// Get.
	got, err := client.GetAgentCredential(ctx, &dbpb.GetAgentCredentialRequest{Id: credID})
	if err != nil {
		t.Fatalf("GetAgentCredential: %v", err)
	}
	if got.GetExpiresAt() == nil || got.GetExpiresAt().AsTime().IsZero() {
		t.Errorf("get: expires_at missing, want the 72h deadline")
	}
	if _, err := client.GetAgentCredential(ctx, &dbpb.GetAgentCredentialRequest{Id: "nope"}); status.Code(err) != codes.NotFound {
		t.Errorf("get missing err = %v, want NotFound", err)
	}

	// List: per-agent and global.
	listMine, err := client.ListAgentCredentials(ctx, &dbpb.ListAgentCredentialsRequest{AgentName: agentNm})
	if err != nil || len(listMine.GetCredentials()) != 1 {
		t.Fatalf("list(agent) = %d rows (err %v), want 1", len(listMine.GetCredentials()), err)
	}
	listOther, err := client.ListAgentCredentials(ctx, &dbpb.ListAgentCredentialsRequest{AgentName: "somebody-else"})
	if err != nil || len(listOther.GetCredentials()) != 0 {
		t.Fatalf("list(other) = %d rows (err %v), want 0", len(listOther.GetCredentials()), err)
	}
	listAll, err := client.ListAgentCredentials(ctx, &dbpb.ListAgentCredentialsRequest{})
	if err != nil || len(listAll.GetCredentials()) != 1 {
		t.Fatalf("list(all) = %d rows (err %v), want 1", len(listAll.GetCredentials()), err)
	}

	origDeadline := got.GetExpiresAt().AsTime()

	// Renew: extend forward.
	extended, err := client.ExtendAgentCredentialExpiration(ctx, &dbpb.ExtendAgentCredentialExpirationRequest{
		Id:        credID,
		ExpiresAt: timestamppb.New(origDeadline.Add(24 * time.Hour)),
	})
	if err != nil {
		t.Fatalf("Extend (forward): %v", err)
	}
	if !extended.GetExpiresAt().AsTime().Equal(origDeadline.Add(24 * time.Hour)) {
		t.Errorf("extend = %v, want %v", extended.GetExpiresAt().AsTime(), origDeadline.Add(24*time.Hour))
	}
	// Renew backward is rejected.
	if _, err := client.ExtendAgentCredentialExpiration(ctx, &dbpb.ExtendAgentCredentialExpirationRequest{
		Id:        credID,
		ExpiresAt: timestamppb.New(origDeadline.Add(-time.Hour)),
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("extend (backward) err = %v, want InvalidArgument", err)
	}

	// Rotation: retire the incumbent, chain to the successor.
	_, err = client.RetireAgentCredential(ctx, &dbpb.RetireAgentCredentialRequest{
		Id:           credID,
		ReplacedById: "cred-successor",
	})
	if err != nil {
		t.Fatalf("Retire: %v", err)
	}
	retired, err := client.GetAgentCredential(ctx, &dbpb.GetAgentCredentialRequest{Id: credID})
	if err != nil {
		t.Fatalf("Get (retired): %v", err)
	}
	if retired.GetReplacedById() != "cred-successor" {
		t.Errorf("retired.replaced_by_id = %q, want %q", retired.GetReplacedById(), "cred-successor")
	}
	// Double-retire is rejected.
	if _, err := client.RetireAgentCredential(ctx, &dbpb.RetireAgentCredentialRequest{
		Id:           credID,
		ReplacedById: "cred-other",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("double retire err = %v, want FailedPrecondition", err)
	}
	// A retired row cannot be renewed.
	if _, err := client.ExtendAgentCredentialExpiration(ctx, &dbpb.ExtendAgentCredentialExpirationRequest{
		Id:        credID,
		ExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("renew retired err = %v, want FailedPrecondition", err)
	}

	// Revoke: immediate + idempotent.
	revoked, err := client.RevokeAgentCredential(ctx, &dbpb.RevokeAgentCredentialRequest{
		Id:     credID,
		Reason: "compromise suspected",
	})
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if revoked.GetRevokedAt() == nil || revoked.GetRevokedReason() != "compromise suspected" {
		t.Errorf("revoked row = (stamp %v, reason %q), want stamped + reasoned", revoked.GetRevokedAt(), revoked.GetRevokedReason())
	}
	again, err := client.RevokeAgentCredential(ctx, &dbpb.RevokeAgentCredentialRequest{Id: credID, Reason: "again"})
	if err != nil {
		t.Fatalf("Revoke (repeat): %v", err)
	}
	if again.GetRevokedReason() != "compromise suspected" {
		t.Errorf("repeat revoke changed the reason to %q, want idempotent", again.GetRevokedReason())
	}

	// Mark-seen: bumps last_seen_at; unknown id is NotFound.
	if _, err := client.MarkAgentCredentialSeen(ctx, &dbpb.MarkAgentCredentialSeenRequest{Id: credID}); err != nil {
		t.Fatalf("MarkAgentCredentialSeen: %v", err)
	}
	seen, err := client.GetAgentCredential(ctx, &dbpb.GetAgentCredentialRequest{Id: credID})
	if err != nil {
		t.Fatalf("Get (seen): %v", err)
	}
	if seen.GetLastSeenAt() == nil {
		t.Errorf("last_seen_at not bumped by mark-seen")
	}
	if _, err := client.MarkAgentCredentialSeen(ctx, &dbpb.MarkAgentCredentialSeenRequest{Id: "ghost"}); status.Code(err) != codes.NotFound {
		t.Errorf("mark-seen unknown err = %v, want NotFound", err)
	}
}
