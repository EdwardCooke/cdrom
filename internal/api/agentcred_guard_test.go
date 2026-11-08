package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/config"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	idppb "cdrom/internal/gen/cdrom/idp/v1"
)

// fakeAgentDB is a Database client answering the FOUR calls the identity
// guards touch: GetAgentCredential (row liveness), RegisterAgent /
// HeartbeatAgent (recorded, together with the PRESENTED bearer), GetAgent
// (the attribute binding for ListPendingJobs), and ListPendingByGroup
// (polled). Every other method panics via the embedded nil interface
// (sentinel).
type fakeAgentDB struct {
	dbpb.DatabaseClient
	mu        sync.Mutex
	credRow   *dbpb.AgentCredential
	agentRows map[string]*dbpb.Agent
	regCalls  int
	hbCalls   int
	pollCalls int
	bearers   []string
}

func (f *fakeAgentDB) GetAgentCredential(_ context.Context, in *dbpb.GetAgentCredentialRequest, _ ...grpc.CallOption) (*dbpb.AgentCredential, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.credRow == nil || f.credRow.GetId() != in.GetId() {
		return nil, status.Error(codes.NotFound, "agent credential not found: "+in.GetId())
	}
	return f.credRow, nil
}

func (f *fakeAgentDB) RegisterAgent(ctx context.Context, in *dbpb.RegisterAgentRequest, _ ...grpc.CallOption) (*dbpb.Agent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.regCalls++
	f.bearers = append(f.bearers, bearerToken(ctx))
	return &dbpb.Agent{Name: in.GetName(), Group: in.GetGroup()}, nil
}

func (f *fakeAgentDB) HeartbeatAgent(ctx context.Context, in *dbpb.HeartbeatAgentRequest, _ ...grpc.CallOption) (*dbpb.Agent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hbCalls++
	f.bearers = append(f.bearers, bearerToken(ctx))
	return &dbpb.Agent{Name: in.GetName()}, nil
}

func (f *fakeAgentDB) GetAgent(_ context.Context, in *dbpb.GetAgentRequest, _ ...grpc.CallOption) (*dbpb.Agent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.agentRows[in.GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "agent not found: "+in.GetName())
	}
	return row, nil
}

func (f *fakeAgentDB) ListPendingByGroup(ctx context.Context, _ *dbpb.ListPendingByGroupRequest, _ ...grpc.CallOption) (*dbpb.ListJobsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pollCalls++
	f.bearers = append(f.bearers, bearerToken(ctx))
	return &dbpb.ListJobsResponse{}, nil
}

func (f *fakeAgentDB) MarkAgentCredentialSeen(_ context.Context, _ *dbpb.MarkAgentCredentialSeenRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeAgentDB) AbandonAgentExecutions(_ context.Context, _ *dbpb.AbandonAgentExecutionsRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeAgentDB) DeleteAgent(_ context.Context, _ *dbpb.DeleteAgentRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (f *fakeAgentDB) PublishAgentEvent(_ context.Context, _ *dbpb.PublishAgentEventRequest, _ ...grpc.CallOption) (*dbpb.PublishEventResponse, error) {
	return &dbpb.PublishEventResponse{}, nil
}

// guardServer builds a gRPC server over the fake DB with (optionally) the
// agent-credential verifier installed (nil = disabled posture).
func guardServer(t *testing.T, db *fakeAgentDB, ver *AgentCredentialVerifier) *GRPCServer {
	t.Helper()
	s := NewGRPCServer(db, nil, nil, nil, nil, quietLog())
	if ver != nil {
		s.SetAgentCredentialVerifier(ver)
	}
	return s
}

// newGuardVerifier builds a REAL verifier (JWKS discovery against the
// in-process IdP) over the fake DB's row.
func newGuardVerifier(t *testing.T, base string, db *fakeAgentDB) *AgentCredentialVerifier {
	t.Helper()
	v, err := NewAgentCredentialVerifier(context.Background(),
		config.GRPCAuthConfig{IdPAddress: base, Audiences: []string{"cdrom-api"}},
		nil, db, quietLog())
	if err != nil {
		t.Fatalf("NewAgentCredentialVerifier: %v", err)
	}
	return v
}

// bearerCtx builds an INCOMING context carrying the bearer — the direction
// the handlers see. (Calling the handler IN-PROCESS skips the transport's
// outgoing→incoming metadata translation, so the test must plant the
// incoming side directly.)
func bearerCtx(tok string) context.Context {
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+tok))
}

// issueCred issues a REAL signed credential for the named agent.
func issueCred(t *testing.T, idpClient idppb.IdPClient, name string) (*dbpb.AgentCredential, string) {
	t.Helper()
	issued, err := idpClient.IssueAgentCredential(context.Background(), &idppb.IssueAgentCredentialRequest{
		AgentName: name, IssuedBy: "setup",
	})
	if err != nil {
		t.Fatalf("issue(%s): %v", name, err)
	}
	return issued.GetCredential(), issued.GetJwt()
}

// TestAgentCredentialGuard_Disabled: with NO verifier installed the identity
// verbs are OPEN (the disabled posture — local dev without an IdP).
func TestAgentCredentialGuard_Disabled(t *testing.T) {
	db := &fakeAgentDB{agentRows: map[string]*dbpb.Agent{"agent-1": {Name: "agent-1", Group: "g"}}}
	s := guardServer(t, db, nil)
	if _, err := s.RegisterAgent(context.Background(), &apipb.RegisterAgentRequest{Name: "agent-1"}); err != nil {
		t.Errorf("disabled posture: RegisterAgent = %v, want open", err)
	}
	if _, err := s.Heartbeat(context.Background(), &apipb.HeartbeatRequest{Name: "agent-1"}); err != nil {
		t.Errorf("disabled posture: Heartbeat = %v, want open", err)
	}
	if _, err := s.ListPendingJobs(context.Background(), &apipb.ListPendingJobsRequest{Group: "g"}); err != nil {
		t.Errorf("disabled posture: ListPendingJobs = %v, want open", err)
	}
}

// TestAgentCredentialGuard_UncredentialedRejected: with the guard ON, every
// identity verb without a bearer is UNAUTHENTICATED and NOTHING leaks into
// the handler bodies (rejected at the door).
func TestAgentCredentialGuard_UncredentialedRejected(t *testing.T) {
	base, idpClient, _ := startTestIDPCredentials(t)
	cred, _ := issueCred(t, idpClient, "agent-1")
	db := &fakeAgentDB{credRow: cred, agentRows: map[string]*dbpb.Agent{"agent-1": {Name: "agent-1", Group: "g"}}}
	s := guardServer(t, db, newGuardVerifier(t, base, db))

	if _, err := s.RegisterAgent(context.Background(), &apipb.RegisterAgentRequest{Name: "agent-1"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("RegisterAgent no-bearer = %v, want UNAUTHENTICATED", err)
	}
	if _, err := s.Heartbeat(context.Background(), &apipb.HeartbeatRequest{Name: "agent-1"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("Heartbeat no-bearer = %v, want UNAUTHENTICATED", err)
	}
	if _, err := s.ListPendingJobs(context.Background(), &apipb.ListPendingJobsRequest{Group: "g"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("ListPendingJobs no-bearer = %v, want UNAUTHENTICATED", err)
	}
	if _, err := s.DeregisterAgent(context.Background(), &apipb.DeregisterAgentRequest{Name: "agent-1"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("DeregisterAgent no-bearer = %v, want UNAUTHENTICATED", err)
	}
	if db.regCalls != 0 || db.hbCalls != 0 || db.pollCalls != 0 {
		t.Errorf("rejected calls leaked into the handlers (reg=%d hb=%d poll=%d)", db.regCalls, db.hbCalls, db.pollCalls)
	}
}

// TestAgentCredentialGuard_GarbledRejected: a nonsense bearer is
// UNAUTHENTICATED (judged on its merits — no RBAC fall-through here either).
func TestAgentCredentialGuard_GarbledRejected(t *testing.T) {
	base, idpClient, _ := startTestIDPCredentials(t)
	cred, _ := issueCred(t, idpClient, "agent-1")
	db := &fakeAgentDB{credRow: cred}
	s := guardServer(t, db, newGuardVerifier(t, base, db))
	if _, err := s.RegisterAgent(bearerCtx("not.a.real.jwt"), &apipb.RegisterAgentRequest{Name: "agent-1"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("RegisterAgent garbled = %v, want UNAUTHENTICATED", err)
	}
}

// TestAgentCredentialGuard_NameBinding: the HAPPY PATH (credential bound to
// the claimed name ⇒ allowed, bearer recorded) and the MASQUERADE CASE
// (credential bound to agent-1 presented for "rogue" ⇒ PERMISSION_DENIED).
func TestAgentCredentialGuard_NameBinding(t *testing.T) {
	base, idpClient, _ := startTestIDPCredentials(t)
	cred, jwt := issueCred(t, idpClient, "agent-1")
	db := &fakeAgentDB{credRow: cred, agentRows: map[string]*dbpb.Agent{"agent-1": {Name: "agent-1", Group: "g"}}}
	s := guardServer(t, db, newGuardVerifier(t, base, db))

	// Happy path: register AS the credential's own name.
	if _, err := s.RegisterAgent(bearerCtx(jwt), &apipb.RegisterAgentRequest{Name: "agent-1"}); err != nil {
		t.Fatalf("RegisterAgent happy = %v, want success", err)
	}
	if db.regCalls != 1 || db.bearers[len(db.bearers)-1] != jwt {
		t.Errorf("happy path did not record the presented bearer (regs=%d)", db.regCalls)
	}
	// Masquerade: the SAME credential presented for a DIFFERENT name.
	if _, err := s.RegisterAgent(bearerCtx(jwt), &apipb.RegisterAgentRequest{Name: "rogue"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("masquerade RegisterAgent = %v, want PERMISSION_DENIED", err)
	}
	if _, err := s.Heartbeat(bearerCtx(jwt), &apipb.HeartbeatRequest{Name: "rogue"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("masquerade Heartbeat = %v, want PERMISSION_DENIED", err)
	}
	// Deregistration of a stranger's name is denied too.
	if _, err := s.DeregisterAgent(bearerCtx(jwt), &apipb.DeregisterAgentRequest{Name: "someone-else"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("stranger DeregisterAgent = %v, want PERMISSION_DENIED", err)
	}
}

// TestAgentCredentialGuard_AttributeBinding: ListPendingJobs CLAIMS A GROUP —
// the credential's holder must be REGISTERED in that group (a credential
// cannot poll another group's queue; an unregistered holder is denied).
func TestAgentCredentialGuard_AttributeBinding(t *testing.T) {
	base, idpClient, _ := startTestIDPCredentials(t)
	cred, jwt := issueCred(t, idpClient, "agent-1")
	db := &fakeAgentDB{credRow: cred, agentRows: map[string]*dbpb.Agent{
		"agent-1": {Name: "agent-1", Group: "mine"},
	}}
	s := guardServer(t, db, newGuardVerifier(t, base, db))

	// Own group: allowed.
	if _, err := s.ListPendingJobs(bearerCtx(jwt), &apipb.ListPendingJobsRequest{Group: "mine"}); err != nil {
		t.Errorf("poll own group = %v, want success", err)
	}
	// Another group: PERMISSION_DENIED.
	if _, err := s.ListPendingJobs(bearerCtx(jwt), &apipb.ListPendingJobsRequest{Group: "theirs"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("poll other group = %v, want PERMISSION_DENIED", err)
	}
	// Holder never registered: PERMISSION_DENIED.
	db2 := &fakeAgentDB{credRow: cred}
	s2 := guardServer(t, db2, newGuardVerifier(t, base, db2))
	if _, err := s2.ListPendingJobs(bearerCtx(jwt), &apipb.ListPendingJobsRequest{Group: "mine"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("unregistered-holder poll = %v, want PERMISSION_DENIED", err)
	}
}

// TestAgentCredentialGuard_RevokedRowDenies: a REVOKED row (whose signature
// is still VALID) is UNAUTHENTICATED — the row decides life, and the guard
// defers to it.
func TestAgentCredentialGuard_RevokedRowDenies(t *testing.T) {
	base, idpClient, _ := startTestIDPCredentials(t)
	cred, jwt := issueCred(t, idpClient, "agent-1")
	// Stamp the row revoked (a fresh twin; the signature is untouched).
	revoked := &dbpb.AgentCredential{
		Id:        cred.GetId(),
		AgentName: cred.GetAgentName(),
		ExpiresAt: cred.ExpiresAt,
		RevokedAt: timestamppb.New(time.Now().UTC()),
	}
	db := &fakeAgentDB{credRow: revoked, agentRows: map[string]*dbpb.Agent{"agent-1": {Name: "agent-1", Group: "g"}}}
	s := guardServer(t, db, newGuardVerifier(t, base, db))
	if _, err := s.RegisterAgent(bearerCtx(jwt), &apipb.RegisterAgentRequest{Name: "agent-1"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("revoked-row RegisterAgent = %v, want UNAUTHENTICATED", err)
	}
}
