package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/audit"
	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
)

// ---------------------------------------------------------------------------
// Pipeline sources (F-21.5.1 — minimal slice for F-21.3)
// ---------------------------------------------------------------------------
//
// Where a pipeline is pulled from: a kind (git / http / file), a reference
// (the <type>:<spec> locator, F-21.2), a refresh schedule, and a credential
// name. The source is a database concept configured through the API (it is not
// in the file). This minimal slice supports the source-credential guards
// (F-21.3): attaching a source (which names a credential, so the delete guard
// has something to count), reading the source + its last sync state (so a
// credential that fails to authenticate is visible via last_error), and a
// refresh that runs the sync routine (select the credential, decrypt it, dial
// the source-fetcher, and record the outcome on the source state).
//
// The full F-21.5.2 sync routine (hash compare, recompile + re-import on
// change, per-pipeline serialization) and the F-21.5.3 refresh triggers land
// with F-21.5; this slice records the fetch outcome (including last_error) so
// a stale credential is diagnosable.

// pipelineSourceRequest is the JSON form of a pipeline-source attach.
type pipelineSourceRequest struct {
	Kind       string `json:"kind,omitempty"`
	Reference  string `json:"reference,omitempty"`
	Schedule   string `json:"schedule,omitempty"`
	Credential string `json:"credential,omitempty"`
}

// pipelineSourceView is the JSON form of a pipeline's source + its last sync
// state, returned by GET /api/pipelines/{id}/source.
type pipelineSourceView struct {
	*dbpb.PipelineSource
	State *dbpb.PipelineSourceState `json:"state,omitempty"`
}

// attachPipelineSource attaches (or replaces) a source on a pipeline
// (F-21.5.1). It requires pipelines.can-edit.
func (s *Server) attachPipelineSource(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesEdit, authz.Resource{PipelineID: id}) {
		return
	}
	var req pipelineSourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	src, err := s.clients.Database.AttachPipelineSource(r.Context(), &dbpb.AttachPipelineSourceRequest{
		PipelineId: id,
		Kind:       req.Kind,
		Reference:  req.Reference,
		Schedule:   req.Schedule,
		Credential: req.Credential,
	})
	if err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionPipelineUpdate,
		TargetKind: audit.TargetPipeline,
		TargetID:   fmt.Sprintf("%d", id),
		TargetName: fmt.Sprintf("source"),
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusCreated, src)
}

// getPipelineSource returns a pipeline's source + its last sync state
// (F-21.5.1). It requires pipelines.can-view.
func (s *Server) getPipelineSource(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{PipelineID: id}) {
		return
	}
	src, err := s.clients.Database.GetPipelineSource(r.Context(), &dbpb.GetPipelineSourceRequest{PipelineId: id})
	if err != nil {
		grpcError(w, err)
		return
	}
	view := &pipelineSourceView{PipelineSource: src}
	// The last sync state is optional (a source that has never synced has no
	// state row); a NotFound is not an error here.
	if state, err := s.clients.Database.GetPipelineSourceState(r.Context(), &dbpb.GetPipelineSourceStateRequest{PipelineId: id}); err == nil {
		view.State = state
	}
	writeJSON(w, http.StatusOK, view)
}

// detachPipelineSource removes a pipeline's source (F-21.5.1). It requires
// pipelines.can-edit.
func (s *Server) detachPipelineSource(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesEdit, authz.Resource{PipelineID: id}) {
		return
	}
	if _, err := s.clients.Database.DetachPipelineSource(r.Context(), &dbpb.DetachPipelineSourceRequest{PipelineId: id}); err != nil {
		grpcError(w, err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionPipelineUpdate,
		TargetKind: audit.TargetPipeline,
		TargetID:   fmt.Sprintf("%d", id),
		TargetName: fmt.Sprintf("source-detach"),
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "detached"})
}

// refreshPipelineSource runs the sync routine for a pipeline's source
// (F-21.5.2, minimal slice): select the credential (an explicit name, else the
// longest matching prefix), decrypt it, dial the source-fetcher, and record the
// outcome on the source state (last_error on a failure — including a
// credential that fails to authenticate — cleared on a success). It requires
// pipelines.can-edit.
func (s *Server) refreshPipelineSource(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesEdit, authz.Resource{PipelineID: id}) {
		return
	}
	state, err := s.syncPipelineSource(r.Context(), id)
	if err != nil {
		// The sync failed (unreachable source, missing file, a credential that
		// fails to authenticate, …). The failure is already recorded on the
		// source state (last_error); surface it to the UI.
		httpError(w, http.StatusUnprocessableEntity, "sync failed: %v", err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionPipelineUpdate,
		TargetKind: audit.TargetPipeline,
		TargetID:   fmt.Sprintf("%d", id),
		TargetName: fmt.Sprintf("source-refresh"),
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusOK, state)
}

// syncPipelineSource is the sync routine (F-21.5.2, minimal slice): select the
// source's credential (an explicit credential name, else the longest matching
// prefix, F-21.3.2), decrypt it for a single fetch, dial the source-fetcher
// (F-21.2), and record the outcome on the source state. On a fetch failure the
// error is recorded on last_error (cleared on the next success) and returned,
// so a stale credential or a dead source is diagnosable without a repo
// checkout. The plaintext credential is never persisted or logged.
func (s *Server) syncPipelineSource(ctx context.Context, pipelineID int64) (*dbpb.PipelineSourceState, error) {
	if s.clients.PipelineSource == nil {
		return nil, fmt.Errorf("the source-fetcher is not configured")
	}
	src, err := s.clients.Database.GetPipelineSource(ctx, &dbpb.GetPipelineSourceRequest{PipelineId: pipelineID})
	if err != nil {
		return nil, fmt.Errorf("get source: %w", err)
	}
	// Select the credential (an explicit name, else the longest matching
	// prefix) and decrypt it for this single fetch (F-21.3.2).
	cred, err := s.selectSourceCredential(ctx, src.GetCredential(), src.GetReference())
	if err != nil {
		if state := s.recordSourceSyncError(ctx, pipelineID, err); state != nil {
			return state, err
		}
		return nil, err
	}
	fetchCred, err := s.decryptSourceCredential(ctx, cred)
	if err != nil {
		if state := s.recordSourceSyncError(ctx, pipelineID, err); state != nil {
			return state, err
		}
		return nil, err
	}
	resp, err := s.clients.PipelineSource.FetchSource(ctx, &pipelinesourcepb.FetchSourceRequest{
		Reference:  src.GetReference(),
		Credential: fetchCred,
	})
	if err != nil {
		// A fetch failure (unreachable source, missing file, a credential that
		// fails to authenticate — a rotated token, a revoked key) is recorded on
		// the source state's last_error so it is visible in the UI.
		if state := s.recordSourceSyncError(ctx, pipelineID, err); state != nil {
			return state, err
		}
		return nil, err
	}
	// Success: record the version + hash and clear last_error.
	state, err := s.clients.Database.SetPipelineSourceState(ctx, &dbpb.SetPipelineSourceStateRequest{
		PipelineId:   pipelineID,
		LastSyncedAt: timestamppb.New(time.Now().UTC()),
		LastVersion:  resp.GetVersion(),
		FileHash:     resp.GetHash(),
		LastError:    "",
	})
	if err != nil {
		return nil, fmt.Errorf("record source state: %w", err)
	}
	return state, nil
}

// recordSourceSyncError records a sync failure on the source state's
// last_error (F-21.5.2) and returns the updated state. It is best-effort: a
// failure to record the error (e.g. no state row yet) returns nil, and the
// caller still returns the original error.
func (s *Server) recordSourceSyncError(ctx context.Context, pipelineID int64, cause error) *dbpb.PipelineSourceState {
	state, err := s.clients.Database.SetPipelineSourceState(ctx, &dbpb.SetPipelineSourceStateRequest{
		PipelineId:   pipelineID,
		LastSyncedAt: timestamppb.New(time.Now().UTC()),
		LastError:    cause.Error(),
	})
	if err != nil {
		return nil
	}
	return state
}
