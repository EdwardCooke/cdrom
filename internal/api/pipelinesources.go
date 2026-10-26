package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/audit"
	"cdrom/internal/authz"
	"cdrom/internal/config"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	pipelinesourcepb "cdrom/internal/gen/cdrom/pipelinesource/v1"
	"cdrom/internal/sourceref"
)

// ---------------------------------------------------------------------------
// Pipeline sources (F-21.5.1 / F-21.5.2 / F-21.5.3)
// ---------------------------------------------------------------------------
//
// Where a pipeline is pulled from: a kind (git / http / file), a reference
// (the <type>:<spec> locator, F-21.2), and a credential name. The source is a
// database concept configured through the API (it is not in the file). HOW
// OFTEN it is pulled is the pipeline's file (spec.periodicRefresh, F-21.5.1),
// bounded by the admin gate (a global config knob + a per-pipeline database
// flag) and defaulted by the system refresh interval.
//
// The SYNC ROUTINE (F-21.5.2) is the single path every refresh trigger
// (F-21.5.3) goes through: select + decrypt the credential → fetch via the
// source-fetcher → compare the returned hash to the stored source_hash → on
// CHANGE recompile the file (F-21.1) and re-import it (creating/updating the
// pipeline, bumping its version, F-11) and store the new source_file /
// source_hash → on UNCHANGE no-op (only last_synced_at advances). A sync is
// SERIALIZED PER PIPELINE (a source is not synced concurrently by two
// triggers), and a failure is recorded on the source state's last_error
// (clearing it on the next success) WITHOUT wiping the last-good
// version/hash — the kickoff-provenance fallback leans on those survivors.
// The routine RETURNS THE SETTLED STATE so a caller (the refresh-on-run hook)
// can stamp kickoff provenance onto the run row.

// pipelineSourceRequest is the JSON form of a pipeline-source attach. The
// source is a pure location fact (F-21.5.1): the cadence the refresh loop
// honors lives in the pipeline's file (spec.periodicRefresh), not here.
type pipelineSourceRequest struct {
	Kind       string `json:"kind,omitempty"`
	Reference  string `json:"reference,omitempty"`
	Credential string `json:"credential,omitempty"`
}

// pipelineSourceView is the JSON form of a pipeline's source + its last sync
// state + its stored definition + its cadence view, returned by
// GET /api/pipelines/{id}/source.
type pipelineSourceView struct {
	*dbpb.PipelineSource
	State      *dbpb.PipelineSourceState `json:"state,omitempty"`
	SourceFile string                    `json:"source_file,omitempty"`
	Cadence    *cadenceView              `json:"cadence,omitempty"`
}

// cadenceView is the "why does this source poll at the rate it does" view
// (F-21.5.1): the file's DECLARED policy, the admin gate's verdict, and the
// RESULTING effective cadence.
type cadenceView struct {
	DeclaredInterval    string `json:"declared_interval,omitempty"`
	DeclaredSchedule    string `json:"declared_schedule,omitempty"`
	GlobalGateAllowed   bool   `json:"global_gate_allowed"`
	PipelineGateAllowed bool   `json:"pipeline_gate_allowed"`
	OverrideHonored     bool   `json:"override_honored"`
	OptedOut            bool   `json:"opted_out,omitempty"`
	EffectiveMode       string `json:"effective_mode"` // "cron" | "interval" | "none"
	EffectiveInterval   string `json:"effective_interval,omitempty"`
	EffectiveSchedule   string `json:"effective_schedule,omitempty"`
	SystemDefault       string `json:"system_default"`
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
		TargetName: "source",
		Outcome:    audit.OutcomeSuccess,
	})
	// Bootstrap: kick off the INITIAL sync asynchronously (F-21.5.3) so a
	// newly attached pipeline is usable promptly; a failure is tolerated
	// (recorded on the source state) and never fails the attach.
	go s.bootstrapSourceSync(id)
	writeJSON(w, http.StatusCreated, src)
}

// getPipelineSource returns a pipeline's source + its last sync state + its
// cadence view (F-21.5.1): the stored source_file (raw YAML), the source
// state, and WHY the source polls at the rate it does (the file's declared
// periodicRefresh, the admin gate's verdict, and the effective cadence). It
// requires pipelines.can-view.
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
	// The stored definition (F-21.5.1): the raw YAML the last successful sync
	// loaded (empty for a UI-authored pipeline or one never synced).
	// The cadence view (F-21.5.1): the pipeline's declared policy + the gate
	// verdict + the effective cadence. A pipeline fetch failure (rare — the
	// source implies existence) degrades gracefully: the view is omitted.
	if pipeline, err := s.clients.Database.GetPipeline(r.Context(), &dbpb.GetPipelineRequest{Id: id}); err == nil {
		view.SourceFile = pipeline.GetSourceFile()
		view.Cadence = s.cadenceView(pipeline)
	}
	writeJSON(w, http.StatusOK, view)
}

// cadenceView resolves a pipeline's effective refresh cadence (F-21.5.1) and
// renders the explanatory view: the file's declared policy, the admin gate's
// verdict (global config knob ∧ per-pipeline flag), and the resulting mode
// (cron / interval / none). Resolution happens HERE (fresh per read) and in
// the refresh loop (cached briefly) — the pure computation is shared via
// effectiveCadence.
func (s *Server) cadenceView(pipeline *dbpb.Pipeline) *cadenceView {
	global := s.globalOverridesAllowed()
	pipelineAllows := pipeline.GetPeriodicRefreshOverridesAllowed()
	honored := global && pipelineAllows
	mode, interval, schedule, optedOut := effectiveCadence(honored, pipeline.GetPeriodicRefreshInterval(), pipeline.GetPeriodicRefreshSchedule(), s.systemDefaultInterval())
	return &cadenceView{
		DeclaredInterval:    pipeline.GetPeriodicRefreshInterval(),
		DeclaredSchedule:    pipeline.GetPeriodicRefreshSchedule(),
		GlobalGateAllowed:   global,
		PipelineGateAllowed: pipelineAllows,
		OverrideHonored:     honored,
		OptedOut:            optedOut,
		EffectiveMode:       mode,
		EffectiveInterval:   interval,
		EffectiveSchedule:   schedule,
		SystemDefault:       s.systemDefaultInterval().String(),
	}
}

// systemDefaultInterval is the system-default steady-state cadence (F-21.5.1):
// the configured value, floored to the built-in default when unset/non-positive
// (a nil cfg floors to the built-in default too — tests without config stay
// sane).
func (s *Server) systemDefaultInterval() time.Duration {
	if s.cfg != nil {
		if d := s.cfg.PipelineSourceRefreshInterval; d > 0 {
			return d
		}
	}
	return config.DefaultPipelineSourceRefreshInterval
}

// globalOverridesAllowed is the GLOBAL half of the admin gate (F-21.5.1): the
// configured knob, defaulting to ALLOWED when unset (tests without config).
func (s *Server) globalOverridesAllowed() bool {
	if s.cfg == nil {
		return true
	}
	return s.cfg.PipelineSourceOverridesAllowed
}

// effectiveCadence resolves a pipeline's effective refresh cadence (F-21.5.1)
// from the admin-gate verdict and the file's declared policy:
//
//   - honored ∧ schedule set → MODE CRON (the cron drives; the interval is
//     ignored — precise timing, e.g. after-hours syncs when load is minimal).
//   - honored ∧ interval set → MODE INTERVAL (due at now >= last_attempt +
//     interval); an interval of ZERO opts the source OUT of periodic polling
//     (reported via optedOut; still refreshable on run kickoff or manually).
//   - otherwise (unhonored, or undeclared) → MODE INTERVAL at the SYSTEM
//     DEFAULT — denial restores the safe default; it never halts polling.
func effectiveCadence(honored bool, interval, schedule string, systemDefault time.Duration) (mode, effInterval, effSchedule string, optedOut bool) {
	if honored && schedule != "" {
		return "cron", "", schedule, false
	}
	if honored && interval != "" {
		if d, err := time.ParseDuration(interval); err == nil && d == 0 {
			return "interval", interval, "", true
		}
		return "interval", interval, "", false
	}
	return "interval", systemDefault.String(), "", false
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
		TargetName: "source-detach",
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "detached"})
}

// refreshPipelineSource runs the sync routine for a pipeline's source
// (MANUAL REFRESH, F-21.5.3): an explicit re-pull that applies the latest
// file regardless of cadence — the "I just pushed, pull it now" path. It
// requires pipelines.can-edit. A sync failure (unreachable source, missing
// file, a credential that fails to authenticate, a file that fails
// compilation) is ALREADY recorded on the source state (last_error) and is
// surfaced as 422.
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
		if errors.Is(err, ErrSyncDeferred) {
			// Another replica is mid-sync for this pipeline: polite refusal,
			// not a failure (the in-flight sync settles the state).
			httpError(w, http.StatusConflict, "a sync of this source is already in progress")
			return
		}
		httpError(w, http.StatusUnprocessableEntity, "sync failed: %v", err)
		return
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionPipelineUpdate,
		TargetKind: audit.TargetPipeline,
		TargetID:   fmt.Sprintf("%d", id),
		TargetName: "source-refresh",
		Outcome:    audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusOK, state)
}

// syncLock serializes a pipeline's syncs (F-21.5.2): a source is not synced
// concurrently by two triggers (a periodic tick and a run-kickoff refresh
// racing cannot double-apply a change). Locks are created lazily per pipeline
// and retained (bounded by the number of sourced pipelines; negligible).
func (s *Server) syncLock(pipelineID int64) *sync.Mutex {
	mu, _ := s.syncLocks.LoadOrStore(pipelineID, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// srcSyncOutcome classifies a source sync's result for the F-20 metrics
// (cdrom_api_source_syncs_total / _duration_seconds, by outcome). Low
// cardinality by construction (a fixed vocabulary, never a dynamic value).
type srcSyncOutcome string

const (
	// outcomeSuccess: a changed file was re-imported (version bump).
	outcomeSuccess srcSyncOutcome = "success"
	// outcomeUnchanged: the hash matched — a no-op (last_synced_at advanced).
	outcomeUnchanged srcSyncOutcome = "unchanged"
	// outcomeError: fetch / compile / re-import failed (last_error recorded).
	outcomeError srcSyncOutcome = "error"
	// outcomeDeferredPeer: another replica held the cross-replica sync lease
	// (ErrSyncDeferred) — this replica stood aside.
	outcomeDeferredPeer srcSyncOutcome = "deferred_peer"
	// outcomeSkippedBusy: the loop's skip-if-in-flight guard declined a due
	// fire (the source's previous sync is still running) — the next due
	// instant is tried again.
	outcomeSkippedBusy srcSyncOutcome = "skipped_busy"
	// outcomeSkippedThrottled: the loop's max-parallel throttle deferred a
	// due fire to the next tick (slots exhausted).
	outcomeSkippedThrottled srcSyncOutcome = "skipped_throttled"
)

// syncPipelineSource is the SYNCHRONOUS sync routine (F-21.5.2), serialized
// per pipeline: it runs doSyncPipelineSource under the pipeline's sync lock,
// behind the cross-replica lease, UNDER A BOUNDED CONTEXT (a zombie sync is
// impossible by construction), and OBSERVED (duration + outcome metrics, the
// in-flight gauge, and a rate-limited warning when a sync outpaces its own
// interval).
func (s *Server) syncPipelineSource(ctx context.Context, pipelineID int64) (*dbpb.PipelineSourceState, error) {
	start := time.Now()
	lock := s.syncLock(pipelineID)
	lock.Lock()
	defer lock.Unlock()
	// Cross-replica serialization (F-21.5.2): the local sync lock orders
	// triggers WITHIN this replica; a per-pipeline lease on the Database
	// service orders them ACROSS replicas (the leader's periodic sync vs. a
	// manual refresh striking another pod). Refuse politely when a peer is
	// mid-sync — the in-flight sync settles the state.
	holder := srcRefreshIdentity()
	if !s.acquireSrcSyncLease(ctx, pipelineID, holder) {
		s.observeSyncOutcome(outcomeDeferredPeer)
		return nil, ErrSyncDeferred
	}
	defer s.releaseSrcSyncLease(pipelineID, holder)
	// Bound the sync (F-21.5.3): a blackholed origin or stalled fetch gives
	// up after the configured timeout rather than occupying its goroutine
	// forever.
	syncCtx, cancel := context.WithTimeout(ctx, s.srcSyncTimeout())
	defer cancel()
	// NB: the in-flight GAUGE is owned by the issuing loop (it wraps its own
	// syncs); manual / bootstrap / refresh-on-run syncs are not counted here —
	// the gauge measures the loop's in-flight population (the throttle's
	// domain). Double-counting would skew both.
	state, outcome, err := s.doSyncPipelineSource(syncCtx, pipelineID)
	elapsed := time.Since(start)
	s.observeSyncOutcome(outcome)
	if s.metrics != nil {
		s.metrics.SourceSyncDuration.WithLabelValues(string(outcome)).Observe(elapsed.Seconds())
	}
	// Operability complement to the histogram: a sync that outpaced its own
	// effective interval WILL be skipped on intermediate fires — say so,
	// rate-limited.
	span := s.effectiveIntervalSpan(pipelineID)
	s.slowSyncWarner.note(s.serverLogger(), pipelineID, span, elapsed)
	return state, err
}

// observeSyncOutcome increments the outcome counter (nil-safe).
func (s *Server) observeSyncOutcome(o srcSyncOutcome) {
	if s.metrics != nil {
		s.metrics.SourceSyncs.WithLabelValues(string(o)).Inc()
	}
}

// effectiveIntervalSpan resolves a pipeline's effective PACING SPAN (F-21.5.3)
// for the slow-sync warning: the effective interval when interval-mode, 0
// when cron-mode (warnings disabled — cron precision is its purpose) or when
// the pipeline cannot be read.
func (s *Server) effectiveIntervalSpan(pipelineID int64) time.Duration {
	pipeline, err := s.clients.Database.GetPipeline(context.Background(), &dbpb.GetPipelineRequest{Id: pipelineID})
	if err != nil {
		return 0
	}
	honored := s.globalOverridesAllowed() && pipeline.GetPeriodicRefreshOverridesAllowed()
	mode, intervalStr, _, _ := effectiveCadence(honored, pipeline.GetPeriodicRefreshInterval(), pipeline.GetPeriodicRefreshSchedule(), s.systemDefaultInterval())
	if mode != "interval" {
		return 0
	}
	d, err := time.ParseDuration(intervalStr)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}

// doSyncPipelineSource is the sync routine CORE (F-21.5.2): select the
// credential (an explicit name, else the longest matching prefix, F-21.3.2) →
// decrypt it for a single fetch → fetch via the source-fetcher (F-21.2) →
// COMPARE the returned hash to the stored source_hash → on CHANGE recompile
// (F-21.1) + re-import (updating the pipeline, bumping its version, F-11) +
// store the new source_file / source_hash → on UNCHANGE no-op (advance
// last_synced_at only). Every failure (selection, fetch, compilation,
// re-import) is recorded on the source state's last_error (surviving
// version/hash intact — the kickoff-provenance fallback leans on them) and
// RETURNED. The plaintext credential is never persisted or logged. The
// routine returns the SETTLED state (even on failure) and the CLASSIFIED
// outcome (for the F-20 metrics) so callers can observe both.
func (s *Server) doSyncPipelineSource(ctx context.Context, pipelineID int64) (*dbpb.PipelineSourceState, srcSyncOutcome, error) {
	if s.clients.PipelineSource == nil {
		return nil, outcomeError, fmt.Errorf("the source-fetcher is not configured")
	}
	src, err := s.clients.Database.GetPipelineSource(ctx, &dbpb.GetPipelineSourceRequest{PipelineId: pipelineID})
	if err != nil {
		return nil, outcomeError, fmt.Errorf("get source: %w", err)
	}
	// Select + decrypt the credential for this single fetch (F-21.3.2).
	cred, err := s.selectSourceCredential(ctx, src.GetCredential(), src.GetReference())
	if err != nil {
		if state := s.setSourceSyncFailure(ctx, pipelineID, err); state != nil {
			return state, outcomeError, err
		}
		return nil, outcomeError, err
	}
	fetchCred, err := s.decryptSourceCredential(ctx, cred)
	if err != nil {
		if state := s.setSourceSyncFailure(ctx, pipelineID, err); state != nil {
			return state, outcomeError, err
		}
		return nil, outcomeError, err
	}
	// Fetch the definition file (F-21.2). A git fetch carries the
	// runtime-managed SSH known hosts (F-21.2.4.1) — strict host-key
	// verification refuses an unrecognized host.
	req := &pipelinesourcepb.FetchSourceRequest{
		Reference:  src.GetReference(),
		Credential: fetchCred,
	}
	if ref, perr := sourceref.ParseReference(src.GetReference()); perr == nil && ref.Kind == sourceref.KindGit {
		req.KnownHosts = s.runtimeKnownHosts(ctx)
	}
	resp, err := s.clients.PipelineSource.FetchSource(ctx, req)
	if err != nil {
		// A fetch failure (unreachable source, missing file, a credential that
		// fails to authenticate — a rotated token, a revoked key) is recorded
		// on the source state's last_error so it is visible in the UI.
		if state := s.setSourceSyncFailure(ctx, pipelineID, err); state != nil {
			return state, outcomeError, err
		}
		return nil, outcomeError, err
	}
	// The change detector: compare the fetched hash to the STORED one.
	pipeline, err := s.clients.Database.GetPipeline(ctx, &dbpb.GetPipelineRequest{Id: pipelineID})
	if err != nil {
		return nil, outcomeError, fmt.Errorf("get pipeline: %w", err)
	}
	now := timestamppb.New(time.Now().UTC())
	if pipeline.GetSourceHash() != "" && pipeline.GetSourceHash() == resp.GetHash() {
		// UNCHANGED: a no-op — no recompile, no version bump, no re-dispatch.
		// Advance last_synced_at only (the database service preserves the
		// surviving version/hash and clears last_error).
		state, err := s.clients.Database.SetPipelineSourceState(ctx, &dbpb.SetPipelineSourceStateRequest{
			PipelineId:   pipelineID,
			LastSyncedAt: now,
		})
		if err != nil {
			return nil, outcomeError, fmt.Errorf("record source state: %w", err)
		}
		return state, outcomeUnchanged, nil
	}
	// CHANGED (or first-ever sync): recompile + re-import (bumps the
	// pipeline's version, F-11) and store the new source_file / source_hash.
	if err := s.reimportPipelineFile(ctx, pipelineID, pipeline.GetName(), src.GetReference(), resp.GetContent(), resp.GetHash()); err != nil {
		// A re-import failure (the new file fails compilation, or the update
		// is rejected) is a SYNC FAILURE: the pipeline keeps its last-good
		// definition and the error is recorded.
		if state := s.setSourceSyncFailure(ctx, pipelineID, err); state != nil {
			return state, outcomeError, err
		}
		return nil, outcomeError, err
	}
	state, err := s.clients.Database.SetPipelineSourceState(ctx, &dbpb.SetPipelineSourceStateRequest{
		PipelineId:   pipelineID,
		LastSyncedAt: now,
		LastVersion:  resp.GetVersion(),
		FileHash:     resp.GetHash(),
	})
	if err != nil {
		return nil, outcomeError, fmt.Errorf("record source state: %w", err)
	}
	return state, outcomeSuccess, nil
}

// reimportPipelineFile recompiles a freshly-fetched definition file (F-21.1)
// and re-imports it as an UPDATE of the pipeline (F-21.5.2): the file's jobs,
// triggers, params, variable sets, secret sources, deployment options, labels,
// and periodic-refresh policy REPLACE the pipeline's (bumping its version,
// F-11), and the new source_file / source_hash are stored. The pipeline's
// NAME is KEPT (adopting the file's metadata.name would rename the pipeline
// and break event triggers / webhook references that name it), and its
// SECRETS are preserved (the file format carries no plaintext secrets —
// values are encrypted ciphertexts or secret-source references). Imports in
// the file resolve against the pipeline's source (relative locators).
func (s *Server) reimportPipelineFile(ctx context.Context, pipelineID int64, name, reference string, content []byte, hash string) error {
	root, err := sourceref.ParseReference(reference)
	if err != nil {
		return fmt.Errorf("parse source reference: %w", err)
	}
	req, variableSets, secretSources, err := requestFromFile(ctx, name, content, root, s.pipelineFileFetcher())
	if err != nil {
		return fmt.Errorf("compile definition: %w", err)
	}
	jobs, err := pipelineJobsToProto(req.Jobs)
	if err != nil {
		return fmt.Errorf("invalid jobs: %w", err)
	}
	policy := periodicRefreshPolicy(req)
	upd := &dbpb.UpdatePipelineRequest{
		Id:              pipelineID,
		Name:            name, // keep the pipeline's existing name (see above)
		Description:     req.Description,
		FailureMode:     failureModeFromName(req.FailureMode),
		Jobs:            jobs,
		Triggers:        triggersToProto(req.Triggers),
		Params:          pipelineParamsToProto(req.Params),
		ConcurrencyKeys: req.ConcurrencyKeys,
		Tags:            req.Tags,
		VariableSets:    variableSets,
		SecretSources:   secretSources,
		// The file's periodic-refresh policy (may be ABSENT — a file that
		// declares none resets the pipeline to the system default cadence).
		HasPeriodicRefresh:          true,
		PeriodicRefreshInterval:     policy.interval,
		PeriodicRefreshSchedule:     policy.schedule,
		PeriodicRefreshOverridesAllowed: policy.allowed,
		// The freshly-fetched definition (verbatim) + its change detector.
		SourceFile: string(content),
		SourceHash: hash,
	}
	if _, err := s.clients.Database.UpdatePipeline(ctx, upd); err != nil {
		return fmt.Errorf("re-import definition: %w", err)
	}
	return nil
}

// setSourceSyncFailure records a sync failure on the source state's
// last_error (F-21.5.2) and returns the UPDATED state. It is best-effort: a
// failure to record the error (e.g. no state row yet) returns nil, and the
// caller still returns the original error. The database service PRESERVES the
// last-good version/hash on a failure record (empty values are not wiped), so
// the kickoff-provenance fallback survives a failed sync.
func (s *Server) setSourceSyncFailure(ctx context.Context, pipelineID int64, cause error) *dbpb.PipelineSourceState {
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

// refreshOnRun performs the REFRESH-ON-RUN hook (F-21.5.3): just before a run
// of an externally-sourced pipeline is created, sync the source so the run
// executes the latest reviewed definition — "what I just kicked off is what
// is in the source right now" — without waiting for the next periodic tick.
// A FAILURE DOES NOT BLOCK THE RUN: the run proceeds on the last
// successfully-synced definition (the kickoff-provenance stamp, made inside
// the run-creation transaction, reads the surviving last-good state), and the
// failure is recorded on the source state (surfaced to the UI). Pipelines
// with no attached source (UI-authored) or no configured fetcher are no-ops.
func (s *Server) refreshOnRun(ctx context.Context, pipelineID int64) {
	if s.clients.PipelineSource == nil {
		return
	}
	if _, err := s.clients.Database.GetPipelineSource(ctx, &dbpb.GetPipelineSourceRequest{PipelineId: pipelineID}); err != nil {
		return // no source (UI-authored pipeline) — nothing to refresh
	}
	if _, err := s.syncPipelineSource(ctx, pipelineID); err != nil {
		if errors.Is(err, ErrSyncDeferred) {
			return // a peer's sync is settling the state; proceed on it
		}
		s.serverLogger().Warn("api: refresh-on-run failed; proceeding on last good definition",
			"pipeline", pipelineID, "err", err)
	}
}

// bootstrapSourceSync kicks off the INITIAL sync after a source is attached
// (F-21.5.3): a newly attached pipeline is usable promptly rather than
// waiting up to one cadence period. It is TOLERANT of failure (recorded on
// the source state, like a refresh-on-run failure) and runs in the CALLER's
// goroutine discipline — attach fires it asynchronously so the attach
// responds immediately.
func (s *Server) bootstrapSourceSync(pipelineID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := s.syncPipelineSource(ctx, pipelineID); err != nil {
		s.serverLogger().Warn("api: initial source sync failed", "pipeline", pipelineID, "err", err)
	}
}
