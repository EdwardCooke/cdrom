package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"text/template"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// Concurrency resource names. A concurrency key is its own name; the worker
// group's cap and the system-wide cap are synthetic resources.
const (
	// globalResourceKey is the synthetic resource for the system-wide cap.
	globalResourceKey = "global"
	// groupResourcePrefix prefixes a worker group's synthetic resource name.
	groupResourcePrefix = "group:"
)

// Concurrency limit rule kinds (F-16): the dimension a limit rule bounds.
const (
	limitKindGlobal = "global"
	limitKindGroup  = "group"
	limitKindKey    = "key"
)

// limitCacheTTL is how long the Database service caches the concurrency limit
// rules before reloading them (F-16). The cache is invalidated immediately
// when a rule is created, updated, or deleted; the TTL is a safety net so a
// rule edited on another database replica takes effect without a restart.
const limitCacheTTL = 5 * time.Second

// errConcurrencyBlocked is a sentinel returned from the AcquireJobSlots
// transaction when a guarded slot increment loses a race (a concurrent
// replica took the last free slot between the check and the increment). It
// aborts the transaction (rolling back any increments already made) and is
// reported to the caller as a blocked admission, not an error.
var errConcurrencyBlocked = errors.New("concurrency: slot increment lost a race")

// resourceSpec is one resource a job consumes and its limit.
type resourceSpec struct {
	key   string
	limit int // > 0: a finite limit (the resource is consumed)
}

// limitRule is one compiled concurrency limit rule (F-16): the dimension it
// bounds, its regex pattern (and the compiled form), and its limit.
type limitRule struct {
	kind    string
	pattern string
	re      *regexp.Regexp
	limit   int
}

// limitRules is the set of concurrency limit rules, grouped by kind, used to
// resolve a concrete resource's effective limit (F-16).
type limitRules struct {
	global *limitRule // the single global rule (empty pattern); nil = none
	groups []limitRule
	keys   []limitRule
}

// globalLimit returns the system-wide cap (0 = unlimited).
func (r *limitRules) globalLimit() int {
	if r.global != nil {
		return r.global.limit
	}
	return 0
}

// groupLimit returns the cap for a worker group: the limit of the most
// specific (longest) matching group rule, or 0 (unlimited) when no rule
// matches.
func (r *limitRules) groupLimit(group string) int {
	if rule := bestMatch(r.groups, group); rule != nil {
		return rule.limit
	}
	return 0
}

// keyLimit returns the cap for a concurrency key: the limit of the most
// specific (longest) matching key rule, or the default of 1 (a lock) when no
// rule matches.
func (r *limitRules) keyLimit(key string) int {
	if rule := bestMatch(r.keys, key); rule != nil {
		return rule.limit
	}
	return 1
}

// bestMatch returns the most specific (longest pattern) rule that matches name,
// or nil when no rule matches. Ties on pattern length are broken by pattern
// (lexicographic) for determinism.
func bestMatch(rules []limitRule, name string) *limitRule {
	var best *limitRule
	for i := range rules {
		rule := &rules[i]
		if !rule.re.MatchString(name) {
			continue
		}
		if best == nil ||
			len(rule.pattern) > len(best.pattern) ||
			(len(rule.pattern) == len(best.pattern) && rule.pattern < best.pattern) {
			best = rule
		}
	}
	return best
}

// loadLimitRules reads every concurrency limit rule from the database and
// compiles it into a limitRules set. A rule with an invalid (non-compiling)
// pattern is skipped (it should never be stored — it is validated at save
// time — but a stray one must not break every admission).
func (s *Server) loadLimitRules(ctx context.Context) (*limitRules, error) {
	var rules []models.ConcurrencyLimit
	if err := s.db.WithContext(ctx).Find(&rules).Error; err != nil {
		return nil, grpcErr(err)
	}
	out := &limitRules{}
	for i := range rules {
		rule := &rules[i]
		re, err := regexp.Compile(rule.Pattern)
		if err != nil {
			continue
		}
		lr := limitRule{kind: rule.Kind, pattern: rule.Pattern, re: re, limit: rule.Limit}
		switch rule.Kind {
		case limitKindGlobal:
			g := lr
			out.global = &g
		case limitKindGroup:
			out.groups = append(out.groups, lr)
		case limitKindKey:
			out.keys = append(out.keys, lr)
		}
	}
	return out, nil
}

// limitRulesCached returns the concurrency limit rules, loading them from the
// database when the cache is empty or older than limitCacheTTL. The cache is
// invalidated immediately when a rule changes (invalidateLimits), so a change
// takes effect on the next admission; the TTL is a safety net for changes made
// on another database replica.
func (s *Server) limitRulesCached(ctx context.Context) (*limitRules, error) {
	s.limitsMu.Lock()
	if s.limitsCache != nil && time.Since(s.limitsAt) < limitCacheTTL {
		cached := s.limitsCache
		s.limitsMu.Unlock()
		return cached, nil
	}
	s.limitsMu.Unlock()
	rules, err := s.loadLimitRules(ctx)
	if err != nil {
		return nil, err
	}
	s.limitsMu.Lock()
	s.limitsCache = rules
	s.limitsAt = time.Now()
	s.limitsMu.Unlock()
	return rules, nil
}

// invalidateLimits drops the cached concurrency limit rules (F-16) so the next
// admission reloads them. It is called after a limit rule is created, updated,
// or deleted.
func (s *Server) invalidateLimits() {
	s.limitsMu.Lock()
	s.limitsCache = nil
	s.limitsMu.Unlock()
}

// jobResources returns the resources a job consumes and each one's limit
// (F-16): each of the job's concurrency keys (a key matching no limit rule
// defaults to a limit of 1 — a lock), the job's target group's synthetic
// resource (when the group has a limit), and the global resource (when a
// global limit is set). A resource with a limit of 0 is unlimited and is not
// consumed.
func (s *Server) jobResources(ctx context.Context, job *models.Job) ([]resourceSpec, error) {
	rules, err := s.limitRulesCached(ctx)
	if err != nil {
		return nil, err
	}
	var specs []resourceSpec
	for _, key := range job.ConcurrencyKeys {
		if key == "" {
			continue
		}
		if limit := rules.keyLimit(key); limit > 0 {
			specs = append(specs, resourceSpec{key: key, limit: limit})
		}
	}
	if job.TargetGroup != "" {
		if limit := rules.groupLimit(job.TargetGroup); limit > 0 {
			specs = append(specs, resourceSpec{key: groupResourcePrefix + job.TargetGroup, limit: limit})
		}
	}
	if limit := rules.globalLimit(); limit > 0 {
		specs = append(specs, resourceSpec{key: globalResourceKey, limit: limit})
	}
	return specs, nil
}

// concurrencyKeyData builds the data a concurrency key template is rendered
// against (F-16): the run's parameter values (top-level, so `{{ .name }}`
// works) plus the structured keys "params" (the run's parameter values) and
// "run" (the run's identity: id, pipeline id, trigger, trigger name). It
// mirrors the executor's spec-interpolation data (F-10) minus secrets and
// step outputs, which are not available at run-creation time. A key that
// references a parameter with no value fails to render (missingkey=error), so
// a run is never created with a concurrency key that could not be resolved.
func concurrencyKeyData(params map[string]string, runID, pipelineID int64, trigger, triggerName string) map[string]any {
	if params == nil {
		params = map[string]string{}
	}
	data := make(map[string]any, len(params)+2)
	for k, v := range params {
		data[k] = v
	}
	data["params"] = params
	data["run"] = struct {
		ID          int64  `json:"id"`
		PipelineID  int64  `json:"pipeline_id"`
		Trigger     string `json:"trigger"`
		TriggerName string `json:"trigger_name"`
	}{ID: runID, PipelineID: pipelineID, Trigger: trigger, TriggerName: triggerName}
	return data
}

// renderConcurrencyKeys renders each concurrency key as a Go template against
// data (F-16) and returns the rendered keys, de-duplicated and in a stable
// order (first occurrence wins). A key that fails to render (e.g. it
// references a parameter with no value) is an error: a run is never created
// with a concurrency key that could not be resolved. Empty keys are dropped.
func renderConcurrencyKeys(keys []string, data map[string]any) ([]string, error) {
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		rendered, err := renderConcurrencyKey(key, data)
		if err != nil {
			return nil, err
		}
		if rendered == "" {
			continue
		}
		if _, ok := seen[rendered]; ok {
			continue
		}
		seen[rendered] = struct{}{}
		out = append(out, rendered)
	}
	return out, nil
}

// renderConcurrencyKey renders one concurrency key as a Go template against
// data. It uses missingkey=error, so a reference to a parameter with no value
// (no supplied value and no default) fails rather than silently rendering to
// an empty string — an undefined reference is a spec error.
func renderConcurrencyKey(key string, data map[string]any) (string, error) {
	tmpl, err := template.New("concurrency").Option("missingkey=error").Parse(key)
	if err != nil {
		return "", fmt.Errorf("parse concurrency key %q: %w", key, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render concurrency key %q: %w", key, err)
	}
	return buf.String(), nil
}

// ensureResource returns the ConcurrencyResource row for key, creating it
// (with Held 0) if it does not exist yet.
func (s *Server) ensureResource(tx *gorm.DB, key string) (*models.ConcurrencyResource, error) {
	var res models.ConcurrencyResource
	if err := tx.Where("key = ?", key).FirstOrCreate(&res, models.ConcurrencyResource{Key: key}).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &res, nil
}

// AcquireJobSlots atomically admits a job for its current attempt (F-16): it
// checks that, for every resource the job consumes, the number of current
// holders is below the resource's limit, and, when every resource has room,
// records the job as a holder of each (incrementing each resource's Held
// counter) and marks the job admitted. When a resource is at its limit the
// job is not admitted (it stays pending) and the response lists the blocking
// resources. The check-and-increment is a single transaction, so multiple
// scheduler replicas can never double-allocate the last free slot. It is an
// idempotent no-op (acquired=false) when the job is already admitted.
func (s *Server) AcquireJobSlots(ctx context.Context, req *dbpb.AcquireJobSlotsRequest) (*dbpb.AcquireJobSlotsResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	jobID := uint(req.GetJobId())
	resp := &dbpb.AcquireJobSlotsResponse{}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job models.Job
		if err := tx.First(&job, jobID).Error; err != nil {
			return grpcErr(err)
		}
		if job.ConcurrencyAdmitted {
			// Already admitted for the current attempt: idempotent no-op. The
			// caller must not dispatch the job again.
			return nil
		}
		specs, err := s.jobResources(ctx, &job)
		if err != nil {
			return err
		}
		// Check each resource has room; record the ones that are full.
		var blocked []string
		for _, spec := range specs {
			res, err := s.ensureResource(tx, spec.key)
			if err != nil {
				return err
			}
			if res.Held >= spec.limit {
				blocked = append(blocked, spec.key)
			}
		}
		if len(blocked) > 0 {
			resp.Acquired = false
			resp.BlockedBy = blocked
			return nil
		}
		// Acquire each resource atomically: the held < limit guard makes the
		// increment safe against a concurrent replica taking the last slot.
		for _, spec := range specs {
			result := tx.Model(&models.ConcurrencyResource{}).
				Where("key = ? AND held < ?", spec.key, spec.limit).
				UpdateColumn("held", gorm.Expr("held + 1"))
			if result.Error != nil {
				return grpcErr(result.Error)
			}
			if result.RowsAffected == 0 {
				// A concurrent replica took the last slot between the check
				// and the increment: abort the transaction (rolling back any
				// increments already made) and report the job as blocked.
				resp.Acquired = false
				resp.BlockedBy = append(resp.BlockedBy, spec.key)
				return errConcurrencyBlocked
			}
		}
		now := time.Now()
		if err := tx.Model(&models.Job{}).
			Where("id = ?", jobID).
			Updates(map[string]any{
				"concurrency_admitted":    true,
				"concurrency_admitted_at": &now,
				"updated_at":              time.Now(),
			}).Error; err != nil {
			return grpcErr(err)
		}
		resp.Acquired = true
		return nil
	})
	if errors.Is(err, errConcurrencyBlocked) {
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// releaseJobSlots releases the slots a job holds for its current attempt: it
// decrements each resource's Held counter (the job's concurrency keys, group,
// and global resources) and clears the job's admitted flag. It is a no-op
// when the job is not admitted.
func (s *Server) releaseJobSlots(ctx context.Context, jobID uint) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job models.Job
		if err := tx.First(&job, jobID).Error; err != nil {
			return grpcErr(err)
		}
		if !job.ConcurrencyAdmitted {
			return nil
		}
		specs, err := s.jobResources(ctx, &job)
		if err != nil {
			return err
		}
		for _, spec := range specs {
			res, err := s.ensureResource(tx, spec.key)
			if err != nil {
				return err
			}
			if res.Held > 0 {
				// Decrement the held count. The held > 0 guard (in addition to
				// the check above) keeps the counter from going negative if a
				// concurrent release races this one; a plain "held - 1" is used
				// rather than GREATEST(held - 1, 0) because GREATEST is not a
				// SQLite function (the pure-Go SQLite driver has no such
				// builtin), and the guard makes the clamp unnecessary.
				if err := tx.Model(&models.ConcurrencyResource{}).
					Where("key = ? AND held > ?", spec.key, 0).
					UpdateColumn("held", gorm.Expr("held - 1")).Error; err != nil {
					return grpcErr(err)
				}
			}
		}
		if err := tx.Model(&models.Job{}).
			Where("id = ?", jobID).
			Updates(map[string]any{
				"concurrency_admitted":    false,
				"concurrency_admitted_at": nil,
				"updated_at":              time.Now(),
			}).Error; err != nil {
			return grpcErr(err)
		}
		return nil
	})
}

// ReleaseJobSlots releases the slots a job holds for its current attempt
// (F-16). The scheduler calls it when a job reaches a terminal state (so its
// slots free for the next job) or when a job is reset for a retry (so the
// retry re-acquires fresh slots). It is a no-op when the job holds no slots.
func (s *Server) ReleaseJobSlots(ctx context.Context, req *dbpb.ReleaseJobSlotsRequest) (*dbpb.ReleaseJobSlotsResponse, error) {
	if req.GetJobId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if err := s.releaseJobSlots(ctx, uint(req.GetJobId())); err != nil {
		return nil, err
	}
	return &dbpb.ReleaseJobSlotsResponse{}, nil
}

// GetQueueStatus returns the current concurrency/queue state (F-16): the
// pending jobs (with the resource(s) blocking each, if any), the per-group
// load, the global load, and the per-key load. This is what the UI's queue
// view (GET /api/queue) and the WebSocket snapshot show.
func (s *Server) GetQueueStatus(ctx context.Context, req *dbpb.GetQueueStatusRequest) (*dbpb.QueueStatus, error) {
	resp := &dbpb.QueueStatus{}
	db := s.db.WithContext(ctx)

	// The concurrency limit rules (F-16) resolve each resource's limit; they
	// are cached and invalidated on change.
	rules, err := s.limitRulesCached(ctx)
	if err != nil {
		return nil, err
	}

	// Load every resource's held count once, so the per-job blocking check
	// below does not issue a query per (job, resource).
	var resources []models.ConcurrencyResource
	if err := db.Find(&resources).Error; err != nil {
		return nil, grpcErr(err)
	}
	heldByKey := make(map[string]int, len(resources))
	for i := range resources {
		heldByKey[resources[i].Key] = resources[i].Held
	}

	// Pending jobs, each with the resource(s) blocking it (when it is not
	// already admitted — an admitted pending job is about to be dispatched).
	var pending []models.Job
	if err := db.Where("status = ?", models.JobStatusPending).Order("id").Find(&pending).Error; err != nil {
		return nil, grpcErr(err)
	}
	for i := range pending {
		job := &pending[i]
		qj := &dbpb.QueuePendingJob{
			Id:          int64(job.ID),
			Name:        job.Name,
			TargetGroup: job.TargetGroup,
			CreatedAt:   timestamppb.New(job.CreatedAt),
		}
		if job.PipelineID != nil {
			qj.PipelineId = int64(*job.PipelineID)
		}
		if job.RunID != nil {
			qj.RunId = int64(*job.RunID)
		}
		if !job.ConcurrencyAdmitted {
			specs, err := s.jobResources(ctx, job)
			if err != nil {
				return nil, err
			}
			for _, spec := range specs {
				if heldByKey[spec.key] >= spec.limit {
					qj.BlockedBy = append(qj.BlockedBy, spec.key)
				}
			}
		}
		resp.Pending = append(resp.Pending, qj)
	}

	// Global load.
	var globalRunning int64
	if err := db.Model(&models.Job{}).Where("status = ?", models.JobStatusRunning).Count(&globalRunning).Error; err != nil {
		return nil, grpcErr(err)
	}
	resp.GlobalRunning = int32(globalRunning)
	resp.GlobalLimit = int32(rules.globalLimit())

	// Per-group load: one per worker group that has a running job, with the
	// group's effective limit (resolved from the group limit rules; 0 =
	// unlimited). The selected columns map onto the struct's exported fields
	// (target_group -> TargetGroup, the count alias -> Running); "group" is
	// not used as an alias because it is a reserved word in SQLite.
	type groupLoad struct {
		TargetGroup string
		Running     int64
	}
	var groupLoads []groupLoad
	if err := db.Model(&models.Job{}).
		Where("status = ? AND target_group <> ''", models.JobStatusRunning).
		Select("target_group, count(*) as running").
		Group("target_group").
		Scan(&groupLoads).Error; err != nil {
		return nil, grpcErr(err)
	}
	for _, gl := range groupLoads {
		resp.Groups = append(resp.Groups, &dbpb.QueueGroupLoad{
			Group:   gl.TargetGroup,
			Running: int32(gl.Running),
			Limit:   int32(rules.groupLimit(gl.TargetGroup)),
		})
	}

	// Per-key load: one per concurrency key that currently has a holder, with
	// the key's effective limit (resolved from the key limit rules; a key
	// matching no rule defaults to a limit of 1).
	for _, res := range resources {
		if res.Held <= 0 {
			continue
		}
		if res.Key == globalResourceKey || strings.HasPrefix(res.Key, groupResourcePrefix) {
			continue
		}
		resp.Keys = append(resp.Keys, &dbpb.QueueKeyLoad{
			Key:     res.Key,
			Holders: int32(res.Held),
			Limit:   int32(rules.keyLimit(res.Key)),
		})
	}
	return resp, nil
}

// ReconcileConcurrency releases the slots of any job that reached a terminal
// state but still holds them (a missed release, e.g. a crash between the
// status change and the release). The scheduler's admission loop calls it as
// a safety net before admitting pending jobs: the terminal-status RPCs
// (UpdateJob, ReapJob, CancelJob, SkipJob) release a job's slots when they
// transition it to a terminal state, and this catches any that were missed,
// so a stuck job cannot wedge a concurrency resource forever.
func (s *Server) ReconcileConcurrency(ctx context.Context, req *dbpb.ReconcileConcurrencyRequest) (*dbpb.ReconcileConcurrencyResponse, error) {
	var stuck []models.Job
	err := s.db.WithContext(ctx).
		Where("concurrency_admitted = ? AND status IN ?", true,
			[]models.JobStatus{
				models.JobStatusSucceeded, models.JobStatusFailed,
				models.JobStatusCancelled, models.JobStatusTimedOut,
				models.JobStatusSkipped,
			}).
		Find(&stuck).Error
	if err != nil {
		return nil, grpcErr(err)
	}
	released := int32(0)
	for i := range stuck {
		if err := s.releaseJobSlots(ctx, stuck[i].ID); err != nil {
			return nil, err
		}
		released++
	}
	return &dbpb.ReconcileConcurrencyResponse{Released: released}, nil
}

// ---------------------------------------------------------------------------
// Concurrency limit rules (F-16)
// ---------------------------------------------------------------------------

// validLimitKind reports whether kind is a concurrency limit rule kind.
func validLimitKind(kind string) bool {
	switch kind {
	case limitKindGlobal, limitKindGroup, limitKindKey:
		return true
	default:
		return false
	}
}

// validateConcurrencyLimitRule checks a limit rule's fields: the kind must be
// one of global/group/key, the pattern must compile (an empty pattern is a
// catch-all that matches every name of the kind), and the limit must be
// non-negative. For the single "global" resource the pattern is forced empty.
func validateConcurrencyLimitRule(kind, pattern string, limit int) error {
	if !validLimitKind(kind) {
		return status.Errorf(codes.InvalidArgument, "kind must be one of %q, %q, or %q", limitKindGlobal, limitKindGroup, limitKindKey)
	}
	if kind == limitKindGlobal {
		pattern = ""
	}
	if _, err := regexp.Compile(pattern); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid pattern %q: %v", pattern, err)
	}
	if limit < 0 {
		return status.Errorf(codes.InvalidArgument, "limit must be non-negative (got %d)", limit)
	}
	return nil
}

// toProtoConcurrencyLimit renders a concurrency limit rule as its
// Database-service form.
func toProtoConcurrencyLimit(l *models.ConcurrencyLimit) *dbpb.ConcurrencyLimit {
	return &dbpb.ConcurrencyLimit{
		Id:        int64(l.ID),
		Kind:      l.Kind,
		Pattern:   l.Pattern,
		Limit:     int32(l.Limit),
		CreatedAt: timestamppb.New(l.CreatedAt),
		UpdatedAt: timestamppb.New(l.UpdatedAt),
	}
}

// ListConcurrencyLimits returns every concurrency limit rule (F-16), ordered
// by kind then pattern, optionally filtered by kind (an empty kind returns
// rules of every kind).
func (s *Server) ListConcurrencyLimits(ctx context.Context, req *dbpb.ListConcurrencyLimitsRequest) (*dbpb.ListConcurrencyLimitsResponse, error) {
	query := s.db.WithContext(ctx).Model(&models.ConcurrencyLimit{})
	if kind := req.GetKind(); kind != "" {
		if !validLimitKind(kind) {
			return nil, status.Errorf(codes.InvalidArgument, "kind must be one of %q, %q, or %q", limitKindGlobal, limitKindGroup, limitKindKey)
		}
		query = query.Where("kind = ?", kind)
	}
	var rules []models.ConcurrencyLimit
	if err := query.Order("kind, pattern").Find(&rules).Error; err != nil {
		return nil, grpcErr(err)
	}
	resp := &dbpb.ListConcurrencyLimitsResponse{}
	for i := range rules {
		resp.Limits = append(resp.Limits, toProtoConcurrencyLimit(&rules[i]))
	}
	return resp, nil
}

// CreateConcurrencyLimit adds a concurrency limit rule (F-16). It rejects an
// invalid kind, an invalid (non-compiling) pattern, a negative limit, and a
// duplicate (kind, pattern) rule. It invalidates the limit cache so the new
// rule takes effect on the next admission.
func (s *Server) CreateConcurrencyLimit(ctx context.Context, req *dbpb.CreateConcurrencyLimitRequest) (*dbpb.ConcurrencyLimit, error) {
	kind := req.GetKind()
	pattern := req.GetPattern()
	limit := int(req.GetLimit())
	if kind == limitKindGlobal {
		pattern = ""
	}
	if err := validateConcurrencyLimitRule(kind, pattern, limit); err != nil {
		return nil, err
	}
	var existing models.ConcurrencyLimit
	if err := s.db.WithContext(ctx).Where("kind = ? AND pattern = ?", kind, pattern).First(&existing).Error; err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "a %q limit rule with pattern %q already exists", kind, pattern)
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, grpcErr(err)
	}
	rule := &models.ConcurrencyLimit{Kind: kind, Pattern: pattern, Limit: limit}
	if err := s.db.WithContext(ctx).Create(rule).Error; err != nil {
		return nil, grpcErr(err)
	}
	s.invalidateLimits()
	return toProtoConcurrencyLimit(rule), nil
}

// UpdateConcurrencyLimit edits a concurrency limit rule's pattern and/or
// limit (F-16). A missing rule returns NotFound. It invalidates the limit
// cache so the change takes effect on the next admission.
func (s *Server) UpdateConcurrencyLimit(ctx context.Context, req *dbpb.UpdateConcurrencyLimitRequest) (*dbpb.ConcurrencyLimit, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var rule models.ConcurrencyLimit
	if err := s.db.WithContext(ctx).First(&rule, uint(req.GetId())).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "concurrency limit rule not found")
		}
		return nil, grpcErr(err)
	}
	pattern := rule.Pattern
	if req.GetPattern() != "" {
		pattern = req.GetPattern()
	}
	limit := rule.Limit
	if req.Limit != nil {
		limit = int(req.GetLimit())
	}
	if rule.Kind == limitKindGlobal {
		pattern = ""
	}
	if err := validateConcurrencyLimitRule(rule.Kind, pattern, limit); err != nil {
		return nil, err
	}
	rule.Pattern = pattern
	rule.Limit = limit
	if err := s.db.WithContext(ctx).Save(&rule).Error; err != nil {
		return nil, grpcErr(err)
	}
	s.invalidateLimits()
	return toProtoConcurrencyLimit(&rule), nil
}

// DeleteConcurrencyLimit removes a concurrency limit rule (F-16). A missing
// rule returns NotFound. It invalidates the limit cache so the removal takes
// effect on the next admission.
func (s *Server) DeleteConcurrencyLimit(ctx context.Context, req *dbpb.DeleteConcurrencyLimitRequest) (*emptypb.Empty, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var rule models.ConcurrencyLimit
	if err := s.db.WithContext(ctx).First(&rule, uint(req.GetId())).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "concurrency limit rule not found")
		}
		return nil, grpcErr(err)
	}
	if err := s.db.WithContext(ctx).Delete(&rule).Error; err != nil {
		return nil, grpcErr(err)
	}
	s.invalidateLimits()
	return &emptypb.Empty{}, nil
}
