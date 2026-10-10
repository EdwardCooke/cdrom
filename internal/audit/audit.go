// Package audit implements the audit log (F-15): an append-only record of
// significant actions — who (actor) did what (action) to which resource
// (target), when, from where (source IP), with what outcome, and what changed
// (old/new value).
//
// Each audited action is recorded twice:
//
//   - to a local audit log file (or stdout, when the configured path is "-"),
//     separate from the process's default log, in JSON or text, rotated
//     hourly or daily. The current file keeps a stable name (the configured
//     path); on rotation the file being closed is renamed with the day/hour
//     appended (e.g. audit.log -> audit.log-20261009 for a daily rotation, or
//     audit.log-2026100914 for an hourly one) and a fresh file is opened at
//     the stable path.
//   - to the shared audit log in the database (via a Sink), which is the
//     queryable, cross-replica store the UI reads (GET /api/audit). Old
//     entries are pruned by age on a schedule (see StartPruning).
//
// Secret values are never recorded: a secret is represented by its name only,
// and any secret plaintext present in a value is redacted (see Redactor)
// before it reaches either sink.
package audit

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// Rotation is the interval at which the audit log file is rotated.
type Rotation string

const (
	// RotationHourly rotates the file every hour; rotated files carry a
	// YYYYMMDDHH suffix.
	RotationHourly Rotation = "hourly"
	// RotationDaily rotates the file every day; rotated files carry a
	// YYYYMMDD suffix.
	RotationDaily Rotation = "daily"
)

// Config configures the audit log (F-15).
type Config struct {
	// File is the path of the current audit log file. It is separate from the
	// process's default log (CDROM_LOG_FILE). A value of "-" writes to stdout
	// instead of a file (no rotation).
	File string
	// Format is the record format: "json" (the default) or "text".
	Format string
	// Rotation is the file rotation interval: "hourly" or "daily" (the
	// default). It is ignored when File is "-".
	Rotation Rotation
}

// EffectiveFormat returns the configured format, or "json" when empty.
func (c Config) EffectiveFormat() string {
	if c.Format == "" {
		return "json"
	}
	return c.Format
}

// EffectiveRotation returns the configured rotation, or RotationDaily when
// empty.
func (c Config) EffectiveRotation() Rotation {
	if c.Rotation == "" {
		return RotationDaily
	}
	return c.Rotation
}

// interval returns the rotation interval for the configured rotation.
func (c Config) interval() time.Duration {
	switch c.EffectiveRotation() {
	case RotationHourly:
		return time.Hour
	default:
		return 24 * time.Hour
	}
}

// Event is one audit record: a significant action and its context. It is the
// in-memory form written to the local audit log and (via a Sink) to the
// database.
type Event struct {
	// Time is when the action was recorded. A zero value is stamped with the
	// current time by the Recorder.
	Time time.Time
	// Actor is the identity of the principal that performed the action (the
	// user's subject, or a synthetic identity such as "system:scheduler").
	Actor string
	// ActorKind is the kind of actor: "user", "service-account", "worker", or
	// "system".
	ActorKind string
	// Action is the action that was performed (see the Action* constants).
	Action string
	// TargetKind is the kind of resource the action acted on (see the
	// Target* constants).
	TargetKind string
	// TargetID is the id (or name, for name-keyed resources such as roles) of
	// the resource the action acted on.
	TargetID string
	// TargetName is a human-readable name of the target.
	TargetName string
	// PipelineID is the pipeline the action is scoped to, when it is
	// pipeline-scoped; 0 otherwise.
	PipelineID int64
	// RunID is the pipeline run the action is scoped to, when it is run-scoped;
	// 0 otherwise.
	RunID int64
	// Outcome is the action's outcome: OutcomeSuccess or OutcomeFailure.
	Outcome string
	// SourceIP is the client's IP address; empty when the action did not come
	// from a network client.
	SourceIP string
	// OldValue is the resource's state before the action, as a compact JSON
	// document (secret values redacted); empty when the action has no
	// before-state.
	OldValue string
	// NewValue is the resource's state after the action, as a compact JSON
	// document (secret values redacted); empty when the action has no
	// after-state.
	NewValue string
	// Details is a free-form JSON document carrying action-specific context.
	Details string
}

// Action names (F-15). Every audited action has a stable, namespaced name
// (<area>.<action>) so the log can be filtered by action.
const (
	ActionPipelineCreate = "pipeline.create"
	ActionPipelineUpdate = "pipeline.update"
	ActionPipelineDelete = "pipeline.delete"
	ActionRunTrigger     = "run.trigger"
	ActionJobCancel      = "job.cancel"
	ActionJobApprove     = "job.approve"
	ActionJobReject      = "job.reject"
	ActionJobRerun       = "job.rerun"
	ActionSecretEncrypt  = "secret.encrypt"
	ActionRoleCreate     = "role.create"
	ActionRoleUpdate     = "role.update"
	ActionRoleDelete     = "role.delete"
	ActionBindingAdd     = "binding.add"
	ActionBindingDelete  = "binding.delete"
	ActionUserCreate     = "user.create"
	ActionUserUpdate     = "user.update"
	ActionUserDelete     = "user.delete"
	// ActionAPIKey* are the API-key management actions (F-25).
	ActionAPIKeyCreate       = "api-key.create"
	ActionAPIKeyUpdate       = "api-key.update"
	ActionAPIKeyRotate       = "api-key.rotate"
	ActionAPIKeyDelete       = "api-key.delete"
	ActionAPIKeyResetLockout = "api-key.reset-lockout"
	ActionLogin              = "auth.login"
	ActionRegister           = "auth.register"
	ActionWorkerRegister     = "worker.register"
	ActionWorkerDeregister   = "worker.deregister"
	// ActionJobStatus is a job status report from an execution target (a
	// worker or agent) to the API: the target's account of the job's progress
	// (its status, step results, and outputs).
	ActionJobStatus = "job.status"
	// ActionTokenExchange is an execution target's request for a new job token
	// scoped to a different audience (a scoped credential for an outside
	// resource the job needs to authenticate to).
	ActionTokenExchange = "token.exchange"
)

// Target kinds (F-15).
const (
	TargetPipeline    = "pipeline"
	TargetRun         = "run"
	TargetJob         = "job"
	TargetRole        = "role"
	TargetRoleBinding = "role-binding"
	TargetUser        = "user"
	TargetSecret      = "secret"
	TargetWorker      = "worker"
	TargetLogin       = "login"
	TargetAPIKey      = "api-key"
)

// Outcomes (F-15).
const (
	OutcomeSuccess = "success"
	OutcomeFailure = "failure"
)

// Actor kinds (F-15).
const (
	ActorKindUser           = "user"
	ActorKindServiceAccount = "service-account"
	ActorKindWorker         = "worker"
	ActorKindSystem         = "system"
)

// Logger writes audit records to a single output (a rotating file, or stdout)
// in the configured format. It is safe for concurrent use.
type Logger struct {
	mu       sync.Mutex
	path     string
	format   string
	interval time.Duration
	// period is the rotation-period label the currently open file belongs to
	// (e.g. "20261009" for a daily rotation). It is compared against the
	// current period on each write to decide whether to rotate.
	period string
	f      *os.File
	stdout io.Writer
	closed bool
}

// NewLogger creates an audit Logger from cfg. When cfg.File is "-" records go
// to stdout (no rotation); otherwise they go to the file at cfg.File, rotated
// at cfg.Rotation. It returns an error if the file cannot be opened or the
// format/rotation is invalid.
func NewLogger(cfg Config) (*Logger, error) {
	format := strings.ToLower(cfg.EffectiveFormat())
	switch format {
	case "json", "text":
	default:
		return nil, fmt.Errorf("audit: unknown format %q (want json or text)", cfg.Format)
	}
	l := &Logger{
		path:     cfg.File,
		format:   format,
		interval: cfg.interval(),
		period:   periodLabel(time.Now(), cfg.EffectiveRotation()),
	}
	if cfg.File == "-" {
		l.stdout = os.Stdout
		return l, nil
	}
	f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("audit: open %s: %w", cfg.File, err)
	}
	l.f = f
	return l, nil
}

// Log writes one audit record to the output, rotating the file first if the
// rotation period has changed. A nil or closed Logger is a no-op.
func (l *Logger) Log(e Event) error {
	if l == nil {
		return nil
	}
	line := l.render(e)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	if l.stdout != nil {
		_, err := io.WriteString(l.stdout, line)
		return err
	}
	if err := l.rotateIfNeeded(); err != nil {
		return err
	}
	_, err := l.f.WriteString(line)
	return err
}

// Close closes the underlying file (if any). It is idempotent.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.f != nil {
		err := l.f.Close()
		l.f = nil
		return err
	}
	return nil
}

// rotateIfNeeded rotates the file if the current rotation period differs from
// the period the open file belongs to. The caller must hold l.mu. The file
// being closed is renamed with its period appended (path -> path-<period>) and
// a fresh file is opened at the stable path.
func (l *Logger) rotateIfNeeded() error {
	period := periodLabel(time.Now(), rotationFromInterval(l.interval))
	if period == l.period {
		return nil
	}
	if l.f != nil {
		if err := l.f.Close(); err != nil {
			return fmt.Errorf("audit: close %s: %w", l.path, err)
		}
		l.f = nil
	}
	// Rename the file being closed to carry the period it covered. The rename
	// is best-effort: if the file is absent (e.g. it was already rotated or
	// removed) there is nothing to rename.
	if l.period != "" {
		_ = os.Rename(l.path, l.path+"-"+l.period)
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("audit: open %s: %w", l.path, err)
	}
	l.f = f
	l.period = period
	return nil
}

// render formats e as a single line in the Logger's format.
func (l *Logger) render(e Event) string {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	if l.format == "json" {
		return l.renderJSON(e)
	}
	return l.renderText(e)
}

// jsonEvent is the on-the-wire shape of an audit record in JSON format. The
// field order is the stable key order of the output. old_value, new_value, and
// details are embedded as nested JSON objects (json.RawMessage) rather than as
// escaped strings, so a record is a single self-describing JSON document that
// is easy to pretty-print and query.
type jsonEvent struct {
	Time       string          `json:"time"`
	Actor      string          `json:"actor"`
	ActorKind  string          `json:"actor_kind,omitempty"`
	Action     string          `json:"action"`
	TargetKind string          `json:"target_kind,omitempty"`
	TargetID   string          `json:"target_id,omitempty"`
	TargetName string          `json:"target_name,omitempty"`
	PipelineID int64           `json:"pipeline_id,omitempty"`
	RunID      int64           `json:"run_id,omitempty"`
	Outcome    string          `json:"outcome"`
	SourceIP   string          `json:"source_ip,omitempty"`
	OldValue   json.RawMessage `json:"old_value,omitempty"`
	NewValue   json.RawMessage `json:"new_value,omitempty"`
	Details    json.RawMessage `json:"details,omitempty"`
}

// renderJSON renders e as a single-line JSON object.
func (l *Logger) renderJSON(e Event) string {
	je := jsonEvent{
		Time:       e.Time.UTC().Format(time.RFC3339Nano),
		Actor:      e.Actor,
		ActorKind:  e.ActorKind,
		Action:     e.Action,
		TargetKind: e.TargetKind,
		TargetID:   e.TargetID,
		TargetName: e.TargetName,
		PipelineID: e.PipelineID,
		RunID:      e.RunID,
		Outcome:    e.Outcome,
		SourceIP:   e.SourceIP,
	}
	if e.OldValue != "" {
		je.OldValue = rawOrString(e.OldValue)
	}
	if e.NewValue != "" {
		je.NewValue = rawOrString(e.NewValue)
	}
	if e.Details != "" {
		je.Details = rawOrString(e.Details)
	}
	b, err := json.Marshal(je)
	if err != nil {
		// The struct only holds strings, ints, and validated raw JSON, so
		// marshaling cannot fail; fall back to a best-effort string rather
		// than dropping the record.
		return fmt.Sprintf("%+v\n", je)
	}
	return string(b) + "\n"
}

// renderText renders e as a single line of key=value pairs.
func (l *Logger) renderText(e Event) string {
	var b strings.Builder
	fmt.Fprintf(&b, "time=%s actor=%q action=%s outcome=%s",
		e.Time.UTC().Format(time.RFC3339Nano), e.Actor, e.Action, e.Outcome)
	if e.ActorKind != "" {
		fmt.Fprintf(&b, " actor_kind=%s", e.ActorKind)
	}
	if e.TargetKind != "" {
		fmt.Fprintf(&b, " target_kind=%s", e.TargetKind)
	}
	if e.TargetID != "" {
		fmt.Fprintf(&b, " target_id=%s", e.TargetID)
	}
	if e.TargetName != "" {
		fmt.Fprintf(&b, " target_name=%q", e.TargetName)
	}
	if e.PipelineID != 0 {
		fmt.Fprintf(&b, " pipeline_id=%d", e.PipelineID)
	}
	if e.RunID != 0 {
		fmt.Fprintf(&b, " run_id=%d", e.RunID)
	}
	if e.SourceIP != "" {
		fmt.Fprintf(&b, " source_ip=%s", e.SourceIP)
	}
	if e.OldValue != "" {
		fmt.Fprintf(&b, " old_value=%s", e.OldValue)
	}
	if e.NewValue != "" {
		fmt.Fprintf(&b, " new_value=%s", e.NewValue)
	}
	if e.Details != "" {
		fmt.Fprintf(&b, " details=%s", e.Details)
	}
	b.WriteByte('\n')
	return b.String()
}

// periodLabel returns the rotation-period label for now under rotation:
// YYYYMMDD for a daily rotation, YYYYMMDDHH for an hourly one.
func periodLabel(now time.Time, r Rotation) string {
	if r == RotationHourly {
		return now.Format("2006010215")
	}
	return now.Format("20060102")
}

// rotationFromInterval recovers the Rotation from an interval (used by
// rotateIfNeeded, which only stores the interval).
func rotationFromInterval(d time.Duration) Rotation {
	if d == time.Hour {
		return RotationHourly
	}
	return RotationDaily
}

// rawOrString returns v as a json.RawMessage when v is itself valid JSON, so
// it is embedded as a nested object; otherwise it returns v as a JSON string,
// so a malformed value can never make the whole record invalid JSON.
func rawOrString(v string) json.RawMessage {
	if json.Valid([]byte(v)) {
		return json.RawMessage(v)
	}
	b, _ := json.Marshal(v)
	return b
}
