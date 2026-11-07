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
	// ActorKind is the kind of actor: "user", "service-account", "agent", or
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
	// OldValue is the resource's state before the action (secret values
	// redacted); typically a STRUCTURED DOCUMENT (map/slice) the renderers
	// marshal to compact JSON, or a plain string for terse values; nil/""
	// when the action has no before-state. Carrying the value (not a
	// pre-rendered string) keeps the producers parser-friendly: the
	// stringification happens exactly once, at the render/store boundary.
	OldValue any
	// NewValue is the resource's state after the action (see OldValue).
	NewValue any
	// Details is a free-form document carrying action-specific context that
	// does not fit the structured fields (see OldValue).
	Details any
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
	// ActionServiceAccount* are the service-account management actions (F-26).
	ActionServiceAccountCreate       = "service-account.create"
	ActionServiceAccountUpdate       = "service-account.update"
	ActionServiceAccountRotate       = "service-account.rotate-key"
	ActionServiceAccountDisable      = "service-account.disable"
	ActionServiceAccountEnable       = "service-account.enable"
	ActionServiceAccountDelete       = "service-account.delete"
	ActionServiceAccountAssignRole   = "service-account.assign-role"
	ActionServiceAccountRemoveRole   = "service-account.remove-role"
	ActionServiceAccountResetLockout = "service-account.reset-lockout"
	// ActionScimKey* are the SCIM-key pool management actions (F-28.2):
	// maintaining the bounded pool of keys that authenticate the corporate
	// SCIM connector. The plaintext is NEVER recorded (only the 8-char prefix);
	// the periodic prune sweep audits as a system actor.
	ActionScimKeyCreate    = "scim-key.create"
	ActionScimKeyUpdate    = "scim-key.update"
	ActionScimKeyRevoke    = "scim-key.revoke"
	ActionScimKeyReinstate = "scim-key.reinstate"
	ActionScimKeyPrune     = "scim-key.prune"
	// ActionBreakGlassCoin is a break-glass escape-hatch invocation (F-28.3):
	// an ephemeral random-identity admin was coined (or coinage was refused).
	// The actor is the CALLER'S CERTIFICATE LEAF (CN, SAN, serial) — the
	// mTLS peer that presented the API's client certificate — never the
	// coined user (the password is the secret, the certificate is the blame).
	ActionBreakGlassCoin = "break-glass.coin"
	// ActionScimAuthFail is a rejected SCIM caller-authentication attempt
	// (F-28.2, Slice 3): an unauthenticated probe of the /scim/v2 surface.
	// The presented key is NEVER echoed (missing, malformed, unknown,
	// revoked, or expired alike — the response discloses none of that either);
	// the event carries the attempted path and the reason category only.
	ActionScimAuthFail = "scim.auth.fail"
	// ActionConcurrencyLimit* are the concurrency-limit management actions
	// (F-16): creating, updating, or deleting a concurrency limit rule.
	ActionConcurrencyLimitCreate = "concurrency-limit.create"
	ActionConcurrencyLimitUpdate = "concurrency-limit.update"
	ActionConcurrencyLimitDelete = "concurrency-limit.delete"
	// ActionEnvironment* are the environment management actions (F-17): creating,
	// updating, or deleting an environment (a named deployment target that
	// carries approval + automation policy).
	ActionEnvironmentCreate = "environment.create"
	ActionEnvironmentUpdate = "environment.update"
	ActionEnvironmentDelete = "environment.delete"
	// ActionRunProceed is a user's excuplicit proceed call that releases a run
	// (or job) held by a `manual` environment (F-17).
	ActionRunProceed = "run.proceed"
	// ActionRunRestore is the initiation of a RESUMPTION of a pipeline (F-22):
	// spawning a run that re-executes the suffix of the pipeline's latest
	// successful run from a named cut point. Recorded for BOTH the manual door
	// (a user's POST /api/pipelines/{id}/restart) and the ARMed door (a
	// rollback step handler's API mediation, actor "job:<id>").
	ActionRunRestore = "run.restore"
	// ActionAgentGroup* are the agent-group management actions (F-29):
	// creating, updating, or deleting a agent group (a named group of
	// long-lived agents that carries tags).
	ActionAgentGroupCreate = "agent-group.create"
	ActionAgentGroupUpdate = "agent-group.update"
	ActionAgentGroupDelete = "agent-group.delete"
	// ActionSSHKnownHost* are the SSH known-host management actions
	// (F-21.2.4.1): adding or removing a runtime-managed SSH host key the
	// source-fetcher uses to verify a git-ssh server's host key.
	ActionSSHKnownHostCreate = "ssh-known-host.create"
	ActionSSHKnownHostDelete = "ssh-known-host.delete"
	// ActionSourceCredential* are the source-credential management actions
	// (F-21.3): creating, updating, or deleting a named credential that
	// authenticates a fetch of a private source.
	ActionSourceCredentialCreate = "source-credential.create"
	ActionSourceCredentialUpdate = "source-credential.update"
	ActionSourceCredentialDelete = "source-credential.delete"
	// Package registry actions (F-21.6): publishing a version into the
	// registry and promoting an artifact between namespaces.
	ActionPackagePublish = "package.publish"
	ActionPackagePromote = "package.promote"
	// ActionApprovalGroup* are the approval-group management actions (F-27):
	// creating, updating, or deleting a group, and adding/removing a single
	// member (the incremental membership churn).
	ActionApprovalGroupCreate       = "approval-group.create"
	ActionApprovalGroupUpdate       = "approval-group.update"
	ActionApprovalGroupDelete       = "approval-group.delete"
	ActionApprovalGroupAddMember    = "approval-group.add-member"
	ActionApprovalGroupRemoveMember = "approval-group.remove-member"
	ActionLogin                     = "auth.login"
	ActionRegister                  = "auth.register"
	ActionAgentRegister            = "agent.register"
	ActionAgentDeregister          = "agent.deregister"
	// ActionJobStatus is a job status report from an execution target (a
	// agent or agent) to the API: the target's account of the job's progress
	// (its status, step results, and outputs).
	ActionJobStatus = "job.status"
	// ActionTokenExchange is an execution target's request for a new job token
	// scoped to a different audience (a scoped credential for an outside
	// resource the job needs to authenticate to).
	ActionTokenExchange = "token.exchange"
)

// Target kinds (F-15).
const (
	TargetPipeline       = "pipeline"
	TargetRun            = "run"
	TargetJob            = "job"
	TargetRole           = "role"
	TargetRoleBinding    = "role-binding"
	TargetUser           = "user"
	TargetSecret         = "secret"
	TargetAgent         = "agent"
	TargetLogin          = "login"
	TargetAPIKey         = "api-key"
	TargetServiceAccount = "service-account"
	// TargetScimKey is the target kind of a SCIM connector key (F-28.2).
	TargetScimKey = "scim-key"
	// TargetConcurrencyLimit is the target kind of a concurrency limit rule
	// (F-16).
	TargetConcurrencyLimit = "concurrency-limit"
	// TargetEnvironment is the target kind of an environment (F-17): a named
	// deployment target that carries approval + automation policy.
	TargetEnvironment = "environment"
	// TargetAgentGroup is the target kind of a agent group (F-29): a named
	// group of long-lived agents that carries tags.
	TargetAgentGroup = "agent-group"
	// TargetSSHKnownHost is the target kind of a runtime-managed SSH host key
	// (F-21.2.4.1).
	TargetSSHKnownHost = "ssh-known-host"
	// TargetSourceCredential is the target kind of a source credential
	// (F-21.3): a named credential that authenticates a fetch of a private
	// source.
	TargetSourceCredential = "source-credential"
	// TargetPackage is the target kind of a package registry entry (F-21.6):
	// a published version ("<name>-<version>.<ext>") or a promotion between
	// namespaces.
	TargetPackage = "package"
	// TargetApprovalGroup is the target kind of an approval group (F-27): a
	// named set of users an approval step can reference.
	TargetApprovalGroup = "approval-group" // TargetSCIMGroup is the target kind of a SCIM group (F-28.5): a group
	// synced from the corporate directory whose membership drives role
	// derivation.
	TargetSCIMGroup = "scim-group"
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
	ActorKindAgent         = "agent"
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
// details are embedded as NATIVE JSON VALUES (nested objects/documents, never
// escaped strings), so a record is a single self-describing JSON document that
// is easy to pretty-print and query.
type jsonEvent struct {
	Time       string `json:"time"`
	Actor      string `json:"actor"`
	ActorKind  string `json:"actor_kind,omitempty"`
	Action     string `json:"action"`
	TargetKind string `json:"target_kind,omitempty"`
	TargetID   string `json:"target_id,omitempty"`
	TargetName string `json:"target_name,omitempty"`
	PipelineID int64  `json:"pipeline_id,omitempty"`
	RunID      int64  `json:"run_id,omitempty"`
	Outcome    string `json:"outcome"`
	SourceIP   string `json:"source_ip,omitempty"`
	OldValue   any    `json:"old_value,omitempty"`
	NewValue   any    `json:"new_value,omitempty"`
	Details    any    `json:"details,omitempty"`
}

// renderJSON renders e as a single-line JSON object. Structured old/new/details
// values are embedded as NATIVE JSON documents (never escaped strings), so the
// emitted line is directly consumable by a JSON parser.
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
		OldValue:   jsonNativeValue(e.OldValue),
		NewValue:   jsonNativeValue(e.NewValue),
		Details:    jsonNativeValue(e.Details),
	}
	b, err := json.Marshal(je)
	if err != nil {
		// Marshaling fails only on pathological values (reference cycles,
		// NaN floats); fall back to a best-effort rendering rather than
		// dropping the record.
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
	if encoded := EncodeFieldValue(e.OldValue); encoded != "" {
		fmt.Fprintf(&b, " old_value=%s", encoded)
	}
	if encoded := EncodeFieldValue(e.NewValue); encoded != "" {
		fmt.Fprintf(&b, " new_value=%s", encoded)
	}
	if encoded := EncodeFieldValue(e.Details); encoded != "" {
		fmt.Fprintf(&b, " details=%s", encoded)
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

// jsonNativeValue prepares one OLD/NEW/DETAILS value for a JSON-NATIVE sink
// (the JSON-format audit log): structured values (map/slice/etc.) pass
// through to be embedded as native JSON documents, and a STRING that is
// itself a JSON document (object/array) is promoted to that document (so a
// producer handing a pre-rendered document still gets native nesting, not an
// escaped string). Plain strings and empty values degrade gracefully.
func jsonNativeValue(v any) any {
	switch sv := v.(type) {
	case nil:
		return nil
	case string:
		if sv == "" {
			return nil
		}
		var doc any
		if err := json.Unmarshal([]byte(sv), &doc); err == nil {
			switch doc.(type) {
			case map[string]any, []any:
				return doc
			}
		}
		return sv
	default:
		return v
	}
}

// EncodeFieldValue renders one OLD/NEW/DETAILS value for a TEXTUAL sink (the
// text-format audit log, the durable store's string columns, and any other
// destination that must carry the value as a SELF-CONTAINED STRING):
//
//   - a nil / empty-string value ⇒ "" (the field is omitted);
//   - a string that is ITSELF a valid JSON document ⇒ that document verbatim
//     (already compact; emitting it as-is avoids needless re-encoding);
//   - any other string ⇒ the string as-is (terse values stay terse);
//   - a structured value (map/slice/etc.) ⇒ its compact JSON encoding.
//
// Encoding never loses the value: a marshal failure (pathological input)
// degrades to the value's fmt representation so the record is still written.
// Callers persisting to a JSON-native destination should embed the structured
// value DIRECTLY (native nesting) instead of calling this.
func EncodeFieldValue(v any) string {
	switch tv := v.(type) {
	case nil:
		return ""
	case string:
		if tv == "" {
			return ""
		}
		// A string that is already a JSON document travels verbatim (it is
		// the producer's rendered form); a plain string stays plain.
		return tv
	default:
		b, err := json.Marshal(tv)
		if err != nil {
			return fmt.Sprint(tv)
		}
		return string(b)
	}
}
