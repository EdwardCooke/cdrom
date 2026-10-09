// Package models defines the core GORM entities shared across services.
//
// This is the single source of truth for the persisted data model. The
// database service runs AutoMigrate over these types, so adding a field or
// entity here is all that is required to evolve the schema.
package models

import (
	"time"

	"gorm.io/gorm"
)

// Parameter is a named, typed input a pipeline declares (F-10): a name, an
// optional default value, and an optional description. A run supplies
// concrete values for the pipeline's parameters (e.g. at trigger time or via
// a webhook); a parameter not supplied falls back to its default. A run's
// concrete parameter values are recorded on the run (its Params) and are
// interpolated into a job's spec (env, command, workdir) by the execution
// target before the job runs, using Go templates (text/template) like a
// step's condition: `{{ .params.name }}` (or `{{ .name }}`) renders the
// parameter's value. A reference to a parameter that has no value (no
// supplied value and no default) is a spec error that fails the job.
type Parameter struct {
	// Name is the parameter's name; it must be unique within the pipeline.
	Name string `json:"name"`
	// Default is the value used when a run does not supply a value for the
	// parameter; empty means the parameter has no default (a run that does
	// not supply it leaves it unset).
	Default string `json:"default,omitempty"`
	// Description is a human-readable description of the parameter.
	Description string `json:"description,omitempty"`
}

// Secret is a named secret a pipeline declares (F-12): a name and the
// encrypted value. The API is the only component that holds the key: it
// encrypts the plaintext when the pipeline is created or updated and stores
// the ciphertext here, and it decrypts the ciphertext when it hands a job to
// an execution target (so a step can reference the value as
// `{{ .secrets.name }}`). The database service stores the ciphertext opaquely
// and never sees the key, so no single service or configuration can decrypt a
// secret on its own. A run's job instances carry the ciphertext (denormalized
// from the pipeline, like a run's parameters); the API decrypts it at
// dispatch time and never returns the plaintext to the UI or logs it.
type Secret struct {
	// Name is the secret's name; it must be unique within the pipeline.
	Name string `json:"name"`
	// Encrypted is the secret's value, encrypted by the API's secret store
	// (F-12). It is self-describing (it carries the nonce) and safe to store
	// opaquely. The plaintext is never persisted.
	Encrypted string `json:"encrypted"`
}

// SecretNonce is the shared nonce counter for the built-in AES secret store
// (F-12). AES-GCM requires a unique key/nonce pair for every encryption, so
// the counter is kept in the database (not in any API replica's memory):
// every API replica draws the next nonce from this single shared sequence
// (via the Database service's NextSecretNonce RPC), so no two encryptions —
// even across replicas — ever reuse a nonce. The nonce itself is embedded in
// each stored ciphertext, so decryption does not read this counter.
type SecretNonce struct {
	gorm.Model
	// Name is the counter's name; there is a single counter, so this is a
	// fixed value (it is the row's unique key).
	Name string `gorm:"uniqueIndex;not null" json:"name"`
	// Value is the next nonce to hand out; it is incremented atomically on
	// each NextSecretNonce call.
	Value int64 `gorm:"not null;default:0" json:"value"`
}

// Pipeline is a named, ordered definition of work to be executed.
type Pipeline struct {
	gorm.Model
	Name        string `gorm:"uniqueIndex;not null" json:"name"`
	Description string `json:"description"`
	// FailureMode is the pipeline's default failure mode (see FailureMode) for
	// its jobs that fan out to a worker group. A job that sets its own
	// FailureMode overrides the pipeline's; a job that sets none inherits this
	// value (defaulting to FailureModeAll when the pipeline sets none).
	FailureMode FailureMode `gorm:"default:all" json:"failure_mode"`
	Jobs        []Job       `json:"jobs,omitempty"`
	// Triggers are the ways a run of the pipeline can be started other than a
	// manual click (F-09): a cron schedule, a webhook, or an event (another
	// pipeline's run reaching a state). They are stored as a JSON document in
	// a text column (portable across SQLite and PostgreSQL).
	Triggers []Trigger `gorm:"type:text;serializer:json" json:"triggers,omitempty"`
	// Params are the pipeline's parameter declarations (F-10): the named,
	// typed inputs a run can supply. They are stored as a JSON document in a
	// text column (portable across SQLite and PostgreSQL). A run's concrete
	// values for them are recorded on the run and interpolated into its jobs'
	// specs by the execution target.
	Params []Parameter `gorm:"type:text;serializer:json" json:"params,omitempty"`
	// Secrets are the pipeline's named secrets (F-12): each carries a name and
	// an encrypted value (the API encrypts the plaintext on create/update and
	// decrypts it at dispatch). They are stored as a JSON document in a text
	// column (portable across SQLite and PostgreSQL), like Params. A run's job
	// instances carry the ciphertext (denormalized from the pipeline); the API
	// decrypts it and hands the plaintext to the execution target, where a step
	// references it as `{{ .secrets.name }}`.
	Secrets []Secret `gorm:"type:text;serializer:json" json:"secrets,omitempty"`
	// Version is the pipeline's current version (F-11): a monotonically
	// increasing counter that is bumped every time the pipeline's definition
	// changes (a create is version 1; each edit is the next version). A run
	// records the version it executed (see PipelineRun.PipelineVersion), so a
	// run is bound to the definition that was active when it started and a
	// re-run of an old run reproduces the old definition.
	Version int `gorm:"not null;default:1" json:"version"`
}

// PipelineVersion is an immutable snapshot of a pipeline's definition at one
// version (F-11). Each change to a pipeline produces a new version; the
// database service stores one PipelineVersion row per version, capturing the
// definition (name, description, failure mode, job definitions, triggers, and
// parameters) exactly as it was. A run is bound to the version that was active
// when it started (see PipelineRun.PipelineVersion), so "what exactly did run
// #42 execute?" is answerable from the version snapshot, and a run can be
// re-executed against its original version even after the pipeline has since
// been edited.
//
// The snapshot is stored as a JSON document in a text column (portable across
// SQLite and PostgreSQL) rather than as separate columns, so a version row is
// a single immutable record and adding a field to the definition never
// requires a schema change.
type PipelineVersion struct {
	gorm.Model
	// PipelineID is the pipeline this version belongs to.
	PipelineID uint `gorm:"uniqueIndex:idx_pipeline_version_unique,priority:1;not null" json:"pipeline_id"`
	// Version is the version number this snapshot represents (1-based; it
	// matches the pipeline's Version at the time the snapshot was taken).
	Version int `gorm:"uniqueIndex:idx_pipeline_version_unique,priority:2;not null" json:"version"`
	// Name is the pipeline's name at this version.
	Name string `json:"name"`
	// Description is the pipeline's description at this version.
	Description string `json:"description"`
	// FailureMode is the pipeline's default failure mode at this version.
	FailureMode FailureMode `json:"failure_mode"`
	// Jobs are the pipeline's job definitions at this version (the declarative
	// form: key, name, target group, needs, and spec). They are stored as a
	// JSON document in a text column (portable across SQLite and PostgreSQL).
	Jobs []JobDefinitionSnapshot `gorm:"type:text;serializer:json" json:"jobs,omitempty"`
	// Triggers are the pipeline's trigger definitions at this version. They
	// are stored as a JSON document in a text column.
	Triggers []Trigger `gorm:"type:text;serializer:json" json:"triggers,omitempty"`
	// Params are the pipeline's parameter declarations at this version. They
	// are stored as a JSON document in a text column.
	Params []Parameter `gorm:"type:text;serializer:json" json:"params,omitempty"`
	// Secrets are the pipeline's named secrets at this version (F-12): each
	// carries a name and an encrypted value. They are stored as a JSON
	// document in a text column, like Params.
	Secrets []Secret `gorm:"type:text;serializer:json" json:"secrets,omitempty"`
}

// JobDefinitionSnapshot is the declarative form of a job definition as captured
// in a pipeline version (F-11): the key (the job's stable name, F-08), the
// display name, the target group, the needs (dependencies, expressed as the
// keys of other jobs in the pipeline, F-08), and the execution spec. It is the
// same shape the UI authors a pipeline with, so a version snapshot can be
// re-executed (or re-saved) without loss.
type JobDefinitionSnapshot struct {
	// Key is the job's stable, pipeline-scoped identifier (F-08).
	Key string `json:"key"`
	// Name is the job's display name; empty means the key is used.
	Name string `json:"name,omitempty"`
	// TargetGroup selects a group of long-lived workers; empty means the job
	// runs on an ephemeral Kubernetes agent.
	TargetGroup string `json:"target_group,omitempty"`
	// Needs lists the keys of the jobs this job depends on (F-08).
	Needs []string `json:"needs,omitempty"`
	// Spec is the execution spec to snapshot onto each of the job's run
	// instances.
	Spec JobSpec `json:"spec,omitempty"`
}

// Trigger is a way to start a pipeline run other than a manual click (F-09).
// The Type selects the trigger kind and the remaining fields carry the
// kind-specific settings:
//
//   - TriggerTypeCron: Cron is a standard five-field cron expression (minute,
//     hour, day-of-month, month, day-of-week). The scheduler's cron loop
//     starts a run at each scheduled time.
//   - TriggerTypeWebhook: an external POST to the pipeline's webhook endpoint
//     (authenticated with the shared Secret and/or the caller's OIDC token
//     claims) starts a run, passing the request's JSON body as run
//     parameters.
//   - TriggerTypeEvent: when the run of another pipeline (EventPipeline)
//     reaches EventStatus, a run of this pipeline is started (chaining).
//
// Params are static run parameters merged into the run's parameters when the
// trigger fires (a webhook's payload overrides them on a key collision).
type Trigger struct {
	// Name is the trigger's name; it must be unique within the pipeline.
	Name string `json:"name"`
	// Type is the trigger kind (cron, webhook, or event).
	Type TriggerType `json:"type"`
	// Cron is the cron expression for a cron trigger (five fields: minute,
	// hour, day-of-month, month, day-of-week).
	Cron string `json:"cron,omitempty"`
	// Secret is the shared secret for a webhook trigger; a webhook POST must
	// present it (in the X-Cdrom-Webhook-Secret header) to start a run.
	Secret string `json:"secret,omitempty"`
	// EventPipeline is the name of the pipeline whose run reaching
	// EventStatus starts a run of this pipeline (an event trigger).
	EventPipeline string `json:"event_pipeline,omitempty"`
	// EventStatus is the run status of EventPipeline that fires the trigger
	// (an event trigger); one of the RunStatus values.
	EventStatus RunStatus `json:"event_status,omitempty"`
	// Params are static run parameters merged into the run's parameters when
	// the trigger fires.
	Params map[string]string `json:"params,omitempty"`
	// OIDCIssuer is the OIDC issuer (a full URL) of the token a webhook
	// caller must present to start a run (a webhook trigger). When set, the
	// API verifies the caller's Bearer token against this issuer's JWKS and
	// matches its claims against OIDCClaims. When empty (and no secret is
	// set) the trigger is open.
	OIDCIssuer string `json:"oidc_issuer,omitempty"`
	// OIDCClaims are the claims a webhook caller's OIDC token must carry for
	// the trigger to match (a webhook trigger). Each key is a claim name and
	// each value is the expected value; a value of "*" is a wildcard that
	// matches any value for that claim (including the claim being absent).
	// Nested claims are addressed with a dot (e.g. "org.name"). When empty
	// the trigger matches any token from OIDCIssuer.
	OIDCClaims map[string]string `json:"oidc_claims,omitempty"`
}

// TriggerType is the kind of a pipeline trigger (F-09).
type TriggerType string

const (
	// TriggerTypeCron starts a run on a cron schedule.
	TriggerTypeCron TriggerType = "cron"
	// TriggerTypeWebhook starts a run from an external POST.
	TriggerTypeWebhook TriggerType = "webhook"
	// TriggerTypeEvent starts a run when another pipeline's run reaches a
	// state (chaining).
	TriggerTypeEvent TriggerType = "event"
)

// TriggerSource is how a run was started (recorded on the run's Trigger
// field, F-09).
const (
	// TriggerSourceManual is a run started by a human (the default).
	TriggerSourceManual = "manual"
	// TriggerSourceCron is a run started by a cron trigger.
	TriggerSourceCron = "cron"
	// TriggerSourceWebhook is a run started by a webhook POST.
	TriggerSourceWebhook = "webhook"
	// TriggerSourceEvent is a run started by an event trigger (another
	// pipeline's run reaching a state).
	TriggerSourceEvent = "event"
)

// Run parameter keys (F-09) recorded on a trigger-fired run so the trigger's
// origin is recoverable from the run and so the database can deduplicate a
// trigger that would otherwise fire twice for the same window. They are stored
// in the run's Params map (a JSON document), not as dedicated columns, so they
// travel with the run and are visible to the UI.
const (
	// ParamKeyTrigger is the name of the trigger that started the run (F-09).
	ParamKeyTrigger = "cdrom.trigger"
	// ParamKeySourceRun is the id of the run that started this run (an event
	// trigger, F-09). It is what the database deduplicates an event trigger
	// against, so the same source run can never start the same downstream run
	// twice.
	ParamKeySourceRun = "cdrom.source_run"
	// ParamKeySourcePipeline is the name of the pipeline whose run started
	// this run (an event trigger, F-09); informational.
	ParamKeySourcePipeline = "cdrom.source_pipeline"
)

// PipelineRun is one execution of a pipeline (F-07): a first-class execution
// entity that owns a set of job instances. A run separates the pipeline
// *definition* from each *run* of it, so the same pipeline can be executed
// many times and each run can be compared, reported on, and browsed
// historically ("run #42 of pipeline X").
//
// A run's job instances are the Job rows that carry the run's id in RunID.
// The run's Status is derived from those jobs (see RunStatus): it is
// succeeded only if every non-skipped job succeeded, failed if any job
// failed or timed out, cancelled if the run was cancelled, and running while
// any job is still pending or running. The derivation is maintained by the
// scheduler's run-status loop.
type PipelineRun struct {
	gorm.Model
	// PipelineID is the pipeline this run executed.
	PipelineID uint `gorm:"index;not null" json:"pipeline_id"`
	// Pipeline is the pipeline this run executed (loaded on demand).
	Pipeline *Pipeline `json:"pipeline,omitempty"`
	// PipelineVersion is the version of the pipeline this run executed (F-11):
	// the version that was active when the run started. It is recorded so a
	// run is bound to the definition it executed — re-running an old run
	// reproduces the old definition, and "what exactly did run #42 execute?"
	// is answerable from the version snapshot. A run created against a
	// specific version (a re-run of an old run) records that version instead
	// of the pipeline's current one.
	PipelineVersion int `json:"pipeline_version"`
	// Status is the run's overall state, derived from its job instances.
	Status RunStatus `gorm:"default:pending;index" json:"status"`
	// Trigger is how the run was started (e.g. "manual", "cron", "webhook",
	// or "event"); it is recorded so a run's origin is visible (F-09).
	Trigger string `json:"trigger"`
	// TriggerName is the name of the trigger that started the run (F-09);
	// empty for a manual run. It is stored separately from Trigger (the
	// source kind) so the database can find the runs a given trigger started
	// (to deduplicate a trigger that would otherwise fire twice for the same
	// window).
	TriggerName string `gorm:"index" json:"trigger_name,omitempty"`
	// SourceRunID is the id of the run that started this run (an event
	// trigger, F-09); nil for a run not started by an event trigger. It is
	// what the database deduplicates an event trigger against, so the same
	// source run can never start the same downstream run twice.
	SourceRunID *uint `gorm:"index" json:"source_run_id,omitempty"`
	// Params are the run's parameters (F-10); empty until parameters land.
	Params map[string]string `gorm:"type:text;serializer:json" json:"params,omitempty"`
	// UpstreamClaims are the claims of the OIDC token a webhook caller
	// presented to start this run (F-09); empty for a run not started by a
	// webhook. They are the token's full claims as a JSON object, so a claim
	// that is itself an object or a list (e.g. GitLab's "user_identities" or
	// "job_config") keeps its structure rather than being flattened to a
	// string. They are denormalized onto the run's job instances so the API
	// can stamp them (prefixed with upstream_) onto the job tokens it hands to
	// execution targets.
	UpstreamClaims map[string]any `gorm:"type:text;serializer:json" json:"upstream_claims,omitempty"`
	// StartedAt is when the run started (its first job began); nil until then.
	StartedAt *time.Time `json:"started_at,omitempty"`
	// FinishedAt is when the run reached a terminal status; nil while the run
	// is still in flight.
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// JobStatus represents the lifecycle state of a job.
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCancelled JobStatus = "cancelled"
	// JobStatusTimedOut is a terminal state: the job (or a step within it)
	// exceeded its declared timeout and was terminated. It is distinct from
	// failed so the UI and operators can tell a hung job apart from a job that
	// ran to completion and reported an error.
	JobStatusTimedOut JobStatus = "timed_out"
	// JobStatusSkipped is a terminal state: the job never ran because one of
	// its dependencies (DependsOn) did not succeed (F-06). It is distinct
	// from failed so reporting and on_failure logic can tell "didn't run"
	// apart from "ran and errored".
	JobStatusSkipped JobStatus = "skipped"
	// JobStatusAwaitingApproval is a non-terminal, in-flight state (F-13): the
	// job is paused at an approval gate, waiting for an authorized user to
	// approve or reject it. It is distinct from pending (not yet dispatched)
	// and running (actively executing a step) so the UI can show the gate and
	// its message. The job leaves this state when the gate is resolved
	// (approved → running, rejected → failed) or the job is cancelled/timed
	// out.
	JobStatusAwaitingApproval JobStatus = "awaiting_approval"
)

// RunStatus is the lifecycle state of a pipeline run (F-07). A run's status
// is derived from the statuses of its job instances: it is succeeded only if
// every non-skipped job succeeded, failed if any job failed or timed out,
// cancelled if it was cancelled, and running while any job is still pending
// or running.
type RunStatus string

const (
	RunStatusPending   RunStatus = "pending"
	RunStatusRunning   RunStatus = "running"
	RunStatusSucceeded RunStatus = "succeeded"
	RunStatusFailed    RunStatus = "failed"
	RunStatusCancelled RunStatus = "cancelled"
)

// StepStatus is the terminal outcome of a single step within a job's
// execution spec (F-06). A step does not have its own pending or cancelled
// state (those belong to the job as a whole) — only the states a step can
// actually reach once the executor has decided its fate.
type StepStatus string

const (
	StepStatusSucceeded StepStatus = "succeeded"
	StepStatusFailed    StepStatus = "failed"
	// StepStatusSkipped means the step's Condition rendered to false, so the
	// step never ran; it does not fail the job.
	StepStatusSkipped  StepStatus = "skipped"
	StepStatusTimedOut StepStatus = "timed_out"
)

// FailureMode controls how a job that runs on several workers (a job targeting
// a worker group, which runs on every worker in the group) is judged from the
// per-worker outcomes. It is set at the pipeline level and may be overridden
// per job; a job that sets no failure mode inherits its pipeline's (defaulting
// to FailureModeAll). It only affects jobs that fan out to more than one
// worker; a job on a single target is unaffected.
type FailureMode string

const (
	// FailureModeAll: the job succeeds only if every worker's run succeeds;
	// any worker that fails or times out fails the job.
	FailureModeAll FailureMode = "all"
	// FailureModeBestEffort: the job succeeds if every worker's run reaches a
	// terminal state (a per-worker failure or timeout is recorded but does not
	// fail the job).
	FailureModeBestEffort FailureMode = "best_effort"
	// FailureModeAny: the job succeeds as soon as one worker's run succeeds.
	FailureModeAny FailureMode = "any"
)

// StepResult is the terminal outcome of one step of a job's execution spec,
// recorded alongside the job's own final status (F-06).
type StepResult struct {
	// Index is the 0-based index of the step in the job's spec.
	Index  int        `json:"index"`
	Status StepStatus `json:"status"`
	// Error is a descriptive error message; empty when Status is Succeeded or
	// Skipped.
	Error string `json:"error,omitempty"`
	// Outputs are the named values the step produced (F-06): each key is the
	// name of a file the step wrote into its per-step output directory, and
	// each value is that file's content (trimmed). Empty when the step wrote
	// no output files or did not run.
	Outputs map[string]string `json:"outputs,omitempty"`
}

// ParamValue is a single value in a step's Params map. A value is either a
// scalar String or a list of Strings (by convention, only one is set), so a
// handler can carry both simple settings (e.g. an ansible inventory name) and
// ordered lists (e.g. a command's arguments) in the same map.
type ParamValue struct {
	// String is a scalar value.
	String string `json:"string,omitempty"`
	// Strings is an ordered list of values.
	Strings []string `json:"strings,omitempty"`
}

// JobStep is a single unit of work in a job's execution spec. A step is
// agnostic about *how* it runs: Type selects a step handler, and the handler
// reads everything it needs from the common fields (Workdir, Env, Timeout)
// and from Params. The built-in default handler ("shell", used when Type is
// empty) runs a command — optionally through a user-chosen shell — in a
// working directory with an environment.
//
// Handler-specific settings live in Params, not in the step's own fields, so
// a new step type can be added without changing the spec schema. The built-in
// shell handler reads its command, args, and shell from Params.
//
// Portability contract (shell handler): the command is resolved and executed
// directly by the execution target's OS (no shell is involved), so the same
// spec executes identically on Windows and Linux. Workdir is interpreted with
// the target's native path separator; a relative workdir is resolved against
// the target's current working directory.
//
// Shell override (shell handler): when the shell param is set, the step is
// run through that shell instead of executing the command directly — the
// target invokes `<shell> <args> <command>`, so the command is passed as the
// final argument (e.g. shell "pwsh", args ["-NoProfile", "-Command"],
// command "Get-ChildItem"). This lets a step opt into shell behavior (pipes,
// globbing, $VAR expansion) with an explicit, user-chosen shell rather than a
// platform default. When shell is empty the step runs the command directly,
// preserving the no-implicit-shell contract.
//
// Extensibility: Type names a step handler registered on the execution
// target (the built-in "shell" handler is always available; a target may
// register more, e.g. "ansible", "terraform", "argo", or a user plugin). A
// step whose Type is not registered on the target fails the job with a clear
// error.
type JobStep struct {
	// Type selects the step handler that runs this step. Empty means the
	// built-in "shell" handler. A target that has not registered the named
	// handler rejects the step.
	Type string `json:"type,omitempty"`
	// Workdir is the directory the step runs in; empty means the target's
	// current working directory.
	Workdir string `json:"workdir,omitempty"`
	// Env are extra environment variables for the step, in addition to the
	// target's inherited environment.
	Env map[string]string `json:"env,omitempty"`
	// Timeout is the maximum duration for this step; zero means no per-step
	// timeout.
	Timeout time.Duration `json:"timeout,omitempty"`
	// Params are the handler-specific settings for the step. The built-in
	// shell handler reads command (string), args (list of strings), and shell
	// (string) from here; a new step type reads the keys it understands.
	Params map[string]*ParamValue `json:"params,omitempty"`
	// Condition is a Go template (text/template) rendered against the step's
	// condition context (its own Env, plus the prior steps' and upstream
	// jobs' status/outputs, and the job's identity); the rendered output must
	// parse as a boolean (strconv.ParseBool). An empty Condition always runs
	// the step. When the rendered value is false the step is skipped
	// (StepStatusSkipped, F-06) — it does not run and does not fail the job.
	// A condition that fails to parse or render is a job-failing error (a bad
	// condition is a spec error, not a reason to skip).
	Condition string `json:"condition,omitempty"`
	// IgnoreFailed, when true, means a failure (or timeout) of this step does
	// not fail the job (F-06): the step is recorded as failed/timed_out and
	// the job continues to the next step. A step that fails without
	// IgnoreFailed stops the job.
	IgnoreFailed bool `json:"ignore_failed,omitempty"`
}

// RetryPolicy is a job's retry policy (F-04): how many times a failed job is
// re-dispatched and how long the scheduler waits between attempts.
type RetryPolicy struct {
	// MaxAttempts is the number of retries after the initial attempt; zero
	// means the job is never retried. A job with MaxAttempts 2 runs at most 3
	// times (the initial attempt plus 2 retries).
	MaxAttempts int `json:"max_attempts,omitempty"`
	// Backoff is the delay before each retry; zero means retries are
	// dispatched immediately.
	Backoff time.Duration `json:"backoff,omitempty"`
}

// JobSpec is the declarative execution spec of a job: an ordered list of
// steps. The execution target runs the steps in order and the job fails on
// the first step that errors.
type JobSpec struct {
	Steps []JobStep `json:"steps,omitempty"`
	// Timeout is the maximum total duration for the whole job (all steps
	// combined); zero means no job-level timeout. A step's own Timeout bounds
	// an individual step; the job-level Timeout bounds the sum of all steps.
	// When both are set, whichever expires first terminates the job. The
	// execution target enforces it by cancelling the running step; the
	// scheduler's watchdog reaps the job if the target goes silent (F-03).
	Timeout time.Duration `json:"timeout,omitempty"`
	// Retry is the job's retry policy (F-04); a nil policy means the job is
	// never retried.
	Retry *RetryPolicy `json:"retry,omitempty"`
	// IgnoreFailed, when true, means a failure of the job does not propagate
	// to its dependents (F-06): the job is still reported failed (or
	// timed_out), but the scheduler's dependency resolver treats it as
	// satisfied, so downstream jobs are dispatched rather than skipped. It
	// does not change the job's own status or stop a failed step from failing
	// the job (that is a step's IgnoreFailed).
	IgnoreFailed bool `json:"ignore_failed,omitempty"`
	// FailureMode controls how a job that fans out to a worker group is judged
	// from the per-worker outcomes (see FailureMode). An empty value means the
	// job inherits its pipeline's failure mode (defaulting to FailureModeAll).
	FailureMode FailureMode `json:"failure_mode,omitempty"`
	// StepBarrier, when true, makes a job that fans out to a worker group
	// synchronize its workers at each step boundary: after a worker completes
	// a step it waits until every worker alive at the step's start has
	// completed it before starting the next step. A worker that dies mid-step
	// is dropped from the barrier (it is no longer alive), so a dead worker
	// cannot wedge the job. It only affects jobs that fan out to more than
	// one worker; a job on a single target runs its steps unbarriered.
	StepBarrier bool `json:"step_barrier,omitempty"`
}

// Job is a single unit of work belonging to a pipeline.
//
// TargetGroup selects a group of long-lived workers; an empty value means
// the job runs on an ephemeral Kubernetes agent.
//
// Spec is a snapshot of the execution spec taken when the job was created,
// so a pipeline edit never changes what a past run did.
//
// Attempt and MaxAttempts track the job's retry progress (F-04): a retry is
// a new attempt on the same row (not a new Job), so the pipeline run stays
// coherent. Attempt is the 1-based number of the attempt currently in flight
// (or the last attempt, once the job is terminal); MaxAttempts is the retry
// budget copied from the spec's retry policy when the job was created.
type Job struct {
	gorm.Model
	PipelineID *uint     `gorm:"index" json:"pipeline_id"`
	Pipeline   *Pipeline `json:"pipeline,omitempty"`
	// RunID is the pipeline run this job is an instance of (F-07); nil when
	// the job is not part of a run (e.g. a standalone job submitted directly).
	// A job is a per-run *instance* of the pipeline's job definition: each run
	// of a pipeline creates its own set of job rows, so two runs of the same
	// pipeline are independent and both queryable.
	RunID *uint        `gorm:"index" json:"run_id"`
	Run   *PipelineRun `json:"run,omitempty"`
	// Key is the job's stable, pipeline-scoped identifier (F-08): a short
	// name (e.g. "build", "test") that is unique within the pipeline the job
	// belongs to. A job's `needs` (its dependencies) reference other jobs by
	// their Key, not by their id, so a pipeline's DAG is stable across runs
	// and survives re-creation of the run's job instances. A job that is not
	// part of a pipeline (a standalone job) may have an empty Key.
	Key         string     `gorm:"index" json:"key"`
	Name        string     `gorm:"not null" json:"name"`
	Status      JobStatus  `gorm:"default:pending;index" json:"status"`
	TargetGroup string     `gorm:"index" json:"target_group"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	// Spec is the execution spec snapshot, serialized to a JSON document in a
	// text column (portable across SQLite and PostgreSQL).
	Spec JobSpec `gorm:"type:text;serializer:json" json:"spec,omitempty"`
	// Attempt is the 1-based number of the attempt currently in flight (or
	// the last attempt, once the job is terminal). It is 1 for a job that has
	// run once and is incremented by the scheduler each time it re-dispatches
	// a failed job (F-04).
	Attempt int `json:"attempt"`
	// MaxAttempts is the retry budget copied from the spec's retry policy when
	// the job was created (0 means the job is never retried). It is
	// denormalized onto the job so the UI can render "attempt N of M" without
	// the spec.
	MaxAttempts int `json:"max_attempts"`
	// DependsOn lists the ids of jobs this job depends on. A job with a
	// non-empty DependsOn is held pending (not dispatched) until every
	// dependency reaches a terminal state: if all of them succeed the job is
	// dispatched, but if any of them does not succeed (failed, cancelled,
	// timed_out, or itself skipped) the job is marked Skipped instead of
	// running. For jobs that belong to a pipeline, the user expresses
	// dependencies as `needs` (the keys of other jobs, F-08); the database
	// service resolves those keys to job ids and stores the result here when
	// the pipeline is saved, and CreateRun remaps them to the run's own
	// instance ids. For standalone jobs (no pipeline) DependsOn is set
	// directly. The scheduler's dependency resolver (F-06/F-08) resolves it.
	DependsOn []uint `gorm:"type:text;serializer:json" json:"depends_on,omitempty"`
	// StepResults is the terminal outcome of each step that ran (or was
	// skipped by its condition) during the job's current attempt (F-06),
	// reported by the execution target alongside the job's final status.
	StepResults []StepResult `gorm:"type:text;serializer:json" json:"step_results,omitempty"`
	// Outputs are the named values the job produced (F-06): the union of its
	// steps' outputs (a later step overrides an earlier one on a name
	// collision). They are reported by the execution target and persisted so
	// downstream jobs on other targets can read them in their conditions.
	Outputs map[string]string `gorm:"type:text;serializer:json" json:"outputs,omitempty"`
	// IgnoreFailed is the job's ignore-failed flag copied from the spec's
	// IgnoreFailed when the job was created (F-06). It is denormalized onto
	// the job so the scheduler's dependency resolver can treat a failed job
	// with the flag set as satisfied without re-reading the spec.
	IgnoreFailed bool `json:"ignore_failed"`
	// FailureMode is the job's failure mode (see FailureMode), denormalized
	// from the spec (or the pipeline's default) when the job was created. The
	// scheduler's job-status loop uses it to derive the job's overall status
	// from the per-worker outcomes of a job that fanned out to a worker group.
	FailureMode FailureMode `gorm:"default:all" json:"failure_mode"`
	// StepBarrier is the job's step-barrier flag (see JobSpec.StepBarrier),
	// denormalized from the spec when the job was created. It is set on the
	// job so the execution target can tell from the job alone whether to
	// synchronize its workers at step boundaries.
	StepBarrier bool `json:"step_barrier"`
	// TriggerName is the name of the trigger that started the run this job is
	// an instance of (F-09); empty for a job not started by a named trigger.
	// It is denormalized from the run onto the job instance so the API can
	// stamp it onto the job token it hands to the execution target.
	TriggerName string `json:"trigger_name,omitempty"`
	// TriggerType is how the run this job is an instance of was started
	// (F-09): "manual", "cron", "webhook", or "event". It is denormalized
	// from the run onto the job instance so the API can stamp it onto the job
	// token it hands to the execution target.
	TriggerType string `json:"trigger_type,omitempty"`
	// UpstreamClaims are the claims of the OIDC token a webhook caller
	// presented to start the run this job is an instance of (F-09); empty for a
	// job not started by a webhook. They are the token's full claims as a JSON
	// object (a claim that is an object or a list keeps its structure) and are
	// denormalized from the run onto the job instance so the API can stamp them
	// (prefixed with upstream_) onto the job token it hands to the execution
	// target.
	UpstreamClaims map[string]any `gorm:"type:text;serializer:json" json:"upstream_claims,omitempty"`
	// RunParams are the run's concrete parameter values (F-10): the values
	// supplied for the pipeline's parameters (a parameter not supplied falls
	// back to its default), denormalized from the run onto the job instance.
	// The execution target interpolates them into the job's spec (env,
	// command, workdir) using Go templates before the job runs. Empty for a
	// job not part of a run (or a run with no parameters).
	RunParams map[string]string `gorm:"type:text;serializer:json" json:"run_params,omitempty"`
	// Secrets are the pipeline's named secrets for this run (F-12): a
	// name -> encrypted value list denormalized from the pipeline (or the
	// version snapshot) onto the job instance at run creation. The ciphertext
	// is opaque to the database service; the API decrypts it and hands the
	// plaintext to the execution target, where a step references it as
	// `{{ .secrets.name }}`.
	Secrets []Secret `gorm:"type:text;serializer:json" json:"secrets,omitempty"`
	// ApprovalMessage is the (rendered) message an approval gate (F-13) shows
	// to the user while the job is awaiting approval. It is set by the
	// execution target when it reports the job awaiting_approval and is
	// visible to the UI (via GET /api/jobs/{id}) so the approver knows what
	// they are approving.
	ApprovalMessage string `json:"approval_message,omitempty"`
	// ApprovalRequestedAt is when the job entered the awaiting_approval state
	// (F-13); nil when the job is not (or was not) awaiting approval.
	ApprovalRequestedAt *time.Time `json:"approval_requested_at,omitempty"`
	// ApprovalDecision is the outcome of the job's approval gate (F-13):
	// "approved" or "rejected"; empty when the job has no approval gate or the
	// gate has not been resolved.
	ApprovalDecision string `json:"approval_decision,omitempty"`
	// ApprovalActor is the identity (the OIDC subject) of the user who resolved
	// the job's approval gate (F-13); empty when the gate has not been
	// resolved.
	ApprovalActor string `json:"approval_actor,omitempty"`
	// ApprovalDecidedAt is when the job's approval gate was resolved (F-13);
	// nil when the gate has not been resolved.
	ApprovalDecidedAt *time.Time `json:"approval_decided_at,omitempty"`
	// ApprovalReason is the (optional) free-text reason the user gave when they
	// resolved the job's approval gate (F-13); empty when the user gave none.
	ApprovalReason string `json:"approval_reason,omitempty"`
}

// StepCompletion records that one worker completed one step of a job's
// current attempt (the cross-worker step barrier). A job that fans out to a
// worker group with the step barrier enabled waits, after each step, until
// every worker alive at the step's start has a StepCompletion row for that
// step; the barrier is satisfied when that is the case. A worker that dies
// mid-step is dropped from the barrier (it is no longer alive), so a dead
// worker cannot wedge the job.
type StepCompletion struct {
	gorm.Model
	// JobID is the job the step belongs to.
	JobID uint `gorm:"uniqueIndex:idx_step_completion_unique,priority:1;not null" json:"job_id"`
	// WorkerName is the worker that completed the step.
	WorkerName string `gorm:"uniqueIndex:idx_step_completion_unique,priority:2;not null" json:"worker_name"`
	// StepIndex is the 0-based index of the step that was completed.
	StepIndex int `gorm:"uniqueIndex:idx_step_completion_unique,priority:3" json:"step_index"`
	// Attempt is the job attempt this completion belongs to (F-04); a retry
	// starts a fresh set of completions.
	Attempt int `gorm:"uniqueIndex:idx_step_completion_unique,priority:4" json:"attempt"`
}

// JobExecution is one worker's run of a job (fan-out). A job that targets a
// worker group runs on every worker in the group; each worker's run is a
// separate JobExecution, tracked independently so the job's overall status can
// be derived from the per-worker outcomes (see FailureMode). A job that runs
// on a single target (an ephemeral agent, or a group with one worker) has a
// single execution.
//
// The Job row's status is the *derived* overall status (maintained by the
// scheduler's job-status loop); each JobExecution's status is that worker's
// own outcome, reported by the worker.
type JobExecution struct {
	gorm.Model
	// JobID is the job this execution runs.
	JobID uint `gorm:"index:idx_job_execution_job_attempt,priority:1;not null" json:"job_id"`
	// WorkerName is the worker that runs (or ran) this execution.
	WorkerName string `gorm:"index;not null" json:"worker_name"`
	// Attempt is the job attempt this execution belongs to (F-04); a retry
	// creates a fresh set of executions.
	Attempt int `gorm:"index:idx_job_execution_job_attempt,priority:2" json:"attempt"`
	// Status is this execution's state: running while the worker is executing
	// the job, and a terminal status (succeeded, failed, cancelled, or
	// timed_out) once the worker reports its outcome.
	Status JobStatus `gorm:"default:running;index" json:"status"`
	// StartedAt is when the worker started the execution.
	StartedAt *time.Time `json:"started_at,omitempty"`
	// FinishedAt is when the execution reached a terminal status; nil while
	// it is still running.
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// StepResults is the terminal outcome of each step that ran (or was
	// skipped by its condition) during this worker's run of the job (F-06),
	// reported by the worker alongside the execution's final status.
	StepResults []StepResult `gorm:"type:text;serializer:json" json:"step_results,omitempty"`
	// Outputs are the named values this worker's run produced (F-06).
	Outputs map[string]string `gorm:"type:text;serializer:json" json:"outputs,omitempty"`
}

// Worker is a long-lived worker process registered on a deployment target.
//
// Workers are targetable by group: a job may be dispatched to all (or a
// selection of) workers sharing a Group value.
type Worker struct {
	gorm.Model
	Name string `gorm:"uniqueIndex;not null" json:"name"`
	// Group is the worker's group. The column is named worker_group because
	// "group" is a reserved SQL keyword (notably in SQLite).
	Group      string     `gorm:"column:worker_group;index;not null" json:"group"`
	Address    string     `json:"address"`
	LastSeenAt *time.Time `json:"last_seen_at,omitempty"`
}

// IDPSigningKey is a single RSA signing key used by the IdP to sign JWTs.
//
// The keyring (the current key plus any not-yet-expired predecessors) is
// stored in the database rather than on the IdP's local filesystem, so
// multiple IdP replicas share the same keys and serve a consistent JWKS.
// This is what makes the IdP horizontally scalable: the only per-process
// state the IdP keeps in memory is its current signing key (used to sign new
// tokens), which is safe to be briefly stale because every key any replica
// signs with is published in the shared JWKS.
type IDPSigningKey struct {
	gorm.Model
	Kid       string    `gorm:"uniqueIndex;not null" json:"kid"`
	IsCurrent bool      `gorm:"index;not null;default:false" json:"is_current"`
	NotBefore time.Time `json:"not_before"`
	ExpiresAt time.Time `gorm:"index" json:"expires_at"`
	// Pem is the PKCS8 PEM encoding of the RSA private key.
	Pem string `gorm:"type:text" json:"-"`
}

// IDPAuthCode is a single-use OIDC authorization code issued by the IdP's
// authorization endpoint and redeemed at the token endpoint.
//
// It is stored in the database (rather than in the IdP's memory) so that the
// /auth request that mints a code and the /token request that redeems it can
// be served by different IdP replicas behind a load balancer.
type IDPAuthCode struct {
	gorm.Model
	Code          string `gorm:"uniqueIndex;not null" json:"code"`
	ClientID      string `gorm:"column:client_id" json:"client_id"`
	RedirectURI   string `gorm:"column:redirect_uri" json:"redirect_uri"`
	CodeChallenge string `gorm:"column:code_challenge" json:"code_challenge"` // empty when PKCE was not used
	Subject       string `json:"subject"`
	Name          string `json:"name"`
	Email         string `json:"email"`
}

// IDPUser is a registered user of the local identity provider. The IdP uses
// it for username/password authentication: it verifies a login against the
// stored password hash and, on success, mints an OIDC token for the user
// (stamping the user's roles onto the token). The password is stored only as
// a salted hash (PasswordHash); the plaintext is never persisted. Roles are
// the user's role names (e.g. "admin", "user"); they are stamped onto the
// user's tokens so the API can enforce role-based access.
//
// The user directory is stored in the database (not on the IdP's local
// filesystem) so multiple IdP replicas share the same users and can run
// behind a load balancer.
//
// The model uses a numeric, database-assigned primary key (auto-increment),
// like the other entities. It does not embed gorm.Model (so there is no
// soft-delete column); the timestamps are declared explicitly instead.
type IDPUser struct {
	// ID is the user's stable identifier (the token's subject); it is the
	// table's primary key, assigned by the database (auto-increment).
	ID        uint   `gorm:"primaryKey" json:"id"`
	FirstName string `gorm:"column:first_name" json:"first_name"`
	LastName  string `gorm:"column:last_name" json:"last_name"`
	// Email is the user's unique login identifier.
	Email string `gorm:"uniqueIndex;not null" json:"email"`
	// PasswordHash is the salted hash of the user's password (bcrypt). It is
	// never returned to the UI; the IdP uses it only to verify logins.
	PasswordHash string `gorm:"column:password_hash;type:text" json:"-"`
	// Roles are the user's role names, serialized to a JSON document in a
	// text column (portable across SQLite and PostgreSQL).
	Roles     []string  `gorm:"type:text;serializer:json" json:"roles"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Event is a row in the shared, append-only event log (F-23, high
// availability). The event log is the coordination bus that makes the control
// plane horizontally scalable: every API pod tails it and fans the events out
// to its local workers and UI clients, and the scheduler's background loops
// publish state changes to it instead of pushing to a specific API pod.
//
// An event is a *pointer*, not the data: the payload is a small JSON object
// with just enough to handle the event (e.g. a job id, or a log's new size),
// never the job spec, the log bytes, or a token. A consumer that sees an
// event does a follow-up lookup (GetJob, a range read of the log delta) to
// fetch the actual data. This keeps the table lean and makes it replay-safe:
// a replayed event can never carry a stale spec or an expired token.
//
// The table is partitioned by CreatedAt (PostgreSQL) so old events can be
// dropped as whole partitions (O(1), no bloat). The log is a *recent buffer*;
// the database state (jobs, runs, workers) is the source of truth. A consumer
// whose cursor has fallen out of the retained window resyncs from a state
// snapshot rather than replaying the log.
type Event struct {
	gorm.Model
	// Name is the event kind: "assignment", "cancel", "job_status",
	// "run_status", "worker", or "job_log_updated".
	Name string `gorm:"index;not null" json:"name"`
	// WorkerGroup is the group the event is relevant to (for group-scoped
	// events like assignment); empty for events that are not group-scoped
	// (status, run status, log updates). An API pod filters events by this
	// against the groups of the workers it holds streams for.
	WorkerGroup string `gorm:"column:worker_group;index" json:"worker_group"`
	// Payload is the small JSON object that carries the event's data (e.g.
	// {"job_id": 42} or {"job_id": 42, "log": "job.log", "size": 183422}).
	// It is stored as a JSON document in a text column (portable across
	// SQLite and PostgreSQL).
	Payload string `gorm:"type:text" json:"payload"`
}

// Lease is a named, expiring lock used for leader election (F-23, high
// availability). A replica acquires a lease, renews it periodically, and runs
// the work that must run on exactly one replica (the scheduler's background
// loops) only while it holds the lease. If the holder stops renewing (a crash
// or a network partition) another replica can take over, so the work is never
// left unattended.
//
// Ownership and liveness are decoupled. The row is keyed by Name (unique);
// Holder records which replica currently holds it, ExpiresAt is the long TTL
// backstop (ownership lapses then even if the holder is still heartbeating),
// and LastHeartbeat is when the holder last renewed it. A follower may take
// over once LastHeartbeat is stale (older than the heartbeat window) even if
// ExpiresAt has not lapsed, so a restarted or wedged leader is detected
// quickly instead of waiting for the full TTL.
type Lease struct {
	gorm.Model
	// Name is the lease's name (e.g. "scheduler"); unique.
	Name string `gorm:"uniqueIndex;not null" json:"name"`
	// Holder is the identity of the replica that currently holds the lease.
	Holder string `gorm:"not null" json:"holder"`
	// ExpiresAt is the lease's TTL backstop: ownership lapses then if the
	// holder has not renewed.
	ExpiresAt time.Time `gorm:"not null" json:"expires_at"`
	// LastHeartbeat is when the holder last renewed the lease; a follower may
	// take over once this is older than the heartbeat window.
	LastHeartbeat time.Time `gorm:"not null" json:"last_heartbeat"`
}

// Role is a named set of permissions (F-14, RBAC). A role is either a
// built-in role (shipped with the platform; it cannot be deleted or have its
// permission set edited) or a custom role (created by a principal holding
// roles.can-manage). A role's effective permissions are the union of its own
// Permissions and the effective permissions of the roles it Includes
// (composition, expanded transitively).
//
// Roles are stored in the Database service (like the IdP's user directory) so
// every API replica sees the same role catalog; each API replica evaluates
// authorization locally against a short-lived cache that is invalidated by
// role/binding change events on the shared event log (F-23).
type Role struct {
	gorm.Model
	// Name is the role's unique name (e.g. "admin", "operator", "viewer",
	// "user", or a custom name such as "pipeline-owner").
	Name string `gorm:"uniqueIndex;not null" json:"name"`
	// Description is a human-readable description of the role.
	Description string `json:"description"`
	// BuiltIn marks a role that ships with the platform. Built-in roles cannot
	// be deleted or have their permission set edited; only custom roles can.
	BuiltIn bool `gorm:"not null;default:false" json:"built_in"`
	// Permissions is the set of permission names the role grants directly
	// (e.g. "pipelines.can-view"). A role's full permission set is the union
	// of these and the effective permissions of every role it Includes.
	Permissions []string `gorm:"type:text;serializer:json" json:"permissions,omitempty"`
	// Includes lists the names of other roles this role composes: the role's
	// effective permissions are the union of its own Permissions and the
	// effective permissions of every included role (expanded transitively, so
	// a role that includes a role that includes another inherits all three).
	Includes []string `gorm:"type:text;serializer:json" json:"includes,omitempty"`
}

// RoleBinding attaches a role to a principal (F-14, RBAC), optionally scoped
// to a single pipeline. An unscoped binding (PipelineID nil) grants the
// role's permissions platform-wide; a pipeline-scoped binding grants them only
// for that pipeline (resource-scoped permissions only — platform-wide
// permissions such as role/user management are never granted by a scoped
// binding).
//
// A principal's effective permissions are the union of the permissions of
// every role bound to it (via a token's roles claim, a configured claim
// mapping, or a stored binding like this one), expanded through role
// composition and filtered by resource scope per request.
type RoleBinding struct {
	gorm.Model
	// PrincipalKind is the kind of principal the role is bound to: "user" or
	// "service-account".
	PrincipalKind string `gorm:"index;not null" json:"principal_kind"`
	// PrincipalID is the principal's identifier: for a user it is the user's
	// id (the token's subject); for a service account it is the account's id.
	PrincipalID string `gorm:"index;not null" json:"principal_id"`
	// RoleName is the name of the role bound to the principal.
	RoleName string `gorm:"index;not null" json:"role_name"`
	// PipelineID is the pipeline the binding is scoped to; nil means the
	// binding is unscoped (platform-wide).
	PipelineID *uint `gorm:"index" json:"pipeline_id,omitempty"`
}

// AuditEvent is one entry in the audit log (F-15): an append-only record of a
// significant action — who (Actor) did what (Action) to which resource
// (TargetKind/TargetID), when (CreatedAt), from where (SourceIP), with what
// outcome (Outcome), and what changed (OldValue/NewValue, plus free-form
// Details).
//
// The log is append-only: the database service only ever inserts rows; there
// is no update or delete path for past events. Old entries are pruned by age
// (the database service's PruneAuditEvents, driven by the API's retention
// config), which is a retention policy, not an edit of the record.
//
// Actor is the identity of the principal that performed the action: the
// authenticated user's subject (e.g. an email or user id) for a human, or a
// synthetic identity for a non-human actor (e.g. "system:scheduler" for a
// cron- or event-fired run, "system:webhook" for a webhook caller that
// authenticated with a shared secret rather than a token). ActorKind is the
// kind of actor ("user", "service-account", or "system").
//
// OldValue and NewValue are the resource's state before and after the action,
// as compact JSON documents (or empty when the action has no before/after
// state, e.g. a trigger or a login). They are the raw values with secret
// values redacted by the API before the event is recorded: a secret's name is
// recorded, but its value is never stored or displayed.
type AuditEvent struct {
	gorm.Model
	// Actor is the identity of the principal that performed the action (the
	// user's subject, or a synthetic identity such as "system:scheduler").
	Actor string `gorm:"index;not null" json:"actor"`
	// ActorKind is the kind of actor: "user", "service-account", or "system".
	ActorKind string `json:"actor_kind,omitempty"`
	// Action is the action that was performed (e.g. "pipeline.create",
	// "run.trigger", "job.cancel", "job.approve", "secret.encrypt",
	// "role.create", "binding.add", "user.create", "login", "register").
	Action string `gorm:"index;not null" json:"action"`
	// TargetKind is the kind of resource the action acted on (e.g.
	// "pipeline", "run", "job", "role", "role-binding", "user", "secret",
	// "worker", "login").
	TargetKind string `gorm:"index" json:"target_kind,omitempty"`
	// TargetID is the id (or name, for name-keyed resources such as roles) of
	// the resource the action acted on; empty when the action has no single
	// target (e.g. a login).
	TargetID string `gorm:"index" json:"target_id,omitempty"`
	// TargetName is a human-readable name of the target (e.g. the pipeline's
	// name); empty when unknown.
	TargetName string `json:"target_name,omitempty"`
	// PipelineID is the pipeline the action is scoped to, when it is
	// pipeline-scoped (a run, job, or secret of a pipeline); 0 otherwise. It
	// is denormalized from the target so events can be filtered by pipeline.
	PipelineID uint `gorm:"index" json:"pipeline_id,omitempty"`
	// RunID is the pipeline run the action is scoped to, when it is run-scoped
	// (triggering or cancelling a run's job); 0 otherwise.
	RunID uint `gorm:"index" json:"run_id,omitempty"`
	// Outcome is the action's outcome: "success" or "failure" (a failed
	// action is recorded too, so an audit trail shows what was attempted and
	// what was rejected).
	Outcome string `gorm:"index" json:"outcome"`
	// SourceIP is the client's IP address (the HTTP request's remote address,
	// or the gRPC peer's address); empty when the action did not come from a
	// network client (e.g. a scheduler loop).
	SourceIP string `json:"source_ip,omitempty"`
	// OldValue is the resource's state before the action, as a compact JSON
	// document (secret values redacted); empty when the action has no
	// before-state (a create, a trigger, a login).
	OldValue string `gorm:"type:text" json:"old_value,omitempty"`
	// NewValue is the resource's state after the action, as a compact JSON
	// document (secret values redacted); empty when the action has no
	// after-state (a delete, a cancel).
	NewValue string `gorm:"type:text" json:"new_value,omitempty"`
	// Details is a free-form JSON document carrying action-specific context
	// that does not fit the structured fields (e.g. a trigger's name and
	// params, an approval's decision and reason, a login's email).
	Details string `gorm:"type:text" json:"details,omitempty"`
}

// All lists every model the database service must migrate. Add new entities
// here so AutoMigrate always sees the complete set.
func All() []any {
	return []any{
		&Pipeline{},
		&PipelineRun{},
		&PipelineVersion{},
		&Job{},
		&JobExecution{},
		&StepCompletion{},
		&Worker{},
		&IDPSigningKey{},
		&IDPAuthCode{},
		&IDPUser{},
		&Event{},
		&Lease{},
		&SecretNonce{},
		&Role{},
		&RoleBinding{},
		&AuditEvent{},
	}
}
