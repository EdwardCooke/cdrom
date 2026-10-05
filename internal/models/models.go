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

// All lists every model the database service must migrate. Add new entities
// here so AutoMigrate always sees the complete set.
func All() []any {
	return []any{
		&Pipeline{},
		&PipelineRun{},
		&Job{},
		&JobExecution{},
		&StepCompletion{},
		&Worker{},
		&IDPSigningKey{},
		&IDPAuthCode{},
		&Event{},
		&Lease{},
	}
}
