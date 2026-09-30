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
	Jobs        []Job  `json:"jobs,omitempty"`
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
)

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
}

// Job is a single unit of work belonging to a pipeline.
//
// TargetGroup selects a group of long-lived workers; an empty value means
// the job runs on an ephemeral Kubernetes agent.
//
// Spec is a snapshot of the execution spec taken when the job was created,
// so a pipeline edit never changes what a past run did.
type Job struct {
	gorm.Model
	PipelineID  *uint      `gorm:"index" json:"pipeline_id"`
	Pipeline    *Pipeline  `json:"pipeline,omitempty"`
	Name        string     `gorm:"not null" json:"name"`
	Status      JobStatus  `gorm:"default:pending;index" json:"status"`
	TargetGroup string     `gorm:"index" json:"target_group"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	// Spec is the execution spec snapshot, serialized to a JSON document in a
	// text column (portable across SQLite and PostgreSQL).
	Spec JobSpec `gorm:"type:text;serializer:json" json:"spec,omitempty"`
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

// All lists every model the database service must migrate. Add new entities
// here so AutoMigrate always sees the complete set.
func All() []any {
	return []any{
		&Pipeline{},
		&Job{},
		&Worker{},
		&IDPSigningKey{},
		&IDPAuthCode{},
	}
}
