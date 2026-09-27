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
)

// JobStep is a single unit of work in a job's execution spec. A step is
// agnostic about *how* it runs: Type selects a step handler, and the
// remaining fields are interpreted by that handler. The built-in default
// handler ("shell", used when Type is empty) runs a command — optionally
// through a user-chosen shell — in a working directory with an environment.
//
// Portability contract (shell handler): Command is resolved and executed
// directly by the execution target's OS (no shell is involved), so the same
// spec executes identically on Windows and Linux. Workdir is interpreted with
// the target's native path separator; a relative workdir is resolved against
// the target's current working directory.
//
// Shell override (shell handler): when Shell is set, the step is run through
// that shell instead of executing Command directly — the target invokes
// `<Shell> <Args> <Command>`, so Command is passed as the final argument
// (e.g. Shell "pwsh", Args ["-NoProfile", "-Command"], Command
// "Get-ChildItem"). This lets a step opt into shell behavior (pipes,
// globbing, $VAR expansion) with an explicit, user-chosen shell rather than a
// platform default. When Shell is empty the step runs Command directly,
// preserving the no-implicit-shell contract.
//
// Extensibility: Type names a step handler registered on the execution
// target (the built-in "shell" handler is always available; a target may
// register more, e.g. "ansible", "terraform", "argo", or a user plugin).
// Params carries handler-specific configuration that does not fit the common
// fields, so a new step type can be added without changing the spec schema.
// A step whose Type is not registered on the target fails the job with a
// clear error.
type JobStep struct {
	// Type selects the step handler that runs this step. Empty means the
	// built-in "shell" handler. A target that has not registered the named
	// handler rejects the step.
	Type string `json:"type,omitempty"`
	// Command is the executable to run (e.g. "go", "pwsh", "bash"). For the
	// shell handler, when Shell is set, Command is instead passed as the final
	// argument to the shell. Other handlers may interpret or ignore it.
	Command string `json:"command"`
	// Args are the command's arguments, in order. For the shell handler, when
	// Shell is set, Args are the shell's own arguments (e.g.
	// ["-NoProfile", "-Command"]) and Command is appended after them.
	Args []string `json:"args,omitempty"`
	// Workdir is the directory the command runs in; empty means the target's
	// current working directory.
	Workdir string `json:"workdir,omitempty"`
	// Env are extra environment variables for the command, in addition to the
	// target's inherited environment.
	Env map[string]string `json:"env,omitempty"`
	// Timeout is the maximum duration for this step; zero means no per-step
	// timeout.
	Timeout time.Duration `json:"timeout,omitempty"`
	// Shell, when set, is the interpreter the step is run through: the target
	// executes `<Shell> <Args> <Command>` instead of Command directly. This is
	// how a step opts into shell behavior with a user-chosen shell (e.g.
	// "pwsh" on Windows, "bash" on Linux) instead of a platform default. Empty
	// means run Command directly (no shell).
	Shell string `json:"shell,omitempty"`
	// Params are handler-specific key/value settings for the step (e.g. an
	// ansible inventory or a terraform workspace). The built-in shell handler
	// ignores them; a new step type reads the keys it understands.
	Params map[string]string `json:"params,omitempty"`
}

// JobSpec is the declarative execution spec of a job: an ordered list of
// steps. The execution target runs the steps in order and the job fails on
// the first step that errors.
type JobSpec struct {
	Steps []JobStep `json:"steps,omitempty"`
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
