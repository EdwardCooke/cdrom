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

// VariableShape is the shape of a variable's value (F-21.1.3): a plain value,
// an encrypted (builtin F-12 ciphertext) value, or a sourced (a value from a
// named secret source) value. A variable is masked when its shape is not
// VariablePlain - that is, when its value is not a plain string.
type VariableShape int

// The shapes a variable's value can be.
const (
	// VariablePlain is a plain value (a scalar string); it is not masked.
	VariablePlain VariableShape = iota
	// VariableEncrypted is a builtin F-12 ciphertext (an {encrypted:
	// <ciphertext>} value); it is masked.
	VariableEncrypted
	// VariableSecretSource is a value from a named secret source (a
	// {secretSource: <name>, key: <key>} value); it is masked.
	VariableSecretSource
)

// Variable is a single variable in a variables map (F-21.1.3): a name and a
// value. There is no separate concept of a secret: every value is a variable,
// and a variable is masked when its value is not a plain string. Exactly one
// of the shape-specific fields is set, selected by Shape:
//
//	VariablePlain:        Plain (the value);
//	VariableEncrypted:    Encrypted (the builtin F-12 ciphertext);
//	VariableSecretSource: SecretSource (the named source) + Key (the value's
//	                       key within the source).
type Variable struct {
	// Name is the variable's name (its key in the variables map).
	Name string `json:"name"`
	// Shape is the value's shape.
	Shape VariableShape `json:"shape"`
	// Plain is the value when Shape is VariablePlain.
	Plain string `json:"plain,omitempty"`
	// Encrypted is the builtin F-12 ciphertext when Shape is
	// VariableEncrypted. It is self-describing (it carries the nonce) and safe
	// to store opaquely; the API decrypts it at dispatch and hands the
	// plaintext to the execution target. The plaintext is never persisted.
	Encrypted string `json:"encrypted,omitempty"`
	// SecretSource is the name of the secret source the value is sourced from
	// when Shape is VariableSecretSource.
	SecretSource string `json:"secret_source,omitempty"`
	// Key is the value's key within the named source when Shape is
	// VariableSecretSource.
	Key string `json:"key,omitempty"`
}

// VariableSet is a named variable set (F-21.1.3): an inline variables map
// declared under a pipeline's spec.variableSets. A job references a set by
// name in its variableSets list (JobDefinitionSnapshot.VariableSetRefs).
type VariableSet struct {
	// Name is the set's name (its key under spec.variableSets).
	Name string `json:"name"`
	// Variables are the set's variables, in file order.
	Variables []Variable `json:"variables,omitempty"`
	// VaultImports are the set's every-key vault imports (F-21.4): a set's
	// `import: [{vault: <name>}]` entry (no keys) imports every value in the
	// named secret source as masked variables. The vault's keys are not known
	// at compile time, so the import is recorded here (a per-key import is
	// materialized as concrete VariableSecretSource variables in Variables)
	// and expanded at dispatch (the API fetches the vault and creates one
	// variable per key). Empty means the set has no every-key vault imports.
	VaultImports []VaultImport `json:"vault_imports,omitempty"`
}

// VaultImport is a variable set's vault import (F-21.4): a reference to a named
// secret source (vault) whose values are imported as masked variables. A
// per-key import (Keys set) is materialized at compile time as concrete
// VariableSecretSource variables (one per key); an every-key import (Keys
// empty) is dynamic and is recorded here, to be expanded at dispatch.
type VaultImport struct {
	// Source is the name of the secret source (vault) to import.
	Source string `json:"source"`
	// Keys are the specific keys to import; empty means every key in the vault.
	Keys []string `json:"keys,omitempty"`
}

// SecretSource is a named secret source (F-21.1.3 / F-12): a Kind (a
// secret-store kind - Vault / OpenBao are reserved kinds; the built-in AES
// store is the default when no source is named), a Path (the source path),
// and an Audience (the token audience used to fetch from the source). The
// source's settings are stored opaquely on the pipeline (like a secret
// declaration, F-12); the API resolves the named source to a concrete value
// at dispatch and hands it to the execution target.
type SecretSource struct {
	// Name is the source's name (its key under spec.secretSources).
	Name string `json:"name"`
	// Kind is the secret-store kind (e.g. Vault, OpenBao); empty means the
	// built-in AES store (the default when no source is named).
	Kind string `json:"kind,omitempty"`
	// Path is the source path.
	Path string `json:"path,omitempty"`
	// Audience is the token audience used to fetch from the source.
	Audience string `json:"audience,omitempty"`
}

// SecretSourceRef is a job instance's reference to a named secret source
// (F-21.1.3 / F-12): the source's name and the value's key within the source.
// It is denormalized onto the job instance at run creation (like the job's
// secrets) so the API can resolve the named source to a concrete value at
// dispatch and hand the plaintext to the execution target, where a step
// references it as `{{ .params.<name> }}`.
type SecretSourceRef struct {
	// Name is the variable's name (the name a step references it as).
	Name string `json:"name"`
	// Source is the name of the secret source the value is sourced from.
	Source string `json:"source"`
	// Key is the value's key within the named source.
	Key string `json:"key"`
}

// PackageArtifact is one entry of a pipeline's spec.artifacts list (F-21.6):
// a named reference to a PACKAGE the pipeline consumes. Name is how the
// pipeline refers to the artifact (in a step's params or env); PackageName
// is the package itself (defaulting to Name when empty); Tag pins the exact
// version to use (ignored when UseLatest is true); AllowPreRelease opts into
// pre-release versions (a tag carrying a "-" suffix, e.g. 6.6.6-rc.1);
// PreReleaseRegex, when set alongside AllowPreRelease, restricts which
// pre-release tags are eligible.
type PackageArtifact struct {
	// Name is the artifact's reference name (unique within the pipeline).
	Name string `json:"name"`
	// PackageName is the package's name in the registry; empty means "use
	// Name".
	PackageName string `json:"package_name,omitempty"`
	// Tag is the exact version to use; unused when UseLatest is true.
	Tag string `json:"tag,omitempty"`
	// UseLatest selects the latest version of the package by date (ignoring
	// Tag).
	UseLatest bool `json:"use_latest"`
	// AllowPreRelease opts into pre-release versions (otherwise only release
	// tags are eligible).
	AllowPreRelease bool `json:"allow_pre_release"`
	// PreReleaseRegex, when non-empty (and AllowPreRelease is set), restricts
	// eligibility to pre-release tags matching this regex.
	PreReleaseRegex string `json:"pre_release_regex,omitempty"`
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
	// ConcurrencyKeys are the pipeline's concurrency keys (F-16): named,
	// global locks that serialize a run of this pipeline against runs of any
	// other pipeline that shares a key. The keys are OR'd: a run is blocked
	// while any one of the pipeline's keys is held, and it holds all of them
	// while it runs. A job's effective key set is the union of the pipeline's
	// keys and the job's own keys (see JobSpec.ConcurrencyKeys). They are
	// stored as a JSON document in a text column (portable across SQLite and
	// PostgreSQL), like Params. Empty means the pipeline holds no concurrency
	// keys.
	ConcurrencyKeys []string `gorm:"type:text;serializer:json" json:"concurrency_keys,omitempty"`
	// Tags are the pipeline's tags (F-29): a set of key/value pairs that label
	// the pipeline. They are metadata (they never gate, route, or serialize
	// work) and are combined with the tags of a run's other resources (job,
	// environment, worker group, worker) onto the run's job instances. They
	// are stored normalized in the Tag table (one row per key/value pair,
	// owner_type "pipeline"), not as a column on this row, so they can be
	// searched by key and/or value. The field is in-memory only (gorm:"-"): the
	// database service populates it from the Tag table on load and writes it
	// back on save. Empty means the pipeline has no tags.
	Tags map[string]string `gorm:"-" json:"tags,omitempty"`
	// VariableSets are the pipeline's named variable sets (F-21.1.3): each is
	// an inline variables map (a name -> value map; a value is a plain string,
	// an encrypted builtin F-12 ciphertext, or a reference to a named
	// SecretSource). A job references a set by name in its variableSets list
	// (JobDefinitionSnapshot.VariableSetRefs); at run creation the database
	// service merges the referenced sets' variables with the job's inline
	// variables (the job's inline variables win on a key collision) and
	// denormalizes the result onto each job instance. They are stored as a
	// JSON document in a text column (portable across SQLite and PostgreSQL),
	// like Params. Empty means the pipeline declares no variable sets.
	VariableSets []VariableSet `gorm:"type:text;serializer:json" json:"variable_sets,omitempty"`
	// SecretSources are the pipeline's named secret sources (F-21.1.3 / F-12):
	// each has a Kind (a secret-store kind - Vault / OpenBao are reserved
	// kinds; the built-in AES store is the default when no source is named), a
	// Path, and an Audience. A variable references a source by name (with a
	// Key selecting the value within the source); the source's settings are
	// stored opaquely on the pipeline (like a secret declaration, F-12) and
	// the API resolves the named source to a concrete value at dispatch. They
	// are stored as a JSON document in a text column, like Params. Empty means
	// the pipeline declares no secret sources.
	SecretSources []SecretSource `gorm:"type:text;serializer:json" json:"secret_sources,omitempty"`
	// SourceFile is the raw definition text of an externally-sourced pipeline
	// (F-21.5.1): the verbatim file the source-fetcher returned at the last
	// successful sync, stored so it is retrievable through the API and is the
	// exact input the compiler ran. Empty for a UI-authored pipeline.
	SourceFile string `gorm:"column:source_file;type:text" json:"source_file,omitempty"`
	// SourceHash is the SHA-256 (hex) of SourceFile (F-21.5.1): the change
	// detector. A sync that yields the same hash is a no-op (no recompile, no
	// version bump); a differing hash re-imports the pipeline (bumping its
	// version, F-11).
	SourceHash string `gorm:"column:source_hash;size:64" json:"source_hash,omitempty"`
	// PeriodicRefreshInterval is the steady-state refresh cadence the
	// pipeline's FILE declares (F-21.5.1, spec.periodicRefresh.interval): a
	// duration string (e.g. "2m"). Empty means unpinned — the system default
	// applies. Stored verbatim as authored; the refresh loop parses it at sync
	// time. Malformed values are rejected at import time (the compiler).
	PeriodicRefreshInterval string `gorm:"column:periodic_refresh_interval;size:64" json:"periodic_refresh_interval,omitempty"`
	// PeriodicRefreshSchedule is the cron expression the pipeline's FILE
	// declares (F-21.5.1, spec.periodicRefresh.schedule): precise timing —
	// e.g. syncing only during after-hours when load is minimal. When
	// non-empty it DRIVES the periodic refresh (the interval is ignored).
	// Malformed expressions are rejected at import time (the compiler).
	PeriodicRefreshSchedule string `gorm:"column:periodic_refresh_schedule;size:128" json:"periodic_refresh_schedule,omitempty"`
	// PeriodicRefreshOverridesAllowed is the per-pipeline half of the admin
	// gate (F-21.5.1): whether this pipeline's file-directed periodicRefresh
	// override is honored. Nil (unset) or true means allowed; false means the
	// file's cadence is ignored and the system default applies — denying
	// restores the safe default, it does not halt polling. Editable through
	// the pipeline API by whoever may edit the pipeline. A pointer (rather
	// than a bool with a default tag) sidesteps GORM's zero-value omission:
	// an explicit false must be writable.
	PeriodicRefreshOverridesAllowed *bool `gorm:"column:periodic_refresh_overrides_allowed;not null;default:true" json:"periodic_refresh_overrides_allowed,omitempty"`
	// Artifacts are the pipeline's named package references (F-21.6,
	// spec.artifacts): the packages the pipeline consumes, with their version
	// resolution settings. They are stored as a JSON document in a text
	// column, like Params. When a run is created the database service
	// resolves each reference against the package registry (the artifacts
	// store's "packages" namespace) and records the resolution on the run
	// (a per-run snapshot, like the rest of the spec — F-01/F-11).
	Artifacts []PackageArtifact `gorm:"type:text;serializer:json" json:"artifacts,omitempty"`
}

// AllowsPeriodicRefreshOverrides reports whether the pipeline's file-directed
// periodicRefresh override is honored (F-21.5.1): an unset (nil) flag counts
// as allowed, so pre-existing rows behave safely.
func (p *Pipeline) AllowsPeriodicRefreshOverrides() bool {
	return p.PeriodicRefreshOverridesAllowed == nil || *p.PeriodicRefreshOverridesAllowed
}

// PipelineSource is where a pipeline is pulled from (F-21.5.1): a `kind` of
// `git` / `http` / `file` and a `reference` — the `<type>:<spec>` locator
// string (F-21.2) that names the file to fetch — a refresh `schedule`, and a
// `credential` (the name of the SourceCredential used to authenticate; empty
// means the source is public / needs no credential). The source is a database
// concept configured through the API — it is not in the file — so the same
// file is portable across source kinds. A pipeline becomes externally-sourced
// when a PipelineSource is attached to it.
type PipelineSource struct {
	gorm.Model
	// PipelineID is the pipeline this source belongs to (one source per
	// pipeline).
	PipelineID uint `gorm:"uniqueIndex;not null" json:"pipeline_id"`
	// Kind is the source kind: "git", "http", or "file".
	Kind string `gorm:"size:16;not null" json:"kind"`
	// Reference is the `<type>:<spec>` locator string (F-21.2) that names the
	// file to fetch. It is the single source of truth for the locator and is
	// what the API passes to FetchSource.
	Reference string `gorm:"size:1024;not null" json:"reference"`
	// Schedule is a cron expression for the scheduled refresh; empty means no
	// scheduled refresh.
	Schedule string `gorm:"size:128" json:"schedule,omitempty"`
	// Credential is the name of the SourceCredential used to authenticate the
	// fetch; empty means the source is public / needs no credential (the
	// longest matching prefix is used instead, F-21.3.2).
	Credential string `gorm:"size:128;index" json:"credential,omitempty"`
}

// PipelineSourceState is the last sync's bookkeeping for a pipeline's source
// (F-21.5.1): when it last synced, the source's version identifier, the
// content hash of the file at that version, and the last error (set when a
// sync fails — including a credential that fails to authenticate — and
// cleared on the next success). It is how a stale credential or a dead source
// is diagnosable without a repo checkout.
type PipelineSourceState struct {
	gorm.Model
	// PipelineID is the pipeline whose source this state belongs to (one state
	// per pipeline).
	PipelineID uint `gorm:"uniqueIndex;not null" json:"pipeline_id"`
	// LastSyncedAt is when the source last synced (successfully or not).
	LastSyncedAt time.Time `json:"last_synced_at"`
	// LastVersion is the source's version identifier at the last successful
	// sync (the git commit hash, an HTTP ETag/Last-Modified, or the file's
	// mtime) — so a no-change can be detected cheaply.
	LastVersion string `gorm:"size:255" json:"last_version,omitempty"`
	// FileHash is the content hash (SHA-256, hex) of the file at the last
	// successful sync.
	FileHash string `gorm:"size:64" json:"file_hash,omitempty"`
	// LastError is the last sync's error (set when a sync fails, cleared on
	// the next success).
	LastError string `gorm:"type:text" json:"last_error,omitempty"`
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
	// ConcurrencyKeys are the pipeline's concurrency keys at this version
	// (F-16); see Pipeline.ConcurrencyKeys. They are captured in the snapshot
	// so a run bound to this version is gated by the keys that were active
	// when it started.
	ConcurrencyKeys []string `gorm:"type:text;serializer:json" json:"concurrency_keys,omitempty"`
	// Tags are the pipeline's tags at this version (F-29); see
	// Pipeline.Tags. They are captured in the snapshot so a run bound to this
	// version combines the tags that were active when it started. They are
	// stored normalized in the Tag table (owner_type "pipeline_version",
	// owner_id the version's id) and are in-memory only on this row (gorm:"-").
	Tags map[string]string `gorm:"-" json:"tags,omitempty"`
	// VariableSets are the pipeline's named variable sets at this version
	// (F-21.1.3); see Pipeline.VariableSets. They are captured in the snapshot
	// so a run bound to this version merges the sets that were active when it
	// started.
	VariableSets []VariableSet `gorm:"type:text;serializer:json" json:"variable_sets,omitempty"`
	// SecretSources are the pipeline's named secret sources at this version
	// (F-21.1.3 / F-12); see Pipeline.SecretSources. They are captured in the
	// snapshot so a run bound to this version resolves the sources that were
	// active when it started.
	SecretSources []SecretSource `gorm:"type:text;serializer:json" json:"secret_sources,omitempty"`
	// PeriodicRefreshInterval is the steady-state refresh cadence the
	// pipeline's file declared at this version (F-21.5.1); see
	// Pipeline.PeriodicRefreshInterval. Captured in the snapshot for history;
	// the refresh loop acts on the CURRENT definition.
	PeriodicRefreshInterval string `gorm:"column:periodic_refresh_interval;size:64" json:"periodic_refresh_interval,omitempty"`
	// PeriodicRefreshSchedule is the cron expression the pipeline's file
	// declared at this version (F-21.5.1); see
	// Pipeline.PeriodicRefreshSchedule.
	PeriodicRefreshSchedule string `gorm:"column:periodic_refresh_schedule;size:128" json:"periodic_refresh_schedule,omitempty"`
	// Artifacts are the pipeline's named package references at this version
	// (F-21.6); see Pipeline.Artifacts. Captured in the snapshot so a run
	// bound to this version resolves the SAME references the version
	// declared.
	Artifacts []PackageArtifact `gorm:"type:text;serializer:json" json:"artifacts,omitempty"`
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
	// Environment is the environment the job targets (F-17); empty means the
	// job targets its raw TargetGroup. It is captured in the snapshot so a run
	// bound to this version is dispatched to, and gated by, the environment
	// that was active when it started.
	Environment string `json:"environment,omitempty"`
	// Automation is the job's own automation setting (F-17); empty means the
	// job sets none (it defers to its environment's policy).
	Automation Automation `json:"automation,omitempty"`
	// Approval is the job's override of its environment's approval policy
	// (F-17); empty means the job defers to the environment's flag.
	Approval ApprovalPolicy `json:"approval,omitempty"`
	// Needs lists the keys of the jobs this job depends on (F-08).
	Needs []string `json:"needs,omitempty"`
	// Spec is the execution spec to snapshot onto each of the job's run
	// instances. The job's own concurrency keys (F-16) are carried in the
	// spec (JobSpec.ConcurrencyKeys), so a run bound to this version is gated
	// by the keys that were active when it started.
	Spec JobSpec `json:"spec,omitempty"`
	// Tags are the job's tags at this version (F-29): a set of key/value pairs
	// that label the job. They are combined with the tags of the run's other
	// resources (pipeline, environment, worker group, worker) onto the run's
	// job instances. Empty means the job has no tags.
	Tags map[string]string `json:"tags,omitempty"`
	// Variables are the job's effective variables (F-21.1.3): the union of the
	// job's inline variables and the variables of the variable sets it
	// references (declared under the pipeline's spec.variableSets), with the
	// job's inline variables winning on a key collision. They are computed at
	// import time (the API merges the job's inline variables with the
	// referenced sets' variables) and denormalized onto each job instance at
	// run creation (Job.Variables). Empty means the job has no variables.
	Variables []Variable `json:"variables,omitempty"`
	// VaultImports are the job's every-key vault imports (F-21.4): the union of
	// the every-key vault imports of the variable sets the job references. They
	// are computed at import time (the API collects the referenced sets'
	// VaultImports) and denormalized onto each job instance at run creation
	// (Job.VaultImports). At dispatch the API expands each into one variable
	// per vault key. Empty means the job has no every-key vault imports.
	VaultImports []VaultImport `json:"vault_imports,omitempty"`
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
	// SourceKind is the kind of the pipeline's external source at kickoff
	// (F-21.5.1): "git" / "http" / "file". Empty for a run of a UI-authored
	// pipeline (one with no attached source). Snapshotted at run creation,
	// after the refresh-on-run hook has settled, so it records the source the
	// definition the run executed was pulled from.
	SourceKind string `gorm:"column:source_kind;size:16" json:"source_kind,omitempty"`
	// SourceReference is the source's <type>:<spec> locator at kickoff
	// (F-21.5.1) — the path/location of the file the run's definition came
	// from (e.g. "git:org/repo@main[pipelines/prod.yaml]"). Empty for a run of
	// a UI-authored pipeline.
	SourceReference string `gorm:"column:source_reference;size:1024" json:"source_reference,omitempty"`
	// SourceVersion is the source's version identifier at the sync the run's
	// definition came from (F-21.5.1): the git commit hash, an HTTP
	// ETag/Last-Modified, or the file's mtime. Empty for a run of a
	// UI-authored pipeline.
	SourceVersion string `gorm:"column:source_version;size:255" json:"source_version,omitempty"`
	// SourceFileHash is the SHA-256 (hex) of the definition file the run
	// executed (F-21.5.1) — the content hash at the last successful sync.
	// Empty for a run of a UI-authored pipeline. Together with
	// SourceReference/SourceVersion it answers "which file, at which
	// revision, produced this run?" even after the source is repointed or
	// detached.
	SourceFileHash string `gorm:"column:source_file_hash;size:64" json:"source_file_hash,omitempty"`
	// Artifacts is the run's RESOLVED package snapshot (F-21.6): a
	// spec.artifacts reference name → the stored file name the reference
	// resolved to when the run started ("<package>-<version>.<ext>", looked
	// up in the package registry). Recorded like the rest of the spec
	// (F-01/F-11) so a run always uses the version resolved when it started —
	// re-resolving on a later run can pick up a newer version without
	// changing what a past run did. Empty for a run whose pipeline declares
	// no spec.artifacts.
	Artifacts map[string]string `gorm:"type:text;serializer:json" json:"artifacts,omitempty"`
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
	// JobStatusAwaitingProceed is a non-terminal, in-flight state (F-17): the
	// job targets a `manual` environment and is held (its triggers and
	// dependencies are satisfied) waiting for an explicit proceed call before
	// it is dispatched. It is distinct from pending (not yet ready) and
	// running (actively executing) so the UI can show the hold and offer a
	// proceed action. The job leaves this state when it is proceeded
	// (→ running) or cancelled / timed out.
	JobStatusAwaitingProceed JobStatus = "awaiting_proceed"
)

// Automation is how a job (or an environment, F-17) is governed once its
// triggers and dependencies are satisfied:
//
//   - AutomationAutomatic: the job is dispatched on its own (the previous
//     environment is irrelevant).
//   - AutomationManual: the job is held (awaiting_proceed) and waits for an
//     explicit proceed call before it is dispatched.
type Automation string

const (
	// AutomationAutomatic dispatches the job on its own.
	AutomationAutomatic Automation = "automatic"
	// AutomationManual holds the job for an explicit proceed call.
	AutomationManual Automation = "manual"
)

// ApprovalPolicy is a job's override of its environment's approval policy
// (F-17). It selects whether an F-13 approval gate is inserted before the job
// runs, taking precedence over the environment's requires_approval flag.
type ApprovalPolicy string

const (
	// ApprovalPolicyNone skips the approval gate even if the environment
	// requires one.
	ApprovalPolicyNone ApprovalPolicy = "none"
	// ApprovalPolicyRequired inserts an approval gate even if the environment
	// does not require one.
	ApprovalPolicyRequired ApprovalPolicy = "required"
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
	// Name is the step's name (its identity in the job's spec).
	Name   string     `json:"name"`
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
// Portability contract (shell handler): when a step sets no shell param and
// no args, its command is a one-line shell command and is run through the
// execution target OS's default shell (bash -c <command> on Linux when bash is
// on the PATH, else sh -c <command>, and on Windows pwsh -Command <command>
// when PowerShell Core is on the PATH, else powershell -Command <command>).
// When a step sets args (it is constructing the invocation explicitly) the
// command is executed directly by the target's OS (no shell is involved), so
// the same spec executes identically on Windows and Linux. Workdir is
// interpreted with the target's native path separator; a relative workdir is
// resolved against the target's current working directory.
//
// Shell override (shell handler): when the shell param is set, the step is
// run through that shell instead of the platform default — the target invokes
// `<shell> <args> <command>`, so the command is passed as the final argument
// (e.g. shell "pwsh", args ["-NoProfile", "-Command"], command
// "Get-ChildItem"). This lets a step opt into shell behavior (pipes, globbing,
// $VAR expansion) with an explicit, user-chosen shell rather than the platform
// default.
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
	// Once (F-01), when true, means the step runs on at most one target of a
	// job that fans out to a worker group: the first target to claim the
	// step's slot runs it, the others skip it (recorded skipped, not failed).
	// A retry (a new attempt) may run it again.
	Once bool `json:"once,omitempty"`
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
	// ConcurrencyKeys are the job's concurrency keys (F-16): named, global
	// locks that serialize this job against other jobs (in any pipeline or
	// run) that share a key. The keys are OR'd: the job is blocked while any
	// one of its keys is held, and it holds all of its keys while it runs. A
	// job's effective key set is the union of its own keys and its pipeline's
	// keys (see Pipeline.ConcurrencyKeys). Empty means the job holds no
	// concurrency keys.
	ConcurrencyKeys []string `json:"concurrency_keys,omitempty"`
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
	// ConcurrencyKeys are the job's concurrency keys (F-16): the union of the
	// job's own keys (from its spec) and its pipeline's keys, denormalized
	// onto the job instance when it is created (like the spec and needs). The
	// scheduler's admission gate (F-16) holds these keys (via the Database
	// service's slot accounting) while the job is admitted and releases them
	// when the job reaches a terminal state. Empty means the job holds no
	// concurrency keys.
	ConcurrencyKeys []string `gorm:"type:text;serializer:json" json:"concurrency_keys,omitempty"`
	// ConcurrencyAdmitted reports whether the job has been admitted (its
	// concurrency keys and group/global slots acquired, F-16) for its current
	// attempt. It is set when the scheduler dispatches the job and cleared
	// when the job reaches a terminal state (or is reset for a retry). It is
	// denormalized onto the job so the scheduler's admission loop can tell a
	// pending job that is waiting for a free slot apart from one that has
	// already been dispatched (and must not be dispatched again).
	ConcurrencyAdmitted bool `json:"concurrency_admitted"`
	// ConcurrencyAdmittedAt is when the job was admitted (F-16); nil when the
	// job is not admitted. It is recorded so the scheduler's admission loop
	// can detect a leaked slot (a job admitted long ago that never reached a
	// terminal state, e.g. a target that died without reporting) and release
	// it, freeing the resource for other jobs.
	ConcurrencyAdmittedAt *time.Time `json:"concurrency_admitted_at,omitempty"`
	// Variables are the job's effective variables (F-21.1.3): the union of the
	// job's inline variables and the variables of the variable sets it
	// references, with the job's inline variables winning on a key collision.
	// They are computed at import time (the API merges the job's inline
	// variables with the referenced sets' variables) and denormalized onto the
	// job instance at run creation, so they are stored as a snapshot:
	// relabeling a set later does not change the variables recorded on a past
	// run. A variable's value is a plain string, an encrypted builtin F-12
	// ciphertext, or a reference to a named secret source; the API resolves
	// each to a concrete value at dispatch and hands the plaintext to the
	// execution target, where a step references it as `{{ .params.<name> }}`.
	// They are stored as a JSON document in a text column (portable across
	// SQLite and PostgreSQL), like RunParams. Empty means the job has no
	// variables.
	Variables []Variable `gorm:"type:text;serializer:json" json:"variables,omitempty"`
	// VaultImports are the job's every-key vault imports (F-21.4): the union of
	// the every-key vault imports of the variable sets the job references. They
	// are denormalized onto the job instance at run creation (a snapshot, like
	// Variables) and expanded at dispatch (the API fetches each vault and
	// creates one variable per key). Empty means the job has no every-key vault
	// imports.
	VaultImports []VaultImport `gorm:"type:text;serializer:json" json:"vault_imports,omitempty"`
	// Artifacts is the run's RESOLVED package snapshot (F-21.6) denormalized
	// onto the job instance at run creation: a spec.artifacts reference name →
	// the stored file name the reference resolved to when the run started. The
	// API hands it to the execution target so the download_artifact step
	// handler can resolve a reference to the version the run picked. Empty for
	// a job not part of a run (or a run whose pipeline declares no
	// spec.artifacts).
	Artifacts map[string]string `gorm:"type:text;serializer:json" json:"artifacts,omitempty"`
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
	// Environment is the name of the environment this job targets (F-17);
	// empty means the job targets its raw TargetGroup (the pre-F-17 behavior).
	// A job targeting an environment is dispatched to the live workers that
	// registered that environment (intersected with the environment's
	// target_group when set), and is subject to the environment's approval and
	// automation policies. It is denormalized from the job definition onto the
	// job instance when it is created.
	Environment string `gorm:"index" json:"environment,omitempty"`
	// Automation is the job's own automation setting (F-17): automatic or
	// manual. It is consulted only when the job's environment has no automation
	// policy configured (both are valid in that case — the job decides); when
	// the environment has a policy it is authoritative and overrides this.
	// Empty means the job sets no automation of its own.
	Automation Automation `json:"automation,omitempty"`
	// Approval is the job's override of its environment's approval policy
	// (F-17): none (skip the gate even if the environment requires one) or
	// required (insert a gate even if the environment does not require one).
	// Empty means the job defers to the environment's requires_approval flag.
	Approval ApprovalPolicy `json:"approval,omitempty"`
	// Tags are the job's combined tags (F-29): the union of the tags of every
	// resource in the job's context — its pipeline, the job itself, the
	// environment it targets (if any), and the worker group it targets (if
	// any) — with the more specific source winning on a key collision. They
	// are computed when the job instance is created (a run's job instances are
	// created by createRunAndInstances) and stored as a snapshot: relabeling a
	// resource later does not change the tags recorded on a past run. A job
	// that is not part of a run (a standalone job) carries only the tags it is
	// given directly. They are metadata (they never gate, route, or serialize
	// work) and are stored normalized in the Tag table (owner_type "job"), not
	// as a column on this row. The field is in-memory only (gorm:"-"). Empty
	// means the job has no tags.
	Tags map[string]string `gorm:"-" json:"tags,omitempty"`
}

// ConcurrencyResource is the durable, atomic slot accounting for one
// concurrency resource (F-16). A resource is a concurrency key, or the
// synthetic "group:<name>" resource (a worker group's cap) or "global"
// resource (the system-wide cap). Held is the number of jobs currently
// holding a slot on the resource. The Database service enforces the resource's
// limit by atomically incrementing Held (only while Held is below the limit)
// when a job is admitted and decrementing it when the job is released: the
// conditional update locks the resource row, so multiple scheduler replicas
// can never double-allocate the last free slot. One row per resource; the
// resource's limit is resolved from the ConcurrencyLimit rules (not stored
// here).
type ConcurrencyResource struct {
	ID uint `gorm:"primaryKey"`
	// Key is the concurrency resource: a concurrency key name, or the
	// synthetic "group:<name>" / "global" resource.
	Key string `gorm:"uniqueIndex;size:255;not null" json:"key"`
	// Held is the number of jobs currently holding a slot on the resource.
	Held int `gorm:"not null;default:0" json:"held"`
}

// ConcurrencyLimit is a concurrency limit rule (F-16): a named, regex-based
// cap on how many jobs may run concurrently against one dimension. A rule has
// a Kind (which dimension it bounds: "global" the system-wide cap, "group" a
// worker group's cap, or "key" a concurrency key's cap), a Pattern (a regular
// expression matched against the dimension's name — a worker group's name or a
// concurrency key's name; empty for the single "global" resource), and a Limit
// (the cap on concurrent jobs; 0 means unlimited).
//
// A concrete resource's effective limit is resolved by matching its name
// against every rule of the matching kind: the most specific (longest)
// matching pattern wins, and a resource that matches no rule falls back to a
// default (a concurrency key defaults to a limit of 1 — a lock; a group or the
// global resource defaults to unlimited). The rules live in the database (not
// configuration) so they can be edited at runtime via the API without a
// redeployment.
type ConcurrencyLimit struct {
	gorm.Model
	// Kind is the dimension the rule bounds: "global", "group", or "key".
	Kind string `gorm:"size:16;not null;index" json:"kind"`
	// Pattern is a regular expression matched against the dimension's name (a
	// worker group's name or a concurrency key's name). It is empty for the
	// single "global" resource. It is validated (compiled) when the rule is
	// saved, so a rule can never be stored that could not match.
	Pattern string `gorm:"size:255;not null" json:"pattern"`
	// Limit is the cap on the number of jobs running concurrently against the
	// resource(s) the rule matches. A limit of 0 means unlimited.
	Limit int `gorm:"not null;default:0" json:"limit"`
}

// SSHKnownHost is a runtime-managed SSH host key (F-21.2.4.1): a known_hosts
// line (the OpenSSH format — a host pattern, a key type, and a base64-encoded
// public key) that the source-fetcher uses to verify a git-ssh server's host
// key. The keys live in the database (owned by the Database service) and are
// managed through the API (GET/POST/DELETE /api/ssh-known-hosts), so an admin
// can add a key for a self-hosted or corporate-firewalled git host at runtime
// (via the UI) without a redeployment and without editing the fetcher's
// built-in common provider keys. The API passes the accumulated keys to the
// fetcher on each git-ssh fetch (FetchSourceRequest.known_hosts); the fetcher
// combines them with its built-in common keys for host-key verification.
type SSHKnownHost struct {
	gorm.Model
	// HostPattern is the known_hosts host pattern (e.g. "git.example.com",
	// "[git.example.com]:2222", or a wildcard). It is the unique key of the
	// row (a host has one stored key line).
	HostPattern string `gorm:"size:255;not null;uniqueIndex" json:"host_pattern"`
	// KeyType is the SSH public key type (e.g. "ssh-ed25519", "ssh-rsa",
	// "ecdsa-sha2-nistp256").
	KeyType string `gorm:"size:64;not null" json:"key_type"`
	// Key is the base64-encoded public key (the third field of a known_hosts
	// line).
	Key string `gorm:"size:512;not null" json:"key"`
	// Description is an optional human-readable note (e.g. "corporate git").
	Description string `gorm:"size:255" json:"description,omitempty"`
}

// HTTPHeader is one header an http source credential sends on its fetch
// (F-21.3): a name and a value, plus a per-header `sensitive` flag. A header
// marked sensitive carries secret material (encrypted at rest with the API's
// F-12 store, redacted on read); a non-sensitive header is stored in plaintext
// and returned as-is, so it is easily viewed and modified.
type HTTPHeader struct {
	// Name is the header name (e.g. "Authorization"). It is not secret
	// material and is returned to the UI.
	Name string `json:"name"`
	// Value is the header value (e.g. "Bearer <token>"). It carries the secret
	// when Sensitive is true (stored as F-12 ciphertext and masked on read);
	// otherwise it is stored in plaintext and returned as-is.
	Value string `json:"value"`
	// Sensitive marks the header value as secret material (encrypted at rest,
	// masked on read). It defaults to false, so a non-sensitive header is
	// stored in plaintext and returned as-is.
	Sensitive bool `json:"sensitive"`
}

// SourceCredential is a named, database-stored credential that authenticates a
// fetch of a private source (F-21.3): a private git repository (git-ssh /
// git-https) or an HTTP/HTTPS endpoint that requires a token (http). It is
// matched to a source by the longest matching prefix (or an explicit
// credential name on the source), and its secret material is encrypted at rest
// with the API's secret store (F-12) when marked sensitive. The Database
// service stores the secret material opaquely (ciphertext or plaintext) and
// never sees the key; the API is the only component that can decrypt it. The
// source-fetcher (F-21.2) holds no credentials at rest — the API decrypts the
// chosen credential for a single fetch and hands the plaintext to the fetcher.
type SourceCredential struct {
	gorm.Model
	// Name is the unique credential name. It is the value a
	// PipelineSource.credential references (an explicit override of the
	// prefix match).
	Name string `gorm:"size:128;not null;uniqueIndex" json:"name"`
	// Kind is the credential kind: "git-ssh", "git-https", or "http".
	Kind string `gorm:"size:16;not null;index" json:"kind"`
	// Prefix is a string prefix matched against a source's reference (the
	// repository URL for a git source, the endpoint URL for an http source).
	// When a fetch needs a credential, the source's reference is matched
	// against every credential's prefix; the longest matching prefix wins. A
	// prefix match is a plain string prefix (not a regex), so a "." in a
	// hostname need not be escaped.
	Prefix string `gorm:"size:512;not null" json:"prefix"`
	// Description is an optional human-readable note.
	Description string `gorm:"size:255" json:"description,omitempty"`
	// Username is the git-https credential's username (may be empty for
	// token-only auth). It is not secret material and is returned to the UI.
	Username string `gorm:"size:255" json:"username,omitempty"`
	// Token is the git-https / http credential's token (a personal access
	// token, bot password, or API key). It is secret material: stored as F-12
	// ciphertext and never returned to the UI.
	Token string `gorm:"size:1024" json:"-"`
	// PrivateKey is the git-ssh credential's PEM-encoded private key. It is
	// secret material: stored as F-12 ciphertext and never returned to the UI.
	PrivateKey string `gorm:"type:text" json:"-"`
	// HostKey is the git-ssh credential's expected host public key (for
	// host-key verification so a fetch is not vulnerable to a MITM). It is
	// secret material: stored as F-12 ciphertext and never returned to the UI.
	HostKey string `gorm:"size:512" json:"-"`
	// Headers is the http credential's list of headers (name + value + a
	// per-header sensitive flag). Each header's value is secret material only
	// when its own Sensitive flag is true (stored as F-12 ciphertext and
	// masked on read); otherwise it is stored in plaintext and returned as-is.
	// Stored as a JSON column (the list is a value, not a separate table).
	Headers []HTTPHeader `gorm:"type:text;serializer:json" json:"headers,omitempty"`
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
	// StepName is the step's name (the step that was completed).
	StepName string `gorm:"size:255;uniqueIndex:idx_step_completion_unique,priority:3;not null" json:"step_name"`
	// Attempt is the job attempt this completion belongs to (F-04); a retry
	// starts a fresh set of completions.
	Attempt int `gorm:"uniqueIndex:idx_step_completion_unique,priority:4" json:"attempt"`
}

// StepOnceClaim is the durable "run this step" slot for a step of a job that
// fans out to a worker group (F-01). A step with `once` set runs on at most
// one target: the first target to claim (job id + step name + attempt) creates
// this row and runs the step; every other target that reaches the same step in
// the same attempt finds the row already claimed and skips the step. A retry
// (F-04) is a new attempt, so it claims a fresh slot. The unique index on
// (job_id, step_name, attempt) is what makes the claim atomic: at most one
// concurrent caller can create the row, so the dedup is correct across every
// API replica and worker process.
type StepOnceClaim struct {
	gorm.Model
	// JobID is the job the step belongs to.
	JobID uint `gorm:"uniqueIndex:idx_step_once_claim_unique,priority:1;not null" json:"job_id"`
	// StepName is the step's name (its identity in the job's spec).
	StepName string `gorm:"size:255;uniqueIndex:idx_step_once_claim_unique,priority:2;not null" json:"step_name"`
	// Attempt is the job attempt this claim belongs to (F-04); a retry claims
	// a fresh slot.
	Attempt int `gorm:"uniqueIndex:idx_step_once_claim_unique,priority:3" json:"attempt"`
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
	// Tags are this worker's execution's combined tags (F-29): the job
	// instance's base tag set (pipeline + job + environment + worker group)
	// merged with the tags of the worker that ran this execution (the worker's
	// tags win on a key collision). They are set when the worker starts the
	// execution (StartJobExecution), so a fan-out job's each-worker execution
	// carries that worker's own tags. They are metadata and are stored
	// normalized in the Tag table (owner_type "job_execution", owner_id the
	// execution's id), not as a column on this row. The field is in-memory
	// only (gorm:"-"). Empty means the execution has no tags.
	Tags map[string]string `gorm:"-" json:"tags,omitempty"`
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
	// Environments are the names of the environments this worker belongs to
	// (F-17). Membership is many-to-many: a worker may list several
	// environments (a `prod-auto` worker can also serve `staging`), and an
	// environment can be served by many workers. A job targeting environment
	// E is dispatched to the live workers that registered E. A worker that
	// belongs to no environment is reachable only by its raw Group (the
	// pre-F-17 behavior). It is stored as a JSON document in a text column
	// (portable across SQLite and PostgreSQL), like the other list fields.
	Environments []string `gorm:"type:text;serializer:json" json:"environments,omitempty"`
	// Tags are the worker's tags (F-29): a set of key/value pairs that label
	// the worker. A worker declares them at registration (configurable, like
	// its environments, F-17); re-registration updates them. When a job is
	// dispatched to the worker, the worker's tags are merged into the job's
	// execution's tag set (the worker's tags win on a key collision). They
	// are metadata (they never gate, route, or serialize work) and are stored
	// normalized in the Tag table (owner_type "worker"), not as a column on
	// this row. The field is in-memory only (gorm:"-"). Empty means the worker
	// has no tags.
	Tags map[string]string `gorm:"-" json:"tags,omitempty"`
}

// Environment is a named deployment target that groups execution targets and
// carries the deployment policy that governs how work reaches it (F-17). An
// environment is the unit of "where does this run, and how strictly is it
// gated": it names the workers that serve it (via worker membership and an
// optional target group) and carries two gates — an approval policy
// (RequiresApproval, an F-13 approval gate) and an automation policy
// (Automation, automatic/manual) that decides whether a run proceeds on its
// own or pauses for an explicit proceed call.
//
// Environments are stored in the database (owned by the Database service) and
// managed through the API (CRUD), so an admin can create `prod-auto`, flip
// `prod` to manual, or add a worker to an environment at runtime without a
// redeployment.
type Environment struct {
	gorm.Model
	// Name is the environment's unique name (e.g. "dev", "staging", "prod",
	// "prod-auto").
	Name string `gorm:"uniqueIndex;not null" json:"name"`
	// Description is a human-readable description of the environment.
	Description string `json:"description"`
	// TargetGroup is the worker group that serves the environment; empty means
	// the environment is served by ephemeral agents (or, when a worker
	// declares the environment, by the workers that registered it). A job
	// targeting the environment is dispatched to the live workers that
	// registered it, intersected with this group when one is set.
	TargetGroup string `gorm:"column:target_group;index" json:"target_group"`
	// RequiresApproval, when true, inserts an F-13 approval gate before a job
	// targeting this environment is dispatched (a job's approval override
	// takes precedence over this flag, see Job.Approval).
	RequiresApproval bool `gorm:"not null;default:false" json:"requires_approval"`
	// Automation is the environment's automation policy (F-17): automatic
	// (a run proceeds on its own once its triggers and dependencies are
	// satisfied) or manual (a run pauses and waits for an explicit proceed
	// call). It is authoritative when set: a job's own automation setting is
	// consulted only when the environment has none configured (an empty value
	// means the environment sets no policy, so the job's own setting — or,
	// when the job sets none, automatic — decides). A manual environment
	// overrides any trigger (cron/webhook/event) and the previous environment,
	// holding the run until someone proceeds it.
	Automation Automation `gorm:"not null" json:"automation"`
	// Tags are the environment's tags (F-29): a set of key/value pairs that
	// label the environment. They are managed through the API (the
	// POST/PUT /api/environments[/{name}] body) and stored in the database, so
	// the UI can label an environment without a redeployment. When a job
	// targets the environment, the environment's tags are combined with the
	// tags of the run's other resources onto the run's job instances. They are
	// metadata (they never gate, route, or serialize work) and are stored
	// normalized in the Tag table (owner_type "environment"), not as a column
	// on this row. The field is in-memory only (gorm:"-"). Empty means the
	// environment has no tags.
	Tags map[string]string `gorm:"-" json:"tags,omitempty"`
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
	// APIKeyFailedCount is the number of consecutive failed API-key
	// verification attempts for this user (F-25). It is reset to zero on a
	// successful verification and is separate from any password-attempt
	// lockout, so brute-forcing API keys does not lock a user out of password
	// sign-in (and vice versa).
	APIKeyFailedCount int `gorm:"column:api_key_failed_count;not null;default:0" json:"-"`
	// APIKeyLockedUntil is when the user's API-key lockout expires (F-25);
	// zero means the user is not locked out. When the configured lockout
	// duration is zero the lockout is permanent (until an admin resets it).
	APIKeyLockedUntil time.Time `gorm:"column:api_key_locked_until" json:"-"`
}

// IDPAPIKey is an API key owned by a user (F-25): a long-lived credential that
// authenticates the user to the API in place of a JWT. A key is a string
// `cdrom-` followed by 64 random alphanumeric characters; only its hash is
// stored (the plaintext is shown to the user exactly once, at creation and at
// each rotation). A key carries a description, an expiration date (at most
// one year in the future), and a pipeline scope (a set of pipeline IDs; an
// empty set means all pipelines). The key's effective permissions are the
// owner's own permissions limited to the key's pipeline scope.
//
// The key directory (hashes, metadata, and the owner's lockout state) is
// stored in the database (like the IdP's user directory) so multiple IdP
// replicas share it. The Database service stores the key hash opaquely and
// never sees the plaintext; the IdP generates and hashes the secret.
type IDPAPIKey struct {
	gorm.Model
	// OwnerID is the user (IDPUser.ID) the key belongs to.
	OwnerID uint `gorm:"index;not null" json:"owner_id"`
	// Description is a human-readable label for the key.
	Description string `json:"description"`
	// KeyHash is the SHA-512 hash of the key's plaintext (optionally mixed
	// with a configured pepper). The plaintext is never persisted.
	KeyHash string `gorm:"column:key_hash;type:text;not null" json:"-"`
	// KeyPrefix is a short, non-secret prefix of the key (e.g. the first
	// dozen characters) shown in listings so a user can tell keys apart
	// without the plaintext.
	KeyPrefix string `gorm:"column:key_prefix" json:"key_prefix"`
	// ExpiresAt is when the key stops working; zero means the key never
	// expires. It is validated to be at most one year in the future.
	ExpiresAt time.Time `json:"expires_at"`
	// PipelineScope is the set of pipeline IDs the key is limited to; an empty
	// set means the key is not limited (it can act on any pipeline the owner
	// can). It is stored as a JSON document in a text column (portable across
	// SQLite and PostgreSQL).
	PipelineScope []uint `gorm:"type:text;serializer:json" json:"pipeline_scope,omitempty"`
}

// ServiceAccount is a non-human identity for automation (F-26), independent
// of any user's login or employment lifecycle. It has assignable F-14 roles
// and exactly two API-key slots (ServiceAccountKey), both usable concurrently
// and individually rotatable. The IdP generates and hashes the keys; only the
// (salted) hashes are stored. An account can be temporarily disabled to stop
// access, or permanently soft-deleted (DeletedAt), which retains its identity
// while zeroing out both key slots.
//
// The account directory is stored in the database (like the IdP's user
// directory) so multiple IdP replicas share it. The Database service stores
// the key hashes opaquely and never sees the plaintext; the IdP generates and
// hashes the secrets.
type ServiceAccount struct {
	gorm.Model
	// LoginName is the account's unique, immutable login identifier. It is
	// separate from the human email namespace and is the identity a client
	// presents in `Authorization: Bearer <login-name>:cdrom-sa-…`.
	LoginName string `gorm:"uniqueIndex;not null" json:"login_name"`
	// DisplayName is a human-readable name for the account.
	DisplayName string `json:"display_name"`
	// Description is a human-readable description of the account.
	Description string `json:"description"`
	// Disabled, when true, rejects both of the account's keys (and any access)
	// while preserving the hashes and role bindings. Enabling restores access
	// with the same keys.
	Disabled bool `gorm:"not null;default:false" json:"disabled"`
	// DeletedAt is the irreversible tombstone (a soft delete, not a hard
	// delete): once set, the account is permanently deleted — its identity,
	// role history, and audit references are retained for attribution, but
	// both key slots are zeroed and no key can authenticate. The login name
	// stays reserved so a new identity cannot impersonate the deleted one.
	DeletedAt *time.Time `gorm:"index" json:"deleted_at,omitempty"`
	// CreatedBy is the identity of the principal that created the account.
	CreatedBy string `json:"created_by,omitempty"`
	// UpdatedBy is the identity of the principal that last mutated the
	// account.
	UpdatedBy string `json:"updated_by,omitempty"`
	// Revision is a concurrency guard for mutations: it is incremented on
	// every mutation, and a mutation that carries a stale revision is rejected
	// (so a conflicting rotation or lifecycle change is not silently
	// overwritten).
	Revision int `gorm:"not null;default:0" json:"revision"`
	// KeyFailedCount is the number of consecutive failed service-account key
	// verification attempts for this account (F-26, F-25-style throttling). It
	// is reset to zero on a successful verification and is separate from any
	// human user's lockout, so brute-forcing a service account's keys does not
	// lock a human out of password sign-in (and vice versa).
	KeyFailedCount int `gorm:"column:key_failed_count;not null;default:0" json:"-"`
	// KeyLockedUntil is when the account's key lockout expires (F-26); zero
	// means the account is not locked out. When the configured lockout duration
	// is zero the lockout is permanent (until an admin resets it).
	KeyLockedUntil time.Time `gorm:"column:key_locked_until" json:"-"`
}

// ServiceAccountKey is one of a service account's two API-key slots (F-26).
// A key is a string `cdrom-sa-` followed by 64 random alphanumeric characters;
// only its salted hash (and a short non-secret prefix) is stored — the
// plaintext is shown to the caller exactly once (at creation and at each
// rotation) and is never persisted.
//
// Every account retains exactly two slot records (Slot 1 and 2); callers
// cannot add a third or delete one. A slot's hash is a salted hash: the salt
// is unique to the key (generated with the key) and is stored alongside the
// hash, so two keys with the same plaintext (or two keys of the same account)
// never collide. When a pepper is configured it is mixed in as well (kept out
// of the database). On delete, both slots' hashes and prefixes are cleared so
// no usable verifier remains; an empty hash never matches a credential.
type ServiceAccountKey struct {
	gorm.Model
	// ServiceAccountID is the account this key slot belongs to.
	ServiceAccountID uint `gorm:"uniqueIndex:idx_sa_key_unique,priority:1;not null" json:"service_account_id"`
	// Slot is the key's slot number: 1 or 2. It is unique within the account
	// (the composite unique index with ServiceAccountID).
	Slot int `gorm:"uniqueIndex:idx_sa_key_unique,priority:2;not null" json:"slot"`
	// KeyHash is the salted hash of the key's plaintext (optionally mixed with
	// a configured pepper). It is never returned to the API, IdP, or UI. On
	// delete it is cleared to the empty value, which never matches a
	// credential.
	KeyHash string `gorm:"column:key_hash;type:text" json:"-"`
	// KeySalt is the per-key salt mixed into KeyHash. It is unique to the key
	// (generated with the key) and is stored alongside the hash. On delete it
	// is cleared.
	KeySalt string `gorm:"column:key_salt;type:text" json:"-"`
	// KeyPrefix is a short, non-secret prefix of the key (e.g. the first dozen
	// characters) shown in listings so a caller can tell keys apart without
	// the plaintext. On delete it is cleared.
	KeyPrefix string `gorm:"column:key_prefix" json:"key_prefix"`
	// Generation is a counter bumped on each rotation, so the UI can show how
	// many times a slot's key has been rotated.
	Generation int `gorm:"not null;default:1" json:"generation"`
	// RotatedAt is when the slot's key was last generated or rotated.
	RotatedAt time.Time `json:"rotated_at"`
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
	// Environment is the environment the event is relevant to (F-17); set for
	// assignment events of a job that targets an environment, so an API pod can
	// nudge its local workers that belong to it. Empty otherwise.
	Environment string `gorm:"index" json:"environment"`
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

// WorkerGroup is a named group of long-lived workers (F-29). A worker group
// is the unit a job targets (a job's TargetGroup names it) and the unit a
// worker belongs to (a worker's Group names it). Before F-29 a group was only
// an implicit string (a worker's Group value); F-29 makes it a first-class,
// taggable row so a group can carry tags (F-29) that are combined onto the
// runs of the jobs that target it. The row is metadata-only: it does not
// change dispatch (which still keys off the worker's declared Group string),
// so a group that has no row simply contributes no tags and jobs targeting it
// still run on its workers.
type WorkerGroup struct {
	gorm.Model
	// Name is the group's unique name (e.g. "build", "prod-fleet"); it is
	// the value a worker's Group field and a job's TargetGroup reference.
	Name string `gorm:"uniqueIndex;not null" json:"name"`
	// Description is a human-readable description of the group.
	Description string `json:"description"`
	// Tags are the group's tags (F-29): a set of key/value pairs that label
	// the group. They are managed through the API (the
	// PUT /api/worker-groups/{name} body) and stored in the database, so the
	// UI can label a group without a redeployment. When a job targets the
	// group, the group's tags are combined with the tags of the run's other
	// resources onto the run's job instances. They are metadata (they never
	// gate, route, or serialize work) and are stored normalized in the Tag
	// table (owner_type "worker_group"), not as a column on this row. The
	// field is in-memory only (gorm:"-"). Empty means the group has no tags.
	Tags map[string]string `gorm:"-" json:"tags,omitempty"`
}

// Tag is a single key/value pair that labels a resource (F-29). Tags are
// normalized into their own table — one row per key/value pair — rather than
// stored as a JSON document on the parent row, so a resource can be searched
// and filtered by tag key and/or value with a plain indexed SQL query (a JSON
// column cannot be queried that way). A tag belongs to exactly one parent
// resource, identified by its OwnerType (the resource kind) and OwnerID (the
// resource's id). The parent's in-memory Tags map (marked gorm:"-" so it is
// not a column) is populated from this table on load and written to it on
// save by the database service.
//
// The model does not embed gorm.Model (and so has no soft-delete column): a
// tag row is replaced by deleting its old rows and inserting new ones, and a
// soft delete would leave the deleted rows in the table and break the unique
// index on (owner_type, owner_id, key) the next time the same key is set. The
// timestamps are declared explicitly instead.
type Tag struct {
	ID uint `gorm:"primaryKey" json:"id"`
	// OwnerType is the kind of resource the tag belongs to (one of the
	// TagOwner* constants).
	OwnerType string `gorm:"size:32;not null;uniqueIndex:idx_tag_owner_key,priority:1" json:"owner_type"`
	// OwnerID is the id of the resource the tag belongs to.
	OwnerID uint `gorm:"not null;uniqueIndex:idx_tag_owner_key,priority:2" json:"owner_id"`
	// Key is the tag's key. It is unique within a parent resource (the
	// composite unique index with OwnerType and OwnerID), so a resource has at
	// most one value per key.
	Key string `gorm:"size:255;not null;uniqueIndex:idx_tag_owner_key,priority:3;index:idx_tag_key_value,priority:1" json:"key"`
	// Value is the tag's value.
	Value     string    `gorm:"size:255;not null;index:idx_tag_key_value,priority:2" json:"value"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Owner-type constants for the Tag model (F-29): the resource kinds that can
// carry tags. Each is distinct so a tag row's (owner_type, owner_id) pair
// identifies exactly one parent resource (a pipeline version and a job
// execution have their own types rather than reusing "pipeline" / "job", so a
// version's id can never be mistaken for a pipeline's id, and an execution's
// id for a job's id).
const (
	TagOwnerPipeline        = "pipeline"
	TagOwnerPipelineVersion = "pipeline_version"
	TagOwnerJob             = "job"
	TagOwnerJobExecution    = "job_execution"
	TagOwnerEnvironment     = "environment"
	TagOwnerWorker          = "worker"
	TagOwnerWorkerGroup     = "worker_group"
)

// StorageObject is the database's record of a file held by the artifacts
// service (F-21.6 storage redesign). The artifacts service is a dumb blob
// store keyed by the record's UID; what a file IS — its kind, namespace,
// display name, size, content hash, package coordinates, provenance, and
// activity counters — is THIS ROW.
//
// Lifecycle is two-phase: AllocateStorageObject inserts a PROVISIONAL row
// (Confirmed=false) reserved for an inbound upload; ConfirmStorageObject
// settles it (size, measured hash, Confirmed=true, CreatedAt refreshed to
// the settlement instant). Provisional rows are invisible to Get/List/
// Resolve; allocating over a (namespace, name) that already HAS a row
// adopts the provisional one (crash recovery) and conflicts with a settled
// one (immutability). At most one row ever occupies a (namespace, name) —
// hence the plain unique index.
//
// The model does NOT embed gorm.Model (and so has HARD deletes, no
// soft-delete column): a soft-deleted row would STILL OCCUPY the unique
// (namespace, name) index, making a republication of the same name
// impossible (the Tag model sidestepped the same hazard, F-29). Timestamps
// are declared explicitly; CreatedAt is the SETTLEMENT instant (refreshed
// by Confirm), not the allocation time.
type StorageObject struct {
	ID uint `gorm:"primaryKey" json:"id"`
	// UID is the record's globally-unique id (a UUID v4 string). It doubles
	// as the artifacts service's blob key: the file's bytes sit at blob(UID).
	UID string `gorm:"column:uid;size:36;uniqueIndex;not null" json:"uid"`
	// Kind is the file's kind: "artifact" (immutable) or "log" (grown by
	// appends). See the StorageObjKind* constants.
	Kind string `gorm:"size:16;not null;index" json:"kind"`
	// Namespace is the logical scope the file belongs to (a job id for a
	// job's artifacts/logs, a run id for a run's files, "packages" for the
	// registry). Free-form; the database is agnostic about its meaning.
	Namespace string `gorm:"size:255;not null;uniqueIndex:idx_storage_obj_coord,index:idx_storage_obj_pkg,priority:1" json:"namespace"`
	// Name is the file's display name within the namespace. Together with
	// Namespace it is unique (among all rows, provisionals included).
	Name string `gorm:"size:512;not null;uniqueIndex:idx_storage_obj_coord" json:"name"`
	// Size is the file's size in bytes (settled at confirm; tracked for
	// logs as they grow).
	Size int64 `json:"size"`
	// ContentSHA256 is the SHA-256 (lowercase hex) of the file's content,
	// MEASURED by the artifacts service at store time. Empty for logs
	// (still-growing; hashing them on every append taxes the tail path).
	ContentSHA256 string `gorm:"column:content_sha256;size:64" json:"content_sha256,omitempty"`
	// IsPackage marks a registry filing (namespace "packages"): the
	// package-coordinate fields are populated for it. Indexed jointly with
	// Namespace for resolution queries.
	IsPackage bool `gorm:"not null;default:false;index:idx_storage_obj_pkg,priority:2" json:"is_package"`
	// PackageName is the package's name (registry filings only). Indexed for
	// resolution.
	PackageName string `gorm:"column:package_name;size:255;index:idx_storage_obj_pkg_ver" json:"package_name"`
	// Version is the package version (registry filings only).
	Version string `gorm:"column:version;size:255;index:idx_storage_obj_pkg_ver,priority:2" json:"version"`
	// IsPreRelease is the CHANNEL stamp set AT PUBLISH TIME (explicit, from
	// the publisher's declaration) — never derived from the version string.
	// Resolution excludes flagged records from the release channel unless
	// opted in.
	IsPreRelease bool `gorm:"column:is_pre_release;not null;default:false;index" json:"is_pre_release"`
	// UploadedBy is the actor that stored the file (a user id, an api-key
	// owner, a service account, or "job:<id>" for machine publications).
	UploadedBy string `gorm:"column:uploaded_by;size:255" json:"uploaded_by,omitempty"`
	// CreatedAt is the record's VISIBILITY instant: for a settled record it
	// is the settlement time (when the file appeared); for a provisional one
	// it is merely the reservation time (invisible anyway). Explicit (the
	// model does not embed gorm.Model — see the type comment).
	CreatedAt time.Time `json:"created_at"`
	// UpdatedAt is the last modification of the row (analytic counters, log
	// size syncs…).
	UpdatedAt time.Time `json:"updated_at"`
	// LastDownloadedAt is the most recent retrieval (analytics; touched
	// best-effort). Zero means never downloaded.
	LastDownloadedAt time.Time `gorm:"column:last_downloaded_at" json:"last_downloaded_at,omitempty"`
	// DownloadCount is the cumulative retrievals (analytics; touched
	// best-effort).
	DownloadCount int64 `gorm:"column:download_count;not null;default:0" json:"download_count"`
	// Extras is a forward-compatible attribute bag (opaque JSON) for
	// metadata that deserves no column yet.
	Extras string `gorm:"type:text" json:"extras,omitempty"`
	// Confirmed distinguishes SETTLED records (visible to listings and
	// resolution) from PROVISIONAL reservations awaiting their upload's
	// confirm.
	Confirmed bool `gorm:"not null;default:false;index" json:"confirmed"`
}

// Kinds of storage object (StorageObject.Kind).
const (
	StorageObjKindArtifact = "artifact"
	StorageObjKindLog      = "log"
)

// TableName pins the storage object's table name.
func (StorageObject) TableName() string { return "storage_objects" }

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
		&StepOnceClaim{},
		&Worker{},
		&WorkerGroup{},
		&IDPSigningKey{},
		&IDPAuthCode{},
		&IDPUser{},
		&IDPAPIKey{},
		&ServiceAccount{},
		&ServiceAccountKey{},
		&Event{},
		&Lease{},
		&SecretNonce{},
		&Role{},
		&RoleBinding{},
		&AuditEvent{},
		&ConcurrencyResource{},
		&ConcurrencyLimit{},
		&Environment{},
		&Tag{},
		&SSHKnownHost{},
		&SourceCredential{},
		&PipelineSource{},
		&PipelineSourceState{},
		&StorageObject{},
	}
}
