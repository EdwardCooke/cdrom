package api

import (
	"fmt"
	"net/http"
	"sort"

	"gopkg.in/yaml.v3"

	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/pipelinefile"
)

// exportPipeline renders a pipeline's current compiled definition back into
// the file format (F-21.1.4 export): the inverse of the import compiler
// (pipelineFileToRequest). The exported document carries the pipeline's
// metadata (name, description, labels), its deployment options (failureMode,
// concurrencyKeys, params, triggers), its named variable sets and secret
// sources, and its jobs (each with its steps, deployment options, and
// variables). The round-trip property holds: the exported file, re-imported
// through the compiler, yields the same compiled definition (the jobs,
// variable sets, secret sources, params, triggers, and failure mode are all
// reproduced; a pipeline-level workerGroup is a file-authoring convenience
// that is not stored in the schema, so it is not reproduced).
func (s *Server) exportPipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.requirePermission(w, r, authz.PermPipelinesView, authz.Resource{PipelineID: pipelineID}) {
		return
	}
	pipeline, err := s.clients.Database.GetPipeline(r.Context(), &dbpb.GetPipelineRequest{Id: pipelineID})
	if err != nil {
		grpcError(w, err)
		return
	}
	doc := pipelineToDocument(pipeline)
	data, err := yaml.Marshal(doc)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "render export: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// pipelineToDocument builds the file-format document (a pipelinefile.Value
// tree) for a pipeline's compiled definition: the manifest envelope
// (apiVersion, kind, metadata) plus the spec (the pipeline's deployment
// options, named variable sets and secret sources, and its jobs). It is the
// inverse of pipelineFileToRequest on the fields the file can express.
func pipelineToDocument(pipeline *dbpb.Pipeline) pipelinefile.Value {
	spec := pipelinefile.Value{Type: pipelinefile.Map}
	// The pipeline's default failure mode (F-01); UNSPECIFIED means the
	// default (ALL) and is omitted.
	if fm := pipeline.GetFailureMode(); fm != dbpb.FailureMode_FAILURE_MODE_UNSPECIFIED {
		spec.Entries = append(spec.Entries, pipelinefile.Entry{Key: "failureMode", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: failureModeExportName(fm)}})
	}
	if keys := pipeline.GetConcurrencyKeys(); len(keys) > 0 {
		spec.Entries = append(spec.Entries, pipelinefile.Entry{Key: "concurrencyKeys", Value: stringListValue(keys)})
	}
	if params := pipeline.GetParams(); len(params) > 0 {
		spec.Entries = append(spec.Entries, pipelinefile.Entry{Key: "params", Value: exportParams(params)})
	}
	if triggers := pipeline.GetTriggers(); len(triggers) > 0 {
		spec.Entries = append(spec.Entries, pipelinefile.Entry{Key: "triggers", Value: exportTriggers(triggers)})
	}
	// The file-declared periodic-refresh policy (F-21.5.1): emitted only when
	// the pipeline actually carries one (a UI-authored pipeline synthesizes
	// no periodicRefresh block).
	if iv := pipeline.GetPeriodicRefreshInterval(); iv != "" {
		spec.Entries = append(spec.Entries, pipelinefile.Entry{Key: "periodicRefresh", Value: exportPeriodicRefresh(iv, pipeline.GetPeriodicRefreshSchedule())})
	} else if sch := pipeline.GetPeriodicRefreshSchedule(); sch != "" {
		spec.Entries = append(spec.Entries, pipelinefile.Entry{Key: "periodicRefresh", Value: exportPeriodicRefresh("", sch)})
	}
	if sets := pipeline.GetVariableSets(); len(sets) > 0 {
		spec.Entries = append(spec.Entries, pipelinefile.Entry{Key: "variableSets", Value: exportVariableSets(sets)})
	}
	if sources := pipeline.GetSecretSources(); len(sources) > 0 {
		spec.Entries = append(spec.Entries, pipelinefile.Entry{Key: "secretSources", Value: exportSecretSources(sources)})
	}
	// The pipeline's named package references (F-21.6, spec.artifacts):
	// emitted only when the pipeline actually carries some (a UI-authored
	// pipeline synthesizes no artifacts block).
	if arts := pipeline.GetArtifacts(); len(arts) > 0 {
		spec.Entries = append(spec.Entries, pipelinefile.Entry{Key: "artifacts", Value: exportPackageArtifacts(arts)})
	}
	if jobs := pipeline.GetJobs(); len(jobs) > 0 {
		spec.Entries = append(spec.Entries, pipelinefile.Entry{Key: "jobs", Value: exportJobs(jobs)})
	}

	doc := pipelinefile.Value{Type: pipelinefile.Map}
	doc.Entries = append(doc.Entries,
		pipelinefile.Entry{Key: "apiVersion", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: pipelinefile.APIVersion}},
		pipelinefile.Entry{Key: "kind", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: string(pipelinefile.KindPipeline)}},
		pipelinefile.Entry{Key: "metadata", Value: exportMetadata(pipeline)},
		pipelinefile.Entry{Key: "spec", Value: spec},
	)
	return doc
}

// exportPeriodicRefresh builds the spec.periodicRefresh mapping (F-21.5.1):
// the steady-state interval (a duration) and/or the cron schedule, in the
// file's key order (interval first, then schedule). At least one argument is
// non-empty (callers suppress the block otherwise).
func exportPeriodicRefresh(interval, schedule string) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.Map}
	if interval != "" {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "interval", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: interval}})
	}
	if schedule != "" {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "schedule", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: schedule}})
	}
	return out
}

// exportPackageArtifacts builds the spec.artifacts sequence (F-21.6): one
// entry per reference (name + a package block carrying name, tag,
// preReleaseRegex, allowPreRelease, useLatest — zeros suppressed).
func exportPackageArtifacts(arts []*dbpb.PackageArtifact) pipelinefile.Value {
	seq := pipelinefile.Value{Type: pipelinefile.List}
	for _, a := range arts {
		entry := pipelinefile.Value{Type: pipelinefile.Map}
		entry.Entries = append(entry.Entries, pipelinefile.Entry{Key: "name", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: a.GetName()}})
		block := pipelinefile.Value{Type: pipelinefile.Map}
		if pn := a.GetPackageName(); pn != "" {
			block.Entries = append(block.Entries, pipelinefile.Entry{Key: "name", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: pn}})
		}
		if tg := a.GetTag(); tg != "" {
			block.Entries = append(block.Entries, pipelinefile.Entry{Key: "tag", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: tg}})
		}
		if px := a.GetPreReleaseRegex(); px != "" {
			block.Entries = append(block.Entries, pipelinefile.Entry{Key: "preReleaseRegex", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: px}})
		}
		if a.GetAllowPreRelease() {
			block.Entries = append(block.Entries, pipelinefile.Entry{Key: "allowPreRelease", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: "true"}})
		}
		if a.GetUseLatest() {
			block.Entries = append(block.Entries, pipelinefile.Entry{Key: "useLatest", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: "true"}})
		}
		entry.Entries = append(entry.Entries, pipelinefile.Entry{Key: "package", Value: block})
		seq.Items = append(seq.Items, entry)
	}
	return seq
}

// exportMetadata builds the document's metadata block: the pipeline's name,
// its description (when set), its labels (F-29), and its free-form
// annotations (F-21.7, UI-search-only).
func exportMetadata(pipeline *dbpb.Pipeline) pipelinefile.Value {
	meta := pipelinefile.Value{Type: pipelinefile.Map}
	meta.Entries = append(meta.Entries, pipelinefile.Entry{Key: "name", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: pipeline.GetName()}})
	if pipeline.GetDescription() != "" {
		meta.Entries = append(meta.Entries, pipelinefile.Entry{Key: "description", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: pipeline.GetDescription()}})
	}
	if labels := pipeline.GetLabels(); len(labels) > 0 {
		meta.Entries = append(meta.Entries, pipelinefile.Entry{Key: "labels", Value: stringMapValue(labels)})
	}
	if ann := pipeline.GetAnnotations(); len(ann) > 0 {
		meta.Entries = append(meta.Entries, pipelinefile.Entry{Key: "annotations", Value: stringMapValue(ann)})
	}
	return meta
}

// exportJobs builds the spec.jobs map (a name → job-spec map): each job's key
// is its name (F-08), and its value carries the job's deployment options,
// steps, and variables.
func exportJobs(jobs []*dbpb.JobDefinition) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.Map}
	for _, job := range jobs {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: job.GetKey(), Value: exportJob(job)})
	}
	return out
}

// exportJob builds a single job's spec (a mapping): its display name,
// dependencies, environment, worker group, automation, approval policy,
// labels, and its execution spec (steps, timeout, retry, failure mode,
// concurrency keys, step barrier, ignore-failed) and variables.
func exportJob(job *dbpb.JobDefinition) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.Map}
	if job.GetName() != "" {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "name", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: job.GetName()}})
	}
	if needs := job.GetNeeds(); len(needs) > 0 {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "needs", Value: stringListValue(needs)})
	}
	if job.GetEnvironment() != "" {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "environment", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: job.GetEnvironment()}})
	}
	if job.GetTargetGroup() != "" {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "workerGroup", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: job.GetTargetGroup()}})
	}
	if a := automationName(job.GetAutomation()); a != "" {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "automation", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: a}})
	}
	if a := approvalPolicyExportName(job.GetApproval()); a != "" {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "approval", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: a}})
	}
	if labels := job.GetLabels(); len(labels) > 0 {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "labels", Value: stringMapValue(labels)})
	}
	// The job's execution spec fields (steps, timeout, retry, failure mode,
	// concurrency keys, step barrier, ignore-failed) are flattened directly
	// into the job value: the job value is the job's spec (the file has no
	// nested `spec` key for a job), which is how the import compiler reads
	// them.
	if spec := job.GetSpec(); spec != nil {
		out.Entries = append(out.Entries, exportJobSpec(spec).Entries...)
	}
	if vars := job.GetVariables(); len(vars) > 0 {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "variables", Value: exportVariables(vars)})
	}
	return out
}

// exportJobSpec builds a job's execution spec (a mapping): its steps, timeout,
// retry policy, failure mode, concurrency keys, step barrier, and ignore-failed
// flag.
func exportJobSpec(spec *dbpb.JobSpec) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.Map}
	if steps := spec.GetSteps(); len(steps) > 0 {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "steps", Value: exportSteps(steps)})
	}
	if spec.GetTimeout() != nil {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "timeout", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: spec.GetTimeout().AsDuration().String()}})
	}
	if retry := spec.GetRetry(); retry != nil {
		retryMap := pipelinefile.Value{Type: pipelinefile.Map}
		retryMap.Entries = append(retryMap.Entries, pipelinefile.Entry{Key: "maxAttempts", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: fmt.Sprintf("%d", retry.GetMaxAttempts())}})
		if retry.GetBackoff() != nil {
			retryMap.Entries = append(retryMap.Entries, pipelinefile.Entry{Key: "backoff", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: retry.GetBackoff().AsDuration().String()}})
		}
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "retry", Value: retryMap})
	}
	if fm := spec.GetFailureMode(); fm != dbpb.FailureMode_FAILURE_MODE_UNSPECIFIED {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "failureMode", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: failureModeExportName(fm)}})
	}
	if keys := spec.GetConcurrencyKeys(); len(keys) > 0 {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "concurrencyKeys", Value: stringListValue(keys)})
	}
	if spec.GetStepBarrier() {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "stepBarrier", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: "true"}})
	}
	if spec.GetIgnoreFailed() {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: "ignoreFailed", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: "true"}})
	}
	return out
}

// exportSteps builds the job's steps map (a name → step-spec map): each step's
// name is its key, and its value carries the step's common fields and its
// handler (a handler-type key whose value is the handler's params, or the
// default handler's params when the step has no type). A shell step whose only
// param is a command is rendered with the `shell: <string>` shorthand.
func exportSteps(steps []*dbpb.JobStep) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.Map}
	for _, step := range steps {
		name := step.GetName()
		if name == "" {
			name = fmt.Sprintf("step%d", len(out.Entries))
		}
		stepMap := pipelinefile.Value{Type: pipelinefile.Map}
		if step.GetWorkdir() != "" {
			stepMap.Entries = append(stepMap.Entries, pipelinefile.Entry{Key: "workdir", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: step.GetWorkdir()}})
		}
		if env := step.GetEnv(); len(env) > 0 {
			stepMap.Entries = append(stepMap.Entries, pipelinefile.Entry{Key: "env", Value: stringMapValue(env)})
		}
		if step.GetTimeout() != nil {
			stepMap.Entries = append(stepMap.Entries, pipelinefile.Entry{Key: "timeout", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: step.GetTimeout().AsDuration().String()}})
		}
		if step.GetCondition() != "" {
			stepMap.Entries = append(stepMap.Entries, pipelinefile.Entry{Key: "condition", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: step.GetCondition()}})
		}
		if step.GetIgnoreFailed() {
			stepMap.Entries = append(stepMap.Entries, pipelinefile.Entry{Key: "ignoreFailed", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: "true"}})
		}
		if step.GetOnce() {
			stepMap.Entries = append(stepMap.Entries, pipelinefile.Entry{Key: "once", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: "true"}})
		}
		stepParams := step.GetParams()
		// commandOnly reports whether the step's params are exactly a single
		// string-only `command` param: the shape the `shell: <string>` shorthand
		// round-trips to.
		commandOnly := false
		if len(stepParams) == 1 {
			if pv, ok := stepParams["command"]; ok && pv.GetString_() != "" && len(pv.GetStrings()) == 0 {
				commandOnly = true
			}
		}
		// The handler is emitted so the export re-imports to the same compiled
		// step: a default-handler step (empty Type) is written as bare sibling
		// keys (no handler-type key, so re-import selects the default handler);
		// an explicit shell step whose only param is a command uses the
		// `shell: <string>` shorthand; any other handler is a handler-type key
		// whose value is the params map.
		switch {
		case step.GetType() == "":
			stepMap.Entries = append(stepMap.Entries, exportParamsFromProto(stepParams).Entries...)
		case step.GetType() == "shell" && commandOnly:
			stepMap.Entries = append(stepMap.Entries, pipelinefile.Entry{Key: "shell", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: stepParams["command"].GetString_()}})
		default:
			stepMap.Entries = append(stepMap.Entries, pipelinefile.Entry{Key: step.GetType(), Value: exportParamsFromProto(stepParams)})
		}
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: name, Value: stepMap})
	}
	return out
}

// exportParamsFromProto converts a step's params (a name → ParamValue map)
// into the file's params map: a string-only param is a scalar, a strings-only
// param is a list of scalars, and a param carrying both is a {string, strings}
// map.
func exportParamsFromProto(params map[string]*dbpb.ParamValue) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.Map}
	// Emit the params in sorted key order so the exported document is
	// deterministic (a step's params are a set; their order is not meaningful).
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		pv := params[name]
		hasString := pv.GetString_() != ""
		hasStrings := len(pv.GetStrings()) > 0
		switch {
		case hasString && !hasStrings:
			out.Entries = append(out.Entries, pipelinefile.Entry{Key: name, Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: pv.GetString_()}})
		case hasStrings && !hasString:
			list := pipelinefile.Value{Type: pipelinefile.List}
			for _, s := range pv.GetStrings() {
				list.Items = append(list.Items, pipelinefile.Value{Type: pipelinefile.Scalar, String: s})
			}
			out.Entries = append(out.Entries, pipelinefile.Entry{Key: name, Value: list})
		default:
			both := pipelinefile.Value{Type: pipelinefile.Map}
			if hasString {
				both.Entries = append(both.Entries, pipelinefile.Entry{Key: "string", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: pv.GetString_()}})
			}
			if hasStrings {
				stringsList := pipelinefile.Value{Type: pipelinefile.List}
				for _, s := range pv.GetStrings() {
					stringsList.Items = append(stringsList.Items, pipelinefile.Value{Type: pipelinefile.Scalar, String: s})
				}
				both.Entries = append(both.Entries, pipelinefile.Entry{Key: "strings", Value: stringsList})
			}
			out.Entries = append(out.Entries, pipelinefile.Entry{Key: name, Value: both})
		}
	}
	return out
}

// exportVariables builds a variables map (a name → value map): a plain
// variable is a scalar, an encrypted variable is an {encrypted: <ciphertext>}
// map, and a sourced variable is a {secretSource: <name>, key: <key>} map.
func exportVariables(vars []*dbpb.Variable) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.Map}
	for _, v := range vars {
		switch v.GetShape() {
		case dbpb.VariableShape_VARIABLE_PLAIN:
			out.Entries = append(out.Entries, pipelinefile.Entry{Key: v.GetName(), Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: v.GetPlain()}})
		case dbpb.VariableShape_VARIABLE_ENCRYPTED:
			m := pipelinefile.Value{Type: pipelinefile.Map}
			m.Entries = append(m.Entries, pipelinefile.Entry{Key: "encrypted", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: v.GetEncrypted()}})
			out.Entries = append(out.Entries, pipelinefile.Entry{Key: v.GetName(), Value: m})
		case dbpb.VariableShape_VARIABLE_SECRET_SOURCE:
			m := pipelinefile.Value{Type: pipelinefile.Map}
			m.Entries = append(m.Entries, pipelinefile.Entry{Key: "secretSource", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: v.GetSecretSource()}})
			m.Entries = append(m.Entries, pipelinefile.Entry{Key: "key", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: v.GetKey()}})
			out.Entries = append(out.Entries, pipelinefile.Entry{Key: v.GetName(), Value: m})
		}
	}
	return out
}

// exportVariableSets builds the spec.variableSets map (a name → set-spec map):
// each set's value is its inline variables map.
func exportVariableSets(sets []*dbpb.VariableSet) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.Map}
	for _, set := range sets {
		setMap := pipelinefile.Value{Type: pipelinefile.Map}
		setMap.Entries = append(setMap.Entries, pipelinefile.Entry{Key: "variables", Value: exportVariables(set.GetVariables())})
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: set.GetName(), Value: setMap})
	}
	return out
}

// exportSecretSources builds the spec.secretSources map (a name → source-spec
// map): each source's value carries its kind, path, and audience.
func exportSecretSources(sources []*dbpb.SecretSource) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.Map}
	for _, src := range sources {
		srcMap := pipelinefile.Value{Type: pipelinefile.Map}
		if src.GetKind() != "" {
			srcMap.Entries = append(srcMap.Entries, pipelinefile.Entry{Key: "kind", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: src.GetKind()}})
		}
		if src.GetPath() != "" {
			srcMap.Entries = append(srcMap.Entries, pipelinefile.Entry{Key: "path", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: src.GetPath()}})
		}
		if src.GetAudience() != "" {
			srcMap.Entries = append(srcMap.Entries, pipelinefile.Entry{Key: "audience", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: src.GetAudience()}})
		}
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: src.GetName(), Value: srcMap})
	}
	return out
}

// exportParams builds the spec.params list (a list of {name, default,
// description} objects).
func exportParams(params []*dbpb.Parameter) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.List}
	for _, p := range params {
		item := pipelinefile.Value{Type: pipelinefile.Map}
		item.Entries = append(item.Entries, pipelinefile.Entry{Key: "name", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: p.GetName()}})
		if p.GetDefault() != "" {
			item.Entries = append(item.Entries, pipelinefile.Entry{Key: "default", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: p.GetDefault()}})
		}
		if p.GetDescription() != "" {
			item.Entries = append(item.Entries, pipelinefile.Entry{Key: "description", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: p.GetDescription()}})
		}
		out.Items = append(out.Items, item)
	}
	return out
}

// exportTriggers builds the spec.triggers list (a list of {name, type, ...}
// objects): each trigger's kind-specific fields (cron for a cron trigger,
// secret / oidcIssuer / oidcClaims for a webhook trigger, eventPipeline /
// eventStatus for an event trigger) are emitted when set.
func exportTriggers(triggers []*dbpb.Trigger) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.List}
	for _, t := range triggers {
		item := pipelinefile.Value{Type: pipelinefile.Map}
		item.Entries = append(item.Entries, pipelinefile.Entry{Key: "name", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: t.GetName()}})
		item.Entries = append(item.Entries, pipelinefile.Entry{Key: "type", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: triggerTypeName(t.GetType())}})
		if t.GetCron() != "" {
			item.Entries = append(item.Entries, pipelinefile.Entry{Key: "cron", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: t.GetCron()}})
		}
		if t.GetSecret() != "" {
			item.Entries = append(item.Entries, pipelinefile.Entry{Key: "secret", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: t.GetSecret()}})
		}
		if t.GetEventPipeline() != "" {
			item.Entries = append(item.Entries, pipelinefile.Entry{Key: "eventPipeline", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: t.GetEventPipeline()}})
		}
		if es := runStatusExportName(t.GetEventStatus()); es != "" {
			item.Entries = append(item.Entries, pipelinefile.Entry{Key: "eventStatus", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: es}})
		}
		if t.GetOidcIssuer() != "" {
			item.Entries = append(item.Entries, pipelinefile.Entry{Key: "oidcIssuer", Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: t.GetOidcIssuer()}})
		}
		if claims := t.GetOidcClaims(); len(claims) > 0 {
			item.Entries = append(item.Entries, pipelinefile.Entry{Key: "oidcClaims", Value: stringMapValue(claims)})
		}
		out.Items = append(out.Items, item)
	}
	return out
}

// stringListValue builds a list of scalar Values from a string slice.
func stringListValue(items []string) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.List}
	for _, s := range items {
		out.Items = append(out.Items, pipelinefile.Value{Type: pipelinefile.Scalar, String: s})
	}
	return out
}

// stringMapValue builds a map Value from a string map.
func stringMapValue(m map[string]string) pipelinefile.Value {
	out := pipelinefile.Value{Type: pipelinefile.Map}
	for k, v := range m {
		out.Entries = append(out.Entries, pipelinefile.Entry{Key: k, Value: pipelinefile.Value{Type: pipelinefile.Scalar, String: v}})
	}
	return out
}

// triggerTypeName maps a proto TriggerType onto its file name (the inverse of
// triggerTypeFromName). UNSPECIFIED maps to "" (no kind).
func triggerTypeName(t dbpb.TriggerType) string {
	switch t {
	case dbpb.TriggerType_TRIGGER_TYPE_CRON:
		return "cron"
	case dbpb.TriggerType_TRIGGER_TYPE_WEBHOOK:
		return "webhook"
	case dbpb.TriggerType_TRIGGER_TYPE_EVENT:
		return "event"
	default:
		return ""
	}
}

// failureModeExportName maps a proto FailureMode onto its file name (the
// inverse of failureModeFromName). UNSPECIFIED maps to "" (the default, ALL).
func failureModeExportName(m dbpb.FailureMode) string {
	switch m {
	case dbpb.FailureMode_FAILURE_MODE_ALL:
		return "all"
	case dbpb.FailureMode_FAILURE_MODE_BEST_EFFORT:
		return "best_effort"
	case dbpb.FailureMode_FAILURE_MODE_ANY:
		return "any"
	default:
		return ""
	}
}

// approvalPolicyExportName maps a proto ApprovalPolicy onto its file name (the
// inverse of approvalPolicyFromName). UNSPECIFIED maps to "" (defer to the
// environment's flag).
func approvalPolicyExportName(p dbpb.ApprovalPolicy) string {
	switch p {
	case dbpb.ApprovalPolicy_APPROVAL_POLICY_NONE:
		return "none"
	case dbpb.ApprovalPolicy_APPROVAL_POLICY_REQUIRED:
		return "required"
	default:
		return ""
	}
}

// runStatusExportName maps a proto RunStatus onto its file name (the inverse
// of runStatusFromName). UNSPECIFIED maps to "" (no status).
func runStatusExportName(s dbpb.RunStatus) string {
	switch s {
	case dbpb.RunStatus_RUN_STATUS_PENDING:
		return "pending"
	case dbpb.RunStatus_RUN_STATUS_RUNNING:
		return "running"
	case dbpb.RunStatus_RUN_STATUS_SUCCEEDED:
		return "succeeded"
	case dbpb.RunStatus_RUN_STATUS_FAILED:
		return "failed"
	case dbpb.RunStatus_RUN_STATUS_CANCELLED:
		return "cancelled"
	default:
		return ""
	}
}
