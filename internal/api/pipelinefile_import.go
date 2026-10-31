package api

import (
	"context"
	"fmt"
	"strconv"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/pipelinefile"
	"cdrom/internal/sourceref"
)

// pipelineFileToRequest runs a pipeline definition in the file format (F-21.1)
// through the compiler (internal/pipelinefile) and maps the result onto the
// schema the API hands to the database service: a pipelineRequest (the
// pipeline's identity, deployment options, and job definitions) plus the
// pipeline's named variable sets and secret sources (F-21.1.3). The file's
// names never reach an execution target: the compiler resolves step names to
// step indices, job names to job keys, and named variables/sources to their
// stored forms.
//
// This entry point resolves no imports (it has no source-fetcher): a document
// that carries an importFrom / import fails with a clear error telling the
// reader to submit it through the API (which wires the fetcher). Handlers use
// (*Server).compilePipelineFile instead.
//
// Only Pipeline documents are importable (a VariableSet / Step / Job document
// is a reusable unit pulled in by import, F-21.4, not a standalone pipeline).
// A non-Pipeline document, or a document that fails validation, is an error
// (the compiler's actionable errors, joined).
func pipelineFileToRequest(name string, data []byte) (*pipelineRequest, []*dbpb.VariableSet, []*dbpb.SecretSource, error) {
	// No server (no fetcher, no source): imports cannot be resolved. A
	// document with no imports compiles fine; one with an import fails with a
	// clear error.
	return (&Server{}).compilePipelineFile(context.Background(), name, data, nil)
}

// compilePipelineFile runs a pipeline definition in the file format through
// the compiler, first resolving its imports (F-21.4) with the given root
// reference (the main file's source, for resolving relative locators; nil =
// only full references) and the server's fetcher (nil when the source-fetcher
// is not configured, in which case a document with an import fails with a
// clear error), and then mapping the flattened document onto the schema the
// API hands to the database service. The every-key vault imports collected by
// the resolver (by set name) are applied onto the compiled variable sets so
// the jobs that reference them carry them (expanded at dispatch).
func (s *Server) compilePipelineFile(ctx context.Context, name string, data []byte, root *sourceref.Reference) (*pipelineRequest, []*dbpb.VariableSet, []*dbpb.SecretSource, error) {
	return requestFromFile(ctx, name, data, root, s.pipelineFileFetcher())
}

// requestFromFile is the import orchestration (F-21.1 / F-21.4): it resolves
// the document's imports with the given root reference (the main file's source,
// for resolving relative locators; nil = only full references) and fetcher
// (nil = imports cannot be resolved), then maps the flattened document onto the
// schema the API hands to the database service. Factored out of
// compilePipelineFile so tests can inject a fake fetcher. The every-key vault
// imports collected by the resolver (by set name) are applied onto the
// compiled variable sets so the jobs that reference them carry them (expanded
// at dispatch).
func requestFromFile(ctx context.Context, name string, data []byte, root *sourceref.Reference, fetch pipelinefile.Fetcher) (*pipelineRequest, []*dbpb.VariableSet, []*dbpb.SecretSource, error) {
	resolver := pipelinefile.NewResolver(ctx, fetch, root)
	doc, vaultImportsBySet, rErrs := resolver.ResolveMain(name, data)
	if len(rErrs) > 0 {
		return nil, nil, nil, rErrs
	}
	if doc.Kind != pipelinefile.KindPipeline {
		return nil, nil, nil, fmt.Errorf("a %s document is a reusable unit (pulled in by import, F-21.4), not a pipeline; import a Pipeline document", doc.Kind)
	}
	file := name
	if file == "" {
		file = "pipeline"
	}

	// The pipeline's named resources (F-21.1.3): the shared steps, the named
	// variable sets, and the named secret sources.
	stepsVal, _ := doc.Spec.Get("steps")
	shared, sErrs := pipelinefile.CompileSharedSteps(file, stepsVal)
	if len(sErrs) > 0 {
		return nil, nil, nil, sErrs
	}
	variableSetsVal, _ := doc.Spec.Get("variableSets")
	variableSets, vsErrs := pipelinefile.CompileVariableSets(file, variableSetsVal)
	if len(vsErrs) > 0 {
		return nil, nil, nil, vsErrs
	}
	secretSourcesVal, _ := doc.Spec.Get("secretSources")
	secretSources, ssErrs := pipelinefile.CompileSecretSources(file, secretSourcesVal)
	if len(ssErrs) > 0 {
		return nil, nil, nil, ssErrs
	}
	// A variable set's own secretSources (F-21.4) are merged into the
	// pipeline's secretSources namespace (the pipeline's own win on a name
	// collision), so a set that declares its own sources is self-contained:
	// its masked variables resolve against a source the API can look up at
	// dispatch, and the sources are carried to the database service alongside
	// the pipeline.
	for _, set := range variableSets {
		for name, src := range set.SecretSources {
			if _, ok := secretSources[name]; !ok {
				secretSources[name] = src
			}
		}
	}
	// Apply the resolver-collected every-key vault imports (F-21.4) onto the
	// compiled sets (bubble-ups from imported VariableSet files arrive here
	// keyed by the importing set's name), so the jobs that reference the sets
	// carry them (expanded at dispatch).
	for name, vis := range vaultImportsBySet {
		if set, ok := variableSets[name]; ok {
			for _, vi := range vis {
				set.VaultImports = append(set.VaultImports, vi)
			}
		}
	}

	req := &pipelineRequest{
		Name:        doc.Metadata.Name,
		Description: doc.Metadata.Description,
		Labels:      doc.Metadata.Labels,
		// The file's free-form annotations (F-21.7, metadata.annotations):
		// stored on the pipeline for UI searchability (they never affect
		// behavior).
		Annotations: doc.Metadata.Annotations,
	}
	// The file-declared periodic-refresh policy (F-21.5.1,
	// spec.periodicRefresh): compiled + validated (a malformed duration or
	// cron is an actionable error) and carried to the database service;
	// absent when the file declares none (the pipeline inherits the system
	// default cadence). A file that declares a policy states the gate's
	// INTENDED value — allowed (the author proposes it; the admin gate
	// decides whether it is honored).
	if pol, polErrs := pipelinefile.CompilePeriodicRefresh(file, doc.Spec); len(polErrs) > 0 {
		return nil, nil, nil, polErrs
	} else if pol != nil {
		req.PeriodicRefresh = &periodicRefreshRequest{Interval: pol.Interval, Schedule: pol.Schedule}
	}
	// The pipeline-level deployment options (F-21.1.4): the default failure
	// mode, the pipeline's concurrency keys, its parameter declarations, and
	// its triggers. The default worker group is not a pipeline field in the
	// schema; it is applied to each job that sets no worker group of its own.
	defaultWorkerGroup, _ := doc.Spec.StringValue("workerGroup")
	if fm, ok := doc.Spec.StringValue("failureMode"); ok {
		req.FailureMode = fm
	}
	if keys, ok := doc.Spec.StringSlice("concurrencyKeys"); ok {
		req.ConcurrencyKeys = keys
	}
	// The pipeline's DEFAULT approval-gate settings (F-27): spec.approvers is
	// a list of approver references and spec.quorum is the quorum depth K. An
	// approval step that sets no `approvers`/`quorum` of its own inherits
	// these (merged into the step's params at save time, by
	// applyApprovalDefaults). A malformed quorum is a loud error.
	if appr, ok := doc.Spec.StringSlice("approvers"); ok {
		req.Approvers = appr
	}
	if qRaw, ok := doc.Spec.StringValue("quorum"); ok && qRaw != "" {
		q, err := strconv.Atoi(qRaw)
		if err != nil || q < 1 {
			return nil, nil, nil, pipelinefile.Errors{{File: file, Key: "spec.quorum", Reason: fmt.Sprintf("%q is not a positive integer", qRaw)}}
		}
		req.Quorum = q
	}
	req.Params = pipelineFileParams(doc.Spec)
	req.Triggers = pipelineFileTriggers(doc.Spec)

	// The pipeline's named variable sets and secret sources (F-21.1.3),
	// carried to the database service alongside the pipeline.
	req.VariableSets = variableSetsToProto(variableSets)
	req.SecretSources = secretSourcesToProto(secretSources)

	// The pipeline's named package references (F-21.6, spec.artifacts):
	// compiled + validated (a malformed entry is an actionable error) and
	// carried to the database service alongside the pipeline. At run
	// creation the database service resolves each reference against the
	// package registry and records the per-run snapshot. Presence of the key
	// (even null/empty) marks the request as mentioning the references, so
	// an UPDATE replaces them (removing the key from the file clears them).
	artifactsVal, hasArtifactsKey := doc.Spec.Get("artifacts")
	artifacts, aErrs := pipelinefile.CompilePackageArtifacts(file, artifactsVal)
	if len(aErrs) > 0 {
		return nil, nil, nil, aErrs
	}
	req.Artifacts = artifacts
	req.HasArtifacts = hasArtifactsKey

	// The jobs (a name -> job-spec map): each job's name is its stable key
	// (F-08), and its deployment options, steps, and variables are mapped onto
	// the schema.
	jobsVal, _ := doc.Spec.Get("jobs")
	var errs pipelinefile.Errors
	for _, e := range jobsVal.Entries {
		job, jobErrs := pipelineFileJob(file, e.Key, e.Value, shared, variableSets, secretSources, defaultWorkerGroup)
		if len(jobErrs) > 0 {
			errs = append(errs, jobErrs...)
			continue
		}
		req.Jobs = append(req.Jobs, job)
	}
	if len(errs) > 0 {
		return nil, nil, nil, errs
	}
	// Merge the pipeline's DEFAULT approval-gate settings (F-27) into every
	// approval step that does not set its own, at SAVE time — so the merged
	// params bake into the per-run spec snapshot (F-11). Covers create,
	// update, and source re-import alike (all funnel through requestFromFile).
	applyApprovalDefaults(req)
	return req, req.VariableSets, req.SecretSources, nil
}

// pipelineFileJob compiles a single job (a name -> job-spec map entry) into a
// pipelineJobRequest: the job's name is its stable key (F-08); its steps are
// compiled against the pipeline's shared steps; its variables are merged with
// the variables of the variable sets it references (the job's inline
// variables win on a key collision, F-21.1.3); and its deployment options
// (needs, environment, workerGroup, timeout, retry, failureMode,
// concurrencyKeys, stepBarrier, ignoreFailed, automation, approval, labels)
// are mapped onto the schema. A job's workerGroup defaults to the pipeline's
// spec.workerGroup.
func pipelineFileJob(file, name string, value pipelinefile.Value, shared map[string]*dbpb.JobStep, variableSets map[string]*pipelinefile.VariableSet, secretSources map[string]*pipelinefile.SecretSource, defaultWorkerGroup string) (pipelineJobRequest, pipelinefile.Errors) {
	var job pipelineJobRequest
	var errs pipelinefile.Errors
	base := pipelinefile.NewPath("spec", "jobs", name)

	// The job's stable key is its name (F-08); the display name defaults to
	// the key.
	job.Key = name
	if n, ok := value.StringValue("name"); ok && n != "" {
		job.Name = n
	}
	// The job's dependencies (F-08): the names of the jobs it depends on.
	if needs, ok := value.StringSlice("needs"); ok {
		job.Needs = needs
	}
	// The job's environment (F-17) and worker group (defaulting to the
	// pipeline's spec.workerGroup).
	if env, ok := value.StringValue("environment"); ok {
		job.Environment = env
	}
	if wg, ok := value.StringValue("workerGroup"); ok {
		job.WorkerGroup = wg
	} else if wg, ok := value.StringValue("targetGroup"); ok {
		job.WorkerGroup = wg
	} else if defaultWorkerGroup != "" {
		job.WorkerGroup = defaultWorkerGroup
	}
	// The job's automation (F-17) and approval policy (F-17).
	if a, ok := value.StringValue("automation"); ok {
		job.Automation = a
	}
	if a, ok := value.StringValue("approval"); ok {
		job.Approval = a
	}
	// The job's labels (F-29).
	if labels, ok := value.StringMap("labels"); ok {
		job.Labels = labels
	}

	// The job's steps: compiled against the pipeline's shared steps (a ref or
	// empty entry resolves to a shared step; an inline entry is compiled in
	// place). The step's name is stored on the JobStep (F-21.1.2). The steps
	// and the job's deployment options (timeout, retry, failureMode,
	// concurrencyKeys, stepBarrier, ignoreFailed) live on the job's execution
	// spec (JobSpec), matching the schema.
	stepsVal, _ := value.Get("steps")
	steps, stepErrs := pipelinefile.CompileJobSteps(file, base.Extend("steps"), shared, stepsVal)
	errs = append(errs, stepErrs...)
	spec := &jobSpecRequest{}
	for _, step := range steps {
		spec.Steps = append(spec.Steps, jobStepRequestFromProto(step))
	}
	if t, ok := value.StringValue("timeout"); ok {
		spec.Timeout = t
	}
	if fm, ok := value.StringValue("failureMode"); ok {
		spec.FailureMode = fm
	}
	if keys, ok := value.StringSlice("concurrencyKeys"); ok {
		spec.ConcurrencyKeys = keys
	}
	if sb, ok := value.AsBoolField("stepBarrier"); ok {
		spec.StepBarrier = sb
	}
	if ig, ok := value.AsBoolField("ignoreFailed"); ok {
		spec.IgnoreFailed = ig
	}
	// The job's branch flag (F-22): when true the job reacts to its
	// dependencies' outcomes (routing on them via conditions) instead of
	// being skipped when one fails.
	if br, ok := value.AsBoolField("branch"); ok {
		spec.Branch = br
	}
	if retryVal, ok := value.Get("retry"); ok {
		spec.Retry = pipelineFileRetry(retryVal)
	}
	job.Spec = spec

	// The job's variables (F-21.1.3): the inline variables merged with the
	// variables of the variable sets the job references (the job's inline
	// variables win on a key collision). The merged variables are the job's
	// effective variables, carried to the database service and denormalized
	// onto each job instance at run creation.
	inlineVal, _ := value.Get("variables")
	inline, inlineErrs := pipelinefile.CompileVariables(file, base.Extend("variables"), inlineVal)
	errs = append(errs, inlineErrs...)
	setsList, _ := value.Get("variableSets")
	resolvedSets, resolveErrs := pipelinefile.ResolveVariableSets(file, base, setsList, variableSets)
	errs = append(errs, resolveErrs...)
	merged := pipelinefile.MergeJobVariables(inline, resolvedSets)
	// A sourced variable must reference a declared secret source (F-21.1.3).
	errs = append(errs, pipelinefile.ValidateSecretSourceRefs(file, base.Extend("variables"), merged, secretSources)...)
	job.Variables = variablesToProto(merged)

	// The job's every-key vault imports (F-21.4): the union of the referenced
	// sets' vault imports (deduplicated by source). Each must name a declared
	// secret source (the merged namespace, which includes the sets' own
	// sources), so a typo in a vault name is caught at import time.
	var jobVaultImports []pipelinefile.VaultImport
	for _, set := range resolvedSets {
		jobVaultImports = append(jobVaultImports, set.VaultImports...)
	}
	jobVaultImports = dedupeVaultImports(jobVaultImports)
	for _, vi := range jobVaultImports {
		if _, ok := secretSources[vi.Source]; !ok {
			errs = append(errs, pipelinefile.Error{File: file, Key: base.Extend("variableSets").String(),
				Reason: fmt.Sprintf("vault import references secret source %q, which is not declared (neither by the pipeline nor by the variable set that imports it)", vi.Source)})
		}
	}
	job.VaultImports = vaultImportsToProto(jobVaultImports)

	if len(errs) > 0 {
		return pipelineJobRequest{}, errs
	}
	return job, nil
}

// pipelineFileParams converts the file's params (a list of {name, default,
// description} objects, F-10) into the pipeline's parameter declarations.
func pipelineFileParams(spec pipelinefile.Value) []parameterRequest {
	paramsVal, ok := spec.Get("params")
	if !ok || paramsVal.Type != pipelinefile.List {
		return nil
	}
	out := make([]parameterRequest, 0, len(paramsVal.Items))
	for _, item := range paramsVal.Items {
		if item.Type != pipelinefile.Map {
			continue
		}
		name, _ := item.StringValue("name")
		if name == "" {
			continue
		}
		p := parameterRequest{Name: name}
		p.Default, _ = item.StringValue("default")
		p.Description, _ = item.StringValue("description")
		out = append(out, p)
	}
	return out
}

// pipelineFileTriggers converts the file's triggers (a list of {name, type,
// cron, params, ...} objects, F-09) into the pipeline's trigger declarations.
func pipelineFileTriggers(spec pipelinefile.Value) []triggerRequest {
	triggersVal, ok := spec.Get("triggers")
	if !ok || triggersVal.Type != pipelinefile.List {
		return nil
	}
	out := make([]triggerRequest, 0, len(triggersVal.Items))
	for _, item := range triggersVal.Items {
		if item.Type != pipelinefile.Map {
			continue
		}
		name, _ := item.StringValue("name")
		if name == "" {
			continue
		}
		t := triggerRequest{Name: name}
		t.Type, _ = item.StringValue("type")
		t.Cron, _ = item.StringValue("cron")
		t.Secret, _ = item.StringValue("secret")
		t.EventPipeline, _ = item.StringValue("eventPipeline")
		t.EventStatus, _ = item.StringValue("eventStatus")
		t.OIDCIssuer, _ = item.StringValue("oidcIssuer")
		if claims, ok := item.StringMap("oidcClaims"); ok {
			t.OIDCClaims = claims
		}
		if params, ok := item.StringMap("params"); ok {
			t.Params = params
		}
		out = append(out, t)
	}
	return out
}

// pipelineFileRetry converts the file's retry policy (a {maxAttempts, backoff}
// object, F-04) into the pipeline's retry policy request. A non-map value
// yields nil (no retry policy).
func pipelineFileRetry(value pipelinefile.Value) *retryPolicyRequest {
	if value.Type != pipelinefile.Map {
		return nil
	}
	retry := &retryPolicyRequest{}
	if maxStr, ok := value.StringValue("maxAttempts"); ok {
		if n, err := parseInt(maxStr); err == nil {
			retry.MaxAttempts = n
		}
	}
	if backoff, ok := value.StringValue("backoff"); ok {
		retry.Backoff = backoff
	}
	return retry
}

// jobStepRequestFromProto converts a compiled JobStep (from the file format)
// into the JSON jobStepRequest the API hands to the database service. The
// step's name is carried on the JobStep (F-21.1.2); the step's params are
// lifted into the JSON params map.
func jobStepRequestFromProto(step *dbpb.JobStep) jobStepRequest {
	req := jobStepRequest{
		Name:         step.GetName(),
		Type:         step.GetType(),
		Workdir:      step.GetWorkdir(),
		Env:          step.GetEnv(),
		Condition:    step.GetCondition(),
		IgnoreFailed: step.GetIgnoreFailed(),
		Once:         step.GetOnce(),
	}
	if step.GetTimeout() != nil {
		req.Timeout = step.GetTimeout().AsDuration().String()
	}
	if len(step.GetParams()) > 0 {
		req.Params = make(map[string]*paramValueRequest, len(step.GetParams()))
		for k, v := range step.GetParams() {
			req.Params[k] = &paramValueRequest{String: v.GetString_(), Strings: v.GetStrings()}
		}
	}
	return req
}

// variablesToProto converts the compiler's variables (F-21.1.3) into the proto
// Variable list carried to the database service. An empty slice yields nil.
func variablesToProto(vars []pipelinefile.Variable) []*dbpb.Variable {
	if len(vars) == 0 {
		return nil
	}
	out := make([]*dbpb.Variable, 0, len(vars))
	for _, v := range vars {
		out = append(out, &dbpb.Variable{
			Name:         v.Name,
			Shape:        variableShapeToProto(v.Value.Shape),
			Plain:        v.Value.Plain,
			Encrypted:    v.Value.Encrypted,
			SecretSource: v.Value.SecretSource,
			Key:          v.Value.Key,
		})
	}
	return out
}

// variableSetsToProto converts the compiler's variable sets (F-21.1.3) into
// the proto VariableSet list carried to the database service. An empty map
// yields nil.
func variableSetsToProto(sets map[string]*pipelinefile.VariableSet) []*dbpb.VariableSet {
	if len(sets) == 0 {
		return nil
	}
	out := make([]*dbpb.VariableSet, 0, len(sets))
	for _, s := range sets {
		out = append(out, &dbpb.VariableSet{
			Name:         s.Name,
			Variables:    variablesToProto(s.Variables),
			VaultImports: vaultImportsToProto(s.VaultImports),
		})
	}
	return out
}

// vaultImportsToProto converts the compiler's vault imports (F-21.4) into the
// proto VaultImport list carried to the database service. An empty slice yields
// nil.
func vaultImportsToProto(vs []pipelinefile.VaultImport) []*dbpb.VaultImport {
	if len(vs) == 0 {
		return nil
	}
	out := make([]*dbpb.VaultImport, 0, len(vs))
	for _, v := range vs {
		out = append(out, &dbpb.VaultImport{Source: v.Source, Keys: v.Keys})
	}
	return out
}

// dedupeVaultImports collapses a job's every-key vault imports (the union of
// the referenced sets' imports) into a unique, order-stable list, merging the
// keys of imports that share a source. Two sets that both import the same
// vault contribute one import (the union of their keys; an every-key import
// swallows a per-key one for the same source).
func dedupeVaultImports(in []pipelinefile.VaultImport) []pipelinefile.VaultImport {
	type acc struct {
		source string
		keys   map[string]bool
		all    bool
		order  []string
	}
	index := make(map[string]*acc)
	var seq []*acc
	for _, v := range in {
		a, ok := index[v.Source]
		if !ok {
			a = &acc{source: v.Source, keys: make(map[string]bool)}
			index[v.Source] = a
			seq = append(seq, a)
		}
		if len(v.Keys) == 0 {
			a.all = true
			continue
		}
		for _, k := range v.Keys {
			if !a.keys[k] {
				a.order = append(a.order, k)
				a.keys[k] = true
			}
		}
	}
	out := make([]pipelinefile.VaultImport, 0, len(seq))
	for _, a := range seq {
		vi := pipelinefile.VaultImport{Source: a.source}
		if !a.all {
			vi.Keys = a.order
		}
		out = append(out, vi)
	}
	return out
}

// secretSourcesToProto converts the compiler's secret sources (F-21.1.3 /
// F-12) into the proto SecretSource list carried to the database service. An
// empty map yields nil.
func secretSourcesToProto(srcs map[string]*pipelinefile.SecretSource) []*dbpb.SecretSource {
	if len(srcs) == 0 {
		return nil
	}
	out := make([]*dbpb.SecretSource, 0, len(srcs))
	for _, s := range srcs {
		out = append(out, &dbpb.SecretSource{
			Name:     s.Name,
			Kind:     s.Kind,
			Path:     s.Path,
			Audience: s.Audience,
		})
	}
	return out
}

// variableShapeToProto converts the compiler's variable shape (F-21.1.3) into
// the proto VariableShape.
func variableShapeToProto(shape pipelinefile.VariableShape) dbpb.VariableShape {
	switch shape {
	case pipelinefile.VariableEncrypted:
		return dbpb.VariableShape_VARIABLE_ENCRYPTED
	case pipelinefile.VariableSecretSource:
		return dbpb.VariableShape_VARIABLE_SECRET_SOURCE
	default:
		return dbpb.VariableShape_VARIABLE_PLAIN
	}
}

// parseInt parses a string as an integer (the file's maxAttempts is a scalar
// that may be written as a number or a numeric string).
func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
