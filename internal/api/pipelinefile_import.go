package api

import (
	"fmt"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/pipelinefile"
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
// Only Pipeline documents are importable (a VariableSet / Step / Job document
// is a reusable unit pulled in by import, F-21.4, not a standalone pipeline).
// A non-Pipeline document, or a document that fails validation, is an error
// (the compiler's actionable errors, joined).
func pipelineFileToRequest(name string, data []byte) (*pipelineRequest, []*dbpb.VariableSet, []*dbpb.SecretSource, error) {
	doc, err := pipelinefile.Parse(name, data)
	if err != nil {
		return nil, nil, nil, err
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

	req := &pipelineRequest{
		Name:        doc.Metadata.Name,
		Description: doc.Metadata.Description,
		Tags:        doc.Metadata.Labels,
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
	req.Params = pipelineFileParams(doc.Spec)
	req.Triggers = pipelineFileTriggers(doc.Spec)

	// The pipeline's named variable sets and secret sources (F-21.1.3),
	// carried to the database service alongside the pipeline.
	req.VariableSets = variableSetsToProto(variableSets)
	req.SecretSources = secretSourcesToProto(secretSources)

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
		job.Tags = labels
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
			Name:      s.Name,
			Variables: variablesToProto(s.Variables),
		})
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
