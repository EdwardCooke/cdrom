package pipelinefile

import (
	"fmt"
	"text/template"
	"text/template/parse"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// KnownStepTypes are the step handler types the file format recognizes: the
// built-in handlers. A step's value may carry a key naming one of these to
// select that handler (the key's value is the handler's params); when no such
// key is present the step uses the default handler (shell) and the remaining
// keys are its params. This set must stay in sync with the built-in step
// handlers (internal/stephandlers): shell, token_exchange, approval, email,
// webhook, and the artifact handlers (F-21.6: download_artifact, extract,
// publish_artifact).
var KnownStepTypes = []string{
	"shell", // the default handler
	"token_exchange",
	"approval",
	"email",
	"webhook",
	"download_artifact",
	"extract",
	"publish_artifact",
}

// isKnownStepType reports whether name names a known step handler type.
func isKnownStepType(name string) bool {
	for _, t := range KnownStepTypes {
		if name == t {
			return true
		}
	}
	return false
}

// commonStepFields are the step fields recognized by name and lifted onto the
// JobStep (rather than treated as a handler's param or a handler-type key).
var commonStepFields = map[string]bool{
	"workdir":      true,
	"env":          true,
	"timeout":      true,
	"condition":    true,
	"ignoreFailed": true,
	"once":         true,
}

// CompileStep compiles the step named name (whose spec is value, a map from
// the file) into a JobStep, storing name on the step so the executor's
// condition context can reference the step by name (F-21.1.2). file is the
// name of the offending file (carried into any validation errors), and base is
// the dotted path of the step within the document (e.g. "spec.steps.build"),
// so a validation error names the offending key (e.g.
// "spec.steps.build.workdir").
//
// The step's value is a map whose keys are partitioned into:
//   - the common fields (workdir, env, timeout, condition, ignoreFailed,
//     once), which are lifted onto the JobStep;
//   - at most one handler-type key (a key naming a known step handler type),
//     which selects the handler and whose value is the handler's params;
//   - when no handler-type key is present, the step uses the default handler
//     (shell) and the remaining keys are its params.
//
// So `shell: {command: …}` (an explicit type) and a bare `{command: …}` (the
// default handler) both describe a shell step. The shell handler's value may
// also be a plain string: `shell: <string>` is shorthand for
// `shell: {command: <string>}` (only the command param is set).
func CompileStep(file string, base Path, name string, value Value) (*dbpb.JobStep, Errors) {
	if value.Type != Map {
		return nil, Errors{Error{File: file, Key: base.String(),
			Reason: "a step's value must be a mapping (its common fields and a handler-type key, or the default handler's params)"}}
	}

	step := &dbpb.JobStep{Name: name}
	var errs Errors

	// Partition the step's keys: the common fields are lifted onto the
	// JobStep; at most one key may name a handler type (selecting the
	// handler, its value being the handler's params); the rest are
	// param-like keys (the default handler's params when no handler type is
	// present).
	var handlerKey string
	var handlerValue Value
	hasHandler := false
	var paramEntries []Entry

	for _, e := range value.Entries {
		switch {
		case commonStepFields[e.Key]:
			// Lifted onto the JobStep below.
		case isKnownStepType(e.Key):
			if hasHandler {
				errs = append(errs, Error{File: file, Key: base.Extend(e.Key).String(),
					Reason: fmt.Sprintf("a step selects at most one handler (it already selects %q)", handlerKey)})
			} else {
				hasHandler = true
				handlerKey = e.Key
				handlerValue = e.Value
			}
		default:
			paramEntries = append(paramEntries, e)
		}
	}

	// Lift the common fields onto the JobStep.
	errs = append(errs, liftCommonFields(file, base, step, value)...)

	// Select the handler and its params.
	if hasHandler {
		step.Type = handlerKey
		// A step that selects a handler takes its params from the handler's
		// value, not from sibling keys; a sibling param-like key is a
		// validation error (it is ambiguous which handler it belongs to).
		for _, e := range paramEntries {
			errs = append(errs, Error{File: file, Key: base.Extend(e.Key).String(),
				Reason: fmt.Sprintf("a step that selects a handler (%q) takes its params from the handler's value, not from sibling keys", handlerKey)})
		}
		params, pErrs := compileHandlerParams(file, base.Extend(handlerKey), handlerKey, handlerValue)
		step.Params = params
		errs = append(errs, pErrs...)
	} else {
		// No handler-type key: the default handler (shell) is used, and the
		// remaining keys are its params.
		params, pErrs := compileParamEntries(file, base, paramEntries)
		step.Params = params
		errs = append(errs, pErrs...)
	}

	if len(errs) > 0 {
		return nil, errs
	}
	return step, nil
}

// liftCommonFields lifts a step's common fields (workdir, env, timeout,
// condition, ignoreFailed, once) onto step, returning a validation error for
// each field that is present but malformed.
func liftCommonFields(file string, base Path, step *dbpb.JobStep, value Value) Errors {
	var errs Errors
	if v, ok := value.Get("workdir"); ok {
		if v.Type != Scalar {
			errs = append(errs, Error{File: file, Key: base.Extend("workdir").String(), Reason: "workdir must be a string"})
		} else {
			step.Workdir = v.String
		}
	}
	if v, ok := value.Get("env"); ok {
		if m, ok := v.AsStringMap(); ok {
			step.Env = m
		} else {
			errs = append(errs, Error{File: file, Key: base.Extend("env").String(), Reason: "env must be a mapping of string values"})
		}
	}
	if v, ok := value.Get("timeout"); ok {
		if v.Type != Scalar {
			errs = append(errs, Error{File: file, Key: base.Extend("timeout").String(), Reason: "timeout must be a duration string (e.g. 10m, 1h30m)"})
		} else if d, err := time.ParseDuration(v.String); err != nil {
			errs = append(errs, Error{File: file, Key: base.Extend("timeout").String(),
				Reason: fmt.Sprintf("timeout %q is not a valid duration: %v", v.String, err)})
		} else {
			step.Timeout = durationpb.New(d)
		}
	}
	if v, ok := value.Get("condition"); ok {
		if v.Type != Scalar {
			errs = append(errs, Error{File: file, Key: base.Extend("condition").String(), Reason: "condition must be a string (a Go template)"})
		} else {
			step.Condition = v.String
		}
	}
	if v, ok := value.Get("ignoreFailed"); ok {
		if b, ok := v.AsBool(); ok {
			step.IgnoreFailed = b
		} else {
			errs = append(errs, Error{File: file, Key: base.Extend("ignoreFailed").String(), Reason: "ignoreFailed must be a boolean"})
		}
	}
	if v, ok := value.Get("once"); ok {
		if b, ok := v.AsBool(); ok {
			step.Once = b
		} else {
			errs = append(errs, Error{File: file, Key: base.Extend("once").String(), Reason: "once must be a boolean"})
		}
	}
	return errs
}

// compileHandlerParams converts a handler's value (from the file) into the
// handler's params (a name -> ParamValue map). For the shell handler the
// value may be a plain string: `shell: <string>` is shorthand for
// `shell: {command: <string>}` (only the command param is set, nothing else).
// For every other handler — and for a shell handler whose value is a mapping —
// the value is a mapping of params, each a scalar string or a list of strings;
// a param that is a mapping is a validation error, and a non-shell handler's
// value that is not a mapping is a validation error.
func compileHandlerParams(file string, base Path, handlerType string, value Value) (map[string]*dbpb.ParamValue, Errors) {
	// The shell handler's value may be a plain string (the command).
	if handlerType == "shell" && value.Type == Scalar {
		return map[string]*dbpb.ParamValue{"command": {String_: value.String}}, nil
	}
	if value.Type != Map {
		return nil, Errors{Error{File: file, Key: base.String(),
			Reason: "a handler's value must be a mapping of params (each param a scalar string or a list of strings)"}}
	}
	return compileParamEntries(file, base, value.Entries)
}

// compileParamEntries converts a list of param entries (in file order) into a
// name -> ParamValue map, returning a validation error for each param whose
// value is neither a scalar string nor a list of strings. An empty entry list
// yields a nil map.
func compileParamEntries(file string, base Path, entries []Entry) (map[string]*dbpb.ParamValue, Errors) {
	if len(entries) == 0 {
		return nil, nil
	}
	params := make(map[string]*dbpb.ParamValue, len(entries))
	var errs Errors
	for _, e := range entries {
		pv, reason := compileParamValue(e.Value)
		if reason != "" {
			errs = append(errs, Error{File: file, Key: base.Extend(e.Key).String(), Reason: reason})
			continue
		}
		params[e.Key] = pv
	}
	return params, errs
}

// compileParamValue converts a single param's value into a ParamValue: a
// scalar becomes a string param, a list of scalars becomes a list param, and a
// mapping is an error (params are scalars or lists, not mappings). The second
// return is a non-empty reason when the value is malformed.
func compileParamValue(v Value) (*dbpb.ParamValue, string) {
	switch v.Type {
	case Scalar:
		return &dbpb.ParamValue{String_: v.String}, ""
	case List:
		strs := make([]string, 0, len(v.Items))
		for _, item := range v.Items {
			if item.Type != Scalar {
				return nil, "a param's list values must be scalars"
			}
			strs = append(strs, item.String)
		}
		return &dbpb.ParamValue{Strings: strs}, ""
	default:
		return nil, "a param must be a scalar string or a list of strings"
	}
}

// CompileSharedSteps compiles the pipeline's shared steps (the `spec.steps`
// map, a name → step-spec map) into a name → JobStep map, in file order. Each
// shared step is compiled with its own name (so it can be referenced by name),
// and a validation error is returned for each shared step whose spec is
// malformed. file is the name of the offending file (carried into any
// validation errors). A nil or non-map value yields an empty map (the
// pipeline declares no shared steps).
func CompileSharedSteps(file string, value Value) (map[string]*dbpb.JobStep, Errors) {
	out := make(map[string]*dbpb.JobStep)
	if value.Type != Map {
		return out, nil
	}
	var errs Errors
	for _, e := range value.Entries {
		step, stepErrs := CompileStep(file, NewPath("spec", "steps", e.Key), e.Key, e.Value)
		if len(stepErrs) > 0 {
			errs = append(errs, stepErrs...)
			continue
		}
		out[e.Key] = step
	}
	return out, errs
}

// CompileJobSteps compiles a job's `steps` map (a name → step-entry map) into
// an ordered list of JobSteps, in the order the step names appear in the file.
// Each entry is one of:
//   - an inline step spec (a map): compiled in place, the entry's name being
//     the step's name;
//   - a `ref` to a shared step (a map with a `ref` key): the referenced shared
//     step's full spec is used, but the step is stored under the job's local
//     name (the entry's name), not the shared step's name;
//   - an empty entry (a null value): shorthand for `ref: <same name>` — it
//     implicitly references the shared step of the same name as the entry.
//
// shared is the pipeline's shared steps (from CompileSharedSteps); a `ref` or
// empty entry that names a step not in shared is a validation error. A step's
// condition that references a step name that is not a *prior* step in the
// order (a later step, the step itself, or a step that does not exist in the
// job) is a validation error — a step cannot read a later step's output
// (F-21.1.2). file is the name of the offending file, and base is the dotted
// path of the job's steps map (e.g. "spec.jobs.deploy-dev.steps"), so a
// validation error names the offending key (e.g.
// "spec.jobs.deploy-dev.steps.build.ref"). A nil or non-map value yields an
// empty list (the job has no steps).
func CompileJobSteps(file string, base Path, shared map[string]*dbpb.JobStep, value Value) ([]*dbpb.JobStep, Errors) {
	if value.Type != Map {
		return nil, nil
	}
	steps := make([]*dbpb.JobStep, 0, len(value.Entries))
	var errs Errors
	// priorNames accumulates the names of the steps seen so far (in file
	// order), so a step's condition can be validated to reference only prior
	// steps (a step cannot read a later step's output, F-21.1.2).
	priorNames := make(map[string]bool)
	for _, e := range value.Entries {
		name := e.Key
		entryBase := base.Extend(name)

		var step *dbpb.JobStep
		switch {
		case e.Value.IsNull():
			// An empty entry is shorthand for `ref: <same name>`: it
			// implicitly references the shared step of the same name.
			resolved, rErrs := resolveSharedStep(file, entryBase, name, shared)
			if len(rErrs) > 0 {
				errs = append(errs, rErrs...)
			} else {
				resolved.Name = name
				step = resolved
				steps = append(steps, step)
			}
		case e.Value.Type == Map:
			if ref, ok := e.Value.StringValue("ref"); ok {
				// A `ref` to a shared step: the referenced step's full spec
				// is used, but the step is stored under the job's local name.
				resolved, rErrs := resolveSharedStep(file, entryBase.Extend("ref"), ref, shared)
				if len(rErrs) > 0 {
					errs = append(errs, rErrs...)
				} else {
					resolved.Name = name
					step = resolved
					steps = append(steps, step)
				}
			} else {
				// An inline step spec: compiled in place under the entry's
				// name.
				compiled, stepErrs := CompileStep(file, entryBase, name, e.Value)
				if len(stepErrs) > 0 {
					errs = append(errs, stepErrs...)
				} else {
					step = compiled
					steps = append(steps, step)
				}
			}
		default:
			errs = append(errs, Error{File: file, Key: entryBase.String(),
				Reason: "a job's step entry must be an inline step spec, a `ref` to a shared step, or an empty entry (shorthand for `ref: <same name>`)"})
		}

		// Validate the step's condition references only prior steps (a step
		// cannot read a later step's output, F-21.1.2). A step that failed to
		// resolve has no condition to validate.
		if step != nil {
			errs = append(errs, validateStepConditionRefs(file, entryBase, step, priorNames)...)
		}
		// The step's name is now a prior step for subsequent steps, whether or
		// not it resolved (its name is still a step in the order).
		priorNames[name] = true
	}
	return steps, errs
}

// resolveSharedStep looks up ref in shared and returns a copy of the shared
// step (so a job's step never aliases the shared step's JobStep). A ref that
// names a step not in shared is a validation error naming the offending key.
func resolveSharedStep(file string, base Path, ref string, shared map[string]*dbpb.JobStep) (*dbpb.JobStep, Errors) {
	sharedStep, ok := shared[ref]
	if !ok {
		return nil, Errors{Error{File: file, Key: base.String(),
			Reason: fmt.Sprintf("no shared step named %q (a job's step entry may only reference a step declared under spec.steps)", ref)}}
	}
	return proto.Clone(sharedStep).(*dbpb.JobStep), nil
}

// validateStepConditionRefs validates that step's condition (when set)
// references only prior steps: every step name it references via
// `.steps.<name>` must be in priorNames (the steps that appear before this
// step in the order). A reference to a step that is not prior — a later step,
// the step itself, or a step that does not exist in the job — is a validation
// error (a step cannot read a later step's output, F-21.1.2). A condition that
// is not a valid template is also a validation error. A step with no condition
// (or a condition that references no steps) yields no errors.
func validateStepConditionRefs(file string, base Path, step *dbpb.JobStep, priorNames map[string]bool) Errors {
	cond := step.GetCondition()
	if cond == "" {
		return nil
	}
	refs, err := stepRefsInTemplate(cond)
	if err != nil {
		return Errors{Error{File: file, Key: base.Extend("condition").String(),
			Reason: fmt.Sprintf("condition is not a valid template: %v", err)}}
	}
	var errs Errors
	for _, name := range refs {
		if !priorNames[name] {
			errs = append(errs, Error{File: file, Key: base.Extend("condition").String(),
				Reason: fmt.Sprintf("condition references step %q, which is not a prior step (a step cannot read a later step's output)", name)})
		}
	}
	return errs
}

// stepRefsInTemplate parses cond as a Go template and returns the step names
// it references via `.steps.<name>`, in the order first referenced (each name
// once). A condition that is not a valid template is an error. This is how the
// compiler finds a step's references to other steps' outputs (the executor's
// condition context is keyed by step name, F-21.1.2).
func stepRefsInTemplate(cond string) ([]string, error) {
	tmpl, err := template.New("condition").Parse(cond)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var refs []string
	walkStepRefs(tmpl.Tree.Root, seen, &refs)
	return refs, nil
}

// walkStepRefs recursively visits the nodes of a parsed template and records,
// in seen/refs, the step names referenced via `.steps.<name>` (each once, in
// order of first reference).
func walkStepRefs(n parse.Node, seen map[string]bool, refs *[]string) {
	switch n := n.(type) {
	case *parse.ListNode:
		for _, child := range n.Nodes {
			walkStepRefs(child, seen, refs)
		}
	case *parse.ActionNode:
		// A simple action (a field evaluation or parenthesized pipeline)
		// bounded by delimiters, e.g. `{{ .steps.build.Outputs.version }}`.
		if n.Pipe != nil {
			walkStepRefs(n.Pipe, seen, refs)
		}
	case *parse.IfNode:
		walkBranch(&n.BranchNode, seen, refs)
	case *parse.RangeNode:
		walkBranch(&n.BranchNode, seen, refs)
	case *parse.WithNode:
		walkBranch(&n.BranchNode, seen, refs)
	case *parse.PipeNode:
		for _, cmd := range n.Cmds {
			walkStepRefs(cmd, seen, refs)
		}
	case *parse.CommandNode:
		for _, arg := range n.Args {
			walkStepRefs(arg, seen, refs)
		}
	case *parse.FieldNode:
		if name, ok := stepNameOfField(n); ok && !seen[name] {
			seen[name] = true
			*refs = append(*refs, name)
		}
	}
}

// walkBranch visits the pipe, list, and else-list of a branch node (an
// if/range/with construct), which is where the step references in a condition
// live.
func walkBranch(b *parse.BranchNode, seen map[string]bool, refs *[]string) {
	if b.Pipe != nil {
		for _, cmd := range b.Pipe.Cmds {
			walkStepRefs(cmd, seen, refs)
		}
	}
	if b.List != nil {
		for _, child := range b.List.Nodes {
			walkStepRefs(child, seen, refs)
		}
	}
	if b.ElseList != nil {
		for _, child := range b.ElseList.Nodes {
			walkStepRefs(child, seen, refs)
		}
	}
}

// stepNameOfField returns the step name a field references when it is a
// `.steps.<name>` access — the field's identifiers start with "steps" and have
// a second identifier (the step name, e.g. `.steps.build.Outputs.version`
// references the step "build") — reporting whether it is such a reference. A
// bare `.steps` (no step name) or a field whose first identifier is not
// "steps" (e.g. `.params.x`) is not a step reference.
func stepNameOfField(fn *parse.FieldNode) (string, bool) {
	if len(fn.Ident) < 2 || fn.Ident[0] != "steps" {
		return "", false
	}
	return fn.Ident[1], true
}
