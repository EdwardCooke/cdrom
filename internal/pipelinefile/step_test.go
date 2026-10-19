package pipelinefile

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// parseStep parses a step spec (a map from the file) into a Value, so the
// tests can hand CompileStep the same ordered Value the compiler produces.
func parseStep(t *testing.T, yaml string) Value {
	t.Helper()
	doc, err := Parse("doc.yaml", []byte("apiVersion: cdrom/alphav1\nkind: Pipeline\nmetadata:\n  name: p\nspec:\n  steps:\n    s:\n"+indent(yaml, "      ")))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	steps, ok := doc.Spec.Get("steps")
	if !ok {
		t.Fatal("spec has no steps")
	}
	v, ok := steps.Get("s")
	if !ok {
		t.Fatal("steps has no s")
	}
	return v
}

// indent prefixes every non-empty line of s with prefix, so a step spec can be
// nested under a step name in a test document.
func indent(s, prefix string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = prefix + l
		}
	}
	return strings.Join(lines, "\n")
}

// TestCompileStepExplicitHandlerType verifies a step that names a handler type
// (shell) selects that handler and takes its params from the handler's value
// (acceptance criterion: a key naming a registered step handler type selects
// that handler and its value is the handler's params).
func TestCompileStepExplicitHandlerType(t *testing.T) {
	v := parseStep(t, `
shell:
  command: make build
  args: ["-c"]
  shell: sh
workdir: /src
timeout: 10m
env:
  GOFLAGS: -mod=vendor
condition: '{{ .params.environment == "prod" }}'
ignoreFailed: true
once: true`)
	step, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) > 0 {
		t.Fatalf("CompileStep returned errors: %v", errs)
	}
	if step.Type != "shell" {
		t.Errorf("Type = %q, want shell", step.Type)
	}
	if step.Name != "s" {
		t.Errorf("Name = %q, want s", step.Name)
	}
	if step.Workdir != "/src" {
		t.Errorf("Workdir = %q, want /src", step.Workdir)
	}
	if step.Timeout == nil || step.Timeout.AsDuration() != 10*time.Minute {
		t.Errorf("Timeout = %v, want 10m", step.Timeout)
	}
	if step.Env["GOFLAGS"] != "-mod=vendor" {
		t.Errorf("Env[GOFLAGS] = %q, want -mod=vendor", step.Env["GOFLAGS"])
	}
	if step.Condition != `{{ .params.environment == "prod" }}` {
		t.Errorf("Condition = %q, want the template", step.Condition)
	}
	if !step.IgnoreFailed {
		t.Errorf("IgnoreFailed = false, want true")
	}
	if !step.Once {
		t.Errorf("Once = false, want true")
	}
	// The handler's params come from the shell value.
	if got := step.Params["command"]; got == nil || got.GetString_() != "make build" {
		t.Errorf("Params[command] = %v, want make build", got)
	}
	if got := step.Params["args"]; got == nil || len(got.GetStrings()) != 1 || got.GetStrings()[0] != "-c" {
		t.Errorf("Params[args] = %v, want [-c]", got)
	}
	if got := step.Params["shell"]; got == nil || got.GetString_() != "sh" {
		t.Errorf("Params[shell] = %v, want sh", got)
	}
}

// TestCompileStepDefaultHandler verifies a step with no handler-type key uses
// the default handler (shell, an empty Type) and takes the remaining keys as
// its params (acceptance criterion: when no handler-type key is present the
// step uses the default handler and the remaining keys are its params).
func TestCompileStepDefaultHandler(t *testing.T) {
	v := parseStep(t, `
command: make
args: ["lint"]
workdir: /src
timeout: 5m`)
	step, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) > 0 {
		t.Fatalf("CompileStep returned errors: %v", errs)
	}
	if step.Type != "" {
		t.Errorf("Type = %q, want empty (the default handler)", step.Type)
	}
	if got := step.Params["command"]; got == nil || got.GetString_() != "make" {
		t.Errorf("Params[command] = %v, want make", got)
	}
	if got := step.Params["args"]; got == nil || len(got.GetStrings()) != 1 || got.GetStrings()[0] != "lint" {
		t.Errorf("Params[args] = %v, want [lint]", got)
	}
	if step.Workdir != "/src" {
		t.Errorf("Workdir = %q, want /src", step.Workdir)
	}
	if step.Timeout == nil || step.Timeout.AsDuration() != 5*time.Minute {
		t.Errorf("Timeout = %v, want 5m", step.Timeout)
	}
}

// TestCompileStepExplicitAndDefaultEquivalent verifies that `shell: {command:
// …}` (an explicit type) and a bare `{command: …}` (the default handler) both
// describe a shell step: the explicit one sets Type to shell, the default one
// leaves Type empty (which the executor dispatches to the shell handler), and
// both carry the same command param.
func TestCompileStepExplicitAndDefaultEquivalent(t *testing.T) {
	explicit := parseStep(t, `
shell:
  command: make build`)
	def := parseStep(t, `
command: make build`)
	se, eErrs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", explicit)
	if len(eErrs) > 0 {
		t.Fatalf("explicit CompileStep: %v", eErrs)
	}
	sd, dErrs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", def)
	if len(dErrs) > 0 {
		t.Fatalf("default CompileStep: %v", dErrs)
	}
	if se.Type != "shell" {
		t.Errorf("explicit Type = %q, want shell", se.Type)
	}
	if sd.Type != "" {
		t.Errorf("default Type = %q, want empty", sd.Type)
	}
	if se.Params["command"].GetString_() != "make build" || sd.Params["command"].GetString_() != "make build" {
		t.Errorf("command params differ: explicit=%v default=%v", se.Params["command"], sd.Params["command"])
	}
}

// TestCompileStepNonShellHandler verifies a step that names a non-shell
// handler type (token_exchange) selects that handler and takes its params from
// the handler's value.
func TestCompileStepNonShellHandler(t *testing.T) {
	v := parseStep(t, `
token_exchange:
  audience: container-registry
  expires_in: 15m
  output: registry_token
timeout: 1m`)
	step, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) > 0 {
		t.Fatalf("CompileStep returned errors: %v", errs)
	}
	if step.Type != "token_exchange" {
		t.Errorf("Type = %q, want token_exchange", step.Type)
	}
	if got := step.Params["audience"]; got == nil || got.GetString_() != "container-registry" {
		t.Errorf("Params[audience] = %v, want container-registry", got)
	}
	if got := step.Params["output"]; got == nil || got.GetString_() != "registry_token" {
		t.Errorf("Params[output] = %v, want registry_token", got)
	}
}

// TestCompileStepShellStringShorthand verifies the shell handler's value may be
// a plain string: `shell: <string>` is shorthand for `shell: {command:
// <string>}` (acceptance criterion). The compiled step has the shell type and
// a single command param equal to the string.
func TestCompileStepShellStringShorthand(t *testing.T) {
	v := parseStep(t, `
shell: echo "Yes"`)
	step, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) > 0 {
		t.Fatalf("CompileStep returned errors: %v", errs)
	}
	if step.Type != "shell" {
		t.Errorf("Type = %q, want shell", step.Type)
	}
	if len(step.Params) != 1 {
		t.Fatalf("Params = %v, want exactly one param (command)", step.Params)
	}
	if got := step.Params["command"]; got == nil || got.GetString_() != `echo "Yes"` {
		t.Errorf("Params[command] = %v, want echo \"Yes\"", got)
	}
}

// TestCompileStepShellStringShorthandOnlyCommand verifies the shorthand sets
// only the command param — nothing else (no args, no shell param) — so the
// step runs the command through the target OS's default shell.
func TestCompileStepShellStringShorthandOnlyCommand(t *testing.T) {
	v := parseStep(t, `
shell: make build
workdir: /src
timeout: 10m`)
	step, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) > 0 {
		t.Fatalf("CompileStep returned errors: %v", errs)
	}
	if step.Type != "shell" {
		t.Errorf("Type = %q, want shell", step.Type)
	}
	if got := step.Params["command"]; got == nil || got.GetString_() != "make build" {
		t.Errorf("Params[command] = %v, want make build", got)
	}
	if _, ok := step.Params["args"]; ok {
		t.Errorf("Params[args] is set, want it unset (the shorthand sets only command)")
	}
	if _, ok := step.Params["shell"]; ok {
		t.Errorf("Params[shell] is set, want it unset (the shorthand sets only command)")
	}
	// The common fields are still lifted onto the JobStep.
	if step.Workdir != "/src" {
		t.Errorf("Workdir = %q, want /src", step.Workdir)
	}
	if step.Timeout == nil || step.Timeout.AsDuration() != 10*time.Minute {
		t.Errorf("Timeout = %v, want 10m", step.Timeout)
	}
}

// TestCompileStepShellStringShorthandEquivalence verifies `shell: <string>`
// compiles to the same params as `shell: {command: <string>}`.
func TestCompileStepShellStringShorthandEquivalence(t *testing.T) {
	shorthand := parseStep(t, `
shell: make build`)
	explicit := parseStep(t, `
shell:
  command: make build`)
	ss, sErrs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", shorthand)
	if len(sErrs) > 0 {
		t.Fatalf("shorthand CompileStep: %v", sErrs)
	}
	se, eErrs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", explicit)
	if len(eErrs) > 0 {
		t.Fatalf("explicit CompileStep: %v", eErrs)
	}
	if ss.Type != "shell" || se.Type != "shell" {
		t.Errorf("Type = %q / %q, want shell for both", ss.Type, se.Type)
	}
	if len(ss.Params) != 1 || len(se.Params) != 1 {
		t.Fatalf("Params = %v / %v, want exactly one param each", ss.Params, se.Params)
	}
	if ss.Params["command"].GetString_() != se.Params["command"].GetString_() {
		t.Errorf("command params differ: shorthand=%v explicit=%v", ss.Params["command"], se.Params["command"])
	}
}

// TestCompileStepNonShellHandlerStringShorthandNotApplied verifies the plain
// string shorthand applies only to the shell handler: a non-shell handler whose
// value is a plain string is a validation error (its value must be a mapping
// of params).
func TestCompileStepNonShellHandlerStringShorthandNotApplied(t *testing.T) {
	v := parseStep(t, `
approval: just a string`)
	_, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) == 0 {
		t.Fatal("CompileStep accepted a non-shell handler with a string value; want a validation error")
	}
	if !strings.Contains(errs.Error(), "approval") {
		t.Errorf("error %q does not name the offending key (approval)", errs.Error())
	}
}

// TestCompileStepMultipleHandlerTypes verifies a step that names more than one
// handler type is a validation error (a step selects at most one handler).
func TestCompileStepMultipleHandlerTypes(t *testing.T) {
	v := parseStep(t, `
shell:
  command: make
approval:
  message: ok`)
	_, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) == 0 {
		t.Fatal("CompileStep accepted a step with two handler types; want a validation error")
	}
	if !strings.Contains(errs.Error(), "approval") {
		t.Errorf("error %q does not name the offending key (approval)", errs.Error())
	}
}

// TestCompileStepSiblingParamWithHandler verifies a step that selects a
// handler and also carries a sibling param-like key is a validation error (the
// handler's params come from the handler's value, not sibling keys).
func TestCompileStepSiblingParamWithHandler(t *testing.T) {
	v := parseStep(t, `
shell:
  command: make
stray: value`)
	_, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) == 0 {
		t.Fatal("CompileStep accepted a sibling param alongside a handler; want a validation error")
	}
	if !strings.Contains(errs.Error(), "stray") {
		t.Errorf("error %q does not name the offending key (stray)", errs.Error())
	}
}

// TestCompileStepMalformedCommonFields verifies each common field that is
// present but malformed is a validation error naming the offending key.
func TestCompileStepMalformedCommonFields(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		key  string
	}{
		{"workdir", "workdir: [a, b]\ncommand: x", "workdir"},
		{"env", "env:\n  a: [b]\ncommand: x", "env"},
		{"timeout", "timeout: not-a-duration\ncommand: x", "timeout"},
		{"condition", "condition: [a]\ncommand: x", "condition"},
		{"ignoreFailed", "ignoreFailed: maybe\ncommand: x", "ignoreFailed"},
		{"once", "once: maybe\ncommand: x", "once"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := parseStep(t, c.yaml)
			_, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
			if len(errs) == 0 {
				t.Fatalf("CompileStep accepted a malformed %s; want a validation error", c.name)
			}
			if !strings.Contains(errs.Error(), c.key) {
				t.Errorf("error %q does not name the offending key (%s)", errs.Error(), c.key)
			}
		})
	}
}

// TestCompileStepMalformedParam verifies a param whose value is a mapping is a
// validation error (params are scalars or lists, not mappings).
func TestCompileStepMalformedParam(t *testing.T) {
	v := parseStep(t, `
command: make
weird:
  a: b`)
	_, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) == 0 {
		t.Fatal("CompileStep accepted a mapping param; want a validation error")
	}
	if !strings.Contains(errs.Error(), "weird") {
		t.Errorf("error %q does not name the offending key (weird)", errs.Error())
	}
}

// TestCompileStepNonMap verifies a step whose value is not a mapping is a
// validation error.
func TestCompileStepNonMap(t *testing.T) {
	_, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", Value{Type: Scalar, String: "just a string"})
	if len(errs) == 0 {
		t.Fatal("CompileStep accepted a non-mapping step value; want a validation error")
	}
}

// TestCompileStepEmpty verifies a step with no keys compiles to a step with
// the default handler and no params (a no-op step).
func TestCompileStepEmpty(t *testing.T) {
	v := Value{Type: Map}
	step, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) > 0 {
		t.Fatalf("CompileStep returned errors for an empty step: %v", errs)
	}
	if step.Type != "" {
		t.Errorf("Type = %q, want empty", step.Type)
	}
	if step.Params != nil {
		t.Errorf("Params = %v, want nil", step.Params)
	}
}

// TestCompileStepTimeoutRoundTrip verifies the timeout is parsed into a
// duration and round-trips through durationpb.
func TestCompileStepTimeoutRoundTrip(t *testing.T) {
	v := parseStep(t, `
command: make
timeout: 1h30m`)
	step, errs := CompileStep("doc.yaml", NewPath("spec", "steps", "s"), "s", v)
	if len(errs) > 0 {
		t.Fatalf("CompileStep returned errors: %v", errs)
	}
	want := durationpb.New(90 * time.Minute)
	if step.Timeout == nil || step.Timeout.AsDuration() != want.AsDuration() {
		t.Errorf("Timeout = %v, want 1h30m", step.Timeout)
	}
}

// sharedStepsDoc is a pipeline document with two shared steps (a plain-string
// shell step and a full shell step) and a job whose steps mix a `ref`, an
// inline step, and an empty entry (shorthand for `ref: <same name>`).
const sharedStepsDoc = `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  steps:
    step1:
      shell: echo "Yes"
    build:
      shell:
        command: make build
      workdir: /src
      timeout: 10m
  jobs:
    deploy:
      steps:
        a-build:
          ref: build
        lint:
          shell:
            command: make lint
        step1:
`

// parseDoc parses a full pipeline document.
func parseDoc(t *testing.T, yaml string) *Document {
	t.Helper()
	doc, err := Parse("doc.yaml", []byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return doc
}

// compileJobStepsFor parses the document, compiles its shared steps, and
// resolves the named job's steps against them. It returns the shared steps,
// the job's resolved steps, and any validation errors.
func compileJobStepsFor(t *testing.T, yaml, job string) (map[string]*dbpb.JobStep, []*dbpb.JobStep, Errors) {
	t.Helper()
	doc := parseDoc(t, yaml)
	stepsVal, _ := doc.Spec.Get("steps")
	shared, sErrs := CompileSharedSteps("doc.yaml", stepsVal)
	if len(sErrs) > 0 {
		t.Fatalf("CompileSharedSteps: %v", sErrs)
	}
	jobs, _ := doc.Spec.Get("jobs")
	jobSpec, ok := jobs.Get(job)
	if !ok {
		t.Fatalf("no job %q", job)
	}
	jobSteps, _ := jobSpec.Get("steps")
	steps, jErrs := CompileJobSteps("doc.yaml", NewPath("spec", "jobs", job, "steps"), shared, jobSteps)
	return shared, steps, jErrs
}

// TestCompileSharedSteps verifies the pipeline's shared steps (spec.steps, a
// name → step-spec map) compile into a name → JobStep map, each stored under
// its own name (acceptance criterion: shared steps live under spec.steps).
func TestCompileSharedSteps(t *testing.T) {
	doc := parseDoc(t, sharedStepsDoc)
	stepsVal, _ := doc.Spec.Get("steps")
	shared, errs := CompileSharedSteps("doc.yaml", stepsVal)
	if len(errs) > 0 {
		t.Fatalf("CompileSharedSteps returned errors: %v", errs)
	}
	if len(shared) != 2 {
		t.Fatalf("shared = %d steps, want 2", len(shared))
	}
	// step1 is a plain-string shell step: the shorthand sets only command.
	if s := shared["step1"]; s == nil {
		t.Fatal("shared has no step1")
	} else {
		if s.Name != "step1" {
			t.Errorf("step1.Name = %q, want step1", s.Name)
		}
		if s.Type != "shell" {
			t.Errorf("step1.Type = %q, want shell", s.Type)
		}
		if got := s.Params["command"]; got == nil || got.GetString_() != `echo "Yes"` {
			t.Errorf("step1.Params[command] = %v, want echo \"Yes\"", got)
		}
	}
	// build is a full shell step with common fields lifted.
	if s := shared["build"]; s == nil {
		t.Fatal("shared has no build")
	} else {
		if s.Name != "build" {
			t.Errorf("build.Name = %q, want build", s.Name)
		}
		if s.Workdir != "/src" {
			t.Errorf("build.Workdir = %q, want /src", s.Workdir)
		}
		if got := s.Params["command"]; got == nil || got.GetString_() != "make build" {
			t.Errorf("build.Params[command] = %v, want make build", got)
		}
	}
}

// TestCompileJobStepsInline verifies a job's step entry that is an inline step
// spec compiles in place under the entry's name (acceptance criterion: a
// job's step entry may be inline).
func TestCompileJobStepsInline(t *testing.T) {
	_, steps, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  jobs:
    deploy:
      steps:
        lint:
          shell:
            command: make lint
          workdir: /src
`, "deploy")
	if len(errs) > 0 {
		t.Fatalf("CompileJobSteps returned errors: %v", errs)
	}
	if len(steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(steps))
	}
	if steps[0].Name != "lint" {
		t.Errorf("steps[0].Name = %q, want lint", steps[0].Name)
	}
	if steps[0].Type != "shell" {
		t.Errorf("steps[0].Type = %q, want shell", steps[0].Type)
	}
	if steps[0].Workdir != "/src" {
		t.Errorf("steps[0].Workdir = %q, want /src", steps[0].Workdir)
	}
}

// TestCompileJobStepsRef verifies a job's step entry that is a `ref` to a
// shared step uses the referenced step's full spec, stored under the job's
// local name (acceptance criterion: a job's step entry may be a `ref` to a
// shared step, the referenced step's full spec is used).
func TestCompileJobStepsRef(t *testing.T) {
	shared, steps, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  steps:
    build:
      shell:
        command: make build
      workdir: /src
  jobs:
    deploy:
      steps:
        a-build:
          ref: build
`, "deploy")
	if len(errs) > 0 {
		t.Fatalf("CompileJobSteps returned errors: %v", errs)
	}
	if len(steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(steps))
	}
	// The step is stored under the job's local name (a-build), not the shared
	// step's name (build).
	if steps[0].Name != "a-build" {
		t.Errorf("steps[0].Name = %q, want a-build (the job's local name)", steps[0].Name)
	}
	// The referenced step's full spec is used.
	if steps[0].Type != "shell" {
		t.Errorf("steps[0].Type = %q, want shell", steps[0].Type)
	}
	if steps[0].Workdir != "/src" {
		t.Errorf("steps[0].Workdir = %q, want /src (from the shared step)", steps[0].Workdir)
	}
	if got := steps[0].Params["command"]; got == nil || got.GetString_() != "make build" {
		t.Errorf("steps[0].Params[command] = %v, want make build (from the shared step)", got)
	}
	// The shared step is unchanged (the job's step is a copy).
	if shared["build"].Name != "build" {
		t.Errorf("shared[build].Name = %q, want build (the shared step must not be renamed)", shared["build"].Name)
	}
}

// TestCompileJobStepsEmptyEntry verifies a job's step entry that is empty is
// shorthand for `ref: <same name>`: it implicitly references the shared step
// of the same name (acceptance criterion: an empty entry is shorthand for
// `ref: <same name>`).
func TestCompileJobStepsEmptyEntry(t *testing.T) {
	shared, steps, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  steps:
    step1:
      shell: echo "Yes"
  jobs:
    deploy:
      steps:
        step1:
`, "deploy")
	if len(errs) > 0 {
		t.Fatalf("CompileJobSteps returned errors: %v", errs)
	}
	if len(steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(steps))
	}
	if steps[0].Name != "step1" {
		t.Errorf("steps[0].Name = %q, want step1", steps[0].Name)
	}
	// The empty entry resolved to the shared step of the same name.
	if got := steps[0].Params["command"]; got == nil || got.GetString_() != `echo "Yes"` {
		t.Errorf("steps[0].Params[command] = %v, want echo \"Yes\" (from the shared step)", got)
	}
	if shared["step1"] == nil {
		t.Fatal("shared has no step1")
	}
}

// TestCompileJobStepsMixed verifies a job's steps that mix a `ref`, an inline
// step, and an empty entry resolve in file order (the complete example's
// shape: a-build refs build, lint is inline, step1 is an empty entry).
func TestCompileJobStepsMixed(t *testing.T) {
	_, steps, errs := compileJobStepsFor(t, sharedStepsDoc, "deploy")
	if len(errs) > 0 {
		t.Fatalf("CompileJobSteps returned errors: %v", errs)
	}
	if len(steps) != 3 {
		t.Fatalf("steps = %d, want 3", len(steps))
	}
	// File order: a-build, lint, step1.
	wantNames := []string{"a-build", "lint", "step1"}
	for i, want := range wantNames {
		if steps[i].Name != want {
			t.Errorf("steps[%d].Name = %q, want %q (file order must be preserved)", i, steps[i].Name, want)
		}
	}
	// a-build is the shared build step's spec under the local name.
	if steps[0].Workdir != "/src" {
		t.Errorf("a-build.Workdir = %q, want /src (from the shared build step)", steps[0].Workdir)
	}
	// lint is the inline step.
	if got := steps[1].Params["command"]; got == nil || got.GetString_() != "make lint" {
		t.Errorf("lint.Params[command] = %v, want make lint", got)
	}
	// step1 is the shared step1 (plain-string shell) under the same name.
	if got := steps[2].Params["command"]; got == nil || got.GetString_() != `echo "Yes"` {
		t.Errorf("step1.Params[command] = %v, want echo \"Yes\" (from the shared step)", got)
	}
}

// TestCompileJobStepsUnknownRef verifies a `ref` to a shared step that does
// not exist is a validation error naming the offending key (acceptance
// criterion: a name with no matching shared step is a validation error).
func TestCompileJobStepsUnknownRef(t *testing.T) {
	_, _, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  steps:
    build:
      shell:
        command: make build
  jobs:
    deploy:
      steps:
        a-build:
          ref: missing
`, "deploy")
	if len(errs) == 0 {
		t.Fatal("CompileJobSteps accepted a ref to a non-existent shared step; want a validation error")
	}
	if !strings.Contains(errs.Error(), "missing") {
		t.Errorf("error %q does not name the offending ref (missing)", errs.Error())
	}
	if !strings.Contains(errs.Error(), "a-build") {
		t.Errorf("error %q does not name the offending step (a-build)", errs.Error())
	}
}

// TestCompileJobStepsUnknownEmptyEntry verifies an empty entry with no
// matching shared step is a validation error naming the offending key
// (acceptance criterion: a name with no matching shared step is a validation
// error — the empty entry implicitly references the shared step of the same
// name, which does not exist here).
func TestCompileJobStepsUnknownEmptyEntry(t *testing.T) {
	_, _, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  steps:
    build:
      shell:
        command: make build
  jobs:
    deploy:
      steps:
        ghost:
`, "deploy")
	if len(errs) == 0 {
		t.Fatal("CompileJobSteps accepted an empty entry with no matching shared step; want a validation error")
	}
	if !strings.Contains(errs.Error(), "ghost") {
		t.Errorf("error %q does not name the offending step (ghost)", errs.Error())
	}
}

// TestCompileJobStepsNonMap verifies a job's steps that is not a mapping
// yields an empty list (the job has no steps).
func TestCompileJobStepsNonMap(t *testing.T) {
	steps, errs := CompileJobSteps("doc.yaml", NewPath("spec", "jobs", "j", "steps"), nil, Value{Type: Scalar, String: "not-a-map"})
	if len(errs) > 0 {
		t.Fatalf("CompileJobSteps returned errors for a non-map: %v", errs)
	}
	if len(steps) != 0 {
		t.Errorf("steps = %d, want 0", len(steps))
	}
}

// TestCompileJobStepsRefIsCopy verifies a job's step that refs a shared step
// is a copy: mutating the job's step does not affect the shared step (so one
// shared step can be referenced by many jobs without aliasing).
func TestCompileJobStepsRefIsCopy(t *testing.T) {
	shared, steps, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  steps:
    build:
      shell:
        command: make build
  jobs:
    deploy:
      steps:
        a-build:
          ref: build
`, "deploy")
	if len(errs) > 0 {
		t.Fatalf("CompileJobSteps returned errors: %v", errs)
	}
	// Mutate the job's step.
	steps[0].Workdir = "/mutated"
	// The shared step is unchanged.
	if shared["build"].Workdir != "" {
		t.Errorf("shared[build].Workdir = %q, want empty (the job's step must be a copy, not an alias)", shared["build"].Workdir)
	}
}

// TestCompileJobStepsNamesStoredVerbatim verifies the compiler stores each
// step's user-defined name (the map key in the file) on the JobStep's name
// field, unchanged (F-21.1.2): a step's outputs are referenced in the file by
// that name (`{{ .steps.build.Outputs.version }}`), and the same name is what
// the executor's condition context is keyed by — so the stored spec and the
// file both use names, with no name → ordinal rewrite.
func TestCompileJobStepsNamesStoredVerbatim(t *testing.T) {
	_, steps, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  jobs:
    deploy:
      steps:
        build:
          shell:
            command: make build
        test:
          shell:
            command: make test
          condition: '{{ if eq .steps.build.Outputs.version "1.2.3" }}true{{ else }}false{{ end }}'
`, "deploy")
	if len(errs) > 0 {
		t.Fatalf("CompileJobSteps returned errors: %v", errs)
	}
	if len(steps) != 2 {
		t.Fatalf("steps = %d, want 2", len(steps))
	}
	// Each step's name is the file's map key (a user-definable name, not an
	// ordinal), stored verbatim on the JobStep.
	if steps[0].Name != "build" {
		t.Errorf("steps[0].Name = %q, want build (the file's step name, verbatim)", steps[0].Name)
	}
	if steps[1].Name != "test" {
		t.Errorf("steps[1].Name = %q, want test (the file's step name, verbatim)", steps[1].Name)
	}
	// The condition that references the prior step by name is stored unchanged
	// (the name flows through to the executor, which keys the context by it).
	if steps[1].Condition != `{{ if eq .steps.build.Outputs.version "1.2.3" }}true{{ else }}false{{ end }}` {
		t.Errorf("steps[1].Condition = %q, want it unchanged (the name-based reference is not rewritten)", steps[1].Condition)
	}
}

// TestCompileJobStepsConditionPriorStep verifies a step whose condition
// references a *prior* step by name is accepted (acceptance criterion: a
// reference to a step name that is a prior step is valid).
func TestCompileJobStepsConditionPriorStep(t *testing.T) {
	_, steps, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  jobs:
    deploy:
      steps:
        build:
          shell:
            command: make build
        test:
          shell:
            command: make test
          condition: '{{ if eq .steps.build.Outputs.version "1.2.3" }}true{{ else }}false{{ end }}'
`, "deploy")
	if len(errs) > 0 {
		t.Fatalf("CompileJobSteps returned errors for a condition referencing a prior step: %v", errs)
	}
	if len(steps) != 2 {
		t.Fatalf("steps = %d, want 2", len(steps))
	}
}

// TestCompileJobStepsConditionLaterStep verifies a step whose condition
// references a *later* step by name is a validation error (acceptance
// criterion: a reference to a step name that is not a prior step is a
// validation error — a step cannot read a later step's output).
func TestCompileJobStepsConditionLaterStep(t *testing.T) {
	_, _, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  jobs:
    deploy:
      steps:
        test:
          shell:
            command: make test
          condition: '{{ .steps.build.Outputs.version }}'
        build:
          shell:
            command: make build
`, "deploy")
	if len(errs) == 0 {
		t.Fatal("CompileJobSteps accepted a condition referencing a later step; want a validation error")
	}
	if !strings.Contains(errs.Error(), "build") {
		t.Errorf("error %q does not name the referenced step (build)", errs.Error())
	}
	if !strings.Contains(errs.Error(), "not a prior step") {
		t.Errorf("error %q does not explain the step is not prior", errs.Error())
	}
}

// TestCompileJobStepsConditionSelf verifies a step whose condition references
// *itself* by name is a validation error (a step is not prior to itself).
func TestCompileJobStepsConditionSelf(t *testing.T) {
	_, _, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  jobs:
    deploy:
      steps:
        build:
          shell:
            command: make build
          condition: '{{ .steps.build.Outputs.version }}'
`, "deploy")
	if len(errs) == 0 {
		t.Fatal("CompileJobSteps accepted a condition referencing the step itself; want a validation error")
	}
	if !strings.Contains(errs.Error(), "build") {
		t.Errorf("error %q does not name the referenced step (build)", errs.Error())
	}
}

// TestCompileJobStepsConditionUnknownStep verifies a step whose condition
// references a step that does not exist in the job is a validation error.
func TestCompileJobStepsConditionUnknownStep(t *testing.T) {
	_, _, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  jobs:
    deploy:
      steps:
        build:
          shell:
            command: make build
          condition: '{{ .steps.ghost.Outputs.version }}'
`, "deploy")
	if len(errs) == 0 {
		t.Fatal("CompileJobSteps accepted a condition referencing a step that does not exist; want a validation error")
	}
	if !strings.Contains(errs.Error(), "ghost") {
		t.Errorf("error %q does not name the referenced step (ghost)", errs.Error())
	}
}

// TestCompileJobStepsConditionInvalidTemplate verifies a step whose condition
// is not a valid template is a validation error.
func TestCompileJobStepsConditionInvalidTemplate(t *testing.T) {
	_, _, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  jobs:
    deploy:
      steps:
        build:
          shell:
            command: make build
          condition: '{{ .steps.build.Outputs.version'
`, "deploy")
	if len(errs) == 0 {
		t.Fatal("CompileJobSteps accepted a condition that is not a valid template; want a validation error")
	}
	if !strings.Contains(errs.Error(), "not a valid template") {
		t.Errorf("error %q does not say the condition is not a valid template", errs.Error())
	}
}

// TestCompileJobStepsConditionNoStepRefs verifies a step whose condition
// references no steps (e.g. only run params) is accepted regardless of order.
func TestCompileJobStepsConditionNoStepRefs(t *testing.T) {
	_, steps, errs := compileJobStepsFor(t, `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: p
spec:
  jobs:
    deploy:
      steps:
        build:
          shell:
            command: make build
          condition: '{{ eq .params.environment "prod" }}'
`, "deploy")
	if len(errs) > 0 {
		t.Fatalf("CompileJobSteps returned errors for a condition referencing no steps: %v", errs)
	}
	if len(steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(steps))
	}
}
