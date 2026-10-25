package pipelinefile

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cdrom/internal/sourceref"
)

// fakeFetcher is an in-memory Fetcher: it serves the YAML stored under each
// full reference string and errors on an unknown reference.
type fakeFetcher struct {
	files map[string]string
}

func (f *fakeFetcher) fetch(ctx context.Context, reference string) ([]byte, error) {
	b, ok := f.files[reference]
	if !ok {
		return nil, fmt.Errorf("no such file: %s", reference)
	}
	return []byte(b), nil
}

// The main file's source reference (a git source) that relative import
// locators resolve against.
const testRoot = "git:https://github.com/org/repo.git@main[/pipelines/webapp.yaml]"

// testRootRef parses testRoot.
func testRootRef(t *testing.T) *sourceref.Reference {
	t.Helper()
	ref, err := sourceref.ParseReference(testRoot)
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}
	return ref
}

// resolveMain runs the resolver over the given pipeline YAML with the given
// fetcher and root reference.
func resolveMain(t *testing.T, pipelineYAML string, fetcher Fetcher) (*Document, map[string][]VaultImport, Errors) {
	t.Helper()
	r := NewResolver(context.Background(), fetcher, testRootRef(t))
	doc, vi, errs := r.ResolveMain("webapp.yaml", []byte(pipelineYAML))
	return doc, vi, errs
}

// stepFile renders a Step manifest whose spec is the given step YAML.
func stepFile(name, specYAML string) string {
	return "apiVersion: cdrom/alphav1\nkind: Step\nmetadata:\n  name: " + name + "\nspec:\n" + indent(specYAML, "  ")
}

// jobFile renders a Job manifest whose spec is the given job YAML.
func jobFile(name, specYAML string) string {
	return "apiVersion: cdrom/alphav1\nkind: Job\nmetadata:\n  name: " + name + "\nspec:\n" + indent(specYAML, "  ")
}

// variableSetFile renders a VariableSet manifest whose spec is the given set
// YAML.
func variableSetFile(name, specYAML string) string {
	return "apiVersion: cdrom/alphav1\nkind: VariableSet\nmetadata:\n  name: " + name + "\nspec:\n" + indent(specYAML, "  ")
}

// TestImportStepSplicesAndOverrides verifies a shared step pulled in with
// importFrom is spliced in place (the imported step's spec becomes the step's
// definition) and any local settings override the imported step's (acceptance
// criteria: a pipeline can pull in steps from other files; an imported unit is
// spliced in place and validated as if it were inline).
func TestImportStepSplicesAndOverrides(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/shared/steps/unit-test.yaml]": stepFile("unit-test", `
shell:
  command: make test
workdir: /src
timeout: 5m`),
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    build:
      shell: make build
    unit-test:
      importFrom: ../shared/steps/unit-test.yaml
      workdir: /opt/src
  jobs:
    test:
      steps:
        ut:
          ref: unit-test
`
	doc, _, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) > 0 {
		t.Fatalf("ResolveMain: %v", errs)
	}
	// The importFrom key is consumed; the step is the spliced definition.
	stepsVal, _ := doc.Spec.Get("steps")
	unitTest, ok := stepsVal.Get("unit-test")
	if !ok {
		t.Fatal("resolved spec has no unit-test step")
	}
	if _, ok := unitTest.Get("importFrom"); ok {
		t.Error("importFrom was not consumed by the splice")
	}
	// The imported command survived; the local workdir won over the imported.
	command, _ := unitTest.Get("shell")
	cmd, _ := command.Get("command")
	if cmd.String != "make test" {
		t.Errorf("unit-test.command = %q, want the imported 'make test'", cmd.String)
	}
	workdir, _ := unitTest.Get("workdir")
	if workdir.String != "/opt/src" {
		t.Errorf("unit-test.workdir = %q, want the local override '/opt/src'", workdir.String)
	}
	timeout, _ := unitTest.Get("timeout")
	if timeout.String != "5m" {
		t.Errorf("unit-test.timeout = %q, want the imported '5m'", timeout.String)
	}
	// Validated as if inline: the spliced step compiles, and a job can ref it.
	shared, sErrs := CompileSharedSteps("webapp.yaml", stepsVal)
	if len(sErrs) > 0 {
		t.Fatalf("CompileSharedSteps(spliced): %v", sErrs)
	}
	if shared["unit-test"] == nil || shared["unit-test"].GetType() != "shell" {
		t.Errorf("spliced unit-test did not compile to a shell step: %+v", shared["unit-test"])
	}
	jobsVal, _ := doc.Spec.Get("jobs")
	testJob, _ := jobsVal.Get("test")
	jobSteps, _ := testJob.Get("steps")
	steps, jErrs := CompileJobSteps("webapp.yaml", NewPath("spec", "jobs", "test", "steps"), shared, jobSteps)
	if len(jErrs) > 0 {
		t.Fatalf("CompileJobSteps(ref to imported step): %v", jErrs)
	}
	if len(steps) != 1 || steps[0].GetName() != "ut" {
		t.Fatalf("job steps = %+v, want one step named ut", steps)
	}
}

// TestRefToImportedSharedStep verifies a job's ref (and empty-entry shorthand)
// can name a shared step that arrived via importFrom (acceptance criterion: a
// ref must name a step that is defined — inline, shared, or imported).
func TestRefToImportedSharedStep(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/shared/steps/integrate.yaml]": stepFile("integrate", `
shell:
  command: make integrate
workdir: /ws`),
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    integrate:
      importFrom: ../shared/steps/integrate.yaml
  jobs:
    ci:
      steps:
        run-integration:
          ref: integrate
        integrate:
`
	doc, _, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) > 0 {
		t.Fatalf("ResolveMain(ref to imported step): %v", errs)
	}
	stepsVal, _ := doc.Spec.Get("steps")
	shared, sErrs := CompileSharedSteps("webapp.yaml", stepsVal)
	if len(sErrs) > 0 {
		t.Fatalf("CompileSharedSteps: %v", sErrs)
	}
	integrated, ok := shared["integrate"]
	if !ok {
		t.Fatal("no shared step 'integrate' (the imported one)")
	}
	if integrated.GetType() != "shell" {
		t.Errorf("integrate.type = %q, want shell", integrated.GetType())
	}
	jobsVal, _ := doc.Spec.Get("jobs")
	ci, _ := jobsVal.Get("ci")
	jobStepsVal, _ := ci.Get("steps")
	steps, jErrs := CompileJobSteps("webapp.yaml", NewPath("spec", "jobs", "ci", "steps"), shared, jobStepsVal)
	if len(jErrs) > 0 {
		t.Fatalf("CompileJobSteps: %v", jErrs)
	}
	if len(steps) != 2 {
		t.Fatalf("ci steps = %d, want 2 (ref + empty-entry shorthand)", len(steps))
	}
	if steps[0].GetName() != "run-integration" || steps[0].GetWorkdir() != "/ws" {
		t.Errorf("run-integration = %+v, want the imported step's spec under the local name", steps[0])
	}
	if steps[1].GetName() != "integrate" {
		t.Errorf("integrate = %+v, want the empty-entry shorthand to resolve to the imported step", steps[1])
	}
}

// TestImportStepTransitive verifies an imported file may itself use importFrom
// to pull in another (transitive imports, acceptance criterion).
func TestImportStepTransitive(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/shared/steps/unit-test.yaml]": stepFile("unit-test", `
importFrom: ../../base/test-base.yaml
workdir: /src`),
		"git:https://github.com/org/repo.git@main[/base/test-base.yaml]": stepFile("test-base", `
shell:
  command: make test`),
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    unit-test:
      importFrom: ../shared/steps/unit-test.yaml
`
	doc, _, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) > 0 {
		t.Fatalf("ResolveMain(transitive): %v", errs)
	}
	stepsVal, _ := doc.Spec.Get("steps")
	unitTest, _ := stepsVal.Get("unit-test")
	command, _ := unitTest.Get("shell")
	cmd, _ := command.Get("command")
	if cmd.String != "make test" {
		t.Errorf("transitive unit-test.command = %q, want 'make test' (two hops)", cmd.String)
	}
	workdir, _ := unitTest.Get("workdir")
	if workdir.String != "/src" {
		t.Errorf("transitive unit-test.workdir = %q, want '/src' (mid-hop override)", workdir.String)
	}
}

// TestImportCycleDetected verifies a cycle of imports is a validation error
// (acceptance criterion).
func TestImportCycleDetected(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/a.yaml]": stepFile("a", `
importFrom: ../b.yaml`),
		"git:https://github.com/org/repo.git@main[/b.yaml]": stepFile("b", `
importFrom: ../a.yaml`),
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    a:
      importFrom: ../a.yaml
`
	_, _, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) == 0 {
		t.Fatal("ResolveMain accepted an import cycle; want a validation error")
	}
	found := false
	for _, e := range errs {
		if strings.Contains(e.Reason, "cycle") {
			found = true
		}
	}
	if !found {
		t.Errorf("no error names the cycle: %v", errs)
	}
}

// TestImportSelfCycleDetected verifies a file that (transitively) imports
// itself is a cycle.
func TestImportSelfCycleDetected(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/loop.yaml]": stepFile("loop", `
importFrom: loop.yaml`),
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    loop:
      importFrom: ../loop.yaml
`
	_, _, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) == 0 {
		t.Fatal("ResolveMain accepted a self-cycle; want a validation error")
	}
}

// TestImportStepWrongKind verifies pulling a non-Step file into a step is a
// validation error naming the kind (acceptance criterion: a file's kind tells
// the compiler how to read it).
func TestImportStepWrongKind(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/wrong.yaml]": jobFile("wrong", `
steps:
  s:
    shell: echo hi`),
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    s:
      importFrom: ../wrong.yaml
`
	_, _, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) == 0 {
		t.Fatal("ResolveMain accepted a Job file as a step import; want a validation error")
	}
	if !strings.Contains(errs[0].Reason, "Step document") {
		t.Errorf("error does not name the expected kind: %v", errs[0])
	}
}

// TestImportStepNoFetcher verifies a document with an import but no fetcher
// fails with a clear error (acceptance criterion: imports are resolved via the
// source-fetcher).
func TestImportStepNoFetcher(t *testing.T) {
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    s:
      importFrom: ../shared/steps/s.yaml
`
	_, _, errs := resolveMain(t, pipeline, nil)
	if len(errs) == 0 {
		t.Fatal("ResolveMain with no fetcher accepted an import; want a validation error")
	}
	if !strings.Contains(errs[0].Reason, "no source-fetcher") {
		t.Errorf("error does not explain the missing fetcher: %v", errs[0])
	}
}

// jobFileDeployDev is the deployed job file: its spec has needs /
// environment / workerGroup / steps (step1, step2) / variables /
// variableSets.
const jobFileDeployDev = `
apiVersion: cdrom/alphav1
kind: Job
metadata:
  name: deploy-dev
spec:
  needs: [build]
  environment: dev
  workerGroup: webapp-workers
  steps:
    step1:
      shell: echo "imported step1"
    step2:
      shell: echo "imported step2"
  variables:
    canary: "true"
  variableSets:
    - dev
`

// TestImportJobSplicesAndOverrides verifies a whole job pulled in with
// importFrom: the imported job's spec is the base, local fields override it,
// and a local steps map overrides the imported job's steps by name (accepted
// criteria: a job can be pulled in from another file; local steps win).
func TestImportJobSplicesAndOverrides(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/jobs/deploy-dev.yaml]": jobFileDeployDev,
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  variableSets:
    dev:
      variables:
        region: eu-west
  jobs:
    build:
      steps:
        b:
          shell: make build
    deploy-dev:
      importFrom: ../jobs/deploy-dev.yaml
      environment: staging
      steps:
        step2:
          shell: echo "locally overridden step2"
`
	doc, _, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) > 0 {
		t.Fatalf("ResolveMain(job import): %v", errs)
	}
	jobsVal, _ := doc.Spec.Get("jobs")
	dd, ok := jobsVal.Get("deploy-dev")
	if !ok {
		t.Fatal("resolved spec has no deploy-dev job")
	}
	if _, ok := dd.Get("importFrom"); ok {
		t.Error("importFrom was not consumed by the job splice")
	}
	// Local fields override the imported job's; imported fields survive.
	env, _ := dd.Get("environment")
	if env.String != "staging" {
		t.Errorf("deploy-dev.environment = %q, want the local override 'staging'", env.String)
	}
	wg, _ := dd.Get("workerGroup")
	if wg.String != "webapp-workers" {
		t.Errorf("deploy-dev.workerGroup = %q, want the imported 'webapp-workers'", wg.String)
	}
	needs, _ := dd.Get("needs")
	if !(needs.Type == List && len(needs.Items) == 1 && needs.Items[0].String == "build") {
		t.Errorf("deploy-dev.needs lost the imported value: %+v", needs)
	}
	vars, _ := dd.Get("variables")
	if cv, _ := vars.Get("canary"); cv.String != "true" {
		t.Errorf("deploy-dev.variables.canary = %q, want the imported 'true'", cv.String)
	}
	// Steps merge by name: the imported step1 survives; the local step2
	// overrides the imported step2 (count proves the merge, not replacement).
	steps, _ := dd.Get("steps")
	if steps.Type != Map || len(steps.Entries) != 2 {
		t.Fatalf("deploy-dev.steps = %+v, want 2 entries (imported step1 + local step2)", steps)
	}
	if _, ok := steps.Get("step1"); !ok {
		t.Error("deploy-dev.steps lost the imported step1")
	}
}

// TestImportJobStepsPullIn verifies a job's steps can be pulled in indirectly:
// a job file whose steps carry their own importFrom (resolved relative to the
// job file) is spliced in with those steps resolved.
func TestImportJobStepsPullIn(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/jobs/big.yaml]": jobFile("big", `
steps:
  lint:
    importFrom: ../shared/steps/lint.yaml
  local:
    shell: echo local`),
		"git:https://github.com/org/repo.git@main[/shared/steps/lint.yaml]": stepFile("lint", `
shell:
  command: make lint`),
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  jobs:
    big:
      importFrom: ../jobs/big.yaml
`
	doc, _, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) > 0 {
		t.Fatalf("ResolveMain(job steps importFrom): %v", errs)
	}
	jobsVal, _ := doc.Spec.Get("jobs")
	big, _ := jobsVal.Get("big")
	steps, _ := big.Get("steps")
	lint, _ := steps.Get("lint")
	if _, ok := lint.Get("importFrom"); ok {
		t.Error("job-file step importFrom was not consumed")
	}
	sh, _ := lint.Get("shell")
	cmd, _ := sh.Get("command")
	if cmd.String != "make lint" {
		t.Errorf("lint.command = %q, want 'make lint' (pulled in via the job file)", cmd.String)
	}
}

// TestImportVariableSetFrom verifies a variable set pulls in another file's
// variables with `import: [{from: <uri>}]` (union, inline wins on collision),
// and the imported file's secretSources bubble up into the set (acceptance
// criteria: a variable set can import from external files; a set file's
// secretSources merge into the pipeline's namespace).
func TestImportVariableSetFrom(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/variablesets/dev.yaml]": variableSetFile("dev", `
variables:
  region: eu-west
  replicas: "2"
  overlap: from-file
secretSources:
  dev-extra:
    kind: AES
    path: enc-dev-extra`),
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  variableSets:
    dev:
      variables:
        overlap: from-inline
      import:
        - from: ../variablesets/dev.yaml
`
	doc, _, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) > 0 {
		t.Fatalf("ResolveMain(varset import): %v", errs)
	}
	setsVal, _ := doc.Spec.Get("variableSets")
	dev, _ := setsVal.Get("dev")
	if _, ok := dev.Get("import"); ok {
		t.Error("import key was not consumed")
	}
	vars, _ := dev.Get("variables")
	overlap, _ := vars.Get("overlap")
	if overlap.String != "from-inline" {
		t.Errorf("dev.overlap = %q, want the inline value (inline wins)", overlap.String)
	}
	region, _ := vars.Get("region")
	if region.String != "eu-west" {
		t.Errorf("dev.region = %q, want the imported 'eu-west'", region.String)
	}
	replicas, _ := vars.Get("replicas")
	if replicas.String != "2" {
		t.Errorf("dev.replicas = %q, want the imported '\"2\"'", replicas.String)
	}
	// The imported file's secretSources bubbled up into the set.
	srcs, _ := dev.Get("secretSources")
	extra, ok := srcs.Get("dev-extra")
	if !ok {
		t.Fatal("dev.secretSources lacks the imported dev-extra")
	}
	if kind, _ := extra.Get("kind"); kind.String != "AES" {
		t.Errorf("dev-extra.kind = %q, want AES", kind.String)
	}
	// Compiles as if inline (the import key is gone).
	compiled, cErrs := CompileVariableSets("webapp.yaml", setsVal)
	if len(cErrs) > 0 {
		t.Fatalf("CompileVariableSets(resolved): %v", cErrs)
	}
	if cs := compiled["dev"]; cs == nil || len(cs.Variables) != 3 {
		t.Errorf("compiled dev set = %+v, want 3 variables", cs)
	}
}

// TestImportVariableSetPerKeyVault verifies `import: [{vault: <name>, keys:
// […]}]` materializes one VariableSecretSource variable per key (acceptance
// criterion: a subset of a vault's keys).
func TestImportVariableSetPerKeyVault(t *testing.T) {
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  secretSources:
    prod-secrets:
      kind: AES
      path: enc-json
  variableSets:
    dev:
      import:
        - vault: prod-secrets
          keys:
            - deploy_token
            - db_password
`
	doc, _, errs := resolveMain(t, pipeline, nil) // no fetch needed (vaults are local)
	if len(errs) > 0 {
		t.Fatalf("ResolveMain(per-key vault): %v", errs)
	}
	setsVal, _ := doc.Spec.Get("variableSets")
	dev, _ := setsVal.Get("dev")
	vars, _ := dev.Get("variables")
	dt, _ := vars.Get("deploy_token")
	ss, _ := dt.Get("secretSource")
	k, _ := dt.Get("key")
	if ss.String != "prod-secrets" || k.String != "deploy_token" {
		t.Errorf("deploy_token = {%q,%q}, want {prod-secrets, deploy_token}", ss.String, k.String)
	}
	dp, _ := vars.Get("db_password")
	if dpSS, _ := dp.Get("secretSource"); dpSS.String != "prod-secrets" {
		t.Errorf("db_password.secretSource = %q, want prod-secrets", dpSS.String)
	}
	// Materialized variables compile to masked sourced variables.
	compiled, cErrs := CompileVariableSets("webapp.yaml", setsVal)
	if len(cErrs) > 0 {
		t.Fatalf("CompileVariableSets(per-key vault): %v", cErrs)
	}
	cs := compiled["dev"]
	if len(cs.Variables) != 2 {
		t.Fatalf("dev.Variables = %d, want 2", len(cs.Variables))
	}
	for _, v := range cs.Variables {
		if v.Value.Shape != VariableSecretSource || v.Value.SecretSource != "prod-secrets" {
			t.Errorf("variable %q = %+v, want a masked prod-secrets sourced value", v.Name, v.Value)
		}
	}
}

// TestImportVariableSetEveryKeyVault verifies `import: [{vault: <name>}]`
// (no keys) records an every-key vault import (dynamic, expanded at dispatch)
// rather than materializing variables at compile time (acceptance criterion:
// every key in the vault, as masked variables).
func TestImportVariableSetEveryKeyVault(t *testing.T) {
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  secretSources:
    prod-secrets:
      kind: AES
      path: enc-json
  variableSets:
    dev:
      variables:
        region: eu-west
      import:
        - vault: prod-secrets
`
	doc, vi, errs := resolveMain(t, pipeline, nil)
	if len(errs) > 0 {
		t.Fatalf("ResolveMain(every-key vault): %v", errs)
	}
	// The set's inline variables remain; no phantom variables were added.
	setsVal, _ := doc.Spec.Get("variableSets")
	dev, _ := setsVal.Get("dev")
	vars, _ := dev.Get("variables")
	if len(vars.Entries) != 1 {
		t.Errorf("dev.Variables = %d entries, want only the inline region", len(vars.Entries))
	}
	// The every-key import is recorded (by set name).
	got, ok := vi["dev"]
	if !ok || len(got) != 1 || got[0].Source != "prod-secrets" || len(got[0].Keys) != 0 {
		t.Errorf("vaultImports[dev] = %+v, want [{prod-secrets, no keys}]", got)
	}
}

// TestImportVariableSetVaultBubblesUp verifies an imported VariableSet file's
// own every-key vault import bubbles up into the importing set (transitive
// vault imports).
func TestImportVariableSetVaultBubblesUp(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/variablesets/base.yaml]": variableSetFile("base", `
variables:
  flavor: vanilla
import:
  - vault: base-vault`),
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  secretSources:
    base-vault:
      kind: AES
      path: enc-json
  variableSets:
    combo:
      import:
        - from: ../variablesets/base.yaml
`
	_, vi, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) > 0 {
		t.Fatalf("ResolveMain(bubble-up vault): %v", errs)
	}
	got, ok := vi["combo"]
	if !ok || len(got) != 1 || got[0].Source != "base-vault" {
		t.Errorf("vaultImports[combo] = %+v, want the bubbled-up [{base-vault}]", got)
	}
}

// TestImportVariableSetBadEntry verifies a malformed import entry (neither
// from nor vault) is a validation error naming the entry.
func TestImportVariableSetBadEntry(t *testing.T) {
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  variableSets:
    dev:
      import:
        - bogus: yes
`
	_, _, errs := resolveMain(t, pipeline, nil)
	if len(errs) == 0 {
		t.Fatal("ResolveMain accepted a malformed import entry; want a validation error")
	}
	if !strings.Contains(errs[0].Reason, "from or vault") {
		t.Errorf("error does not name the allowed keys: %v", errs[0])
	}
}

// TestImportVariableSetWrongKind verifies pulling a non-VariableSet file into
// a set's import is a validation error.
func TestImportVariableSetWrongKind(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{
		"git:https://github.com/org/repo.git@main[/variablesets/notaset.yaml]": stepFile("notaset", `
shell: echo nope`),
	}}
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  variableSets:
    dev:
      import:
        - from: ../variablesets/notaset.yaml
`
	_, _, errs := resolveMain(t, pipeline, fetcher.fetch)
	if len(errs) == 0 {
		t.Fatal("ResolveMain accepted a Step file as a variable-set import; want a validation error")
	}
	if !strings.Contains(errs[0].Reason, "VariableSet document") {
		t.Errorf("error does not name the expected kind: %v", errs[0])
	}
}

// TestImportRelativeRequiresSource verifies a relative locator in a document
// with no source reference (nil root) fails with an actionable hint (use a
// full reference or attach a source).
func TestImportRelativeRequiresSource(t *testing.T) {
	fetcher := &fakeFetcher{files: map[string]string{}}
	r := NewResolver(context.Background(), fetcher.fetch, nil)
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    s:
      importFrom: ../shared/steps/s.yaml
`
	_, _, errs := r.ResolveMain("webapp.yaml", []byte(pipeline))
	if len(errs) == 0 {
		t.Fatal("ResolveMain accepted a relative locator without a source; want a validation error")
	}
	if !strings.Contains(errs[0].Reason, "full reference") {
		t.Errorf("error does not advise using a full reference: %v", errs[0])
	}
}

// TestImportFullReferenceWorks verifies a full reference (not relative)
// resolves even when the main file has no source.
func TestImportFullReferenceWorks(t *testing.T) {
	const full = "git:https://github.com/org/repo.git@main[/shared/steps/full.yaml]"
	fetcher := &fakeFetcher{files: map[string]string{
		full: stepFile("full", `
shell:
  command: make full`),
	}}
	r := NewResolver(context.Background(), fetcher.fetch, nil)
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    full:
      importFrom: ` + full + `
`
	doc, _, errs := r.ResolveMain("webapp.yaml", []byte(pipeline))
	if len(errs) > 0 {
		t.Fatalf("ResolveMain(full reference): %v", errs)
	}
	stepsVal, _ := doc.Spec.Get("steps")
	fullStep, _ := stepsVal.Get("full")
	sh, _ := fullStep.Get("shell")
	cmd, _ := sh.Get("command")
	if cmd.String != "make full" {
		t.Errorf("full.command = %q, want 'make full'", cmd.String)
	}
}

// TestImportMemoized verifies a file imported by several units is fetched
// once (memoization): the fetcher observes a single call for the shared file.
func TestImportMemoized(t *testing.T) {
	const shared = "git:https://github.com/org/repo.git@main[/shared/steps/once.yaml]"
	counts := map[string]int{}
	fetcher := Fetcher(func(ctx context.Context, reference string) ([]byte, error) {
		counts[reference]++
		return []byte(stepFile("once", "\nshell: echo once")), nil
	})
	pipeline := `
apiVersion: cdrom/alphav1
kind: Pipeline
metadata:
  name: webapp
spec:
  steps:
    a:
      importFrom: ../shared/steps/once.yaml
    b:
      importFrom: ../shared/steps/once.yaml
`
	_, _, errs := resolveMain(t, pipeline, fetcher)
	if len(errs) > 0 {
		t.Fatalf("ResolveMain(memoized): %v", errs)
	}
	if n := counts[shared]; n != 1 {
		t.Errorf("shared file fetched %d times, want 1 (memoized)", n)
	}
}
